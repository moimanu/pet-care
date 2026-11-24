"""Processo cliente Pyro5 RPC: consenso por quorum 2f+1, failover transparente e push UI."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
import uuid

import Pyro5.api

from app.config import ALL_NODES, CLIENT_OBJECT_NAME, CLIENT_OBSERVER_NAME, CLIENT_PORT, MIN_QUORUM, peer_uri
from app.domain import RpcError
from app.event_bus import emit_event
from app.infra import ObserverRegistry, PushReceiver, install_shutdown_handler, rpc_proxy, suppress_rpc_errors

DEBOUNCE_SECONDS = 0.08


def evaluate_quorum(results: list[dict]) -> dict:
    """Funcao pura que apura o consenso 2f+1 (maioria >= MIN_QUORUM) sobre as respostas dos nos."""
    def no_quorum(reason: str) -> dict:
        return {"quorum_reached": False, "reason": reason, "nodes": results, "state": None, "divergent": []}

    valid = [item for item in results if item["ok"]]
    if len(valid) < MIN_QUORUM:
        return no_quorum(f"Quorum indisponivel: apenas {len(valid)}/{len(ALL_NODES)} nos ativos")

    majority_hash, majority_count = Counter(item["data"]["hash"] for item in valid).most_common(1)[0]
    if majority_count < MIN_QUORUM:
        return no_quorum("Sem consenso 2f+1: todas as respostas divergem")

    majority_nodes = [item for item in valid if item["data"]["hash"] == majority_hash]
    divergent_ids = [item["node_id"] for item in valid if item["data"]["hash"] != majority_hash]
    return {
        "quorum_reached": True,
        "majority_hash": majority_hash,
        "votes": f"{majority_count}/{len(valid)}",
        "state": majority_nodes[0]["data"],
        "divergent": divergent_ids,
        "nodes": results,
    }


@Pyro5.api.expose
class ClusterRpcClient:
    """Servico cliente RPC com leitura por quorum (2f+1), destituicao de lider e failover."""

    def __init__(self) -> None:
        self._last_divergent: tuple[int, ...] = ()
        self._last_consensus: dict | None = None
        self._observer_uri: str | None = None
        self._ui_observers = ObserverRegistry()
        self._push_lock = threading.Lock()
        self._debounce_timer: threading.Timer | None = None

    def set_observer_uri(self, uri: str) -> None:
        self._observer_uri = uri

    def register_ui_observer(self, ui_uri: str) -> bool:
        return self._ui_observers.register(ui_uri)

    def unregister_ui_observer(self, ui_uri: str) -> bool:
        return self._ui_observers.unregister(ui_uri)

    def register_observer_on_node(self, node_id: int) -> bool:
        if not self._observer_uri:
            return False
        try:
            with rpc_proxy(peer_uri(node_id)) as proxy:
                return bool(proxy.register_observer(self._observer_uri))
        except Exception:
            return False

    def register_all_observers(self) -> None:
        if self._observer_uri:
            with ThreadPoolExecutor(max_workers=len(ALL_NODES)) as pool:
                list(pool.map(self.register_observer_on_node, ALL_NODES))

    def unregister_all_observers(self) -> None:
        with self._push_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
                self._debounce_timer = None
        if not self._observer_uri:
            return
        for node_id in ALL_NODES:
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(node_id)) as proxy:
                    proxy.unregister_observer(self._observer_uri)

    def handle_observer_push(self, _payload: dict) -> None:
        """Agrupa notificacoes simultaneas das replicas (debounce) antes de recalcular o consenso."""
        with self._push_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
            self._debounce_timer = threading.Timer(DEBOUNCE_SECONDS, self._evaluate_and_push)
            self._debounce_timer.daemon = True
            self._debounce_timer.start()

    def _evaluate_and_push(self) -> None:
        self._ui_observers.notify(self.get_consensus_state(log_result=False, enforce_leader=True))

    def _query_all_nodes(self, method: str) -> list[dict]:
        """Executa `method` (ping/get_state) em todos os controladores em paralelo."""
        def query(node_id: int) -> dict:
            try:
                with rpc_proxy(peer_uri(node_id)) as proxy:
                    return {"node_id": node_id, "ok": True, "data": getattr(proxy, method)()}
            except Exception as exc:
                return {"node_id": node_id, "ok": False, "error": str(exc)}

        with ThreadPoolExecutor(max_workers=len(ALL_NODES)) as pool:
            return list(pool.map(query, ALL_NODES))

    def get_nodes_status(self) -> list[dict]:
        return self._query_all_nodes("ping")

    def get_last_consensus(self) -> dict | None:
        return self._last_consensus

    def get_consensus_state(self, log_result: bool = True, enforce_leader: bool = True) -> dict:
        """Consulta o cluster, valida quorum 2f+1 e destitui o lider caso ele esteja divergente."""
        results = self._query_all_nodes("get_state")
        consensus = evaluate_quorum(results)
        self._last_consensus = consensus

        if not consensus["quorum_reached"]:
            if log_result:
                emit_event("rpc", consensus["reason"], "ERROR")
            return consensus

        majority_hash = consensus["majority_hash"]
        divergent_key = tuple(sorted(consensus["divergent"]))
        should_log = log_result or divergent_key != self._last_divergent
        self._last_divergent = divergent_key

        replacement_id = consensus["state"]["node_id"]
        for item in results:
            if not item["ok"] or item["node_id"] not in divergent_key:
                continue
            node_id, node_data = item["node_id"], item["data"]
            if should_log:
                emit_event("rpc", f"Quorum 2f+1: No {node_id} divergente (hash {node_data['hash']} != {majority_hash}); descartado", "ERROR")
            if enforce_leader and node_data.get("is_primary"):
                self._demote_and_replace(node_id, replacement_id)

        if log_result and not divergent_key:
            emit_event("rpc", f"Quorum 2f+1 OK ({consensus['votes']} votos, hash {majority_hash})")
        return consensus

    def _demote_and_replace(self, bad_leader_id: int, replacement_id: int) -> None:
        with suppress_rpc_errors():
            with rpc_proxy(peer_uri(bad_leader_id)) as proxy:
                proxy.set_primary(False)
        try:
            with rpc_proxy(peer_uri(replacement_id)) as proxy:
                proxy.set_primary(True)
            emit_event("rpc", f"Lider {bad_leader_id} destituido por divergencia; No {replacement_id} promovido", "WARN")
        except Exception as exc:
            emit_event("rpc", f"Falha ao promover substituto {replacement_id}: {exc}", "ERROR")

    def promote_node(self, target_id: int) -> bool:
        promoted = False
        for node_id in ALL_NODES:
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(node_id)) as proxy:
                    proxy.set_primary(node_id == target_id)
                    promoted = promoted or (node_id == target_id)
        return promoted

    def set_byzantine(self, target_id: int, flag: bool) -> bool:
        try:
            with rpc_proxy(peer_uri(target_id)) as proxy:
                return bool(proxy.set_byzantine(bool(flag)))
        except Exception as exc:
            emit_event("rpc", f"Falha ao alterar modo bizantino no No {target_id}: {exc}", "ERROR")
            return False

    def _find_leader(self) -> int | None:
        """Descobre o lider ativo em paralelo; se nenhum for primario, promove o primeiro no vivo."""
        alive = [item for item in self.get_nodes_status() if item["ok"]]
        for item in alive:
            if item["data"].get("is_primary"):
                return item["node_id"]
        if alive:
            chosen = alive[0]["node_id"]
            self.promote_node(chosen)
            return chosen
        return None

    def execute_command(self, action: str, command_id: str | None = None) -> dict:
        """Envia comando ao lider reutilizando o mesmo command_id em caso de failover."""
        cmd_id = (command_id or "").strip() or str(uuid.uuid4())
        if not self.get_consensus_state(log_result=True, enforce_leader=True)["quorum_reached"]:
            emit_event("rpc", f"Comando {action} abortado: sem quorum 2f+1", "ERROR")
            return {"ok": False, "error": str(RpcError.NO_QUORUM), "command_id": cmd_id}

        tried: set[int] = set()
        for _ in ALL_NODES:
            leader_id = self._find_leader()
            if leader_id is None or leader_id in tried:
                break
            tried.add(leader_id)
            try:
                with rpc_proxy(peer_uri(leader_id), timeout=2.0) as proxy:
                    response = proxy.execute_command(action, cmd_id)
                if response.get("ok"):
                    tag = " (idempotente)" if response.get("idempotent") else ""
                    emit_event("rpc", f"RPC {action} ({cmd_id[:8]}) -> No {leader_id} OK{tag}")
                    return {**response, "command_id": cmd_id, "node_id": leader_id}
                emit_event("rpc", f"No {leader_id} recusou ({response.get('error')}); tentando failover", "WARN")
            except Exception as exc:
                emit_event("rpc", f"Falha RPC no Lider {leader_id}: {exc}; iniciando failover", "WARN")

            for candidate_id in ALL_NODES:
                if candidate_id not in tried and self.promote_node(candidate_id):
                    break

        emit_event("rpc", f"Falha ao executar {action}: nenhum controlador disponivel", "ERROR")
        return {"ok": False, "error": str(RpcError.CLUSTER_UNAVAILABLE), "command_id": cmd_id}


def main() -> None:
    client = ClusterRpcClient()
    daemon = Pyro5.api.Daemon(host="localhost", port=CLIENT_PORT)
    daemon.register(client, CLIENT_OBJECT_NAME)
    observer_uri = daemon.register(PushReceiver(client.handle_observer_push), CLIENT_OBSERVER_NAME)
    client.set_observer_uri(str(observer_uri))
    client.register_all_observers()
    client.get_consensus_state(log_result=False)
    emit_event("rpc", f"Cliente RPC Pyro5 ativo na porta {CLIENT_PORT} (observer {observer_uri})")

    def shutdown(signum: int) -> None:
        emit_event("rpc", f"Cliente RPC encerrado (sinal {signum})", "WARN")
        client.unregister_all_observers()
        daemon.shutdown()
        sys.exit(0)

    install_shutdown_handler(shutdown)
    daemon.requestLoop()


if __name__ == "__main__":
    main()
