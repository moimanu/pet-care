"""Processo cliente Pyro5 RPC: consenso por quorum 2f+1, failover transparente e push UI."""

from concurrent.futures import ThreadPoolExecutor
import sys
import threading
import uuid

import Pyro5.api

from app.config import ALL_NODES, CLIENT_OBJECT_NAME, CLIENT_OBSERVER_NAME, CLIENT_PORT, peer_uri
from app.domain import RpcError, evaluate_quorum
from app.event_bus import emit_event
from app.infra import (
    ObserverRegistry, PushReceiver, install_shutdown_handler,
    query_cluster_nodes, rpc_proxy, suppress_rpc_errors,
)

DEBOUNCE_SECONDS = 0.08


@Pyro5.api.expose
class ClusterRpcClient:
    """Servico cliente RPC com leitura/confirmacao por quorum (2f+1) e failover transparente."""

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

    def get_nodes_status(self) -> list[dict]:
        return query_cluster_nodes(ALL_NODES, "ping")

    def get_last_consensus(self) -> dict | None:
        return self._last_consensus

    def _request_cluster_election(self, majority_ids: list[int]) -> None:
        """Aciona o candidato deterministico da maioria para conduzir a eleicao P2P no cluster."""
        if not majority_ids:
            return
        candidate_id = min(majority_ids)
        with suppress_rpc_errors():
            with rpc_proxy(peer_uri(candidate_id), timeout=1.5) as proxy:
                proxy.reconcile_leadership()

    def get_consensus_state(self, log_result: bool = True, enforce_leader: bool = True) -> dict:
        """Consulta o cluster em paralelo, valida quorum 2f+1 e solicita eleicao P2P se sem lider integro."""
        results = query_cluster_nodes(ALL_NODES, "get_state")
        consensus = evaluate_quorum(results)

        if consensus["quorum_reached"] and enforce_leader and consensus["leader_id"] is None:
            self._request_cluster_election(consensus["majority_ids"])
            results = query_cluster_nodes(ALL_NODES, "get_state")
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

        for item in results:
            if item["ok"] and item["node_id"] in divergent_key and should_log:
                node_hash = item["data"]["hash"]
                emit_event(
                    "rpc",
                    f"Quorum 2f+1: No {item['node_id']} divergente (hash {node_hash} != {majority_hash}); descartado",
                    "ERROR",
                )

        if log_result and not divergent_key:
            emit_event("rpc", f"Quorum 2f+1 OK ({consensus['votes']} votos, mandato {consensus['term']}, hash {majority_hash})")
        return consensus

    def promote_node(self, target_id: int) -> bool:
        try:
            with rpc_proxy(peer_uri(target_id), timeout=2.0) as proxy:
                return bool(proxy.promote_self())
        except Exception as exc:
            emit_event("rpc", f"Falha ao solicitar promocao do No {target_id}: {exc}", "ERROR")
            return False

    def set_byzantine(self, target_id: int, flag: bool) -> bool:
        try:
            with rpc_proxy(peer_uri(target_id)) as proxy:
                return bool(proxy.set_byzantine(bool(flag)))
        except Exception as exc:
            emit_event("rpc", f"Falha ao alterar modo bizantino no No {target_id}: {exc}", "ERROR")
            return False

    def execute_command(self, action: str, command_id: str | None = None) -> dict:
        """Envia comando ao lider eleito pelo quorum e confirma pos-execucao por maioria 2f+1."""
        cmd_id = (command_id or "").strip() or str(uuid.uuid4())
        consensus = self.get_consensus_state(log_result=True, enforce_leader=True)
        if not consensus["quorum_reached"]:
            emit_event("rpc", f"Comando {action} abortado: sem quorum 2f+1", "ERROR")
            return {"ok": False, "error": str(RpcError.NO_QUORUM), "command_id": cmd_id}

        tried: set[int] = set()
        for _ in ALL_NODES:
            leader_id = consensus.get("leader_id")
            if leader_id is None or leader_id in tried:
                break
            tried.add(leader_id)
            try:
                with rpc_proxy(peer_uri(leader_id), timeout=2.0) as proxy:
                    response = proxy.execute_command(action, cmd_id)
                if response.get("ok"):
                    post = self.get_consensus_state(log_result=False, enforce_leader=True)
                    if (
                        post["quorum_reached"]
                        and leader_id in post["majority_ids"]
                        and post["majority_hash"] == response.get("hash")
                    ):
                        tag = " (idempotente)" if response.get("idempotent") else ""
                        emit_event("rpc", f"RPC {action} ({cmd_id[:8]}) -> No {leader_id} OK{tag}")
                        return {**response, "command_id": cmd_id, "node_id": leader_id}
                    emit_event("rpc", f"Resposta do No {leader_id} diverge do quorum 2f+1; iniciando failover", "ERROR")
                    consensus = post
                    continue
                emit_event("rpc", f"No {leader_id} recusou ({response.get('error')}); iniciando failover", "WARN")
            except Exception as exc:
                emit_event("rpc", f"Falha RPC no Lider {leader_id}: {exc}; iniciando failover", "WARN")

            consensus = self.get_consensus_state(log_result=False, enforce_leader=True)

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
