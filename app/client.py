from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import threading
from typing import Callable
import uuid

import Pyro5.api

from app.config import ALL_NODES, CLIENT_PORT, peer_uri
from app.event_bus import emit_event


@Pyro5.api.expose
class ClientObserver:
    """Objeto remoto Pyro5 exposto pelo cliente para receber notificacoes push."""

    def __init__(self, on_notify: Callable[[dict], None]) -> None:
        self._on_notify = on_notify

    @Pyro5.api.oneway
    def on_state_changed(self, payload: dict) -> None:
        self._on_notify(payload)


def _proxy(node_id: int, timeout: float = 1.0) -> Pyro5.api.Proxy:
    proxy = Pyro5.api.Proxy(peer_uri(node_id))
    proxy._pyroTimeout = timeout
    return proxy


@Pyro5.api.expose
class ClusterRpcClient:
    """Processo/servico cliente Pyro5 RPC com quorum bizantino (2f+1) e failover transparente."""

    def __init__(self) -> None:
        self._last_divergent: tuple[int, ...] = ()
        self._last_consensus: dict | None = None
        self._observer_uri: str | None = None
        self._ui_lock = threading.Lock()
        self._ui_observers: set[str] = set()
        self._push_lock = threading.Lock()
        self._debounce_timer: threading.Timer | None = None

    def set_observer_uri(self, uri: str) -> None:
        self._observer_uri = uri

    def register_ui_observer(self, ui_uri: str) -> bool:
        with self._ui_lock:
            self._ui_observers.add(ui_uri)
        return True

    def unregister_ui_observer(self, ui_uri: str) -> bool:
        with self._ui_lock:
            self._ui_observers.discard(ui_uri)
        return True

    def _notify_ui_observers(self, consensus: dict) -> None:
        with self._ui_lock:
            uris = list(self._ui_observers)
        if not uris:
            return

        def _push() -> None:
            dead: list[str] = []
            for uri in uris:
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    proxy._pyroTimeout = 1.0
                    with proxy:
                        proxy.on_consensus(consensus)
                except Exception:
                    dead.append(uri)
            if dead:
                with self._ui_lock:
                    for uri in dead:
                        self._ui_observers.discard(uri)

        threading.Thread(target=_push, daemon=True).start()

    def register_observer_on_node(self, node_id: int) -> bool:
        if not self._observer_uri:
            return False
        try:
            with _proxy(node_id) as p:
                return bool(p.register_observer(self._observer_uri))
        except Exception:
            return False

    def register_all_observers(self) -> None:
        if not self._observer_uri:
            return
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(self.register_observer_on_node, ALL_NODES))

    def unregister_all_observers(self) -> None:
        with self._push_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
                self._debounce_timer = None
        if self._observer_uri:
            for nid in ALL_NODES:
                try:
                    with _proxy(nid) as p:
                        p.unregister_observer(self._observer_uri)
                except Exception:
                    pass

    def handle_observer_push(self, payload: dict) -> None:
        with self._push_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
            self._debounce_timer = threading.Timer(0.08, self._evaluate_consensus)
            self._debounce_timer.daemon = True
            self._debounce_timer.start()

    def _evaluate_consensus(self) -> None:
        consensus = self.get_consensus_state(log_result=False, enforce_leader=True)
        self._notify_ui_observers(consensus)

    def _query_all_nodes(self, method: str) -> list[dict]:
        """Chama `method` (ping/get_state) em todos os nos em paralelo."""

        def query(node_id: int) -> dict:
            try:
                with _proxy(node_id) as p:
                    return {"node_id": node_id, "ok": True, "data": getattr(p, method)()}
            except Exception as exc:
                return {"node_id": node_id, "ok": False, "error": str(exc)}

        with ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(query, ALL_NODES))

    def get_nodes_status(self) -> list[dict]:
        return self._query_all_nodes("ping")

    def get_last_consensus(self) -> dict | None:
        return self._last_consensus

    def get_consensus_state(self, log_result: bool = True, enforce_leader: bool = True) -> dict:
        results = self._query_all_nodes("get_state")

        valid = [r for r in results if r["ok"]]
        if len(valid) < 2:
            if log_result:
                emit_event("rpc", f"Quorum indisponivel: apenas {len(valid)}/3 nos ativos", "ERROR")
            consensus = {"quorum_reached": False, "nodes": results, "state": None, "divergent": []}
            self._last_consensus = consensus
            return consensus

        counts = Counter(r["data"]["hash"] for r in valid)
        majority_hash, majority_count = counts.most_common(1)[0]

        if majority_count < 2:
            if log_result:
                emit_event("rpc", "Sem consenso 2f+1: todas as respostas divergem", "ERROR")
            consensus = {"quorum_reached": False, "nodes": results, "state": None, "divergent": []}
            self._last_consensus = consensus
            return consensus

        majority_nodes = [r for r in valid if r["data"]["hash"] == majority_hash]
        divergent = [r["node_id"] for r in valid if r["data"]["hash"] != majority_hash]
        div_key = tuple(sorted(divergent))
        should_log_div = log_result or div_key != self._last_divergent
        self._last_divergent = div_key

        for nid in divergent:
            node_data = next(r["data"] for r in valid if r["node_id"] == nid)
            if should_log_div:
                emit_event(
                    "rpc",
                    f"Quorum 2f+1: No {nid} divergente (hash {node_data['hash']} != {majority_hash}); descartado",
                    "ERROR",
                )
            if enforce_leader and node_data.get("is_primary"):
                self._demote_and_replace(nid, majority_nodes[0]["node_id"])

        if log_result and not divergent:
            emit_event("rpc", f"Quorum 2f+1 OK ({majority_count}/{len(valid)} votos, hash {majority_hash})")

        consensus = {
            "quorum_reached": True,
            "majority_hash": majority_hash,
            "votes": f"{majority_count}/{len(valid)}",
            "state": majority_nodes[0]["data"],
            "divergent": divergent,
            "nodes": results,
        }
        self._last_consensus = consensus
        return consensus

    def _demote_and_replace(self, bad_leader_id: int, replacement_id: int) -> None:
        try:
            with _proxy(bad_leader_id) as p:
                p.set_primary(False)
        except Exception:
            pass
        try:
            with _proxy(replacement_id) as p:
                p.set_primary(True)
            emit_event(
                "rpc",
                f"Lider {bad_leader_id} destituido por divergencia; No {replacement_id} promovido",
                "WARN",
            )
        except Exception as exc:
            emit_event("rpc", f"Falha ao promover substituto {replacement_id}: {exc}", "ERROR")

    def promote_node(self, target_id: int) -> bool:
        ok = False
        for nid in ALL_NODES:
            try:
                with _proxy(nid) as p:
                    p.set_primary(nid == target_id)
                    if nid == target_id:
                        ok = True
            except Exception:
                continue
        return ok

    def set_byzantine(self, target_id: int, flag: bool) -> bool:
        try:
            with _proxy(target_id) as p:
                return bool(p.set_byzantine(bool(flag)))
        except Exception as exc:
            emit_event("rpc", f"Falha ao alterar modo bizantino no No {target_id}: {exc}", "ERROR")
            return False

    def _promote_first_available(self, candidates: list[int]) -> None:
        for cand in candidates:
            if self.promote_node(cand):
                break

    def _find_leader(self) -> int | None:
        alive = []
        for nid in ALL_NODES:
            try:
                with _proxy(nid) as p:
                    info = p.ping()
                    if info.get("is_primary"):
                        return nid
                    alive.append(nid)
            except Exception:
                continue
        if alive:
            chosen = alive[0]
            self.promote_node(chosen)
            return chosen
        return None

    def execute_command(self, action: str, command_id: str | None = None) -> dict:
        cmd_id = (command_id or "").strip() or str(uuid.uuid4())
        consensus = self.get_consensus_state(log_result=True, enforce_leader=True)
        if not consensus.get("quorum_reached"):
            emit_event("rpc", f"Comando {action} abortado: sem quorum 2f+1", "ERROR")
            return {"ok": False, "error": "NO_QUORUM", "command_id": cmd_id}

        tried: set[int] = set()
        for _ in range(len(ALL_NODES)):
            leader_id = self._find_leader()
            if leader_id is None or leader_id in tried:
                break
            tried.add(leader_id)
            try:
                with _proxy(leader_id, timeout=2.0) as p:
                    res = p.execute_command(action, cmd_id)
                if res.get("ok"):
                    tag = " (idempotente)" if res.get("idempotent") else ""
                    emit_event("rpc", f"RPC {action} ({cmd_id[:8]}) -> No {leader_id} OK{tag}")
                    return {**res, "command_id": cmd_id, "node_id": leader_id}
                emit_event("rpc", f"No {leader_id} recusou ({res.get('error')}); tentando failover", "WARN")
            except Exception as exc:
                emit_event("rpc", f"Falha RPC no Lider {leader_id}: {exc}; iniciando failover", "WARN")
            self._promote_first_available([n for n in ALL_NODES if n not in tried])

        emit_event("rpc", f"Falha ao executar {action}: nenhum controlador disponivel", "ERROR")
        return {"ok": False, "error": "CLUSTER_UNAVAILABLE", "command_id": cmd_id}


def main() -> None:
    client = ClusterRpcClient()
    daemon = Pyro5.api.Daemon(host="localhost", port=CLIENT_PORT)
    daemon.register(client, "petcare.client")
    obs_uri = daemon.register(ClientObserver(client.handle_observer_push), "petcare.client.observer")
    client.set_observer_uri(str(obs_uri))
    client.register_all_observers()
    client.get_consensus_state(log_result=False)
    emit_event("rpc", f"Cliente RPC Pyro5 ativo na porta {CLIENT_PORT} (observer {obs_uri})")

    def shutdown(signum, frame):
        emit_event("rpc", f"Cliente RPC encerrado (sinal {signum})", "WARN")
        client.unregister_all_observers()
        daemon.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    daemon.requestLoop()


if __name__ == "__main__":
    main()
