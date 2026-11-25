"""No controlador do cluster: MQTT, replicacao validada por hash, eleicao P2P 2f+1 e Pyro5 RPC."""

import argparse
import json
import sys
import threading
import uuid

import Pyro5.api

from app.config import ALL_NODES, BASE_PORT, MIN_QUORUM, TOPIC_SENSORS, controller_object_name, node_db_path, peer_uri
from app.domain import (
    Perimeter, PetState, RpcError, TransitionKind,
    apply_transition, evaluate_quorum, is_valid_state,
)
from app.event_bus import emit_event
from app.infra import (
    ObserverRegistry, connect_mqtt, install_shutdown_handler,
    publish_json, query_cluster_nodes, rpc_proxy, suppress_rpc_errors,
)
from app.storage import NodeStorage


@Pyro5.api.expose
class PetController:
    """Processo controlador independente com persistencia SQLite e consenso P2P de lideranca."""

    def __init__(self, node_id: int, is_primary: bool = False) -> None:
        self.node_id = node_id
        self.term: int = 1 if is_primary else 0
        self.leader_id: int | None = node_id if is_primary else None
        self.byzantine = False
        self.storage = NodeStorage(node_db_path(node_id))
        self._lock = threading.Lock()
        self._election_lock = threading.Lock()
        self._observers = ObserverRegistry()
        self.mqtt = connect_mqtt(
            f"petcare_ctrl_{node_id}",
            on_connect=self._on_mqtt_connect,
            on_message=self._on_mqtt_message,
            on_error=lambda exc: self._log(f"Falha ao conectar ao MQTT: {exc}", "ERROR"),
        )
        self._catch_up_from_peers()
        self._log(f"Controlador {node_id} iniciado na porta {BASE_PORT + node_id} ({self._role()}, mandato {self.term})")

    @property
    def is_primary(self) -> bool:
        return self.leader_id == self.node_id

    def _log(self, message: str, level: str = "INFO") -> None:
        emit_event(f"controller_{self.node_id}", message, level)

    def _role(self) -> str:
        return "LIDER" if self.is_primary else "REPLICA"

    def _peer_ids(self) -> list[int]:
        return [peer_id for peer_id in ALL_NODES if peer_id != self.node_id]

    def _visible_state(self) -> PetState:
        """Retorna o estado real do SQLite, ou um estado forjado em modo bizantino."""
        if self.byzantine:
            return PetState(
                racao_gramas=9000.0 + self.node_id,
                agua_ml=0.0,
                areia_usos=99,
                status_fuga=str(Perimeter.FORA),
                alarme_ativo=True,
            )
        return self.storage.load_state()

    def _state_snapshot(self, state: PetState, term: int, leader_id: int | None, byzantine: bool) -> dict:
        return {
            **state.to_dict(),
            "node_id": self.node_id,
            "term": term,
            "leader_id": leader_id,
            "is_primary": leader_id == self.node_id,
            "byzantine": byzantine,
            "hash": state.canonical_hash(),
        }

    def _notify_observers(self, state: PetState, reason: str, command_id: str | None = None) -> None:
        with self._lock:
            snap = self._state_snapshot(state, self.term, self.leader_id, self.byzantine)
        self._observers.notify({**snap, "reason": reason, "command_id": command_id})

    # --- Coordenacao P2P por quorum 2f+1 ---

    def _evaluate_cluster_quorum(self) -> dict:
        results = query_cluster_nodes(ALL_NODES, "get_state", timeout=0.8, local=(self.node_id, self.get_state()))
        return evaluate_quorum(results)

    def commit_term(self, term: int, leader_id: int | None) -> bool:
        """Aplica um mandato ja aprovado por maioria 2f+1, rebaixando lideres de mandatos anteriores."""
        with self._lock:
            if term < self.term or (term == self.term and self.leader_id == leader_id):
                return False
            was_primary = self.is_primary
            self.term = term
            self.leader_id = leader_id
            state = self._visible_state()
            role_changed = was_primary != self.is_primary
        if role_changed:
            self._log(f"Papel alterado para {self._role()} (mandato {term})", "WARN")
        self._notify_observers(state, "ROLE_CHANGE")
        return True

    def _propose_leadership(self, candidate_id: int, base_term: int = 0) -> bool:
        """Coleta votos P2P (`request_vote`); se atingir quorum 2f+1, oficializa via `commit_term`."""
        new_term = max(self.term, base_term) + 1
        votes = 1
        for peer_id in self._peer_ids():
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:
                    if proxy.request_vote(new_term, candidate_id):
                        votes += 1
        if votes < MIN_QUORUM:
            return False

        self.commit_term(new_term, candidate_id)
        for peer_id in self._peer_ids():
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:
                    proxy.commit_term(new_term, candidate_id)

        self._log(
            f"Quorum P2P ({votes}/{len(ALL_NODES)}): No {candidate_id} eleito Lider no mandato {new_term}",
            "WARN",
        )
        return True

    def reconcile_leadership(self) -> int | None:
        """Audita o cluster via quorum 2f+1; sem lider integro na maioria, o menor ID assume."""
        if self.byzantine:
            return self.leader_id
        with self._election_lock:
            consensus = self._evaluate_cluster_quorum()
            if not consensus["quorum_reached"]:
                return None
            if consensus["leader_id"] is not None:
                self.commit_term(consensus["term"], consensus["leader_id"])
                return consensus["leader_id"]

            candidate_id = min(consensus["majority_ids"])
            if candidate_id == self.node_id and self._propose_leadership(candidate_id, consensus["term"]):
                return self.leader_id
            return None

    def _catch_up_from_peers(self) -> None:
        """Sincroniza SQLite e mandato com o estado majoritario integro ao iniciar ou recuperar."""
        peer_results = query_cluster_nodes(self._peer_ids(), "get_state", timeout=0.8)
        consensus = evaluate_quorum([{"node_id": self.node_id, "ok": True, "data": self.get_state()}, *peer_results])

        target_data: dict | None = None
        if consensus["quorum_reached"]:
            target_data = {**consensus["state"], "term": consensus["term"], "leader_id": consensus["leader_id"]}
        elif self.term == 0:
            active_leaders = [
                item["data"]
                for item in peer_results
                if item["ok"]
                and int(item["data"].get("term", 0)) > 0
                and item["data"].get("leader_id") == item["node_id"]
                and is_valid_state(PetState.from_dict(item["data"]))
            ]
            if active_leaders:
                target_data = max(active_leaders, key=lambda d: int(d.get("term", 0)))

        if target_data is None:
            return

        synced_state = PetState.from_dict(target_data)
        if not is_valid_state(synced_state):
            return

        with self._lock:
            self.storage.save_state(synced_state)
            self.term = max(self.term, int(target_data.get("term", 0)))
            if target_data.get("leader_id") is not None:
                self.leader_id = int(target_data["leader_id"])
        emit_event(
            f"storage_{self.node_id}",
            f"Catch-up concluido (mandato {self.term}, lider {self.leader_id}, hash {synced_state.canonical_hash()})",
        )
        self._notify_observers(synced_state, "CATCH_UP")

    def _commit_and_replicate(
        self, kind: str, target: str, payload: dict, command_id: str, log_msg: str
    ) -> dict:
        """Calcula transicao deterministica, exige quorum P2P de replicacao e persiste/atua."""
        with self._lock:
            term, leader_id, byzantine = self.term, self.node_id, self.byzantine
            next_state, actuators = apply_transition(self._visible_state(), kind, target, payload)
            expected_hash = next_state.canonical_hash()

        if byzantine:
            self._log(f"Controlador {self.node_id} (BIZANTINO) forjou transicao {target}", "ERROR")
            for peer_id in self._peer_ids():
                with suppress_rpc_errors():
                    with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:
                        proxy.sync_state(term, leader_id, command_id, kind, target, payload, expected_hash)
            return {"ok": True, "idempotent": False, "state": next_state.to_dict(), "hash": expected_hash}

        acks = 1
        for peer_id in self._peer_ids():
            try:
                with rpc_proxy(peer_uri(peer_id), timeout=1.0) as proxy:
                    if proxy.sync_state(term, leader_id, command_id, kind, target, payload, expected_hash):
                        acks += 1
            except Exception as exc:
                self._log(f"Replica {peer_id} inacessivel no sync: {exc}", "WARN")

        if acks < MIN_QUORUM:
            self._log(f"Transicao {target} abortada: sem quorum P2P ({acks}/{len(ALL_NODES)})", "ERROR")
            return {"ok": False, "error": str(RpcError.NO_QUORUM)}

        event_label = target if kind == TransitionKind.COMMAND else f"TELEMETRIA_{target.upper()}"
        with self._lock:
            self.storage.save_state(next_state, event=(command_id, event_label, json.dumps(payload)))
        self._log(log_msg)
        emit_event(f"storage_{self.node_id}", f"Estado salvo ({event_label}, hash {expected_hash})")

        for command in actuators:
            publish_json(self.mqtt, command["topic"], {**command["payload"], "command_id": command_id}, qos=2)

        self._notify_observers(next_state, event_label, command_id)
        return {"ok": True, "idempotent": False, "state": next_state.to_dict(), "hash": expected_hash}

    # --- Entrada MQTT ---

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        client.subscribe(f"{TOPIC_SENSORS}/#", qos=1)
        self._log(f"Inscrito em {TOPIC_SENSORS}/# (QoS 1)")

    def _on_mqtt_message(self, client, userdata, msg) -> None:
        sensor = msg.topic.split("/")[-1]
        raw = msg.payload.decode("utf-8", errors="replace")

        if not self.is_primary and self.reconcile_leadership() != self.node_id:
            return
        if not self.is_primary:
            return

        try:
            payload = json.loads(raw)
            command_id = str(uuid.uuid4())
            self._commit_and_replicate(
                TransitionKind.TELEMETRY, sensor, payload, command_id,
                f"Telemetria MQTT {sensor} processada ({command_id[:8]})",
            )
        except Exception as exc:
            self._log(f"Telemetria rejeitada ({sensor}): {exc}", "ERROR")

    # --- Interface RPC Pyro5 ---

    def register_observer(self, observer_uri: str) -> bool:
        return self._observers.register(observer_uri)

    def unregister_observer(self, observer_uri: str) -> bool:
        return self._observers.unregister(observer_uri)

    def ping(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "term": self.term,
                "leader_id": self.leader_id,
                "is_primary": self.is_primary,
                "byzantine": self.byzantine,
                "hash": self._visible_state().canonical_hash(),
            }

    def get_state(self) -> dict:
        with self._lock:
            return self._state_snapshot(self._visible_state(), self.term, self.leader_id, self.byzantine)

    def request_vote(self, new_term: int, candidate_id: int) -> bool:
        """Voto P2P de mandato: nos bizantinos nunca votam; nos honestos exigem maioria 2f+1."""
        if self.byzantine:
            return False
        with self._lock:
            if new_term < self.term:
                return False
            if new_term == self.term and self.leader_id == candidate_id:
                return True
        consensus = self._evaluate_cluster_quorum()
        if not consensus["quorum_reached"] or candidate_id not in consensus["majority_ids"]:
            return False
        self._log(f"Voto P2P concedido no mandato {new_term} para o No {candidate_id}")
        return True

    def promote_self(self) -> bool:
        """Solicitacao de promocao sujeita a votacao P2P de quorum 2f+1."""
        if self.byzantine:
            return False
        with self._election_lock:
            consensus = self._evaluate_cluster_quorum()
            if not consensus["quorum_reached"] or self.node_id not in consensus["majority_ids"]:
                return False
            return self._propose_leadership(self.node_id, consensus["term"])

    def set_byzantine(self, flag: bool) -> bool:
        with self._lock:
            self.byzantine = bool(flag)
        if flag:
            self._log(f"Modo BIZANTINO ativado: No {self.node_id} enviando estado adulterado", "ERROR")
        else:
            self._log(f"Modo BIZANTINO desativado: No {self.node_id} restaurado", "WARN")
            self._catch_up_from_peers()
        return bool(flag)

    def sync_state(
        self,
        term: int,
        leader_id: int,
        command_id: str,
        kind: str,
        target: str,
        payload: dict,
        expected_hash: str,
    ) -> bool:
        """Valida mandato e recalcula deterministicamente a transicao antes de gravar no SQLite."""
        if self.byzantine:
            return False
        with self._lock:
            if term < self.term or (term == self.term and self.leader_id not in (None, leader_id)):
                return False
            if self.storage.has_event(command_id):
                return self.storage.load_state().canonical_hash() == expected_hash
            try:
                next_state, _ = apply_transition(self.storage.load_state(), kind, target, payload)
            except Exception as exc:
                self._log(f"Sync invalido do No {leader_id}: {exc}", "ERROR")
                return False
            if next_state.canonical_hash() != expected_hash:
                self._log(f"Sync rejeitado do No {leader_id}: hash divergente ({expected_hash})", "ERROR")
                return False
            self.term = term
            self.leader_id = leader_id
            event_label = target if kind == TransitionKind.COMMAND else f"TELEMETRIA_{target.upper()}"
            self.storage.save_state(next_state, event=(command_id, event_label, json.dumps(payload)))

        self._log(f"Sync P2P validado e aplicado ({event_label})")
        emit_event(f"storage_{self.node_id}", f"Sync replicado (hash {expected_hash})")
        self._notify_observers(next_state, f"SYNC_{event_label}", command_id)
        return True

    def execute_command(self, action: str, command_id: str) -> dict:
        """Executa comando RPC com verificacao de lideranca, idempotencia e quorum P2P."""
        if not self.is_primary:
            return {"ok": False, "error": str(RpcError.NOT_PRIMARY)}
        with self._lock:
            if not self.byzantine and self.storage.has_event(command_id):
                state = self.storage.load_state()
                self._log(f"Idempotencia: comando {command_id[:8]} ({action}) ja registrado; ignorado", "WARN")
                return {"ok": True, "idempotent": True, "state": state.to_dict(), "hash": state.canonical_hash()}
        return self._commit_and_replicate(
            TransitionKind.COMMAND, action, {"acao": action}, command_id,
            f"Comando {action} executado ({command_id[:8]})",
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=list(ALL_NODES))
    parser.add_argument("--primary", action="store_true")
    args = parser.parse_args()

    controller = PetController(args.id, is_primary=args.primary)
    daemon = Pyro5.api.Daemon(host="localhost", port=BASE_PORT + args.id)
    daemon.register(controller, controller_object_name(args.id))

    def shutdown(signum: int) -> None:
        emit_event(f"controller_{args.id}", f"Controlador {args.id} encerrado (sinal {signum})", "WARN")
        controller.mqtt.loop_stop()
        daemon.shutdown()
        sys.exit(0)

    install_shutdown_handler(shutdown)
    daemon.requestLoop()


if __name__ == "__main__":
    main()
