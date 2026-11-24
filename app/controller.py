"""No controlador do cluster: MQTT, replicacao primario-replica, eleicao P2P e Pyro5 RPC."""

import argparse
import json
import sys
import threading
import uuid

import Pyro5.api

from app.config import ALL_NODES, BASE_PORT, TOPIC_SENSORS, controller_object_name, node_db_path, peer_uri
from app.domain import Perimeter, PetState, RpcError, apply_command, apply_telemetry
from app.event_bus import emit_event
from app.infra import ObserverRegistry, connect_mqtt, install_shutdown_handler, publish_json, rpc_proxy, suppress_rpc_errors
from app.storage import NodeStorage


@Pyro5.api.expose
class PetController:
    """Processo controlador independente com persistencia SQLite e replicacao P2P."""

    def __init__(self, node_id: int, is_primary: bool = False) -> None:
        self.node_id = node_id
        self.is_primary = is_primary
        self.byzantine = False
        self.storage = NodeStorage(node_db_path(node_id))
        self._lock = threading.Lock()
        self._observers = ObserverRegistry()
        self.mqtt = connect_mqtt(
            f"petcare_ctrl_{node_id}",
            on_connect=self._on_mqtt_connect,
            on_message=self._on_mqtt_message,
            on_error=lambda exc: self._log(f"Falha ao conectar ao MQTT: {exc}", "ERROR"),
        )
        self._catch_up_from_peers()
        self._log(f"Controlador {node_id} iniciado na porta {BASE_PORT + node_id} ({self._role()})")

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

    def _state_snapshot(self, state: PetState) -> dict:
        return {
            **state.to_dict(),
            "node_id": self.node_id,
            "is_primary": self.is_primary,
            "byzantine": self.byzantine,
            "hash": state.canonical_hash(),
        }

    def _notify_observers(self, state: PetState, reason: str, command_id: str | None = None) -> None:
        self._observers.notify({**self._state_snapshot(state), "reason": reason, "command_id": command_id})

    # --- Coordenacao P2P: sondagem, catch-up, eleicao e replicacao ---

    def _probe_peers(self) -> dict[int, dict]:
        """Consulta `ping()` de todos os peers acessiveis no cluster."""
        alive: dict[int, dict] = {}
        for peer_id in self._peer_ids():
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(peer_id)) as proxy:
                    alive[peer_id] = proxy.ping()
        return alive

    def _catch_up_from_peers(self) -> None:
        """Ao subir ou se recuperar, sincroniza o SQLite com o lider ativo do cluster."""
        for peer_id, info in self._probe_peers().items():
            if not info.get("is_primary") or info.get("byzantine"):
                continue
            with suppress_rpc_errors():
                with rpc_proxy(peer_uri(peer_id)) as proxy:
                    remote = proxy.get_state()
                self.is_primary = False
                self.storage.save_state(PetState.from_dict(remote))
                emit_event(f"storage_{self.node_id}", f"Catch-up concluido a partir do Lider {peer_id} (hash {remote.get('hash')})")
                return

    def _maybe_elect_self(self) -> bool:
        """Eleicao deterministica P2P: sem lider saudavel ativo, o menor node_id nao-bizantino assume."""
        alive_peers = self._probe_peers()
        healthy_peers = {pid: info for pid, info in alive_peers.items() if not info.get("byzantine")}
        if any(info.get("is_primary") for info in healthy_peers.values()):
            return False
        if min([self.node_id, *healthy_peers.keys()]) == self.node_id:
            for peer_id, info in alive_peers.items():
                if info.get("is_primary") and info.get("byzantine"):
                    with suppress_rpc_errors():
                        with rpc_proxy(peer_uri(peer_id)) as proxy:
                            proxy.set_primary(False)
            self.is_primary = True
            self._log(f"Failover P2P: nenhum lider saudavel ativo; No {self.node_id} assumiu lideranca", "WARN")
            return True
        return False

    def _commit_and_replicate(
        self, next_state: PetState, actuators: list[dict], command_id: str, kind: str, payload_str: str, log_msg: str
    ) -> None:
        """Pipeline atomico do lider: persiste estado+log, atua via MQTT, replica P2P e notifica push."""
        self.storage.save_state(next_state, event=(command_id, kind, payload_str))
        self._log(log_msg)
        emit_event(f"storage_{self.node_id}", f"Estado salvo ({kind}, hash {next_state.canonical_hash()})")

        for command in actuators:
            publish_json(self.mqtt, command["topic"], {**command["payload"], "command_id": command_id}, qos=2)

        state_dict = next_state.to_dict()
        for peer_id in self._peer_ids():
            try:
                with rpc_proxy(peer_uri(peer_id)) as proxy:
                    proxy.sync_state(state_dict, command_id, kind, payload_str)
            except Exception as exc:
                self._log(f"Replica {peer_id} inacessivel no sync: {exc}", "WARN")

        self._notify_observers(next_state, kind, command_id)

    # --- Entrada MQTT ---

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        client.subscribe(f"{TOPIC_SENSORS}/#", qos=1)
        self._log(f"Inscrito em {TOPIC_SENSORS}/# (QoS 1)")

    def _on_mqtt_message(self, client, userdata, msg) -> None:
        sensor = msg.topic.split("/")[-1]
        raw = msg.payload.decode("utf-8", errors="replace")

        if self.byzantine or (not self.is_primary and not self._maybe_elect_self()):
            return

        with self._lock:
            if not self.is_primary:
                return
            try:
                next_state, actuators = apply_telemetry(self.storage.load_state(), sensor, json.loads(raw))
                command_id = str(uuid.uuid4())
                self._commit_and_replicate(
                    next_state, actuators, command_id, f"TELEMETRIA_{sensor.upper()}", raw,
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
        return {
            "node_id": self.node_id,
            "is_primary": self.is_primary,
            "byzantine": self.byzantine,
            "hash": self._visible_state().canonical_hash(),
        }

    def get_state(self) -> dict:
        return self._state_snapshot(self._visible_state())

    def set_primary(self, flag: bool) -> bool:
        self.is_primary = bool(flag)
        self._log(f"Papel alterado para {self._role()}", "WARN")
        self._notify_observers(self._visible_state(), "ROLE_CHANGE")
        return self.is_primary

    def set_byzantine(self, flag: bool) -> bool:
        with self._lock:
            self.byzantine = bool(flag)
            if self.byzantine:
                self._log(f"Modo BIZANTINO ativado: No {self.node_id} enviando estado adulterado", "ERROR")
            else:
                self._log(f"Modo BIZANTINO desativado: No {self.node_id} restaurado", "WARN")
                self._catch_up_from_peers()
            return self.byzantine

    def sync_state(self, state_dict: dict, command_id: str | None = None, kind: str = "SYNC", payload: str = "{}") -> bool:
        """Aplica replicacao recebida do lider na replica local."""
        with self._lock:
            if self.byzantine:
                return False
            state = PetState.from_dict(state_dict)
            event = (command_id, kind, payload) if command_id else None
            self.storage.save_state(state, event=event)
            self._log(f"Sync P2P aplicado ({kind})")
            emit_event(f"storage_{self.node_id}", f"Sync replicado (hash {state.canonical_hash()})")
            self._notify_observers(state, f"SYNC_{kind}", command_id)
            return True

    def execute_command(self, action: str, command_id: str) -> dict:
        """Executa comando RPC com verificacao de lideranca e idempotencia por command_id."""
        with self._lock:
            if self.byzantine:
                self._log(f"Controlador {self.node_id} (BIZANTINO) adulterou comando {action}", "ERROR")
                return {"ok": False, "error": str(RpcError.BYZANTINE_FAULT)}
            if not self.is_primary:
                return {"ok": False, "error": str(RpcError.NOT_PRIMARY)}
            if self.storage.has_event(command_id):
                self._log(f"Idempotencia: comando {command_id[:8]} ({action}) ja registrado; ignorado", "WARN")
                return {"ok": True, "idempotent": True, "state": self.storage.load_state().to_dict()}

            next_state, actuators = apply_command(self.storage.load_state(), action)
            self._commit_and_replicate(
                next_state, actuators, command_id, action, json.dumps({"acao": action}),
                f"Comando {action} executado ({command_id[:8]})",
            )
            return {"ok": True, "idempotent": False, "state": next_state.to_dict()}


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
