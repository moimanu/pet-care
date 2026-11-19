import argparse
import json
import signal
import sys
import threading
import uuid

import paho.mqtt.client as mqtt
import Pyro5.api

from app.config import ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT, peer_uri
from app.domain import PetState, apply_command, apply_telemetry
from app.event_bus import emit_event
from app.storage import NodeStorage


@Pyro5.api.expose
class PetController:
    def __init__(self, node_id: int, is_primary: bool = False) -> None:
        self.node_id = node_id
        self.is_primary = is_primary
        self.byzantine = False
        self.storage = NodeStorage(f"node_{node_id}.db")
        self._lock = threading.Lock()
        self._obs_lock = threading.Lock()
        self._observers: set[str] = set()
        self.mqtt = self._setup_mqtt()
        self._catch_up_from_peers()
        self._log(f"Controlador {node_id} iniciado na porta {BASE_PORT + node_id} ({self._role()})")

    def _log(self, message: str, level: str = "INFO") -> None:
        emit_event(f"controller_{self.node_id}", message, level)

    def _role(self) -> str:
        return "LIDER" if self.is_primary else "REPLICA"

    def _visible_state(self) -> PetState:
        """Estado exposto aos demais nos: adulterado se o no estiver em modo bizantino."""
        return self._forged_state() if self.byzantine else self.storage.load_state()

    def _state_snapshot(self, state: PetState) -> dict:
        return {
            **state.to_dict(),
            "node_id": self.node_id,
            "is_primary": self.is_primary,
            "byzantine": self.byzantine,
            "hash": state.canonical_hash(),
        }

    def _setup_mqtt(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"petcare_ctrl_{self.node_id}")
        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            client.loop_start()
        except Exception as exc:
            self._log(f"Falha ao conectar ao MQTT: {exc}", "ERROR")
        return client

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        client.subscribe("petcare/sensores/#", qos=1)
        self._log("Inscrito em petcare/sensores/# (QoS 1)")

    def _peer_proxy(self, pid: int) -> Pyro5.api.Proxy:
        proxy = Pyro5.api.Proxy(peer_uri(pid))
        proxy._pyroTimeout = 1.0
        return proxy

    def _peer_ids(self) -> list[int]:
        return [pid for pid in ALL_NODES if pid != self.node_id]

    def _catch_up_from_peers(self) -> None:
        for pid in self._peer_ids():
            try:
                with self._peer_proxy(pid) as proxy:
                    info = proxy.ping()
                    if info.get("is_primary"):
                        self.is_primary = False
                        remote_state = proxy.get_state()
                        self.storage.save_state(PetState.from_dict(remote_state))
                        emit_event(
                            f"storage_{self.node_id}",
                            f"Catch-up concluido a partir do Lider {pid} (hash {remote_state.get('hash')})",
                        )
                        return
            except Exception:
                continue

    def _active_peers(self) -> list[tuple[int, dict]]:
        alive = []
        for pid in self._peer_ids():
            try:
                with self._peer_proxy(pid) as proxy:
                    alive.append((pid, proxy.ping()))
            except Exception:
                continue
        return alive

    def _maybe_elect_self(self) -> bool:
        alive = self._active_peers()
        if any(info.get("is_primary") for _, info in alive):
            return False
        lowest_alive = min([self.node_id] + [pid for pid, _ in alive])
        if lowest_alive == self.node_id:
            self.is_primary = True
            self._log(f"Failover P2P: nenhum lider ativo; No {self.node_id} assumiu lideranca", "WARN")
            return True
        return False

    def _replicate_to_peers(self, state: PetState, command_id: str | None, kind: str, payload: str) -> None:
        state_dict = state.to_dict()
        for pid in self._peer_ids():
            try:
                with self._peer_proxy(pid) as proxy:
                    proxy.sync_state(state_dict, command_id, kind, payload)
            except Exception as exc:
                self._log(f"Replica {pid} inacessivel no sync: {exc}", "WARN")

    def _publish_actuators(self, actuators: list[dict], command_id: str) -> None:
        for cmd in actuators:
            body = {**cmd["payload"], "command_id": command_id}
            self.mqtt.publish(cmd["topic"], json.dumps(body), qos=2)
            emit_event("mqtt", f"PUB {cmd['topic']} -> {body}")

    def register_observer(self, client_uri: str) -> bool:
        with self._obs_lock:
            self._observers.add(client_uri)
        return True

    def unregister_observer(self, client_uri: str) -> bool:
        with self._obs_lock:
            self._observers.discard(client_uri)
        return True

    def _notify_observers(self, state: PetState, reason: str, event_id: str | None = None) -> None:
        with self._obs_lock:
            uris = list(self._observers)
        if not uris:
            return
        payload = {**self._state_snapshot(state), "reason": reason, "event_id": event_id}

        def _push() -> None:
            dead: list[str] = []
            for uri in uris:
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    proxy._pyroTimeout = 1.0
                    with proxy:
                        proxy.on_state_changed(payload)
                except Exception:
                    dead.append(uri)
            if dead:
                with self._obs_lock:
                    for uri in dead:
                        self._observers.discard(uri)

        threading.Thread(target=_push, daemon=True).start()

    def _forged_state(self) -> PetState:
        """Retorna um estado adulterado unico por no para simular falha bizantina."""
        return PetState(
            racao_gramas=9000.0 + self.node_id,
            agua_ml=0.0,
            areia_usos=99,
            status_fuga="FORA",
            alarme_ativo=True,
        )

    def _on_mqtt_message(self, client, userdata, msg) -> None:
        sensor = msg.topic.split("/")[-1]
        raw = msg.payload.decode("utf-8", errors="replace")

        if self.byzantine:
            return

        if not self.is_primary and not self._maybe_elect_self():
            return

        with self._lock:
            if not self.is_primary:
                return
            try:
                payload = json.loads(raw)
                current = self.storage.load_state()
                nxt, actuators = apply_telemetry(current, sensor, payload)
                cmd_id = str(uuid.uuid4())
                self.storage.save_state(nxt)
                self.storage.record_event(cmd_id, f"TELEMETRIA_{sensor.upper()}", raw)
                self._log(f"Telemetria MQTT {sensor} processada ({cmd_id[:8]})")
                emit_event(f"storage_{self.node_id}", f"Persistido {sensor} (hash {nxt.canonical_hash()})")
                self._publish_actuators(actuators, cmd_id)
                self._replicate_to_peers(nxt, cmd_id, f"TELEMETRIA_{sensor.upper()}", raw)
                self._notify_observers(nxt, f"TELEMETRIA_{sensor.upper()}", cmd_id)
            except Exception as exc:
                self._log(f"Telemetria rejeitada ({sensor}): {exc}", "ERROR")

    def ping(self) -> dict:
        state = self._visible_state()
        return {
            "id": self.node_id,
            "is_primary": self.is_primary,
            "byzantine": self.byzantine,
            "hash": state.canonical_hash(),
        }

    def is_leader(self) -> bool:
        return self.is_primary

    def is_byzantine(self) -> bool:
        return self.byzantine

    def set_byzantine(self, flag: bool) -> bool:
        with self._lock:
            self.byzantine = bool(flag)
            if self.byzantine:
                self._log(f"Modo BIZANTINO ativado: No {self.node_id} enviando estado adulterado", "ERROR")
            else:
                self._log(f"Modo BIZANTINO desativado: No {self.node_id} restaurado", "WARN")
                self._catch_up_from_peers()
            self._notify_observers(self._visible_state(), "BYZANTINE_TOGGLE")
            return self.byzantine

    def set_primary(self, flag: bool) -> bool:
        self.is_primary = bool(flag)
        self._log(f"Papel alterado para {self._role()}", "WARN")
        self._notify_observers(self._visible_state(), "ROLE_CHANGE")
        return self.is_primary

    def get_state(self) -> dict:
        return self._state_snapshot(self._visible_state())

    def sync_state(self, state_dict: dict, command_id: str | None = None, kind: str = "SYNC", payload: str = "{}") -> bool:
        with self._lock:
            if self.byzantine:
                return False
            state = PetState.from_dict(state_dict)
            self.storage.save_state(state)
            if command_id:
                self.storage.record_event(command_id, kind, payload)
            self._log(f"Sync P2P aplicado ({kind})")
            emit_event(f"storage_{self.node_id}", f"Sync replicado (hash {state.canonical_hash()})")
            self._notify_observers(state, f"SYNC_{kind}", command_id)
            return True

    def execute_command(self, action: str, command_id: str) -> dict:
        with self._lock:
            if self.byzantine:
                self._log(f"Controlador {self.node_id} (BIZANTINO) adulterou comando {action}", "ERROR")
                return {"ok": False, "error": "BYZANTINE_FAULT"}
            if not self.is_primary:
                return {"ok": False, "error": "NOT_PRIMARY"}
            if self.storage.has_event(command_id):
                current = self.storage.load_state()
                self._log(f"Idempotencia: comando {command_id[:8]} ({action}) ja registrado; ignorado", "WARN")
                return {"ok": True, "idempotent": True, "state": current.to_dict()}

            current = self.storage.load_state()
            nxt, actuators = apply_command(current, action)
            payload_str = json.dumps({"acao": action})
            self.storage.save_state(nxt)
            self.storage.record_event(command_id, action, payload_str)
            self._log(f"Comando {action} executado ({command_id[:8]})")
            emit_event(f"storage_{self.node_id}", f"Estado salvo (hash {nxt.canonical_hash()})")
            self._publish_actuators(actuators, command_id)
            self._replicate_to_peers(nxt, command_id, action, payload_str)
            self._notify_observers(nxt, action, command_id)
            return {"ok": True, "idempotent": False, "state": nxt.to_dict()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--primary", action="store_true")
    args = parser.parse_args()

    controller = PetController(args.id, is_primary=args.primary)
    daemon = Pyro5.api.Daemon(host="localhost", port=BASE_PORT + args.id)
    daemon.register(controller, f"petcare.controller.{args.id}")

    def shutdown(signum, frame):
        emit_event(f"controller_{args.id}", f"Controlador {args.id} encerrado (sinal {signum})", "WARN")
        controller.mqtt.loop_stop()
        daemon.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    daemon.requestLoop()


if __name__ == "__main__":
    main()

