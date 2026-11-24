"""Servidor HTTP/SSE do painel web e ponte com o cliente RPC, SQLite e MQTT."""

from contextlib import suppress
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
from typing import Any, Callable

import Pyro5.api

from app.config import (
    ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT, TOPIC_SIM_CONTROL,
    WEB_HOST, WEB_OBSERVER_NAME, WEB_PORT, client_uri, node_db_path,
)
from app.event_bus import LOG_FILE, add_listener, emit_event
from app.infra import PushReceiver, connect_mqtt, install_shutdown_handler, publish_json, rpc_proxy, suppress_rpc_errors
from app.process_manager import ProcessManager
from app.storage import NodeStorage

WEB_DIR = Path("web")


class AppContext:
    """Orquestra estado do painel web, stream SSE, ponte MQTT e comandos para o cluster."""

    def __init__(self) -> None:
        self.procs = ProcessManager()
        self.storages = {node_id: NodeStorage(node_db_path(node_id)) for node_id in ALL_NODES}
        self.last_consensus: dict | None = None
        self.mqtt_connected = False
        self._sse_cond = threading.Condition()
        self._version = 0
        self._closing = False
        self._ui_daemon: Pyro5.api.Daemon | None = None
        self._ui_observer_uri: str | None = None

        add_listener(self.notify_clients)
        self.mqtt = connect_mqtt(
            "petcare_web_bridge",
            on_connect=self._on_mqtt_connect,
            on_disconnect=self._on_mqtt_disconnect,
            on_error=lambda exc: emit_event("mqtt", f"Broker indisponivel em {MQTT_HOST}:{MQTT_PORT} ({exc})", "ERROR"),
        )

    def _client_rpc(self, timeout: float = 1.5) -> Pyro5.api.Proxy:
        return rpc_proxy(client_uri(), timeout=timeout)

    def start_ui_observer(self) -> None:
        self._ui_daemon = Pyro5.api.Daemon(host="localhost", port=0)
        uri = self._ui_daemon.register(PushReceiver(self.on_consensus_push), WEB_OBSERVER_NAME)
        self._ui_observer_uri = str(uri)
        threading.Thread(target=self._ui_daemon.requestLoop, daemon=True).start()
        self._sync_client_async(lambda rpc: bool(rpc.register_ui_observer(self._ui_observer_uri)))

    def stop_ui_observer(self) -> None:
        if self._ui_observer_uri:
            with suppress_rpc_errors():
                with self._client_rpc(1.0) as rpc:
                    rpc.unregister_ui_observer(self._ui_observer_uri)
        if self._ui_daemon is not None:
            with suppress_rpc_errors():
                self._ui_daemon.shutdown()
            self._ui_daemon = None

    def _sync_client_async(self, step: Callable[[Any], bool], attempts: int = 25, delay: float = 0.15) -> None:
        """Executa `step(rpc)` em background com retentativas e sincroniza o consenso inicial."""
        def _run() -> None:
            for _ in range(attempts):
                with suppress_rpc_errors():
                    with self._client_rpc(1.0) as rpc:
                        if step(rpc):
                            self.last_consensus = rpc.get_consensus_state(log_result=False)
                            self.notify_clients()
                            return
                time.sleep(delay)

        threading.Thread(target=_run, daemon=True).start()

    def notify_clients(self) -> None:
        with self._sse_cond:
            self._version += 1
            self._sse_cond.notify_all()

    def wait_next_version(self, last_version: int) -> int | None:
        """Bloqueia ate haver nova atualizacao SSE ou encerramento do servidor."""
        with self._sse_cond:
            while self._version == last_version and not self._closing:
                self._sse_cond.wait()
            return None if self._closing else self._version

    def close(self) -> None:
        self._closing = True
        self.notify_clients()

    def on_consensus_push(self, consensus: dict) -> None:
        self.last_consensus = consensus
        self.notify_clients()

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self.mqtt_connected = True
        emit_event("mqtt", f"Broker Mosquitto conectado em {MQTT_HOST}:{MQTT_PORT}")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self.mqtt_connected = False
        emit_event("mqtt", "Desconectado do broker Mosquitto", "WARN")

    def build_state_payload(self) -> dict:
        node_map: dict[int, dict] = {}
        with suppress_rpc_errors():
            with self._client_rpc(1.5) as rpc:
                node_map = {item["node_id"]: item for item in rpc.get_nodes_status()}
                latest = rpc.get_last_consensus()
                if latest is not None:
                    self.last_consensus = latest

        consensus = self.last_consensus or {}
        controllers, storage = [], []
        for node_id in ALL_NODES:
            rpc_info = node_map.get(node_id, {})
            data = rpc_info.get("data") or {}
            controllers.append({
                "id": node_id,
                "alive": self.procs.is_alive(f"controller_{node_id}"),
                "rpc_ok": bool(rpc_info.get("ok")),
                "is_primary": bool(data.get("is_primary", False)),
                "byzantine": bool(data.get("byzantine", False)),
                "hash": data.get("hash"),
                "divergent": node_id in consensus.get("divergent", []),
                "port": BASE_PORT + node_id,
            })
            storage.append({"id": node_id, "db": node_db_path(node_id), **self.storages[node_id].get_snapshot()})

        return {
            "clients": {
                "alive": self.procs.is_alive("client"),
                "quorum_reached": consensus.get("quorum_reached"),
                "votes": consensus.get("votes", "--"),
                "majority_hash": consensus.get("majority_hash"),
                "state": consensus.get("state"),
                "divergent": consensus.get("divergent", []),
            },
            "controllers": controllers,
            "storage": storage,
            "mqtt": {"connected": self.mqtt_connected, "endpoint": f"{MQTT_HOST}:{MQTT_PORT}"},
            "sensors": {"alive": self.procs.is_alive("sensors")},
        }

    def read_recent_logs(self, limit: int = 120) -> list[dict]:
        if not LOG_FILE.exists():
            return []
        entries: list[dict] = []
        for line in LOG_FILE.read_text(encoding="utf-8").splitlines()[-limit:]:
            with suppress(json.JSONDecodeError):
                entries.append(json.loads(line))
        return entries

    def dispatch_action(self, body: dict) -> dict:
        """Roteia acoes do painel web (POST /api/action) para RPC, processos, SQLite ou MQTT."""
        match body.get("op"):
            case "client_command":
                with self._client_rpc(4.0) as rpc:
                    res = rpc.execute_command(str(body["action"]), body.get("command_id"))
                    self.last_consensus = rpc.get_consensus_state(log_result=False, enforce_leader=False)
                self.notify_clients()
                return res
            case "rpc_quorum" | "rpc_read":
                with self._client_rpc(2.5) as rpc:
                    self.last_consensus = rpc.get_consensus_state(log_result=True)
                self.notify_clients()
                return self.last_consensus
            case "rpc_promote":
                with self._client_rpc(2.0) as rpc:
                    ok = rpc.promote_node(int(body["node_id"]))
                self.notify_clients()
                return {"ok": ok}
            case "controller_stop":
                ok = self.procs.stop_controller(int(body["node_id"]))
                self.notify_clients()
                return {"ok": ok}
            case "controller_start":
                node_id = int(body["node_id"])
                ok = self.procs.start_controller(node_id)
                if ok:
                    self._sync_client_async(lambda rpc: bool(rpc.register_observer_on_node(node_id)), attempts=20)
                self.notify_clients()
                return {"ok": ok}
            case "controller_byzantine":
                with self._client_rpc(2.0) as rpc:
                    ok = rpc.set_byzantine(int(body["node_id"]), bool(body.get("enabled", True)))
                self.notify_clients()
                return {"ok": ok}
            case "client_toggle_proc":
                if self.procs.is_alive("client"):
                    return {"ok": self.procs.stop_service("client")}
                ok = self.procs.start_service("client")
                if ok and self._ui_observer_uri:
                    self._sync_client_async(lambda rpc: bool(rpc.register_ui_observer(self._ui_observer_uri)))
                return {"ok": ok}
            case "storage_mutate":
                node_id, field, value = int(body["node_id"]), str(body["field"]), body["value"]
                snap = self.storages[node_id].update_field_directly(field, value)
                emit_event(f"storage_{node_id}", f"Edicao direta em {node_db_path(node_id)}: {field}={value} (hash {snap['hash']})", "WARN")
                return {"ok": True, "snapshot": snap}
            case "mqtt_publish":
                publish_json(self.mqtt, str(body["topic"]), body["payload"], qos=1, log_prefix="PUB manual")
                return {"ok": True}
            case "sensor_publish":
                publish_json(self.mqtt, TOPIC_SIM_CONTROL, {"kind": "telemetry", "sensor": body["sensor"], "payload": body["payload"]}, qos=1, log_prefix="")
                return {"ok": True}
            case "sensor_auto":
                publish_json(self.mqtt, TOPIC_SIM_CONTROL, {"kind": "auto", "enabled": bool(body["enabled"])}, qos=1, log_prefix="")
                return {"ok": True}
            case "sensor_toggle_proc":
                return {"ok": self.procs.stop_service("sensors") if self.procs.is_alive("sensors") else self.procs.start_service("sensors")}
            case op:
                raise ValueError(f"Operacao desconhecida: {op}")


def make_handler(ctx: AppContext):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(WEB_DIR), **kwargs)

        def log_message(self, format, *args):
            return

        def _send_json(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            if self.path == "/api/state":
                self._send_json(200, ctx.build_state_payload())
                return
            if self.path == "/api/events":
                self._stream_sse()
                return
            if self.path == "/":
                self.path = "/index.html"
            super().do_GET()

        def _stream_sse(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            last_version = -1
            try:
                while (next_version := ctx.wait_next_version(last_version)) is not None:
                    last_version = next_version
                    payload = {"state": ctx.build_state_payload(), "logs": ctx.read_recent_logs(150)}
                    self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return

        def do_POST(self) -> None:
            if self.path != "/api/action":
                self._send_json(404, {"error": "Not found"})
                return
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
            try:
                self._send_json(200, ctx.dispatch_action(json.loads(raw)))
            except Exception as exc:
                self._send_json(400, {"ok": False, "error": str(exc)})

    return Handler


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text("", encoding="utf-8")

    ctx = AppContext()
    ctx.procs.start_all()
    ctx.start_ui_observer()
    server = ThreadingHTTPServer((WEB_HOST, WEB_PORT), make_handler(ctx))
    server.daemon_threads = True
    emit_event("clients", f"Servidor web ativo em http://localhost:{WEB_PORT}")

    def shutdown(_signum: int) -> None:
        ctx.close()
        threading.Thread(target=server.shutdown, daemon=True).start()

    install_shutdown_handler(shutdown)
    try:
        server.serve_forever()
    finally:
        ctx.close()
        ctx.stop_ui_observer()
        ctx.procs.stop_all()
        ctx.mqtt.loop_stop()
        server.server_close()


if __name__ == "__main__":
    main()
