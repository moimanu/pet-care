from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Callable

import paho.mqtt.client as mqtt
import Pyro5.api

from app.config import ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT, client_uri
from app.event_bus import LOG_FILE, add_listener, emit_event
from app.storage import NodeStorage

WEB_DIR = Path("web")
HOST = "0.0.0.0"
PORT = 8080


@Pyro5.api.expose
class WebConsensusObserver:
    """Recebe notificacoes de consenso do processo client via Pyro5."""

    def __init__(self, on_consensus: Callable[[dict], None]) -> None:
        self._on_consensus = on_consensus

    @Pyro5.api.oneway
    def on_consensus(self, consensus: dict) -> None:
        self._on_consensus(consensus)


class ProcessManager:
    """Gerencia os processos reais dos controladores, sensores e cliente RPC."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.controllers: dict[int, subprocess.Popen] = {}
        self._stopping: set[int] = set()
        self.sensors: subprocess.Popen | None = None
        self.client: subprocess.Popen | None = None

    def start_all(self) -> None:
        for nid in ALL_NODES:
            self.start_controller(nid, primary=(nid == 1))
        time.sleep(1.0)
        self.start_sensors()
        self.start_client()

    def start_controller(self, node_id: int, primary: bool = False) -> bool:
        with self._lock:
            proc = self.controllers.get(node_id)
            if proc and proc.poll() is None:
                return False
            self._stopping.discard(node_id)
            cmd = [sys.executable, "-m", "app.controller", "--id", str(node_id)]
            if primary:
                cmd.append("--primary")
            self.controllers[node_id] = subprocess.Popen(cmd)
            return True

    def stop_controller(self, node_id: int) -> bool:
        with self._lock:
            proc = self.controllers.get(node_id)
            if not proc or proc.poll() is not None:
                return False
            self._stopping.add(node_id)
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1.0)
            finally:
                self._stopping.discard(node_id)
            emit_event(f"controller_{node_id}", f"Processo do Controlador {node_id} parado", "WARN")
            return True

    def is_controller_alive(self, node_id: int) -> bool:
        if node_id in self._stopping:
            return False
        proc = self.controllers.get(node_id)
        return proc is not None and proc.poll() is None

    def start_sensors(self) -> bool:
        with self._lock:
            if self.sensors and self.sensors.poll() is None:
                return False
            self.sensors = subprocess.Popen([sys.executable, "-m", "app.sensors"])
            return True

    def stop_sensors(self) -> bool:
        with self._lock:
            if not self.sensors or self.sensors.poll() is not None:
                return False
            self.sensors.terminate()
            try:
                self.sensors.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                self.sensors.kill()
            emit_event("sensors", "Processo dos Sensores parado", "WARN")
            return True

    def is_sensors_alive(self) -> bool:
        return self.sensors is not None and self.sensors.poll() is None

    def start_client(self) -> bool:
        with self._lock:
            if self.client and self.client.poll() is None:
                return False
            self.client = subprocess.Popen([sys.executable, "-m", "app.client"])
            return True

    def stop_client(self) -> bool:
        with self._lock:
            if not self.client or self.client.poll() is not None:
                return False
            self.client.terminate()
            try:
                self.client.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                self.client.kill()
            emit_event("rpc", "Processo do Cliente RPC parado", "WARN")
            return True

    def is_client_alive(self) -> bool:
        return self.client is not None and self.client.poll() is None

    def stop_all(self) -> None:
        self.stop_client()
        self.stop_sensors()
        for nid in ALL_NODES:
            self.stop_controller(nid)


class AppContext:
    def __init__(self) -> None:
        self.procs = ProcessManager()
        self.last_consensus: dict | None = None
        self.mqtt_connected = False
        self._sse_cond = threading.Condition()
        self._version = 0
        self._closing = False
        self._ui_daemon: Pyro5.api.Daemon | None = None
        self._ui_observer_uri: str | None = None
        add_listener(self.notify_clients)
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="petcare_web_bridge")
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_disconnect = self._on_disconnect
        self._connect_mqtt()

    def _rpc_proxy(self, timeout: float = 2.0) -> Pyro5.api.Proxy:
        proxy = Pyro5.api.Proxy(client_uri())
        proxy._pyroTimeout = timeout
        return proxy

    def start_ui_observer(self) -> None:
        self._ui_daemon = Pyro5.api.Daemon(host="localhost", port=0)
        uri = self._ui_daemon.register(WebConsensusObserver(self.on_consensus_push), "petcare.web.observer")
        self._ui_observer_uri = str(uri)
        threading.Thread(target=self._ui_daemon.requestLoop, daemon=True).start()
        self._register_ui_observer_async()

    def stop_ui_observer(self) -> None:
        if self._ui_observer_uri:
            try:
                with self._rpc_proxy(timeout=1.0) as rpc:
                    rpc.unregister_ui_observer(self._ui_observer_uri)
            except Exception:
                pass
        if self._ui_daemon is not None:
            try:
                self._ui_daemon.shutdown()
            except Exception:
                pass
            self._ui_daemon = None

    def _register_ui_observer_async(self) -> None:
        def _run() -> None:
            if not self._ui_observer_uri:
                return
            for _ in range(25):
                try:
                    with self._rpc_proxy(timeout=1.0) as rpc:
                        rpc.register_ui_observer(self._ui_observer_uri)
                        self.last_consensus = rpc.get_consensus_state(log_result=False)
                        self.notify_clients()
                        return
                except Exception:
                    time.sleep(0.15)

        threading.Thread(target=_run, daemon=True).start()

    def notify_clients(self) -> None:
        with self._sse_cond:
            self._version += 1
            self._sse_cond.notify_all()

    def on_consensus_push(self, consensus: dict) -> None:
        self.last_consensus = consensus
        self.notify_clients()

    def _register_node_async(self, node_id: int) -> None:
        def _run() -> None:
            for _ in range(20):
                try:
                    with self._rpc_proxy(timeout=1.0) as rpc:
                        if rpc.register_observer_on_node(node_id):
                            self.last_consensus = rpc.get_consensus_state(log_result=False)
                            self.notify_clients()
                            return
                except Exception:
                    pass
                time.sleep(0.15)

        threading.Thread(target=_run, daemon=True).start()

    def _connect_mqtt(self) -> None:
        try:
            self.mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            self.mqtt.loop_start()
        except Exception as exc:
            emit_event("mqtt", f"Broker indisponivel em {MQTT_HOST}:{MQTT_PORT} ({exc})", "ERROR")

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self.mqtt_connected = True
        emit_event("mqtt", f"Broker Mosquitto conectado em {MQTT_HOST}:{MQTT_PORT}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self.mqtt_connected = False
        emit_event("mqtt", "Desconectado do broker Mosquitto", "WARN")

    def build_state_payload(self) -> dict:
        node_map: dict[int, dict] = {}
        try:
            with self._rpc_proxy(timeout=1.5) as rpc:
                node_map = {n["node_id"]: n for n in rpc.get_nodes_status()}
                latest = rpc.get_last_consensus()
                if latest is not None:
                    self.last_consensus = latest
        except Exception:
            pass
        consensus = self.last_consensus or {}

        controllers = []
        storage = []
        for nid in ALL_NODES:
            alive = self.procs.is_controller_alive(nid)
            rpc_info = node_map.get(nid, {})
            rpc_ok = bool(rpc_info.get("ok"))
            data = rpc_info.get("data") or {}
            controllers.append({
                "id": nid,
                "alive": alive,
                "rpc_ok": rpc_ok,
                "is_primary": bool(data.get("is_primary", False)),
                "byzantine": bool(data.get("byzantine", False)),
                "hash": data.get("hash"),
                "divergent": nid in consensus.get("divergent", []),
                "port": BASE_PORT + nid,
            })
            snap = NodeStorage(f"node_{nid}.db").get_snapshot()
            storage.append({"id": nid, "db": f"node_{nid}.db", **snap})

        return {
            "clients": {
                "alive": self.procs.is_client_alive(),
                "quorum_reached": consensus.get("quorum_reached"),
                "votes": consensus.get("votes", "--"),
                "majority_hash": consensus.get("majority_hash"),
                "state": consensus.get("state"),
                "divergent": consensus.get("divergent", []),
            },
            "controllers": controllers,
            "storage": storage,
            "mqtt": {"connected": self.mqtt_connected, "endpoint": f"{MQTT_HOST}:{MQTT_PORT}"},
            "sensors": {"alive": self.procs.is_sensors_alive()},
        }

    def read_recent_logs(self, limit: int = 120) -> list[dict]:
        if not LOG_FILE.exists():
            return []
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()[-limit:]
        out = []
        for line in lines:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    def dispatch_action(self, body: dict) -> dict:
        op = body.get("op")

        if op == "client_command":
            with self._rpc_proxy(timeout=4.0) as rpc:
                res = rpc.execute_command(str(body["action"]), body.get("command_id"))
                self.last_consensus = rpc.get_consensus_state(log_result=False, enforce_leader=False)
            self.notify_clients()
            return res

        if op in ("rpc_quorum", "rpc_read"):
            with self._rpc_proxy(timeout=2.5) as rpc:
                self.last_consensus = rpc.get_consensus_state(log_result=True)
            self.notify_clients()
            return self.last_consensus

        if op == "rpc_promote":
            with self._rpc_proxy(timeout=2.0) as rpc:
                ok = rpc.promote_node(int(body["node_id"]))
            self.notify_clients()
            return {"ok": ok}

        if op == "controller_stop":
            ok = self.procs.stop_controller(int(body["node_id"]))
            self.notify_clients()
            return {"ok": ok}

        if op == "controller_start":
            nid = int(body["node_id"])
            ok = self.procs.start_controller(nid)
            if ok:
                self._register_node_async(nid)
            self.notify_clients()
            return {"ok": ok}

        if op == "controller_byzantine":
            with self._rpc_proxy(timeout=2.0) as rpc:
                return {"ok": rpc.set_byzantine(int(body["node_id"]), bool(body.get("enabled", True)))}

        if op == "client_toggle_proc":
            if self.procs.is_client_alive():
                return {"ok": self.procs.stop_client()}
            ok = self.procs.start_client()
            if ok:
                self._register_ui_observer_async()
            return {"ok": ok}

        if op == "storage_mutate":
            nid = int(body["node_id"])
            field = str(body["field"])
            value = body["value"]
            snap = NodeStorage(f"node_{nid}.db").update_field_directly(field, value)
            emit_event(
                f"storage_{nid}",
                f"Edicao direta em node_{nid}.db: {field}={value} (hash {snap['hash']})",
                "WARN",
            )
            return {"ok": True, "snapshot": snap}

        if op == "mqtt_publish":
            topic = str(body["topic"])
            payload = body["payload"]
            raw = payload if isinstance(payload, str) else json.dumps(payload)
            self.mqtt.publish(topic, raw, qos=1)
            emit_event("mqtt", f"PUB manual {topic} -> {raw}")
            return {"ok": True}

        if op == "sensor_publish":
            msg = {"kind": "telemetry", "sensor": body["sensor"], "payload": body["payload"]}
            self.mqtt.publish("petcare/sim/control", json.dumps(msg), qos=1)
            return {"ok": True}

        if op == "sensor_auto":
            msg = {"kind": "auto", "enabled": bool(body["enabled"])}
            self.mqtt.publish("petcare/sim/control", json.dumps(msg), qos=1)
            return {"ok": True}

        if op == "sensor_toggle_proc":
            if self.procs.is_sensors_alive():
                return {"ok": self.procs.stop_sensors()}
            return {"ok": self.procs.start_sensors()}

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
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                last_version = -1
                try:
                    while not ctx._closing:
                        with ctx._sse_cond:
                            while ctx._version == last_version and not ctx._closing:
                                ctx._sse_cond.wait()
                            if ctx._closing:
                                return
                            last_version = ctx._version
                        payload = {
                            "state": ctx.build_state_payload(),
                            "logs": ctx.read_recent_logs(150),
                        }
                        chunk = f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return
            if self.path == "/":
                self.path = "/index.html"
            super().do_GET()

        def do_POST(self) -> None:
            if self.path != "/api/action":
                self._send_json(404, {"error": "Not found"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8")
            try:
                body = json.loads(raw)
                res = ctx.dispatch_action(body)
                self._send_json(200, res)
            except Exception as exc:
                self._send_json(400, {"ok": False, "error": str(exc)})

    return Handler


def main() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text("", encoding="utf-8")

    ctx = AppContext()
    ctx.procs.start_all()
    ctx.start_ui_observer()
    server = ThreadingHTTPServer((HOST, PORT), make_handler(ctx))
    server.daemon_threads = True
    emit_event("clients", f"Servidor web ativo em http://localhost:{PORT}")

    def shutdown(signum, frame):
        ctx._closing = True
        ctx.notify_clients()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever()
    finally:
        ctx._closing = True
        ctx.notify_clients()
        ctx.stop_ui_observer()
        ctx.procs.stop_all()
        ctx.mqtt.loop_stop()
        server.server_close()


if __name__ == "__main__":
    main()
