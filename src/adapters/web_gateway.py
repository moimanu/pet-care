from __future__ import annotations

from collections import deque
from dataclasses import asdict, is_dataclass
from datetime import datetime
import http.server
import json
import logging
from pathlib import Path
import queue
import socketserver
import threading
import time
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

MIME_TYPES = {
    ".html": "text/html", ".css": "text/css", ".js": "application/javascript",
    ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon",
}
CORS_HEADERS = (
    ("Access-Control-Allow-Origin", "*"),
    ("Access-Control-Allow-Headers", "Content-Type"),
    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
)


class WebGatewayLogHandler(logging.Handler):
    """Logging handler streaming log records over WebGateway SSE."""

    def __init__(self, gateway: WebGateway, component: str = "") -> None:
        super().__init__()
        self.gateway, self.component = gateway, component

    def emit(self, record: logging.LogRecord) -> None:
        try:
            lvl = "WARN" if record.levelname == "WARNING" else record.levelname
            ts = datetime.fromtimestamp(record.created).astimezone().isoformat()
            entry = {"source": "web", "line": f"{ts} [{lvl}] ({self.component or record.name}) {record.getMessage()}"}
            if self.gateway.tailer:
                self.gateway.tailer.record_entry(entry)
            self.gateway.broadcast("raw_log", entry)
        except Exception:
            self.handleError(record)


class LogFileTailer:
    """Continuously tails raw process log files (*.log) and streams lines over SSE."""

    SOURCE_MAP = {"controller_1": "node-1", "controller_2": "node-2", "controller_3": "node-3", "iot_devices": "iot"}

    def __init__(
        self, gateway: WebGateway, logs_dir: str | Path = "logs",
        poll_interval: float = 0.25, initial_lines: int = 20,
    ) -> None:
        self.gateway, self.logs_dir = gateway, Path(logs_dir)
        self.poll_interval, self.initial_lines = poll_interval, initial_lines
        self._offsets: dict[str, int] = {}
        self._history: deque[dict[str, str]] = deque(maxlen=200)
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def get_recent_history(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._history)

    def record_entry(self, entry: dict[str, str]) -> None:
        with self._lock:
            self._history.append(entry)

    def _emit_lines(self, source_id: str, lines: list[str]) -> None:
        for raw in lines:
            if line := raw.rstrip("\r\n"):
                entry = {"source": source_id, "line": line}
                self.record_entry(entry)
                self.gateway.broadcast("raw_log", entry)

    def poll_once(self) -> None:
        if not self.logs_dir.is_dir():
            return
        for log_file in sorted(self.logs_dir.glob("*.log")):
            source_id, key = self.SOURCE_MAP.get(log_file.stem, log_file.stem), str(log_file)
            try:
                size = log_file.stat().st_size
                if key not in self._offsets:
                    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    self._emit_lines(source_id, lines[-self.initial_lines:])
                    self._offsets[key] = size
                elif size > (offset := 0 if size < self._offsets[key] else self._offsets[key]):
                    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(offset)
                        self._emit_lines(source_id, f.read().splitlines())
                        self._offsets[key] = f.tell()
            except Exception:
                self._offsets.setdefault(key, 0)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()

    def _loop(self) -> None:
        while self._running:
            self.poll_once()
            time.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
            self._thread = None


class _GatewayHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for REST endpoints, SSE streams, and static assets."""

    server: _GatewayServer

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_json(self, status_code: int, data: Mapping[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        for k, v in (("Content-Type", "application/json"), ("Content-Length", str(len(body))), *CORS_HEADERS):
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        if (length := int(self.headers.get("Content-Length", 0))) <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            return {}

    def _with_mw(self, fn: Callable[[Any], Mapping[str, Any]], err_code: int = 500) -> None:
        if (mw := self.server.gateway.middleware) is None:
            return self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
        try:
            self._send_json(200, fn(mw))
        except Exception as exc:
            self._send_json(err_code, {"sucesso": False, "mensagem": str(exc)})

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        for k, v in CORS_HEADERS:
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/api/status":
            self._with_mw(lambda mw: mw.get_pet_status())
        elif path == "/api/nodes":
            self._with_mw(lambda mw: {"sucesso": True, "nodes": getattr(mw, "get_nodes_info", lambda: [])()})
        elif path == "/api/events":
            self._handle_sse()
        else:
            self._handle_static({"/docs": "docs.html", "/openapi.json": "openapi.json"}.get(path, path))

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        if path.startswith("/api/command/"):
            self._handle_command(path.removeprefix("/api/command/"))
        elif path.startswith("/api/nodes/"):
            parts = path.removeprefix("/api/nodes/").split("/")
            if len(parts) >= 2:
                self._handle_node_action(parts[0], parts[1])
            else:
                self._send_json(400, {"sucesso": False, "mensagem": f"Caminho inválido: {path}"})
        else:
            self._send_json(404, {"sucesso": False, "mensagem": f"Rota não encontrada: {path}"})

    def _handle_node_action(self, node_id_str: str, action: str) -> None:
        try:
            node_id = int(node_id_str)
        except ValueError:
            return self._send_json(400, {"sucesso": False, "mensagem": f"ID inválido: {node_id_str}"})

        actions = {"toggle_offline": ("offline", "set_node_offline"), "toggle_corrupt": ("corrupt", "set_node_corrupt")}
        if action not in actions:
            return self._send_json(404, {"sucesso": False, "mensagem": f"Ação desconhecida: {action}"})

        field, method = actions[action]

        def _toggle(mw: Any) -> Mapping[str, Any]:
            nodes = getattr(mw, "get_nodes_info", lambda: [])()
            new_val = not next((n.get(field, False) for n in nodes if n.get("id") == node_id), False)
            res = getattr(mw, method)(node_id, new_val)
            return res if isinstance(res, Mapping) else {"sucesso": bool(res), "node_id": node_id, field: new_val}

        self._with_mw(_toggle)

    def _handle_command(self, action: str) -> None:
        body = self._read_json()
        cid = body.get("command_id")
        cmds: dict[str, Callable[[Any], Any]] = {
            "dispense_food": lambda mw: mw.dispensar_racao(float(body.get("quantidade", 50.0)), command_id=cid),
            "refill_water": lambda mw: mw.repor_agua(float(body.get("quantidade", 200.0)), command_id=cid),
            "clean_litter": lambda mw: mw.resetar_areia(command_id=cid),
            "silence_alarm": lambda mw: mw.silenciar_alarme(command_id=cid),
        }
        if action not in cmds:
            return self._send_json(404, {"sucesso": False, "mensagem": f"Comando desconhecido: {action}"})

        def _exec(mw: Any) -> Mapping[str, Any]:
            raw = cmds[action](mw)
            if is_dataclass(raw) and not isinstance(raw, type):
                st = getattr(raw, "estado", None)
                res = {"sucesso": bool(raw.sucesso), "mensagem": str(raw.mensagem), "estado": asdict(st) if is_dataclass(st) else st}
            else:
                res = dict(raw)
            if res.get("sucesso") and res.get("estado"):
                self.server.gateway.broadcast("state", res["estado"])
            return res

        self._with_mw(_exec, err_code=400)

    def _handle_sse(self) -> None:
        gw = self.server.gateway
        client_q = gw.subscribe_sse()
        self.send_response(200)
        for k, v in (("Content-Type", "text/event-stream"), ("Cache-Control", "no-cache"), ("Connection", "keep-alive"), ("Access-Control-Allow-Origin", "*")):
            self.send_header(k, v)
        self.end_headers()

        try:
            if gw.middleware and (st := gw.middleware.get_pet_status()).get("sucesso") and st.get("estado"):
                self.wfile.write(f"event: state\ndata: {json.dumps(st['estado'])}\n\n".encode("utf-8"))
            if gw.tailer:
                for entry in gw.tailer.get_recent_history():
                    self.wfile.write(f"event: raw_log\ndata: {json.dumps(entry)}\n\n".encode("utf-8"))
            self.wfile.flush()

            while gw.is_running:
                try:
                    self.wfile.write(client_q.get(timeout=1.0).encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except Exception:
            pass
        finally:
            gw.unsubscribe_sse(client_q)

    def _handle_static(self, req_path: str) -> None:
        if not (static_dir := self.server.gateway.static_dir):
            return self._send_json(404, {"sucesso": False, "mensagem": "Diretório estático ausente"})

        base_dir = Path(static_dir).resolve()
        file_path = (base_dir / (req_path.lstrip("/") or "index.html")).resolve()
        if not str(file_path).startswith(str(base_dir)):
            return self._send_json(403, {"sucesso": False, "mensagem": "Acesso negado"})
        if not file_path.is_file():
            return self._send_json(404, {"sucesso": False, "mensagem": "Arquivo não encontrado"})

        try:
            content = file_path.read_bytes()
            self.send_response(200)
            for k, v in (("Content-Type", MIME_TYPES.get(file_path.suffix.lower(), "application/octet-stream")), ("Content-Length", str(len(content))), ("Cache-Control", "no-cache")):
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(content)
        except Exception as exc:
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})


class _GatewayServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], gateway: WebGateway) -> None:
        self.gateway = gateway
        super().__init__(server_address, _GatewayHTTPRequestHandler)


class WebGateway:
    """Thread-safe HTTP/SSE Web Gateway aggregating telemetry, logs, and RPC commands."""

    def __init__(
        self, host: str = "0.0.0.0", port: int = 8080, middleware: Optional[Any] = None,
        static_dir: Optional[str] = None, logs_dir: Optional[str] = None,
    ) -> None:
        self.host, self._requested_port = host, port
        self.middleware, self.static_dir, self.logs_dir = middleware, static_dir, logs_dir
        self.tailer = LogFileTailer(gateway=self, logs_dir=logs_dir) if logs_dir else None
        self._clients_lock = threading.Lock()
        self._subscribers: set[queue.Queue[str]] = set()
        self._server: Optional[_GatewayServer] = None
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

    @property
    def port(self) -> int:
        return int(self._server.server_address[1]) if self._server else self._requested_port

    @property
    def is_running(self) -> bool:
        return self._is_running

    def subscribe_sse(self) -> queue.Queue[str]:
        q: queue.Queue[str] = queue.Queue(maxsize=100)
        with self._clients_lock:
            self._subscribers.add(q)
        return q

    def unsubscribe_sse(self, q: queue.Queue[str]) -> None:
        with self._clients_lock:
            self._subscribers.discard(q)

    def broadcast(self, event_type: str, data: Any) -> None:
        raw = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
        with self._clients_lock:
            subscribers = list(self._subscribers)
        for q in subscribers:
            try:
                q.put_nowait(raw)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(raw)
                except Exception:
                    pass

    def start(self) -> None:
        if self._is_running:
            return
        self._server = _GatewayServer((self.host, self._requested_port), self)
        self._is_running = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if self.tailer:
            self.tailer.start()

    def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        if self.tailer:
            self.tailer.stop()
            self.tailer = None
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        with self._clients_lock:
            self._subscribers.clear()
