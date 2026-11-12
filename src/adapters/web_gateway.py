from __future__ import annotations

from collections import deque
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


class WebGatewayLogHandler(logging.Handler):
    """Logging handler streaming log records over WebGateway SSE."""

    def __init__(self, gateway: WebGateway, component: str = "") -> None:
        super().__init__()
        self.gateway, self.component = gateway, component

    def emit(self, record: logging.LogRecord) -> None:
        try:
            lvl = "WARN" if record.levelname == "WARNING" else record.levelname
            ts = datetime.fromtimestamp(record.created).astimezone().isoformat()
            line = f"{ts} [{lvl}] ({self.component or record.name}) {record.getMessage()}"
            self.gateway.broadcast("raw_log", {"source": "web", "line": line})
        except Exception:
            self.handleError(record)


class LogFileTailer:
    """Continuously tails raw process log files (*.log) and streams lines over SSE."""

    SOURCE_MAP = {
        "controller_1": "node-1", "controller_2": "node-2", "controller_3": "node-3",
        "middleware": "middleware", "iot_devices": "iot",
    }

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

    def _emit_lines(self, source_id: str, lines: list[str]) -> None:
        for raw in lines:
            if line := raw.rstrip("\r\n"):
                entry = {"source": source_id, "line": line}
                with self._lock:
                    self._history.append(entry)
                self.gateway.broadcast("raw_log", entry)

    def poll_once(self) -> None:
        if not self.logs_dir.is_dir():
            return
        for log_file in sorted(self.logs_dir.glob("*.log")):
            source_id, file_key = self.SOURCE_MAP.get(log_file.stem, log_file.stem), str(log_file)
            if file_key not in self._offsets:
                try:
                    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    self._emit_lines(source_id, lines[-self.initial_lines:])
                except Exception:
                    pass
                try:
                    self._offsets[file_key] = log_file.stat().st_size
                except OSError:
                    self._offsets[file_key] = 0
                continue
            try:
                size = log_file.stat().st_size
                offset = 0 if size < self._offsets[file_key] else self._offsets[file_key]
                if size > offset:
                    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(offset)
                        self._emit_lines(source_id, f.read().splitlines())
                        self._offsets[file_key] = f.tell()
            except Exception as exc:
                logger.debug("Falha ao ler arquivo de log %s: %s", log_file, exc)

    def _loop(self) -> None:
        while self._running:
            self.poll_once()
            time.sleep(self.poll_interval)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()

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

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def _send_json(self, status_code: int, data: Mapping[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            return {}

    def _require_middleware(self) -> Optional[Any]:
        if (mw := self.server.gateway.middleware) is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
        return mw

    def _run_mw(self, action: Callable[[Any], Mapping[str, Any]]) -> None:
        if mw := self._require_middleware():
            try:
                self._send_json(200, action(mw))
            except Exception as exc:
                self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/api/status":
            self._run_mw(lambda mw: mw.get_pet_status())
        elif path == "/api/nodes":
            self._run_mw(lambda mw: {"sucesso": True, "nodes": mw.get_nodes_info() if hasattr(mw, "get_nodes_info") else []})
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
                self._send_json(400, {"sucesso": False, "mensagem": f"Caminho de nó incompleto: {path}"})
        else:
            self._send_json(404, {"sucesso": False, "mensagem": f"Rota não encontrada: {path}"})

    def _handle_node_action(self, node_id_str: str, action: str) -> None:
        if not self._require_middleware():
            return
        try:
            node_id = int(node_id_str)
        except ValueError:
            return self._send_json(400, {"sucesso": False, "mensagem": f"ID de nó inválido: {node_id_str}"})

        actions = {"toggle_offline": ("offline", "set_node_offline"), "toggle_corrupt": ("corrupt", "set_node_corrupt")}
        if action not in actions:
            return self._send_json(404, {"sucesso": False, "mensagem": f"Ação de nó desconhecida: {action}"})

        field, method = actions[action]
        self._run_mw(lambda mw: getattr(mw, method)(
            node_id,
            not next((n for n in (mw.get_nodes_info() if hasattr(mw, "get_nodes_info") else []) if n.get("id") == node_id), {}).get(field, False),
        ))

    def _handle_command(self, action: str) -> None:
        if not (mw := self._require_middleware()):
            return
        body = self._read_json_body()
        cid = body.get("command_id")
        commands: dict[str, Callable[[], dict[str, Any]]] = {
            "dispense_food": lambda: mw.dispensar_racao(float(body.get("quantidade", 50.0)), command_id=cid),
            "refill_water": lambda: mw.repor_agua(float(body.get("quantidade", 200.0)), command_id=cid),
            "clean_litter": lambda: mw.resetar_areia(command_id=cid),
            "silence_alarm": lambda: mw.silenciar_alarme(command_id=cid),
        }
        if action not in commands:
            return self._send_json(404, {"sucesso": False, "mensagem": f"Comando desconhecido: {action}"})

        try:
            res = commands[action]()
            if res.get("sucesso") and res.get("estado"):
                self.server.gateway.broadcast("state", res["estado"])
            self._send_json(200, res)
        except Exception as exc:
            logger.warning("Falha na validação/execução do comando %s: %s", action, exc)
            self._send_json(400, {"sucesso": False, "mensagem": str(exc)})

    def _handle_sse(self) -> None:
        gateway = self.server.gateway
        client_queue = gateway.subscribe_sse()

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if gateway.middleware is not None:
            try:
                status_res = gateway.middleware.get_pet_status()
                if status_res.get("sucesso") and status_res.get("estado"):
                    self.wfile.write(f"event: state\ndata: {json.dumps(status_res['estado'])}\n\n".encode("utf-8"))
                    self.wfile.flush()
            except Exception as exc:
                logger.debug("Falha ao enviar estado inicial SSE: %s", exc)

        if gateway.tailer is not None:
            try:
                for entry in gateway.tailer.get_recent_history():
                    self.wfile.write(f"event: raw_log\ndata: {json.dumps(entry)}\n\n".encode("utf-8"))
                self.wfile.flush()
            except Exception as exc:
                logger.debug("Falha ao enviar historico inicial de logs SSE: %s", exc)

        try:
            while gateway.is_running:
                try:
                    self.wfile.write(client_queue.get(timeout=1.0).encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            gateway.unsubscribe_sse(client_queue)

    def _handle_static(self, req_path: str) -> None:
        static_dir = self.server.gateway.static_dir
        if not static_dir:
            return self._send_json(404, {"sucesso": False, "mensagem": "Nenhum diretório estático configurado"})

        clean = req_path.lstrip("/") or "index.html"
        base_dir = Path(static_dir).resolve()
        file_path = (base_dir / clean).resolve()
        if not str(file_path).startswith(str(base_dir)):
            return self._send_json(403, {"sucesso": False, "mensagem": "Acesso negado"})
        if not file_path.is_file():
            return self._send_json(404, {"sucesso": False, "mensagem": "Arquivo não encontrado"})

        try:
            content = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", MIME_TYPES.get(file_path.suffix.lower(), "application/octet-stream"))
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache")
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
        logger.info("WebGateway iniciado em http://%s:%d", self.host, self.port)

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
        logger.info("WebGateway encerrado")


