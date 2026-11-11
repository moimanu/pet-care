from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
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
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
}


class WebGatewayLogHandler(logging.Handler):
    """Logging handler streaming log records over WebGateway SSE."""

    def __init__(self, gateway: WebGateway, component: str = "") -> None:
        super().__init__()
        self.gateway = gateway
        self.component = component

    def emit(self, record: logging.LogRecord) -> None:
        try:
            lvl = "WARN" if record.levelname == "WARNING" else record.levelname
            ts = datetime.fromtimestamp(record.created).astimezone().isoformat()
            msg = record.getMessage()
            line = f"{ts} [{lvl}] ({self.component or record.name}) {msg}"
            self.gateway.broadcast("raw_log", {"source": "web", "line": line})
            entry = {
                "timestamp": ts,
                "level": lvl,
                "component": self.component or record.name,
                "message": msg,
                "payload": getattr(record, "payload", None),
            }
            self.gateway.broadcast("log", entry)
        except Exception:
            self.handleError(record)


class LogFileTailer:
    """Continuously tails raw process log files (*.log) and streams lines over SSE."""

    SOURCE_MAP = {
        "controller_1": "node-1",
        "controller_2": "node-2",
        "controller_3": "node-3",
        "middleware": "middleware",
        "iot_devices": "iot",
    }

    def __init__(
        self,
        gateway: WebGateway,
        logs_dir: str | Path = "logs",
        poll_interval: float = 0.25,
        initial_lines: int = 20,
    ) -> None:
        self.gateway = gateway
        self.logs_dir = Path(logs_dir)
        self.poll_interval = poll_interval
        self.initial_lines = initial_lines
        self._offsets: dict[str, int] = {}
        self._history: deque[dict[str, str]] = deque(maxlen=200)
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def get_recent_history(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._history)

    def _append_and_broadcast(self, source_id: str, line: str) -> None:
        entry = {"source": source_id, "line": line}
        with self._lock:
            self._history.append(entry)
        self.gateway.broadcast("raw_log", entry)

    def _read_recent_lines(self, file_path: Path, max_lines: int) -> list[str]:
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                return lines[-max_lines:] if len(lines) > max_lines else lines
        except Exception:
            return []

    def poll_once(self) -> None:
        if not self.logs_dir.is_dir():
            return

        for log_file in sorted(self.logs_dir.glob("*.log")):
            stem = log_file.stem
            source_id = self.SOURCE_MAP.get(stem, stem)
            file_key = str(log_file)

            if file_key not in self._offsets:
                recent = self._read_recent_lines(log_file, self.initial_lines)
                for line in recent:
                    clean_line = line.rstrip("\r\n")
                    if clean_line:
                        self._append_and_broadcast(source_id, clean_line)
                try:
                    self._offsets[file_key] = log_file.stat().st_size
                except OSError:
                    self._offsets[file_key] = 0
                continue

            try:
                current_size = log_file.stat().st_size
            except OSError:
                continue

            last_offset = self._offsets.get(file_key, 0)
            if current_size < last_offset:
                last_offset = 0

            if current_size > last_offset:
                try:
                    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_offset)
                        new_content = f.read()
                        self._offsets[file_key] = f.tell()
                        for line in new_content.splitlines():
                            clean_line = line.rstrip("\r\n")
                            if clean_line:
                                self._append_and_broadcast(source_id, clean_line)
                except Exception as exc:
                    logger.debug("Falha ao ler arquivo de log %s: %s", log_file, exc)

    def _loop(self) -> None:
        while self._running:
            self.poll_once()
            time.sleep(self.poll_interval)

    def start(self) -> None:
        if self._running:
            return
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
        # Suppress noisy standard stdout logs during HTTP handling
        pass

    def _send_json(self, status_code: int, data: Mapping[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        content_len = int(self.headers.get("Content-Length", 0))
        if content_len <= 0:
            return {}
        raw = self.rfile.read(content_len)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/api/status":
            self._handle_get_status()
        elif path == "/api/nodes":
            self._handle_get_nodes()
        elif path in ("/api/events", "/events", "/stream"):
            self._handle_sse()
        elif path in ("/docs", "/swagger"):
            self._handle_static("docs.html")
        elif path == "/openapi.json":
            self._handle_static("openapi.json")
        else:
            self._handle_static(path)

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        if path.startswith("/api/command/"):
            action = path[len("/api/command/"):]
            self._handle_command(action)
        elif path.startswith("/api/nodes/"):
            parts = path[len("/api/nodes/"):].split("/")
            if len(parts) >= 2:
                node_id_str, action = parts[0], parts[1]
                self._handle_node_action(node_id_str, action)
            else:
                self._send_json(400, {"sucesso": False, "mensagem": f"Caminho de nó incompleto: {path}"})
        else:
            self._send_json(404, {"sucesso": False, "mensagem": f"Rota não encontrada: {path}"})

    def _handle_get_status(self) -> None:
        mw = self.server.gateway.middleware
        if mw is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
            return
        try:
            res = mw.get_pet_status()
            self._send_json(200, res)
        except Exception as exc:
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

    def _handle_get_nodes(self) -> None:
        mw = self.server.gateway.middleware
        if mw is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
            return
        try:
            nodes = mw.get_nodes_info() if hasattr(mw, "get_nodes_info") else []
            self._send_json(200, {"sucesso": True, "nodes": nodes})
        except Exception as exc:
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

    def _handle_node_action(self, node_id_str: str, action: str) -> None:
        mw = self.server.gateway.middleware
        if mw is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
            return
        try:
            node_id = int(node_id_str)
        except ValueError:
            self._send_json(400, {"sucesso": False, "mensagem": f"ID de nó inválido: {node_id_str}"})
            return

        body = self._read_json_body()
        try:
            if action == "offline":
                offline = bool(body.get("offline", True))
                res = mw.set_node_offline(node_id, offline)
                self._send_json(200, res)
            elif action == "corrupt":
                corrupt = bool(body.get("corrupt", True))
                res = mw.set_node_corrupt(node_id, corrupt)
                self._send_json(200, res)
            elif action == "toggle_offline":
                nodes = mw.get_nodes_info() if hasattr(mw, "get_nodes_info") else []
                target = next((n for n in nodes if n.get("id") == node_id), None)
                current = target.get("offline", False) if target else False
                res = mw.set_node_offline(node_id, not current)
                self._send_json(200, res)
            elif action == "toggle_corrupt":
                nodes = mw.get_nodes_info() if hasattr(mw, "get_nodes_info") else []
                target = next((n for n in nodes if n.get("id") == node_id), None)
                current = target.get("corrupt", False) if target else False
                res = mw.set_node_corrupt(node_id, not current)
                self._send_json(200, res)
            else:
                self._send_json(404, {"sucesso": False, "mensagem": f"Ação de nó desconhecida: {action}"})
        except Exception as exc:
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

    def _handle_command(self, action: str) -> None:
        mw = self.server.gateway.middleware
        if mw is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
            return

        body = self._read_json_body()
        gateway = self.server.gateway

        try:
            cid = body.get("command_id")
            if action == "dispense_food":
                qty = float(body.get("quantidade", body.get("quantidade_gramas", 50.0)))
                res = mw.dispensar_racao(qty, command_id=cid)
            elif action == "refill_water":
                qty = float(body.get("quantidade", body.get("quantidade_ml", 200.0)))
                res = mw.repor_agua(qty, command_id=cid)
            elif action == "clean_litter":
                res = mw.resetar_areia(command_id=cid)
            elif action == "silence_alarm":
                res = mw.silenciar_alarme(command_id=cid)
            else:
                self._send_json(404, {"sucesso": False, "mensagem": f"Comando desconhecido: {action}"})
                return

            if res.get("sucesso") and res.get("estado"):
                gateway.broadcast("state", res["estado"])

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

        # Send initial status snapshot if middleware is available
        if gateway.middleware is not None:
            try:
                status_res = gateway.middleware.get_pet_status()
                if status_res.get("sucesso") and status_res.get("estado"):
                    init_data = f"event: state\ndata: {json.dumps(status_res['estado'])}\n\n".encode("utf-8")
                    self.wfile.write(init_data)
                    self.wfile.flush()
            except Exception as exc:
                logger.debug("Falha ao enviar estado inicial SSE: %s", exc)

        # Send recent raw logs to populate terminal immediately on connection
        if gateway.tailer is not None:
            try:
                for entry in gateway.tailer.get_recent_history():
                    raw_data = f"event: raw_log\ndata: {json.dumps(entry)}\n\n".encode("utf-8")
                    self.wfile.write(raw_data)
                self.wfile.flush()
            except Exception as exc:
                logger.debug("Falha ao enviar historico inicial de logs SSE: %s", exc)

        try:
            while gateway.is_running:
                try:
                    raw_msg = client_queue.get(timeout=1.0)
                    self.wfile.write(raw_msg.encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    # Keep-alive heartbeat comment
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            gateway.unsubscribe_sse(client_queue)

    def _handle_static(self, req_path: str) -> None:
        static_dir = self.server.gateway.static_dir
        if not static_dir:
            self._send_json(404, {"sucesso": False, "mensagem": "Nenhum diretório estático configurado"})
            return

        clean = req_path.lstrip("/")
        if not clean or clean == "/":
            clean = "index.html"

        file_path = (Path(static_dir) / clean).resolve()
        # Prevent directory traversal outside static_dir
        if not str(file_path).startswith(str(Path(static_dir).resolve())):
            self._send_json(403, {"sucesso": False, "mensagem": "Acesso negado"})
            return

        if not file_path.is_file():
            self._send_json(404, {"sucesso": False, "mensagem": "Arquivo não encontrado"})
            return

        content_type = MIME_TYPES.get(file_path.suffix.lower(), "application/octet-stream")
        try:
            content = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
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
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        middleware: Optional[Any] = None,
        static_dir: Optional[str] = None,
        logs_dir: Optional[str] = None,
        mqtt_client: Optional[Any] = None,
    ) -> None:
        self.host = host
        self._requested_port = port
        self.middleware = middleware
        self.static_dir = static_dir
        self.logs_dir = logs_dir
        self.mqtt_client = mqtt_client
        self.tailer: Optional[LogFileTailer] = None
        if self.logs_dir:
            self.tailer = LogFileTailer(gateway=self, logs_dir=self.logs_dir)

        self._clients_lock = threading.Lock()
        self._subscribers: set[queue.Queue[str]] = set()

        self._server: Optional[_GatewayServer] = None
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

    @property
    def port(self) -> int:
        if self._server is not None:
            return int(self._server.server_address[1])
        return self._requested_port

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
        if self.tailer is not None:
            self.tailer.start()
        logger.info("WebGateway iniciado em http://%s:%d", self.host, self.port)

    def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        if self.tailer is not None:
            self.tailer.stop()
            self.tailer = None
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        with self._clients_lock:
            self._subscribers.clear()
        logger.info("WebGateway encerrado")

