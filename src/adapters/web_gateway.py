from __future__ import annotations

from datetime import datetime, timezone
import http.server
import json
import logging
from pathlib import Path
import queue
import socketserver
import threading
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
            entry = {
                "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "component": self.component or record.name,
                "message": record.getMessage(),
                "payload": getattr(record, "payload", None),
            }
            self.gateway.broadcast("log", entry)
        except Exception:
            self.handleError(record)


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
        elif path in ("/api/events", "/events", "/stream"):
            self._handle_sse()
        else:
            self._handle_static(path)

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        if path.startswith("/api/command/"):
            action = path[len("/api/command/"):]
            self._handle_command(action)
        elif path.startswith("/api/scenarios/"):
            scenario = path[len("/api/scenarios/"):]
            self._handle_scenario(scenario)
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

    def _handle_command(self, action: str) -> None:
        mw = self.server.gateway.middleware
        if mw is None:
            self._send_json(503, {"sucesso": False, "mensagem": "Middleware não conectado"})
            return

        body = self._read_json_body()
        gateway = self.server.gateway

        try:
            if action == "dispense_food":
                qty = float(body.get("quantidade", body.get("quantidade_gramas", 50.0)))
                res = mw.dispensar_racao(qty)
            elif action == "refill_water":
                qty = float(body.get("quantidade", body.get("quantidade_ml", 200.0)))
                res = mw.repor_agua(qty)
            elif action == "clean_litter":
                res = mw.resetar_areia()
            elif action == "silence_alarm":
                res = mw.silenciar_alarme()
            else:
                self._send_json(404, {"sucesso": False, "mensagem": f"Comando desconhecido: {action}"})
                return

            if res.get("sucesso") and res.get("estado"):
                gateway.broadcast("state", res["estado"])

            self._send_json(200, res)
        except Exception as exc:
            logger.exception("Falha na execução do comando %s", action)
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

    def _handle_scenario(self, scenario_name: str) -> None:
        handler = self.server.gateway.scenario_handlers.get(scenario_name)
        if handler is None:
            self._send_json(404, {"sucesso": False, "mensagem": f"Cenário desconhecido: {scenario_name}"})
            return
        try:
            body = self._read_json_body()
            result = handler(body)
            self._send_json(200, result)
        except Exception as exc:
            logger.exception("Falha ao executar cenário %s", scenario_name)
            self._send_json(500, {"sucesso": False, "mensagem": str(exc)})

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
    ) -> None:
        self.host = host
        self._requested_port = port
        self.middleware = middleware
        self.static_dir = static_dir
        self.scenario_handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}

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

    def register_scenario(
        self, name: str, handler: Callable[[dict[str, Any]], dict[str, Any]]
    ) -> None:
        self.scenario_handlers[name] = handler

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
        logger.info("WebGateway iniciado em http://%s:%d", self.host, self.port)

    def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
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

