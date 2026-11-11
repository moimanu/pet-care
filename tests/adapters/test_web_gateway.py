from __future__ import annotations

import json
import logging
import queue
import tempfile
from pathlib import Path
from typing import Any
import urllib.error
import urllib.request
import pytest

from src.adapters.web_gateway import WebGateway, WebGatewayLogHandler


class MockMiddleware:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {
            "racao_gramas": 150.0,
            "agua_ml": 350.0,
            "areia_usos": 1,
            "status_fuga": "DENTRO",
            "alarme_ativo": False,
            "hash": "abc123hash",
        }
        self.calls: list[tuple[str, Any]] = []

    def get_pet_status(self) -> dict[str, Any]:
        self.calls.append(("get_pet_status", None))
        return {"sucesso": True, "estado": self.state}

    def dispensar_racao(self, quantidade: float = 50.0) -> dict[str, Any]:
        self.calls.append(("dispensar_racao", quantidade))
        self.state["racao_gramas"] += quantidade
        return {"sucesso": True, "mensagem": f"Dispensado {quantidade}g", "estado": self.state}

    def repor_agua(self, quantidade: float = 200.0) -> dict[str, Any]:
        self.calls.append(("repor_agua", quantidade))
        self.state["agua_ml"] += quantidade
        return {"sucesso": True, "mensagem": f"Reposto {quantidade}ml", "estado": self.state}

    def resetar_areia(self) -> dict[str, Any]:
        self.calls.append(("resetar_areia", None))
        self.state["areia_usos"] = 0
        return {"sucesso": True, "mensagem": "Areia resetada", "estado": self.state}

    def silenciar_alarme(self) -> dict[str, Any]:
        self.calls.append(("silenciar_alarme", None))
        self.state["alarme_ativo"] = False
        return {"sucesso": True, "mensagem": "Alarme silenciado", "estado": self.state}


@pytest.fixture
def static_dir():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        (path / "index.html").write_text("<!DOCTYPE html><html><body><h1>PetCare</h1></body></html>", encoding="utf-8")
        (path / "style.css").write_text("body { background: #000; }", encoding="utf-8")
        yield str(path)


@pytest.fixture
def mock_middleware():
    return MockMiddleware()


@pytest.fixture
def gateway(mock_middleware, static_dir):
    server = WebGateway(
        host="127.0.0.1",
        port=0,
        middleware=mock_middleware,
        static_dir=static_dir,
    )
    server.start()
    yield server
    server.stop()


def test_get_static_index(gateway):
    url = f"http://127.0.0.1:{gateway.port}/"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200
        assert resp.headers.get_content_type() == "text/html"
        body = resp.read().decode("utf-8")
        assert "PetCare" in body


def test_get_static_css(gateway):
    url = f"http://127.0.0.1:{gateway.port}/style.css"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200
        assert resp.headers.get_content_type() == "text/css"
        body = resp.read().decode("utf-8")
        assert "background: #000" in body


def test_get_status_api(gateway, mock_middleware):
    url = f"http://127.0.0.1:{gateway.port}/api/status"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["sucesso"] is True
        assert data["estado"]["racao_gramas"] == 150.0


def test_post_command_dispense_food(gateway, mock_middleware):
    url = f"http://127.0.0.1:{gateway.port}/api/command/dispense_food"
    payload = json.dumps({"quantidade": 40.0}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["sucesso"] is True
        assert ("dispensar_racao", 40.0) in mock_middleware.calls
        assert mock_middleware.state["racao_gramas"] == 190.0


def test_post_command_refill_water(gateway, mock_middleware):
    url = f"http://127.0.0.1:{gateway.port}/api/command/refill_water"
    payload = json.dumps({"quantidade": 150.0}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["sucesso"] is True
        assert ("repor_agua", 150.0) in mock_middleware.calls
        assert mock_middleware.state["agua_ml"] == 500.0


def test_post_command_clean_litter(gateway, mock_middleware):
    url = f"http://127.0.0.1:{gateway.port}/api/command/clean_litter"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["sucesso"] is True
        assert ("resetar_areia", None) in mock_middleware.calls
        assert mock_middleware.state["areia_usos"] == 0


def test_post_command_silence_alarm(gateway, mock_middleware):
    mock_middleware.state["alarme_ativo"] = True
    url = f"http://127.0.0.1:{gateway.port}/api/command/silence_alarm"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["sucesso"] is True
        assert ("silenciar_alarme", None) in mock_middleware.calls
        assert mock_middleware.state["alarme_ativo"] is False


def test_sse_broadcast_format(gateway):
    client_q = gateway.subscribe_sse()
    gateway.broadcast("state", {"test_key": "test_val"})
    msg = client_q.get(timeout=2.0)
    assert msg.startswith("event: state\n")
    assert '"test_key": "test_val"' in msg
    gateway.unsubscribe_sse(client_q)


def test_web_gateway_log_handler(gateway):
    logger = logging.getLogger("TestLogCapture")
    handler = WebGatewayLogHandler(gateway, component="TestComp")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    client_q = gateway.subscribe_sse()
    try:
        logger.info("Hello SSE Log")
        msg = client_q.get(timeout=2.0)
        assert msg.startswith("event: log\n")
        assert "Hello SSE Log" in msg
        assert "TestComp" in msg
    finally:
        logger.removeHandler(handler)
        gateway.unsubscribe_sse(client_q)


def test_not_found(gateway):
    url = f"http://127.0.0.1:{gateway.port}/non_existing_route"
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(url)
    assert exc_info.value.code == 404


def test_get_docs_swagger(static_dir, mock_middleware):
    # Ensure docs.html is written in static_dir
    docs_file = Path(static_dir) / "docs.html"
    docs_file.write_text("<!DOCTYPE html><html><body><div id='swagger-ui'></div></body></html>", encoding="utf-8")
    server = WebGateway(host="127.0.0.1", port=0, middleware=mock_middleware, static_dir=static_dir)
    server.start()
    try:
        url = f"http://127.0.0.1:{server.port}/docs"
        with urllib.request.urlopen(url) as resp:
            assert resp.status == 200
            assert "swagger-ui" in resp.read().decode("utf-8")
    finally:
        server.stop()


def test_get_openapi_json(static_dir, mock_middleware):
    openapi_file = Path(static_dir) / "openapi.json"
    openapi_file.write_text('{"openapi": "3.0.3", "info": {"title": "Test API"}}', encoding="utf-8")
    server = WebGateway(host="127.0.0.1", port=0, middleware=mock_middleware, static_dir=static_dir)
    server.start()
    try:
        url = f"http://127.0.0.1:{server.port}/openapi.json"
        with urllib.request.urlopen(url) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["info"]["title"] == "Test API"
    finally:
        server.stop()


def test_log_file_tailer_streams_raw_logs(mock_middleware):
    from src.adapters.web_gateway import LogFileTailer

    with tempfile.TemporaryDirectory() as tmp_logs:
        logs_path = Path(tmp_logs)
        c1_log = logs_path / "controller_1.log"
        c1_log.write_text("2026-11-11 [INFO] Initial node line\n", encoding="utf-8")

        server = WebGateway(
            host="127.0.0.1",
            port=0,
            middleware=mock_middleware,
            logs_dir=None,
        )
        server.start()
        client_q = server.subscribe_sse()
        tailer = LogFileTailer(gateway=server, logs_dir=str(logs_path), poll_interval=0.1)
        try:
            tailer.poll_once()
            msg = client_q.get(timeout=2.0)
            assert "event: raw_log" in msg
            assert "Initial node line" in msg
            assert '"source": "node-1"' in msg

            with open(c1_log, "a", encoding="utf-8") as f:
                f.write("2026-11-11 [WARN] Dynamic warning line\n")

            tailer.poll_once()
            msg2 = client_q.get(timeout=2.0)
            assert "event: raw_log" in msg2
            assert "Dynamic warning line" in msg2
        finally:
            server.unsubscribe_sse(client_q)
            server.stop()


