from __future__ import annotations

import json
import urllib.request
import pytest

from src.entrypoints.run_web import build_web_server, make_mqtt_message_handler


class DummyMiddleware:
    def get_pet_status(self):
        return {"sucesso": True, "estado": {"racao_gramas": 100.0, "hash": "abc"}}

    def dispensar_racao(self, qty=50.0):
        return {"sucesso": True, "mensagem": "ok", "estado": {"racao_gramas": 150.0}}

    def repor_agua(self, qty=200.0):
        return {"sucesso": True, "mensagem": "ok"}

    def resetar_areia(self):
        return {"sucesso": True, "mensagem": "ok"}

    def silenciar_alarme(self):
        return {"sucesso": True, "mensagem": "ok"}


@pytest.fixture
def running_web_server():
    server = build_web_server(
        host="127.0.0.1",
        port=0,
        middleware=DummyMiddleware(),
        static_dir="web",
    )
    server.start()
    yield server
    server.stop()


def test_scenario_nominal_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/scenarios/nominal"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("success") is True


def test_scenario_failover_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/scenarios/failover"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("success") is True


def test_scenario_byzantine_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/scenarios/byzantine"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("success") is True


def test_scenario_sanitization_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/scenarios/sanitization"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("success") is True


def test_scenario_idempotency_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/scenarios/idempotency"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("success") is True
        assert data.get("duplicates_ignored") is True


def test_mqtt_message_handler_broadcasts(running_web_server):
    sub = running_web_server.subscribe_sse()
    handler = make_mqtt_message_handler(running_web_server)
    try:
        handler("petcare/sensores/racao", {"nivel_gramas": 120.0, "timestamp": "2026-11-10T12:00:00Z"})
        msg_raw = sub.get(timeout=2.0)
        assert "event: raw_log" in msg_raw
        assert "MQTT-Broker" in msg_raw

        msg_log = sub.get(timeout=2.0)
        assert "event: log" in msg_log
        assert "racao" in msg_log
    finally:
        running_web_server.unsubscribe_sse(sub)

