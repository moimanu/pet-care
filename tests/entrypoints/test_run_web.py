from __future__ import annotations

import json
import urllib.error
import urllib.request
import pytest

from src.entrypoints.run_web import build_web_server, make_mqtt_message_handler


class DummyMiddleware:
    def __init__(self):
        self.nodes = [
            {"id": 1, "primary": True, "corrupt": False, "offline": False},
            {"id": 2, "primary": False, "corrupt": False, "offline": False},
            {"id": 3, "primary": False, "corrupt": False, "offline": False},
        ]

    def get_pet_status(self):
        return {"sucesso": True, "estado": {"racao_gramas": 100.0, "hash": "abc"}}

    def dispensar_racao(self, qty=50.0, command_id=None):
        if qty <= 0:
            raise ValueError(f"Quantidade de racao invalida: {qty}g")
        return {
            "sucesso": True,
            "mensagem": f"Dispensado {qty}g com cid={command_id}",
            "estado": {"racao_gramas": 150.0},
        }

    def repor_agua(self, qty=200.0, command_id=None):
        if qty <= 0:
            raise ValueError(f"Quantidade de agua invalida: {qty}ml")
        return {"sucesso": True, "mensagem": "ok"}

    def resetar_areia(self, command_id=None):
        return {"sucesso": True, "mensagem": "ok"}

    def silenciar_alarme(self, command_id=None):
        return {"sucesso": True, "mensagem": "ok"}

    def get_nodes_info(self):
        return self.nodes

    def set_node_offline(self, node_id, offline):
        for n in self.nodes:
            if n["id"] == node_id:
                n["offline"] = offline
        return {"sucesso": True, "node_id": node_id, "offline": offline}

    def set_node_corrupt(self, node_id, corrupt):
        for n in self.nodes:
            if n["id"] == node_id:
                n["corrupt"] = corrupt
        return {"sucesso": True, "node_id": node_id, "corrupt": corrupt}


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


def test_get_nodes_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/nodes"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("sucesso") is True
        assert len(data.get("nodes", [])) == 3


def test_toggle_node_offline_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/nodes/1/toggle_offline"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("sucesso") is True
        assert data.get("offline") is True


def test_toggle_node_corrupt_endpoint(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/nodes/3/toggle_corrupt"
    req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("sucesso") is True
        assert data.get("corrupt") is True


def test_dispense_food_idempotency_body(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/command/dispense_food"
    payload = json.dumps({
        "quantidade": 50.0,
        "command_id": "00000000-0000-0000-0000-000000000001",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data.get("sucesso") is True
        assert "00000000-0000-0000-0000-000000000001" in data.get("mensagem", "")


def test_dispense_food_sanitization_rejection(running_web_server):
    url = f"http://127.0.0.1:{running_web_server.port}/api/command/dispense_food"
    payload = json.dumps({"quantidade": -50.0}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code == 400
    body = json.loads(exc_info.value.read().decode("utf-8"))
    assert body.get("sucesso") is False
    assert "Quantidade de racao invalida" in body.get("mensagem", "")


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
