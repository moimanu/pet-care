"""Constantes compartilhadas entre controladores, sensores, cliente RPC e servidor web."""

ALL_NODES = (1, 2, 3)
BASE_PORT = 9090
CLIENT_PORT = 9094
MQTT_HOST = "localhost"
MQTT_PORT = 1883


def peer_uri(node_id: int) -> str:
    return f"PYRO:petcare.controller.{node_id}@localhost:{BASE_PORT + node_id}"


def client_uri() -> str:
    return f"PYRO:petcare.client@localhost:{CLIENT_PORT}"
