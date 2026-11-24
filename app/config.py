"""Configuracoes, constantes de protocolo e enderecos compartilhados do cluster."""

ALL_NODES = (1, 2, 3)
BYZANTINE_FAULT_LIMIT = 1          # f = 1 em um cluster de N = 2f + 1 = 3 nos
MIN_QUORUM = BYZANTINE_FAULT_LIMIT + 1  # maioria simples (>= 2 respostas concordantes)

BASE_PORT = 9090
CLIENT_PORT = 9094
WEB_HOST = "0.0.0.0"
WEB_PORT = 8080

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 30

TOPIC_SENSORS = "petcare/sensores"
TOPIC_ACTUATORS = "petcare/atuadores"
TOPIC_SIM_CONTROL = "petcare/sim/control"

CLIENT_OBJECT_NAME = "petcare.client"
CLIENT_OBSERVER_NAME = "petcare.client.observer"
WEB_OBSERVER_NAME = "petcare.web.observer"


def controller_object_name(node_id: int) -> str:
    return f"petcare.controller.{node_id}"


def peer_uri(node_id: int) -> str:
    return f"PYRO:{controller_object_name(node_id)}@localhost:{BASE_PORT + node_id}"


def client_uri() -> str:
    return f"PYRO:{CLIENT_OBJECT_NAME}@localhost:{CLIENT_PORT}"


def node_db_path(node_id: int) -> str:
    return f"node_{node_id}.db"
