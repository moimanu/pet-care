# app/config.py

Módulo de configuração centralizada que define as constantes do cluster ($N = 2f+1$), portas de rede, parâmetros MQTT, tópicos, nomes de objetos Pyro5 e funções auxiliares para montagem de URIs e caminhos de banco SQLite.

```python
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
    # Retorna o nome lógico registrado no daemon Pyro5 para o controlador especificado (`petcare.controller.<id>`).
    pass


def peer_uri(node_id: int) -> str:
    # Constrói e retorna a URI Pyro5 completa para localizar um nó controlador (`PYRO:petcare.controller.<id>@localhost:<BASE_PORT + node_id>`).
    pass


def client_uri() -> str:
    # Constrói e retorna a URI Pyro5 do processo cliente (`petcare.client`) na porta `CLIENT_PORT` (9094).
    pass


def node_db_path(node_id: int) -> str:
    # Retorna o caminho padrão do arquivo SQLite isolado de um controlador (`node_<id>.db`).
    pass
```
