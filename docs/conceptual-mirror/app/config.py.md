# app/config.py

Módulo de configuração centralizada que define as constantes de rede, identificação dos nós do cluster, porta do cliente RPC e parâmetros de conexão MQTT compartilhados entre controladores, sensores, cliente RPC e servidor web.

```python
"""Constantes compartilhadas entre controladores, sensores, cliente RPC e servidor web."""

ALL_NODES = (1, 2, 3)
BASE_PORT = 9090
CLIENT_PORT = 9094
MQTT_HOST = "localhost"
MQTT_PORT = 1883


def peer_uri(node_id: int) -> str:
    # Constrói e retorna a URI Pyro5 para localizar remotamente um nó controlador específico
    # a partir do seu identificador (1, 2 ou 3) e da porta base configurada (BASE_PORT + node_id).
    pass


def client_uri() -> str:
    # Constrói e retorna a URI Pyro5 do processo cliente (`petcare.client`) na porta CLIENT_PORT (9094).
    pass
```
