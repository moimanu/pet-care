# app/config.py

Módulo de configuração centralizada que define as constantes de rede, identificação dos nós do cluster e parâmetros de conexão MQTT compartilhados entre controladores, sensores e servidor web.

```python
"""Constantes compartilhadas entre controladores, sensores e servidor web."""

ALL_NODES = (1, 2, 3)
BASE_PORT = 9090
MQTT_HOST = "localhost"
MQTT_PORT = 1883


def peer_uri(node_id: int) -> str:
    # Constrói e retorna a URI Pyro5 para localizar remotamente um nó controlador específico
    # a partir do seu identificador (1, 2 ou 3) e da porta base configurada (BASE_PORT + node_id).
    pass
```

