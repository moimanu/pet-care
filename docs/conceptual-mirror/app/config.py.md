# app/config.py

Módulo central de configurações do cluster Pet Care: define os IDs dos nós, parâmetros de quórum bizantino ($2f+1$), portas Pyro5/HTTP, tópicos MQTT e helpers de URI/banco.

```python
"""Configuracoes, constantes de protocolo e enderecos compartilhados do cluster."""  # Docstring do módulo resumindo seu papel de configuração global

ALL_NODES = (1, 2, 3)  # Tupla imutável com os IDs dos 3 nós controladores do cluster
BYZANTINE_FAULT_LIMIT = 1          # f = 1 em um cluster de N = 2f + 1 = 3 nos (tolerância a 1 nó bizantino)
MIN_QUORUM = BYZANTINE_FAULT_LIMIT + 1  # maioria simples (>= 2 respostas concordantes) — mínimo necessário para validar consenso

BASE_PORT = 9090  # Porta base Pyro5; cada controlador usa BASE_PORT + node_id (9091, 9092, 9093)
CLIENT_PORT = 9094  # Porta Pyro5 dedicada ao processo do cliente RPC
WEB_HOST = "0.0.0.0"  # Interface de rede onde o servidor HTTP/SSE escuta (todas as interfaces)
WEB_PORT = 8080  # Porta HTTP do painel web e dos endpoints /api/*

MQTT_HOST = "localhost"  # Endereço do broker MQTT (Mosquitto)
MQTT_PORT = 1883  # Porta padrão do broker MQTT
MQTT_KEEPALIVE = 30  # Intervalo em segundos para pacotes keepalive da conexão MQTT

TOPIC_SENSORS = "petcare/sensores"  # Prefixo de tópico MQTT para publicações de telemetria dos sensores
TOPIC_ACTUATORS = "petcare/atuadores"  # Prefixo de tópico MQTT para envio de comandos aos atuadores
TOPIC_SIM_CONTROL = "petcare/sim/control"  # Tópico MQTT de controle do simulador de sensores (modo auto/manual)

CLIENT_OBJECT_NAME = "petcare.client"  # Nome lógico do objeto remoto Pyro5 exposto pelo cliente RPC
CLIENT_OBSERVER_NAME = "petcare.client.observer"  # Nome lógico do PushReceiver do cliente RPC para notificações dos nós
WEB_OBSERVER_NAME = "petcare.web.observer"  # Nome lógico do PushReceiver do servidor web para notificações do cliente RPC


def controller_object_name(node_id: int) -> str:  # Monta o identificador lógico Pyro5 de um controlador pelo seu ID
    return f"petcare.controller.{node_id}"  # Retorna o nome padronizado usado no registro do Daemon Pyro5


def peer_uri(node_id: int) -> str:  # Constrói a URI Pyro5 completa para acessar um nó controlador específico
    return f"PYRO:{controller_object_name(node_id)}@localhost:{BASE_PORT + node_id}"  # Combina nome do objeto, host e porta dedicada do nó


def client_uri() -> str:  # Constrói a URI Pyro5 completa do serviço cliente RPC
    return f"PYRO:{CLIENT_OBJECT_NAME}@localhost:{CLIENT_PORT}"  # Combina nome do objeto cliente com localhost e CLIENT_PORT


def node_db_path(node_id: int) -> str:  # Retorna o caminho do arquivo SQLite isolado de um controlador
    return f"node_{node_id}.db"  # Gera o nome de arquivo local exclusivo por réplica (node_1.db, node_2.db, node_3.db)
```
