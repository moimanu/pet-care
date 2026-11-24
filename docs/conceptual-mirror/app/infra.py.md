# app/infra.py

Módulo de utilitários compartilhados de infraestrutura que elimina duplicação entre controladores, sensores, cliente RPC e servidor web: encapsula criação de proxies Pyro5, receptor e registro de notificações push (`PushReceiver` / `ObserverRegistry`), conexão e publicação JSON no MQTT e tratamento unificado de sinais de encerramento.

```python
from contextlib import suppress
import json
import signal
import threading
from typing import Callable

import paho.mqtt.client as mqtt
import Pyro5.api

from app.config import MQTT_HOST, MQTT_KEEPALIVE, MQTT_PORT
from app.event_bus import emit_event


def suppress_rpc_errors():
    """Context manager semantico para ignorar falhas de rede esperadas em failover/teardown."""
    # Retorna `contextlib.suppress(Exception)` para tornar explícita a tolerância a falhas de rede pontuais.
    pass


def rpc_proxy(uri: str, timeout: float = 1.0) -> Pyro5.api.Proxy:
    # Instancia um `Pyro5.api.Proxy` para a URI informada com `_pyroTimeout` configurado.
    pass


@Pyro5.api.expose
class PushReceiver:
    """Objeto remoto Pyro5 padrao para receber notificacoes unidirecionais (`on_push`)."""

    def __init__(self, on_push: Callable[[dict], None]) -> None:
        # Armazena o callback local que será invocado ao receber uma notificação push remota.
        pass

    @Pyro5.api.oneway
    def on_push(self, payload: dict) -> None:
        # Método remoto assíncrono (`oneway`) que repassa o payload recebido ao callback local.
        pass


class ObserverRegistry:
    """Mantem URIs de observadores Pyro5 (`PushReceiver`) e remove automaticamente nos inativos."""

    def __init__(self, timeout: float = 1.0) -> None:
        # Inicializa o timeout de entrega, lock de concorrência e o conjunto de URIs registradas.
        pass

    def register(self, uri: str) -> bool:
        # Adiciona de forma thread-safe a URI de um observador ao conjunto.
        pass

    def unregister(self, uri: str) -> bool:
        # Remove de forma thread-safe a URI de um observador do conjunto.
        pass

    def notify(self, payload: dict) -> None:
        # Dispara uma thread daemon que envia `proxy.on_push(payload)` para todas as URIs registradas,
        # removendo automaticamente aquelas que falharem na comunicação.
        pass


def connect_mqtt(client_id: str, on_error: Callable[[Exception], None] | None = None, **callbacks) -> mqtt.Client:
    # Cria um cliente `paho.mqtt` (CallbackAPIVersion.VERSION2), vincula callbacks (`on_connect`, `on_message`, etc.),
    # conecta ao broker definido em `app.config` e inicia o loop de rede em background (`loop_start()`),
    # delegando eventuais erros de conexão para `on_error` se fornecido.
    pass


def publish_json(client: mqtt.Client, topic: str, payload: dict | str, qos: int = 1, log_prefix: str = "PUB") -> str:
    # Serializa `payload` em JSON (quando dict), publica no tópico MQTT com o QoS especificado,
    # emite evento de log padronizado caso `log_prefix` não esteja vazio e retorna a string publicada.
    pass


def install_shutdown_handler(handler: Callable[[int], None]) -> None:
    # Registra o mesmo callback de encerramento para os sinais `SIGTERM` e `SIGINT`.
    pass
```

