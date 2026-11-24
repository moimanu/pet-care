# app/web_server.py

Módulo de interface web e observação acima do sistema: orquestra o `ProcessManager`, conversa com o cliente RPC (`app.client`) via Pyro5, recebe atualizações push via `PushReceiver`, mantém a ponte MQTT/SSE em tempo real e expõe a API HTTP REST (`/api/state`, `/api/events`, `/api/action`) para o painel de controle.

```python
from contextlib import suppress
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
from typing import Any, Callable

import Pyro5.api

from app.config import (
    ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT, TOPIC_SIM_CONTROL,
    WEB_HOST, WEB_OBSERVER_NAME, WEB_PORT, client_uri, node_db_path,
)
from app.event_bus import LOG_FILE, add_listener, emit_event
from app.infra import PushReceiver, connect_mqtt, install_shutdown_handler, publish_json, rpc_proxy, suppress_rpc_errors
from app.process_manager import ProcessManager
from app.storage import NodeStorage

WEB_DIR = Path("web")


class AppContext:
    """Orquestra estado do painel web, stream SSE, ponte MQTT e comandos para o cluster."""

    def __init__(self) -> None:
        # Instancia `ProcessManager`, os snapshots locais `NodeStorage`, a variável de condição SSE,
        # registra `notify_clients` no barramento de eventos e inicia a conexão MQTT da ponte web via `connect_mqtt`.
        pass

    def _client_rpc(self, timeout: float = 1.5) -> Pyro5.api.Proxy:
        # Cria um proxy Pyro5 apontando para o processo `app.client` (`client_uri()`).
        pass

    def start_ui_observer(self) -> None:
        # Sobe um `Pyro5.api.Daemon` leve expondo `PushReceiver(self.on_consensus_push)` sob `WEB_OBSERVER_NAME`
        # e registra sua URI no processo `app.client` de forma assíncrona.
        pass

    def stop_ui_observer(self) -> None:
        # Remove o registro do observador no `app.client` e encerra o daemon Pyro5 da UI.
        pass

    def _sync_client_async(self, step: Callable[[Any], bool], attempts: int = 25, delay: float = 0.15) -> None:
        # Executa `step(rpc)` em thread background com retentativas até o `app.client` estar disponível
        # e sincroniza o consenso inicial antes de notificar os clientes SSE.
        pass

    def notify_clients(self) -> None:
        # Incrementa o contador de versão e acorda todas as threads SSE aguardando atualização de estado/logs.
        pass

    def wait_next_version(self, last_version: int) -> int | None:
        # Bloqueia na condition variable SSE até haver nova versão de estado ou o servidor entrar em encerramento.
        pass

    def close(self) -> None:
        # Marca `_closing = True` e acorda todas as threads SSE para encerramento gracioso.
        pass

    def on_consensus_push(self, consensus: dict) -> None:
        # Atualiza `last_consensus` a partir de notificações push do `app.client` e notifica clientes SSE.
        pass

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        # Marca a conexão MQTT como ativa e emite evento de confirmação no barramento.
        pass

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        # Marca a conexão MQTT como inativa e emite alerta de desconexão.
        pass

    def build_state_payload(self) -> dict:
        # Consolida o snapshot completo do sistema para o frontend: consulta status/consenso no `app.client` via RPC,
        # status de processo de cada controlador, snapshots SQLite locais, status MQTT, sensores e cliente.
        pass

    def read_recent_logs(self, limit: int = 120) -> list[dict]:
        # Lê as últimas `limit` linhas do arquivo JSONL de eventos e faz o parse seguro para uma lista de dicionários.
        pass

    def dispatch_action(self, body: dict) -> dict:
        # Roteia ações vindas da API POST `/api/action` para o componente adequado:
        # encaminha comandos RPC e consultas ao `app.client`, gerencia start/stop de processos
        # (controladores, sensores e cliente), alternância de modo bizantino (notificando clientes SSE
        # sem recalcular quórum), mutação direta em SQLite e publicação/controle MQTT.
        pass


def make_handler(ctx: AppContext):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            # Configura o servidor de arquivos estáticos apontando para o diretório `WEB_DIR`.
            pass

        def log_message(self, format, *args):
            # Suprime os logs padrão de acesso HTTP no console para manter a saída limpa.
            pass

        def _send_json(self, status: int, payload: dict) -> None:
            # Serializa `payload` em JSON UTF-8 e envia a resposta HTTP com os cabeçalhos apropriados.
            pass

        def do_GET(self) -> None:
            # Atende `/api/state` (snapshot JSON), `/api/events` (stream SSE via `_stream_sse`)
            # ou serve os arquivos estáticos do painel web (redirecionando `/` para `/index.html`).
            pass

        def _stream_sse(self) -> None:
            # Mantém um stream SSE contínuo bloqueado em `ctx.wait_next_version()`, enviando `state` e `logs` a cada atualização.
            pass

        def do_POST(self) -> None:
            # Recebe requisições JSON em `/api/action`, delega para `ctx.dispatch_action`
            # e retorna o resultado em JSON (200) ou erro formatado (400/404).
            pass

    # Retorna a classe `Handler` vinculada ao `AppContext` compartilhado.
    pass


def main() -> None:
    # 1. Limpa o arquivo de log inicial e instancia o `AppContext`.
    # 2. Inicia todos os processos do cluster (`controller`, `sensors`, `client`) e ativa o observador de UI.
    # 3. Sobe o `ThreadingHTTPServer` em `WEB_HOST:WEB_PORT` (8080) e garante encerramento limpo no `finally`.
    pass


if __name__ == "__main__":
    main()
```
