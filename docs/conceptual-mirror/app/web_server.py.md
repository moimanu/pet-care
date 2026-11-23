# app/web_server.py

Módulo de interface web e orquestração local: gerencia os processos dos controladores e sensores, mantém a ponte MQTT/SSE em tempo real e expõe a API HTTP REST para o painel de controle.

```python
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import paho.mqtt.client as mqtt

from app.config import ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT
from app.event_bus import LOG_FILE, add_listener, emit_event
from app.rpc_client import ClusterRpcClient
from app.storage import NodeStorage

WEB_DIR = Path("web")
HOST = "0.0.0.0"
PORT = 8080


class ProcessManager:
    """Gerencia os processos reais dos controladores e sensores."""

    def __init__(self) -> None:
        # Inicializa o lock de gerenciamento, o dicionário de subprocessos dos controladores,
        # o conjunto de nós em encerramento e a referência ao processo dos sensores.
        pass

    def start_all(self) -> None:
        # Inicia todos os nós controladores (definindo o nó 1 como líder inicial) e, após breve pausa,
        # inicia o processo simulador dos sensores IoT.
        pass

    def start_controller(self, node_id: int, primary: bool = False) -> bool:
        # Inicia o subprocesso `app.controller` para o `node_id` informado caso ainda não esteja em execução.
        pass

    def stop_controller(self, node_id: int) -> bool:
        # Encerra graciosamente (SIGTERM com fallback para SIGKILL) o subprocesso do controlador especificado
        # e registra o evento de parada.
        pass

    def is_controller_alive(self, node_id: int) -> bool:
        # Verifica se o processo do controlador está ativo e não se encontra em processo de parada.
        pass

    def start_sensors(self) -> bool:
        # Inicia o subprocesso `app.sensors` caso não esteja em execução.
        pass

    def stop_sensors(self) -> bool:
        # Encerra o subprocesso dos sensores IoT com timeout de espera e fallback para kill forçado.
        pass

    def is_sensors_alive(self) -> bool:
        # Verifica se o processo simulador de sensores encontra-se atualmente em execução.
        pass

    def stop_all(self) -> None:
        # Interrompe primeiro o processo de sensores e em seguida todos os processos controladores.
        pass


class AppContext:
    def __init__(self) -> None:
        # Instancia o gerenciador de processos, o cliente RPC do cluster, a variável de condição SSE,
        # registra-se como listener no barramento de eventos e inicia a conexão MQTT da ponte web.
        pass

    def notify_clients(self) -> None:
        # Incrementa o contador de versão e acorda todas as threads SSE aguardando atualização de estado/logs.
        pass

    def on_consensus_push(self, consensus: dict) -> None:
        # Atualiza o último consenso conhecido a partir de notificações push do observer Pyro5 e notifica clientes SSE.
        pass

    def _register_node_async(self, node_id: int) -> None:
        # Inicia uma thread que tenta registrar o observador Pyro5 em um controlador recém-iniciado
        # com múltiplas tentativas até que seu daemon RPC esteja pronto.
        pass

    def _connect_mqtt(self) -> None:
        # Conecta o cliente MQTT do servidor web ao broker e inicia seu loop em background.
        pass

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        # Marca a conexão MQTT como ativa e emite evento de confirmação no barramento.
        pass

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        # Marca a conexão MQTT como inativa e emite alerta de desconexão.
        pass

    def build_state_payload(self) -> dict:
        # Consolida o snapshot completo do sistema para o frontend: estado de quórum dos clientes,
        # status RPC/processo de cada controlador, snapshots SQLite locais, status MQTT e dos sensores.
        pass

    def read_recent_logs(self, limit: int = 120) -> list[dict]:
        # Lê as últimas `limit` linhas do arquivo JSONL de eventos e faz o parse seguro para uma lista de dicionários.
        pass

    def dispatch_action(self, body: dict) -> dict:
        # Roteia ações vindas da API POST `/api/action` para o componente adequado:
        # execução de comandos RPC, leitura de quórum, promoção de líder, start/stop de processos,
        # alternância de modo bizantino, mutação direta em SQLite e publicação/controle MQTT dos sensores.
        pass


def make_handler(ctx: AppContext):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            # Configura o servidor de arquivos estáticos apontando para o diretório `WEB_DIR`.
            pass

        def log_message(self, format, *args):
            # Suprime os logs padrão de acesso HTTP no console para manter a saída limpa.
            pass

        def _send_json(self, code: int, data: dict) -> None:
            # Serializa `data` em JSON UTF-8 e envia a resposta HTTP com os cabeçalhos apropriados.
            pass

        def do_GET(self) -> None:
            # Atende `/api/events` mantendo um stream SSE contínuo acionado por `ctx._sse_cond`,
            # ou serve os arquivos estáticos do painel web (redirecionando `/` para `/index.html`).
            pass

        def do_POST(self) -> None:
            # Recebe requisições JSON em `/api/action`, delega para `ctx.dispatch_action`
            # e retorna o resultado em JSON (200) ou erro formatado (400/404).
            pass

    # Retorna a classe `Handler` vinculada ao `AppContext` compartilhado.
    pass


def main() -> None:
    # 1. Limpa o arquivo de log inicial e instancia o `AppContext`.
    # 2. Inicia todos os processos do cluster, ativa o observador Pyro5 e obtém o consenso inicial.
    # 3. Sobe o `ThreadingHTTPServer` na porta 8080 e garante encerramento limpo de processos e conexões no `finally`.
    pass


if __name__ == "__main__":
    main()
```

