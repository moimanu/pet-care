# app/web_server.py

Servidor HTTP/SSE do painel web (`web/`): orquestra os subprocessos via `ProcessManager`, comunica-se com o `ClusterRpcClient` via Pyro5 (incluindo receptor push), lê snapshots diretos do SQLite, publica ações MQTT de teste e transmite atualizações em tempo real via Server-Sent Events (`/api/events`).

```python
"""Servidor HTTP/SSE do painel web e ponte com o cliente RPC, SQLite e MQTT."""  # Docstring resumindo o papel da camada web/orquestração

from contextlib import suppress  # Ignora JSON inválido ao ler linhas incompletas do arquivo de log
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer  # Servidor HTTP multi-thread e handler de arquivos estáticos
import json  # Serializa respostas JSON/SSE e decodifica requisições POST
from pathlib import Path  # Define o diretório raiz dos arquivos estáticos do frontend
import threading  # Fornece Condition e Thread para sinalização SSE e tarefas em background
import time  # Usado no intervalo de espera entre retentativas de conexão com o cliente RPC
from typing import Any, Callable  # Tipagem para callbacks de sincronização assíncrona

import Pyro5.api  # Cria o Daemon local que hospeda o PushReceiver da interface web

from app.config import (  # Importa constantes de configuração do cluster, MQTT e servidor web
    ALL_NODES, BASE_PORT, MQTT_HOST, MQTT_PORT, TOPIC_SIM_CONTROL,  # Nós, portas, host MQTT e tópico do simulador
    WEB_HOST, WEB_OBSERVER_NAME, WEB_PORT, client_uri, node_db_path,  # Endereço web, nome do observador e helpers de URI/DB
)  # Fecha importação de app.config
from app.event_bus import LOG_FILE, add_listener, emit_event  # Arquivo de log JSONL, registro de listener SSE e emissão de eventos
from app.infra import PushReceiver, connect_mqtt, install_shutdown_handler, publish_json, rpc_proxy, suppress_rpc_errors  # Infraestrutura Pyro5, MQTT e sinais
from app.process_manager import ProcessManager  # Gerenciador dos subprocessos do sistema distribuído
from app.storage import NodeStorage  # Acesso direto ao SQLite das réplicas para snapshots e corrupção simulada

WEB_DIR = Path("web")  # Pasta contendo index.html, CSS e JS do painel de monitoramento


class AppContext:  # Estado compartilhado e fachada de operações entre o servidor HTTP e o sistema distribuído
    """Orquestra estado do painel web, stream SSE, ponte MQTT e comandos para o cluster."""  # Docstring da classe AppContext

    def __init__(self) -> None:  # Inicializa gerenciador de processos, storages locais, SSE e ponte MQTT
        self.procs = ProcessManager()  # Instancia o gerenciador de subprocessos
        self.storages = {node_id: NodeStorage(node_db_path(node_id)) for node_id in ALL_NODES}  # Abre acesso direto aos 3 bancos SQLite para inspeção
        self.last_consensus: dict | None = None  # Cache local do último estado de consenso recebido do cliente RPC
        self.mqtt_connected = False  # Flag indicando se a ponte MQTT do painel está conectada ao broker
        self._sse_cond = threading.Condition()  # Variável de condição usada para acordar threads SSE conectadas
        self._version = 0  # Contador monotônico de versão incrementado a cada evento/mudança
        self._closing = False  # Flag sinalizando encerramento do servidor para liberar conexões SSE
        self._ui_daemon: Pyro5.api.Daemon | None = None  # Daemon Pyro5 efêmero para receber pushes do cliente RPC
        self._ui_observer_uri: str | None = None  # URI do PushReceiver deste servidor web

        add_listener(self.notify_clients)  # Inscreve notify_clients no event_bus para atualizar o SSE a cada novo log
        self.mqtt = connect_mqtt(  # Conecta cliente MQTT auxiliar usado para testes manuais e controle do simulador
            "petcare_web_bridge",  # Client ID exclusivo da ponte web no broker Mosquitto
            on_connect=self._on_mqtt_connect,  # Atualiza flag mqtt_connected e loga conexão
            on_disconnect=self._on_mqtt_disconnect,  # Atualiza flag mqtt_connected e loga desconexão
            on_error=lambda exc: emit_event("mqtt", f"Broker indisponivel em {MQTT_HOST}:{MQTT_PORT} ({exc})", "ERROR"),  # Loga erro se broker estiver offline
        )  # Fecha chamada connect_mqtt

    def _client_rpc(self, timeout: float = 1.5) -> Pyro5.api.Proxy:  # Cria proxy Pyro5 apontando para o processo ClusterRpcClient
        return rpc_proxy(client_uri(), timeout=timeout)  # Usa client_uri() e timeout configurável

    def start_ui_observer(self) -> None:  # Sobe Daemon Pyro5 local da UI e registra-o como observador no cliente RPC
        self._ui_daemon = Pyro5.api.Daemon(host="localhost", port=0)  # Usa port=0 para o SO alocar uma porta livre automaticamente
        uri = self._ui_daemon.register(PushReceiver(self.on_consensus_push), WEB_OBSERVER_NAME)  # Registra PushReceiver ligado a on_consensus_push
        self._ui_observer_uri = str(uri)  # Converte a URI gerada para string
        threading.Thread(target=self._ui_daemon.requestLoop, daemon=True).start()  # Roda o loop Pyro5 da UI em thread daemon
        self._sync_client_async(lambda rpc: bool(rpc.register_ui_observer(self._ui_observer_uri)))  # Inscreve a UI no cliente RPC com retentativas

    def stop_ui_observer(self) -> None:  # Desinscreve o observador da UI e encerra seu Daemon Pyro5 no shutdown
        if self._ui_observer_uri:  # Se a UI chegou a registrar uma URI
            with suppress_rpc_errors():  # Ignora erro se o cliente RPC já tiver parado
                with self._client_rpc(1.0) as rpc:  # Conecta ao cliente RPC
                    rpc.unregister_ui_observer(self._ui_observer_uri)  # Remove a inscrição da UI
        if self._ui_daemon is not None:  # Se o Daemon local está ativo
            with suppress_rpc_errors():  # Ignora erro no encerramento
                self._ui_daemon.shutdown()  # Para o requestLoop do Daemon da UI
            self._ui_daemon = None  # Limpa a referência

    def _sync_client_async(self, step: Callable[[Any], bool], attempts: int = 25, delay: float = 0.15) -> None:  # Tenta executar ação no cliente RPC em background até ele responder
        """Executa `step(rpc)` em background com retentativas e sincroniza o consenso inicial."""  # Docstring explicando o mecanismo de retry assíncrono
        def _run() -> None:  # Função executada em thread daemon
            for _ in range(attempts):  # Tenta até `attempts` vezes (útil enquanto subprocessos sobem)
                with suppress_rpc_errors():  # Ignora erro enquanto o serviço alvo ainda está inicializando
                    with self._client_rpc(1.0) as rpc:  # Abre proxy com o cliente RPC
                        if step(rpc):  # Executa a operação desejada (ex.: register_ui_observer)
                            self.last_consensus = rpc.get_consensus_state(log_result=False)  # Sincroniza o consenso inicial
                            self.notify_clients()  # Dispara atualização para os navegadores conectados via SSE
                            return  # Encerra a thread após sucesso
                time.sleep(delay)  # Aguarda breve intervalo antes da próxima tentativa

        threading.Thread(target=_run, daemon=True).start()  # Dispara a rotina de sincronização em background

    def notify_clients(self) -> None:  # Acorda todos os clientes SSE conectados incrementando a versão de estado
        with self._sse_cond:  # Adquire a Condition
            self._version += 1  # Incrementa o número de versão
            self._sse_cond.notify_all()  # Desbloqueia todas as threads que aguardam em wait_next_version

    def wait_next_version(self, last_version: int) -> int | None:  # Aguarda bloqueado até existir uma versão mais nova ou o servidor fechar
        """Bloqueia ate haver nova atualizacao SSE ou encerramento do servidor."""  # Docstring do método wait_next_version
        with self._sse_cond:  # Adquire a Condition
            while self._version == last_version and not self._closing:  # Espera enquanto não houver novidade nem encerramento
                self._sse_cond.wait()  # Libera o lock e dorme até notify_all()
            return None if self._closing else self._version  # Retorna None no shutdown ou a nova versão disponível

    def close(self) -> None:  # Sinaliza encerramento para liberar imediatamente todas as conexões SSE abertas
        self._closing = True  # Marca flag de encerramento
        self.notify_clients()  # Acorda threads SSE bloqueadas em wait()

    def on_consensus_push(self, consensus: dict) -> None:  # Recebe novo consenso empurrado pelo ClusterRpcClient
        self.last_consensus = consensus  # Atualiza o cache local de consenso
        self.notify_clients()  # Notifica o stream SSE para atualizar o painel web

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:  # Callback de conexão bem-sucedida ao Mosquitto
        self.mqtt_connected = True  # Marca ponte MQTT como conectada
        emit_event("mqtt", f"Broker Mosquitto conectado em {MQTT_HOST}:{MQTT_PORT}")  # Registra conexão no log

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:  # Callback de queda/desconexão do broker Mosquitto
        self.mqtt_connected = False  # Marca ponte MQTT como desconectada
        emit_event("mqtt", "Desconectado do broker Mosquitto", "WARN")  # Registra aviso no log

    def build_state_payload(self) -> dict:  # Monta o JSON completo de estado do sistema enviado ao frontend (/api/state e SSE)
        node_map: dict[int, dict] = {}  # Mapa {node_id: resposta_ping} obtido do cliente RPC
        with suppress_rpc_errors():  # Não quebra o painel se o processo cliente RPC estiver parado
            with self._client_rpc(1.5) as rpc:  # Conecta ao cliente RPC
                node_map = {item["node_id"]: item for item in rpc.get_nodes_status()}  # Indexa status dos nós por ID
                latest = rpc.get_last_consensus()  # Busca o último consenso em cache no cliente RPC
                if latest is not None:  # Se já houver consenso calculado
                    self.last_consensus = latest  # Atualiza o cache local do servidor web

        consensus = self.last_consensus or {}  # Usa dicionário vazio caso ainda não exista consenso
        alive_datas = [item["data"] for item in node_map.values() if item.get("ok") and item.get("data")]  # Coleta metadados de todos os nós vivos
        latest_leader = (  # Identifica o líder apontado pelo maior mandato (`term`) ativo como fallback sem quórum
            max(alive_datas, key=lambda d: int(d.get("term", 0))).get("leader_id")  # Busca leader_id do nó com maior term
            if alive_datas  # Apenas se houver pelo menos um controlador respondendo
            else None  # Caso todos os controladores estejam parados
        )  # Fecha cálculo de latest_leader
        effective_leader = consensus.get("leader_id") or latest_leader  # Prioriza o líder validado pelo quórum 2f+1 ou o maior mandato ativo
        controllers, storage = [], []  # Listas de saída para o estado dos nós e dos bancos SQLite
        for node_id in ALL_NODES:  # Percorre os 3 nós do cluster
            rpc_info = node_map.get(node_id, {})  # Obtém resposta RPC deste nó, se disponível
            data = rpc_info.get("data") or {}  # Extrai payload de dados do ping
            is_primary = (node_id == effective_leader) if effective_leader is not None else bool(data.get("is_primary", False))  # Garante exibição de um único líder por mandato
            controllers.append({  # Adiciona resumo operacional do controlador
                "id": node_id,  # ID do nó
                "alive": self.procs.is_alive(f"controller_{node_id}"),  # Se o subprocesso do SO está vivo
                "rpc_ok": bool(rpc_info.get("ok")),  # Se respondeu ao ping Pyro5
                "is_primary": is_primary,  # Se é o líder efetivo do mandato/quórum
                "byzantine": bool(data.get("byzantine", False)),  # Se está em modo bizantino
                "hash": data.get("hash"),  # Hash reportado via RPC
                "divergent": node_id in consensus.get("divergent", []),  # Se foi marcado como divergente no último quórum
                "port": BASE_PORT + node_id,  # Porta Pyro5 do controlador
            })  # Fecha item do controlador
            storage.append({"id": node_id, "db": node_db_path(node_id), **self.storages[node_id].get_snapshot()})  # Lê snapshot direto do SQLite local

        return {  # Retorna o payload unificado consumido pelo frontend
            "clients": {  # Seção do cliente RPC e do consenso 2f+1
                "alive": self.procs.is_alive("client"),  # Se o processo app.client está vivo
                "quorum_reached": consensus.get("quorum_reached"),  # Se o quórum foi atingido
                "votes": consensus.get("votes", "--"),  # Placar de votos (ex.: "3/3")
                "majority_hash": consensus.get("majority_hash"),  # Hash consensual
                "state": consensus.get("state"),  # Estado consensual do pet
                "divergent": consensus.get("divergent", []),  # IDs dos nós divergentes
            },  # Fecha seção clients
            "controllers": controllers,  # Lista dos 3 controladores
            "storage": storage,  # Lista dos 3 snapshots SQLite
            "mqtt": {"connected": self.mqtt_connected, "endpoint": f"{MQTT_HOST}:{MQTT_PORT}"},  # Status da conexão MQTT
            "sensors": {"alive": self.procs.is_alive("sensors")},  # Se o processo app.sensors está vivo
        }  # Fecha retorno de build_state_payload

    def read_recent_logs(self, limit: int = 120) -> list[dict]:  # Lê as últimas `limit` entradas válidas do arquivo JSONL de logs
        if not LOG_FILE.exists():  # Se o arquivo de log ainda não foi criado
            return []  # Retorna lista vazia
        entries: list[dict] = []  # Acumulador de entradas parseadas
        for line in LOG_FILE.read_text(encoding="utf-8").splitlines()[-limit:]:  # Percorre apenas as últimas N linhas do arquivo
            with suppress(json.JSONDecodeError):  # Ignora eventual linha parcial durante escrita concorrente
                entries.append(json.loads(line))  # Converte a linha JSON em dict
        return entries  # Retorna a lista de logs recentes

    def dispatch_action(self, body: dict) -> dict:  # Despacha ações recebidas via POST /api/action para o componente apropriado
        """Roteia acoes do painel web (POST /api/action) para RPC, processos, SQLite ou MQTT."""  # Docstring do método dispatch_action
        match body.get("op"):  # Seleciona operação pelo campo `op`
            case "client_command":  # Envia comando de negócio (ração, água, areia, alarme) via cliente RPC
                with self._client_rpc(4.0) as rpc:  # Timeout maior (4s) para acomodar eventual failover
                    res = rpc.execute_command(str(body["action"]), body.get("command_id"))  # Executa comando no cliente RPC
                    self.last_consensus = rpc.get_consensus_state(log_result=False, enforce_leader=True)  # Sincroniza consenso e liderança atualizados
                self.notify_clients()  # Atualiza painel via SSE
                return res  # Retorna resultado da execução RPC
            case "rpc_quorum" | "rpc_read":  # Solicita leitura explícita com apuração de quórum 2f+1
                with self._client_rpc(2.5) as rpc:  # Conecta ao cliente RPC
                    self.last_consensus = rpc.get_consensus_state(log_result=True)  # Apura consenso logando placar/divergências
                self.notify_clients()  # Atualiza painel via SSE
                return self.last_consensus  # Retorna o consenso apurado
            case "rpc_promote":  # Solicita promoção manual de um nó específico via votação P2P
                with self._client_rpc(2.5) as rpc:  # Conecta ao cliente RPC
                    ok = rpc.promote_node(int(body["node_id"]))  # Dispara promote_self() e votação P2P no nó alvo
                    self.last_consensus = rpc.get_consensus_state(log_result=False, enforce_leader=False)  # Atualiza cache com o novo líder eleito
                self.notify_clients()  # Atualiza painel via SSE
                return {"ok": ok}  # Retorna resultado booleano
            case "controller_stop":  # Mata o subprocesso de um controlador (simulação de falha crash)
                ok = self.procs.stop_controller(int(body["node_id"]))  # Para o processo do nó
                self.notify_clients()  # Atualiza painel via SSE
                return {"ok": ok}  # Retorna se foi parado
            case "controller_start":  # Sobe novamente o subprocesso de um controlador parado
                node_id = int(body["node_id"])  # Extrai o ID do nó alvo
                ok = self.procs.start_controller(node_id)  # Inicia o subprocesso do controlador
                if ok:  # Se o processo subiu agora
                    self._sync_client_async(lambda rpc: bool(rpc.register_observer_on_node(node_id)), attempts=20)  # Reinscreve observador do cliente no nó
                self.notify_clients()  # Atualiza painel via SSE
                return {"ok": ok}  # Retorna se foi iniciado
            case "controller_byzantine":  # Ativa ou desativa falha bizantina em um controlador
                with self._client_rpc(2.0) as rpc:  # Conecta ao cliente RPC
                    ok = rpc.set_byzantine(int(body["node_id"]), bool(body.get("enabled", True)))  # Alterna modo bizantino no nó
                self.notify_clients()  # Atualiza painel via SSE
                return {"ok": ok}  # Retorna resultado
            case "client_toggle_proc":  # Liga ou desliga o subprocesso do cliente RPC
                if self.procs.is_alive("client"):  # Se está rodando
                    return {"ok": self.procs.stop_service("client")}  # Encerra o processo cliente
                ok = self.procs.start_service("client")  # Caso contrário, inicia o processo cliente
                if ok and self._ui_observer_uri:  # Se iniciou e há observador web ativo
                    self._sync_client_async(lambda rpc: bool(rpc.register_ui_observer(self._ui_observer_uri)))  # Reinscreve a UI no cliente RPC
                return {"ok": ok}  # Retorna resultado
            case "storage_mutate":  # Edita diretamente o SQLite de uma réplica para simular corrupção de disco
                node_id, field, value = int(body["node_id"]), str(body["field"]), body["value"]  # Extrai nó, coluna e novo valor
                snap = self.storages[node_id].update_field_directly(field, value)  # Executa UPDATE direto no banco da réplica
                emit_event(f"storage_{node_id}", f"Edicao direta em {node_db_path(node_id)}: {field}={value} (hash {snap['hash']})", "WARN")  # Loga mutação direta
                return {"ok": True, "snapshot": snap}  # Retorna o snapshot atualizado do banco
            case "mqtt_publish":  # Publica mensagem MQTT arbitrária (para testes manuais no painel)
                publish_json(self.mqtt, str(body["topic"]), body["payload"], qos=1, log_prefix="PUB manual")  # Publica no tópico informado
                return {"ok": True}  # Confirma publicação
            case "sensor_publish":  # Solicita ao simulador IoT que publique uma leitura manual de sensor
                publish_json(self.mqtt, TOPIC_SIM_CONTROL, {"kind": "telemetry", "sensor": body["sensor"], "payload": body["payload"]}, qos=1, log_prefix="")  # Envia comando ao simulador
                return {"ok": True}  # Confirma envio
            case "sensor_auto":  # Liga ou pausa o ciclo automático de consumo no simulador IoT
                publish_json(self.mqtt, TOPIC_SIM_CONTROL, {"kind": "auto", "enabled": bool(body["enabled"])}, qos=1, log_prefix="")  # Envia flag auto ao simulador
                return {"ok": True}  # Confirma envio
            case "sensor_toggle_proc":  # Liga ou desliga o subprocesso simulador de sensores
                return {"ok": self.procs.stop_service("sensors") if self.procs.is_alive("sensors") else self.procs.start_service("sensors")}  # Alterna estado do processo sensors
            case op:  # Operação não reconhecida
                raise ValueError(f"Operacao desconhecida: {op}")  # Lança erro tratado como HTTP 400


def make_handler(ctx: AppContext):  # Fábrica que cria a classe Handler HTTP ligada à instância de AppContext
    class Handler(SimpleHTTPRequestHandler):  # Handler HTTP que serve arquivos estáticos e endpoints da API
        def __init__(self, *args, **kwargs):  # Inicializa o handler apontando o diretório raiz para `web/`
            super().__init__(*args, directory=str(WEB_DIR), **kwargs)  # Delega ao SimpleHTTPRequestHandler com directory=WEB_DIR

        def log_message(self, format, *args):  # Sobrescreve log padrão do http.server para não poluir o console
            return  # Silencia logs de acesso HTTP individuais

        def _send_json(self, status: int, payload: dict) -> None:  # Serializa um dicionário como resposta HTTP JSON UTF-8
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")  # Codifica JSON preservando acentos
            self.send_response(status)  # Envia código de status HTTP
            self.send_header("Content-Type", "application/json; charset=utf-8")  # Define Content-Type JSON
            self.send_header("Content-Length", str(len(data)))  # Informa tamanho exato do corpo
            self.end_headers()  # Finaliza cabeçalhos HTTP
            self.wfile.write(data)  # Escreve o corpo JSON na resposta

        def do_GET(self) -> None:  # Trata requisições GET para API, SSE e arquivos estáticos
            if self.path == "/api/state":  # Endpoint de snapshot completo sob demanda
                self._send_json(200, ctx.build_state_payload())  # Retorna estado atual do sistema em JSON
                return  # Encerra requisição
            if self.path == "/api/events":  # Endpoint de streaming em tempo real (Server-Sent Events)
                self._stream_sse()  # Mantém conexão aberta enviando atualizações incrementais
                return  # Encerra quando o cliente desconecta ou o servidor fecha
            if self.path == "/":  # Raiz do servidor web
                self.path = "/index.html"  # Redireciona internamente para index.html
            super().do_GET()  # Serve arquivos estáticos da pasta `web/`

        def _stream_sse(self) -> None:  # Implementa o loop de envio contínuo via Server-Sent Events
            self.send_response(200)  # Responde HTTP 200 OK para iniciar o stream
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")  # Define MIME type obrigatório de SSE
            self.send_header("Cache-Control", "no-cache")  # Desabilita cache intermediário
            self.send_header("Connection", "keep-alive")  # Mantém conexão TCP persistente
            self.end_headers()  # Finaliza cabeçalhos do stream
            last_version = -1  # Força envio imediato do estado inicial na primeira iteração
            try:  # Captura desconexão abrupta do navegador
                while (next_version := ctx.wait_next_version(last_version)) is not None:  # Aguarda próxima mudança ou encerramento
                    last_version = next_version  # Atualiza última versão enviada a este cliente
                    payload = {"state": ctx.build_state_payload(), "logs": ctx.read_recent_logs(150)}  # Monta estado + últimos 150 logs
                    self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))  # Envia evento no formato `data: <json>\n\n`
                    self.wfile.flush()  # Descarrega o buffer imediatamente para o navegador
            except (BrokenPipeError, ConnectionResetError):  # Se o usuário fechou a aba ou recarregou a página
                return  # Encerra a thread deste stream silenciosamente

        def do_POST(self) -> None:  # Trata requisições POST enviadas pelo painel web
            if self.path != "/api/action":  # Aceita apenas o endpoint unificado /api/action
                self._send_json(404, {"error": "Not found"})  # Retorna 404 para qualquer outra rota POST
                return  # Encerra requisição
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")  # Lê o corpo da requisição pelo Content-Length
            try:  # Protege contra JSON inválido ou erro de execução da ação
                self._send_json(200, ctx.dispatch_action(json.loads(raw)))  # Executa a ação e retorna o resultado com HTTP 200
            except Exception as exc:  # Caso ocorra erro de validação ou falha RPC
                self._send_json(400, {"ok": False, "error": str(exc)})  # Retorna HTTP 400 com a mensagem de erro

    return Handler  # Retorna a classe Handler configurada com closure sobre `ctx`


def main() -> None:  # Ponto de entrada principal que sobe todo o ambiente Pet Care
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)  # Garante existência do diretório logs/
    LOG_FILE.write_text("", encoding="utf-8")  # Limpa o log anterior ao iniciar uma nova execução

    ctx = AppContext()  # Cria o contexto da aplicação (ProcessManager, storages, SSE e MQTT)
    ctx.procs.start_all()  # Sobe os 3 controladores, o simulador de sensores e o cliente RPC
    ctx.start_ui_observer()  # Inicia o receptor push Pyro5 da interface web
    server = ThreadingHTTPServer((WEB_HOST, WEB_PORT), make_handler(ctx))  # Cria o servidor HTTP multi-thread na porta 8080
    server.daemon_threads = True  # Permite encerrar threads de requisições abertas no shutdown
    emit_event("clients", f"Servidor web ativo em http://localhost:{WEB_PORT}")  # Loga URL de acesso ao painel

    def shutdown(_signum: int) -> None:  # Handler de encerramento gracioso para SIGTERM/SIGINT
        ctx.close()  # Libera conexões SSE ativas
        threading.Thread(target=server.shutdown, daemon=True).start()  # Solicita parada de serve_forever() em outra thread (evita deadlock)

    install_shutdown_handler(shutdown)  # Registra handler para SIGTERM e SIGINT
    try:  # Mantém o servidor rodando até receber sinal de parada
        server.serve_forever()  # Entra no loop de atendimento HTTP/SSE
    finally:  # Executado sempre ao sair de serve_forever()
        ctx.close()  # Garante fechamento de conexões SSE
        ctx.stop_ui_observer()  # Desinscreve e encerra o Daemon Pyro5 da UI
        ctx.procs.stop_all()  # Encerra cliente RPC, sensores e os 3 controladores
        ctx.mqtt.loop_stop()  # Para a thread de rede do cliente MQTT da ponte web
        server.server_close()  # Fecha o socket do servidor HTTP


if __name__ == "__main__":  # Executa main() apenas quando rodado diretamente como módulo/script
    main()  # Inicia o servidor web e o cluster completo
```
