# app/infra.py

Utilitários compartilhados de infraestrutura: criação de proxies Pyro5 com timeout, consulta paralela aos nós do cluster (`query_cluster_nodes`), supressão de erros de rede esperados, padrão push (`PushReceiver` / `ObserverRegistry`), conexão MQTT e tratamento de sinais.

```python
"""Utilitarios compartilhados de infraestrutura: Pyro5, push assincrono, MQTT e sinais."""  # Docstring descrevendo as primitivas compartilhadas de infraestrutura

from concurrent.futures import ThreadPoolExecutor  # Executa chamadas RPC em paralelo para todos os nós do cluster
from contextlib import suppress  # Base para o context manager que suprime falhas esperadas de RPC
import json  # Serializa payloads dict em JSON para publicação MQTT
import signal  # Registra handlers para SIGTERM e SIGINT
import threading  # Usado para locks e disparo assíncrono de notificações push
from typing import Callable, Iterable  # Tipagem para callbacks e coleções de IDs de nós

import paho.mqtt.client as mqtt  # Cliente MQTT (Paho v2) usado pelos nós, sensores e servidor web
import Pyro5.api  # Biblioteca RPC usada na comunicação P2P e cliente-servidor

from app.config import MQTT_HOST, MQTT_KEEPALIVE, MQTT_PORT, peer_uri  # Configurações MQTT e helper de URI Pyro5 dos controladores
from app.event_bus import emit_event  # Emite logs padronizados ao publicar mensagens MQTT


def suppress_rpc_errors():  # Cria um context manager semântico para ignorar erros transitórios de RPC
    """Context manager semantico para ignorar falhas de rede esperadas em failover/teardown."""  # Docstring explicando o propósito semântico da função
    return suppress(Exception)  # Suprime exceções durante sondagens, votações P2P ou encerramento


def rpc_proxy(uri: str, timeout: float = 1.0) -> Pyro5.api.Proxy:  # Instancia um proxy Pyro5 já configurado com timeout
    """Cria um proxy Pyro5 configurado com timeout explicito."""  # Docstring da função helper
    proxy = Pyro5.api.Proxy(uri)  # Cria o proxy remoto apontando para a URI Pyro5 informada
    proxy._pyroTimeout = timeout  # Define limite de espera para não travar threads em nós mortos
    return proxy  # Retorna o proxy pronto para uso em bloco with


def query_cluster_nodes(  # Faz fan-out paralelo de um método RPC (`ping` ou `get_state`) nos controladores indicados
    node_ids: Iterable[int],  # Coleção de IDs de nós que serão consultados
    method: str,  # Nome do método remoto a invocar em cada controlador
    timeout: float = 1.0,  # Timeout individual de cada chamada Pyro5
    local: tuple[int, dict] | None = None,  # Tupla opcional (node_id, dados_locais) para evitar auto-RPC quando chamado por um controlador
) -> list[dict]:  # Retorna lista padronizada de resultados `{"node_id", "ok", "data" | "error"}`
    """Executa `method` em paralelo nos controladores (reaproveitando snapshot local se informado)."""  # Docstring da função query_cluster_nodes
    local_map = {local[0]: local[1]} if local is not None else {}  # Indexa o dado local pelo ID do próprio nó, se fornecido

    def _fetch(node_id: int) -> dict:  # Worker executado em paralelo para cada node_id
        if node_id in local_map:  # Se este ID corresponde ao próprio controlador chamador
            return {"node_id": node_id, "ok": True, "data": local_map[node_id]}  # Usa o snapshot em memória sem abrir socket RPC
        try:  # Captura falhas de conexão ou timeout de nós inativos
            with rpc_proxy(peer_uri(node_id), timeout=timeout) as proxy:  # Abre proxy Pyro5 para o controlador alvo
                return {"node_id": node_id, "ok": True, "data": getattr(proxy, method)()}  # Invoca `method` remotamente e empacota o resultado
        except Exception as exc:  # Se o nó estiver desligado ou exceder o timeout
            return {"node_id": node_id, "ok": False, "error": str(exc)}  # Retorna falha estruturada preservando a execução dos demais nós

    ids = list(node_ids)  # Materializa a lista de IDs para dimensionar o pool de threads
    with ThreadPoolExecutor(max_workers=len(ids)) as pool:  # Cria pool com uma thread por controlador consultado
        return list(pool.map(_fetch, ids))  # Executa as consultas simultaneamente mantendo a ordem de `ids`


@Pyro5.api.expose  # Expõe a classe para receber chamadas remotas via Pyro5
class PushReceiver:  # Receptor genérico de notificações push unidirecionais
    """Objeto remoto Pyro5 padrao para receber notificacoes unidirecionais (`on_push`)."""  # Docstring da classe PushReceiver

    def __init__(self, on_push: Callable[[dict], None]) -> None:  # Recebe o callback local que tratará os payloads recebidos
        self._on_push = on_push  # Armazena a função de callback para invocar em cada push

    @Pyro5.api.oneway  # Marca o método como fire-and-forget (o emissor não aguarda retorno)
    def on_push(self, payload: dict) -> None:  # Método remoto chamado pelo ObserverRegistry
        self._on_push(payload)  # Encaminha o payload recebido para o callback local


class ObserverRegistry:  # Mantém a lista de URIs inscritas e envia notificações push em background
    """Mantem URIs de observadores Pyro5 (`PushReceiver`) e remove automaticamente nos inativos."""  # Docstring da classe ObserverRegistry

    def __init__(self, timeout: float = 1.0) -> None:  # Inicializa o registro com timeout configurável para entregas push
        self._timeout = timeout  # Timeout aplicado a cada chamada RPC de notificação
        self._lock = threading.Lock()  # Protege o conjunto de URIs contra acesso concorrente
        self._uris: set[str] = set()  # Conjunto de URIs únicas dos observadores registrados

    def register(self, uri: str) -> bool:  # Adiciona a URI de um observador à lista de assinantes
        with self._lock:  # Adquire o lock antes de alterar o set
            self._uris.add(uri)  # Insere a URI no conjunto
        return True  # Confirma registro ao chamador remoto

    def unregister(self, uri: str) -> bool:  # Remove a URI de um observador da lista de assinantes
        with self._lock:  # Adquire o lock antes de alterar o set
            self._uris.discard(uri)  # Remove a URI sem lançar erro caso já não esteja presente
        return True  # Confirma desregistro ao chamador remoto

    def notify(self, payload: dict) -> None:  # Dispara notificação assíncrona para todos os observadores atuais
        with self._lock:  # Copia as URIs sob proteção do lock
            targets = list(self._uris)  # Cria snapshot da lista de destinos
        if not targets:  # Se não há observadores inscritos, evita criar thread à toa
            return  # Sai imediatamente

        def _push() -> None:  # Função executada em thread daemon para entregar o payload
            dead: list[str] = []  # Acumula URIs que falharam para limpeza posterior
            for uri in targets:  # Tenta enviar para cada observador
                try:  # Protege contra observadores desconectados ou encerrados
                    with rpc_proxy(uri, timeout=self._timeout) as proxy:  # Abre proxy com timeout curto
                        proxy.on_push(payload)  # Envia o payload via método oneway
                except Exception:  # Se o observador estiver inacessível
                    dead.append(uri)  # Marca a URI como morta para remoção
            if dead:  # Se houve falhas de entrega
                with self._lock:  # Adquire o lock para limpar o conjunto
                    self._uris.difference_update(dead)  # Remove todas as URIs inativas de uma vez

        threading.Thread(target=_push, daemon=True).start()  # Inicia o envio em thread daemon sem bloquear o chamador


def connect_mqtt(client_id: str, on_error: Callable[[Exception], None] | None = None, **callbacks) -> mqtt.Client:  # Cria, configura e conecta um cliente MQTT em background
    """Inicializa e conecta um cliente MQTT em background (`loop_start`)."""  # Docstring da função connect_mqtt
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)  # Instancia cliente Paho usando API de callbacks v2
    for name, fn in callbacks.items():  # Percorre os callbacks nomeados (on_connect, on_message, on_disconnect)
        if fn is not None:  # Ignora callbacks passados como None
            setattr(client, name, fn)  # Associa o callback ao atributo correspondente do cliente Paho
    try:  # Tenta estabelecer conexão TCP com o broker MQTT
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)  # Conecta no host/porta configurados
        client.loop_start()  # Inicia thread de rede em background para processar I/O MQTT
    except Exception as exc:  # Captura falha caso o broker esteja offline
        if on_error is None:  # Se nenhum handler de erro foi fornecido
            raise  # Propaga a exceção ao chamador
        on_error(exc)  # Notifica o erro pelo callback fornecido sem abortar o processo
    return client  # Retorna a instância do cliente MQTT


def publish_json(client: mqtt.Client, topic: str, payload: dict | str, qos: int = 1, log_prefix: str = "PUB") -> str:  # Publica mensagem JSON no MQTT com log opcional
    """Serializa payload JSON (se necessario), publica no MQTT e emite log padronizado."""  # Docstring da função publish_json
    raw = payload if isinstance(payload, str) else json.dumps(payload)  # Converte dict para string JSON se ainda não for string
    client.publish(topic, raw, qos=qos)  # Publica no broker com o nível de QoS especificado
    if log_prefix:  # Emite log somente quando log_prefix não é vazio
        shown = payload if not isinstance(payload, str) else raw  # Escolhe representação legível para o log
        emit_event("mqtt", f"{log_prefix} {topic} -> {shown}")  # Registra a publicação no barramento de eventos
    return raw  # Retorna a string JSON publicada


def install_shutdown_handler(handler: Callable[[int], None]) -> None:  # Instala callback único para encerramento via sinais do SO
    """Registra o mesmo callback de encerramento para SIGTERM e SIGINT."""  # Docstring da função install_shutdown_handler
    def _on_signal(signum: int, _frame) -> None:  # Adaptador com assinatura esperada por signal.signal(signum, frame)
        handler(signum)  # Repassa apenas o número do sinal para o handler do processo

    for sig in (signal.SIGTERM, signal.SIGINT):  # Itera sobre SIGTERM (kill/terminate) e SIGINT (Ctrl+C)
        signal.signal(sig, _on_signal)  # Registra o adaptador para ambos os sinais
```
