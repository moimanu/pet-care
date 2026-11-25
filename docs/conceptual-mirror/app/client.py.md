# app/client.py

Processo cliente Pyro5 RPC independente: realiza leituras e confirmações pós-escrita por quórum bizantino ($2f+1$), aciona reconciliação P2P no cluster quando não há líder íntegro, executa comandos com failover transparente preservando `command_id` e agrupa notificações push com debounce.

```python
"""Processo cliente Pyro5 RPC: consenso por quorum 2f+1, failover transparente e push UI."""  # Docstring resumindo as responsabilidades do cliente RPC

from concurrent.futures import ThreadPoolExecutor  # Inscreve observadores nos 3 controladores em paralelo
import sys  # Usado para encerrar o processo no shutdown
import threading  # Fornece Lock e Timer para o mecanismo de debounce de push
import uuid  # Gera command_id único quando o chamador não fornece um

import Pyro5.api  # Expõe ClusterRpcClient e PushReceiver via Daemon Pyro5

from app.config import ALL_NODES, CLIENT_OBJECT_NAME, CLIENT_OBSERVER_NAME, CLIENT_PORT, peer_uri  # Constantes de nós, porta 9094 e URIs Pyro5
from app.domain import RpcError, evaluate_quorum  # Erros padronizados e função pura de consenso 2f+1 do domínio
from app.event_bus import emit_event  # Barramento de logs estruturados
from app.infra import (  # Utilitários compartilhados de infraestrutura Pyro5 e sinais
    ObserverRegistry, PushReceiver, install_shutdown_handler,  # Registro push, receptor oneway e handler de sinais
    query_cluster_nodes, rpc_proxy, suppress_rpc_errors,  # Fan-out paralelo aos nós, proxy com timeout e supressão semântica
)  # Fecha importação de app.infra

DEBOUNCE_SECONDS = 0.08  # Janela de 80ms para agrupar pushes simultâneos das réplicas antes de recalcular o consenso


@Pyro5.api.expose  # Expõe a classe como serviço remoto Pyro5 na porta CLIENT_PORT
class ClusterRpcClient:  # Orquestra leituras/confirmações por quórum, failover idempotente e push para a UI
    """Servico cliente RPC com leitura/confirmacao por quorum (2f+1) e failover transparente."""  # Docstring da classe ClusterRpcClient

    def __init__(self) -> None:  # Inicializa caches de consenso, registro de observadores e controle de debounce
        self._last_divergent: tuple[int, ...] = ()  # Guarda os últimos nós divergentes logados para evitar repetição em pushes
        self._last_consensus: dict | None = None  # Cache em memória do último consenso calculado
        self._observer_uri: str | None = None  # URI do PushReceiver deste cliente registrada nos controladores
        self._ui_observers = ObserverRegistry()  # Observadores da camada web inscritos para receber consenso atualizado
        self._push_lock = threading.Lock()  # Sincroniza o agendamento/cancelamento do timer de debounce
        self._debounce_timer: threading.Timer | None = None  # Timer ativo que agrupa múltiplos pushes próximos

    def set_observer_uri(self, uri: str) -> None:  # Define a URI local do PushReceiver após registro no Daemon
        self._observer_uri = uri  # Salva URI para inscrição nos controladores

    def register_ui_observer(self, ui_uri: str) -> bool:  # Registra o servidor web para receber notificações de consenso
        return self._ui_observers.register(ui_uri)  # Delega ao ObserverRegistry da UI

    def unregister_ui_observer(self, ui_uri: str) -> bool:  # Remove o servidor web da lista de observadores
        return self._ui_observers.unregister(ui_uri)  # Delega ao ObserverRegistry da UI

    def register_observer_on_node(self, node_id: int) -> bool:  # Inscreve este cliente como observador em um controlador específico
        if not self._observer_uri:  # Se a URI local ainda não foi configurada
            return False  # Não há observador para registrar
        try:  # Protege contra controlador offline no momento da inscrição
            with rpc_proxy(peer_uri(node_id)) as proxy:  # Abre proxy para o controlador alvo
                return bool(proxy.register_observer(self._observer_uri))  # Registra a URI no controlador
        except Exception:  # Caso o controlador não esteja acessível
            return False  # Retorna False silenciosamente

    def register_all_observers(self) -> None:  # Inscreve este cliente nos 3 controladores simultaneamente
        if self._observer_uri:  # Confirma que a URI do receptor local já existe
            with ThreadPoolExecutor(max_workers=len(ALL_NODES)) as pool:  # Cria pool paralelo para os 3 nós
                list(pool.map(self.register_observer_on_node, ALL_NODES))  # Dispara inscrição em todos os nós

    def unregister_all_observers(self) -> None:  # Cancela debounce pendente e desinscreve observador no encerramento
        with self._push_lock:  # Protege acesso ao timer de debounce
            if self._debounce_timer is not None:  # Se existir timer aguardando disparo
                self._debounce_timer.cancel()  # Cancela o timer antes de encerrar
                self._debounce_timer = None  # Limpa referência
        if not self._observer_uri:  # Se nenhuma URI foi registrada
            return  # Encerra imediatamente
        for node_id in ALL_NODES:  # Percorre todos os controladores do cluster
            with suppress_rpc_errors():  # Ignora nós que já estejam desligados
                with rpc_proxy(peer_uri(node_id)) as proxy:  # Abre proxy para o nó
                    proxy.unregister_observer(self._observer_uri)  # Remove inscrição deste cliente

    def handle_observer_push(self, _payload: dict) -> None:  # Recebe push de qualquer controlador e reinicia janela de debounce
        """Agrupa notificacoes simultaneas das replicas (debounce) antes de recalcular o consenso."""  # Docstring do debounce
        with self._push_lock:  # Garante exclusão mútua ao recriar o timer
            if self._debounce_timer is not None:  # Se já havia um timer correndo
                self._debounce_timer.cancel()  # Cancela para aguardar as demais réplicas concluírem o sync
            self._debounce_timer = threading.Timer(DEBOUNCE_SECONDS, self._evaluate_and_push)  # Agenda apuração após 80ms
            self._debounce_timer.daemon = True  # Thread daemon para não bloquear encerramento do processo
            self._debounce_timer.start()  # Inicia contagem do timer

    def _evaluate_and_push(self) -> None:  # Recalcula consenso 2f+1 e envia resultado para o painel web
        self._ui_observers.notify(self.get_consensus_state(log_result=False, enforce_leader=True))  # Empurra novo consenso à UI

    def get_nodes_status(self) -> list[dict]:  # Consulta `ping()` nos 3 controladores em paralelo
        return query_cluster_nodes(ALL_NODES, "ping")  # Usa utilitário compartilhado de fan-out paralelo

    def get_last_consensus(self) -> dict | None:  # Devolve o último consenso armazenado em cache
        return self._last_consensus  # Evita novas chamadas RPC quando apenas inspecionando estado recente

    def _request_cluster_election(self, majority_ids: list[int]) -> None:  # Pede à maioria saudável que conduza sua eleição P2P
        """Aciona o candidato deterministico da maioria para conduzir a eleicao P2P no cluster."""  # Docstring de _request_cluster_election
        if not majority_ids:  # Se não houver nós na maioria
            return  # Nada a acionar
        candidate_id = min(majority_ids)  # Identifica o mesmo candidato determinístico usado pelos controladores
        with suppress_rpc_errors():  # Protege contra queda simultânea do candidato
            with rpc_proxy(peer_uri(candidate_id), timeout=1.5) as proxy:  # Conecta ao controlador íntegro de menor ID
                proxy.reconcile_leadership()  # Faz os próprios controladores votarem e oficializarem o novo mandato

    def get_consensus_state(self, log_result: bool = True, enforce_leader: bool = True) -> dict:  # Apura quórum 2f+1 e garante líder íntegro
        """Consulta o cluster em paralelo, valida quorum 2f+1 e solicita eleicao P2P se sem lider integro."""  # Docstring de get_consensus_state
        results = query_cluster_nodes(ALL_NODES, "get_state")  # Coleta estado completo + mandato dos 3 controladores
        consensus = evaluate_quorum(results)  # Executa apuração pura de consenso 2f+1

        if consensus["quorum_reached"] and enforce_leader and consensus["leader_id"] is None:  # Se há quórum mas o líder caiu ou divergiu
            self._request_cluster_election(consensus["majority_ids"])  # Dispara eleição P2P entre os controladores saudáveis
            results = query_cluster_nodes(ALL_NODES, "get_state")  # Reconsulta os nós para capturar o novo mandato eleito
            consensus = evaluate_quorum(results)  # Recalcula o consenso já com o novo líder

        self._last_consensus = consensus  # Atualiza cache do último consenso
        if not consensus["quorum_reached"]:  # Se não atingiu maioria mínima (2 de 3)
            if log_result:  # Emite erro caso o log tenha sido solicitado
                emit_event("rpc", consensus["reason"], "ERROR")  # Registra motivo da ausência de quórum
            return consensus  # Retorna envelope indicando `quorum_reached: False`

        majority_hash = consensus["majority_hash"]  # Hash SHA-256 vencedor
        divergent_key = tuple(sorted(consensus["divergent"]))  # Conjunto ordenado de nós divergentes
        should_log = log_result or divergent_key != self._last_divergent  # Loga se explícito ou se mudou o conjunto de divergentes
        self._last_divergent = divergent_key  # Atualiza memória de divergentes já reportados

        for item in results:  # Percorre resultados individuais para reportar nós descartados pelo quórum
            if item["ok"] and item["node_id"] in divergent_key and should_log:  # Se o nó respondeu com hash inválido
                node_hash = item["data"]["hash"]  # Extrai o hash divergente retornado
                emit_event(  # Registra no log do cliente o descarte pelo quórum bizantino
                    "rpc",  # Componente RPC/cliente
                    f"Quorum 2f+1: No {item['node_id']} divergente (hash {node_hash} != {majority_hash}); descartado",  # Detalha divergência
                    "ERROR",  # Nível ERROR para visibilidade imediata no painel
                )  # Fecha emit_event

        if log_result and not divergent_key:  # Quando todos os nós ativos concordam e log explícito foi pedido
            emit_event("rpc", f"Quorum 2f+1 OK ({consensus['votes']} votos, mandato {consensus['term']}, hash {majority_hash})")  # Loga consenso íntegro
        return consensus  # Retorna dicionário completo do consenso

    def promote_node(self, target_id: int) -> bool:  # Solicita promoção manual de um controlador via votação P2P
        try:  # Protege contra nó alvo desligado
            with rpc_proxy(peer_uri(target_id), timeout=2.0) as proxy:  # Abre proxy para o nó que se deseja promover
                return bool(proxy.promote_self())  # O próprio controlador propõe novo mandato aos pares via request_vote/commit_term
        except Exception as exc:  # Se houver falha de comunicação com o alvo
            emit_event("rpc", f"Falha ao solicitar promocao do No {target_id}: {exc}", "ERROR")  # Registra falha no log
            return False  # Retorna False indicando que a promoção não ocorreu

    def set_byzantine(self, target_id: int, flag: bool) -> bool:  # Ativa ou desativa simulação bizantina em um controlador
        try:  # Protege contra controlador desligado
            with rpc_proxy(peer_uri(target_id)) as proxy:  # Conecta ao nó alvo
                return bool(proxy.set_byzantine(bool(flag)))  # Alterna flag bizantina no controlador
        except Exception as exc:  # Se o nó estiver inacessível
            emit_event("rpc", f"Falha ao alterar modo bizantino no No {target_id}: {exc}", "ERROR")  # Loga erro
            return False  # Indica falha

    def execute_command(self, action: str, command_id: str | None = None) -> dict:  # Executa comando validando quórum antes e depois do RPC
        """Envia comando ao lider eleito pelo quorum e confirma pos-execucao por maioria 2f+1."""  # Docstring de execute_command
        cmd_id = (command_id or "").strip() or str(uuid.uuid4())  # Mantém UUID informado ou gera um novo para idempotência
        consensus = self.get_consensus_state(log_result=True, enforce_leader=True)  # Pré-valida quórum 2f+1 e garante líder íntegro eleito
        if not consensus["quorum_reached"]:  # Aborta imediatamente se o cluster não tiver maioria saudável
            emit_event("rpc", f"Comando {action} abortado: sem quorum 2f+1", "ERROR")  # Loga bloqueio por falta de quórum
            return {"ok": False, "error": str(RpcError.NO_QUORUM), "command_id": cmd_id}  # Retorna erro NO_QUORUM

        tried: set[int] = set()  # Registra líderes já tentados nesta chamada para evitar repetição
        for _ in ALL_NODES:  # Permite failover passando pelos controladores disponíveis
            leader_id = consensus.get("leader_id")  # Obtém o líder confirmado pelo mandato do quórum atual
            if leader_id is None or leader_id in tried:  # Se não há líder elegível ou já foi tentado
                break  # Encerra o laço de tentativas
            tried.add(leader_id)  # Marca este líder como tentado
            try:  # Envia comando RPC ao líder do mandato
                with rpc_proxy(peer_uri(leader_id), timeout=2.0) as proxy:  # Timeout de 2s cobrindo replicação P2P
                    response = proxy.execute_command(action, cmd_id)  # Invoca comando passando o mesmo cmd_id
                if response.get("ok"):  # Se o líder respondeu sucesso, audita a pós-condição no quórum 2f+1
                    post = self.get_consensus_state(log_result=False, enforce_leader=True)  # Reconsulta os 3 nós (e destitui líder se ele mentiu)
                    if (  # Confirma que há quórum, que o líder continua na maioria íntegra e que o hash bate com o quórum
                        post["quorum_reached"]  # Quórum pós-escrita válido
                        and leader_id in post["majority_ids"]  # Líder respondente não divergiu
                        and post["majority_hash"] == response.get("hash")  # Estado retornado coincide com a maioria replicada
                    ):  # Fecha condição de confirmação pós-escrita
                        tag = " (idempotente)" if response.get("idempotent") else ""  # Indica se foi reaproveitamento idempotente
                        emit_event("rpc", f"RPC {action} ({cmd_id[:8]}) -> No {leader_id} OK{tag}")  # Loga conclusão verificada
                        return {**response, "command_id": cmd_id, "node_id": leader_id}  # Retorna sucesso ao chamador
                    emit_event("rpc", f"Resposta do No {leader_id} diverge do quorum 2f+1; iniciando failover", "ERROR")  # Detectou mentira bizantina durante o RPC
                    consensus = post  # Usa o novo consenso (já com novo líder eleito) para repetir o comando
                    continue  # Reexecuta na próxima iteração com o mesmo cmd_id
                emit_event("rpc", f"No {leader_id} recusou ({response.get('error')}); iniciando failover", "WARN")  # Loga recusa explícita (ex.: NOT_PRIMARY)
            except Exception as exc:  # Captura queda abrupta ou timeout do líder durante a execução
                emit_event("rpc", f"Falha RPC no Lider {leader_id}: {exc}; iniciando failover", "WARN")  # Loga falha Crash-Stop e inicia failover

            consensus = self.get_consensus_state(log_result=False, enforce_leader=True)  # Reapura consenso acionando eleição P2P do substituto

        emit_event("rpc", f"Falha ao executar {action}: nenhum controlador disponivel", "ERROR")  # Loga esgotamento de tentativas
        return {"ok": False, "error": str(RpcError.CLUSTER_UNAVAILABLE), "command_id": cmd_id}  # Retorna indisponibilidade do cluster


def main() -> None:  # Ponto de entrada do processo `python -m app.client`
    client = ClusterRpcClient()  # Instancia o serviço cliente RPC
    daemon = Pyro5.api.Daemon(host="localhost", port=CLIENT_PORT)  # Cria Daemon Pyro5 na porta 9094
    daemon.register(client, CLIENT_OBJECT_NAME)  # Expõe o objeto remoto ClusterRpcClient
    observer_uri = daemon.register(PushReceiver(client.handle_observer_push), CLIENT_OBSERVER_NAME)  # Registra receptor push para ouvir controladores
    client.set_observer_uri(str(observer_uri))  # Armazena URI gerada pelo Daemon
    client.register_all_observers()  # Inscreve este cliente nos 3 controladores
    client.get_consensus_state(log_result=False)  # Calcula estado consensual inicial na subida
    emit_event("rpc", f"Cliente RPC Pyro5 ativo na porta {CLIENT_PORT} (observer {observer_uri})")  # Loga inicialização concluída

    def shutdown(signum: int) -> None:  # Handler de encerramento para SIGTERM/SIGINT
        emit_event("rpc", f"Cliente RPC encerrado (sinal {signum})", "WARN")  # Registra encerramento no log
        client.unregister_all_observers()  # Desinscreve observador nos controladores ativos
        daemon.shutdown()  # Encerra o loop Pyro5
        sys.exit(0)  # Finaliza o processo

    install_shutdown_handler(shutdown)  # Registra handler para sinais de parada
    daemon.requestLoop()  # Atende requisições Pyro5 continuamente


if __name__ == "__main__":  # Executa main() apenas como script principal
    main()  # Inicia o processo cliente RPC
```
