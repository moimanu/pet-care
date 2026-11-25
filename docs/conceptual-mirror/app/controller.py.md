# app/controller.py

Nó controlador independente do cluster distribuído: consome telemetria MQTT, executa comandos RPC Pyro5 com idempotência, valida replicação P2P determinística por hash (`sync_state`), conduz eleição/destituição P2P por mandato (`term`) com quórum $2f+1$ (`request_vote` / `commit_term`) e suporta simulação de falha bizantina.

```python
"""No controlador do cluster: MQTT, replicacao validada por hash, eleicao P2P 2f+1 e Pyro5 RPC."""  # Docstring resumindo as responsabilidades do nó controlador

import argparse  # Faz parsing dos argumentos CLI (--id e --primary)
import json  # Decodifica telemetrias MQTT e serializa payloads de eventos
import sys  # Usado para encerrar o processo no shutdown
import threading  # Fornece Locks separados para estado local e coordenação de eleição
import uuid  # Gera command_id único para cada telemetria MQTT recebida

import Pyro5.api  # Expõe o controlador como objeto remoto RPC e cria seu Daemon

from app.config import ALL_NODES, BASE_PORT, MIN_QUORUM, TOPIC_SENSORS, controller_object_name, node_db_path, peer_uri  # Constantes do cluster, quórum e helpers
from app.domain import (  # Importa entidades e funções puras do domínio
    Perimeter, PetState, RpcError, TransitionKind,  # Enums de perímetro, erro RPC, tipo de transição e dataclass PetState
    apply_transition, evaluate_quorum, is_valid_state,  # Transição determinística, consenso 2f+1 e validação física de estado
)  # Fecha importação de app.domain
from app.event_bus import emit_event  # Barramento de logs estruturados
from app.infra import (  # Importa primitivas compartilhadas de rede e concorrência
    ObserverRegistry, connect_mqtt, install_shutdown_handler,  # Registro push, conexão MQTT e sinais de encerramento
    publish_json, query_cluster_nodes, rpc_proxy, suppress_rpc_errors,  # Publicação MQTT, consulta paralela P2P e proxies Pyro5
)  # Fecha importação de app.infra
from app.storage import NodeStorage  # Persistência SQLite isolada do nó


@Pyro5.api.expose  # Torna os métodos públicos e propriedades da classe acessíveis via Pyro5
class PetController:  # Representa uma réplica/controlador independente do cluster Pet Care
    """Processo controlador independente com persistencia SQLite e consenso P2P de lideranca."""  # Docstring da classe PetController

    def __init__(self, node_id: int, is_primary: bool = False) -> None:  # Inicializa mandato, SQLite, locks, MQTT e catch-up P2P
        self.node_id = node_id  # ID numérico deste nó (1, 2 ou 3)
        self.term: int = 1 if is_primary else 0  # Mandato inicial (1 para o líder de partida, 0 para réplicas até sincronizarem)
        self.leader_id: int | None = node_id if is_primary else None  # ID do líder reconhecido no mandato atual
        self.byzantine = False  # Flag de simulação de falha bizantina (adulteração de estado/respostas)
        self.storage = NodeStorage(node_db_path(node_id))  # Abre/inicializa o banco SQLite exclusivo deste nó
        self._lock = threading.Lock()  # Protege leituras/escritas locais (memória + SQLite), nunca mantido durante RPCs externos
        self._election_lock = threading.Lock()  # Serializa tentativas de eleição/promoção iniciadas por este nó
        self._observers = ObserverRegistry()  # Gerencia observadores Pyro5 inscritos para receber push de atualizações
        self.mqtt = connect_mqtt(  # Conecta este controlador ao broker MQTT
            f"petcare_ctrl_{node_id}",  # Client ID exclusivo no broker MQTT
            on_connect=self._on_mqtt_connect,  # Callback que assina petcare/sensores/# ao conectar
            on_message=self._on_mqtt_message,  # Callback que processa telemetrias recebidas
            on_error=lambda exc: self._log(f"Falha ao conectar ao MQTT: {exc}", "ERROR"),  # Loga erro sem derrubar o nó se o broker estiver fora
        )  # Fecha chamada connect_mqtt
        self._catch_up_from_peers()  # Sincroniza SQLite e mandato atual consultando o quórum de pares ativos
        self._log(f"Controlador {node_id} iniciado na porta {BASE_PORT + node_id} ({self._role()}, mandato {self.term})")  # Registra inicialização e mandato

    @property  # Deriva liderança diretamente de leader_id para evitar estado inconsistente
    def is_primary(self) -> bool:  # Indica se este controlador é o líder do mandato atual
        return self.leader_id == self.node_id  # Verdadeiro somente quando leader_id aponta para este nó

    def _log(self, message: str, level: str = "INFO") -> None:  # Helper para emitir logs marcados com o ID deste controlador
        emit_event(f"controller_{self.node_id}", message, level)  # Encaminha para o event_bus com component="controller_<id>"

    def _role(self) -> str:  # Retorna o papel textual do nó para exibição nos logs
        return "LIDER" if self.is_primary else "REPLICA"  # Traduz is_primary para LIDER ou REPLICA

    def _peer_ids(self) -> list[int]:  # Lista os IDs dos demais nós do cluster (excluindo a si mesmo)
        return [peer_id for peer_id in ALL_NODES if peer_id != self.node_id]  # Filtra ALL_NODES removendo self.node_id

    def _visible_state(self) -> PetState:  # Retorna o estado que o nó expõe em consultas RPC (real ou forjado)
        """Retorna o estado real do SQLite, ou um estado forjado em modo bizantino."""  # Docstring explicando a simulação bizantina
        if self.byzantine:  # Se o modo bizantino estiver ativado neste nó
            return PetState(  # Retorna um estado sintético inválido/adulterado para provocar divergência de hash
                racao_gramas=9000.0 + self.node_id,  # Valor adulterado fora da escala física e único por node_id
                agua_ml=0.0,  # Água zerada artificialmente
                areia_usos=99,  # Contador de areia forjado
                status_fuga=str(Perimeter.FORA),  # Falsa indicação de fuga
                alarme_ativo=True,  # Alarme forjado como ativo
            )  # Fecha construção do estado bizantino
        return self.storage.load_state()  # Em operação honesta, lê o estado real do SQLite

    def _state_snapshot(self, state: PetState, term: int, leader_id: int | None, byzantine: bool) -> dict:  # Monta snapshot completo com estado, mandato e hash
        return {  # Combina campos de negócio com metadados de consenso do nó
            **state.to_dict(),  # Espalha campos do PetState (racao_gramas, agua_ml, etc.)
            "node_id": self.node_id,  # Identificador do nó respondente
            "term": term,  # Número do mandato atual conhecido por este nó
            "leader_id": leader_id,  # Líder associado ao mandato atual
            "is_primary": leader_id == self.node_id,  # Booleano derivado do líder do mandato
            "byzantine": byzantine,  # Metadado para visualização de status no painel web
            "hash": state.canonical_hash(),  # Hash SHA-256 canônico usado na votação de quórum 2f+1
        }  # Fecha dicionário de snapshot

    def _notify_observers(self, state: PetState, reason: str, command_id: str | None = None) -> None:  # Envia push assíncrono aos observadores inscritos
        with self._lock:  # Captura metadados atuais de mandato de forma atômica
            snap = self._state_snapshot(state, self.term, self.leader_id, self.byzantine)  # Gera snapshot consistente sob lock
        self._observers.notify({**snap, "reason": reason, "command_id": command_id})  # Dispara push sem manter o lock preso

    # --- Coordenacao P2P por quorum 2f+1 ---  # Separador das rotinas de auditoria, votação e replicação entre controladores

    def _evaluate_cluster_quorum(self) -> dict:  # Apura o consenso 2f+1 diretamente a partir deste controlador
        results = query_cluster_nodes(ALL_NODES, "get_state", timeout=0.8, local=(self.node_id, self.get_state()))  # Consulta pares em paralelo + estado local
        return evaluate_quorum(results)  # Executa a regra pura de quórum do domínio

    def commit_term(self, term: int, leader_id: int | None) -> bool:  # Oficializa um mandato já eleito pelo quórum 2f+1
        """Aplica um mandato ja aprovado por maioria 2f+1, rebaixando lideres de mandatos anteriores."""  # Docstring de commit_term
        with self._lock:  # Atualiza term e leader_id atomicamente
            if term < self.term or (term == self.term and self.leader_id == leader_id):  # Ignora mandatos antigos ou idênticos ao atual
                return False  # Nenhuma mudança de mandato necessária
            was_primary = self.is_primary  # Registra se este nó era líder antes da atualização
            self.term = term  # Avança para o novo mandato aprovado
            self.leader_id = leader_id  # Define o novo líder do mandato
            state = self._visible_state()  # Captura estado visível para notificar observadores
            role_changed = was_primary != self.is_primary  # Verifica se houve transição LIDER <-> REPLICA neste nó
        if role_changed:  # Emite log apenas quando o papel efetivamente mudou
            self._log(f"Papel alterado para {self._role()} (mandato {term})", "WARN")  # Loga promoção a LIDER ou rebaixamento a REPLICA
        self._notify_observers(state, "ROLE_CHANGE")  # Notifica cliente RPC e painel web sobre a mudança de mandato
        return True  # Confirma aplicação do novo mandato

    def _propose_leadership(self, candidate_id: int, base_term: int = 0) -> bool:  # Conduz eleição P2P em duas etapas: votação e oficialização
        """Coleta votos P2P (`request_vote`); se atingir quorum 2f+1, oficializa via `commit_term`."""  # Docstring explicando o protocolo de eleição
        new_term = max(self.term, base_term) + 1  # Incrementa o mandato acima do maior mandato observado
        votes = 1  # Contabiliza o próprio voto do nó proponente
        for peer_id in self._peer_ids():  # Solicita voto aos demais controladores do cluster
            with suppress_rpc_errors():  # Ignora pares desligados ou inacessíveis
                with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:  # Abre proxy com timeout curto para votação P2P
                    if proxy.request_vote(new_term, candidate_id):  # Pede voto (nós bizantinos ou divergentes negam)
                        votes += 1  # Soma voto favorável confirmado pelo par honesto
        if votes < MIN_QUORUM:  # Exige maioria mínima (2 de 3) para aprovar o novo líder
            return False  # Aborta eleição caso não alcance quórum 2f+1

        self.commit_term(new_term, candidate_id)  # Aplica o novo mandato localmente
        for peer_id in self._peer_ids():  # Propaga o mandato aprovado para todos os pares acessíveis
            with suppress_rpc_errors():  # Não falha se algum par estiver offline
                with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:  # Conecta a cada par
                    proxy.commit_term(new_term, candidate_id)  # Garante que líderes antigos sejam rebaixados a REPLICA

        self._log(  # Registra no log a vitória da eleição P2P com placar e mandato
            f"Quorum P2P ({votes}/{len(ALL_NODES)}): No {candidate_id} eleito Lider no mandato {new_term}",  # Mensagem detalhando votos e mandato
            "WARN",  # Nível WARN para destacar evento de governança do cluster
        )  # Fecha chamada de log
        return True  # Confirma que o novo líder foi eleito e empossado

    def reconcile_leadership(self) -> int | None:  # Verifica se há líder íntegro na maioria; caso contrário, inicia eleição determinística
        """Audita o cluster via quorum 2f+1; sem lider integro na maioria, o menor ID assume."""  # Docstring de reconcile_leadership
        if self.byzantine:  # Nó bizantino não conduz reconciliação legítima
            return self.leader_id  # Retorna seu leader_id atual sem iniciar eleição
        with self._election_lock:  # Evita eleições concorrentes disparadas pelo mesmo nó
            consensus = self._evaluate_cluster_quorum()  # Apura quórum 2f+1 entre os 3 controladores
            if not consensus["quorum_reached"]:  # Se menos de 2 nós concordarem, não há quórum seguro para eleger
                return None  # Retorna None indicando ausência de líder consensual
            if consensus["leader_id"] is not None:  # Se já existe um líder vivo e pertencente à maioria íntegra
                self.commit_term(consensus["term"], consensus["leader_id"])  # Alinha mandato local caso estivesse atrasado
                return consensus["leader_id"]  # Retorna o ID do líder saudável atual

            candidate_id = min(consensus["majority_ids"])  # Regra determinística: escolhe o menor ID dentro da maioria íntegra
            if candidate_id == self.node_id and self._propose_leadership(candidate_id, consensus["term"]):  # Apenas o vencedor determinístico propõe a votação
                return self.leader_id  # Retorna o próprio ID após vencer a votação P2P
            return None  # Caso não seja o candidato ou a votação falhe

    def _catch_up_from_peers(self) -> None:  # Clona estado íntegro e mandato ao iniciar um nó ou sair do modo bizantino
        """Sincroniza SQLite e mandato com o estado majoritario integro ao iniciar ou recuperar."""  # Docstring de _catch_up_from_peers
        peer_results = query_cluster_nodes(self._peer_ids(), "get_state", timeout=0.8)  # Busca estado de todos os pares em paralelo
        consensus = evaluate_quorum([{"node_id": self.node_id, "ok": True, "data": self.get_state()}, *peer_results])  # Tenta apurar quórum 2f+1 completo

        target_data: dict | None = None  # Guardar snapshot escolhido para sincronização
        if consensus["quorum_reached"]:  # Caminho preferencial: existe maioria 2f+1 concordante no cluster
            target_data = {**consensus["state"], "term": consensus["term"], "leader_id": consensus["leader_id"]}  # Seleciona estado e mandato da maioria
        elif self.term == 0:  # Fallback apenas no boot inicial quando outro nó está parado (1 par vivo)
            active_leaders = [  # Filtra pares vivos com mandato válido e estado fisicamente íntegro
                item["data"]  # Extrai dicionário de estado do par
                for item in peer_results  # Percorre resultados dos pares
                if item["ok"]  # Exige que o par tenha respondido
                and int(item["data"].get("term", 0)) > 0  # Exige mandato já inicializado (> 0)
                and item["data"].get("leader_id") == item["node_id"]  # Exige que o par seja o líder daquele mandato
                and is_valid_state(PetState.from_dict(item["data"]))  # Bloqueia cópia de valores forjados fora dos limites físicos
            ]  # Fecha filtro de líderes ativos íntegros
            if active_leaders:  # Se encontrou líder íntegro ativo
                target_data = max(active_leaders, key=lambda d: int(d.get("term", 0)))  # Escolhe o de maior mandato

        if target_data is None:  # Se não há referência segura para sincronizar
            return  # Mantém o SQLite local intacto

        synced_state = PetState.from_dict(target_data)  # Converte o dicionário alvo em PetState tipado
        if not is_valid_state(synced_state):  # Dupla barreira contra estados corrompidos/bizantinos
            return  # Nunca sobrescreve o SQLite com dados fisicamente inválidos

        with self._lock:  # Grava estado sincronizado e atualiza mandato de forma atômica
            self.storage.save_state(synced_state)  # Atualiza tabela pet_state no SQLite local
            self.term = max(self.term, int(target_data.get("term", 0)))  # Adota o mandato vigente do cluster
            if target_data.get("leader_id") is not None:  # Se houver líder definido na referência
                self.leader_id = int(target_data["leader_id"])  # Atualiza leader_id local (reingressando como REPLICA)
        emit_event(  # Registra no log do storage a conclusão do catch-up
            f"storage_{self.node_id}",  # Componente de armazenamento deste nó
            f"Catch-up concluido (mandato {self.term}, lider {self.leader_id}, hash {synced_state.canonical_hash()})",  # Resumo com mandato, líder e hash
        )  # Fecha emissão de evento
        self._notify_observers(synced_state, "CATCH_UP")  # Notifica cliente RPC e UI que este nó voltou a ficar sincronizado

    def _commit_and_replicate(  # Pipeline de escrita do líder com validação por quórum P2P antes de persistir/atuar
        self, kind: str, target: str, payload: dict, command_id: str, log_msg: str  # Categoria, ação/sensor, dados, UUID e mensagem de log
    ) -> dict:  # Retorna dicionário padronizado com status `ok`, `state` e `hash`
        """Calcula transicao deterministica, exige quorum P2P de replicacao e persiste/atua."""  # Docstring do pipeline _commit_and_replicate
        with self._lock:  # Lê estado base e calcula transição sem segurar lock durante chamadas de rede
            term, leader_id, byzantine = self.term, self.node_id, self.byzantine  # Captura contexto do mandato e flag bizantina
            next_state, actuators = apply_transition(self._visible_state(), kind, target, payload)  # Calcula próximo estado (real ou forjado se bizantino)
            expected_hash = next_state.canonical_hash()  # Gera hash esperado após a transição

        if byzantine:  # Simulação realista de líder bizantino: tenta propagar hash forjado e mente sucesso ao cliente
            self._log(f"Controlador {self.node_id} (BIZANTINO) forjou transicao {target}", "ERROR")  # Loga tentativa de adulteração pelo líder comprometido
            for peer_id in self._peer_ids():  # Envia sync_state com hash forjado aos pares (que rejeitarão deterministicamente)
                with suppress_rpc_errors():  # Ignora erros de rede ao tentar enganar os pares
                    with rpc_proxy(peer_uri(peer_id), timeout=0.8) as proxy:  # Abre proxy para a réplica
                        proxy.sync_state(term, leader_id, command_id, kind, target, payload, expected_hash)  # Réplicas honestas barrarão pelo expected_hash inválido
            return {"ok": True, "idempotent": False, "state": next_state.to_dict(), "hash": expected_hash}  # Mente `ok: True` ao cliente (pego pelo quórum pós-escrita)

        acks = 1  # Líder honesto conta sua própria validação como 1 confirmação
        for peer_id in self._peer_ids():  # Envia a operação para cada réplica validar e aplicar
            try:  # Isola falhas individuais de rede (Crash-Stop de até 1 réplica)
                with rpc_proxy(peer_uri(peer_id), timeout=1.0) as proxy:  # Abre proxy Pyro5 para a réplica
                    if proxy.sync_state(term, leader_id, command_id, kind, target, payload, expected_hash):  # Réplica recalcula e confirma se o hash bater
                        acks += 1  # Incrementa confirmações válidas recebidas
            except Exception as exc:  # Caso a réplica esteja parada/inacessível
                self._log(f"Replica {peer_id} inacessivel no sync: {exc}", "WARN")  # Registra aviso e continua com as demais

        if acks < MIN_QUORUM:  # Só efetiva a escrita se pelo menos MIN_QUORUM (2) nós concordarem com o hash resultante
            self._log(f"Transicao {target} abortada: sem quorum P2P ({acks}/{len(ALL_NODES)})", "ERROR")  # Loga aborto por falta de maioria
            return {"ok": False, "error": str(RpcError.NO_QUORUM)}  # Aborta sem alterar o SQLite local nem acionar atuadores

        event_label = target if kind == TransitionKind.COMMAND else f"TELEMETRIA_{target.upper()}"  # Formata rótulo do evento para o event_log
        with self._lock:  # Persiste estado e command_id atomicamente no SQLite do líder
            self.storage.save_state(next_state, event=(command_id, event_label, json.dumps(payload)))  # Salva na mesma transação SQLite
        self._log(log_msg)  # Emite log operacional da transição concluída
        emit_event(f"storage_{self.node_id}", f"Estado salvo ({event_label}, hash {expected_hash})")  # Loga hash persistido no banco do líder

        for command in actuators:  # Publica comandos físicos gerados para os atuadores no MQTT (QoS 2)
            publish_json(self.mqtt, command["topic"], {**command["payload"], "command_id": command_id}, qos=2)  # Anexa command_id para deduplicação na borda

        self._notify_observers(next_state, event_label, command_id)  # Notifica observadores inscritos via push assíncrono
        return {"ok": True, "idempotent": False, "state": next_state.to_dict(), "hash": expected_hash}  # Retorna sucesso confirmado com o novo hash

    # --- Entrada MQTT ---  # Separador visual dos callbacks de mensageria MQTT

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties=None) -> None:  # Callback executado ao conectar ao Mosquitto
        client.subscribe(f"{TOPIC_SENSORS}/#", qos=1)  # Assina todos os tópicos de sensores com QoS 1
        self._log(f"Inscrito em {TOPIC_SENSORS}/# (QoS 1)")  # Confirma assinatura no log

    def _on_mqtt_message(self, client, userdata, msg) -> None:  # Recebe telemetria MQTT e aciona reconciliação P2P se necessário
        sensor = msg.topic.split("/")[-1]  # Extrai o nome do sensor do último segmento do tópico
        raw = msg.payload.decode("utf-8", errors="replace")  # Decodifica o payload UTF-8 recebido

        if not self.is_primary and self.reconcile_leadership() != self.node_id:  # Se não é líder, audita o cluster (assume se o líder caiu/divergiu)
            return  # Sai se outro nó íntegro já é o líder ou tem menor ID
        if not self.is_primary:  # Garante que apenas o líder efetivo prossiga
            return  # Evita processamento duplicado nas réplicas

        try:  # Protege contra JSON inválido ou leituras fora dos limites físicos
            payload = json.loads(raw)  # Converte string JSON em dicionário
            command_id = str(uuid.uuid4())  # Gera UUID único para rastrear e replicar esta telemetria
            self._commit_and_replicate(  # Valida, replica por quórum P2P, persiste e aciona eventuais atuadores
                TransitionKind.TELEMETRY, sensor, payload, command_id,  # Informa categoria TELEMETRY e sensor alvo
                f"Telemetria MQTT {sensor} processada ({command_id[:8]})",  # Mensagem resumida de log
            )  # Fecha chamada _commit_and_replicate
        except Exception as exc:  # Captura falhas de parsing ou validação de domínio (`validate_telemetry`)
            self._log(f"Telemetria rejeitada ({sensor}): {exc}", "ERROR")  # Registra descarte da telemetria inválida

    # --- Interface RPC Pyro5 ---  # Separador visual dos métodos remotos expostos pelo controlador

    def register_observer(self, observer_uri: str) -> bool:  # Inscreve um PushReceiver remoto neste controlador
        return self._observers.register(observer_uri)  # Delega inscrição ao ObserverRegistry

    def unregister_observer(self, observer_uri: str) -> bool:  # Remove um PushReceiver remoto deste controlador
        return self._observers.unregister(observer_uri)  # Delega remoção ao ObserverRegistry

    def ping(self) -> dict:  # Health-check rápido com metadados de mandato e hash canônico
        with self._lock:  # Lê atributos locais de forma consistente
            return {  # Retorna resumo leve do controlador
                "node_id": self.node_id,  # ID numérico deste nó
                "term": self.term,  # Mandato atual do nó
                "leader_id": self.leader_id,  # Líder reconhecido neste mandato
                "is_primary": self.is_primary,  # Papel derivado (leader_id == node_id)
                "byzantine": self.byzantine,  # Flag para exibição no painel
                "hash": self._visible_state().canonical_hash(),  # Hash SHA-256 do estado visível
            }  # Fecha retorno do ping

    def get_state(self) -> dict:  # Retorna snapshot completo usado na apuração de quórum 2f+1
        with self._lock:  # Garante leitura consistente de estado e mandato
            return self._state_snapshot(self._visible_state(), self.term, self.leader_id, self.byzantine)  # Monta dicionário completo

    def request_vote(self, new_term: int, candidate_id: int) -> bool:  # Endpoint RPC de votação P2P para eleição/troca de líder
        """Voto P2P de mandato: nos bizantinos nunca votam; nos honestos exigem maioria 2f+1."""  # Docstring de request_vote
        if self.byzantine:  # Nó comprometido se recusa a cooperar na eleição
            return False  # Nega voto (a maioria honesta elege sem depender dele)
        with self._lock:  # Verifica monotonia do mandato proposto
            if new_term < self.term:  # Rejeita propostas com mandato inferior ao atual
                return False  # Voto negado por mandato obsoleto
            if new_term == self.term and self.leader_id == candidate_id:  # Idempotência caso já tenha confirmado o mesmo candidato
                return True  # Mantém voto favorável
        consensus = self._evaluate_cluster_quorum()  # Audita independentemente os hashes 2f+1 do cluster
        if not consensus["quorum_reached"] or candidate_id not in consensus["majority_ids"]:  # Exige que o candidato pertença à maioria íntegra
            return False  # Impede que qualquer nó divergente/bizantino seja eleito
        self._log(f"Voto P2P concedido no mandato {new_term} para o No {candidate_id}")  # Registra concessão do voto no log
        return True  # Concede voto ao candidato íntegro

    def promote_self(self) -> bool:  # Endpoint RPC acionado pelo botão "Promover a Líder" no painel
        """Solicitacao de promocao sujeita a votacao P2P de quorum 2f+1."""  # Docstring de promote_self
        if self.byzantine:  # Impede promoção manual de nó em falha bizantina
            return False  # Recusa promoção
        with self._election_lock:  # Serializa com eventuais eleições em andamento neste nó
            consensus = self._evaluate_cluster_quorum()  # Verifica se o nó está sincronizado com a maioria 2f+1
            if not consensus["quorum_reached"] or self.node_id not in consensus["majority_ids"]:  # Só permite promoção de réplica íntegra
                return False  # Recusa se estiver divergente ou sem quórum
            return self._propose_leadership(self.node_id, consensus["term"])  # Submete promoção à votação P2P normal do cluster

    def set_byzantine(self, flag: bool) -> bool:  # Ativa ou restaura a simulação de falha bizantina neste controlador
        with self._lock:  # Altera apenas a flag em memória sob lock (sem fazer RPC com lock preso)
            self.byzantine = bool(flag)  # Atualiza estado bizantino
        if flag:  # Se o modo bizantino foi ligado
            self._log(f"Modo BIZANTINO ativado: No {self.node_id} enviando estado adulterado", "ERROR")  # Emite alerta de adulteração ativa
        else:  # Se o nó foi restaurado para operação honesta
            self._log(f"Modo BIZANTINO desativado: No {self.node_id} restaurado", "WARN")  # Registra restauração
            self._catch_up_from_peers()  # Ressincroniza SQLite e mandato com a maioria íntegra do cluster
        return bool(flag)  # Retorna novo valor da flag

    def sync_state(  # Recebe replicação P2P do líder e valida mandato + transição determinística antes de persistir
        self,  # Instância da réplica receptora
        term: int,  # Mandato informado pelo líder remetente
        leader_id: int,  # ID do líder remetente
        command_id: str,  # UUID único do comando ou telemetria
        kind: str,  # Categoria da transição (COMMAND ou TELEMETRY)
        target: str,  # Nome da ação ou do sensor
        payload: dict,  # Dados de entrada da transição
        expected_hash: str,  # Hash canônico que o líder afirma ter obtido
    ) -> bool:  # Retorna True somente se a transição for legítima e persistida
        """Valida mandato e recalcula deterministicamente a transicao antes de gravar no SQLite."""  # Docstring de sync_state
        if self.byzantine:  # Nó em modo bizantino não aplica replicação legítima
            return False  # Recusa sincronização
        with self._lock:  # Serializa validação e gravação no SQLite da réplica
            if term < self.term or (term == self.term and self.leader_id not in (None, leader_id)):  # Barragem contra líderes destituídos ou de mandato antigo
                return False  # Ignora mensagens de ex-líderes
            if self.storage.has_event(command_id):  # Se o command_id já consta no event_log local
                return self.storage.load_state().canonical_hash() == expected_hash  # Confirma idempotência se o hash bater
            try:  # Executa a regra de negócio pura sobre o próprio SQLite local
                next_state, _ = apply_transition(self.storage.load_state(), kind, target, payload)  # Recalcula estado localmente
            except Exception as exc:  # Se o payload violar regras/limites físicos do domínio
                self._log(f"Sync invalido do No {leader_id}: {exc}", "ERROR")  # Loga tentativa inválida
                return False  # Rejeita replicação
            if next_state.canonical_hash() != expected_hash:  # Compara hash recalculado localmente com o hash enviado pelo líder
                self._log(f"Sync rejeitado do No {leader_id}: hash divergente ({expected_hash})", "ERROR")  # Detecta líder bizantino/corrompido tentando contaminar réplica
                return False  # Bloqueia gravação no SQLite da réplica honesta
            self.term = term  # Atualiza mandato local acompanhando líder legítimo
            self.leader_id = leader_id  # Confirma ID do líder remetente
            event_label = target if kind == TransitionKind.COMMAND else f"TELEMETRIA_{target.upper()}"  # Define rótulo para o histórico de eventos
            self.storage.save_state(next_state, event=(command_id, event_label, json.dumps(payload)))  # Persiste novo estado e UUID atomicamente

        self._log(f"Sync P2P validado e aplicado ({event_label})")  # Registra sucesso da replicação validada
        emit_event(f"storage_{self.node_id}", f"Sync replicado (hash {expected_hash})")  # Emite evento para o card de banco no painel
        self._notify_observers(next_state, f"SYNC_{event_label}", command_id)  # Dispara notificação push aos observadores
        return True  # Confirma voto de replicação ao líder

    def execute_command(self, action: str, command_id: str) -> dict:  # Executa comando RPC enviado pelo cliente ao líder
        """Executa comando RPC com verificacao de lideranca, idempotencia e quorum P2P."""  # Docstring de execute_command
        if not self.is_primary:  # Recusa caso este controlador seja apenas réplica no mandato atual
            return {"ok": False, "error": str(RpcError.NOT_PRIMARY)}  # Orienta o cliente a consultar/eleger o líder correto
        with self._lock:  # Verifica idempotência no SQLite antes de reexecutar (apenas em nós honestos)
            if not self.byzantine and self.storage.has_event(command_id):  # Se este UUID já foi registrado anteriormente
                state = self.storage.load_state()  # Carrega o estado atual já persistido
                self._log(f"Idempotencia: comando {command_id[:8]} ({action}) ja registrado; ignorado", "WARN")  # Registra detecção de duplicata
                return {"ok": True, "idempotent": True, "state": state.to_dict(), "hash": state.canonical_hash()}  # Devolve sucesso idempotente com hash atual
        return self._commit_and_replicate(  # Executa transição, votação de sync P2P, persistência e atuação MQTT
            TransitionKind.COMMAND, action, {"acao": action}, command_id,  # Passa metadados do comando
            f"Comando {action} executado ({command_id[:8]})",  # Mensagem de log em caso de sucesso
        )  # Fecha chamada _commit_and_replicate


def main() -> None:  # Ponto de entrada CLI para `python -m app.controller --id <n> [--primary]`
    parser = argparse.ArgumentParser()  # Cria parser de argumentos da linha de comando
    parser.add_argument("--id", type=int, required=True, choices=list(ALL_NODES))  # Exige --id entre 1, 2 ou 3
    parser.add_argument("--primary", action="store_true")  # Flag opcional que inicia o nó como líder do mandato 1
    args = parser.parse_args()  # Faz parsing dos argumentos recebidos

    controller = PetController(args.id, is_primary=args.primary)  # Instancia o controlador com ID e papel inicial
    daemon = Pyro5.api.Daemon(host="localhost", port=BASE_PORT + args.id)  # Cria servidor Pyro5 na porta dedicada (9091..9093)
    daemon.register(controller, controller_object_name(args.id))  # Registra o controlador no Daemon Pyro5

    def shutdown(signum: int) -> None:  # Handler de encerramento gracioso para SIGTERM/SIGINT
        emit_event(f"controller_{args.id}", f"Controlador {args.id} encerrado (sinal {signum})", "WARN")  # Registra encerramento no log
        controller.mqtt.loop_stop()  # Encerra a thread de rede do cliente MQTT
        daemon.shutdown()  # Interrompe o loop de requisições Pyro5
        sys.exit(0)  # Finaliza o processo com código 0

    install_shutdown_handler(shutdown)  # Associa SIGTERM e SIGINT ao handler de shutdown
    daemon.requestLoop()  # Entra no loop bloqueante atendendo chamadas RPC Pyro5


if __name__ == "__main__":  # Executa main() apenas quando chamado como módulo principal
    main()  # Inicia o processo do controlador
```
