# app/domain.py

Regras de negócio puras do Pet Care (sem dependências de rede ou banco): define os enums de protocolo (`Sensor`, `Perimeter`, `Action`, `TransitionKind`, `RpcError`), o modelo `PetState` com `canonical_hash()`, a especificação declarativa `SENSOR_SPECS`, as transições determinísticas (`validate_telemetry()`, `is_valid_state()`, `apply_telemetry()`, `apply_command()`, `apply_transition()`) e a apuração pura de consenso bizantino `evaluate_quorum()`.

```python
"""Regras de negocio puras e especificacao de protocolos do Pet Care (sem rede ou banco)."""  # Docstring indicando que este módulo contém apenas lógica pura de domínio

from collections import Counter  # Conta a frequência de cada hash nas respostas para apurar maioria 2f+1
from dataclasses import asdict, dataclass, replace  # Utilitários para criar modelos copiáveis e converter para dict
from datetime import datetime, timezone  # Usados para gerar timestamps ISO 8601 em UTC
from enum import StrEnum  # Base para enums cujos valores já são strings compatíveis com JSON/MQTT
import hashlib  # Gera o hash SHA-256 canônico usado no consenso entre réplicas
import json  # Serializa o núcleo do estado com chaves ordenadas para hash determinístico
from typing import Any, Callable  # Tipagem genérica para validadores de telemetria

from app.config import ALL_NODES, MIN_QUORUM, TOPIC_ACTUATORS  # Nós do cluster, quórum mínimo (2) e prefixo MQTT de atuadores

MAX_FOOD_GRAMS = 500.0  # Capacidade máxima do reservatório/tigela de ração em gramas
FOOD_STEP_GRAMS = 50.0  # Quantidade padrão liberada a cada comando LIBERAR_RACAO
MAX_WATER_ML = 1000.0  # Capacidade máxima do bebedouro em mililitros
WATER_STEP_ML = 200.0  # Quantidade padrão reposta a cada comando REPOR_AGUA


class Sensor(StrEnum):  # Identificadores dos sensores IoT reconhecidos pelo sistema
    RACAO = "racao"  # Sensor de nível de ração
    AGUA = "agua"  # Sensor de nível de água
    AREIA = "areia"  # Sensor de contagem de usos da caixa de areia
    FUGA = "fuga"  # Sensor de perímetro / detecção de fuga


class Perimeter(StrEnum):  # Valores válidos para o estado de perímetro do pet
    DENTRO = "DENTRO"  # Pet dentro da área segura
    FORA = "FORA"  # Pet fora da área segura (dispara alarme)


class Action(StrEnum):  # Comandos de atuação suportados via RPC e/ou MQTT
    LIBERAR_RACAO = "LIBERAR_RACAO"  # Comando para liberar uma porção de ração
    REPOR_AGUA = "REPOR_AGUA"  # Comando para repor água no bebedouro
    RESETAR_AREIA = "RESETAR_AREIA"  # Comando para zerar o contador de usos da areia
    DISPARAR_ALARME = "DISPARAR_ALARME"  # Ordem emitida aos atuadores quando uma fuga é detectada
    SILENCIAR_ALARME = "SILENCIAR_ALARME"  # Comando para desligar o alarme ativo


class TransitionKind(StrEnum):  # Categorias de transição determinística replicadas entre controladores
    COMMAND = "COMMAND"  # Transição originada por um comando RPC do cliente
    TELEMETRY = "TELEMETRY"  # Transição originada por uma leitura MQTT de sensor


class RpcError(StrEnum):  # Códigos de erro padronizados retornados nas chamadas RPC
    NO_QUORUM = "NO_QUORUM"  # Não houve respostas ou confirmações concordantes suficientes (2f+1)
    NOT_PRIMARY = "NOT_PRIMARY"  # O nó consultado não é o líder do mandato atual
    CLUSTER_UNAVAILABLE = "CLUSTER_UNAVAILABLE"  # Nenhum controlador saudável concluiu a operação após failover


def _bounded_float(low: float, high: float) -> Callable[[Any, str], float]:  # Fábrica de validador numérico para floats dentro de [low, high]
    def _parse(val: Any, field: str) -> float:  # Função interna que valida e converte o valor recebido
        if isinstance(val, bool) or not isinstance(val, (int, float)):  # Rejeita booleanos (subclasse de int em Python) e não-números
            raise ValueError(f"Campo '{field}' invalido: {val}")  # Lança erro descritivo com o nome do campo
        num = float(val)  # Converte int ou float para float padronizado
        if not (low <= num <= high):  # Verifica se o número está dentro do intervalo permitido
            raise ValueError(f"Valor fora dos limites [{low}, {high}]: {num}")  # Rejeita leituras fisicamente impossíveis
        return num  # Retorna o valor validado como float

    return _parse  # Retorna a função validadora configurada


def _non_negative_int(val: Any, field: str) -> int:  # Validador para contadores inteiros maiores ou iguais a zero
    if isinstance(val, bool) or not isinstance(val, int) or val < 0:  # Rejeita bool, float, string ou inteiro negativo
        raise ValueError(f"Campo '{field}' invalido: {val}")  # Lança erro de validação
    return val  # Retorna o inteiro validado


def _perimeter_str(val: Any, field: str) -> str:  # Validador para o campo de perímetro (DENTRO ou FORA)
    if not isinstance(val, str) or val not in Perimeter:  # Garante que seja string e pertença ao enum Perimeter
        raise ValueError(f"Campo '{field}' invalido: {val}")  # Lança erro caso o valor seja inválido
    return str(val)  # Retorna a string validada


# Mapa declarativo: sensor -> (campo_payload_mqtt, atributo_PetState, validador)  # Mapeia cada sensor ao campo de entrada, atributo de estado e função validadora
SENSOR_SPECS: dict[str, tuple[str, str, Callable[[Any, str], Any]]] = {  # Tabela de especificação usada por validate_telemetry e is_valid_state
    Sensor.RACAO: ("nivel_gramas", "racao_gramas", _bounded_float(0.0, MAX_FOOD_GRAMS)),  # Sensor de ração lê `nivel_gramas` [0, 500] para `racao_gramas`
    Sensor.AGUA: ("nivel_ml", "agua_ml", _bounded_float(0.0, MAX_WATER_ML)),  # Sensor de água lê `nivel_ml` [0, 1000] para `agua_ml`
    Sensor.AREIA: ("usos_acumulados", "areia_usos", _non_negative_int),  # Sensor de areia lê `usos_acumulados` >= 0 para `areia_usos`
    Sensor.FUGA: ("perimetro", "status_fuga", _perimeter_str),  # Sensor de fuga lê `perimetro` (DENTRO/FORA) para `status_fuga`
}  # Fecha o dicionário SENSOR_SPECS


def utc_now_iso() -> str:  # Gera timestamp UTC atual no formato ISO-8601 compacto
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # Retorna string UTC com sufixo Z


@dataclass  # Gera __init__, __repr__ e suporte a asdict/replace automaticamente
class PetState:  # Modelo de dados que representa o estado completo do ambiente do pet
    """Estado canonico compartilhado entre todas as replicas do cluster."""  # Docstring da dataclass PetState

    racao_gramas: float = 250.0  # Nível inicial padrão de ração (250g)
    agua_ml: float = 500.0  # Nível inicial padrão de água (500ml)
    areia_usos: int = 0  # Contador inicial de usos da caixa de areia
    status_fuga: str = str(Perimeter.DENTRO)  # Estado inicial seguro do perímetro
    alarme_ativo: bool = False  # Alarme inicialmente desligado
    atualizado_em: str = ""  # Timestamp da última alteração (preenchido no __post_init__ se vazio)

    def __post_init__(self) -> None:  # Hook executado após __init__ da dataclass
        if not self.atualizado_em:  # Se nenhum timestamp foi passado explicitamente
            self.atualizado_em = utc_now_iso()  # Preenche com o horário UTC atual

    def to_dict(self) -> dict:  # Converte o estado em dicionário Python simples
        return asdict(self)  # Serializa todos os campos da dataclass

    @classmethod  # Método de fábrica que constrói PetState a partir de um dicionário
    def from_dict(cls, data: dict) -> "PetState":  # Converte um dict (ex.: vindo do SQLite ou RPC) em instância tipada de PetState
        return cls(  # Instancia PetState fazendo coerção explícita de tipos
            racao_gramas=float(data["racao_gramas"]),  # Garante tipo float para ração
            agua_ml=float(data["agua_ml"]),  # Garante tipo float para água
            areia_usos=int(data["areia_usos"]),  # Garante tipo int para usos da areia
            status_fuga=str(data["status_fuga"]),  # Garante tipo str para status de fuga
            alarme_ativo=bool(data["alarme_ativo"]),  # Converte 0/1 ou bool para booleano Python
            atualizado_em=str(data.get("atualizado_em") or utc_now_iso()),  # Usa timestamp existente ou gera um novo se ausente
        )  # Fecha construção da instância

    def canonical_hash(self) -> str:  # Calcula assinatura determinística do estado para comparação de quórum
        """Hash deterministico do estado (ignora timestamp) para consenso 2f+1."""  # Docstring explicando por que `atualizado_em` é excluído
        core = {**self.to_dict(), "racao_gramas": round(self.racao_gramas, 2), "agua_ml": round(self.agua_ml, 2)}  # Copia campos normalizando precisão de floats
        del core["atualizado_em"]  # Remove o timestamp para que réplicas sincronizadas gerem o mesmo hash
        return hashlib.sha256(json.dumps(core, sort_keys=True).encode("utf-8")).hexdigest()[:12]  # Retorna os 12 primeiros hex chars do SHA-256


def validate_telemetry(sensor: str, payload: dict) -> dict:  # Valida o tipo, o sensor e o campo esperado no payload de telemetria
    """Valida e normaliza uma leitura de telemetria segundo `SENSOR_SPECS`."""  # Docstring da função validate_telemetry
    if not isinstance(payload, dict):  # Garante que o payload decodificado seja um objeto JSON
        raise ValueError("Payload deve ser um objeto JSON")  # Rejeita listas, strings ou escalares soltos
    spec = SENSOR_SPECS.get(sensor)  # Busca a especificação declarativa do sensor informado
    if spec is None:  # Se o sensor não existe no catálogo SENSOR_SPECS
        raise ValueError(f"Sensor desconhecido: {sensor}")  # Rejeita sensor não reconhecido
    payload_field, _, validator = spec  # Extrai o nome do campo esperado e a função validadora
    return {payload_field: validator(payload.get(payload_field), payload_field)}  # Valida o campo e retorna dicionário normalizado


def is_valid_state(state: PetState) -> bool:  # Confere se um PetState completo está dentro dos limites físicos (impede clonar estados forjados)
    """Verifica se todos os campos do estado respeitam os limites fisicos do dominio."""  # Docstring da função is_valid_state
    try:  # Captura qualquer violação lançada pelos validadores de SENSOR_SPECS
        for _, state_attr, validator in SENSOR_SPECS.values():  # Percorre cada atributo físico do estado e seu respectivo validador
            validator(getattr(state, state_attr), state_attr)  # Valida o valor atual do atributo em `state`
        return isinstance(state.alarme_ativo, bool)  # Confirma também que o flag de alarme é booleano
    except (ValueError, TypeError):  # Caso algum campo viole tipo ou faixa operacional (ex.: 9000g bizantino)
        return False  # Marca o estado como fisicamente inválido


def _actuator(target: str, acao: Action | str, **extra) -> dict:  # Helper que monta o envelope de comando para publicação MQTT
    return {"topic": f"{TOPIC_ACTUATORS}/{target}", "payload": {"acao": str(acao), **extra}}  # Retorna tópico alvo e payload JSON da ação


def apply_telemetry(state: PetState, sensor: str, payload: dict) -> tuple[PetState, list[dict]]:  # Função pura que aplica uma leitura de sensor ao estado
    """Aplica telemetria validada e retorna (novo_estado, comandos_de_atuadores)."""  # Docstring da transição de telemetria
    clean = validate_telemetry(sensor, payload)  # Valida e normaliza o payload recebido
    payload_field, state_attr, _ = SENSOR_SPECS[sensor]  # Obtém campo de entrada e atributo correspondente em PetState
    next_state = replace(state, **{state_attr: clean[payload_field], "atualizado_em": utc_now_iso()})  # Cria novo PetState com o campo atualizado e novo timestamp
    actuators: list[dict] = []  # Lista de comandos de atuadores gerados como efeito colateral lógico

    if sensor == Sensor.FUGA and next_state.status_fuga == Perimeter.FORA:  # Regra de negócio: se o pet saiu do perímetro
        next_state.alarme_ativo = True  # Ativa o flag de alarme no estado
        actuators.append(_actuator("alarme", Action.DISPARAR_ALARME, motivo="FUGA_DETECTADA"))  # Emite ordem MQTT para disparar o alarme físico

    return next_state, actuators  # Retorna a tupla (novo estado, lista de atuadores)


def apply_command(state: PetState, action: str) -> tuple[PetState, list[dict]]:  # Função pura que aplica um comando de usuário ao estado
    """Aplica um comando RPC ao estado e retorna (novo_estado, comandos_de_atuadores)."""  # Docstring da transição de comando
    next_state = replace(state, atualizado_em=utc_now_iso())  # Cria cópia do estado com timestamp renovado
    actuators: list[dict] = []  # Inicializa lista vazia de ordens para atuadores MQTT

    match action:  # Despacha conforme a ação solicitada
        case Action.LIBERAR_RACAO:  # Caso seja liberação de ração
            next_state.racao_gramas = min(MAX_FOOD_GRAMS, next_state.racao_gramas + FOOD_STEP_GRAMS)  # Soma 50g respeitando o limite máximo de 500g
            actuators.append(_actuator("dispenser", action, quantidade_gramas=FOOD_STEP_GRAMS))  # Envia comando ao dispenser físico
        case Action.REPOR_AGUA:  # Caso seja reposição de água
            next_state.agua_ml = min(MAX_WATER_ML, next_state.agua_ml + WATER_STEP_ML)  # Soma 200ml respeitando o limite máximo de 1000ml
            actuators.append(_actuator("valvula", action, quantidade_ml=WATER_STEP_ML))  # Envia comando à válvula de água
        case Action.RESETAR_AREIA:  # Caso seja limpeza/reset da caixa de areia
            next_state.areia_usos = 0  # Zera o contador de usos sem necessidade de atuador físico
        case Action.SILENCIAR_ALARME:  # Caso seja silenciamento do alarme
            next_state.alarme_ativo = False  # Desativa o flag de alarme no estado
            actuators.append(_actuator("alarme", action))  # Envia ordem para silenciar o atuador de alarme
        case _:  # Qualquer ação não prevista no protocolo
            raise ValueError(f"Comando desconhecido: {action}")  # Rejeita o comando com erro de valor

    return next_state, actuators  # Retorna a tupla (novo estado, lista de atuadores)


def apply_transition(state: PetState, kind: str, target: str, payload: dict) -> tuple[PetState, list[dict]]:  # Despachante determinístico usado pelo líder e pelas réplicas no sync_state
    """Aplica deterministicamente um comando ou telemetria ao estado."""  # Docstring da função apply_transition
    if kind == TransitionKind.COMMAND:  # Se a transição representa um comando RPC
        return apply_command(state, target)  # Delega para apply_command usando `target` como nome da ação
    if kind == TransitionKind.TELEMETRY:  # Se a transição representa uma leitura de sensor MQTT
        return apply_telemetry(state, target, payload)  # Delega para apply_telemetry usando `target` como sensor
    raise ValueError(f"Tipo de transicao desconhecido: {kind}")  # Rejeita categorias fora do enum TransitionKind


def evaluate_quorum(results: list[dict]) -> dict:  # Função pura de consenso 2f+1 compartilhada por PetController e ClusterRpcClient
    """Apura consenso bizantino 2f+1 (maioria >= MIN_QUORUM) e identifica o mandato/lider valido."""  # Docstring explicando a apuração de estado e mandato
    def no_quorum(reason: str) -> dict:  # Helper interno para padronizar retorno quando não há maioria suficiente
        return {  # Monta envelope indicando ausência de consenso
            "quorum_reached": False,  # Sinaliza que o quórum mínimo não foi atingido
            "reason": reason,  # Explicação legível do motivo da falha
            "nodes": results,  # Mantém respostas brutas para diagnóstico/UI
            "state": None,  # Nenhum estado consensual disponível
            "majority_ids": [],  # Nenhum grupo majoritário formado
            "divergent": [],  # Sem maioria de referência para classificar divergentes
            "term": 0,  # Mandato indefinido na ausência de quórum
            "leader_id": None,  # Nenhum líder válido confirmado pelo quórum
        }  # Fecha dicionário de falha de quórum

    valid = [item for item in results if item["ok"]]  # Filtra apenas os controladores acessíveis que responderam sem exceção
    if len(valid) < MIN_QUORUM:  # Exige ao menos MIN_QUORUM (2) nós ativos para formar maioria
        return no_quorum(f"Quorum indisponivel: apenas {len(valid)}/{len(ALL_NODES)} nos ativos")  # Falha por insuficiência de nós vivos

    majority_hash, majority_count = Counter(item["data"]["hash"] for item in valid).most_common(1)[0]  # Obtém o hash mais frequente e sua contagem de votos
    if majority_count < MIN_QUORUM:  # Se todas as respostas divergirem entre si (ex.: 3 hashes diferentes)
        return no_quorum("Sem consenso 2f+1: todas as respostas divergem")  # Rejeita por falta de maioria concordante

    majority_nodes = [item for item in valid if item["data"]["hash"] == majority_hash]  # Filtra os nós integrantes da maioria íntegra
    majority_ids = [item["node_id"] for item in majority_nodes]  # Extrai os IDs dos nós que compõem o quórum vencedor
    divergent_ids = [item["node_id"] for item in valid if item["data"]["hash"] != majority_hash]  # Identifica os IDs cujo hash divergiu da maioria

    newest = max(majority_nodes, key=lambda item: int(item["data"].get("term", 0)))["data"]  # Seleciona o registro de maior mandato (`term`) dentro da maioria
    term = int(newest.get("term", 0))  # Extrai o número do mandato mais recente entre os nós íntegros
    reported_leader = newest.get("leader_id")  # Lê quem esse mandato aponta como líder atual
    leader_id = reported_leader if reported_leader in majority_ids else None  # Só valida o líder se ele próprio estiver vivo e dentro da maioria íntegra

    return {  # Retorna o veredito completo do quórum 2f+1
        "quorum_reached": True,  # Confirma obtenção de consenso majoritário
        "majority_hash": majority_hash,  # Hash SHA-256 canônico vencedor
        "votes": f"{majority_count}/{len(valid)}",  # Placar textual de votos (ex.: "2/3" ou "3/3")
        "state": majority_nodes[0]["data"],  # Snapshot completo de um nó íntegro da maioria
        "majority_ids": majority_ids,  # Lista de IDs pertencentes à maioria saudável
        "divergent": divergent_ids,  # Lista de IDs descartados por divergência bizantina/corrupção
        "term": term,  # Mandato vigente reconhecido pela maioria
        "leader_id": leader_id,  # ID do líder íntegro ativo (ou None se precisar de nova eleição P2P)
        "nodes": results,  # Respostas brutas originais dos nós consultados
    }  # Fecha dicionário de consenso
```
