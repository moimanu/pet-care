# app/domain.py

Módulo de domínio puro (sem rede ou banco) que define os enums de protocolo (`Sensor`, `Perimeter`, `Action`, `RpcError`), o modelo de estado do pet (`PetState`), o cálculo de hash canônico para verificação de consenso e as regras declarativas de validação e transição de estado para telemetria e comandos.

```python
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
from typing import Any, Callable

from app.config import TOPIC_ACTUATORS

MAX_FOOD_GRAMS = 500.0
FOOD_STEP_GRAMS = 50.0
MAX_WATER_ML = 1000.0
WATER_STEP_ML = 200.0


class Sensor(StrEnum):
    RACAO = "racao"
    AGUA = "agua"
    AREIA = "areia"
    FUGA = "fuga"


class Perimeter(StrEnum):
    DENTRO = "DENTRO"
    FORA = "FORA"


class Action(StrEnum):
    LIBERAR_RACAO = "LIBERAR_RACAO"
    REPOR_AGUA = "REPOR_AGUA"
    RESETAR_AREIA = "RESETAR_AREIA"
    DISPARAR_ALARME = "DISPARAR_ALARME"
    SILENCIAR_ALARME = "SILENCIAR_ALARME"


class RpcError(StrEnum):
    NO_QUORUM = "NO_QUORUM"
    NOT_PRIMARY = "NOT_PRIMARY"
    BYZANTINE_FAULT = "BYZANTINE_FAULT"
    CLUSTER_UNAVAILABLE = "CLUSTER_UNAVAILABLE"


def _bounded_float(low: float, high: float) -> Callable[[Any, str], float]:
    # Fábrica de validador que rejeita booleanos/não-numéricos e exige valor float dentro do intervalo `[low, high]`.
    pass


def _non_negative_int(val: Any, field: str) -> int:
    # Valida que `val` é um inteiro não-negativo (rejeitando booleanos).
    pass


def _perimeter_str(val: Any, field: str) -> str:
    # Valida que `val` é uma string pertencente ao enum `Perimeter` (`DENTRO` ou `FORA`).
    pass


# Mapa declarativo: sensor -> (campo_payload_mqtt, atributo_PetState, validador)
SENSOR_SPECS: dict[str, tuple[str, str, Callable[[Any, str], Any]]] = {
    Sensor.RACAO: ("nivel_gramas", "racao_gramas", _bounded_float(0.0, MAX_FOOD_GRAMS)),
    Sensor.AGUA: ("nivel_ml", "agua_ml", _bounded_float(0.0, MAX_WATER_ML)),
    Sensor.AREIA: ("usos_acumulados", "areia_usos", _non_negative_int),
    Sensor.FUGA: ("perimetro", "status_fuga", _perimeter_str),
}


def utc_now_iso() -> str:
    # Gera um timestamp UTC atual formatado em ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`)
    # para registrar quando o estado ou evento foi atualizado.
    pass


@dataclass
class PetState:
    """Estado canonico compartilhado entre todas as replicas do cluster."""

    racao_gramas: float = 250.0
    agua_ml: float = 500.0
    areia_usos: int = 0
    status_fuga: str = str(Perimeter.DENTRO)
    alarme_ativo: bool = False
    atualizado_em: str = ""

    def __post_init__(self) -> None:
        # Garante que toda instância criada sem timestamp explícito receba automaticamente
        # o horário UTC atual como valor inicial de `atualizado_em`.
        pass

    def to_dict(self) -> dict:
        # Serializa os atributos do dataclass `PetState` para um dicionário Python padrão,
        # facilitando o envio via Pyro5, JSON e persistência em SQLite.
        pass

    @classmethod
    def from_dict(cls, data: dict) -> "PetState":
        # Reconstrói uma instância tipada de `PetState` a partir de um dicionário bruto,
        # convertendo explicitamente cada campo para seu tipo esperado e preenchendo o timestamp se ausente.
        pass

    def canonical_hash(self) -> str:
        # Calcula um resumo SHA-256 determinístico (12 caracteres) sobre os campos de negócio do estado,
        # arredondando valores decimais e excluindo `atualizado_em` para permitir comparação justa entre réplicas.
        pass


def validate_telemetry(sensor: str, payload: dict) -> dict:
    # 1. Valida se o payload recebido é um dicionário JSON e se `sensor` existe em `SENSOR_SPECS`.
    # 2. Aplica o validador específico do sensor ao campo esperado e retorna o dicionário higienizado;
    #    lança `ValueError` em caso de violação de contrato.
    pass


def _actuator(target: str, acao: Action | str, **extra) -> dict:
    # Função auxiliar que padroniza a estrutura de um comando de atuador MQTT,
    # encapsulando o tópico (`TOPIC_ACTUATORS/<target>`) e o payload com `acao` e campos extras.
    pass


def apply_telemetry(state: PetState, sensor: str, payload: dict) -> tuple[PetState, list[dict]]:
    # 1. Valida os dados de telemetria via `validate_telemetry` e cria uma nova cópia do estado com timestamp atualizado.
    # 2. Atualiza o atributo correspondente definido em `SENSOR_SPECS`.
    # 3. Caso o sensor de fuga indique `Perimeter.FORA`, ativa o alarme no estado e gera um comando de atuador
    #    `DISPARAR_ALARME` com motivo `FUGA_DETECTADA`.
    # 4. Retorna a tupla `(novo_estado, lista_de_comandos_de_atuadores)`.
    pass


def apply_command(state: PetState, action: str) -> tuple[PetState, list[dict]]:
    # 1. Cria uma nova cópia do estado atual com timestamp renovado para aplicar a ação solicitada.
    # 2. Interpreta comandos suportados (`LIBERAR_RACAO`, `REPOR_AGUA`, `RESETAR_AREIA`, `SILENCIAR_ALARME`) via `match/case`,
    #    aplicando tetos máximos de capacidade e gerando os respectivos comandos de atuadores MQTT quando aplicável.
    # 3. Lança `ValueError` para comandos desconhecidos e retorna `(novo_estado, lista_de_atuadores)`.
    pass
```
