"""Regras de negocio puras e especificacao de protocolos do Pet Care (sem rede ou banco)."""

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
    def _parse(val: Any, field: str) -> float:
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            raise ValueError(f"Campo '{field}' invalido: {val}")
        num = float(val)
        if not (low <= num <= high):
            raise ValueError(f"Valor fora dos limites [{low}, {high}]: {num}")
        return num

    return _parse


def _non_negative_int(val: Any, field: str) -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val < 0:
        raise ValueError(f"Campo '{field}' invalido: {val}")
    return val


def _perimeter_str(val: Any, field: str) -> str:
    if not isinstance(val, str) or val not in Perimeter:
        raise ValueError(f"Campo '{field}' invalido: {val}")
    return str(val)


# Mapa declarativo: sensor -> (campo_payload_mqtt, atributo_PetState, validador)
SENSOR_SPECS: dict[str, tuple[str, str, Callable[[Any, str], Any]]] = {
    Sensor.RACAO: ("nivel_gramas", "racao_gramas", _bounded_float(0.0, MAX_FOOD_GRAMS)),
    Sensor.AGUA: ("nivel_ml", "agua_ml", _bounded_float(0.0, MAX_WATER_ML)),
    Sensor.AREIA: ("usos_acumulados", "areia_usos", _non_negative_int),
    Sensor.FUGA: ("perimetro", "status_fuga", _perimeter_str),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        if not self.atualizado_em:
            self.atualizado_em = utc_now_iso()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PetState":
        return cls(
            racao_gramas=float(data["racao_gramas"]),
            agua_ml=float(data["agua_ml"]),
            areia_usos=int(data["areia_usos"]),
            status_fuga=str(data["status_fuga"]),
            alarme_ativo=bool(data["alarme_ativo"]),
            atualizado_em=str(data.get("atualizado_em") or utc_now_iso()),
        )

    def canonical_hash(self) -> str:
        """Hash deterministico do estado (ignora timestamp) para consenso 2f+1."""
        core = {**self.to_dict(), "racao_gramas": round(self.racao_gramas, 2), "agua_ml": round(self.agua_ml, 2)}
        del core["atualizado_em"]
        return hashlib.sha256(json.dumps(core, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def validate_telemetry(sensor: str, payload: dict) -> dict:
    """Valida e normaliza uma leitura de telemetria segundo `SENSOR_SPECS`."""
    if not isinstance(payload, dict):
        raise ValueError("Payload deve ser um objeto JSON")
    spec = SENSOR_SPECS.get(sensor)
    if spec is None:
        raise ValueError(f"Sensor desconhecido: {sensor}")
    payload_field, _, validator = spec
    return {payload_field: validator(payload.get(payload_field), payload_field)}


def _actuator(target: str, acao: Action | str, **extra) -> dict:
    return {"topic": f"{TOPIC_ACTUATORS}/{target}", "payload": {"acao": str(acao), **extra}}


def apply_telemetry(state: PetState, sensor: str, payload: dict) -> tuple[PetState, list[dict]]:
    """Aplica telemetria validada e retorna (novo_estado, comandos_de_atuadores)."""
    clean = validate_telemetry(sensor, payload)
    payload_field, state_attr, _ = SENSOR_SPECS[sensor]
    next_state = replace(state, **{state_attr: clean[payload_field], "atualizado_em": utc_now_iso()})
    actuators: list[dict] = []

    if sensor == Sensor.FUGA and next_state.status_fuga == Perimeter.FORA:
        next_state.alarme_ativo = True
        actuators.append(_actuator("alarme", Action.DISPARAR_ALARME, motivo="FUGA_DETECTADA"))

    return next_state, actuators


def apply_command(state: PetState, action: str) -> tuple[PetState, list[dict]]:
    """Aplica um comando RPC ao estado e retorna (novo_estado, comandos_de_atuadores)."""
    next_state = replace(state, atualizado_em=utc_now_iso())
    actuators: list[dict] = []

    match action:
        case Action.LIBERAR_RACAO:
            next_state.racao_gramas = min(MAX_FOOD_GRAMS, next_state.racao_gramas + FOOD_STEP_GRAMS)
            actuators.append(_actuator("dispenser", action, quantidade_gramas=FOOD_STEP_GRAMS))
        case Action.REPOR_AGUA:
            next_state.agua_ml = min(MAX_WATER_ML, next_state.agua_ml + WATER_STEP_ML)
            actuators.append(_actuator("valvula", action, quantidade_ml=WATER_STEP_ML))
        case Action.RESETAR_AREIA:
            next_state.areia_usos = 0
        case Action.SILENCIAR_ALARME:
            next_state.alarme_ativo = False
            actuators.append(_actuator("alarme", action))
        case _:
            raise ValueError(f"Comando desconhecido: {action}")

    return next_state, actuators
