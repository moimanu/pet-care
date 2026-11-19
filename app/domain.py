from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json

SENSOR_LIMITS = {
    "racao": ("nivel_gramas", 0.0, 500.0),
    "agua": ("nivel_ml", 0.0, 1000.0),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class PetState:
    racao_gramas: float = 250.0
    agua_ml: float = 500.0
    areia_usos: int = 0
    status_fuga: str = "DENTRO"
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
        """Hash do estado (sem o timestamp), usado para comparar replicas."""
        core = {**self.to_dict(), "racao_gramas": round(self.racao_gramas, 2), "agua_ml": round(self.agua_ml, 2)}
        del core["atualizado_em"]
        raw = json.dumps(core, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:12]


def validate_telemetry(sensor: str, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload deve ser um objeto JSON")

    if sensor in SENSOR_LIMITS:
        field, low, high = SENSOR_LIMITS[sensor]
        val = payload.get(field)
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            raise ValueError(f"Campo '{field}' invalido: {val}")
        num = float(val)
        if not (low <= num <= high):
            raise ValueError(f"Valor fora dos limites [{low}, {high}]: {num}")
        return {field: num}

    if sensor == "areia":
        val = payload.get("usos_acumulados")
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise ValueError(f"Campo 'usos_acumulados' invalido: {val}")
        return {"usos_acumulados": val}

    if sensor == "fuga":
        val = payload.get("perimetro")
        if val not in ("DENTRO", "FORA"):
            raise ValueError(f"Campo 'perimetro' invalido: {val}")
        return {"perimetro": val}

    raise ValueError(f"Sensor desconhecido: {sensor}")


def _actuator(topic: str, **payload) -> dict:
    return {"topic": f"petcare/atuadores/{topic}", "payload": payload}


def apply_telemetry(state: PetState, sensor: str, payload: dict) -> tuple[PetState, list[dict]]:
    clean = validate_telemetry(sensor, payload)
    nxt = replace(state, atualizado_em=utc_now_iso())
    actuators: list[dict] = []

    if sensor == "racao":
        nxt.racao_gramas = clean["nivel_gramas"]
    elif sensor == "agua":
        nxt.agua_ml = clean["nivel_ml"]
    elif sensor == "areia":
        nxt.areia_usos = clean["usos_acumulados"]
    elif sensor == "fuga":
        nxt.status_fuga = clean["perimetro"]
        if nxt.status_fuga == "FORA":
            nxt.alarme_ativo = True
            actuators.append(_actuator("alarme", acao="DISPARAR_ALARME", motivo="FUGA_DETECTADA"))

    return nxt, actuators


def apply_command(state: PetState, action: str) -> tuple[PetState, list[dict]]:
    nxt = replace(state, atualizado_em=utc_now_iso())
    actuators: list[dict] = []

    if action == "LIBERAR_RACAO":
        nxt.racao_gramas = min(500.0, nxt.racao_gramas + 50.0)
        actuators.append(_actuator("dispenser", acao=action, quantidade_gramas=50.0))
    elif action == "REPOR_AGUA":
        nxt.agua_ml = min(1000.0, nxt.agua_ml + 200.0)
        actuators.append(_actuator("valvula", acao=action, quantidade_ml=200.0))
    elif action == "RESETAR_AREIA":
        nxt.areia_usos = 0
    elif action == "SILENCIAR_ALARME":
        nxt.alarme_ativo = False
        actuators.append(_actuator("alarme", acao=action))
    else:
        raise ValueError(f"Comando desconhecido: {action}")

    return nxt, actuators
