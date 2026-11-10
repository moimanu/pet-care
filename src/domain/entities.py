from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Optional


@dataclass(frozen=True)
class PetState:
    racao_gramas: float
    agua_ml: float
    areia_usos: int
    status_fuga: str
    alarme_ativo: bool
    atualizado_em: str

    def state_hash(self) -> str:
        canonical_dict = {
            "racao_gramas": round(float(self.racao_gramas), 2),
            "agua_ml": round(float(self.agua_ml), 2),
            "areia_usos": int(self.areia_usos),
            "status_fuga": str(self.status_fuga).strip().upper(),
            "alarme_ativo": bool(self.alarme_ativo),
        }
        encoded = json.dumps(canonical_dict, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class TelemetryEvent:
    sensor: str
    payload: dict[str, Any]
    timestamp: str


@dataclass(frozen=True)
class Command:
    command_id: str
    acao: str
    parametros: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""


@dataclass(frozen=True)
class CommandResult:
    sucesso: bool
    mensagem: str
    estado: Optional[PetState] = None
