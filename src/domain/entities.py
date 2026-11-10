from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class PetState:
    racao_gramas: float = 0.0
    agua_ml: float = 0.0
    areia_usos: int = 0
    status_fuga: str = "DENTRO"
    alarme_ativo: bool = False
    atualizado_em: str = ""

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> PetState:
        return cls(
            racao_gramas=float(data["racao_gramas"]),
            agua_ml=float(data["agua_ml"]),
            areia_usos=int(data["areia_usos"]),
            status_fuga=str(data["status_fuga"]),
            alarme_ativo=bool(data["alarme_ativo"]),
            atualizado_em=str(data["atualizado_em"]),
        )

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
