from __future__ import annotations

from typing import Any, Mapping

from src.domain.exceptions import SanityError


class TelemetrySanityValidator:
    """Validates physical sanity constraints on edge sensor telemetry."""

    MIN_RACAO_GRAMAS: float = 0.0
    MAX_RACAO_GRAMAS: float = 500.0
    MIN_AGUA_ML: float = 0.0
    MAX_AGUA_ML: float = 1000.0
    MIN_AREIA_USOS: int = 0
    VALID_PERIMETERS: frozenset[str] = frozenset({"DENTRO", "FORA"})

    _NUMERIC_RULES: dict[str, tuple[str, type, float, float]] = {
        "racao": ("nivel_gramas", float, MIN_RACAO_GRAMAS, MAX_RACAO_GRAMAS),
        "agua": ("nivel_ml", float, MIN_AGUA_ML, MAX_AGUA_ML),
        "areia": ("usos_acumulados", int, MIN_AREIA_USOS, float("inf")),
    }

    @classmethod
    def validate(cls, sensor: str, payload: Mapping[str, Any]) -> None:
        key = sensor.strip().lower()

        if key == "fuga":
            if "perimetro" not in payload:
                raise SanityError("campo ausente: 'perimetro' obrigatorio para telemetria de fuga")
            perimetro = str(payload["perimetro"]).strip().upper()
            if perimetro not in cls.VALID_PERIMETERS:
                raise SanityError(
                    f"status de fuga invalido '{perimetro}'. Valores esperados: {sorted(cls.VALID_PERIMETERS)}"
                )
            return

        if key not in cls._NUMERIC_RULES:
            raise SanityError(f"sensor desconhecido: '{sensor}'")

        field_name, field_type, min_val, max_val = cls._NUMERIC_RULES[key]
        if field_name not in payload:
            raise SanityError(f"campo ausente: '{field_name}' obrigatorio para telemetria de {key}")

        try:
            val = field_type(payload[field_name])
        except (TypeError, ValueError) as exc:
            raise SanityError(f"valor invalido para {key}: {exc}") from exc

        if not (min_val <= val <= max_val):
            bounds = f"[{min_val}, {max_val}]" if max_val != float("inf") else f">= {min_val}"
            raise SanityError(f"nivel de {key} fora dos limites permitidos {bounds}: {val}")
