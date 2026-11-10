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

    @classmethod
    def validate(cls, sensor: str, payload: Mapping[str, Any]) -> None:
        sensor_key = sensor.strip().lower()
        if sensor_key == "racao":
            cls._validate_racao(payload)
        elif sensor_key == "agua":
            cls._validate_agua(payload)
        elif sensor_key == "areia":
            cls._validate_areia(payload)
        elif sensor_key == "fuga":
            cls._validate_fuga(payload)
        else:
            raise SanityError(f"sensor desconhecido: '{sensor}'")

    @classmethod
    def _validate_racao(cls, payload: Mapping[str, Any]) -> None:
        if "nivel_gramas" not in payload:
            raise SanityError("campo ausente: 'nivel_gramas' obrigatorio para telemetria de racao")
        try:
            val = float(payload["nivel_gramas"])
        except (TypeError, ValueError) as exc:
            raise SanityError(f"valor invalido para racao: {exc}") from exc

        if not (cls.MIN_RACAO_GRAMAS <= val <= cls.MAX_RACAO_GRAMAS):
            raise SanityError(
                f"nivel de racao fora dos limites permitidos [{cls.MIN_RACAO_GRAMAS}, {cls.MAX_RACAO_GRAMAS}]: {val}"
            )

    @classmethod
    def _validate_agua(cls, payload: Mapping[str, Any]) -> None:
        if "nivel_ml" not in payload:
            raise SanityError("campo ausente: 'nivel_ml' obrigatorio para telemetria de agua")
        try:
            val = float(payload["nivel_ml"])
        except (TypeError, ValueError) as exc:
            raise SanityError(f"valor invalido para agua: {exc}") from exc

        if not (cls.MIN_AGUA_ML <= val <= cls.MAX_AGUA_ML):
            raise SanityError(
                f"nivel de agua fora dos limites permitidos [{cls.MIN_AGUA_ML}, {cls.MAX_AGUA_ML}]: {val}"
            )

    @classmethod
    def _validate_areia(cls, payload: Mapping[str, Any]) -> None:
        if "usos_acumulados" not in payload:
            raise SanityError("campo ausente: 'usos_acumulados' obrigatorio para telemetria de areia")
        try:
            val = int(payload["usos_acumulados"])
        except (TypeError, ValueError) as exc:
            raise SanityError(f"valor invalido para areia: {exc}") from exc

        if val < cls.MIN_AREIA_USOS:
            raise SanityError(
                f"usos acumulados de areia nao podem ser negativos: {val}"
            )

    @classmethod
    def _validate_fuga(cls, payload: Mapping[str, Any]) -> None:
        if "perimetro" not in payload:
            raise SanityError("campo ausente: 'perimetro' obrigatorio para telemetria de fuga")
        perimetro = str(payload["perimetro"]).strip().upper()
        if perimetro not in cls.VALID_PERIMETERS:
            raise SanityError(
                f"status de fuga invalido '{perimetro}'. Valores esperados: {sorted(cls.VALID_PERIMETERS)}"
            )
