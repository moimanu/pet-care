import pytest

from src.domain.exceptions import NodeUnreachableError, QuorumError, SanityError
from src.domain.validators import TelemetrySanityValidator


def test_domain_exceptions_hierarchy():
    assert issubclass(SanityError, Exception)
    assert issubclass(QuorumError, Exception)
    assert issubclass(NodeUnreachableError, Exception)


def test_validator_accepts_valid_racao_telemetry():
    TelemetrySanityValidator.validate("racao", {"nivel_gramas": 0.0})
    TelemetrySanityValidator.validate("racao", {"nivel_gramas": 250.5})
    TelemetrySanityValidator.validate("racao", {"nivel_gramas": 500.0})


def test_validator_rejects_out_of_bounds_racao():
    with pytest.raises(SanityError, match="racao"):
        TelemetrySanityValidator.validate("racao", {"nivel_gramas": -0.1})

    with pytest.raises(SanityError, match="racao"):
        TelemetrySanityValidator.validate("racao", {"nivel_gramas": 500.1})


def test_validator_accepts_valid_agua_telemetry():
    TelemetrySanityValidator.validate("agua", {"nivel_ml": 0.0})
    TelemetrySanityValidator.validate("agua", {"nivel_ml": 500.0})
    TelemetrySanityValidator.validate("agua", {"nivel_ml": 1000.0})


def test_validator_rejects_out_of_bounds_agua():
    with pytest.raises(SanityError, match="agua"):
        TelemetrySanityValidator.validate("agua", {"nivel_ml": -1.0})

    with pytest.raises(SanityError, match="agua"):
        TelemetrySanityValidator.validate("agua", {"nivel_ml": 1000.5})


def test_validator_accepts_valid_areia_telemetry():
    TelemetrySanityValidator.validate("areia", {"usos_acumulados": 0})
    TelemetrySanityValidator.validate("areia", {"usos_acumulados": 15})


def test_validator_rejects_negative_areia():
    with pytest.raises(SanityError, match="areia"):
        TelemetrySanityValidator.validate("areia", {"usos_acumulados": -1})


def test_validator_accepts_valid_fuga_telemetry():
    TelemetrySanityValidator.validate("fuga", {"perimetro": "DENTRO"})
    TelemetrySanityValidator.validate("fuga", {"perimetro": "FORA"})


def test_validator_rejects_invalid_fuga_perimetro():
    with pytest.raises(SanityError, match="fuga"):
        TelemetrySanityValidator.validate("fuga", {"perimetro": "DESCONHECIDO"})


def test_validator_rejects_missing_payload_or_unknown_sensor():
    with pytest.raises(SanityError, match="sensor desconhecido"):
        TelemetrySanityValidator.validate("sensor_invalido", {})

    with pytest.raises(SanityError, match="campo ausente"):
        TelemetrySanityValidator.validate("racao", {})
