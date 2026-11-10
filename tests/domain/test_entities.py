from dataclasses import asdict
import hashlib
import json
import pytest

from src.domain.entities import Command, CommandResult, PetState, TelemetryEvent


def test_pet_state_creation():
    state = PetState(
        racao_gramas=180.0,
        agua_ml=420.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    assert state.racao_gramas == 180.0
    assert state.agua_ml == 420.0
    assert state.areia_usos == 2
    assert state.status_fuga == "DENTRO"
    assert state.alarme_ativo is False
    assert state.atualizado_em == "2026-11-10T15:00:00Z"


def test_pet_state_quorum_hash_ignores_timestamp_and_rounds_floats():
    state1 = PetState(
        racao_gramas=180.004,
        agua_ml=420.001,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    state2 = PetState(
        racao_gramas=180.001,
        agua_ml=420.003,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:05:00Z",
    )
    # Both round to 180.00 and 420.00, differ only in sub-cent floating or timestamps
    assert state1.state_hash() == state2.state_hash()


def test_pet_state_quorum_hash_detects_meaningful_differences():
    state1 = PetState(
        racao_gramas=180.0,
        agua_ml=420.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    state2 = PetState(
        racao_gramas=180.0,
        agua_ml=420.0,
        areia_usos=2,
        status_fuga="FORA",
        alarme_ativo=True,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    assert state1.state_hash() != state2.state_hash()


def test_telemetry_event_creation():
    event = TelemetryEvent(
        sensor="racao",
        payload={"nivel_gramas": 180.0},
        timestamp="2026-11-10T15:00:00Z",
    )
    assert event.sensor == "racao"
    assert event.payload["nivel_gramas"] == 180.0


def test_command_and_result_creation():
    cmd = Command(
        command_id="test-uuid-1",
        acao="LIBERAR_RACAO",
        parametros={"quantidade_gramas": 50.0},
        timestamp="2026-11-10T15:00:00Z",
    )
    assert cmd.command_id == "test-uuid-1"
    assert cmd.acao == "LIBERAR_RACAO"

    state = PetState(
        racao_gramas=230.0,
        agua_ml=420.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:01:00Z",
    )
    result = CommandResult(
        sucesso=True,
        mensagem="Racao liberada com sucesso",
        estado=state,
    )
    assert result.sucesso is True
    assert result.estado == state
