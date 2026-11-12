import os
import tempfile
import pytest

from src.domain.entities import PetState
from src.domain.interfaces import StateRepository
from src.adapters.sqlite_repository import SqliteRepository


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


def test_sqlite_repository_implements_interface(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    assert isinstance(repo, StateRepository)


def test_sqlite_repository_returns_none_when_empty(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    assert repo.get_state() is None


def test_sqlite_repository_saves_and_retrieves_state(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    state = PetState(
        racao_gramas=250.0,
        agua_ml=600.0,
        areia_usos=3,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:10:00Z",
    )
    repo.save_state(state)

    retrieved = repo.get_state()
    assert retrieved is not None
    assert retrieved.racao_gramas == 250.0
    assert retrieved.agua_ml == 600.0
    assert retrieved.areia_usos == 3
    assert retrieved.status_fuga == "DENTRO"
    assert retrieved.alarme_ativo is False
    assert retrieved.atualizado_em == "2026-11-10T15:10:00Z"


def test_sqlite_repository_updates_existing_singleton_state(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    initial_state = PetState(
        racao_gramas=100.0,
        agua_ml=200.0,
        areia_usos=1,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    repo.save_state(initial_state)

    updated_state = PetState(
        racao_gramas=80.0,
        agua_ml=150.0,
        areia_usos=2,
        status_fuga="FORA",
        alarme_ativo=True,
        atualizado_em="2026-11-10T15:15:00Z",
    )
    repo.save_state(updated_state)

    current = repo.get_state()
    assert current == updated_state


def test_sqlite_repository_records_event_with_idempotency(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    assert not repo.has_event("cmd-100")

    recorded_first = repo.record_event(
        command_id="cmd-100",
        tipo_evento="LIBERAR_RACAO",
        payload='{"quantidade_gramas": 50.0}',
        registrado_em="2026-11-10T15:10:00Z",
    )
    assert recorded_first is True
    assert repo.has_event("cmd-100")

    recorded_second = repo.record_event(
        command_id="cmd-100",
        tipo_evento="LIBERAR_RACAO",
        payload='{"quantidade_gramas": 50.0}',
        registrado_em="2026-11-10T15:10:05Z",
    )
    assert recorded_second is False


def test_sqlite_repository_tracks_multiple_distinct_events(temp_db_path):
    repo = SqliteRepository(temp_db_path)
    repo.record_event("cmd-1", "REPOR_AGUA", "{}", "2026-11-10T15:10:00Z")
    repo.record_event("cmd-2", "DISPARAR_ALARME", "{}", "2026-11-10T15:11:00Z")

    assert repo.has_event("cmd-1")
    assert repo.has_event("cmd-2")
    assert not repo.has_event("cmd-3")

