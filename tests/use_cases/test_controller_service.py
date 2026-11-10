from typing import Any, Callable, Optional
import pytest

from src.domain.entities import PetState
from src.domain.interfaces import IBrokerClient, IStateRepository
from src.use_cases.controller_service import ControllerService


class MockRepository:
    def __init__(self, initial_state: Optional[PetState] = None) -> None:
        self.state: Optional[PetState] = initial_state
        self.events: dict[str, dict[str, str]] = {}

    def get_state(self) -> Optional[PetState]:
        return self.state

    def save_state(self, state: PetState) -> None:
        self.state = state

    def record_event(self, command_id: str, tipo_evento: str, payload: str, registrado_em: str) -> bool:
        if command_id in self.events:
            return False
        self.events[command_id] = {
            "tipo_evento": tipo_evento,
            "payload": payload,
            "registrado_em": registrado_em,
        }
        return True

    def has_event(self, command_id: str) -> bool:
        return command_id in self.events


class MockBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any], int]] = []
        self.subscriptions: dict[str, Callable[[str, dict[str, Any]], None]] = {}

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        self.published.append((topic, payload, qos))

    def subscribe(self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1) -> None:
        self.subscriptions[topic] = callback


def test_controller_handles_valid_telemetry_and_persists_state():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker)

    service.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 180.0, "timestamp": "2026-11-10T15:00:00Z"})

    state = repo.get_state()
    assert state is not None
    assert state.racao_gramas == 180.0

    service.handle_telemetry("petcare/sensores/agua", {"nivel_ml": 420.0, "timestamp": "2026-11-10T15:01:00Z"})
    state = repo.get_state()
    assert state.agua_ml == 420.0
    assert state.racao_gramas == 180.0


def test_controller_discards_invalid_telemetry_without_db_modification():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker)

    service.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 200.0, "timestamp": "2026-11-10T15:00:00Z"})
    saved_state = repo.get_state()
    assert saved_state is not None
    assert saved_state.racao_gramas == 200.0

    # Negative water level violates sanity constraint
    service.handle_telemetry("petcare/sensores/agua", {"nivel_ml": -50.0, "timestamp": "2026-11-10T15:01:00Z"})

    # State in DB must remain unmodified
    current_state = repo.get_state()
    assert current_state.agua_ml == saved_state.agua_ml
    assert current_state.racao_gramas == 200.0
