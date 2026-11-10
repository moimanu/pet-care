from typing import Any, Callable, Optional
import pytest

from src.domain.entities import Command, CommandResult, PetState
from src.domain.interfaces import (
    BrokerClient,
    ControllerNode,
    IBrokerClient,
    IControllerNode,
    IStateRepository,
    StateRepository,
)


class DummyRepository:
    def __init__(self) -> None:
        self.state: Optional[PetState] = None
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


class DummyBroker:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any], int]] = []

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        self.messages.append((topic, payload, qos))

    def subscribe(self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1) -> None:
        pass


class DummyControllerNode:
    def __init__(self, primary: bool = False) -> None:
        self._primary = primary
        self._state = PetState()

    def get_state(self) -> PetState:
        return self._state

    def execute_command(self, command: Command) -> CommandResult:
        return CommandResult(sucesso=True, mensagem="ok", estado=self._state)

    def is_primary(self) -> bool:
        return self._primary

    def set_primary(self, primary: bool) -> None:
        self._primary = primary


def test_interfaces_runtime_checking():
    repo = DummyRepository()
    assert isinstance(repo, StateRepository)
    assert isinstance(repo, IStateRepository)

    broker = DummyBroker()
    assert isinstance(broker, BrokerClient)
    assert isinstance(broker, IBrokerClient)

    node = DummyControllerNode()
    assert isinstance(node, ControllerNode)
    assert isinstance(node, IControllerNode)
