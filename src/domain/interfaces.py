from __future__ import annotations

from typing import Any, Callable, Optional, Protocol, runtime_checkable

from src.domain.entities import Command, CommandResult, PetState


@runtime_checkable
class StateRepository(Protocol):
    """Persistence contract for local controller state and event log."""

    def get_state(self) -> Optional[PetState]:
        ...

    def save_state(self, state: PetState) -> None:
        ...

    def record_event(
        self, command_id: str, tipo_evento: str, payload: str, registrado_em: str
    ) -> bool:
        ...

    def has_event(self, command_id: str) -> bool:
        ...


@runtime_checkable
class BrokerClient(Protocol):
    """Message broker contract for pub/sub operations."""

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        ...

    def subscribe(
        self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1
    ) -> None:
        ...


@runtime_checkable
class ControllerNode(Protocol):
    """RPC contract exposed by controller replicas."""

    def get_state(self) -> PetState:
        ...

    def execute_command(self, command: Command) -> CommandResult:
        ...

    def is_primary(self) -> bool:
        ...

    def set_primary(self, primary: bool) -> None:
        ...


# Backwards compatibility aliases during refactor
IStateRepository = StateRepository
IBrokerClient = BrokerClient
IControllerNode = ControllerNode
