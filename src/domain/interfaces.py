from __future__ import annotations

from typing import Any, Callable, Optional, Protocol, runtime_checkable

from src.domain.entities import Command, CommandResult, PetState


@runtime_checkable
class IStateRepository(Protocol):
    """Abstract persistence contract for local controller state and event log."""

    def get_state(self) -> Optional[PetState]:
        """Retrieve the latest consolidated pet state."""
        ...

    def save_state(self, state: PetState) -> None:
        """Persist or update the consolidated pet state."""
        ...

    def record_event(
        self, command_id: str, tipo_evento: str, payload: str, registrado_em: str
    ) -> bool:
        """Record an event by idempotency key. Return True if new, False if duplicate."""
        ...

    def has_event(self, command_id: str) -> bool:
        """Check if an event key has already been processed."""
        ...


@runtime_checkable
class IBrokerClient(Protocol):
    """Abstract message broker contract for pub/sub operations."""

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        """Publish a message payload to a topic with specified QoS level."""
        ...

    def subscribe(
        self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1
    ) -> None:
        """Subscribe to a topic pattern and dispatch messages to the callback."""
        ...


@runtime_checkable
class IControllerNode(Protocol):
    """Abstract RPC contract exposed by controller replicas."""

    def get_state(self) -> PetState:
        """Retrieve local canonical PetState."""
        ...

    def execute_command(self, command: Command) -> CommandResult:
        """Execute a state-changing or actuation command idempotently."""
        ...

    def is_primary(self) -> bool:
        """Check if node currently acts as active leader for edge actuation."""
        ...

    def set_primary(self, primary: bool) -> None:
        """Update node leadership status."""
        ...
