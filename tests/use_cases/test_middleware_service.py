import time
import pytest

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, QuorumError
from src.use_cases.middleware_service import MiddlewareService


class MockControllerNode:
    def __init__(self, node_id: int, state: PetState, is_primary: bool = False, delay: float = 0.0, fail: bool = False) -> None:
        self.node_id = node_id
        self._state = state
        self._primary = is_primary
        self.delay = delay
        self.fail = fail
        self.commands_received: list[Command] = []

    def get_state(self) -> PetState:
        if self.fail:
            raise NodeUnreachableError(f"Node {self.node_id} is unreachable")
        if self.delay > 0:
            time.sleep(self.delay)
        return self._state

    def execute_command(self, command: Command) -> CommandResult:
        if self.fail:
            raise NodeUnreachableError(f"Node {self.node_id} failed during command")
        self.commands_received.append(command)
        return CommandResult(sucesso=True, mensagem="ok", estado=self._state)

    def is_primary(self) -> bool:
        return self._primary

    def set_primary(self, primary: bool) -> None:
        self._primary = primary


def test_middleware_byzantine_quorum_resolves_majority():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    corrupted_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="FORA",
        alarme_ativo=True,
        atualizado_em="2026-11-10T15:00:00Z",
    )

    node1 = MockControllerNode(1, canonical_state, is_primary=True)
    node2 = MockControllerNode(2, canonical_state, is_primary=False)
    node3 = MockControllerNode(3, corrupted_state, is_primary=False)  # Byzantine divergent

    middleware = MiddlewareService({1: node1, 2: node2, 3: node3})

    result_state = middleware.get_consensus_state()
    assert result_state.status_fuga == "DENTRO"
    assert result_state.alarme_ativo is False
    assert result_state.state_hash() == canonical_state.state_hash()


def test_middleware_byzantine_quorum_fails_when_majority_unreachable():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    node1 = MockControllerNode(1, canonical_state, fail=True)
    node2 = MockControllerNode(2, canonical_state, fail=True)
    node3 = MockControllerNode(3, canonical_state, fail=False)

    middleware = MiddlewareService({1: node1, 2: node2, 3: node3})

    with pytest.raises(QuorumError, match="Quorum"):
        middleware.get_consensus_state()
