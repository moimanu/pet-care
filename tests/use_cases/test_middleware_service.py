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


def test_middleware_dispatches_command_with_uuid_and_replicates():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    node1 = MockControllerNode(1, canonical_state, is_primary=True)
    node2 = MockControllerNode(2, canonical_state, is_primary=False)
    node3 = MockControllerNode(3, canonical_state, is_primary=False)

    middleware = MiddlewareService({1: node1, 2: node2, 3: node3})

    result = middleware.dispensar_racao(50.0)
    assert result.sucesso is True

    # Check all reachable nodes received the exact same command_id
    assert len(node1.commands_received) == 1
    assert len(node2.commands_received) == 1
    assert len(node3.commands_received) == 1

    cmd_id_1 = node1.commands_received[0].command_id
    cmd_id_2 = node2.commands_received[0].command_id
    cmd_id_3 = node3.commands_received[0].command_id

    assert cmd_id_1 == cmd_id_2 == cmd_id_3
    assert len(cmd_id_1) > 0
    assert node1.commands_received[0].acao == "LIBERAR_RACAO"


def test_middleware_failover_transparently_promotes_secondary_on_primary_crash():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    # Node 1 is primary but fails
    node1 = MockControllerNode(1, canonical_state, is_primary=True, fail=True)
    node2 = MockControllerNode(2, canonical_state, is_primary=False, fail=False)
    node3 = MockControllerNode(3, canonical_state, is_primary=False, fail=False)

    middleware = MiddlewareService({1: node1, 2: node2, 3: node3})

    result = middleware.repor_agua(200.0)
    assert result.sucesso is True

    # Node 2 must have been promoted to primary
    assert node2.is_primary() is True
    # Node 2 and Node 3 received the command
    assert len(node2.commands_received) == 1
    assert len(node3.commands_received) == 1
    assert node2.commands_received[0].command_id == node3.commands_received[0].command_id


