import time
from typing import Optional
import pytest

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError, QuorumError
from src.use_cases.cluster_client_service import ClusterClientService


class MockControllerNode:
    def __init__(
        self,
        node_id: int,
        state: PetState,
        is_primary: bool = False,
        delay: float = 0.0,
        fail: bool = False,
        corrupt: bool = False,
    ) -> None:
        self.node_id = node_id
        self._state = state
        self._primary = is_primary
        self.delay = delay
        self.fail = fail
        self.corrupt = corrupt
        self.commands_received: list[Command] = []
        self.synced_states: list[tuple[PetState, Optional[str]]] = []

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

    def sync_state(
        self,
        state: PetState,
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> bool:
        if self.fail:
            raise NodeUnreachableError(f"Node {self.node_id} failed during sync_state")
        self._state = state
        self.synced_states.append((state, command_id))
        return True

    def is_primary(self) -> bool:
        return self._primary

    def set_primary(self, primary: bool) -> None:
        self._primary = primary

    def is_offline(self) -> bool:
        return self.fail

    def set_offline(self, offline: bool) -> None:
        self.fail = offline

    def is_corrupted(self) -> bool:
        return self.corrupt

    def set_corrupt(self, corrupt: bool) -> None:
        self.corrupt = corrupt

    def get_info(self) -> dict:
        return {
            "id": self.node_id,
            "primary": self._primary,
            "corrupt": self.corrupt,
            "offline": self.fail,
        }


def test_cluster_client_byzantine_quorum_resolves_majority():
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
    node3 = MockControllerNode(3, corrupted_state, is_primary=False, corrupt=True)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})

    result_state = client.get_consensus_state()
    assert result_state.status_fuga == "DENTRO"
    assert result_state.alarme_ativo is False
    assert result_state.state_hash() == canonical_state.state_hash()


def test_cluster_client_demotes_byzantine_leader_on_quorum_divergence():
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

    # Node 1 is Leader but returns Byzantine corrupted state
    node1 = MockControllerNode(1, corrupted_state, is_primary=True, corrupt=True)
    node2 = MockControllerNode(2, canonical_state, is_primary=False)
    node3 = MockControllerNode(3, canonical_state, is_primary=False)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})
    result_state = client.get_consensus_state()

    assert result_state.status_fuga == "DENTRO"
    assert node1.is_primary() is False
    assert node2.is_primary() is True


def test_cluster_client_byzantine_quorum_fails_when_majority_unreachable():
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

    client = ClusterClientService({1: node1, 2: node2, 3: node3})

    with pytest.raises(QuorumError, match="Quorum"):
        client.get_consensus_state()


def test_cluster_client_dispatches_command_to_leader_and_syncs_state():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    node1 = MockControllerNode(1, canonical_state, is_primary=True)
    node2 = MockControllerNode(2, PetState(), is_primary=False)
    node3 = MockControllerNode(3, PetState(), is_primary=False)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})

    result = client.dispensar_racao(50.0)
    assert result.sucesso is True

    # Leader executed the command and replicas received canonical state sync with the same command_id
    assert len(node1.commands_received) == 1
    cmd_id = node1.commands_received[0].command_id
    assert len(cmd_id) > 0
    assert node1.commands_received[0].acao == "LIBERAR_RACAO"
    assert len(node2.synced_states) == 1
    assert len(node3.synced_states) == 1
    assert node2.synced_states[0][1] == cmd_id
    assert node3.synced_states[0][1] == cmd_id


def test_cluster_client_failover_transparently_promotes_secondary_on_primary_crash():
    canonical_state = PetState(
        racao_gramas=200.0,
        agua_ml=500.0,
        areia_usos=2,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    node1 = MockControllerNode(1, canonical_state, is_primary=True, fail=True)
    node2 = MockControllerNode(2, canonical_state, is_primary=False, fail=False)
    node3 = MockControllerNode(3, canonical_state, is_primary=False, fail=False)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})

    result = client.repor_agua(200.0)
    assert result.sucesso is True

    assert node2.is_primary() is True
    assert len(node2.commands_received) == 1
    assert len(node3.synced_states) == 1
    assert node2.commands_received[0].command_id == node3.synced_states[0][1]


def test_recovered_node_does_not_preempt_active_leader_and_clones_state():
    leader_state = PetState(
        racao_gramas=350.0,
        agua_ml=700.0,
        areia_usos=0,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    stale_state = PetState(racao_gramas=50.0, agua_ml=100.0)

    node1 = MockControllerNode(1, stale_state, is_primary=True, fail=True)
    node2 = MockControllerNode(2, leader_state, is_primary=False, fail=False)
    node3 = MockControllerNode(3, leader_state, is_primary=False, fail=False)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})

    # Trigger failover - Node 2 is promoted to primary
    client.repor_agua(200.0)
    assert node2.is_primary() is True

    # Node 1 returns online (recovery)
    client.set_node_offline(1, False)

    # Invariant: Node 1 must NOT preempt Node 2, and its storage must sync with Node 2's canonical state
    assert node2.is_primary() is True
    assert node1.is_primary() is False
    assert node1.get_state() == leader_state

    prim_id, prim_node = client.get_primary_node()
    assert prim_id == 2
    assert prim_node is node2

