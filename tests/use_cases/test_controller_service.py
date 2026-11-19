from typing import Any, Callable, Optional
import pytest

from src.domain.entities import Command, PetState
from src.domain.interfaces import ControllerNode
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
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

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
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

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


def test_primary_controller_triggers_autonomous_actuation():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    # Low food triggers dispenser
    service.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 30.0, "timestamp": "2026-11-10T15:00:00Z"})
    assert len(broker.published) == 1
    topic, payload, qos = broker.published[0]
    assert topic == "petcare/atuadores/dispenser"
    assert payload["acao"] == "LIBERAR_RACAO"
    assert payload["quantidade_gramas"] == 50.0
    assert qos == 2

    # Low water triggers valve
    service.handle_telemetry("petcare/sensores/agua", {"nivel_ml": 100.0, "timestamp": "2026-11-10T15:01:00Z"})
    assert len(broker.published) == 2
    topic, payload, qos = broker.published[1]
    assert topic == "petcare/atuadores/valvula"
    assert payload["acao"] == "REPOR_AGUA"
    assert payload["quantidade_ml"] == 200.0
    assert qos == 2

    # Fuga FORA triggers alarm and activates alarm in state
    service.handle_telemetry("petcare/sensores/fuga", {"perimetro": "FORA", "timestamp": "2026-11-10T15:02:00Z"})
    assert len(broker.published) == 3
    topic, payload, qos = broker.published[2]
    assert topic == "petcare/atuadores/alarme"
    assert payload["acao"] == "DISPARAR_ALARME"
    assert payload["motivo"] == "FUGA_DETECTADA"
    assert repo.get_state().alarme_ativo is True


def test_secondary_replica_ignores_direct_mqtt_and_receives_canonical_state_from_leader():
    repo1, repo2, repo3 = MockRepository(), MockRepository(), MockRepository()
    broker1, broker2, broker3 = MockBroker(), MockBroker(), MockBroker()

    # Pre-populate node 2 with a drifted/stale state to verify canonical overwrite
    repo2.save_state(PetState(racao_gramas=10.0, agua_ml=10.0, areia_usos=99))

    c1 = ControllerService(node_id=1, repository=repo1, broker=broker1, is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=broker2, is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=broker3, is_primary=False)

    c1.peers = [c2, c3]
    c2.peers = [c1, c3]
    c3.peers = [c1, c2]

    # Secondary replica receives MQTT message first -> must ignore direct write while leader is active
    c2.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 20.0, "timestamp": "2026-11-10T15:00:00Z"})
    assert len(broker2.published) == 0
    assert repo2.get_state().racao_gramas == 10.0  # Unchanged prior to leader's sync_state

    # Primary processes telemetry -> updates local DB, actuates, and replicates canonical snapshot via sync_state
    c1.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 20.0, "timestamp": "2026-11-10T15:00:00Z"})
    assert len(broker1.published) == 1
    assert repo1.get_state() == repo2.get_state() == repo3.get_state()
    assert repo2.get_state().racao_gramas == 20.0
    assert repo2.get_state().areia_usos == 0  # Stale drift was overwritten by leader's canonical state!


def test_primary_replicates_canonical_state_to_divergent_replicas_on_command():
    repo1 = MockRepository(PetState(racao_gramas=100.0, agua_ml=300.0))
    # Replica 2 has drifted state (e.g. missed an earlier update)
    repo2 = MockRepository(PetState(racao_gramas=15.0, agua_ml=50.0))
    repo3 = MockRepository(PetState(racao_gramas=100.0, agua_ml=300.0))

    c1 = ControllerService(node_id=1, repository=repo1, broker=MockBroker(), is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=MockBroker(), is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=MockBroker(), is_primary=False)
    c1.peers = [c2, c3]

    cmd = Command(
        command_id="cmd-food-50",
        acao="LIBERAR_RACAO",
        parametros={"quantidade_gramas": 50.0},
        timestamp="2026-11-10T15:05:00Z",
    )
    res = c1.execute_command(cmd)
    assert res.sucesso is True

    # All storages must match the Leader's exact canonical state (150.0g, 300.0ml) and event_log
    assert repo1.get_state().racao_gramas == 150.0
    assert repo2.get_state() == repo1.get_state()
    assert repo3.get_state() == repo1.get_state()
    assert repo2.has_event("cmd-food-50")
    assert repo3.has_event("cmd-food-50")


def test_proactive_p2p_failover_on_leader_crash_stop_and_telemetry():
    repo1, repo2, repo3 = MockRepository(), MockRepository(), MockRepository()
    broker1, broker2, broker3 = MockBroker(), MockBroker(), MockBroker()

    c1 = ControllerService(node_id=1, repository=repo1, broker=broker1, is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=broker2, is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=broker3, is_primary=False)
    c1.peers = [c2, c3]
    c2.peers = [c1, c3]
    c3.peers = [c1, c2]

    # Simulate crash-stop on Primary (Node 1)
    c1.set_offline(True)
    assert c1.is_primary() is False
    assert c2.is_primary() is True
    assert c3.is_primary() is False

    # Now Node 2 processes telemetry as the new Leader and replicates state to Node 3
    c2.handle_telemetry("petcare/sensores/agua", {"nivel_ml": 100.0, "timestamp": "2026-11-10T15:10:00Z"})
    assert len(broker2.published) == 1
    assert repo2.get_state().agua_ml == 100.0
    assert repo3.get_state() == repo2.get_state()


def test_byzantine_corruption_on_leader_isolated_and_transfers_leadership():
    initial = PetState(racao_gramas=200.0, agua_ml=400.0, status_fuga="DENTRO", alarme_ativo=False)
    repo1, repo2, repo3 = MockRepository(initial), MockRepository(initial), MockRepository(initial)

    c1 = ControllerService(node_id=1, repository=repo1, broker=MockBroker(), is_primary=True)
    c2 = ControllerService(node_id=2, repository=repo2, broker=MockBroker(), is_primary=False)
    c3 = ControllerService(node_id=3, repository=repo3, broker=MockBroker(), is_primary=False)
    c1.peers = [c2, c3]
    c2.peers = [c1, c3]
    c3.peers = [c1, c2]

    # Corrupting Node 1 (Leader) must automatically demote it and promote Node 2
    c1.set_corrupt(True)
    assert c1.is_primary() is False
    assert c2.is_primary() is True
    assert c2.get_state().status_fuga == "DENTRO"
    assert c3.get_state().status_fuga == "DENTRO"


def test_controller_executes_command_idempotently():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    initial_state = PetState(
        racao_gramas=100.0,
        agua_ml=200.0,
        areia_usos=5,
        status_fuga="FORA",
        alarme_ativo=True,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    repo.save_state(initial_state)

    cmd_silenciar = Command(
        command_id="cmd-uuid-1",
        acao="SILENCIAR_ALARME",
        parametros={},
        timestamp="2026-11-10T15:05:00Z",
    )

    result1 = service.execute_command(cmd_silenciar)
    assert result1.sucesso is True
    assert result1.estado.alarme_ativo is False
    assert repo.has_event("cmd-uuid-1")

    # Duplicate execution returns idempotent success without reprocessing
    result2 = service.execute_command(cmd_silenciar)
    assert result2.sucesso is True
    assert "idempotente" in result2.mensagem.lower()


def test_controller_sync_boot_state_from_peers_when_local_empty():
    repo = MockRepository(initial_state=None)
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker)

    class PeerMockNode:
        def __init__(self, state: PetState):
            self._state = state

        def get_state(self) -> PetState:
            return self._state

        def is_primary(self):
            return False

        def set_primary(self, primary):
            pass

    peer_state = PetState(
        racao_gramas=250.0,
        agua_ml=500.0,
        areia_usos=1,
        status_fuga="DENTRO",
        alarme_ativo=False,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    peer = PeerMockNode(peer_state)

    service.sync_boot_state([peer])
    assert repo.get_state() == peer_state


def test_controller_yields_leadership_on_boot_if_peer_already_primary():
    repo = MockRepository(initial_state=PetState())
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    class PrimaryPeerMock:
        def get_state(self) -> PetState:
            return PetState()

        def is_primary(self) -> bool:
            return True

    peer = PrimaryPeerMock()
    service.sync_boot_state([peer])
    assert service.is_primary() is False


def test_controller_service_implements_controller_node():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker)
    assert isinstance(service, ControllerNode)
