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


def test_secondary_replica_does_not_publish_actuation_to_broker():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=2, repository=repo, broker=broker, is_primary=False)

    service.handle_telemetry("petcare/sensores/racao", {"nivel_gramas": 20.0, "timestamp": "2026-11-10T15:00:00Z"})
    service.handle_telemetry("petcare/sensores/agua", {"nivel_ml": 50.0, "timestamp": "2026-11-10T15:01:00Z"})
    service.handle_telemetry("petcare/sensores/fuga", {"perimetro": "FORA", "timestamp": "2026-11-10T15:02:00Z"})

    # Broker must receive zero messages from secondary node
    assert len(broker.published) == 0
    # But local state is updated
    state = repo.get_state()
    assert state.racao_gramas == 20.0
    assert state.agua_ml == 50.0
    assert state.status_fuga == "FORA"
    assert state.alarme_ativo is True


def test_controller_executes_command_idempotently():
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    # Initial state
    initial_state = PetState(
        racao_gramas=100.0,
        agua_ml=200.0,
        areia_usos=5,
        status_fuga="FORA",
        alarme_ativo=True,
        atualizado_em="2026-11-10T15:00:00Z",
    )
    repo.save_state(initial_state)

    from src.domain.entities import Command
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

        def execute_command(self, cmd):
            pass

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


def test_controller_service_implements_icontroller_node():
    from src.domain.interfaces import IControllerNode
    repo = MockRepository()
    broker = MockBroker()
    service = ControllerService(node_id=1, repository=repo, broker=broker)
    assert isinstance(service, IControllerNode)



