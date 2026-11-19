import tempfile
import threading
import time
import Pyro5.api

from src.adapters.pyro_gateway import PyroControllerNodeProxy, PyroControllerNodeWrapper
from src.domain.entities import PetState
from src.entrypoints.run_controller import build_controller
from src.use_cases.cluster_client_service import ClusterClientService
from src.use_cases.controller_service import ControllerService


class DummyBroker:
    def __init__(self):
        self.subs = []

    def publish(self, topic, payload, qos=1):
        pass

    def subscribe(self, topic, callback, qos=1):
        self.subs.append((topic, callback))


class DummyNode:
    def __init__(self, primary=False):
        self._primary = primary
        self._state = PetState(
            racao_gramas=100.0,
            agua_ml=250.0,
            areia_usos=1,
            status_fuga="DENTRO",
            alarme_ativo=False,
            atualizado_em="2026-11-10T15:00:00Z",
        )

    def get_state(self):
        return self._state

    def execute_command(self, cmd):
        from src.domain.entities import CommandResult
        return CommandResult(sucesso=True, mensagem="ok", estado=self._state)

    def sync_state(self, state, command_id=None, tipo_evento="", payload=""):
        self._state = state
        return True

    def is_primary(self):
        return self._primary

    def set_primary(self, primary):
        self._primary = primary


def test_build_controller_initializes_service_and_subscribes():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        broker = DummyBroker()
        controller = build_controller(
            node_id=1,
            is_primary=True,
            is_corrupt=False,
            db_path=tmp.name,
            broker_client=broker,
            peer_proxies=[],
        )
        assert isinstance(controller, ControllerService)
        assert controller.node_id == 1
        assert controller.is_primary() is True
        assert len(broker.subs) == 1
        assert broker.subs[0][0] == "petcare/sensores/#"


def test_cluster_client_service_status_and_commands():
    node1 = DummyNode(primary=True)
    node2 = DummyNode(primary=False)
    node3 = DummyNode(primary=False)

    client = ClusterClientService({1: node1, 2: node2, 3: node3})
    status_res = client.get_pet_status()
    assert status_res["sucesso"] is True
    status = status_res["estado"]
    assert status["racao_gramas"] == 100.0
    assert status["status_fuga"] == "DENTRO"

    res = client.dispensar_racao(50.0)
    assert res.sucesso is True


def test_full_real_decentralized_pyro_pipeline(tmp_path):
    p1 = PyroControllerNodeProxy("PYRO:c1@localhost:19881")
    p2 = PyroControllerNodeProxy("PYRO:c2@localhost:19882")
    p3 = PyroControllerNodeProxy("PYRO:c3@localhost:19883")

    c1 = build_controller(
        1,
        is_primary=True,
        db_path=str(tmp_path / "n1.db"),
        broker_client=DummyBroker(),
        peer_proxies=[p2, p3],
    )
    c2 = build_controller(
        2,
        is_primary=False,
        db_path=str(tmp_path / "n2.db"),
        broker_client=DummyBroker(),
        peer_proxies=[p1, p3],
    )
    c3 = build_controller(
        3,
        is_primary=False,
        db_path=str(tmp_path / "n3.db"),
        broker_client=DummyBroker(),
        peer_proxies=[p1, p2],
    )

    d1 = Pyro5.api.Daemon(port=19881)
    d1.register(PyroControllerNodeWrapper(c1), "c1")
    d2 = Pyro5.api.Daemon(port=19882)
    d2.register(PyroControllerNodeWrapper(c2), "c2")
    d3 = Pyro5.api.Daemon(port=19883)
    d3.register(PyroControllerNodeWrapper(c3), "c3")

    threading.Thread(target=d1.requestLoop, daemon=True).start()
    threading.Thread(target=d2.requestLoop, daemon=True).start()
    threading.Thread(target=d3.requestLoop, daemon=True).start()
    time.sleep(0.1)

    try:
        client = ClusterClientService({1: p1, 2: p2, 3: p3})
        status = client.get_pet_status()
        assert status["sucesso"] is True

        # Execute command on Primary (c1); c1 replicates canonical state directly to c2 and c3 over Pyro5 P2P
        res = client.dispensar_racao(50.0)
        assert res.sucesso is True
        assert c1.get_state().racao_gramas == 50.0
        assert c2.get_state().racao_gramas == 50.0
        assert c3.get_state().racao_gramas == 50.0
    finally:
        d1.close()
        d2.close()
        d3.close()
