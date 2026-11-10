import os
import tempfile
import pytest

from src.domain.entities import PetState
from src.entrypoints.run_controller import build_controller
from src.entrypoints.run_middleware import PyroMiddlewareWrapper, build_middleware
from src.use_cases.controller_service import ControllerService
from src.use_cases.middleware_service import MiddlewareService


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


def test_build_middleware_and_wrapper():
    node1 = DummyNode(primary=True)
    node2 = DummyNode(primary=False)
    node3 = DummyNode(primary=False)

    middleware = build_middleware({1: node1, 2: node2, 3: node3})
    assert isinstance(middleware, MiddlewareService)

    wrapper = PyroMiddlewareWrapper(middleware)
    status = wrapper.get_pet_status()
    assert status["racao_gramas"] == 100.0
    assert status["status_fuga"] == "DENTRO"

    res = wrapper.dispensar_racao(50.0)
    assert res["sucesso"] is True


def test_client_cli_methods():
    from src.entrypoints.run_client_cli import ClientCLI

    mock_middleware = DummyNode(primary=True)
    cli_middleware = PyroMiddlewareWrapper(build_middleware({1: mock_middleware, 2: mock_middleware}))
    cli = ClientCLI(cli_middleware)

    assert cli.handle_option("1") is True
    assert cli.handle_option("4") is True
    assert cli.handle_option("5") is True
    assert cli.handle_option("0") is False

