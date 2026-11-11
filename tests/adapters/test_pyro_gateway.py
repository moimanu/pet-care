from unittest.mock import MagicMock
import pytest

from src.adapters.pyro_gateway import PyroControllerNodeProxy, PyroControllerNodeWrapper
from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError
from src.domain.interfaces import IControllerNode


class DummyNode:
    def __init__(self):
        self._primary = True
        self._state = PetState(
            racao_gramas=150.0,
            agua_ml=300.0,
            areia_usos=1,
            status_fuga="DENTRO",
            alarme_ativo=False,
            atualizado_em="2026-11-10T15:00:00Z",
        )

    def get_state(self) -> PetState:
        return self._state

    def execute_command(self, cmd: Command) -> CommandResult:
        return CommandResult(sucesso=True, mensagem="ok", estado=self._state)

    def is_primary(self) -> bool:
        return self._primary

    def set_primary(self, primary: bool) -> None:
        self._primary = primary


def test_pyro_wrapper_serializes_state_and_command():
    node = DummyNode()
    wrapper = PyroControllerNodeWrapper(node)

    state_dict = wrapper.get_state()
    assert state_dict["racao_gramas"] == 150.0
    assert state_dict["status_fuga"] == "DENTRO"

    cmd_dict = {
        "command_id": "test-cmd",
        "acao": "LIBERAR_RACAO",
        "parametros": {"quantidade_gramas": 50.0},
        "timestamp": "2026-11-10T15:00:00Z",
    }
    result_dict = wrapper.execute_command(cmd_dict)
    assert result_dict["sucesso"] is True
    assert result_dict["estado"]["racao_gramas"] == 150.0

    assert wrapper.is_primary() is True
    wrapper.set_primary(False)
    assert wrapper.is_primary() is False


def test_pyro_proxy_implements_icontroller_node_and_deserializes():
    mock_raw_proxy = MagicMock()
    mock_raw_proxy.get_state.return_value = {
        "racao_gramas": 200.0,
        "agua_ml": 400.0,
        "areia_usos": 2,
        "status_fuga": "DENTRO",
        "alarme_ativo": False,
        "atualizado_em": "2026-11-10T15:00:00Z",
    }
    mock_raw_proxy.is_primary.return_value = False

    proxy = PyroControllerNodeProxy(raw_proxy=mock_raw_proxy)
    assert isinstance(proxy, IControllerNode)

    state = proxy.get_state()
    assert isinstance(state, PetState)
    assert state.racao_gramas == 200.0

    assert proxy.is_primary() is False
    proxy.set_primary(True)
    mock_raw_proxy.set_primary.assert_called_with(True)


def test_pyro_proxy_translates_communication_error_to_domain_exception():
    import Pyro5.errors
    mock_raw_proxy = MagicMock()
    mock_raw_proxy.get_state.side_effect = Pyro5.errors.CommunicationError("Connection refused")

    proxy = PyroControllerNodeProxy(raw_proxy=mock_raw_proxy)
    with pytest.raises(NodeUnreachableError, match="Connection refused"):
        proxy.get_state()


def test_pyro_middleware_proxy_forwarding():
    from src.adapters.pyro_gateway import PyroMiddlewareProxy

    mock_raw = MagicMock()
    mock_raw.get_pet_status.return_value = {"sucesso": True, "estado": {"racao_gramas": 100.0}}
    mock_raw.dispensar_racao.return_value = {"sucesso": True, "mensagem": "ok"}
    mock_raw.repor_agua.return_value = {"sucesso": True, "mensagem": "ok"}
    mock_raw.resetar_areia.return_value = {"sucesso": True, "mensagem": "ok"}
    mock_raw.silenciar_alarme.return_value = {"sucesso": True, "mensagem": "ok"}

    proxy = PyroMiddlewareProxy(raw_proxy=mock_raw)

    status = proxy.get_pet_status()
    assert status["sucesso"] is True
    assert status["estado"]["racao_gramas"] == 100.0

    res = proxy.dispensar_racao(50.0)
    assert res["sucesso"] is True
    mock_raw.dispensar_racao.assert_called_with(50.0)

    res_agua = proxy.repor_agua(200.0)
    assert res_agua["sucesso"] is True

    res_areia = proxy.resetar_areia()
    assert res_areia["sucesso"] is True

    res_alarme = proxy.silenciar_alarme()
    assert res_alarme["sucesso"] is True


def test_pyro_middleware_proxy_thread_local_isolation():
    import threading
    from src.adapters.pyro_gateway import PyroMiddlewareProxy

    proxy = PyroMiddlewareProxy(uri="PYRO:petcare.middleware@localhost:9090")
    results = {}

    def worker(tid: int):
        # Access thread-local proxy without crashing across threads
        local_p = proxy._get_proxy()
        results[tid] = id(local_p)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Proxies instantiated on separate threads must be distinct instances
    assert results[1] != results[2]
