from typing import Optional
from unittest.mock import MagicMock
import threading
import pytest

from src.adapters.pyro_gateway import PyroControllerNodeProxy, PyroControllerNodeWrapper
from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError
from src.domain.interfaces import ControllerNode


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

    def sync_state(
        self,
        state: PetState,
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> bool:
        self._state = state
        return True

    def is_primary(self) -> bool:
        return self._primary

    def set_primary(self, primary: bool) -> None:
        self._primary = primary


def test_pyro_wrapper_serializes_state_command_and_sync_state():
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

    new_state_dict = {
        "racao_gramas": 275.0,
        "agua_ml": 450.0,
        "areia_usos": 0,
        "status_fuga": "DENTRO",
        "alarme_ativo": False,
        "atualizado_em": "2026-11-10T16:00:00Z",
    }
    assert wrapper.sync_state(new_state_dict, command_id="sync-1", tipo_evento="LIBERAR_RACAO", payload="{}") is True
    assert wrapper.get_state()["racao_gramas"] == 275.0

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
    mock_raw_proxy.sync_state.return_value = True

    proxy = PyroControllerNodeProxy(raw_proxy=mock_raw_proxy)
    assert isinstance(proxy, ControllerNode)

    state = proxy.get_state()
    assert isinstance(state, PetState)
    assert state.racao_gramas == 200.0

    assert proxy.sync_state(state, command_id="cid-1", tipo_evento="LIBERAR_RACAO", payload="{}") is True
    mock_raw_proxy.sync_state.assert_called_once()

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


def test_pyro_controller_proxy_thread_local_isolation():
    proxy = PyroControllerNodeProxy(uri="PYRO:petcare.controller.1@localhost:9091")
    results = {}

    def worker(tid: int):
        local_p = proxy._get_proxy()
        results[tid] = id(local_p)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results[1] != results[2]
