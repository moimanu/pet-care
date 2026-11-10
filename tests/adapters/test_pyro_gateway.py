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
