from __future__ import annotations

from dataclasses import asdict
import logging
from typing import Any, Mapping, Optional
import Pyro5.api
import Pyro5.errors

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import NodeUnreachableError
from src.domain.interfaces import IControllerNode

logger = logging.getLogger(__name__)


@Pyro5.api.expose
class PyroControllerNodeWrapper:
    """Pyro5 exposable RPC wrapper translating raw RPC dictionaries to domain types."""

    def __init__(self, node: IControllerNode) -> None:
        self._node = node

    def get_state(self) -> dict[str, Any]:
        state = self._node.get_state()
        return asdict(state)

    def execute_command(self, cmd_dict: Mapping[str, Any]) -> dict[str, Any]:
        cmd = Command(
            command_id=str(cmd_dict.get("command_id", "")),
            acao=str(cmd_dict.get("acao", "")),
            parametros=dict(cmd_dict.get("parametros", {})),
            timestamp=str(cmd_dict.get("timestamp", "")),
        )
        result = self._node.execute_command(cmd)
        return {
            "sucesso": result.sucesso,
            "mensagem": result.mensagem,
            "estado": asdict(result.estado) if result.estado is not None else None,
        }

    def is_primary(self) -> bool:
        return self._node.is_primary()

    def set_primary(self, primary: bool) -> None:
        self._node.set_primary(bool(primary))


class PyroControllerNodeProxy:
    """Client proxy implementing IControllerNode with translation of network errors."""

    def __init__(self, uri: str = "", raw_proxy: Optional[Any] = None) -> None:
        self._uri = uri
        if raw_proxy is not None:
            self._proxy = raw_proxy
        else:
            self._proxy = Pyro5.api.Proxy(uri)

    def _call(self, func_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            method = getattr(self._proxy, func_name)
            return method(*args, **kwargs)
        except (Pyro5.errors.CommunicationError, Pyro5.errors.TimeoutError, Exception) as exc:
            logger.warning("RPC failure invoking %s on %s: %s", func_name, self._uri, exc)
            raise NodeUnreachableError(f"RPC failure on node {self._uri}: {exc}") from exc

    def get_state(self) -> PetState:
        data = self._call("get_state")
        return PetState(
            racao_gramas=float(data["racao_gramas"]),
            agua_ml=float(data["agua_ml"]),
            areia_usos=int(data["areia_usos"]),
            status_fuga=str(data["status_fuga"]),
            alarme_ativo=bool(data["alarme_ativo"]),
            atualizado_em=str(data["atualizado_em"]),
        )

    def execute_command(self, command: Command) -> CommandResult:
        cmd_dict = asdict(command)
        res = self._call("execute_command", cmd_dict)
        state_dict = res.get("estado")
        state: Optional[PetState] = None
        if state_dict is not None:
            state = PetState(
                racao_gramas=float(state_dict["racao_gramas"]),
                agua_ml=float(state_dict["agua_ml"]),
                areia_usos=int(state_dict["areia_usos"]),
                status_fuga=str(state_dict["status_fuga"]),
                alarme_ativo=bool(state_dict["alarme_ativo"]),
                atualizado_em=str(state_dict["atualizado_em"]),
            )
        return CommandResult(
            sucesso=bool(res["sucesso"]),
            mensagem=str(res["mensagem"]),
            estado=state,
        )

    def is_primary(self) -> bool:
        return bool(self._call("is_primary"))

    def set_primary(self, primary: bool) -> None:
        self._call("set_primary", bool(primary))
