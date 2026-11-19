from __future__ import annotations

from dataclasses import asdict
import logging
import threading
from typing import Any, Mapping, Optional
import Pyro5.api
import Pyro5.errors

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import DomainError, NodeUnreachableError, QuorumError, SanityError
from src.domain.interfaces import ControllerNode

logger = logging.getLogger(__name__)

# Register custom domain exceptions with Pyro5 deserializer to prevent SerializeError
for _exc in (DomainError, NodeUnreachableError, QuorumError, SanityError):
    _fullname = f"{_exc.__module__}.{_exc.__name__}"
    Pyro5.api.register_dict_to_class(
        _fullname, lambda classname, d, cls=_exc: cls(*d.get("args", ()))
    )


@Pyro5.api.expose
class PyroControllerNodeWrapper:
    """Pyro5 exposable RPC wrapper translating raw RPC dictionaries to domain types."""

    def __init__(self, node: ControllerNode) -> None:
        self._node = node

    def get_state(self) -> dict[str, Any]:
        return asdict(self._node.get_state())

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

    def sync_state(
        self,
        state_dict: Mapping[str, Any],
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> bool:
        state = PetState.from_mapping(state_dict)
        return bool(
            self._node.sync_state(
                state,
                command_id=command_id,
                tipo_evento=str(tipo_evento),
                payload=str(payload),
            )
        )

    def is_primary(self) -> bool:
        return self._node.is_primary()

    def set_primary(self, primary: bool) -> None:
        self._node.set_primary(bool(primary))

    def is_corrupted(self) -> bool:
        return self._node.is_corrupted()

    def set_corrupt(self, corrupt: bool) -> None:
        self._node.set_corrupt(bool(corrupt))

    def is_offline(self) -> bool:
        return self._node.is_offline()

    def set_offline(self, offline: bool) -> None:
        self._node.set_offline(bool(offline))

    def get_info(self) -> dict[str, Any]:
        return dict(self._node.get_info())


class PyroControllerNodeProxy:
    """Thread-safe client proxy implementing ControllerNode with translation of network errors."""

    def __init__(self, uri: str = "", raw_proxy: Optional[Any] = None) -> None:
        self._uri = uri
        self._raw_proxy = raw_proxy
        self._local = threading.local()

    def _get_proxy(self) -> Any:
        if self._raw_proxy is not None:
            return self._raw_proxy
        if not hasattr(self._local, "proxy"):
            self._local.proxy = Pyro5.api.Proxy(self._uri)
        return self._local.proxy

    def _call(self, func_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            return getattr(self._get_proxy(), func_name)(*args, **kwargs)
        except Exception as exc:
            logger.warning("RPC failure invoking %s on %s: %s", func_name, self._uri, exc)
            raise NodeUnreachableError(f"RPC failure on node {self._uri}: {exc}") from exc

    def get_state(self) -> PetState:
        return PetState.from_mapping(self._call("get_state"))

    def execute_command(self, command: Command) -> CommandResult:
        res = self._call("execute_command", asdict(command))
        state = PetState.from_mapping(res["estado"]) if res.get("estado") else None
        return CommandResult(sucesso=bool(res["sucesso"]), mensagem=str(res["mensagem"]), estado=state)

    def sync_state(
        self,
        state: PetState,
        command_id: Optional[str] = None,
        tipo_evento: str = "",
        payload: str = "",
    ) -> bool:
        return bool(
            self._call(
                "sync_state",
                asdict(state),
                command_id,
                str(tipo_evento),
                str(payload),
            )
        )

    def is_primary(self) -> bool:
        return bool(self._call("is_primary"))

    def set_primary(self, primary: bool) -> None:
        self._call("set_primary", bool(primary))

    def is_corrupted(self) -> bool:
        return bool(self._call("is_corrupted"))

    def set_corrupt(self, corrupt: bool) -> None:
        self._call("set_corrupt", bool(corrupt))

    def is_offline(self) -> bool:
        return bool(self._call("is_offline"))

    def set_offline(self, offline: bool) -> None:
        self._call("set_offline", bool(offline))

    def get_info(self) -> dict[str, Any]:
        return dict(self._call("get_info"))
