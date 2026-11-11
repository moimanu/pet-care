from __future__ import annotations

from dataclasses import asdict
import logging
import threading
from typing import Any, Callable, Mapping, Optional
import Pyro5.api
import Pyro5.errors

from src.domain.entities import Command, CommandResult, PetState
from src.domain.exceptions import DomainError, NodeUnreachableError, QuorumError, SanityError
from src.domain.interfaces import ControllerNode
from src.use_cases.middleware_service import MiddlewareService

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

    def is_primary(self) -> bool:
        return self._node.is_primary()

    def set_primary(self, primary: bool) -> None:
        self._node.set_primary(bool(primary))


class PyroControllerNodeProxy:
    """Client proxy implementing ControllerNode with translation of network errors."""

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

    def is_primary(self) -> bool:
        return bool(self._call("is_primary"))

    def set_primary(self, primary: bool) -> None:
        self._call("set_primary", bool(primary))


@Pyro5.api.expose
class PyroMiddlewareWrapper:
    """Pyro5 exposable interface consumed by client CLI dashboard."""

    def __init__(self, middleware: MiddlewareService) -> None:
        self._middleware = middleware

    def _execute(self, action_fn: Callable[[], CommandResult], label: str) -> dict[str, Any]:
        try:
            result = action_fn()
            return {
                "sucesso": result.sucesso,
                "mensagem": result.mensagem,
                "estado": asdict(result.estado) if result.estado else None,
            }
        except Exception as exc:
            logger.warning("Falha ao %s: %s", label, exc)
            return {"sucesso": False, "mensagem": str(exc), "estado": None}

    def get_pet_status(self) -> dict[str, Any]:
        try:
            state = self._middleware.get_consensus_state()
            data = asdict(state)
            data["hash"] = state.state_hash()
            return {"sucesso": True, "estado": data}
        except Exception as exc:
            logger.warning("Falha ao obter estado por quórum: %s", exc)
            return {"sucesso": False, "mensagem": str(exc), "estado": None}

    def dispensar_racao(self, quantidade: float = 50.0) -> dict[str, Any]:
        return self._execute(lambda: self._middleware.dispensar_racao(float(quantidade)), "dispensar ração")

    def repor_agua(self, quantidade: float = 200.0) -> dict[str, Any]:
        return self._execute(lambda: self._middleware.repor_agua(float(quantidade)), "repor água")

    def resetar_areia(self) -> dict[str, Any]:
        return self._execute(self._middleware.resetar_areia, "resetar areia")

    def silenciar_alarme(self) -> dict[str, Any]:
        return self._execute(self._middleware.silenciar_alarme, "silenciar alarme")


class PyroMiddlewareProxy:
    """Thread-safe client proxy communicating with PyroMiddlewareWrapper via thread-local Proxies."""

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
            logger.warning("Falha RPC no Middleware %s em %s: %s", func_name, self._uri, exc)
            return {"sucesso": False, "mensagem": f"Falha de conexão com Middleware: {exc}", "estado": None}

    def get_pet_status(self) -> dict[str, Any]:
        return self._call("get_pet_status")

    def dispensar_racao(self, quantidade: float = 50.0) -> dict[str, Any]:
        return self._call("dispensar_racao", float(quantidade))

    def repor_agua(self, quantidade: float = 200.0) -> dict[str, Any]:
        return self._call("repor_agua", float(quantidade))

    def resetar_areia(self) -> dict[str, Any]:
        return self._call("resetar_areia")

    def silenciar_alarme(self) -> dict[str, Any]:
        return self._call("silenciar_alarme")
