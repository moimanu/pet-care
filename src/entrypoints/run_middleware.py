from __future__ import annotations

import argparse
from dataclasses import asdict
import logging
from pathlib import Path
import sys
from typing import Any, Mapping

# Ensure project root is in sys.path when executed directly as a script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import Pyro5.api

from src.adapters.pyro_gateway import PyroControllerNodeProxy

from src.domain.interfaces import IControllerNode
from src.use_cases.middleware_service import MiddlewareService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (Middleware) %(message)s")
logger = logging.getLogger("Middleware")


@Pyro5.api.expose
class PyroMiddlewareWrapper:
    """Pyro5 exposable interface consumed by client CLI dashboard."""

    def __init__(self, middleware: MiddlewareService) -> None:
        self._middleware = middleware

    def get_pet_status(self) -> dict[str, Any]:
        state = self._middleware.get_consensus_state()
        data = asdict(state)
        data["hash"] = state.state_hash()
        return data

    def dispensar_racao(self, quantidade: float = 50.0) -> dict[str, Any]:
        result = self._middleware.dispensar_racao(float(quantidade))
        return {
            "sucesso": result.sucesso,
            "mensagem": result.mensagem,
            "estado": asdict(result.estado) if result.estado else None,
        }

    def repor_agua(self, quantidade: float = 200.0) -> dict[str, Any]:
        result = self._middleware.repor_agua(float(quantidade))
        return {
            "sucesso": result.sucesso,
            "mensagem": result.mensagem,
            "estado": asdict(result.estado) if result.estado else None,
        }

    def resetar_areia(self) -> dict[str, Any]:
        result = self._middleware.resetar_areia()
        return {
            "sucesso": result.sucesso,
            "mensagem": result.mensagem,
            "estado": asdict(result.estado) if result.estado else None,
        }

    def silenciar_alarme(self) -> dict[str, Any]:
        result = self._middleware.silenciar_alarme()
        return {
            "sucesso": result.sucesso,
            "mensagem": result.mensagem,
            "estado": asdict(result.estado) if result.estado else None,
        }


def build_middleware(nodes: Mapping[int, IControllerNode], timeout: float = 1.0) -> MiddlewareService:
    return MiddlewareService(nodes=nodes, timeout=timeout)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pet Care Middleware Service")
    parser.add_argument("--port", type=int, default=9090, help="Porta Pyro5 do Middleware")
    parser.add_argument("--node1", default="PYRO:petcare.controller.1@localhost:9091", help="URI do Nó 1")
    parser.add_argument("--node2", default="PYRO:petcare.controller.2@localhost:9092", help="URI do Nó 2")
    parser.add_argument("--node3", default="PYRO:petcare.controller.3@localhost:9093", help="URI do Nó 3")
    args = parser.parse_args()

    nodes: dict[int, IControllerNode] = {
        1: PyroControllerNodeProxy(args.node1),
        2: PyroControllerNodeProxy(args.node2),
        3: PyroControllerNodeProxy(args.node3),
    }

    middleware = build_middleware(nodes=nodes)
    wrapper = PyroMiddlewareWrapper(middleware)

    daemon = Pyro5.api.Daemon(port=args.port)
    uri = daemon.register(wrapper, objectId="petcare.middleware")
    logger.info("Middleware registrado com URI Pyro5: %s", uri)

    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        logger.info("Encerrando Middleware...")
        daemon.close()


if __name__ == "__main__":
    main()
