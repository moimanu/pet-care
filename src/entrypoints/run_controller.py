from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import time
from typing import Iterable, Optional

# Ensure project root is in sys.path when executed directly as a script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import Pyro5.api

from src.adapters.mqtt_client import MqttClient

from src.adapters.pyro_gateway import PyroControllerNodeProxy, PyroControllerNodeWrapper
from src.adapters.sqlite_repository import SqliteRepository
from src.domain.interfaces import IBrokerClient, IControllerNode
from src.use_cases.controller_service import ControllerService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (Controller-%(name)s) %(message)s")


def build_controller(
    node_id: int,
    is_primary: bool = False,
    is_corrupt: bool = False,
    db_path: Optional[str] = None,
    broker_client: Optional[IBrokerClient] = None,
    peer_proxies: Optional[Iterable[IControllerNode]] = None,
) -> ControllerService:
    """Instantiates repository, broker, autonomous controller service and synchronizes boot."""
    database_file = db_path or f"node_{node_id}.db"
    repo = SqliteRepository(database_file)

    if broker_client is None:
        broker = MqttClient(host="localhost", port=1883, client_id=f"controller_node_{node_id}")
        broker.connect()
    else:
        broker = broker_client

    controller = ControllerService(
        node_id=node_id,
        repository=repo,
        broker=broker,
        is_primary=is_primary,
        is_corrupt=is_corrupt,
    )

    # Subscribe to edge telemetry
    broker.subscribe("petcare/sensores/#", controller.handle_telemetry, qos=1)

    # Boot sync from peers
    if peer_proxies:
        controller.sync_boot_state(peer_proxies)

    return controller


def main() -> None:
    parser = argparse.ArgumentParser(description="Pet Care Controller Replica")
    parser.add_argument("--id", type=int, required=True, help="Identificador do nó (ex: 1, 2, 3)")
    parser.add_argument("--primary", action="store_true", help="Define nó como líder primário de atuação")
    parser.add_argument("--corrupt", action="store_true", help="Injeta divergência bizantina intencional")
    parser.add_argument("--port", type=int, default=None, help="Porta Pyro5 (padrão: 9090 + id)")
    parser.add_argument("--broker", default="localhost", help="Host do broker MQTT")
    parser.add_argument("--broker-port", type=int, default=1883, help="Porta do broker MQTT")
    parser.add_argument("--db", default=None, help="Caminho do arquivo de banco SQLite")
    args = parser.parse_args()

    port = args.port if args.port is not None else (9090 + args.id)
    logger = logging.getLogger(f"Node{args.id}")
    logger.info("Iniciando Controlador %d (Primary=%s, Corrupt=%s) na porta Pyro5 %d", args.id, args.primary, args.corrupt, port)

    # Discover peer ports for boot synchronization
    peer_ids = [nid for nid in [1, 2, 3] if nid != args.id]
    peer_proxies = [
        PyroControllerNodeProxy(f"PYRO:petcare.controller.{pid}@localhost:{9090 + pid}")
        for pid in peer_ids
    ]

    broker = MqttClient(host=args.broker, port=args.broker_port, client_id=f"controller_node_{args.id}")
    broker.connect()

    controller = build_controller(
        node_id=args.id,
        is_primary=args.primary,
        is_corrupt=args.corrupt,
        db_path=args.db,
        broker_client=broker,
        peer_proxies=peer_proxies,
    )

    daemon = Pyro5.api.Daemon(port=port)
    wrapper = PyroControllerNodeWrapper(controller)
    uri = daemon.register(wrapper, objectId=f"petcare.controller.{args.id}")
    logger.info("Nó registrado com URI Pyro5: %s", uri)

    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        logger.info("Encerrando Controlador %d...", args.id)
        broker.disconnect()
        daemon.close()


if __name__ == "__main__":
    main()
