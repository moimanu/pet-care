from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any, Callable, Optional

# Ensure project root is in sys.path when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.adapters.mqtt_client import MqttClient
from src.adapters.pyro_gateway import PyroControllerNodeProxy
from src.adapters.web_gateway import WebGateway, WebGatewayLogHandler
from src.domain.interfaces import ControllerNode
from src.use_cases.cluster_client_service import ClusterClientService

logger = logging.getLogger(__name__)


def make_mqtt_message_handler(gateway: WebGateway) -> Callable[[str, dict[str, Any]], None]:
    """Creates a callback function forwarding MQTT sensor/actuator messages to SSE."""
    def on_mqtt_message(topic: str, payload: dict[str, Any]) -> None:
        ts = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
        gateway.broadcast("raw_log", {
            "source": "broker",
            "line": f"{ts} [INFO] (MQTT-Broker) Mensagem em {topic}: {json.dumps(payload)}",
        })
        gateway.broadcast("log", {
            "timestamp": ts,
            "level": "INFO",
            "component": "MQTT Edge",
            "message": f"Mensagem recebida em {topic}",
            "payload": payload,
        })
    return on_mqtt_message


def build_web_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    middleware: Optional[Any] = None,
    static_dir: Optional[str] = "web",
    logs_dir: Optional[str] = "logs",
) -> WebGateway:
    """Configures and returns WebGateway with all REST and SSE routes."""
    return WebGateway(
        host=host,
        port=port,
        middleware=middleware,
        static_dir=static_dir,
        logs_dir=logs_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor Web PetCare (Cliente RPC Descentralizado)")
    parser.add_argument("--host", default="0.0.0.0", help="Endereço de bind HTTP")
    parser.add_argument("--port", type=int, default=8080, help="Porta HTTP")
    parser.add_argument(
        "--node1",
        default="PYRO:petcare.controller.1@localhost:9091",
        help="URI Pyro5 do Controlador 1",
    )
    parser.add_argument(
        "--node2",
        default="PYRO:petcare.controller.2@localhost:9092",
        help="URI Pyro5 do Controlador 2",
    )
    parser.add_argument(
        "--node3",
        default="PYRO:petcare.controller.3@localhost:9093",
        help="URI Pyro5 do Controlador 3",
    )
    parser.add_argument("--static-dir", default="web", help="Diretório de assets estáticos")
    parser.add_argument("--logs-dir", default="logs", help="Diretório de logs a monitorar")
    parser.add_argument("--mqtt-host", default="localhost", help="Host do broker MQTT")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="Porta do broker MQTT")
    parser.add_argument("--no-mqtt", action="store_true", help="Desabilitar conexão MQTT")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    )

    logger.info(
        "Conectando cliente diretamente aos Controladores Pyro5 (%s, %s, %s)...",
        args.node1,
        args.node2,
        args.node3,
    )
    nodes: dict[int, ControllerNode] = {
        1: PyroControllerNodeProxy(args.node1),
        2: PyroControllerNodeProxy(args.node2),
        3: PyroControllerNodeProxy(args.node3),
    }
    cluster_client = ClusterClientService(nodes=nodes)

    gateway = build_web_server(
        host=args.host,
        port=args.port,
        middleware=cluster_client,
        static_dir=args.static_dir,
        logs_dir=args.logs_dir,
    )

    # Attach SSE log streamer to root logger
    sse_handler = WebGatewayLogHandler(gateway, component="ClusterClient")
    logging.getLogger().addHandler(sse_handler)

    if not args.no_mqtt:
        try:
            mqtt = MqttClient(host=args.mqtt_host, port=args.mqtt_port, client_id="petcare-web-gateway")
            mqtt.connect()
            handler = make_mqtt_message_handler(gateway)
            mqtt.subscribe("petcare/sensores/#", handler)
            mqtt.subscribe("petcare/atuadores/#", handler)
            logger.info("Conectado ao MQTT Broker em %s:%d", args.mqtt_host, args.mqtt_port)
        except Exception as exc:
            logger.warning("Não foi possível conectar ao broker MQTT: %s", exc)

    gateway.start()
    print(f"\n=======================================================")
    print(f"   PAINEL WEB PETCARE INICIADO COM SUCESSO!            ")
    print(f"   Acesse no navegador: http://localhost:{gateway.port}/ ")
    print(f"=======================================================\n")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Encerrando servidor web...")
    finally:
        gateway.stop()


if __name__ == "__main__":
    main()
