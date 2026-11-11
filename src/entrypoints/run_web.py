from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable, Optional

# Ensure project root is in sys.path when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import Pyro5.api

from src.adapters.mqtt_client import MqttClient
from src.adapters.pyro_gateway import PyroMiddlewareProxy
from src.adapters.web_gateway import WebGateway, WebGatewayLogHandler

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
    mqtt_client: Optional[Any] = None,
) -> WebGateway:
    """Configures and returns WebGateway with all REST and SSE routes."""
    return WebGateway(
        host=host,
        port=port,
        middleware=middleware,
        static_dir=static_dir,
        logs_dir=logs_dir,
        mqtt_client=mqtt_client,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor Web PetCare (HTTP/REST/SSE)")
    parser.add_argument("--host", default="0.0.0.0", help="Endereço de bind HTTP")
    parser.add_argument("--port", type=int, default=8080, help="Porta HTTP")
    parser.add_argument(
        "--middleware",
        default="PYRO:petcare.middleware@localhost:9090",
        help="URI Pyro5 do Middleware",
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

    logger.info("Conectando proxy thread-safe ao Middleware Pyro5 em %s...", args.middleware)
    proxy = PyroMiddlewareProxy(args.middleware)

    gateway = build_web_server(
        host=args.host,
        port=args.port,
        middleware=proxy,
        static_dir=args.static_dir,
        logs_dir=args.logs_dir,
    )

    # Attach SSE log streamer to root logger
    sse_handler = WebGatewayLogHandler(gateway, component="WebGateway")
    logging.getLogger().addHandler(sse_handler)

    if not args.no_mqtt:
        try:
            mqtt = MqttClient(host=args.mqtt_host, port=args.mqtt_port, client_id="petcare-web-gateway")
            mqtt.connect()
            gateway.mqtt_client = mqtt
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
