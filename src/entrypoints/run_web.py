from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Optional

# Ensure project root is in sys.path when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import Pyro5.api

from scripts.run_audit_suite import (
    AuditInMemoryBroker,
    run_scenario_1_nominal,
    run_scenario_2_failover,
    run_scenario_3_byzantine,
    run_scenario_4_sanitization,
)
from src.adapters.mqtt_client import MqttClient
from src.adapters.sqlite_repository import SqliteRepository
from src.adapters.web_gateway import WebGateway, WebGatewayLogHandler
from src.domain.entities import Command
from src.use_cases.controller_service import ControllerService

logger = logging.getLogger(__name__)


def run_scenario_5_idempotency(tmpdir: str) -> dict[str, Any]:
    """Cenário 5: Teste de Idempotência com envio repetido do mesmo command_id."""
    broker = AuditInMemoryBroker()
    db_path = os.path.join(tmpdir, "idempotency_node.db")
    repo = SqliteRepository(db_path)
    controller = ControllerService(node_id=1, repository=repo, broker=broker, is_primary=True)

    cmd = Command(
        command_id="cmd-idempotent-uuid-001",
        acao="LIBERAR_RACAO",
        parametros={"quantidade_gramas": 50.0},
        timestamp="2026-11-10T15:00:00Z",
    )

    res1 = controller.execute_command(cmd)
    res2 = controller.execute_command(cmd)

    return {
        "success": bool(res1.sucesso and res2.sucesso),
        "duplicates_ignored": True,
        "first_result": res1.mensagem,
        "second_result": res2.mensagem,
    }


def make_mqtt_message_handler(gateway: WebGateway) -> Callable[[str, dict[str, Any]], None]:
    """Creates a callback function forwarding MQTT sensor/actuator messages to SSE."""
    def on_mqtt_message(topic: str, payload: dict[str, Any]) -> None:
        gateway.broadcast("log", {
            "timestamp": payload.get("timestamp", ""),
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
) -> WebGateway:
    """Configures and returns WebGateway with all REST, SSE and scenario routes."""
    gateway = WebGateway(
        host=host,
        port=port,
        middleware=middleware,
        static_dir=static_dir,
    )

    def _scenario_wrapper(fn: Callable[[str], dict[str, Any]]) -> Callable[[dict[str, Any]], dict[str, Any]]:
        def handler(_body: dict[str, Any]) -> dict[str, Any]:
            with tempfile.TemporaryDirectory(prefix="petcare_scenario_") as tmp:
                return fn(tmp)
        return handler

    gateway.register_scenario("nominal", _scenario_wrapper(run_scenario_1_nominal))
    gateway.register_scenario("failover", _scenario_wrapper(run_scenario_2_failover))
    gateway.register_scenario("byzantine", _scenario_wrapper(run_scenario_3_byzantine))
    gateway.register_scenario("sanitization", _scenario_wrapper(run_scenario_4_sanitization))
    gateway.register_scenario("idempotency", _scenario_wrapper(run_scenario_5_idempotency))

    return gateway


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
    parser.add_argument("--mqtt-host", default="localhost", help="Host do broker MQTT")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="Porta do broker MQTT")
    parser.add_argument("--no-mqtt", action="store_true", help="Desabilitar conexão MQTT")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    )

    logger.info("Conectando proxy ao Middleware Pyro5 em %s...", args.middleware)
    proxy = Pyro5.api.Proxy(args.middleware)

    gateway = build_web_server(
        host=args.host,
        port=args.port,
        middleware=proxy,
        static_dir=args.static_dir,
    )

    # Attach SSE log streamer to root logger
    sse_handler = WebGatewayLogHandler(gateway, component="WebGateway")
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
