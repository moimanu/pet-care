from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import threading
import time
from typing import Any, Mapping, Optional

from src.adapters.mqtt_client import MqttClient
from src.domain.interfaces import IBrokerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (IoT Edge) %(message)s")
logger = logging.getLogger("IoTDevices")


class IoTDeviceSimulator:
    """Simulates edge sensors (telemetry emission) and actuators (with idempotent cache)."""

    TOPIC_SENSORES_RACAO = "petcare/sensores/racao"
    TOPIC_SENSORES_AGUA = "petcare/sensores/agua"
    TOPIC_SENSORES_AREIA = "petcare/sensores/areia"
    TOPIC_SENSORES_FUGA = "petcare/sensores/fuga"
    TOPIC_ATUADORES_WILDCARD = "petcare/atuadores/#"

    def __init__(
        self,
        broker: IBrokerClient,
        interval: float = 3.0,
        cache_window_seconds: float = 2.0,
    ) -> None:
        self.broker = broker
        self.interval = interval
        self.cache_window_seconds = cache_window_seconds

        # Physical simulated variables
        self.racao_gramas: float = 200.0
        self.agua_ml: float = 500.0
        self.areia_usos: int = 0
        self.perimetro: str = "DENTRO"
        self.alarme_ativo: bool = False

        self._cache: dict[str, float] = {}
        self._lock = threading.Lock()
        self._running = False
        self._sensor_thread: Optional[threading.Thread] = None

    def _cleanup_cache(self, now: float) -> None:
        cutoff = now - self.cache_window_seconds
        expired = [cid for cid, ts in self._cache.items() if ts < cutoff]
        for cid in expired:
            del self._cache[cid]

    def handle_actuator_message(self, topic: str, payload: Mapping[str, Any]) -> bool:
        """Processes actuator command with sliding-window deduplication."""
        now = time.time()
        command_id = str(payload.get("command_id", ""))

        with self._lock:
            self._cleanup_cache(now)
            if command_id and command_id in self._cache:
                logger.warning(
                    "[Borda] Retransmissao descartada por cache idempotente: command_id=%s no topico %s",
                    command_id,
                    topic,
                )
                return False

            if command_id:
                self._cache[command_id] = now

            acao = str(payload.get("acao", "")).strip().upper()

            if "dispenser" in topic or acao == "LIBERAR_RACAO":
                qty = float(payload.get("quantidade_gramas", 50.0))
                self.racao_gramas = min(500.0, self.racao_gramas + qty)
                logger.info("[Atuador Dispenser] +%.1fg ração liberada! Nível: %.1fg", qty, self.racao_gramas)

            elif "valvula" in topic or acao == "REPOR_AGUA":
                qty = float(payload.get("quantidade_ml", 200.0))
                self.agua_ml = min(1000.0, self.agua_ml + qty)
                logger.info("[Atuador Válvula] +%.1fml água reposta! Nível: %.1fml", qty, self.agua_ml)

            elif "alarme" in topic or "ALARME" in acao:
                if acao == "DISPARAR_ALARME":
                    self.alarme_ativo = True
                    motivo = payload.get("motivo", "ALERTA")
                    logger.warning("[Atuador Alarme] BUZZER ATIVADO! Motivo: %s", motivo)
                elif acao == "SILENCIAR_ALARME":
                    self.alarme_ativo = False
                    logger.info("[Atuador Alarme] Alarme silenciado.")

            return True

    def step_sensors(self) -> None:
        """Emits one periodic cycle of sensor readings to MQTT."""
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._lock:
            # Simulate natural consumption
            self.racao_gramas = max(0.0, self.racao_gramas - 1.5)
            self.agua_ml = max(0.0, self.agua_ml - 3.0)

            racao_val = round(self.racao_gramas, 1)
            agua_val = round(self.agua_ml, 1)
            areia_val = self.areia_usos
            fuga_val = self.perimetro

        self.broker.publish(
            self.TOPIC_SENSORES_RACAO,
            {"nivel_gramas": racao_val, "timestamp": now_iso},
            qos=1,
        )
        self.broker.publish(
            self.TOPIC_SENSORES_AGUA,
            {"nivel_ml": agua_val, "timestamp": now_iso},
            qos=1,
        )
        self.broker.publish(
            self.TOPIC_SENSORES_AREIA,
            {"usos_acumulados": areia_val, "timestamp": now_iso},
            qos=1,
        )
        self.broker.publish(
            self.TOPIC_SENSORES_FUGA,
            {"perimetro": fuga_val, "timestamp": now_iso},
            qos=1,
        )
        logger.debug(
            "[Sensores] Telemetria emitida: Racao=%.1fg, Agua=%.1fml, Areia=%d, Fuga=%s",
            racao_val,
            agua_val,
            areia_val,
            fuga_val,
        )

    def _sensor_loop(self) -> None:
        while self._running:
            self.step_sensors()
            time.sleep(self.interval)

    def start(self) -> None:
        """Starts subscription and background sensor simulation."""
        self._running = True
        self.broker.subscribe(
            self.TOPIC_ATUADORES_WILDCARD,
            self.handle_actuator_message,
            qos=2,
        )
        self._sensor_thread = threading.Thread(target=self._sensor_loop, daemon=True)
        self._sensor_thread.start()
        logger.info("IoT Device Simulator iniciado com ciclo de %.1fs", self.interval)

    def stop(self) -> None:
        self._running = False
        if self._sensor_thread and self._sensor_thread.is_alive():
            self._sensor_thread.join(timeout=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulador de Dispositivos IoT de Borda")
    parser.add_argument("--broker", default="localhost", help="Endereço do broker MQTT")
    parser.add_argument("--port", type=int, default=1883, help="Porta do broker MQTT")
    parser.add_argument("--interval", type=float, default=3.0, help="Intervalo de telemetria dos sensores (s)")
    args = parser.parse_args()

    client = MqttClient(host=args.broker, port=args.port, client_id="iot_edge_devices")
    client.connect()

    sim = IoTDeviceSimulator(broker=client, interval=args.interval)
    sim.start()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Encerrando simulador de borda...")
        sim.stop()
        client.disconnect()


if __name__ == "__main__":
    main()
