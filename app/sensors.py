"""Processo de borda IoT: publica telemetria MQTT (QoS 1) e executa atuadores (QoS 2)."""

from contextlib import suppress
import json
import random
import sys
import time

from app.config import MQTT_PORT, TOPIC_ACTUATORS, TOPIC_SENSORS, TOPIC_SIM_CONTROL
from app.domain import (
    Action, FOOD_STEP_GRAMS, MAX_FOOD_GRAMS, MAX_WATER_ML,
    PetState, Sensor, WATER_STEP_ML, apply_telemetry,
)
from app.event_bus import emit_event
from app.infra import connect_mqtt, install_shutdown_handler, publish_json

DEDUP_WINDOW_SECONDS = 2.0
AUTO_LOOP_INTERVAL_SECONDS = 4.0


class IoTDeviceNode:
    """Simula os sensores e atuadores fisicos conectados ao broker MQTT."""

    def __init__(self) -> None:
        self.running = True
        self.auto_loop = False
        self.state = PetState()
        self._seen_commands: dict[str, float] = {}
        self.mqtt = connect_mqtt("petcare_iot_edge", on_connect=self._on_connect, on_message=self._on_message)
        emit_event("sensors", f"Dispositivos IoT conectados ao broker MQTT :{MQTT_PORT}")

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        client.subscribe(f"{TOPIC_ACTUATORS}/#", qos=2)
        client.subscribe(TOPIC_SIM_CONTROL, qos=1)
        emit_event("sensors", f"Inscrito em {TOPIC_ACTUATORS}/# (QoS 2)")

    def _is_duplicate(self, command_id: str) -> bool:
        """Descarta comandos repetidos dentro da janela de deduplicacao."""
        now = time.monotonic()
        self._seen_commands = {cid: ts for cid, ts in self._seen_commands.items() if now - ts < DEDUP_WINDOW_SECONDS}
        if not command_id:
            return False
        if command_id in self._seen_commands:
            return True
        self._seen_commands[command_id] = now
        return False

    def _on_message(self, client, userdata, msg) -> None:
        raw = msg.payload.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            emit_event("sensors", f"Payload invalido recebido em {msg.topic}: {raw}", "ERROR")
            return

        if msg.topic == TOPIC_SIM_CONTROL:
            self._handle_control(data)
            return

        command_id = str(data.get("command_id", ""))
        if self._is_duplicate(command_id):
            emit_event("sensors", f"Atuador filtrou comando duplicado {command_id[:8]}", "WARN")
            return

        action = data.get("acao")
        match action:
            case Action.LIBERAR_RACAO:
                self.state.racao_gramas = min(MAX_FOOD_GRAMS, self.state.racao_gramas + float(data.get("quantidade_gramas", FOOD_STEP_GRAMS)))
            case Action.REPOR_AGUA:
                self.state.agua_ml = min(MAX_WATER_ML, self.state.agua_ml + float(data.get("quantidade_ml", WATER_STEP_ML)))
            case Action.DISPARAR_ALARME:
                self.state.alarme_ativo = True
            case Action.SILENCIAR_ALARME:
                self.state.alarme_ativo = False
        emit_event("sensors", f"Atuador executou {action} ({msg.topic})")

    def _handle_control(self, data: dict) -> None:
        match data.get("kind"):
            case "auto":
                self.auto_loop = bool(data.get("enabled"))
                emit_event("sensors", f"Ciclo automatico {'ativado' if self.auto_loop else 'pausado'}")
            case "telemetry":
                sensor, payload = data["sensor"], data["payload"]
                topic = self._publish_telemetry(sensor, payload)
                emit_event("sensors", f"Telemetria publicada em {topic}: {payload}")
                with suppress(ValueError, TypeError):
                    self.state, _ = apply_telemetry(self.state, sensor, payload)

    def _publish_telemetry(self, sensor: str, payload: dict) -> str:
        topic = f"{TOPIC_SENSORS}/{sensor}"
        publish_json(self.mqtt, topic, payload, qos=1)
        return topic

    def step_auto(self) -> None:
        self.state.racao_gramas = max(0.0, round(self.state.racao_gramas - random.uniform(10.0, 35.0), 1))
        self.state.agua_ml = max(0.0, round(self.state.agua_ml - random.uniform(20.0, 60.0), 1))
        for sensor, payload in (
            (Sensor.RACAO, {"nivel_gramas": self.state.racao_gramas}),
            (Sensor.AGUA, {"nivel_ml": self.state.agua_ml}),
        ):
            topic = self._publish_telemetry(sensor, payload)
            emit_event("sensors", f"[AUTO] {topic}: {payload}")


def main() -> None:
    node = IoTDeviceNode()

    def shutdown(_signum: int) -> None:
        node.running = False
        node.mqtt.loop_stop()
        emit_event("sensors", "Dispositivos IoT encerrados", "WARN")
        sys.exit(0)

    install_shutdown_handler(shutdown)
    while node.running:
        if node.auto_loop:
            node.step_auto()
        time.sleep(AUTO_LOOP_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
