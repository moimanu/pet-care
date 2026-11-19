import json
import random
import signal
import sys
import time

import paho.mqtt.client as mqtt

from app.config import MQTT_HOST, MQTT_PORT
from app.event_bus import emit_event


class IoTDeviceNode:
    """Processo real de borda: publica telemetria MQTT e recebe comandos de atuadores."""

    def __init__(self) -> None:
        self.running = True
        self.auto_loop = False
        self.racao = 250.0
        self.agua = 500.0
        self.areia = 0
        self.perimetro = "DENTRO"
        self.alarme_ativo = False
        self._seen_commands: dict[str, float] = {}

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="petcare_iot_edge")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_start()
        emit_event("sensors", "Dispositivos IoT conectados ao broker MQTT :1883")

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        client.subscribe("petcare/atuadores/#", qos=2)
        client.subscribe("petcare/sim/control", qos=1)
        emit_event("sensors", "Inscrito em petcare/atuadores/# (QoS 2)")

    def _is_duplicate(self, cmd_id: str) -> bool:
        now = time.monotonic()
        self._seen_commands = {k: t for k, t in self._seen_commands.items() if now - t < 2.0}
        if cmd_id and cmd_id in self._seen_commands:
            return True
        if cmd_id:
            self._seen_commands[cmd_id] = now
        return False

    def _on_message(self, client, userdata, msg) -> None:
        raw = msg.payload.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            emit_event("sensors", f"Payload invalido recebido em {msg.topic}: {raw}", "ERROR")
            return

        if msg.topic == "petcare/sim/control":
            self._handle_control(data)
            return

        cmd_id = str(data.get("command_id", ""))
        if self._is_duplicate(cmd_id):
            emit_event("sensors", f"Atuador filtrou comando duplicado {cmd_id[:8]}", "WARN")
            return

        acao = data.get("acao")
        if acao == "LIBERAR_RACAO":
            self.racao = min(500.0, self.racao + float(data.get("quantidade_gramas", 50.0)))
        elif acao == "REPOR_AGUA":
            self.agua = min(1000.0, self.agua + float(data.get("quantidade_ml", 200.0)))
        elif acao == "DISPARAR_ALARME":
            self.alarme_ativo = True
        elif acao == "SILENCIAR_ALARME":
            self.alarme_ativo = False
        emit_event("sensors", f"Atuador executou {acao} ({msg.topic})")

    def _handle_control(self, data: dict) -> None:
        kind = data.get("kind")
        if kind == "auto":
            self.auto_loop = bool(data.get("enabled"))
            emit_event("sensors", f"Ciclo automatico {'ativado' if self.auto_loop else 'pausado'}")
        elif kind == "telemetry":
            sensor = data["sensor"]
            payload = data["payload"]
            topic = self._publish_telemetry(sensor, payload)
            emit_event("sensors", f"Telemetria publicada em {topic}: {payload}")
            emit_event("mqtt", f"PUB {topic} -> {payload}")
            self._sync_local_reading(sensor, payload)

    def _publish_telemetry(self, sensor: str, payload: dict) -> str:
        topic = f"petcare/sensores/{sensor}"
        self.client.publish(topic, json.dumps(payload), qos=1)
        return topic

    def _sync_local_reading(self, sensor: str, payload: dict) -> None:
        try:
            if sensor == "racao" and 0 <= float(payload["nivel_gramas"]) <= 500:
                self.racao = float(payload["nivel_gramas"])
            elif sensor == "agua" and 0 <= float(payload["nivel_ml"]) <= 1000:
                self.agua = float(payload["nivel_ml"])
            elif sensor == "areia" and int(payload["usos_acumulados"]) >= 0:
                self.areia = int(payload["usos_acumulados"])
            elif sensor == "fuga" and payload.get("perimetro") in ("DENTRO", "FORA"):
                self.perimetro = payload["perimetro"]
        except Exception:
            pass

    def step_auto(self) -> None:
        self.racao = max(0.0, round(self.racao - random.uniform(10.0, 35.0), 1))
        self.agua = max(0.0, round(self.agua - random.uniform(20.0, 60.0), 1))
        for sensor, payload in (
            ("racao", {"nivel_gramas": self.racao}),
            ("agua", {"nivel_ml": self.agua}),
        ):
            topic = self._publish_telemetry(sensor, payload)
            emit_event("sensors", f"[AUTO] {topic}: {payload}")
            emit_event("mqtt", f"PUB {topic} -> {payload}")


def main() -> None:
    node = IoTDeviceNode()

    def shutdown(signum, frame):
        node.running = False
        node.client.loop_stop()
        emit_event("sensors", "Dispositivos IoT encerrados", "WARN")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while node.running:
        if node.auto_loop:
            node.step_auto()
        time.sleep(4.0)


if __name__ == "__main__":
    main()

