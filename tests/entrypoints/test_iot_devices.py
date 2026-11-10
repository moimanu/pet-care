import time
from unittest.mock import MagicMock
import pytest

from src.entrypoints.run_iot_devices import IoTDeviceSimulator


class MockBroker:
    def __init__(self):
        self.published = []
        self.subscribed = []

    def publish(self, topic: str, payload: dict, qos: int = 1):
        self.published.append((topic, payload, qos))

    def subscribe(self, topic: str, callback, qos: int = 1):
        self.subscribed.append((topic, callback))


def test_iot_devices_step_sensor_publishes_telemetry():
    broker = MockBroker()
    sim = IoTDeviceSimulator(broker=broker, interval=1.0)

    sim.step_sensors()

    # Must publish to all 4 telemetry topics
    topics = [item[0] for item in broker.published]
    assert "petcare/sensores/racao" in topics
    assert "petcare/sensores/agua" in topics
    assert "petcare/sensores/areia" in topics
    assert "petcare/sensores/fuga" in topics


def test_iot_devices_actuators_update_physical_state():
    broker = MockBroker()
    sim = IoTDeviceSimulator(broker=broker, interval=1.0)
    sim.racao_gramas = 100.0
    sim.agua_ml = 200.0
    sim.alarme_ativo = False

    # Dispenser actuator
    res1 = sim.handle_actuator_message(
        "petcare/atuadores/dispenser",
        {"acao": "LIBERAR_RACAO", "quantidade_gramas": 50.0, "command_id": "cmd-1"},
    )
    assert res1 is True
    assert sim.racao_gramas == 150.0

    # Valve actuator
    res2 = sim.handle_actuator_message(
        "petcare/atuadores/valvula",
        {"acao": "REPOR_AGUA", "quantidade_ml": 200.0, "command_id": "cmd-2"},
    )
    assert res2 is True
    assert sim.agua_ml == 400.0

    # Alarm actuator
    res3 = sim.handle_actuator_message(
        "petcare/atuadores/alarme",
        {"acao": "DISPARAR_ALARME", "motivo": "FUGA_DETECTADA", "command_id": "cmd-3"},
    )
    assert res3 is True
    assert sim.alarme_ativo is True

    # Silence alarm
    res4 = sim.handle_actuator_message(
        "petcare/atuadores/alarme",
        {"acao": "SILENCIAR_ALARME", "command_id": "cmd-4"},
    )
    assert res4 is True
    assert sim.alarme_ativo is False


def test_iot_devices_sliding_window_deduplication():
    broker = MockBroker()
    sim = IoTDeviceSimulator(broker=broker, interval=1.0, cache_window_seconds=2.0)
    sim.racao_gramas = 100.0

    # First call processed
    p1 = sim.handle_actuator_message(
        "petcare/atuadores/dispenser",
        {"acao": "LIBERAR_RACAO", "quantidade_gramas": 50.0, "command_id": "dup-cmd-1"},
    )
    assert p1 is True
    assert sim.racao_gramas == 150.0

    # Immediate second call with same command_id dropped by cache
    p2 = sim.handle_actuator_message(
        "petcare/atuadores/dispenser",
        {"acao": "LIBERAR_RACAO", "quantidade_gramas": 50.0, "command_id": "dup-cmd-1"},
    )
    assert p2 is False
    assert sim.racao_gramas == 150.0
