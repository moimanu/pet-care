import json
from unittest.mock import MagicMock
import pytest

from src.adapters.mqtt_client import MqttClient
from src.domain.interfaces import BrokerClient


def test_mqtt_client_implements_interface():
    client = MqttClient(host="localhost", port=1883)
    assert isinstance(client, BrokerClient)


def test_mqtt_client_publish_serializes_json():
    mock_paho = MagicMock()
    client = MqttClient(host="localhost", port=1883, paho_client=mock_paho)

    payload = {"nivel_gramas": 120.0, "timestamp": "2026-11-10T15:00:00Z"}
    client.publish("petcare/sensores/racao", payload, qos=1)

    mock_paho.publish.assert_called_once()
    topic, body = mock_paho.publish.call_args[0][:2]
    kwargs = mock_paho.publish.call_args[1]

    assert topic == "petcare/sensores/racao"
    assert json.loads(body) == payload
    assert kwargs.get("qos", mock_paho.publish.call_args[0][2] if len(mock_paho.publish.call_args[0]) > 2 else 1) == 1


def test_mqtt_client_subscription_and_message_dispatch():
    mock_paho = MagicMock()
    client = MqttClient(host="localhost", port=1883, paho_client=mock_paho)

    received = []

    def callback(topic: str, payload: dict):
        received.append((topic, payload))

    client.subscribe("petcare/sensores/#", callback, qos=1)
    mock_paho.subscribe.assert_called_with("petcare/sensores/#", qos=1)

    # Simulate incoming message
    msg = MagicMock()
    msg.topic = "petcare/sensores/agua"
    msg.payload = json.dumps({"nivel_ml": 350.0}).encode("utf-8")

    client._on_message(mock_paho, None, msg)

    assert len(received) == 1
    assert received[0][0] == "petcare/sensores/agua"
    assert received[0][1] == {"nivel_ml": 350.0}
