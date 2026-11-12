from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttClient:
    """Paho-MQTT client adapter implementing BrokerClient with topic routing and JSON codec."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "",
        paho_client: Optional[mqtt.Client] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._callbacks: list[tuple[str, Callable[[str, dict[str, Any]], None]]] = []

        if paho_client is not None:
            self._client = paho_client
        else:
            if hasattr(mqtt, "CallbackAPIVersion"):
                self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
            else:
                self._client = mqtt.Client(client_id=client_id)


        self._client.on_message = self._on_message

    def connect(self) -> None:
        """Connects to broker and starts background loop."""
        try:
            self._client.connect(self.host, self.port, keepalive=60)
            self._client.loop_start()
            logger.info("Connected to MQTT broker at %s:%s", self.host, self.port)
        except Exception as exc:
            logger.warning("Failed to connect to MQTT broker %s:%s: %s", self.host, self.port, exc)

    def disconnect(self) -> None:
        """Stops background loop and disconnects."""
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as exc:
            logger.warning("Error disconnecting MQTT client: %s", exc)

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 1) -> None:
        """Serializes payload to JSON and publishes to broker."""
        body = json.dumps(payload)
        self._client.publish(topic, body, qos=qos)

    def subscribe(
        self, topic: str, callback: Callable[[str, dict[str, Any]], None], qos: int = 1
    ) -> None:
        """Registers a subscription pattern and associated callback."""
        self._callbacks.append((topic, callback))
        self._client.subscribe(topic, qos=qos)

    def _on_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Dispatches incoming message to registered subscriptions."""
        try:
            payload_str = msg.payload.decode("utf-8")
            data = json.loads(payload_str)
        except Exception as exc:
            logger.warning("Failed to decode MQTT message on %s: %s", msg.topic, exc)
            return

        for sub_topic, callback in self._callbacks:
            if mqtt.topic_matches_sub(sub_topic, msg.topic):
                try:
                    callback(msg.topic, data)
                except Exception as exc:
                    logger.error("Callback error for topic %s: %s", msg.topic, exc)
