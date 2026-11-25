"""Utilitarios compartilhados de infraestrutura: Pyro5, push assincrono, MQTT e sinais."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
import json
import signal
import threading
from typing import Callable, Iterable

import paho.mqtt.client as mqtt
import Pyro5.api

from app.config import MQTT_HOST, MQTT_KEEPALIVE, MQTT_PORT, peer_uri
from app.event_bus import emit_event


def suppress_rpc_errors():
    """Context manager semantico para ignorar falhas de rede esperadas em failover/teardown."""
    return suppress(Exception)


def rpc_proxy(uri: str, timeout: float = 1.0) -> Pyro5.api.Proxy:
    """Cria um proxy Pyro5 configurado com timeout explicito."""
    proxy = Pyro5.api.Proxy(uri)
    proxy._pyroTimeout = timeout
    return proxy


def query_cluster_nodes(
    node_ids: Iterable[int],
    method: str,
    timeout: float = 1.0,
    local: tuple[int, dict] | None = None,
) -> list[dict]:
    """Executa `method` em paralelo nos controladores (reaproveitando snapshot local se informado)."""
    local_map = {local[0]: local[1]} if local is not None else {}

    def _fetch(node_id: int) -> dict:
        if node_id in local_map:
            return {"node_id": node_id, "ok": True, "data": local_map[node_id]}
        try:
            with rpc_proxy(peer_uri(node_id), timeout=timeout) as proxy:
                return {"node_id": node_id, "ok": True, "data": getattr(proxy, method)()}
        except Exception as exc:
            return {"node_id": node_id, "ok": False, "error": str(exc)}

    ids = list(node_ids)
    with ThreadPoolExecutor(max_workers=len(ids)) as pool:
        return list(pool.map(_fetch, ids))


@Pyro5.api.expose
class PushReceiver:
    """Objeto remoto Pyro5 padrao para receber notificacoes unidirecionais (`on_push`)."""

    def __init__(self, on_push: Callable[[dict], None]) -> None:
        self._on_push = on_push

    @Pyro5.api.oneway
    def on_push(self, payload: dict) -> None:
        self._on_push(payload)


class ObserverRegistry:
    """Mantem URIs de observadores Pyro5 (`PushReceiver`) e remove automaticamente nos inativos."""

    def __init__(self, timeout: float = 1.0) -> None:
        self._timeout = timeout
        self._lock = threading.Lock()
        self._uris: set[str] = set()

    def register(self, uri: str) -> bool:
        with self._lock:
            self._uris.add(uri)
        return True

    def unregister(self, uri: str) -> bool:
        with self._lock:
            self._uris.discard(uri)
        return True

    def notify(self, payload: dict) -> None:
        with self._lock:
            targets = list(self._uris)
        if not targets:
            return

        def _push() -> None:
            dead: list[str] = []
            for uri in targets:
                try:
                    with rpc_proxy(uri, timeout=self._timeout) as proxy:
                        proxy.on_push(payload)
                except Exception:
                    dead.append(uri)
            if dead:
                with self._lock:
                    self._uris.difference_update(dead)

        threading.Thread(target=_push, daemon=True).start()


def connect_mqtt(client_id: str, on_error: Callable[[Exception], None] | None = None, **callbacks) -> mqtt.Client:
    """Inicializa e conecta um cliente MQTT em background (`loop_start`)."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    for name, fn in callbacks.items():
        if fn is not None:
            setattr(client, name, fn)
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        client.loop_start()
    except Exception as exc:
        if on_error is None:
            raise
        on_error(exc)
    return client


def publish_json(client: mqtt.Client, topic: str, payload: dict | str, qos: int = 1, log_prefix: str = "PUB") -> str:
    """Serializa payload JSON (se necessario), publica no MQTT e emite log padronizado."""
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    client.publish(topic, raw, qos=qos)
    if log_prefix:
        shown = payload if not isinstance(payload, str) else raw
        emit_event("mqtt", f"{log_prefix} {topic} -> {shown}")
    return raw


def install_shutdown_handler(handler: Callable[[int], None]) -> None:
    """Registra o mesmo callback de encerramento para SIGTERM e SIGINT."""
    def _on_signal(signum: int, _frame) -> None:
        handler(signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _on_signal)
