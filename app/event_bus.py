"""Barramento simples de logs estruturados (JSONL) com notificacao para listeners em memoria."""

from contextlib import suppress
from datetime import datetime
import json
from pathlib import Path
import threading
from typing import Callable

LOG_FILE = Path("logs/events.jsonl")
_listeners_lock = threading.Lock()
_listeners: list[Callable[[], None]] = []


def add_listener(listener: Callable[[], None]) -> None:
    with _listeners_lock:
        if listener not in _listeners:
            _listeners.append(listener)


def emit_event(component: str, message: str, level: str = "INFO") -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now().strftime("%H:%M:%S"),
        "component": component,
        "level": level,
        "message": message,
    }
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    print(f"[{entry['ts']}] [{component}] [{level}] {message}", flush=True)

    with _listeners_lock:
        listeners = list(_listeners)
    for listener in listeners:
        with suppress(Exception):
            listener()
