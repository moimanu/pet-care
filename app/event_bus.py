from datetime import datetime
import json
from pathlib import Path
import threading
from typing import Callable

LOG_FILE = Path("logs/events.jsonl")
_listeners_lock = threading.Lock()
_listeners: list[Callable[[], None]] = []


def add_listener(fn: Callable[[], None]) -> None:
    with _listeners_lock:
        if fn not in _listeners:
            _listeners.append(fn)


def emit_event(component: str, message: str, level: str = "INFO") -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now().strftime("%H:%M:%S"),
        "component": component,
        "level": level,
        "message": message,
    }
    line = json.dumps(entry, ensure_ascii=False)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(f"[{entry['ts']}] [{component}] [{level}] {message}", flush=True)
    with _listeners_lock:
        listeners = list(_listeners)
    for fn in listeners:
        try:
            fn()
        except Exception:
            pass

