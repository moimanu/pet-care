# app/event_bus.py

Módulo de barramento de eventos e logging estruturado responsável por registrar acontecimentos em arquivo JSONL, exibi-los no console e notificar listeners em memória (como o servidor SSE).

```python
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
    # Registra uma função de callback de forma thread-safe na lista global de observadores,
    # evitando duplicatas para que seja acionada sempre que um novo evento for emitido.
    pass


def emit_event(component: str, message: str, level: str = "INFO") -> None:
    # 1. Garante a existência do diretório de logs e monta um registro estruturado com timestamp, componente, nível e mensagem.
    # 2. Persiste o evento em formato JSONL (append) no arquivo `LOG_FILE` e imprime no stdout com flush imediato.
    # 3. Obtém uma cópia thread-safe dos listeners registrados e invoca cada callback isolando eventuais exceções.
    pass
```
