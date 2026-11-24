# app/process_manager.py

Módulo responsável por gerenciar o ciclo de vida dos subprocessos reais do sistema distribuído (os 3 controladores, o simulador IoT e o cliente RPC).

```python
import subprocess
import sys
import threading
import time

from app.config import ALL_NODES
from app.event_bus import emit_event

SERVICE_META = {
    "client": ("rpc", "Processo do Cliente RPC parado"),
    "sensors": ("sensors", "Processo dos Sensores parado"),
}


class ProcessManager:
    """Inicia, monitora e encerra os processos dos controladores, sensores e cliente RPC."""

    def __init__(self) -> None:
        # Inicializa o lock de gerenciamento, o dicionário de subprocessos (`_procs`) e o conjunto `_stopping`.
        pass

    def is_alive(self, key: str) -> bool:
        # Verifica se o subprocesso identificado por `key` está em execução (`poll() is None`) e não está em parada.
        pass

    def _start(self, key: str, cmd: list[str]) -> bool:
        # Inicia um `subprocess.Popen` para `cmd` caso ainda não exista processo ativo para `key`.
        pass

    def _stop(self, key: str, component: str, message: str) -> bool:
        # Encerra graciosamente (SIGTERM com timeout de 3s e fallback para SIGKILL) o subprocesso `key`
        # e emite o evento de parada no barramento.
        pass

    def start_controller(self, node_id: int, primary: bool = False) -> bool:
        # Inicia o subprocesso `python -m app.controller --id <node_id>` (com `--primary` se aplicável).
        pass

    def stop_controller(self, node_id: int) -> bool:
        # Encerra o subprocesso do controlador especificado.
        pass

    def start_service(self, name: str) -> bool:
        # Inicia um serviço auxiliar (`"sensors"` ou `"client"`) via `python -m app.<name>`.
        pass

    def stop_service(self, name: str) -> bool:
        # Encerra o serviço auxiliar (`"sensors"` ou `"client"`) utilizando os metadados de `SERVICE_META`.
        pass

    def start_all(self) -> None:
        # Inicia os 3 controladores (nó 1 como líder inicial) e, após breve pausa, inicia `sensors` e `client`.
        pass

    def stop_all(self) -> None:
        # Interrompe primeiro `client` e `sensors`, e em seguida todos os controladores.
        pass
```

