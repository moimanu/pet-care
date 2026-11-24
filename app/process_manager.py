"""Gerenciamento do ciclo de vida dos subprocessos reais do sistema distribuido."""

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
        self._lock = threading.Lock()
        self._procs: dict[str, subprocess.Popen] = {}
        self._stopping: set[str] = set()

    def is_alive(self, key: str) -> bool:
        if key in self._stopping:
            return False
        proc = self._procs.get(key)
        return proc is not None and proc.poll() is None

    def _start(self, key: str, cmd: list[str]) -> bool:
        with self._lock:
            proc = self._procs.get(key)
            if proc and proc.poll() is None:
                return False
            self._stopping.discard(key)
            self._procs[key] = subprocess.Popen(cmd)
            return True

    def _stop(self, key: str, component: str, message: str) -> bool:
        with self._lock:
            proc = self._procs.get(key)
            if not proc or proc.poll() is not None:
                return False
            self._stopping.add(key)
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1.0)
            finally:
                self._stopping.discard(key)
            emit_event(component, message, "WARN")
            return True

    def start_controller(self, node_id: int, primary: bool = False) -> bool:
        cmd = [sys.executable, "-m", "app.controller", "--id", str(node_id)]
        if primary:
            cmd.append("--primary")
        return self._start(f"controller_{node_id}", cmd)

    def stop_controller(self, node_id: int) -> bool:
        return self._stop(f"controller_{node_id}", f"controller_{node_id}", f"Processo do Controlador {node_id} parado")

    def start_service(self, name: str) -> bool:
        return self._start(name, [sys.executable, "-m", f"app.{name}"])

    def stop_service(self, name: str) -> bool:
        component, message = SERVICE_META[name]
        return self._stop(name, component, message)

    def start_all(self) -> None:
        for node_id in ALL_NODES:
            self.start_controller(node_id, primary=(node_id == 1))
        time.sleep(1.0)
        self.start_service("sensors")
        self.start_service("client")

    def stop_all(self) -> None:
        self.stop_service("client")
        self.stop_service("sensors")
        for node_id in ALL_NODES:
            self.stop_controller(node_id)
