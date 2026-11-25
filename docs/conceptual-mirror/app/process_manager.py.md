# app/process_manager.py

Gerenciador do ciclo de vida dos subprocessos do sistema distribuído: inicia, monitora e encerra os 3 controladores, o simulador de sensores IoT e o cliente RPC.

```python
"""Gerenciamento do ciclo de vida dos subprocessos reais do sistema distribuido."""  # Docstring resumindo a responsabilidade do gerenciador de processos

import subprocess  # Permite criar, monitorar e encerrar processos filhos do SO
import sys  # Fornece sys.executable para rodar os módulos com o mesmo interpretador Python
import threading  # Fornece Lock para serializar operações de start/stop sobre processos
import time  # Usado para aguardar breve intervalo após subir os controladores

from app.config import ALL_NODES  # Tupla (1, 2, 3) com os IDs dos controladores do cluster
from app.event_bus import emit_event  # Emite eventos de log quando serviços são encerrados

SERVICE_META = {  # Metadados (componente_log, mensagem_warn) para encerramento dos serviços auxiliares
    "client": ("rpc", "Processo do Cliente RPC parado"),  # Metadados de log ao parar o cliente Pyro5
    "sensors": ("sensors", "Processo dos Sensores parado"),  # Metadados de log ao parar o simulador IoT
}  # Fecha o mapeamento SERVICE_META


class ProcessManager:  # Gerencia o mapa de subprocessos ativos e seu encerramento gracioso
    """Inicia, monitora e encerra os processos dos controladores, sensores e cliente RPC."""  # Docstring da classe ProcessManager

    def __init__(self) -> None:  # Inicializa estruturas internas de controle dos subprocessos
        self._lock = threading.Lock()  # Lock que evita condições de corrida entre start e stop simultâneos
        self._procs: dict[str, subprocess.Popen] = {}  # Dicionário mapeando chave lógica (ex.: controller_1) ao objeto Popen
        self._stopping: set[str] = set()  # Conjunto de chaves em processo de encerramento para reportar is_alive=False imediatamente

    def is_alive(self, key: str) -> bool:  # Verifica se o subprocesso identificado por `key` está em execução
        if key in self._stopping:  # Se está sendo encerrado agora, já considera inativo
            return False  # Retorna False durante o teardown
        proc = self._procs.get(key)  # Obtém o handle Popen armazenado, se existir
        return proc is not None and proc.poll() is None  # poll() retorna None enquanto o processo filho ainda está vivo

    def _start(self, key: str, cmd: list[str]) -> bool:  # Inicia um subprocesso caso ainda não esteja rodando
        with self._lock:  # Garante exclusão mútua ao verificar e criar o processo
            proc = self._procs.get(key)  # Busca processo existente para essa chave
            if proc and proc.poll() is None:  # Se já existe e está ativo, não duplica
                return False  # Indica que nenhum novo processo foi iniciado
            self._stopping.discard(key)  # Remove eventual marcação residual de encerramento
            self._procs[key] = subprocess.Popen(cmd)  # Lança o subprocesso executando o comando informado
            return True  # Confirma que o processo foi iniciado

    def _stop(self, key: str, component: str, message: str) -> bool:  # Encerra um subprocesso ativo (SIGTERM com fallback para SIGKILL)
        with self._lock:  # Sincroniza o encerramento com outras chamadas de ciclo de vida
            proc = self._procs.get(key)  # Recupera o handle do processo alvo
            if not proc or proc.poll() is not None:  # Se não existe ou já terminou, nada a fazer
                return False  # Indica que não havia processo ativo para parar
            self._stopping.add(key)  # Marca a chave como em encerramento antes de enviar o sinal
            try:  # Bloco try/finally para garantir limpeza da flag _stopping
                proc.terminate()  # Envia SIGTERM solicitando encerramento gracioso
                try:  # Aguarda o processo terminar dentro do timeout
                    proc.wait(timeout=3.0)  # Espera até 3 segundos pelo término limpo
                except subprocess.TimeoutExpired:  # Caso o processo não responda ao SIGTERM a tempo
                    proc.kill()  # Força encerramento imediato com SIGKILL
                    proc.wait(timeout=1.0)  # Aguarda até 1 segundo para coletar o status do processo morto
            finally:  # Executado sempre após tentativa de encerramento
                self._stopping.discard(key)  # Limpa a marcação de encerramento da chave
            emit_event(component, message, "WARN")  # Registra no barramento que o processo foi parado
            return True  # Confirma que o processo foi encerrado

    def start_controller(self, node_id: int, primary: bool = False) -> bool:  # Inicia o subprocesso de um controlador específico
        cmd = [sys.executable, "-m", "app.controller", "--id", str(node_id)]  # Monta comando `python -m app.controller --id <n>`
        if primary:  # Verifica se o nó deve subir como líder inicial
            cmd.append("--primary")  # Adiciona a flag --primary ao comando
        return self._start(f"controller_{node_id}", cmd)  # Dispara o subprocesso sob a chave controller_<node_id>

    def stop_controller(self, node_id: int) -> bool:  # Encerra o subprocesso de um controlador específico
        return self._stop(f"controller_{node_id}", f"controller_{node_id}", f"Processo do Controlador {node_id} parado")  # Chama _stop com chave e mensagem do nó

    def start_service(self, name: str) -> bool:  # Inicia um serviço auxiliar (`sensors` ou `client`)
        return self._start(name, [sys.executable, "-m", f"app.{name}"])  # Executa `python -m app.<name>` sob a chave `name`

    def stop_service(self, name: str) -> bool:  # Encerra um serviço auxiliar usando os metadados de SERVICE_META
        component, message = SERVICE_META[name]  # Obtém componente e mensagem de log associados ao serviço
        return self._stop(name, component, message)  # Executa o encerramento padronizado

    def start_all(self) -> None:  # Sobe todos os processos do sistema na ordem correta
        for node_id in ALL_NODES:  # Percorre os nós (1, 2, 3)
            self.start_controller(node_id, primary=(node_id == 1))  # Inicia os 3 controladores definindo o nó 1 como líder inicial
        time.sleep(1.0)  # Aguarda 1 segundo para os Daemons Pyro5 dos controladores estarem prontos
        self.start_service("sensors")  # Inicia o processo simulador de dispositivos IoT
        self.start_service("client")  # Inicia o processo cliente RPC Pyro5

    def stop_all(self) -> None:  # Encerra todos os processos do sistema na ordem inversa da inicialização
        self.stop_service("client")  # Para primeiro o cliente RPC
        self.stop_service("sensors")  # Para em seguida o simulador de sensores
        for node_id in ALL_NODES:  # Percorre os nós (1, 2, 3)
            self.stop_controller(node_id)  # Encerra cada um dos 3 controladores
```
