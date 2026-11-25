# app/event_bus.py

Barramento simples de logs estruturados em JSONL (`logs/events.jsonl`) com saída no console e notificação thread-safe para listeners em memória (usado pelo stream SSE).

```python
"""Barramento simples de logs estruturados (JSONL) com notificacao para listeners em memoria."""  # Docstring descrevendo o propósito do barramento de eventos

from contextlib import suppress  # Utilitário para ignorar exceções pontuais ao notificar listeners
from datetime import datetime  # Usado para formatar o timestamp local de cada evento de log
import json  # Serializa cada entrada de log como uma linha JSON válida
from pathlib import Path  # Manipulação segura de caminhos para o arquivo de log
import threading  # Fornece Lock para proteger acesso concorrente à lista de listeners
from typing import Callable  # Tipagem para funções callback registradas como listeners

LOG_FILE = Path("logs/events.jsonl")  # Caminho padrão do arquivo de log em formato JSON Lines
_listeners_lock = threading.Lock()  # Mutex que sincroniza leitura e escrita na lista de listeners
_listeners: list[Callable[[], None]] = []  # Lista em memória de callbacks notificados a cada novo evento


def add_listener(listener: Callable[[], None]) -> None:  # Registra um callback para ser avisado sempre que um evento for emitido
    with _listeners_lock:  # Adquire o lock antes de inspecionar e modificar a lista global
        if listener not in _listeners:  # Evita registrar o mesmo listener mais de uma vez
            _listeners.append(listener)  # Adiciona o callback à lista de assinantes


def emit_event(component: str, message: str, level: str = "INFO") -> None:  # Grava um evento no JSONL, imprime no stdout e notifica os listeners
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)  # Garante que o diretório logs/ exista antes de abrir o arquivo
    entry = {  # Monta o dicionário estruturado do evento
        "ts": datetime.now().strftime("%H:%M:%S"),  # Hora local formatada (HH:MM:SS) para exibição no painel
        "component": component,  # Nome do componente emissor (ex.: controller_1, rpc, mqtt)
        "level": level,  # Nível de severidade do log (INFO, WARN, ERROR)
        "message": message,  # Mensagem descritiva do evento ocorrido
    }  # Fecha o dicionário da entrada de log
    with LOG_FILE.open("a", encoding="utf-8") as fh:  # Abre o arquivo JSONL em modo append com codificação UTF-8
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")  # Escreve o JSON em uma única linha preservando acentuação
    print(f"[{entry['ts']}] [{component}] [{level}] {message}", flush=True)  # Imprime o mesmo evento no console com flush imediato

    with _listeners_lock:  # Adquire o lock apenas para copiar a lista atual de listeners
        listeners = list(_listeners)  # Faz cópia rasa para não segurar o lock durante as chamadas externas
    for listener in listeners:  # Percorre todos os listeners registrados
        with suppress(Exception):  # Impede que uma falha em um listener quebre a emissão de logs
            listener()  # Dispara o callback (ex.: acorda o stream SSE do servidor web)
```
