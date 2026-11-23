# app/storage.py

Módulo de persistência local em SQLite por réplica, responsável por armazenar o estado único do pet (`pet_state`) e o histórico idempotente de comandos/eventos (`event_log`).

```python
import sqlite3

from app.domain import PetState, utc_now_iso

STATE_COLUMNS = ("racao_gramas", "agua_ml", "areia_usos", "status_fuga", "alarme_ativo", "atualizado_em")


class NodeStorage:
    """Persistencia local SQLite por replica (pet_state + event_log)."""

    def __init__(self, db_path: str) -> None:
        # Armazena o caminho do arquivo SQLite da réplica e inicializa o schema do banco de dados.
        pass

    def _connect(self) -> sqlite3.Connection:
        # Abre uma conexão SQLite com timeout configurado para evitar travamentos por concorrência
        # e define `row_factory` para retornar linhas acessíveis por nome de coluna.
        pass

    def _init_schema(self) -> None:
        # Cria as tabelas `pet_state` (linha única com id=1) e `event_log` (chave primária `command_id`)
        # caso não existam, e insere o estado inicial padrão de `PetState` de forma idempotente.
        pass

    def load_state(self) -> PetState:
        # Consulta a linha única (id=1) da tabela `pet_state` e reconstrói o objeto `PetState` correspondente.
        pass

    def save_state(self, state: PetState) -> None:
        # Atualiza todas as colunas definidas em `STATE_COLUMNS` na linha única da tabela `pet_state`
        # com os valores serializados do estado informado.
        pass

    def has_event(self, command_id: str) -> bool:
        # Verifica na tabela `event_log` se já existe um registro para o `command_id` informado,
        # servindo de base para a garantia de idempotência no processamento de comandos.
        pass

    def record_event(self, command_id: str, kind: str, payload: str) -> None:
        # Registra um comando ou evento de telemetria no `event_log` com timestamp UTC atual,
        # ignorando inserções duplicadas pelo mesmo `command_id`.
        pass

    def get_snapshot(self) -> dict:
        # Carrega o estado atual, calcula seu hash canônico e consulta a contagem total e os 5 eventos
        # mais recentes do `event_log` para exibição e auditoria da réplica.
        pass

    def update_field_directly(self, field: str, value) -> dict:
        # Valida se o campo pertence às colunas editáveis de estado e executa um UPDATE direto no SQLite,
        # permitindo simular corrupção ou divergência manual de dados em uma réplica específica.
        pass
```

