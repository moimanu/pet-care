# app/storage.py

Módulo de persistência local em SQLite por réplica, responsável por armazenar o estado único do pet (`pet_state`) e o histórico idempotente de comandos/eventos (`event_log`) na mesma transação.

```python
import sqlite3

from app.domain import PetState, utc_now_iso

STATE_COLUMNS = ("racao_gramas", "agua_ml", "areia_usos", "status_fuga", "alarme_ativo", "atualizado_em")
EDITABLE_COLUMNS = set(STATE_COLUMNS[:-1])


class NodeStorage:
    """Encapsula o banco SQLite isolado de um controlador."""

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

    def save_state(self, state: PetState, event: tuple[str, str, str] | None = None) -> None:
        # Atualiza todas as colunas de `STATE_COLUMNS` na linha única da tabela `pet_state` e,
        # caso `event` `(command_id, kind, payload)` seja fornecido, insere o registro em `event_log`
        # na mesma transação (com `INSERT OR IGNORE`) para garantir atomicidade e idempotência.
        pass

    def has_event(self, command_id: str) -> bool:
        # Verifica na tabela `event_log` se já existe um registro para o `command_id` informado,
        # servindo de base para a garantia de idempotência no processamento de comandos.
        pass

    def get_snapshot(self) -> dict:
        # Carrega o estado atual, calcula seu hash canônico e consulta a contagem total e os 5 eventos
        # mais recentes do `event_log` para exibição e auditoria da réplica.
        pass

    def update_field_directly(self, field: str, value) -> dict:
        # Valida se o campo pertence a `EDITABLE_COLUMNS` e executa um UPDATE direto no SQLite,
        # permitindo simular corrupção ou divergência manual de dados em uma réplica específica.
        pass
```
