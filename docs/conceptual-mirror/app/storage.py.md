# app/storage.py

Módulo de persistência local em SQLite por réplica, responsável por armazenar o estado único do pet (`pet_state`) e o histórico idempotente de comandos/eventos (`event_log`) na mesma transação.

```python
"""Persistencia local SQLite por replica (pet_state + event_log transacional)."""  # Docstring descrevendo a persistência isolada por réplica

import sqlite3  # Driver padrão do Python para banco de dados SQLite embarcado

from app.domain import PetState, utc_now_iso  # Importa o modelo de estado do domínio e o gerador de timestamp UTC

STATE_COLUMNS = ("racao_gramas", "agua_ml", "areia_usos", "status_fuga", "alarme_ativo", "atualizado_em")  # Ordem fixa das colunas persistidas na tabela pet_state
EDITABLE_COLUMNS = set(STATE_COLUMNS[:-1])  # Colunas editáveis manualmente (exclui atualizado_em, que é preenchido automaticamente)


class NodeStorage:  # Classe que encapsula o acesso ao SQLite exclusivo de um controlador
    """Encapsula o banco SQLite isolado de um controlador."""  # Docstring da classe NodeStorage

    def __init__(self, db_path: str) -> None:  # Inicializa o storage apontando para o arquivo .db da réplica
        self.db_path = db_path  # Armazena o caminho do arquivo SQLite desta réplica
        self._init_schema()  # Garante que as tabelas e a linha inicial existam ao instanciar

    def _connect(self) -> sqlite3.Connection:  # Abre uma nova conexão SQLite configurada para a réplica
        conn = sqlite3.connect(self.db_path, timeout=5.0)  # Conecta com timeout de 5s para evitar erro imediato em contenção de lock
        conn.row_factory = sqlite3.Row  # Permite acessar colunas pelo nome (dict-like) além de índice
        return conn  # Retorna a conexão pronta para uso em bloco with

    def _init_schema(self) -> None:  # Cria as tabelas necessárias e insere o estado inicial padrão se o banco estiver vazio
        cols = ", ".join(STATE_COLUMNS)  # Monta a lista de colunas separada por vírgula para o INSERT inicial
        placeholders = ", ".join(f":{col}" for col in STATE_COLUMNS)  # Monta os placeholders nomeados (:col) correspondentes
        with self._connect() as conn:  # Abre transação no SQLite (faz commit automático ao sair sem erro)
            conn.executescript(  # Executa múltiplas instruções DDL em sequência para criar o schema
                """  # Início do script SQL de criação das tabelas
                CREATE TABLE IF NOT EXISTS pet_state (  # Cria a tabela de estado único do pet se ainda não existir
                    id INTEGER PRIMARY KEY CHECK (id = 1),  # Chave primária travada em id=1 para garantir apenas 1 linha
                    racao_gramas REAL NOT NULL,  # Quantidade atual de ração em gramas
                    agua_ml REAL NOT NULL,  # Quantidade atual de água em mililitros
                    areia_usos INTEGER NOT NULL,  # Contador de usos da caixa de areia
                    status_fuga TEXT NOT NULL,  # Status do perímetro (DENTRO ou FORA)
                    alarme_ativo INTEGER NOT NULL,  # Flag booleana (0/1) indicando se o alarme está disparado
                    atualizado_em TEXT NOT NULL  # Timestamp ISO UTC da última atualização
                );  # Fim da definição da tabela pet_state
                CREATE TABLE IF NOT EXISTS event_log (  # Cria a tabela de log de eventos para auditoria e idempotência
                    command_id TEXT PRIMARY KEY,  # UUID do comando como chave primária para impedir duplicidade
                    tipo_evento TEXT NOT NULL,  # Tipo/ação do evento (ex.: LIBERAR_RACAO, TELEMETRIA_RACAO)
                    payload TEXT NOT NULL,  # Payload JSON bruto associado ao evento
                    registrado_em TEXT NOT NULL  # Timestamp ISO UTC de quando o evento foi gravado
                );  # Fim da definição da tabela event_log
                """  # Fim do script SQL
            )  # Fecha chamada executescript
            conn.execute(  # Garante a existência da linha única inicial (id=1) em pet_state
                f"INSERT OR IGNORE INTO pet_state (id, {cols}) VALUES (1, {placeholders})",  # Insere valores padrão apenas se id=1 ainda não existir
                PetState().to_dict(),  # Passa os valores default de PetState como parâmetros nomeados
            )  # Fecha chamada execute

    def load_state(self) -> PetState:  # Lê o estado atual persistido no SQLite e retorna como PetState
        with self._connect() as conn:  # Abre conexão de leitura
            row = conn.execute("SELECT * FROM pet_state WHERE id = 1").fetchone()  # Busca a única linha de estado (id=1)
        return PetState.from_dict(dict(row))  # Converte a linha SQLite em dicionário e reconstrói o objeto PetState

    def save_state(self, state: PetState, event: tuple[str, str, str] | None = None) -> None:  # Salva o estado e opcionalmente o evento na mesma transação
        """Persiste estado e (opcionalmente) o registro de idempotencia na mesma transacao."""  # Docstring explicando a garantia atômica e idempotente
        assignments = ", ".join(f"{col} = :{col}" for col in STATE_COLUMNS)  # Monta a cláusula SET col = :col para todas as colunas
        with self._connect() as conn:  # Inicia transação única no SQLite
            conn.execute(f"UPDATE pet_state SET {assignments} WHERE id = 1", state.to_dict())  # Atualiza a linha única de pet_state com o novo estado
            if event is not None:  # Verifica se há um evento associado para registrar no event_log
                command_id, kind, payload = event  # Desempacota identificador único, tipo e payload serializado
                conn.execute(  # Insere o registro de idempotência na mesma transação do UPDATE
                    "INSERT OR IGNORE INTO event_log (command_id, tipo_evento, payload, registrado_em) VALUES (?, ?, ?, ?)",  # Ignora se command_id já existir
                    (command_id, kind, payload, utc_now_iso()),  # Fornece os valores posicionais incluindo timestamp UTC atual
                )  # Fecha chamada execute

    def has_event(self, command_id: str) -> bool:  # Verifica se um command_id já foi processado e gravado anteriormente
        with self._connect() as conn:  # Abre conexão de consulta
            return conn.execute("SELECT 1 FROM event_log WHERE command_id = ?", (command_id,)).fetchone() is not None  # Retorna True se encontrar a linha

    def get_snapshot(self) -> dict:  # Retorna um resumo completo do banco da réplica para exibição no painel web
        with self._connect() as conn:  # Abre conexão para consultar estado e histórico
            row = conn.execute("SELECT * FROM pet_state WHERE id = 1").fetchone()  # Lê a linha atual de pet_state
            total = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]  # Conta o total de eventos registrados no log
            recent = conn.execute(  # Busca os 5 eventos mais recentes em ordem decrescente de inserção
                "SELECT command_id, tipo_evento, registrado_em FROM event_log ORDER BY rowid DESC LIMIT 5"  # Query usando rowid para ordenação cronológica reversa
            ).fetchall()  # Recupera todas as linhas retornadas (até 5)
        state = PetState.from_dict(dict(row))  # Reconstrói o PetState para calcular o hash canônico
        return {  # Monta o dicionário de snapshot consumido pela UI
            "state": state.to_dict(),  # Estado atual serializado
            "hash": state.canonical_hash(),  # Hash determinístico do estado atual
            "event_count": int(total),  # Quantidade total de eventos persistidos
            "recent_events": [dict(item) for item in recent],  # Lista dos últimos 5 eventos convertidos em dicts
        }  # Fecha o dicionário de retorno

    def update_field_directly(self, field: str, value) -> dict:  # Altera diretamente uma coluna no SQLite para simular corrupção de dados
        """Edita diretamente um campo no SQLite para simular corrupcao/divergencia de replica."""  # Docstring explicando o uso em testes de divergência
        if field not in EDITABLE_COLUMNS:  # Valida se o campo informado é permitido para edição direta
            raise ValueError(f"Campo nao editavel: {field}")  # Rejeita campos desconhecidos ou protegido (atualizado_em)
        with self._connect() as conn:  # Abre transação para aplicar a mutação direta
            conn.execute(f"UPDATE pet_state SET {field} = ?, atualizado_em = ? WHERE id = 1", (value, utc_now_iso()))  # Atualiza apenas o campo alvo e o timestamp
        return self.get_snapshot()  # Retorna o snapshot atualizado já com o novo hash divergente
```
