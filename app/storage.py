import sqlite3

from app.domain import PetState, utc_now_iso

STATE_COLUMNS = ("racao_gramas", "agua_ml", "areia_usos", "status_fuga", "alarme_ativo", "atualizado_em")


class NodeStorage:
    """Persistencia local SQLite por replica (pet_state + event_log)."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pet_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    racao_gramas REAL NOT NULL,
                    agua_ml REAL NOT NULL,
                    areia_usos INTEGER NOT NULL,
                    status_fuga TEXT NOT NULL,
                    alarme_ativo INTEGER NOT NULL,
                    atualizado_em TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_log (
                    command_id TEXT PRIMARY KEY,
                    tipo_evento TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    registrado_em TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO pet_state (id, racao_gramas, agua_ml, areia_usos, status_fuga, alarme_ativo, atualizado_em) "
                "VALUES (1, :racao_gramas, :agua_ml, :areia_usos, :status_fuga, :alarme_ativo, :atualizado_em)",
                PetState().to_dict(),
            )

    def load_state(self) -> PetState:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM pet_state WHERE id = 1").fetchone()
        return PetState.from_dict(dict(row))

    def save_state(self, state: PetState) -> None:
        assignments = ", ".join(f"{col} = :{col}" for col in STATE_COLUMNS)
        with self._connect() as conn:
            conn.execute(f"UPDATE pet_state SET {assignments} WHERE id = 1", state.to_dict())

    def has_event(self, command_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM event_log WHERE command_id = ?", (command_id,)).fetchone()
        return row is not None

    def record_event(self, command_id: str, kind: str, payload: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO event_log (command_id, tipo_evento, payload, registrado_em) VALUES (?, ?, ?, ?)",
                (command_id, kind, payload, utc_now_iso()),
            )

    def get_snapshot(self) -> dict:
        state = self.load_state()
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
            recent = conn.execute(
                "SELECT command_id, tipo_evento, registrado_em FROM event_log ORDER BY rowid DESC LIMIT 5"
            ).fetchall()
        return {
            "state": state.to_dict(),
            "hash": state.canonical_hash(),
            "event_count": int(total),
            "recent_events": [dict(r) for r in recent],
        }

    def update_field_directly(self, field: str, value) -> dict:
        """Permite edicao direta de um campo no SQLite (ex: simular divergencia real)."""
        if field not in STATE_COLUMNS[:-1]:
            raise ValueError(f"Campo nao editavel: {field}")
        with self._connect() as conn:
            conn.execute(f"UPDATE pet_state SET {field} = ?, atualizado_em = ? WHERE id = 1", (value, utc_now_iso()))
        return self.get_snapshot()
