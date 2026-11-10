from __future__ import annotations

import sqlite3
from typing import Optional

from src.domain.entities import PetState


class SqliteRepository:
    """SQLite implementation of IStateRepository with singleton pet_state and event_log."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
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
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_log (
                    command_id TEXT PRIMARY KEY,
                    tipo_evento TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    registrado_em TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def get_state(self) -> Optional[PetState]:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT racao_gramas, agua_ml, areia_usos, status_fuga, alarme_ativo, atualizado_em
                FROM pet_state
                WHERE id = 1;
                """
            )
            row = cursor.fetchone()
            if row is None:
                return None

            return PetState(
                racao_gramas=float(row["racao_gramas"]),
                agua_ml=float(row["agua_ml"]),
                areia_usos=int(row["areia_usos"]),
                status_fuga=str(row["status_fuga"]),
                alarme_ativo=bool(row["alarme_ativo"]),
                atualizado_em=str(row["atualizado_em"]),
            )

    def save_state(self, state: PetState) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO pet_state (id, racao_gramas, agua_ml, areia_usos, status_fuga, alarme_ativo, atualizado_em)
                VALUES (1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    racao_gramas = excluded.racao_gramas,
                    agua_ml = excluded.agua_ml,
                    areia_usos = excluded.areia_usos,
                    status_fuga = excluded.status_fuga,
                    alarme_ativo = excluded.alarme_ativo,
                    atualizado_em = excluded.atualizado_em;
                """,
                (
                    state.racao_gramas,
                    state.agua_ml,
                    state.areia_usos,
                    state.status_fuga,
                    1 if state.alarme_ativo else 0,
                    state.atualizado_em,
                ),
            )
            conn.commit()

    def record_event(
        self, command_id: str, tipo_evento: str, payload: str, registrado_em: str
    ) -> bool:
        with self._get_connection() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO event_log (command_id, tipo_evento, payload, registrado_em)
                    VALUES (?, ?, ?, ?);
                    """,
                    (command_id, tipo_evento, payload, registrado_em),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def has_event(self, command_id: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM event_log WHERE command_id = ?;", (command_id,)
            )
            return cursor.fetchone() is not None
