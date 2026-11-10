from __future__ import annotations

import sqlite3
from typing import Optional

from src.domain.entities import PetState


class SqliteRepository:
    """SQLite implementation of StateRepository with singleton pet_state and event_log."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript(
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
                CREATE TABLE IF NOT EXISTS event_log (
                    command_id TEXT PRIMARY KEY,
                    tipo_evento TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    registrado_em TEXT NOT NULL
                );
                """
            )

    def get_state(self) -> Optional[PetState]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT racao_gramas, agua_ml, areia_usos, status_fuga, alarme_ativo, atualizado_em "
                "FROM pet_state WHERE id = 1;"
            ).fetchone()
            return PetState.from_mapping(dict(row)) if row else None

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
                    int(state.alarme_ativo),
                    state.atualizado_em,
                ),
            )

    def record_event(
        self, command_id: str, tipo_evento: str, payload: str, registrado_em: str
    ) -> bool:
        with self._get_connection() as conn:
            try:
                conn.execute(
                    "INSERT INTO event_log (command_id, tipo_evento, payload, registrado_em) VALUES (?, ?, ?, ?);",
                    (command_id, tipo_evento, payload, registrado_em),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def has_event(self, command_id: str) -> bool:
        with self._get_connection() as conn:
            return conn.execute(
                "SELECT 1 FROM event_log WHERE command_id = ?;", (command_id,)
            ).fetchone() is not None
