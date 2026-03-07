from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency for sqlite-only dev envs
    psycopg = None


class DedupStore:
    def __init__(self, database: str | Path) -> None:
        database_str = str(database).strip()
        if not database_str:
            raise ValueError("Database path/URL must not be empty")
        self._driver = (
            "postgres"
            if database_str.startswith("postgres://")
            or database_str.startswith("postgresql://")
            else "sqlite"
        )
        self._connection: Any
        if self._driver == "postgres":
            if psycopg is None:
                raise RuntimeError(
                    "Postgres driver is not installed. Add `psycopg[binary]` to requirements."
                )
            self._connection = psycopg.connect(database_str, autocommit=False)
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS published_signals (
                    dedup_key TEXT PRIMARY KEY,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        else:
            db_path = Path(database_str)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(db_path)
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS published_signals (
                    dedup_key TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        self._connection.commit()

    def seen(self, dedup_key: str) -> bool:
        if self._driver == "postgres":
            cursor = self._connection.execute(
                "SELECT 1 FROM published_signals WHERE dedup_key = %s LIMIT 1",
                (dedup_key,),
            )
        else:
            cursor = self._connection.execute(
                "SELECT 1 FROM published_signals WHERE dedup_key = ? LIMIT 1", (dedup_key,)
            )
        return cursor.fetchone() is not None

    def remember(self, dedup_key: str) -> None:
        if self._driver == "postgres":
            self._connection.execute(
                "INSERT INTO published_signals(dedup_key) VALUES (%s) ON CONFLICT(dedup_key) DO NOTHING",
                (dedup_key,),
            )
        else:
            self._connection.execute(
                "INSERT OR IGNORE INTO published_signals(dedup_key) VALUES (?)", (dedup_key,)
            )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
