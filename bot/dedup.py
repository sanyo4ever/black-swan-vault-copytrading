from __future__ import annotations

import sqlite3
import time
from threading import Lock
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency for sqlite-only dev envs
    psycopg = None


class DedupStore:
    def __init__(
        self,
        database: str | Path,
        *,
        retention_days: int = 14,
        cleanup_interval_seconds: int = 900,
    ) -> None:
        database_str = str(database).strip()
        if not database_str:
            raise ValueError("Database path/URL must not be empty")
        self._retention_days = max(1, int(retention_days))
        self._cleanup_interval_seconds = max(30, int(cleanup_interval_seconds))
        self._last_cleanup_monotonic = 0.0
        self._lock = Lock()
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
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_published_signals_first_seen
                ON published_signals(first_seen_at)
                """
            )
        else:
            db_path = Path(database_str)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(db_path, check_same_thread=False)
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS published_signals (
                    dedup_key TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_published_signals_first_seen
                ON published_signals(first_seen_at)
                """
            )
        self._connection.commit()

    def seen(self, dedup_key: str) -> bool:
        with self._lock:
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
        with self._lock:
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
        self.cleanup_if_due()

    def cleanup_if_due(self) -> int:
        now_mono = time.monotonic()
        if now_mono - self._last_cleanup_monotonic < self._cleanup_interval_seconds:
            return 0
        removed = self.cleanup_expired()
        self._last_cleanup_monotonic = now_mono
        return removed

    def cleanup_expired(self, *, now: datetime | None = None) -> int:
        if now is None:
            now = datetime.now(tz=UTC)
        cutoff = now - timedelta(days=self._retention_days)

        with self._lock:
            if self._driver == "postgres":
                cursor = self._connection.execute(
                    """
                    DELETE FROM published_signals
                    WHERE first_seen_at < %s
                    """,
                    (cutoff,),
                )
            else:
                cursor = self._connection.execute(
                    """
                    DELETE FROM published_signals
                    WHERE first_seen_at < ?
                    """,
                    (cutoff.strftime("%Y-%m-%d %H:%M:%S"),),
                )
            self._connection.commit()
            return int(cursor.rowcount or 0)

    def close(self) -> None:
        with self._lock:
            self._connection.close()
