from __future__ import annotations

import sqlite3
from pathlib import Path


class DedupStore:
    def __init__(self, db_path: Path) -> None:
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
        cursor = self._connection.execute(
            "SELECT 1 FROM published_signals WHERE dedup_key = ? LIMIT 1", (dedup_key,)
        )
        return cursor.fetchone() is not None

    def remember(self, dedup_key: str) -> None:
        self._connection.execute(
            "INSERT OR IGNORE INTO published_signals(dedup_key) VALUES (?)", (dedup_key,)
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
