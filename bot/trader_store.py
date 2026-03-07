from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


STATUS_ACTIVE = "ACTIVE"
STATUS_PAUSED = "PAUSED"


@dataclass(frozen=True)
class TrackedTrader:
    address: str
    label: str | None
    source: str
    status: str
    auto_discovered: bool
    manual_status_override: bool
    trades_24h: int | None
    active_hours_24h: int | None
    trades_7d: int | None
    trades_30d: int | None
    active_days_30d: int | None
    first_fill_time: int | None
    last_fill_time: int | None
    age_days: float | None
    volume_usd_30d: float | None
    realized_pnl_30d: float | None
    fees_30d: float | None
    win_rate_30d: float | None
    long_ratio_30d: float | None
    avg_notional_30d: float | None
    max_notional_30d: float | None
    account_value: float | None
    total_ntl_pos: float | None
    total_margin_used: float | None
    score: float | None
    stats_json: str | None
    last_metrics_at: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DiscoveryRun:
    id: int
    started_at: str
    finished_at: str
    source: str
    status: str
    candidates: int
    qualified: int
    upserted: int
    error_message: str | None


@dataclass(frozen=True)
class ChatSubscription:
    chat_id: str
    trader_address: str
    trader_label: str | None
    status: str
    created_at: str
    updated_at: str


class TraderStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(db_path)
        self._connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_traders (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')),
                auto_discovered INTEGER NOT NULL DEFAULT 0,
                manual_status_override INTEGER NOT NULL DEFAULT 0,
                trades_24h INTEGER,
                active_hours_24h INTEGER,
                trades_7d INTEGER,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                first_fill_time INTEGER,
                last_fill_time INTEGER,
                age_days REAL,
                volume_usd_30d REAL,
                realized_pnl_30d REAL,
                fees_30d REAL,
                win_rate_30d REAL,
                long_ratio_30d REAL,
                avg_notional_30d REAL,
                max_notional_30d REAL,
                account_value REAL,
                total_ntl_pos REAL,
                total_margin_used REAL,
                score REAL,
                stats_json TEXT,
                last_metrics_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        expected_columns: dict[str, str] = {
            "manual_status_override": "INTEGER NOT NULL DEFAULT 0",
            "trades_24h": "INTEGER",
            "active_hours_24h": "INTEGER",
            "trades_7d": "INTEGER",
            "trades_30d": "INTEGER",
            "active_days_30d": "INTEGER",
            "first_fill_time": "INTEGER",
            "last_fill_time": "INTEGER",
            "age_days": "REAL",
            "volume_usd_30d": "REAL",
            "realized_pnl_30d": "REAL",
            "fees_30d": "REAL",
            "win_rate_30d": "REAL",
            "long_ratio_30d": "REAL",
            "avg_notional_30d": "REAL",
            "max_notional_30d": "REAL",
            "account_value": "REAL",
            "total_ntl_pos": "REAL",
            "total_margin_used": "REAL",
            "score": "REAL",
            "stats_json": "TEXT",
            "last_metrics_at": "TEXT",
        }

        existing_columns = {
            str(row["name"])
            for row in self._connection.execute("PRAGMA table_info(tracked_traders)").fetchall()
        }
        for column, ddl in expected_columns.items():
            if column in existing_columns:
                continue
            self._connection.execute(f"ALTER TABLE tracked_traders ADD COLUMN {column} {ddl}")

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_status
            ON tracked_traders(status)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_score
            ON tracked_traders(score DESC)
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ok', 'error')),
                candidates INTEGER NOT NULL DEFAULT 0,
                qualified INTEGER NOT NULL DEFAULT 0,
                upserted INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_discovery_runs_started_at
            ON discovery_runs(started_at DESC)
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS subscription_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trader_address TEXT NOT NULL,
                client_ip TEXT,
                user_agent TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscription_requests_trader_created
            ON subscription_requests(trader_address, created_at DESC)
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_trader_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')) DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, trader_address),
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_trader_status
            ON telegram_trader_subscriptions(trader_address, status)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_chat_status
            ON telegram_trader_subscriptions(chat_id, status)
            """
        )
        self._connection.commit()

    @staticmethod
    def normalize_address(address: str) -> str:
        return address.strip().lower()

    @staticmethod
    def _row_to_model(row: sqlite3.Row) -> TrackedTrader:
        return TrackedTrader(
            address=row["address"],
            label=row["label"],
            source=row["source"],
            status=row["status"],
            auto_discovered=bool(row["auto_discovered"]),
            manual_status_override=bool(row["manual_status_override"]),
            trades_24h=row["trades_24h"],
            active_hours_24h=row["active_hours_24h"],
            trades_7d=row["trades_7d"],
            trades_30d=row["trades_30d"],
            active_days_30d=row["active_days_30d"],
            first_fill_time=row["first_fill_time"],
            last_fill_time=row["last_fill_time"],
            age_days=row["age_days"],
            volume_usd_30d=row["volume_usd_30d"],
            realized_pnl_30d=row["realized_pnl_30d"],
            fees_30d=row["fees_30d"],
            win_rate_30d=row["win_rate_30d"],
            long_ratio_30d=row["long_ratio_30d"],
            avg_notional_30d=row["avg_notional_30d"],
            max_notional_30d=row["max_notional_30d"],
            account_value=row["account_value"],
            total_ntl_pos=row["total_ntl_pos"],
            total_margin_used=row["total_margin_used"],
            score=row["score"],
            stats_json=row["stats_json"],
            last_metrics_at=row["last_metrics_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_discovery_run(row: sqlite3.Row) -> DiscoveryRun:
        return DiscoveryRun(
            id=int(row["id"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            source=row["source"],
            status=row["status"],
            candidates=int(row["candidates"]),
            qualified=int(row["qualified"]),
            upserted=int(row["upserted"]),
            error_message=row["error_message"],
        )

    @staticmethod
    def _row_to_chat_subscription(row: sqlite3.Row) -> ChatSubscription:
        return ChatSubscription(
            chat_id=str(row["chat_id"]),
            trader_address=row["trader_address"],
            trader_label=row["trader_label"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def list_traders(self, *, limit: int = 500) -> list[TrackedTrader]:
        rows = self._connection.execute(
            """
            SELECT *
            FROM tracked_traders
            ORDER BY
                CASE status WHEN 'ACTIVE' THEN 0 ELSE 1 END,
                COALESCE(score, -1) DESC,
                updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._row_to_model(row) for row in rows]

    def list_active_addresses(self, *, limit: int = 100) -> list[str]:
        rows = self._connection.execute(
            """
            SELECT address
            FROM tracked_traders
            WHERE status = ?
            ORDER BY COALESCE(score, -1) DESC, updated_at DESC
            LIMIT ?
            """,
            (STATUS_ACTIVE, limit),
        ).fetchall()
        return [str(row["address"]) for row in rows]

    def get_trader(self, *, address: str) -> TrackedTrader | None:
        normalized = self.normalize_address(address)
        row = self._connection.execute(
            """
            SELECT *
            FROM tracked_traders
            WHERE address = ?
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_model(row)

    def subscribe_chat_to_trader(self, *, chat_id: str | int, trader_address: str) -> None:
        normalized = self.normalize_address(trader_address)
        if not normalized:
            raise ValueError("Trader address must not be empty")
        if self.get_trader(address=normalized) is None:
            raise ValueError("Trader does not exist")

        self._connection.execute(
            """
            INSERT INTO telegram_trader_subscriptions(chat_id, trader_address, status, updated_at)
            VALUES (?, ?, 'ACTIVE', CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id, trader_address) DO UPDATE SET
                status = 'ACTIVE',
                updated_at = CURRENT_TIMESTAMP
            """,
            (str(chat_id), normalized),
        )
        self._connection.commit()

    def unsubscribe_chat_from_trader(self, *, chat_id: str | int, trader_address: str) -> int:
        normalized = self.normalize_address(trader_address)
        cursor = self._connection.execute(
            """
            DELETE FROM telegram_trader_subscriptions
            WHERE chat_id = ? AND trader_address = ?
            """,
            (str(chat_id), normalized),
        )
        self._connection.commit()
        return cursor.rowcount

    def list_subscriptions_for_chat(self, *, chat_id: str | int) -> list[ChatSubscription]:
        rows = self._connection.execute(
            """
            SELECT
                sub.chat_id,
                sub.trader_address,
                sub.status,
                sub.created_at,
                sub.updated_at,
                t.label AS trader_label
            FROM telegram_trader_subscriptions sub
            JOIN tracked_traders t ON t.address = sub.trader_address
            WHERE sub.chat_id = ?
            ORDER BY
                CASE sub.status WHEN 'ACTIVE' THEN 0 ELSE 1 END,
                t.label ASC,
                sub.trader_address ASC
            """,
            (str(chat_id),),
        ).fetchall()
        return [self._row_to_chat_subscription(row) for row in rows]

    def list_active_subscriber_chat_ids_by_trader(self) -> dict[str, list[str]]:
        rows = self._connection.execute(
            """
            SELECT trader_address, chat_id
            FROM telegram_trader_subscriptions
            WHERE status = 'ACTIVE'
            ORDER BY trader_address ASC, chat_id ASC
            """
        ).fetchall()
        mapping: dict[str, list[str]] = {}
        for row in rows:
            address = str(row["trader_address"])
            mapping.setdefault(address, []).append(str(row["chat_id"]))
        return mapping

    def add_manual(self, *, address: str, label: str | None = None) -> None:
        normalized = self.normalize_address(address)
        if not normalized:
            raise ValueError("Address must not be empty")

        self._connection.execute(
            """
            INSERT INTO tracked_traders(
                address, label, source, status, auto_discovered, manual_status_override, updated_at
            )
            VALUES (?, ?, 'manual', ?, 0, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(address) DO UPDATE SET
                label = COALESCE(excluded.label, tracked_traders.label),
                source = tracked_traders.source,
                status = tracked_traders.status,
                manual_status_override = 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized, (label or None), STATUS_ACTIVE),
        )
        self._connection.commit()

    def upsert_discovered(
        self,
        *,
        address: str,
        label: str | None,
        source: str,
        trades_24h: int,
        active_hours_24h: int,
        trades_7d: int,
        trades_30d: int,
        active_days_30d: int,
        first_fill_time: int | None,
        last_fill_time: int | None,
        age_days: float | None,
        volume_usd_30d: float,
        realized_pnl_30d: float,
        fees_30d: float,
        win_rate_30d: float | None,
        long_ratio_30d: float | None,
        avg_notional_30d: float | None,
        max_notional_30d: float | None,
        account_value: float | None,
        total_ntl_pos: float | None,
        total_margin_used: float | None,
        score: float,
        stats_json: str | None,
    ) -> None:
        normalized = self.normalize_address(address)
        if not normalized:
            return

        self._connection.execute(
            """
            INSERT INTO tracked_traders(
                address, label, source, status, auto_discovered, manual_status_override,
                trades_24h, active_hours_24h, trades_7d, trades_30d, active_days_30d,
                first_fill_time, last_fill_time, age_days,
                volume_usd_30d, realized_pnl_30d, fees_30d, win_rate_30d, long_ratio_30d,
                avg_notional_30d, max_notional_30d,
                account_value, total_ntl_pos, total_margin_used,
                score, stats_json, last_metrics_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(address) DO UPDATE SET
                label = COALESCE(excluded.label, tracked_traders.label),
                source = excluded.source,
                auto_discovered = 1,
                status = CASE
                    WHEN tracked_traders.manual_status_override = 0 THEN 'PAUSED'
                    ELSE tracked_traders.status
                END,
                trades_24h = excluded.trades_24h,
                active_hours_24h = excluded.active_hours_24h,
                trades_7d = excluded.trades_7d,
                trades_30d = excluded.trades_30d,
                active_days_30d = excluded.active_days_30d,
                first_fill_time = COALESCE(tracked_traders.first_fill_time, excluded.first_fill_time),
                last_fill_time = COALESCE(excluded.last_fill_time, tracked_traders.last_fill_time),
                age_days = excluded.age_days,
                volume_usd_30d = excluded.volume_usd_30d,
                realized_pnl_30d = excluded.realized_pnl_30d,
                fees_30d = excluded.fees_30d,
                win_rate_30d = excluded.win_rate_30d,
                long_ratio_30d = excluded.long_ratio_30d,
                avg_notional_30d = excluded.avg_notional_30d,
                max_notional_30d = excluded.max_notional_30d,
                account_value = excluded.account_value,
                total_ntl_pos = excluded.total_ntl_pos,
                total_margin_used = excluded.total_margin_used,
                score = excluded.score,
                stats_json = excluded.stats_json,
                last_metrics_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized,
                (label or None),
                source,
                STATUS_PAUSED,
                trades_24h,
                active_hours_24h,
                trades_7d,
                trades_30d,
                active_days_30d,
                first_fill_time,
                last_fill_time,
                age_days,
                volume_usd_30d,
                realized_pnl_30d,
                fees_30d,
                win_rate_30d,
                long_ratio_30d,
                avg_notional_30d,
                max_notional_30d,
                account_value,
                total_ntl_pos,
                total_margin_used,
                score,
                stats_json,
            ),
        )
        self._connection.commit()

    def set_status(self, *, address: str, status: str) -> None:
        normalized = self.normalize_address(address)
        if status not in {STATUS_ACTIVE, STATUS_PAUSED}:
            raise ValueError(f"Unsupported status: {status}")
        self._connection.execute(
            """
            UPDATE tracked_traders
            SET status = ?, manual_status_override = 1, updated_at = CURRENT_TIMESTAMP
            WHERE address = ?
            """,
            (status, normalized),
        )
        self._connection.commit()

    def delete(self, *, address: str) -> None:
        normalized = self.normalize_address(address)
        self._connection.execute(
            "DELETE FROM tracked_traders WHERE address = ?", (normalized,)
        )
        self._connection.commit()

    def touch_last_fill_times(self, updates: Iterable[tuple[str, int]]) -> None:
        payload = [
            (timestamp, self.normalize_address(address))
            for address, timestamp in updates
            if self.normalize_address(address)
        ]
        if not payload:
            return

        self._connection.executemany(
            """
            UPDATE tracked_traders
            SET last_fill_time = MAX(COALESCE(last_fill_time, 0), ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE address = ?
            """,
            payload,
        )
        self._connection.commit()

    def log_discovery_run(
        self,
        *,
        source: str,
        status: str,
        candidates: int,
        qualified: int,
        upserted: int,
        error_message: str | None = None,
    ) -> None:
        if status not in {"ok", "error"}:
            raise ValueError(f"Unsupported discovery status: {status}")
        self._connection.execute(
            """
            INSERT INTO discovery_runs(
                started_at, finished_at, source, status, candidates, qualified, upserted, error_message
            )
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
            """,
            (source, status, candidates, qualified, upserted, error_message),
        )
        self._connection.commit()

    def list_recent_discovery_runs(self, *, limit: int = 30) -> list[DiscoveryRun]:
        rows = self._connection.execute(
            """
            SELECT *
            FROM discovery_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._row_to_discovery_run(row) for row in rows]

    def prune_auto_discovered(
        self,
        *,
        source: str,
        keep_addresses: Iterable[str],
    ) -> int:
        keep = [self.normalize_address(address) for address in keep_addresses if self.normalize_address(address)]
        if keep:
            placeholders = ",".join("?" for _ in keep)
            cursor = self._connection.execute(
                f"""
                DELETE FROM tracked_traders
                WHERE auto_discovered = 1
                  AND manual_status_override = 0
                  AND source = ?
                  AND address NOT IN ({placeholders})
                """,
                (source, *keep),
            )
        else:
            cursor = self._connection.execute(
                """
                DELETE FROM tracked_traders
                WHERE auto_discovered = 1
                  AND manual_status_override = 0
                  AND source = ?
                """,
                (source,),
            )
        self._connection.commit()
        return cursor.rowcount

    def record_subscription_request(
        self,
        *,
        trader_address: str,
        client_ip: str | None,
        user_agent: str | None,
    ) -> None:
        normalized = self.normalize_address(trader_address)
        if not normalized:
            return
        self._connection.execute(
            """
            INSERT INTO subscription_requests(trader_address, client_ip, user_agent)
            VALUES (?, ?, ?)
            """,
            (
                normalized,
                (client_ip or None),
                (user_agent or None),
            ),
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> TraderStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
