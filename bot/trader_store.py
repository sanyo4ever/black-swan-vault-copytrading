from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency for sqlite-only dev envs
    psycopg = None
    dict_row = None

STATUS_ACTIVE = "ACTIVE"
STATUS_PAUSED = "PAUSED"
MODERATION_NEUTRAL = "NEUTRAL"
MODERATION_WHITELIST = "WHITELIST"
MODERATION_BLACKLIST = "BLACKLIST"
SUBSCRIPTION_ACTIVE = "ACTIVE"
SUBSCRIPTION_EXPIRED = "EXPIRED"
SUBSCRIPTION_CANCELLED = "CANCELLED"
SESSION_ACTIVE = "ACTIVE"
SESSION_EXPIRED = "EXPIRED"
SESSION_ERROR = "ERROR"
RETRY_PENDING = "PENDING"
RETRY_SENT = "SENT"
RETRY_DEAD = "DEAD"


@dataclass(frozen=True)
class TrackedTrader:
    address: str
    label: str | None
    source: str
    status: str
    auto_discovered: bool
    manual_status_override: bool
    moderation_state: str
    moderation_note: str | None
    moderated_at: str | None
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


@dataclass(frozen=True)
class LiveTopTrader:
    rank_position: int
    activity_score: float
    address: str
    label: str | None
    source: str
    status: str
    moderation_state: str
    age_days: float | None
    trades_30d: int | None
    active_days_30d: int | None
    win_rate_30d: float | None
    realized_pnl_30d: float | None
    volume_usd_30d: float | None
    score: float | None
    last_fill_time: int | None
    refreshed_at: str


@dataclass(frozen=True)
class CatalogTrader:
    address: str
    label: str | None
    source: str
    status: str
    moderation_state: str
    moderation_note: str | None
    age_days: float | None
    trades_24h: int | None
    active_hours_24h: int | None
    trades_7d: int | None
    trades_30d: int | None
    active_days_30d: int | None
    win_rate_30d: float | None
    realized_pnl_30d: float | None
    volume_usd_30d: float | None
    score: float | None
    activity_score: float | None
    last_fill_time: int | None
    refreshed_at: str


@dataclass(frozen=True)
class DeliveryTarget:
    session_id: int
    subscription_id: int
    chat_id: str
    trader_address: str
    message_thread_id: int | None
    expires_at: str


@dataclass(frozen=True)
class DeliverySessionInfo:
    session_id: int
    subscription_id: int
    chat_id: str
    trader_address: str
    message_thread_id: int | None
    topic_name: str | None
    expires_at: str


@dataclass(frozen=True)
class DeliveryRetryJob:
    id: int
    dedup_key: str
    chat_id: str
    trader_address: str | None
    message_thread_id: int | None
    message_text: str
    attempt_count: int
    next_attempt_at: str
    last_error: str | None


class TraderStore:
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
            self._connection = psycopg.connect(
                database_str,
                autocommit=False,
                row_factory=dict_row,
            )
        else:
            db_path = Path(database_str)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(db_path)
            self._connection.row_factory = sqlite3.Row
        self._ensure_schema()
        self._enforce_active_only_trader_status()

    def _q(self, sql: str) -> str:
        if self._driver == "postgres":
            return sql.replace("?", "%s")
        return sql

    def _execute(self, sql: str, params: Iterable[Any] | tuple[Any, ...] = ()) -> Any:
        return self._connection.execute(self._q(sql), tuple(params))

    def _executemany(self, sql: str, params_seq: Iterable[Iterable[Any]]) -> Any:
        if self._driver == "postgres":
            with self._connection.cursor() as cursor:
                cursor.executemany(self._q(sql), params_seq)
            return None
        return self._connection.executemany(self._q(sql), params_seq)

    def _insert_and_get_id(
        self,
        sql: str,
        params: Iterable[Any] | tuple[Any, ...] = (),
    ) -> int:
        if self._driver == "postgres":
            row = self._execute(f"{sql.rstrip()} RETURNING id", params).fetchone()
            if row is None:
                raise RuntimeError("Failed to fetch generated id")
            if isinstance(row, Mapping):
                return int(row["id"])
            return int(row[0])
        cursor = self._execute(sql, params)
        return int(cursor.lastrowid)

    def _ensure_schema(self) -> None:
        if self._driver == "postgres":
            self._ensure_schema_postgres()
        else:
            self._ensure_schema_sqlite()

    def _enforce_active_only_trader_status(self) -> None:
        # Current product mode: every tracked trader remains ACTIVE.
        try:
            self._execute(
                """
                UPDATE tracked_traders
                SET status = ?,
                    manual_status_override = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status <> ?
                """,
                (STATUS_ACTIVE, STATUS_ACTIVE),
            )
            self._execute(
                """
                UPDATE catalog_current
                SET status = ?
                WHERE status <> ?
                """,
                (STATUS_ACTIVE, STATUS_ACTIVE),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    def _ensure_schema_sqlite(self) -> None:
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_traders (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')),
                auto_discovered INTEGER NOT NULL DEFAULT 0,
                manual_status_override INTEGER NOT NULL DEFAULT 0,
                moderation_state TEXT NOT NULL
                    CHECK(moderation_state IN ('NEUTRAL', 'WHITELIST', 'BLACKLIST'))
                    DEFAULT 'NEUTRAL',
                moderation_note TEXT,
                moderated_at TEXT,
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
            "moderation_state": (
                "TEXT NOT NULL CHECK(moderation_state IN "
                "('NEUTRAL', 'WHITELIST', 'BLACKLIST')) DEFAULT 'NEUTRAL'"
            ),
            "moderation_note": "TEXT",
            "moderated_at": "TEXT",
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
            str(row["name"]) for row in self._execute("PRAGMA table_info(tracked_traders)").fetchall()
        }
        for column, ddl in expected_columns.items():
            if column in existing_columns:
                continue
            self._execute(f"ALTER TABLE tracked_traders ADD COLUMN {column} {ddl}")

        self._ensure_common_tables_sqlite()
        self._connection.commit()

    def _ensure_schema_postgres(self) -> None:
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_traders (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')),
                auto_discovered SMALLINT NOT NULL DEFAULT 0,
                manual_status_override SMALLINT NOT NULL DEFAULT 0,
                moderation_state TEXT NOT NULL
                    CHECK(moderation_state IN ('NEUTRAL', 'WHITELIST', 'BLACKLIST'))
                    DEFAULT 'NEUTRAL',
                moderation_note TEXT,
                moderated_at TIMESTAMP,
                trades_24h INTEGER,
                active_hours_24h INTEGER,
                trades_7d INTEGER,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                first_fill_time BIGINT,
                last_fill_time BIGINT,
                age_days DOUBLE PRECISION,
                volume_usd_30d DOUBLE PRECISION,
                realized_pnl_30d DOUBLE PRECISION,
                fees_30d DOUBLE PRECISION,
                win_rate_30d DOUBLE PRECISION,
                long_ratio_30d DOUBLE PRECISION,
                avg_notional_30d DOUBLE PRECISION,
                max_notional_30d DOUBLE PRECISION,
                account_value DOUBLE PRECISION,
                total_ntl_pos DOUBLE PRECISION,
                total_margin_used DOUBLE PRECISION,
                score DOUBLE PRECISION,
                stats_json TEXT,
                last_metrics_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for column, ddl in [
            ("manual_status_override", "SMALLINT NOT NULL DEFAULT 0"),
            (
                "moderation_state",
                (
                    "TEXT NOT NULL CHECK(moderation_state IN "
                    "('NEUTRAL', 'WHITELIST', 'BLACKLIST')) DEFAULT 'NEUTRAL'"
                ),
            ),
            ("moderation_note", "TEXT"),
            ("moderated_at", "TIMESTAMP"),
            ("trades_24h", "INTEGER"),
            ("active_hours_24h", "INTEGER"),
            ("trades_7d", "INTEGER"),
            ("trades_30d", "INTEGER"),
            ("active_days_30d", "INTEGER"),
            ("first_fill_time", "BIGINT"),
            ("last_fill_time", "BIGINT"),
            ("age_days", "DOUBLE PRECISION"),
            ("volume_usd_30d", "DOUBLE PRECISION"),
            ("realized_pnl_30d", "DOUBLE PRECISION"),
            ("fees_30d", "DOUBLE PRECISION"),
            ("win_rate_30d", "DOUBLE PRECISION"),
            ("long_ratio_30d", "DOUBLE PRECISION"),
            ("avg_notional_30d", "DOUBLE PRECISION"),
            ("max_notional_30d", "DOUBLE PRECISION"),
            ("account_value", "DOUBLE PRECISION"),
            ("total_ntl_pos", "DOUBLE PRECISION"),
            ("total_margin_used", "DOUBLE PRECISION"),
            ("score", "DOUBLE PRECISION"),
            ("stats_json", "TEXT"),
            ("last_metrics_at", "TIMESTAMP"),
        ]:
            self._execute(
                f"ALTER TABLE tracked_traders ADD COLUMN IF NOT EXISTS {column} {ddl}"
            )

        self._ensure_common_tables_postgres()
        self._connection.commit()

    def _ensure_common_tables_sqlite(self) -> None:
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_status
            ON tracked_traders(status)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_score
            ON tracked_traders(score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_last_fill
            ON tracked_traders(last_fill_time DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_status_mod
            ON tracked_traders(status, moderation_state)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_trades_30d
            ON tracked_traders(trades_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_win_rate
            ON tracked_traders(win_rate_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_pnl_30d
            ON tracked_traders(realized_pnl_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_lower_address
            ON tracked_traders(lower(address))
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_lower_label
            ON tracked_traders(lower(label))
            """
        )
        self._execute(
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
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_discovery_runs_started_at
            ON discovery_runs(started_at DESC)
            """
        )
        self._execute(
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
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscription_requests_trader_created
            ON subscription_requests(trader_address, created_at DESC)
            """
        )
        self._execute(
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
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_trader_status
            ON telegram_trader_subscriptions(trader_address, status)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_chat_status
            ON telegram_trader_subscriptions(chat_id, status)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS traders_universe (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                score REAL,
                win_rate_30d REAL,
                realized_pnl_30d REAL,
                volume_usd_30d REAL,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                age_days REAL,
                last_fill_time INTEGER,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_traders_universe_score
            ON traders_universe(score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_traders_universe_last_fill
            ON traders_universe(last_fill_time DESC)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS traders_top100_live (
                rank_position INTEGER PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                activity_score REAL NOT NULL DEFAULT 0,
                last_fill_time INTEGER,
                refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_top100_live_address
            ON traders_top100_live(address)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_current (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')),
                moderation_state TEXT NOT NULL
                    CHECK(moderation_state IN ('NEUTRAL', 'WHITELIST', 'BLACKLIST')),
                moderation_note TEXT,
                age_days REAL,
                trades_24h INTEGER,
                active_hours_24h INTEGER,
                trades_7d INTEGER,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                win_rate_30d REAL,
                realized_pnl_30d REAL,
                volume_usd_30d REAL,
                score REAL,
                activity_score REAL,
                last_fill_time INTEGER,
                refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_activity
            ON catalog_current(activity_score DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_score
            ON catalog_current(score DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_last_fill
            ON catalog_current(last_fill_time DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_trades_30d
            ON catalog_current(trades_30d DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_pnl_30d
            ON catalog_current(realized_pnl_30d DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_status_mod
            ON catalog_current(status, moderation_state)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_lower_address
            ON catalog_current(lower(address))
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_lower_label
            ON catalog_current(lower(label))
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')),
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscriptions_trader_status_exp
            ON subscriptions(trader_address, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_status_exp
            ON subscriptions(chat_id, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL UNIQUE,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                message_thread_id INTEGER,
                topic_name TEXT,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'EXPIRED', 'ERROR')),
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TEXT NOT NULL,
                closed_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_sessions_trader_status_exp
            ON delivery_sessions(trader_address, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_sessions_chat_status_exp
            ON delivery_sessions(chat_id, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_retry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedup_key TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                trader_address TEXT,
                message_thread_id INTEGER NOT NULL DEFAULT 0,
                message_text TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL CHECK(status IN ('PENDING', 'SENT', 'DEAD')) DEFAULT 'PENDING',
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dedup_key, chat_id, message_thread_id)
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_retry_queue_due
            ON delivery_retry_queue(status, next_attempt_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_retry_queue_chat
            ON delivery_retry_queue(chat_id, status)
            """
        )

    def _ensure_common_tables_postgres(self) -> None:
        try:
            self._execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        except Exception:
            self._connection.rollback()

        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_status
            ON tracked_traders(status)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_score
            ON tracked_traders(score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_last_fill
            ON tracked_traders(last_fill_time DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_status_mod
            ON tracked_traders(status, moderation_state)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_trades_30d
            ON tracked_traders(trades_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_win_rate
            ON tracked_traders(win_rate_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_pnl_30d
            ON tracked_traders(realized_pnl_30d DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_lower_address
            ON tracked_traders((lower(address)))
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_traders_lower_label
            ON tracked_traders((lower(label)))
            """
        )
        try:
            self._execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tracked_traders_label_trgm
                ON tracked_traders USING gin (label gin_trgm_ops)
                """
            )
        except Exception:
            self._connection.rollback()
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_runs (
                id BIGSERIAL PRIMARY KEY,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ok', 'error')),
                candidates INTEGER NOT NULL DEFAULT 0,
                qualified INTEGER NOT NULL DEFAULT 0,
                upserted INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_discovery_runs_started_at
            ON discovery_runs(started_at DESC)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS subscription_requests (
                id BIGSERIAL PRIMARY KEY,
                trader_address TEXT NOT NULL,
                client_ip TEXT,
                user_agent TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscription_requests_trader_created
            ON subscription_requests(trader_address, created_at DESC)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_trader_subscriptions (
                id BIGSERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')) DEFAULT 'ACTIVE',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, trader_address),
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_trader_status
            ON telegram_trader_subscriptions(trader_address, status)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_subscriptions_chat_status
            ON telegram_trader_subscriptions(chat_id, status)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS traders_universe (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                score DOUBLE PRECISION,
                win_rate_30d DOUBLE PRECISION,
                realized_pnl_30d DOUBLE PRECISION,
                volume_usd_30d DOUBLE PRECISION,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                age_days DOUBLE PRECISION,
                last_fill_time BIGINT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_traders_universe_score
            ON traders_universe(score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_traders_universe_last_fill
            ON traders_universe(last_fill_time DESC)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS traders_top100_live (
                rank_position INTEGER PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                activity_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_fill_time BIGINT,
                refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_top100_live_address
            ON traders_top100_live(address)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_current (
                address TEXT PRIMARY KEY,
                label TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'PAUSED')),
                moderation_state TEXT NOT NULL
                    CHECK(moderation_state IN ('NEUTRAL', 'WHITELIST', 'BLACKLIST')),
                moderation_note TEXT,
                age_days DOUBLE PRECISION,
                trades_24h INTEGER,
                active_hours_24h INTEGER,
                trades_7d INTEGER,
                trades_30d INTEGER,
                active_days_30d INTEGER,
                win_rate_30d DOUBLE PRECISION,
                realized_pnl_30d DOUBLE PRECISION,
                volume_usd_30d DOUBLE PRECISION,
                score DOUBLE PRECISION,
                activity_score DOUBLE PRECISION,
                last_fill_time BIGINT,
                refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_activity
            ON catalog_current(activity_score DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_score
            ON catalog_current(score DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_last_fill
            ON catalog_current(last_fill_time DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_trades_30d
            ON catalog_current(trades_30d DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_pnl_30d
            ON catalog_current(realized_pnl_30d DESC, address ASC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_status_mod
            ON catalog_current(status, moderation_state)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_lower_address
            ON catalog_current((lower(address)))
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_current_lower_label
            ON catalog_current((lower(label)))
            """
        )
        try:
            self._execute(
                """
                CREATE INDEX IF NOT EXISTS idx_catalog_current_label_trgm
                ON catalog_current USING gin (label gin_trgm_ops)
                """
            )
        except Exception:
            self._connection.rollback()
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id BIGSERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')),
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscriptions_trader_status_exp
            ON subscriptions(trader_address, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_status_exp
            ON subscriptions(chat_id, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_sessions (
                id BIGSERIAL PRIMARY KEY,
                subscription_id BIGINT NOT NULL UNIQUE,
                chat_id TEXT NOT NULL,
                trader_address TEXT NOT NULL,
                message_thread_id INTEGER,
                topic_name TEXT,
                status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'EXPIRED', 'ERROR')),
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                closed_at TIMESTAMP,
                last_error TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE,
                FOREIGN KEY(trader_address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_sessions_trader_status_exp
            ON delivery_sessions(trader_address, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_sessions_chat_status_exp
            ON delivery_sessions(chat_id, status, expires_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_retry_queue (
                id BIGSERIAL PRIMARY KEY,
                dedup_key TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                trader_address TEXT,
                message_thread_id INTEGER NOT NULL DEFAULT 0,
                message_text TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL CHECK(status IN ('PENDING', 'SENT', 'DEAD')) DEFAULT 'PENDING',
                last_error TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dedup_key, chat_id, message_thread_id)
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_retry_queue_due
            ON delivery_retry_queue(status, next_attempt_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_retry_queue_chat
            ON delivery_retry_queue(chat_id, status)
            """
        )

    @staticmethod
    def _format_datetime(value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(UTC).replace(tzinfo=None)
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return value

    @staticmethod
    def normalize_address(address: str) -> str:
        return address.strip().lower()

    @staticmethod
    def _row_to_model(row: Mapping[str, Any]) -> TrackedTrader:
        return TrackedTrader(
            address=row["address"],
            label=row["label"],
            source=row["source"],
            status=row["status"],
            auto_discovered=bool(row["auto_discovered"]),
            manual_status_override=bool(row["manual_status_override"]),
            moderation_state=row["moderation_state"],
            moderation_note=row["moderation_note"],
            moderated_at=TraderStore._format_datetime(row["moderated_at"]),
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
            last_metrics_at=TraderStore._format_datetime(row["last_metrics_at"]),
            created_at=TraderStore._format_datetime(row["created_at"]),
            updated_at=TraderStore._format_datetime(row["updated_at"]),
        )

    @staticmethod
    def _row_to_discovery_run(row: Mapping[str, Any]) -> DiscoveryRun:
        return DiscoveryRun(
            id=int(row["id"]),
            started_at=TraderStore._format_datetime(row["started_at"]),
            finished_at=TraderStore._format_datetime(row["finished_at"]),
            source=row["source"],
            status=row["status"],
            candidates=int(row["candidates"]),
            qualified=int(row["qualified"]),
            upserted=int(row["upserted"]),
            error_message=row["error_message"],
        )

    @staticmethod
    def _row_to_chat_subscription(row: Mapping[str, Any]) -> ChatSubscription:
        return ChatSubscription(
            chat_id=str(row["chat_id"]),
            trader_address=row["trader_address"],
            trader_label=row["trader_label"],
            status=row["status"],
            created_at=TraderStore._format_datetime(row["created_at"]),
            updated_at=TraderStore._format_datetime(row["updated_at"]),
        )

    @staticmethod
    def _row_to_live_top_trader(row: Mapping[str, Any]) -> LiveTopTrader:
        return LiveTopTrader(
            rank_position=int(row["rank_position"]),
            activity_score=float(row["activity_score"]),
            address=row["address"],
            label=row["label"],
            source=row["source"],
            status=row["status"],
            moderation_state=row["moderation_state"],
            age_days=row["age_days"],
            trades_30d=row["trades_30d"],
            active_days_30d=row["active_days_30d"],
            win_rate_30d=row["win_rate_30d"],
            realized_pnl_30d=row["realized_pnl_30d"],
            volume_usd_30d=row["volume_usd_30d"],
            score=row["score"],
            last_fill_time=row["last_fill_time"],
            refreshed_at=TraderStore._format_datetime(row["refreshed_at"]),
        )

    @staticmethod
    def _row_to_catalog_trader(row: Mapping[str, Any]) -> CatalogTrader:
        return CatalogTrader(
            address=row["address"],
            label=row["label"],
            source=row["source"],
            status=row["status"],
            moderation_state=row["moderation_state"],
            moderation_note=row["moderation_note"],
            age_days=row["age_days"],
            trades_24h=row["trades_24h"],
            active_hours_24h=row["active_hours_24h"],
            trades_7d=row["trades_7d"],
            trades_30d=row["trades_30d"],
            active_days_30d=row["active_days_30d"],
            win_rate_30d=row["win_rate_30d"],
            realized_pnl_30d=row["realized_pnl_30d"],
            volume_usd_30d=row["volume_usd_30d"],
            score=row["score"],
            activity_score=row["activity_score"],
            last_fill_time=row["last_fill_time"],
            refreshed_at=TraderStore._format_datetime(row["refreshed_at"]),
        )

    @staticmethod
    def _row_to_delivery_target(row: Mapping[str, Any]) -> DeliveryTarget:
        return DeliveryTarget(
            session_id=int(row["session_id"]),
            subscription_id=int(row["subscription_id"]),
            chat_id=str(row["chat_id"]),
            trader_address=row["trader_address"],
            message_thread_id=(
                int(row["message_thread_id"])
                if row["message_thread_id"] is not None
                else None
            ),
            expires_at=TraderStore._format_datetime(row["expires_at"]),
        )

    @staticmethod
    def _row_to_delivery_session_info(row: Mapping[str, Any]) -> DeliverySessionInfo:
        return DeliverySessionInfo(
            session_id=int(row["session_id"]),
            subscription_id=int(row["subscription_id"]),
            chat_id=str(row["chat_id"]),
            trader_address=row["trader_address"],
            message_thread_id=(
                int(row["message_thread_id"])
                if row["message_thread_id"] is not None
                else None
            ),
            topic_name=row["topic_name"],
            expires_at=TraderStore._format_datetime(row["expires_at"]),
        )

    @staticmethod
    def _row_to_delivery_retry_job(row: Mapping[str, Any]) -> DeliveryRetryJob:
        thread_raw = int(row["message_thread_id"] or 0)
        return DeliveryRetryJob(
            id=int(row["id"]),
            dedup_key=str(row["dedup_key"]),
            chat_id=str(row["chat_id"]),
            trader_address=(str(row["trader_address"]) if row["trader_address"] else None),
            message_thread_id=(thread_raw if thread_raw > 0 else None),
            message_text=str(row["message_text"]),
            attempt_count=int(row["attempt_count"] or 0),
            next_attempt_at=TraderStore._format_datetime(row["next_attempt_at"]),
            last_error=(str(row["last_error"]) if row["last_error"] else None),
        )

    def list_traders(self, *, limit: int = 500) -> list[TrackedTrader]:
        rows = self._execute(
            """
            SELECT *
            FROM tracked_traders
            ORDER BY
                COALESCE(score, -1) DESC,
                updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._row_to_model(row) for row in rows]

    def list_active_addresses(self, *, limit: int = 100) -> list[str]:
        rows = self._execute(
            """
            SELECT address
            FROM tracked_traders
            WHERE COALESCE(moderation_state, ?) <> ?
            ORDER BY COALESCE(score, -1) DESC, updated_at DESC
            LIMIT ?
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST, limit),
        ).fetchall()
        return [str(row["address"]) for row in rows]

    def list_active_subscription_addresses(self, *, limit: int = 200) -> list[str]:
        rows = self._execute(
            """
            SELECT DISTINCT s.trader_address AS address
            FROM subscriptions s
            JOIN tracked_traders t ON t.address = s.trader_address
            WHERE s.status = 'ACTIVE'
              AND s.expires_at > CURRENT_TIMESTAMP
              AND COALESCE(t.moderation_state, ?) <> ?
            ORDER BY s.trader_address ASC
            LIMIT ?
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST, limit),
        ).fetchall()
        return [str(row["address"]) for row in rows]

    def list_monitored_addresses(self, *, limit: int = 200) -> list[str]:
        addresses: list[str] = []
        addresses.extend(self.list_active_subscription_addresses(limit=limit))
        if len(addresses) < limit:
            addresses.extend(self.list_active_addresses(limit=limit))
        dedup: list[str] = []
        seen: set[str] = set()
        for address in addresses:
            normalized = self.normalize_address(address)
            if not normalized or normalized in seen:
                continue
            dedup.append(normalized)
            seen.add(normalized)
            if len(dedup) >= limit:
                break
        return dedup

    def refresh_traders_universe_from_tracked(
        self,
        *,
        min_age_days: int,
        min_trades_30d: int,
        min_win_rate_30d: float,
        min_realized_pnl_30d: float,
        min_score: float,
        max_size: int = 3000,
    ) -> int:
        rows = self._execute(
            """
            SELECT
                address,
                label,
                source,
                score,
                win_rate_30d,
                realized_pnl_30d,
                volume_usd_30d,
                trades_30d,
                active_days_30d,
                age_days,
                last_fill_time
            FROM tracked_traders
            WHERE
                COALESCE(moderation_state, ?) <> ?
                AND
                COALESCE(age_days, 0) >= ?
                AND COALESCE(trades_30d, 0) >= ?
                AND COALESCE(win_rate_30d, 0) >= ?
                AND COALESCE(realized_pnl_30d, -1000000000) >= ?
                AND COALESCE(score, -1000000000) >= ?
            ORDER BY COALESCE(score, -1000000000) DESC, COALESCE(last_fill_time, 0) DESC
            LIMIT ?
            """,
            (
                MODERATION_NEUTRAL,
                MODERATION_BLACKLIST,
                float(min_age_days),
                int(min_trades_30d),
                float(min_win_rate_30d),
                float(min_realized_pnl_30d),
                float(min_score),
                int(max_size),
            ),
        ).fetchall()

        payload = [
            (
                str(row["address"]),
                row["label"],
                str(row["source"]),
                row["score"],
                row["win_rate_30d"],
                row["realized_pnl_30d"],
                row["volume_usd_30d"],
                row["trades_30d"],
                row["active_days_30d"],
                row["age_days"],
                row["last_fill_time"],
            )
            for row in rows
        ]

        keep = [item[0] for item in payload]
        try:
            self._executemany(
                """
                INSERT INTO traders_universe(
                    address,
                    label,
                    source,
                    score,
                    win_rate_30d,
                    realized_pnl_30d,
                    volume_usd_30d,
                    trades_30d,
                    active_days_30d,
                    age_days,
                    last_fill_time,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(address) DO UPDATE SET
                    label = excluded.label,
                    source = excluded.source,
                    score = excluded.score,
                    win_rate_30d = excluded.win_rate_30d,
                    realized_pnl_30d = excluded.realized_pnl_30d,
                    volume_usd_30d = excluded.volume_usd_30d,
                    trades_30d = excluded.trades_30d,
                    active_days_30d = excluded.active_days_30d,
                    age_days = excluded.age_days,
                    last_fill_time = excluded.last_fill_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                payload,
            )
            if keep:
                placeholders = ",".join("?" for _ in keep)
                self._execute(
                    f"DELETE FROM traders_universe WHERE address NOT IN ({placeholders})",
                    keep,
                )
            else:
                self._execute("DELETE FROM traders_universe")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return len(payload)

    def refresh_top100_live(
        self,
        *,
        max_rows: int = 100,
        active_window_minutes: int = 60,
    ) -> int:
        now = datetime.now(tz=UTC)
        now_ms = int(now.timestamp() * 1000)
        cutoff_ms = now_ms - (active_window_minutes * 60 * 1000)

        rows = self._execute(
            """
            SELECT
                address,
                score,
                trades_30d,
                last_fill_time
            FROM traders_universe
            WHERE COALESCE(last_fill_time, 0) >= ?
            """,
            (cutoff_ms,),
        ).fetchall()

        ranked: list[tuple[str, float, int | None]] = []
        for row in rows:
            address = str(row["address"])
            score = float(row["score"] or 0.0)
            trades_30d = int(row["trades_30d"] or 0)
            last_fill_time = int(row["last_fill_time"]) if row["last_fill_time"] is not None else None
            if last_fill_time is None:
                continue

            age_minutes = max(0.0, (now_ms - last_fill_time) / 60000.0)
            recency_component = max(0.0, 1.0 - (age_minutes / max(1, active_window_minutes))) * 40.0
            frequency_component = min(30.0, trades_30d / 12.0)
            quality_component = min(30.0, max(0.0, score))
            activity_score = recency_component + frequency_component + quality_component
            ranked.append((address, round(activity_score, 4), last_fill_time))

        ranked.sort(key=lambda item: (item[1], item[2]), reverse=True)
        ranked = ranked[: max_rows]

        try:
            self._execute("DELETE FROM traders_top100_live")
            self._executemany(
                """
                INSERT INTO traders_top100_live(
                    rank_position, address, activity_score, last_fill_time, refreshed_at
                )
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (idx + 1, item[0], item[1], item[2])
                    for idx, item in enumerate(ranked)
                ],
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return len(ranked)

    def refresh_catalog_current(self, *, activity_window_minutes: int = 60) -> int:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        rows = self._execute(
            """
            SELECT
                address,
                label,
                source,
                status,
                moderation_state,
                moderation_note,
                age_days,
                trades_24h,
                active_hours_24h,
                trades_7d,
                trades_30d,
                active_days_30d,
                win_rate_30d,
                realized_pnl_30d,
                volume_usd_30d,
                score,
                last_fill_time
            FROM tracked_traders
            """
        ).fetchall()

        payload: list[tuple[Any, ...]] = []
        for row in rows:
            score = float(row["score"] or 0.0)
            trades_30d = int(row["trades_30d"] or 0)
            last_fill = int(row["last_fill_time"]) if row["last_fill_time"] is not None else None
            if last_fill is None:
                recency_component = 0.0
            else:
                age_minutes = max(0.0, (now_ms - last_fill) / 60000.0)
                recency_component = (
                    max(0.0, 1.0 - (age_minutes / max(1, activity_window_minutes))) * 40.0
                )
            frequency_component = min(30.0, trades_30d / 12.0)
            quality_component = min(30.0, max(0.0, score))
            activity_score = round(recency_component + frequency_component + quality_component, 4)
            payload.append(
                (
                    str(row["address"]),
                    row["label"],
                    str(row["source"]),
                    str(row["status"]),
                    str(row["moderation_state"]),
                    row["moderation_note"],
                    row["age_days"],
                    row["trades_24h"],
                    row["active_hours_24h"],
                    row["trades_7d"],
                    row["trades_30d"],
                    row["active_days_30d"],
                    row["win_rate_30d"],
                    row["realized_pnl_30d"],
                    row["volume_usd_30d"],
                    row["score"],
                    activity_score,
                    last_fill,
                )
            )

        keep = [str(item[0]) for item in payload]
        try:
            if payload:
                self._executemany(
                    """
                    INSERT INTO catalog_current(
                        address,
                        label,
                        source,
                        status,
                        moderation_state,
                        moderation_note,
                        age_days,
                        trades_24h,
                        active_hours_24h,
                        trades_7d,
                        trades_30d,
                        active_days_30d,
                        win_rate_30d,
                        realized_pnl_30d,
                        volume_usd_30d,
                        score,
                        activity_score,
                        last_fill_time,
                        refreshed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(address) DO UPDATE SET
                        label = excluded.label,
                        source = excluded.source,
                        status = excluded.status,
                        moderation_state = excluded.moderation_state,
                        moderation_note = excluded.moderation_note,
                        age_days = excluded.age_days,
                        trades_24h = excluded.trades_24h,
                        active_hours_24h = excluded.active_hours_24h,
                        trades_7d = excluded.trades_7d,
                        trades_30d = excluded.trades_30d,
                        active_days_30d = excluded.active_days_30d,
                        win_rate_30d = excluded.win_rate_30d,
                        realized_pnl_30d = excluded.realized_pnl_30d,
                        volume_usd_30d = excluded.volume_usd_30d,
                        score = excluded.score,
                        activity_score = excluded.activity_score,
                        last_fill_time = excluded.last_fill_time,
                        refreshed_at = CURRENT_TIMESTAMP
                    """,
                    payload,
                )
            if keep:
                placeholders = ",".join("?" for _ in keep)
                self._execute(
                    f"DELETE FROM catalog_current WHERE address NOT IN ({placeholders})",
                    keep,
                )
            else:
                self._execute("DELETE FROM catalog_current")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return len(payload)

    def list_catalog_traders(
        self,
        *,
        limit: int = 101,
        q: str = "",
        status: str = "ALL",
        moderation_state: str = "ALL",
        min_age_days: float | None = None,
        min_trades_30d: int | None = None,
        min_active_days_30d: int | None = None,
        min_win_rate_30d: float | None = None,
        min_realized_pnl_30d: float | None = None,
        min_score: float | None = None,
        min_activity_score: float | None = None,
        active_within_minutes: int | None = None,
        sort_by: str = "activity_desc",
        cursor_value: float | int | None = None,
        cursor_address: str | None = None,
    ) -> list[CatalogTrader]:
        sort_map: dict[str, tuple[str, str]] = {
            "activity_desc": ("COALESCE(activity_score, -1000000000)", "DESC"),
            "score_desc": ("COALESCE(score, -1000000000)", "DESC"),
            "pnl_desc": ("COALESCE(realized_pnl_30d, -1000000000)", "DESC"),
            "win_desc": ("COALESCE(win_rate_30d, -1)", "DESC"),
            "trades_desc": ("COALESCE(trades_30d, -1)", "DESC"),
            "recent_desc": ("COALESCE(last_fill_time, 0)", "DESC"),
            "age_desc": ("COALESCE(age_days, -1)", "DESC"),
        }
        sort_expr, direction = sort_map.get(sort_by, sort_map["activity_desc"])

        where: list[str] = []
        params: list[Any] = []

        q_norm = str(q or "").strip().lower()
        if q_norm:
            if q_norm.startswith("0x"):
                where.append(
                    "("
                    "lower(address) = ? "
                    "OR lower(address) LIKE ? "
                    "OR lower(COALESCE(label, '')) LIKE ? "
                    "OR lower(source) LIKE ?"
                    ")"
                )
                params.extend([q_norm, f"{q_norm}%", f"%{q_norm}%", f"%{q_norm}%"])
            else:
                where.append(
                    "("
                    "lower(COALESCE(label, '')) LIKE ? "
                    "OR lower(address) LIKE ? "
                    "OR lower(source) LIKE ?"
                    ")"
                )
                params.extend([f"%{q_norm}%", f"%{q_norm}%", f"%{q_norm}%"])

        status_norm = str(status or "").upper()
        if status_norm == STATUS_ACTIVE:
            where.append("status = ?")
            params.append(status_norm)

        moderation_norm = str(moderation_state or "").upper()
        if moderation_norm in {
            MODERATION_NEUTRAL,
            MODERATION_WHITELIST,
            MODERATION_BLACKLIST,
        }:
            where.append("moderation_state = ?")
            params.append(moderation_norm)

        if min_age_days is not None:
            where.append("COALESCE(age_days, 0) >= ?")
            params.append(float(min_age_days))
        if min_trades_30d is not None:
            where.append("COALESCE(trades_30d, 0) >= ?")
            params.append(int(min_trades_30d))
        if min_active_days_30d is not None:
            where.append("COALESCE(active_days_30d, 0) >= ?")
            params.append(int(min_active_days_30d))
        if min_win_rate_30d is not None:
            where.append("COALESCE(win_rate_30d, 0) >= ?")
            params.append(float(min_win_rate_30d))
        if min_realized_pnl_30d is not None:
            where.append("COALESCE(realized_pnl_30d, -1000000000) >= ?")
            params.append(float(min_realized_pnl_30d))
        if min_score is not None:
            where.append("COALESCE(score, -1000000000) >= ?")
            params.append(float(min_score))
        if min_activity_score is not None:
            where.append("COALESCE(activity_score, -1000000000) >= ?")
            params.append(float(min_activity_score))

        if active_within_minutes is not None and int(active_within_minutes) > 0:
            cutoff_ms = int(datetime.now(tz=UTC).timestamp() * 1000) - (
                int(active_within_minutes) * 60 * 1000
            )
            where.append("COALESCE(last_fill_time, 0) >= ?")
            params.append(cutoff_ms)

        if cursor_value is not None and cursor_address:
            if direction == "DESC":
                where.append(
                    f"(({sort_expr}) < ? OR (({sort_expr}) = ? AND address > ?))"
                )
            else:
                where.append(
                    f"(({sort_expr}) > ? OR (({sort_expr}) = ? AND address > ?))"
                )
            params.extend([cursor_value, cursor_value, self.normalize_address(cursor_address)])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = self._execute(
            f"""
            SELECT
                address,
                label,
                source,
                status,
                moderation_state,
                moderation_note,
                age_days,
                trades_24h,
                active_hours_24h,
                trades_7d,
                trades_30d,
                active_days_30d,
                win_rate_30d,
                realized_pnl_30d,
                volume_usd_30d,
                score,
                activity_score,
                last_fill_time,
                refreshed_at
            FROM catalog_current
            {where_sql}
            ORDER BY {sort_expr} {direction}, address ASC
            LIMIT ?
            """,
            (*params, int(limit)),
        ).fetchall()
        return [self._row_to_catalog_trader(row) for row in rows]

    def list_top100_live_traders(self, *, limit: int = 100) -> list[LiveTopTrader]:
        rows = self._execute(
            """
            SELECT
                top.rank_position,
                top.activity_score,
                top.refreshed_at,
                t.address,
                t.label,
                t.source,
                t.status,
                t.moderation_state,
                t.age_days,
                t.trades_30d,
                t.active_days_30d,
                t.win_rate_30d,
                t.realized_pnl_30d,
                t.volume_usd_30d,
                t.score,
                t.last_fill_time
            FROM traders_top100_live top
            JOIN tracked_traders t ON t.address = top.address
            WHERE COALESCE(t.moderation_state, ?) <> ?
            ORDER BY top.rank_position ASC
            LIMIT ?
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST, limit),
        ).fetchall()
        return [self._row_to_live_top_trader(row) for row in rows]

    def get_trader(self, *, address: str) -> TrackedTrader | None:
        normalized = self.normalize_address(address)
        row = self._execute(
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

        self._execute(
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
        cursor = self._execute(
            """
            DELETE FROM telegram_trader_subscriptions
            WHERE chat_id = ? AND trader_address = ?
            """,
            (str(chat_id), normalized),
        )
        self._connection.commit()
        return cursor.rowcount

    def list_subscriptions_for_chat(self, *, chat_id: str | int) -> list[ChatSubscription]:
        rows = self._execute(
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
        rows = self._execute(
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

    def create_subscription_with_session(
        self,
        *,
        chat_id: str | int,
        trader_address: str,
        message_thread_id: int | None,
        topic_name: str | None,
        lifetime_hours: int = 24,
    ) -> DeliverySessionInfo:
        normalized = self.normalize_address(trader_address)
        if not normalized:
            raise ValueError("Trader address must not be empty")
        if self.get_trader(address=normalized) is None:
            raise ValueError("Trader does not exist")

        chat_id_str = str(chat_id)
        expires_at = (datetime.now(tz=UTC) + timedelta(hours=max(1, lifetime_hours))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        try:
            # New subscription means new session: expire any previous active records for this pair.
            self._execute(
                """
                UPDATE subscriptions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = ? AND trader_address = ? AND status = ?
                """,
                (SUBSCRIPTION_EXPIRED, chat_id_str, normalized, SUBSCRIPTION_ACTIVE),
            )
            self._execute(
                """
                UPDATE delivery_sessions
                SET status = ?, closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = ? AND trader_address = ? AND status = ?
                """,
                (SESSION_EXPIRED, chat_id_str, normalized, SESSION_ACTIVE),
            )

            subscription_id = self._insert_and_get_id(
                """
                INSERT INTO subscriptions(
                    chat_id, trader_address, status, started_at, expires_at, updated_at
                )
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
                """,
                (chat_id_str, normalized, SUBSCRIPTION_ACTIVE, expires_at),
            )

            session_id = self._insert_and_get_id(
                """
                INSERT INTO delivery_sessions(
                    subscription_id,
                    chat_id,
                    trader_address,
                    message_thread_id,
                    topic_name,
                    status,
                    started_at,
                    expires_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
                """,
                (
                    subscription_id,
                    chat_id_str,
                    normalized,
                    message_thread_id,
                    (topic_name or None),
                    SESSION_ACTIVE,
                    expires_at,
                ),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return DeliverySessionInfo(
            session_id=session_id,
            subscription_id=subscription_id,
            chat_id=chat_id_str,
            trader_address=normalized,
            message_thread_id=message_thread_id,
            topic_name=topic_name,
            expires_at=expires_at,
        )

    def list_delivery_sessions_for_chat(self, *, chat_id: str | int) -> list[DeliverySessionInfo]:
        rows = self._execute(
            """
            SELECT
                ds.id AS session_id,
                ds.subscription_id,
                ds.chat_id,
                ds.trader_address,
                ds.message_thread_id,
                ds.topic_name,
                ds.expires_at
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            WHERE ds.chat_id = ?
              AND ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
              AND s.expires_at > CURRENT_TIMESTAMP
            ORDER BY ds.expires_at ASC
            """,
            (str(chat_id),),
        ).fetchall()
        return [self._row_to_delivery_session_info(row) for row in rows]

    def list_active_delivery_targets_by_trader(self) -> dict[str, list[DeliveryTarget]]:
        rows = self._execute(
            """
            SELECT
                ds.id AS session_id,
                ds.subscription_id,
                ds.chat_id,
                ds.trader_address,
                ds.message_thread_id,
                ds.expires_at
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            WHERE ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
              AND s.expires_at > CURRENT_TIMESTAMP
            ORDER BY ds.trader_address ASC, ds.chat_id ASC
            """
        ).fetchall()

        mapping: dict[str, list[DeliveryTarget]] = {}
        for row in rows:
            target = self._row_to_delivery_target(row)
            mapping.setdefault(target.trader_address, []).append(target)
        return mapping

    def expire_due_delivery_sessions(self) -> list[DeliverySessionInfo]:
        rows = self._execute(
            """
            SELECT
                ds.id AS session_id,
                ds.subscription_id,
                ds.chat_id,
                ds.trader_address,
                ds.message_thread_id,
                ds.topic_name,
                ds.expires_at
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            WHERE ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
              AND s.expires_at <= CURRENT_TIMESTAMP
            ORDER BY s.expires_at ASC
            """
        ).fetchall()
        expired = [self._row_to_delivery_session_info(row) for row in rows]
        if not expired:
            return []

        sub_ids = [item.subscription_id for item in expired]
        sess_ids = [item.session_id for item in expired]
        sub_ph = ",".join("?" for _ in sub_ids)
        sess_ph = ",".join("?" for _ in sess_ids)

        try:
            self._execute(
                f"""
                UPDATE subscriptions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sub_ph})
                """,
                (SUBSCRIPTION_EXPIRED, *sub_ids),
            )
            self._execute(
                f"""
                UPDATE delivery_sessions
                SET status = ?, closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sess_ph})
                """,
                (SESSION_EXPIRED, *sess_ids),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return expired

    def set_delivery_session_cleanup_error(self, *, session_id: int, error: str) -> None:
        self._execute(
            """
            UPDATE delivery_sessions
            SET status = 'ERROR',
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (error[:1000], int(session_id)),
        )
        self._connection.commit()

    def cancel_chat_trader_subscriptions(
        self,
        *,
        chat_id: str | int,
        trader_address: str,
    ) -> list[DeliverySessionInfo]:
        chat_id_str = str(chat_id)
        normalized = self.normalize_address(trader_address)
        rows = self._execute(
            """
            SELECT
                ds.id AS session_id,
                ds.subscription_id,
                ds.chat_id,
                ds.trader_address,
                ds.message_thread_id,
                ds.topic_name,
                ds.expires_at
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            WHERE ds.chat_id = ?
              AND ds.trader_address = ?
              AND ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
            """,
            (chat_id_str, normalized),
        ).fetchall()
        sessions = [self._row_to_delivery_session_info(row) for row in rows]
        if not sessions:
            return []

        sub_ids = [item.subscription_id for item in sessions]
        sess_ids = [item.session_id for item in sessions]
        sub_ph = ",".join("?" for _ in sub_ids)
        sess_ph = ",".join("?" for _ in sess_ids)

        try:
            self._execute(
                f"""
                UPDATE subscriptions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sub_ph})
                """,
                (SUBSCRIPTION_CANCELLED, *sub_ids),
            )
            self._execute(
                f"""
                UPDATE delivery_sessions
                SET status = ?, closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sess_ph})
                """,
                (SESSION_EXPIRED, *sess_ids),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return sessions

    def cancel_all_chat_subscriptions(self, *, chat_id: str | int) -> list[DeliverySessionInfo]:
        chat_id_str = str(chat_id)
        rows = self._execute(
            """
            SELECT
                ds.id AS session_id,
                ds.subscription_id,
                ds.chat_id,
                ds.trader_address,
                ds.message_thread_id,
                ds.topic_name,
                ds.expires_at
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            WHERE ds.chat_id = ?
              AND ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
            """,
            (chat_id_str,),
        ).fetchall()
        sessions = [self._row_to_delivery_session_info(row) for row in rows]
        if not sessions:
            return []

        sub_ids = [item.subscription_id for item in sessions]
        sess_ids = [item.session_id for item in sessions]
        sub_ph = ",".join("?" for _ in sub_ids)
        sess_ph = ",".join("?" for _ in sess_ids)

        try:
            self._execute(
                f"""
                UPDATE subscriptions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sub_ph})
                """,
                (SUBSCRIPTION_CANCELLED, *sub_ids),
            )
            self._execute(
                f"""
                UPDATE delivery_sessions
                SET status = ?, closed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({sess_ph})
                """,
                (SESSION_EXPIRED, *sess_ids),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return sessions

    def enqueue_delivery_retry(
        self,
        *,
        dedup_key: str,
        chat_id: str | int,
        trader_address: str | None,
        message_thread_id: int | None,
        message_text: str,
        delay_seconds: int,
        error: str | None = None,
    ) -> None:
        retry_at = (datetime.now(tz=UTC) + timedelta(seconds=max(1, delay_seconds))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        thread_key = int(message_thread_id or 0)
        chat_id_str = str(chat_id)
        trader_norm = (
            self.normalize_address(trader_address) if trader_address is not None else None
        )
        self._execute(
            """
            INSERT INTO delivery_retry_queue(
                dedup_key,
                chat_id,
                trader_address,
                message_thread_id,
                message_text,
                attempt_count,
                next_attempt_at,
                status,
                last_error,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(dedup_key, chat_id, message_thread_id) DO UPDATE SET
                trader_address = excluded.trader_address,
                message_text = excluded.message_text,
                attempt_count = delivery_retry_queue.attempt_count + 1,
                next_attempt_at = excluded.next_attempt_at,
                status = excluded.status,
                last_error = excluded.last_error,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                dedup_key,
                chat_id_str,
                trader_norm,
                thread_key,
                message_text,
                retry_at,
                RETRY_PENDING,
                (error or None),
            ),
        )
        self._connection.commit()

    def list_due_delivery_retries(self, *, limit: int = 200) -> list[DeliveryRetryJob]:
        rows = self._execute(
            """
            SELECT
                id,
                dedup_key,
                chat_id,
                trader_address,
                message_thread_id,
                message_text,
                attempt_count,
                next_attempt_at,
                last_error
            FROM delivery_retry_queue
            WHERE status = ? AND next_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY next_attempt_at ASC, id ASC
            LIMIT ?
            """,
            (RETRY_PENDING, int(limit)),
        ).fetchall()
        return [self._row_to_delivery_retry_job(row) for row in rows]

    def reschedule_delivery_retry(
        self,
        *,
        retry_id: int,
        delay_seconds: int,
        error: str,
    ) -> None:
        retry_at = (datetime.now(tz=UTC) + timedelta(seconds=max(1, delay_seconds))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self._execute(
            """
            UPDATE delivery_retry_queue
            SET status = ?,
                attempt_count = attempt_count + 1,
                next_attempt_at = ?,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (RETRY_PENDING, retry_at, error[:1000], int(retry_id)),
        )
        self._connection.commit()

    def mark_delivery_retry_sent(self, *, retry_id: int) -> None:
        self._execute(
            """
            UPDATE delivery_retry_queue
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (RETRY_SENT, int(retry_id)),
        )
        self._connection.commit()

    def mark_delivery_retry_dead(self, *, retry_id: int, error: str) -> None:
        self._execute(
            """
            UPDATE delivery_retry_queue
            SET status = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (RETRY_DEAD, error[:1000], int(retry_id)),
        )
        self._connection.commit()

    def delete_pending_retries_for_chat(self, *, chat_id: str | int) -> int:
        cursor = self._execute(
            """
            DELETE FROM delivery_retry_queue
            WHERE chat_id = ? AND status = ?
            """,
            (str(chat_id), RETRY_PENDING),
        )
        self._connection.commit()
        return int(cursor.rowcount)

    def delete_pending_retries_for_target(
        self,
        *,
        chat_id: str | int,
        trader_address: str | None,
        message_thread_id: int | None,
    ) -> int:
        thread_key = int(message_thread_id or 0)
        chat_id_str = str(chat_id)
        if trader_address:
            cursor = self._execute(
                """
                DELETE FROM delivery_retry_queue
                WHERE chat_id = ?
                  AND status = ?
                  AND trader_address = ?
                  AND message_thread_id = ?
                """,
                (
                    chat_id_str,
                    RETRY_PENDING,
                    self.normalize_address(trader_address),
                    thread_key,
                ),
            )
        else:
            cursor = self._execute(
                """
                DELETE FROM delivery_retry_queue
                WHERE chat_id = ?
                  AND status = ?
                  AND message_thread_id = ?
                """,
                (chat_id_str, RETRY_PENDING, thread_key),
            )
        self._connection.commit()
        return int(cursor.rowcount)

    def add_manual(self, *, address: str, label: str | None = None) -> None:
        normalized = self.normalize_address(address)
        if not normalized:
            raise ValueError("Address must not be empty")

        self._execute(
            """
            INSERT INTO tracked_traders(
                address, label, source, status, auto_discovered, manual_status_override,
                moderation_state, updated_at
            )
            VALUES (?, ?, 'manual', ?, 0, 1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(address) DO UPDATE SET
                label = COALESCE(excluded.label, tracked_traders.label),
                source = tracked_traders.source,
                status = tracked_traders.status,
                manual_status_override = 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized, (label or None), STATUS_ACTIVE, MODERATION_NEUTRAL),
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

        self._execute(
            """
            INSERT INTO tracked_traders(
                address, label, source, status, auto_discovered, manual_status_override,
                moderation_state,
                trades_24h, active_hours_24h, trades_7d, trades_30d, active_days_30d,
                first_fill_time, last_fill_time, age_days,
                volume_usd_30d, realized_pnl_30d, fees_30d, win_rate_30d, long_ratio_30d,
                avg_notional_30d, max_notional_30d,
                account_value, total_ntl_pos, total_margin_used,
                score, stats_json, last_metrics_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(address) DO UPDATE SET
                label = COALESCE(excluded.label, tracked_traders.label),
                source = excluded.source,
                auto_discovered = 1,
                status = 'ACTIVE',
                manual_status_override = 0,
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
                STATUS_ACTIVE,
                MODERATION_NEUTRAL,
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
        if status != STATUS_ACTIVE:
            raise ValueError(
                "PAUSED mode is deprecated. Only ACTIVE status is supported in current mode."
            )
        self._execute(
            """
            UPDATE tracked_traders
            SET status = ?, manual_status_override = 0, updated_at = CURRENT_TIMESTAMP
            WHERE address = ?
            """,
            (STATUS_ACTIVE, normalized),
        )
        self._connection.commit()

    def set_status_bulk(self, *, addresses: Iterable[str], status: str) -> int:
        if status != STATUS_ACTIVE:
            raise ValueError(
                "PAUSED mode is deprecated. Only ACTIVE status is supported in current mode."
            )

        normalized = [
            self.normalize_address(address)
            for address in addresses
            if self.normalize_address(address)
        ]
        if not normalized:
            return 0

        placeholders = ",".join("?" for _ in normalized)
        cursor = self._execute(
            f"""
            UPDATE tracked_traders
            SET status = ?, manual_status_override = 0, updated_at = CURRENT_TIMESTAMP
            WHERE address IN ({placeholders})
            """,
            (STATUS_ACTIVE, *normalized),
        )
        self._connection.commit()
        return int(cursor.rowcount)

    def set_moderation(
        self,
        *,
        address: str,
        moderation_state: str,
        note: str | None = None,
    ) -> None:
        self.set_moderation_bulk(
            addresses=[address],
            moderation_state=moderation_state,
            note=note,
        )

    def set_moderation_bulk(
        self,
        *,
        addresses: Iterable[str],
        moderation_state: str,
        note: str | None = None,
    ) -> int:
        if moderation_state not in {
            MODERATION_NEUTRAL,
            MODERATION_WHITELIST,
            MODERATION_BLACKLIST,
        }:
            raise ValueError(f"Unsupported moderation_state: {moderation_state}")

        normalized = [
            self.normalize_address(address)
            for address in addresses
            if self.normalize_address(address)
        ]
        if not normalized:
            return 0

        note_value = (note or "").strip() or None
        placeholders = ",".join("?" for _ in normalized)
        cursor = self._execute(
            f"""
            UPDATE tracked_traders
            SET moderation_state = ?,
                moderation_note = ?,
                moderated_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE address IN ({placeholders})
            """,
            (moderation_state, note_value, *normalized),
        )

        # Blacklisted traders are detached from delivery flows.
        if moderation_state == MODERATION_BLACKLIST:
            self._execute(
                f"""
                UPDATE subscriptions
                SET status = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trader_address IN ({placeholders})
                  AND status = ?
                """,
                (SUBSCRIPTION_CANCELLED, *normalized, SUBSCRIPTION_ACTIVE),
            )
            self._execute(
                f"""
                UPDATE delivery_sessions
                SET status = ?,
                    closed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trader_address IN ({placeholders})
                  AND status = ?
                """,
                (SESSION_EXPIRED, *normalized, SESSION_ACTIVE),
            )
            self._execute(
                f"""
                UPDATE telegram_trader_subscriptions
                SET status = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trader_address IN ({placeholders})
                """,
                (STATUS_PAUSED, *normalized),
            )
            self._execute(
                f"""
                DELETE FROM delivery_retry_queue
                WHERE trader_address IN ({placeholders})
                  AND status = ?
                """,
                (*normalized, RETRY_PENDING),
            )

        self._connection.commit()
        return int(cursor.rowcount)

    def delete(self, *, address: str) -> None:
        normalized = self.normalize_address(address)
        self._execute(
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

        self._executemany(
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
        self._execute(
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
        rows = self._execute(
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
            cursor = self._execute(
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
            cursor = self._execute(
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
        self._execute(
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
