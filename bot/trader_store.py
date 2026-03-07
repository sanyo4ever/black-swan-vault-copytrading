from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
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
TIER_HOT = "HOT"
TIER_WARM = "WARM"
TIER_COLD = "COLD"
TIER_PRIORITY = {TIER_HOT: 0, TIER_WARM: 1, TIER_COLD: 2}
PERMANENT_SUBSCRIPTION_EXPIRES_AT = "9999-12-31 23:59:59"


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
    stats_json: str | None
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


@dataclass(frozen=True)
class MonitoringTarget:
    address: str
    tier: str
    poll_interval_seconds: int
    lookback_minutes: int


@dataclass(frozen=True)
class DeliveryMonitorTarget:
    address: str
    subscriber_count: int
    priority_score: float
    poll_interval_seconds: int
    safety_lookback_seconds: int
    bootstrap_lookback_minutes: int
    last_seen_fill_time: int | None
    idle_cycles: int
    consecutive_errors: int


class TraderStore:
    _bootstrap_lock = Lock()
    _bootstrapped_databases: set[str] = set()

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
        self._database_key = database_str
        self._ensure_schema_once_per_process()
        self._enforce_active_only_trader_status()
        self._enforce_permanent_subscriptions()

    def _ensure_schema_once_per_process(self) -> None:
        if self._database_key in self.__class__._bootstrapped_databases:
            return
        with self.__class__._bootstrap_lock:
            if self._database_key in self.__class__._bootstrapped_databases:
                return
            self._ensure_schema()
            self.__class__._bootstrapped_databases.add(self._database_key)

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

    def _enforce_permanent_subscriptions(self) -> None:
        # Current product mode: subscriptions remain active until user cancellation.
        try:
            self._execute(
                """
                UPDATE subscriptions
                SET expires_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE status = ?
                  AND expires_at <> ?
                """,
                (
                    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
                    SUBSCRIPTION_ACTIVE,
                    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
                ),
            )
            self._execute(
                """
                UPDATE delivery_sessions
                SET expires_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE status = ?
                  AND expires_at <> ?
                """,
                (
                    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
                    SESSION_ACTIVE,
                    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
                ),
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
        # Serialize schema/bootstrap DDL across processes to prevent startup deadlocks.
        self._execute("SELECT pg_advisory_xact_lock(?)", (91386024570631,))
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
            CREATE TABLE IF NOT EXISTS trader_monitoring_pool (
                address TEXT PRIMARY KEY,
                tier TEXT NOT NULL CHECK(tier IN ('HOT', 'WARM', 'COLD')),
                rank_position INTEGER NOT NULL,
                rank_score REAL NOT NULL DEFAULT 0,
                poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
                lookback_minutes INTEGER NOT NULL DEFAULT 30,
                next_poll_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_polled_at TEXT,
                refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monitoring_pool_tier_next
            ON trader_monitoring_pool(tier, next_poll_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monitoring_pool_next_poll
            ON trader_monitoring_pool(next_poll_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_monitor_state (
                address TEXT PRIMARY KEY,
                subscriber_count INTEGER NOT NULL DEFAULT 0,
                priority_score REAL NOT NULL DEFAULT 0,
                poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
                safety_lookback_seconds INTEGER NOT NULL DEFAULT 90,
                bootstrap_lookback_minutes INTEGER NOT NULL DEFAULT 180,
                last_seen_fill_time INTEGER,
                last_polled_at TEXT,
                next_poll_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                idle_cycles INTEGER NOT NULL DEFAULT 0,
                consecutive_errors INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_monitor_state_due
            ON delivery_monitor_state(next_poll_at, priority_score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_monitor_state_priority
            ON delivery_monitor_state(priority_score DESC, subscriber_count DESC)
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
                stats_json TEXT,
                refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        existing_catalog_columns = {
            str(row["name"]) for row in self._execute("PRAGMA table_info(catalog_current)").fetchall()
        }
        for column, ddl in {"stats_json": "TEXT"}.items():
            if column in existing_catalog_columns:
                continue
            self._execute(f"ALTER TABLE catalog_current ADD COLUMN {column} {ddl}")
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
            CREATE TABLE IF NOT EXISTS trader_monitoring_pool (
                address TEXT PRIMARY KEY,
                tier TEXT NOT NULL CHECK(tier IN ('HOT', 'WARM', 'COLD')),
                rank_position INTEGER NOT NULL,
                rank_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
                lookback_minutes INTEGER NOT NULL DEFAULT 30,
                next_poll_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_polled_at TIMESTAMP,
                refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monitoring_pool_tier_next
            ON trader_monitoring_pool(tier, next_poll_at)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monitoring_pool_next_poll
            ON trader_monitoring_pool(next_poll_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_monitor_state (
                address TEXT PRIMARY KEY,
                subscriber_count INTEGER NOT NULL DEFAULT 0,
                priority_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
                safety_lookback_seconds INTEGER NOT NULL DEFAULT 90,
                bootstrap_lookback_minutes INTEGER NOT NULL DEFAULT 180,
                last_seen_fill_time BIGINT,
                last_polled_at TIMESTAMP,
                next_poll_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                idle_cycles INTEGER NOT NULL DEFAULT 0,
                consecutive_errors INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_monitor_state_due
            ON delivery_monitor_state(next_poll_at, priority_score DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivery_monitor_state_priority
            ON delivery_monitor_state(priority_score DESC, subscriber_count DESC)
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
                stats_json TEXT,
                refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(address) REFERENCES tracked_traders(address) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            ALTER TABLE catalog_current ADD COLUMN IF NOT EXISTS stats_json TEXT
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
            stats_json=row["stats_json"],
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

    @staticmethod
    def _row_to_monitoring_target(row: Mapping[str, Any]) -> MonitoringTarget:
        return MonitoringTarget(
            address=str(row["address"]),
            tier=str(row["tier"]),
            poll_interval_seconds=int(row["poll_interval_seconds"] or 60),
            lookback_minutes=int(row["lookback_minutes"] or 30),
        )

    @staticmethod
    def _row_to_delivery_monitor_target(row: Mapping[str, Any]) -> DeliveryMonitorTarget:
        return DeliveryMonitorTarget(
            address=str(row["address"]),
            subscriber_count=int(row["subscriber_count"] or 0),
            priority_score=float(row["priority_score"] or 0.0),
            poll_interval_seconds=int(row["poll_interval_seconds"] or 60),
            safety_lookback_seconds=int(row["safety_lookback_seconds"] or 90),
            bootstrap_lookback_minutes=int(row["bootstrap_lookback_minutes"] or 180),
            last_seen_fill_time=(
                int(row["last_seen_fill_time"])
                if row["last_seen_fill_time"] is not None
                else None
            ),
            idle_cycles=int(row["idle_cycles"] or 0),
            consecutive_errors=int(row["consecutive_errors"] or 0),
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

    def list_due_monitoring_targets(
        self,
        *,
        limit: int = 200,
        only_subscribed: bool = True,
    ) -> list[MonitoringTarget]:
        where = [
            "mp.next_poll_at <= CURRENT_TIMESTAMP",
            "COALESCE(t.moderation_state, ?) <> ?",
        ]
        params: list[Any] = [MODERATION_NEUTRAL, MODERATION_BLACKLIST]
        if only_subscribed:
            where.append(
                """
                EXISTS (
                    SELECT 1
                    FROM subscriptions s
                    WHERE s.trader_address = mp.address
                      AND s.status = 'ACTIVE'
                      AND s.expires_at > CURRENT_TIMESTAMP
                )
                """
            )
        rows = self._execute(
            f"""
            SELECT
                mp.address,
                mp.tier,
                mp.poll_interval_seconds,
                mp.lookback_minutes
            FROM trader_monitoring_pool mp
            JOIN tracked_traders t ON t.address = mp.address
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE mp.tier
                    WHEN '{TIER_HOT}' THEN 0
                    WHEN '{TIER_WARM}' THEN 1
                    ELSE 2
                END ASC,
                mp.next_poll_at ASC,
                mp.rank_position ASC
            LIMIT ?
            """,
            (*params, int(limit)),
        ).fetchall()
        return [self._row_to_monitoring_target(row) for row in rows]

    def mark_monitoring_targets_polled(self, *, targets: list[MonitoringTarget]) -> None:
        if not targets:
            return
        now = datetime.now(tz=UTC)
        last_polled = now.strftime("%Y-%m-%d %H:%M:%S")
        prepared: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for item in targets:
            address = self.normalize_address(item.address)
            if not address or address in seen:
                continue
            seen.add(address)
            next_poll = (now + timedelta(seconds=max(1, int(item.poll_interval_seconds)))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            prepared.append((last_polled, next_poll, address))
        if not prepared:
            return
        self._executemany(
            """
            UPDATE trader_monitoring_pool
            SET last_polled_at = ?,
                next_poll_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE address = ?
            """,
            prepared,
        )
        self._connection.commit()

    def refresh_delivery_monitor_state(
        self,
        *,
        base_poll_seconds: int = 60,
        min_poll_seconds: int = 20,
        max_poll_seconds: int = 180,
        priority_recency_minutes: int = 120,
        safety_lookback_seconds: int = 90,
        bootstrap_lookback_minutes: int = 180,
        max_targets_per_cycle: int = 120,
    ) -> dict[str, int]:
        rows = self._execute(
            """
            SELECT
                ds.trader_address AS address,
                COUNT(*) AS subscriber_count,
                MAX(COALESCE(t.last_fill_time, 0)) AS last_fill_time,
                MAX(COALESCE(t.score, 0)) AS score
            FROM delivery_sessions ds
            JOIN subscriptions s ON s.id = ds.subscription_id
            JOIN tracked_traders t ON t.address = ds.trader_address
            WHERE ds.status = 'ACTIVE'
              AND s.status = 'ACTIVE'
              AND s.expires_at > CURRENT_TIMESTAMP
              AND COALESCE(t.moderation_state, ?) <> ?
            GROUP BY ds.trader_address
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST),
        ).fetchall()

        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        payload: list[tuple[Any, ...]] = []
        active_addresses: list[str] = []
        high_demand = 0
        for row in rows:
            address = self.normalize_address(str(row["address"]))
            if not address:
                continue
            subscriber_count = max(1, int(row["subscriber_count"] or 0))
            if subscriber_count >= 3:
                high_demand += 1
            score = float(row["score"] or 0.0)
            last_fill_time = int(row["last_fill_time"] or 0)
            if last_fill_time > 0:
                age_minutes = max(0.0, (now_ms - last_fill_time) / 60000.0)
            else:
                age_minutes = float("inf")

            demand_score = min(1.0, math.log1p(subscriber_count) / math.log(25.0))
            recency_score = (
                max(0.0, 1.0 - (age_minutes / max(1, priority_recency_minutes)))
                if age_minutes != float("inf")
                else 0.0
            )
            quality_score = min(1.0, max(0.0, score) / 100.0)
            priority_score = (
                (demand_score * 0.55) + (recency_score * 0.30) + (quality_score * 0.15)
            ) * 100.0

            demand_factor = 1.0 - min(0.45, demand_score * 0.45)
            if age_minutes <= 30:
                recency_factor = 0.75
            elif age_minutes <= 60:
                recency_factor = 0.85
            elif age_minutes <= max(1, priority_recency_minutes):
                recency_factor = 1.0
            else:
                recency_factor = 1.2

            poll_interval_seconds = int(round(base_poll_seconds * demand_factor * recency_factor))
            poll_interval_seconds = max(min_poll_seconds, min(max_poll_seconds, poll_interval_seconds))

            active_addresses.append(address)
            payload.append(
                (
                    address,
                    subscriber_count,
                    round(priority_score, 6),
                    poll_interval_seconds,
                    max(15, int(safety_lookback_seconds)),
                    max(5, int(bootstrap_lookback_minutes)),
                )
            )

        try:
            if payload:
                self._executemany(
                    """
                    INSERT INTO delivery_monitor_state(
                        address,
                        subscriber_count,
                        priority_score,
                        poll_interval_seconds,
                        safety_lookback_seconds,
                        bootstrap_lookback_minutes,
                        next_poll_at,
                        refreshed_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(address) DO UPDATE SET
                        subscriber_count = excluded.subscriber_count,
                        priority_score = excluded.priority_score,
                        poll_interval_seconds = excluded.poll_interval_seconds,
                        safety_lookback_seconds = excluded.safety_lookback_seconds,
                        bootstrap_lookback_minutes = excluded.bootstrap_lookback_minutes,
                        next_poll_at = (
                            CASE
                                WHEN delivery_monitor_state.next_poll_at < CURRENT_TIMESTAMP
                                THEN CURRENT_TIMESTAMP
                                ELSE delivery_monitor_state.next_poll_at
                            END
                        ),
                        refreshed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    payload,
                )
            if active_addresses:
                placeholders = ",".join("?" for _ in active_addresses)
                self._execute(
                    f"DELETE FROM delivery_monitor_state WHERE address NOT IN ({placeholders})",
                    active_addresses,
                )
            else:
                self._execute("DELETE FROM delivery_monitor_state")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        total = len(payload)
        return {
            "total": total,
            "high_demand": high_demand,
            "over_capacity": max(0, total - max(1, int(max_targets_per_cycle))),
        }

    def list_due_delivery_monitor_targets(self, *, limit: int = 200) -> list[DeliveryMonitorTarget]:
        rows = self._execute(
            """
            SELECT
                dms.address,
                dms.subscriber_count,
                dms.priority_score,
                dms.poll_interval_seconds,
                dms.safety_lookback_seconds,
                dms.bootstrap_lookback_minutes,
                dms.last_seen_fill_time,
                dms.idle_cycles,
                dms.consecutive_errors
            FROM delivery_monitor_state dms
            JOIN tracked_traders t ON t.address = dms.address
            WHERE dms.subscriber_count > 0
              AND dms.next_poll_at <= CURRENT_TIMESTAMP
              AND COALESCE(t.moderation_state, ?) <> ?
            ORDER BY dms.priority_score DESC, dms.next_poll_at ASC, dms.address ASC
            LIMIT ?
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST, int(limit)),
        ).fetchall()
        return [self._row_to_delivery_monitor_target(row) for row in rows]

    def mark_delivery_monitor_polled(
        self,
        *,
        address: str,
        next_poll_seconds: int,
        newest_fill_time: int | None = None,
        had_new_fill: bool = False,
        error: str | None = None,
    ) -> None:
        normalized = self.normalize_address(address)
        if not normalized:
            return
        row = self._execute(
            """
            SELECT last_seen_fill_time, idle_cycles, consecutive_errors
            FROM delivery_monitor_state
            WHERE address = ?
            """,
            (normalized,),
        ).fetchone()
        if row is None:
            return

        previous_last_seen = int(row["last_seen_fill_time"] or 0)
        incoming_last_seen = int(newest_fill_time or 0)
        last_seen_fill_time = max(previous_last_seen, incoming_last_seen)
        if last_seen_fill_time <= 0 and not error:
            last_seen_fill_time = int(datetime.now(tz=UTC).timestamp() * 1000)

        if had_new_fill:
            idle_cycles = 0
        else:
            idle_cycles = min(100, int(row["idle_cycles"] or 0) + 1)

        if error:
            consecutive_errors = min(100, int(row["consecutive_errors"] or 0) + 1)
            last_error = str(error)[:1000]
        else:
            consecutive_errors = 0
            last_error = None

        next_poll_at = (
            datetime.now(tz=UTC) + timedelta(seconds=max(5, int(next_poll_seconds)))
        ).strftime("%Y-%m-%d %H:%M:%S")
        self._execute(
            """
            UPDATE delivery_monitor_state
            SET
                last_seen_fill_time = ?,
                last_polled_at = CURRENT_TIMESTAMP,
                next_poll_at = ?,
                idle_cycles = ?,
                consecutive_errors = ?,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE address = ?
            """,
            (
                (last_seen_fill_time if last_seen_fill_time > 0 else None),
                next_poll_at,
                idle_cycles,
                consecutive_errors,
                last_error,
                normalized,
            ),
        )
        self._connection.commit()

    def refresh_traders_universe_from_tracked(
        self,
        *,
        min_age_days: int,
        min_trades_30d: int,
        min_active_days_30d: int = 0,
        min_win_rate_30d: float,
        max_drawdown_30d_pct: float = 1_000_000_000_000.0,
        max_last_activity_minutes: int = 60,
        min_realized_pnl_30d: float,
        min_score: float,
        max_size: int = 3000,
    ) -> int:
        if self._driver == "postgres":
            drawdown_expr = (
                "NULLIF((stats_json::jsonb -> 'metrics_30d' ->> 'max_drawdown_pct'), '')"
                "::double precision"
            )
        else:
            drawdown_expr = "CAST(json_extract(stats_json, '$.metrics_30d.max_drawdown_pct') AS REAL)"
        cutoff_ms = int(datetime.now(tz=UTC).timestamp() * 1000) - (
            max(1, int(max_last_activity_minutes)) * 60 * 1000
        )

        rows = self._execute(
            f"""
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
                AND COALESCE(active_days_30d, 0) >= ?
                AND COALESCE(win_rate_30d, 0) >= ?
                AND COALESCE(realized_pnl_30d, -1000000000) > ?
                AND COALESCE(last_fill_time, 0) >= ?
                AND COALESCE({drawdown_expr}, 1000000000) <= ?
                AND COALESCE(score, -1000000000) >= ?
            ORDER BY COALESCE(score, -1000000000) DESC, COALESCE(last_fill_time, 0) DESC
            LIMIT ?
            """,
            (
                MODERATION_NEUTRAL,
                MODERATION_BLACKLIST,
                float(min_age_days),
                int(min_trades_30d),
                int(min_active_days_30d),
                float(min_win_rate_30d),
                float(min_realized_pnl_30d),
                int(cutoff_ms),
                float(max_drawdown_30d_pct),
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

    def refresh_monitoring_pool(
        self,
        *,
        hot_size: int = 100,
        warm_size: int = 400,
        hot_poll_seconds: int = 60,
        warm_poll_seconds: int = 600,
        cold_poll_seconds: int = 3600,
        hot_recency_minutes: int = 60,
        warm_recency_minutes: int = 360,
    ) -> dict[str, int]:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        rows = self._execute(
            """
            SELECT
                u.address,
                u.score,
                u.last_fill_time,
                t.trades_24h,
                t.trades_7d
            FROM traders_universe u
            JOIN tracked_traders t ON t.address = u.address
            WHERE COALESCE(t.moderation_state, ?) <> ?
            """,
            (MODERATION_NEUTRAL, MODERATION_BLACKLIST),
        ).fetchall()

        ranked: list[tuple[str, float, int, int, int]] = []
        for row in rows:
            address = self.normalize_address(str(row["address"]))
            if not address:
                continue
            last_fill_time = int(row["last_fill_time"] or 0)
            score = float(row["score"] or 0.0)
            trades_24h = int(row["trades_24h"] or 0)
            trades_7d = int(row["trades_7d"] or 0)

            age_minutes = (
                max(0.0, (now_ms - last_fill_time) / 60000.0)
                if last_fill_time > 0
                else float("inf")
            )
            recency_score = (
                max(0.0, 1.0 - (age_minutes / max(1, warm_recency_minutes)))
                if age_minutes != float("inf")
                else 0.0
            )
            quality_score = min(1.0, max(0.0, score) / 100.0)
            activity_score = min(1.0, trades_24h / 24.0) * 0.55 + min(1.0, trades_7d / 84.0) * 0.45
            rank_score = ((quality_score * 0.60) + (recency_score * 0.25) + (activity_score * 0.15)) * 100.0
            ranked.append((address, round(rank_score, 6), last_fill_time, trades_24h, trades_7d))

        ranked.sort(key=lambda item: (item[1], item[2]), reverse=True)

        hot_count = 0
        warm_count = 0
        payload: list[tuple[Any, ...]] = []
        for rank_position, item in enumerate(ranked, start=1):
            address, rank_score, last_fill_time, _trades_24h, _trades_7d = item
            age_minutes = (
                max(0.0, (now_ms - last_fill_time) / 60000.0)
                if last_fill_time > 0
                else float("inf")
            )

            if age_minutes <= max(1, hot_recency_minutes) and hot_count < max(1, hot_size):
                tier = TIER_HOT
                poll_interval_seconds = max(10, int(hot_poll_seconds))
                hot_count += 1
            elif age_minutes <= max(1, warm_recency_minutes) and warm_count < max(0, warm_size):
                tier = TIER_WARM
                poll_interval_seconds = max(30, int(warm_poll_seconds))
                warm_count += 1
            else:
                tier = TIER_COLD
                poll_interval_seconds = max(60, int(cold_poll_seconds))

            lookback_minutes = max(30, int(math.ceil(poll_interval_seconds / 60.0) * 6))
            payload.append(
                (
                    address,
                    tier,
                    rank_position,
                    rank_score,
                    poll_interval_seconds,
                    lookback_minutes,
                )
            )

        keep = [item[0] for item in payload]
        try:
            if payload:
                self._executemany(
                    """
                    INSERT INTO trader_monitoring_pool(
                        address,
                        tier,
                        rank_position,
                        rank_score,
                        poll_interval_seconds,
                        lookback_minutes,
                        next_poll_at,
                        refreshed_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(address) DO UPDATE SET
                        tier = excluded.tier,
                        rank_position = excluded.rank_position,
                        rank_score = excluded.rank_score,
                        poll_interval_seconds = excluded.poll_interval_seconds,
                        lookback_minutes = excluded.lookback_minutes,
                        next_poll_at = (
                            CASE
                                WHEN trader_monitoring_pool.tier <> excluded.tier THEN CURRENT_TIMESTAMP
                                ELSE trader_monitoring_pool.next_poll_at
                            END
                        ),
                        refreshed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    payload,
                )
            if keep:
                placeholders = ",".join("?" for _ in keep)
                self._execute(
                    f"DELETE FROM trader_monitoring_pool WHERE address NOT IN ({placeholders})",
                    keep,
                )
            else:
                self._execute("DELETE FROM trader_monitoring_pool")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        return {
            "total": len(payload),
            "hot": hot_count,
            "warm": warm_count,
            "cold": max(0, len(payload) - hot_count - warm_count),
        }

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
                last_fill_time,
                stats_json
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
                    row["stats_json"],
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
                        stats_json,
                        refreshed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
                        stats_json = excluded.stats_json,
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
        def _json_metric_raw(*, period: str, key: str) -> str:
            period_norm = period if period in {"1d", "7d", "30d"} else "7d"
            if self._driver == "postgres":
                return (
                    f"NULLIF((stats_json::jsonb -> 'metrics_{period_norm}' ->> '{key}'), '')"
                    "::double precision"
                )
            return f"CAST(json_extract(stats_json, '$.metrics_{period_norm}.{key}') AS REAL)"

        def _null_last_expr(
            *,
            raw_expr: str,
            direction: str,
            low_sentinel: float | int = -1000000000,
            high_sentinel: float | int = 1000000000,
        ) -> str:
            if direction == "ASC":
                return f"COALESCE({raw_expr}, {high_sentinel})"
            return f"COALESCE({raw_expr}, {low_sentinel})"

        sort_map: dict[str, tuple[str, str]] = {
            "activity_desc": ("COALESCE(activity_score, -1000000000)", "DESC"),
            "activity_asc": ("COALESCE(activity_score, 1000000000)", "ASC"),
            "score_desc": ("COALESCE(score, -1000000000)", "DESC"),
            "score_asc": ("COALESCE(score, 1000000000)", "ASC"),
            "recent_desc": ("COALESCE(last_fill_time, -1)", "DESC"),
            "recent_asc": ("COALESCE(last_fill_time, 9223372036854775807)", "ASC"),
            "age_desc": ("COALESCE(age_days, -1)", "DESC"),
            "age_asc": ("COALESCE(age_days, 1000000000)", "ASC"),
        }

        metric_keys = {
            "roi": "roi_pct",
            "drawdown": "max_drawdown_pct",
            "pnl": "realized_pnl",
            "win": "win_rate",
            "pl": "profit_to_loss_ratio",
            "sharpe": "sharpe",
        }
        for period in ("1d", "7d", "30d"):
            for metric_alias, metric_key in metric_keys.items():
                raw_expr = _json_metric_raw(period=period, key=metric_key)
                sort_map[f"{metric_alias}_{period}_desc"] = (
                    _null_last_expr(raw_expr=raw_expr, direction="DESC"),
                    "DESC",
                )
                sort_map[f"{metric_alias}_{period}_asc"] = (
                    _null_last_expr(raw_expr=raw_expr, direction="ASC"),
                    "ASC",
                )

            trade_expr = {
                "1d": "trades_24h",
                "7d": "trades_7d",
                "30d": "trades_30d",
            }[period]
            sort_map[f"trades_{period}_desc"] = (
                _null_last_expr(raw_expr=trade_expr, direction="DESC", low_sentinel=-1),
                "DESC",
            )
            sort_map[f"trades_{period}_asc"] = (
                _null_last_expr(raw_expr=trade_expr, direction="ASC", high_sentinel=1000000000),
                "ASC",
            )

        # Backward compatibility for existing links/API consumers.
        sort_map["pnl_desc"] = sort_map["pnl_30d_desc"]
        sort_map["win_desc"] = sort_map["win_30d_desc"]
        sort_map["trades_desc"] = sort_map["trades_30d_desc"]

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
                stats_json,
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
        # Subscriptions are indefinite and can be stopped only by explicit cancellation.
        _ = lifetime_hours
        expires_at = PERMANENT_SUBSCRIPTION_EXPIRES_AT

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
