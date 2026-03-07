from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Iterable

try:
    import psycopg
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "psycopg is required for migration. Install dependencies from requirements.txt"
    ) from exc

from bot.dedup import DedupStore
from bot.trader_store import TraderStore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate SQLite data into PostgreSQL")
    parser.add_argument(
        "--sqlite-path",
        default=os.getenv("DATABASE_PATH", "data/signals.db"),
        help="Path to existing SQLite database",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="PostgreSQL DSN (postgresql://...)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per batch insert",
    )
    return parser.parse_args()


def _sqlite_has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _copy_table(
    *,
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table_name: str,
    columns: list[str],
    conflict_sql: str,
    batch_size: int,
) -> int:
    if not _sqlite_has_table(sqlite_conn, table_name):
        return 0

    select_sql = f"SELECT {', '.join(columns)} FROM {table_name}"
    placeholders = ", ".join("%s" for _ in columns)
    insert_sql = (
        f"INSERT INTO {table_name}({', '.join(columns)}) "
        f"VALUES ({placeholders}) {conflict_sql}"
    )

    total = 0
    source = sqlite_conn.execute(select_sql)
    while True:
        rows = source.fetchmany(batch_size)
        if not rows:
            break
        payload = [tuple(row[col] for col in columns) for row in rows]
        with pg_conn.cursor() as cur:
            cur.executemany(insert_sql, payload)
        total += len(payload)

    return total


def _reset_sequence(pg_conn, table_name: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table_name}', 'id'),
                COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                true
            )
            """
        )


def main() -> None:
    args = _parse_args()
    sqlite_path = Path(args.sqlite_path)
    postgres_url = args.postgres_url.strip()

    if not sqlite_path.exists():
        raise SystemExit(f"SQLite DB not found: {sqlite_path}")
    if not postgres_url:
        raise SystemExit("Missing --postgres-url (or DATABASE_URL env)")
    if not (
        postgres_url.startswith("postgresql://")
        or postgres_url.startswith("postgres://")
    ):
        raise SystemExit("--postgres-url must start with postgresql:// or postgres://")

    # Ensure target schema exists before copy.
    with TraderStore(postgres_url):
        pass
    with DedupStore(postgres_url):
        pass

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg.connect(postgres_url, autocommit=False)

    try:
        migrations: list[tuple[str, list[str], str]] = [
            (
                "tracked_traders",
                [
                    "address",
                    "label",
                    "source",
                    "status",
                    "auto_discovered",
                    "manual_status_override",
                    "trades_24h",
                    "active_hours_24h",
                    "trades_7d",
                    "trades_30d",
                    "active_days_30d",
                    "first_fill_time",
                    "last_fill_time",
                    "age_days",
                    "volume_usd_30d",
                    "realized_pnl_30d",
                    "fees_30d",
                    "win_rate_30d",
                    "long_ratio_30d",
                    "avg_notional_30d",
                    "max_notional_30d",
                    "account_value",
                    "total_ntl_pos",
                    "total_margin_used",
                    "score",
                    "stats_json",
                    "last_metrics_at",
                    "created_at",
                    "updated_at",
                ],
                """
                ON CONFLICT(address) DO UPDATE SET
                    label = excluded.label,
                    source = excluded.source,
                    status = excluded.status,
                    auto_discovered = excluded.auto_discovered,
                    manual_status_override = excluded.manual_status_override,
                    trades_24h = excluded.trades_24h,
                    active_hours_24h = excluded.active_hours_24h,
                    trades_7d = excluded.trades_7d,
                    trades_30d = excluded.trades_30d,
                    active_days_30d = excluded.active_days_30d,
                    first_fill_time = excluded.first_fill_time,
                    last_fill_time = excluded.last_fill_time,
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
                    last_metrics_at = excluded.last_metrics_at,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                """,
            ),
            (
                "discovery_runs",
                [
                    "id",
                    "started_at",
                    "finished_at",
                    "source",
                    "status",
                    "candidates",
                    "qualified",
                    "upserted",
                    "error_message",
                ],
                "ON CONFLICT(id) DO NOTHING",
            ),
            (
                "subscription_requests",
                ["id", "trader_address", "client_ip", "user_agent", "created_at"],
                "ON CONFLICT(id) DO NOTHING",
            ),
            (
                "telegram_trader_subscriptions",
                ["id", "chat_id", "trader_address", "status", "created_at", "updated_at"],
                "ON CONFLICT(id) DO NOTHING",
            ),
            (
                "traders_universe",
                [
                    "address",
                    "label",
                    "source",
                    "score",
                    "win_rate_30d",
                    "realized_pnl_30d",
                    "volume_usd_30d",
                    "trades_30d",
                    "active_days_30d",
                    "age_days",
                    "last_fill_time",
                    "updated_at",
                ],
                """
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
                    updated_at = excluded.updated_at
                """,
            ),
            (
                "traders_top100_live",
                ["rank_position", "address", "activity_score", "last_fill_time", "refreshed_at"],
                """
                ON CONFLICT(rank_position) DO UPDATE SET
                    address = excluded.address,
                    activity_score = excluded.activity_score,
                    last_fill_time = excluded.last_fill_time,
                    refreshed_at = excluded.refreshed_at
                """,
            ),
            (
                "subscriptions",
                [
                    "id",
                    "chat_id",
                    "trader_address",
                    "status",
                    "started_at",
                    "expires_at",
                    "created_at",
                    "updated_at",
                ],
                "ON CONFLICT(id) DO NOTHING",
            ),
            (
                "delivery_sessions",
                [
                    "id",
                    "subscription_id",
                    "chat_id",
                    "trader_address",
                    "message_thread_id",
                    "topic_name",
                    "status",
                    "started_at",
                    "expires_at",
                    "closed_at",
                    "last_error",
                    "created_at",
                    "updated_at",
                ],
                "ON CONFLICT(id) DO NOTHING",
            ),
            (
                "published_signals",
                ["dedup_key", "first_seen_at"],
                "ON CONFLICT(dedup_key) DO NOTHING",
            ),
        ]

        summary: list[tuple[str, int]] = []
        for table_name, columns, conflict_sql in migrations:
            count = _copy_table(
                sqlite_conn=sqlite_conn,
                pg_conn=pg_conn,
                table_name=table_name,
                columns=columns,
                conflict_sql=" ".join(conflict_sql.split()),
                batch_size=max(1, args.batch_size),
            )
            summary.append((table_name, count))

        for seq_table in [
            "discovery_runs",
            "subscription_requests",
            "telegram_trader_subscriptions",
            "subscriptions",
            "delivery_sessions",
        ]:
            _reset_sequence(pg_conn, seq_table)

        pg_conn.commit()

        print("Migration complete")
        for table_name, count in summary:
            print(f"- {table_name}: {count} row(s)")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
