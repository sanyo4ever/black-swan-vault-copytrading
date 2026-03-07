#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import psycopg


@dataclass(frozen=True)
class SuspectTrader:
    address: str
    label: str
    source: str
    created_at: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect and optionally delete test trader rows from tracked_traders "
            "(and cascaded dependent rows)."
        )
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="PostgreSQL connection string (defaults to DATABASE_URL env).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete rows. Without this flag script is dry-run.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirmation flag required together with --apply.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max suspect rows to inspect/delete.",
    )
    return parser.parse_args()


def load_suspects(conn: psycopg.Connection, *, limit: int) -> list[SuspectTrader]:
    sql = """
        SELECT address, COALESCE(label, ''), COALESCE(source, ''), created_at::text
        FROM tracked_traders
        WHERE
            lower(source) IN ('test', 'manual_test', 'e2e_test', 'qa_test')
            OR source ILIKE 'test:%'
            OR source ILIKE '%_test'
            OR label LIKE '[TEST] %'
            OR label ILIKE 'test %'
            OR label ILIKE '% [test]%'
            OR address ~ '^0x([0-9A-Fa-f])\\1{39}$'
            OR lower(address) IN (
                '0x0000000000000000000000000000000000000000',
                '0x1111111111111111111111111111111111111111'
            )
        ORDER BY created_at DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (max(1, int(limit)),))
        rows = cur.fetchall()
    return [
        SuspectTrader(
            address=str(row[0]),
            label=str(row[1]),
            source=str(row[2]),
            created_at=str(row[3]),
        )
        for row in rows
    ]


def delete_suspects(conn: psycopg.Connection, *, addresses: list[str]) -> int:
    if not addresses:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM tracked_traders WHERE address = ANY(%s)",
            (addresses,),
        )
        deleted = int(cur.rowcount or 0)
    conn.commit()
    return deleted


def main() -> int:
    args = parse_args()
    if not args.postgres_url:
        print("Missing DATABASE_URL / --postgres-url", file=sys.stderr)
        return 2
    if args.apply and not args.yes:
        print("Refusing delete: use --apply --yes to confirm.", file=sys.stderr)
        return 2

    with psycopg.connect(args.postgres_url, autocommit=False) as conn:
        suspects = load_suspects(conn, limit=args.limit)

        print(f"suspects_found={len(suspects)}")
        for item in suspects:
            print(f"{item.address} | {item.label} | {item.source} | {item.created_at}")

        if not args.apply:
            print("dry_run=true (use --apply to delete)")
            conn.rollback()
            return 0

        deleted = delete_suspects(
            conn,
            addresses=[item.address for item in suspects],
        )
        print(f"deleted={deleted}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
