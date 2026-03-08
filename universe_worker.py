from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
import time

from bot.config import load_settings
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.trader_store import TraderStore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Universe builder worker")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Override universe refresh interval in seconds",
    )
    return parser.parse_args()


async def _run() -> None:
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    setup_logging(
        service_name="cryptoinsider.universe-worker",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.universe-worker")
    interval_seconds = args.interval_seconds or settings.universe_interval_seconds
    lease_holder = f"{socket.gethostname()}:{os.getpid()}:universe"
    lease_ttl_seconds = max(30, int(interval_seconds) * 2)
    logger.info(
        "Service started interval_seconds=%s min_age_days=%s min_trades_30d=%s min_active_days_30d=%s min_win_rate_30d=%s max_drawdown_30d_pct=%s max_last_activity_minutes=%s",
        interval_seconds,
        settings.universe_min_age_days,
        settings.universe_min_trades_30d,
        settings.universe_min_active_days_30d,
        settings.universe_min_win_rate_30d,
        settings.universe_max_drawdown_30d_pct,
        settings.universe_max_last_activity_minutes,
    )

    cycle = 0
    while True:
        cycle += 1
        cycle_started = time.monotonic()
        with bind_log_context(cycle=cycle, cycle_id=new_trace_id("uni")):
            cycle_skipped = False
            try:
                with TraderStore(settings.database_dsn) as store:
                    has_lease = store.acquire_runtime_lease(
                        lock_name="tracked-traders-write",
                        holder=lease_holder,
                        ttl_seconds=lease_ttl_seconds,
                    )
                    if not has_lease:
                        logger.info("Universe cycle skipped: lease is held by another worker")
                        cycle_skipped = True
                    else:
                        try:
                            lifecycle_stats = store.apply_trader_lifecycle(
                                listed_within_minutes=settings.trader_listed_within_minutes,
                                stale_after_minutes=settings.trader_stale_after_minutes,
                                archive_after_days=settings.trader_archive_after_days,
                            )
                            universe_size = store.refresh_traders_universe_from_tracked(
                                min_age_days=settings.universe_min_age_days,
                                min_trades_30d=settings.universe_min_trades_30d,
                                min_active_days_30d=settings.universe_min_active_days_30d,
                                min_win_rate_30d=settings.universe_min_win_rate_30d,
                                max_drawdown_30d_pct=settings.universe_max_drawdown_30d_pct,
                                max_last_activity_minutes=settings.universe_max_last_activity_minutes,
                                min_realized_pnl_30d=settings.universe_min_realized_pnl_30d,
                                min_score=settings.universe_min_score,
                                max_size=settings.universe_max_size,
                            )
                        finally:
                            store.release_runtime_lease(
                                lock_name="tracked-traders-write",
                                holder=lease_holder,
                            )
                if not cycle_skipped:
                    logger.info(
                        "Lifecycle changed=%s listed=%s unlisted=%s stale=%s archived=%s | Universe size=%s",
                        lifecycle_stats.get("changed", 0),
                        lifecycle_stats.get("ACTIVE_LISTED", 0),
                        lifecycle_stats.get("ACTIVE_UNLISTED", 0),
                        lifecycle_stats.get("STALE", 0),
                        lifecycle_stats.get("ARCHIVED", 0),
                        universe_size,
                    )
            except Exception as exc:
                logger.exception("Universe cycle failed: %s", exc)

        if args.once:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_for = max(1, int(interval_seconds - elapsed))
        logger.info("Sleeping %s second(s) before next cycle", sleep_for)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
