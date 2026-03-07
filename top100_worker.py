from __future__ import annotations

import argparse
import asyncio
import logging
import time

from bot.config import load_settings
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.trader_store import TraderStore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live top100 builder worker")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Override live top100 refresh interval in seconds",
    )
    return parser.parse_args()


async def _run() -> None:
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    setup_logging(
        service_name="cryptoinsider.top100-worker",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.top100-worker")
    interval_seconds = args.interval_seconds or settings.live_top100_interval_seconds
    logger.info(
        "Service started interval_seconds=%s active_window_minutes=%s top_size=%s hot_size=%s warm_size=%s",
        interval_seconds,
        settings.live_top100_active_window_minutes,
        settings.live_top100_size,
        settings.monitor_hot_size,
        settings.monitor_warm_size,
    )

    cycle = 0
    while True:
        cycle += 1
        cycle_started = time.monotonic()
        with bind_log_context(cycle=cycle, cycle_id=new_trace_id("top")):
            try:
                with TraderStore(settings.database_dsn) as store:
                    lifecycle_stats = store.apply_trader_lifecycle(
                        listed_within_minutes=settings.trader_listed_within_minutes,
                        stale_after_minutes=settings.trader_stale_after_minutes,
                        archive_after_days=settings.trader_archive_after_days,
                    )
                    catalog_size = store.refresh_catalog_current(
                        activity_window_minutes=settings.live_top100_active_window_minutes,
                    )
                    count = store.refresh_top100_live(
                        max_rows=settings.live_top100_size,
                        active_window_minutes=settings.live_top100_active_window_minutes,
                    )
                    monitoring_stats = store.refresh_monitoring_pool(
                        hot_size=settings.monitor_hot_size,
                        warm_size=settings.monitor_warm_size,
                        hot_poll_seconds=settings.monitor_hot_poll_seconds,
                        warm_poll_seconds=settings.monitor_warm_poll_seconds,
                        cold_poll_seconds=settings.monitor_cold_poll_seconds,
                        hot_recency_minutes=settings.monitor_hot_recency_minutes,
                        warm_recency_minutes=settings.monitor_warm_recency_minutes,
                    )
                logger.info(
                    "Lifecycle changed=%s listed=%s unlisted=%s stale=%s archived=%s | Catalog size=%s | Top100 size=%s | Monitoring pool total=%s hot=%s warm=%s cold=%s",
                    lifecycle_stats.get("changed", 0),
                    lifecycle_stats.get("ACTIVE_LISTED", 0),
                    lifecycle_stats.get("ACTIVE_UNLISTED", 0),
                    lifecycle_stats.get("STALE", 0),
                    lifecycle_stats.get("ARCHIVED", 0),
                    catalog_size,
                    count,
                    monitoring_stats.get("total", 0),
                    monitoring_stats.get("hot", 0),
                    monitoring_stats.get("warm", 0),
                    monitoring_stats.get("cold", 0),
                )
            except Exception as exc:
                logger.exception("Top100 cycle failed: %s", exc)

        if args.once:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_for = max(1, int(interval_seconds - elapsed))
        logger.info("Sleeping %s second(s) before next cycle", sleep_for)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
