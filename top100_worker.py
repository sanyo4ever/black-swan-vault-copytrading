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
        "Service started interval_seconds=%s active_window_minutes=%s top_size=%s",
        interval_seconds,
        settings.live_top100_active_window_minutes,
        settings.live_top100_size,
    )

    cycle = 0
    while True:
        cycle += 1
        cycle_started = time.monotonic()
        with bind_log_context(cycle=cycle, cycle_id=new_trace_id("top")):
            try:
                with TraderStore(settings.database_dsn) as store:
                    catalog_size = store.refresh_catalog_current(
                        activity_window_minutes=settings.live_top100_active_window_minutes,
                    )
                    count = store.refresh_top100_live(
                        max_rows=settings.live_top100_size,
                        active_window_minutes=settings.live_top100_active_window_minutes,
                    )
                logger.info(
                    "Catalog refresh complete: size=%s | Top100 refresh complete: size=%s",
                    catalog_size,
                    count,
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
