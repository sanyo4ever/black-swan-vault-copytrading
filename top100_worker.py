from __future__ import annotations

import argparse
import asyncio
import logging
import time

from bot.config import load_settings
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


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | top100.worker | %(message)s",
    )


async def _run() -> None:
    _setup_logging()
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    interval_seconds = args.interval_seconds or settings.live_top100_interval_seconds

    while True:
        cycle_started = time.monotonic()
        try:
            with TraderStore(settings.database_dsn) as store:
                catalog_size = store.refresh_catalog_current(
                    activity_window_minutes=settings.live_top100_active_window_minutes,
                )
                count = store.refresh_top100_live(
                    max_rows=settings.live_top100_size,
                    active_window_minutes=settings.live_top100_active_window_minutes,
                )
            logging.info(
                "Catalog refresh complete: size=%s | Top100 refresh complete: size=%s",
                catalog_size,
                count,
            )
        except Exception as exc:
            logging.exception("Top100 cycle failed: %s", exc)

        if args.once:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_for = max(1, int(interval_seconds - elapsed))
        logging.info("Sleeping %s second(s) before next cycle", sleep_for)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
