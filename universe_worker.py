from __future__ import annotations

import argparse
import asyncio
import logging
import time

from bot.config import load_settings
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


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | universe.worker | %(message)s",
    )


async def _run() -> None:
    _setup_logging()
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    interval_seconds = args.interval_seconds or settings.universe_interval_seconds

    while True:
        cycle_started = time.monotonic()
        try:
            with TraderStore(settings.database_path) as store:
                universe_size = store.refresh_traders_universe_from_tracked(
                    min_age_days=settings.universe_min_age_days,
                    min_trades_30d=settings.universe_min_trades_30d,
                    min_win_rate_30d=settings.universe_min_win_rate_30d,
                    min_realized_pnl_30d=settings.universe_min_realized_pnl_30d,
                    min_score=settings.universe_min_score,
                    max_size=settings.universe_max_size,
                )
            logging.info("Universe refresh complete: size=%s", universe_size)
        except Exception as exc:
            logging.exception("Universe cycle failed: %s", exc)

        if args.once:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_for = max(1, int(interval_seconds - elapsed))
        logging.info("Sleeping %s second(s) before next cycle", sleep_for)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
