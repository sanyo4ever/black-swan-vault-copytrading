from __future__ import annotations

import argparse
import asyncio
import logging
import time

import aiohttp

from bot.config import load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.trader_store import TraderStore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous futures trader discovery worker")
    parser.add_argument("--once", action="store_true", help="Run one discovery cycle and exit")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Override discovery interval in seconds",
    )
    return parser.parse_args()


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | discovery.worker | %(message)s",
    )


async def _run() -> None:
    _setup_logging()
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    interval_seconds = args.interval_seconds or settings.discovery_interval_seconds

    config = HyperliquidDiscoveryConfig(
        info_url=settings.hyperliquid_info_url,
        candidate_limit=settings.discovery_candidate_limit,
        min_age_days=settings.discovery_min_age_days,
        min_trades_30d=settings.discovery_min_trades_30d,
        min_active_days_30d=settings.discovery_min_active_days_30d,
        min_trades_7d=settings.discovery_min_trades_7d,
        window_hours=settings.discovery_window_hours,
        concurrency=settings.discovery_concurrency,
        fill_cap_hint=settings.discovery_fill_cap_hint,
        age_probe_enabled=settings.discovery_age_probe_enabled,
    )

    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            cycle_started = time.monotonic()
            try:
                with TraderStore(settings.database_path) as store:
                    service = HyperliquidDiscoveryService(
                        http_session=session,
                        store=store,
                        config=config,
                    )
                    summary = await service.discover()
                logging.info(
                    "Cycle done: candidates=%s qualified=%s upserted=%s pruned=%s",
                    summary["candidates"],
                    summary["qualified"],
                    summary["upserted"],
                    summary.get("pruned", 0),
                )
            except Exception as exc:
                logging.exception("Discovery cycle failed: %s", exc)

            if args.once:
                return

            elapsed = time.monotonic() - cycle_started
            sleep_for = max(1, int(interval_seconds - elapsed))
            logging.info("Sleeping %s second(s) before next cycle", sleep_for)
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
