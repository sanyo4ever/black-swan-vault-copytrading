from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
import time

import aiohttp

from bot.config import load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
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


async def _run() -> None:
    args = _parse_args()
    settings = load_settings(require_telegram=False)
    setup_logging(
        service_name="cryptoinsider.discovery-worker",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.discovery-worker")
    interval_seconds = args.interval_seconds or settings.discovery_interval_seconds
    lease_holder = f"{socket.gethostname()}:{os.getpid()}:discovery"
    lease_ttl_seconds = max(30, int(interval_seconds) * 2)
    logger.info(
        "Service started interval_seconds=%s min_age_days=%s min_trades_30d=%s min_active_days_30d=%s min_win_rate_30d=%s max_drawdown_30d_pct=%s max_last_activity_minutes=%s",
        interval_seconds,
        settings.discovery_min_age_days,
        settings.discovery_min_trades_30d,
        settings.discovery_min_active_days_30d,
        settings.discovery_min_win_rate_30d,
        settings.discovery_max_drawdown_30d_pct,
        settings.discovery_max_last_activity_minutes,
    )

    config = HyperliquidDiscoveryConfig(
        info_url=settings.hyperliquid_info_url,
        candidate_limit=settings.discovery_candidate_limit,
        min_age_days=settings.discovery_min_age_days,
        min_trades_30d=settings.discovery_min_trades_30d,
        min_active_days_30d=settings.discovery_min_active_days_30d,
        min_win_rate_30d=settings.discovery_min_win_rate_30d,
        max_drawdown_30d_pct=settings.discovery_max_drawdown_30d_pct,
        max_last_activity_minutes=settings.discovery_max_last_activity_minutes,
        min_realized_pnl_30d=settings.discovery_min_realized_pnl_30d,
        require_positive_pnl_30d=settings.discovery_require_positive_pnl_30d,
        min_trades_7d=settings.discovery_min_trades_7d,
        window_hours=settings.discovery_window_hours,
        concurrency=settings.discovery_concurrency,
        fill_cap_hint=settings.discovery_fill_cap_hint,
        age_probe_enabled=settings.discovery_age_probe_enabled,
        seed_addresses=settings.discovery_seed_addresses,
        nansen_api_url=settings.nansen_api_url,
        nansen_api_key=settings.nansen_api_key,
        nansen_candidate_limit=settings.nansen_candidate_limit,
    )

    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    cycle = 0
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            cycle += 1
            cycle_started = time.monotonic()
            with bind_log_context(cycle=cycle, cycle_id=new_trace_id("disc")):
                cycle_skipped = False
                try:
                    with TraderStore(settings.database_dsn) as store:
                        has_lease = store.acquire_runtime_lease(
                            lock_name="tracked-traders-write",
                            holder=lease_holder,
                            ttl_seconds=lease_ttl_seconds,
                        )
                        if not has_lease:
                            logger.info("Discovery cycle skipped: lease is held by another worker")
                            cycle_skipped = True
                        else:
                            service = HyperliquidDiscoveryService(
                                http_session=session,
                                store=store,
                                config=config,
                                logger=logger,
                            )
                            summary = await service.discover()
                    if not cycle_skipped:
                        logger.info(
                            "Cycle done: candidates=%s qualified=%s upserted=%s unlisted=%s",
                            summary["candidates"],
                            summary["qualified"],
                            summary["upserted"],
                            summary.get("unlisted", summary.get("pruned", 0)),
                        )
                except Exception as exc:
                    logger.exception("Discovery cycle failed: %s", exc)

                if args.once:
                    return

                elapsed = time.monotonic() - cycle_started
                sleep_for = max(1, int(interval_seconds - elapsed))
                logger.info("Sleeping %s second(s) before next cycle", sleep_for)
                await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(_run())
