from __future__ import annotations

import asyncio
import json

import aiohttp

from bot.config import load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.logging_setup import build_logging_options, setup_logging
from bot.trader_store import TraderStore


async def _main() -> None:
    settings = load_settings(require_telegram=False)
    setup_logging(
        service_name="cryptoinsider.discovery-once",
        options=build_logging_options(settings),
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
    async with aiohttp.ClientSession(timeout=timeout) as session:
        with TraderStore(settings.database_dsn) as store:
            service = HyperliquidDiscoveryService(
                http_session=session,
                store=store,
                config=config,
            )
            summary = await service.discover()

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
