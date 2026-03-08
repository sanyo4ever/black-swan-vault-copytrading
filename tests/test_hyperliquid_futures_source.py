from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from bot.sources.hyperliquid_futures import HyperliquidFuturesSource
from bot.trader_store import DeliveryMonitorTarget


def _make_source() -> HyperliquidFuturesSource:
    return HyperliquidFuturesSource(
        config={
            "id": "hl_futures_feed",
            "name": "Hyperliquid Futures",
            "enabled": True,
        },
        http_session=None,
        database_dsn=":memory:",
        info_url="https://api.hyperliquid.xyz/info",
    )


def _make_target(*, last_seen_fill_time: int | None) -> DeliveryMonitorTarget:
    return DeliveryMonitorTarget(
        address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        subscriber_count=1,
        priority_score=50.0,
        poll_interval_seconds=60,
        safety_lookback_seconds=90,
        bootstrap_lookback_minutes=180,
        last_seen_fill_time=last_seen_fill_time,
        idle_cycles=0,
        consecutive_errors=0,
    )


class HyperliquidFuturesSourceTests(unittest.IsolatedAsyncioTestCase):
    def test_build_side_prefers_exchange_side_code(self) -> None:
        self.assertEqual(
            HyperliquidFuturesSource._build_side({"dir": "Close Short", "side": "B"}),
            "BUY",
        )
        self.assertEqual(
            HyperliquidFuturesSource._build_side({"dir": "Open Long"}),
            "LONG",
        )
        self.assertEqual(
            HyperliquidFuturesSource._build_side({"side": "A"}),
            "SELL",
        )

    async def test_poll_delivery_target_bootstrap_uses_newest_slice(self) -> None:
        source = _make_source()
        source._fetch_fills_with_retry = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {"time": 100, "coin": "DOGE"},
                {"time": 101, "coin": "DOGE"},
                {"time": 102, "coin": "DOGE"},
                {"time": 103, "coin": "DOGE"},
                {"time": 104, "coin": "DOGE"},
                {"time": 105, "coin": "DOGE"},
                {"time": 106, "coin": "DOGE"},
            ]
        )

        outcome = await source._poll_delivery_target(  # type: ignore[attr-defined]
            target=_make_target(last_seen_fill_time=None),
            max_fills_per_trader=3,
        )

        selected_times = [int(item["time"]) for item in outcome.fills]
        self.assertEqual(selected_times, [104, 105, 106])
        self.assertEqual(outcome.watermark_fill_time, 106)
        self.assertEqual(outcome.newest_observed_fill_time, 106)
        self.assertEqual(outcome.dropped_fills, 4)

    async def test_poll_delivery_target_watermark_uses_oldest_slice(self) -> None:
        source = _make_source()
        source._fetch_fills_with_retry = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {"time": 101, "coin": "DOGE"},
                {"time": 102, "coin": "DOGE"},
                {"time": 103, "coin": "DOGE"},
                {"time": 104, "coin": "DOGE"},
                {"time": 105, "coin": "DOGE"},
                {"time": 106, "coin": "DOGE"},
            ]
        )

        outcome = await source._poll_delivery_target(  # type: ignore[attr-defined]
            target=_make_target(last_seen_fill_time=100),
            max_fills_per_trader=2,
        )

        selected_times = [int(item["time"]) for item in outcome.fills]
        self.assertEqual(selected_times, [101, 102])
        self.assertEqual(outcome.watermark_fill_time, 102)
        self.assertEqual(outcome.newest_observed_fill_time, 106)
        self.assertEqual(outcome.dropped_fills, 4)


if __name__ == "__main__":
    unittest.main()
