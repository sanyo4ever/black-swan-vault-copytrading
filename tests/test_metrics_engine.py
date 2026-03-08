from __future__ import annotations

import json
import logging
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from bot.discovery.hyperliquid import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.trader_store import TraderStore


class _StubDiscoveryService(HyperliquidDiscoveryService):
    def __init__(
        self,
        *,
        store: TraderStore,
        fills: list[dict],
        account_value: float = 10_000.0,
    ) -> None:
        super().__init__(
            http_session=object(),  # network is stubbed in tests
            store=store,
            config=HyperliquidDiscoveryConfig(
                age_probe_enabled=False,
                fill_cap_hint=10_000,
            ),
            logger=logging.getLogger("test.metrics"),
        )
        self._fills = fills
        self._account_value = account_value

    async def _info(self, payload: dict) -> object:
        payload_type = str(payload.get("type", ""))
        if payload_type == "userFillsByTime":
            return list(self._fills)
        if payload_type == "clearinghouseState":
            return {
                "marginSummary": {
                    "accountValue": str(self._account_value),
                    "totalNtlPos": "2000",
                    "totalMarginUsed": "500",
                }
            }
        if payload_type == "userNonFundingLedgerUpdates":
            return []
        raise AssertionError(f"Unexpected payload in test: {payload}")


class MetricsComputationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "metrics.db"
        self.store = TraderStore(self.db_path)
        self.service = _StubDiscoveryService(store=self.store, fills=[])

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_compute_period_stats_golden_fixture(self) -> None:
        fills = [
            {"px": "100", "sz": "1", "closedPnl": "10"},
            {"px": "200", "sz": "1", "closedPnl": "-20"},
            {"px": "50", "sz": "2", "closedPnl": "0"},
            {"px": "150", "sz": "1", "closedPnl": "30"},
        ]
        stats = self.service._compute_period_stats(fills=fills, account_value=1000.0)

        self.assertEqual(stats["trade_count"], 4)
        self.assertEqual(stats["closed_trade_count"], 3)
        self.assertAlmostEqual(float(stats["realized_pnl"] or 0.0), 20.0, places=6)
        self.assertAlmostEqual(float(stats["win_rate"] or 0.0), 2 / 3, places=6)
        self.assertAlmostEqual(float(stats["profit_to_loss_ratio"] or 0.0), 2.0, places=6)
        self.assertAlmostEqual(float(stats["roi_pct"] or 0.0), 2.0, places=6)
        self.assertAlmostEqual(float(stats["max_drawdown_pct"] or 0.0), 10.0, places=3)
        self.assertGreater(float(stats["sharpe"] or 0.0), 0.0)
        self.assertIsNone(stats["sortino"])  # downside sample contains one point -> 0 std

    def test_compute_period_stats_handles_empty_or_flat_trades(self) -> None:
        fills = [
            {"px": "100", "sz": "1", "closedPnl": "0"},
            {"px": "250", "sz": "0.5", "closedPnl": "0.0"},
        ]
        stats = self.service._compute_period_stats(fills=fills, account_value=2000.0)

        self.assertEqual(stats["trade_count"], 2)
        self.assertEqual(stats["closed_trade_count"], 0)
        self.assertIsNone(stats["win_rate"])
        self.assertIsNone(stats["profit_to_loss_ratio"])
        self.assertIsNone(stats["roi_volatility_pct"])
        self.assertIsNone(stats["max_drawdown_pct"])
        self.assertAlmostEqual(float(stats["realized_pnl"] or 0.0), 0.0, places=6)


class MetricsFetchPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_metrics_builds_full_stats_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "metrics-pipeline.db"
            store = TraderStore(db_path)
            now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
            fills = [
                {
                    "time": now_ms - (10 * 60 * 1000),
                    "px": "100",
                    "sz": "1.5",
                    "closedPnl": "14",
                    "dir": "Open Long",
                    "side": "B",
                    "fee": "0.8",
                },
                {
                    "time": now_ms - (3 * 24 * 60 * 60 * 1000),
                    "px": "120",
                    "sz": "1.0",
                    "closedPnl": "-8",
                    "dir": "Close Long",
                    "side": "A",
                    "fee": "0.6",
                },
                {
                    "time": now_ms - (12 * 24 * 60 * 60 * 1000),
                    "px": "140",
                    "sz": "0.8",
                    "closedPnl": "10",
                    "dir": "Open Short",
                    "side": "A",
                    "fee": "0.7",
                },
            ]
            service = _StubDiscoveryService(store=store, fills=fills, account_value=20_000.0)

            metrics = await service._fetch_metrics(
                {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "label": "Fixture",
                    "source": "hyperliquid_recent_trades",
                    "vault_tvl": 150_000.0,
                },
                now_ms=now_ms,
            )
            stats_payload = json.loads(str(metrics["stats_json"]))

            self.assertIn("metrics_1d", stats_payload)
            self.assertIn("metrics_7d", stats_payload)
            self.assertIn("metrics_30d", stats_payload)
            for period in ("metrics_1d", "metrics_7d", "metrics_30d"):
                block = stats_payload[period]
                for key in (
                    "roi_pct",
                    "realized_pnl",
                    "win_rate",
                    "wins",
                    "losses",
                    "profit_to_loss_ratio",
                    "trade_count",
                    "avg_pnl_per_trade",
                    "max_drawdown_pct",
                    "sharpe",
                    "sortino",
                    "roi_volatility_pct",
                ):
                    self.assertIn(key, block)

            self.assertGreaterEqual(float(metrics["score"]), 0.0)
            self.assertLessEqual(float(metrics["score"]), 100.0)
            self.assertGreaterEqual(float(metrics["max_drawdown_30d"] or 0.0), 0.0)
            self.assertGreaterEqual(float(metrics["roi_volatility_30d"] or 0.0), 0.0)
            self.assertGreaterEqual(float(metrics["trades_30d"]), 1.0)
            self.assertIsNotNone(metrics["win_rate_30d"])

            store.close()


if __name__ == "__main__":
    unittest.main()
