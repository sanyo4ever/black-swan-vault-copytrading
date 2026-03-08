from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from bot.qa import run_data_quality_audit, validate_stats_payload
from bot.trader_store import TraderStore


def _stats_payload() -> str:
    period = {
        "roi_pct": 12.5,
        "realized_pnl": 1250.0,
        "win_rate": 0.6,
        "wins": 12,
        "losses": 8,
        "profit_to_loss_ratio": 1.8,
        "trade_count": 24,
        "avg_pnl_per_trade": 52.0833,
        "max_drawdown_pct": 8.4,
        "sharpe": 1.9,
        "sortino": 2.6,
        "roi_volatility_pct": 3.1,
    }
    return json.dumps(
        {
            "metrics_1d": period,
            "metrics_7d": period,
            "metrics_30d": period,
        },
        separators=(",", ":"),
    )


def _insert_trader(*, store: TraderStore, address: str, stats_json: str) -> None:
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    store.upsert_discovered(
        address=address,
        label="QA",
        source="hyperliquid_recent_trades",
        trades_24h=12,
        active_hours_24h=8,
        trades_7d=95,
        trades_30d=180,
        active_days_30d=20,
        first_fill_time=now_ms - (60 * 86_400_000),
        last_fill_time=now_ms - (20 * 60_000),
        age_days=60.0,
        volume_usd_30d=230000.0,
        realized_pnl_30d=8400.0,
        fees_30d=420.0,
        win_rate_30d=0.62,
        long_ratio_30d=0.55,
        avg_notional_30d=1800.0,
        max_notional_30d=14000.0,
        account_value=50000.0,
        total_ntl_pos=12000.0,
        total_margin_used=3500.0,
        score=44.2,
        stats_json=stats_json,
    )


class DataQualityStatsSchemaTests(unittest.TestCase):
    def test_validate_stats_payload_accepts_valid_schema(self) -> None:
        issues = validate_stats_payload(_stats_payload(), address="0xabc")
        self.assertEqual(issues, [])

    def test_validate_stats_payload_flags_missing_and_invalid_ranges(self) -> None:
        payload = json.dumps(
            {
                "metrics_1d": {
                    "trade_count": -1,
                    "wins": 5,
                    "losses": 6,
                    "win_rate": 1.4,
                    "max_drawdown_pct": -3,
                    "roi_volatility_pct": -1.2,
                    "profit_to_loss_ratio": -2,
                }
            }
        )
        issues = validate_stats_payload(payload, address="0xbad")
        codes = {item.code for item in issues}
        self.assertIn("missing_period_metrics", codes)
        self.assertIn("negative_trade_count", codes)
        self.assertIn("win_rate_out_of_range", codes)
        self.assertIn("negative_drawdown", codes)
        self.assertIn("negative_volatility", codes)
        self.assertIn("negative_profit_to_loss_ratio", codes)


class DataQualityAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "qa.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_audit_passes_for_clean_dataset(self) -> None:
        with TraderStore(self.db_path) as store:
            _insert_trader(
                store=store,
                address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                stats_json=_stats_payload(),
            )

        summary = run_data_quality_audit(
            database_dsn=str(self.db_path),
            max_rows=100,
            freshness_minutes=60,
        )
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["critical_issues"], 0)
        self.assertEqual(summary["warning_issues"], 0)
        self.assertEqual(summary["checked_traders"], 1)

    def test_audit_detects_critical_issues(self) -> None:
        address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        with TraderStore(self.db_path) as store:
            _insert_trader(
                store=store,
                address=address,
                stats_json=_stats_payload(),
            )
            store._connection.execute(
                """
                UPDATE tracked_traders
                SET win_rate_30d = 1.5,
                    long_ratio_30d = -0.1,
                    stats_json = '{"metrics_30d":{"trade_count":-10}}'
                WHERE address = ?
                """,
                (address,),
            )
            store._connection.commit()

        summary = run_data_quality_audit(
            database_dsn=str(self.db_path),
            max_rows=100,
            freshness_minutes=60,
        )
        self.assertFalse(summary["passed"])
        self.assertGreaterEqual(summary["critical_issues"], 1)
        self.assertGreaterEqual(summary["warning_issues"], 1)


if __name__ == "__main__":
    unittest.main()
