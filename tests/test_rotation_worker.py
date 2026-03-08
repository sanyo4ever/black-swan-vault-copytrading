from __future__ import annotations

import logging
import time
import unittest
from types import SimpleNamespace

from rotation_worker import LightweightRotationWorker


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        hyperliquid_info_url="https://api.hyperliquid.xyz/info",
        rotation_bootstrap_candidates=60,
        rotation_scout_candidates=30,
        showcase_slots=25,
        discovery_min_age_days=30,
        discovery_min_trades_30d=120,
        discovery_min_active_days_30d=12,
        discovery_min_win_rate_30d=0.52,
        discovery_max_drawdown_30d_pct=25.0,
        discovery_max_last_activity_minutes=60,
        discovery_min_realized_pnl_30d=0.0,
        discovery_require_positive_pnl_30d=True,
        discovery_min_trades_7d=1,
        discovery_window_hours=24,
        discovery_concurrency=6,
        discovery_fill_cap_hint=1900,
        discovery_age_probe_enabled=True,
        discovery_seed_addresses=(),
        nansen_api_url="https://api.nansen.ai",
        nansen_api_key="",
        nansen_candidate_limit=60,
        rotation_scout_max_last_activity_minutes=1440,
    )


class RotationWorkerFilterTests(unittest.TestCase):
    def test_scout_last_activity_override_relaxes_only_activity_window(self) -> None:
        worker = LightweightRotationWorker(
            settings=_settings(),
            http_session=None,
            logger=logging.getLogger("test.rotation"),
        )
        now_ms = int(time.time() * 1000)
        candidate = {
            "age_days": 45,
            "trades_30d": 260,
            "active_days_30d": 18,
            "trades_7d": 20,
            "last_fill_time": now_ms - (2 * 60 * 60 * 1000),  # 2h ago
            "win_rate_30d": 0.61,
            "max_drawdown_30d": 12.0,
            "realized_pnl_30d": 1200.0,
        }

        self.assertFalse(worker._passes_hard_filters(candidate, now_ms=now_ms))
        self.assertTrue(
            worker._passes_hard_filters(
                candidate,
                now_ms=now_ms,
                max_last_activity_minutes=worker._settings.rotation_scout_max_last_activity_minutes,
            )
        )


if __name__ == "__main__":
    unittest.main()
