from __future__ import annotations

import unittest

from bot.sources.rss import _extract_signal_fields


class RssPatternTests(unittest.TestCase):
    def test_extract_signal_fields_matches_expected_tokens(self) -> None:
        parsed = _extract_signal_fields(
            "LONG BTCUSDT entry: 62000 sl: 60000 tp: 68000 timeframe 1h"
        )
        self.assertEqual(parsed["side"], "LONG")
        self.assertEqual(parsed["symbol"], "BTCUSDT")
        self.assertEqual(parsed["entry"], "62000")
        self.assertEqual(parsed["stop_loss"], "60000")
        self.assertEqual(parsed["take_profit"], "68000")
        self.assertEqual(parsed["timeframe"], "1H")


if __name__ == "__main__":
    unittest.main()
