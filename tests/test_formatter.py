from __future__ import annotations

import unittest

from bot.formatter import TELEGRAM_MAX_MESSAGE_LENGTH, format_signal
from bot.models import TradeSignal


class FormatterTests(unittest.TestCase):
    def test_format_signal_caps_message_length(self) -> None:
        signal = TradeSignal(
            source_id="rss",
            source_name="Very Long Source",
            external_id="x1",
            trader_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            symbol="BTCUSDT",
            side="LONG",
            entry="100000",
            timeframe="1H",
            note="x" * 10000,
            url="https://example.com/" + ("p" * 3000),
        )
        rendered = format_signal(signal)
        self.assertLessEqual(len(rendered), TELEGRAM_MAX_MESSAGE_LENGTH)
        self.assertIn("Very Long Source", rendered)


if __name__ == "__main__":
    unittest.main()
