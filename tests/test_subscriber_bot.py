from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from bot.subscriber_bot import _fmt_remaining


class SubscriberBotTests(unittest.TestCase):
    def test_fmt_remaining_invalid(self) -> None:
        self.assertEqual(_fmt_remaining("not-a-date"), "-")

    def test_fmt_remaining_expired(self) -> None:
        old = "2000-01-01 00:00:00"
        self.assertEqual(_fmt_remaining(old), "expired")

    def test_fmt_remaining_future(self) -> None:
        future = (datetime.now(tz=UTC) + timedelta(hours=1, minutes=10)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        value = _fmt_remaining(future)
        self.assertIn("h", value)
        self.assertIn("m", value)


if __name__ == "__main__":
    unittest.main()
