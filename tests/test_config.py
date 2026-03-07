from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from bot.config import load_settings


class ConfigTests(unittest.TestCase):
    def test_bot_username_is_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old = dict(os.environ)
            try:
                os.environ["TELEGRAM_BOT_TOKEN"] = "123:abc"
                os.environ["TELEGRAM_CHANNEL_ID"] = "-1001"
                os.environ["TELEGRAM_BOT_USERNAME"] = "@test_bot"
                os.environ["DATABASE_PATH"] = str(Path(tmpdir) / "db.sqlite")
                os.environ["SOURCES_CONFIG_PATH"] = str(Path(tmpdir) / "sources.yaml")

                settings = load_settings()
                self.assertEqual(settings.telegram_bot_username, "test_bot")
            finally:
                os.environ.clear()
                os.environ.update(old)


if __name__ == "__main__":
    unittest.main()
