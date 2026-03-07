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

    def test_discovery_age_probe_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old = dict(os.environ)
            try:
                os.environ["TELEGRAM_BOT_TOKEN"] = "123:abc"
                os.environ["TELEGRAM_CHANNEL_ID"] = "-1001"
                os.environ["DATABASE_PATH"] = str(Path(tmpdir) / "db.sqlite")
                os.environ["SOURCES_CONFIG_PATH"] = str(Path(tmpdir) / "sources.yaml")
                os.environ["DISCOVERY_AGE_PROBE_ENABLED"] = "false"
                os.environ["DISCOVERY_FILL_CAP_HINT"] = "1234"

                settings = load_settings()
                self.assertFalse(settings.discovery_age_probe_enabled)
                self.assertEqual(settings.discovery_fill_cap_hint, 1234)
            finally:
                os.environ.clear()
                os.environ.update(old)


if __name__ == "__main__":
    unittest.main()
