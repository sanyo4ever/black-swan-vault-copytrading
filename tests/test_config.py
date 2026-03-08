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
                self.assertTrue(settings.database_dsn.endswith("db.sqlite"))
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
                self.assertTrue(settings.database_dsn.endswith("db.sqlite"))
            finally:
                os.environ.clear()
                os.environ.update(old)

    def test_database_url_overrides_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old = dict(os.environ)
            try:
                os.environ["TELEGRAM_BOT_TOKEN"] = "123:abc"
                os.environ["TELEGRAM_CHANNEL_ID"] = "-1001"
                os.environ["DATABASE_PATH"] = str(Path(tmpdir) / "db.sqlite")
                os.environ["DATABASE_URL"] = "postgresql://user:pass@localhost:5432/cryptoinsider"
                os.environ["SOURCES_CONFIG_PATH"] = str(Path(tmpdir) / "sources.yaml")

                settings = load_settings()
                self.assertEqual(
                    settings.database_dsn,
                    "postgresql://user:pass@localhost:5432/cryptoinsider",
                )
            finally:
                os.environ.clear()
                os.environ.update(old)

    def test_logging_env_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old = dict(os.environ)
            try:
                os.environ["TELEGRAM_BOT_TOKEN"] = "123:abc"
                os.environ["TELEGRAM_CHANNEL_ID"] = "-1001"
                os.environ["DATABASE_PATH"] = str(Path(tmpdir) / "db.sqlite")
                os.environ["SOURCES_CONFIG_PATH"] = str(Path(tmpdir) / "sources.yaml")
                os.environ["LOG_LEVEL"] = "debug"
                os.environ["LOG_FORMAT"] = "json"
                os.environ["LOG_DIRECTORY"] = "/tmp/cryptoinsider-logs"
                os.environ["LOG_FILE_MAX_BYTES"] = "2048"
                os.environ["LOG_FILE_BACKUP_COUNT"] = "7"
                os.environ["LOG_TELEGRAM_HTTP"] = "true"
                os.environ["DELIVERY_SEND_CONCURRENCY"] = "17"
                os.environ["DELIVERY_CHAT_MIN_INTERVAL_MS"] = "333"
                os.environ["ADMIN_RATE_LIMIT_WINDOW_SECONDS"] = "45"
                os.environ["ADMIN_RATE_LIMIT_MAX_REQUESTS"] = "22"
                os.environ["PUBLIC_RATE_LIMIT_WINDOW_SECONDS"] = "30"
                os.environ["PUBLIC_RATE_LIMIT_MAX_REQUESTS"] = "90"

                settings = load_settings()
                self.assertEqual(settings.log_level, "DEBUG")
                self.assertEqual(settings.log_format, "json")
                self.assertEqual(settings.log_directory, "/tmp/cryptoinsider-logs")
                self.assertEqual(settings.log_file_max_bytes, 2048)
                self.assertEqual(settings.log_file_backup_count, 7)
                self.assertTrue(settings.log_telegram_http)
                self.assertEqual(settings.delivery_send_concurrency, 17)
                self.assertEqual(settings.delivery_chat_min_interval_ms, 333)
                self.assertEqual(settings.admin_rate_limit_window_seconds, 45)
                self.assertEqual(settings.admin_rate_limit_max_requests, 22)
                self.assertEqual(settings.public_rate_limit_window_seconds, 30)
                self.assertEqual(settings.public_rate_limit_max_requests, 90)
            finally:
                os.environ.clear()
                os.environ.update(old)

    def test_showcase_rotation_env_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            old = dict(os.environ)
            try:
                os.environ["TELEGRAM_BOT_TOKEN"] = "123:abc"
                os.environ["TELEGRAM_CHANNEL_ID"] = "-1001"
                os.environ["DATABASE_PATH"] = str(Path(tmpdir) / "db.sqlite")
                os.environ["SOURCES_CONFIG_PATH"] = str(Path(tmpdir) / "sources.yaml")
                os.environ["SHOWCASE_MODE_ENABLED"] = "true"
                os.environ["SHOWCASE_SLOTS"] = "25"
                os.environ["ROTATION_SCOUT_INTERVAL_HOURS"] = "8"
                os.environ["ROTATION_HEALTH_INTERVAL_MINUTES"] = "45"
                os.environ["ROTATION_BOOTSTRAP_INTERVAL_MINUTES"] = "360"
                os.environ["ROTATION_SCOUT_ON_STALE_IMMEDIATE"] = "true"
                os.environ["ROTATION_STALE_HOURS"] = "96"
                os.environ["ROTATION_STALE_CYCLES"] = "4"
                os.environ["ROTATION_SCORE_THRESHOLD_PCT"] = "6.5"
                os.environ["ROTATION_SCOUT_CANDIDATES"] = "40"
                os.environ["ROTATION_BOOTSTRAP_CANDIDATES"] = "70"
                os.environ["ROTATION_METRICS_REFRESH_HOURS"] = "12"

                settings = load_settings()
                self.assertTrue(settings.showcase_mode_enabled)
                self.assertEqual(settings.showcase_slots, 25)
                self.assertEqual(settings.rotation_scout_interval_hours, 8)
                self.assertEqual(settings.rotation_health_interval_minutes, 45)
                self.assertEqual(settings.rotation_bootstrap_interval_minutes, 360)
                self.assertTrue(settings.rotation_scout_on_stale_immediate)
                self.assertEqual(settings.rotation_stale_hours, 96)
                self.assertEqual(settings.rotation_stale_cycles, 4)
                self.assertEqual(settings.rotation_score_threshold_pct, 6.5)
                self.assertEqual(settings.rotation_scout_candidates, 40)
                self.assertEqual(settings.rotation_bootstrap_candidates, 70)
                self.assertEqual(settings.rotation_metrics_refresh_hours, 12)
            finally:
                os.environ.clear()
                os.environ.update(old)


if __name__ == "__main__":
    unittest.main()
