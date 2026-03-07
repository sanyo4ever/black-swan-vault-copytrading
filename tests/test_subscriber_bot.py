from __future__ import annotations

import logging
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.subscriber_bot import _fmt_remaining, _handle_start_with_payload
from bot.telegram_client import TelegramClientError
from bot.trader_store import TraderStore


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


class SubscriberBotFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_to_direct_chat_when_forum_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            trader_address = "0xabcabcabcabcabcabcabcabcabcabcabcabcabca"
            with TraderStore(db_path) as store:
                store.add_manual(address=trader_address, label="Fallback")

            settings = SimpleNamespace(
                telegram_bot_token="123:abc",
                database_dsn=str(db_path),
                subscription_lifetime_hours=24,
            )
            logger = logging.getLogger("test.subscriber-bot")

            forum_error = TelegramClientError(
                method="createForumTopic",
                status_code=400,
                error_code=400,
                description="Bad Request: the chat is not a forum",
            )
            send_mock = AsyncMock()
            with (
                patch("bot.subscriber_bot.create_forum_topic", AsyncMock(side_effect=forum_error)),
                patch("bot.subscriber_bot.send_message", send_mock),
            ):
                await _handle_start_with_payload(
                    session=None,  # not used by mocked telegram helpers
                    settings=settings,
                    chat_id=777001,
                    payload=f"sub_{trader_address}",
                    logger=logger,
                )

            with TraderStore(db_path) as store:
                sessions = store.list_delivery_sessions_for_chat(chat_id=777001)
            self.assertEqual(len(sessions), 1)
            self.assertIsNone(sessions[0].message_thread_id)
            self.assertIsNone(sessions[0].topic_name)
            self.assertGreaterEqual(send_mock.await_count, 2)


if __name__ == "__main__":
    unittest.main()
