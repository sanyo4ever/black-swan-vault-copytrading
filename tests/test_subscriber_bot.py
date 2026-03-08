from __future__ import annotations

import asyncio
import logging
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.subscriber_bot import (
    _fmt_remaining,
    _gc_lock_registry,
    _handle_start_with_payload,
    _short,
)
from bot.trader_store import PERMANENT_SUBSCRIPTION_EXPIRES_AT, TraderStore


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

    def test_fmt_remaining_permanent(self) -> None:
        self.assertEqual(_fmt_remaining(PERMANENT_SUBSCRIPTION_EXPIRES_AT), "until cancellation")

    def test_short_escapes_html(self) -> None:
        rendered = _short("</code><b>evil</b>")
        self.assertNotIn("<", rendered)
        self.assertNotIn(">", rendered)
        self.assertIn("&lt;", rendered)

    def test_gc_lock_registry_prunes_stale_entries_when_over_limit(self) -> None:
        locks = {f"k{i}": asyncio.Lock() for i in range(5)}
        used = {key: 0.0 for key in locks}
        removed = _gc_lock_registry(
            locks,
            used,
            max_tracked=2,
            stale_seconds=10.0,
            now=100.0,
        )
        self.assertEqual(removed, 5)
        self.assertEqual(locks, {})
        self.assertEqual(used, {})


class SubscriberBotFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_payload_creates_shared_topic_in_forum_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            trader_address = "0xabcabcabcabcabcabcabcabcabcabcabcabcabca"
            with TraderStore(db_path) as store:
                store.add_manual(address=trader_address, label="Fallback")

            settings = SimpleNamespace(
                telegram_bot_token="123:abc",
                database_dsn=str(db_path),
                telegram_forum_chat_id="-1001234567890",
                telegram_join_url="https://t.me/blackswanvaultcopytrading",
                subscriber_telegram_retry_attempts=1,
            )
            logger = logging.getLogger("test.subscriber-bot")

            send_mock = AsyncMock()
            create_mock = AsyncMock(return_value={"message_thread_id": 4433})
            with (
                patch("bot.subscriber_bot.create_forum_topic", create_mock),
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
                topic = store.get_trader_forum_topic(
                    trader_address=trader_address,
                    forum_chat_id=settings.telegram_forum_chat_id,
                )
            self.assertIsNotNone(topic)
            self.assertEqual(topic.message_thread_id, 4433)
            create_mock.assert_awaited_once()
            self.assertEqual(create_mock.await_args.kwargs.get("chat_id"), settings.telegram_forum_chat_id)
            self.assertGreaterEqual(send_mock.await_count, 1)
            texts = [
                str(call.kwargs.get("text", ""))
                for call in send_mock.await_args_list
            ]
            self.assertTrue(any("Open Group" in text for text in texts))
            self.assertTrue(any("Trader Thread" in text for text in texts))

    async def test_start_payload_reuses_existing_shared_topic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            trader_address = "0xdefdefdefdefdefdefdefdefdefdefdefdefdefd"
            with TraderStore(db_path) as store:
                store.add_manual(address=trader_address, label="Reuse")
                store.upsert_trader_forum_topic(
                    trader_address=trader_address,
                    forum_chat_id="-1005566778899",
                    message_thread_id=9901,
                    topic_name="reuse-topic",
                )

            settings = SimpleNamespace(
                telegram_bot_token="123:abc",
                database_dsn=str(db_path),
                telegram_forum_chat_id="-1005566778899",
                telegram_join_url="https://t.me/blackswanvaultcopytrading",
                subscriber_telegram_retry_attempts=1,
            )
            logger = logging.getLogger("test.subscriber-bot")

            send_mock = AsyncMock()
            create_mock = AsyncMock(return_value={"message_thread_id": 1})
            with (
                patch("bot.subscriber_bot.create_forum_topic", create_mock),
                patch("bot.subscriber_bot.send_message", send_mock),
            ):
                await _handle_start_with_payload(
                    session=None,  # not used by mocked telegram helpers
                    settings=settings,
                    chat_id=888002,
                    payload=f"sub_{trader_address}",
                    logger=logger,
                )

            create_mock.assert_not_awaited()
            texts = [str(call.kwargs.get("text", "")) for call in send_mock.await_args_list]
            self.assertTrue(any("/9901" in text for text in texts))


if __name__ == "__main__":
    unittest.main()
