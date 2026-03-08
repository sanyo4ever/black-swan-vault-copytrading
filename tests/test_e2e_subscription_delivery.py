from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.app import _process_retry_queue, _send_live_target
from bot.dedup import DedupStore
from bot.subscriber_bot import _handle_message, _handle_start_with_payload
from bot.telegram_client import TelegramClientError
from bot.trader_store import TraderStore


class _AlwaysFailDispatcher:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def send(self, *_args, **_kwargs) -> None:
        raise self._exc


class _RecordingDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None, str]] = []

    async def send(self, _session, *, chat_id, text: str, message_thread_id: int | None = None) -> None:
        self.calls.append((str(chat_id), message_thread_id, text))


class SubscriptionDeliveryE2ETests(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_stop_keeps_other_trader_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "e2e.db"
            a1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            a2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            with TraderStore(db_path) as store:
                store.add_manual(address=a1, label="A1")
                store.add_manual(address=a2, label="A2")

            settings = SimpleNamespace(
                telegram_bot_token="123:abc",
                database_dsn=str(db_path),
                subscription_lifetime_hours=0,
            )
            logger = logging.getLogger("test.e2e.sub")
            send_mock = AsyncMock()
            with (
                patch(
                    "bot.subscriber_bot.create_forum_topic",
                    AsyncMock(side_effect=[{"message_thread_id": 101}, {"message_thread_id": 202}]),
                ),
                patch("bot.subscriber_bot.delete_forum_topic", AsyncMock()),
                patch("bot.subscriber_bot.send_message", send_mock),
            ):
                await _handle_start_with_payload(
                    session=None,
                    settings=settings,
                    chat_id=7001,
                    payload=f"sub_{a1}",
                    logger=logger,
                )
                await _handle_start_with_payload(
                    session=None,
                    settings=settings,
                    chat_id=7001,
                    payload=f"sub_{a2}",
                    logger=logger,
                )

                await _handle_message(
                    session=None,
                    settings=settings,
                    message={
                        "chat": {"id": 7001, "type": "private"},
                        "text": f"/stop {a1}",
                    },
                    logger=logger,
                )

            with TraderStore(db_path) as store:
                sessions = store.list_delivery_sessions_for_chat(chat_id=7001)
            self.assertEqual(len(sessions), 1)
            self.assertEqual(sessions[0].trader_address, a2)
            self.assertGreaterEqual(send_mock.await_count, 3)

    async def test_fanout_mapping_for_multiple_chats_same_trader(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "fanout.db"
            address = "0xcccccccccccccccccccccccccccccccccccccccc"
            with TraderStore(db_path) as store:
                store.add_manual(address=address, label="Fanout")
                store.create_subscription_with_session(
                    chat_id=111,
                    trader_address=address,
                    message_thread_id=11,
                    topic_name="F1",
                    lifetime_hours=0,
                )
                store.create_subscription_with_session(
                    chat_id=222,
                    trader_address=address,
                    message_thread_id=22,
                    topic_name="F2",
                    lifetime_hours=0,
                )
                mapping = store.list_active_delivery_targets_by_trader()

            self.assertIn(address, mapping)
            targets = mapping[address]
            self.assertEqual(len(targets), 2)
            self.assertEqual({item.chat_id for item in targets}, {"111", "222"})
            self.assertEqual({item.message_thread_id for item in targets}, {11, 22})

    async def test_transient_error_queues_retry_and_retry_processor_sends(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "retry.db"
            dedup_path = Path(tmpdir) / "dedup.db"
            settings = SimpleNamespace(database_dsn=str(db_path))
            logger = logging.getLogger("test.e2e.retry")
            dedup_store = DedupStore(dedup_path)
            retry_error = TelegramClientError(
                method="sendMessage",
                status_code=429,
                error_code=429,
                description="Too Many Requests: retry after 1",
            )

            first_status = await _send_live_target(
                settings=settings,
                http_session=None,
                dispatcher=_AlwaysFailDispatcher(retry_error),
                logger=logger,
                dedup_key="sig-retry-1",
                message_text="payload",
                chat_id=10001,
                trader_address="0xdddddddddddddddddddddddddddddddddddddddd",
                message_thread_id=77,
            )
            self.assertEqual(first_status, "queued")

            with TraderStore(db_path) as store:
                store._connection.execute(
                    "UPDATE delivery_retry_queue SET next_attempt_at = '2000-01-01 00:00:00'"
                )
                store._connection.commit()

            dispatcher = _RecordingDispatcher()
            sent = await _process_retry_queue(
                settings=settings,
                http_session=None,
                dedup_store=dedup_store,
                dispatcher=dispatcher,
                logger=logger,
            )
            self.assertEqual(sent, 1)
            self.assertEqual(len(dispatcher.calls), 1)
            self.assertTrue(dedup_store.seen("sig-retry-1"))
            dedup_store.close()

    async def test_topic_missing_deactivates_only_requested_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "topic.db"
            a1 = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            a2 = "0xffffffffffffffffffffffffffffffffffffffff"
            with TraderStore(db_path) as store:
                store.add_manual(address=a1, label="A1")
                store.add_manual(address=a2, label="A2")
                store.create_subscription_with_session(
                    chat_id=888,
                    trader_address=a1,
                    message_thread_id=91,
                    topic_name="A1",
                    lifetime_hours=0,
                )
                store.create_subscription_with_session(
                    chat_id=888,
                    trader_address=a2,
                    message_thread_id=92,
                    topic_name="A2",
                    lifetime_hours=0,
                )

            settings = SimpleNamespace(database_dsn=str(db_path))
            logger = logging.getLogger("test.e2e.topic")
            error = TelegramClientError(
                method="sendMessage",
                status_code=400,
                error_code=400,
                description="Bad Request: message thread not found",
            )
            status = await _send_live_target(
                settings=settings,
                http_session=None,
                dispatcher=_AlwaysFailDispatcher(error),
                logger=logger,
                dedup_key="sig-topic-1",
                message_text="payload",
                chat_id=888,
                trader_address=a1,
                message_thread_id=91,
            )
            self.assertEqual(status, "dropped")
            with TraderStore(db_path) as store:
                sessions = store.list_delivery_sessions_for_chat(chat_id=888)
            self.assertEqual(len(sessions), 1)
            self.assertEqual(sessions[0].trader_address, a2)

    async def test_chat_blocked_deactivates_all_targets_for_chat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "chat-blocked.db"
            a1 = "0x1212121212121212121212121212121212121212"
            a2 = "0x1313131313131313131313131313131313131313"
            with TraderStore(db_path) as store:
                store.add_manual(address=a1, label="A1")
                store.add_manual(address=a2, label="A2")
                store.create_subscription_with_session(
                    chat_id=889,
                    trader_address=a1,
                    message_thread_id=51,
                    topic_name="A1",
                    lifetime_hours=0,
                )
                store.create_subscription_with_session(
                    chat_id=889,
                    trader_address=a2,
                    message_thread_id=52,
                    topic_name="A2",
                    lifetime_hours=0,
                )

            settings = SimpleNamespace(database_dsn=str(db_path))
            logger = logging.getLogger("test.e2e.chat")
            error = TelegramClientError(
                method="sendMessage",
                status_code=403,
                error_code=403,
                description="Forbidden: bot was blocked by the user",
            )
            status = await _send_live_target(
                settings=settings,
                http_session=None,
                dispatcher=_AlwaysFailDispatcher(error),
                logger=logger,
                dedup_key="sig-chat-1",
                message_text="payload",
                chat_id=889,
                trader_address=a1,
                message_thread_id=51,
            )
            self.assertEqual(status, "dropped")
            with TraderStore(db_path) as store:
                sessions = store.list_delivery_sessions_for_chat(chat_id=889)
            self.assertEqual(sessions, [])


if __name__ == "__main__":
    unittest.main()
