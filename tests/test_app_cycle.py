from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot.app import _process_retry_queue, _run_cycle
from bot.dedup import DedupStore
from bot.models import TradeSignal
from bot.telegram_client import TelegramClientError
from bot.trader_store import TraderStore


class _SingleSignalSource:
    id = "hl_futures_feed"

    async def fetch_signals(self) -> list[TradeSignal]:
        return [
            TradeSignal(
                source_id="hl_futures_feed",
                source_name="Hyperliquid Futures",
                external_id="same-signal-1",
                trader_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                symbol="DOGE-PERP",
                side="BUY",
                entry="0.1",
                note="test signal",
            )
        ]


class _FailingDispatcher:
    def __init__(self) -> None:
        self.calls = 0
        self.backoffs: list[int | None] = []

    async def send(self, *_args, **_kwargs) -> None:
        self.calls += 1
        raise TelegramClientError(
            method="sendMessage",
            status_code=429,
            error_code=429,
            description="Too Many Requests: retry after 1",
        )

    async def apply_global_backoff(self, *, retry_after: int | None) -> int:
        self.backoffs.append(retry_after)
        return int(retry_after or 1)


class _RecordingDispatcher:
    def __init__(self) -> None:
        self.calls = 0
        self.backoffs: list[int | None] = []

    async def send(self, *_args, **_kwargs) -> None:
        self.calls += 1

    async def apply_global_backoff(self, *, retry_after: int | None) -> int:
        self.backoffs.append(retry_after)
        return int(retry_after or 1)


class AppCycleDedupTests(unittest.IsolatedAsyncioTestCase):
    async def test_flood_backoff_queues_remaining_targets_without_sending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cycle-flood.db"
            dedup_path = Path(tmpdir) / "dedup-flood.db"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            with TraderStore(db_path) as store:
                store.add_manual(address=address, label="A")
                store.create_subscription_with_session(
                    chat_id=5001,
                    trader_address=address,
                    message_thread_id=11,
                    topic_name="A1",
                    lifetime_hours=0,
                )
                store.create_subscription_with_session(
                    chat_id=5002,
                    trader_address=address,
                    message_thread_id=12,
                    topic_name="A2",
                    lifetime_hours=0,
                )

            settings = SimpleNamespace(
                sources_config_path=Path("config/sources.yaml"),
                database_dsn=str(db_path),
                max_signals_per_cycle=20,
                telegram_channel_id="",
                monitor_delivery_only_subscribed=True,
            )
            dedup_store = DedupStore(dedup_path)
            dispatcher = _FailingDispatcher()
            logger = logging.getLogger("test.app.flood-batch")
            try:
                with (
                    patch("bot.app.load_sources_config", return_value=[{"id": "source"}]),
                    patch("bot.app.build_source", return_value=_SingleSignalSource()),
                ):
                    await _run_cycle(
                        settings=settings,
                        http_session=None,
                        dedup_store=dedup_store,
                        dispatcher=dispatcher,
                        logger=logger,
                    )
                self.assertEqual(dispatcher.calls, 1)
                with TraderStore(db_path) as store:
                    row = store._connection.execute(
                        "SELECT COUNT(*) AS c FROM delivery_retry_queue WHERE status = 'PENDING'"
                    ).fetchone()
                count = int(row["c"] if isinstance(row, dict) else row[0])
                self.assertEqual(count, 2)
            finally:
                dedup_store.close()

    async def test_queue_only_delivery_marks_dedup_and_prevents_republish(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cycle.db"
            dedup_path = Path(tmpdir) / "dedup.db"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            with TraderStore(db_path) as store:
                store.add_manual(address=address, label="A")
                store.create_subscription_with_session(
                    chat_id=5001,
                    trader_address=address,
                    message_thread_id=11,
                    topic_name="A",
                    lifetime_hours=0,
                )

            settings = SimpleNamespace(
                sources_config_path=Path("config/sources.yaml"),
                database_dsn=str(db_path),
                max_signals_per_cycle=20,
                telegram_channel_id="",
                monitor_delivery_only_subscribed=True,
            )
            logger = logging.getLogger("test.app.cycle")
            dedup_store = DedupStore(dedup_path)
            failing_dispatcher = _FailingDispatcher()

            try:
                with (
                    patch("bot.app.load_sources_config", return_value=[{"id": "source"}]),
                    patch("bot.app.build_source", return_value=_SingleSignalSource()),
                ):
                    published_1 = await _run_cycle(
                        settings=settings,
                        http_session=None,
                        dedup_store=dedup_store,
                        dispatcher=failing_dispatcher,
                        logger=logger,
                    )

                self.assertEqual(published_1, 0)
                self.assertEqual(failing_dispatcher.calls, 1)
                self.assertTrue(dedup_store.seen("hl_futures_feed:same-signal-1"))

                recording_dispatcher = _RecordingDispatcher()
                with (
                    patch("bot.app.load_sources_config", return_value=[{"id": "source"}]),
                    patch("bot.app.build_source", return_value=_SingleSignalSource()),
                ):
                    published_2 = await _run_cycle(
                        settings=settings,
                        http_session=None,
                        dedup_store=dedup_store,
                        dispatcher=recording_dispatcher,
                        logger=logger,
                    )

                self.assertEqual(published_2, 0)
                self.assertEqual(recording_dispatcher.calls, 0)
            finally:
                dedup_store.close()

    async def test_retry_queue_skips_jobs_with_known_dedup_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "retry.db"
            dedup_path = Path(tmpdir) / "dedup.db"
            dedup_store = DedupStore(dedup_path)
            settings = SimpleNamespace(database_dsn=str(db_path))
            logger = logging.getLogger("test.app.retry-skip")
            dispatcher = _RecordingDispatcher()

            try:
                with TraderStore(db_path) as store:
                    store.enqueue_delivery_retry(
                        dedup_key="sig-1",
                        chat_id=123,
                        trader_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        message_thread_id=7,
                        message_text="payload",
                        delay_seconds=1,
                        error="test",
                    )
                    store._connection.execute(
                        "UPDATE delivery_retry_queue SET next_attempt_at = '2000-01-01 00:00:00'"
                    )
                    store._connection.commit()

                dedup_store.remember("sig-1")
                sent = await _process_retry_queue(
                    settings=settings,
                    http_session=None,
                    dedup_store=dedup_store,
                    dispatcher=dispatcher,
                    logger=logger,
                )

                self.assertEqual(sent, 0)
                self.assertEqual(dispatcher.calls, 0)

                with TraderStore(db_path) as store:
                    jobs = store.list_due_delivery_retries(limit=10)
                self.assertEqual(jobs, [])
            finally:
                dedup_store.close()


if __name__ == "__main__":
    unittest.main()
