from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from bot.trader_store import STATUS_ACTIVE, TraderStore


class TraderStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "test.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_add_and_get_trader(self) -> None:
        with TraderStore(self.db_path) as store:
            store.add_manual(
                address="0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                label="Manual Trader",
            )
            trader = store.get_trader(address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

        self.assertIsNotNone(trader)
        assert trader is not None
        self.assertEqual(trader.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(trader.label, "Manual Trader")
        self.assertEqual(trader.status, STATUS_ACTIVE)

    def test_record_subscription_request(self) -> None:
        with TraderStore(self.db_path) as store:
            store.add_manual(address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", label="B")
            store.record_subscription_request(
                trader_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                client_ip="10.0.0.1",
                user_agent="pytest-agent",
            )

        connection = sqlite3.connect(self.db_path)
        row = connection.execute(
            """
            SELECT trader_address, client_ip, user_agent
            FROM subscription_requests
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        connection.close()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row[0], "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        self.assertEqual(row[1], "10.0.0.1")
        self.assertEqual(row[2], "pytest-agent")

    def test_telegram_subscriptions_lifecycle(self) -> None:
        with TraderStore(self.db_path) as store:
            store.add_manual(address="0xcccccccccccccccccccccccccccccccccccccccc", label="C")
            store.subscribe_chat_to_trader(
                chat_id=123456,
                trader_address="0xcccccccccccccccccccccccccccccccccccccccc",
            )

            subs = store.list_subscriptions_for_chat(chat_id=123456)
            self.assertEqual(len(subs), 1)
            self.assertEqual(subs[0].trader_address, "0xcccccccccccccccccccccccccccccccccccccccc")
            self.assertEqual(subs[0].status, STATUS_ACTIVE)

            mapping = store.list_active_subscriber_chat_ids_by_trader()
            self.assertEqual(mapping["0xcccccccccccccccccccccccccccccccccccccccc"], ["123456"])

            removed = store.unsubscribe_chat_from_trader(
                chat_id=123456,
                trader_address="0xcccccccccccccccccccccccccccccccccccccccc",
            )
            self.assertEqual(removed, 1)
            self.assertEqual(store.list_subscriptions_for_chat(chat_id=123456), [])

    def test_topic_delivery_session_lifecycle(self) -> None:
        address = "0xdddddddddddddddddddddddddddddddddddddddd"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="D")
            session = store.create_subscription_with_session(
                chat_id=777,
                trader_address=address,
                message_thread_id=42,
                topic_name="D | 24h",
                lifetime_hours=24,
            )

            sessions = store.list_delivery_sessions_for_chat(chat_id=777)
            self.assertEqual(len(sessions), 1)
            self.assertEqual(sessions[0].message_thread_id, 42)
            self.assertEqual(sessions[0].subscription_id, session.subscription_id)

            targets = store.list_active_delivery_targets_by_trader()
            self.assertIn(address, targets)
            self.assertEqual(targets[address][0].message_thread_id, 42)

            store._connection.execute(
                "UPDATE subscriptions SET expires_at = '2000-01-01 00:00:00' WHERE id = ?",
                (session.subscription_id,),
            )
            store._connection.commit()

            expired = store.expire_due_delivery_sessions()
            self.assertEqual(len(expired), 1)
            self.assertEqual(expired[0].message_thread_id, 42)

    def test_universe_and_top100_refresh(self) -> None:
        address = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)

        with TraderStore(self.db_path) as store:
            store.upsert_discovered(
                address=address,
                label="E",
                source="hyperliquid_recent_trades",
                trades_24h=12,
                active_hours_24h=9,
                trades_7d=20,
                trades_30d=80,
                active_days_30d=18,
                first_fill_time=now_ms - (45 * 86_400_000),
                last_fill_time=now_ms - (20 * 60_000),
                age_days=45.0,
                volume_usd_30d=250000.0,
                realized_pnl_30d=15000.0,
                fees_30d=800.0,
                win_rate_30d=0.62,
                long_ratio_30d=0.55,
                avg_notional_30d=3125.0,
                max_notional_30d=18000.0,
                account_value=200000.0,
                total_ntl_pos=50000.0,
                total_margin_used=12000.0,
                score=34.5,
                stats_json="{}",
            )

            universe_count = store.refresh_traders_universe_from_tracked(
                min_age_days=30,
                min_trades_30d=20,
                min_win_rate_30d=0.5,
                min_realized_pnl_30d=1000.0,
                min_score=10.0,
                max_size=100,
            )
            self.assertEqual(universe_count, 1)

            top_count = store.refresh_top100_live(max_rows=100, active_window_minutes=60)
            self.assertEqual(top_count, 1)
            live = store.list_top100_live_traders(limit=5)
            self.assertEqual(len(live), 1)
            self.assertEqual(live[0].address, address)

    def test_delivery_retry_queue_lifecycle_and_dedup(self) -> None:
        address = "0xffffffffffffffffffffffffffffffffffffffff"

        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="Retry")
            store.enqueue_delivery_retry(
                dedup_key="sig-1",
                chat_id=777,
                trader_address=address,
                message_thread_id=42,
                message_text="first",
                delay_seconds=60,
                error="first error",
            )
            store.enqueue_delivery_retry(
                dedup_key="sig-1",
                chat_id=777,
                trader_address=address,
                message_thread_id=42,
                message_text="second",
                delay_seconds=60,
                error="second error",
            )

            row = store._connection.execute(
                """
                SELECT attempt_count, message_text, status
                FROM delivery_retry_queue
                WHERE dedup_key = 'sig-1' AND chat_id = '777' AND message_thread_id = 42
                """
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row[0], 2)
            self.assertEqual(row[1], "second")
            self.assertEqual(row[2], "PENDING")

            store._connection.execute(
                """
                UPDATE delivery_retry_queue
                SET next_attempt_at = '2000-01-01 00:00:00'
                WHERE dedup_key = 'sig-1'
                """
            )
            store._connection.commit()

            due = store.list_due_delivery_retries(limit=10)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0].message_thread_id, 42)
            self.assertEqual(due[0].attempt_count, 2)
            self.assertEqual(due[0].message_text, "second")

            store.reschedule_delivery_retry(
                retry_id=due[0].id,
                delay_seconds=120,
                error="still failing",
            )
            row = store._connection.execute(
                "SELECT attempt_count, status, last_error FROM delivery_retry_queue WHERE id = ?",
                (due[0].id,),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row[0], 3)
            self.assertEqual(row[1], "PENDING")
            self.assertEqual(row[2], "still failing")

            store.mark_delivery_retry_sent(retry_id=due[0].id)
            row = store._connection.execute(
                "SELECT status FROM delivery_retry_queue WHERE id = ?",
                (due[0].id,),
            ).fetchone()
            self.assertEqual(row[0], "SENT")

    def test_cancel_all_chat_subscriptions_and_retry_cleanup(self) -> None:
        address1 = "0x1111111111111111111111111111111111111111"
        address2 = "0x2222222222222222222222222222222222222222"

        with TraderStore(self.db_path) as store:
            store.add_manual(address=address1, label="A1")
            store.add_manual(address=address2, label="A2")

            store.create_subscription_with_session(
                chat_id=12345,
                trader_address=address1,
                message_thread_id=11,
                topic_name="A1",
                lifetime_hours=24,
            )
            store.create_subscription_with_session(
                chat_id=12345,
                trader_address=address2,
                message_thread_id=22,
                topic_name="A2",
                lifetime_hours=24,
            )

            store.enqueue_delivery_retry(
                dedup_key="sig-a",
                chat_id=12345,
                trader_address=address1,
                message_thread_id=11,
                message_text="payload-a",
                delay_seconds=60,
            )
            store.enqueue_delivery_retry(
                dedup_key="sig-b",
                chat_id=12345,
                trader_address=address2,
                message_thread_id=22,
                message_text="payload-b",
                delay_seconds=60,
            )
            store.enqueue_delivery_retry(
                dedup_key="sig-c",
                chat_id=99999,
                trader_address=address1,
                message_thread_id=11,
                message_text="payload-c",
                delay_seconds=60,
            )

            cancelled = store.cancel_all_chat_subscriptions(chat_id=12345)
            self.assertEqual(len(cancelled), 2)

            deleted_target = store.delete_pending_retries_for_target(
                chat_id=12345,
                trader_address=address1,
                message_thread_id=11,
            )
            self.assertEqual(deleted_target, 1)

            deleted_chat = store.delete_pending_retries_for_chat(chat_id=12345)
            self.assertEqual(deleted_chat, 1)

            remaining = store._connection.execute(
                """
                SELECT COUNT(*)
                FROM delivery_retry_queue
                WHERE status = 'PENDING'
                """
            ).fetchone()
            self.assertIsNotNone(remaining)
            assert remaining is not None
            self.assertEqual(remaining[0], 1)


if __name__ == "__main__":
    unittest.main()
