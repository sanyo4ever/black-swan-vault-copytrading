from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from bot.trader_store import (
    MODERATION_BLACKLIST,
    MODERATION_NEUTRAL,
    MODERATION_WHITELIST,
    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
    STATUS_ACTIVE,
    STATUS_ACTIVE_UNLISTED,
    STATUS_ARCHIVED,
    STATUS_STALE,
    TIER_HOT,
    TIER_WARM,
    TraderStore,
)


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

    def test_normalizes_legacy_paused_status_to_unlisted(self) -> None:
        address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="Legacy")
            store._connection.execute(
                """
                UPDATE tracked_traders
                SET status = 'PAUSED', manual_status_override = 1
                WHERE address = ?
                """,
                (address,),
            )
            store._connection.commit()

        with TraderStore(self.db_path) as store:
            trader = store.get_trader(address=address)
        self.assertIsNotNone(trader)
        assert trader is not None
        self.assertEqual(trader.status, STATUS_ACTIVE_UNLISTED)
        self.assertTrue(trader.manual_status_override)

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
            self.assertEqual(subs[0].status, "ACTIVE")

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
                topic_name="D | Live",
                lifetime_hours=24,
            )
            self.assertEqual(session.expires_at, PERMANENT_SUBSCRIPTION_EXPIRES_AT)

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
                    stats_json='{"metrics_30d":{"max_drawdown_pct":8.5}}',
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
            self.assertEqual(row[0], 1)
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
            self.assertEqual(due[0].attempt_count, 1)
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
            self.assertEqual(row[0], 2)
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

    def test_blacklist_moderation_detaches_trader_from_delivery(self) -> None:
        address = "0x3333333333333333333333333333333333333333"
        other = "0x4444444444444444444444444444444444444444"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="Blk")
            store.add_manual(address=other, label="Other")
            store.create_subscription_with_session(
                chat_id=555,
                trader_address=address,
                message_thread_id=77,
                topic_name="blk",
                lifetime_hours=24,
            )
            store.enqueue_delivery_retry(
                dedup_key="sig-blacklist",
                chat_id=555,
                trader_address=address,
                message_thread_id=77,
                message_text="retry",
                delay_seconds=60,
            )
            store.set_moderation(
                address=address,
                moderation_state=MODERATION_BLACKLIST,
                note="manual review",
            )

            trader = store.get_trader(address=address)
            self.assertIsNotNone(trader)
            assert trader is not None
            self.assertEqual(trader.moderation_state, MODERATION_BLACKLIST)
            self.assertEqual(trader.status, STATUS_ACTIVE)
            self.assertEqual(trader.moderation_note, "manual review")

            active_addresses = store.list_active_addresses(limit=10)
            self.assertNotIn(address, active_addresses)

            monitored = store.list_monitored_addresses(limit=10)
            self.assertNotIn(address, monitored)
            self.assertIn(other, monitored)

            pending = store._connection.execute(
                """
                SELECT COUNT(*)
                FROM delivery_retry_queue
                WHERE trader_address = ? AND status = 'PENDING'
                """,
                (address,),
            ).fetchone()
            self.assertIsNotNone(pending)
            assert pending is not None
            self.assertEqual(pending[0], 0)

    def test_bulk_status_and_moderation_actions(self) -> None:
        a1 = "0x5555555555555555555555555555555555555555"
        a2 = "0x6666666666666666666666666666666666666666"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=a1, label="A1")
            store.add_manual(address=a2, label="A2")

            changed = store.set_status_bulk(addresses=[a1, a2], status=STATUS_ACTIVE)
            self.assertEqual(changed, 2)
            t1 = store.get_trader(address=a1)
            t2 = store.get_trader(address=a2)
            self.assertIsNotNone(t1)
            self.assertIsNotNone(t2)
            assert t1 is not None
            assert t2 is not None
            self.assertEqual(t1.status, STATUS_ACTIVE)
            self.assertEqual(t2.status, STATUS_ACTIVE)

            changed = store.set_status_bulk(addresses=[a1], status="PAUSED")
            self.assertEqual(changed, 1)
            t1 = store.get_trader(address=a1)
            assert t1 is not None
            self.assertEqual(t1.status, STATUS_ACTIVE_UNLISTED)

            with self.assertRaises(ValueError):
                store.set_status_bulk(addresses=[a1], status="BROKEN")

            changed = store.set_moderation_bulk(
                addresses=[a1, a2],
                moderation_state=MODERATION_WHITELIST,
                note="trusted",
            )
            self.assertEqual(changed, 2)
            t1 = store.get_trader(address=a1)
            t2 = store.get_trader(address=a2)
            assert t1 is not None
            assert t2 is not None
            self.assertEqual(t1.moderation_state, MODERATION_WHITELIST)
            self.assertEqual(t2.moderation_state, MODERATION_WHITELIST)
            self.assertEqual(t1.moderation_note, "trusted")

            changed = store.set_moderation_bulk(
                addresses=[a1],
                moderation_state=MODERATION_NEUTRAL,
            )
            self.assertEqual(changed, 1)
            t1 = store.get_trader(address=a1)
            assert t1 is not None
            self.assertEqual(t1.moderation_state, MODERATION_NEUTRAL)

    def test_catalog_refresh_and_keyset_filters(self) -> None:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        addresses = [
            "0x7777777777777777777777777777777777777771",
            "0x7777777777777777777777777777777777777772",
            "0x7777777777777777777777777777777777777773",
        ]
        with TraderStore(self.db_path) as store:
            for idx, address in enumerate(addresses):
                store.upsert_discovered(
                    address=address,
                    label=f"C{idx + 1}",
                    source="hyperliquid_recent_trades",
                    trades_24h=5 + idx,
                    active_hours_24h=3 + idx,
                    trades_7d=10 + idx,
                    trades_30d=40 + (idx * 10),
                    active_days_30d=8 + idx,
                    first_fill_time=now_ms - (60 * 86_400_000),
                    last_fill_time=now_ms - (idx * 15 * 60_000),
                    age_days=60.0,
                    volume_usd_30d=120000.0 + (idx * 50000.0),
                    realized_pnl_30d=2000.0 + (idx * 1000.0),
                    fees_30d=200.0 + (idx * 50.0),
                    win_rate_30d=0.50 + (idx * 0.05),
                    long_ratio_30d=0.5,
                    avg_notional_30d=2500.0,
                    max_notional_30d=12000.0,
                    account_value=100000.0,
                    total_ntl_pos=20000.0,
                    total_margin_used=7000.0,
                    score=20.0 + idx,
                    stats_json="{}",
                )

            refreshed = store.refresh_catalog_current(activity_window_minutes=60)
            self.assertEqual(refreshed, 3)

            page1 = store.list_catalog_traders(limit=2, sort_by="activity_desc")
            self.assertEqual(len(page1), 2)
            self.assertTrue((page1[0].activity_score or 0.0) >= (page1[1].activity_score or 0.0))

            cursor_value = float(page1[-1].activity_score or -10**9)
            cursor_address = page1[-1].address
            page2 = store.list_catalog_traders(
                limit=2,
                sort_by="activity_desc",
                cursor_value=cursor_value,
                cursor_address=cursor_address,
            )
            self.assertGreaterEqual(len(page2), 1)
            self.assertNotEqual(page1[0].address, page2[0].address)

            filtered = store.list_catalog_traders(
                limit=10,
                q=addresses[0][:18],
                status=STATUS_ACTIVE,
            )
            self.assertGreaterEqual(len(filtered), 1)
            self.assertTrue(any(item.address == addresses[0] for item in filtered))

    def test_soft_delete_archives_without_active_subscription(self) -> None:
        address = "0x1212121212121212121212121212121212121212"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="ToArchive")
            store.delete(address=address)
            trader = store.get_trader(address=address)
            self.assertIsNotNone(trader)
            assert trader is not None
            self.assertEqual(trader.status, STATUS_ARCHIVED)

    def test_soft_delete_keeps_unlisted_with_active_subscription(self) -> None:
        address = "0x1313131313131313131313131313131313131313"
        with TraderStore(self.db_path) as store:
            store.add_manual(address=address, label="ToUnlist")
            store.create_subscription_with_session(
                chat_id=9191,
                trader_address=address,
                message_thread_id=91,
                topic_name="live",
                lifetime_hours=0,
            )
            store.delete(address=address)
            trader = store.get_trader(address=address)
            self.assertIsNotNone(trader)
            assert trader is not None
            self.assertEqual(trader.status, STATUS_ACTIVE_UNLISTED)

    def test_apply_trader_lifecycle_transitions(self) -> None:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        listed = "0x1414141414141414141414141414141414141414"
        unlisted = "0x1515151515151515151515151515151515151515"
        stale = "0x1616161616161616161616161616161616161616"
        archived = "0x1717171717171717171717171717171717171717"

        with TraderStore(self.db_path) as store:
            for address, age_minutes in (
                (listed, 10),
                (unlisted, 120),
                (stale, 3 * 24 * 60),
                (archived, 20 * 24 * 60),
            ):
                age_days = max(1.0, age_minutes / (24 * 60))
                store.upsert_discovered(
                    address=address,
                    label="L",
                    source="hyperliquid_recent_trades",
                    trades_24h=10,
                    active_hours_24h=5,
                    trades_7d=80,
                    trades_30d=300,
                    active_days_30d=20,
                    first_fill_time=now_ms - (40 * 86_400_000),
                    last_fill_time=now_ms - (age_minutes * 60_000),
                    age_days=age_days,
                    volume_usd_30d=10_000.0,
                    realized_pnl_30d=1_000.0,
                    fees_30d=100.0,
                    win_rate_30d=0.6,
                    long_ratio_30d=0.5,
                    avg_notional_30d=1_000.0,
                    max_notional_30d=5_000.0,
                    account_value=20_000.0,
                    total_ntl_pos=4_000.0,
                    total_margin_used=1_500.0,
                    score=30.0,
                    stats_json='{"metrics_30d":{"max_drawdown_pct":10.0}}',
                )

            stats = store.apply_trader_lifecycle(
                listed_within_minutes=60,
                stale_after_minutes=24 * 60,
                archive_after_days=7,
            )
            self.assertGreaterEqual(stats["changed"], 1)

            t_listed = store.get_trader(address=listed)
            t_unlisted = store.get_trader(address=unlisted)
            t_stale = store.get_trader(address=stale)
            t_archived = store.get_trader(address=archived)
            assert t_listed is not None
            assert t_unlisted is not None
            assert t_stale is not None
            assert t_archived is not None
            self.assertEqual(t_listed.status, STATUS_ACTIVE)
            self.assertEqual(t_unlisted.status, STATUS_ACTIVE_UNLISTED)
            self.assertEqual(t_stale.status, STATUS_STALE)
            self.assertEqual(t_archived.status, STATUS_ARCHIVED)

    def test_monitoring_pool_due_targets_and_poll_mark(self) -> None:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        addresses = [
            "0x8888888888888888888888888888888888888881",
            "0x8888888888888888888888888888888888888882",
            "0x8888888888888888888888888888888888888883",
        ]
        with TraderStore(self.db_path) as store:
            for idx, address in enumerate(addresses):
                store.upsert_discovered(
                    address=address,
                    label=f"M{idx + 1}",
                    source="hyperliquid_recent_trades",
                    trades_24h=20 - (idx * 5),
                    active_hours_24h=6,
                    trades_7d=120 - (idx * 30),
                    trades_30d=220 - (idx * 40),
                    active_days_30d=20 - idx,
                    first_fill_time=now_ms - (90 * 86_400_000),
                    last_fill_time=now_ms - (idx * 90 * 60_000),
                    age_days=90.0,
                    volume_usd_30d=400000.0,
                    realized_pnl_30d=5000.0,
                    fees_30d=400.0,
                    win_rate_30d=0.65,
                    long_ratio_30d=0.5,
                    avg_notional_30d=2000.0,
                    max_notional_30d=10000.0,
                    account_value=150000.0,
                    total_ntl_pos=35000.0,
                    total_margin_used=9000.0,
                    score=80.0 - (idx * 10),
                    stats_json='{"metrics_30d":{"max_drawdown_pct":10.0}}',
                )

            store.refresh_traders_universe_from_tracked(
                min_age_days=30,
                min_trades_30d=120,
                min_active_days_30d=10,
                min_win_rate_30d=0.52,
                max_drawdown_30d_pct=25.0,
                max_last_activity_minutes=720,
                min_realized_pnl_30d=0.0,
                min_score=0.0,
                max_size=100,
            )
            stats = store.refresh_monitoring_pool(
                hot_size=1,
                warm_size=1,
                hot_poll_seconds=60,
                warm_poll_seconds=600,
                cold_poll_seconds=3600,
                hot_recency_minutes=60,
                warm_recency_minutes=360,
            )
            self.assertEqual(stats["total"], 3)
            self.assertEqual(stats["hot"], 1)
            self.assertEqual(stats["warm"], 1)

            store.create_subscription_with_session(
                chat_id=123,
                trader_address=addresses[0],
                message_thread_id=10,
                topic_name="A",
                lifetime_hours=0,
            )
            store.create_subscription_with_session(
                chat_id=123,
                trader_address=addresses[1],
                message_thread_id=11,
                topic_name="B",
                lifetime_hours=0,
            )

            due = store.list_due_monitoring_targets(limit=10, only_subscribed=True)
            self.assertGreaterEqual(len(due), 2)
            due_addresses = {item.address for item in due}
            self.assertIn(addresses[0], due_addresses)
            self.assertIn(addresses[1], due_addresses)
            self.assertNotIn(addresses[2], due_addresses)
            due_tiers = {item.tier for item in due}
            self.assertTrue(TIER_HOT in due_tiers or TIER_WARM in due_tiers)

            first = [item for item in due if item.address == addresses[0]]
            self.assertTrue(first)
            store.mark_monitoring_targets_polled(targets=[first[0]])

            due_after = store.list_due_monitoring_targets(limit=10, only_subscribed=True)
            self.assertNotIn(addresses[0], {item.address for item in due_after})

    def test_delivery_monitor_state_refresh_and_polling(self) -> None:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        active = "0x9999999999999999999999999999999999999991"
        passive = "0x9999999999999999999999999999999999999992"
        with TraderStore(self.db_path) as store:
            store.upsert_discovered(
                address=active,
                label="Active",
                source="hyperliquid_recent_trades",
                trades_24h=25,
                active_hours_24h=8,
                trades_7d=140,
                trades_30d=260,
                active_days_30d=23,
                first_fill_time=now_ms - (120 * 86_400_000),
                last_fill_time=now_ms - (5 * 60_000),
                age_days=120.0,
                volume_usd_30d=550000.0,
                realized_pnl_30d=12000.0,
                fees_30d=900.0,
                win_rate_30d=0.66,
                long_ratio_30d=0.52,
                avg_notional_30d=2300.0,
                max_notional_30d=15000.0,
                account_value=240000.0,
                total_ntl_pos=45000.0,
                total_margin_used=12000.0,
                score=78.0,
                stats_json='{"metrics_30d":{"max_drawdown_pct":9.0}}',
            )
            store.upsert_discovered(
                address=passive,
                label="Passive",
                source="hyperliquid_recent_trades",
                trades_24h=4,
                active_hours_24h=2,
                trades_7d=30,
                trades_30d=130,
                active_days_30d=13,
                first_fill_time=now_ms - (90 * 86_400_000),
                last_fill_time=now_ms - (240 * 60_000),
                age_days=90.0,
                volume_usd_30d=130000.0,
                realized_pnl_30d=2500.0,
                fees_30d=350.0,
                win_rate_30d=0.55,
                long_ratio_30d=0.47,
                avg_notional_30d=1500.0,
                max_notional_30d=9000.0,
                account_value=90000.0,
                total_ntl_pos=18000.0,
                total_margin_used=5000.0,
                score=42.0,
                stats_json='{"metrics_30d":{"max_drawdown_pct":12.0}}',
            )

            store.create_subscription_with_session(
                chat_id=101,
                trader_address=active,
                message_thread_id=31,
                topic_name="A",
                lifetime_hours=0,
            )
            store.create_subscription_with_session(
                chat_id=102,
                trader_address=active,
                message_thread_id=32,
                topic_name="A2",
                lifetime_hours=0,
            )
            store.create_subscription_with_session(
                chat_id=103,
                trader_address=passive,
                message_thread_id=33,
                topic_name="P",
                lifetime_hours=0,
            )

            stats = store.refresh_delivery_monitor_state(
                base_poll_seconds=60,
                min_poll_seconds=20,
                max_poll_seconds=180,
                priority_recency_minutes=120,
                safety_lookback_seconds=90,
                bootstrap_lookback_minutes=180,
                max_targets_per_cycle=10,
            )
            self.assertEqual(stats["total"], 2)
            self.assertEqual(stats["over_capacity"], 0)

            due = store.list_due_delivery_monitor_targets(limit=10)
            self.assertEqual(len(due), 2)
            active_target = [item for item in due if item.address == active][0]
            passive_target = [item for item in due if item.address == passive][0]
            self.assertGreater(active_target.priority_score, passive_target.priority_score)
            self.assertGreaterEqual(active_target.subscriber_count, 2)

            store.mark_delivery_monitor_polled(
                address=active,
                next_poll_seconds=120,
                newest_fill_time=now_ms + 1000,
                had_new_fill=True,
                error=None,
            )
            due_after = store.list_due_delivery_monitor_targets(limit=10)
            self.assertNotIn(active, {item.address for item in due_after})

            row = store._connection.execute(
                """
                SELECT last_seen_fill_time, idle_cycles, consecutive_errors
                FROM delivery_monitor_state
                WHERE address = ?
                """,
                (active,),
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(int(row[0]), now_ms + 1000)
            self.assertEqual(int(row[1]), 0)
            self.assertEqual(int(row[2]), 0)


if __name__ == "__main__":
    unittest.main()
