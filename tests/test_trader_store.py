from __future__ import annotations

import sqlite3
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
