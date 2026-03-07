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


if __name__ == "__main__":
    unittest.main()
