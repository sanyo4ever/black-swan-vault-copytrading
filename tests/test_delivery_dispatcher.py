from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

from bot.delivery_dispatcher import DeliveryDispatcher, DeliveryDispatcherConfig


class DeliveryDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_chat_is_serialized(self) -> None:
        dispatcher = DeliveryDispatcher(
            config=DeliveryDispatcherConfig(
                bot_token="123:abc",
                send_concurrency=8,
                chat_min_interval_ms=0,
            )
        )

        async def _fake_send(*_args, **_kwargs) -> None:
            await asyncio.sleep(0.05)

        with patch("bot.delivery_dispatcher.send_message", side_effect=_fake_send):
            started = time.perf_counter()
            await asyncio.gather(
                dispatcher.send(None, chat_id=777, text="a"),
                dispatcher.send(None, chat_id=777, text="b"),
            )
            elapsed = time.perf_counter() - started

        self.assertGreaterEqual(elapsed, 0.095)

    async def test_different_chats_can_send_in_parallel(self) -> None:
        dispatcher = DeliveryDispatcher(
            config=DeliveryDispatcherConfig(
                bot_token="123:abc",
                send_concurrency=8,
                chat_min_interval_ms=0,
            )
        )

        async def _fake_send(*_args, **_kwargs) -> None:
            await asyncio.sleep(0.05)

        with patch("bot.delivery_dispatcher.send_message", side_effect=_fake_send):
            started = time.perf_counter()
            await asyncio.gather(
                dispatcher.send(None, chat_id=1001, text="a"),
                dispatcher.send(None, chat_id=1002, text="b"),
            )
            elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 0.14)

    async def test_chat_min_interval_is_enforced(self) -> None:
        dispatcher = DeliveryDispatcher(
            config=DeliveryDispatcherConfig(
                bot_token="123:abc",
                send_concurrency=8,
                chat_min_interval_ms=120,
            )
        )
        call_times: list[float] = []

        async def _fake_send(*_args, **_kwargs) -> None:
            call_times.append(time.perf_counter())

        with patch("bot.delivery_dispatcher.send_message", side_effect=_fake_send):
            await asyncio.gather(
                dispatcher.send(None, chat_id=222, text="a"),
                dispatcher.send(None, chat_id=222, text="b"),
                dispatcher.send(None, chat_id=222, text="c"),
            )

        self.assertEqual(len(call_times), 3)
        deltas = [call_times[idx] - call_times[idx - 1] for idx in range(1, len(call_times))]
        self.assertTrue(all(delta >= 0.10 for delta in deltas))


if __name__ == "__main__":
    unittest.main()
