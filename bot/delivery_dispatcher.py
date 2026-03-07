from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from bot.telegram_client import send_message


@dataclass(frozen=True)
class DeliveryDispatcherConfig:
    bot_token: str
    send_concurrency: int = 8
    chat_min_interval_ms: int = 250


class DeliveryDispatcher:
    """Telegram sender with global concurrency and per-chat pacing."""

    def __init__(
        self,
        *,
        config: DeliveryDispatcherConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or logging.getLogger("cryptoinsider.delivery-dispatcher")
        self._global_sem = asyncio.Semaphore(max(1, int(config.send_concurrency)))
        self._chat_min_interval_seconds = max(0.0, int(config.chat_min_interval_ms) / 1000.0)
        self._chat_locks: dict[str, asyncio.Lock] = {}
        self._chat_next_allowed_at: dict[str, float] = {}
        self._registry_lock = asyncio.Lock()

    async def _get_chat_lock(self, *, chat_key: str) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._chat_locks.get(chat_key)
            if lock is None:
                lock = asyncio.Lock()
                self._chat_locks[chat_key] = lock
            return lock

    async def _wait_for_chat_window(self, *, chat_key: str) -> None:
        if self._chat_min_interval_seconds <= 0:
            return
        now = time.monotonic()
        next_allowed = self._chat_next_allowed_at.get(chat_key, 0.0)
        sleep_for = next_allowed - now
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    def _mark_chat_sent(self, *, chat_key: str) -> None:
        if self._chat_min_interval_seconds <= 0:
            return
        self._chat_next_allowed_at[chat_key] = (
            time.monotonic() + self._chat_min_interval_seconds
        )

    async def send(
        self,
        session,
        *,
        chat_id: str | int,
        text: str,
        message_thread_id: int | None = None,
    ) -> None:
        chat_key = str(chat_id)
        chat_lock = await self._get_chat_lock(chat_key=chat_key)
        async with self._global_sem:
            async with chat_lock:
                await self._wait_for_chat_window(chat_key=chat_key)
                await send_message(
                    session,
                    bot_token=self._config.bot_token,
                    chat_id=chat_id,
                    text=text,
                    message_thread_id=message_thread_id,
                )
                self._mark_chat_sent(chat_key=chat_key)
