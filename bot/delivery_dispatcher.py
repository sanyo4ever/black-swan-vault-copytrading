from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from bot.telegram_client import send_message
from bot.telegram_client import TelegramClientError


@dataclass(frozen=True)
class DeliveryDispatcherConfig:
    bot_token: str
    send_concurrency: int = 8
    chat_min_interval_ms: int = 250
    global_flood_default_retry_seconds: int = 30
    global_flood_max_retry_seconds: int = 300


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
        self._chat_last_used_at: dict[str, float] = {}
        self._registry_lock = asyncio.Lock()
        self._max_tracked_chats = 5000
        self._stale_chat_seconds = 3600.0
        self._global_next_allowed_at = 0.0
        self._global_lock = asyncio.Lock()
        self._global_flood_default_retry_seconds = max(
            1,
            int(config.global_flood_default_retry_seconds),
        )
        self._global_flood_max_retry_seconds = max(
            self._global_flood_default_retry_seconds,
            int(config.global_flood_max_retry_seconds),
        )

    async def _get_chat_lock(self, *, chat_key: str) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._chat_locks.get(chat_key)
            if lock is None:
                lock = asyncio.Lock()
                self._chat_locks[chat_key] = lock
            self._chat_last_used_at[chat_key] = time.monotonic()
            if len(self._chat_locks) > self._max_tracked_chats:
                self._gc_chat_state()
            return lock

    def _gc_chat_state(self) -> None:
        now = time.monotonic()
        stale_keys = [
            key
            for key, used_at in self._chat_last_used_at.items()
            if now - used_at >= self._stale_chat_seconds
        ]
        if not stale_keys:
            return
        for key in stale_keys:
            lock = self._chat_locks.get(key)
            if lock is not None and lock.locked():
                continue
            self._chat_locks.pop(key, None)
            self._chat_next_allowed_at.pop(key, None)
            self._chat_last_used_at.pop(key, None)

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
        self._chat_last_used_at[chat_key] = time.monotonic()
        self._chat_next_allowed_at[chat_key] = (
            time.monotonic() + self._chat_min_interval_seconds
        )

    async def _wait_for_global_window(self) -> None:
        while True:
            now = time.monotonic()
            sleep_for = self._global_next_allowed_at - now
            if sleep_for <= 0:
                return
            await asyncio.sleep(sleep_for)

    async def _mark_global_flood(self, *, retry_after: int | None) -> int:
        if retry_after is None or retry_after <= 0:
            retry_after = self._global_flood_default_retry_seconds
        delay_seconds = max(1, min(self._global_flood_max_retry_seconds, int(retry_after)))
        async with self._global_lock:
            now = time.monotonic()
            next_allowed = now + delay_seconds
            if next_allowed > self._global_next_allowed_at:
                self._global_next_allowed_at = next_allowed
                self._logger.warning(
                    "Global Telegram flood backoff applied delay_seconds=%s",
                    delay_seconds,
                )
        return delay_seconds

    async def apply_global_backoff(self, *, retry_after: int | None) -> int:
        return await self._mark_global_flood(retry_after=retry_after)

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
        await self._wait_for_global_window()
        async with self._global_sem:
            async with chat_lock:
                await self._wait_for_global_window()
                await self._wait_for_chat_window(chat_key=chat_key)
                try:
                    await send_message(
                        session,
                        bot_token=self._config.bot_token,
                        chat_id=chat_id,
                        text=text,
                        message_thread_id=message_thread_id,
                    )
                except TelegramClientError as exc:
                    if exc.is_flood_limit():
                        await self._mark_global_flood(retry_after=exc.retry_after)
                    raise
                self._mark_chat_sent(chat_key=chat_key)
