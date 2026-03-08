from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Awaitable, Callable, TypeVar

import aiohttp

from bot.config import ConfigError, load_settings
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.telegram_client import (
    TelegramClientError,
    create_forum_topic,
    delete_forum_topic,
    get_updates,
    send_message,
    set_telegram_http_logging,
)
from bot.trader_store import (
    MODERATION_BLACKLIST,
    PERMANENT_SUBSCRIPTION_EXPIRES_AT,
    STATUS_ARCHIVED,
    STATUS_STALE,
    TraderStore,
)

_START_LOCKS: dict[str, asyncio.Lock] = {}
_START_LOCKS_GUARD = asyncio.Lock()
_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_CHAT_LOCKS_GUARD = asyncio.Lock()
_T = TypeVar("_T")


def _short(address: str) -> str:
    if len(address) < 12:
        return address
    return f"{address[:6]}...{address[-4:]}"


def _normalize_command(raw_text: str) -> tuple[str, str]:
    text = raw_text.strip()
    if not text.startswith("/"):
        return "", ""

    parts = text.split(maxsplit=1)
    head = parts[0][1:]
    command = head.split("@", maxsplit=1)[0].strip().lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    return command, arg


def _parse_db_ts(value) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except Exception:
        return None


def _fmt_expiry(value: str) -> str:
    parsed = _parse_db_ts(value)
    if parsed is None:
        return value
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def _fmt_remaining(value: str) -> str:
    if str(value).strip() == PERMANENT_SUBSCRIPTION_EXPIRES_AT:
        return "until cancellation"
    parsed = _parse_db_ts(value)
    if parsed is None:
        return "-"
    now = datetime.now(tz=UTC)
    seconds = int((parsed - now).total_seconds())
    if seconds <= 0:
        return "expired"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m"


async def _get_start_lock(*, chat_id: int, trader_address: str) -> asyncio.Lock:
    key = f"{int(chat_id)}:{str(trader_address).strip().lower()}"
    async with _START_LOCKS_GUARD:
        lock = _START_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _START_LOCKS[key] = lock
        return lock


async def _get_chat_lock(*, chat_id: int) -> asyncio.Lock:
    async with _CHAT_LOCKS_GUARD:
        lock = _CHAT_LOCKS.get(int(chat_id))
        if lock is None:
            lock = asyncio.Lock()
            _CHAT_LOCKS[int(chat_id)] = lock
        return lock


def _store_call_sync(settings, fn):
    with TraderStore(settings.database_dsn) as store:
        return fn(store)


async def _store_call(settings, fn):
    return await asyncio.to_thread(_store_call_sync, settings, fn)


async def _telegram_call_with_retry(
    *,
    logger: logging.Logger,
    label: str,
    max_attempts: int,
    call: Callable[[], Awaitable[_T]],
) -> _T:
    attempts = max(1, int(max_attempts))
    backoff_seconds = 1.0
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await call()
        except TelegramClientError as exc:
            last_error = exc
            retryable = exc.is_flood_limit() or exc.is_transient()
            if not retryable or attempt >= attempts:
                raise
            retry_after = exc.retry_after if exc.retry_after and exc.retry_after > 0 else None
            delay_seconds = int(retry_after or backoff_seconds)
            delay_seconds = max(1, min(30, delay_seconds))
            logger.warning(
                "Telegram call retry label=%s attempt=%s/%s delay=%ss error=%s",
                label,
                attempt,
                attempts,
                delay_seconds,
                exc,
            )
            await asyncio.sleep(delay_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 30.0)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            last_error = exc
            if attempt >= attempts:
                raise
            delay_seconds = int(max(1.0, min(30.0, backoff_seconds)))
            logger.warning(
                "Network retry label=%s attempt=%s/%s delay=%ss error=%s",
                label,
                attempt,
                attempts,
                delay_seconds,
                exc,
            )
            await asyncio.sleep(delay_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 30.0)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unexpected retry loop termination for {label}")


def _retry_attempts(settings) -> int:
    return max(1, int(getattr(settings, "subscriber_telegram_retry_attempts", 5)))


async def _send_message_safe(
    *,
    session: aiohttp.ClientSession,
    settings,
    logger: logging.Logger,
    chat_id: int,
    text: str,
    message_thread_id: int | None = None,
) -> None:
    await _telegram_call_with_retry(
        logger=logger,
        label="sendMessage",
        max_attempts=_retry_attempts(settings),
        call=lambda: send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text=text,
            message_thread_id=message_thread_id,
        ),
    )


async def _send_welcome(
    *,
    session: aiohttp.ClientSession,
    settings,
    chat_id: int,
    logger: logging.Logger,
) -> None:
    await _send_message_safe(
        session=session,
        settings=settings,
        logger=logger,
        chat_id=chat_id,
        text=(
            "<b>CryptoInsider Bot</b>\n\n"
            "1) Open the traders catalog\n"
            "2) Click <b>Open Trader Chat</b>\n"
            "3) The bot starts your trader feed (thread when supported, otherwise this chat)\n\n"
            "Commands:\n"
            "<code>/my</code> - list active trader subscriptions\n"
            "<code>/stop 0x...</code> - stop trader subscription"
        ),
    )


async def _delete_topics_best_effort(
    *,
    session: aiohttp.ClientSession,
    settings,
    chat_id: int,
    sessions,
    logger: logging.Logger,
) -> int:
    failed = 0
    for item in sessions:
        if item.message_thread_id is None:
            continue
        try:
            await _telegram_call_with_retry(
                logger=logger,
                label="deleteForumTopic",
                max_attempts=_retry_attempts(settings),
                call=lambda: delete_forum_topic(
                    session,
                    bot_token=settings.telegram_bot_token,
                    chat_id=chat_id,
                    message_thread_id=item.message_thread_id,
                ),
            )
        except Exception as exc:
            failed += 1
            logger.warning(
                "Failed to delete forum topic chat_id=%s thread=%s: %s",
                chat_id,
                item.message_thread_id,
                exc,
            )
            await _store_call(
                settings,
                lambda store: store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
                ),
            )
    return failed


async def _handle_start_with_payload(
    *,
    session: aiohttp.ClientSession,
    settings,
    chat_id: int,
    payload: str,
    logger: logging.Logger,
) -> None:
    if not payload.startswith("sub_"):
        await _send_welcome(session=session, settings=settings, chat_id=chat_id, logger=logger)
        return

    address = payload.removeprefix("sub_").strip().lower()
    if not address:
        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text="Invalid subscription payload.",
        )
        return

    start_lock = await _get_start_lock(chat_id=chat_id, trader_address=address)
    async with start_lock:
        trader = await _store_call(settings, lambda store: store.get_trader(address=address))
        if trader is None:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text="Trader not found in database.",
            )
            return
        if trader.moderation_state == MODERATION_BLACKLIST:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text="Trader is not available for subscription.",
            )
            return
        if trader.status in {STATUS_STALE, STATUS_ARCHIVED}:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "Trader is currently not eligible for new subscriptions.\n"
                    "Pick another trader from the listed catalog."
                ),
            )
            return
        previous_sessions = await _store_call(
            settings,
            lambda store: [
                item
                for item in store.list_delivery_sessions_for_chat(chat_id=chat_id)
                if item.trader_address == trader.address
            ],
        )

        topic_name = f"{_short(address)} | Live"
        topic_result = None
        thread_id: int | None = None
        feed_mode = "chat"
        try:
            try:
                topic_result = await _telegram_call_with_retry(
                    logger=logger,
                    label="createForumTopic",
                    max_attempts=_retry_attempts(settings),
                    call=lambda: create_forum_topic(
                        session,
                        bot_token=settings.telegram_bot_token,
                        chat_id=chat_id,
                        name=topic_name,
                    ),
                )
                candidate_thread_id = int(topic_result.get("message_thread_id", 0))
                if candidate_thread_id <= 0:
                    raise RuntimeError(f"Invalid forum topic response: {topic_result}")
                thread_id = candidate_thread_id
                feed_mode = "thread"
            except TelegramClientError as exc:
                if exc.is_forum_not_supported():
                    logger.info(
                        "Forum topics unsupported for chat %s; falling back to direct chat mode",
                        chat_id,
                    )
                    thread_id = None
                    topic_name = None
                    feed_mode = "chat"
                else:
                    raise

            session_info = await _store_call(
                settings,
                lambda store: store.create_subscription_with_session(
                    chat_id=chat_id,
                    trader_address=address,
                    message_thread_id=thread_id,
                    topic_name=topic_name,
                    lifetime_hours=settings.subscription_lifetime_hours,
                ),
            )
            logger.info(
                "Subscription started trader=%s chat_id=%s feed_mode=%s thread=%s expires_at=%s",
                address,
                chat_id,
                feed_mode,
                thread_id,
                session_info.expires_at,
            )

            if thread_id is not None:
                await _send_message_safe(
                    session=session,
                    settings=settings,
                    logger=logger,
                    chat_id=chat_id,
                    message_thread_id=thread_id,
                    text=(
                        "<b>Session started ✅</b>\n"
                        f"Trader: <code>{_short(address)}</code>\n"
                        "<b>Status:</b> Active until cancellation\n\n"
                        "New trades from this trader will be posted in this thread."
                    ),
                )
            else:
                await _send_message_safe(
                    session=session,
                    settings=settings,
                    logger=logger,
                    chat_id=chat_id,
                    text=(
                        "<b>Session started ✅</b>\n"
                        f"Trader: <code>{_short(address)}</code>\n"
                        "<b>Status:</b> Active until cancellation\n\n"
                        "This chat does not support forum threads, so new trades will be posted directly here."
                    ),
                )

            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "<b>Done ✅</b>\n"
                    f"Subscription is active for <code>{_short(address)}</code>.\n"
                    "It stays active until you send <code>/stop</code>.\n\n"
                    "View active subscriptions: <code>/my</code>"
                ),
            )

            if previous_sessions:
                await _delete_topics_best_effort(
                    session=session,
                    settings=settings,
                    chat_id=chat_id,
                    sessions=previous_sessions,
                    logger=logger,
                )
        except Exception as exc:
            logger.exception("Failed to start subscription for chat %s: %s", chat_id, exc)
            if topic_result and topic_result.get("message_thread_id"):
                try:
                    await _telegram_call_with_retry(
                        logger=logger,
                        label="deleteForumTopic",
                        max_attempts=_retry_attempts(settings),
                        call=lambda: delete_forum_topic(
                            session,
                            bot_token=settings.telegram_bot_token,
                            chat_id=chat_id,
                            message_thread_id=int(topic_result["message_thread_id"]),
                        ),
                    )
                except Exception:
                    pass
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "Failed to start the subscription.\n"
                    "Please try again in 10-20 seconds."
                ),
            )


async def _handle_message(
    *,
    session: aiohttp.ClientSession,
    settings,
    message: dict,
    logger: logging.Logger,
) -> None:
    chat = message.get("chat")
    if not isinstance(chat, dict):
        return
    if chat.get("type") != "private":
        return

    chat_id = int(chat.get("id"))
    text = str(message.get("text", "") or "")
    command, arg = _normalize_command(text)

    if command in {"start", "help"}:
        await _handle_start_with_payload(
            session=session,
            settings=settings,
            chat_id=chat_id,
            payload=arg,
            logger=logger,
        )
        return

    if command == "my":
        sessions = await _store_call(
            settings,
            lambda store: store.list_delivery_sessions_for_chat(chat_id=chat_id),
        )

        if not sessions:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text="No active trader subscriptions yet. Pick a trader in the catalog.",
            )
            return

        lines = ["<b>Your active trader subscriptions</b>"]
        for item in sessions:
            topic = item.topic_name or "Direct chat feed"
            thread = (
                f"thread={item.message_thread_id}"
                if item.message_thread_id is not None
                else "mode=chat"
            )
            lines.append(
                f"• <code>{_short(item.trader_address)}</code> | {topic} | {thread} | active until cancellation"
            )
        lines.append("\nStop a trader: <code>/stop 0x...</code>")

        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text="\n".join(lines),
        )
        return

    if command == "stop":
        parts = arg.strip().split()
        address = parts[0].strip().lower() if parts else ""
        if not address:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text="Usage: <code>/stop 0x...</code>",
            )
            return

        sessions = await _store_call(
            settings,
            lambda store: store.cancel_chat_trader_subscriptions(
                chat_id=chat_id,
                trader_address=address,
            ),
        )

        if not sessions:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text="No active subscription found for this trader.",
            )
            return

        failed = await _delete_topics_best_effort(
            session=session,
            settings=settings,
            chat_id=chat_id,
            sessions=sessions,
            logger=logger,
        )

        suffix = "" if failed == 0 else f" (topic delete failed: {failed})"
        logger.info(
            "Subscription stopped trader=%s chat_id=%s sessions=%s cleanup_failed=%s",
            address,
            chat_id,
            len(sessions),
            failed,
        )
        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text=f"Subscription for <code>{_short(address)}</code> has been stopped{suffix}.",
        )
        return

    await _send_welcome(session=session, settings=settings, chat_id=chat_id, logger=logger)


async def run_bot() -> None:
    try:
        settings = load_settings()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
    setup_logging(
        service_name="cryptoinsider.subscriber-bot",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.subscriber-bot")
    set_telegram_http_logging(settings.log_telegram_http)
    logger.info(
        "Service started long_poll_timeout=%s update_concurrency=%s retry_attempts=%s",
        50,
        settings.subscriber_update_concurrency,
        settings.subscriber_telegram_retry_attempts,
    )

    long_poll_timeout = 50
    timeout = aiohttp.ClientTimeout(total=long_poll_timeout + 20)
    offset: int | None = None
    cycle = 0
    update_concurrency = max(1, int(settings.subscriber_update_concurrency))
    update_sem = asyncio.Semaphore(update_concurrency)
    max_inflight = max(update_concurrency * 4, 32)

    async def _process_update(update: dict) -> None:
        update_id = update.get("update_id")
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat")
        chat_id_raw = chat.get("id") if isinstance(chat, dict) else None
        chat_id = int(chat_id_raw) if isinstance(chat_id_raw, (int, float)) else None
        text = str(message.get("text", "") or "")
        command, _ = _normalize_command(text)
        lock: asyncio.Lock | None = None
        if chat_id is not None:
            lock = await _get_chat_lock(chat_id=chat_id)
        try:
            async with update_sem:
                if lock is not None:
                    async with lock:
                        with bind_log_context(
                            update_id=update_id,
                            update_ctx_id=new_trace_id("upd"),
                            chat_id=chat_id,
                            command=command or "-",
                        ):
                            logger.info("Processing update")
                            await _handle_message(
                                session=session,
                                settings=settings,
                                message=message,
                                logger=logger,
                            )
                else:
                    with bind_log_context(
                        update_id=update_id,
                        update_ctx_id=new_trace_id("upd"),
                        chat_id=chat_id,
                        command=command or "-",
                    ):
                        logger.info("Processing update")
                        await _handle_message(
                            session=session,
                            settings=settings,
                            message=message,
                            logger=logger,
                        )
        except Exception as exc:
            logger.exception("Failed to process update %s: %s", update_id, exc)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        inflight: set[asyncio.Task[None]] = set()
        while True:
            cycle += 1
            try:
                with bind_log_context(poll_cycle=cycle, poll_id=new_trace_id("poll")):
                    updates = await _telegram_call_with_retry(
                        logger=logger,
                        label="getUpdates",
                        max_attempts=_retry_attempts(settings),
                        call=lambda: get_updates(
                            session,
                            bot_token=settings.telegram_bot_token,
                            offset=offset,
                            timeout=long_poll_timeout,
                        ),
                    )
            except asyncio.TimeoutError:
                logger.warning("getUpdates timeout; retrying")
                continue
            except Exception as exc:
                logger.exception("Failed to fetch updates: %s", exc)
                await asyncio.sleep(3)
                continue

            if not updates:
                continue

            max_update_id: int | None = None
            for update in updates:
                update_id = update.get("update_id")
                if isinstance(update_id, int):
                    if max_update_id is None or update_id > max_update_id:
                        max_update_id = update_id
                task = asyncio.create_task(_process_update(update))
                inflight.add(task)
                if len(inflight) >= max_inflight:
                    done, pending = await asyncio.wait(
                        inflight,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    inflight = set(pending)
                    for completed in done:
                        try:
                            completed.result()
                        except Exception as exc:  # pragma: no cover - defensive
                            logger.exception("Unhandled update task error: %s", exc)

            if max_update_id is not None:
                offset = max_update_id + 1


if __name__ == "__main__":
    asyncio.run(run_bot())
