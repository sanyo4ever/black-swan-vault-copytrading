from __future__ import annotations

import asyncio
import html
import logging
import time
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, TypeVar

import aiohttp

from bot.config import ConfigError, load_settings
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.telegram_client import (
    TelegramClientError,
    create_forum_topic,
    get_chat_member,
    get_me,
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
_START_LOCKS_LAST_USED: dict[str, float] = {}
_CHAT_LOCKS_LAST_USED: dict[int, float] = {}
_LOCK_MAX_TRACKED = 5000
_LOCK_STALE_SECONDS = 3600.0
_T = TypeVar("_T")


def _short(address: str) -> str:
    safe = html.escape(str(address), quote=False)
    if len(safe) < 12:
        return safe
    return f"{safe[:6]}...{safe[-4:]}"


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
        now = time.monotonic()
        lock = _START_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _START_LOCKS[key] = lock
        _START_LOCKS_LAST_USED[key] = now
        _gc_lock_registry(
            _START_LOCKS,
            _START_LOCKS_LAST_USED,
            max_tracked=_LOCK_MAX_TRACKED,
            stale_seconds=_LOCK_STALE_SECONDS,
            now=now,
        )
        return lock


async def _get_chat_lock(*, chat_id: int) -> asyncio.Lock:
    async with _CHAT_LOCKS_GUARD:
        key = int(chat_id)
        now = time.monotonic()
        lock = _CHAT_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _CHAT_LOCKS[key] = lock
        _CHAT_LOCKS_LAST_USED[key] = now
        _gc_lock_registry(
            _CHAT_LOCKS,
            _CHAT_LOCKS_LAST_USED,
            max_tracked=_LOCK_MAX_TRACKED,
            stale_seconds=_LOCK_STALE_SECONDS,
            now=now,
        )
        return lock


def _gc_lock_registry(
    lock_map: dict[Any, asyncio.Lock],
    last_used_map: dict[Any, float],
    *,
    max_tracked: int,
    stale_seconds: float,
    now: float | None = None,
) -> int:
    if len(lock_map) <= max(1, int(max_tracked)):
        return 0
    current = float(now) if now is not None else time.monotonic()
    stale_after = max(60.0, float(stale_seconds))
    removed = 0
    stale_keys = [
        key
        for key, used_at in list(last_used_map.items())
        if current - float(used_at) >= stale_after
    ]
    for key in stale_keys:
        lock = lock_map.get(key)
        if lock is not None and lock.locked():
            continue
        if key in lock_map:
            lock_map.pop(key, None)
            last_used_map.pop(key, None)
            removed += 1
    return removed


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
    join_url = str(getattr(settings, "telegram_join_url", "") or "").strip()
    join_line = (
        f"Join channel: <a href=\"{html.escape(join_url, quote=True)}\">Open Channel</a>\n"
        if join_url
        else "Join link is not configured yet. Ask admin for channel invite.\n"
    )
    await _send_message_safe(
        session=session,
        settings=settings,
        logger=logger,
        chat_id=chat_id,
        text=(
            "<b>Black Swan Vault Bot</b>\n\n"
            "Signals are posted in shared forum topics inside the main Telegram group.\n"
            f"{join_line}\n"
            "Deep-link format:\n"
            "<code>/start sub_0x...</code> to open a trader topic.\n\n"
            "Commands:\n"
            "<code>/my</code> - show your access and usage tips\n"
            "<code>/stop</code> - deprecated (no personal subscriptions in shared mode)"
        ),
    )


def _forum_chat_id(settings) -> str:
    return str(getattr(settings, "telegram_forum_chat_id", "") or "").strip()


def _normalize_address(value: str) -> str:
    return str(value or "").strip().lower()


def _forum_topic_name(address: str) -> str:
    normalized = _normalize_address(address)
    if not normalized:
        return "Trader"
    if len(normalized) <= 18:
        return normalized
    return f"{normalized[:10]}...{normalized[-6:]}"


def _build_forum_topic_link(*, forum_chat_id: str, message_thread_id: int) -> str | None:
    chat = str(forum_chat_id or "").strip()
    if not chat:
        return None
    thread_id = int(message_thread_id or 0)
    if thread_id <= 0:
        return None
    if chat.startswith("-100") and chat[4:].isdigit():
        return f"https://t.me/c/{chat[4:]}/{thread_id}"
    if chat.startswith("@"):
        username = chat.removeprefix("@").strip()
        if username:
            return f"https://t.me/{username}/{thread_id}"
    return None


async def _ensure_shared_forum_topic(
    *,
    session: aiohttp.ClientSession,
    settings,
    trader_address: str,
    logger: logging.Logger,
) -> tuple[int | None, str | None]:
    forum_chat_id = _forum_chat_id(settings)
    if not forum_chat_id:
        return None, None

    normalized = _normalize_address(trader_address)
    existing = await _store_call(
        settings,
        lambda store: store.get_trader_forum_topic(
            trader_address=normalized,
            forum_chat_id=forum_chat_id,
        ),
    )
    if existing is not None and int(existing.message_thread_id or 0) > 0:
        return int(existing.message_thread_id), existing.topic_name

    topic_name = _forum_topic_name(normalized)
    topic_result = await _telegram_call_with_retry(
        logger=logger,
        label="createForumTopic",
        max_attempts=_retry_attempts(settings),
        call=lambda: create_forum_topic(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=forum_chat_id,
            name=topic_name,
        ),
    )
    thread_id = int(topic_result.get("message_thread_id", 0))
    if thread_id <= 0:
        raise RuntimeError(f"Invalid forum topic response: {topic_result}")
    await _store_call(
        settings,
        lambda store: store.upsert_trader_forum_topic(
            trader_address=normalized,
            forum_chat_id=forum_chat_id,
            message_thread_id=thread_id,
            topic_name=topic_name,
        ),
    )
    logger.info(
        "Created shared forum topic forum_chat_id=%s trader=%s thread=%s",
        forum_chat_id,
        normalized,
        thread_id,
    )
    return thread_id, topic_name


async def _send_trader_topic_link(
    *,
    session: aiohttp.ClientSession,
    settings,
    chat_id: int,
    trader_address: str,
    thread_id: int,
    logger: logging.Logger,
) -> None:
    forum_chat_id = _forum_chat_id(settings)
    join_url = str(getattr(settings, "telegram_join_url", "") or "").strip()
    topic_url = _build_forum_topic_link(
        forum_chat_id=forum_chat_id,
        message_thread_id=thread_id,
    )
    lines = [
        "<b>Trader topic is ready ✅</b>",
        f"Trader: <code>{_short(trader_address)}</code>",
    ]
    if join_url:
        lines.append(
            f"1) Join the group: <a href=\"{html.escape(join_url, quote=True)}\">Open Group</a>"
        )
    else:
        lines.append("1) Join link is missing. Ask admin for group invite.")
    if topic_url:
        lines.append(
            f"2) Open topic: <a href=\"{html.escape(topic_url, quote=True)}\">Trader Thread</a>"
        )
    else:
        lines.append(
            "2) Open the group and select the topic by wallet label."
        )
    lines.append("This is a shared channel mode. No personal subscription is created.")
    await _send_message_safe(
        session=session,
        settings=settings,
        logger=logger,
        chat_id=chat_id,
        text="\n".join(lines),
    )


async def _validate_forum_permissions(
    *,
    session: aiohttp.ClientSession,
    settings,
    logger: logging.Logger,
) -> None:
    forum_chat_id = _forum_chat_id(settings)
    if not forum_chat_id:
        logger.error("TELEGRAM_FORUM_CHAT_ID is not configured")
        return
    try:
        me = await _telegram_call_with_retry(
            logger=logger,
            label="getMe",
            max_attempts=_retry_attempts(settings),
            call=lambda: get_me(session, bot_token=settings.telegram_bot_token),
        )
        bot_id = int(me.get("id", 0) or 0)
        if bot_id <= 0:
            logger.error("Could not resolve bot id from getMe response")
            return
        member = await _telegram_call_with_retry(
            logger=logger,
            label="getChatMember",
            max_attempts=_retry_attempts(settings),
            call=lambda: get_chat_member(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=forum_chat_id,
                user_id=bot_id,
            ),
        )
    except Exception as exc:
        logger.error(
            "Forum permission preflight failed chat_id=%s error=%s",
            forum_chat_id,
            exc,
        )
        return

    status = str(member.get("status", "")).strip().lower()
    can_manage_topics = bool(member.get("can_manage_topics"))
    is_ok = status == "creator" or (status == "administrator" and can_manage_topics)
    if is_ok:
        logger.info(
            "Forum preflight ok chat_id=%s bot_status=%s can_manage_topics=%s",
            forum_chat_id,
            status,
            can_manage_topics,
        )
        return
    logger.error(
        "Forum preflight failed: bot requires admin + Manage Topics in chat_id=%s (status=%s can_manage_topics=%s)",
        forum_chat_id,
        status or "-",
        can_manage_topics,
    )


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

    address = _normalize_address(payload.removeprefix("sub_"))
    if not address:
        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text="Invalid trader payload.",
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
                text="Trader is not available.",
            )
            return
        if trader.status in {STATUS_STALE, STATUS_ARCHIVED}:
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "Trader is currently not eligible for listing.\n"
                    "Pick another trader from the listed catalog."
                ),
            )
            return

        try:
            thread_id, _topic_name = await _ensure_shared_forum_topic(
                session=session,
                settings=settings,
                trader_address=address,
                logger=logger,
            )
            if thread_id is None:
                await _send_message_safe(
                    session=session,
                    settings=settings,
                    logger=logger,
                    chat_id=chat_id,
                    text=(
                        "Shared forum chat is not configured yet.\n"
                        "Ask admin to set <code>TELEGRAM_FORUM_CHAT_ID</code> and grant bot Manage Topics."
                    ),
                )
                return
            logger.info(
                "Resolved shared topic trader=%s forum_chat_id=%s thread=%s",
                address,
                _forum_chat_id(settings),
                thread_id,
            )
            await _send_trader_topic_link(
                session=session,
                settings=settings,
                chat_id=chat_id,
                trader_address=address,
                thread_id=thread_id,
                logger=logger,
            )
        except TelegramClientError as exc:
            if exc.is_forum_not_supported():
                await _send_message_safe(
                    session=session,
                    settings=settings,
                    logger=logger,
                    chat_id=chat_id,
                    text=(
                        "The configured group is not a forum.\n"
                        "Enable Topics in the Telegram supergroup settings."
                    ),
                )
                return
            if exc.is_chat_unavailable():
                await _send_message_safe(
                    session=session,
                    settings=settings,
                    logger=logger,
                    chat_id=chat_id,
                    text=(
                        "Bot cannot access the forum group.\n"
                        "Ensure the bot is admin with <b>Manage Topics</b> permission."
                    ),
                )
                return
            logger.exception("Failed to resolve shared topic for chat %s: %s", chat_id, exc)
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "Failed to open trader topic.\n"
                    "Please try again in 10-20 seconds."
                ),
            )
        except Exception as exc:
            logger.exception("Failed to resolve shared topic for chat %s: %s", chat_id, exc)
            await _send_message_safe(
                session=session,
                settings=settings,
                logger=logger,
                chat_id=chat_id,
                text=(
                    "Failed to open trader topic.\n"
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
        join_url = str(getattr(settings, "telegram_join_url", "") or "").strip()
        forum_chat = _forum_chat_id(settings)
        lines = [
            "<b>Shared channel mode</b>",
            "There are no personal subscriptions in DM.",
            "All listed traders are posted to shared forum topics in the group.",
        ]
        if join_url:
            lines.append(
                f"Join group: <a href=\"{html.escape(join_url, quote=True)}\">Open Channel</a>"
            )
        lines.append(f"Forum chat id: <code>{html.escape(forum_chat, quote=False) or '-'}</code>")
        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text="\n".join(lines),
        )
        return

    if command == "stop":
        await _send_message_safe(
            session=session,
            settings=settings,
            logger=logger,
            chat_id=chat_id,
            text=(
                "Command <code>/stop</code> is deprecated in shared channel mode.\n"
                "Use channel mute/hide settings in Telegram if you want fewer notifications."
            ),
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
        await _validate_forum_permissions(
            session=session,
            settings=settings,
            logger=logger,
        )
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
