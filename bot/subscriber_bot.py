from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import aiohttp

from bot.config import ConfigError, load_settings
from bot.telegram_client import (
    create_forum_topic,
    delete_forum_topic,
    get_updates,
    send_message,
)
from bot.trader_store import STATUS_ACTIVE, TraderStore


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


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


async def _send_welcome(*, session: aiohttp.ClientSession, settings, chat_id: int) -> None:
    await send_message(
        session,
        bot_token=settings.telegram_bot_token,
        chat_id=chat_id,
        text=(
            "<b>CryptoInsider Bot</b>\n\n"
            "1) Відкрий каталог трейдерів\n"
            "2) Натисни <b>Open Trader Chat</b>\n"
            "3) Бот створить окремий тред на 24h і буде постити угоди туди\n\n"
            "Команди:\n"
            "<code>/my</code> - активні треди\n"
            "<code>/stop 0x...</code> - зупинити трейдера і видалити тред"
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
            await delete_forum_topic(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                message_thread_id=item.message_thread_id,
            )
        except Exception as exc:
            failed += 1
            logger.warning(
                "Failed to delete forum topic chat_id=%s thread=%s: %s",
                chat_id,
                item.message_thread_id,
                exc,
            )
            with TraderStore(settings.database_dsn) as store:
                store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
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
        await _send_welcome(session=session, settings=settings, chat_id=chat_id)
        return

    address = payload.removeprefix("sub_").strip().lower()
    if not address:
        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text="Некоректний payload підписки.",
        )
        return

    with TraderStore(settings.database_dsn) as store:
        trader = store.get_trader(address=address)
        if trader is None:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Трейдера не знайдено в базі.",
            )
            return

        # Each new subscription starts a clean 24h session for this trader in this chat.
        previous_sessions = store.cancel_chat_trader_subscriptions(
            chat_id=chat_id,
            trader_address=trader.address,
        )

    await _delete_topics_best_effort(
        session=session,
        settings=settings,
        chat_id=chat_id,
        sessions=previous_sessions,
        logger=logger,
    )

    topic_name = f"{_short(address)} | 24h"
    topic_result = None
    try:
        topic_result = await create_forum_topic(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            name=topic_name,
        )
        thread_id = int(topic_result.get("message_thread_id", 0))
        if thread_id <= 0:
            raise RuntimeError(f"Invalid forum topic response: {topic_result}")

        with TraderStore(settings.database_dsn) as store:
            session_info = store.create_subscription_with_session(
                chat_id=chat_id,
                trader_address=address,
                message_thread_id=thread_id,
                topic_name=topic_name,
                lifetime_hours=settings.subscription_lifetime_hours,
            )
            if trader.status != STATUS_ACTIVE:
                store.set_status(address=address, status=STATUS_ACTIVE)

        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=(
                "<b>Session started ✅</b>\n"
                f"Trader: <code>{_short(address)}</code>\n"
                f"Expires: <b>{_fmt_expiry(session_info.expires_at)}</b>\n\n"
                "Сюди будуть приходити нові угоди цього трейдера."
            ),
        )

        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text=(
                "<b>Готово ✅</b>\n"
                f"Створено окремий тред для <code>{_short(address)}</code>.\n"
                f"TTL: {settings.subscription_lifetime_hours}h\n\n"
                "Переглянути активні треди: <code>/my</code>"
            ),
        )
    except Exception as exc:
        logger.exception("Failed to create topic subscription for chat %s: %s", chat_id, exc)
        if topic_result and topic_result.get("message_thread_id"):
            try:
                await delete_forum_topic(
                    session,
                    bot_token=settings.telegram_bot_token,
                    chat_id=chat_id,
                    message_thread_id=int(topic_result["message_thread_id"]),
                )
            except Exception:
                pass
        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text=(
                "Не вдалося створити новий тред для підписки.\n"
                "Спробуй ще раз через 10-20 секунд."
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
        with TraderStore(settings.database_dsn) as store:
            sessions = store.list_delivery_sessions_for_chat(chat_id=chat_id)

        if not sessions:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Активних тредів поки немає. Обери трейдера в каталозі.",
            )
            return

        lines = ["<b>Твої активні треди</b>"]
        for item in sessions:
            topic = item.topic_name or "Trader thread"
            thread = item.message_thread_id if item.message_thread_id is not None else "-"
            lines.append(
                f"• <code>{_short(item.trader_address)}</code> | {topic} | thread={thread} | exp={_fmt_expiry(item.expires_at)}"
            )
        lines.append("\nЗупинити: <code>/stop 0x...</code>")

        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text="\n".join(lines),
        )
        return

    if command == "stop":
        address = arg.strip().lower()
        if not address:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Використай: <code>/stop 0x...</code>",
            )
            return

        with TraderStore(settings.database_dsn) as store:
            sessions = store.cancel_chat_trader_subscriptions(
                chat_id=chat_id,
                trader_address=address,
            )

        if not sessions:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Активної підписки на цього трейдера не знайдено.",
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
        await send_message(
            session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            text=f"Підписку на <code>{_short(address)}</code> зупинено{suffix}.",
        )
        return

    await _send_welcome(session=session, settings=settings, chat_id=chat_id)


async def run_bot() -> None:
    _setup_logging()
    logger = logging.getLogger("cryptoinsider.subscriber-bot")

    try:
        settings = load_settings()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc

    long_poll_timeout = 50
    timeout = aiohttp.ClientTimeout(total=long_poll_timeout + 20)
    offset: int | None = None

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                updates = await get_updates(
                    session,
                    bot_token=settings.telegram_bot_token,
                    offset=offset,
                    timeout=long_poll_timeout,
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

            for update in updates:
                update_id = update.get("update_id")
                if isinstance(update_id, int):
                    offset = update_id + 1

                message = update.get("message")
                if not isinstance(message, dict):
                    continue

                try:
                    await _handle_message(
                        session=session,
                        settings=settings,
                        message=message,
                        logger=logger,
                    )
                except Exception as exc:
                    logger.exception("Failed to process update %s: %s", update_id, exc)


if __name__ == "__main__":
    asyncio.run(run_bot())
