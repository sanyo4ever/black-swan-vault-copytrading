from __future__ import annotations

import asyncio
import logging

import aiohttp

from bot.config import ConfigError, load_settings
from bot.telegram_client import get_updates, send_message
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


async def _send_welcome(*, session: aiohttp.ClientSession, settings, chat_id: int) -> None:
    await send_message(
        session,
        bot_token=settings.telegram_bot_token,
        chat_id=chat_id,
        text=(
            "<b>CryptoInsider Bot</b>\n\n"
            "1) Відкрий каталог трейдерів\n"
            "2) Натисни <b>Open Trader Chat</b>\n"
            "3) Бот автоматично підпише тебе на обраного трейдера\n\n"
            "Команди:\n"
            "<code>/my</code> - мої підписки\n"
            "<code>/stop 0x...</code> - відписатись від трейдера"
        ),
    )


async def _handle_start_with_payload(
    *,
    session: aiohttp.ClientSession,
    settings,
    chat_id: int,
    payload: str,
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

    with TraderStore(settings.database_path) as store:
        trader = store.get_trader(address=address)
        if trader is None:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Трейдера не знайдено в базі.",
            )
            return

        store.subscribe_chat_to_trader(chat_id=chat_id, trader_address=trader.address)
        if trader.status != STATUS_ACTIVE:
            store.set_status(address=trader.address, status=STATUS_ACTIVE)

    await send_message(
        session,
        bot_token=settings.telegram_bot_token,
        chat_id=chat_id,
        text=(
            "<b>Готово ✅</b>\n"
            f"Тепер ти отримуватимеш трейди для <code>{_short(address)}</code>.\n\n"
            "Подивитися підписки: <code>/my</code>\n"
            "Відписатись: <code>/stop 0x...</code>"
        ),
    )


async def _handle_message(*, session: aiohttp.ClientSession, settings, message: dict) -> None:
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
        )
        return

    if command == "my":
        with TraderStore(settings.database_path) as store:
            subscriptions = store.list_subscriptions_for_chat(chat_id=chat_id)

        if not subscriptions:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Підписок поки немає. Обери трейдера в каталозі.",
            )
            return

        lines = ["<b>Твої підписки</b>"]
        for sub in subscriptions:
            label = sub.trader_label or "Unnamed"
            lines.append(
                f"• <code>{_short(sub.trader_address)}</code> - {label} ({sub.status})"
            )
        lines.append("\nВідписатись: <code>/stop 0x...</code>")

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

        with TraderStore(settings.database_path) as store:
            removed = store.unsubscribe_chat_from_trader(chat_id=chat_id, trader_address=address)

        if removed > 0:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text=f"Відписку від <code>{_short(address)}</code> вимкнено.",
            )
        else:
            await send_message(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=chat_id,
                text="Такої підписки не знайдено.",
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
                    await _handle_message(session=session, settings=settings, message=message)
                except Exception as exc:
                    logger.exception("Failed to process update %s: %s", update_id, exc)


if __name__ == "__main__":
    asyncio.run(run_bot())
