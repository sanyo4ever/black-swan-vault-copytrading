from __future__ import annotations

from typing import Any

import aiohttp


class TelegramClientError(RuntimeError):
    pass


async def send_message(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    text: str,
    message_thread_id: int | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if message_thread_id is not None:
        payload["message_thread_id"] = int(message_thread_id)
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    async with session.post(url, json=payload) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("ok", False):
            raise TelegramClientError(
                f"Telegram API error ({response.status}): {data}"
            )


async def get_updates(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    offset: int | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    payload: dict[str, Any] = {
        "timeout": timeout,
        "allowed_updates": ["message"],
    }
    if offset is not None:
        payload["offset"] = offset

    async with session.post(url, json=payload) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("ok", False):
            raise TelegramClientError(
                f"Telegram API error ({response.status}): {data}"
            )
        result = data.get("result")
        if not isinstance(result, list):
            return []
        return [item for item in result if isinstance(item, dict)]


async def create_forum_topic(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    name: str,
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/createForumTopic"
    payload = {
        "chat_id": chat_id,
        "name": name,
    }
    async with session.post(url, json=payload) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("ok", False):
            raise TelegramClientError(
                f"Telegram API error ({response.status}): {data}"
            )
        result = data.get("result")
        if not isinstance(result, dict):
            raise TelegramClientError(f"Unexpected createForumTopic result: {data}")
        return result


async def delete_forum_topic(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    message_thread_id: int,
) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/deleteForumTopic"
    payload = {
        "chat_id": chat_id,
        "message_thread_id": int(message_thread_id),
    }
    async with session.post(url, json=payload) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("ok", False):
            raise TelegramClientError(
                f"Telegram API error ({response.status}): {data}"
            )
