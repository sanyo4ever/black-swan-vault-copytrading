from __future__ import annotations

import aiohttp


class TelegramClientError(RuntimeError):
    pass


async def send_message(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    text: str,
) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    async with session.post(url, json=payload) as response:
        data = await response.json(content_type=None)
        if response.status >= 400 or not data.get("ok", False):
            raise TelegramClientError(
                f"Telegram API error ({response.status}): {data}"
            )
