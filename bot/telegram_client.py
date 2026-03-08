from __future__ import annotations

import logging
import re
import time
from typing import Any
from typing import Mapping

import aiohttp

_LOGGER = logging.getLogger("cryptoinsider.telegram")
_TELEGRAM_HTTP_LOG_ENABLED = False


def set_telegram_http_logging(enabled: bool) -> None:
    global _TELEGRAM_HTTP_LOG_ENABLED
    _TELEGRAM_HTTP_LOG_ENABLED = bool(enabled)


class TelegramClientError(RuntimeError):
    def __init__(
        self,
        *,
        method: str,
        status_code: int,
        error_code: int | None = None,
        description: str | None = None,
        retry_after: int | None = None,
        payload: Any = None,
    ) -> None:
        self.method = method
        self.status_code = int(status_code)
        self.error_code = int(error_code) if error_code is not None else None
        self.description = (description or "").strip()
        self.retry_after = int(retry_after) if retry_after is not None else None
        self.payload = payload
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        return (
            f"Telegram API {self.method} failed "
            f"(status={self.status_code}, error_code={self.error_code}, "
            f"retry_after={self.retry_after}): {self.description or self.payload}"
        )

    def _desc(self) -> str:
        return self.description.lower()

    def is_flood_limit(self) -> bool:
        desc = self._desc()
        return (
            self.status_code == 429
            or self.error_code == 429
            or self.retry_after is not None
            or "too many requests" in desc
            or "retry after" in desc
        )

    def is_topic_missing(self) -> bool:
        desc = self._desc()
        return (
            "message thread not found" in desc
            or "topic was deleted" in desc
            or "topic_deleted" in desc
            or "forum topic" in desc and "not found" in desc
        )

    def is_forum_not_supported(self) -> bool:
        desc = self._desc()
        return (
            "chat is not a forum" in desc
            or "available only for forum supergroups" in desc
            or "method is available only for forum" in desc
        )

    def is_chat_blocked(self) -> bool:
        desc = self._desc()
        return (
            "bot was blocked by the user" in desc
            or "user is deactivated" in desc
            or "bot can't initiate conversation with a user" in desc
            or "have no rights to send a message" in desc
        )

    def is_chat_not_found(self) -> bool:
        desc = self._desc()
        return (
            "chat not found" in desc
            or "bot is not a member of the channel chat" in desc
            or "kicked from the channel chat" in desc
        )

    def is_transient(self) -> bool:
        return self.status_code >= 500 or self.error_code in {500, 502, 503, 504}

    def is_chat_unavailable(self) -> bool:
        desc = self._desc()
        return (
            self.is_chat_blocked()
            or self.is_chat_not_found()
            or "forbidden" in desc and "not a member" in desc
        )


_RETRY_AFTER_RE = re.compile(r"retry after\s+(\d+)", flags=re.IGNORECASE)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_retry_after(*, description: str, parameters: Any) -> int | None:
    if isinstance(parameters, Mapping):
        parsed = _safe_int(parameters.get("retry_after"))
        if parsed is not None and parsed > 0:
            return parsed
    matched = _RETRY_AFTER_RE.search(description)
    if matched is None:
        return None
    parsed = _safe_int(matched.group(1))
    if parsed is None or parsed <= 0:
        return None
    return parsed


async def _read_json_or_text(response: aiohttp.ClientResponse) -> Any:
    try:
        return await response.json(content_type=None)
    except Exception:
        text = await response.text()
        return {"ok": False, "description": text[:3000]}


def _raise_telegram_error(*, method: str, status_code: int, payload: Any) -> None:
    if isinstance(payload, Mapping):
        error_code = _safe_int(payload.get("error_code"))
        description = str(payload.get("description", "") or "").strip()
        retry_after = _extract_retry_after(
            description=description,
            parameters=payload.get("parameters"),
        )
    else:
        error_code = None
        description = str(payload)
        retry_after = None

    raise TelegramClientError(
        method=method,
        status_code=status_code,
        error_code=error_code,
        description=description,
        retry_after=retry_after,
        payload=payload,
    )


async def _telegram_request(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    method: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    started = time.monotonic()
    async with session.post(url, json=payload) as response:
        data = await _read_json_or_text(response)
        duration_ms = int((time.monotonic() - started) * 1000)
        if response.status >= 400:
            description = ""
            retry_after = None
            error_code = None
            if isinstance(data, Mapping):
                description = str(data.get("description", "") or "").strip()
                retry_after = _extract_retry_after(
                    description=description,
                    parameters=data.get("parameters"),
                )
                error_code = _safe_int(data.get("error_code"))
            _LOGGER.warning(
                "Telegram API request failed method=%s status=%s duration_ms=%s error_code=%s retry_after=%s description=%s",
                method,
                response.status,
                duration_ms,
                error_code,
                retry_after,
                (description[:500] if description else "-"),
            )
            _raise_telegram_error(method=method, status_code=response.status, payload=data)
        if not isinstance(data, Mapping):
            _raise_telegram_error(
                method=method,
                status_code=response.status,
                payload={"ok": False, "description": f"Unexpected payload type: {type(data)}"},
            )
        if not data.get("ok", False):
            _LOGGER.warning(
                "Telegram API request returned ok=false method=%s status=%s duration_ms=%s",
                method,
                response.status,
                duration_ms,
            )
            _raise_telegram_error(method=method, status_code=response.status, payload=data)
        if _TELEGRAM_HTTP_LOG_ENABLED:
            _LOGGER.info(
                "Telegram API request ok method=%s status=%s duration_ms=%s",
                method,
                response.status,
                duration_ms,
            )
        return dict(data)


async def send_message(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    text: str,
    message_thread_id: int | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> None:
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

    await _telegram_request(
        session,
        bot_token=bot_token,
        method="sendMessage",
        payload=payload,
    )


async def get_updates(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    offset: int | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {
        "timeout": timeout,
        "allowed_updates": ["message"],
    }
    if offset is not None:
        payload["offset"] = offset

    data = await _telegram_request(
        session,
        bot_token=bot_token,
        method="getUpdates",
        payload=payload,
    )
    result = data.get("result")
    if not isinstance(result, list):
        return []
    return [item for item in result if isinstance(item, dict)]


async def get_me(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
) -> dict[str, Any]:
    data = await _telegram_request(
        session,
        bot_token=bot_token,
        method="getMe",
        payload={},
    )
    result = data.get("result")
    if not isinstance(result, dict):
        raise TelegramClientError(
            method="getMe",
            status_code=200,
            description=f"Unexpected getMe result: {data}",
            payload=data,
        )
    return result


async def get_chat_member(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    user_id: int,
) -> dict[str, Any]:
    data = await _telegram_request(
        session,
        bot_token=bot_token,
        method="getChatMember",
        payload={
            "chat_id": chat_id,
            "user_id": int(user_id),
        },
    )
    result = data.get("result")
    if not isinstance(result, dict):
        raise TelegramClientError(
            method="getChatMember",
            status_code=200,
            description=f"Unexpected getChatMember result: {data}",
            payload=data,
        )
    return result


async def create_forum_topic(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    name: str,
) -> dict[str, Any]:
    payload = {
        "chat_id": chat_id,
        "name": name,
    }
    data = await _telegram_request(
        session,
        bot_token=bot_token,
        method="createForumTopic",
        payload=payload,
    )
    result = data.get("result")
    if not isinstance(result, dict):
        raise TelegramClientError(
            method="createForumTopic",
            status_code=200,
            description=f"Unexpected createForumTopic result: {data}",
            payload=data,
        )
    return result


async def delete_forum_topic(
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str | int,
    message_thread_id: int,
) -> None:
    payload = {
        "chat_id": chat_id,
        "message_thread_id": int(message_thread_id),
    }
    await _telegram_request(
        session,
        bot_token=bot_token,
        method="deleteForumTopic",
        payload=payload,
    )
