from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import aiohttp

from bot.models import TradeSignal


class Source(ABC):
    def __init__(self, config: dict[str, Any], *, http_session: aiohttp.ClientSession) -> None:
        self.config = config
        self.id = str(config["id"]).strip()
        self.name = str(config.get("name") or self.id)
        self.enabled = bool(config.get("enabled", True))
        self.http_session = http_session

    @abstractmethod
    async def fetch_signals(self) -> list[TradeSignal]:
        raise NotImplementedError

    @staticmethod
    def parse_timestamp(raw: Any) -> datetime | None:
        if raw is None:
            return None
        if hasattr(raw, "tm_year"):
            return datetime(
                raw.tm_year,
                raw.tm_mon,
                raw.tm_mday,
                raw.tm_hour,
                raw.tm_min,
                raw.tm_sec,
                tzinfo=UTC,
            )
        if isinstance(raw, (tuple, list)) and len(raw) >= 6:
            return datetime(
                int(raw[0]),
                int(raw[1]),
                int(raw[2]),
                int(raw[3]),
                int(raw[4]),
                int(raw[5]),
                tzinfo=UTC,
            )
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=UTC)
        if isinstance(raw, str):
            value = raw.strip()
            if not value:
                return None
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(value)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except ValueError:
                return None
        return None

    @staticmethod
    def stable_hash(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
