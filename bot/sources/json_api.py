from __future__ import annotations

from typing import Any

from bot.models import TradeSignal
from bot.sources.base import Source


def _dig(value: Any, path: str) -> Any:
    current = value
    for chunk in path.split("."):
        chunk = chunk.strip()
        if not chunk:
            continue
        if isinstance(current, dict):
            current = current.get(chunk)
            continue
        if isinstance(current, list):
            try:
                idx = int(chunk)
            except ValueError:
                return None
            if idx < 0 or idx >= len(current):
                return None
            current = current[idx]
            continue
        return None
    return current


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class JsonApiSource(Source):
    async def fetch_signals(self) -> list[TradeSignal]:
        if not self.enabled:
            return []

        url = _stringify(self.config.get("url"))
        if not url:
            return []

        method = _stringify(self.config.get("method")) or "GET"
        headers = self.config.get("headers")
        if not isinstance(headers, dict):
            headers = {}

        payload = self.config.get("payload")

        request_kwargs: dict[str, Any] = {"headers": headers}
        if payload is not None:
            request_kwargs["json"] = payload

        async with self.http_session.request(method, url, **request_kwargs) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)

        item_path = _stringify(self.config.get("item_path")) or ""
        items = _dig(data, item_path) if item_path else data

        if not isinstance(items, list):
            return []

        field_map = self.config.get("field_map")
        if not isinstance(field_map, dict):
            field_map = {}

        static_values = self.config.get("static_values")
        if not isinstance(static_values, dict):
            static_values = {}

        limit = int(self.config.get("limit", len(items) or 100))

        signals: list[TradeSignal] = []
        for item in items[:limit]:
            if not isinstance(item, dict):
                continue

            def pick(name: str) -> Any:
                if name in static_values:
                    return static_values[name]
                path = _stringify(field_map.get(name))
                if not path:
                    return item.get(name)
                return _dig(item, path)

            note = _stringify(pick("note")) or _stringify(pick("title")) or "New signal"
            source_name = _stringify(pick("source")) or self.name
            external_id = _stringify(pick("id"))
            if not external_id:
                fallback = "|".join(
                    filter(
                        None,
                        [
                            _stringify(pick("symbol")),
                            _stringify(pick("side")),
                            _stringify(pick("entry")),
                            _stringify(pick("timestamp")),
                            note,
                        ],
                    )
                )
                external_id = self.stable_hash(fallback or str(item))

            signals.append(
                TradeSignal(
                    source_id=self.id,
                    source_name=source_name,
                    external_id=external_id,
                    symbol=_stringify(pick("symbol")),
                    side=(_stringify(pick("side")) or "").upper() or None,
                    entry=_stringify(pick("entry")),
                    stop_loss=_stringify(pick("stop_loss")),
                    take_profit=_stringify(pick("take_profit")),
                    timeframe=_stringify(pick("timeframe")),
                    note=note,
                    timestamp=self.parse_timestamp(pick("timestamp")),
                    url=_stringify(pick("url")),
                    raw=item,
                )
            )

        return signals
