from __future__ import annotations

import re
from html import unescape
from typing import Any

import feedparser

from bot.models import TradeSignal
from bot.sources.base import Source


SIDE_PATTERNS = {
    "LONG": re.compile(r"\b(long|buy)\b", re.IGNORECASE),
    "SHORT": re.compile(r"\b(short|sell)\b", re.IGNORECASE),
}
SYMBOL_PATTERN = re.compile(r"\b([A-Z]{2,15}(?:/USDT|USDT)?)\b")
ENTRY_PATTERN = re.compile(r"\b(entry|buy|sell)\s*[:@]?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
SL_PATTERN = re.compile(r"\b(sl|stop(?:[- ]?loss)?)\s*[:@]?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
TP_PATTERN = re.compile(r"\b(tp|take(?:[- ]?profit)?)\s*[:@]?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
TF_PATTERN = re.compile(r"\b(1m|3m|5m|15m|30m|1h|4h|12h|1d|1w)\b", re.IGNORECASE)


def _extract_signal_fields(text: str) -> dict[str, str | None]:
    symbol = None
    skip_tokens = {
        "LONG",
        "SHORT",
        "BUY",
        "SELL",
        "ENTRY",
        "SL",
        "TP",
        "STOP",
        "LOSS",
        "TAKE",
        "PROFIT",
    }
    for matched in SYMBOL_PATTERN.finditer(text):
        candidate = str(matched.group(1) or "").strip().upper()
        if not candidate:
            continue
        if candidate in skip_tokens:
            continue
        symbol = candidate
        break

    side = None
    for label, pattern in SIDE_PATTERNS.items():
        if pattern.search(text):
            side = label
            break

    entry_match = ENTRY_PATTERN.search(text)
    sl_match = SL_PATTERN.search(text)
    tp_match = TP_PATTERN.search(text)
    tf_match = TF_PATTERN.search(text)

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry_match.group(2) if entry_match else None,
        "stop_loss": sl_match.group(2) if sl_match else None,
        "take_profit": tp_match.group(2) if tp_match else None,
        "timeframe": tf_match.group(1).upper() if tf_match else None,
    }


class RssSource(Source):
    async def fetch_signals(self) -> list[TradeSignal]:
        if not self.enabled:
            return []

        url = str(self.config.get("url", "")).strip()
        if not url:
            return []

        limit = int(self.config.get("limit", 10))

        async with self.http_session.get(url) as response:
            response.raise_for_status()
            body = await response.read()

        feed = feedparser.parse(body)
        entries = list(feed.entries)[:limit]

        signals: list[TradeSignal] = []
        for entry in entries:
            title = unescape(str(entry.get("title", "")).strip())
            summary = unescape(str(entry.get("summary", "")).strip())
            text_blob = f"{title}\n{summary}".strip()

            extracted = _extract_signal_fields(text_blob)

            published_raw: Any = (
                entry.get("published_parsed")
                or entry.get("updated_parsed")
                or entry.get("published")
                or entry.get("updated")
            )
            external_id = str(entry.get("id") or entry.get("link") or "").strip()
            if not external_id:
                external_id = self.stable_hash(text_blob)

            signals.append(
                TradeSignal(
                    source_id=self.id,
                    source_name=self.name,
                    external_id=external_id,
                    symbol=extracted["symbol"],
                    side=extracted["side"],
                    entry=extracted["entry"],
                    stop_loss=extracted["stop_loss"],
                    take_profit=extracted["take_profit"],
                    timeframe=extracted["timeframe"],
                    note=title or summary[:300] or "New signal",
                    timestamp=self.parse_timestamp(published_raw),
                    url=str(entry.get("link", "")).strip() or None,
                    raw={"title": title, "summary": summary},
                )
            )

        return signals
