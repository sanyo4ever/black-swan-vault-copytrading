from __future__ import annotations

from html import escape

from bot.models import TradeSignal

TELEGRAM_MAX_MESSAGE_LENGTH = 4096


def _line(label: str, value: str | None) -> str:
    if not value:
        return ""
    return f"<b>{escape(label)}:</b> {escape(value)}"


def _cap(value: str | None, *, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= max(1, int(limit)):
        return text
    clipped = max(1, int(limit)) - 1
    return f"{text[:clipped]}…"


def _fallback_compact(signal: TradeSignal) -> str:
    trader_line = None
    if signal.trader_address:
        trader_line = f"{signal.trader_address[:6]}...{signal.trader_address[-4:]}"

    lines = [
        _cap(signal.source_name or "Signal", limit=120) or "Signal",
        f"Trader: {_cap(trader_line, limit=48) or '-'}",
        f"Symbol: {_cap(signal.symbol, limit=48) or '-'}",
        f"Side: {_cap(signal.side, limit=16) or '-'}",
        f"Entry: {_cap(signal.entry, limit=32) or '-'}",
        f"Timeframe: {_cap(signal.timeframe, limit=24) or '-'}",
        f"Note: {_cap(signal.note or 'New trade signal', limit=1200) or 'New trade signal'}",
    ]
    if signal.url:
        lines.append(f"Link: {_cap(signal.url, limit=1800) or '-'}")
    return "\n".join(lines)


def format_signal(signal: TradeSignal) -> str:
    header = f"<b>{escape(signal.source_name)}</b>"
    trader_line = None
    if signal.trader_address:
        trader_line = f"{signal.trader_address[:6]}...{signal.trader_address[-4:]}"

    body_parts = [
        _line("Trader", _cap(trader_line, limit=48)),
        _line("Symbol", _cap(signal.symbol, limit=48)),
        _line("Side", _cap(signal.side, limit=16)),
        _line("Entry", _cap(signal.entry, limit=32)),
        _line("SL", _cap(signal.stop_loss, limit=32)),
        _line("TP", _cap(signal.take_profit, limit=32)),
        _line("Timeframe", _cap(signal.timeframe, limit=24)),
        _line("Note", _cap(signal.note, limit=2000)),
    ]

    body = "\n".join(part for part in body_parts if part)

    footer = ""
    if signal.url:
        footer = f"\n\n<a href=\"{escape(_cap(signal.url, limit=1800) or signal.url)}\">Open Source</a>"

    if body:
        message = f"{header}\n\n{body}{footer}"
    else:
        message = f"{header}\n\n{escape(_cap(signal.note or 'New trade signal', limit=2200) or 'New trade signal')}{footer}"

    if len(message) <= TELEGRAM_MAX_MESSAGE_LENGTH:
        return message

    fallback = _fallback_compact(signal)
    if len(fallback) <= TELEGRAM_MAX_MESSAGE_LENGTH:
        return fallback
    return fallback[: TELEGRAM_MAX_MESSAGE_LENGTH - 1] + "…"
