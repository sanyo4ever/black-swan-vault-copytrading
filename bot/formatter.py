from __future__ import annotations

from html import escape

from bot.models import TradeSignal


def _line(label: str, value: str | None) -> str:
    if not value:
        return ""
    return f"<b>{escape(label)}:</b> {escape(value)}"


def format_signal(signal: TradeSignal) -> str:
    header = f"<b>{escape(signal.source_name)}</b>"

    body_parts = [
        _line("Symbol", signal.symbol),
        _line("Side", signal.side),
        _line("Entry", signal.entry),
        _line("SL", signal.stop_loss),
        _line("TP", signal.take_profit),
        _line("Timeframe", signal.timeframe),
        _line("Note", signal.note),
    ]

    body = "\n".join(part for part in body_parts if part)

    footer = ""
    if signal.url:
        footer = f"\n\n<a href=\"{escape(signal.url)}\">Open Source</a>"

    if body:
        return f"{header}\n\n{body}{footer}"

    return f"{header}\n\n{escape(signal.note or 'New trade signal')}{footer}"
