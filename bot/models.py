from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class TradeSignal:
    source_id: str
    source_name: str
    external_id: str
    symbol: str | None = None
    side: str | None = None
    entry: str | None = None
    stop_loss: str | None = None
    take_profit: str | None = None
    timeframe: str | None = None
    note: str | None = None
    timestamp: datetime | None = None
    url: str | None = None
    raw: dict[str, Any] | None = None

    def dedup_key(self) -> str:
        return f"{self.source_id}:{self.external_id}"
