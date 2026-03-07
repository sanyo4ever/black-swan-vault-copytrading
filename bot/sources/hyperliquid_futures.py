from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bot.models import TradeSignal
from bot.sources.base import Source
from bot.trader_store import TraderStore


class HyperliquidFuturesSource(Source):
    def __init__(
        self,
        config: dict[str, Any],
        *,
        http_session,
        database_path,
        info_url: str,
    ) -> None:
        super().__init__(config, http_session=http_session)
        self._database_path = database_path
        self._info_url = info_url

    async def _info(self, payload: dict[str, Any]) -> Any:
        async with self.http_session.post(self._info_url, json=payload) as response:
            response.raise_for_status()
            return await response.json(content_type=None)

    @staticmethod
    def _build_side(fill: dict[str, Any]) -> str | None:
        direction = str(fill.get("dir", "")).lower()
        if "long" in direction:
            return "LONG"
        if "short" in direction:
            return "SHORT"

        side = str(fill.get("side", "")).upper()
        if side == "B":
            return "BUY"
        if side == "A":
            return "SELL"
        return None

    @staticmethod
    def _build_note(fill: dict[str, Any], *, trader: str) -> str:
        direction = str(fill.get("dir", "")).strip() or "Fill"
        size = str(fill.get("sz", "")).strip()
        pnl = str(fill.get("closedPnl", "")).strip()
        parts = [f"Trader {trader[:6]}...{trader[-4:]}", direction]
        if size:
            parts.append(f"size={size}")
        if pnl and pnl != "0.0":
            parts.append(f"closedPnL={pnl}")
        return " | ".join(parts)

    async def fetch_signals(self) -> list[TradeSignal]:
        if not self.enabled:
            return []

        trader_limit = int(self.config.get("trader_limit", 20))
        max_fills_per_trader = int(self.config.get("max_fills_per_trader", 5))
        lookback_minutes = int(self.config.get("lookback_minutes", 90))

        start_ms = int((datetime.now(tz=UTC).timestamp() - (lookback_minutes * 60)) * 1000)

        with TraderStore(self._database_path) as store:
            traders = store.list_active_addresses(limit=trader_limit)

            signals: list[TradeSignal] = []
            last_fill_updates: list[tuple[str, int]] = []
            seen_ids: set[str] = set()

            for trader in traders:
                fills = await self._info(
                    {
                        "type": "userFillsByTime",
                        "user": trader,
                        "startTime": start_ms,
                    }
                )
                if not isinstance(fills, list):
                    continue

                newest_fill = None
                sorted_fills = sorted(
                    (item for item in fills if isinstance(item, dict)),
                    key=lambda item: int(item.get("time", 0)),
                    reverse=True,
                )
                for fill in sorted_fills[:max_fills_per_trader]:
                    if not isinstance(fill, dict):
                        continue

                    fill_time = int(fill.get("time", 0))
                    if fill_time <= 0:
                        continue

                    if newest_fill is None or fill_time > newest_fill:
                        newest_fill = fill_time

                    tx_hash = str(fill.get("hash", "")).strip()
                    oid = str(fill.get("oid", "")).strip()
                    external_id = (
                        f"{tx_hash}:{oid or fill_time}"
                        if tx_hash
                        else f"{trader}:{oid}:{fill_time}"
                    )
                    if external_id in seen_ids:
                        continue
                    seen_ids.add(external_id)

                    signals.append(
                        TradeSignal(
                            source_id=self.id,
                            source_name=self.name,
                            external_id=external_id,
                            symbol=f"{str(fill.get('coin', '')).strip()}-PERP",
                            side=self._build_side(fill),
                            entry=str(fill.get("px", "")).strip() or None,
                            note=self._build_note(fill, trader=trader),
                            timeframe="PERP",
                            timestamp=self.parse_timestamp(fill_time / 1000),
                            url=(
                                f"https://app.hyperliquid.xyz/explorer/tx/{tx_hash}"
                                if tx_hash
                                else None
                            ),
                            raw=fill,
                        )
                    )

                if newest_fill:
                    last_fill_updates.append((trader, newest_fill))

            if last_fill_updates:
                store.touch_last_fill_times(last_fill_updates)

        return signals
