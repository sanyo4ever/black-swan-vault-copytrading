from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bot.models import TradeSignal
from bot.sources.base import Source
from bot.trader_store import MonitoringTarget, TraderStore, TIER_HOT


class HyperliquidFuturesSource(Source):
    def __init__(
        self,
        config: dict[str, Any],
        *,
        http_session,
        database_dsn,
        info_url: str,
    ) -> None:
        super().__init__(config, http_session=http_session)
        self._database_dsn = database_dsn
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
        delivery_only_subscribed = bool(self.config.get("delivery_only_subscribed", True))

        with TraderStore(self._database_dsn) as store:
            targets = store.list_due_monitoring_targets(
                limit=trader_limit,
                only_subscribed=delivery_only_subscribed,
            )
            targets_from_pool = True
            if not targets:
                fallback_addresses = (
                    store.list_active_subscription_addresses(limit=trader_limit)
                    if delivery_only_subscribed
                    else store.list_monitored_addresses(limit=trader_limit)
                )
                targets = [
                    MonitoringTarget(
                        address=address,
                        tier=TIER_HOT,
                        poll_interval_seconds=max(60, lookback_minutes * 60),
                        lookback_minutes=lookback_minutes,
                    )
                    for address in fallback_addresses
                ]
                targets_from_pool = False
            if not targets:
                return []

            signals: list[TradeSignal] = []
            last_fill_updates: list[tuple[str, int]] = []
            polled_targets = []
            seen_ids: set[str] = set()

            now_ts = datetime.now(tz=UTC).timestamp()
            for target in targets:
                trader = target.address
                dynamic_lookback = max(lookback_minutes, int(target.lookback_minutes or lookback_minutes))
                start_ms = int((now_ts - (dynamic_lookback * 60)) * 1000)

                try:
                    fills = await self._info(
                        {
                            "type": "userFillsByTime",
                            "user": trader,
                            "startTime": start_ms,
                        }
                    )
                except Exception:
                    polled_targets.append(target)
                    continue
                if not isinstance(fills, list):
                    polled_targets.append(target)
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
                            trader_address=trader,
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
                polled_targets.append(target)

            if last_fill_updates:
                store.touch_last_fill_times(last_fill_updates)
            if polled_targets and targets_from_pool:
                store.mark_monitoring_targets_polled(targets=polled_targets)

        return signals
