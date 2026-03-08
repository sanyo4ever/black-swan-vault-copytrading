from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aiohttp

from bot.models import TradeSignal
from bot.sources.base import Source
from bot.trader_store import DeliveryMonitorTarget, MonitoringTarget, TraderStore, TIER_HOT


@dataclass(frozen=True)
class _PollOutcome:
    address: str
    fills: tuple[dict[str, Any], ...]
    watermark_fill_time: int | None
    newest_observed_fill_time: int | None
    dropped_fills: int
    error: str | None
    delivery_target: DeliveryMonitorTarget | None
    legacy_target: MonitoringTarget | None


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
        self._logger = logging.getLogger("cryptoinsider.source.hyperliquid-futures")

    async def _info(self, payload: dict[str, Any]) -> Any:
        async with self.http_session.post(self._info_url, json=payload) as response:
            response.raise_for_status()
            return await response.json(content_type=None)

    async def _fetch_fills_with_retry(self, *, trader: str, start_ms: int) -> list[dict[str, Any]]:
        attempts = 4
        backoff = 0.7
        payload = {
            "type": "userFillsByTime",
            "user": trader,
            "startTime": int(max(0, start_ms)),
        }
        for attempt in range(attempts):
            try:
                async with self.http_session.post(self._info_url, json=payload) as response:
                    if response.status in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                        retry_after_raw = str(response.headers.get("Retry-After", "")).strip()
                        retry_after = int(retry_after_raw) if retry_after_raw.isdigit() else 0
                        await asyncio.sleep(max(backoff, float(retry_after)))
                        backoff = min(8.0, backoff * 2.0)
                        continue

                    response.raise_for_status()
                    data = await response.json(content_type=None)
                    if isinstance(data, list):
                        return [item for item in data if isinstance(item, dict)]
                    return []
            except aiohttp.ClientError as exc:
                if attempt >= attempts - 1:
                    raise
                self._logger.warning(
                    "Retrying userFillsByTime trader=%s attempt=%s error=%s",
                    trader,
                    attempt + 1,
                    exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(8.0, backoff * 2.0)
        return []

    @staticmethod
    def _build_side(fill: dict[str, Any]) -> str | None:
        # Exchange-native side code (B/A) is the most precise action signal.
        side = str(fill.get("side", "")).upper()
        if side == "B":
            return "BUY"
        if side == "A":
            return "SELL"

        direction = str(fill.get("dir", "")).lower()
        if "long" in direction:
            return "BUY"
        if "short" in direction:
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

    @staticmethod
    def _to_int(raw: Any) -> int:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def _resolve_delivery_next_poll_seconds(
        self,
        *,
        target: DeliveryMonitorTarget,
        had_new_fill: bool,
        error: str | None,
        min_poll_seconds: int,
        max_poll_seconds: int,
    ) -> int:
        base_poll = max(min_poll_seconds, int(target.poll_interval_seconds or min_poll_seconds))
        if error:
            error_factor = 2 ** min(4, max(1, target.consecutive_errors + 1))
            return max(min_poll_seconds, min(max_poll_seconds, int(base_poll * error_factor)))

        if had_new_fill:
            demand_boost = min(0.35, max(0.0, float(target.priority_score)) / 100.0 * 0.35)
            next_poll = int(base_poll * (0.75 - demand_boost))
            return max(min_poll_seconds, min(max_poll_seconds, next_poll))

        idle_factor = 1.0 + min(2.5, (max(0, int(target.idle_cycles)) + 1) * 0.2)
        next_poll = int(base_poll * idle_factor)
        return max(min_poll_seconds, min(max_poll_seconds, next_poll))

    def _build_signal(self, *, fill: dict[str, Any], trader: str, seen_ids: set[str]) -> TradeSignal | None:
        fill_time = self._to_int(fill.get("time"))
        if fill_time <= 0:
            return None

        tx_hash = str(fill.get("hash", "")).strip()
        oid = str(fill.get("oid", "")).strip()
        external_id = (
            f"{tx_hash}:{oid or fill_time}"
            if tx_hash
            else f"{trader}:{oid}:{fill_time}"
        )
        if external_id in seen_ids:
            return None
        seen_ids.add(external_id)

        return TradeSignal(
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

    async def _poll_delivery_target(
        self,
        *,
        target: DeliveryMonitorTarget,
        max_fills_per_trader: int,
    ) -> _PollOutcome:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        watermark = int(target.last_seen_fill_time or 0)
        if watermark > 0:
            start_ms = max(0, watermark - (max(15, target.safety_lookback_seconds) * 1000))
        else:
            bootstrap_minutes = max(5, int(target.bootstrap_lookback_minutes))
            start_ms = max(0, now_ms - (bootstrap_minutes * 60 * 1000))

        try:
            fills = await self._fetch_fills_with_retry(trader=target.address, start_ms=start_ms)
        except Exception as exc:
            return _PollOutcome(
                address=target.address,
                fills=(),
                watermark_fill_time=None,
                newest_observed_fill_time=None,
                dropped_fills=0,
                error=str(exc),
                delivery_target=target,
                legacy_target=None,
            )

        newest_observed_fill = None
        fresh: list[dict[str, Any]] = []
        for item in fills:
            fill_time = self._to_int(item.get("time"))
            if fill_time <= 0:
                continue
            if newest_observed_fill is None or fill_time > newest_observed_fill:
                newest_observed_fill = fill_time
            if watermark > 0 and fill_time <= watermark:
                continue
            fresh.append(item)

        fresh.sort(key=lambda row: self._to_int(row.get("time")))
        limit = max(1, int(max_fills_per_trader))
        dropped_fills = max(0, len(fresh) - limit)
        if dropped_fills > 0:
            # Bootstrap starts from the most recent slice to avoid replaying very old history.
            # Regular cycles process oldest-first chunks to prevent watermark skips under load.
            selected = fresh[-limit:] if watermark <= 0 else fresh[:limit]
        else:
            selected = fresh

        watermark_fill_time = None
        if selected:
            watermark_fill_time = max(self._to_int(item.get("time")) for item in selected)
        return _PollOutcome(
            address=target.address,
            fills=tuple(selected),
            watermark_fill_time=watermark_fill_time,
            newest_observed_fill_time=newest_observed_fill,
            dropped_fills=dropped_fills,
            error=None,
            delivery_target=target,
            legacy_target=None,
        )

    async def _poll_legacy_target(
        self,
        *,
        target: MonitoringTarget,
        default_lookback_minutes: int,
        max_fills_per_trader: int,
    ) -> _PollOutcome:
        now_ts = datetime.now(tz=UTC).timestamp()
        dynamic_lookback = max(default_lookback_minutes, int(target.lookback_minutes or default_lookback_minutes))
        start_ms = int((now_ts - (dynamic_lookback * 60)) * 1000)
        try:
            fills_raw = await self._fetch_fills_with_retry(trader=target.address, start_ms=start_ms)
        except Exception as exc:
            return _PollOutcome(
                address=target.address,
                fills=(),
                watermark_fill_time=None,
                newest_observed_fill_time=None,
                dropped_fills=0,
                error=str(exc),
                delivery_target=None,
                legacy_target=target,
            )
        fills_sorted = sorted(
            fills_raw,
            key=lambda row: self._to_int(row.get("time")),
            reverse=True,
        )
        newest = self._to_int(fills_sorted[0].get("time")) if fills_sorted else None
        selected = tuple(fills_sorted[: max(1, int(max_fills_per_trader))])
        watermark_fill_time = None
        if selected:
            watermark_fill_time = max(self._to_int(item.get("time")) for item in selected)
        return _PollOutcome(
            address=target.address,
            fills=selected,
            watermark_fill_time=watermark_fill_time,
            newest_observed_fill_time=(newest if newest and newest > 0 else None),
            dropped_fills=max(0, len(fills_sorted) - len(selected)),
            error=None,
            delivery_target=None,
            legacy_target=target,
        )

    async def fetch_signals(self) -> list[TradeSignal]:
        if not self.enabled:
            return []

        max_fills_per_trader = int(self.config.get("max_fills_per_trader", 5))
        lookback_minutes = int(self.config.get("lookback_minutes", 90))
        delivery_only_subscribed = bool(self.config.get("delivery_only_subscribed", True))
        showcase_mode = bool(self.config.get("showcase_mode", False))

        max_traders_per_cycle = int(self.config.get("max_traders_per_cycle", self.config.get("trader_limit", 120)))
        base_poll_seconds = int(self.config.get("base_poll_seconds", 60))
        min_poll_seconds = int(self.config.get("min_poll_seconds", 20))
        max_poll_seconds = int(self.config.get("max_poll_seconds", 180))
        priority_recency_minutes = int(self.config.get("priority_recency_minutes", 120))
        safety_lookback_seconds = int(self.config.get("safety_lookback_seconds", 90))
        bootstrap_lookback_minutes = int(self.config.get("bootstrap_lookback_minutes", 180))
        http_concurrency = int(self.config.get("http_concurrency", 8))

        with TraderStore(self._database_dsn) as store:
            delivery_targets: list[DeliveryMonitorTarget] = []
            legacy_targets: list[MonitoringTarget] = []
            used_delivery_state = False
            if delivery_only_subscribed:
                monitor_stats = store.refresh_delivery_monitor_state(
                    base_poll_seconds=base_poll_seconds,
                    min_poll_seconds=min_poll_seconds,
                    max_poll_seconds=max_poll_seconds,
                    priority_recency_minutes=priority_recency_minutes,
                    safety_lookback_seconds=safety_lookback_seconds,
                    bootstrap_lookback_minutes=bootstrap_lookback_minutes,
                    max_targets_per_cycle=max_traders_per_cycle,
                )
                self._logger.info(
                    "Delivery monitor state refreshed total=%s high_demand=%s over_capacity=%s",
                    monitor_stats["total"],
                    monitor_stats["high_demand"],
                    monitor_stats["over_capacity"],
                )
                delivery_targets = store.list_due_delivery_monitor_targets(limit=max_traders_per_cycle)
                used_delivery_state = True
            else:
                legacy_targets = store.list_due_monitoring_targets(
                    limit=max_traders_per_cycle,
                    only_subscribed=False,
                )
                if not legacy_targets:
                    fallback_addresses = store.list_active_addresses(
                        limit=max_traders_per_cycle,
                        showcase_only=showcase_mode,
                    )
                    legacy_targets = [
                        MonitoringTarget(
                            address=address,
                            tier=TIER_HOT,
                            poll_interval_seconds=max(60, lookback_minutes * 60),
                            lookback_minutes=lookback_minutes,
                        )
                        for address in fallback_addresses
                    ]

            if not delivery_targets and not legacy_targets:
                return []

            sem = asyncio.Semaphore(max(1, http_concurrency))

            async def run_delivery(target: DeliveryMonitorTarget) -> _PollOutcome:
                async with sem:
                    return await self._poll_delivery_target(
                        target=target,
                        max_fills_per_trader=max_fills_per_trader,
                    )

            async def run_legacy(target: MonitoringTarget) -> _PollOutcome:
                async with sem:
                    return await self._poll_legacy_target(
                        target=target,
                        default_lookback_minutes=lookback_minutes,
                        max_fills_per_trader=max_fills_per_trader,
                    )

            coroutines: list[Any] = []
            coroutines.extend(run_delivery(target) for target in delivery_targets)
            coroutines.extend(run_legacy(target) for target in legacy_targets)
            outcomes_raw = await asyncio.gather(*coroutines, return_exceptions=True)

            outcomes: list[_PollOutcome] = []
            for item in outcomes_raw:
                if isinstance(item, Exception):
                    self._logger.warning("Unexpected polling task failure: %s", item)
                    continue
                outcomes.append(item)

            signals: list[TradeSignal] = []
            last_fill_updates: list[tuple[str, int]] = []
            seen_ids: set[str] = set()

            for outcome in outcomes:
                if outcome.newest_observed_fill_time:
                    last_fill_updates.append((outcome.address, outcome.newest_observed_fill_time))
                if outcome.dropped_fills > 0:
                    self._logger.warning(
                        "Signal backlog trader=%s selected=%s dropped=%s",
                        outcome.address,
                        len(outcome.fills),
                        outcome.dropped_fills,
                    )

                for fill in outcome.fills:
                    signal = self._build_signal(fill=fill, trader=outcome.address, seen_ids=seen_ids)
                    if signal is not None:
                        signals.append(signal)

                if outcome.delivery_target is not None:
                    next_poll_seconds = self._resolve_delivery_next_poll_seconds(
                        target=outcome.delivery_target,
                        had_new_fill=bool(outcome.fills),
                        error=outcome.error,
                        min_poll_seconds=min_poll_seconds,
                        max_poll_seconds=max_poll_seconds,
                    )
                    store.mark_delivery_monitor_polled(
                        address=outcome.address,
                        next_poll_seconds=next_poll_seconds,
                        newest_fill_time=outcome.watermark_fill_time,
                        had_new_fill=bool(outcome.fills),
                        error=outcome.error,
                    )

            if last_fill_updates:
                store.touch_last_fill_times(last_fill_updates)

            if legacy_targets and not used_delivery_state:
                successful_legacy = [
                    item.legacy_target
                    for item in outcomes
                    if item.legacy_target is not None
                ]
                store.mark_monitoring_targets_polled(
                    targets=[target for target in successful_legacy if target is not None]
                )

        return signals
