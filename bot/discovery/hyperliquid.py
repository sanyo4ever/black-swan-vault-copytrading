from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aiohttp

from bot.trader_store import TraderStore


MS_HOUR = 3_600_000
MS_DAY = 86_400_000


@dataclass(frozen=True)
class HyperliquidDiscoveryConfig:
    info_url: str = "https://api.hyperliquid.xyz/info"
    candidate_limit: int = 60
    min_age_days: int = 30
    min_trades_30d: int = 10
    min_active_days_30d: int = 4
    min_trades_7d: int = 1
    window_hours: int = 24
    concurrency: int = 6


class HyperliquidDiscoveryService:
    def __init__(
        self,
        *,
        http_session: aiohttp.ClientSession,
        store: TraderStore,
        config: HyperliquidDiscoveryConfig,
    ) -> None:
        self._http_session = http_session
        self._store = store
        self._config = config

    async def _info(self, payload: dict[str, Any]) -> Any:
        attempts = 5
        backoff = 1.0
        last_error: Exception | None = None

        for attempt in range(attempts):
            try:
                async with self._http_session.post(self._config.info_url, json=payload) as response:
                    if response.status == 429 and attempt < attempts - 1:
                        retry_after_raw = response.headers.get("Retry-After", "").strip()
                        retry_after = float(retry_after_raw) if retry_after_raw else 0.0
                        await asyncio.sleep(max(backoff, retry_after))
                        backoff = min(backoff * 2, 12.0)
                        continue

                    response.raise_for_status()
                    return await response.json(content_type=None)
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                if exc.status in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 12.0)
                    continue
                raise
            except aiohttp.ClientError as exc:
                last_error = exc
                if attempt < attempts - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 12.0)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Unexpected _info() failure without error")

    @staticmethod
    def _to_float(raw: Any) -> float:
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _to_int(raw: Any) -> int:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    async def _fetch_candidates(self) -> list[dict[str, Any]]:
        try:
            data = await self._info({"type": "vaultSummaries"})
        except Exception:
            data = []
        if not isinstance(data, list):
            data = []

        prepared: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            leader = str(item.get("leader", "")).strip().lower()
            if not leader:
                continue
            prepared.append(
                {
                    "address": leader,
                    "label": str(item.get("name", "")).strip() or None,
                    "source": "hyperliquid_vault_leader",
                    "vault_tvl": self._to_float(item.get("tvl")),
                }
            )

        prepared.sort(key=lambda item: item["vault_tvl"], reverse=True)

        dedup: dict[str, dict[str, Any]] = {}
        for item in prepared:
            dedup[item["address"]] = item
        vault_candidates = list(dedup.values())[: self._config.candidate_limit]
        if vault_candidates:
            return vault_candidates

        return await self._fetch_recent_trade_candidates()

    async def _fetch_recent_trade_candidates(self) -> list[dict[str, Any]]:
        meta = await self._info({"type": "meta"})
        if not isinstance(meta, dict):
            return []

        universe = meta.get("universe")
        if not isinstance(universe, list):
            return []

        coins: list[str] = []
        for item in universe:
            if not isinstance(item, dict):
                continue
            coin = str(item.get("name", "")).strip()
            if coin:
                coins.append(coin)
        if not coins:
            return []

        coin_limit = min(max(self._config.candidate_limit, 15), len(coins))
        selected = coins[:coin_limit]
        stats: dict[str, dict[str, float]] = {}
        sem = asyncio.Semaphore(max(1, self._config.concurrency))

        async def scan_coin(coin: str) -> None:
            async with sem:
                trades = await self._info({"type": "recentTrades", "coin": coin})
            if not isinstance(trades, list):
                return

            for trade in trades:
                if not isinstance(trade, dict):
                    continue
                users = trade.get("users")
                if not isinstance(users, list):
                    continue

                px = self._to_float(trade.get("px"))
                sz = self._to_float(trade.get("sz"))
                notional = abs(px * sz) if px > 0 and sz > 0 else 0.0
                ts = self._to_int(trade.get("time"))

                for user in users:
                    address = str(user).strip().lower()
                    if not address.startswith("0x") or len(address) < 10:
                        continue
                    item = stats.setdefault(
                        address,
                        {"trade_count": 0.0, "volume": 0.0, "last_time": 0.0},
                    )
                    item["trade_count"] += 1.0
                    item["volume"] += notional
                    if ts > item["last_time"]:
                        item["last_time"] = float(ts)

        await asyncio.gather(*(scan_coin(coin) for coin in selected), return_exceptions=True)

        ranked = sorted(
            stats.items(),
            key=lambda kv: (
                kv[1]["trade_count"],
                kv[1]["volume"],
                kv[1]["last_time"],
            ),
            reverse=True,
        )

        candidates: list[dict[str, Any]] = []
        for address, item in ranked[: self._config.candidate_limit]:
            candidates.append(
                {
                    "address": address,
                    "label": None,
                    "source": "hyperliquid_recent_trades",
                    "vault_tvl": item["volume"],
                }
            )
        return candidates

    async def _fetch_metrics(self, candidate: dict[str, Any], *, now_ms: int) -> dict[str, Any]:
        start_60d = now_ms - (60 * MS_DAY)
        cut_30d = now_ms - (30 * MS_DAY)
        cut_7d = now_ms - (7 * MS_DAY)
        cut_24h = now_ms - MS_DAY

        fills = await self._info(
            {
                "type": "userFillsByTime",
                "user": candidate["address"],
                "startTime": start_60d,
            }
        )
        if not isinstance(fills, list):
            fills = []

        normalized: list[dict[str, Any]] = []
        for item in fills:
            if not isinstance(item, dict):
                continue
            ts = self._to_int(item.get("time"))
            if ts <= 0:
                continue
            normalized.append(item)

        normalized.sort(key=lambda item: self._to_int(item.get("time")))

        fills_24h = [item for item in normalized if self._to_int(item.get("time")) >= cut_24h]
        fills_7d = [item for item in normalized if self._to_int(item.get("time")) >= cut_7d]
        fills_30d = [item for item in normalized if self._to_int(item.get("time")) >= cut_30d]

        trades_24h = len(fills_24h)
        trades_7d = len(fills_7d)
        trades_30d = len(fills_30d)

        active_hours_24h = len(
            {
                self._to_int(item.get("time")) // MS_HOUR
                for item in fills_24h
            }
        )
        active_days_30d = len(
            {
                self._to_int(item.get("time")) // MS_DAY
                for item in fills_30d
            }
        )

        first_fill_time = self._to_int(normalized[0].get("time")) if normalized else None
        last_fill_time = self._to_int(normalized[-1].get("time")) if normalized else None
        age_days = ((now_ms - first_fill_time) / MS_DAY) if first_fill_time else None

        notionals: list[float] = []
        closed_pnls: list[float] = []
        fees_30d = 0.0
        long_count = 0
        for item in fills_30d:
            px = self._to_float(item.get("px"))
            sz = self._to_float(item.get("sz"))
            if px > 0 and sz > 0:
                notionals.append(abs(px * sz))

            closed_pnl = self._to_float(item.get("closedPnl"))
            closed_pnls.append(closed_pnl)
            fees_30d += abs(self._to_float(item.get("fee")))

            direction = str(item.get("dir", "")).lower()
            side = str(item.get("side", "")).upper()
            if "long" in direction or side == "B":
                long_count += 1

        volume_usd_30d = sum(notionals)
        realized_pnl_30d = sum(closed_pnls)

        wins = sum(1 for value in closed_pnls if value > 0)
        losses = sum(1 for value in closed_pnls if value < 0)
        win_rate_30d = (wins / (wins + losses)) if (wins + losses) else None

        long_ratio_30d = (long_count / trades_30d) if trades_30d else None
        avg_notional_30d = (volume_usd_30d / trades_30d) if trades_30d else None
        max_notional_30d = max(notionals) if notionals else None

        clearinghouse_state = await self._info(
            {
                "type": "clearinghouseState",
                "user": candidate["address"],
            }
        )
        margin_summary = {}
        if isinstance(clearinghouse_state, dict):
            raw_margin = clearinghouse_state.get("marginSummary")
            if isinstance(raw_margin, dict):
                margin_summary = raw_margin

        account_value = self._to_float(margin_summary.get("accountValue")) if margin_summary else None
        total_ntl_pos = self._to_float(margin_summary.get("totalNtlPos")) if margin_summary else None
        total_margin_used = self._to_float(margin_summary.get("totalMarginUsed")) if margin_summary else None

        consistency_component = min(1.0, active_days_30d / 30.0) * 25.0
        frequency_component = min(1.0, trades_30d / 120.0) * 20.0
        win_component = ((win_rate_30d if win_rate_30d is not None else 0.5) * 15.0)
        pnl_component = max(-10.0, min(20.0, realized_pnl_30d / 1000.0))
        age_component = min(1.0, (age_days or 0.0) / 180.0) * 10.0
        volume_component = min(1.0, volume_usd_30d / 2_000_000.0) * 20.0
        fee_penalty = min(8.0, fees_30d / 5000.0)
        score = max(
            0.0,
            consistency_component
            + frequency_component
            + win_component
            + pnl_component
            + age_component
            + volume_component
            - fee_penalty,
        )

        stats_payload = {
            "vault_tvl": candidate["vault_tvl"],
            "wins_30d": wins,
            "losses_30d": losses,
            "score_components": {
                "consistency": round(consistency_component, 4),
                "frequency": round(frequency_component, 4),
                "win": round(win_component, 4),
                "pnl": round(pnl_component, 4),
                "age": round(age_component, 4),
                "volume": round(volume_component, 4),
                "fee_penalty": round(fee_penalty, 4),
            },
            "has_month_history": bool(age_days is not None and age_days >= self._config.min_age_days),
        }

        return {
            "address": candidate["address"],
            "label": candidate["label"],
            "source": candidate["source"],
            "trades_24h": trades_24h,
            "active_hours_24h": active_hours_24h,
            "trades_7d": trades_7d,
            "trades_30d": trades_30d,
            "active_days_30d": active_days_30d,
            "first_fill_time": first_fill_time,
            "last_fill_time": last_fill_time,
            "age_days": age_days,
            "volume_usd_30d": volume_usd_30d,
            "realized_pnl_30d": realized_pnl_30d,
            "fees_30d": fees_30d,
            "win_rate_30d": win_rate_30d,
            "long_ratio_30d": long_ratio_30d,
            "avg_notional_30d": avg_notional_30d,
            "max_notional_30d": max_notional_30d,
            "account_value": account_value,
            "total_ntl_pos": total_ntl_pos,
            "total_margin_used": total_margin_used,
            "score": round(score, 4),
            "stats_json": json.dumps(stats_payload, ensure_ascii=True, separators=(",", ":")),
        }

    async def discover(self) -> dict[str, Any]:
        discovery_source = "hyperliquid_vault_leader_scan"
        try:
            candidates = await self._fetch_candidates()
            if not candidates:
                summary = {
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                    "candidates": 0,
                    "qualified": 0,
                    "upserted": 0,
                    "pruned": 0,
                }
                self._store.log_discovery_run(
                    source=discovery_source,
                    status="ok",
                    candidates=0,
                    qualified=0,
                    upserted=0,
                )
                return summary

            now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
            sem = asyncio.Semaphore(self._config.concurrency)

            async def gather_metrics(candidate: dict[str, Any]) -> dict[str, Any] | None:
                async with sem:
                    try:
                        return await self._fetch_metrics(candidate, now_ms=now_ms)
                    except Exception:
                        return None

            metrics = await asyncio.gather(*(gather_metrics(c) for c in candidates))

            qualified = 0
            upserted = 0
            qualified_by_source: dict[str, set[str]] = {}
            for item in metrics:
                if not item:
                    continue

                age_days = item.get("age_days")
                if age_days is None or age_days < self._config.min_age_days:
                    continue
                if item["trades_30d"] < self._config.min_trades_30d:
                    continue
                if item["active_days_30d"] < self._config.min_active_days_30d:
                    continue
                if item["trades_7d"] < self._config.min_trades_7d:
                    continue

                qualified += 1
                self._store.upsert_discovered(
                    address=item["address"],
                    label=item["label"],
                    source=item["source"],
                    trades_24h=item["trades_24h"],
                    active_hours_24h=item["active_hours_24h"],
                    trades_7d=item["trades_7d"],
                    trades_30d=item["trades_30d"],
                    active_days_30d=item["active_days_30d"],
                    first_fill_time=item["first_fill_time"],
                    last_fill_time=item["last_fill_time"],
                    age_days=item["age_days"],
                    volume_usd_30d=item["volume_usd_30d"],
                    realized_pnl_30d=item["realized_pnl_30d"],
                    fees_30d=item["fees_30d"],
                    win_rate_30d=item["win_rate_30d"],
                    long_ratio_30d=item["long_ratio_30d"],
                    avg_notional_30d=item["avg_notional_30d"],
                    max_notional_30d=item["max_notional_30d"],
                    account_value=item["account_value"],
                    total_ntl_pos=item["total_ntl_pos"],
                    total_margin_used=item["total_margin_used"],
                    score=item["score"],
                    stats_json=item["stats_json"],
                )
                upserted += 1
                source = str(item["source"]).strip()
                if source:
                    qualified_by_source.setdefault(source, set()).add(item["address"])

            pruned = 0
            candidate_sources = {
                str(candidate.get("source", "")).strip()
                for candidate in candidates
                if str(candidate.get("source", "")).strip()
            }
            for source in sorted(candidate_sources):
                pruned += self._store.prune_auto_discovered(
                    source=source,
                    keep_addresses=qualified_by_source.get(source, set()),
                )

            self._store.log_discovery_run(
                source=discovery_source,
                status="ok",
                candidates=len(candidates),
                qualified=qualified,
                upserted=upserted,
            )
            return {
                "timestamp": datetime.now(tz=UTC).isoformat(),
                "candidates": len(candidates),
                "qualified": qualified,
                "upserted": upserted,
                "pruned": pruned,
            }
        except Exception as exc:
            self._store.log_discovery_run(
                source=discovery_source,
                status="error",
                candidates=0,
                qualified=0,
                upserted=0,
                error_message=str(exc)[:1000],
            )
            raise
