from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
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
    min_trades_30d: int = 120
    min_active_days_30d: int = 12
    min_win_rate_30d: float = 0.52
    max_drawdown_30d_pct: float = 25.0
    max_last_activity_minutes: int = 60
    min_realized_pnl_30d: float = 0.0
    require_positive_pnl_30d: bool = True
    min_trades_7d: int = 1
    window_hours: int = 24
    concurrency: int = 6
    fill_cap_hint: int = 1900
    age_probe_enabled: bool = True
    seed_addresses: tuple[str, ...] = ()
    nansen_api_url: str = "https://api.nansen.ai"
    nansen_api_key: str = ""
    nansen_candidate_limit: int = 60


class HyperliquidDiscoveryService:
    def __init__(
        self,
        *,
        http_session: aiohttp.ClientSession,
        store: TraderStore,
        config: HyperliquidDiscoveryConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        self._http_session = http_session
        self._store = store
        self._config = config
        self._logger = logger or logging.getLogger("cryptoinsider.discovery")
        self._rate_limit_counts: dict[str, int] = {}
        self._rate_limit_max_retry_after: dict[str, float] = {}
        self._response_retry_counts: dict[str, int] = {}
        self._client_retry_counts: dict[str, int] = {}

    def _reset_retry_stats(self) -> None:
        self._rate_limit_counts.clear()
        self._rate_limit_max_retry_after.clear()
        self._response_retry_counts.clear()
        self._client_retry_counts.clear()

    def _record_rate_limit(self, *, payload_type: str, retry_after: float) -> None:
        key = payload_type or "unknown"
        self._rate_limit_counts[key] = self._rate_limit_counts.get(key, 0) + 1
        current_max = self._rate_limit_max_retry_after.get(key, 0.0)
        self._rate_limit_max_retry_after[key] = max(current_max, retry_after)

    def _record_response_retry(self, *, payload_type: str, status: int) -> None:
        key = f"{payload_type or 'unknown'}:{status}"
        self._response_retry_counts[key] = self._response_retry_counts.get(key, 0) + 1

    def _record_client_retry(self, *, payload_type: str, error_name: str) -> None:
        key = f"{payload_type or 'unknown'}:{error_name or 'ClientError'}"
        self._client_retry_counts[key] = self._client_retry_counts.get(key, 0) + 1

    @staticmethod
    def _parse_retry_after(raw: str) -> float:
        value = str(raw or "").strip()
        if not value:
            return 0.0
        try:
            parsed = float(value)
            return max(0.0, parsed)
        except (TypeError, ValueError):
            pass
        try:
            ts = parsedate_to_datetime(value)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            delta = (ts - datetime.now(tz=UTC)).total_seconds()
            return max(0.0, delta)
        except Exception:
            return 0.0

    def _log_retry_summary(self) -> None:
        rate_total = sum(self._rate_limit_counts.values())
        response_total = sum(self._response_retry_counts.values())
        client_total = sum(self._client_retry_counts.values())
        if rate_total <= 0 and response_total <= 0 and client_total <= 0:
            return

        max_retry_after = max(self._rate_limit_max_retry_after.values(), default=0.0)
        level = logging.WARNING if rate_total >= 50 else logging.INFO
        self._logger.log(
            level,
            "Hyperliquid retry summary rate_limits=%s response_retries=%s client_retries=%s rate_limit_breakdown=%s response_breakdown=%s client_breakdown=%s max_retry_after=%s",
            rate_total,
            response_total,
            client_total,
            self._rate_limit_counts,
            self._response_retry_counts,
            self._client_retry_counts,
            round(max_retry_after, 3),
        )

    def _merge_with_existing_tracked(
        self, candidates: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            address = str(candidate.get("address", "")).strip().lower()
            if not address:
                continue
            merged[address] = {
                "address": address,
                "label": candidate.get("label"),
                "source": candidate.get("source") or "hyperliquid_recent_trades",
                "vault_tvl": float(candidate.get("vault_tvl") or 0.0),
            }

        for seed in self._config.seed_addresses:
            normalized_seed = str(seed).strip().lower()
            if not normalized_seed.startswith("0x") or len(normalized_seed) < 10:
                continue
            merged.setdefault(
                normalized_seed,
                {
                    "address": normalized_seed,
                    "label": None,
                    "source": "manual_seed",
                    "vault_tvl": 0.0,
                },
            )

        tracked = self._store.list_traders(limit=max(self._config.candidate_limit * 4, 200))
        tracked_added = 0
        for trader in tracked:
            address = str(trader.address).strip().lower()
            if not address or address in merged:
                continue
            if len(merged) >= self._config.candidate_limit:
                break
            merged[address] = {
                "address": address,
                "label": trader.label,
                "source": trader.source,
                "vault_tvl": 0.0,
            }
            tracked_added += 1
        if tracked_added > 0:
            self._logger.info(
                "Discovery candidate expansion added_tracked=%s total=%s",
                tracked_added,
                len(merged),
            )
        ranked = sorted(
            merged.values(),
            key=lambda item: float(item.get("vault_tvl") or 0.0),
            reverse=True,
        )
        return ranked[: self._config.candidate_limit]

    async def _info(self, payload: dict[str, Any]) -> Any:
        attempts = 5
        backoff = 1.0
        last_error: Exception | None = None
        payload_type = str(payload.get("type") or "unknown")

        for attempt in range(attempts):
            try:
                async with self._http_session.post(self._config.info_url, json=payload) as response:
                    if response.status == 429 and attempt < attempts - 1:
                        retry_after_raw = response.headers.get("Retry-After", "").strip()
                        retry_after = self._parse_retry_after(retry_after_raw)
                        self._record_rate_limit(payload_type=payload_type, retry_after=retry_after)
                        self._logger.debug(
                            "Hyperliquid rate limit payload_type=%s attempt=%s retry_after=%s",
                            payload_type,
                            attempt + 1,
                            retry_after,
                        )
                        await asyncio.sleep(max(backoff, retry_after))
                        backoff = min(backoff * 2, 12.0)
                        continue

                    response.raise_for_status()
                    return await response.json(content_type=None)
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                if exc.status in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                    self._record_response_retry(payload_type=payload_type, status=exc.status)
                    self._logger.debug(
                        "Hyperliquid response error payload_type=%s attempt=%s status=%s",
                        payload_type,
                        attempt + 1,
                        exc.status,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 12.0)
                    continue
                raise
            except aiohttp.ClientError as exc:
                last_error = exc
                if attempt < attempts - 1:
                    self._record_client_retry(
                        payload_type=payload_type,
                        error_name=exc.__class__.__name__,
                    )
                    self._logger.debug(
                        "Hyperliquid client error payload_type=%s attempt=%s error=%s",
                        payload_type,
                        attempt + 1,
                        exc,
                    )
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

    @staticmethod
    def _population_std(values: list[float]) -> float:
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        return math.sqrt(max(0.0, variance))

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _trade_key(self, item: dict[str, Any]) -> str:
        oid = item.get("oid")
        if oid is not None and str(oid).strip():
            return f"oid:{str(oid).strip()}"
        tid = item.get("tid")
        if tid is not None and str(tid).strip():
            return f"tid:{str(tid).strip()}"
        coin = str(item.get("coin", "") or item.get("symbol", "")).strip().upper()
        direction = str(item.get("dir", "")).strip().lower()
        side = str(item.get("side", "")).strip().upper()
        ts_bucket = self._to_int(item.get("time")) // 1000
        px = round(self._to_float(item.get("px")), 8)
        sz = round(abs(self._to_float(item.get("sz"))), 8)
        return f"synthetic:{coin}:{direction}:{side}:{ts_bucket}:{px}:{sz}"

    def _aggregate_trades(self, fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for item in fills:
            if not isinstance(item, dict):
                continue
            key = self._trade_key(item)
            bucket = grouped.setdefault(
                key,
                {
                    "notional": 0.0,
                    "closed_pnl": 0.0,
                    "direction": "",
                    "first_time": 0,
                    "last_time": 0,
                },
            )
            ts = self._to_int(item.get("time"))
            if bucket["first_time"] <= 0 or (ts > 0 and ts < int(bucket["first_time"])):
                bucket["first_time"] = ts
            if ts > int(bucket["last_time"]):
                bucket["last_time"] = ts

            px = self._to_float(item.get("px"))
            sz = self._to_float(item.get("sz"))
            notional = abs(px * sz) if px > 0 and sz > 0 else 0.0
            if notional > 0:
                bucket["notional"] = float(bucket["notional"]) + notional

            closed_pnl = self._to_float(item.get("closedPnl"))
            if abs(closed_pnl) > 1e-12:
                bucket["closed_pnl"] = float(bucket["closed_pnl"]) + closed_pnl

            direction = str(item.get("dir", "")).strip().lower()
            if direction:
                bucket["direction"] = direction

        aggregated = list(grouped.values())
        aggregated.sort(key=lambda trade: int(trade.get("last_time") or 0))
        return aggregated

    def _compute_period_stats(
        self,
        *,
        fills: list[dict[str, Any]],
        account_value: float | None,
    ) -> dict[str, float | int | None]:
        trades = self._aggregate_trades(fills)
        notionals = [
            float(item.get("notional") or 0.0)
            for item in trades
            if float(item.get("notional") or 0.0) > 0.0
        ]
        closed_trades = [
            item
            for item in trades
            if abs(float(item.get("closed_pnl") or 0.0)) > 1e-12
        ]
        closed_pnls = [float(item.get("closed_pnl") or 0.0) for item in closed_trades]
        realized_pnl = sum(closed_pnls)

        returns: list[float] = []
        for item in closed_trades:
            notional = float(item.get("notional") or 0.0)
            closed_pnl = float(item.get("closed_pnl") or 0.0)
            if notional <= 1e-12:
                continue
            trade_return = closed_pnl / notional
            trade_return = max(-0.99, min(10.0, trade_return))
            returns.append(trade_return)

        wins = sum(1 for value in closed_pnls if value > 0)
        losses = sum(1 for value in closed_pnls if value < 0)
        closed_count = wins + losses

        win_rate = (wins / closed_count) if closed_count else None
        total_profit = sum(value for value in closed_pnls if value > 0)
        total_loss_abs = abs(sum(value for value in closed_pnls if value < 0))
        profit_to_loss_ratio = (
            (total_profit / total_loss_abs) if total_loss_abs > 0 else None
        )
        avg_pnl_per_trade = (realized_pnl / closed_count) if closed_count else None

        roi_base = None
        if account_value is not None and abs(account_value) > 1e-9:
            start_equity_estimate = abs(account_value - realized_pnl)
            current_equity = abs(account_value)
            candidates = [value for value in (start_equity_estimate, current_equity) if value > 1e-9]
            roi_base = max(candidates) if candidates else None
        else:
            volume = sum(notionals)
            if volume > 1e-9:
                roi_base = max(volume / max(1, len(notionals)), 1.0)
        roi_pct = ((realized_pnl / roi_base) * 100.0) if roi_base else None

        equity_base = roi_base or max(1.0, sum(notionals) / max(1, len(notionals)))
        equity = equity_base
        peak_equity = equity_base
        max_drawdown = 0.0
        for trade_pnl in closed_pnls:
            equity += trade_pnl
            peak_equity = max(peak_equity, equity)
            if peak_equity > 0:
                drawdown = (peak_equity - equity) / peak_equity
                max_drawdown = max(max_drawdown, drawdown)

        mean_return = (sum(returns) / len(returns)) if returns else 0.0
        volatility = self._population_std(returns) if returns else 0.0
        roi_volatility_pct = (volatility * 100.0) if returns else None
        sharpe = (
            (mean_return / volatility) * math.sqrt(len(returns))
            if volatility > 1e-12
            else None
        )

        downside = [min(0.0, value) for value in returns]
        downside_vol = self._population_std(downside) if downside else 0.0
        sortino = (
            (mean_return / downside_vol) * math.sqrt(len(returns))
            if downside_vol > 1e-12
            else None
        )

        return {
            "trade_count": len(trades),
            "closed_trade_count": closed_count,
            "realized_pnl": realized_pnl,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
            "profit_to_loss_ratio": profit_to_loss_ratio,
            "avg_pnl_per_trade": avg_pnl_per_trade,
            "roi_pct": roi_pct,
            "max_drawdown_pct": (max_drawdown * 100.0) if returns else None,
            "sharpe": sharpe,
            "sortino": sortino,
            "roi_volatility_pct": roi_volatility_pct,
            "volume_usd": sum(notionals),
            "avg_notional": ((sum(notionals) / len(notionals)) if notionals else None),
            "max_notional": (max(notionals) if notionals else None),
        }

    async def _probe_account_first_activity_time(self, address: str) -> int | None:
        try:
            updates = await self._info(
                {
                    "type": "userNonFundingLedgerUpdates",
                    "user": address,
                    "startTime": 0,
                }
            )
        except Exception:
            return None
        if not isinstance(updates, list):
            return None

        times = [
            self._to_int(item.get("time"))
            for item in updates
            if isinstance(item, dict) and self._to_int(item.get("time")) > 0
        ]
        if not times:
            return None
        return min(times)

    async def _fetch_hyperliquid_candidates(self) -> list[dict[str, Any]]:
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
            dedup.setdefault(item["address"], item)
        vault_candidates = list(dedup.values())[: self._config.candidate_limit]
        if vault_candidates:
            self._logger.info("Using vault candidates count=%s", len(vault_candidates))
            return self._merge_with_existing_tracked(vault_candidates)

        self._logger.info("Vault candidates empty, fallback to recent trade candidates")
        fallback = await self._fetch_recent_trade_candidates()
        return self._merge_with_existing_tracked(fallback)

    def _extract_nansen_candidate(self, item: dict[str, Any]) -> dict[str, Any] | None:
        def _pick_address(payload: dict[str, Any]) -> str:
            candidate = ""
            for key in (
                "walletAddress",
                "wallet",
                "address",
                "traderAddress",
                "user",
                "owner",
                "wallet_address",
                "trader_address",
            ):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    candidate = value.strip()
                    break
            if not candidate:
                trader_obj = payload.get("trader")
                if isinstance(trader_obj, dict):
                    nested = trader_obj.get("address")
                    if isinstance(nested, str) and nested.strip():
                        candidate = nested.strip()
            return candidate.lower()

        address = _pick_address(item)
        if not address.startswith("0x") or len(address) < 10:
            return None

        label: str | None = None
        for key in ("label", "name", "traderName", "walletLabel"):
            raw_label = item.get(key)
            if isinstance(raw_label, str) and raw_label.strip():
                label = raw_label.strip()
                break

        score_hint = 0.0
        for key in ("notionalUsd", "volumeUsd", "valueUsd", "sizeUsd", "pnlUsd"):
            raw_value = item.get(key)
            if raw_value is None:
                continue
            score_hint = max(score_hint, abs(self._to_float(raw_value)))

        return {
            "address": address,
            "label": label,
            "source": "nansen_smart_money_perps",
            "vault_tvl": score_hint,
        }

    async def _fetch_nansen_candidates(self) -> list[dict[str, Any]]:
        api_key = str(self._config.nansen_api_key or "").strip()
        if not api_key:
            return []

        base_url = str(self._config.nansen_api_url or "").rstrip("/")
        if not base_url:
            return []
        endpoint = f"{base_url}/smart-money/perp-trades"

        headers = {"api-key": api_key}
        params = {
            "limit": max(1, int(self._config.nansen_candidate_limit)),
            "label": "Smart HL Perps Trader",
        }
        try:
            async with self._http_session.get(
                endpoint,
                headers=headers,
                params=params,
            ) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)
        except Exception as exc:
            self._logger.warning("Nansen ingest failed: %s", exc)
            return []

        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("data")
            if not isinstance(raw_items, list):
                raw_items = payload.get("results")
            items = raw_items if isinstance(raw_items, list) else []
        else:
            items = []

        prepared: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            candidate = self._extract_nansen_candidate(item)
            if candidate is None:
                continue
            prepared.append(candidate)

        dedup: dict[str, dict[str, Any]] = {}
        for item in prepared:
            address = item["address"]
            existing = dedup.get(address)
            if existing is None or float(item["vault_tvl"]) > float(existing["vault_tvl"]):
                dedup[address] = item

        ranked = sorted(
            dedup.values(),
            key=lambda item: float(item.get("vault_tvl") or 0.0),
            reverse=True,
        )[: max(1, int(self._config.nansen_candidate_limit))]
        if ranked:
            self._logger.info("Nansen candidates prepared count=%s", len(ranked))
        return ranked

    async def _fetch_userfills_activity_candidates(self) -> list[dict[str, Any]]:
        seed_addresses = self._store.list_monitored_addresses(
            limit=max(50, self._config.candidate_limit)
        )
        if not seed_addresses:
            return []

        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        start_ms = now_ms - (max(1, self._config.window_hours) * MS_HOUR)
        sem = asyncio.Semaphore(max(1, self._config.concurrency))

        async def scan(address: str) -> dict[str, Any] | None:
            async with sem:
                fills = await self._info(
                    {
                        "type": "userFillsByTime",
                        "user": address,
                        "startTime": start_ms,
                    }
                )
            if not isinstance(fills, list):
                return None

            trade_count = 0
            last_time = 0
            volume = 0.0
            for item in fills:
                if not isinstance(item, dict):
                    continue
                ts = self._to_int(item.get("time"))
                if ts <= 0:
                    continue
                px = self._to_float(item.get("px"))
                sz = self._to_float(item.get("sz"))
                trade_count += 1
                if ts > last_time:
                    last_time = ts
                if px > 0 and sz > 0:
                    volume += abs(px * sz)
            if trade_count <= 0 or last_time <= 0:
                return None
            return {
                "address": address,
                "label": None,
                "source": "hyperliquid_userfills_activity",
                "vault_tvl": volume,
                "_trade_count": trade_count,
                "_last_time": last_time,
            }

        scanned = await asyncio.gather(*(scan(address) for address in seed_addresses), return_exceptions=True)
        prepared: list[dict[str, Any]] = []
        for item in scanned:
            if isinstance(item, Exception):
                self._logger.warning("UserFills candidate scan failed: %s", item)
                continue
            if isinstance(item, dict):
                prepared.append(item)

        ranked = sorted(
            prepared,
            key=lambda item: (
                int(item.get("_trade_count") or 0),
                float(item.get("vault_tvl") or 0.0),
                int(item.get("_last_time") or 0),
            ),
            reverse=True,
        )
        candidates = [
            {
                "address": item["address"],
                "label": item.get("label"),
                "source": "hyperliquid_userfills_activity",
                "vault_tvl": float(item.get("vault_tvl") or 0.0),
            }
            for item in ranked[: self._config.candidate_limit]
        ]
        if candidates:
            self._logger.info("UserFills activity candidates prepared count=%s", len(candidates))
        return candidates

    async def _fetch_candidates(self) -> list[dict[str, Any]]:
        hyperliquid_candidates = await self._fetch_hyperliquid_candidates()
        userfills_candidates = await self._fetch_userfills_activity_candidates()
        nansen_candidates = await self._fetch_nansen_candidates()

        merged_raw: list[dict[str, Any]] = []
        merged_raw.extend(hyperliquid_candidates)
        merged_raw.extend(userfills_candidates)
        merged_raw.extend(nansen_candidates)
        if not merged_raw:
            return []

        dedup: dict[str, dict[str, Any]] = {}
        for item in merged_raw:
            address = str(item.get("address", "")).strip().lower()
            if not address:
                continue
            source = str(item.get("source", "")).strip() or "unknown"
            label = item.get("label")
            vault_tvl = float(item.get("vault_tvl") or 0.0)
            existing = dedup.get(address)
            if existing is None:
                dedup[address] = {
                    "address": address,
                    "label": label,
                    "source": source,
                    "vault_tvl": vault_tvl,
                }
                continue
            # Keep strongest score-hint; preserve Nansen source if present.
            if vault_tvl > float(existing.get("vault_tvl") or 0.0):
                existing["vault_tvl"] = vault_tvl
            if not existing.get("label") and label:
                existing["label"] = label
            if existing.get("source") != "nansen_smart_money_perps" and source == "nansen_smart_money_perps":
                existing["source"] = source

        ranked = sorted(
            dedup.values(),
            key=lambda item: float(item.get("vault_tvl") or 0.0),
            reverse=True,
        )[: self._config.candidate_limit]
        self._logger.info(
            "Combined candidates prepared count=%s (hyperliquid=%s userfills=%s nansen=%s)",
            len(ranked),
            len(hyperliquid_candidates),
            len(userfills_candidates),
            len(nansen_candidates),
        )
        return self._merge_with_existing_tracked(ranked)

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

        results = await asyncio.gather(*(scan_coin(coin) for coin in selected), return_exceptions=True)
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            self._logger.warning(
                "Recent trade candidate scan failures count=%s sample=%s",
                len(failures),
                failures[0],
            )

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
        self._logger.info("Recent trade candidates prepared count=%s", len(candidates))
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

        trades_24h = len(self._aggregate_trades(fills_24h))
        trades_7d = len(self._aggregate_trades(fills_7d))
        aggregated_30d = self._aggregate_trades(fills_30d)
        trades_30d = len(aggregated_30d)

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
        fills_capped = len(normalized) >= max(1, self._config.fill_cap_hint)
        age_probe_used = False
        ledger_first_activity_time = None
        probe_age_threshold_days = 30.0
        if (
            self._config.age_probe_enabled
            and fills_capped
            and (age_days is None or age_days < probe_age_threshold_days)
        ):
            ledger_first_activity_time = await self._probe_account_first_activity_time(
                candidate["address"]
            )
            if ledger_first_activity_time is not None and ledger_first_activity_time > 0:
                ledger_age_days = (now_ms - ledger_first_activity_time) / MS_DAY
                if age_days is None or ledger_age_days > age_days:
                    age_days = ledger_age_days
                    age_probe_used = True

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

        period_1d = self._compute_period_stats(fills=fills_24h, account_value=account_value)
        period_7d = self._compute_period_stats(fills=fills_7d, account_value=account_value)
        period_30d = self._compute_period_stats(fills=fills_30d, account_value=account_value)

        fees_30d = sum(abs(self._to_float(item.get("fee"))) for item in fills_30d)
        long_count = sum(
            1
            for trade in aggregated_30d
            if "long" in str(trade.get("direction", "")).lower()
        )

        volume_usd_30d = float(period_30d["volume_usd"] or 0.0)
        realized_pnl_30d = float(period_30d["realized_pnl"] or 0.0)
        win_rate_30d = (
            float(period_30d["win_rate"])
            if period_30d["win_rate"] is not None
            else None
        )
        wins = int(period_30d["wins"] or 0)
        losses = int(period_30d["losses"] or 0)

        long_ratio_30d = (long_count / trades_30d) if trades_30d else None
        avg_notional_30d = (
            float(period_30d["avg_notional"])
            if period_30d["avg_notional"] is not None
            else None
        )
        max_notional_30d = (
            float(period_30d["max_notional"])
            if period_30d["max_notional"] is not None
            else None
        )

        roi_30d = float(period_30d["roi_pct"]) if period_30d["roi_pct"] is not None else 0.0
        sharpe_30d = float(period_30d["sharpe"]) if period_30d["sharpe"] is not None else 0.0
        sortino_30d = float(period_30d["sortino"]) if period_30d["sortino"] is not None else 0.0
        drawdown_30d = (
            float(period_30d["max_drawdown_pct"])
            if period_30d["max_drawdown_pct"] is not None
            else None
        )
        volatility_30d = (
            float(period_30d["roi_volatility_pct"])
            if period_30d["roi_volatility_pct"] is not None
            else None
        )

        roi_score = self._clamp((roi_30d + 20.0) / 120.0, 0.0, 1.0)
        sharpe_score = self._clamp((sharpe_30d + 1.0) / 4.0, 0.0, 1.0)
        sortino_score = self._clamp((sortino_30d + 1.0) / 5.0, 0.0, 1.0)
        win_score = self._clamp(float(win_rate_30d or 0.0), 0.0, 1.0)
        activity_score = (
            min(1.0, trades_30d / 180.0) * 0.5
            + min(1.0, active_days_30d / 30.0) * 0.5
        )

        drawdown_risk = (
            min(1.0, max(0.0, float(drawdown_30d)) / 35.0)
            if drawdown_30d is not None
            else 0.0
        )
        volatility_risk = (
            min(1.0, max(0.0, float(volatility_30d)) / 12.0)
            if volatility_30d is not None
            else 0.0
        )
        fee_risk = min(1.0, fees_30d / 10_000.0)

        weighted_base = (
            (roi_score * 0.26)
            + (sharpe_score * 0.16)
            + (sortino_score * 0.16)
            + (win_score * 0.20)
            + (activity_score * 0.22)
        )
        risk_penalty = (
            (drawdown_risk * 0.60)
            + (volatility_risk * 0.25)
            + (fee_risk * 0.15)
        )
        score = self._clamp(weighted_base * (1.0 - risk_penalty), 0.0, 1.0) * 100.0

        stats_payload = {
            "vault_tvl": candidate["vault_tvl"],
            "realized_pnl_1d": period_1d["realized_pnl"],
            "realized_pnl_7d": period_7d["realized_pnl"],
            "wins_1d": period_1d["wins"],
            "losses_1d": period_1d["losses"],
            "wins_30d": wins,
            "losses_30d": losses,
            "wins_7d": period_7d["wins"],
            "losses_7d": period_7d["losses"],
            "win_rate_1d": period_1d["win_rate"],
            "win_rate_7d": period_7d["win_rate"],
            "weekly_trades": period_7d["trade_count"],
            "metrics_1d": {
                "roi_pct": period_1d["roi_pct"],
                "realized_pnl": period_1d["realized_pnl"],
                "win_rate": period_1d["win_rate"],
                "wins": period_1d["wins"],
                "losses": period_1d["losses"],
                "profit_to_loss_ratio": period_1d["profit_to_loss_ratio"],
                "trade_count": period_1d["trade_count"],
                "avg_pnl_per_trade": period_1d["avg_pnl_per_trade"],
                "max_drawdown_pct": period_1d["max_drawdown_pct"],
                "sharpe": period_1d["sharpe"],
                "sortino": period_1d["sortino"],
                "roi_volatility_pct": period_1d["roi_volatility_pct"],
            },
            "metrics_7d": {
                "roi_pct": period_7d["roi_pct"],
                "realized_pnl": period_7d["realized_pnl"],
                "win_rate": period_7d["win_rate"],
                "wins": period_7d["wins"],
                "losses": period_7d["losses"],
                "profit_to_loss_ratio": period_7d["profit_to_loss_ratio"],
                "trade_count": period_7d["trade_count"],
                "weekly_trades": period_7d["trade_count"],
                "avg_pnl_per_trade": period_7d["avg_pnl_per_trade"],
                "max_drawdown_pct": period_7d["max_drawdown_pct"],
                "sharpe": period_7d["sharpe"],
                "sortino": period_7d["sortino"],
                "roi_volatility_pct": period_7d["roi_volatility_pct"],
            },
            "metrics_30d": {
                "roi_pct": period_30d["roi_pct"],
                "realized_pnl": period_30d["realized_pnl"],
                "win_rate": period_30d["win_rate"],
                "wins": period_30d["wins"],
                "losses": period_30d["losses"],
                "profit_to_loss_ratio": period_30d["profit_to_loss_ratio"],
                "trade_count": period_30d["trade_count"],
                "weekly_trades": period_7d["trade_count"],
                "avg_pnl_per_trade": period_30d["avg_pnl_per_trade"],
                "max_drawdown_pct": period_30d["max_drawdown_pct"],
                "sharpe": period_30d["sharpe"],
                "sortino": period_30d["sortino"],
                "roi_volatility_pct": period_30d["roi_volatility_pct"],
            },
            "fills_sample_size": len(normalized),
            "fills_capped": fills_capped,
            "age_probe_used": age_probe_used,
            "ledger_first_activity_time": ledger_first_activity_time,
            "score_components": {
                "roi_score": round(roi_score, 6),
                "sharpe_score": round(sharpe_score, 6),
                "sortino_score": round(sortino_score, 6),
                "win_rate_score": round(win_score, 6),
                "activity_score": round(activity_score, 6),
                "drawdown_risk": round(drawdown_risk, 6),
                "volatility_risk": round(volatility_risk, 6),
                "fee_risk": round(fee_risk, 6),
                "weighted_base": round(weighted_base, 6),
                "risk_penalty": round(risk_penalty, 6),
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
            "max_drawdown_30d": drawdown_30d,
            "roi_volatility_30d": volatility_30d,
            "roi_30d": roi_30d,
            "sharpe_30d": sharpe_30d,
            "sortino_30d": sortino_30d,
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
        self._reset_retry_stats()
        try:
            candidates = await self._fetch_candidates()
            self._logger.info("Discovery candidates fetched count=%s", len(candidates))
            if not candidates:
                summary = {
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                    "candidates": 0,
                    "qualified": 0,
                    "upserted": 0,
                    "unlisted": 0,
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
            fetch_failures = 0
            fetch_failure_reasons: Counter[str] = Counter()
            fetch_failure_samples: list[str] = []

            async def gather_metrics(candidate: dict[str, Any]) -> dict[str, Any] | None:
                nonlocal fetch_failures
                async with sem:
                    try:
                        return await self._fetch_metrics(candidate, now_ms=now_ms)
                    except Exception as exc:
                        fetch_failures += 1
                        reason_parts = [exc.__class__.__name__]
                        status = getattr(exc, "status", None)
                        if status is not None:
                            reason_parts.append(f"status={status}")
                        reason = " ".join(reason_parts)
                        fetch_failure_reasons[reason] += 1
                        if len(fetch_failure_samples) < 5:
                            fetch_failure_samples.append(
                                f"{candidate.get('address')}:{reason}"
                            )
                        self._logger.debug(
                            "Failed to fetch metrics for candidate=%s source=%s error=%s",
                            candidate.get("address"),
                            candidate.get("source"),
                            exc,
                        )
                        return None

            metrics = await asyncio.gather(*(gather_metrics(c) for c in candidates))
            if fetch_failures > 0:
                self._logger.log(
                    logging.WARNING if fetch_failures >= max(10, len(candidates) // 3) else logging.INFO,
                    "Metrics fetch failures count=%s candidates=%s reasons=%s samples=%s",
                    fetch_failures,
                    len(candidates),
                    dict(fetch_failure_reasons),
                    fetch_failure_samples,
                )

            qualified = 0
            upserted = 0
            skipped_age = 0
            skipped_trades_30d = 0
            skipped_active_days = 0
            skipped_trades_7d = 0
            skipped_recent = 0
            skipped_win_rate = 0
            skipped_drawdown = 0
            skipped_pnl = 0
            qualified_by_source: dict[str, set[str]] = {}
            recent_cutoff_ms = now_ms - (
                max(1, int(self._config.max_last_activity_minutes)) * 60 * 1000
            )
            for item in metrics:
                if not item:
                    continue

                age_days = item.get("age_days")
                if age_days is None or age_days < self._config.min_age_days:
                    skipped_age += 1
                    continue
                if item["trades_30d"] < self._config.min_trades_30d:
                    skipped_trades_30d += 1
                    continue
                if item["active_days_30d"] < self._config.min_active_days_30d:
                    skipped_active_days += 1
                    continue
                if item["trades_7d"] < self._config.min_trades_7d:
                    skipped_trades_7d += 1
                    continue
                if int(item.get("last_fill_time") or 0) < recent_cutoff_ms:
                    skipped_recent += 1
                    continue
                win_rate_30d = item.get("win_rate_30d")
                if win_rate_30d is None or float(win_rate_30d) < self._config.min_win_rate_30d:
                    skipped_win_rate += 1
                    continue
                drawdown_30d = item.get("max_drawdown_30d")
                if (
                    drawdown_30d is not None
                    and float(drawdown_30d) > self._config.max_drawdown_30d_pct
                ):
                    skipped_drawdown += 1
                    continue
                realized_pnl_30d = float(item.get("realized_pnl_30d") or 0.0)
                if self._config.require_positive_pnl_30d:
                    if realized_pnl_30d <= self._config.min_realized_pnl_30d:
                        skipped_pnl += 1
                        continue
                elif realized_pnl_30d < self._config.min_realized_pnl_30d:
                    skipped_pnl += 1
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

            unlisted = 0
            candidate_sources = {
                str(candidate.get("source", "")).strip()
                for candidate in candidates
                if str(candidate.get("source", "")).strip()
            }
            for source in sorted(candidate_sources):
                unlisted += self._store.prune_auto_discovered(
                    source=source,
                    keep_addresses=qualified_by_source.get(source, set()),
                )
            self._logger.info(
                "Discovery filtering summary qualified=%s upserted=%s unlisted=%s skipped_age=%s skipped_trades30=%s skipped_active_days=%s skipped_trades7=%s skipped_recent=%s skipped_win_rate=%s skipped_drawdown=%s skipped_pnl=%s",
                qualified,
                upserted,
                unlisted,
                skipped_age,
                skipped_trades_30d,
                skipped_active_days,
                skipped_trades_7d,
                skipped_recent,
                skipped_win_rate,
                skipped_drawdown,
                skipped_pnl,
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
                "unlisted": unlisted,
                "pruned": unlisted,
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
            self._logger.exception("Discovery failed: %s", exc)
            raise
        finally:
            self._log_retry_summary()
