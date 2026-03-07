from __future__ import annotations

import asyncio
import json
import logging
import math
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

        for attempt in range(attempts):
            try:
                async with self._http_session.post(self._config.info_url, json=payload) as response:
                    if response.status == 429 and attempt < attempts - 1:
                        retry_after_raw = response.headers.get("Retry-After", "").strip()
                        retry_after = float(retry_after_raw) if retry_after_raw else 0.0
                        self._logger.warning(
                            "Hyperliquid rate limit payload_type=%s attempt=%s retry_after=%s",
                            payload.get("type"),
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
                    self._logger.warning(
                        "Hyperliquid response error payload_type=%s attempt=%s status=%s",
                        payload.get("type"),
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
                    self._logger.warning(
                        "Hyperliquid client error payload_type=%s attempt=%s error=%s",
                        payload.get("type"),
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

    def _compute_period_stats(
        self,
        *,
        fills: list[dict[str, Any]],
        account_value: float | None,
    ) -> dict[str, float | int | None]:
        closed_pnls: list[float] = []
        returns: list[float] = []
        notionals: list[float] = []

        equity = 1.0
        peak_equity = 1.0
        max_drawdown = 0.0

        for item in fills:
            px = self._to_float(item.get("px"))
            sz = self._to_float(item.get("sz"))
            notional = abs(px * sz) if px > 0 and sz > 0 else 0.0
            if notional > 0:
                notionals.append(notional)

            closed_pnl = self._to_float(item.get("closedPnl"))
            if abs(closed_pnl) <= 1e-12:
                continue
            closed_pnls.append(closed_pnl)

            if notional <= 0:
                continue
            trade_return = closed_pnl / notional
            # Guard against extreme outliers from tiny notionals.
            trade_return = max(-0.99, min(10.0, trade_return))
            returns.append(trade_return)
            equity *= 1.0 + trade_return
            peak_equity = max(peak_equity, equity)
            if peak_equity > 0:
                drawdown = (peak_equity - equity) / peak_equity
                max_drawdown = max(max_drawdown, drawdown)

        realized_pnl = sum(closed_pnls)
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
            roi_base = abs(account_value)
        else:
            volume = sum(notionals)
            if volume > 1e-9:
                roi_base = volume
        roi_pct = ((realized_pnl / roi_base) * 100.0) if roi_base else None

        mean_return = (sum(returns) / len(returns)) if returns else 0.0
        volatility = self._population_std(returns) if returns else 0.0
        roi_volatility_pct = (volatility * 100.0) if returns else None
        sharpe = (
            (mean_return / volatility) * math.sqrt(len(returns))
            if volatility > 1e-12
            else None
        )

        downside = [value for value in returns if value < 0]
        downside_vol = self._population_std(downside) if downside else 0.0
        sortino = (
            (mean_return / downside_vol) * math.sqrt(len(returns))
            if downside_vol > 1e-12
            else None
        )

        return {
            "trade_count": len(fills),
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
            "avg_notional": ((sum(notionals) / len(fills)) if fills else None),
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
            dedup[item["address"]] = item
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
        long_count = 0
        for item in fills_30d:
            direction = str(item.get("dir", "")).lower()
            side = str(item.get("side", "")).upper()
            if "long" in direction or side == "B":
                long_count += 1

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

        roi_score = self._clamp(roi_30d, -50.0, 200.0) / 200.0
        sharpe_score = self._clamp(sharpe_30d, -2.0, 5.0) / 5.0
        sortino_score = self._clamp(sortino_30d, -2.0, 7.0) / 7.0
        win_score = self._clamp(float(win_rate_30d or 0.0), 0.0, 1.0)
        activity_score = (
            min(1.0, trades_30d / 180.0) * 0.6
            + min(1.0, active_days_30d / 30.0) * 0.4
        )

        drawdown_penalty = (
            min(1.0, max(0.0, float(drawdown_30d)) / 35.0)
            if drawdown_30d is not None
            else 1.0
        )
        volatility_penalty = (
            min(1.0, max(0.0, float(volatility_30d)) / 12.0)
            if volatility_30d is not None
            else 1.0
        )
        fee_penalty = min(1.0, fees_30d / 10_000.0)

        weighted_base = (
            (roi_score * 0.28)
            + (sharpe_score * 0.18)
            + (sortino_score * 0.16)
            + (win_score * 0.20)
            + (activity_score * 0.18)
        )
        weighted_penalty = (
            (drawdown_penalty * 0.18)
            + (volatility_penalty * 0.10)
            + (fee_penalty * 0.04)
        )
        score = max(0.0, (weighted_base - weighted_penalty) * 100.0)

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
                "weekly_trades": period_30d["trade_count"],
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
                "drawdown_penalty": round(drawdown_penalty, 6),
                "volatility_penalty": round(volatility_penalty, 6),
                "fee_penalty": round(fee_penalty, 6),
                "weighted_base": round(weighted_base, 6),
                "weighted_penalty": round(weighted_penalty, 6),
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
        try:
            candidates = await self._fetch_candidates()
            self._logger.info("Discovery candidates fetched count=%s", len(candidates))
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
                    except Exception as exc:
                        self._logger.warning(
                            "Failed to fetch metrics for candidate=%s source=%s error=%s",
                            candidate.get("address"),
                            candidate.get("source"),
                            exc,
                        )
                        return None

            metrics = await asyncio.gather(*(gather_metrics(c) for c in candidates))

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
                if drawdown_30d is None or float(drawdown_30d) > self._config.max_drawdown_30d_pct:
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
            self._logger.info(
                "Discovery filtering summary qualified=%s upserted=%s pruned=%s skipped_age=%s skipped_trades30=%s skipped_active_days=%s skipped_trades7=%s skipped_recent=%s skipped_win_rate=%s skipped_drawdown=%s skipped_pnl=%s",
                qualified,
                upserted,
                pruned,
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
            self._logger.exception("Discovery failed: %s", exc)
            raise
