from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
import time
from datetime import UTC, datetime
from typing import Any

import aiohttp

from bot.config import ConfigError, load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.telegram_client import TelegramClientError, create_forum_topic, delete_forum_topic
from bot.trader_store import (
    SHOWCASE_STATUS_ACTIVE,
    SHOWCASE_STATUS_STALE,
    ShowcaseWallet,
    TraderForumTopic,
    TraderStore,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight showcase rotation worker")
    parser.add_argument("--once", action="store_true", help="Run one scheduler tick and exit")
    return parser.parse_args()


def _short_wallet(address: str) -> str:
    normalized = str(address or "").strip().lower()
    if not normalized:
        return "wallet"
    if len(normalized) <= 18:
        return normalized
    return f"{normalized[:10]}...{normalized[-6:]}"


class LightweightRotationWorker:
    def __init__(
        self,
        *,
        settings,
        http_session: aiohttp.ClientSession,
        logger: logging.Logger,
    ) -> None:
        self._settings = settings
        self._http_session = http_session
        self._logger = logger
        self._lease_holder = f"{socket.gethostname()}:{os.getpid()}:rotation"
        # Keep runtime lease short to avoid long stale-lock windows after restarts.
        self._lease_ttl_seconds = 180
        self._discovery_config = HyperliquidDiscoveryConfig(
            info_url=settings.hyperliquid_info_url,
            candidate_limit=max(
                int(settings.rotation_bootstrap_candidates),
                int(settings.rotation_scout_candidates),
                int(settings.showcase_slots) * 3,
            ),
            min_age_days=settings.discovery_min_age_days,
            min_trades_30d=settings.discovery_min_trades_30d,
            min_active_days_30d=settings.discovery_min_active_days_30d,
            min_win_rate_30d=settings.discovery_min_win_rate_30d,
            max_drawdown_30d_pct=settings.discovery_max_drawdown_30d_pct,
            max_last_activity_minutes=settings.discovery_max_last_activity_minutes,
            min_realized_pnl_30d=settings.discovery_min_realized_pnl_30d,
            require_positive_pnl_30d=settings.discovery_require_positive_pnl_30d,
            min_trades_7d=settings.discovery_min_trades_7d,
            window_hours=settings.discovery_window_hours,
            concurrency=settings.discovery_concurrency,
            fill_cap_hint=settings.discovery_fill_cap_hint,
            age_probe_enabled=settings.discovery_age_probe_enabled,
            seed_addresses=settings.discovery_seed_addresses,
            nansen_api_url=settings.nansen_api_url,
            nansen_api_key=settings.nansen_api_key,
            nansen_candidate_limit=settings.nansen_candidate_limit,
        )
        self._last_health_ts = 0.0
        self._last_scout_ts = 0.0
        self._last_bootstrap_ts = 0.0
        self._last_metrics_ts = 0.0

    def _acquire_lease(self) -> bool:
        with TraderStore(self._settings.database_dsn) as store:
            return store.acquire_runtime_lease(
                lock_name="showcase-rotation",
                holder=self._lease_holder,
                ttl_seconds=self._lease_ttl_seconds,
            )

    def _release_lease(self) -> None:
        with TraderStore(self._settings.database_dsn) as store:
            store.release_runtime_lease(
                lock_name="showcase-rotation",
                holder=self._lease_holder,
            )

    def _passes_hard_filters(self, item: dict[str, Any], *, now_ms: int) -> bool:
        age_days = item.get("age_days")
        if age_days is None or float(age_days) < float(self._settings.discovery_min_age_days):
            return False
        if int(item.get("trades_30d") or 0) < int(self._settings.discovery_min_trades_30d):
            return False
        if int(item.get("active_days_30d") or 0) < int(self._settings.discovery_min_active_days_30d):
            return False
        if int(item.get("trades_7d") or 0) < int(self._settings.discovery_min_trades_7d):
            return False

        cutoff_ms = now_ms - (max(1, int(self._settings.discovery_max_last_activity_minutes)) * 60 * 1000)
        if int(item.get("last_fill_time") or 0) < cutoff_ms:
            return False

        win_rate_30d = item.get("win_rate_30d")
        if win_rate_30d is None or float(win_rate_30d) < float(self._settings.discovery_min_win_rate_30d):
            return False

        drawdown_30d = item.get("max_drawdown_30d")
        if (
            drawdown_30d is not None
            and float(drawdown_30d) > float(self._settings.discovery_max_drawdown_30d_pct)
        ):
            return False

        realized_pnl_30d = float(item.get("realized_pnl_30d") or 0.0)
        if self._settings.discovery_require_positive_pnl_30d:
            if realized_pnl_30d <= float(self._settings.discovery_min_realized_pnl_30d):
                return False
        elif realized_pnl_30d < float(self._settings.discovery_min_realized_pnl_30d):
            return False
        return True

    @staticmethod
    def _passes_soft_filters(item: dict[str, Any], *, now_ms: int) -> bool:
        age_days = item.get("age_days")
        if age_days is None or float(age_days) < 7.0:
            return False
        if int(item.get("trades_30d") or 0) < 30:
            return False
        last_fill_time = int(item.get("last_fill_time") or 0)
        if last_fill_time <= 0:
            return False
        if last_fill_time < (now_ms - (24 * 60 * 60 * 1000)):
            return False
        return True

    @staticmethod
    def _upsert_discovered_from_metrics(*, store: TraderStore, item: dict[str, Any]) -> None:
        store.upsert_discovered(
            address=str(item["address"]),
            label=(str(item["label"]) if item.get("label") is not None else None),
            source=str(item["source"]),
            trades_24h=int(item.get("trades_24h") or 0),
            active_hours_24h=int(item.get("active_hours_24h") or 0),
            trades_7d=int(item.get("trades_7d") or 0),
            trades_30d=int(item.get("trades_30d") or 0),
            active_days_30d=int(item.get("active_days_30d") or 0),
            first_fill_time=(int(item["first_fill_time"]) if item.get("first_fill_time") else None),
            last_fill_time=(int(item["last_fill_time"]) if item.get("last_fill_time") else None),
            age_days=(float(item["age_days"]) if item.get("age_days") is not None else None),
            volume_usd_30d=float(item.get("volume_usd_30d") or 0.0),
            realized_pnl_30d=float(item.get("realized_pnl_30d") or 0.0),
            fees_30d=float(item.get("fees_30d") or 0.0),
            win_rate_30d=(float(item["win_rate_30d"]) if item.get("win_rate_30d") is not None else None),
            long_ratio_30d=(
                float(item["long_ratio_30d"])
                if item.get("long_ratio_30d") is not None
                else None
            ),
            avg_notional_30d=(
                float(item["avg_notional_30d"])
                if item.get("avg_notional_30d") is not None
                else None
            ),
            max_notional_30d=(
                float(item["max_notional_30d"])
                if item.get("max_notional_30d") is not None
                else None
            ),
            account_value=(
                float(item["account_value"])
                if item.get("account_value") is not None
                else None
            ),
            total_ntl_pos=(
                float(item["total_ntl_pos"])
                if item.get("total_ntl_pos") is not None
                else None
            ),
            total_margin_used=(
                float(item["total_margin_used"])
                if item.get("total_margin_used") is not None
                else None
            ),
            score=float(item.get("score") or 0.0),
            stats_json=(str(item["stats_json"]) if item.get("stats_json") is not None else None),
        )

    async def _ensure_topic_for_wallet(self, *, address: str, label: str | None) -> int | None:
        forum_chat_id = str(self._settings.telegram_forum_chat_id or "").strip()
        if not forum_chat_id:
            return None

        existing: TraderForumTopic | None
        with TraderStore(self._settings.database_dsn) as store:
            existing = store.get_trader_forum_topic(
                trader_address=address,
                forum_chat_id=forum_chat_id,
            )
        if existing is not None and int(existing.message_thread_id) > 0:
            return int(existing.message_thread_id)

        topic_name = _short_wallet(address)
        if label:
            compact_label = str(label).strip()
            if compact_label:
                topic_name = f"{compact_label} | {_short_wallet(address)}"

        try:
            payload = await create_forum_topic(
                self._http_session,
                bot_token=self._settings.telegram_bot_token,
                chat_id=forum_chat_id,
                name=topic_name,
            )
            thread_id = int(payload.get("message_thread_id") or 0)
            if thread_id <= 0:
                raise RuntimeError(f"Invalid createForumTopic response: {payload}")
        except TelegramClientError as exc:
            if exc.is_forum_not_supported():
                self._logger.error(
                    "Forum topics are not supported for chat_id=%s",
                    forum_chat_id,
                )
            else:
                self._logger.warning(
                    "Failed to create topic for address=%s: %s",
                    address,
                    exc,
                )
            return None
        except Exception as exc:  # pragma: no cover - network/runtime edge path
            self._logger.warning("Topic creation failed for address=%s: %s", address, exc)
            return None

        with TraderStore(self._settings.database_dsn) as store:
            store.upsert_trader_forum_topic(
                trader_address=address,
                forum_chat_id=forum_chat_id,
                message_thread_id=thread_id,
                topic_name=topic_name,
            )
        return thread_id

    async def _discover_scored_candidates(self, *, limit: int) -> list[dict[str, Any]]:
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        with TraderStore(self._settings.database_dsn) as store:
            service = HyperliquidDiscoveryService(
                http_session=self._http_session,
                store=store,
                config=self._discovery_config,
                logger=self._logger,
            )
            candidates = await service._fetch_candidates()
            selected = candidates[: max(1, int(limit))]
            if not selected:
                return []

            sem = asyncio.Semaphore(max(1, int(self._settings.discovery_concurrency)))

            async def gather_one(candidate: dict[str, Any]) -> dict[str, Any] | None:
                async with sem:
                    try:
                        return await service._fetch_metrics(candidate, now_ms=now_ms)
                    except Exception as exc:
                        self._logger.debug(
                            "Rotation candidate metrics fetch failed address=%s source=%s: %s",
                            candidate.get("address"),
                            candidate.get("source"),
                            exc,
                        )
                        return None

            metrics = await asyncio.gather(*(gather_one(item) for item in selected))

        qualified: list[dict[str, Any]] = []
        soft_pool: list[dict[str, Any]] = []
        for item in metrics:
            if not item:
                continue
            if self._passes_hard_filters(item, now_ms=now_ms):
                qualified.append(item)
                continue
            if self._passes_soft_filters(item, now_ms=now_ms):
                soft_pool.append(item)

        qualified.sort(
            key=lambda item: (
                float(item.get("score") or 0.0),
                int(item.get("last_fill_time") or 0),
            ),
            reverse=True,
        )
        soft_pool.sort(
            key=lambda item: (
                float(item.get("score") or 0.0),
                int(item.get("last_fill_time") or 0),
            ),
            reverse=True,
        )
        hard_count = len(qualified)
        if len(qualified) < int(limit):
            seen = {str(item.get("address")) for item in qualified}
            for candidate in soft_pool:
                address = str(candidate.get("address"))
                if not address or address in seen:
                    continue
                qualified.append(candidate)
                seen.add(address)
                if len(qualified) >= int(limit):
                    break
            if soft_pool:
                self._logger.info(
                    "Rotation fallback expanded candidate pool hard=%s soft=%s target=%s final=%s",
                    hard_count,
                    len(soft_pool),
                    limit,
                    len(qualified),
                )
        return qualified

    async def _bootstrap_if_needed(self) -> int:
        with TraderStore(self._settings.database_dsn) as store:
            current = store.count_showcase_wallets(active_only=False)
        slots = max(1, int(self._settings.showcase_slots))
        if current >= slots:
            return 0

        needed = slots - current
        candidates = await self._discover_scored_candidates(
            limit=max(int(self._settings.rotation_bootstrap_candidates), slots * 2),
        )
        if not candidates:
            self._logger.warning("Bootstrap found no qualified candidates")
            return 0

        with TraderStore(self._settings.database_dsn) as store:
            existing = set(store.list_showcase_addresses(limit=slots * 10, include_stale=True))

        selected = [item for item in candidates if str(item.get("address")) not in existing][:needed]
        if not selected:
            return 0

        inserted = 0
        for item in selected:
            address = str(item["address"])
            label = (str(item["label"]) if item.get("label") is not None else None)
            with TraderStore(self._settings.database_dsn) as store:
                self._upsert_discovered_from_metrics(store=store, item=item)
                store.upsert_showcase_wallet(
                    address=address,
                    status=SHOWCASE_STATUS_ACTIVE,
                    metrics_at=datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
                )
                store.log_rotation(
                    old_address=None,
                    new_address=address,
                    old_score=None,
                    new_score=float(item.get("score") or 0.0),
                    reason="bootstrap",
                )
            await self._ensure_topic_for_wallet(address=address, label=label)
            inserted += 1

        with TraderStore(self._settings.database_dsn) as store:
            store.refresh_catalog_current_from_showcase(
                activity_window_minutes=self._settings.live_top100_active_window_minutes,
            )
        self._logger.info("Bootstrap inserted showcase wallets count=%s", inserted)
        return inserted

    async def _refresh_showcase_metrics(self) -> int:
        with TraderStore(self._settings.database_dsn) as store:
            wallets = store.list_showcase_wallets(limit=max(1, int(self._settings.showcase_slots) * 2))
        if not wallets:
            return 0

        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        with TraderStore(self._settings.database_dsn) as store:
            service = HyperliquidDiscoveryService(
                http_session=self._http_session,
                store=store,
                config=self._discovery_config,
                logger=self._logger,
            )
            sem = asyncio.Semaphore(max(1, int(self._settings.discovery_concurrency)))

            async def refresh_one(wallet: ShowcaseWallet) -> dict[str, Any] | None:
                candidate = {
                    "address": wallet.address,
                    "label": wallet.label,
                    "source": wallet.source or "showcase_wallet",
                    "vault_tvl": 0.0,
                }
                async with sem:
                    try:
                        return await service._fetch_metrics(candidate, now_ms=now_ms)
                    except Exception as exc:
                        self._logger.debug(
                            "Metrics refresh failed for showcase wallet=%s: %s",
                            wallet.address,
                            exc,
                        )
                        return None

            refreshed = await asyncio.gather(*(refresh_one(wallet) for wallet in wallets))

            changed = 0
            for item in refreshed:
                if not item:
                    continue
                self._upsert_discovered_from_metrics(store=store, item=item)
                store.touch_showcase_metrics(address=str(item["address"]))
                changed += 1

            store.refresh_catalog_current_from_showcase(
                activity_window_minutes=self._settings.live_top100_active_window_minutes,
            )

        self._logger.info("Showcase metrics refresh updated=%s", changed)
        return changed

    async def _health_check(self) -> int:
        with TraderStore(self._settings.database_dsn) as store:
            wallets = store.list_showcase_wallets(limit=max(1, int(self._settings.showcase_slots) * 2))
        if not wallets:
            return 0

        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        stale_hours = float(max(1, int(self._settings.rotation_stale_hours)))
        stale_cycles_required = int(max(1, int(self._settings.rotation_stale_cycles)))
        changed = 0

        for wallet in wallets:
            if wallet.last_fill_time is not None and wallet.last_fill_time > 0:
                idle_hours = max(0.0, (now_ms - int(wallet.last_fill_time)) / 3_600_000.0)
            else:
                idle_hours = stale_hours + 1.0

            if idle_hours >= stale_hours:
                idle_cycles = int(wallet.idle_cycles) + 1
            else:
                idle_cycles = 0

            next_status = (
                SHOWCASE_STATUS_STALE if idle_cycles >= stale_cycles_required else SHOWCASE_STATUS_ACTIVE
            )
            if next_status != wallet.status or abs(float(wallet.idle_hours) - idle_hours) > 0.01:
                with TraderStore(self._settings.database_dsn) as store:
                    store.update_showcase_health(
                        address=wallet.address,
                        idle_hours=idle_hours,
                        idle_cycles=idle_cycles,
                        status=next_status,
                    )
                changed += 1

        if changed > 0:
            with TraderStore(self._settings.database_dsn) as store:
                store.refresh_catalog_current_from_showcase(
                    activity_window_minutes=self._settings.live_top100_active_window_minutes,
                )

        self._logger.info("Showcase health check changed=%s", changed)
        return changed

    async def _swap_wallet(self, *, old_wallet: ShowcaseWallet, candidate: dict[str, Any], reason: str) -> bool:
        old_address = str(old_wallet.address)
        new_address = str(candidate["address"])
        if old_address == new_address:
            return False

        forum_chat_id = str(self._settings.telegram_forum_chat_id or "").strip()
        old_topic: TraderForumTopic | None = None
        if forum_chat_id:
            with TraderStore(self._settings.database_dsn) as store:
                old_topic = store.get_trader_forum_topic(
                    trader_address=old_address,
                    forum_chat_id=forum_chat_id,
                )

        new_label = str(candidate.get("label") or "").strip() or None
        with TraderStore(self._settings.database_dsn) as store:
            self._upsert_discovered_from_metrics(store=store, item=candidate)
        await self._ensure_topic_for_wallet(address=new_address, label=new_label)

        old_score = old_wallet.score if old_wallet.score is not None else 0.0
        new_score = float(candidate.get("score") or 0.0)
        with TraderStore(self._settings.database_dsn) as store:
            store.upsert_showcase_wallet(
                address=new_address,
                status=SHOWCASE_STATUS_ACTIVE,
                metrics_at=datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
            )
            store.remove_showcase_wallet(address=old_address)
            store.cleanup_rotated_trader(address=old_address)
            store.delete_trader_forum_topic(
                trader_address=old_address,
                forum_chat_id=forum_chat_id if forum_chat_id else None,
            )
            store.log_rotation(
                old_address=old_address,
                new_address=new_address,
                old_score=(float(old_score) if old_score is not None else None),
                new_score=new_score,
                reason=reason,
            )
            store.refresh_catalog_current_from_showcase(
                activity_window_minutes=self._settings.live_top100_active_window_minutes,
            )

        if old_topic is not None and forum_chat_id:
            try:
                await delete_forum_topic(
                    self._http_session,
                    bot_token=self._settings.telegram_bot_token,
                    chat_id=forum_chat_id,
                    message_thread_id=old_topic.message_thread_id,
                )
            except TelegramClientError as exc:
                if not (exc.is_topic_missing() or exc.is_chat_unavailable()):
                    self._logger.warning(
                        "Failed to remove old showcase topic wallet=%s thread=%s: %s",
                        old_address,
                        old_topic.message_thread_id,
                        exc,
                    )
            except Exception as exc:  # pragma: no cover - network/runtime edge path
                self._logger.warning(
                    "Failed to remove old showcase topic wallet=%s thread=%s: %s",
                    old_address,
                    old_topic.message_thread_id,
                    exc,
                )

        self._logger.info(
            "Showcase rotation swapped old=%s new=%s reason=%s old_score=%s new_score=%s",
            old_address,
            new_address,
            reason,
            old_score,
            new_score,
        )
        return True

    async def _scout_and_rotate(self) -> int:
        slots = max(1, int(self._settings.showcase_slots))
        with TraderStore(self._settings.database_dsn) as store:
            showcase_all = store.list_showcase_wallets(limit=slots * 3)
            stale_wallets = store.list_showcase_wallets(status=SHOWCASE_STATUS_STALE, limit=slots)

        if not showcase_all:
            return 0

        candidates = await self._discover_scored_candidates(
            limit=max(int(self._settings.rotation_scout_candidates), slots * 2),
        )
        if not candidates:
            self._logger.info("Scout found no qualified replacement candidates")
            return 0

        current_addresses = {wallet.address for wallet in showcase_all}
        pool = [item for item in candidates if str(item.get("address")) not in current_addresses]
        if not pool:
            return 0

        rotated = 0
        stale_sorted = sorted(
            stale_wallets,
            key=lambda wallet: (wallet.idle_cycles, wallet.idle_hours),
            reverse=True,
        )
        for old_wallet in stale_sorted:
            if not pool:
                break
            new_candidate = pool.pop(0)
            if await self._swap_wallet(old_wallet=old_wallet, candidate=new_candidate, reason="stale"):
                rotated += 1

        if rotated > 0:
            return rotated

        active_wallets = [wallet for wallet in showcase_all if wallet.status == SHOWCASE_STATUS_ACTIVE]
        if not active_wallets or not pool:
            return 0

        worst_current = min(active_wallets, key=lambda wallet: float(wallet.score or 0.0))
        best_candidate = pool[0]
        worst_score = float(worst_current.score or 0.0)
        best_score = float(best_candidate.get("score") or 0.0)
        threshold_multiplier = 1.0 + (float(self._settings.rotation_score_threshold_pct) / 100.0)
        if best_score > (worst_score * threshold_multiplier):
            if await self._swap_wallet(
                old_wallet=worst_current,
                candidate=best_candidate,
                reason="outperformed",
            ):
                rotated += 1
        return rotated

    async def run_scheduler_tick(self) -> None:
        if not self._acquire_lease():
            self._logger.info("Rotation tick skipped: lease is held by another worker")
            return
        try:
            now = time.monotonic()
            slots = max(1, int(self._settings.showcase_slots))

            with TraderStore(self._settings.database_dsn) as store:
                trimmed = store.trim_showcase_wallets(max_slots=slots)
            if trimmed > 0:
                self._logger.warning("Showcase trim removed overflow wallets count=%s", trimmed)

            bootstrap_interval = max(
                1,
                int(self._settings.rotation_bootstrap_interval_minutes),
            ) * 60
            health_interval = max(1, int(self._settings.rotation_health_interval_minutes)) * 60
            scout_interval = max(1, int(self._settings.rotation_scout_interval_hours)) * 3600
            metrics_interval = max(1, int(self._settings.rotation_metrics_refresh_hours)) * 3600

            if self._last_bootstrap_ts <= 0 or (now - self._last_bootstrap_ts) >= bootstrap_interval:
                inserted = await self._bootstrap_if_needed()
                if inserted > 0:
                    self._logger.info("Bootstrap tick filled=%s", inserted)
                self._last_bootstrap_ts = time.monotonic()
                now = time.monotonic()

            if self._last_health_ts <= 0 or (now - self._last_health_ts) >= health_interval:
                await self._health_check()
                self._last_health_ts = time.monotonic()

            if self._last_scout_ts <= 0 or (now - self._last_scout_ts) >= scout_interval:
                rotated = await self._scout_and_rotate()
                self._logger.info("Scout tick finished rotations=%s", rotated)
                self._last_scout_ts = time.monotonic()

            if self._last_metrics_ts <= 0 or (now - self._last_metrics_ts) >= metrics_interval:
                await self._refresh_showcase_metrics()
                self._last_metrics_ts = time.monotonic()
        finally:
            self._release_lease()


async def _run() -> None:
    args = _parse_args()
    try:
        settings = load_settings(require_telegram=True)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc

    setup_logging(
        service_name="cryptoinsider.rotation-worker",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.rotation-worker")

    if not settings.showcase_mode_enabled:
        logger.info("SHOWCASE_MODE_ENABLED=false, rotation worker is idle")
        if args.once:
            return
        while True:
            await asyncio.sleep(60)

    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    cycle = 0
    async with aiohttp.ClientSession(timeout=timeout) as http_session:
        worker = LightweightRotationWorker(
            settings=settings,
            http_session=http_session,
            logger=logger,
        )
        while True:
            cycle += 1
            with bind_log_context(cycle=cycle, cycle_id=new_trace_id("rot")):
                try:
                    await worker.run_scheduler_tick()
                except Exception as exc:
                    logger.exception("Rotation cycle failed: %s", exc)
            if args.once:
                return
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(_run())
