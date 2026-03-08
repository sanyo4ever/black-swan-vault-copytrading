from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime

import aiohttp

from bot.config import ConfigError, load_settings, load_sources_config
from bot.dedup import DedupStore
from bot.delivery_dispatcher import DeliveryDispatcher, DeliveryDispatcherConfig
from bot.formatter import format_signal
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.sources import build_source
from bot.telegram_client import (
    TelegramClientError,
    create_forum_topic,
    delete_forum_topic,
    get_chat_member,
    get_me,
    set_telegram_http_logging,
)
from bot.trader_store import TraderStore

RETRY_BASE_DELAY_SECONDS = 15
RETRY_MAX_DELAY_SECONDS = 15 * 60
RETRY_MAX_ATTEMPTS = 8
RETRY_BATCH_LIMIT = 200


@dataclass(frozen=True)
class DeliveryOutcome:
    status: str
    category: str | None = None
    retry_after: int | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto trade signal broadcaster")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one fetch cycle and exit",
    )
    return parser.parse_args()


def _sort_key(signal) -> tuple[int, datetime]:
    if signal.timestamp is None:
        return (1, datetime.min)
    return (0, signal.timestamp)


def _short_error(exc: Exception, *, limit: int = 500) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _retry_delay_seconds(*, attempt_count: int, retry_after: int | None) -> int:
    if retry_after is not None and retry_after > 0:
        return max(1, min(RETRY_MAX_DELAY_SECONDS, int(retry_after)))
    exponent = max(0, int(attempt_count) - 1)
    delay = RETRY_BASE_DELAY_SECONDS * (2**exponent)
    return max(1, min(RETRY_MAX_DELAY_SECONDS, delay))


def _classify_delivery_error(exc: Exception) -> str:
    if isinstance(exc, TelegramClientError):
        if exc.is_flood_limit():
            return "flood"
        if exc.is_topic_missing():
            return "topic_missing"
        if exc.is_chat_unavailable():
            return "chat_unavailable"
        if exc.is_transient():
            return "transient"
        return "permanent"
    if isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError, OSError)):
        return "transient"
    return "permanent"


def _store_call_sync(settings, fn):
    with TraderStore(settings.database_dsn) as store:
        return fn(store)


async def _store_call(settings, fn):
    return await asyncio.to_thread(_store_call_sync, settings, fn)


def _normalize_trader_address(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _forum_topic_name(trader_address: str) -> str:
    normalized = _normalize_trader_address(trader_address)
    if not normalized:
        return "Trader"
    if len(normalized) <= 18:
        return normalized
    return f"{normalized[:10]}...{normalized[-6:]}"


def _forum_chat_id(settings) -> str:
    return str(getattr(settings, "telegram_forum_chat_id", "") or "").strip()


async def _validate_forum_permissions(*, settings, http_session, logger: logging.Logger) -> None:
    forum_chat_id = _forum_chat_id(settings)
    if not forum_chat_id:
        logger.warning("Forum mode disabled: TELEGRAM_FORUM_CHAT_ID is empty")
        return
    try:
        me = await get_me(
            http_session,
            bot_token=settings.telegram_bot_token,
        )
        bot_id = int(me.get("id", 0) or 0)
        if bot_id <= 0:
            logger.error("Forum preflight: failed to resolve bot id from getMe response")
            return
        member = await get_chat_member(
            http_session,
            bot_token=settings.telegram_bot_token,
            chat_id=forum_chat_id,
            user_id=bot_id,
        )
    except Exception as exc:
        logger.error(
            "Forum preflight failed chat_id=%s error=%s",
            forum_chat_id,
            exc,
        )
        return

    status = str(member.get("status", "")).strip().lower()
    can_manage_topics = bool(member.get("can_manage_topics"))
    if status == "creator" or (status == "administrator" and can_manage_topics):
        logger.info(
            "Forum preflight ok chat_id=%s bot_status=%s can_manage_topics=%s",
            forum_chat_id,
            status,
            can_manage_topics,
        )
        return
    logger.error(
        "Forum preflight failed: bot requires admin + Manage Topics in chat_id=%s (status=%s can_manage_topics=%s)",
        forum_chat_id,
        status or "-",
        can_manage_topics,
    )


def _is_forum_topic_delivery(
    *,
    settings,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
) -> bool:
    forum_chat_id = _forum_chat_id(settings)
    if not forum_chat_id:
        return False
    if message_thread_id is None:
        return False
    if _normalize_trader_address(trader_address) is None:
        return False
    return str(chat_id).strip() == forum_chat_id


def _deactivate_trader_target(
    *,
    settings,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
    logger: logging.Logger,
    reason: str,
) -> None:
    if not trader_address:
        return
    with TraderStore(settings.database_dsn) as store:
        sessions = store.cancel_chat_trader_subscriptions(
            chat_id=chat_id,
            trader_address=trader_address,
        )
        dropped = store.delete_pending_retries_for_target(
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
        )
    logger.warning(
        "Deactivated target chat_id=%s trader=%s thread=%s sessions=%s pending_retries_deleted=%s reason=%s",
        chat_id,
        trader_address,
        message_thread_id,
        len(sessions),
        dropped,
        reason,
    )


def _forget_forum_topic_mapping(
    *,
    settings,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
    logger: logging.Logger,
    reason: str,
) -> None:
    normalized = _normalize_trader_address(trader_address)
    if not normalized:
        return
    with TraderStore(settings.database_dsn) as store:
        deleted_topics = store.delete_trader_forum_topic(
            trader_address=normalized,
            forum_chat_id=chat_id,
        )
        dropped_retries = store.delete_pending_retries_for_target(
            chat_id=chat_id,
            trader_address=normalized,
            message_thread_id=message_thread_id,
        )
    logger.warning(
        "Dropped forum topic mapping chat_id=%s trader=%s thread=%s deleted_topics=%s pending_retries_deleted=%s reason=%s",
        chat_id,
        normalized,
        message_thread_id,
        deleted_topics,
        dropped_retries,
        reason,
    )


async def _ensure_forum_topic_for_trader(
    *,
    settings,
    http_session,
    logger: logging.Logger,
    chat_id: str,
    trader_address: str,
    topic_cache: dict[str, int],
) -> int | None:
    unsupported_key = "__forum_unsupported__"
    if topic_cache.get(unsupported_key) == 1:
        return None

    normalized = _normalize_trader_address(trader_address)
    if not normalized:
        return None

    cached_thread_id = topic_cache.get(normalized)
    if cached_thread_id is not None and cached_thread_id > 0:
        return cached_thread_id

    existing = await _store_call(
        settings,
        lambda store: store.get_trader_forum_topic(
            trader_address=normalized,
            forum_chat_id=chat_id,
        ),
    )
    if existing is not None and int(existing.message_thread_id) > 0:
        topic_cache[normalized] = int(existing.message_thread_id)
        return int(existing.message_thread_id)

    topic_name = _forum_topic_name(normalized)
    try:
        topic_result = await create_forum_topic(
            http_session,
            bot_token=settings.telegram_bot_token,
            chat_id=chat_id,
            name=topic_name,
        )
        thread_id = int(topic_result.get("message_thread_id") or 0)
        if thread_id <= 0:
            raise RuntimeError(f"Invalid createForumTopic response: {topic_result}")
    except TelegramClientError as exc:
        if exc.is_forum_not_supported():
            topic_cache[unsupported_key] = 1
            logger.error(
                "Forum topics are not supported for chat_id=%s; switch chat to supergroup with Topics",
                chat_id,
            )
        elif exc.is_chat_unavailable():
            logger.error(
                "Forum chat unavailable while creating topic chat_id=%s trader=%s: %s",
                chat_id,
                normalized,
                exc,
            )
        else:
            logger.warning(
                "Failed to create forum topic chat_id=%s trader=%s: %s",
                chat_id,
                normalized,
                exc,
            )
        return None
    except Exception as exc:
        logger.warning(
            "Unexpected failure creating forum topic chat_id=%s trader=%s: %s",
            chat_id,
            normalized,
            exc,
        )
        return None

    await _store_call(
        settings,
        lambda store: store.upsert_trader_forum_topic(
            trader_address=normalized,
            forum_chat_id=chat_id,
            message_thread_id=thread_id,
            topic_name=topic_name,
        ),
    )
    topic_cache[normalized] = thread_id
    logger.info(
        "Created forum topic chat_id=%s trader=%s thread=%s",
        chat_id,
        normalized,
        thread_id,
    )
    return thread_id


def _queue_initial_retry(
    *,
    settings,
    dedup_key: str,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
    message_text: str,
    error: str,
    attempt_count: int,
    retry_after: int | None,
    logger: logging.Logger,
) -> int:
    delay_seconds = _retry_delay_seconds(
        attempt_count=attempt_count,
        retry_after=retry_after,
    )
    with TraderStore(settings.database_dsn) as store:
        store.enqueue_delivery_retry(
            dedup_key=dedup_key,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
            message_text=message_text,
            delay_seconds=delay_seconds,
            error=error,
        )
    logger.warning(
        "Queued retry dedup=%s chat=%s thread=%s delay=%ss error=%s",
        dedup_key,
        chat_id,
        message_thread_id,
        delay_seconds,
        error,
    )
    return delay_seconds


async def _send_live_target(
    *,
    settings,
    http_session,
    dispatcher: DeliveryDispatcher,
    logger: logging.Logger,
    dedup_key: str,
    message_text: str,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None = None,
) -> DeliveryOutcome:
    try:
        await dispatcher.send(
            http_session,
            chat_id=chat_id,
            text=message_text,
            message_thread_id=message_thread_id,
        )
        return DeliveryOutcome(status="sent")
    except Exception as exc:
        category = _classify_delivery_error(exc)
        error_text = _short_error(exc)

        if category == "chat_unavailable" and trader_address is not None:
            if _is_forum_topic_delivery(
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
            ):
                logger.error(
                    "Forum chat unavailable for delivery chat_id=%s trader=%s thread=%s reason=%s",
                    chat_id,
                    trader_address,
                    message_thread_id,
                    error_text,
                )
                return DeliveryOutcome(status="dropped", category=category)
            await asyncio.to_thread(
                _deactivate_trader_target,
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
                logger=logger,
                reason=error_text,
            )
            return DeliveryOutcome(status="dropped", category=category)

        if category == "topic_missing" and message_thread_id is not None:
            if _is_forum_topic_delivery(
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
            ):
                await asyncio.to_thread(
                    _forget_forum_topic_mapping,
                    settings=settings,
                    chat_id=chat_id,
                    trader_address=trader_address,
                    message_thread_id=message_thread_id,
                    logger=logger,
                    reason=error_text,
                )
                return DeliveryOutcome(status="dropped", category=category)
            await asyncio.to_thread(
                _deactivate_trader_target,
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
                logger=logger,
                reason=error_text,
            )
            return DeliveryOutcome(status="dropped", category=category)

        if category in {"flood", "transient"}:
            retry_after = exc.retry_after if isinstance(exc, TelegramClientError) else None
            await asyncio.to_thread(
                _queue_initial_retry,
                settings=settings,
                dedup_key=dedup_key,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
                message_text=message_text,
                error=error_text,
                attempt_count=1,
                retry_after=retry_after,
                logger=logger,
            )
            if category == "flood":
                apply_backoff = getattr(dispatcher, "apply_global_backoff", None)
                if callable(apply_backoff):
                    await apply_backoff(retry_after=retry_after)
            return DeliveryOutcome(
                status="queued",
                category=category,
                retry_after=retry_after,
            )

        logger.warning(
            "Dropping delivery dedup=%s chat=%s thread=%s reason=%s",
            dedup_key,
            chat_id,
            message_thread_id,
            error_text,
        )
        return DeliveryOutcome(status="dropped", category=category)


async def _handle_retry_delivery_failure(
    *,
    settings,
    logger: logging.Logger,
    retry_id: int,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
    attempt_count: int,
    exc: Exception,
) -> tuple[str, str | None, int | None]:
    category = _classify_delivery_error(exc)
    error_text = _short_error(exc)

    if category == "chat_unavailable" and trader_address is not None:
        if _is_forum_topic_delivery(
            settings=settings,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
        ):
            await _store_call(
                settings,
                lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
            )
            return ("dead", category, None)
        await asyncio.to_thread(
            _deactivate_trader_target,
            settings=settings,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
            logger=logger,
            reason=error_text,
        )
        await _store_call(
            settings,
            lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
        )
        return ("dead", category, None)

    if category == "topic_missing" and message_thread_id is not None:
        if _is_forum_topic_delivery(
            settings=settings,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
        ):
            await asyncio.to_thread(
                _forget_forum_topic_mapping,
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
                logger=logger,
                reason=error_text,
            )
            await _store_call(
                settings,
                lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
            )
            return ("dead", category, None)
        await asyncio.to_thread(
            _deactivate_trader_target,
            settings=settings,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
            logger=logger,
            reason=error_text,
        )
        await _store_call(
            settings,
            lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
        )
        return ("dead", category, None)

    if category in {"flood", "transient"}:
        if int(attempt_count) >= RETRY_MAX_ATTEMPTS:
            await _store_call(
                settings,
                lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
            )
            logger.warning(
                "Retry attempts exhausted retry_id=%s attempts=%s error=%s",
                retry_id,
                attempt_count,
                error_text,
            )
            return ("dead", category, None)

        retry_after = exc.retry_after if isinstance(exc, TelegramClientError) else None
        delay_seconds = _retry_delay_seconds(
            attempt_count=attempt_count,
            retry_after=retry_after,
        )
        await _store_call(
            settings,
            lambda store: store.reschedule_delivery_retry(
                retry_id=retry_id,
                delay_seconds=delay_seconds,
                error=error_text,
            ),
        )
        return ("rescheduled", category, delay_seconds)

    await _store_call(
        settings,
        lambda store: store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text),
    )
    return ("dead", category, None)


async def _process_retry_queue(
    *,
    settings,
    http_session,
    dedup_store,
    dispatcher: DeliveryDispatcher,
    logger,
) -> int:
    jobs = await _store_call(
        settings,
        lambda store: store.list_due_delivery_retries(limit=RETRY_BATCH_LIMIT),
    )

    if not jobs:
        return 0

    sent = 0
    skipped = 0
    rescheduled = 0
    dead = 0
    global_flood_delay: int | None = None
    for job in jobs:
        if global_flood_delay is not None:
            await _store_call(
                settings,
                lambda store: store.reschedule_delivery_retry(
                    retry_id=job.id,
                    delay_seconds=global_flood_delay,
                    error="global_flood_backoff",
                ),
            )
            rescheduled += 1
            continue

        if await asyncio.to_thread(dedup_store.seen, job.dedup_key):
            await _store_call(
                settings,
                lambda store: store.mark_delivery_retry_sent(retry_id=job.id),
            )
            skipped += 1
            continue
        try:
            await dispatcher.send(
                http_session,
                chat_id=job.chat_id,
                text=job.message_text,
                message_thread_id=job.message_thread_id,
            )
            await _store_call(
                settings,
                lambda store: store.mark_delivery_retry_sent(retry_id=job.id),
            )
            await asyncio.to_thread(dedup_store.remember, job.dedup_key)
            sent += 1
        except Exception as exc:
            status, category, delay_seconds = await _handle_retry_delivery_failure(
                settings=settings,
                logger=logger,
                retry_id=job.id,
                chat_id=job.chat_id,
                trader_address=job.trader_address,
                message_thread_id=job.message_thread_id,
                attempt_count=job.attempt_count,
                exc=exc,
            )
            if status == "rescheduled":
                rescheduled += 1
                if category == "flood":
                    global_flood_delay = delay_seconds or int(
                        getattr(settings, "delivery_global_flood_default_retry_seconds", 30)
                    )
                    apply_backoff = getattr(dispatcher, "apply_global_backoff", None)
                    if callable(apply_backoff):
                        await apply_backoff(retry_after=global_flood_delay)
            else:
                dead += 1

    logger.info(
        "Retry queue processed due=%s sent=%s skipped=%s rescheduled=%s dead=%s",
        len(jobs),
        sent,
        skipped,
        rescheduled,
        dead,
    )
    return sent


async def _cleanup_expired_sessions(*, settings, http_session, logger) -> int:
    expired = await _store_call(
        settings,
        lambda store: store.expire_due_delivery_sessions(),
    )

    if not expired:
        return 0

    cleanup_ok = 0
    cleanup_failed = 0
    for item in expired:
        if item.message_thread_id is None:
            cleanup_ok += 1
            continue
        try:
            await delete_forum_topic(
                http_session,
                bot_token=settings.telegram_bot_token,
                chat_id=item.chat_id,
                message_thread_id=item.message_thread_id,
            )
            cleanup_ok += 1
        except TelegramClientError as exc:
            if exc.is_topic_missing() or exc.is_chat_unavailable():
                cleanup_ok += 1
                logger.info(
                    "Topic already unavailable during cleanup chat_id=%s thread=%s session_id=%s",
                    item.chat_id,
                    item.message_thread_id,
                    item.session_id,
                )
                continue
            cleanup_failed += 1
            logger.warning(
                "Failed to delete expired topic chat_id=%s thread=%s session_id=%s: %s",
                item.chat_id,
                item.message_thread_id,
                item.session_id,
                exc,
            )
            await _store_call(
                settings,
                lambda store: store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
                ),
            )
        except Exception as exc:
            cleanup_failed += 1
            logger.warning(
                "Failed to delete expired topic chat_id=%s thread=%s session_id=%s: %s",
                item.chat_id,
                item.message_thread_id,
                item.session_id,
                exc,
            )
            await _store_call(
                settings,
                lambda store: store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
                ),
            )

    logger.info(
        "Expired sessions cleanup: total=%s ok=%s failed=%s",
        len(expired),
        cleanup_ok,
        cleanup_failed,
    )
    return cleanup_ok


async def _run_cycle(
    *,
    settings,
    http_session,
    dedup_store,
    dispatcher: DeliveryDispatcher,
    logger,
) -> int:
    source_configs = load_sources_config(settings.sources_config_path)
    sources = [
        build_source(cfg, http_session=http_session, settings=settings)
        for cfg in source_configs
    ]

    all_signals = []
    for source in sources:
        try:
            signals = await source.fetch_signals()
            logger.info("Fetched %s signal(s) from %s", len(signals), source.id)
            all_signals.extend(signals)
        except Exception as exc:
            logger.exception("Source %s failed: %s", source.id, exc)

    if not all_signals:
        logger.info("No signals fetched this cycle")
        return 0

    forum_chat_id = _forum_chat_id(settings)
    forum_mode_enabled = bool(forum_chat_id)
    topic_cache: dict[str, int] = {}
    subscriber_map: dict[str, list] = {}
    if not forum_mode_enabled:
        subscriber_map = await _store_call(
            settings,
            lambda store: store.list_active_delivery_targets_by_trader(),
        )

    published = 0
    for signal in sorted(all_signals, key=_sort_key):
        if published >= settings.max_signals_per_cycle:
            break

        dedup_key = signal.dedup_key()
        if await asyncio.to_thread(dedup_store.seen, dedup_key):
            continue

        delivered = 0
        failed = 0
        queued = 0
        try:
            text = format_signal(signal)
            delivery_targets_payload: list[tuple[str | int, str | None, int | None]] = []
            signal_trader = _normalize_trader_address(signal.trader_address)
            if forum_mode_enabled and signal_trader:
                thread_id = await _ensure_forum_topic_for_trader(
                    settings=settings,
                    http_session=http_session,
                    logger=logger,
                    chat_id=forum_chat_id,
                    trader_address=signal_trader,
                    topic_cache=topic_cache,
                )
                if thread_id is not None:
                    delivery_targets_payload.append((forum_chat_id, signal_trader, thread_id))
                elif settings.telegram_channel_id:
                    logger.warning(
                        "Forum topic unavailable for trader=%s, fallback to channel root",
                        signal_trader,
                    )
                    delivery_targets_payload.append(
                        (settings.telegram_channel_id, signal_trader, None)
                    )
            elif forum_mode_enabled and settings.telegram_channel_id:
                delivery_targets_payload.append((settings.telegram_channel_id, None, None))
            else:
                if settings.telegram_channel_id and not settings.monitor_delivery_only_subscribed:
                    delivery_targets_payload.append(
                        (
                            settings.telegram_channel_id,
                            signal_trader,
                            None,
                        )
                    )
                delivery_targets = (
                    subscriber_map.get(signal_trader, []) if signal_trader is not None else []
                )
                seen_sub_targets: set[tuple[str, int | None]] = set()
                for target in delivery_targets:
                    target_key = (target.chat_id, target.message_thread_id)
                    if target_key in seen_sub_targets:
                        continue
                    seen_sub_targets.add(target_key)
                    delivery_targets_payload.append(
                        (
                            target.chat_id,
                            target.trader_address,
                            target.message_thread_id,
                        )
                    )

            if not delivery_targets_payload:
                logger.info("No targets for signal %s; skipping", dedup_key)
                continue

            flood_backoff_triggered = False
            flood_retry_after: int | None = None
            for idx, (chat_id, trader_address, message_thread_id) in enumerate(
                delivery_targets_payload
            ):
                if flood_backoff_triggered:
                    await asyncio.to_thread(
                        _queue_initial_retry,
                        settings=settings,
                        dedup_key=dedup_key,
                        chat_id=chat_id,
                        trader_address=trader_address,
                        message_thread_id=message_thread_id,
                        message_text=text,
                        error="global_flood_backoff",
                        attempt_count=1,
                        retry_after=flood_retry_after,
                        logger=logger,
                    )
                    queued += 1
                    failed += 1
                    continue

                outcome = await _send_live_target(
                    settings=settings,
                    http_session=http_session,
                    dispatcher=dispatcher,
                    logger=logger,
                    dedup_key=dedup_key,
                    message_text=text,
                    chat_id=chat_id,
                    trader_address=trader_address,
                    message_thread_id=message_thread_id,
                )
                if outcome.status == "sent":
                    delivered += 1
                else:
                    failed += 1
                    if outcome.status == "queued":
                        queued += 1
                    if outcome.category == "flood":
                        flood_backoff_triggered = True
                        flood_retry_after = outcome.retry_after
                        remaining = len(delivery_targets_payload) - (idx + 1)
                        if remaining > 0:
                            logger.warning(
                                "Flood detected while publishing signal %s, queueing remaining targets=%s",
                                dedup_key,
                                remaining,
                            )
        except Exception as exc:
            logger.exception("Failed to publish signal %s: %s", dedup_key, exc)
            continue

        if delivered <= 0:
            if queued > 0 or failed > 0:
                await asyncio.to_thread(dedup_store.remember, dedup_key)
                logger.info(
                    "Signal %s had no live success queued=%s failed=%s; dedup marked",
                    dedup_key,
                    queued,
                    failed,
                )
            else:
                logger.warning("Signal %s had no successful deliveries", dedup_key)
            continue

        await asyncio.to_thread(dedup_store.remember, dedup_key)
        published += 1
        logger.info(
            "Published signal %s to %s target(s), failed=%s queued=%s",
            dedup_key,
            delivered,
            failed,
            queued,
        )

    logger.info("Cycle complete. Published %s signal(s)", published)
    return published


async def _run() -> None:
    args = _parse_args()

    try:
        settings = load_settings()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc
    setup_logging(
        service_name="cryptoinsider.poster",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.poster")
    set_telegram_http_logging(settings.log_telegram_http)
    logger.info(
        "Service started poll_interval=%s max_signals_per_cycle=%s database_driver=%s",
        settings.poll_interval_seconds,
        settings.max_signals_per_cycle,
        ("postgres" if settings.database_dsn.startswith("postgres") else "sqlite"),
    )

    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    dedup_store = DedupStore(
        settings.database_dsn,
        retention_days=settings.dedup_retention_days,
    )
    dispatcher = DeliveryDispatcher(
        config=DeliveryDispatcherConfig(
            bot_token=settings.telegram_bot_token,
            send_concurrency=settings.delivery_send_concurrency,
            chat_min_interval_ms=settings.delivery_chat_min_interval_ms,
            global_flood_default_retry_seconds=settings.delivery_global_flood_default_retry_seconds,
            global_flood_max_retry_seconds=settings.delivery_global_flood_max_retry_seconds,
        )
    )
    cycle = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            await _validate_forum_permissions(
                settings=settings,
                http_session=http_session,
                logger=logger,
            )
            while True:
                cycle += 1
                cycle_started = time.monotonic()
                with bind_log_context(cycle=cycle, cycle_id=new_trace_id("poster")):
                    await _cleanup_expired_sessions(
                        settings=settings,
                        http_session=http_session,
                        logger=logger,
                    )
                    await _process_retry_queue(
                        settings=settings,
                        http_session=http_session,
                        dedup_store=dedup_store,
                        dispatcher=dispatcher,
                        logger=logger,
                    )
                    await _run_cycle(
                        settings=settings,
                        http_session=http_session,
                        dedup_store=dedup_store,
                        dispatcher=dispatcher,
                        logger=logger,
                    )
                    removed = await asyncio.to_thread(dedup_store.cleanup_if_due)
                    if removed > 0:
                        logger.info("Dedup retention cleanup removed=%s", removed)
                    elapsed_ms = int((time.monotonic() - cycle_started) * 1000)
                    logger.info("Cycle finished elapsed_ms=%s", elapsed_ms)
                if args.once:
                    break
                await asyncio.sleep(settings.poll_interval_seconds)
    finally:
        dedup_store.close()


if __name__ == "__main__":
    asyncio.run(_run())
