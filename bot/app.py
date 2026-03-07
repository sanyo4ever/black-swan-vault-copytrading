from __future__ import annotations

import argparse
import asyncio
import logging
import time
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
    delete_forum_topic,
    set_telegram_http_logging,
)
from bot.trader_store import TraderStore

RETRY_BASE_DELAY_SECONDS = 15
RETRY_MAX_DELAY_SECONDS = 15 * 60
RETRY_MAX_ATTEMPTS = 8
RETRY_BATCH_LIMIT = 200


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


def _deactivate_chat_targets(
    *,
    settings,
    chat_id: str | int,
    logger: logging.Logger,
    reason: str,
) -> None:
    with TraderStore(settings.database_dsn) as store:
        sessions = store.cancel_all_chat_subscriptions(chat_id=chat_id)
        dropped = store.delete_pending_retries_for_chat(chat_id=chat_id)
    logger.warning(
        "Deactivated chat targets chat_id=%s sessions=%s pending_retries_deleted=%s reason=%s",
        chat_id,
        len(sessions),
        dropped,
        reason,
    )


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
) -> None:
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
) -> str:
    try:
        await dispatcher.send(
            http_session,
            chat_id=chat_id,
            text=message_text,
            message_thread_id=message_thread_id,
        )
        return "sent"
    except Exception as exc:
        category = _classify_delivery_error(exc)
        error_text = _short_error(exc)

        if (
            category == "chat_unavailable"
            and trader_address is not None
            and message_thread_id is not None
        ):
            _deactivate_chat_targets(
                settings=settings,
                chat_id=chat_id,
                logger=logger,
                reason=error_text,
            )
            return "dropped"

        if category == "topic_missing" and message_thread_id is not None:
            _deactivate_trader_target(
                settings=settings,
                chat_id=chat_id,
                trader_address=trader_address,
                message_thread_id=message_thread_id,
                logger=logger,
                reason=error_text,
            )
            return "dropped"

        if category in {"flood", "transient"}:
            retry_after = exc.retry_after if isinstance(exc, TelegramClientError) else None
            _queue_initial_retry(
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
            return "queued"

        logger.warning(
            "Dropping delivery dedup=%s chat=%s thread=%s reason=%s",
            dedup_key,
            chat_id,
            message_thread_id,
            error_text,
        )
        return "dropped"


def _handle_retry_delivery_failure(
    *,
    settings,
    logger: logging.Logger,
    retry_id: int,
    chat_id: str | int,
    trader_address: str | None,
    message_thread_id: int | None,
    attempt_count: int,
    exc: Exception,
) -> str:
    category = _classify_delivery_error(exc)
    error_text = _short_error(exc)

    if (
        category == "chat_unavailable"
        and trader_address is not None
        and message_thread_id is not None
    ):
        _deactivate_chat_targets(
            settings=settings,
            chat_id=chat_id,
            logger=logger,
            reason=error_text,
        )
        with TraderStore(settings.database_dsn) as store:
            store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text)
        return "dead"

    if category == "topic_missing" and message_thread_id is not None:
        _deactivate_trader_target(
            settings=settings,
            chat_id=chat_id,
            trader_address=trader_address,
            message_thread_id=message_thread_id,
            logger=logger,
            reason=error_text,
        )
        with TraderStore(settings.database_dsn) as store:
            store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text)
        return "dead"

    if category in {"flood", "transient"}:
        if int(attempt_count) >= RETRY_MAX_ATTEMPTS:
            with TraderStore(settings.database_dsn) as store:
                store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text)
            logger.warning(
                "Retry attempts exhausted retry_id=%s attempts=%s error=%s",
                retry_id,
                attempt_count,
                error_text,
            )
            return "dead"

        retry_after = exc.retry_after if isinstance(exc, TelegramClientError) else None
        delay_seconds = _retry_delay_seconds(
            attempt_count=attempt_count,
            retry_after=retry_after,
        )
        with TraderStore(settings.database_dsn) as store:
            store.reschedule_delivery_retry(
                retry_id=retry_id,
                delay_seconds=delay_seconds,
                error=error_text,
            )
        return "rescheduled"

    with TraderStore(settings.database_dsn) as store:
        store.mark_delivery_retry_dead(retry_id=retry_id, error=error_text)
    return "dead"


async def _process_retry_queue(
    *,
    settings,
    http_session,
    dedup_store,
    dispatcher: DeliveryDispatcher,
    logger,
) -> int:
    with TraderStore(settings.database_dsn) as store:
        jobs = store.list_due_delivery_retries(limit=RETRY_BATCH_LIMIT)

    if not jobs:
        return 0

    sent = 0
    rescheduled = 0
    dead = 0
    for job in jobs:
        try:
            await dispatcher.send(
                http_session,
                chat_id=job.chat_id,
                text=job.message_text,
                message_thread_id=job.message_thread_id,
            )
            with TraderStore(settings.database_dsn) as store:
                store.mark_delivery_retry_sent(retry_id=job.id)
            dedup_store.remember(job.dedup_key)
            sent += 1
        except Exception as exc:
            outcome = _handle_retry_delivery_failure(
                settings=settings,
                logger=logger,
                retry_id=job.id,
                chat_id=job.chat_id,
                trader_address=job.trader_address,
                message_thread_id=job.message_thread_id,
                attempt_count=job.attempt_count,
                exc=exc,
            )
            if outcome == "rescheduled":
                rescheduled += 1
            else:
                dead += 1

    logger.info(
        "Retry queue processed due=%s sent=%s rescheduled=%s dead=%s",
        len(jobs),
        sent,
        rescheduled,
        dead,
    )
    return sent


async def _cleanup_expired_sessions(*, settings, http_session, logger) -> int:
    with TraderStore(settings.database_dsn) as store:
        expired = store.expire_due_delivery_sessions()

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
            with TraderStore(settings.database_dsn) as store:
                store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
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
            with TraderStore(settings.database_dsn) as store:
                store.set_delivery_session_cleanup_error(
                    session_id=item.session_id,
                    error=str(exc),
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

    with TraderStore(settings.database_dsn) as store:
        subscriber_map = store.list_active_delivery_targets_by_trader()

    published = 0
    for signal in sorted(all_signals, key=_sort_key):
        if published >= settings.max_signals_per_cycle:
            break

        dedup_key = signal.dedup_key()
        if dedup_store.seen(dedup_key):
            continue

        channel_target_count = 0
        if settings.telegram_channel_id and not settings.monitor_delivery_only_subscribed:
            channel_target_count = 1
        delivery_targets = []
        if signal.trader_address:
            delivery_targets = subscriber_map.get(signal.trader_address, [])

        if channel_target_count == 0 and not delivery_targets:
            logger.info("No targets for signal %s; skipping", dedup_key)
            continue

        delivered = 0
        failed = 0
        queued = 0
        try:
            text = format_signal(signal)
            delivery_tasks: list[asyncio.Task[str]] = []
            if settings.telegram_channel_id and not settings.monitor_delivery_only_subscribed:
                delivery_tasks.append(
                    asyncio.create_task(
                        _send_live_target(
                            settings=settings,
                            http_session=http_session,
                            dispatcher=dispatcher,
                            logger=logger,
                            dedup_key=dedup_key,
                            message_text=text,
                            chat_id=settings.telegram_channel_id,
                            trader_address=signal.trader_address,
                        )
                    )
                )
            seen_sub_targets: set[tuple[str, int | None]] = set()
            for target in delivery_targets:
                target_key = (target.chat_id, target.message_thread_id)
                if target_key in seen_sub_targets:
                    continue
                seen_sub_targets.add(target_key)
                delivery_tasks.append(
                    asyncio.create_task(
                        _send_live_target(
                            settings=settings,
                            http_session=http_session,
                            dispatcher=dispatcher,
                            logger=logger,
                            dedup_key=dedup_key,
                            message_text=text,
                            chat_id=target.chat_id,
                            trader_address=target.trader_address,
                            message_thread_id=target.message_thread_id,
                        )
                    )
                )
            results = await asyncio.gather(*delivery_tasks)
            for status in results:
                if status == "sent":
                    delivered += 1
                else:
                    failed += 1
                    if status == "queued":
                        queued += 1
        except Exception as exc:
            logger.exception("Failed to publish signal %s: %s", dedup_key, exc)
            continue

        if delivered <= 0:
            logger.warning("Signal %s had no successful deliveries", dedup_key)
            continue

        dedup_store.remember(dedup_key)
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
    dedup_store = DedupStore(settings.database_dsn)
    dispatcher = DeliveryDispatcher(
        config=DeliveryDispatcherConfig(
            bot_token=settings.telegram_bot_token,
            send_concurrency=settings.delivery_send_concurrency,
            chat_min_interval_ms=settings.delivery_chat_min_interval_ms,
        )
    )
    cycle = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
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
                    await _process_retry_queue(
                        settings=settings,
                        http_session=http_session,
                        dedup_store=dedup_store,
                        dispatcher=dispatcher,
                        logger=logger,
                    )
                    elapsed_ms = int((time.monotonic() - cycle_started) * 1000)
                    logger.info("Cycle finished elapsed_ms=%s", elapsed_ms)
                if args.once:
                    break
                await asyncio.sleep(settings.poll_interval_seconds)
    finally:
        dedup_store.close()


if __name__ == "__main__":
    asyncio.run(_run())
