from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime

import aiohttp

from bot.config import ConfigError, load_settings, load_sources_config
from bot.dedup import DedupStore
from bot.formatter import format_signal
from bot.sources import build_source
from bot.telegram_client import delete_forum_topic, send_message
from bot.trader_store import TraderStore


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


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


async def _run_cycle(*, settings, http_session, dedup_store, logger) -> int:
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

        channel_target_count = 1 if settings.telegram_channel_id else 0
        delivery_targets = []
        if signal.trader_address:
            delivery_targets = subscriber_map.get(signal.trader_address, [])

        if channel_target_count == 0 and not delivery_targets:
            logger.info("No targets for signal %s; skipping", dedup_key)
            continue

        delivered = 0
        failed = 0
        try:
            text = format_signal(signal)

            if settings.telegram_channel_id:
                try:
                    await send_message(
                        http_session,
                        bot_token=settings.telegram_bot_token,
                        chat_id=settings.telegram_channel_id,
                        text=text,
                    )
                    delivered += 1
                except Exception as exc:
                    failed += 1
                    logger.exception(
                        "Failed to send signal %s to channel %s: %s",
                        dedup_key,
                        settings.telegram_channel_id,
                        exc,
                    )

            seen_sub_targets: set[tuple[str, int | None]] = set()
            for target in delivery_targets:
                target_key = (target.chat_id, target.message_thread_id)
                if target_key in seen_sub_targets:
                    continue
                seen_sub_targets.add(target_key)
                try:
                    await send_message(
                        http_session,
                        bot_token=settings.telegram_bot_token,
                        chat_id=target.chat_id,
                        text=text,
                        message_thread_id=target.message_thread_id,
                    )
                    delivered += 1
                except Exception as exc:
                    failed += 1
                    logger.exception(
                        "Failed to send signal %s to chat=%s thread=%s: %s",
                        dedup_key,
                        target.chat_id,
                        target.message_thread_id,
                        exc,
                    )
        except Exception as exc:
            logger.exception("Failed to publish signal %s: %s", dedup_key, exc)
            continue

        if delivered <= 0:
            logger.warning("Signal %s had no successful deliveries", dedup_key)
            continue

        dedup_store.remember(dedup_key)
        published += 1
        logger.info(
            "Published signal %s to %s target(s), failed=%s",
            dedup_key,
            delivered,
            failed,
        )

    logger.info("Cycle complete. Published %s signal(s)", published)
    return published


async def _run() -> None:
    _setup_logging()
    logger = logging.getLogger("cryptoinsider.bot")
    args = _parse_args()

    try:
        settings = load_settings()
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc

    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    dedup_store = DedupStore(settings.database_dsn)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            while True:
                await _cleanup_expired_sessions(
                    settings=settings,
                    http_session=http_session,
                    logger=logger,
                )
                await _run_cycle(
                    settings=settings,
                    http_session=http_session,
                    dedup_store=dedup_store,
                    logger=logger,
                )
                if args.once:
                    break
                await asyncio.sleep(settings.poll_interval_seconds)
    finally:
        dedup_store.close()


if __name__ == "__main__":
    asyncio.run(_run())
