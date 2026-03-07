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
from bot.telegram_client import send_message


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

    published = 0
    for signal in sorted(all_signals, key=_sort_key):
        if published >= settings.max_signals_per_cycle:
            break

        dedup_key = signal.dedup_key()
        if dedup_store.seen(dedup_key):
            continue

        try:
            await send_message(
                http_session,
                bot_token=settings.telegram_bot_token,
                chat_id=settings.telegram_channel_id,
                text=format_signal(signal),
            )
            dedup_store.remember(dedup_key)
            published += 1
            logger.info("Published signal %s", dedup_key)
        except Exception as exc:
            logger.exception("Failed to publish signal %s: %s", dedup_key, exc)

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
    dedup_store = DedupStore(settings.database_path)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            while True:
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
