from bot.sources.base import Source
from bot.sources.hyperliquid_futures import HyperliquidFuturesSource
from bot.sources.json_api import JsonApiSource
from bot.sources.rss import RssSource


def build_source(source_cfg: dict, *, http_session, settings):
    source_type = str(source_cfg["type"]).strip().lower()

    if source_type == "rss":
        return RssSource(source_cfg, http_session=http_session)
    if source_type == "json_api":
        return JsonApiSource(source_cfg, http_session=http_session)
    if source_type == "hyperliquid_futures":
        return HyperliquidFuturesSource(
            source_cfg,
            http_session=http_session,
            database_dsn=settings.database_dsn,
            info_url=settings.hyperliquid_info_url,
        )

    raise ValueError(f"Unsupported source type: {source_type}")


__all__ = [
    "Source",
    "RssSource",
    "JsonApiSource",
    "HyperliquidFuturesSource",
    "build_source",
]
