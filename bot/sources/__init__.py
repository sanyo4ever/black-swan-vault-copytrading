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
        merged_cfg = dict(source_cfg)
        merged_cfg.setdefault("trader_limit", settings.monitor_max_targets_per_cycle)
        forum_mode_enabled = bool(str(getattr(settings, "telegram_forum_chat_id", "")).strip())
        showcase_mode_enabled = bool(getattr(settings, "showcase_mode_enabled", False))
        if forum_mode_enabled:
            # Shared channel/forum mode tracks the global pool, not per-user subscriptions.
            merged_cfg["delivery_only_subscribed"] = False
        else:
            merged_cfg.setdefault(
                "delivery_only_subscribed",
                settings.monitor_delivery_only_subscribed,
            )
        if showcase_mode_enabled:
            # Showcase mode uses delivery monitor state sourced from showcase_wallets.
            merged_cfg["delivery_only_subscribed"] = True
            merged_cfg["showcase_mode"] = True
            merged_cfg["trader_limit"] = max(1, int(settings.showcase_slots))
            merged_cfg["max_traders_per_cycle"] = max(1, int(settings.showcase_slots))
        merged_cfg.setdefault(
            "max_traders_per_cycle",
            settings.delivery_monitor_max_traders_per_cycle,
        )
        merged_cfg.setdefault(
            "base_poll_seconds",
            settings.delivery_monitor_base_poll_seconds,
        )
        merged_cfg.setdefault(
            "min_poll_seconds",
            settings.delivery_monitor_min_poll_seconds,
        )
        merged_cfg.setdefault(
            "max_poll_seconds",
            settings.delivery_monitor_max_poll_seconds,
        )
        merged_cfg.setdefault(
            "priority_recency_minutes",
            settings.delivery_monitor_priority_recency_minutes,
        )
        merged_cfg.setdefault(
            "safety_lookback_seconds",
            settings.delivery_monitor_safety_lookback_seconds,
        )
        merged_cfg.setdefault(
            "bootstrap_lookback_minutes",
            settings.delivery_monitor_bootstrap_lookback_minutes,
        )
        merged_cfg.setdefault(
            "http_concurrency",
            settings.delivery_monitor_http_concurrency,
        )
        return HyperliquidFuturesSource(
            merged_cfg,
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
