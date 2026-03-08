from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_channel_id: str
    telegram_forum_chat_id: str
    telegram_join_url: str
    telegram_bot_username: str
    sources_config_path: Path
    poll_interval_seconds: int
    database_dsn: str
    max_signals_per_cycle: int
    http_timeout_seconds: int
    hyperliquid_info_url: str
    discovery_candidate_limit: int
    discovery_min_age_days: int
    discovery_min_trades_30d: int
    discovery_min_active_days_30d: int
    discovery_min_win_rate_30d: float
    discovery_max_drawdown_30d_pct: float
    discovery_max_last_activity_minutes: int
    discovery_min_realized_pnl_30d: float
    discovery_require_positive_pnl_30d: bool
    discovery_min_trades_7d: int
    discovery_window_hours: int
    discovery_concurrency: int
    discovery_fill_cap_hint: int
    discovery_age_probe_enabled: bool
    discovery_seed_addresses: tuple[str, ...]
    nansen_api_url: str
    nansen_api_key: str
    nansen_candidate_limit: int
    discovery_interval_seconds: int
    showcase_mode_enabled: bool
    showcase_slots: int
    rotation_scout_interval_hours: int
    rotation_health_interval_minutes: int
    rotation_stale_hours: int
    rotation_stale_cycles: int
    rotation_score_threshold_pct: float
    rotation_scout_candidates: int
    rotation_bootstrap_candidates: int
    rotation_metrics_refresh_hours: int
    admin_panel_username: str
    admin_panel_password: str
    google_analytics_measurement_id: str
    subscription_lifetime_hours: int
    universe_interval_seconds: int
    universe_min_age_days: int
    universe_min_trades_30d: int
    universe_min_active_days_30d: int
    universe_min_win_rate_30d: float
    universe_max_drawdown_30d_pct: float
    universe_max_last_activity_minutes: int
    universe_min_realized_pnl_30d: float
    universe_min_score: float
    universe_max_size: int
    live_top100_interval_seconds: int
    live_top100_active_window_minutes: int
    live_top100_size: int
    trader_listed_within_minutes: int
    trader_stale_after_minutes: int
    trader_archive_after_days: int
    monitor_hot_size: int
    monitor_warm_size: int
    monitor_hot_poll_seconds: int
    monitor_warm_poll_seconds: int
    monitor_cold_poll_seconds: int
    monitor_hot_recency_minutes: int
    monitor_warm_recency_minutes: int
    monitor_max_targets_per_cycle: int
    monitor_delivery_only_subscribed: bool
    delivery_monitor_base_poll_seconds: int
    delivery_monitor_min_poll_seconds: int
    delivery_monitor_max_poll_seconds: int
    delivery_monitor_priority_recency_minutes: int
    delivery_monitor_safety_lookback_seconds: int
    delivery_monitor_bootstrap_lookback_minutes: int
    delivery_monitor_max_traders_per_cycle: int
    delivery_monitor_http_concurrency: int
    delivery_send_concurrency: int
    delivery_chat_min_interval_ms: int
    delivery_global_flood_default_retry_seconds: int
    delivery_global_flood_max_retry_seconds: int
    dedup_retention_days: int
    subscriber_update_concurrency: int
    subscriber_telegram_retry_attempts: int
    log_level: str
    log_format: str
    log_directory: str
    log_file_max_bytes: int
    log_file_backup_count: int
    log_telegram_http: bool


class ConfigError(RuntimeError):
    pass


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "on"}


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _get_log_format() -> str:
    raw = os.getenv("LOG_FORMAT", "text").strip().lower()
    if raw in {"text", "json"}:
        return raw
    return "text"


def _get_csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return ()
    values = [item.strip() for item in raw.split(",")]
    normalized = [item for item in values if item]
    return tuple(normalized)


def load_settings(
    *,
    require_telegram: bool = True,
    require_admin_password: bool = False,
) -> Settings:
    load_dotenv()

    if require_telegram:
        telegram_bot_token = _get_required_env("TELEGRAM_BOT_TOKEN")
        telegram_channel_id = _get_required_env("TELEGRAM_CHANNEL_ID")
    else:
        telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        telegram_channel_id = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()

    admin_panel_username = os.getenv("ADMIN_PANEL_USERNAME", "admin").strip() or "admin"
    admin_panel_password = os.getenv("ADMIN_PANEL_PASSWORD", "").strip()
    if require_admin_password and not admin_panel_password:
        raise ConfigError(
            "Missing ADMIN_PANEL_PASSWORD. Set it in .env before starting admin server."
        )

    return Settings(
        telegram_bot_token=telegram_bot_token,
        telegram_channel_id=telegram_channel_id,
        telegram_forum_chat_id=(
            os.getenv("TELEGRAM_FORUM_CHAT_ID", "").strip() or telegram_channel_id
        ),
        telegram_join_url=os.getenv("TELEGRAM_JOIN_URL", "").strip(),
        telegram_bot_username=os.getenv("TELEGRAM_BOT_USERNAME", "").strip().removeprefix("@"),
        sources_config_path=Path(os.getenv("SOURCES_CONFIG_PATH", "config/sources.yaml")),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        database_dsn=(
            os.getenv("DATABASE_URL", "").strip()
            or os.getenv("DATABASE_PATH", "data/signals.db").strip()
        ),
        max_signals_per_cycle=int(os.getenv("MAX_SIGNALS_PER_CYCLE", "20")),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        hyperliquid_info_url=os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info"),
        discovery_candidate_limit=int(os.getenv("DISCOVERY_CANDIDATE_LIMIT", "60")),
        discovery_min_age_days=int(os.getenv("DISCOVERY_MIN_AGE_DAYS", "30")),
        discovery_min_trades_30d=int(os.getenv("DISCOVERY_MIN_TRADES_30D", "120")),
        discovery_min_active_days_30d=int(os.getenv("DISCOVERY_MIN_ACTIVE_DAYS_30D", "12")),
        discovery_min_win_rate_30d=float(os.getenv("DISCOVERY_MIN_WIN_RATE_30D", "0.52")),
        discovery_max_drawdown_30d_pct=float(
            os.getenv("DISCOVERY_MAX_DRAWDOWN_30D_PCT", "25.0")
        ),
        discovery_max_last_activity_minutes=int(
            os.getenv("DISCOVERY_MAX_LAST_ACTIVITY_MINUTES", "60")
        ),
        discovery_min_realized_pnl_30d=float(
            os.getenv("DISCOVERY_MIN_REALIZED_PNL_30D", "0.0")
        ),
        discovery_require_positive_pnl_30d=_get_bool_env(
            "DISCOVERY_REQUIRE_POSITIVE_PNL_30D",
            True,
        ),
        discovery_min_trades_7d=int(os.getenv("DISCOVERY_MIN_TRADES_7D", "1")),
        discovery_window_hours=int(os.getenv("DISCOVERY_WINDOW_HOURS", "24")),
        discovery_concurrency=int(os.getenv("DISCOVERY_CONCURRENCY", "6")),
        discovery_fill_cap_hint=int(os.getenv("DISCOVERY_FILL_CAP_HINT", "1900")),
        discovery_age_probe_enabled=_get_bool_env("DISCOVERY_AGE_PROBE_ENABLED", True),
        discovery_seed_addresses=_get_csv_env("DISCOVERY_SEED_ADDRESSES"),
        nansen_api_url=os.getenv("NANSEN_API_URL", "https://api.nansen.ai"),
        nansen_api_key=os.getenv("NANSEN_API_KEY", "").strip(),
        nansen_candidate_limit=int(os.getenv("NANSEN_CANDIDATE_LIMIT", "60")),
        discovery_interval_seconds=int(os.getenv("DISCOVERY_INTERVAL_SECONDS", "900")),
        showcase_mode_enabled=_get_bool_env("SHOWCASE_MODE_ENABLED", False),
        showcase_slots=max(1, int(os.getenv("SHOWCASE_SLOTS", "25"))),
        rotation_scout_interval_hours=max(
            1,
            int(os.getenv("ROTATION_SCOUT_INTERVAL_HOURS", "12")),
        ),
        rotation_health_interval_minutes=max(
            1,
            int(os.getenv("ROTATION_HEALTH_INTERVAL_MINUTES", "60")),
        ),
        rotation_stale_hours=max(1, int(os.getenv("ROTATION_STALE_HOURS", "72"))),
        rotation_stale_cycles=max(1, int(os.getenv("ROTATION_STALE_CYCLES", "3"))),
        rotation_score_threshold_pct=max(
            0.0,
            float(os.getenv("ROTATION_SCORE_THRESHOLD_PCT", "5")),
        ),
        rotation_scout_candidates=max(
            1,
            int(os.getenv("ROTATION_SCOUT_CANDIDATES", "30")),
        ),
        rotation_bootstrap_candidates=max(
            1,
            int(os.getenv("ROTATION_BOOTSTRAP_CANDIDATES", "60")),
        ),
        rotation_metrics_refresh_hours=max(
            1,
            int(os.getenv("ROTATION_METRICS_REFRESH_HOURS", "24")),
        ),
        admin_panel_username=admin_panel_username,
        admin_panel_password=admin_panel_password,
        google_analytics_measurement_id=os.getenv(
            "GOOGLE_ANALYTICS_MEASUREMENT_ID",
            "",
        ).strip(),
        subscription_lifetime_hours=int(os.getenv("SUBSCRIPTION_LIFETIME_HOURS", "0")),
        universe_interval_seconds=int(os.getenv("UNIVERSE_INTERVAL_SECONDS", "300")),
        universe_min_age_days=int(os.getenv("UNIVERSE_MIN_AGE_DAYS", "30")),
        universe_min_trades_30d=int(os.getenv("UNIVERSE_MIN_TRADES_30D", "120")),
        universe_min_active_days_30d=int(os.getenv("UNIVERSE_MIN_ACTIVE_DAYS_30D", "12")),
        universe_min_win_rate_30d=float(os.getenv("UNIVERSE_MIN_WIN_RATE_30D", "0.52")),
        universe_max_drawdown_30d_pct=float(
            os.getenv("UNIVERSE_MAX_DRAWDOWN_30D_PCT", "25.0")
        ),
        universe_max_last_activity_minutes=int(
            os.getenv("UNIVERSE_MAX_LAST_ACTIVITY_MINUTES", "60")
        ),
        universe_min_realized_pnl_30d=float(
            os.getenv("UNIVERSE_MIN_REALIZED_PNL_30D", "0.000001")
        ),
        universe_min_score=float(os.getenv("UNIVERSE_MIN_SCORE", "0.0")),
        universe_max_size=int(os.getenv("UNIVERSE_MAX_SIZE", "3000")),
        live_top100_interval_seconds=int(os.getenv("LIVE_TOP100_INTERVAL_SECONDS", "60")),
        live_top100_active_window_minutes=int(
            os.getenv("LIVE_TOP100_ACTIVE_WINDOW_MINUTES", "60")
        ),
        live_top100_size=int(os.getenv("LIVE_TOP100_SIZE", "100")),
        trader_listed_within_minutes=int(os.getenv("TRADER_LISTED_WITHIN_MINUTES", "60")),
        trader_stale_after_minutes=int(os.getenv("TRADER_STALE_AFTER_MINUTES", "4320")),
        trader_archive_after_days=int(os.getenv("TRADER_ARCHIVE_AFTER_DAYS", "180")),
        monitor_hot_size=int(os.getenv("MONITOR_HOT_SIZE", "100")),
        monitor_warm_size=int(os.getenv("MONITOR_WARM_SIZE", "400")),
        monitor_hot_poll_seconds=int(os.getenv("MONITOR_HOT_POLL_SECONDS", "60")),
        monitor_warm_poll_seconds=int(os.getenv("MONITOR_WARM_POLL_SECONDS", "600")),
        monitor_cold_poll_seconds=int(os.getenv("MONITOR_COLD_POLL_SECONDS", "3600")),
        monitor_hot_recency_minutes=int(os.getenv("MONITOR_HOT_RECENCY_MINUTES", "60")),
        monitor_warm_recency_minutes=int(os.getenv("MONITOR_WARM_RECENCY_MINUTES", "360")),
        monitor_max_targets_per_cycle=int(os.getenv("MONITOR_MAX_TARGETS_PER_CYCLE", "120")),
        monitor_delivery_only_subscribed=_get_bool_env(
            "MONITOR_DELIVERY_ONLY_SUBSCRIBED",
            True,
        ),
        delivery_monitor_base_poll_seconds=int(
            os.getenv("DELIVERY_MONITOR_BASE_POLL_SECONDS", "60")
        ),
        delivery_monitor_min_poll_seconds=int(
            os.getenv("DELIVERY_MONITOR_MIN_POLL_SECONDS", "20")
        ),
        delivery_monitor_max_poll_seconds=int(
            os.getenv("DELIVERY_MONITOR_MAX_POLL_SECONDS", "180")
        ),
        delivery_monitor_priority_recency_minutes=int(
            os.getenv("DELIVERY_MONITOR_PRIORITY_RECENCY_MINUTES", "120")
        ),
        delivery_monitor_safety_lookback_seconds=int(
            os.getenv("DELIVERY_MONITOR_SAFETY_LOOKBACK_SECONDS", "90")
        ),
        delivery_monitor_bootstrap_lookback_minutes=int(
            os.getenv("DELIVERY_MONITOR_BOOTSTRAP_LOOKBACK_MINUTES", "180")
        ),
        delivery_monitor_max_traders_per_cycle=int(
            os.getenv("DELIVERY_MONITOR_MAX_TRADERS_PER_CYCLE", "120")
        ),
        delivery_monitor_http_concurrency=int(
            os.getenv("DELIVERY_MONITOR_HTTP_CONCURRENCY", "8")
        ),
        delivery_send_concurrency=int(os.getenv("DELIVERY_SEND_CONCURRENCY", "12")),
        delivery_chat_min_interval_ms=int(
            os.getenv("DELIVERY_CHAT_MIN_INTERVAL_MS", "250")
        ),
        delivery_global_flood_default_retry_seconds=int(
            os.getenv("DELIVERY_GLOBAL_FLOOD_DEFAULT_RETRY_SECONDS", "30")
        ),
        delivery_global_flood_max_retry_seconds=int(
            os.getenv("DELIVERY_GLOBAL_FLOOD_MAX_RETRY_SECONDS", "300")
        ),
        dedup_retention_days=int(os.getenv("DEDUP_RETENTION_DAYS", "14")),
        subscriber_update_concurrency=int(os.getenv("SUBSCRIBER_UPDATE_CONCURRENCY", "12")),
        subscriber_telegram_retry_attempts=int(
            os.getenv("SUBSCRIBER_TELEGRAM_RETRY_ATTEMPTS", "5")
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        log_format=_get_log_format(),
        log_directory=os.getenv("LOG_DIRECTORY", "").strip(),
        log_file_max_bytes=int(os.getenv("LOG_FILE_MAX_BYTES", "10485760")),
        log_file_backup_count=int(os.getenv("LOG_FILE_BACKUP_COUNT", "5")),
        log_telegram_http=_get_bool_env("LOG_TELEGRAM_HTTP", False),
    )


def load_sources_config(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ConfigError(
            f"Sources config not found: {path}. "
            "Copy config/sources.example.yaml to config/sources.yaml and update it."
        )

    content = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    sources = content.get("sources")

    if not isinstance(sources, list) or not sources:
        raise ConfigError(f"No sources configured in {path}")

    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(sources):
        if not isinstance(item, dict):
            raise ConfigError(f"Invalid source at index {index}: expected object")
        source_id = str(item.get("id", "")).strip()
        source_type = str(item.get("type", "")).strip()
        if not source_id or not source_type:
            raise ConfigError(f"Source index {index} must include non-empty id and type")
        normalized.append(item)

    return normalized
