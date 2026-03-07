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
    telegram_bot_username: str
    sources_config_path: Path
    poll_interval_seconds: int
    database_path: Path
    max_signals_per_cycle: int
    http_timeout_seconds: int
    hyperliquid_info_url: str
    discovery_candidate_limit: int
    discovery_min_age_days: int
    discovery_min_trades_30d: int
    discovery_min_active_days_30d: int
    discovery_min_trades_7d: int
    discovery_window_hours: int
    discovery_concurrency: int
    discovery_interval_seconds: int
    admin_panel_username: str
    admin_panel_password: str
    subscription_lifetime_hours: int
    universe_interval_seconds: int
    universe_min_age_days: int
    universe_min_trades_30d: int
    universe_min_win_rate_30d: float
    universe_min_realized_pnl_30d: float
    universe_min_score: float
    universe_max_size: int
    live_top100_interval_seconds: int
    live_top100_active_window_minutes: int
    live_top100_size: int


class ConfigError(RuntimeError):
    pass


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


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
        telegram_bot_username=os.getenv("TELEGRAM_BOT_USERNAME", "").strip().removeprefix("@"),
        sources_config_path=Path(os.getenv("SOURCES_CONFIG_PATH", "config/sources.yaml")),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        database_path=Path(os.getenv("DATABASE_PATH", "data/signals.db")),
        max_signals_per_cycle=int(os.getenv("MAX_SIGNALS_PER_CYCLE", "20")),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        hyperliquid_info_url=os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info"),
        discovery_candidate_limit=int(os.getenv("DISCOVERY_CANDIDATE_LIMIT", "60")),
        discovery_min_age_days=int(os.getenv("DISCOVERY_MIN_AGE_DAYS", "30")),
        discovery_min_trades_30d=int(os.getenv("DISCOVERY_MIN_TRADES_30D", "10")),
        discovery_min_active_days_30d=int(os.getenv("DISCOVERY_MIN_ACTIVE_DAYS_30D", "4")),
        discovery_min_trades_7d=int(os.getenv("DISCOVERY_MIN_TRADES_7D", "1")),
        discovery_window_hours=int(os.getenv("DISCOVERY_WINDOW_HOURS", "24")),
        discovery_concurrency=int(os.getenv("DISCOVERY_CONCURRENCY", "6")),
        discovery_interval_seconds=int(os.getenv("DISCOVERY_INTERVAL_SECONDS", "900")),
        admin_panel_username=admin_panel_username,
        admin_panel_password=admin_panel_password,
        subscription_lifetime_hours=int(os.getenv("SUBSCRIPTION_LIFETIME_HOURS", "24")),
        universe_interval_seconds=int(os.getenv("UNIVERSE_INTERVAL_SECONDS", "300")),
        universe_min_age_days=int(os.getenv("UNIVERSE_MIN_AGE_DAYS", "30")),
        universe_min_trades_30d=int(os.getenv("UNIVERSE_MIN_TRADES_30D", "10")),
        universe_min_win_rate_30d=float(os.getenv("UNIVERSE_MIN_WIN_RATE_30D", "0.0")),
        universe_min_realized_pnl_30d=float(
            os.getenv("UNIVERSE_MIN_REALIZED_PNL_30D", "0.0")
        ),
        universe_min_score=float(os.getenv("UNIVERSE_MIN_SCORE", "0.0")),
        universe_max_size=int(os.getenv("UNIVERSE_MAX_SIZE", "3000")),
        live_top100_interval_seconds=int(os.getenv("LIVE_TOP100_INTERVAL_SECONDS", "60")),
        live_top100_active_window_minutes=int(
            os.getenv("LIVE_TOP100_ACTIVE_WINDOW_MINUTES", "60")
        ),
        live_top100_size=int(os.getenv("LIVE_TOP100_SIZE", "100")),
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
