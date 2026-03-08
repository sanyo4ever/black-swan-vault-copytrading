from __future__ import annotations

import asyncio
import argparse
import base64
import hmac
import json
import logging
import re
import time
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
from aiohttp import web

from bot.config import load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.logging_setup import bind_log_context, build_logging_options, new_trace_id, setup_logging
from bot.telegram_client import TelegramClientError, create_forum_topic
from bot.trader_store import (
    CatalogTrader,
    MODERATION_BLACKLIST,
    MODERATION_NEUTRAL,
    MODERATION_WHITELIST,
    STATUS_ACTIVE,
    STATUS_ACTIVE_LISTED,
    STATUS_ACTIVE_UNLISTED,
    STATUS_ARCHIVED,
    STATUS_STALE,
    TraderStore,
)

PUBLIC_SITE_URL = "https://blackswanvault.online"
PAYPAL_DONATION_EMAIL = "sanyo4ever@gmail.com"
USDT_TRC20_DONATION_ADDRESS = "TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ"


def _render_google_analytics_tag(measurement_id: str) -> str:
    value = str(measurement_id or "").strip().upper()
    if not value or not re.fullmatch(r"G-[A-Z0-9]+", value):
        return ""
    escaped = escape(value)
    return (
        f"  <script async src='https://www.googletagmanager.com/gtag/js?id={escaped}'></script>\n"
        "  <script>\n"
        "    window.dataLayer = window.dataLayer || [];\n"
        "    function gtag(){dataLayer.push(arguments);}\n"
        "    gtag('js', new Date());\n"
        f"    gtag('config', '{escaped}');\n"
        "  </script>\n"
    )


def _split_addresses(raw: str) -> list[str]:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return []
    return [item for item in re.split(r"[\s,;]+", cleaned) if item]


def _parse_moderation_state(raw: str) -> str | None:
    mapping = {
        "neutral": MODERATION_NEUTRAL,
        "whitelist": MODERATION_WHITELIST,
        "blacklist": MODERATION_BLACKLIST,
    }
    return mapping.get(str(raw or "").strip().lower())


def _discovery_config_from_settings(settings) -> HyperliquidDiscoveryConfig:
    return HyperliquidDiscoveryConfig(
        info_url=settings.hyperliquid_info_url,
        candidate_limit=settings.discovery_candidate_limit,
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


def _to_float(raw: Any, default: float) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _to_int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _fmt(raw: Any, digits: int = 2) -> str:
    if raw is None:
        return "-"
    if isinstance(raw, float):
        return f"{raw:.{digits}f}"
    return escape(str(raw))


def _minutes_since(last_fill_time: int | None) -> int | None:
    if last_fill_time is None or last_fill_time <= 0:
        return None
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    delta_ms = max(0, now_ms - int(last_fill_time))
    return int(delta_ms // 60000)


def _fmt_utc_timestamp_ms(raw: int | None) -> str:
    if raw is None or raw <= 0:
        return "-"
    try:
        dt = datetime.fromtimestamp(int(raw) / 1000.0, tz=UTC)
    except Exception:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_signed(raw: Any, *, digits: int = 2, suffix: str = "") -> str:
    if raw is None:
        return "-"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return escape(str(raw))
    css = "metric-pos" if value > 0 else "metric-neg" if value < 0 else "metric-flat"
    sign = "+" if value > 0 else ""
    return f"<span class='{css}'>{sign}{value:.{digits}f}{suffix}</span>"


def _fmt_percent(raw: Any, *, digits: int = 2, ratio: bool = False) -> str:
    if raw is None:
        return "-"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return escape(str(raw))
    if ratio:
        value *= 100.0
    return f"{value:.{digits}f}%"


def _extract_stat_metrics(stats_json: str | None) -> dict[str, Any]:
    if not stats_json:
        return {}
    try:
        payload = json.loads(stats_json)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    metrics_1d = payload.get("metrics_1d")
    if not isinstance(metrics_1d, dict):
        metrics_1d = {}
    metrics_7d = payload.get("metrics_7d")
    if not isinstance(metrics_7d, dict):
        metrics_7d = {}
    metrics_30d = payload.get("metrics_30d")
    if not isinstance(metrics_30d, dict):
        metrics_30d = {}

    def _period_trades(metrics: dict[str, Any]) -> Any:
        return metrics.get("trade_count", metrics.get("weekly_trades"))

    return {
        "roi_1d": metrics_1d.get("roi_pct"),
        "roi_7d": metrics_7d.get("roi_pct"),
        "roi_30d": metrics_30d.get("roi_pct"),
        "pnl_1d": metrics_1d.get("realized_pnl"),
        "pnl_7d": metrics_7d.get("realized_pnl"),
        "pnl_30d": metrics_30d.get("realized_pnl"),
        "win_rate_1d": metrics_1d.get("win_rate"),
        "win_rate_7d": metrics_7d.get("win_rate"),
        "win_rate_30d": metrics_30d.get("win_rate"),
        "wins_1d": metrics_1d.get("wins"),
        "wins_7d": metrics_7d.get("wins"),
        "wins_30d": metrics_30d.get("wins"),
        "losses_1d": metrics_1d.get("losses"),
        "losses_7d": metrics_7d.get("losses"),
        "losses_30d": metrics_30d.get("losses"),
        "profit_to_loss_ratio_1d": metrics_1d.get("profit_to_loss_ratio"),
        "profit_to_loss_ratio_7d": metrics_7d.get("profit_to_loss_ratio"),
        "profit_to_loss_ratio_30d": metrics_30d.get("profit_to_loss_ratio"),
        "trades_1d": _period_trades(metrics_1d),
        "trades_7d": _period_trades(metrics_7d),
        "trades_30d": _period_trades(metrics_30d),
        "weekly_trades": metrics_7d.get("weekly_trades", _period_trades(metrics_7d)),
        "avg_pnl_per_trade_1d": metrics_1d.get("avg_pnl_per_trade"),
        "avg_pnl_per_trade_7d": metrics_7d.get("avg_pnl_per_trade"),
        "avg_pnl_per_trade_30d": metrics_30d.get("avg_pnl_per_trade"),
        "max_drawdown_1d": metrics_1d.get("max_drawdown_pct"),
        "max_drawdown_7d": metrics_7d.get("max_drawdown_pct"),
        "max_drawdown_30d": metrics_30d.get("max_drawdown_pct"),
        "sharpe_1d": metrics_1d.get("sharpe"),
        "sharpe_7d": metrics_7d.get("sharpe"),
        "sharpe_30d": metrics_30d.get("sharpe"),
        "sortino_1d": metrics_1d.get("sortino"),
        "sortino_7d": metrics_7d.get("sortino"),
        "sortino_30d": metrics_30d.get("sortino"),
        "roi_volatility_1d": metrics_1d.get("roi_volatility_pct"),
        "roi_volatility_7d": metrics_7d.get("roi_volatility_pct"),
        "roi_volatility_30d": metrics_30d.get("roi_volatility_pct"),
    }


def _period_trade_count(trader: CatalogTrader, *, period: str, metrics: dict[str, Any]) -> Any:
    period_norm = _normalize_catalog_period(period)
    if period_norm == "1d":
        return metrics.get("trades_1d", trader.trades_24h)
    if period_norm == "7d":
        return metrics.get("trades_7d", trader.trades_7d)
    return metrics.get("trades_30d", trader.trades_30d)


def _catalog_sort_value(*, trader: CatalogTrader, sort_by: str) -> float | int:
    field, _direction, metric_period = _catalog_sort_parts(sort_by)
    metrics = _extract_stat_metrics(trader.stats_json)

    if field == "activity":
        return float(trader.activity_score or -10**9)
    if field == "recent":
        return int(trader.last_fill_time or 0)
    if field == "score":
        return float(trader.score or -10**9)
    if field == "age":
        return float(trader.age_days or -1.0)

    period_norm = _normalize_catalog_period(metric_period)
    if field == "trades":
        return int(_period_trade_count(trader, period=period_norm, metrics=metrics) or -1)

    metric_key_map = {
        "roi": f"roi_{period_norm}",
        "drawdown": f"max_drawdown_{period_norm}",
        "pnl": f"pnl_{period_norm}",
        "win": f"win_rate_{period_norm}",
        "pl": f"profit_to_loss_ratio_{period_norm}",
        "sharpe": f"sharpe_{period_norm}",
    }
    metric_key = metric_key_map.get(field)
    if metric_key:
        return float(metrics.get(metric_key) or -10**9)
    return float(trader.activity_score or -10**9)


def _resolve_join_url(settings) -> str:
    return str(getattr(settings, "telegram_join_url", "") or "").strip()


def _is_admin_authorized(request: web.Request) -> bool:
    settings = request.app["settings"]
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        return False

    token = auth_header.removeprefix("Basic ").strip()
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except Exception:
        return False

    expected = f"{settings.admin_panel_username}:{settings.admin_panel_password}"
    return hmac.compare_digest(decoded, expected)


def _unauthorized() -> web.Response:
    return web.Response(
        status=401,
        text="Unauthorized",
        headers={"WWW-Authenticate": "Basic realm=\"Discovery Admin\""},
    )


def _client_ip(request: web.Request) -> str:
    xff = request.headers.get("X-Forwarded-For", "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return str(request.remote or "-")


def _request_origin(request: web.Request) -> str:
    xfp = request.headers.get("X-Forwarded-Proto", "").strip()
    proto = xfp.split(",")[0].strip() if xfp else request.scheme
    if proto not in {"http", "https"}:
        proto = request.scheme

    xfh = request.headers.get("X-Forwarded-Host", "").strip()
    host = xfh.split(",")[0].strip() if xfh else request.host
    host = host or request.host

    return f"{proto}://{host}"


def _http_log_level_for_status(status: int) -> int:
    if status >= 500:
        return logging.ERROR
    if status == 429:
        return logging.WARNING
    if status in {401, 403}:
        return logging.WARNING
    return logging.INFO


@web.middleware
async def _request_logging_middleware(request: web.Request, handler):
    logger: logging.Logger = request.app["logger"]
    request_id = request.headers.get("X-Request-ID", "").strip() or new_trace_id("http")
    started = time.monotonic()
    method = request.method
    path_qs = request.path_qs
    remote = _client_ip(request)
    ua = (request.headers.get("User-Agent", "") or "").strip()[:300]

    with bind_log_context(request_id=request_id, method=method, path=request.path):
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            logger.log(
                _http_log_level_for_status(exc.status),
                "HTTP exception status=%s duration_ms=%s remote=%s path_qs=%s ua=%s",
                exc.status,
                duration_ms,
                remote,
                path_qs,
                ua or "-",
            )
            exc.headers["X-Request-ID"] = request_id
            raise
        except Exception as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            logger.exception(
                "HTTP request failed duration_ms=%s remote=%s path_qs=%s ua=%s error=%s",
                duration_ms,
                remote,
                path_qs,
                ua or "-",
                exc,
            )
            raise

        duration_ms = int((time.monotonic() - started) * 1000)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "HTTP request complete status=%s duration_ms=%s remote=%s path_qs=%s ua=%s",
            response.status,
            duration_ms,
            remote,
            path_qs,
            ua or "-",
        )
        return response


@web.middleware
async def _admin_auth_middleware(request: web.Request, handler):
    if request.path.startswith("/admin"):
        if not _is_admin_authorized(request):
            request.app["logger"].warning("Admin auth failed remote=%s path=%s", _client_ip(request), request.path)
            return _unauthorized()
    return await handler(request)


def _is_same_origin(request: web.Request) -> bool:
    expected = _request_origin(request).rstrip("/").lower()
    if not expected:
        return False
    for header in ("Origin", "Referer"):
        value = (request.headers.get(header, "") or "").strip().lower()
        if value:
            return value.startswith(expected)
    return False


@web.middleware
async def _admin_csrf_middleware(request: web.Request, handler):
    if request.path.startswith("/admin") and request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        if not _is_same_origin(request):
            request.app["logger"].warning(
                "Admin CSRF blocked remote=%s path=%s origin=%s referer=%s",
                _client_ip(request),
                request.path,
                request.headers.get("Origin", "-"),
                request.headers.get("Referer", "-"),
            )
            raise web.HTTPForbidden(text="CSRF validation failed")
    return await handler(request)


@web.middleware
async def _security_headers_middleware(request: web.Request, handler):
    response = await handler(request)
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline' https://www.googletagmanager.com https://www.google-analytics.com; connect-src 'self' https://www.google-analytics.com https://region1.google-analytics.com;",
    )
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if request.secure or request.headers.get("X-Forwarded-Proto", "").split(",")[0].strip() == "https":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


_CATALOG_PERIODS = {"1d", "7d", "30d"}
_CATALOG_PERIOD_METRIC_FIELDS = {"roi", "drawdown", "pnl", "win", "pl", "sharpe", "trades"}
_CATALOG_STATIC_FIELDS = {"activity", "recent", "score", "age"}
_CATALOG_DIRECTIONS = {"asc", "desc"}
_CATALOG_DEFAULT_PERIOD = "7d"
_CATALOG_DEFAULT_SORT = "activity_desc"


def _normalize_catalog_period(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in _CATALOG_PERIODS:
        return value
    return _CATALOG_DEFAULT_PERIOD


def _catalog_sort_key(*, field: str, direction: str, period: str) -> str:
    direction_norm = "asc" if str(direction).strip().lower() == "asc" else "desc"
    period_norm = _normalize_catalog_period(period)
    if field in _CATALOG_PERIOD_METRIC_FIELDS:
        return f"{field}_{period_norm}_{direction_norm}"
    if field in _CATALOG_STATIC_FIELDS:
        return f"{field}_{direction_norm}"
    return _CATALOG_DEFAULT_SORT


def _catalog_sort_parts(sort_key: str) -> tuple[str, str, str | None]:
    value = str(sort_key or "").strip().lower()
    static_match = re.fullmatch(r"(activity|recent|score|age)_(asc|desc)", value)
    if static_match:
        return static_match.group(1), static_match.group(2), None

    metric_match = re.fullmatch(
        r"(roi|drawdown|pnl|win|pl|sharpe|trades)_(1d|7d|30d)_(asc|desc)",
        value,
    )
    if metric_match:
        return metric_match.group(1), metric_match.group(3), metric_match.group(2)

    return "activity", "desc", None


def _normalize_catalog_sort(raw: Any, *, period: str) -> str:
    period_norm = _normalize_catalog_period(period)
    value = str(raw or "").strip().lower()
    if not value:
        return _CATALOG_DEFAULT_SORT

    legacy_map = {
        "activity_desc": "activity_desc",
        "recent_desc": "recent_desc",
        "score_desc": "score_desc",
        "age_desc": "age_desc",
        "pnl_desc": _catalog_sort_key(field="pnl", direction="desc", period=period_norm),
        "win_desc": _catalog_sort_key(field="win", direction="desc", period=period_norm),
        "trades_desc": _catalog_sort_key(field="trades", direction="desc", period=period_norm),
    }
    mapped = legacy_map.get(value)
    if mapped:
        return mapped

    field, direction, _metric_period = _catalog_sort_parts(value)
    if field in _CATALOG_PERIOD_METRIC_FIELDS:
        return _catalog_sort_key(
            field=field,
            direction=direction,
            period=period_norm,
        )
    if field in _CATALOG_STATIC_FIELDS and direction in _CATALOG_DIRECTIONS:
        return _catalog_sort_key(field=field, direction=direction, period=period_norm)
    return _CATALOG_DEFAULT_SORT


def _encode_cursor(*, sort_by: str, value: float | int, address: str) -> str:
    payload = {"s": sort_by, "v": value, "a": address}
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    return encoded.decode("utf-8")


def _decode_cursor(raw: str, *, expected_sort: str) -> tuple[float | int, str] | None:
    token = str(raw or "").strip()
    if not token:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        data = json.loads(decoded)
        if not isinstance(data, dict):
            return None
        if str(data.get("s", "")) != expected_sort:
            return None
        address = str(data.get("a", "")).strip().lower()
        if not address:
            return None
        value = data.get("v")
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return int(value), address
        return float(value), address
    except Exception:
        return None


def _render_public_directory(
    *,
    traders: list[CatalogTrader],
    request: web.Request,
    join_url: str,
    topic_links: dict[str, str],
    next_cursor: str | None,
    google_analytics_measurement_id: str,
) -> str:
    selected_period = _normalize_catalog_period(request.query.get("period"))
    sort_selected = _normalize_catalog_sort(
        request.query.get("sort"),
        period=selected_period,
    )

    def _sort_link(*, field: str, direction: str) -> str:
        params = {key: value for key, value in request.query.items() if key != "cursor"}
        params["period"] = selected_period
        params["sort"] = _catalog_sort_key(
            field=field,
            direction=direction,
            period=selected_period,
        )
        return "/?" + urlencode(params)

    def _sortable_th(*, label: str, field: str) -> str:
        asc_key = _catalog_sort_key(field=field, direction="asc", period=selected_period)
        desc_key = _catalog_sort_key(field=field, direction="desc", period=selected_period)
        asc_active = " active" if sort_selected == asc_key else ""
        desc_active = " active" if sort_selected == desc_key else ""
        asc_url = _sort_link(field=field, direction="asc")
        desc_url = _sort_link(field=field, direction="desc")
        return (
            "<th>"
            "<div class='th-sort'>"
            f"<span>{escape(label)}</span>"
            "<span class='th-arrows'>"
            f"<a class='th-arrow{asc_active}' href='{escape(asc_url)}' title='Sort ascending'>&#9650;</a>"
            f"<a class='th-arrow{desc_active}' href='{escape(desc_url)}' title='Sort descending'>&#9660;</a>"
            "</span>"
            "</div>"
            "</th>"
        )

    period_title = selected_period
    trades_column_label = f"Trades {period_title}"
    if join_url:
        hero_join_button_html = (
            f"<a class='hero-btn' href='{escape(join_url)}' target='_blank' rel='noopener'>Join Channel</a>"
        )
    else:
        hero_join_button_html = "<span class='hero-btn hero-btn-disabled'>Join Channel Unavailable</span>"
    rows = []
    for index, trader in enumerate(traders, start=1):
        metrics = _extract_stat_metrics(trader.stats_json)
        drawdown_raw = metrics.get(f"max_drawdown_{selected_period}")
        drawdown_value = -float(drawdown_raw) if drawdown_raw is not None else None
        win_rate = _fmt_percent(metrics.get(f"win_rate_{selected_period}"), digits=2, ratio=True)
        pl_ratio_raw = metrics.get(f"profit_to_loss_ratio_{selected_period}")
        pl_ratio = (
            f"{float(pl_ratio_raw):.2f} : 1"
            if pl_ratio_raw is not None
            else "-"
        )
        trades_in_period = _period_trade_count(
            trader,
            period=selected_period,
            metrics=metrics,
        )
        last_traded_at = _fmt_utc_timestamp_ms(trader.last_fill_time)
        minutes_since = _minutes_since(trader.last_fill_time)
        freshness = f"{minutes_since}m ago" if minutes_since is not None else "-"
        trader_label = trader.label or trader.address[:10]
        view_link = topic_links.get(trader.address)
        action_html = (
            f"<a class='row-action-btn' href='{escape(view_link)}' target='_blank' rel='noopener'>View in Telegram</a>"
            if view_link
            else "<span class='row-action-btn row-action-btn-disabled'>View in Telegram</span>"
        )
        rows.append(
            "<tr>"
            f"<td>{_fmt(index, 0)}</td>"
            "<td class='trader-cell'>"
            f"<div class='trader-label'>{escape(trader_label)}</div>"
            f"<code>{escape(trader.address)}</code>"
            "</td>"
            f"<td>{_fmt_signed(metrics.get(f'roi_{selected_period}'), digits=2, suffix='%')}</td>"
            f"<td>{_fmt_signed(drawdown_value, digits=2, suffix='%')}</td>"
            f"<td>{_fmt_signed(metrics.get(f'pnl_{selected_period}'), digits=2)}</td>"
            f"<td>{win_rate}</td>"
            f"<td>{pl_ratio}</td>"
            f"<td>{_fmt_signed(metrics.get(f'sharpe_{selected_period}'), digits=2)}</td>"
            f"<td>{_fmt(trades_in_period, 0)}</td>"
            "<td>"
            f"<div>{escape(last_traded_at)}</div>"
            f"<div class='muted-mini'>{escape(freshness)}</div>"
            "</td>"
            f"<td>{action_html}</td>"
            "</tr>"
        )

    table_rows = (
        "\n".join(rows)
        if rows
        else "<tr><td colspan='11'>No traders match your filters.</td></tr>"
    )
    table_headers = (
        "<th>#</th>"
        "<th>Trader</th>"
        f"{_sortable_th(label=f'{period_title} ROI', field='roi')}"
        f"{_sortable_th(label=f'{period_title} Drawdown', field='drawdown')}"
        f"{_sortable_th(label=f'{period_title} PnL', field='pnl')}"
        f"{_sortable_th(label=f'{period_title} Win Rate', field='win')}"
        f"{_sortable_th(label=f'{period_title} Profit-to-Loss', field='pl')}"
        f"{_sortable_th(label=f'{period_title} Sharpe', field='sharpe')}"
        f"{_sortable_th(label=trades_column_label, field='trades')}"
        f"{_sortable_th(label='Last Traded At', field='recent')}"
        "<th>Action</th>"
    )
    refreshed_at = traders[0].refreshed_at if traders else "-"
    pager = ""
    if next_cursor:
        params = {key: value for key, value in request.query.items() if key != "cursor"}
        params["cursor"] = next_cursor
        next_url = "/?" + urlencode(params)
        pager = (
            "<div style='margin-top:10px'>"
            f"<a class='btn-link' href='{escape(next_url)}'>Next page</a>"
            "</div>"
        )

    filter_values = {
        "q": str(request.query.get("q", "")).strip(),
        "status": str(request.query.get("status", STATUS_ACTIVE_LISTED)).strip().upper(),
        "min_age_days": str(request.query.get("min_age_days", "")).strip(),
        "min_trades_30d": str(request.query.get("min_trades_30d", "")).strip(),
        "min_active_days_30d": str(request.query.get("min_active_days_30d", "")).strip(),
        "min_win_rate_30d": str(request.query.get("min_win_rate_30d", "")).strip(),
        "min_realized_pnl_30d": str(request.query.get("min_realized_pnl_30d", "")).strip(),
        "min_score": str(request.query.get("min_score", "")).strip(),
        "min_activity_score": str(request.query.get("min_activity_score", "")).strip(),
        "active_within_minutes": str(request.query.get("active_within_minutes", "")).strip(),
        "period": selected_period,
    }
    period_options_html = "".join(
        (
            f"<option value='{value}'{' selected' if selected_period == value else ''}>"
            f"{escape(label)}</option>"
        )
        for value, label in (("1d", "1 day"), ("7d", "7 days"), ("30d", "30 days"))
    )
    status_options_html = "".join(
        (
            f"<option value='{value}'{' selected' if filter_values['status'] == value else ''}>"
            f"{escape(label)}</option>"
        )
        for value, label in (
            (STATUS_ACTIVE_LISTED, "Listed only"),
            (STATUS_ACTIVE_UNLISTED, "Unlisted"),
            (STATUS_STALE, "Stale"),
            (STATUS_ARCHIVED, "Archived"),
            ("ALL", "All statuses"),
        )
    )
    active_filter_count = sum(
        1
        for key, value in filter_values.items()
        if key not in {"period", "status"} and value
    )
    if filter_values["status"] != STATUS_ACTIVE_LISTED:
        active_filter_count += 1
    if selected_period != _CATALOG_DEFAULT_PERIOD:
        active_filter_count += 1
    if sort_selected != _CATALOG_DEFAULT_SORT:
        active_filter_count += 1
    filter_state_label = (
        f"{active_filter_count} active filter{'s' if active_filter_count != 1 else ''}"
        if active_filter_count
        else "No active filters"
    )

    origin = _request_origin(request)
    canonical_url = f"{origin}/"
    current_url = f"{origin}{request.path_qs}"
    logo_mark_url = "/assets/blackswanvault-mark.svg"
    logo_banner_url = "/assets/blackswanvault-logo.svg"
    logo_og_url = f"{origin}{logo_banner_url}"
    has_query_filters = bool(request.query)
    robots = (
        "noindex,follow"
        if has_query_filters
        else "index,follow,max-snippet:-1,max-image-preview:large,max-video-preview:-1"
    )
    page_title = "Free Crypto Copy Trading Signals for Futures | Black Swan Vault"
    page_description = (
        "Discover active futures traders, compare real performance stats, and start Telegram copy "
        "trading feeds in one click."
    )
    page_keywords = (
        "copy trading, crypto copy trading, futures copy trading, free crypto signals, "
        "telegram trading signals, crypto futures signals, copytrading bot"
    )
    website_schema = json.dumps(
        {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "Black Swan Vault",
            "url": canonical_url,
            "description": page_description,
            "potentialAction": {
                "@type": "SearchAction",
                "target": f"{canonical_url}?q={{search_term_string}}",
                "query-input": "required name=search_term_string",
            },
        },
        separators=(",", ":"),
    )
    faq_schema = json.dumps(
        {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": "Is this crypto copy trading directory free?",
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": (
                            "Yes. You can browse traders and join the Telegram channel without a paywall."
                        ),
                    },
                },
                {
                    "@type": "Question",
                    "name": "Do you provide free crypto signals?",
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": (
                            "The service tracks trader activity and forwards trade fills to Telegram chats. "
                            "It is informational and not financial advice."
                        ),
                    },
                },
                {
                    "@type": "Question",
                    "name": "How does Telegram copy trading work here?",
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": (
                            "Click Join Channel. The channel uses dedicated forum topics per trader wallet, "
                            "and new fills are posted in the matching topic."
                        ),
                    },
                },
            ],
        },
        separators=(",", ":"),
    )
    analytics_tag = _render_google_analytics_tag(google_analytics_measurement_id)

    return f"""
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <meta name='description' content='{escape(page_description)}' />
  <meta name='keywords' content='{escape(page_keywords)}' />
  <meta name='robots' content='{escape(robots)}' />
  <link rel='canonical' href='{escape(canonical_url)}' />
  <meta property='og:type' content='website' />
  <meta property='og:site_name' content='Black Swan Vault' />
  <meta property='og:title' content='{escape(page_title)}' />
  <meta property='og:description' content='{escape(page_description)}' />
  <meta property='og:url' content='{escape(current_url)}' />
  <meta property='og:image' content='{escape(logo_og_url)}' />
  <meta name='twitter:card' content='summary_large_image' />
  <meta name='twitter:title' content='{escape(page_title)}' />
  <meta name='twitter:description' content='{escape(page_description)}' />
  <meta name='twitter:image' content='{escape(logo_og_url)}' />
  <link rel='icon' type='image/svg+xml' href='{escape(logo_mark_url)}' />
  <meta name='theme-color' content='#06080d' />
  <title>{escape(page_title)}</title>
  {analytics_tag}
  <script type='application/ld+json'>{website_schema}</script>
  <script type='application/ld+json'>{faq_schema}</script>
  <style>
    :root {{
      --bg:#06080d;
      --panel:#0d1119;
      --panel-soft:#121722;
      --line:#242b38;
      --text:#e9edf6;
      --muted:#96a0b8;
      --accent:#ff9f1a;
      --accent-soft:#ffbf47;
      --green:#1dd38a;
      --red:#ff5f78;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0;
      color:var(--text);
      font-family:"Space Grotesk","Segoe UI",sans-serif;
      background-color:var(--bg);
      background-image:
        linear-gradient(rgba(255,159,26,.07) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,159,26,.07) 1px, transparent 1px),
        radial-gradient(circle at 8% -20%, rgba(255,159,26,.26), transparent 34%),
        radial-gradient(circle at 92% 0%, rgba(255,159,26,.16), transparent 32%);
      background-size: 132px 132px, 132px 132px, 100% 100%, 100% 100%;
    }}
    .wrap {{ max-width:1540px; margin:22px auto; padding:0 18px; }}
    .card {{
      background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.00));
      border:1px solid var(--line);
      border-radius:14px;
      padding:14px;
      margin-bottom:14px;
      box-shadow:0 16px 38px rgba(0,0,0,.24);
    }}
    .hero-card {{ background:linear-gradient(180deg,rgba(255,180,60,.07),rgba(255,255,255,.01)); }}
    .brand-row {{ display:flex; gap:12px; align-items:center; margin-bottom:8px; }}
    .brand-mark {{
      width:74px;
      height:74px;
      border-radius:18px;
      background:#0a0f16;
      border:1px solid #2c3442;
      padding:6px;
      box-shadow:inset 0 0 24px rgba(255,159,26,.08);
      flex:0 0 auto;
      overflow:hidden;
    }}
    .brand-mark img {{ display:block; width:100%; height:100%; object-fit:cover; }}
    .brand-copy {{ min-width:0; }}
    .brand-name {{
      font-size:19px;
      font-weight:700;
      letter-spacing:.2px;
      margin-bottom:3px;
      color:#f1f4fb;
    }}
    .brand-tagline {{
      margin:0 0 4px;
      color:#b1bdd6;
      font-size:12px;
      letter-spacing:.25px;
    }}
    .domain-chip {{
      display:inline-block;
      border-radius:999px;
      border:1px solid #7f5417;
      background:rgba(255,159,26,.12);
      color:#ffbc55;
      text-decoration:none;
      font-size:12px;
      font-weight:600;
      padding:4px 10px;
    }}
    .domain-chip:hover {{ background:rgba(255,159,26,.2); }}
    h1 {{ margin:0 0 6px; font-size:29px; letter-spacing:.2px; }}
    h2 {{ margin:0 0 8px; font-size:20px; }}
    h3 {{ margin:0 0 6px; font-size:15px; }}
    p {{ margin:0; color:var(--muted); }}
    .top-tabs {{ margin-top:14px; display:flex; gap:24px; flex-wrap:wrap; }}
    .tab {{ color:#8f98ad; font-size:20px; padding-bottom:10px; border-bottom:2px solid transparent; }}
    .tab-active {{ color:var(--text); border-bottom-color:var(--accent); }}
    .quick-info {{ margin-top:12px; color:var(--muted); font-size:13px; line-height:1.55; }}
    .seo-grid {{ margin-top:12px; display:grid; gap:10px; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); }}
    .seo-box {{ border:1px solid var(--line); border-radius:10px; padding:10px; background:var(--panel-soft); }}
    .seo-box ul {{ margin:6px 0 0 18px; color:var(--muted); }}
    .seo-box li {{ margin:4px 0; }}
    .hero-actions {{ margin-top:16px; display:flex; gap:14px; flex-wrap:wrap; }}
    .hero-btn {{
      display:inline-block;
      border-radius:999px;
      border:1px solid #805215;
      background:rgba(255,159,26,.12);
      color:#ffc66a;
      text-decoration:none;
      padding:16px 26px;
      font-size:24px;
      line-height:1.1;
      font-weight:600;
    }}
    .hero-btn-disabled {{
      opacity:.65;
      pointer-events:none;
      cursor:not-allowed;
    }}
    .hero-btn:hover {{ background:rgba(255,159,26,.2); }}
    .hero-thanks {{ margin-top:10px; color:#d5a556; font-size:12px; }}
    .filters-panel {{ display:grid; gap:12px; }}
    .filters-head {{
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:10px;
      flex-wrap:wrap;
    }}
    .filters-title {{ margin:0; font-size:16px; font-weight:700; }}
    .filters-state {{
      border:1px solid #3f3a27;
      border-radius:999px;
      padding:5px 10px;
      color:#ffc66a;
      background:rgba(255,159,26,.1);
      font-size:12px;
      font-weight:600;
    }}
    .filters-note {{ color:var(--muted); font-size:12px; line-height:1.45; }}
    .filter-form {{ display:grid; gap:10px; }}
    .filter-grid {{
      display:grid;
      gap:10px;
      grid-template-columns:repeat(auto-fit,minmax(190px,1fr));
    }}
    .filter-item {{ display:grid; gap:6px; }}
    .filter-item label {{
      color:#aab3c7;
      font-size:12px;
      letter-spacing:.15px;
      font-weight:600;
    }}
    .filter-item input,.filter-item select {{
      width:100%;
      background:#0b1018;
      border:1px solid #2f3442;
      color:var(--text);
      border-radius:12px;
      padding:9px 12px;
      font-size:13px;
    }}
    .filter-item input::placeholder {{ color:#6f7c99; }}
    .filter-actions {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; }}
    .filter-apply {{
      background:rgba(255,159,26,.16);
      border:1px solid #8a5a18;
      color:#ffbf54;
      border-radius:999px;
      padding:9px 14px;
      font-size:12px;
      font-weight:700;
      cursor:pointer;
    }}
    .filter-apply:hover {{ background:rgba(255,159,26,.25); }}
    .filter-reset {{
      display:inline-block;
      text-decoration:none;
      border:1px solid #3a4252;
      color:#d6dde9;
      border-radius:999px;
      padding:8px 13px;
      font-size:12px;
      background:#10141d;
    }}
    .filter-reset:hover {{ background:#161c27; }}
    .btn-link {{
      display:inline-block;
      background:#10141d;
      border:1px solid #3a4252;
      color:#d6dde9;
      border-radius:8px;
      padding:5px 9px;
      text-decoration:none;
      font-size:12px;
    }}
    .btn-link:hover {{ background:#161c27; }}
    .table-wrap {{ overflow-x:auto; border-radius:12px; border:1px solid #212736; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; min-width:1180px; background:#0a0d13; }}
    thead th {{
      background:#10141d;
      color:#a7afc2;
      font-weight:600;
      font-size:12px;
      letter-spacing:.2px;
      border-bottom:1px solid #242c3d;
      padding:12px 10px;
      text-align:left;
      white-space:nowrap;
    }}
    .th-sort {{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
    }}
    .th-arrows {{
      display:inline-flex;
      flex-direction:column;
      align-items:center;
      gap:1px;
      line-height:1;
    }}
    .th-arrow {{
      color:#6d7894;
      text-decoration:none;
      font-size:10px;
      padding:0 2px;
    }}
    .th-arrow:hover {{ color:#c9d4ee; }}
    .th-arrow.active {{ color:#ffbf54; }}
    tbody td {{
      padding:12px 10px;
      border-bottom:1px solid #1b2130;
      color:#d6ddeb;
      white-space:nowrap;
    }}
    tbody tr:hover {{ background:rgba(255,159,26,.06); }}
    code {{ color:#95a4c8; }}
    .trader-cell code {{ font-size:11px; opacity:.78; }}
    .trader-label {{ font-weight:700; margin-bottom:3px; font-size:21px; line-height:1.05; }}
    .metric-pos {{ color:var(--green); font-weight:700; }}
    .metric-neg {{ color:var(--red); font-weight:700; }}
    .metric-flat {{ color:#b9c0d1; }}
    .muted-mini {{ color:var(--muted); font-size:11px; margin-top:3px; }}
    .row-action-btn {{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-width:156px;
      border-radius:999px;
      border:1px solid #805215;
      background:rgba(255,159,26,.12);
      color:#ffc66a;
      text-decoration:none;
      padding:8px 12px;
      font-size:12px;
      font-weight:600;
      line-height:1.1;
    }}
    .row-action-btn:hover {{ background:rgba(255,159,26,.2); }}
    .row-action-btn-disabled {{
      opacity:.65;
      pointer-events:none;
      cursor:not-allowed;
    }}
    .faq-item {{ border-top:1px solid var(--line); padding:10px 0; }}
    .faq-item:first-of-type {{ border-top:none; padding-top:0; }}
    .faq-item p {{ line-height:1.5; }}
    @media (max-width: 900px) {{
      .tab {{ font-size:17px; }}
      h1 {{ font-size:25px; }}
      .card {{ padding:12px; }}
      .brand-mark {{ width:62px; height:62px; border-radius:14px; }}
      .brand-name {{ font-size:16px; }}
      .hero-btn {{
        padding:12px 20px;
        font-size:18px;
      }}
    }}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='card hero-card'>
      <div class='brand-row'>
        <div class='brand-mark' aria-label='Black Swan Vault logo'>
          <img src='/assets/blackswanvault-mark.svg' alt='Black Swan Vault mark' />
        </div>
        <div class='brand-copy'>
          <div class='brand-name'>Black Swan Vault</div>
          <p class='brand-tagline'>Crypto copytrading intelligence</p>
          <a class='domain-chip' href='{escape(PUBLIC_SITE_URL)}' target='_blank' rel='noopener'>blackswanvault.online</a>
        </div>
      </div>
      <h1>Crypto Copy Trading Signals for Futures Traders</h1>
      <p>Find active traders, filter by performance, and open Telegram copy trading feeds in one click.</p>
      <div class='top-tabs'>
        <span class='tab'>Leaderboard</span>
        <span class='tab tab-active'>All Traders</span>
        <span class='tab'>Favorites</span>
        <span class='tab'>Subscribed</span>
      </div>
      <div class='hero-actions'>
        {hero_join_button_html}
      </div>
      <div class='hero-thanks'>
        Thank you to everyone helping keep this server running. Your support makes public access possible.
      </div>
      <div class='quick-info'>
        <strong>How it works:</strong> Discovery workers collect and score futures traders continuously.<br/>
        Catalog refresh: <strong>{escape(refreshed_at)}</strong>.<br/>
        Table shows objective strategy metrics only (ROI, Drawdown, PnL, Win Rate, P/L Ratio, Sharpe, trade activity).<br/>
        Use the <strong>Range</strong> filter to switch table metrics between 1d, 7d, and 30d windows.<br/>
        Click <strong>Join Channel</strong> to open the Telegram channel with forum topics per trader wallet.<br/>
        Project is donation-supported: PayPal <code>{escape(PAYPAL_DONATION_EMAIL)}</code> or USDT TRC20 <code>{escape(USDT_TRC20_DONATION_ADDRESS)}</code>.<br/>
        Informational only. Not financial advice.
      </div>
      <div class='seo-grid'>
        <div class='seo-box'>
          <h2>Why Use This Copy Trading Directory</h2>
          <ul>
            <li>Free access to crypto futures trader data and Telegram signal delivery.</li>
            <li>Performance filters: ROI, win rate, PnL, activity, and consistency proxies.</li>
            <li>One-click access to trader-specific Telegram topics.</li>
          </ul>
        </div>
        <div class='seo-box'>
          <h2>Best For</h2>
          <ul>
            <li>Users searching for copy trading setups without closed paywalls.</li>
            <li>Traders comparing active accounts before choosing who to follow.</li>
            <li>Communities that need fast, structured futures trade updates.</li>
          </ul>
        </div>
      </div>
    </div>

    <div class='card'>
      <div class='filters-panel'>
        <div class='filters-head'>
          <h2 class='filters-title'>Trader Filters</h2>
          <span class='filters-state'>{escape(filter_state_label)}</span>
        </div>
        <p class='filters-note'>
          Leave any field empty to ignore that filter. Numeric values are minimum thresholds.
        </p>
        <form class='filter-form' method='get' action='/'>
          <div class='filter-grid'>
            <div class='filter-item'>
              <label for='f-q'>Search</label>
              <input id='f-q' name='q' value='{escape(filter_values["q"])}' placeholder='Label, wallet, or source' />
            </div>
            <div class='filter-item'>
              <label for='f-period'>Range</label>
              <select id='f-period' name='period'>
                {period_options_html}
              </select>
            </div>
            <div class='filter-item'>
              <label for='f-status'>Status</label>
              <select id='f-status' name='status'>
                {status_options_html}
              </select>
            </div>
            <div class='filter-item'>
              <label for='f-age'>Min Age (days)</label>
              <input id='f-age' name='min_age_days' type='number' step='1' min='0' value='{escape(filter_values["min_age_days"])}' placeholder='e.g. 30' />
            </div>
            <div class='filter-item'>
              <label for='f-trades30'>Min Trades (30d)</label>
              <input id='f-trades30' name='min_trades_30d' type='number' step='1' min='0' value='{escape(filter_values["min_trades_30d"])}' placeholder='e.g. 120' />
            </div>
            <div class='filter-item'>
              <label for='f-active-days'>Min Active Days (30d)</label>
              <input id='f-active-days' name='min_active_days_30d' type='number' step='1' min='0' value='{escape(filter_values["min_active_days_30d"])}' placeholder='e.g. 12' />
            </div>
            <div class='filter-item'>
              <label for='f-winrate'>Min Win Rate (30d, %)</label>
              <input id='f-winrate' name='min_win_rate_30d' type='number' step='0.1' min='0' max='100' value='{escape(filter_values["min_win_rate_30d"])}' placeholder='e.g. 55' />
            </div>
            <div class='filter-item'>
              <label for='f-pnl'>Min Realized PnL (30d, USDT)</label>
              <input id='f-pnl' name='min_realized_pnl_30d' type='number' step='0.01' value='{escape(filter_values["min_realized_pnl_30d"])}' placeholder='e.g. 500' />
            </div>
            <div class='filter-item'>
              <label for='f-score'>Min Quality Score</label>
              <input id='f-score' name='min_score' type='number' step='0.01' value='{escape(filter_values["min_score"])}' placeholder='e.g. 2.5' />
            </div>
            <div class='filter-item'>
              <label for='f-activity'>Min Activity Score</label>
              <input id='f-activity' name='min_activity_score' type='number' step='0.01' value='{escape(filter_values["min_activity_score"])}' placeholder='e.g. 1.2' />
            </div>
            <div class='filter-item'>
              <label for='f-recent'>Active Within (minutes)</label>
              <input id='f-recent' name='active_within_minutes' type='number' step='1' min='0' value='{escape(filter_values["active_within_minutes"])}' placeholder='e.g. 60' />
            </div>
          </div>
          <input type='hidden' name='sort' value='{escape(sort_selected)}' />
          <div class='filter-actions'>
            <button class='filter-apply' type='submit'>Apply Filters</button>
            <a class='filter-reset' href='/'>Reset</a>
          </div>
        </form>
      </div>
    </div>

    <div class='card'>
      <div class='table-wrap'>
      <table>
        <thead>
          <tr>
            {table_headers}
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
      </div>
      {pager}
    </div>

    <div class='card'>
      <h2>FAQ: Copy Trading and Free Crypto Signals</h2>
      <div class='faq-item'>
        <h3>Is this copy trading service free?</h3>
        <p>Yes. The directory and Telegram flow are available without a paywall.</p>
      </div>
      <div class='faq-item'>
        <h3>What kind of signals do I get?</h3>
        <p>You receive trader fill updates captured by the monitoring pipeline and posted to your Telegram thread.</p>
      </div>
      <div class='faq-item'>
        <h3>Is this financial advice?</h3>
        <p>No. It is an informational copy trading and analytics tool, not investment advice.</p>
      </div>
      <div class='faq-item'>
        <p>
          Legal: <a href='/terms'>Terms</a> | <a href='/privacy'>Privacy</a> | <a href='/disclaimer'>Disclaimer</a>
        </p>
      </div>
    </div>
  </div>
</body>
</html>
"""


def _render_legal_page(
    *,
    request: web.Request,
    title: str,
    body_html: str,
    google_analytics_measurement_id: str,
) -> str:
    origin = _request_origin(request)
    canonical_url = f"{origin}{request.path}"
    analytics_tag = _render_google_analytics_tag(google_analytics_measurement_id)
    updated_on = "March 8, 2026"
    return f"""
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <meta name='robots' content='index,follow' />
  <link rel='canonical' href='{escape(canonical_url)}' />
  <title>{escape(title)} | Black Swan Vault</title>
  {analytics_tag}
  <style>
    :root {{
      --bg:#06080d;
      --panel:#0d1119;
      --line:#242b38;
      --text:#e9edf6;
      --muted:#96a0b8;
      --accent:#ff9f1a;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0;
      color:var(--text);
      font-family:"Space Grotesk","Segoe UI",sans-serif;
      background-color:var(--bg);
      background-image:
        linear-gradient(rgba(255,159,26,.07) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,159,26,.07) 1px, transparent 1px),
        radial-gradient(circle at 18% 12%, rgba(255,159,26,.18), transparent 54%);
      background-size:96px 96px,96px 96px,100% 100%;
    }}
    .wrap {{ max-width:900px; margin:0 auto; padding:24px 16px 40px; }}
    .card {{
      background:linear-gradient(180deg,rgba(255,159,26,.06),rgba(255,159,26,.015) 42%,rgba(13,17,25,.95));
      border:1px solid var(--line);
      border-radius:18px;
      padding:22px;
    }}
    h1 {{ margin:0 0 10px; font-size:34px; }}
    h2 {{ margin:22px 0 8px; font-size:20px; color:#ffd18a; }}
    p, li {{ color:#d4daea; line-height:1.6; }}
    ul {{ margin:0; padding-left:20px; }}
    .top-links {{
      display:flex;
      flex-wrap:wrap;
      gap:10px;
      margin-bottom:16px;
    }}
    .top-links a {{
      color:#ffc66a;
      text-decoration:none;
      border:1px solid #805215;
      border-radius:999px;
      padding:8px 12px;
      font-size:14px;
      background:rgba(255,159,26,.12);
    }}
    .top-links a:hover {{ background:rgba(255,159,26,.2); }}
    .updated {{ color:var(--muted); font-size:13px; margin-top:18px; }}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='card'>
      <div class='top-links'>
        <a href='/'>Back to Directory</a>
        <a href='/terms'>Terms</a>
        <a href='/privacy'>Privacy</a>
        <a href='/disclaimer'>Disclaimer</a>
      </div>
      <h1>{escape(title)}</h1>
      {body_html}
      <p class='updated'>Last updated: {escape(updated_on)}</p>
    </div>
  </div>
</body>
</html>
"""


def _render_terms_page(request: web.Request, *, google_analytics_measurement_id: str) -> str:
    return _render_legal_page(
        request=request,
        title="Terms of Service",
        google_analytics_measurement_id=google_analytics_measurement_id,
        body_html="""
<p>
  By using Black Swan Vault, you agree to these Terms. If you do not agree, do not use the service.
</p>
<h2>Service Scope</h2>
<ul>
  <li>The service publishes trade activity and analytics for informational purposes.</li>
  <li>Availability, data sources, features, and filters may change at any time without notice.</li>
  <li>We may suspend, limit, or remove access for abuse, misuse, or security reasons.</li>
</ul>
<h2>User Responsibilities</h2>
<ul>
  <li>You are solely responsible for your decisions, trades, risk management, and legal compliance.</li>
  <li>You must not use this service for unlawful, abusive, or automated attacks on infrastructure.</li>
  <li>You must not rely on service uptime or signal completeness as guaranteed.</li>
</ul>
<h2>Risk and No Warranty</h2>
<ul>
  <li>Trading derivatives and crypto assets carries substantial risk, including total loss of capital.</li>
  <li>Data may be delayed, incomplete, or inaccurate; no performance outcome is guaranteed.</li>
  <li>The service is provided "as is" and "as available", without warranties of any kind.</li>
</ul>
<h2>Liability</h2>
<ul>
  <li>To the maximum extent permitted by law, Black Swan Vault is not liable for losses, damages, or missed opportunities resulting from use of this service.</li>
  <li>This includes direct, indirect, incidental, consequential, and special damages.</li>
</ul>
<h2>Contact</h2>
<p>Operational and legal contact: <code>sanyo4ever@gmail.com</code></p>
""",
    )


def _render_privacy_page(request: web.Request, *, google_analytics_measurement_id: str) -> str:
    return _render_legal_page(
        request=request,
        title="Privacy Policy",
        google_analytics_measurement_id=google_analytics_measurement_id,
        body_html="""
<p>
  This Privacy Policy explains what data we process to run Black Swan Vault.
</p>
<h2>What We Collect</h2>
<ul>
  <li>Basic server logs: request metadata, IP address, timestamp, and user-agent.</li>
  <li>Subscription request telemetry used for service operations and abuse prevention.</li>
  <li>Analytics events through Google Analytics (if enabled on this deployment).</li>
</ul>
<h2>Why We Process Data</h2>
<ul>
  <li>Operate, secure, and debug the service.</li>
  <li>Measure usage and improve performance and reliability.</li>
  <li>Investigate abuse, incidents, and policy violations.</li>
</ul>
<h2>Data Sharing</h2>
<ul>
  <li>We do not sell personal data.</li>
  <li>Limited data may be processed by infrastructure and analytics providers as needed to operate the service.</li>
</ul>
<h2>Retention</h2>
<ul>
  <li>Operational logs and telemetry are retained only as long as necessary for security, troubleshooting, and service quality.</li>
</ul>
<h2>Your Choices</h2>
<ul>
  <li>You can stop using the service at any time.</li>
  <li>You can block analytics scripts in your browser if you prefer not to share analytics data.</li>
</ul>
<h2>Contact</h2>
<p>Privacy contact: <code>sanyo4ever@gmail.com</code></p>
""",
    )


def _render_disclaimer_page(
    request: web.Request,
    *,
    google_analytics_measurement_id: str,
) -> str:
    return _render_legal_page(
        request=request,
        title="Disclaimer",
        google_analytics_measurement_id=google_analytics_measurement_id,
        body_html="""
<p><strong>Not financial advice.</strong></p>
<p>
  Black Swan Vault is an informational analytics and signal-delivery tool. It does not provide
  investment, financial, tax, legal, or accounting advice.
</p>
<h2>No Recommendation</h2>
<ul>
  <li>Nothing on this website, in Telegram topics, or in related messages is a recommendation to buy, sell, or hold any asset.</li>
  <li>Signals reflect observed activity and may be delayed, partial, or incorrect.</li>
</ul>
<h2>High-Risk Activity</h2>
<ul>
  <li>Crypto and derivatives trading involve high risk and may result in full capital loss.</li>
  <li>Past performance does not guarantee future results.</li>
</ul>
<h2>Your Responsibility</h2>
<ul>
  <li>You are solely responsible for your own decisions and risk controls.</li>
  <li>Always do your own research and consult a licensed advisor where appropriate.</li>
</ul>
""",
    )


def _render_admin_index(*, traders, discovery_runs, message: str | None = None) -> str:
    active_count = sum(1 for trader in traders if trader.status == STATUS_ACTIVE)
    blacklisted_count = sum(
        1 for trader in traders if trader.moderation_state == MODERATION_BLACKLIST
    )
    whitelisted_count = sum(
        1 for trader in traders if trader.moderation_state == MODERATION_WHITELIST
    )
    recent_run = discovery_runs[0] if discovery_runs else None
    recent_run_label = (
        f"{recent_run.status.upper()} at {recent_run.finished_at}"
        if recent_run
        else "-"
    )

    rows = []
    for trader in traders:
        encoded = quote(trader.address, safe="")
        moderation_class = "moder-neutral"
        if trader.moderation_state == MODERATION_BLACKLIST:
            moderation_class = "moder-blacklist"
        elif trader.moderation_state == MODERATION_WHITELIST:
            moderation_class = "moder-whitelist"
        moderation_note = trader.moderation_note or "-"

        rows.append(
            "<tr>"
            f"<td><input type='checkbox' name='addresses' value='{escape(trader.address)}' form='bulk-form' /></td>"
            f"<td><code>{escape(trader.address)}</code></td>"
            f"<td>{escape(trader.label or '-')}</td>"
            f"<td>{escape(trader.source)}</td>"
            f"<td>{escape(trader.status)}</td>"
            f"<td><span class='moder-tag {moderation_class}'>{escape(trader.moderation_state)}</span></td>"
            f"<td>{escape(moderation_note)}</td>"
            f"<td>{_fmt(trader.age_days, 1)}</td>"
            f"<td>{_fmt(trader.trades_30d, 0)}</td>"
            f"<td>{_fmt(trader.active_days_30d, 0)}</td>"
            f"<td>{_fmt((trader.win_rate_30d or 0.0) * 100.0, 1)}%</td>"
            f"<td>{_fmt(trader.realized_pnl_30d, 2)}</td>"
            f"<td>{_fmt(trader.score, 2)}</td>"
            "<td>"
            f"<form method='post' action='/admin/traders/{encoded}/moderate/whitelist' style='display:inline'>"
            "<button class='btn whitelist' type='submit'>Whitelist</button></form> "
            f"<form method='post' action='/admin/traders/{encoded}/moderate/blacklist' style='display:inline'>"
            "<button class='btn blacklist' type='submit'>Blacklist</button></form> "
            f"<form method='post' action='/admin/traders/{encoded}/moderate/neutral' style='display:inline'>"
            "<button class='btn' type='submit'>Neutral</button></form> "
            f"<form method='post' action='/admin/traders/{encoded}/delete' style='display:inline' onsubmit='return confirm(\"Archive trader?\")'>"
            "<button class='btn danger' type='submit'>Archive</button></form>"
            "</td>"
            "</tr>"
        )

    table_rows = (
        "\n".join(rows)
        if rows
        else "<tr><td colspan='14'>No tracked traders yet.</td></tr>"
    )
    discovery_rows = []
    for run in discovery_runs:
        discovery_rows.append(
            "<tr>"
            f"<td>{run.id}</td>"
            f"<td>{escape(run.status.upper())}</td>"
            f"<td>{escape(run.source)}</td>"
            f"<td>{run.candidates}</td>"
            f"<td>{run.qualified}</td>"
            f"<td>{run.upserted}</td>"
            f"<td>{escape(run.finished_at)}</td>"
            f"<td>{escape(run.error_message or '-')}</td>"
            "</tr>"
        )

    discovery_table_rows = (
        "\n".join(discovery_rows)
        if discovery_rows
        else "<tr><td colspan='8'>No discovery runs yet.</td></tr>"
    )
    flash = f"<div class='flash'>{escape(message)}</div>" if message else ""

    return f"""
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>Admin Discovery Panel</title>
  <style>
    :root {{ --bg:#0b1020; --panel:#141b2d; --line:#29324b; --text:#e7edf8; --muted:#9fb2d1; --ok:#38c172; --danger:#ff5d5d; --accent:#3f8cff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Space Grotesk","Segoe UI",sans-serif; color:var(--text); background:radial-gradient(circle at top right,#17244a 0%,var(--bg) 55%); }}
    .container {{ max-width:1400px; margin:24px auto; padding:0 20px; }}
    .card {{ background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.01)); border:1px solid var(--line); border-radius:16px; padding:16px; margin-bottom:16px; }}
    .row {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; }}
    .stat {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:10px 14px; min-width:140px; }}
    .stat strong {{ display:block; font-size:16px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ border-bottom:1px solid var(--line); padding:8px 6px; text-align:left; }}
    th {{ color:var(--muted); }}
    .btn {{ border:1px solid var(--line); color:var(--text); background:#1e2740; border-radius:8px; padding:6px 10px; cursor:pointer; }}
    .whitelist {{ border-color:var(--ok); }}
    .blacklist {{ border-color:#ff9f43; }}
    .danger {{ border-color:var(--danger); }}
    input, select, textarea {{ border:1px solid var(--line); background:#0e1427; color:var(--text); border-radius:8px; padding:8px 10px; }}
    textarea {{ width:380px; min-height:74px; resize:vertical; }}
    input[type=checkbox] {{ min-width:auto; }}
    .flash {{ border:1px solid var(--accent); background:rgba(63,140,255,.15); border-radius:10px; padding:10px 12px; margin-bottom:14px; }}
    .moder-tag {{ font-size:11px; padding:2px 8px; border-radius:999px; border:1px solid var(--line); }}
    .moder-neutral {{ color:var(--muted); }}
    .moder-whitelist {{ color:var(--ok); border-color:var(--ok); }}
    .moder-blacklist {{ color:#ff9f43; border-color:#ff9f43; }}
    code {{ color:#90e5ff; }}
  </style>
</head>
<body>
  <div class='container'>
    <div class='card'>
      <h1>Discovery Admin</h1>
      {flash}
      <div class='row'>
        <div class='stat'><span>Tracked</span><strong>{len(traders)}</strong></div>
        <div class='stat'><span>Listed</span><strong>{active_count}</strong></div>
        <div class='stat'><span>Whitelist</span><strong>{whitelisted_count}</strong></div>
        <div class='stat'><span>Blacklist</span><strong>{blacklisted_count}</strong></div>
        <div class='stat'><span>Last Discovery</span><strong>{escape(recent_run_label)}</strong></div>
      </div>
    </div>

    <div class='card'>
      <form method='post' action='/admin/discover'>
        <button class='btn' type='submit'>Run Discovery Now</button>
      </form>
    </div>

    <div class='card'>
      <h3>Bulk Actions</h3>
      <form id='bulk-form' method='post' action='/admin/traders/bulk' class='row'>
        <select name='action'>
          <option value='whitelist'>Whitelist selected</option>
          <option value='blacklist'>Blacklist selected</option>
          <option value='neutral'>Set neutral selected</option>
        </select>
        <input name='moderation_note' placeholder='Optional moderation note' />
        <textarea name='bulk_addresses' placeholder='Optional: paste addresses (space/newline/comma separated)'></textarea>
        <button class='btn' type='submit'>Apply Bulk Action</button>
      </form>
      <p style='color:var(--muted); margin:8px 0 0;'>Tip: tick checkboxes in table and/or paste addresses.</p>
    </div>

    <div class='card'>
      <form method='post' action='/admin/traders/add'>
        <input name='address' placeholder='0x... trader address' required />
        <input name='label' placeholder='Optional label' />
        <button class='btn' type='submit'>Add Trader (LISTED)</button>
      </form>
    </div>

    <div class='card'>
      <table>
        <thead>
          <tr>
            <th>#</th><th>Address</th><th>Label</th><th>Source</th><th>Status</th><th>Moderation</th><th>Note</th>
            <th>Age d</th><th>Trades 30d</th><th>Active Days 30d</th><th>Win 30d</th><th>PnL 30d</th><th>Score</th><th>Actions</th>
          </tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>

    <div class='card'>
      <h3>Recent Discovery Runs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>Status</th><th>Source</th><th>Candidates</th><th>Qualified</th><th>Upserted</th><th>Finished</th><th>Error</th></tr>
        </thead>
        <tbody>{discovery_table_rows}</tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


def _subscription_redirect_url(settings) -> str:
    join_url = _resolve_join_url(settings)
    if join_url:
        return join_url
    fallback = str(getattr(settings, "telegram_channel_id", "") or "").strip()
    if fallback.startswith("@"):
        return f"https://t.me/{fallback.removeprefix('@')}"
    return ""


def _forum_chat_id(settings) -> str:
    value = str(getattr(settings, "telegram_forum_chat_id", "") or "").strip()
    if value:
        return value
    return str(getattr(settings, "telegram_channel_id", "") or "").strip()


def _forum_topic_name(address: str) -> str:
    normalized = str(address or "").strip().lower()
    if len(normalized) <= 18:
        return normalized or "Trader"
    return f"{normalized[:10]}...{normalized[-6:]}"


def _forum_topic_url(*, forum_chat_id: str, message_thread_id: int) -> str | None:
    chat = str(forum_chat_id or "").strip()
    thread_id = int(message_thread_id or 0)
    if not chat or thread_id <= 0:
        return None
    if chat.startswith("@"):
        username = chat.removeprefix("@").strip()
        if username:
            return f"https://t.me/{username}/{thread_id}"
    if chat.startswith("-100") and chat[4:].isdigit():
        return f"https://t.me/c/{chat[4:]}/{thread_id}"
    return None


async def telegram_topic_redirect(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    session: aiohttp.ClientSession = request.app["http_session"]
    logger: logging.Logger = request.app["logger"]
    address = str(request.match_info.get("address", "")).strip().lower()

    with TraderStore(settings.database_dsn) as store:
        trader = store.get_trader(address=address)
        if trader is None:
            raise web.HTTPNotFound(text="Trader not found")
        if trader.moderation_state == MODERATION_BLACKLIST:
            raise web.HTTPForbidden(text="Trader is not available")
        if trader.status in {STATUS_STALE, STATUS_ARCHIVED}:
            raise web.HTTPServiceUnavailable(text="Trader is currently not eligible")

    forum_chat_id = _forum_chat_id(settings)
    join_url = _subscription_redirect_url(settings)
    if not forum_chat_id:
        if join_url:
            raise web.HTTPFound(join_url)
        return web.Response(status=503, text="Forum chat is not configured")

    thread_id: int | None = None
    with TraderStore(settings.database_dsn) as store:
        existing = store.get_trader_forum_topic(
            trader_address=address,
            forum_chat_id=forum_chat_id,
        )
    if existing is not None and int(existing.message_thread_id or 0) > 0:
        thread_id = int(existing.message_thread_id)
    else:
        try:
            result = await create_forum_topic(
                session,
                bot_token=settings.telegram_bot_token,
                chat_id=forum_chat_id,
                name=_forum_topic_name(address),
            )
            candidate = int(result.get("message_thread_id") or 0)
            if candidate <= 0:
                raise RuntimeError(f"Invalid createForumTopic response: {result}")
            thread_id = candidate
            with TraderStore(settings.database_dsn) as store:
                store.upsert_trader_forum_topic(
                    trader_address=address,
                    forum_chat_id=forum_chat_id,
                    message_thread_id=thread_id,
                    topic_name=_forum_topic_name(address),
                )
            logger.info(
                "Created forum topic from web redirect trader=%s chat_id=%s thread=%s",
                address,
                forum_chat_id,
                thread_id,
            )
        except TelegramClientError as exc:
            logger.warning(
                "Failed to create forum topic from web redirect trader=%s chat_id=%s: %s",
                address,
                forum_chat_id,
                exc,
            )
            if join_url:
                raise web.HTTPFound(join_url)
            return web.Response(status=502, text="Failed to open Telegram topic")

    topic_url = _forum_topic_url(
        forum_chat_id=forum_chat_id,
        message_thread_id=int(thread_id or 0),
    )
    if topic_url:
        raise web.HTTPFound(topic_url)
    if join_url:
        raise web.HTTPFound(join_url)
    return web.Response(status=503, text="Topic URL is unavailable")


async def subscriber_directory(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    selected_period = _normalize_catalog_period(request.query.get("period"))
    sort_by = _normalize_catalog_sort(request.query.get("sort"), period=selected_period)
    limit = max(1, min(200, _to_int(request.query.get("limit"), settings.live_top100_size)))
    cursor_raw = str(request.query.get("cursor", "")).strip()
    cursor = _decode_cursor(cursor_raw, expected_sort=sort_by) if cursor_raw else None

    def _opt_float(name: str, default: float | None = None) -> float | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _opt_int(name: str, default: int | None = None) -> int | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    min_age_days = _opt_float("min_age_days")
    min_trades_30d = _opt_int("min_trades_30d")
    min_active_days_30d = _opt_int("min_active_days_30d")
    min_win_rate_30d_pct = _opt_float("min_win_rate_30d")
    min_realized_pnl_30d = _opt_float("min_realized_pnl_30d")
    min_score = _opt_float("min_score")
    min_activity_score = _opt_float("min_activity_score")
    active_within_minutes = _opt_int("active_within_minutes")

    forum_chat_id = _forum_chat_id(settings)
    with TraderStore(settings.database_dsn) as store:
        traders = store.list_catalog_traders(
            limit=limit + 1,
            q=str(request.query.get("q", "")),
            status=str(request.query.get("status", STATUS_ACTIVE_LISTED)).upper(),
            min_age_days=min_age_days,
            min_trades_30d=min_trades_30d,
            min_active_days_30d=min_active_days_30d,
            min_win_rate_30d=(
                (min_win_rate_30d_pct / 100.0)
                if min_win_rate_30d_pct is not None
                else None
            ),
            min_realized_pnl_30d=min_realized_pnl_30d,
            min_score=min_score,
            min_activity_score=min_activity_score,
            active_within_minutes=active_within_minutes,
            sort_by=sort_by,
            cursor_value=cursor[0] if cursor else None,
            cursor_address=cursor[1] if cursor else None,
        )
        topic_links: dict[str, str] = {}
        for trader in traders[:limit]:
            topic = store.get_trader_forum_topic(
                trader_address=trader.address,
                forum_chat_id=forum_chat_id,
            )
            if topic is not None and int(topic.message_thread_id or 0) > 0:
                url = _forum_topic_url(
                    forum_chat_id=forum_chat_id,
                    message_thread_id=int(topic.message_thread_id),
                )
                if url:
                    topic_links[trader.address] = url
            else:
                encoded = quote(trader.address, safe="")
                topic_links[trader.address] = f"/telegram/{encoded}/view"

    next_cursor: str | None = None
    if len(traders) > limit:
        visible = traders[:limit]
        last = visible[-1]
        next_cursor = _encode_cursor(
            sort_by=sort_by,
            value=_catalog_sort_value(trader=last, sort_by=sort_by),
            address=last.address,
        )
        traders = visible

    return web.Response(
        text=_render_public_directory(
            traders=traders,
            request=request,
            join_url=_resolve_join_url(settings),
            topic_links=topic_links,
            next_cursor=next_cursor,
            google_analytics_measurement_id=settings.google_analytics_measurement_id,
        ),
        content_type="text/html",
    )


async def traders_api(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    selected_period = _normalize_catalog_period(request.query.get("period"))
    sort_by = _normalize_catalog_sort(request.query.get("sort"), period=selected_period)
    limit = max(1, min(200, _to_int(request.query.get("limit"), 100)))
    cursor_raw = str(request.query.get("cursor", "")).strip()
    cursor = _decode_cursor(cursor_raw, expected_sort=sort_by) if cursor_raw else None

    def _opt_float(name: str, default: float | None = None) -> float | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _opt_int(name: str, default: int | None = None) -> int | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    with TraderStore(settings.database_dsn) as store:
        traders = store.list_catalog_traders(
            limit=limit + 1,
            q=str(request.query.get("q", "")),
            status=str(request.query.get("status", STATUS_ACTIVE_LISTED)).upper(),
            moderation_state=str(request.query.get("moderation_state", "ALL")).upper(),
            min_age_days=_opt_float("min_age_days"),
            min_trades_30d=_opt_int("min_trades_30d"),
            min_active_days_30d=_opt_int("min_active_days_30d"),
            min_win_rate_30d=(
                (_opt_float("min_win_rate_30d") or 0.0) / 100.0
                if request.query.get("min_win_rate_30d") is not None
                else None
            ),
            min_realized_pnl_30d=_opt_float("min_realized_pnl_30d"),
            min_score=_opt_float("min_score"),
            min_activity_score=_opt_float("min_activity_score"),
            active_within_minutes=_opt_int("active_within_minutes"),
            sort_by=sort_by,
            cursor_value=cursor[0] if cursor else None,
            cursor_address=cursor[1] if cursor else None,
        )

    next_cursor: str | None = None
    if len(traders) > limit:
        visible = traders[:limit]
        last = visible[-1]
        next_cursor = _encode_cursor(
            sort_by=sort_by,
            value=_catalog_sort_value(trader=last, sort_by=sort_by),
            address=last.address,
        )
        traders = visible

    is_admin = _is_admin_authorized(request)
    items: list[dict[str, Any]] = []
    for trader in traders:
        item = {
            "address": trader.address,
            "label": trader.label,
            "source": trader.source,
            "status": trader.status,
            "age_days": trader.age_days,
            "trades_24h": trader.trades_24h,
            "active_hours_24h": trader.active_hours_24h,
            "trades_7d": trader.trades_7d,
            "trades_30d": trader.trades_30d,
            "active_days_30d": trader.active_days_30d,
            "win_rate_30d": trader.win_rate_30d,
            "realized_pnl_30d": trader.realized_pnl_30d,
            "volume_usd_30d": trader.volume_usd_30d,
            "score": trader.score,
            "activity_score": trader.activity_score,
            "last_fill_time": trader.last_fill_time,
            "refreshed_at": trader.refreshed_at,
            **_extract_stat_metrics(trader.stats_json),
        }
        if is_admin:
            item["moderation_state"] = trader.moderation_state
            item["moderation_note"] = trader.moderation_note
        items.append(item)

    return web.json_response(
        {
            "items": items,
            "next_cursor": next_cursor,
            "sort": sort_by,
            "period": selected_period,
            "limit": limit,
        }
    )


async def subscribe_redirect(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        trader = store.get_trader(address=address)
        if trader is None:
            raise web.HTTPNotFound(text="Trader not found")
        if trader.moderation_state == MODERATION_BLACKLIST:
            raise web.HTTPForbidden(text="Trader is not available for subscription")
        if trader.status in {STATUS_STALE, STATUS_ARCHIVED}:
            raise web.HTTPServiceUnavailable(
                text="Trader is currently not eligible for new subscriptions."
            )

        xff = request.headers.get("X-Forwarded-For", "")
        client_ip = xff.split(",")[0].strip() if xff else request.remote
        store.record_subscription_request(
            trader_address=trader.address,
            client_ip=client_ip,
            user_agent=request.headers.get("User-Agent", ""),
        )
    logger.info("Subscription redirect trader=%s client_ip=%s", trader.address, _client_ip(request))

    destination_url = _subscription_redirect_url(settings)
    if not destination_url:
        return web.Response(
            status=503,
            text="TELEGRAM_JOIN_URL is not configured on server.",
        )

    raise web.HTTPFound(destination_url)


async def subscribe_landing(request: web.Request) -> web.Response:
    address = quote(request.match_info.get("address", ""), safe="")
    raise web.HTTPFound(f"/subscribe/{address}/go")


async def terms_page(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    return web.Response(
        text=_render_terms_page(
            request,
            google_analytics_measurement_id=settings.google_analytics_measurement_id,
        ),
        content_type="text/html",
    )


async def privacy_page(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    return web.Response(
        text=_render_privacy_page(
            request,
            google_analytics_measurement_id=settings.google_analytics_measurement_id,
        ),
        content_type="text/html",
    )


async def disclaimer_page(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    return web.Response(
        text=_render_disclaimer_page(
            request,
            google_analytics_measurement_id=settings.google_analytics_measurement_id,
        ),
        content_type="text/html",
    )


async def admin_index(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    message = request.query.get("msg")

    with TraderStore(settings.database_dsn) as store:
        traders = store.list_traders(limit=1000)
        discovery_runs = store.list_recent_discovery_runs(limit=30)

    return web.Response(
        text=_render_admin_index(traders=traders, discovery_runs=discovery_runs, message=message),
        content_type="text/html",
    )


async def add_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    payload = await request.post()

    address = str(payload.get("address", "")).strip()
    label = str(payload.get("label", "")).strip() or None
    if not address:
        raise web.HTTPFound("/admin?msg=Address+is+required")

    with TraderStore(settings.database_dsn) as store:
        store.add_manual(address=address, label=label)
    logger.info("Trader added manually address=%s label=%s", address.lower(), label or "-")

    raise web.HTTPFound("/admin?msg=Trader+added")


async def bulk_trader_action(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    payload = await request.post()
    action = str(payload.get("action", "")).strip().lower()
    note = str(payload.get("moderation_note", "")).strip() or None

    addresses = [str(item).strip() for item in payload.getall("addresses", []) if str(item).strip()]
    addresses.extend(_split_addresses(payload.get("bulk_addresses", "")))

    # Preserve order while deduplicating.
    unique_addresses: list[str] = []
    seen: set[str] = set()
    for address in addresses:
        lowered = address.lower()
        if lowered in seen:
            continue
        unique_addresses.append(address)
        seen.add(lowered)

    if not unique_addresses:
        raise web.HTTPFound("/admin?msg=No+addresses+selected+for+bulk+action")

    with TraderStore(settings.database_dsn) as store:
        moderation_state = _parse_moderation_state(action)
        if moderation_state is None:
            raise web.HTTPFound("/admin?msg=Unknown+bulk+action")
        changed = store.set_moderation_bulk(
            addresses=unique_addresses,
            moderation_state=moderation_state,
            note=note,
        )
        logger.info(
            "Bulk moderation action=%s addresses=%s changed=%s",
            moderation_state,
            len(unique_addresses),
            changed,
        )
        raise web.HTTPFound(f"/admin?msg=Bulk+moderation+applied:+{changed}")


async def moderate_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    address = request.match_info.get("address", "")
    state_slug = request.match_info.get("state", "")
    moderation_state = _parse_moderation_state(state_slug)
    if moderation_state is None:
        raise web.HTTPFound("/admin?msg=Unsupported+moderation+state")

    with TraderStore(settings.database_dsn) as store:
        if store.get_trader(address=address) is None:
            raise web.HTTPFound("/admin?msg=Trader+not+found")
        store.set_moderation(address=address, moderation_state=moderation_state)
    logger.info("Trader moderation updated address=%s state=%s", address.lower(), moderation_state)

    raise web.HTTPFound(f"/admin?msg=Moderation+set+to+{quote(moderation_state)}")


async def delete_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        store.delete(address=address)
    logger.info("Trader archived (soft-delete) address=%s", address.lower())

    raise web.HTTPFound("/admin?msg=Trader+archived")


async def run_discovery(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    session = request.app["http_session"]
    logger: logging.Logger = request.app["logger"]

    discovery_lock: asyncio.Lock = request.app["discovery_lock"]
    if discovery_lock.locked():
        raise web.HTTPFound("/admin?msg=Discovery+already+running")
    async with discovery_lock:
        with TraderStore(settings.database_dsn) as store:
            service = HyperliquidDiscoveryService(
                http_session=session,
                store=store,
                config=_discovery_config_from_settings(settings),
                logger=logger,
            )
            summary = await service.discover()
    logger.info(
        "Discovery triggered from admin: candidates=%s qualified=%s upserted=%s unlisted=%s",
        summary["candidates"],
        summary["qualified"],
        summary["upserted"],
        summary.get("unlisted", summary.get("pruned", 0)),
    )

    msg = (
        f"Discovery complete: candidates={summary['candidates']}, "
        f"qualified={summary['qualified']}, upserted={summary['upserted']}, "
        f"unlisted={summary.get('unlisted', summary.get('pruned', 0))}"
    )
    raise web.HTTPFound(f"/admin?msg={quote(msg)}")


async def _on_startup(app: web.Application) -> None:
    settings = app["settings"]
    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    app["http_session"] = aiohttp.ClientSession(timeout=timeout)
    app["logger"].info("Admin app startup complete")


async def _on_cleanup(app: web.Application) -> None:
    session = app.get("http_session")
    if session is not None:
        await session.close()
    app["logger"].info("Admin app cleanup complete")


def create_app(*, settings=None, logger: logging.Logger | None = None) -> web.Application:
    resolved_settings = settings or load_settings(require_telegram=False, require_admin_password=True)
    resolved_logger = logger or logging.getLogger("cryptoinsider.admin")
    app = web.Application(
        middlewares=[
            _request_logging_middleware,
            _admin_auth_middleware,
            _admin_csrf_middleware,
            _security_headers_middleware,
        ]
    )
    app["settings"] = resolved_settings
    app["logger"] = resolved_logger
    app["discovery_lock"] = asyncio.Lock()

    assets_dir = Path(__file__).resolve().parents[1] / "assets"
    if assets_dir.exists():
        app.router.add_static("/assets", str(assets_dir), show_index=False)
    else:
        resolved_logger.warning("Assets directory not found path=%s", assets_dir)

    app.add_routes(
        [
            web.get("/", subscriber_directory),
            web.get("/directory", subscriber_directory),
            web.get("/terms", terms_page),
            web.get("/privacy", privacy_page),
            web.get("/disclaimer", disclaimer_page),
            web.get("/api/traders", traders_api),
            web.get("/subscribe/{address}", subscribe_landing),
            web.get("/subscribe/{address}/go", subscribe_redirect),
            web.get("/telegram/{address}/view", telegram_topic_redirect),
            web.get("/admin", admin_index),
            web.post("/admin/discover", run_discovery),
            web.post("/admin/traders/add", add_trader),
            web.post("/admin/traders/bulk", bulk_trader_action),
            web.post("/admin/traders/{address}/moderate/{state}", moderate_trader),
            web.post("/admin/traders/{address}/delete", delete_trader),
        ]
    )

    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Futures trader admin server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()


def run() -> None:
    args = _parse_args()
    settings = load_settings(require_telegram=False, require_admin_password=True)
    setup_logging(
        service_name="cryptoinsider.admin",
        options=build_logging_options(settings),
    )
    logger = logging.getLogger("cryptoinsider.admin")
    logger.info("Starting admin server host=%s port=%s", args.host, args.port)
    web.run_app(
        create_app(settings=settings, logger=logger),
        host=args.host,
        port=args.port,
        access_log=None,
    )


if __name__ == "__main__":
    run()
