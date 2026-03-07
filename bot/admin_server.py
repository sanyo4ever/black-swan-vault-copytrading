from __future__ import annotations

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
from bot.trader_store import (
    CatalogTrader,
    MODERATION_BLACKLIST,
    MODERATION_NEUTRAL,
    MODERATION_WHITELIST,
    STATUS_ACTIVE,
    TraderStore,
)

PROJECT_REPO_URL = "https://github.com/sanyo4ever/black-swan-vault-copytrading"
PUBLIC_SITE_URL = "https://blackswanvault.online"
PAYPAL_DONATION_EMAIL = "sanyo4ever@gmail.com"
USDT_TRC20_DONATION_ADDRESS = "TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ"


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
        min_trades_7d=settings.discovery_min_trades_7d,
        window_hours=settings.discovery_window_hours,
        concurrency=settings.discovery_concurrency,
        fill_cap_hint=settings.discovery_fill_cap_hint,
        age_probe_enabled=settings.discovery_age_probe_enabled,
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

    metrics_7d = payload.get("metrics_7d")
    if not isinstance(metrics_7d, dict):
        metrics_7d = {}
    metrics_30d = payload.get("metrics_30d")
    if not isinstance(metrics_30d, dict):
        metrics_30d = {}

    return {
        "roi_7d": metrics_7d.get("roi_pct"),
        "roi_30d": metrics_30d.get("roi_pct"),
        "pnl_7d": metrics_7d.get("realized_pnl"),
        "pnl_30d": metrics_30d.get("realized_pnl"),
        "win_rate_7d": metrics_7d.get("win_rate"),
        "win_rate_30d": metrics_30d.get("win_rate"),
        "wins_7d": metrics_7d.get("wins"),
        "losses_7d": metrics_7d.get("losses"),
        "wins_30d": metrics_30d.get("wins"),
        "losses_30d": metrics_30d.get("losses"),
        "profit_to_loss_ratio_7d": metrics_7d.get("profit_to_loss_ratio"),
        "profit_to_loss_ratio_30d": metrics_30d.get("profit_to_loss_ratio"),
        "weekly_trades": metrics_7d.get("weekly_trades"),
        "avg_pnl_per_trade_7d": metrics_7d.get("avg_pnl_per_trade"),
        "avg_pnl_per_trade_30d": metrics_30d.get("avg_pnl_per_trade"),
        "max_drawdown_7d": metrics_7d.get("max_drawdown_pct"),
        "max_drawdown_30d": metrics_30d.get("max_drawdown_pct"),
        "sharpe_7d": metrics_7d.get("sharpe"),
        "sharpe_30d": metrics_30d.get("sharpe"),
        "sortino_7d": metrics_7d.get("sortino"),
        "sortino_30d": metrics_30d.get("sortino"),
        "roi_volatility_7d": metrics_7d.get("roi_volatility_pct"),
        "roi_volatility_30d": metrics_30d.get("roi_volatility_pct"),
    }


def _subscribe_button(*, trader_address: str, bot_username: str) -> str:
    if not bot_username:
        return "<span style='opacity:.7'>Bot not configured</span>"
    encoded = quote(trader_address, safe="")
    return (
        f"<a class='copy-btn' href='/subscribe/{encoded}' target='_blank' rel='noopener'>"
        "Copy"
        "</a>"
    )


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
                logging.INFO if exc.status < 400 else logging.WARNING,
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


_CATALOG_SORTS = {
    "activity_desc",
    "recent_desc",
    "score_desc",
    "pnl_desc",
    "win_desc",
    "trades_desc",
    "age_desc",
}


def _normalize_catalog_sort(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in _CATALOG_SORTS:
        return value
    return "activity_desc"


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
    bot_username: str,
    next_cursor: str | None,
) -> str:
    rows = []
    for index, trader in enumerate(traders, start=1):
        metrics = _extract_stat_metrics(trader.stats_json)
        drawdown_7d = metrics.get("max_drawdown_7d")
        drawdown_value = -float(drawdown_7d) if drawdown_7d is not None else None
        win_rate_7d = _fmt_percent(metrics.get("win_rate_7d"), digits=2, ratio=True)
        pl_ratio_7d_raw = metrics.get("profit_to_loss_ratio_7d")
        pl_ratio_7d = (
            f"{float(pl_ratio_7d_raw):.2f} : 1"
            if pl_ratio_7d_raw is not None
            else "-"
        )
        last_traded_at = _fmt_utc_timestamp_ms(trader.last_fill_time)
        minutes_since = _minutes_since(trader.last_fill_time)
        freshness = f"{minutes_since}m ago" if minutes_since is not None else "-"
        trader_label = trader.label or trader.address[:10]
        rows.append(
            "<tr>"
            f"<td>{_fmt(index, 0)}</td>"
            "<td class='trader-cell'>"
            f"<div class='trader-label'>{escape(trader_label)}</div>"
            f"<code>{escape(trader.address)}</code>"
            "</td>"
            f"<td>{_fmt_signed(metrics.get('roi_7d'), digits=2, suffix='%')}</td>"
            f"<td>{_fmt_signed(drawdown_value, digits=2, suffix='%')}</td>"
            f"<td>{_fmt_signed(metrics.get('pnl_7d'), digits=2)}</td>"
            f"<td>{win_rate_7d}</td>"
            f"<td>{pl_ratio_7d}</td>"
            f"<td>{_fmt_signed(metrics.get('sharpe_7d'), digits=2)}</td>"
            f"<td>{_fmt(metrics.get('weekly_trades'), 0)}</td>"
            f"<td>{_fmt(trader.trades_30d, 0)}</td>"
            "<td>"
            f"<div>{escape(last_traded_at)}</div>"
            f"<div class='muted-mini'>{escape(freshness)}</div>"
            "</td>"
            f"<td>{_subscribe_button(trader_address=trader.address, bot_username=bot_username)}</td>"
            "</tr>"
        )

    table_rows = (
        "\n".join(rows)
        if rows
        else "<tr><td colspan='12'>No traders match your filters.</td></tr>"
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
        "trading feeds in one click. Open-source and donation-supported."
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
                            "Yes. Access is open and donation-supported. You can browse traders and open "
                            "Telegram trader chats without a paywall."
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
                            "Click Open Trader Chat, start the bot, and the bot posts new fills from the "
                            "selected trader into your Telegram chat thread."
                        ),
                    },
                },
            ],
        },
        separators=(",", ":"),
    )

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
  <meta http-equiv='refresh' content='30' />
  <title>{escape(page_title)}</title>
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
    .hero-actions {{ margin-top:12px; display:flex; gap:10px; flex-wrap:wrap; }}
    .hero-btn {{
      display:inline-block;
      border-radius:999px;
      border:1px solid #805215;
      background:rgba(255,159,26,.12);
      color:#ffc66a;
      text-decoration:none;
      padding:8px 13px;
      font-size:12px;
      font-weight:600;
    }}
    .hero-btn:hover {{ background:rgba(255,159,26,.2); }}
    .hero-thanks {{ margin-top:10px; color:#d5a556; font-size:12px; }}
    .filter-form {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .filter-form input,.filter-form select {{
      background:#0b1018;
      border:1px solid #2f3442;
      color:var(--text);
      border-radius:999px;
      padding:9px 12px;
      font-size:12px;
    }}
    .filter-form button {{
      background:rgba(255,159,26,.16);
      border:1px solid #8a5a18;
      color:#ffbf54;
      border-radius:999px;
      padding:9px 14px;
      font-size:12px;
      font-weight:700;
      cursor:pointer;
    }}
    .filter-form button:hover {{ background:rgba(255,159,26,.25); }}
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
    .copy-btn {{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-width:98px;
      padding:8px 12px;
      border-radius:999px;
      border:1px solid #9c650f;
      color:#ffb23f;
      background:rgba(255,159,26,.1);
      text-decoration:none;
      font-size:13px;
      font-weight:700;
      letter-spacing:.2px;
    }}
    .copy-btn:hover {{ background:rgba(255,159,26,.2); }}
    .faq-item {{ border-top:1px solid var(--line); padding:10px 0; }}
    .faq-item:first-of-type {{ border-top:none; padding-top:0; }}
    .faq-item p {{ line-height:1.5; }}
    @media (max-width: 900px) {{
      .tab {{ font-size:17px; }}
      h1 {{ font-size:25px; }}
      .card {{ padding:12px; }}
      .brand-mark {{ width:62px; height:62px; border-radius:14px; }}
      .brand-name {{ font-size:16px; }}
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
          <p class='brand-tagline'>Open-source crypto copytrading intelligence</p>
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
        <a class='hero-btn' href='{escape(PROJECT_REPO_URL)}' target='_blank' rel='noopener'>Contribute on GitHub</a>
        <a class='hero-btn' href='{escape(PROJECT_REPO_URL)}/issues/new' target='_blank' rel='noopener'>Report Issue / Idea</a>
      </div>
      <div class='hero-thanks'>
        Thank you to everyone helping keep this server running. Your support makes public access possible.
      </div>
      <div class='quick-info'>
        <strong>How it works:</strong> Discovery workers collect and score futures traders continuously.<br/>
        Catalog refresh: <strong>{escape(refreshed_at)}</strong>.<br/>
        Table shows objective strategy metrics only (ROI, Drawdown, PnL, Win Rate, P/L Ratio, Sharpe, trade activity).<br/>
        Click <strong>Copy</strong> to receive free crypto signals from your selected trader in Telegram.<br/>
        Open-source repository: <a href='{escape(PROJECT_REPO_URL)}' target='_blank' rel='noopener'>GitHub</a>.<br/>
        Project is donation-supported: PayPal <code>{escape(PAYPAL_DONATION_EMAIL)}</code> or USDT TRC20 <code>{escape(USDT_TRC20_DONATION_ADDRESS)}</code>.<br/>
        Informational only. Not financial advice.
      </div>
      <div class='seo-grid'>
        <div class='seo-box'>
          <h2>Why Use This Copy Trading Directory</h2>
          <ul>
            <li>Free access to crypto futures trader data and Telegram signal delivery.</li>
            <li>Performance filters: ROI, win rate, PnL, activity, and consistency proxies.</li>
            <li>Open-source infrastructure you can audit, fork, and improve.</li>
          </ul>
        </div>
        <div class='seo-box'>
          <h2>Best For</h2>
          <ul>
            <li>Users searching for copy trading setups without closed paywalls.</li>
            <li>Traders comparing active accounts before choosing who to follow.</li>
            <li>Contributors building transparent crypto signal tooling.</li>
          </ul>
        </div>
      </div>
    </div>

    <div class='card'>
      <form class='filter-form' method='get' action='/'>
        <input name='q' placeholder='search label/address' value='{escape(str(request.query.get("q", "")))}' />
        <input name='min_age_days' type='number' step='1' min='0' value='{escape(str(request.query.get("min_age_days", "0")))}' placeholder='min age days' />
        <input name='min_trades_30d' type='number' step='1' min='0' value='{escape(str(request.query.get("min_trades_30d", "0")))}' placeholder='min trades 30d' />
        <input name='min_active_days_30d' type='number' step='1' min='0' value='{escape(str(request.query.get("min_active_days_30d", "0")))}' placeholder='min active days 30d' />
        <input name='min_win_rate_30d' type='number' step='0.1' min='0' max='100' value='{escape(str(request.query.get("min_win_rate_30d", "0")))}' placeholder='min winrate %' />
        <input name='min_realized_pnl_30d' type='number' step='0.01' value='{escape(str(request.query.get("min_realized_pnl_30d", "-1000000000")))}' placeholder='min pnl 30d' />
        <input name='min_score' type='number' step='0.01' value='{escape(str(request.query.get("min_score", "0")))}' placeholder='min score' />
        <input name='min_activity_score' type='number' step='0.01' value='{escape(str(request.query.get("min_activity_score", "0")))}' placeholder='min activity score' />
        <input name='active_within_minutes' type='number' step='1' min='0' value='{escape(str(request.query.get("active_within_minutes", "0")))}' placeholder='active within minutes' />
        <select name='sort'>
          <option value='activity_desc'>activity desc</option>
          <option value='recent_desc'>recent desc</option>
          <option value='score_desc'>score desc</option>
          <option value='pnl_desc'>pnl desc</option>
          <option value='win_desc'>win rate desc</option>
          <option value='trades_desc'>trades desc</option>
          <option value='age_desc'>age desc</option>
        </select>
        <button type='submit'>Apply Filters</button>
      </form>
    </div>

    <div class='card'>
      <div class='table-wrap'>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Trader</th>
            <th>7d ROI</th>
            <th>7d Drawdown</th>
            <th>7d PnL</th>
            <th>7d Win Rate</th>
            <th>7d Profit-to-Loss</th>
            <th>7d Sharpe</th>
            <th>Trades 7d</th>
            <th>Trades 30d</th>
            <th>Last Traded At</th>
            <th>Action</th>
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
        <p>Yes. The directory and Telegram flow are open and donation-supported.</p>
      </div>
      <div class='faq-item'>
        <h3>What kind of signals do I get?</h3>
        <p>You receive trader fill updates captured by the monitoring pipeline and posted to your Telegram thread.</p>
      </div>
      <div class='faq-item'>
        <h3>Is this financial advice?</h3>
        <p>No. It is an informational copy trading and analytics tool, not investment advice.</p>
      </div>
    </div>
  </div>
</body>
</html>
"""


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
            f"<form method='post' action='/admin/traders/{encoded}/delete' style='display:inline' onsubmit='return confirm(\"Delete trader?\")'>"
            "<button class='btn danger' type='submit'>Delete</button></form>"
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
  <meta http-equiv='refresh' content='20' />
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
        <div class='stat'><span>Active</span><strong>{active_count}</strong></div>
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
        <button class='btn' type='submit'>Add Trader (ACTIVE)</button>
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


def _build_subscribe_deep_link(*, bot_username: str, trader_address: str) -> str:
    start_payload = f"sub_{trader_address}"
    return f"https://t.me/{bot_username}?start={quote(start_payload, safe='')}"


def _render_subscribe_landing(
    *,
    trader,
    deep_link: str,
    go_link: str,
) -> str:
    label = trader.label or "-"
    return f"""
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>Subscribe Trader</title>
  <style>
    :root {{ --bg:#0b1220; --panel:#131f36; --line:#2b4267; --text:#e8f0ff; --muted:#9eb4d8; --accent:#4ca7ff; --ok:#4bd39e; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; min-height:100vh; display:grid; place-items:center; font-family:"Space Grotesk","Segoe UI",sans-serif; background:radial-gradient(circle at top,#1a2f52 0%,var(--bg) 62%); color:var(--text); padding:16px; }}
    .card {{ width:min(760px,100%); background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.01)); border:1px solid var(--line); border-radius:16px; padding:18px; }}
    h1 {{ margin:0 0 10px; font-size:24px; }}
    p {{ color:var(--muted); margin:6px 0; }}
    .meta {{ margin-top:12px; display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .box {{ border:1px solid var(--line); border-radius:12px; padding:10px; background:#0f1b31; }}
    .box strong {{ display:block; margin-bottom:3px; font-size:13px; color:var(--muted); }}
    code {{ color:#88d8ff; }}
    .cta {{ margin-top:16px; display:flex; flex-wrap:wrap; gap:10px; align-items:center; }}
    .btn {{ display:inline-block; text-decoration:none; border-radius:10px; padding:10px 14px; border:1px solid #3476bd; background:#1b4679; color:var(--text); }}
    .btn:hover {{ background:#235998; }}
    .note {{ margin-top:10px; color:var(--muted); font-size:13px; }}
    .ok {{ color:var(--ok); }}
  </style>
</head>
<body>
  <div class='card'>
    <h1>Trader Subscription</h1>
    <p>Open Telegram and start your personal trader chat thread.</p>
    <div class='meta'>
      <div class='box'><strong>Trader Address</strong><code>{escape(trader.address)}</code></div>
      <div class='box'><strong>Label</strong>{escape(label)}</div>
      <div class='box'><strong>Subscription</strong><span class='ok'>Active until cancellation</span></div>
      <div class='box'><strong>Support Model</strong><span class='ok'>Donation-supported (no paywall)</span></div>
      <div class='box'><strong>Open Source</strong><a href='{escape(PROJECT_REPO_URL)}' target='_blank' rel='noopener'>GitHub Repository</a></div>
      <div class='box'><strong>Donate (PayPal)</strong><code>{escape(PAYPAL_DONATION_EMAIL)}</code></div>
      <div class='box'><strong>Donate (USDT TRC20)</strong><code>{escape(USDT_TRC20_DONATION_ADDRESS)}</code></div>
    </div>
    <div class='cta'>
      <a class='btn' href='{escape(go_link)}'>Create Chat in Telegram</a>
      <a class='btn' href='{escape(deep_link)}' target='_blank' rel='noopener'>Open Bot Directly</a>
      <span class='note'>Auto-open in <span id='count'>5</span>s...</span>
    </div>
    <p class='note'>After <code>/start</code> in Telegram, bot creates a dedicated thread and posts trades until you cancel with <code>/stop 0x...</code>.</p>
  </div>
  <script>
    (function() {{
      var secs = 5;
      var el = document.getElementById('count');
      var link = {json.dumps(go_link)};
      var timer = setInterval(function() {{
        secs -= 1;
        if (secs <= 0) {{
          clearInterval(timer);
          window.location.href = link;
          return;
        }}
        if (el) el.textContent = String(secs);
      }}, 1000);
    }})();
  </script>
</body>
</html>
"""


async def subscriber_directory(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    sort_by = _normalize_catalog_sort(request.query.get("sort"))
    limit = max(1, min(200, _to_int(request.query.get("limit"), settings.live_top100_size)))
    cursor_raw = str(request.query.get("cursor", "")).strip()
    cursor = _decode_cursor(cursor_raw, expected_sort=sort_by) if cursor_raw else None

    def _opt_float(name: str, default: float | None = None) -> float | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        return _to_float(value, default if default is not None else 0.0)

    def _opt_int(name: str, default: int | None = None) -> int | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        return _to_int(value, default if default is not None else 0)

    with TraderStore(settings.database_dsn) as store:
        traders = store.list_catalog_traders(
            limit=limit + 1,
            q=str(request.query.get("q", "")),
            status=str(request.query.get("status", "ALL")).upper(),
            min_age_days=_opt_float("min_age_days", 0.0),
            min_trades_30d=_opt_int("min_trades_30d", 0),
            min_active_days_30d=_opt_int("min_active_days_30d", 0),
            min_win_rate_30d=(_opt_float("min_win_rate_30d", 0.0) or 0.0) / 100.0,
            min_realized_pnl_30d=_opt_float("min_realized_pnl_30d", -10**9),
            min_score=_opt_float("min_score", 0.0),
            min_activity_score=_opt_float("min_activity_score", 0.0),
            active_within_minutes=_opt_int("active_within_minutes", 0),
            sort_by=sort_by,
            cursor_value=cursor[0] if cursor else None,
            cursor_address=cursor[1] if cursor else None,
        )

    next_cursor: str | None = None
    if len(traders) > limit:
        visible = traders[:limit]
        last = visible[-1]
        sort_value_map: dict[str, float | int] = {
            "activity_desc": float(last.activity_score or -10**9),
            "recent_desc": int(last.last_fill_time or 0),
            "score_desc": float(last.score or -10**9),
            "pnl_desc": float(last.realized_pnl_30d or -10**9),
            "win_desc": float(last.win_rate_30d or -1.0),
            "trades_desc": int(last.trades_30d or -1),
            "age_desc": float(last.age_days or -1.0),
        }
        next_cursor = _encode_cursor(
            sort_by=sort_by,
            value=sort_value_map[sort_by],
            address=last.address,
        )
        traders = visible

    return web.Response(
        text=_render_public_directory(
            traders=traders,
            request=request,
            bot_username=settings.telegram_bot_username,
            next_cursor=next_cursor,
        ),
        content_type="text/html",
    )


async def traders_api(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    sort_by = _normalize_catalog_sort(request.query.get("sort"))
    limit = max(1, min(200, _to_int(request.query.get("limit"), 100)))
    cursor_raw = str(request.query.get("cursor", "")).strip()
    cursor = _decode_cursor(cursor_raw, expected_sort=sort_by) if cursor_raw else None

    def _opt_float(name: str, default: float | None = None) -> float | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        return _to_float(value, default if default is not None else 0.0)

    def _opt_int(name: str, default: int | None = None) -> int | None:
        value = request.query.get(name)
        if value is None or str(value).strip() == "":
            return default
        return _to_int(value, default if default is not None else 0)

    with TraderStore(settings.database_dsn) as store:
        traders = store.list_catalog_traders(
            limit=limit + 1,
            q=str(request.query.get("q", "")),
            status=str(request.query.get("status", "ALL")).upper(),
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
        sort_value_map: dict[str, float | int] = {
            "activity_desc": float(last.activity_score or -10**9),
            "recent_desc": int(last.last_fill_time or 0),
            "score_desc": float(last.score or -10**9),
            "pnl_desc": float(last.realized_pnl_30d or -10**9),
            "win_desc": float(last.win_rate_30d or -1.0),
            "trades_desc": int(last.trades_30d or -1),
            "age_desc": float(last.age_days or -1.0),
        }
        next_cursor = _encode_cursor(
            sort_by=sort_by,
            value=sort_value_map[sort_by],
            address=last.address,
        )
        traders = visible

    return web.json_response(
        {
            "items": [
                {
                    "address": trader.address,
                    "label": trader.label,
                    "source": trader.source,
                    "status": trader.status,
                    "moderation_state": trader.moderation_state,
                    "moderation_note": trader.moderation_note,
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
                for trader in traders
            ],
            "next_cursor": next_cursor,
            "sort": sort_by,
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

        xff = request.headers.get("X-Forwarded-For", "")
        client_ip = xff.split(",")[0].strip() if xff else request.remote
        store.record_subscription_request(
            trader_address=trader.address,
            client_ip=client_ip,
            user_agent=request.headers.get("User-Agent", ""),
        )
    logger.info("Subscription redirect trader=%s client_ip=%s", trader.address, _client_ip(request))

    if not settings.telegram_bot_username:
        return web.Response(
            status=503,
            text="TELEGRAM_BOT_USERNAME is not configured on server.",
        )

    deep_link = _build_subscribe_deep_link(
        bot_username=settings.telegram_bot_username,
        trader_address=trader.address,
    )
    raise web.HTTPFound(deep_link)


async def subscribe_landing(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        trader = store.get_trader(address=address)
        if trader is None:
            raise web.HTTPNotFound(text="Trader not found")
        if trader.moderation_state == MODERATION_BLACKLIST:
            raise web.HTTPForbidden(text="Trader is not available for subscription")

    if not settings.telegram_bot_username:
        return web.Response(
            status=503,
            text="TELEGRAM_BOT_USERNAME is not configured on server.",
        )

    deep_link = _build_subscribe_deep_link(
        bot_username=settings.telegram_bot_username,
        trader_address=trader.address,
    )
    go_link = f"/subscribe/{quote(trader.address, safe='')}/go"
    return web.Response(
        text=_render_subscribe_landing(
            trader=trader,
            deep_link=deep_link,
            go_link=go_link,
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
        store.set_moderation(address=address, moderation_state=moderation_state)
    logger.info("Trader moderation updated address=%s state=%s", address.lower(), moderation_state)

    raise web.HTTPFound(f"/admin?msg=Moderation+set+to+{quote(moderation_state)}")


async def delete_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    logger: logging.Logger = request.app["logger"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        store.delete(address=address)
    logger.info("Trader deleted address=%s", address.lower())

    raise web.HTTPFound("/admin?msg=Trader+deleted")


async def run_discovery(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    session = request.app["http_session"]
    logger: logging.Logger = request.app["logger"]

    with TraderStore(settings.database_dsn) as store:
        service = HyperliquidDiscoveryService(
            http_session=session,
            store=store,
            config=_discovery_config_from_settings(settings),
            logger=logger,
        )
        summary = await service.discover()
    logger.info(
        "Discovery triggered from admin: candidates=%s qualified=%s upserted=%s pruned=%s",
        summary["candidates"],
        summary["qualified"],
        summary["upserted"],
        summary.get("pruned", 0),
    )

    msg = (
        f"Discovery complete: candidates={summary['candidates']}, "
        f"qualified={summary['qualified']}, upserted={summary['upserted']}, "
        f"pruned={summary.get('pruned', 0)}"
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
    app = web.Application(middlewares=[_request_logging_middleware, _admin_auth_middleware])
    app["settings"] = resolved_settings
    app["logger"] = resolved_logger

    assets_dir = Path(__file__).resolve().parents[1] / "assets"
    if assets_dir.exists():
        app.router.add_static("/assets", str(assets_dir), show_index=False)
    else:
        resolved_logger.warning("Assets directory not found path=%s", assets_dir)

    app.add_routes(
        [
            web.get("/", subscriber_directory),
            web.get("/directory", subscriber_directory),
            web.get("/api/traders", traders_api),
            web.get("/subscribe/{address}", subscribe_landing),
            web.get("/subscribe/{address}/go", subscribe_redirect),
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
