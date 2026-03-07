from __future__ import annotations

import argparse
import base64
import hmac
import json
import re
from datetime import UTC, datetime
from html import escape
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
from aiohttp import web

from bot.config import load_settings
from bot.discovery import HyperliquidDiscoveryConfig, HyperliquidDiscoveryService
from bot.trader_store import (
    CatalogTrader,
    MODERATION_BLACKLIST,
    MODERATION_NEUTRAL,
    MODERATION_WHITELIST,
    STATUS_ACTIVE,
    STATUS_PAUSED,
    TraderStore,
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


def _subscribe_button(*, trader_address: str, bot_username: str) -> str:
    if not bot_username:
        return "<span style='opacity:.7'>Bot not configured</span>"
    encoded = quote(trader_address, safe="")
    return (
        f"<a class='btn-link' href='/subscribe/{encoded}' target='_blank' rel='noopener'>"
        "Subscribe (24h Free)"
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


@web.middleware
async def _admin_auth_middleware(request: web.Request, handler):
    if request.path.startswith("/admin"):
        if not _is_admin_authorized(request):
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
        minutes_since = _minutes_since(trader.last_fill_time)
        freshness = "-" if minutes_since is None else f"{minutes_since}m ago"
        rows.append(
            "<tr>"
            f"<td>{_fmt(index, 0)}</td>"
            f"<td><code>{escape(trader.address)}</code></td>"
            f"<td>{escape(trader.label or '-')}</td>"
            f"<td>{escape(trader.status)}</td>"
            f"<td>{escape(trader.moderation_state)}</td>"
            f"<td>{escape(freshness)}</td>"
            f"<td>{_fmt(trader.trades_30d, 0)}</td>"
            f"<td>{_fmt(trader.active_days_30d, 0)}</td>"
            f"<td>{_fmt((trader.win_rate_30d or 0.0) * 100.0, 1)}%</td>"
            f"<td>{_fmt(trader.realized_pnl_30d, 2)}</td>"
            f"<td>{_fmt(trader.score, 2)}</td>"
            f"<td>{_fmt(trader.activity_score, 2)}</td>"
            f"<td>{_subscribe_button(trader_address=trader.address, bot_username=bot_username)}</td>"
            "</tr>"
        )

    table_rows = (
        "\n".join(rows)
        if rows
        else "<tr><td colspan='13'>No traders match your filters.</td></tr>"
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

    return f"""
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <meta http-equiv='refresh' content='30' />
  <title>Trader Directory</title>
  <style>
    :root {{ --bg:#08111f; --panel:#12233d; --line:#2b3e5e; --text:#e5eefc; --muted:#9ab0d3; --accent:#6dd3ff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--text); font-family:"Space Grotesk","Segoe UI",sans-serif; background:radial-gradient(circle at top left,#1a2f52 0%,var(--bg) 60%); }}
    .wrap {{ max-width:1400px; margin:20px auto; padding:0 18px; }}
    .card {{ background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.01)); border:1px solid var(--line); border-radius:14px; padding:14px; margin-bottom:14px; }}
    h1 {{ margin:0 0 8px; }}
    p {{ margin:0; color:var(--muted); }}
    .quick-info {{ margin-top:10px; color:var(--muted); font-size:13px; line-height:1.5; }}
    form {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    input,select {{ background:#0e1a30; border:1px solid var(--line); color:var(--text); border-radius:8px; padding:7px 9px; }}
    button {{ background:#20477a; border:1px solid #2f6ab5; color:var(--text); border-radius:8px; padding:7px 10px; cursor:pointer; }}
    .btn-link {{ display:inline-block; background:#1d3f6d; border:1px solid #2f6ab5; color:var(--text); border-radius:8px; padding:5px 9px; text-decoration:none; font-size:12px; }}
    .btn-link:hover {{ background:#24538f; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ padding:9px 7px; border-bottom:1px solid var(--line); text-align:left; }}
    th {{ color:var(--muted); }}
    code {{ color:var(--accent); }}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='card'>
      <h1>Futures Traders Catalog</h1>
      <p>Browse all traders stored in database and open a personal Telegram chat in one click.</p>
      <div class='quick-info'>
        <strong>How it works:</strong> Discovery workers collect and score traders continuously.<br/>
        Catalog refresh: <strong>{escape(refreshed_at)}</strong>.<br/>
        Click <strong>Open Trader Chat</strong> to receive new fills from that trader in Telegram.<br/>
        Informational only. Not financial advice.
      </div>
    </div>

    <div class='card'>
      <form method='get' action='/'>
        <input name='q' placeholder='search label/address' value='{escape(str(request.query.get("q", "")))}' />
        <select name='status'>
          <option value='ALL'>ALL</option>
          <option value='ACTIVE'>ACTIVE</option>
          <option value='PAUSED'>PAUSED</option>
        </select>
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
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Address</th>
            <th>Label</th>
            <th>Status</th>
            <th>Moderation</th>
            <th>Last Fill</th>
            <th>Trades 30d</th>
            <th>Active Days 30d</th>
            <th>Win Rate 30d</th>
            <th>Realized PnL 30d</th>
            <th>Score</th>
            <th>Activity</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
      {pager}
    </div>
  </div>
</body>
</html>
"""


def _render_admin_index(*, traders, discovery_runs, message: str | None = None) -> str:
    active_count = sum(1 for trader in traders if trader.status == STATUS_ACTIVE)
    paused_count = sum(1 for trader in traders if trader.status == STATUS_PAUSED)
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
        action_button = (
            f"<form method='post' action='/admin/traders/{encoded}/pause' style='display:inline'>"
            "<button class='btn pause' type='submit'>Pause</button></form>"
            if trader.status == STATUS_ACTIVE
            else f"<form method='post' action='/admin/traders/{encoded}/resume' style='display:inline'>"
            "<button class='btn resume' type='submit'>Resume</button></form>"
        )
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
            f"{action_button} "
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
    .pause {{ border-color:#c3871f; }}
    .resume {{ border-color:var(--ok); }}
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
        <div class='stat'><span>Paused</span><strong>{paused_count}</strong></div>
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
          <option value='pause'>Pause selected</option>
          <option value='resume'>Resume selected</option>
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
    lifetime_hours: int,
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
      <div class='box'><strong>Subscription Duration</strong><span class='ok'>{int(lifetime_hours)}h</span> from activation</div>
      <div class='box'><strong>Payment</strong><span class='ok'>Free now (no payment)</span></div>
    </div>
    <div class='cta'>
      <a class='btn' href='{escape(go_link)}'>Create Chat in Telegram</a>
      <a class='btn' href='{escape(deep_link)}' target='_blank' rel='noopener'>Open Bot Directly</a>
      <span class='note'>Auto-open in <span id='count'>5</span>s...</span>
    </div>
    <p class='note'>After you press <code>/start</code> in Telegram, bot creates a dedicated thread and posts trades until expiry.</p>
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
            lifetime_hours=settings.subscription_lifetime_hours,
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
    payload = await request.post()

    address = str(payload.get("address", "")).strip()
    label = str(payload.get("label", "")).strip() or None
    if not address:
        raise web.HTTPFound("/admin?msg=Address+is+required")

    with TraderStore(settings.database_dsn) as store:
        store.add_manual(address=address, label=label)

    raise web.HTTPFound("/admin?msg=Trader+added")


async def bulk_trader_action(request: web.Request) -> web.Response:
    settings = request.app["settings"]
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
        if action == "pause":
            changed = store.set_status_bulk(addresses=unique_addresses, status=STATUS_PAUSED)
            raise web.HTTPFound(f"/admin?msg=Bulk+pause+applied:+{changed}")
        if action == "resume":
            changed = store.set_status_bulk(addresses=unique_addresses, status=STATUS_ACTIVE)
            raise web.HTTPFound(f"/admin?msg=Bulk+resume+applied:+{changed}")
        moderation_state = _parse_moderation_state(action)
        if moderation_state is None:
            raise web.HTTPFound("/admin?msg=Unknown+bulk+action")
        changed = store.set_moderation_bulk(
            addresses=unique_addresses,
            moderation_state=moderation_state,
            note=note,
        )
        raise web.HTTPFound(f"/admin?msg=Bulk+moderation+applied:+{changed}")


async def set_pause_state(request: web.Request, status: str) -> web.Response:
    settings = request.app["settings"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        store.set_status(address=address, status=status)

    action = "paused" if status == STATUS_PAUSED else "resumed"
    raise web.HTTPFound(f"/admin?msg=Trader+{action}")


async def pause_trader(request: web.Request) -> web.Response:
    return await set_pause_state(request, STATUS_PAUSED)


async def resume_trader(request: web.Request) -> web.Response:
    return await set_pause_state(request, STATUS_ACTIVE)


async def moderate_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    address = request.match_info.get("address", "")
    state_slug = request.match_info.get("state", "")
    moderation_state = _parse_moderation_state(state_slug)
    if moderation_state is None:
        raise web.HTTPFound("/admin?msg=Unsupported+moderation+state")

    with TraderStore(settings.database_dsn) as store:
        store.set_moderation(address=address, moderation_state=moderation_state)

    raise web.HTTPFound(f"/admin?msg=Moderation+set+to+{quote(moderation_state)}")


async def delete_trader(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    address = request.match_info.get("address", "")

    with TraderStore(settings.database_dsn) as store:
        store.delete(address=address)

    raise web.HTTPFound("/admin?msg=Trader+deleted")


async def run_discovery(request: web.Request) -> web.Response:
    settings = request.app["settings"]
    session = request.app["http_session"]

    with TraderStore(settings.database_dsn) as store:
        service = HyperliquidDiscoveryService(
            http_session=session,
            store=store,
            config=_discovery_config_from_settings(settings),
        )
        summary = await service.discover()

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


async def _on_cleanup(app: web.Application) -> None:
    session = app.get("http_session")
    if session is not None:
        await session.close()


def create_app() -> web.Application:
    app = web.Application(middlewares=[_admin_auth_middleware])
    app["settings"] = load_settings(require_telegram=False, require_admin_password=True)

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
            web.post("/admin/traders/{address}/pause", pause_trader),
            web.post("/admin/traders/{address}/resume", resume_trader),
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
    web.run_app(create_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    run()
