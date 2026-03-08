from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from bot.trader_store import STATUS_ACTIVE_LISTED, TRACKED_TRADER_STATUSES, TraderStore

_ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")
_REQUIRED_PERIOD_KEYS = (
    "roi_pct",
    "realized_pnl",
    "win_rate",
    "wins",
    "losses",
    "profit_to_loss_ratio",
    "trade_count",
    "avg_pnl_per_trade",
    "max_drawdown_pct",
    "sharpe",
    "sortino",
    "roi_volatility_pct",
)


@dataclass(frozen=True)
class AuditIssue:
    severity: str
    code: str
    message: str
    address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "address": self.address,
        }


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _append(
    issues: list[AuditIssue],
    *,
    severity: str,
    code: str,
    message: str,
    address: str | None,
) -> None:
    issues.append(
        AuditIssue(
            severity=severity,
            code=code,
            message=message,
            address=address,
        )
    )


def validate_stats_payload(
    stats_json: str | None,
    *,
    address: str | None = None,
) -> list[AuditIssue]:
    issues: list[AuditIssue] = []
    if not stats_json:
        _append(
            issues,
            severity="warning",
            code="missing_stats_json",
            message="stats_json is empty",
            address=address,
        )
        return issues

    try:
        payload = json.loads(stats_json)
    except Exception as exc:
        _append(
            issues,
            severity="critical",
            code="invalid_stats_json",
            message=f"stats_json parse error: {exc}",
            address=address,
        )
        return issues

    if not isinstance(payload, dict):
        _append(
            issues,
            severity="critical",
            code="invalid_stats_json_type",
            message="stats_json must be an object",
            address=address,
        )
        return issues

    for period in ("1d", "7d", "30d"):
        key = f"metrics_{period}"
        metrics = payload.get(key)
        if not isinstance(metrics, dict):
            _append(
                issues,
                severity="critical",
                code="missing_period_metrics",
                message=f"{key} is missing or invalid",
                address=address,
            )
            continue

        for required in _REQUIRED_PERIOD_KEYS:
            if required not in metrics:
                _append(
                    issues,
                    severity="warning",
                    code="missing_metric_key",
                    message=f"{key}.{required} is missing",
                    address=address,
                )

        trade_count = _to_float(metrics.get("trade_count"))
        wins = _to_float(metrics.get("wins"))
        losses = _to_float(metrics.get("losses"))
        win_rate = _to_float(metrics.get("win_rate"))
        drawdown = _to_float(metrics.get("max_drawdown_pct"))
        volatility = _to_float(metrics.get("roi_volatility_pct"))
        pl_ratio = _to_float(metrics.get("profit_to_loss_ratio"))

        if trade_count is not None and trade_count < 0:
            _append(
                issues,
                severity="critical",
                code="negative_trade_count",
                message=f"{key}.trade_count cannot be negative",
                address=address,
            )
        if wins is not None and wins < 0:
            _append(
                issues,
                severity="critical",
                code="negative_wins",
                message=f"{key}.wins cannot be negative",
                address=address,
            )
        if losses is not None and losses < 0:
            _append(
                issues,
                severity="critical",
                code="negative_losses",
                message=f"{key}.losses cannot be negative",
                address=address,
            )
        if (
            trade_count is not None
            and wins is not None
            and losses is not None
            and (wins + losses) > (trade_count + 1e-9)
        ):
            _append(
                issues,
                severity="warning",
                code="closed_trades_exceed_trade_count",
                message=f"{key}.wins + {key}.losses exceeds {key}.trade_count",
                address=address,
            )
        if win_rate is not None and (win_rate < 0.0 or win_rate > 1.0):
            _append(
                issues,
                severity="critical",
                code="win_rate_out_of_range",
                message=f"{key}.win_rate must be within [0, 1]",
                address=address,
            )
        if drawdown is not None and drawdown < 0.0:
            _append(
                issues,
                severity="critical",
                code="negative_drawdown",
                message=f"{key}.max_drawdown_pct cannot be negative",
                address=address,
            )
        if volatility is not None and volatility < 0.0:
            _append(
                issues,
                severity="critical",
                code="negative_volatility",
                message=f"{key}.roi_volatility_pct cannot be negative",
                address=address,
            )
        if pl_ratio is not None and pl_ratio < 0.0:
            _append(
                issues,
                severity="critical",
                code="negative_profit_to_loss_ratio",
                message=f"{key}.profit_to_loss_ratio cannot be negative",
                address=address,
            )

    return issues


def audit_trader(
    trader,
    *,
    now_ms: int,
    freshness_minutes: int,
) -> list[AuditIssue]:
    issues: list[AuditIssue] = []
    address = str(trader.address).strip().lower()
    cutoff_ms = now_ms - (max(1, freshness_minutes) * 60 * 1000)

    if not _ADDRESS_RE.fullmatch(address):
        _append(
            issues,
            severity="critical",
            code="invalid_address",
            message=f"address format is invalid: {address}",
            address=address,
        )

    if trader.status not in TRACKED_TRADER_STATUSES:
        _append(
            issues,
            severity="critical",
            code="invalid_status",
            message=f"unknown status: {trader.status}",
            address=address,
        )

    for field_name in (
        "trades_24h",
        "active_hours_24h",
        "trades_7d",
        "trades_30d",
        "active_days_30d",
    ):
        value = _to_float(getattr(trader, field_name))
        if value is not None and value < 0:
            _append(
                issues,
                severity="critical",
                code="negative_counter",
                message=f"{field_name} cannot be negative",
                address=address,
            )

    for ratio_field in ("win_rate_30d", "long_ratio_30d"):
        value = _to_float(getattr(trader, ratio_field))
        if value is not None and (value < 0.0 or value > 1.0):
            _append(
                issues,
                severity="critical",
                code="ratio_out_of_range",
                message=f"{ratio_field} must be within [0, 1]",
                address=address,
            )

    for non_negative_field in (
        "age_days",
        "volume_usd_30d",
        "fees_30d",
        "avg_notional_30d",
        "max_notional_30d",
        "account_value",
        "total_ntl_pos",
        "total_margin_used",
    ):
        value = _to_float(getattr(trader, non_negative_field))
        if value is not None and value < 0:
            _append(
                issues,
                severity="critical",
                code="negative_metric",
                message=f"{non_negative_field} cannot be negative",
                address=address,
            )

    last_fill_time = int(trader.last_fill_time or 0)
    if trader.status == STATUS_ACTIVE_LISTED and last_fill_time > 0 and last_fill_time < cutoff_ms:
        _append(
            issues,
            severity="warning",
            code="stale_active_trader",
            message=(
                f"ACTIVE_LISTED trader has last activity older than {freshness_minutes} minutes"
            ),
            address=address,
        )

    issues.extend(validate_stats_payload(trader.stats_json, address=address))
    return issues


def run_data_quality_audit(
    *,
    database_dsn: str,
    max_rows: int = 5000,
    freshness_minutes: int = 60,
    issue_sample_size: int = 50,
) -> dict[str, Any]:
    with TraderStore(database_dsn) as store:
        traders = store.list_traders(limit=max(1, int(max_rows)))

    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    issues: list[AuditIssue] = []
    for trader in traders:
        issues.extend(
            audit_trader(
                trader,
                now_ms=now_ms,
                freshness_minutes=freshness_minutes,
            )
        )

    critical = [item for item in issues if item.severity == "critical"]
    warnings = [item for item in issues if item.severity == "warning"]
    active_listed = [item for item in traders if item.status == STATUS_ACTIVE_LISTED]
    fresh_cutoff_ms = now_ms - (max(1, freshness_minutes) * 60 * 1000)
    fresh_active = [
        item
        for item in active_listed
        if int(item.last_fill_time or 0) >= fresh_cutoff_ms
    ]

    sample = [item.to_dict() for item in issues[: max(0, issue_sample_size)]]
    return {
        "checked_traders": len(traders),
        "active_listed_traders": len(active_listed),
        "fresh_active_listed_traders": len(fresh_active),
        "critical_issues": len(critical),
        "warning_issues": len(warnings),
        "issues_sample": sample,
        "passed": len(critical) == 0,
    }

