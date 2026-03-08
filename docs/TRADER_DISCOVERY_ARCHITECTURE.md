# Trader Discovery and Catalog Architecture

Last updated: 2026-03-08

## 1. Objectives

- Continuously discover active futures traders.
- Compute comparable quality/risk metrics (1d/7d/30d windows).
- Keep all discovered traders in history while controlling catalog visibility via lifecycle statuses.
- Serve fast search/filter/sort for the public catalog and API.

## 2. Current Data Sources

Implemented now:

- Hyperliquid:
  - candidate discovery (`vaultSummaries`, `recentTrades`, `userFillsByTime`)
  - account/margin context (`clearinghouseState`)
- Optional Nansen Smart Money candidates (when API key configured)

Design supports additional sources later, but current production ranking is centered on Hyperliquid metrics.

## 3. Discovery Pipeline (Current)

`cryptoinsider-discovery` loop:

1. Build candidate list (dedup + source merge + optional seed addresses).
2. Fetch per-trader fills and account context.
3. Compute period metrics for 1d/7d/30d.
4. Compute composite quality score.
5. Apply hard filters.
6. Upsert into `tracked_traders`.
7. Soft-prune by source (unlist stale auto-discovered identities, keep history).
8. Write `discovery_runs` audit row.

## 4. Hard Filters

Default thresholds:

- `age_days >= 30`
- `trades_30d >= 120`
- `active_days_30d >= 12`
- `trades_7d >= 1`
- `last_fill_time <= 60 minutes`
- `win_rate_30d >= 0.52`
- `max_drawdown_30d <= 25%` (if drawdown is available)
- `realized_pnl_30d > 0` (or configured threshold)

## 5. Metrics Model

Calculated per period (1d/7d/30d):

- ROI
- Realized PnL
- Win rate
- Wins/Losses
- Profit-to-loss ratio
- Trade count
- Avg PnL per trade
- Max drawdown
- Sharpe
- Sortino
- ROI volatility

Important implementation detail:

- metrics are computed on aggregated order-level trades (not raw partial fills), which reduces activity inflation.

Additional trader-level fields:

- activity counters (`trades_24h`, `trades_7d`, `trades_30d`, `active_days_30d`)
- volume and fee metrics
- long/short ratio proxies
- account summary fields
- score + score component payload in `stats_json`

## 6. Quality Score (Current)

Composite score uses weighted signal + multiplicative risk penalty:

- positive components: ROI, Sharpe, Sortino, win rate, activity
- risk penalties: drawdown, volatility, fee pressure (can reduce score to near-zero under extreme risk)

Result is normalized to `0..100` and stored in `tracked_traders.score`.

## 7. Lifecycle and Visibility

Statuses are soft-state, not deletion:

- `ACTIVE_LISTED`: visible and eligible for shared-channel delivery.
- `ACTIVE_UNLISTED`: hidden by default, retained for continuity/history.
- `STALE`: hidden from actionable pool due to inactivity.
- `ARCHIVED`: long-inactive historical state.

Critical rule: discovered traders are not hard-deleted; transitions preserve continuity and history.

## 8. Catalog Projection

`cryptoinsider-top100` refreshes:

- `traders_top100_live` (activity shortlist)
- `trader_monitoring_pool` (HOT/WARM/COLD over all non-archived tracked traders)
- `catalog_current` (public serving projection)

Public serving path:

- `/` renders from `catalog_current`
- `/api/traders` provides keyset pagination, filter allowlist, and sortable columns

## 9. Database Entities (Current Core)

- `tracked_traders`
- `traders_universe`
- `traders_top100_live`
- `trader_monitoring_pool`
- `catalog_current`
- `discovery_runs`

## 10. Testing and QA

Automated checks for discovery/metrics quality include:

- metric schema/range validation (`tests/test_data_quality.py`)
- deterministic metric fixtures (`tests/test_metrics_engine.py`)
- store lifecycle and projection tests (`tests/test_trader_store.py`)
- QA gate runner (`scripts/qa_certification.py`)

Use commands:

```bash
python scripts/qa_certification.py --skip-db-audit
python scripts/qa_certification.py --skip-tests --database "$DATABASE_URL"
```

## 11. Extension Path

Near-term extension priorities:

1. Add pluggable source connectors with source-level confidence weights.
2. Add benchmark/backtest datasets for score calibration.
3. Add anti-manipulation heuristics (sample-size and anomaly penalties).
4. Expose transparent score component explanations in API/UI.
