# QA Test Strategy

This project uses a release gate that combines automated tests with live data-quality checks.

## Scope

1. Data quality:
- field validity (`address`, `status`, counters, ratios)
- stats payload schema (`metrics_1d`, `metrics_7d`, `metrics_30d`)
- metric ranges (win rate, drawdown, volatility, P/L ratio)
- freshness checks for `ACTIVE_LISTED` traders

2. Metrics correctness:
- deterministic fixtures for ROI, PnL, win/loss, drawdown
- Sharpe/Sortino/volatility sanity checks
- end-to-end `_fetch_metrics` payload integrity checks

3. Business E2E:
- join redirect flow (`/subscribe/<address>/go` -> channel link)
- forum topic mapping lifecycle (create/reuse/reset on `topic_missing`)
- fanout and retry queue lifecycle on transient Telegram failures
- edge-case handling (`message thread not found`, bot blocked)
- legacy DM subscription compatibility tests remain in CI

## Gate Command

```bash
python scripts/qa_certification.py --skip-db-audit
```

For production checks:

```bash
python scripts/qa_certification.py --database "$DATABASE_URL" --json-out data/qa-report.json
```

## Pass Criteria

- all tests pass
- zero critical data-quality issues
- optional threshold checks (for example minimum active listed traders)
