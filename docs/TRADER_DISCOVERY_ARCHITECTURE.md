# Trader Discovery & Catalog Architecture

Last updated: 2026-03-07

## 1. Goal

Build a production-ready discovery + storage + catalog system that:

1. Continuously finds active futures traders from reliable sources.
2. Stores raw and derived metrics with auditability.
3. Keeps only useful traders in active catalog while preserving history.
4. Exposes fast filtering/sorting/search over **all** traders in DB (not top-100 only).

---

## 2. Source Strategy (what to ingest and why)

### 2.1 Tier A: Trader-address level sources (best for copy-signal product)

These sources expose identities (address / subaccount) + per-trader activity.

1. Hyperliquid (official API + WS + optional S3 backfill)
2. dYdX v4 Indexer API (accounts, fills, positions, pnl, trades)
3. Bitget CopyTrade API (trader list/detail/history; mostly authenticated access)

### 2.2 Tier B: Market-regime sources (not trader-level, but useful ranking context)

These sources provide crowd/top-trader aggregates, not individual trader identities.

1. Binance Futures public market-data endpoints:
   - top trader long/short position ratio
   - top trader long/short account ratio
   - global long/short ratio
   - taker buy/sell volume

Use Tier B metrics as market regime features (confidence, risk mode), not as trader discovery identities.

### 2.3 Tier C: Exchange-native copy APIs with limited public discoverability

Bybit/OKX copy trading APIs are useful for execution/copy contexts, but public leader discovery depth is typically lower or gated compared to onchain-style datasets.

---

## 3. Data We Can Collect (metrics model)

### 3.1 Identity and lifecycle

- `source` (`hyperliquid`, `dydx`, `bitget`)
- `venue_account_id` (address/subaccount/trader_id)
- first seen / last seen / last trade timestamps
- active status windows: 1h, 24h, 7d, 30d
- moderation state: `NEUTRAL | WHITELIST | BLACKLIST`

### 3.2 Trading activity

- trade count: 1h / 24h / 7d / 30d
- active intervals:
  - active minutes (1h)
  - active hours (24h)
  - active days (30d)
- symbol breadth (distinct traded symbols)
- long/short split

### 3.3 Performance

- realized pnl (1d/7d/30d/90d)
- win rate (trade-level and day-level)
- average pnl per trade
- profit factor (gross profit / gross loss)
- streak metrics (wins/losses)

### 3.4 Risk and quality

- max drawdown (rolling)
- pnl volatility (std-dev)
- leverage proxy / notional utilization
- avg/max notional per trade
- fee-to-volume ratio
- liquidation-related events (if source provides)

### 3.5 Confidence and reliability

- `data_quality_score` by source freshness, gaps, and API reliability
- sample-size confidence (avoid over-ranking tiny histories)
- stale flags when source data is delayed

---

## 4. Service Architecture

## 4.1 Worker topology

1. `candidate_discovery_worker`
   - scans source-specific candidate pools
   - emits candidate identities to queue
2. `enrichment_worker`
   - fetches detailed per-trader metrics for candidates
   - writes snapshots + updates current state
3. `score_worker`
   - computes ranking/risk scores from normalized metrics
4. `catalog_projection_worker`
   - builds denormalized searchable table/view for UI/API
5. `retention_worker`
   - moves inactive traders to dormant/archive states
6. `delivery_worker` (already exists)
   - consumes selected tracked traders for Telegram posting

### 4.2 Reliability primitives

- Retry with exponential backoff + jitter on each source connector
- Idempotent upserts by `(source, venue_account_id)`
- Source cursor/checkpoint tables for resumable ingestion
- Dead-letter records for repeated source failures
- SLO metrics: ingestion lag, stale ratio, API error rate

### 4.3 Queue choice

Short term: PostgreSQL job table (simple, fewer dependencies)
Medium term: Redis Streams (better throughput, consumer groups)

---

## 5. Database Architecture (PostgreSQL)

Keep current `tracked_traders` for compatibility, but move to normalized core.

### 5.1 Core entities

1. `sources`
   - id, name, type, enabled, rate_limit_profile
2. `traders`
   - internal canonical entity (`trader_id`)
   - moderation + lifecycle status
3. `trader_accounts`
   - `(source_id, venue_account_id)` unique
   - maps source identities to canonical `trader_id`
4. `trader_metrics_snapshots`
   - current windowed metrics (1h/24h/7d/30d/90d)
5. `trader_metrics_timeseries`
   - daily/hourly historical aggregates for charting/backtests
6. `trader_events`
   - normalized fills/trade events (append-only, partitioned)
7. `trader_scores`
   - derived score components + final score + confidence
8. `catalog_current`
   - denormalized serving table for fast UI listing
9. `ingestion_runs`, `ingestion_errors`, `source_cursors`
   - operational observability

### 5.2 Suggested indexes for catalog scale

On `catalog_current`:

- `btree (is_visible, moderation_state, last_trade_at DESC, trader_id)`
- `btree (is_visible, score DESC, trader_id)`
- `btree (is_visible, trades_30d DESC, trader_id)`
- `btree (is_visible, realized_pnl_30d DESC, trader_id)`
- `btree (is_visible, win_rate_30d DESC, trader_id)`
- partial index for active set:
  - `WHERE is_visible = true AND moderation_state <> 'BLACKLIST'`
- search:
  - `GIN (search_text gin_trgm_ops)` with `pg_trgm`
  - optional `GIN (search_tsv)` for full text

On event/history tables:

- partition by month (`event_time`)
- `BRIN(event_time)` for long-range scans

---

## 6. Filtering / Sorting / Search (All Traders Page)

### 6.1 API contract

`GET /api/traders`

Query params:

- pagination: `limit`, `cursor`
- search: `q` (address, label, source account id)
- filters:
  - `source[]`
  - `status` (`ACTIVE_LISTED`, `ACTIVE_UNLISTED`, `STALE`, `ARCHIVED`)
  - `moderation_state`
  - `active_within_minutes`
  - ranges: `trades_30d_min/max`, `win_rate_30d_min/max`,
    `pnl_30d_min/max`, `score_min/max`, `age_days_min/max`
- sorting (allowlist only):
  - `last_trade_at_desc`
  - `score_desc`
  - `activity_score_desc`
  - `realized_pnl_30d_desc`
  - `win_rate_30d_desc`
  - `trades_30d_desc`

### 6.2 Pagination strategy

Use **keyset pagination** (cursor-based), not offset, to keep response times stable on large datasets.

### 6.3 Query execution model

1. Apply mandatory visibility predicates first (`is_visible`, blacklist rules).
2. Apply structured numeric filters.
3. Apply text search (`q`) via trigram/tsvector.
4. Apply sort allowlist.
5. Return `next_cursor`.

---

## 7. Keeping "needed" traders in DB

Use lifecycle statuses instead of hard deletion:

1. `ACTIVE_LISTED`: visible in public catalog, open for new subscriptions
2. `ACTIVE_UNLISTED`: hidden from default catalog, retained for continuity
3. `STALE`: inactive beyond stale threshold, blocked for new subscriptions
4. `ARCHIVED`: long-inactive historical record

Retention policy:

- Raw events older than N days -> compressed/archive storage
- Daily snapshots kept long-term for ranking history
- Never hard-delete moderated/blacklisted records without audit trail
- Discovery prune should soft-unlist (status change), not remove rows

---

## 8. Ranking model (v1)

`final_score = activity * 0.30 + performance * 0.30 + risk_adjusted * 0.25 + reliability * 0.15`

Where:

- `activity`: recency + frequency + consistency
- `performance`: pnl, win-rate, profit-factor
- `risk_adjusted`: drawdown and pnl volatility penalties
- `reliability`: sample size + source data quality

Keep score components in separate columns for transparent UI filters.

---

## 9. Current Project Gap vs Target

Current:

- Single-source-heavy discovery path.
- Top100 projection table optimized for shortlist.
- Limited historical metric structure.

Target:

- Multi-source discovery with unified identity graph.
- Full catalog serving table over all qualified traders.
- Historical snapshots + robust scoring pipeline.
- Search/filter/sort designed for >100k rows.

---

## 10. Implementation Roadmap

### Phase 1 (fast, high impact)

1. Add `catalog_current` and serve `/` from it (all traders, cursor pagination).
2. Add API endpoint `/api/traders` with filter/sort allowlist.
3. Add trigram + composite indexes.

### Phase 2

1. Introduce `trader_accounts`, `trader_metrics_snapshots`, `trader_scores`.
2. Split discovery -> enrichment -> score workers.
3. Add retention + lifecycle states.

### Phase 3

1. Add dYdX and Bitget connectors.
2. Add regime features (Binance aggregate sentiment endpoints).
3. Add monitoring dashboards and alerting.

### Phase 4

1. Historical backfill pipeline.
2. Advanced risk metrics and anti-gaming checks.
3. User-facing custom watchlists + per-trader alert subscriptions.

---

## 11. Verified References (official docs)

- Hyperliquid info endpoint:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Hyperliquid WS subscriptions:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Hyperliquid API rate limits:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- Hyperliquid historical data:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
- dYdX indexer HTTP API:
  - https://docs.dydx.xyz/indexer-client/http
- Binance Futures market-data REST:
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Long-Short-Account-Ratio
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Long-Short-Ratio
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume
- Bybit v5 copy trading + instruments:
  - https://bybit-exchange.github.io/docs/v5/copytrade
  - https://bybit-exchange.github.io/docs/v5/market/instrument
- Bitget copy trade docs:
  - https://bitgetlimited.github.io/apidoc/en/copyTrade/
