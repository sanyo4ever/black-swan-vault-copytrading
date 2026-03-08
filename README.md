# Black Swan Vault Copytrading

[![CI](https://github.com/sanyo4ever/black-swan-vault-copytrading/actions/workflows/ci.yml/badge.svg)](https://github.com/sanyo4ever/black-swan-vault-copytrading/actions/workflows/ci.yml)
[![License: AGPL-3.0-only](https://img.shields.io/badge/License-AGPL--3.0--only-blue.svg)](./LICENSE)
[![Stars](https://img.shields.io/github/stars/sanyo4ever/black-swan-vault-copytrading?style=social)](https://github.com/sanyo4ever/black-swan-vault-copytrading/stargazers)

Free and open-source copytrading infrastructure for crypto communities.

Goal: make trader discovery and signal delivery available to everyone with one click.

- Public server: [blackswanvault.online](https://blackswanvault.online)
- Donations: PayPal `sanyo4ever@gmail.com`
- Donations: USDT (TRC20) `TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ`
- Source code: [github.com/sanyo4ever/black-swan-vault-copytrading](https://github.com/sanyo4ever/black-swan-vault-copytrading)

![Black Swan Vault logo](./assets/blackswanvault-logo.svg)

## Why this project

Most copytrading tools are closed, expensive, or difficult to self-host. Black Swan Vault focuses on:

- Free access for retail users and small communities
- Transparent ranking metrics and data pipeline
- Self-hosted deployment with production-ready services
- One-click Join Channel flow to trader-specific Telegram forum topics
- Open contribution model for researchers, backend engineers, and traders

## What it does

- Discovers active futures traders from public data sources
- Computes 1d/7d/30d performance and risk metrics
- Builds ranked trader universe and monitoring tiers (HOT/WARM/COLD)
- Serves a public searchable catalog page + API
- Delivers trader fills into dedicated Telegram forum topics per tracked wallet

## Current Product Rules

- Public UI uses `Join Channel`; no DM subscription flow is required for delivery.
- Poster auto-creates and reuses one forum topic per trader wallet in the configured forum chat.
- If a topic is removed manually, mapping is dropped and recreated automatically on the next signal.
- The project is donation-supported in the current production mode (no mandatory paywall).

## Feature Highlights

- Multi-source candidate ingest: Hyperliquid + optional Nansen Smart Money
- Hard quality gates:
  - age >= 30 days
  - trades_30d >= 120
  - active_days_30d >= 12
  - recent activity <= 60 minutes
  - win_rate_30d >= 52%
  - max_drawdown_30d <= 25%
  - realized_pnl_30d > 0
- Composite quality score:
  - ROI + Sharpe + Sortino + Win Rate + Activity
  - penalties for drawdown, volatility, and fees
  - ROI baseline is capital-flow adjusted (ledger deposits/withdrawals/transfers)
- Tiered monitoring to reduce load:
  - HOT (frequent)
  - WARM (medium)
  - COLD (slow)
- Shared-forum delivery mode:
  - scans tracked trader pools and posts signals into per-trader forum topics
  - adaptive polling scheduler keeps API load bounded while prioritizing active traders

## Shared Forum Delivery Architecture

The signal pipeline is optimized for a shared Telegram forum chat:

- each signal trader resolves to a `trader_forum_topics` mapping (`trader -> chat_id + thread_id`)
- missing mappings trigger `createForumTopic` and are persisted for reuse
- `topic_missing` errors purge stale mappings and pending retries for that topic
- each trader gets adaptive `poll_interval_seconds` based on:
  - activity score + quality score
  - recency of latest known activity
  - scheduler pressure limits
- poster scans only due targets (`next_poll_at <= now`) with strict cycle limits
- each trader keeps a watermark (`last_seen_fill_time`) to fetch incrementally and avoid heavy back-scans
- on errors, poll interval backs off automatically; on new fills, polling tightens

This gives low latency for active traders while preventing uncontrolled CPU/API load growth.

## Architecture (high level)

```text
[Discovery Worker] ---> tracked_traders ---> [Universe Worker] ---> traders_universe
                                                          |
                                                          v
                                                    [Top100 Worker]
                                                          |
                                                          v
                                 trader_monitoring_pool (HOT/WARM/COLD)
                                                          |
                                                          v
                    [Poster Worker] ---> Telegram forum topics (one topic per trader)
                                                          ^
                                                          |
                                  [Web UI + /api/traders + Join Channel CTA]
```

## Quick Start (local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set required environment variables in `.env`:

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHANNEL_ID=-100...
TELEGRAM_FORUM_CHAT_ID=-100...
TELEGRAM_JOIN_URL=https://t.me/YourChannelOrInvite
DATABASE_URL=postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=strong_password
GOOGLE_ANALYTICS_MEASUREMENT_ID=G-XXXXXXXXXX
```

Run services:

```bash
source .venv/bin/activate
python admin.py --host 127.0.0.1 --port 8080
python discovery_worker.py
python universe_worker.py
python top100_worker.py
python main.py
# optional DM helper bot (opens shared forum topic links):
# python subscriber_bot.py
```

Delivery-monitor tuning (optional `.env`):

```env
DELIVERY_MONITOR_BASE_POLL_SECONDS=60
DELIVERY_MONITOR_MIN_POLL_SECONDS=20
DELIVERY_MONITOR_MAX_POLL_SECONDS=180
DELIVERY_MONITOR_PRIORITY_RECENCY_MINUTES=120
DELIVERY_MONITOR_MAX_TRADERS_PER_CYCLE=120
DELIVERY_MONITOR_HTTP_CONCURRENCY=8
DELIVERY_SEND_CONCURRENCY=12
DELIVERY_CHAT_MIN_INTERVAL_MS=250
TRADER_LISTED_WITHIN_MINUTES=60
TRADER_STALE_AFTER_MINUTES=4320
TRADER_ARCHIVE_AFTER_DAYS=180
```

## Public and Admin URLs

- `/` -> public trader catalog
- `/api/traders` -> filterable/sortable JSON API
- `/subscribe/<trader_address>/go` -> tracks click and redirects to channel join link
- `/admin` -> password-protected admin panel

## Documentation Map

- [`docs/README.md`](./docs/README.md) -> complete documentation index
- [`docs/TRADER_DISCOVERY_ARCHITECTURE.md`](./docs/TRADER_DISCOVERY_ARCHITECTURE.md) -> discovery/scoring/catalog internals
- [`docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md`](./docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md) -> shared forum delivery internals
- [`docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`](./docs/PRODUCTION_ARCHITECTURE_UBUNTU.md) -> production runtime and operations

## Data Model

Core tables:

- `tracked_traders`: raw discovered traders + metrics + moderation state
- `traders_universe`: filtered qualified pool
- `traders_top100_live`: rolling active shortlist
- `trader_monitoring_pool`: HOT/WARM/COLD due-scan scheduling
- `delivery_monitor_state`: adaptive scheduler + per-trader watermark state
- `catalog_current`: denormalized public catalog view
- `trader_forum_topics`: shared forum topic mapping (`trader_address -> message_thread_id`)
- `subscriptions`, `delivery_sessions`: legacy DM-subscription lifecycle (kept for compatibility/migration)
- `discovery_runs`: discovery run observability

### Trader Lifecycle Consistency

Tracked traders are never hard-deleted in normal operation.

- `ACTIVE_LISTED`: visible in public catalog.
- `ACTIVE_UNLISTED`: hidden from default catalog, kept for continuity/history.
- `STALE`: inactive beyond stale threshold, hidden from default actionable pool.
- `ARCHIVED`: long-inactive historical record.

Lifecycle transitions are automatic (`universe_worker`, coordinated with discovery via DB lease) and delivery-safe:

- historically active records -> never hard-deleted
- blacklisted trader -> delivery detached by moderation rules
- discovery "prune" -> soft unlist, not delete

## QA Certification Gate

Run documentation review before every push:

```bash
python scripts/doc_review.py
```

Run the full QA gate locally (unit/integration/e2e tests):

```bash
python scripts/qa_certification.py --skip-db-audit
```

Run with live DB quality audit and JSON report:

```bash
python scripts/qa_certification.py \
  --database "$DATABASE_URL" \
  --freshness-minutes 60 \
  --min-active-listed 1 \
  --json-out data/qa-report.json
```

What the gate checks:

- Data quality invariants (schema/ranges/freshness)
- Metrics integrity (ROI/Win Rate/Drawdown/Sharpe/Sortino/volatility)
- Business e2e flows (join redirect/fanout/retry/Telegram edge cases)

## Test Data Hygiene

Never leave synthetic traders in shared databases after manual/E2E testing.

```bash
# dry run
python scripts/cleanup_test_traders.py --postgres-url "$DATABASE_URL"

# apply deletion (explicit confirm)
python scripts/cleanup_test_traders.py --postgres-url "$DATABASE_URL" --apply --yes
```

## Open Source, License, and Commercial Use

This project uses a dual licensing policy:

- Community License: AGPL-3.0-only (`LICENSE`)
- Commercial License: available for closed-source/enterprise terms

Details: [`LICENSE_POLICY.md`](./LICENSE_POLICY.md)

## Contributing

Contributions are welcome and actively encouraged.

Start here:

- [`CONTRIBUTING.md`](./CONTRIBUTING.md)
- [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
- [`SECURITY.md`](./SECURITY.md)
- [`ROADMAP.md`](./ROADMAP.md)
- [`GOVERNANCE.md`](./GOVERNANCE.md)
- [`SUPPORT.md`](./SUPPORT.md)

If you are looking for a first task, open issues labeled `good first issue` or `help wanted`.

## Deployment

Production deployment docs:

- [`deploy/README.md`](./deploy/README.md)
- [`docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`](./docs/PRODUCTION_ARCHITECTURE_UBUNTU.md)
- [`docs/TRADER_DISCOVERY_ARCHITECTURE.md`](./docs/TRADER_DISCOVERY_ARCHITECTURE.md)
- [`docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md`](./docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md)

## SEO Keywords

copytrading, free crypto signals, Telegram copy trading bot, futures trader tracker, open-source copytrading, Hyperliquid trader discovery

## Support the project

This project is maintained on a self-hosted server and funded by community donations.

- PayPal: `sanyo4ever@gmail.com`
- USDT (TRC20): `TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ`

If this repository helps you, please star it and share it.
