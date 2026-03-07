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
- One-click subscription flow to trader-specific Telegram delivery
- Open contribution model for researchers, backend engineers, and traders

## What it does

- Discovers active futures traders from public data sources
- Computes 1d/7d/30d performance and risk metrics
- Builds ranked trader universe and monitoring tiers (HOT/WARM/COLD)
- Serves a public searchable catalog page + API
- Delivers trader fills to Telegram sessions for active subscribers

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
- Tiered monitoring to reduce load:
  - HOT (frequent)
  - WARM (medium)
  - COLD (slow)
- Demand-only delivery mode:
  - scans and posts signals only for traders with active subscriptions

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
                    [Poster Worker] ---> Telegram topics/sessions for active subscribers
                                                          ^
                                                          |
                                  [Subscriber Bot + Web UI + /api/traders]
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
TELEGRAM_BOT_USERNAME=your_bot_username
DATABASE_URL=postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=strong_password
```

Run services:

```bash
source .venv/bin/activate
python admin.py --host 127.0.0.1 --port 8080
python discovery_worker.py
python universe_worker.py
python top100_worker.py
python main.py
python subscriber_bot.py
```

## Public and Admin URLs

- `/` -> public trader catalog
- `/api/traders` -> filterable/sortable JSON API
- `/subscribe/<trader_address>` -> one-click subscription landing page
- `/admin` -> password-protected admin panel

## Data Model

Core tables:

- `tracked_traders`: raw discovered traders + metrics + moderation state
- `traders_universe`: filtered qualified pool
- `traders_top100_live`: rolling active shortlist
- `trader_monitoring_pool`: HOT/WARM/COLD due-scan scheduling
- `catalog_current`: denormalized public catalog view
- `subscriptions`, `delivery_sessions`: Telegram delivery lifecycle
- `discovery_runs`: discovery run observability

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

## SEO Keywords

copytrading, free crypto signals, Telegram copy trading bot, futures trader tracker, open-source copytrading, Hyperliquid trader discovery

## Support the project

This project is maintained on a self-hosted server and funded by community donations.

- PayPal: `sanyo4ever@gmail.com`
- USDT (TRC20): `TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ`

If this repository helps you, please star it and share it.
