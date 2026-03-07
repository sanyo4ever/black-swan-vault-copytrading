# CryptoInsider Futures Discovery

Service stack for discovering futures traders, storing rich metrics, and publishing selected traders to Telegram.

Open-source repository: [github.com/sanyo4ever/black-swan-vault-copytrading](https://github.com/sanyo4ever/black-swan-vault-copytrading)  
I run a self-hosted public server at [blackswanvault.online](https://blackswanvault.online), where anyone can connect to copytrading feeds.  
Project is donation-supported. PayPal: `sanyo4ever@gmail.com` or USDT TRC20: `TBFmAiNBK9eze43nhAkWXvir9yV6tUzpgQ`

## License Model

- Community/Open-Source License: **AGPL-3.0-only** (see `LICENSE`).
- Free for community use, self-hosting, modification, and redistribution under AGPL terms.
- If you run a modified version as a network service, AGPL requires publishing source code of that modified version.
- Commercial/Enterprise License: available for companies that need closed-source, white-label, OEM, or AGPL-waiver terms.
- Contact for commercial licensing/partnerships: `sanyo4ever@gmail.com`

More details: `LICENSE_POLICY.md`

## What you have now

- Continuous discovery worker (`discovery_worker.py`) that updates DB on schedule
- Universe worker (`universe_worker.py`) that builds long-lived qualified trader pool
- Top100 worker (`top100_worker.py`) that refreshes `traders_top100_live`, `catalog_current`, and HOT/WARM/COLD monitoring pool
- Auto-discovered traders are kept `ACTIVE` by default (blacklist controls delivery blocking)
- Rich trader stats (7d/30d activity, PnL, fees, win rate, volume, age, score, margin stats)
- Extended performance stats per trader (7d/30d ROI, PnL, Win/Lose, Profit-to-Loss, Avg PnL/trade, Max Drawdown, Sharpe, Sortino, ROI volatility)
- Public subscriber directory page (`/`) over `catalog_current` with keyset pagination
- Public JSON API for full catalog filtering/sorting/search: `/api/traders`
- One-click trader chat flow via Telegram bot (`/subscribe/<trader_address>`)
- Topic-based delivery sessions per subscription (`createForumTopic`)
- Password-protected admin panel (`/admin`) with add/delete, blacklist/whitelist, bulk moderation, and run discovery now
- Telegram posting pipeline that delivers fills only for active subscriber demand (tiered polling cadence)

## Entrypoints

- `python main.py` -> Telegram posting loop
- `python discover.py` -> one discovery cycle
- `python discovery_worker.py` -> continuous discovery service
- `python universe_worker.py` -> continuous universe refresh service
- `python top100_worker.py` -> continuous live top100 refresh service
- `python admin.py --host 127.0.0.1 --port 8080` -> web server
- `python subscriber_bot.py` -> Telegram command bot for user subscriptions

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Required `.env`

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHANNEL_ID=-100...
TELEGRAM_BOT_USERNAME=your_bot_username
DATABASE_URL=postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
# optional local fallback for dev only:
# DATABASE_PATH=data/signals.db

HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
DISCOVERY_CANDIDATE_LIMIT=60
DISCOVERY_MIN_AGE_DAYS=30
DISCOVERY_MIN_TRADES_30D=120
DISCOVERY_MIN_ACTIVE_DAYS_30D=12
DISCOVERY_MIN_WIN_RATE_30D=0.52
DISCOVERY_MAX_DRAWDOWN_30D_PCT=25.0
DISCOVERY_MAX_LAST_ACTIVITY_MINUTES=60
DISCOVERY_MIN_REALIZED_PNL_30D=0.0
DISCOVERY_REQUIRE_POSITIVE_PNL_30D=true
DISCOVERY_MIN_TRADES_7D=1
DISCOVERY_CONCURRENCY=6
DISCOVERY_FILL_CAP_HINT=1900
DISCOVERY_AGE_PROBE_ENABLED=true
DISCOVERY_SEED_ADDRESSES=
NANSEN_API_URL=https://api.nansen.ai
NANSEN_API_KEY=
NANSEN_CANDIDATE_LIMIT=60
DISCOVERY_INTERVAL_SECONDS=900

ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=your_strong_password

# 0 = permanent until user cancels with /stop
SUBSCRIPTION_LIFETIME_HOURS=0

UNIVERSE_INTERVAL_SECONDS=300
UNIVERSE_MIN_AGE_DAYS=30
UNIVERSE_MIN_TRADES_30D=120
UNIVERSE_MIN_ACTIVE_DAYS_30D=12
UNIVERSE_MIN_WIN_RATE_30D=0.52
UNIVERSE_MAX_DRAWDOWN_30D_PCT=25.0
UNIVERSE_MAX_LAST_ACTIVITY_MINUTES=60
UNIVERSE_MIN_REALIZED_PNL_30D=0.000001
UNIVERSE_MIN_SCORE=0.0
UNIVERSE_MAX_SIZE=3000

LIVE_TOP100_INTERVAL_SECONDS=60
LIVE_TOP100_ACTIVE_WINDOW_MINUTES=60
LIVE_TOP100_SIZE=100

MONITOR_HOT_SIZE=100
MONITOR_WARM_SIZE=400
MONITOR_HOT_POLL_SECONDS=60
MONITOR_WARM_POLL_SECONDS=600
MONITOR_COLD_POLL_SECONDS=3600
MONITOR_HOT_RECENCY_MINUTES=60
MONITOR_WARM_RECENCY_MINUTES=360
MONITOR_MAX_TARGETS_PER_CYCLE=120
MONITOR_DELIVERY_ONLY_SUBSCRIBED=true

LOG_LEVEL=INFO
LOG_FORMAT=text
# Optional rotating logs (in addition to journald):
# LOG_DIRECTORY=/opt/cryptoinsider/app/data/logs
LOG_FILE_MAX_BYTES=10485760
LOG_FILE_BACKUP_COUNT=5
LOG_TELEGRAM_HTTP=false
```

## PostgreSQL migration (from existing SQLite)

```bash
source .venv/bin/activate
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path data/signals.db \
  --postgres-url postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
```

## Run

1. Start web server:
```bash
source .venv/bin/activate
python admin.py --host 127.0.0.1 --port 8080
```

2. Start continuous discovery:
```bash
source .venv/bin/activate
python discovery_worker.py
```

3. Start universe worker:
```bash
source .venv/bin/activate
python universe_worker.py
```

4. Start top100 worker:
```bash
source .venv/bin/activate
python top100_worker.py
```

5. Start Telegram posting:
```bash
source .venv/bin/activate
python main.py
```

6. Start subscriber bot:
```bash
source .venv/bin/activate
python subscriber_bot.py
```

## Pages

- `http://127.0.0.1:8080/` -> public subscriber directory with filters
- `http://127.0.0.1:8080/subscribe/<trader_address>` -> subscription landing page (open-source + donation info, subscription active until cancel) and redirects to Telegram
- `http://127.0.0.1:8080/subscribe/<trader_address>/go` -> direct deep-link redirect endpoint
- `http://127.0.0.1:8080/admin` -> admin panel (HTTP Basic Auth)

`/api/traders` now includes extra computed fields:
- `roi_1d`, `roi_7d`, `roi_30d`
- `pnl_1d`, `pnl_7d`, `pnl_30d`
- `wins_1d`, `wins_7d`, `wins_30d`, `losses_1d`, `losses_7d`, `losses_30d`
- `profit_to_loss_ratio_1d`, `profit_to_loss_ratio_7d`, `profit_to_loss_ratio_30d`
- `trades_1d`, `weekly_trades`, `avg_pnl_per_trade_1d`, `avg_pnl_per_trade_7d`, `avg_pnl_per_trade_30d`
- `max_drawdown_1d`, `max_drawdown_7d`, `max_drawdown_30d`
- `sharpe_1d`, `sharpe_7d`, `sharpe_30d`, `sortino_1d`, `sortino_7d`, `sortino_30d`
- `roi_volatility_1d`, `roi_volatility_7d`, `roi_volatility_30d`

## Logging and Debugging

- All services use a shared logger format and include debug context IDs (`cycle_id`, `poll_id`, `request_id`, `update_id`).
- Default output goes to `journald`; optionally enable rotating files with `LOG_DIRECTORY`.
- Switch to JSON logs with `LOG_FORMAT=json` for ingestion into Loki/ELK.
- To see Telegram HTTP calls (`sendMessage`, `createForumTopic`, etc.), set `LOG_TELEGRAM_HTTP=true`.

Useful commands:

```bash
sudo journalctl -u cryptoinsider-admin.service -f
sudo journalctl -u cryptoinsider-subscriberbot.service -f
sudo journalctl -u cryptoinsider-poster.service -f
sudo journalctl -u cryptoinsider-discovery.service -f
sudo journalctl -u cryptoinsider-universe.service -f
sudo journalctl -u cryptoinsider-top100.service -f
```

## Production Planning

- Project memory / decision log: `PROJECT_MEMORY.md`
- Ubuntu production architecture: `docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`
- Trader discovery + full catalog architecture: `docs/TRADER_DISCOVERY_ARCHITECTURE.md`
- Deployment templates (systemd + nginx): `deploy/`

## Main DB tables

### `tracked_traders`

- identity: `address`, `label`, `source`
- control: `status`, `manual_status_override`, `moderation_state`, `moderation_note`, `moderated_at`
- activity: `trades_24h`, `active_hours_24h`, `trades_7d`, `trades_30d`, `active_days_30d`
- quality/finance: `realized_pnl_30d`, `win_rate_30d`, `volume_usd_30d`, `fees_30d`, `score`
- risk/state: `account_value`, `total_ntl_pos`, `total_margin_used`
- history: `first_fill_time`, `last_fill_time`, `age_days`, `stats_json`

### `discovery_runs`

- run metadata: `source`, `status`, `candidates`, `qualified`, `upserted`, `error_message`, `finished_at`

### `traders_universe`

- persistent qualified pool built from `tracked_traders`
- supports thresholds: age, activity, winrate, pnl, score

### `traders_top100_live`

- rolling live shortlist of active traders (`last_fill_time` within configured window)
- maintained for shortlist/ranking use-cases

### `trader_monitoring_pool`

- HOT/WARM/COLD tier map with per-tier polling intervals and lookback windows
- worker-maintained `next_poll_at` scheduling for efficient delivery monitoring
- poster fetches only due targets (optionally only active subscribers)

### `catalog_current`

- denormalized full trader catalog for subscriber page and API
- includes activity score + moderation fields + sortable metrics
- supports keyset pagination for stable performance at scale

### `subscriptions` + `delivery_sessions`

- each subscription creates a dedicated session
- bot creates Telegram forum topic and delivers fills in that `message_thread_id`
- session is active until explicit cancellation (`/stop 0x...`)

## Important behavior

- Discovery updates `tracked_traders` stats.
- Universe/top100 workers derive rankings from discovery results.
- `catalog_current` powers full trader directory and `/api/traders`.
- Discovery keeps all tracked traders active in DB; moderation (`BLACKLIST`) controls blocking from delivery/monitoring.
- Blacklisted traders are removed from active delivery/monitoring flows until moderation changes.
- Telegram source monitors due tier targets and (by default) only active-subscriber demand.
- New subscription creates a new topic thread and runs until cancellation.
