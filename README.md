# CryptoInsider Futures Discovery

Service stack for discovering futures traders, storing rich metrics, and publishing selected traders to Telegram.

## What you have now

- Continuous discovery worker (`discovery_worker.py`) that updates DB on schedule
- Auto-discovered traders are added as `PAUSED` by default
- Rich trader stats (7d/30d activity, PnL, fees, win rate, volume, age, score, margin stats)
- Public subscriber directory page with filters (`/`)
- One-click trader chat flow via Telegram bot (`/subscribe/<trader_address>`)
- Password-protected admin panel (`/admin`) for add/delete/pause/resume + run discovery now
- Telegram posting pipeline that sends fills for `ACTIVE` traders to channel + subscribed users

## Entrypoints

- `python main.py` -> Telegram posting loop
- `python discover.py` -> one discovery cycle
- `python discovery_worker.py` -> continuous discovery service
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
DATABASE_PATH=data/signals.db

HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
DISCOVERY_CANDIDATE_LIMIT=60
DISCOVERY_MIN_AGE_DAYS=30
DISCOVERY_MIN_TRADES_30D=10
DISCOVERY_MIN_ACTIVE_DAYS_30D=4
DISCOVERY_MIN_TRADES_7D=1
DISCOVERY_CONCURRENCY=6
DISCOVERY_INTERVAL_SECONDS=900

ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=your_strong_password
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

3. Start Telegram posting:
```bash
source .venv/bin/activate
python main.py
```

4. Start subscriber bot:
```bash
source .venv/bin/activate
python subscriber_bot.py
```

## Pages

- `http://127.0.0.1:8080/` -> public subscriber directory with filters
- `http://127.0.0.1:8080/subscribe/<trader_address>` -> logs request and redirects user to bot deep-link
- `http://127.0.0.1:8080/admin` -> admin panel (HTTP Basic Auth)

## Production Planning

- Project memory / decision log: `PROJECT_MEMORY.md`
- Ubuntu production architecture: `docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`
- Deployment templates (systemd + nginx): `deploy/`

## Main DB tables

### `tracked_traders`

- identity: `address`, `label`, `source`
- control: `status`, `manual_status_override`
- activity: `trades_24h`, `active_hours_24h`, `trades_7d`, `trades_30d`, `active_days_30d`
- quality/finance: `realized_pnl_30d`, `win_rate_30d`, `volume_usd_30d`, `fees_30d`, `score`
- risk/state: `account_value`, `total_ntl_pos`, `total_margin_used`
- history: `first_fill_time`, `last_fill_time`, `age_days`, `stats_json`

### `discovery_runs`

- run metadata: `source`, `status`, `candidates`, `qualified`, `upserted`, `error_message`, `finished_at`

### `telegram_trader_subscriptions`

- links a private Telegram chat (`chat_id`) to selected `trader_address`
- supports one-click subscribe via bot deep-link payload `sub_<trader_address>`

## Important behavior

- Discovery can update stats for a trader, but does not auto-enable posting.
- Traders stay `PAUSED` until you manually click `Resume` in admin.
- Telegram source posts only `ACTIVE` traders.
- Subscriber receives only trades from traders they selected in bot.
