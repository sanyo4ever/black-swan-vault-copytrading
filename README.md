# CryptoInsider Futures Discovery

Service stack for discovering futures traders, storing rich metrics, and publishing selected traders to Telegram.

## What you have now

- Continuous discovery worker (`discovery_worker.py`) that updates DB on schedule
- Universe worker (`universe_worker.py`) that builds long-lived qualified trader pool
- Top100 worker (`top100_worker.py`) that refreshes live active list (<1h) for subscriber page
- Auto-discovered traders are added as `PAUSED` by default
- Rich trader stats (7d/30d activity, PnL, fees, win rate, volume, age, score, margin stats)
- Public subscriber directory page with filters (`/`) over `traders_top100_live`
- One-click trader chat flow via Telegram bot (`/subscribe/<trader_address>`)
- Topic-based 24h delivery sessions per subscription (`createForumTopic` + `deleteForumTopic`)
- Password-protected admin panel (`/admin`) for add/delete/pause/resume + run discovery now
- Telegram posting pipeline that sends fills to channel + subscriber topic threads

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
DATABASE_PATH=data/signals.db

HYPERLIQUID_INFO_URL=https://api.hyperliquid.xyz/info
DISCOVERY_CANDIDATE_LIMIT=60
DISCOVERY_MIN_AGE_DAYS=30
DISCOVERY_MIN_TRADES_30D=10
DISCOVERY_MIN_ACTIVE_DAYS_30D=4
DISCOVERY_MIN_TRADES_7D=1
DISCOVERY_CONCURRENCY=6
DISCOVERY_FILL_CAP_HINT=1900
DISCOVERY_AGE_PROBE_ENABLED=true
DISCOVERY_INTERVAL_SECONDS=900

ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=your_strong_password

SUBSCRIPTION_LIFETIME_HOURS=24

UNIVERSE_INTERVAL_SECONDS=300
UNIVERSE_MIN_AGE_DAYS=30
UNIVERSE_MIN_TRADES_30D=10
UNIVERSE_MIN_WIN_RATE_30D=0.0
UNIVERSE_MIN_REALIZED_PNL_30D=0.0
UNIVERSE_MIN_SCORE=0.0
UNIVERSE_MAX_SIZE=3000

LIVE_TOP100_INTERVAL_SECONDS=60
LIVE_TOP100_ACTIVE_WINDOW_MINUTES=60
LIVE_TOP100_SIZE=100
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

### `traders_universe`

- persistent qualified pool built from `tracked_traders`
- supports thresholds: age, activity, winrate, pnl, score

### `traders_top100_live`

- rolling live shortlist of active traders (`last_fill_time` within configured window)
- ranked by activity score for subscriber page

### `subscriptions` + `delivery_sessions`

- each subscription creates a dedicated 24h session
- bot creates Telegram forum topic and delivers fills in that `message_thread_id`
- expiry flow marks session expired and deletes topic

## Important behavior

- Discovery updates `tracked_traders` stats.
- Universe/top100 workers derive subscriber-facing list from discovery results.
- Traders stay `PAUSED` until you manually click `Resume` in admin.
- Telegram source monitors addresses from active sessions + active trader set.
- New subscription creates a new topic thread with TTL 24h.
