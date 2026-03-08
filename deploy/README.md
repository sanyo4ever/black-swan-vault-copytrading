# Deploy on Ubuntu (Current Stack)

This guide describes the current deployment workflow for the single-server production setup.

## 1. Prerequisites

- Ubuntu server with systemd and nginx
- Python virtualenv at `/opt/cryptoinsider/app/.venv`
- PostgreSQL running locally (`127.0.0.1:5432`)
- Repository cloned to `/opt/cryptoinsider/app`
- Secrets stored in `/etc/cryptoinsider/env`

## 2. Required paths and permissions

- App root: `/opt/cryptoinsider/app`
- Env file: `/etc/cryptoinsider/env`

Recommended permissions:

```bash
sudo mkdir -p /opt/cryptoinsider /etc/cryptoinsider
sudo chown -R cryptoinsider:cryptoinsider /opt/cryptoinsider
sudo chmod 700 /etc/cryptoinsider
sudo chmod 640 /etc/cryptoinsider/env
```

## 3. Environment example (`/etc/cryptoinsider/env`)

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHANNEL_ID=...
TELEGRAM_FORUM_CHAT_ID=...
TELEGRAM_JOIN_URL=https://t.me/YourChannelOrInvite
DATABASE_URL=postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
SHOWCASE_MODE_ENABLED=true
SHOWCASE_SLOTS=25
ROTATION_BOOTSTRAP_INTERVAL_MINUTES=360
ROTATION_SCOUT_INTERVAL_HOURS=24
ROTATION_HEALTH_INTERVAL_MINUTES=180
ROTATION_SCOUT_ON_STALE_IMMEDIATE=true
ADMIN_PANEL_USERNAME=admin
ADMIN_PANEL_PASSWORD=strong_password
GOOGLE_ANALYTICS_MEASUREMENT_ID=G-XXXXXXXXXX
LOG_LEVEL=INFO
LOG_FORMAT=text
LOG_DIRECTORY=/opt/cryptoinsider/app/data/logs
LOG_FILE_MAX_BYTES=10485760
LOG_FILE_BACKUP_COUNT=5
LOG_TELEGRAM_HTTP=false
```

## 4. Install/enable systemd units

```bash
sudo cp deploy/systemd/cryptoinsider-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cryptoinsider-admin.service
sudo systemctl enable --now cryptoinsider-discovery.service
sudo systemctl enable --now cryptoinsider-universe.service
sudo systemctl enable --now cryptoinsider-top100.service
sudo systemctl enable --now cryptoinsider-poster.service
sudo systemctl enable --now cryptoinsider-rotation.service
# optional legacy DM flow only:
# sudo systemctl enable --now cryptoinsider-subscriberbot.service
```

If `SHOWCASE_MODE_ENABLED=true`, keep `cryptoinsider-rotation` enabled and
disable heavy ranking workers:

```bash
sudo systemctl disable --now cryptoinsider-discovery.service
sudo systemctl disable --now cryptoinsider-universe.service
sudo systemctl disable --now cryptoinsider-top100.service
```

Recommended for low API pressure: keep bootstrap/scout intervals high
(`ROTATION_BOOTSTRAP_INTERVAL_MINUTES`, `ROTATION_SCOUT_INTERVAL_HOURS`).

## 5. Deploy update (recommended workflow)

On CI/local machine:

```bash
git push origin main
```

On server:

```bash
cd /opt/cryptoinsider/app
git pull --ff-only
```

Restart only changed services (common web/bot update):

```bash
sudo systemctl restart cryptoinsider-admin
sudo systemctl restart cryptoinsider-poster
```

## 6. Post-deploy verification (required)

Run QA gate test suite:

```bash
cd /opt/cryptoinsider/app
/opt/cryptoinsider/app/.venv/bin/python scripts/qa_certification.py --skip-db-audit
```

Run live data-quality audit against production DB:

```bash
source /etc/cryptoinsider/env
/opt/cryptoinsider/app/.venv/bin/python scripts/qa_certification.py \
  --skip-tests \
  --database "$DATABASE_URL" \
  --freshness-minutes 60 \
  --json-out /opt/cryptoinsider/app/data/qa-report-prod.json
```

Check services:

```bash
sudo systemctl is-active \
  cryptoinsider-admin \
  cryptoinsider-rotation \
  cryptoinsider-poster
```

## 7. Logs and debugging

```bash
sudo journalctl -u cryptoinsider-admin -f
sudo journalctl -u cryptoinsider-poster -f
sudo journalctl -u cryptoinsider-rotation -f
```

Optional file logs (if `LOG_DIRECTORY` configured):

```bash
sudo ls -lah /opt/cryptoinsider/app/data/logs
```

## 8. Nginx reverse proxy

```bash
sudo cp deploy/nginx/cryptoinsider.conf /etc/nginx/sites-available/cryptoinsider.conf
sudo ln -sf /etc/nginx/sites-available/cryptoinsider.conf /etc/nginx/sites-enabled/cryptoinsider.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 9. Legacy SQLite migration (one-time)

```bash
cd /opt/cryptoinsider/app
source /etc/cryptoinsider/env
/opt/cryptoinsider/app/.venv/bin/python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path data/signals.db \
  --postgres-url "$DATABASE_URL"
```
