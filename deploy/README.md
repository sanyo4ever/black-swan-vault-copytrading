# Deploy on Ubuntu (Current Stack)

## 1. Create service user
```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin cryptoinsider
```

## 2. App path
- App root: `/opt/cryptoinsider/app`
- Env file: `/etc/cryptoinsider/env`

Example DB env (`/etc/cryptoinsider/env`):
```bash
DATABASE_URL=postgresql://cryptoinsider:strong_password@127.0.0.1:5432/cryptoinsider
LOG_LEVEL=INFO
LOG_FORMAT=text
LOG_DIRECTORY=/opt/cryptoinsider/app/data/logs
LOG_FILE_MAX_BYTES=10485760
LOG_FILE_BACKUP_COUNT=5
LOG_TELEGRAM_HTTP=false
```

Example permissions:
```bash
sudo mkdir -p /opt/cryptoinsider /etc/cryptoinsider
sudo chown -R cryptoinsider:cryptoinsider /opt/cryptoinsider
sudo chmod 700 /etc/cryptoinsider
sudo chmod 600 /etc/cryptoinsider/env
```

## 3. Install units
```bash
sudo cp deploy/systemd/cryptoinsider-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cryptoinsider-admin.service
sudo systemctl enable --now cryptoinsider-discovery.service
sudo systemctl enable --now cryptoinsider-universe.service
sudo systemctl enable --now cryptoinsider-top100.service
sudo systemctl enable --now cryptoinsider-poster.service
sudo systemctl enable --now cryptoinsider-subscriberbot.service
```

## 4. Check status
```bash
sudo systemctl status cryptoinsider-admin.service
sudo systemctl status cryptoinsider-discovery.service
sudo systemctl status cryptoinsider-universe.service
sudo systemctl status cryptoinsider-top100.service
sudo systemctl status cryptoinsider-poster.service
sudo systemctl status cryptoinsider-subscriberbot.service
```

## 5. Run tests on server (required)
Always run repository tests directly on the server after deploy/restart:

```bash
cd /opt/cryptoinsider/app
/opt/cryptoinsider/app/.venv/bin/python -m unittest -q \
  tests/test_config.py \
  tests/test_telegram_client.py \
  tests/test_subscriber_bot.py \
  tests/test_trader_store.py
```

## 6. Logs
```bash
sudo journalctl -u cryptoinsider-admin.service -f
sudo journalctl -u cryptoinsider-discovery.service -f
sudo journalctl -u cryptoinsider-universe.service -f
sudo journalctl -u cryptoinsider-top100.service -f
sudo journalctl -u cryptoinsider-poster.service -f
sudo journalctl -u cryptoinsider-subscriberbot.service -f
```

Optional file logs (if `LOG_DIRECTORY` is configured):

```bash
sudo ls -lah /opt/cryptoinsider/app/data/logs
sudo tail -f /opt/cryptoinsider/app/data/logs/cryptoinsider_subscriber_bot.log
```

## 7. Nginx reverse proxy
```bash
sudo cp deploy/nginx/cryptoinsider.conf /etc/nginx/sites-available/cryptoinsider.conf
sudo ln -s /etc/nginx/sites-available/cryptoinsider.conf /etc/nginx/sites-enabled/cryptoinsider.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 8. Next production step
If you have old SQLite data, migrate once:
```bash
cd /opt/cryptoinsider/app
/opt/cryptoinsider/app/.venv/bin/python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path data/signals.db \
  --postgres-url "$DATABASE_URL"
```

For full production architecture see:
- `docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`
