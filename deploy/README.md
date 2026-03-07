# Deploy on Ubuntu (Current Stack)

## 1. Create service user
```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin cryptoinsider
```

## 2. App path
- App root: `/opt/cryptoinsider/app`
- Env file: `/etc/cryptoinsider/env`

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

## 5. Logs
```bash
sudo journalctl -u cryptoinsider-admin.service -f
sudo journalctl -u cryptoinsider-discovery.service -f
sudo journalctl -u cryptoinsider-universe.service -f
sudo journalctl -u cryptoinsider-top100.service -f
sudo journalctl -u cryptoinsider-poster.service -f
sudo journalctl -u cryptoinsider-subscriberbot.service -f
```

## 6. Nginx reverse proxy
```bash
sudo cp deploy/nginx/cryptoinsider.conf /etc/nginx/sites-available/cryptoinsider.conf
sudo ln -s /etc/nginx/sites-available/cryptoinsider.conf /etc/nginx/sites-enabled/cryptoinsider.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 7. Next production step
Current units run the existing codebase (SQLite + single-node). For full production architecture see:
- `docs/PRODUCTION_ARCHITECTURE_UBUNTU.md`
