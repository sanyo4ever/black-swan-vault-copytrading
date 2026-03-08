# Production Architecture (Ubuntu)

Last updated: 2026-03-08

## 1. Purpose

This document describes the current production architecture used by Black Swan Vault on Ubuntu.

Primary goals:

1. Continuously discover and rank futures traders.
2. Expose a public catalog with fast filtering/sorting/search.
3. Let users join one shared Telegram channel in one click.
4. Deliver trader fills to Telegram with bounded latency and resilient retries.
5. Keep operations simple for a single-server setup.

## 2. Product Model (Current)

- The project is open-source and donation-supported.
- There is no mandatory paywall in the current production flow.
- Public catalog action is `Join Channel` (redirect via `/subscribe/{address}/go`).
- Poster creates/uses one forum topic per trader wallet in the configured forum chat.
- If forum topics are unsupported in that chat, poster falls back to root channel posting (no per-wallet thread isolation).

## 3. Runtime Topology

```text
Internet -> Nginx (80/443) -> cryptoinsider-admin (127.0.0.1:8080)
                                       |
                                       +-> PostgreSQL (127.0.0.1:5432)

Hyperliquid/Nansen -> cryptoinsider-discovery
                         -> cryptoinsider-universe
                         -> cryptoinsider-top100
                         -> cryptoinsider-poster -> Telegram sendMessage
```

## 4. Services and Responsibilities

- `cryptoinsider-admin`
  - serves `/`, `/api/traders`, `/subscribe/{address}`, `/admin`.
  - public catalog + password-protected admin panel.

- `cryptoinsider-discovery`
  - fetches candidate traders, enriches metrics, applies hard filters, upserts `tracked_traders`.

- `cryptoinsider-universe`
  - builds qualified pool (`traders_universe`) from tracked traders.

- `cryptoinsider-top100`
  - refreshes `traders_top100_live`, monitoring pools, and `catalog_current` projection.

- `cryptoinsider-poster`
  - scans due traders (`userFillsByTime`) using per-trader watermarks.
  - resolves/creates forum-topic mapping (`trader_forum_topics`) for each trader signal.
  - formats and delivers signals to active targets.
  - handles dedup, retry queue, and Telegram topic recovery rules.

- `cryptoinsider-subscriberbot` (legacy/optional)
  - kept for backward compatibility with old DM subscription flow.
  - not required for shared forum-channel production mode.

## 5. Data Stores

- PostgreSQL (single-node, localhost only)
  - source of truth for traders, catalog, topic mappings, retries.

Key tables:

- `tracked_traders`
- `traders_universe`
- `traders_top100_live`
- `trader_monitoring_pool`
- `delivery_monitor_state`
- `catalog_current`
- `trader_forum_topics`
- `subscriptions`
- `delivery_sessions`
- `delivery_retry_queue`
- `published_signals`
- `discovery_runs`

## 6. Delivery Reliability Rules

- Dedup key: `source_id:external_id` (stored in `published_signals`).
- Dedup retention: TTL cleanup on `published_signals`.
- Retry queue: `delivery_retry_queue` with exponential backoff and `retry_after` support.
- Error policies:
  - `flood/transient` -> queue retry + batch-level global flood backoff.
  - `topic_missing` -> drop stale `trader_forum_topics` mapping and recreate later.
  - `chat_unavailable`/bot blocked -> drop failed target and keep DB consistent.
- Dispatcher enforces:
  - global send concurrency,
  - per-chat serialization,
  - per-chat minimum interval.

## 7. Security Baseline

- SSH key-only access.
- Firewall enabled (`ufw`) with minimal exposed ports.
- Application secrets in `/etc/cryptoinsider/env` with restricted permissions.
- PostgreSQL bound to localhost.
- Admin panel protected by Basic Auth.
- Fail2ban enabled for SSH brute-force mitigation.

## 8. Deployment Workflow (Current)

From your local machine:

```bash
git push origin main
```

On server:

```bash
cd /opt/cryptoinsider/app
git pull --ff-only
```

Restart only changed services (typical web/bot changes):

```bash
sudo systemctl restart cryptoinsider-admin
sudo systemctl restart cryptoinsider-poster
```

## 9. Post-Deploy Validation

Run QA gate on server:

```bash
cd /opt/cryptoinsider/app
/opt/cryptoinsider/app/.venv/bin/python scripts/qa_certification.py --skip-db-audit
```

Run live data-quality audit:

```bash
source /etc/cryptoinsider/env
/opt/cryptoinsider/app/.venv/bin/python scripts/qa_certification.py \
  --skip-tests \
  --database "$DATABASE_URL" \
  --freshness-minutes 60 \
  --json-out /opt/cryptoinsider/app/data/qa-report-prod.json
```

Check service state:

```bash
sudo systemctl is-active \
  cryptoinsider-admin \
  cryptoinsider-discovery \
  cryptoinsider-universe \
  cryptoinsider-top100 \
  cryptoinsider-poster
```

Check logs:

```bash
sudo journalctl -u cryptoinsider-admin -f
sudo journalctl -u cryptoinsider-poster -f
```

## 10. Backups and Recovery

Minimum policy:

- Daily PostgreSQL dump.
- Weekly restore test to a disposable DB.
- Keep at least 7 daily snapshots.

Example backup command:

```bash
pg_dump "$DATABASE_URL" > /var/backups/cryptoinsider/$(date +%F)-cryptoinsider.sql
```

## 11. References

- Telegram Bot API: https://core.telegram.org/bots/api
- Hyperliquid API docs: https://hyperliquid.gitbook.io/hyperliquid-docs/
