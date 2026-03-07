# Subscription-Driven Delivery Architecture (v2)

## 1) Goal
Build a production-ready service that:
- scans only traders that currently have active subscriptions,
- emits trade signals with <= 60s typical lag,
- fans out updates to all subscribed users without cross-chat mixing,
- survives API limits, temporary outages, and Telegram edge cases.

## 2) External Benchmark References
- Hyperliquid API docs:
  - WebSocket subscriptions: [docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
  - Info endpoint (`userFillsByTime`): [docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
  - Rate limits and capacity constraints: [docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
- Telegram Bot API and FAQ:
  - Bot API reference (`sendMessage`, `createForumTopic`, `deleteForumTopic`): [docs](https://core.telegram.org/bots/api)
  - Broadcast and rate-limit guidance (`429`, `retry_after`, throughput notes): [FAQ](https://core.telegram.org/bots/faq)
- Copy-trading product reference (follower lifecycle and account linking):  
  - Bybit Copy Trading API docs: [docs](https://bybit-exchange.github.io/docs/v5/copytrade)
- Open-source implementation reference:
  - HyperCopy (Hyperliquid copy-trading bot): [repo](https://github.com/sunmoon97/HyperCopy)

## 3) Hard Constraints That Shape Design
- Telegram is push-rate limited and can return `429` with `retry_after`.
- Telegram topics can be missing/deleted manually and chats can become unavailable.
- Hyperliquid polling can hit `429`/5xx under burst load.
- Scanner cost must scale with active demand (subscribed traders), not full universe size.

## 4) Target Runtime Topology
One process (`cryptoinsider-poster`) with five internal stages per cycle:

1. Session lifecycle stage:
- expire/cancel old sessions,
- best-effort topic cleanup.

2. Retry stage:
- send due jobs from `delivery_retry_queue`,
- classify errors and reschedule/dead-letter.

3. Scan stage:
- poll only due addresses from `delivery_monitor_state` (built from active sessions),
- fetch incremental fills via `userFillsByTime` from a watermark.

4. Fan-out stage:
- map signal -> subscribed chat/thread targets,
- send via controlled dispatcher (global concurrency + per-chat pacing).

5. Retry stage (again):
- flush immediate transient failures quickly in same cycle.

## 5) Data Plane
Existing tables already support this architecture:
- `subscriptions`, `delivery_sessions`: source of truth for active recipients.
- `delivery_monitor_state`: demand-aware polling schedule per trader.
- `delivery_retry_queue`: durable retries with dedup key `(dedup_key, chat_id, message_thread_id)`.
- `tracked_traders`: moderation/status guardrails.

## 6) Delivery Algorithm (v2)
For each cycle:
1. Refresh `delivery_monitor_state` from active sessions.
2. Select due traders ordered by priority score.
3. Poll fills concurrently with bounded HTTP concurrency.
4. Convert fills to canonical `TradeSignal` IDs.
5. For each signal, fan out to all active targets for that trader.
6. On delivery failure:
- `flood/transient`: enqueue retry with exponential backoff and optional `retry_after`,
- `topic_missing`: deactivate only that chat+trader target,
- `chat_unavailable`: deactivate all chat subscriptions.

## 7) Dispatcher Contract
The dispatcher enforces:
- global in-flight send concurrency cap,
- per-chat serialization (message order stability),
- per-chat minimum interval between sends (reduces flood risk).

This gives predictable fan-out behavior under spikes.

## 8) SLOs and Operational Metrics
- `scan_lag_seconds` (fill timestamp -> post timestamp)
- `cycle_elapsed_ms`
- `delivery_attempts_total`, `delivery_success_total`, `delivery_retry_queued_total`
- `retry_sent_total`, `retry_rescheduled_total`, `retry_dead_total`
- `flood_errors_total`, `topic_missing_total`, `chat_unavailable_total`
- `due_targets_count`, `signals_per_cycle`

## 9) Failure-Mode Policy
- Hyperliquid `429/5xx`: retry with capped backoff, summarize in cycle logs.
- Telegram `429`: reschedule using `retry_after` when present.
- Topic deleted manually: deactivate only affected trader thread.
- Bot removed/blocked in chat: deactivate entire chat targets.
- Process restart: retry queue + DB session state allow clean recovery.

## 10) Why This Is Better Than Naive Fan-Out
- Demand-driven scanning (only subscribed traders) lowers server/API load.
- Controlled dispatch avoids Telegram flood spikes.
- Durable retry queue prevents message loss during transient failures.
- Clear deactivation rules keep stale/dead targets from poisoning delivery.
