# Shared Forum Delivery Architecture

Last updated: 2026-03-08

## 1. Goals

- Deliver trader fills into one shared Telegram forum chat.
- Keep one deterministic topic per trader wallet.
- Deliver updates with typical lag <= 60 seconds.
- Keep routing deterministic (no cross-target duplication).
- Recover safely from transient API failures.

## 2. Delivery Model

Entry point:

1. User clicks `Join Channel` in catalog.
2. User is redirected to configured Telegram join URL.
3. Poster auto-creates forum topics when new trader signals appear.
4. Optional `subscriber_bot` DM deep-links (`/start sub_0x...`) reuse the same shared topic mapping and return direct topic links.

Current lifecycle model:

- Topic mapping is persisted in `trader_forum_topics`.
- If a topic is deleted manually, mapping is purged and recreated on next signal.
- If forum mode is unavailable, poster can fallback to root channel posting.

## 3. Core Tables

- `trader_forum_topics`
  - durable mapping (`trader_address`, `forum_chat_id`, `message_thread_id`).
- `delivery_monitor_state`
  - adaptive scanning schedule per trader.
- `delivery_retry_queue`
  - durable retries for transient Telegram failures.
- `published_signals`
  - dedup store for already-published signals.
- `subscriptions`, `delivery_sessions`
  - legacy DM-subscription model kept for compatibility/migration.

## 4. Cycle Pipeline (`cryptoinsider-poster`)

Per cycle:

1. Refresh monitor state and pick due trader targets (`next_poll_at <= now`).
2. Poll new fills incrementally using per-trader watermark (`last_seen_fill_time`).
3. Build normalized signals and map each signal to forum target.
4. Ensure topic exists (`createForumTopic` on first signal for trader).
5. Send through dispatcher with per-chat pacing and bounded concurrency.
6. Process retry queue once per cycle (before live sends).

## 5. Routing and Isolation Rules

- Signal target key: `(forum_chat_id, message_thread_id)`.
- Duplicate target keys in one cycle are removed.
- One topic per trader wallet prevents cross-wallet mixing.

## 6. Error Classification and Actions

- `flood` / transient transport errors:
  - enqueue retry with backoff (`RETRY_BASE_DELAY_SECONDS`, capped).
  - apply batch-level global flood backoff to avoid 429 cascades.
- `topic_missing` (thread deleted/not found):
  - delete stale `trader_forum_topics` mapping for that trader/chat.
- `chat_unavailable` / bot blocked:
  - drop live attempt and mark retries dead (no subscription side effects).
- non-retryable permanent errors:
  - mark dropped/dead to prevent endless loops.

## 7. Reliability Controls

- Dedup key: `source_id:external_id`.
- Dedup retention: old keys are TTL-cleaned from `published_signals`.
- Retry queue unique key: `(dedup_key, chat_id, message_thread_id)` for idempotent resend.
- Dispatcher:
  - global send concurrency limit,
  - per-topic pacing key (`chat_id:message_thread_id`),
  - minimum interval per topic.
- Monitor backoff:
  - faster polling when fresh fills appear,
  - slower polling on idle/error streaks.

## 8. Observability

Track at minimum:

- `signals_fetched_per_cycle`
- `published_signals_per_cycle`
- `delivery_success_total`
- `retry_queued_total`
- `retry_sent_total`
- `retry_dead_total`
- `topic_missing_total`
- `chat_unavailable_total`
- `cycle_elapsed_ms`

## 9. Business Scenarios Covered by Tests

Automated tests cover:

- join redirect tracking endpoint,
- forum topic creation + mapping reuse,
- topic deletion recovery (`topic_missing` -> mapping reset),
- transient send failure -> retry queue -> successful resend,
- global flood backoff across target batch.

See:

- `tests/test_delivery_dispatcher.py`
- `tests/test_e2e_subscription_delivery.py`

## 10. Known Constraints

- Telegram Bot API cannot create a brand-new standalone chat on demand.
- Forum topics require supergroup/forum chat capabilities.
- External API limits (Telegram + data sources) require backpressure and retry logic.
