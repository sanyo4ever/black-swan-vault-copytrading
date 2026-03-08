# Subscription-Driven Delivery Architecture

Last updated: 2026-03-08

## 1. Goals

- Scan only traders that currently have active subscriptions.
- Deliver updates with typical lag <= 60 seconds.
- Keep routing deterministic (no cross-target duplication).
- Recover safely from transient API failures.

## 2. Subscription Lifecycle

Entry point:

1. User clicks `Copy` in catalog.
2. User is redirected to bot deep-link: `/start sub_<trader_address>`.
3. Bot validates trader status/moderation and creates a subscription session.

Current lifecycle model:

- Subscription status: `ACTIVE` until explicit stop (`/stop 0x...`).
- Session status: `ACTIVE` until cancellation/expiration/error handling.

Notes:

- `SUBSCRIPTION_LIFETIME_HOURS` is currently treated as permanent mode in runtime logic.
- If forum topics are unsupported (`createForumTopic` fails with "chat is not a forum"), the bot switches to direct-chat mode (`message_thread_id = NULL`).

## 3. Core Tables

- `subscriptions`
  - subscriber intent and lifecycle state.
- `delivery_sessions`
  - active delivery target binding (`chat_id`, `trader_address`, optional `message_thread_id`).
- `delivery_monitor_state`
  - demand-aware scanning schedule per trader.
- `delivery_retry_queue`
  - durable retries for transient Telegram failures.
- `published_signals`
  - dedup store for already-published signals.

## 4. Cycle Pipeline (`cryptoinsider-poster`)

Per cycle:

1. Refresh demand monitor from active sessions.
2. Pick due trader targets (`next_poll_at <= now`).
3. Poll new fills incrementally using per-trader watermark (`last_seen_fill_time`).
4. Build normalized signals and map each signal to subscribed targets.
5. Send through dispatcher with per-chat pacing and bounded concurrency.
6. Process retry queue before/after live sends.

## 5. Routing and Isolation Rules

- Signal fanout target key: `(chat_id, message_thread_id)`.
- Duplicate target keys in one cycle are removed.
- If two users subscribe to the same trader, each gets their own delivery target.
- If one user subscribes to multiple traders in direct-chat mode, messages share chat but remain identifiable by trader line in message body.

## 6. Error Classification and Actions

- `flood` / transient transport errors:
  - enqueue retry with backoff (`RETRY_BASE_DELAY_SECONDS`, capped).
- `topic_missing` (thread deleted/not found):
  - deactivate only affected chat+trader target.
- `chat_unavailable` / bot blocked:
  - deactivate all active targets for that chat.
- non-retryable permanent errors:
  - mark dropped/dead to prevent endless loops.

## 7. Reliability Controls

- Dedup key: `source_id:external_id`.
- Retry queue unique key: `(dedup_key, chat_id, message_thread_id)`.
- Dispatcher:
  - global send concurrency limit,
  - per-chat serialization,
  - per-chat minimum interval.
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

- subscribe -> active session creation,
- stop one trader while keeping other subscriptions active,
- fanout to multiple chats for same trader,
- transient send failure -> retry queue -> successful resend,
- topic missing deactivates one target only,
- bot blocked deactivates all targets for that chat.

See:

- `tests/test_e2e_subscription_delivery.py`
- `tests/test_subscriber_bot.py`
- `tests/test_delivery_dispatcher.py`

## 10. Known Constraints

- Telegram Bot API cannot create a brand-new standalone chat on demand.
- Topic mode depends on chat capabilities; direct-chat fallback is mandatory.
- External API limits (Telegram + data sources) require backpressure and retry logic.
