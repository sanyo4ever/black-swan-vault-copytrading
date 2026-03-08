# Contributing to Black Swan Vault Copytrading

Thanks for helping improve the project.

## Where to start

Read these first:

- [`README.md`](./README.md)
- [`docs/README.md`](./docs/README.md)
- [`docs/TRADER_DISCOVERY_ARCHITECTURE.md`](./docs/TRADER_DISCOVERY_ARCHITECTURE.md)
- [`docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md`](./docs/SUBSCRIBED_DELIVERY_ARCHITECTURE.md)
- [`docs/QA_TEST_STRATEGY.md`](./docs/QA_TEST_STRATEGY.md)

## Typical contribution areas

- Discovery connectors and candidate quality improvements
- Metrics/scoring model improvements
- Delivery reliability and Telegram edge-case handling
- Query/index optimization and worker throughput
- Tests, runbooks, and documentation quality

## Development setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Test and QA commands

Run full local QA gate:

```bash
python scripts/qa_certification.py --skip-db-audit
```

Run live data quality audit (if DB available):

```bash
python scripts/qa_certification.py --skip-tests --database "$DATABASE_URL"
```

Run a focused module quickly:

```bash
python -m unittest -q tests/test_trader_store.py
python -m unittest -q tests/test_e2e_subscription_delivery.py
```

After manual/E2E tests against shared DBs, clean synthetic rows:

```bash
python scripts/cleanup_test_traders.py --postgres-url "$DATABASE_URL" --apply --yes
```

## Pull request expectations

- Changes are focused and explainable.
- Tests added/updated for behavior changes.
- QA gate passes locally.
- No secrets/tokens in code/docs/logs/screenshots.
- Docs updated in the same PR when behavior changed.

## Documentation update rule (required)

If you change behavior, update the relevant docs in the same PR:

1. `README.md` for setup/user-facing behavior.
2. `docs/*.md` for architecture/data flow/state machine changes.
3. `deploy/README.md` for deployment/ops changes.

## Branching and commits

- Branch from `main`.
- Keep commits descriptive and scoped.
- Prefer commit types:
  - `feat:`
  - `fix:`
  - `docs:`
  - `refactor:`
  - `test:`

## Reporting issues

Use GitHub Issues and include:

- environment (local/prod, Python version, DB type)
- reproducible steps
- expected vs actual behavior
- relevant logs/tracebacks

## Security

Do not post sensitive vulnerabilities publicly.

Follow [`SECURITY.md`](./SECURITY.md).
