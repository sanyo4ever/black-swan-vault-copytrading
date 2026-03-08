# Documentation Index

Start here if you are new to the project.

## Product and Architecture

- [`../README.md`](../README.md): project overview, quick start, data model, QA gate commands.
- [`TRADER_DISCOVERY_ARCHITECTURE.md`](./TRADER_DISCOVERY_ARCHITECTURE.md): how discovery, scoring, lifecycle, and catalog projection work.
- [`SUBSCRIBED_DELIVERY_ARCHITECTURE.md`](./SUBSCRIBED_DELIVERY_ARCHITECTURE.md): subscription lifecycle, demand-driven scanning, Telegram delivery/retry behavior.
- [`PRODUCTION_ARCHITECTURE_UBUNTU.md`](./PRODUCTION_ARCHITECTURE_UBUNTU.md): production deployment and runtime architecture on Ubuntu.

## Quality and Testing

- [`QA_TEST_STRATEGY.md`](./QA_TEST_STRATEGY.md): quality gate scope and pass criteria.
- `scripts/qa_certification.py`: executable gate for unit/integration/e2e tests and optional live DB audit.

## Operational Guides

- [`../deploy/README.md`](../deploy/README.md): systemd/nginx deployment workflow and post-deploy checks.
- [`../SECURITY.md`](../SECURITY.md): vulnerability reporting and secret hygiene.
- [`../SUPPORT.md`](../SUPPORT.md): support channels and donation info.

## Documentation Rules

When behavior changes, update docs in the same PR/commit:

1. Update architecture docs (`docs/*.md`) if service behavior, state machine, or data flow changed.
2. Update `README.md` if setup, commands, or product behavior changed.
3. Update `deploy/README.md` if deployment or operational runbook changed.
4. Keep documentation English-only for consistency.
