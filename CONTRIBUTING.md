# Contributing to Black Swan Vault Copytrading

Thanks for your interest in improving this project.

## Ways to contribute

- Add new discovery connectors or improve existing ones
- Improve quality metrics and scoring models
- Optimize database queries and worker throughput
- Improve Telegram delivery reliability and UX
- Improve docs, onboarding, and observability
- Add tests for edge-cases and regressions

## Development setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run tests

```bash
source .venv/bin/activate
python -m unittest -q tests/test_trader_store.py
python -m unittest -q tests/test_config.py
python -m unittest -q tests/test_telegram_client.py
python -m unittest -q tests/test_subscriber_bot.py
```

## Branch and commit style

- Create a feature branch from `main`
- Keep PRs focused and reasonably small
- Use descriptive commit messages
- Include tests for behavior changes when possible

Recommended commit style:

- `feat: ...`
- `fix: ...`
- `docs: ...`
- `refactor: ...`
- `test: ...`

## Pull Request checklist

- Code builds and tests pass locally
- New behavior is documented
- No secrets/tokens in code, docs, screenshots, or logs
- Migration steps are included if schema/env changed
- PR description explains why the change is needed

## Reporting issues

Use GitHub Issues and include:

- Environment (local/prod, Python version, DB type)
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or stack traces

## Good first issues

Look for labels:

- `good first issue`
- `help wanted`
- `documentation`

## Security issues

Do not open public issues for sensitive vulnerabilities.

Please follow [`SECURITY.md`](./SECURITY.md).
