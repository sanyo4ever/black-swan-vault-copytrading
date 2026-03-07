# Security Policy

## Supported versions

Security updates are applied to the latest `main` branch.

## Reporting a vulnerability

If you discover a security vulnerability, do not open a public issue.

Report privately to: `sanyo4ever@gmail.com`

Please include:

- Affected component and impact
- Reproduction steps or proof-of-concept
- Suggested remediation if available

## Response targets

- Initial acknowledgment: within 72 hours
- Triage and severity classification: as soon as possible
- Patch timeline: depends on severity and exploitability

## Security best practices for contributors

- Never commit secrets (`.env`, API keys, bot tokens, SSH keys)
- Sanitize logs and screenshots before sharing
- Prefer least-privilege credentials and short-lived tokens
- Validate all external inputs and handle API error paths

## Secrets hygiene

If a secret is accidentally exposed:

1. Revoke/rotate immediately
2. Remove from code and git history if committed
3. Update affected credentials in production
4. Document remediation in the incident notes
