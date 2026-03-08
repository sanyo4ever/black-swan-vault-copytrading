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

## Production hardening baseline

Recommended baseline for Ubuntu production hosts:

- SSH key-only login (`PasswordAuthentication no`)
- UFW enabled with default deny incoming, allow only `22`, `80`, `443`
- Fail2ban enabled for SSH brute-force protection
- TLS certificates via Let's Encrypt with auto-renew timer enabled
- App services run as non-root user with systemd hardening (`NoNewPrivileges`, `ProtectSystem`, `ProtectHome`)
- Secrets stored outside git (for example `/etc/cryptoinsider/env`) with strict file permissions
- Security updates enabled (`unattended-upgrades`)

Application-level controls:

- Admin endpoints protected by HTTP Basic auth + same-origin CSRF guard
- Security headers enabled (CSP, HSTS, X-Frame-Options, X-Content-Type-Options)
- In-memory IP rate limiting for `/admin`, `/api/traders`, `/subscribe/*`, `/telegram/*`
