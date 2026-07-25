# Security Policy

## Deployment Boundary

MudKat Token Tracker is designed for a trusted local machine or private network. The built-in server has no user-account system and does not provide TLS.

- The default bind address is `127.0.0.1`.
- Do not expose port `9130` directly to the public internet.
- Use a VPN or an authenticated TLS reverse proxy for remote access.
- If LAN access is enabled, restrict the port to trusted private subnets with a firewall.

Reverse proxies must enforce authentication themselves. A proxy connecting over loopback is treated as a trusted local client.

## Secrets

- Generate a unique random `MUDKAT_TRACKER_SECRET` with at least 32 characters.
- Store it in an environment file readable only by the service account.
- Do not put secrets in repository files, command-line arguments, screenshots, or issue reports.
- Rotate the shared secret if a collector device or configuration is compromised.

The shared secret authorizes write access to ingestion endpoints. Dashboard read access is controlled by the network boundary.

## Collected Metadata

The tracker stores usage metadata including host names, client names, provider and model identifiers, session IDs, timestamps, token counts, and cost information. Treat the SQLite database and backups as private operational data.

The supplied collectors do not transmit prompts, responses, tool output, OAuth tokens, or provider API keys.

## Built-in Controls

- HMAC-SHA256 request authentication
- Constant-time signature comparison
- Five-minute request-age limit
- Persistent replay protection
- Stable idempotency keys and database deduplication
- One MiB request limit and 1,000-event batch limit
- Allowlisted event fields with length and numeric bounds
- Read-only Hermes database access
- LAN/loopback address checks
- Content Security Policy and defensive browser headers
- systemd filesystem and privilege restrictions

## Reporting a Vulnerability

Use GitHub's private security-advisory feature. Do not include live secrets, databases, collector state, prompts, or production logs in a public issue.

Include the affected version, reproduction steps using synthetic data, impact, and any suggested mitigation.
