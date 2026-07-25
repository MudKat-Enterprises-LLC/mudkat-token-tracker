# MudKat Token Tracker

A self-hosted, privacy-first telemetry dashboard for tracking token usage and estimated costs across AI coding clients, agents, and model harnesses.

MudKat Token Tracker passively collects usage metadata from Codex Desktop, Hermes, OpenCode, and compatible custom integrations. It does not proxy model traffic and does not collect prompts, responses, tool output, OAuth tokens, or provider API keys.

## Features

- Real-time token tracking with five-second dashboard refreshes
- Input, cache-read, cache-write, output, and reasoning-token breakdowns
- Daily, weekly, monthly, yearly, rolling-30-day, and lifetime totals
- Client, host, provider, model, session, and subagent attribution
- Actual billed cost separated from API-equivalent estimates
- Versioned pricing that does not rewrite historical event costs
- Automatic browser time-zone detection and international time zones
- HMAC-SHA256 ingestion, replay protection, event deduplication, and payload limits
- Responsive daily dashboard for full-screen or narrow monitor layouts
- Python standard library, SQLite WAL, and vanilla HTML/CSS/JavaScript

## Architecture

```text
Codex collector ─┐
Hermes database ─┼─> signed/readonly ingestion ─> Python service ─> SQLite WAL
OpenCode plugin ─┤                                  │
Generic clients ─┘                                  └─> LAN dashboard + JSON API
```

- `tracker.py`: event store, read-only Hermes import, pricing, backup, HTTP API, and dashboard server
- `collector.py`: Windows Codex rollout metadata collector
- `opencode-plugin.js`: OpenCode completed-message usage connector
- `generic-ingest.py`: signed sender for other clients
- `dashboard.html`: responsive five-second-polling interface
- `deploy/`: hardened user-level systemd units and timers

## Requirements

- Python 3.11 or newer
- Linux for the supplied systemd units
- Windows Python for the Codex Desktop collector
- No required third-party Python packages on Linux

Linux supplies the IANA time-zone database used for DST-aware reporting. Windows servers can use UTC without another package or install the optional `tzdata` package for full IANA time-zone support.

## Quick Start

Generate a unique ingestion secret. Do not commit it:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Set the secret and initialize pricing:

```bash
export MUDKAT_TRACKER_SECRET="paste-your-generated-secret"
python3 tracker.py refresh-pricing
python3 tracker.py serve
```

The secure default listens only on `http://127.0.0.1:9130`.

For trusted LAN access, bind to `0.0.0.0` and restrict port `9130` to your private subnet with the host firewall:

```bash
python3 tracker.py serve --host 0.0.0.0 --port 9130
```

Do not expose the built-in HTTP server directly to the public internet. Use a VPN or an authenticated TLS reverse proxy if remote access is required.

## Install with an AI Agent

If your coding agent can access a terminal and GitHub, give it this repository URL and the instruction below:

```text
Install MudKat Token Tracker from:
https://github.com/MudKat/mudkat-token-tracker

Follow the repository README and SECURITY.md. First inspect the machine and
explain the deployment plan. Then:

1. Run the repository tests before installation.
2. Install the central tracker on Linux as a user service using the supplied
   systemd units. Do not overwrite an existing installation or configuration.
3. Generate the HMAC secret locally, store it outside the repository with
   owner-only permissions, and never print it in chat, logs, commits, or issue
   reports.
4. Keep the tracker bound to 127.0.0.1 unless I explicitly approve LAN access.
   Ask before changing firewall rules, adding a reverse proxy, using sudo, or
   exposing any network port.
5. Detect supported clients already present on my machines and install only
   their metadata collectors: Codex Desktop, Hermes, OpenCode, or the generic
   sender. Open Hermes databases read-only.
6. Do not read, copy, store, or transmit prompts, responses, tool output,
   OAuth tokens, provider API keys, or unrelated agent configuration. Do not
   reroute model traffic or modify provider settings.
7. Verify the service, timers, /healthz response, collector heartbeat, and one
   synthetic or newly completed usage event. Report every file, service, task,
   firewall rule, and endpoint changed.

Stop and ask me before any destructive action or security-boundary change.
```

For a shorter request, tell the agent:

```text
Install https://github.com/MudKat/mudkat-token-tracker by following its
"Install with an AI Agent" instructions. Keep the deployment private and do
not modify model-provider credentials or traffic routing.
```

The agent still needs terminal access to the target machines and permission to create the documented service, configuration, and collector files. Repository access alone does not grant machine access.

## Configuration

Copy `.env.example` outside the repository and restrict it to the service account:

```bash
install -m 600 .env.example ~/.config/mudkat-token-tracker.env
```

Required:

```text
MUDKAT_TRACKER_SECRET=<random value containing at least 32 characters>
```

Optional:

```text
MUDKAT_TIMEZONE=UTC
MUDKAT_HERMES_DB=/path/to/hermes/state.db
MUDKAT_MANUAL_PRICES=/path/to/manual-prices.json
MUDKAT_HERMES_PRICING_SOURCE=/path/to/hermes/usage_pricing.py
```

Hermes is optional. When configured, its SQLite database is opened in read-only and query-only mode.

## Codex Desktop

1. Copy `collector.example.json` outside the repository.
2. Set `MUDKAT_TRACKER_SECRET` in the collector process environment.
3. Run:

```powershell
pyw.exe collector.py --config C:\path\to\collector.json
```

The collector reads only top-level session metadata, model context, authentication mode, and token-count events. File offsets advance only after server acknowledgement.

## OpenCode

Place `opencode-plugin.js` in the OpenCode plugins directory and set:

```text
MUDKAT_TRACKER_URL=http://tracker-host:9130
MUDKAT_TRACKER_SECRET=<same ingestion secret>
```

## Generic Ingestion

Set `MUDKAT_TRACKER_URL` and `MUDKAT_TRACKER_SECRET`, then send an allowlisted JSON event:

```bash
python3 generic-ingest.py event.json
```

Write endpoints require:

```text
X-MudKat-Timestamp: Unix seconds
X-MudKat-Signature: hex(HMAC-SHA256(secret, timestamp + "\n" + raw_body))
```

Requests older than five minutes, replayed signatures, bodies over 1 MiB, and batches over 1,000 events are rejected.

## API

Read:

- `GET /api/v1/summary`
- `GET /api/v1/sessions`
- `GET /api/v1/pricing`
- `GET /healthz`

Write:

- `POST /api/v1/ingest/codex`
- `POST /api/v1/status/codex`
- `POST /api/v1/ingest/opencode`
- `POST /api/v1/ingest/usage`

## Pricing

OpenRouter rates refresh from its machine-readable model API. Ollama/local model rates are recorded as zero. Other official pricing pages are monitored for changes, but rates are not guessed when a stable machine-readable source is unavailable.

Manual rates use `manual-prices.example.json` as the schema. Rates are USD per million tokens.

## Deployment

The systemd units use `%h` and contain no machine-specific usernames or paths. Install the repository at:

```text
~/apps/mudkat-token-tracker
```

Then copy the units to `~/.config/systemd/user/`, reload systemd, and enable the tracker, pricing, and backup timers.

```bash
install -d -m 700 ~/.config/systemd/user ~/.local/share/mudkat-token-tracker/backups
cp deploy/*.service deploy/*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now mudkat-token-tracker.service mudkat-token-pricing.timer mudkat-token-backup.timer
```

The supplied service binds to loopback by default. Review the network guidance above before changing it.

## Security

Read [SECURITY.md](SECURITY.md) before deployment and see the completed [security review](SECURITY-REVIEW.md). Runtime databases, collector state, environment files, backups, and local configuration are excluded by `.gitignore`.

## Tests

```bash
python3 -m unittest -v
```

## License

MIT
