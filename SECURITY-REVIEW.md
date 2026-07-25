# Security Review

Review date: 2026-07-25

## Scope

The review covered the Python service, Codex collector, OpenCode connector, generic sender, browser dashboard, systemd units, examples, and release exclusions.

The intended security boundary is one trusted machine or private network. Public-internet deployment of the built-in server is outside the supported threat model.

## Review Results

### Remediated

- Removed private host names, user names, home paths, LAN addresses, and fixed regional defaults.
- Excluded databases, backups, environment files, collector configuration, state, logs, and archives from Git.
- Changed the default server bind from all interfaces to loopback.
- Made Hermes optional so public installations do not need private local paths.
- Removed the generic sender's command-line secret option to avoid shell-history and process-list exposure.
- Made collectors prefer `MUDKAT_TRACKER_SECRET` from the process environment.
- Added request content-type enforcement and malformed-length handling.
- Added finite, timezone-aware, bounded timestamp validation.
- Added safe handling for invalid decimal cost values.
- Added persistent replay protection and retained constant-time signature comparison.
- Delayed replay acknowledgement until signed payloads have been validated and committed.
- Rejected fractional, non-finite, and internally inconsistent token counts.
- Rejected destructive backup-retention values and missing source databases.
- Added CSP, clickjacking, referrer, MIME-sniffing, and browser-permission headers.
- Escaped a model-derived dashboard field before inserting it into HTML.
- Restricted newly created database, backup, and collector-state files to owner access where supported.
- Reapplied owner-only database permissions on startup and set a restrictive systemd umask.
- Rejected pricing-source redirects outside HTTPS.
- Added generic systemd paths and loopback-only service defaults.

### Verified

- Event fields are allowlisted before database insertion.
- Prompt, response, tool-output, and unknown fields are discarded.
- Hermes uses SQLite read-only URI mode and `PRAGMA query_only`.
- SQL filter values are parameterized.
- Request and batch sizes are bounded.
- Stable idempotency keys and database constraints prevent duplicate usage events.
- Collector offsets advance only after acknowledgement.
- Browser rendering escapes stored client, provider, model, host, and session values.

## Residual Risks

### Network authentication

Dashboard read endpoints do not require a user login. Access control relies on loopback binding, private-address checks, firewall rules, a VPN, or an authenticated reverse proxy.

### Transport encryption

The built-in server provides HTTP only. Use a TLS reverse proxy or VPN whenever traffic crosses an untrusted network.

### Shared ingestion secret

All collectors currently share one HMAC secret. A compromised collector can submit synthetic usage events until the secret is rotated. Per-client secrets and revocation should be added if the tracker is used by multiple untrusted users or devices.

### Sensitive metadata

Host names, session identifiers, model names, timestamps, token counts, and costs are operational metadata and may still be sensitive. Protect the database and backups accordingly.

### Inline dashboard assets

The single-file dashboard requires inline CSS and JavaScript, so the CSP permits inline styles and scripts. User-controlled values are escaped, but deployments with stricter browser requirements should split assets and use CSP hashes or nonces.

## Validation

- Python compilation completed successfully.
- Dashboard JavaScript parsed successfully.
- The automated suite completed with 16 tests; the only local skip was the expected optional Windows IANA time-zone database check.
- GitHub Actions has read-only repository permissions and uses full commit SHAs for every action.
- The automated suite covers ingestion authentication, replay, skew, payload limits, content rejection, collector recovery, database WAL and backup behavior, pricing preservation, time boundaries, and security headers.
- Current-tree and full-history scans found no private machine identifiers, credentials, runtime databases, collector state, or local configuration.

## Assessment

The package is appropriate for public source distribution and private-network deployment with the documented defaults. It is not appropriate for direct unauthenticated public-internet exposure.
