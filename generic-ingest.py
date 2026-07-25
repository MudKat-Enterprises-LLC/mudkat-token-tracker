#!/usr/bin/env python3
"""Send one allowlisted usage event to MudKat Token Tracker."""

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("event", help="Path to a JSON event or {'events': [...]} batch")
    parser.add_argument("--server", default=os.environ.get("MUDKAT_TRACKER_URL"))
    args = parser.parse_args()
    secret = os.environ.get("MUDKAT_TRACKER_SECRET")
    if not args.server or not secret:
        raise SystemExit("Set MUDKAT_TRACKER_URL and MUDKAT_TRACKER_SECRET")
    if len(secret) < 32:
        raise SystemExit("MUDKAT_TRACKER_SECRET must contain at least 32 characters")

    value = json.loads(Path(args.event).read_text(encoding="utf-8"))
    payload = value if isinstance(value, dict) and "events" in value else {"events": [value]}
    body = json.dumps(payload, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode(), timestamp.encode() + b"\n" + body, hashlib.sha256
    ).hexdigest()
    request = urllib.request.Request(
        args.server.rstrip("/") + "/api/v1/ingest/usage",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-MudKat-Timestamp": timestamp,
            "X-MudKat-Signature": signature,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            print(response.read().decode())
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"Tracker rejected event: {exc.code} {exc.read().decode()}") from exc


if __name__ == "__main__":
    main()
