#!/usr/bin/env python3
"""Tail Codex rollout JSONL files and send token events to MudKat Token Tracker."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import platform
import re
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

SAFE_TYPE = re.compile(rb'"type"\s*:\s*"(session_meta|turn_context|event_msg)"')
TOKEN_EVENT = re.compile(rb'"type"\s*:\s*"token_count"')


def read_auth_mode(path: Path) -> str:
    pattern = re.compile(r'"auth_mode"\s*:\s*"([^"]{1,64})"')
    with path.open("r", encoding="utf-8", errors="ignore") as stream:
        text = ""
        while len(text) < 8192:
            text += stream.read(256)
            match = pattern.search(text)
            if match:
                return match.group(1)
            if not text or len(text) % 256:
                break
    return "unknown"


def event_key(host: str, session_id: str, timestamp: str, cumulative: dict, ordinal: int) -> str:
    raw = "|".join((host, session_id, timestamp,
                    json.dumps(cumulative, sort_keys=True, separators=(",", ":")), str(ordinal)))
    return hashlib.sha256(raw.encode()).hexdigest()


def parse_safe_line(line: bytes, context: dict, ordinal: int, host: str, auth_mode: str):
    match = SAFE_TYPE.search(line[:512])
    if not match:
        return None
    kind = match.group(1).decode()
    if kind == "event_msg" and not TOKEN_EVENT.search(line[:2048]):
        return None
    record = json.loads(line)
    payload = record.get("payload") or {}
    if kind == "session_meta":
        context["session_id"] = str(payload.get("id") or "")
        context["parent_session_id"] = payload.get("parent_thread_id")
        context["provider"] = str(payload.get("model_provider") or "openai")
        return None
    if kind == "turn_context":
        context["model"] = str(payload.get("model") or context.get("model") or "unknown")
        context["provider"] = str(payload.get("provider") or context.get("provider") or "openai")
        return None
    info = payload.get("info") or {}
    usage = info.get("last_token_usage")
    cumulative = info.get("total_token_usage") or {}
    if not isinstance(usage, dict) or not context.get("session_id"):
        return None
    raw_input = int(usage.get("input_tokens") or 0)
    cached = int(usage.get("cached_input_tokens") or 0)
    cache_write = int(usage.get("cache_write_input_tokens") or 0)
    output = int(usage.get("output_tokens") or 0)
    timestamp = str(record.get("timestamp") or "")
    billing_mode = "subscription_included" if auth_mode == "chatgpt" else auth_mode
    provider = "openai-codex" if auth_mode == "chatgpt" else context.get("provider", "openai")
    return {
        "idempotency_key": "codex:" + event_key(
            host, context["session_id"], timestamp, cumulative, ordinal),
        "client": "codex",
        "host": host,
        "provider": provider,
        "model": context.get("model") or "unknown",
        "session_id": context["session_id"],
        "parent_session_id": context.get("parent_session_id"),
        "event_time": timestamp,
        "input_tokens": max(0, raw_input - cached - cache_write),
        "cached_input_tokens": cached,
        "cache_write_tokens": cache_write,
        "output_tokens": output,
        "reasoning_output_tokens": int(usage.get("reasoning_output_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or raw_input + output),
        "api_call_count": 1,
        "billing_mode": billing_mode,
        "attribution": "exact",
        "actual_cost_usd": 0 if auth_mode == "chatgpt" else None,
    }


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"files": {}}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)
    path.chmod(0o600)


def scan_file(path: Path, saved: dict, host: str, auth_mode: str,
              max_events: int = 500) -> tuple[list[dict], dict]:
    offset = int(saved.get("offset", 0))
    context = dict(saved.get("context") or {})
    events, ordinal, committed_offset = [], int(saved.get("ordinal", 0)), offset
    try:
        size = path.stat().st_size
    except OSError:
        return [], saved
    if size < offset:
        offset, ordinal, context = 0, 0, {}
    with path.open("rb") as stream:
        stream.seek(offset)
        while len(events) < max_events:
            line_start = stream.tell()
            line = stream.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                stream.seek(line_start)
                break
            ordinal += 1
            try:
                event = parse_safe_line(line, context, ordinal, host, auth_mode)
            except (ValueError, TypeError, json.JSONDecodeError):
                event = None
            committed_offset = stream.tell()
            if event:
                events.append(event)
    return events, {"offset": committed_offset, "ordinal": ordinal, "context": context}


def post_payload(url: str, secret: bytes, path: str, payload: dict) -> dict | None:
    body = json.dumps(payload, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(secret, timestamp.encode() + b"\n" + body, hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        url.rstrip("/") + path, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-MudKat-Timestamp": timestamp,
            "X-MudKat-Signature": signature,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response) if response.status == 200 else None
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            try:
                return json.load(exc)
            except Exception:
                pass
        return None
    except (OSError, ValueError):
        return None


def post_batch(url: str, secret: bytes, events: list[dict]) -> bool:
    result = post_payload(url, secret, "/api/v1/ingest/codex", {"events": events})
    return bool(result and result.get("acknowledged") is True)


def codex_running() -> bool:
    if os.name != "nt":
        return False
    try:
        result = subprocess.run(
            ["tasklist.exe", "/FI", "IMAGENAME eq codex.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=5, check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return '"codex.exe"' in result.stdout.lower()
    except OSError:
        return False


def post_status(config: dict) -> bool:
    secret = os.environ.get("MUDKAT_TRACKER_SECRET") or config.get("secret", "")
    result = post_payload(
        config["server"], secret.encode(), "/api/v1/status/codex",
        {"client": "codex", "host": config.get("host") or platform.node(),
         "app_running": codex_running()},
    )
    return bool(result and result.get("acknowledged") is True)


def run_once(config: dict, state: dict) -> tuple[int, dict]:
    sessions = Path(os.path.expandvars(config["sessions_path"]))
    auth_path = Path(os.path.expandvars(config["auth_path"]))
    auth_mode = read_auth_mode(auth_path) if auth_path.exists() else "unknown"
    host = config.get("host") or platform.node()
    secret = os.environ.get("MUDKAT_TRACKER_SECRET") or config.get("secret", "")
    sent = 0
    for path in sorted(sessions.glob("*/*/*/rollout-*.jsonl")):
        key = str(path)
        saved = state["files"].get(key, {})
        events, candidate = scan_file(path, saved, host, auth_mode)
        if not events:
            if candidate != saved:
                state["files"][key] = candidate
            continue
        if post_batch(config["server"], secret.encode(), events):
            state["files"][key] = candidate
            sent += len(events)
        else:
            break
    return sent, state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    secret = os.environ.get("MUDKAT_TRACKER_SECRET") or config.get("secret", "")
    if len(secret) < 32:
        raise SystemExit("Set MUDKAT_TRACKER_SECRET to at least 32 characters")
    state_path = Path(os.path.expandvars(config.get(
        "state_path", r"%USERPROFILE%\.mudkat-token-tracker\collector-state.json")))
    while True:
        state = load_state(state_path)
        sent, state = run_once(config, state)
        post_status(config)
        save_state(state_path, state)
        if args.once:
            print(json.dumps({"sent": sent, "files": len(state["files"])}))
            return
        time.sleep(10)


if __name__ == "__main__":
    main()
