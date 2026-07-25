#!/usr/bin/env python3
"""MudKat Token Tracker: stdlib-only usage store, Hermes importer, API, and UI."""

from __future__ import annotations

import argparse
import ast
import hashlib
import hmac
import ipaddress
import json
import math
import os
import platform
import re
import sqlite3
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal, InvalidOperation
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

VERSION = "1.0.0"

try:
    TZ = ZoneInfo(os.environ.get("MUDKAT_TIMEZONE", "UTC"))
except ZoneInfoNotFoundError:
    TZ = timezone.utc
TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
HERMES_COUNTERS = (
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "api_call_count",
    "estimated_cost_usd",
    "actual_cost_usd",
)
MAX_BATCH_BYTES = 1_048_576
MAX_BATCH_EVENTS = 1_000
ALLOWED_ATTRIBUTION = {"exact", "estimated", "session_aggregate"}
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS events (
  idempotency_key TEXT PRIMARY KEY,
  client TEXT NOT NULL,
  host TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  session_id TEXT NOT NULL,
  parent_session_id TEXT,
  event_time REAL NOT NULL,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  cached_input_tokens INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  api_call_count INTEGER NOT NULL DEFAULT 0,
  billing_mode TEXT NOT NULL DEFAULT '',
  attribution TEXT NOT NULL,
  actual_cost_usd REAL,
  estimated_cost_usd REAL,
  currency TEXT NOT NULL DEFAULT 'USD',
  pricing_version TEXT,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_time ON events(event_time);
CREATE INDEX IF NOT EXISTS events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS events_dimensions ON events(client, provider, model);
CREATE TABLE IF NOT EXISTS hermes_snapshots (
  source_key TEXT PRIMARY KEY,
  counters_json TEXT NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS prices (
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  category TEXT NOT NULL,
  effective_at REAL NOT NULL,
  price_per_million TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  retrieved_at REAL NOT NULL,
  version TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(provider, model, category, effective_at)
);
CREATE INDEX IF NOT EXISTS prices_lookup ON prices(provider, model, active, effective_at);
CREATE TABLE IF NOT EXISTS pricing_sources (
  provider TEXT PRIMARY KEY,
  source_url TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  last_hash TEXT,
  last_success REAL,
  last_checked REAL,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS request_replays (
  signature TEXT PRIMARY KEY,
  seen_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""
SOURCE_URLS = {
    "openai": ("https://developers.openai.com/api/docs/pricing", "official_page"),
    "anthropic": ("https://docs.anthropic.com/en/docs/about-claude/pricing", "official_page"),
    "google": ("https://ai.google.dev/gemini-api/docs/pricing", "official_page"),
    "xai": ("https://docs.x.ai/docs/models", "official_page"),
    "openrouter": ("https://openrouter.ai/api/v1/models", "machine_readable"),
    "huggingface": ("https://huggingface.co/docs/inference-providers/pricing", "official_page"),
    "deepseek": ("https://api-docs.deepseek.com/quick_start/pricing", "official_page"),
    "kimi": ("https://platform.moonshot.ai/docs/pricing/chat", "official_page"),
    "alibaba": ("https://www.alibabacloud.com/help/en/model-studio/model-pricing", "official_page"),
    "ollama": ("local://zero-cost", "local"),
}


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    path.chmod(0o600)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    return db


def now_ts() -> float:
    return time.time()


def parse_time(value) -> float:
    if isinstance(value, (int, float)):
        timestamp = float(value)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("event_time must include a timezone")
        timestamp = parsed.timestamp()
    else:
        raise ValueError("event_time must be an ISO timestamp or Unix timestamp")
    if not math.isfinite(timestamp) or not 0 <= timestamp <= 4_102_444_800:
        raise ValueError("event_time is outside the accepted range")
    return timestamp


def clean_text(value, name: str, maximum: int = 256, required: bool = False) -> str:
    text = "" if value is None else str(value).strip()
    if required and not text:
        raise ValueError(f"{name} is required")
    if len(text) > maximum:
        raise ValueError(f"{name} is too long")
    return text


def clean_count(value, name: str) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        raise ValueError(f"{name} must be an integer")
    number = int(value)
    if number < 0 or number > 10**15:
        raise ValueError(f"{name} is outside the accepted range")
    return number


def normalize_event(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("each event must be an object")
    event = {
        "idempotency_key": clean_text(raw.get("idempotency_key"), "idempotency_key", 128, True),
        "client": clean_text(raw.get("client"), "client", 32, True).lower(),
        "host": clean_text(raw.get("host"), "host", 128, True),
        "provider": clean_text(raw.get("provider") or "unknown", "provider", 128).lower(),
        "model": clean_text(raw.get("model") or "unknown", "model", 256),
        "session_id": clean_text(raw.get("session_id"), "session_id", 256, True),
        "parent_session_id": clean_text(raw.get("parent_session_id"), "parent_session_id", 256) or None,
        "event_time": parse_time(raw.get("event_time")),
        "billing_mode": clean_text(raw.get("billing_mode"), "billing_mode", 64),
        "attribution": clean_text(raw.get("attribution") or "exact", "attribution", 32),
        "currency": clean_text(raw.get("currency") or "USD", "currency", 8).upper(),
        "pricing_version": clean_text(raw.get("pricing_version"), "pricing_version", 128) or None,
        "actual_cost_usd": None,
        "estimated_cost_usd": None,
    }
    if event["attribution"] not in ALLOWED_ATTRIBUTION:
        raise ValueError("invalid attribution")
    for field in TOKEN_FIELDS:
        event[field] = clean_count(raw.get(field), field)
    event["api_call_count"] = clean_count(raw.get("api_call_count"), "api_call_count")
    calculated_total = (
        event["input_tokens"]
        + event["cached_input_tokens"]
        + event["cache_write_tokens"]
        + event["output_tokens"]
    )
    if not event["total_tokens"]:
        event["total_tokens"] = calculated_total
    elif event["total_tokens"] < calculated_total:
        raise ValueError("total_tokens cannot be less than the token category total")
    for field in ("actual_cost_usd", "estimated_cost_usd"):
        value = raw.get(field)
        if value is not None:
            try:
                number = Decimal(str(value))
            except InvalidOperation as exc:
                raise ValueError(f"invalid {field}") from exc
            if not number.is_finite() or number < 0 or number > Decimal("1000000000"):
                raise ValueError(f"invalid {field}")
            event[field] = float(number)
    return event


def pricing_provider(provider: str, model: str) -> str:
    provider = provider.lower()
    if provider == "openai-codex":
        return "openai"
    if provider in {"kimi-coding", "kimi-for-coding", "moonshot"}:
        return "kimi"
    if provider == "auto":
        if model.startswith(("gpt-", "o1", "o3", "o4")):
            return "openai"
        if model.startswith("kimi"):
            return "kimi"
        if model.startswith("deepseek"):
            return "deepseek"
        if model.startswith(("qwen", "qwq")):
            return "alibaba"
    return provider


def active_rates(db: sqlite3.Connection, provider: str, model: str) -> tuple[dict[str, Decimal], str | None]:
    provider = pricing_provider(provider, model)
    rows = db.execute(
        """SELECT category, price_per_million, version, model
           FROM prices WHERE provider=? AND active=1 AND model IN (?, '*')
           ORDER BY CASE WHEN model=? THEN 0 ELSE 1 END, effective_at DESC""",
        (provider.lower(), model, model),
    ).fetchall()
    rates: dict[str, Decimal] = {}
    version = None
    for row in rows:
        if row["category"] not in rates:
            rates[row["category"]] = Decimal(row["price_per_million"])
            version = version or row["version"]
    return rates, version


def price_event(db: sqlite3.Connection, event: dict) -> None:
    if event["estimated_cost_usd"] is not None:
        return
    rates, version = active_rates(db, event["provider"], event["model"])
    required = {k for k, f in (
        ("input", "input_tokens"),
        ("cache_read", "cached_input_tokens"),
        ("cache_write", "cache_write_tokens"),
        ("output", "output_tokens"),
    ) if event[f]}
    if not required or not required.issubset(rates):
        return
    cost = sum(Decimal(event[field]) * rates[category] for category, field in (
        ("input", "input_tokens"),
        ("cache_read", "cached_input_tokens"),
        ("cache_write", "cache_write_tokens"),
        ("output", "output_tokens"),
    ) if event[field]) / Decimal(1_000_000)
    event["estimated_cost_usd"] = float(cost)
    event["pricing_version"] = version


def insert_events(db: sqlite3.Connection, raw_events: list[dict]) -> tuple[int, int]:
    accepted = duplicates = 0
    for raw in raw_events:
        event = normalize_event(raw)
        price_event(db, event)
        values = [event[k] for k in (
            "idempotency_key", "client", "host", "provider", "model", "session_id",
            "parent_session_id", "event_time", *TOKEN_FIELDS, "api_call_count",
            "billing_mode", "attribution", "actual_cost_usd", "estimated_cost_usd",
            "currency", "pricing_version",
        )] + [now_ts()]
        try:
            db.execute(
                """INSERT INTO events (
                   idempotency_key,client,host,provider,model,session_id,parent_session_id,
                   event_time,input_tokens,cached_input_tokens,cache_write_tokens,
                   output_tokens,reasoning_output_tokens,total_tokens,api_call_count,
                   billing_mode,attribution,actual_cost_usd,estimated_cost_usd,currency,
                   pricing_version,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
            accepted += 1
        except sqlite3.IntegrityError:
            duplicates += 1
    db.commit()
    return accepted, duplicates


def hermes_rows(source: sqlite3.Connection) -> list[dict]:
    columns = {r[1] for r in source.execute("PRAGMA table_info(session_model_usage)")}
    required = {"session_id", "model", "input_tokens", "output_tokens"}
    rows: list[dict] = []
    if required.issubset(columns):
        sql = """SELECT u.*, s.parent_session_id, s.started_at, s.ended_at, s.source
                 FROM session_model_usage u JOIN sessions s ON s.id=u.session_id"""
        for row in source.execute(sql):
            item = dict(row)
            item["_source_key"] = "model:" + "|".join(str(item.get(k) or "") for k in (
                "session_id", "model", "billing_provider", "billing_base_url",
                "billing_mode", "task",
            ))
            rows.append(item)
        modeled = {r["session_id"] for r in rows}
    else:
        modeled = set()
    for row in source.execute("SELECT * FROM sessions"):
        item = dict(row)
        if item["id"] in modeled:
            continue
        item.update({
            "session_id": item["id"],
            "first_seen": item.get("started_at"),
            "last_seen": item.get("ended_at") or item.get("started_at"),
            "task": "",
        })
        item["_source_key"] = "session:" + item["id"]
        rows.append(item)
    return rows


def import_hermes(tracker: sqlite3.Connection, hermes_path: str, host: str | None = None) -> dict:
    host = host or platform.node() or "localhost"
    source = sqlite3.connect(f"file:{hermes_path}?mode=ro", uri=True, timeout=5)
    source.row_factory = sqlite3.Row
    source.execute("PRAGMA query_only=ON")
    created = duplicates = resets = 0
    try:
        source.execute("BEGIN")
        rows = hermes_rows(source)
    finally:
        source.close()
    for row in rows:
        key = row["_source_key"]
        current = {name: row.get(name) or 0 for name in HERMES_COUNTERS}
        snap = tracker.execute(
            "SELECT counters_json FROM hermes_snapshots WHERE source_key=?", (key,)
        ).fetchone()
        previous = json.loads(snap[0]) if snap else None
        if previous is None:
            delta = current
            attribution = "session_aggregate"
            event_time = row.get("last_seen") or row.get("ended_at") or row.get("started_at") or now_ts()
        elif any(float(current[k]) < float(previous.get(k, 0)) for k in HERMES_COUNTERS[:6]):
            resets += 1
            tracker.execute(
                "INSERT INTO meta(key,value) VALUES('hermes_last_warning',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (f"counter reset rebased for {key} at {datetime.now(TZ).isoformat()}",),
            )
            delta = {k: 0 for k in HERMES_COUNTERS}
            attribution = "exact"
            event_time = now_ts()
        else:
            delta = {
                k: max(0, float(current[k]) - float(previous.get(k, 0)))
                for k in HERMES_COUNTERS
            }
            attribution = "exact"
            event_time = row.get("last_seen") or now_ts()
        tracker.execute(
            """INSERT INTO hermes_snapshots(source_key,counters_json,updated_at) VALUES(?,?,?)
               ON CONFLICT(source_key) DO UPDATE SET counters_json=excluded.counters_json,
               updated_at=excluded.updated_at""",
            (key, json.dumps(current, sort_keys=True), now_ts()),
        )
        if not any(delta[k] for k in HERMES_COUNTERS):
            continue
        digest = hashlib.sha256(
            (key + "|" + json.dumps(current, sort_keys=True)).encode()
        ).hexdigest()
        actual = float(delta["actual_cost_usd"]) if delta["actual_cost_usd"] else None
        estimated = float(delta["estimated_cost_usd"]) if delta["estimated_cost_usd"] else None
        billing_mode = str(row.get("billing_mode") or "")
        if billing_mode == "subscription_included":
            actual = 0.0
        provider = row.get("billing_provider") or "unknown"
        base_url = str(row.get("billing_base_url") or "")
        if provider == "custom" and (
            "localhost" in base_url or "127.0.0.1" in base_url or base_url.startswith("ollama://")
        ):
            provider = "ollama"
        elif provider == "auto":
            if "openrouter.ai" in base_url:
                provider = "openrouter"
            elif "deepseek.com" in base_url:
                provider = "deepseek"
            elif "moonshot.ai" in base_url:
                provider = "kimi"
            elif "chatgpt.com" in base_url:
                provider = "openai-codex"
        accepted, dupe = insert_events(tracker, [{
            "idempotency_key": "hermes:" + digest,
            "client": "hermes",
            "host": host,
            "provider": provider,
            "model": row.get("model") or "unknown",
            "session_id": row.get("session_id"),
            "parent_session_id": row.get("parent_session_id"),
            "event_time": event_time,
            "input_tokens": int(delta["input_tokens"]),
            "cached_input_tokens": int(delta["cache_read_tokens"]),
            "cache_write_tokens": int(delta["cache_write_tokens"]),
            "output_tokens": int(delta["output_tokens"]),
            "reasoning_output_tokens": int(delta["reasoning_tokens"]),
            "api_call_count": int(delta["api_call_count"]),
            "billing_mode": billing_mode,
            "attribution": attribution,
            "actual_cost_usd": actual,
            "estimated_cost_usd": estimated,
        }])
        created += accepted
        duplicates += dupe
    tracker.execute(
        "INSERT INTO meta(key,value) VALUES('hermes_last_sync',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(now_ts()),)
    )
    tracker.commit()
    return {"created": created, "duplicates": duplicates, "resets": resets, "rows": len(rows)}


def set_price(
    db: sqlite3.Connection, provider: str, model: str, category: str, rate,
    source_url: str, source_hash: str, retrieved_at: float, version: str,
    effective_at: float | None = None,
) -> None:
    if category not in {"input", "cache_read", "cache_write", "output"}:
        raise ValueError("invalid price category")
    number = Decimal(str(rate))
    if not number.is_finite() or number < 0:
        raise ValueError("invalid price")
    effective_at = effective_at or retrieved_at
    previous = db.execute(
        """SELECT price_per_million FROM prices
           WHERE provider=? AND model=? AND category=? AND active=1
           ORDER BY effective_at DESC LIMIT 1""",
        (provider.lower(), model, category),
    ).fetchone()
    if previous and Decimal(previous[0]) == number:
        return
    db.execute(
        "UPDATE prices SET active=0 WHERE provider=? AND model=? AND category=? AND active=1",
        (provider.lower(), model, category),
    )
    db.execute(
        """INSERT OR IGNORE INTO prices(provider,model,category,effective_at,
           price_per_million,source_url,source_hash,retrieved_at,version,active)
           VALUES(?,?,?,?,?,?,?,?,?,1)""",
        (provider.lower(), model, category, effective_at, str(number), source_url,
         source_hash, retrieved_at, version),
    )


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={
        "User-Agent": f"MudKat-Token-Tracker/{VERSION}",
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.1",
    })
    with urllib.request.urlopen(req, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        if urlparse(response.geturl()).scheme != "https":
            raise RuntimeError("pricing source redirected outside HTTPS")
        return response.read(10_000_001)


def _literal(node):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Decimal":
        return _literal(node.args[0])
    return None


def import_hermes_pricing(db: sqlite3.Connection, source_path: str) -> int:
    """Reuse Hermes's audited official-doc snapshots without importing its runtime."""
    path = Path(source_path)
    if not path.exists():
        return 0
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    tree = ast.parse(raw.decode())
    mapping = next(
        (n.value for n in ast.walk(tree)
         if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)
         and n.target.id == "_OFFICIAL_DOCS_PRICING"),
        None,
    )
    if not isinstance(mapping, ast.Dict):
        raise ValueError("Hermes official pricing table not found")
    when, added = path.stat().st_mtime, 0
    fields = {
        "input_cost_per_million": "input",
        "output_cost_per_million": "output",
        "cache_read_cost_per_million": "cache_read",
        "cache_write_cost_per_million": "cache_write",
    }
    for key, value in zip(mapping.keys, mapping.values):
        if not isinstance(key, ast.Tuple) or len(key.elts) != 2 or not isinstance(value, ast.Call):
            continue
        provider, model = (_literal(n) for n in key.elts)
        keywords = {kw.arg: _literal(kw.value) for kw in value.keywords if kw.arg}
        source_url = keywords.get("source_url")
        version = keywords.get("pricing_version")
        if not all(isinstance(v, str) and v for v in (provider, model, source_url, version)):
            continue
        for field, category in fields.items():
            rate = keywords.get(field)
            if rate is not None:
                set_price(db, provider, model, category, rate, source_url, digest,
                          when, version, when)
                added += 1
    return added


def refresh_pricing(
    db: sqlite3.Connection, manual_path: str | None = None,
    hermes_pricing_source: str | None = None,
) -> dict:
    checked = updated = errors = 0
    when = now_ts()
    for provider, (url, kind) in SOURCE_URLS.items():
        checked += 1
        try:
            if kind == "local":
                body = b"local models are zero-cost"
            else:
                body = fetch(url)
                if len(body) > 10_000_000:
                    raise ValueError("pricing response too large")
            digest = hashlib.sha256(body).hexdigest()
            version = datetime.fromtimestamp(when, TZ).strftime("%Y%m%d") + "-" + digest[:12]
            before = db.total_changes
            if provider == "openrouter":
                payload = json.loads(body)
                for model in payload.get("data", []):
                    name = clean_text(model.get("id"), "model", 256, True)
                    pricing = model.get("pricing") or {}
                    for source_name, category in (
                        ("prompt", "input"), ("completion", "output"),
                        ("input_cache_read", "cache_read"),
                        ("input_cache_write", "cache_write"),
                    ):
                        value = pricing.get(source_name)
                        if value not in (None, ""):
                            try:
                                rate = Decimal(str(value)) * Decimal(1_000_000)
                            except InvalidOperation:
                                continue
                            if rate >= 0:
                                set_price(db, provider, name, category, rate,
                                          url, digest, when, version)
            elif provider == "ollama":
                for category in ("input", "cache_read", "cache_write", "output"):
                    set_price(db, provider, "*", category, 0, url, digest, when, version)
            # Official HTML sources are change monitors. Rates are accepted only
            # through a validated manual override until a stable machine format exists.
            changed = db.total_changes > before
            updated += int(changed)
            db.execute(
                """INSERT INTO pricing_sources(provider,source_url,source_kind,last_hash,
                   last_success,last_checked,last_error) VALUES(?,?,?,?,?,?,NULL)
                   ON CONFLICT(provider) DO UPDATE SET source_url=excluded.source_url,
                   source_kind=excluded.source_kind,last_hash=excluded.last_hash,
                   last_success=excluded.last_success,last_checked=excluded.last_checked,
                   last_error=NULL""",
                (provider, url, kind, digest, when, when),
            )
        except Exception as exc:
            errors += 1
            db.execute(
                """INSERT INTO pricing_sources(provider,source_url,source_kind,last_checked,last_error)
                   VALUES(?,?,?,?,?) ON CONFLICT(provider) DO UPDATE SET
                   last_checked=excluded.last_checked,last_error=excluded.last_error""",
                (provider, url, kind, when, str(exc)[:500]),
            )
    if hermes_pricing_source:
        try:
            updated += int(import_hermes_pricing(db, hermes_pricing_source) > 0)
        except Exception as exc:
            errors += 1
            db.execute(
                """INSERT INTO pricing_sources(provider,source_url,source_kind,last_checked,last_error)
                   VALUES('hermes-snapshot',?,'official_snapshot',?,?)
                   ON CONFLICT(provider) DO UPDATE SET last_checked=excluded.last_checked,
                   last_error=excluded.last_error""",
                (hermes_pricing_source, when, str(exc)[:500]),
            )
    if manual_path and Path(manual_path).exists():
        raw = Path(manual_path).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        payload = json.loads(raw)
        version = "manual-" + digest[:12]
        for item in payload:
            for category, rate in (item.get("rates") or {}).items():
                set_price(
                    db, item["provider"], item["model"], category, rate,
                    item["source_url"], digest, when, version,
                    parse_time(item["effective_at"]) if item.get("effective_at") else when,
                )
        updated += 1
    db.execute(
        "INSERT INTO meta(key,value) VALUES('pricing_last_refresh',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(when),)
    )
    db.commit()
    return {"checked": checked, "updated_sources": updated, "errors": errors}


def reporting_timezone(query: dict) -> tzinfo:
    name = query.get("timezone", [str(TZ)])[0]
    if len(name) > 64 or not re.fullmatch(r"[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*", name):
        raise ValueError("invalid timezone")
    if name == str(TZ):
        return TZ
    if name == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("timezone data unavailable") from exc


def boundaries(now: datetime | None = None, zone: tzinfo = TZ) -> dict[str, float]:
    now = now.astimezone(zone) if now else datetime.now(zone)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        "today": today.timestamp(),
        "week": (today - timedelta(days=today.weekday())).timestamp(),
        "month": today.replace(day=1).timestamp(),
        "rolling30": (now - timedelta(days=30)).timestamp(),
        "year": today.replace(month=1, day=1).timestamp(),
        "lifetime": 0,
    }


def where_filters(query: dict, start: float | None = None) -> tuple[str, list]:
    clauses, args = [], []
    if start is not None:
        clauses.append("event_time>=?")
        args.append(start)
    for key, column in (
        ("client", "client"), ("provider", "provider"), ("model", "model"),
        ("session", "session_id"), ("attribution", "attribution"),
    ):
        value = query.get(key, [""])[0]
        if value:
            clauses.append(f"{column}=?")
            args.append(value)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", args


def totals(db: sqlite3.Connection, query: dict, start: float) -> dict:
    where, args = where_filters(query, start)
    row = db.execute(
        f"""SELECT COUNT(*) events, COALESCE(SUM(input_tokens),0) input_tokens,
            COALESCE(SUM(cached_input_tokens),0) cached_input_tokens,
            COALESCE(SUM(cache_write_tokens),0) cache_write_tokens,
            COALESCE(SUM(output_tokens),0) output_tokens,
            COALESCE(SUM(reasoning_output_tokens),0) reasoning_output_tokens,
            COALESCE(SUM(total_tokens),0) total_tokens,
            COALESCE(SUM(api_call_count),0) api_call_count,
            SUM(actual_cost_usd) actual_cost_usd,
            SUM(estimated_cost_usd) estimated_cost_usd
            FROM events{where}""", args
    ).fetchone()
    return dict(row)


def summary(db: sqlite3.Connection, query: dict) -> dict:
    zone = reporting_timezone(query)
    ranges = boundaries(zone=zone)
    selected = query.get("range", [""])[0]
    if selected and selected not in ranges:
        raise ValueError("invalid range")
    cards = {name: totals(db, query, start) for name, start in ranges.items()}
    start = ranges.get(selected or "rolling30", ranges["rolling30"])
    where, args = where_filters(query, start)
    daily = {}
    for row in db.execute(
        f"""SELECT event_time,total_tokens,actual_cost_usd,estimated_cost_usd
            FROM events{where} ORDER BY event_time""", args
    ):
        day = datetime.fromtimestamp(row["event_time"], timezone.utc).astimezone(zone).date().isoformat()
        bucket = daily.setdefault(day, {
            "day": day, "total_tokens": 0, "actual_cost_usd": 0, "estimated_cost_usd": 0})
        bucket["total_tokens"] += row["total_tokens"]
        bucket["actual_cost_usd"] += row["actual_cost_usd"] or 0
        bucket["estimated_cost_usd"] += row["estimated_cost_usd"] or 0
    trend = list(daily.values())
    dimensions = {}
    for key in ("client", "provider", "model", "attribution"):
        dimensions[key] = [
            r[0] for r in db.execute(f"SELECT DISTINCT {key} FROM events ORDER BY {key}") if r[0]
        ]
    return {"timezone": str(zone), "cards": cards, "trend": trend, "filters": dimensions}


def sessions(db: sqlite3.Connection, query: dict) -> list[dict]:
    zone = reporting_timezone(query)
    ranges = boundaries(zone=zone)
    selected = query.get("range", [""])[0]
    if selected and selected not in ranges:
        raise ValueError("invalid range")
    where, args = where_filters(query, ranges.get(selected) if selected else None)
    return [
        dict(r) for r in db.execute(
            f"""SELECT session_id,parent_session_id,client,host,provider,model,
                MIN(event_time) first_event,MAX(event_time) last_event,
                SUM(total_tokens) total_tokens,SUM(input_tokens) input_tokens,
                SUM(cached_input_tokens) cached_input_tokens,SUM(output_tokens) output_tokens,
                SUM(reasoning_output_tokens) reasoning_output_tokens,
                SUM(actual_cost_usd) actual_cost_usd,
                SUM(estimated_cost_usd) estimated_cost_usd
                FROM events{where} GROUP BY session_id,parent_session_id,client,host,provider,model
                ORDER BY last_event DESC LIMIT 1000""", args
        )
    ]


def pricing_status(db: sqlite3.Connection, include_rates: bool = True) -> dict:
    sources = [dict(r) for r in db.execute("SELECT * FROM pricing_sources ORDER BY provider")]
    prices = [
        dict(r) for r in db.execute(
            """SELECT provider,model,category,price_per_million,source_url,retrieved_at,version
               FROM prices WHERE active=1 ORDER BY provider,model,category"""
        )
    ] if include_rates else []
    now = now_ts()
    for item in sources:
        item["stale"] = not item["last_success"] or now - item["last_success"] > 48 * 3600
    return {"sources": sources, "rates": prices}


def health(db: sqlite3.Connection) -> tuple[dict, bool]:
    meta = {r["key"]: r["value"] for r in db.execute("SELECT key,value FROM meta")}
    now = now_ts()
    hermes_sync = float(meta.get("hermes_last_sync", 0))
    pricing_sync = float(meta.get("pricing_last_refresh", 0))
    codex_sync = db.execute(
        "SELECT MAX(created_at) FROM events WHERE client='codex'"
    ).fetchone()[0] or 0
    codex_heartbeat = float(meta.get("codex_heartbeat", 0))
    codex_collector_fresh = bool(codex_heartbeat and now - codex_heartbeat < 30)
    opencode_sync = db.execute(
        "SELECT MAX(created_at) FROM events WHERE client='opencode'"
    ).fetchone()[0] or 0
    hermes_enabled = meta.get("hermes_enabled") == "1"
    data = {
        "ok": bool(pricing_sync and (not hermes_enabled or (
            hermes_sync and now - hermes_sync < 60))),
        "database": "ok",
        "hermes_last_sync": hermes_sync or None,
        "codex_last_sync": codex_sync or None,
        "codex_collector_last_seen": codex_heartbeat or None,
        "codex_app_running": (
            meta.get("codex_app_running") == "1" if codex_collector_fresh else None
        ),
        "opencode_last_sync": opencode_sync or None,
        "pricing_last_refresh": pricing_sync or None,
        "hermes_warning": meta.get("hermes_last_warning"),
        "version": VERSION,
    }
    return data, data["ok"]


class App:
    def __init__(self, db_path: str, hermes_path: str | None, secret: bytes, dashboard_path: str):
        self.db_path = db_path
        self.hermes_path = hermes_path
        self.secret = secret
        self.dashboard_path = dashboard_path
        self.stop = threading.Event()

    def db(self) -> sqlite3.Connection:
        return connect(self.db_path)

    def poll_hermes(self):
        while not self.stop.is_set():
            try:
                db = self.db()
                import_hermes(db, self.hermes_path)
                db.close()
            except Exception as exc:
                db = self.db()
                db.execute(
                    "INSERT INTO meta(key,value) VALUES('hermes_last_warning',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"{datetime.now(TZ).isoformat()}: {str(exc)[:400]}",),
                )
                db.commit()
                db.close()
            self.stop.wait(10)

    def handler(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            server_version = f"MudKatTokenTracker/{VERSION}"
            sys_version = ""

            def log_message(self, fmt, *args):
                print(f"{self.address_string()} - {fmt % args}", flush=True)

            def reply(self, status: int, payload, content_type="application/json"):
                body = payload if isinstance(payload, bytes) else json.dumps(
                    payload, separators=(",", ":"), default=str
                ).encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header(
                    "Permissions-Policy", "camera=(), microphone=(), geolocation=()")
                self.send_header("Content-Security-Policy",
                                 "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                                 "script-src 'self' 'unsafe-inline'; img-src 'self' data:; "
                                 "frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
                self.end_headers()
                self.wfile.write(body)

            def lan_allowed(self) -> bool:
                try:
                    ip = ipaddress.ip_address(self.client_address[0].split("%")[0])
                    return ip.is_private or ip.is_loopback
                except ValueError:
                    return False

            def do_GET(self):
                if not self.lan_allowed():
                    return self.reply(HTTPStatus.FORBIDDEN, {"error": "LAN access only"})
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                db = app.db()
                try:
                    if parsed.path == "/":
                        body = Path(app.dashboard_path).read_bytes()
                        return self.reply(200, body, "text/html; charset=utf-8")
                    setup_files = {
                        "/setup/opencode-plugin.js": ("opencode-plugin.js", "text/javascript; charset=utf-8"),
                        "/setup/codex-collector.py": ("collector.py", "text/x-python; charset=utf-8"),
                        "/setup/generic-ingest.py": ("generic-ingest.py", "text/x-python; charset=utf-8"),
                    }
                    if parsed.path in setup_files:
                        filename, content_type = setup_files[parsed.path]
                        body = Path(app.dashboard_path).with_name(filename).read_bytes()
                        return self.reply(200, body, content_type)
                    if parsed.path == "/api/v1/summary":
                        return self.reply(200, summary(db, query))
                    if parsed.path == "/api/v1/sessions":
                        return self.reply(200, {"sessions": sessions(db, query)})
                    if parsed.path == "/api/v1/pricing":
                        return self.reply(200, pricing_status(
                            db, query.get("summary", [""])[0] != "1"))
                    if parsed.path == "/healthz":
                        data, ok = health(db)
                        return self.reply(200 if ok else 503, data)
                    return self.reply(404, {"error": "not found"})
                except ValueError as exc:
                    return self.reply(400, {"error": str(exc)})
                finally:
                    db.close()

            def do_POST(self):
                if not self.lan_allowed():
                    return self.reply(HTTPStatus.FORBIDDEN, {"error": "LAN access only"})
                parsed = urlparse(self.path)
                status_endpoint = parsed.path == "/api/v1/status/codex"
                endpoint_client = {
                    "/api/v1/ingest/codex": "codex",
                    "/api/v1/ingest/opencode": "opencode",
                    "/api/v1/ingest/usage": None,
                }.get(parsed.path, False)
                if endpoint_client is False and not status_endpoint:
                    return self.reply(404, {"error": "not found"})
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    return self.reply(413, {"error": "invalid batch size"})
                if length <= 0 or length > MAX_BATCH_BYTES:
                    return self.reply(413, {"error": "invalid batch size"})
                if self.headers.get_content_type() != "application/json":
                    return self.reply(415, {"error": "application/json required"})
                body = self.rfile.read(length)
                timestamp = self.headers.get("X-MudKat-Timestamp", "")
                signature = self.headers.get("X-MudKat-Signature", "")
                try:
                    request_time = int(timestamp)
                except ValueError:
                    return self.reply(401, {"error": "invalid timestamp"})
                if abs(time.time() - request_time) > 300:
                    return self.reply(401, {"error": "stale timestamp"})
                expected = hmac.new(app.secret, timestamp.encode() + b"\n" + body,
                                    hashlib.sha256).hexdigest()
                if not hmac.compare_digest(signature, expected):
                    return self.reply(401, {"error": "invalid signature"})
                db = app.db()
                try:
                    db.execute("DELETE FROM request_replays WHERE seen_at<?", (now_ts() - 600,))
                    try:
                        db.execute("INSERT INTO request_replays VALUES(?,?)", (signature, now_ts()))
                    except sqlite3.IntegrityError:
                        return self.reply(409, {"error": "replay", "acknowledged": True})
                    payload = json.loads(body)
                    if status_endpoint:
                        if (
                            not isinstance(payload, dict)
                            or payload.get("client") != "codex"
                            or not isinstance(payload.get("app_running"), bool)
                        ):
                            return self.reply(400, {"error": "invalid Codex status"})
                        host = clean_text(payload.get("host"), "host", 256, True)
                        values = {
                            "codex_heartbeat": str(now_ts()),
                            "codex_app_running": "1" if payload["app_running"] else "0",
                            "codex_heartbeat_host": host,
                        }
                        db.executemany(
                            """INSERT INTO meta(key,value) VALUES(?,?)
                               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                            values.items(),
                        )
                        db.commit()
                        return self.reply(200, {"acknowledged": True})
                    events = payload.get("events") if isinstance(payload, dict) else None
                    if not isinstance(events, list) or len(events) > MAX_BATCH_EVENTS:
                        return self.reply(400, {"error": "events must be a list of at most 1000"})
                    if endpoint_client and any(
                        not isinstance(e, dict)
                        or str(e.get("client", "")).lower() != endpoint_client for e in events
                    ):
                        return self.reply(400, {
                            "error": f"{endpoint_client.title()} endpoint accepts "
                                     f"{endpoint_client.title()} events only"
                        })
                    accepted, duplicates = insert_events(db, events)
                    return self.reply(200, {
                        "acknowledged": True, "accepted": accepted, "duplicates": duplicates
                    })
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    return self.reply(400, {"error": str(exc)})
                finally:
                    db.close()

        return Handler


def backup(db_path: str, backup_dir: str, retention_days: int = 30) -> str:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    if not Path(db_path).is_file():
        raise FileNotFoundError(db_path)
    target_dir = Path(backup_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"tracker-{datetime.now(TZ):%Y%m%d-%H%M%S}.sqlite3"
    source = sqlite3.connect(db_path)
    destination = sqlite3.connect(target)
    source.backup(destination)
    destination.close()
    target.chmod(0o600)
    source.close()
    cutoff = now_ts() - retention_days * 86400
    for old in target_dir.glob("tracker-*.sqlite3"):
        if old.stat().st_mtime < cutoff:
            old.unlink()
    return str(target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.environ.get("MUDKAT_TRACKER_DB", "tracker.sqlite3"))
    parser.add_argument("--hermes-db", default=os.environ.get("MUDKAT_HERMES_DB"))
    parser.add_argument("--manual-prices", default=os.environ.get("MUDKAT_MANUAL_PRICES"))
    parser.add_argument("--hermes-pricing-source", default=os.environ.get(
        "MUDKAT_HERMES_PRICING_SOURCE"))
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=9130)
    refresh = sub.add_parser("refresh-pricing")
    refresh.add_argument("--json", dest="price_json")
    sub.add_parser("import-hermes")
    back = sub.add_parser("backup")
    back.add_argument("--dir", required=True)
    back.add_argument("--retention-days", type=int, default=30)
    price = sub.add_parser("set-price")
    price.add_argument("provider")
    price.add_argument("model")
    price.add_argument("category")
    price.add_argument("rate_per_million")
    price.add_argument("source_url")
    args = parser.parse_args()
    db = connect(args.db)
    if args.command == "refresh-pricing":
        print(json.dumps(refresh_pricing(
            db, args.price_json or args.manual_prices, args.hermes_pricing_source), indent=2))
    elif args.command == "import-hermes":
        if not args.hermes_db:
            raise SystemExit("Set MUDKAT_HERMES_DB or pass --hermes-db")
        print(json.dumps(import_hermes(db, args.hermes_db), indent=2))
    elif args.command == "backup":
        db.close()
        print(backup(args.db, args.dir, args.retention_days))
        return
    elif args.command == "set-price":
        when = now_ts()
        digest = hashlib.sha256(
            f"{args.provider}|{args.model}|{args.category}|{args.rate_per_million}|{args.source_url}".encode()
        ).hexdigest()
        set_price(db, args.provider, args.model, args.category, args.rate_per_million,
                  args.source_url, digest, when, "manual-" + digest[:12])
        db.commit()
        print("price saved")
    elif args.command == "serve":
        db.close()
        secret = os.environ.get("MUDKAT_TRACKER_SECRET", "").encode()
        if len(secret) < 32:
            raise SystemExit("MUDKAT_TRACKER_SECRET must contain at least 32 characters")
        dashboard = str(Path(__file__).with_name("dashboard.html"))
        app = App(args.db, args.hermes_db, secret, dashboard)
        db = app.db()
        db.execute(
            """INSERT INTO meta(key,value) VALUES('hermes_enabled',?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            ("1" if args.hermes_db else "0",),
        )
        db.commit()
        db.close()
        if args.hermes_db:
            worker = threading.Thread(
                target=app.poll_hermes, name="hermes-poller", daemon=True)
            worker.start()
        server = ThreadingHTTPServer((args.host, args.port), app.handler())
        print(f"MudKat Token Tracker {VERSION} listening on {args.host}:{args.port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            app.stop.set()
            server.server_close()
        return
    db.close()


if __name__ == "__main__":
    main()
