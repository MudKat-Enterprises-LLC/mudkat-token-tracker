import hashlib
import hmac
import json
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import collector
import tracker


def sample_event(key="one", **overrides):
    event = {
        "idempotency_key": key, "client": "codex", "host": "pc",
        "provider": "openai", "model": "gpt-test", "session_id": "root",
        "event_time": "2026-07-23T12:00:00Z", "input_tokens": 100,
        "cached_input_tokens": 20, "output_tokens": 10,
        "reasoning_output_tokens": 5, "total_tokens": 130,
        "api_call_count": 1, "billing_mode": "api", "attribution": "exact",
    }
    event.update(overrides)
    return event


class TempCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "tracker.sqlite3"
        self.db = tracker.connect(self.db_path)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()


class CodexParserTests(TempCase):
    def test_codex_process_detection(self):
        running = subprocess.CompletedProcess([], 0, stdout='"codex.exe","123"', stderr="")
        stopped = subprocess.CompletedProcess([], 0, stdout="INFO: No tasks", stderr="")
        with patch.object(collector.os, "name", "nt"):
            with patch.object(collector.subprocess, "run", return_value=running):
                self.assertTrue(collector.codex_running())
            with patch.object(collector.subprocess, "run", return_value=stopped):
                self.assertFalse(collector.codex_running())

    def records(self, parent=None, models=("gpt-a",)):
        lines = [
            {"timestamp": "2026-07-23T12:00:00Z", "type": "session_meta",
             "payload": {"id": "session", "parent_thread_id": parent,
                         "model_provider": "openai"}},
        ]
        for index, model in enumerate(models):
            lines += [
                {"timestamp": f"2026-07-23T12:00:0{index + 1}Z", "type": "turn_context",
                 "payload": {"model": model}},
                {"timestamp": f"2026-07-23T12:00:0{index + 2}Z", "type": "event_msg",
                 "payload": {"type": "token_count", "info": {
                     "last_token_usage": {
                         "input_tokens": 100, "cached_input_tokens": 40,
                         "cache_write_input_tokens": 10, "output_tokens": 20,
                         "reasoning_output_tokens": 5, "total_tokens": 120},
                     "total_token_usage": {"total_tokens": 120 * (index + 1)}}}},
            ]
        return b"".join(json.dumps(x).encode() + b"\n" for x in lines)

    def test_root_subagent_model_change_and_cache_normalization(self):
        context = {}
        events = []
        for ordinal, line in enumerate(self.records("parent", ("gpt-a", "gpt-b")).splitlines(True), 1):
            value = collector.parse_safe_line(line, context, ordinal, "pc", "chatgpt")
            if value:
                events.append(value)
        self.assertEqual(["gpt-a", "gpt-b"], [e["model"] for e in events])
        self.assertEqual("parent", events[0]["parent_session_id"])
        self.assertEqual("openai-codex", events[0]["provider"])
        self.assertEqual((50, 40, 10, 20), tuple(
            events[0][k] for k in (
                "input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens")))

    def test_partial_restart_rotation_and_duplicate(self):
        rollout = self.root / "rollout.jsonl"
        full = self.records()
        rollout.write_bytes(full + b'{"timestamp":"partial"')
        events, state = collector.scan_file(rollout, {}, "pc", "chatgpt")
        self.assertEqual(1, len(events))
        self.assertLess(state["offset"], rollout.stat().st_size)
        self.assertEqual((1, 0), tracker.insert_events(self.db, events))
        self.assertEqual((0, 1), tracker.insert_events(self.db, events))
        restarted, same = collector.scan_file(rollout, state, "pc", "chatgpt")
        self.assertEqual([], restarted)
        rollout.write_bytes(self.records(models=("gpt-new",)))
        rotated, rotated_state = collector.scan_file(rollout, {
            "offset": 10**9, "ordinal": 99, "context": {"model": "old"}}, "pc", "chatgpt")
        self.assertEqual("gpt-new", rotated[0]["model"])
        self.assertLess(rotated_state["ordinal"], 99)


def make_hermes(path: Path):
    db = sqlite3.connect(path)
    db.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE sessions (
      id TEXT PRIMARY KEY, source TEXT, model TEXT, parent_session_id TEXT,
      started_at REAL, ended_at REAL, input_tokens INTEGER, output_tokens INTEGER,
      cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER,
      billing_provider TEXT, billing_base_url TEXT, billing_mode TEXT,
      estimated_cost_usd REAL, actual_cost_usd REAL, api_call_count INTEGER);
    CREATE TABLE session_model_usage (
      session_id TEXT, model TEXT, billing_provider TEXT, billing_base_url TEXT,
      billing_mode TEXT, task TEXT, api_call_count INTEGER, input_tokens INTEGER,
      output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER,
      reasoning_tokens INTEGER, estimated_cost_usd REAL, actual_cost_usd REAL,
      first_seen REAL, last_seen REAL);
    """)
    return db


class HermesImporterTests(TempCase):
    def test_model_rows_fallback_increase_reset_and_live_writer(self):
        hp = self.root / "hermes.sqlite3"
        h = make_hermes(hp)
        h.execute("INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("model-session", "cli", "ignored", None, 10, 20, 0, 0, 0, 0, 0,
                   "openrouter", "https://openrouter.ai/api/v1", "api", 0, 0, 0))
        h.execute("INSERT INTO session_model_usage VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("model-session", "vendor/model", "openrouter",
                   "https://openrouter.ai/api/v1", "api", "", 1, 100, 20, 30, 0, 5, 0, 0, 10, 20))
        h.execute("INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("fallback", "cli", "fallback-model", None, 10, 20, 7, 3, 2, 1, 1,
                   "custom", "http://localhost:11434/v1", "", 0, 0, 1))
        h.commit()
        first = tracker.import_hermes(self.db, str(hp))
        self.assertEqual(2, first["created"])
        rows = self.db.execute("SELECT provider,total_tokens,attribution FROM events ORDER BY session_id").fetchall()
        self.assertEqual("ollama", rows[0]["provider"])
        self.assertTrue(all(r["attribution"] == "session_aggregate" for r in rows))
        h.execute("UPDATE session_model_usage SET input_tokens=125,api_call_count=2")
        h.commit()
        second = tracker.import_hermes(self.db, str(hp))
        self.assertEqual(1, second["created"])
        self.assertEqual(25, self.db.execute(
            "SELECT input_tokens FROM events WHERE attribution='exact'").fetchone()[0])
        h.execute("UPDATE session_model_usage SET input_tokens=5")
        h.commit()
        reset = tracker.import_hermes(self.db, str(hp))
        self.assertEqual(1, reset["resets"])
        self.assertEqual(0, reset["created"])
        writer = sqlite3.connect(hp)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE session_model_usage SET input_tokens=9")
        writer.commit()
        live = tracker.import_hermes(self.db, str(hp))
        self.assertEqual(4, self.db.execute(
            "SELECT input_tokens FROM events WHERE attribution='exact' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0])
        writer.close()
        h.close()


class AggregationPricingTests(TempCase):
    def test_openai_pricing_source_uses_official_developer_docs(self):
        self.assertEqual(
            ("https://developers.openai.com/api/docs/pricing", "official_page"),
            tracker.SOURCE_URLS["openai"],
        )

    def test_boundaries_dst_monday_month_year_and_lifetime(self):
        with self.assertRaises(ValueError):
            tracker.parse_time(float("nan"))
        with self.assertRaises(ValueError):
            tracker.parse_time("2026-01-01T00:00:00")
        try:
            central = ZoneInfo("America/Chicago")
        except ZoneInfoNotFoundError:
            self.skipTest("IANA timezone database is unavailable")
        spring = datetime(2026, 3, 9, 0, 30, tzinfo=central)
        b = tracker.boundaries(spring, central)
        self.assertEqual(datetime(2026, 3, 9, tzinfo=central).timestamp(), b["week"])
        self.assertEqual(datetime(2026, 3, 1, tzinfo=central).timestamp(), b["month"])
        self.assertEqual(datetime(2026, 1, 1, tzinfo=central).timestamp(), b["year"])
        self.assertEqual(0, b["lifetime"])
        self.assertEqual("UTC", str(tracker.reporting_timezone({"timezone": ["UTC"]})))
        with self.assertRaises(ValueError):
            tracker.reporting_timezone({"timezone": ["Not/A_Zone"]})
        before = tracker.boundaries(
            datetime(2026, 3, 8, 12, tzinfo=central), central)["today"]
        after = tracker.boundaries(
            datetime(2026, 3, 9, 12, tzinfo=central), central)["today"]
        self.assertEqual(23 * 3600, after - before)

    def test_sessions_respect_today_range(self):
        now = datetime.now(timezone.utc)
        midnight = datetime.now(tracker.TZ).replace(
            hour=0, minute=0, second=0, microsecond=0)
        old = (midnight - timedelta(seconds=1)).astimezone(timezone.utc)
        tracker.insert_events(self.db, [
            sample_event("old", session_id="old", event_time=old.isoformat()),
            sample_event("new", session_id="new", event_time=now.isoformat()),
        ])
        rows = tracker.sessions(
            self.db, {"range": ["today"], "timezone": [str(tracker.TZ)]})
        self.assertEqual(["new"], [row["session_id"] for row in rows])

    def test_token_counts_are_integral_and_total_is_consistent(self):
        for value in (1.5, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                tracker.normalize_event(sample_event(input_tokens=value))
        with self.assertRaises(ValueError):
            tracker.normalize_event(sample_event(total_tokens=1))

    def test_price_update_override_and_historical_preservation(self):
        when = time.time()
        tracker.set_price(self.db, "openai", "gpt-test", "input", "2", "official", "a", when, "v1")
        tracker.set_price(self.db, "openai", "gpt-test", "cache_read", ".2", "official", "a", when, "v1")
        tracker.set_price(self.db, "openai", "gpt-test", "output", "10", "official", "a", when, "v1")
        tracker.insert_events(self.db, [sample_event()])
        original = self.db.execute(
            "SELECT estimated_cost_usd,pricing_version FROM events").fetchone()
        tracker.set_price(self.db, "openai", "gpt-test", "input", "99", "manual", "b", when + 1, "v2")
        unchanged = self.db.execute(
            "SELECT estimated_cost_usd,pricing_version FROM events").fetchone()
        self.assertEqual(tuple(original), tuple(unchanged))
        self.assertEqual("v1", original["pricing_version"])

    def test_machine_pricing_valid_unchanged_changed_malformed_and_stale(self):
        old_sources = tracker.SOURCE_URLS
        tracker.SOURCE_URLS = {"openrouter": ("https://example/models", "machine_readable")}
        payload = {"data": [{"id": "a/model", "pricing": {"prompt": "0.000001", "completion": "0.000002"}}]}
        try:
            with patch.object(tracker, "fetch", return_value=json.dumps(payload).encode()):
                one = tracker.refresh_pricing(self.db)
                two = tracker.refresh_pricing(self.db)
            self.assertEqual(0, one["errors"])
            self.assertEqual(2, self.db.execute("SELECT COUNT(*) FROM prices").fetchone()[0])
            with patch.object(tracker, "fetch", return_value=b"{bad"):
                bad = tracker.refresh_pricing(self.db)
            self.assertEqual(1, bad["errors"])
            self.db.execute("UPDATE pricing_sources SET last_success=?", (time.time() - 49 * 3600,))
            self.db.commit()
            self.assertTrue(tracker.pricing_status(self.db)["sources"][0]["stale"])
        finally:
            tracker.SOURCE_URLS = old_sources


class SecurityDatabaseTests(TempCase):
    def setUp(self):
        super().setUp()
        self.secret = b"x" * 32
        dashboard = self.root / "dashboard.html"
        dashboard.write_text("ok")
        for filename in ("collector.py", "opencode-plugin.js", "generic-ingest.py"):
            (self.root / filename).write_text(filename)
        app = tracker.App(str(self.db_path), str(self.root / "missing"), self.secret, str(dashboard))
        self.server = tracker.ThreadingHTTPServer(("127.0.0.1", 0), app.handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/api/v1/ingest/codex"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def send(self, payload, secret=None, timestamp=None, endpoint="codex"):
        body = json.dumps(payload, separators=(",", ":")).encode()
        timestamp = str(timestamp or int(time.time()))
        signature = hmac.new(secret or self.secret, timestamp.encode() + b"\n" + body,
                             hashlib.sha256).hexdigest()
        url = self.url.rsplit("/", 1)[0] + "/" + endpoint
        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json", "X-MudKat-Timestamp": timestamp,
            "X-MudKat-Signature": signature})
        try:
            with urllib.request.urlopen(req) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def send_status(self, running):
        payload = {"client": "codex", "host": "pc", "app_running": running}
        body = json.dumps(payload, separators=(",", ":")).encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(
            self.secret, timestamp.encode() + b"\n" + body, hashlib.sha256
        ).hexdigest()
        request = urllib.request.Request(
            self.url.replace("/ingest/codex", "/status/codex"), data=body,
            headers={"Content-Type": "application/json", "X-MudKat-Timestamp": timestamp,
                     "X-MudKat-Signature": signature},
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_signature_replay_skew_malformed_and_content_discard(self):
        payload = {"events": [sample_event(secret_prompt="discard me", response="discard me")]}
        self.assertEqual(401, self.send(payload, b"y" * 32)[0])
        self.assertEqual(200, self.send(payload)[0])
        self.assertEqual(409, self.send(payload)[0])
        self.assertEqual(401, self.send(payload, timestamp=int(time.time()) - 301)[0])
        self.assertEqual(400, self.send({"events": ["bad"]})[0])
        self.assertEqual(400, self.send({"events": ["bad"]})[0])
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(events)")}
        self.assertFalse({"secret_prompt", "response"} & columns)
        body = b"{}"
        timestamp = str(int(time.time()))
        signature = hmac.new(
            self.secret, timestamp.encode() + b"\n" + body, hashlib.sha256
        ).hexdigest()
        request = urllib.request.Request(self.url, data=body, headers={
            "X-MudKat-Timestamp": timestamp, "X-MudKat-Signature": signature})
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        self.assertEqual(415, caught.exception.code)

    def test_opencode_endpoint_accepts_only_opencode_events(self):
        event = sample_event("opencode", client="opencode")
        self.assertEqual(200, self.send({"events": [event]}, endpoint="opencode")[0])
        self.assertEqual(
            400,
            self.send({"events": [sample_event("wrong-client")]}, endpoint="opencode")[0],
        )
        self.assertEqual(
            "opencode",
            self.db.execute("SELECT client FROM events WHERE idempotency_key='opencode'").fetchone()[0],
        )

    def test_codex_status_heartbeat(self):
        self.assertEqual(200, self.send_status(True)[0])
        status = tracker.health(self.db)[0]
        self.assertTrue(status["codex_app_running"])
        self.assertIsNotNone(status["codex_collector_last_seen"])
        self.assertEqual(200, self.send_status(False)[0])
        self.assertFalse(tracker.health(self.db)[0]["codex_app_running"])
        self.assertEqual(400, self.send_status("yes")[0])

    def test_health_allows_installations_without_hermes(self):
        self.db.executemany(
            "INSERT INTO meta(key,value) VALUES(?,?)",
            (("pricing_last_refresh", str(time.time())), ("hermes_enabled", "0")),
        )
        self.db.commit()
        self.assertTrue(tracker.health(self.db)[0]["ok"])
        self.db.execute("UPDATE meta SET value='1' WHERE key='hermes_enabled'")
        self.db.commit()
        self.assertFalse(tracker.health(self.db)[0]["ok"])

    def test_generic_endpoint_and_setup_downloads(self):
        event = sample_event("generic", client="custom-harness")
        self.assertEqual(200, self.send({"events": [event]}, endpoint="usage")[0])
        self.assertEqual(
            "custom-harness",
            self.db.execute("SELECT client FROM events WHERE idempotency_key='generic'").fetchone()[0],
        )
        base = self.url.split("/api/", 1)[0]
        expected = {
            "/setup/codex-collector.py": b"collector.py",
            "/setup/opencode-plugin.js": b"opencode-plugin.js",
            "/setup/generic-ingest.py": b"generic-ingest.py",
        }
        for path, body in expected.items():
            with self.subTest(path=path), urllib.request.urlopen(base + path) as response:
                self.assertEqual(200, response.status)
                self.assertEqual(body, response.read())
                self.assertEqual("DENY", response.headers["X-Frame-Options"])
                self.assertEqual("no-referrer", response.headers["Referrer-Policy"])

    def test_batch_limits_wal_backup_restore(self):
        self.assertEqual(400, self.send({"events": [sample_event(str(i)) for i in range(1001)]})[0])
        oversized = urllib.request.Request(self.url, data=b"x" * (tracker.MAX_BATCH_BYTES + 1))
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(oversized)
        self.assertEqual(413, caught.exception.code)
        self.assertEqual("wal", self.db.execute("PRAGMA journal_mode").fetchone()[0])
        tracker.insert_events(self.db, [sample_event("backup")])
        self.db.close()
        self.db = tracker.connect(self.db_path)
        self.assertEqual(1, self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        with self.assertRaises(ValueError):
            tracker.backup(str(self.db_path), str(self.root / "backups"), 0)
        with self.assertRaises(FileNotFoundError):
            tracker.backup(str(self.root / "missing.sqlite3"), str(self.root / "backups"))
        target = tracker.backup(str(self.db_path), str(self.root / "backups"))
        restored = sqlite3.connect(target)
        self.assertEqual(1, restored.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        restored.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
