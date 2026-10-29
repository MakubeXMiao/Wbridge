"""Synthetic, offline tests for the Muse adapter; never use live bot state."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge import worker_state as ws
from wechat_bridge.store import atomic_write_json, load_json

SPEC = importlib.util.spec_from_file_location("muse_worker", ROOT / "hooks" / "muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class MuseWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-muse-adapter-")
        self.addCleanup(self.tmp.cleanup)
        self.base = str(Path(self.tmp.name) / "bridge")
        self.state_dir = str(Path(self.tmp.name) / "hook-state")
        self.now = 2000000000
        self.state = ws.WorkerState(self.base, self.state_dir, lambda: self.now)
        Path(self.state.store.session_path).write_text("{}")

    def incoming(self, mid, text="synthetic message", sender="synthetic-owner"):
        atomic_write_json(str(Path(self.base) / "inbox" / (mid + ".json")),
                          {"message_id": mid, "from_user_id": sender,
                           "context_token": "SYNTHETIC-CONTEXT-SECRET", "text": text,
                           "create_time_ms": None, "received_at": self.now})

    def call(self, request, factory=None):
        return adapter.handle_request(request, self.base, self.state_dir,
                                      factory or (lambda base, state_dir: self.state))

    def begin(self, mid="a", text="synthetic message"):
        self.incoming(mid, text)
        event = self.state.plan()["payload"]
        result = self.call({"action": "begin", "lease": event["lease"]})
        self.assertTrue(result["ok"], result)
        return event, result

    def session(self, handle):
        return load_json(str(Path(self.state_dir) / "muse-adapter" / (handle + ".json")))

    def test_begin_hides_private_state_and_stores_only_batch_digest(self):
        event, result = self.begin()
        private = self.session(result["handle"])
        self.assertNotEqual(private["lease"], event["lease"])
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["owned_count"], 1)
        self.assertEqual(result["messages"][0]["text"], "synthetic message")
        self.assertEqual(result["expires_at"], self.now + 90)
        self.assertEqual(result["hard_deadline"], self.now + 570)
        self.assertNotIn("messages", private)
        encoded = json.dumps(result)
        for secret in (private["lease"], "SYNTHETIC-CONTEXT-SECRET", "synthetic-owner", '"to"', '"context"'):
            self.assertNotIn(secret, encoded)
        directory = Path(self.state_dir) / "muse-adapter"
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        for path in directory.iterdir():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertFalse(self.call({"action": "begin", "lease": event["lease"]})["ok"])

    def test_round_trip_uses_code_owned_ids_and_exact_text(self):
        _, batch = self.begin(text="只回复指定文字")
        text = '合成测试 "引号"\n$(touch /tmp/never-run) `id` \\ 结束'
        result = self.call({"action": "reply", "handle": batch["handle"],
                            "revision": batch["revision"], "text": text})
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["delivery_confirmed"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["text"], text)
        self.assertEqual(job["covers_message_ids"], ["a"])
        self.assertEqual(job["context_token"], "SYNTHETIC-CONTEXT-SECRET")
        self.assertEqual(self.session(batch["handle"])["phase"], "queued")
        for action in ("reply", "refresh", "release"):
            request = {"action": action, "handle": batch["handle"]}
            if action == "reply":
                request.update(revision=1, text="must not enqueue")
            self.assertEqual(self.call(request)["error"], "session_closed")
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)

    def test_late_arrival_requires_new_messages_refresh_and_revision(self):
        _, batch = self.begin()
        handle = batch["handle"]
        self.incoming("b", text="new synthetic question")
        old_reply = {"action": "reply", "handle": handle, "revision": 1, "text": "old answer"}
        result = self.call(old_reply)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertEqual(result["revision"], 2)
        self.assertEqual([message["message_id"] for message in result["messages"]], ["a", "b"])
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))
        self.assertTrue((Path(self.base) / "inbox" / "a.json").exists())
        self.assertEqual(self.call(old_reply)["error"], "revision_mismatch")
        self.assertEqual(self.call(dict(old_reply, revision=2))["error"], "needs_refresh")
        refreshed = self.call({"action": "refresh", "handle": handle})
        self.assertTrue(refreshed["ok"])
        self.assertEqual(refreshed["revision"], 3)
        result = self.call(dict(old_reply, revision=3, text="regenerated complete answer"))
        self.assertTrue(result["ok"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["covers_message_ids"], ["a", "b"])
        self.assertEqual(job["text"], "regenerated complete answer")

    def test_refresh_revision_and_hard_deadline_unchanged(self):
        _, batch = self.begin()
        handle = batch["handle"]
        for elapsed in range(60, 541, 60):
            self.now = 2000000000 + elapsed
            result = self.call({"action": "refresh", "handle": handle})
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["hard_deadline"], 2000000570)
            self.assertLessEqual(result["expires_at"], 2000000570)
        self.now = 2000000570
        result = self.call({"action": "refresh", "handle": handle})
        self.assertEqual(result["error"], "lease_lost")
        self.assertTrue(result["must_stop"])

    def test_private_call_times_are_observed_not_derived_and_keep_latest_refresh(self):
        # The adapter wall clock deliberately differs from the core lease
        # clock. A lease-deadline calculation cannot pass these assertions.
        with patch.object(adapter.time, "time", side_effect=[120.25, 123.5]):
            _, batch = self.begin(text="PRIVATE-INCOMING-BODY")
        handle = batch["handle"]
        self.assertEqual(self.session(handle)["call_timings"], {
            "begin": {"started_at": 120.25, "returned_at": 123.5, "result": "ok"}})
        self.assertEqual(batch["expires_at"], 2000000090)
        self.assertNotIn("call_timings", batch)
        for elapsed, start, end in ((30, 200.5, 204.75), (60, 310.25, 315.5)):
            self.now = 2000000000 + elapsed
            with patch.object(adapter.time, "time", side_effect=[start, end]):
                refreshed = self.call({"action": "refresh", "handle": handle})
            self.assertTrue(refreshed["ok"])
            self.assertEqual(self.session(handle)["call_timings"]["refresh"], {
                "started_at": start, "returned_at": end, "result": "ok"})
            self.assertNotIn("call_timings", refreshed)
        with patch.object(adapter.time, "time", side_effect=[400.25, 408.5]):
            result = self.call({"action": "reply", "handle": handle,
                                "revision": refreshed["revision"],
                                "text": "PRIVATE-OUTGOING-BODY"})
        self.assertTrue(result["ok"])
        private = self.session(handle)
        self.assertEqual(set(private["call_timings"]), {"begin", "refresh", "reply"})
        self.assertEqual(private["call_timings"]["reply"], {
            "started_at": 400.25, "returned_at": 408.5, "result": "queued"})
        self.assertNotIn("call_timings", result)
        encoded = json.dumps(private)
        for secret in ("PRIVATE-INCOMING-BODY", "PRIVATE-OUTGOING-BODY", "SYNTHETIC-CONTEXT-SECRET"):
            self.assertNotIn(secret, encoded)
        path = Path(self.state_dir) / "muse-adapter" / (handle + ".json")
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_failed_reply_and_refresh_keep_real_call_times(self):
        for action in ("reply", "refresh"):
            with self.subTest(action=action):
                _, batch = self.begin(mid=action)
                self.now += 90
                request = {"action": action, "handle": batch["handle"]}
                if action == "reply":
                    request.update(revision=1, text="late synthetic answer")
                with patch.object(adapter.time, "time", side_effect=[21.5, 24.25]):
                    result = self.call(request)
                self.assertEqual(result, {"ok": False, "error": "lease_lost", "must_stop": True})
                private = self.session(batch["handle"])
                self.assertEqual(private["call_timings"][action], {
                    "started_at": 21.5, "returned_at": 24.25, "result": "lease_lost"})
                self.assertEqual(private["phase"], "lease_lost")

    def test_refresh_exception_records_boundary_without_changing_existing_failure(self):
        _, batch = self.begin()
        def fail(*args):
            raise OSError("PRIVATE-EXCEPTION-TEXT")
        self.state.refresh = fail
        with patch.object(adapter.time, "time", side_effect=[33.0, 36.0]):
            result = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertEqual(result, {"ok": False, "error": "adapter_state_error", "must_stop": True})
        private = self.session(batch["handle"])
        self.assertEqual(private["phase"], "active")
        self.assertEqual(private["call_timings"]["refresh"], {
            "started_at": 33.0, "returned_at": 36.0, "result": "outcome_unknown"})
        self.assertNotIn("PRIVATE-EXCEPTION-TEXT", json.dumps(private) + json.dumps(result))

    def test_legacy_session_without_call_times_remains_usable(self):
        _, batch = self.begin()
        handle = batch["handle"]
        private = self.session(handle)
        del private["call_timings"]
        adapter._save(Path(self.state_dir) / "muse-adapter", handle, private)
        with patch.object(adapter.time, "time", side_effect=[1.0, 2.0]):
            refreshed = self.call({"action": "refresh", "handle": handle})
        self.assertTrue(refreshed["ok"])
        with patch.object(adapter.time, "time", side_effect=[3.0, 4.0]):
            result = self.call({"action": "reply", "handle": handle,
                                "revision": refreshed["revision"], "text": "legacy reply"})
        self.assertTrue(result["ok"])
        self.assertEqual(set(self.session(handle)["call_timings"]), {"refresh", "reply"})

    def test_soft_expiry_and_event_expiry_cannot_be_revived(self):
        self.incoming("a")
        event = self.state.plan()["payload"]
        self.now += 180
        self.assertEqual(self.call({"action": "begin", "lease": event["lease"]})["error"], "lease_lost")
        self.assertFalse(list((Path(self.state_dir) / "muse-adapter").glob("*.json")))
        event = self.state.plan()["payload"]
        batch = self.call({"action": "begin", "lease": event["lease"]})
        self.now += 90
        result = self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "late"})
        self.assertEqual(result["error"], "lease_lost")
        self.assertEqual(self.call({"action": "refresh", "handle": batch["handle"]})["error"], "lease_lost")
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))

    def test_reply_failure_after_publication_is_unknown_and_never_retried(self):
        _, batch = self.begin()
        original = self.state.reply
        called = []

        def publish_then_fail(*args):
            called.append(True)
            original(*args)
            raise OSError("SYNTHETIC-CONTEXT-SECRET private-lease-must-not-leak")

        self.state.reply = publish_then_fail
        request = {"action": "reply", "handle": batch["handle"], "revision": 1, "text": "one reply"}
        with patch.object(adapter.time, "time", side_effect=[61.5, 63.0]):
            result = self.call(request)
        self.assertEqual(result, {"ok": False, "error": "outcome_unknown", "must_stop": True})
        for action in ("reply", "refresh", "release"):
            retry = {"action": action, "handle": batch["handle"]}
            if action == "reply":
                retry.update(revision=1, text="would duplicate")
            self.assertEqual(self.call(retry)["error"], "outcome_unknown")
        self.assertEqual(len(called), 1)
        self.assertEqual(self.session(batch["handle"])["phase"], "uncertain")
        self.assertEqual(self.session(batch["handle"])["call_timings"]["reply"], {
            "started_at": 61.5, "returned_at": 63.0, "result": "outcome_unknown"})
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)

    def test_reply_requires_durable_marker_and_crash_cannot_fake_return_or_replay(self):
        _, batch = self.begin()
        handle = batch["handle"]
        request = {"action": "reply", "handle": handle, "revision": 1, "text": "once"}
        original_save = adapter._save
        called = []

        def cannot_mark(directory, current_handle, session):
            if session["phase"] == "replying":
                raise OSError("marker could not be saved")
            original_save(directory, current_handle, session)

        def crash(*args):
            called.append(True)
            private = self.session(handle)
            self.assertEqual(private["phase"], "replying")
            self.assertEqual(private["call_timings"]["reply"], {
                "started_at": None, "returned_at": None, "result": "outcome_unknown"})
            raise KeyboardInterrupt()

        self.state.reply = crash
        with patch.object(adapter, "_save", side_effect=cannot_mark):
            self.assertEqual(self.call(request)["error"], "adapter_state_error")
        self.assertFalse(called)
        self.assertEqual(self.session(handle)["phase"], "active")
        # One time read proves that an interrupted call never records return.
        with patch.object(adapter.time, "time", side_effect=[71.5]), self.assertRaises(KeyboardInterrupt):
            self.call(request)
        for action in ("reply", "refresh", "release"):
            retry = dict(request) if action == "reply" else {"action": action, "handle": handle}
            self.assertEqual(self.call(retry)["error"], "outcome_unknown")
        self.assertEqual(called, [True])
        self.assertIsNone(self.session(handle)["call_timings"]["reply"]["returned_at"])
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))

    def test_invalid_reply_result_is_timed_and_stays_closed(self):
        _, batch = self.begin()
        self.state.reply = lambda *args: {"queued": False, "detail": "PRIVATE-UNTRUSTED-RESULT"}
        request = {"action": "reply", "handle": batch["handle"], "revision": 1, "text": "once"}
        with patch.object(adapter.time, "time", side_effect=[81.5, 85.75]):
            result = self.call(request)
        self.assertEqual(result, {"ok": False, "error": "outcome_unknown", "must_stop": True})
        private = self.session(batch["handle"])
        self.assertEqual(private["call_timings"]["reply"], {
            "started_at": 81.5, "returned_at": 85.75, "result": "outcome_unknown"})
        self.assertNotIn("PRIVATE-UNTRUSTED-RESULT", json.dumps(private))
        self.assertEqual(self.call(request)["error"], "outcome_unknown")

    def test_crash_marker_and_concurrent_reply_stop_replay(self):
        _, batch = self.begin()
        request = {"action": "reply", "handle": batch["handle"], "revision": 1, "text": "once"}
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.call(request), range(2)))
        self.assertEqual(sum(result["ok"] for result in results), 1)
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)
        session = self.session(batch["handle"])
        session["phase"] = "replying"
        adapter._save(Path(self.state_dir) / "muse-adapter", batch["handle"], session)
        self.assertEqual(self.call({"action": "release", "handle": batch["handle"]})["error"], "outcome_unknown")

    def test_different_senders_not_joined_and_release_is_not_cancellation(self):
        self.incoming("a", sender="synthetic-alice")
        self.incoming("b", sender="synthetic-bob")
        event = self.state.plan()["payload"]
        batch = self.call({"action": "begin", "lease": event["lease"]})
        self.assertEqual(batch["owned_count"], 1)
        self.assertEqual([message["message_id"] for message in batch["messages"]], ["a"])
        result = self.call({"action": "release", "handle": batch["handle"]})
        self.assertTrue(result["released"])
        self.assertFalse(result["external_tools_cancelled"])
        self.assertEqual(self.call({"action": "refresh", "handle": batch["handle"]})["error"], "session_closed")

    def test_validation_precedes_state_construction_and_rejects_injection(self):
        def forbidden_factory(*args):
            self.fail("invalid request constructed state")
        cases = [None, [], {}, {"action": "claim", "lease": "x"},
                 {"action": "begin", "lease": "x y"},
                 {"action": "begin", "lease": "x", "base": "/tmp/untrusted"},
                 {"action": "refresh", "handle": "../escape"},
                 {"action": "refresh", "handle": "a" * 32, "lease": "not-allowed"},
                 {"action": "reply", "handle": "a" * 32, "revision": True, "text": "x"},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": 1},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": ""},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": "x", "ids": ["a"]},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "media_path": "relative"}]
        for request in cases:
            with self.subTest(request=request):
                self.assertEqual(self.call(request, forbidden_factory)["error"], "invalid_request")
        self.assertFalse((Path(self.state_dir) / "muse-adapter").exists())

    def test_strict_json_unknown_handle_and_wrong_environment(self):
        for raw in ('{"action":"begin","action":"reply"}', '{"lease":NaN}', '[] []', ''):
            with self.subTest(raw=raw), self.assertRaises(adapter.InvalidRequest):
                adapter.parse_request(io.StringIO(raw))
        self.assertEqual(self.call({"action": "refresh", "handle": "a" * 32})["error"], "unknown_handle")
        _, batch = self.begin()
        result = adapter.handle_request({"action": "refresh", "handle": batch["handle"]},
                                        str(Path(self.tmp.name) / "different-bridge"), self.state_dir)
        self.assertEqual(result["error"], "invalid_session")

    def test_pure_media_reply_uses_same_core_queue(self):
        _, batch = self.begin()
        media = Path(self.tmp.name) / "synthetic.png"
        media.write_bytes(b"synthetic fixture, no real media")
        result = self.call({"action": "reply", "handle": batch["handle"], "revision": 1,
                            "media_path": str(media)})
        self.assertTrue(result["ok"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["media_path"], str(media.resolve()))
        self.assertEqual(job["text"], "")

    @staticmethod
    def diagnostic_item(index=0, source="param", outcome="failed", size=50380,
                        reason="ciphertext_not_block_aligned"):
        return {"item_index": index, "source": source, "outcome": outcome,
                "downloaded_bytes": size, "reason": reason}

    @staticmethod
    def diagnostic_snapshot(value):
        session = {"lease": "synthetic-private"}
        message = {"message_id": "a", "text": "available text",
                   "media_paths": ["/synthetic/available.png"], "media_failed": True,
                   "media_failed_count": 2, "media_diagnostics": value}
        result = adapter._snapshot(session, {
            "lease": session["lease"], "owned": ["a"], "messages": [message],
            "expires_at": 2000000090, "hard_deadline": 2000000570,
            "renew_after_seconds": 30})
        return session, result

    def test_optional_diagnostic_bad_containers_preserve_useful_message(self):
        invalid = [None, [], True, "private-untrusted-value", {},
                   {"version": True, "items": [], "truncated": False},
                   {"version": 1.0, "items": [], "truncated": False},
                   {"version": 2, "items": [], "truncated": False},
                   {"version": 1, "items": None, "truncated": False},
                   {"version": 1, "items": {}, "truncated": False},
                   {"version": 1, "items": [], "truncated": 0},
                   {"version": 1, "items": []}]
        for value in invalid:
            with self.subTest(value=value):
                _, result = self.diagnostic_snapshot(value)
                message = result["messages"][0]
                self.assertNotIn("media_diagnostics", message)
                self.assertEqual(message["text"], "available text")
                self.assertEqual(message["media_paths"], ["/synthetic/available.png"])
                self.assertIs(message["media_failed"], True)
                self.assertEqual(message["media_failed_count"], 2)

    def test_diagnostic_integer_bounds_enums_and_required_size(self):
        valid_items = [self.diagnostic_item(index=0, size=0),
                       self.diagnostic_item(index=999, size=104857633),
                       self.diagnostic_item(index=1, size=None)]
        clean = adapter._safe_media_diagnostics({
            "version": 1, "items": valid_items, "truncated": False})
        self.assertEqual(clean["items"], valid_items)
        self.assertFalse(clean["truncated"])
        bad_fields = [("item_index", value) for value in (True, 0.0, -1, 1000, None)]
        bad_fields += [("downloaded_bytes", value) for value in
                       (False, 1.0, -1, 104857634, float("nan"), float("inf"), "1")]
        bad_fields += [("source", value) for value in (None, False, [], "other")]
        bad_fields += [("outcome", value) for value in (None, [], "queued")]
        bad_fields += [("reason", value) for value in (None, [], "raw error", "ok")]
        for field, value in bad_fields:
            item = self.diagnostic_item()
            item[field] = value
            result = adapter._safe_media_diagnostics({
                "version": 1, "items": [item], "truncated": False})
            self.assertEqual(result, {"version": 1, "items": [], "truncated": True},
                             (field, value))
        missing_size = self.diagnostic_item()
        del missing_size["downloaded_bytes"]
        for item in (missing_size, self.diagnostic_item(outcome="success"), None, [], False):
            result = adapter._safe_media_diagnostics({
                "version": 1, "items": [item], "truncated": False})
            self.assertEqual(result["items"], [])
            self.assertTrue(result["truncated"])
        for source in ("param", "full_url", "unknown"):
            item = self.diagnostic_item(source=source, outcome="success", reason="ok")
            self.assertEqual(adapter._safe_media_diagnostics({
                "version": 1, "items": [item], "truncated": True}),
                {"version": 1, "items": [item], "truncated": True})

    def test_diagnostic_scan_retention_and_two_distinct_sources(self):
        items = [self.diagnostic_item(index=index) for index in range(33)]
        result = adapter._safe_media_diagnostics({
            "version": 1, "items": items, "truncated": False})
        self.assertEqual(result["items"], items[:16])
        self.assertTrue(result["truncated"])
        # The valid 33rd entry must not be inspected or retained.
        result = adapter._safe_media_diagnostics({
            "version": 1, "items": [None] * 32 + [self.diagnostic_item()], "truncated": False})
        self.assertEqual(result, {"version": 1, "items": [], "truncated": True})
        first = self.diagnostic_item()
        second = self.diagnostic_item(source="full_url", outcome="success", reason="ok", size=69)
        items = [dict(first, downloaded_bytes=-1), first, dict(first), second,
                 self.diagnostic_item(source="unknown"), self.diagnostic_item(index=1)]
        result = adapter._safe_media_diagnostics({
            "version": 1, "items": items, "truncated": False})
        self.assertEqual(result["items"], [first, second, items[-1]])
        self.assertTrue(result["truncated"])
        self.assertEqual(adapter._safe_media_diagnostics({
            "version": 1, "items": [], "truncated": False}),
            {"version": 1, "items": [], "truncated": False})

    def test_diagnostic_unknown_sensitive_subtrees_are_never_traversed(self):
        class Unreadable:
            def __getattribute__(self, name):
                raise AssertionError("unknown diagnostic data was inspected")

        item = dict(self.diagnostic_item(), url="SYNTHETIC-URL-SECRET",
                    key={"raw": "SYNTHETIC-KEY-SECRET", "nested": Unreadable()})
        value = {"version": 1, "items": [item], "truncated": False,
                 "context": Unreadable()}
        session, result = self.diagnostic_snapshot(value)
        expected = {"version": 1, "items": [self.diagnostic_item()], "truncated": False}
        self.assertEqual(result["messages"][0]["media_diagnostics"], expected)
        encoded = json.dumps(result)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("messages", session)
        self.assertNotIn("media_diagnostics", session)
        self.assertEqual(session["batch_digest"], hashlib.sha256(json.dumps(
            result["messages"], ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest())
        equivalent = dict(value, items=[self.diagnostic_item()], context="different secret")
        self.assertEqual(self.diagnostic_snapshot(equivalent)[0]["batch_digest"], session["batch_digest"])

    def test_diagnostic_compact_json_stays_within_byte_budget(self):
        items = [self.diagnostic_item(index=999 - index, source="full_url",
                                     size=104857633, reason="ciphertext_not_block_aligned")
                 for index in range(16)]
        result = adapter._safe_media_diagnostics({
            "version": 1, "items": items, "truncated": False})
        self.assertEqual(result["items"], items)
        self.assertLessEqual(len(json.dumps(result, separators=(",", ":")).encode("utf-8")), 4096)
        # Exercise the defensive byte check at a smaller budget; the V1 enums
        # and 16-item cap themselves already keep valid metadata below 4096.
        with patch.object(adapter, "MEDIA_DIAGNOSTIC_MAX_BYTES", 256):
            limited = adapter._safe_media_diagnostics({
                "version": 1, "items": items, "truncated": False})
        self.assertTrue(limited["truncated"])
        self.assertLessEqual(len(json.dumps(limited, separators=(",", ":")).encode("utf-8")), 256)
        self.assertEqual(limited["items"], items[:len(limited["items"])])

    def test_diagnostic_three_entries_preserve_counts_and_single_enqueue(self):
        def attach(mid, value):
            path = Path(self.base) / "inbox" / (mid + ".json")
            record = load_json(str(path))
            record.update(media_failed=True, media_failed_count=1,
                          media_paths=["/synthetic/available.png"], media_diagnostics=value)
            atomic_write_json(str(path), record)

        first = self.diagnostic_item()
        second = self.diagnostic_item(source="full_url", outcome="success", reason="ok", size=69)
        failed_attachment = self.diagnostic_item(index=1, source="unknown", size=None, reason="unknown")
        expected = {"version": 1, "items": [first, second, failed_attachment], "truncated": False}
        self.incoming("a", text="available text and one failed attachment")
        attach("a", dict(expected, private_url="SYNTHETIC-PRIVATE-URL"))
        event = self.state.plan()["payload"]
        begun = self.call({"action": "begin", "lease": event["lease"]})
        self.assertTrue(begun["ok"])
        refreshed = self.call({"action": "refresh", "handle": begun["handle"]})
        self.assertTrue(refreshed["ok"])
        self.incoming("b", text="later question")
        attach("b", None)
        pending = self.call({"action": "reply", "handle": begun["handle"],
                             "revision": refreshed["revision"], "text": "old answer"})
        self.assertEqual(pending["error"], "needs_refresh")
        for batch in (begun, refreshed, pending):
            message = batch["messages"][0]
            self.assertEqual(message["media_diagnostics"], expected)
            self.assertEqual(message["media_failed_count"], 1)
            self.assertEqual(message["media_paths"], ["/synthetic/available.png"])
            self.assertEqual(message["text"], "available text and one failed attachment")
        self.assertNotIn("media_diagnostics", pending["messages"][1])
        self.assertEqual(pending["messages"][1]["media_failed_count"], 1)
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))
        latest = self.call({"action": "refresh", "handle": begun["handle"]})
        result = self.call({"action": "reply", "handle": begun["handle"],
                            "revision": latest["revision"], "text": "complete final answer"})
        self.assertTrue(result["ok"])
        retry = self.call({"action": "reply", "handle": begun["handle"],
                           "revision": latest["revision"], "text": "do not repeat"})
        self.assertEqual(retry["error"], "session_closed")
        jobs = list((Path(self.base) / "outbox" / "pending").glob("*.json"))
        self.assertEqual(len(jobs), 1)
        job = load_json(str(jobs[0]))
        self.assertEqual(job["covers_message_ids"], ["a", "b"])
        self.assertEqual(job["text"], "complete final answer")
        self.assertNotIn("media_diagnostics", job)
        private = self.session(begun["handle"])
        self.assertNotIn("messages", private)
        self.assertNotIn("media_diagnostics", private)
        self.assertNotIn("SYNTHETIC-PRIVATE-URL", json.dumps(private))

    def test_cli_emits_one_json_and_no_raw_error_or_context(self):
        self.incoming("a")
        event = self.state.plan()["payload"]
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1")
        command = [sys.executable, str(ROOT / "hooks" / "muse-worker.py"),
                   "--base", self.base, "--state-dir", self.state_dir]
        result = subprocess.run(command, input=json.dumps({"action": "begin", "lease": event["lease"]}),
                                text=True, capture_output=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        parsed = json.loads(result.stdout)
        self.assertTrue(parsed["ok"])
        self.assertNotIn("SYNTHETIC-CONTEXT-SECRET", result.stdout + result.stderr)
        self.assertNotIn(self.session(parsed["handle"])["lease"], result.stdout + result.stderr)
        self.assertIn('"stage": "adopted"', result.stderr)
        bad = subprocess.run(command, input='{"action":"begin","lease":"SECRET with spaces"}',
                             text=True, capture_output=True, env=env)
        self.assertEqual(bad.returncode, 2)
        self.assertEqual(json.loads(bad.stdout)["error"], "invalid_request")
        self.assertNotIn("SECRET", bad.stdout + bad.stderr)
        missing = subprocess.run(command[:2] + ["--base", "SECRET"], input="{}",
                                 text=True, capture_output=True, env=env)
        self.assertEqual(json.loads(missing.stdout)["error"], "invalid_request")
        self.assertNotIn("SECRET", missing.stdout + missing.stderr)


class AttachmentAdapterTests(unittest.TestCase):
    """Frozen wire fixtures; real delivery and model pixels are separate gates."""
    setUp = MuseWorkerTests.setUp
    incoming = MuseWorkerTests.incoming
    call = MuseWorkerTests.call
    begin = MuseWorkerTests.begin
    session = MuseWorkerTests.session

    def install_wire(self):
        self.caps = {"schema_version": 1, "protocol": "attachment_inbox_v1", "version": 1,
                     "features": ["attachments_v1", "references_v1", "attachment_groups_v1"],
                     "limits": {"request_bytes": 65536, "response_bytes": 65536,
                                "query_limit": 20, "attachment_ids": 16, "reference_ordinals": 16}}
        self.context = {"schema_version": 1, "snapshot_id": "snapshot-original", "text_message_id": "a",
                        "cutoff_seq": 12, "group_id": "image-group-original", "login_epoch": "epoch-original",
                        "created_at": self.now - 2, "expires_at": self.now + 100,
                        "slots": [{"ordinal": 1, "message_id": "historical-message", "item_index": 0,
                                   "attachment_id": "att-1", "received_at": self.now - 3, "status": "ready"}],
                        "overflow": False, "total_positions": 1}
        for name in ("begin", "refresh"):
            original = getattr(self.state, name)
            def wrapped(*args, original=original):
                batch = original(*args)
                batch["capabilities"] = self.caps
                for message in batch.get("messages", []):
                    message["attachment_refs"] = []
                    message["reference_context"] = self.context if message["message_id"] == "a" else None
                return batch
            setattr(self.state, name, wrapped)
        self.calls = []
        self.response = None
        def attachment_call(ids, lease, request):
            self.calls.append((list(ids), lease, request))
            if self.response is not None:
                return self.response(request) if callable(self.response) else self.response
            return {"schema_version": 1, "action": request["action"], "ok": True,
                    "result": self.business_result(request), "error": None}
        self.state.attachment_call = attachment_call

    def metadata(self):
        return {"attachment_id": "att-1", "message_id": "historical-message", "item_index": 0,
                "received_at": self.now - 3, "kind": "image", "original_name": None,
                "name_source": "unknown", "declared_type": None, "detected_type": "image/png",
                "size": 3, "sha256": "a" * 64, "status": "ready", "decryption_state": "protocol_decoded",
                "preview_state": "readable", "tags": [], "expires_at": self.now + 100, "pinned": False}

    def material(self, reference=False, ordinal=1, attachment_id="att-1"):
        item = {"attachment_id": attachment_id,
                "path": str(Path(self.base) / "attachments" / "objects" / "synthetic.png"),
                "hold_id": "read-hold", "expires_at": self.now + 100}
        if reference:
            item.update(ordinal=ordinal, preview_state="readable", detected_type="image/png", size=3, sha256="a" * 64)
        return item

    def status(self):
        return {"schema_version": 1, "group_id": "delivery-group", "created_at": self.now,
                "state": "awaiting_continue", "attachment_count": 1, "accepted_attachment_ids": [],
                "pending_attachment_ids": ["att-1"], "unknown_attachment_ids": [],
                "units": {"total": 2, "accepted": 0, "pending": 2, "unknown": 0},
                "can_continue": True, "requires_explicit_continue": True}

    def business_result(self, request):
        action, args = request["action"], request["arguments"]
        if action == "query":
            return {"items": [self.metadata()], "next_cursor": None, "complete": True}
        if action == "get":
            return {"attachment": self.metadata(), "material": self.material()}
        if action == "reference":
            return {"reference_context": self.context, "materials": [self.material(True)],
                    "unavailable": [], "complete": True}
        if action == "release":
            return {"released": True}
        if action == "pin":
            return {"attachment": dict(self.metadata(), pinned=args["pinned"])}
        if action == "label":
            ids = args.get("attachment_ids", ["att-1"])
            return {"attachments": [dict(self.metadata(), attachment_id=aid,
                                          tags=[{"value": args["label"], "source": "user", "message_id": args["text_message_id"]}])
                                     for aid in ids]}
        if action == "cancel":
            group = self.status()
            group.update(state="cancelled", can_continue=False, pending_attachment_ids=[],
                         units={"total": 2, "accepted": 0, "pending": 0, "unknown": 0})
            return {"group": group}
        if action in ("reply", "continue"):
            return {"queued": True, "delivery_confirmed": False, "group_id": "delivery-group",
                    "job_id": "page-job", "status": self.status()}
        return {"group": self.status()} if "group_id" in args else {"groups": [self.status()], "next_cursor": None}

    def request(self, batch, action, **arguments):
        return {"action": "attachment", "handle": batch["handle"], "revision": batch["revision"],
                "request": {"schema_version": 1, "action": action, "arguments": arguments}}

    def error_envelope(self, action, code, retryable=False):
        return {"schema_version": 1, "action": action, "ok": False, "result": None,
                "error": {"code": code, "retryable": retryable, "refresh_required": code == "needs_refresh"}}

    def test_capabilities_reference_projection_and_no_historical_ownership(self):
        self.install_wire()
        class Unreadable:
            def __iter__(self):
                raise AssertionError("unknown subtree traversed")
            def __str__(self):
                raise AssertionError("unknown subtree printed")
        self.caps["private"] = Unreadable()
        self.context["token"] = Unreadable()
        self.context["slots"][0]["private"] = Unreadable()
        _, batch = self.begin()
        self.assertEqual(batch["capabilities"]["limits"]["request_bytes"], 65536)
        self.assertNotIn("private", batch["capabilities"])
        self.assertNotIn("token", batch["messages"][0]["reference_context"])
        self.assertEqual(batch["owned_count"], 1)
        private = self.session(batch["handle"])
        self.assertEqual(private["owned"], ["a"])
        self.assertNotIn("historical-message", json.dumps(private))
        self.assertNotIn("snapshot-original", json.dumps(private))
        self.assertNotIn(private["lease"], json.dumps(batch))

    def test_reference_state_transition_keeps_identity_and_digest(self):
        self.install_wire()
        self.context["slots"][0]["status"] = "pending"
        _, batch = self.begin()
        digest = self.session(batch["handle"])["batch_digest"]
        self.context["slots"][0]["status"] = "ready"
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(self.session(batch["handle"])["batch_digest"], digest)
        self.assertEqual(refreshed["revision"], 2)
        self.context["snapshot_id"] = "late-image-must-not-replace"
        bad = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertEqual(bad["error"], "invalid_state_result")

    def test_effective_policy_is_optional_and_zero_is_not_a_default(self):
        self.install_wire()
        _, batch = self.begin()
        self.assertNotIn("policy", batch["capabilities"])
        for policy in (None, {}, {"ambiguity_seconds": True}, {"ambiguity_seconds": -1},
                       {"ambiguity_seconds": "900"}, {"ambiguity_seconds": 1.5}):
            self.caps["policy"] = policy
            batch = self.call({"action": "refresh", "handle": batch["handle"]})
            self.assertTrue(batch["ok"], batch)
            self.assertNotIn("policy", batch["capabilities"])
            result = self.call(self.request(batch, "reference", text_message_id="a", snapshot_id="snapshot-original"))
            self.assertTrue(result["ok"], result)  # Explicit reference remains usable.
        for seconds in (0, 37, 900):
            self.caps["policy"] = {"ambiguity_seconds": seconds, "private": object()}
            batch = self.call({"action": "refresh", "handle": batch["handle"]})
            self.assertEqual(batch["capabilities"]["policy"], {"ambiguity_seconds": seconds})
            self.assertNotIn("private", batch["capabilities"]["policy"])
        slot = batch["messages"][0]["reference_context"]["slots"][0]
        self.assertEqual(slot["received_at"], self.now - 3)
        self.assertNotEqual(slot["received_at"], batch["messages"][0]["reference_context"]["created_at"])

    def test_frozen_public_enums_reject_internal_or_invented_values(self):
        self.install_wire()
        for field, valid, bad in (("kind", ("image", "video", "file", "voice"), "audio"),
                                  ("status", ("pending", "ready", "failed", "quota", "missing"), "prepared"),
                                  ("decryption_state", ("pending", "unavailable", "protocol_decoded", "content_verified", "plaintext_verified"), "guessed"),
                                  ("preview_state", ("readable", "unavailable", "invalid", "resource_limited"), "available")):
            for value in valid:
                self.assertEqual(adapter._metadata(dict(self.metadata(), **{field: value}))[field], value)
            with self.assertRaises(adapter.SessionError):
                adapter._metadata(dict(self.metadata(), **{field: bad}))
        for source in ("user", "batch_text", "original_name", "inferred"):
            metadata = dict(self.metadata(), tags=[{"value": "tag", "source": source, "message_id": None}])
            self.assertEqual(adapter._metadata(metadata)["tags"][0]["source"], source)
        with self.assertRaises(adapter.SessionError):
            adapter._metadata(dict(self.metadata(), tags=[{"value": "tag", "source": "confirmed_by_ai", "message_id": None}]))

    def test_all_read_actions_keep_handle_revision_deadline_and_code_owned_ids(self):
        self.install_wire()
        _, batch = self.begin()
        actions = [("query", {}), ("get", {"attachment_id": "att-1"}),
                   ("reference", {"text_message_id": "a", "snapshot_id": "snapshot-original", "ordinals": [1]}),
                   ("release", {"hold_id": "read-hold"}), ("pin", {"attachment_id": "att-1", "pinned": True}),
                   ("status", {}), ("status", {"group_id": "delivery-group"})]
        for action, args in actions:
            with self.subTest(action=action, args=args):
                result = self.call(self.request(batch, action, **args))
                self.assertTrue(result["ok"], result)
                self.assertEqual(set(result), {"ok", "handle", "revision", "result"})
                self.assertEqual(result["revision"], batch["revision"])
                private = self.session(batch["handle"])
                self.assertEqual(private["phase"], "active")
                self.assertEqual(private["expires_at"], batch["expires_at"])
                self.assertEqual(self.calls[-1][0], ["a"])
                self.assertEqual(self.calls[-1][1], private["lease"])
                self.assertNotIn(private["lease"], json.dumps(result))
        self.assertNotIn("refresh", self.session(batch["handle"])["call_timings"])
        # Releasing a read hold is distinct from releasing the worker.
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "done"})["ok"])

    def test_label_feature_is_optional_and_absence_never_claims_success(self):
        self.install_wire()
        _, batch = self.begin(text="这是房东图")
        self.assertNotIn("attachment_labels_v1", batch["capabilities"]["features"])
        request = self.request(batch, "label", text_message_id="a", label="房东", attachment_ids=["att-1"])
        result = self.call(request)
        self.assertEqual(result["error"], "attachment_unavailable")
        self.assertFalse(self.calls)
        self.assertEqual(self.session(batch["handle"])["phase"], "active")
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "not labeled"})["ok"])

    def test_label_both_selection_shapes_use_private_owned_and_keep_task_active(self):
        self.install_wire()
        self.caps["features"].append("attachment_labels_v1")
        _, batch = self.begin(text="这是房东图，请标记房东")
        self.assertIn("attachment_labels_v1", batch["capabilities"]["features"])
        selections = [{"attachment_ids": ["att-1", "att-2"]},
                      {"snapshot_id": "snapshot-original", "ordinals": [1]}]
        for selection in selections:
            result = self.call(self.request(batch, "label", text_message_id="a", label="房东", **selection))
            self.assertTrue(result["ok"], result)
            metadata = result["result"]["attachments"][0]
            self.assertIn({"value": "房东", "source": "user", "message_id": "a"}, metadata["tags"])
            self.assertNotIn("path", metadata)
            self.assertNotIn("model_read", json.dumps(result))
            private = self.session(batch["handle"])
            self.assertEqual(private["phase"], "active")
            self.assertEqual(private["revision"], batch["revision"])
            self.assertEqual(private["expires_at"], batch["expires_at"])
            self.assertEqual(self.calls[-1][0], ["a"])
            self.assertEqual(self.calls[-1][1], private["lease"])
            self.assertNotIn(private["lease"], json.dumps(result))
        nonowned = self.call(self.request(batch, "label", text_message_id="historical-message", label="房东", attachment_ids=["att-1"]))
        self.assertEqual(nonowned["error"], "invalid_request")
        self.assertEqual(len(self.calls), 2)
        self.assertNotIn("refresh", self.session(batch["handle"])["call_timings"])
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "index operation done"})["ok"])

    def test_label_rejects_selection_conflicts_and_unbounded_inputs_before_state(self):
        def forbidden(*args):
            self.fail("invalid label reached worker")
        batch = {"handle": "a" * 32, "revision": 1}
        cases = [{}, {"snapshot_id": "s"}, {"ordinals": [1]},
                 {"attachment_ids": ["att-1"], "snapshot_id": "s", "ordinals": [1]},
                 {"attachment_ids": []}, {"attachment_ids": ["att-1", "att-1"]},
                 {"attachment_ids": [str(i) for i in range(17)]},
                 {"snapshot_id": "s", "ordinals": [True]},
                 {"snapshot_id": "s", "ordinals": [1, 1]},
                 {"attachment_ids": ["att-1"], "path": "/private/any"}]
        for selection in cases:
            request = self.request(batch, "label", text_message_id="a", label="房东", **selection)
            self.assertEqual(self.call(request, forbidden)["error"], "invalid_request")
        for label in ("", "字" * 129, None, True):
            request = self.request(batch, "label", text_message_id="a", label=label, attachment_ids=["att-1"])
            self.assertEqual(self.call(request, forbidden)["error"], "invalid_request")
        adapter.validate_request(self.request(batch, "label", text_message_id="a", label="字" * 128,
                                              snapshot_id="s", ordinals=[1]))

    def test_label_result_requires_requested_ids_count_and_user_evidence(self):
        self.install_wire()
        self.caps["features"].append("attachment_labels_v1")
        _, batch = self.begin(text="请标注房东")
        request = self.request(batch, "label", text_message_id="a", label="房东", attachment_ids=["att-1"])
        correct = {"value": "房东", "source": "user", "message_id": "a"}
        bad_metadata = [dict(self.metadata(), attachment_id="different-id", tags=[correct]),
                        dict(self.metadata(), tags=[dict(correct, source="inferred")]),
                        dict(self.metadata(), tags=[dict(correct, message_id="historical-message")]),
                        dict(self.metadata(), tags=[dict(correct, value="房客")]),
                        dict(self.metadata(), tags=[])]
        for metadata in bad_metadata:
            self.response = {"schema_version": 1, "action": "label", "ok": True, "error": None,
                             "result": {"attachments": [metadata]}}
            result = self.call(request)
            self.assertEqual(result["error"], "storage_invalid")
            self.assertNotIn("attachments", result)
        self.response = {"schema_version": 1, "action": "label", "ok": True, "error": None,
                         "result": {"attachments": []}}
        self.assertEqual(self.call(request)["error"], "storage_invalid")
        # C.2 guarantees the selected attachments, not a metadata result order.
        # Match the full unique selection and preserve the public return order.
        metadata = [dict(self.metadata(), attachment_id=aid, tags=[correct]) for aid in ("att-2", "att-1")]
        self.response = {"schema_version": 1, "action": "label", "ok": True, "error": None,
                         "result": {"attachments": metadata}}
        request = self.request(batch, "label", text_message_id="a", label="房东", attachment_ids=["att-1", "att-2"])
        result = self.call(request)
        self.assertTrue(result["ok"], result)
        self.assertEqual([item["attachment_id"] for item in result["result"]["attachments"]], ["att-2", "att-1"])

    def test_pending_label_projection_does_not_claim_reading_and_failure_never_retries(self):
        self.install_wire()
        self.caps["features"].append("attachment_labels_v1")
        _, batch = self.begin(text="这是房东图")
        metadata = dict(self.metadata(), status="pending", decryption_state="pending", preview_state="unavailable",
                        size=None, sha256=None, tags=[{"value": "房东", "source": "user", "message_id": "a"}])
        self.response = {"schema_version": 1, "action": "label", "ok": True, "error": None,
                         "result": {"attachments": [metadata], "private": object()}}
        request = self.request(batch, "label", text_message_id="a", label="房东", attachment_ids=["att-1"])
        result = self.call(request)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["result"]["attachments"][0]["status"], "pending")
        self.assertNotIn("private", json.dumps(result))
        def fail(request):
            raise OSError("SENSITIVE-INDEX-ERROR")
        self.response = fail
        result = self.call(request)
        self.assertEqual(result["error"], "storage_invalid")
        self.assertEqual(len(self.calls), 2)  # Only the two explicit requests.
        self.assertNotIn("SENSITIVE", json.dumps(result))
        self.assertEqual(self.session(batch["handle"])["phase"], "active")

    def test_cancel_feature_absence_never_claims_cancelled(self):
        self.install_wire()
        _, batch = self.begin(text="请停止发送这个组")
        request = self.request(batch, "cancel", text_message_id="a", group_id="delivery-group")
        result = self.call(request)
        self.assertEqual(result["error"], "attachment_unavailable")
        self.assertNotIn("result", result)
        self.assertFalse(self.calls)
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1,
                                  "text": "cancel capability unavailable"})["ok"])

    def test_cancel_exact_arguments_and_current_owned_text_are_required(self):
        def forbidden(*args):
            self.fail("invalid cancel constructed state")
        batch = {"handle": "a" * 32, "revision": 1}
        for args in ({}, {"group_id": "delivery-group"}, {"text_message_id": "a"},
                     {"text_message_id": "a", "group_id": "delivery-group", "allow_cancel": True},
                     {"text_message_id": "a", "group_id": "delivery-group", "context": "private"},
                     {"text_message_id": True, "group_id": "delivery-group"}):
            self.assertEqual(self.call(self.request(batch, "cancel", **args), forbidden)["error"], "invalid_request")
        self.install_wire()
        self.caps["features"].append("attachment_cancel_v1")
        _, batch = self.begin(text="请取消这个组")
        result = self.call(self.request(batch, "cancel", text_message_id="historical-message", group_id="delivery-group"))
        self.assertEqual(result["error"], "invalid_request")
        self.assertFalse(self.calls)

    def test_cancel_preserves_accepted_unknown_without_closing_or_renewing(self):
        self.install_wire()
        self.caps["features"].append("attachment_cancel_v1")  # Independent of label support.
        _, batch = self.begin(text="请取消这个组")
        self.assertIn("attachment_cancel_v1", batch["capabilities"]["features"])
        self.assertNotIn("attachment_labels_v1", batch["capabilities"]["features"])
        group = dict(self.status(), state="cancelled", can_continue=False,
                     accepted_attachment_ids=["att-1"], pending_attachment_ids=[],
                     units={"total": 2, "accepted": 1, "pending": 0, "unknown": 1}, private=object())
        self.response = {"schema_version": 1, "action": "cancel", "ok": True, "error": None,
                         "result": {"group": group}}
        result = self.call(self.request(batch, "cancel", text_message_id="a", group_id="delivery-group"))
        self.assertTrue(result["ok"], result)
        status = result["result"]["group"]
        self.assertEqual(status["accepted_attachment_ids"], ["att-1"])
        self.assertEqual(status["units"]["unknown"], 1)
        self.assertFalse(status["can_continue"])
        self.assertNotIn("private", status)
        self.assertNotIn("revoked", result)
        self.assertEqual(result["revision"], batch["revision"])
        private = self.session(batch["handle"])
        self.assertEqual(private["phase"], "active")
        self.assertEqual(private["expires_at"], batch["expires_at"])
        self.assertNotIn("refresh", private["call_timings"])
        self.assertEqual(self.calls[-1][0], ["a"])
        self.assertEqual(self.calls[-1][1], private["lease"])
        self.assertNotIn(private["lease"], json.dumps(result))
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1,
                                  "text": "accepted and unknown remain"})["ok"])

    def test_cancel_rejects_wrong_group_and_cancelled_continue_permission(self):
        self.install_wire()
        self.caps["features"].append("attachment_cancel_v1")
        _, batch = self.begin(text="请取消这个组")
        request = self.request(batch, "cancel", text_message_id="a", group_id="delivery-group")
        groups = [dict(self.status(), group_id="another-group"),
                  dict(self.status(), state="cancelled", can_continue=True)]
        for group in groups:
            self.response = {"schema_version": 1, "action": "cancel", "ok": True, "error": None,
                             "result": {"group": group}}
            result = self.call(request)
            self.assertEqual(result["error"], "storage_invalid")
            self.assertNotIn("group", result)

    def test_cancel_needs_refresh_and_index_exception_never_auto_repeat(self):
        self.install_wire()
        self.caps["features"].append("attachment_cancel_v1")
        _, batch = self.begin(text="请取消这个组")
        self.incoming("b", text="后来消息")
        self.response = self.error_envelope("cancel", "needs_refresh")
        request = self.request(batch, "cancel", text_message_id="a", group_id="delivery-group")
        result = self.call(request)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertTrue(result["requires_refresh"])
        self.assertEqual(result["revision"], batch["revision"])
        self.assertEqual(self.call(request)["error"], "needs_refresh")
        self.assertEqual(len(self.calls), 1)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        def fail(request):
            raise OSError("PRIVATE-CANCEL-ERROR")
        self.response = fail
        request = self.request(refreshed, "cancel", text_message_id="a", group_id="delivery-group")
        result = self.call(request)
        self.assertEqual(result["error"], "storage_invalid")
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.session(batch["handle"])["phase"], "active")

    def test_reference_original_second_slot_and_partial_unavailable(self):
        self.install_wire()
        first = self.context["slots"][0]
        first["status"] = "failed"
        self.context["slots"].append(dict(first, ordinal=2, attachment_id="att-2", status="ready", item_index=1))
        self.context["total_positions"] = 2
        _, batch = self.begin()
        self.response = {"schema_version": 1, "action": "reference", "ok": True, "error": None,
                         "result": {"reference_context": self.context, "materials": [self.material(True, 2, "att-2")],
                                    "unavailable": [{"ordinal": 1, "attachment_id": "att-1", "status": "failed"}], "complete": False}}
        result = self.call(self.request(batch, "reference", text_message_id="a", snapshot_id="snapshot-original"))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["result"]["materials"][0]["ordinal"], 2)
        self.assertFalse(result["result"]["complete"])
        self.assertNotIn("model_read", json.dumps(result))

    def test_reference_rejects_wrong_snapshot_members_and_nonowned_text(self):
        self.install_wire()
        _, batch = self.begin()
        bad = self.call(self.request(batch, "reference", text_message_id="historical-message", snapshot_id="snapshot-original"))
        self.assertEqual(bad["error"], "attachment_unavailable")
        self.assertFalse(self.calls)
        raw = self.business_result({"action": "reference", "arguments": {}})
        raw["materials"][0]["attachment_id"] = "different-image"
        self.response = {"schema_version": 1, "action": "reference", "ok": True, "result": raw, "error": None}
        bad = self.call(self.request(batch, "reference", text_message_id="a", snapshot_id="snapshot-original"))
        self.assertEqual(bad["error"], "storage_invalid")
        self.assertNotIn("materials", bad)

    def assert_publish_once(self, action):
        self.install_wire()
        _, batch = self.begin()
        args = {"attachment_ids": ["att-1"], "caption": "one caption"} if action == "reply" else {"group_id": "delivery-group"}
        request = self.request(batch, action, **args)
        result = self.call(request)
        self.assertTrue(result["ok"], result)
        self.assertTrue(result["result"]["queued"])
        self.assertFalse(result["result"]["delivery_confirmed"])
        self.assertEqual(self.session(batch["handle"])["phase"], "queued")
        self.assertEqual(self.call(request)["error"], "session_closed")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "again"})["error"], "session_closed")

    def test_attachment_reply_is_one_logical_submission(self):
        self.assert_publish_once("reply")

    def test_explicit_continue_closes_only_after_queued(self):
        self.assert_publish_once("continue")

    def test_attachment_needs_refresh_and_stale_revision_never_resubmit(self):
        self.install_wire()
        _, batch = self.begin()
        self.incoming("b", text="later text")
        self.response = self.error_envelope("reply", "needs_refresh")
        old = self.request(batch, "reply", attachment_ids=["att-1"])
        result = self.call(old)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertTrue(result["requires_refresh"])
        self.assertFalse(result["must_stop"])
        self.assertEqual(result["revision"], 1)
        self.assertEqual(self.call(old)["error"], "needs_refresh")
        self.assertEqual(len(self.calls), 1)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertEqual([item["message_id"] for item in refreshed["messages"]], ["a", "b"])
        self.assertEqual(self.call(old)["error"], "revision_mismatch")
        self.response = None
        self.assertTrue(self.call(self.request(refreshed, "reply", attachment_ids=["att-1"]))["ok"])
        self.assertEqual(self.calls[-1][0], ["a", "b"])

    def test_attachment_publish_exception_and_storage_error_are_unknown_no_replay(self):
        self.install_wire()
        _, batch = self.begin()
        def fail(request):
            self.state.reply(["a"], "already published", self.session(batch["handle"])["lease"])
            raise OSError("PRIVATE-LEASE-AND-CONTEXT")
        self.response = fail
        request = self.request(batch, "reply", attachment_ids=["att-1"])
        result = self.call(request)
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)

    def test_crash_and_invalid_publish_result_preserve_durable_marker(self):
        self.install_wire()
        _, batch = self.begin()
        def crash(request):
            self.assertEqual(self.session(batch["handle"])["phase"], "replying")
            raise KeyboardInterrupt()
        self.response = crash
        request = self.request(batch, "continue", group_id="delivery-group")
        with self.assertRaises(KeyboardInterrupt):
            self.call(request)
        self.assertIsNone(self.session(batch["handle"])["call_timings"]["reply"]["returned_at"])
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)

    def test_known_business_error_is_explicit_not_an_automatic_retry(self):
        self.install_wire()
        _, batch = self.begin()
        self.response = self.error_envelope("query", "storage_busy", True)
        result = self.call(self.request(batch, "query"))
        self.assertEqual(result["error"], "storage_busy")
        self.assertTrue(result["retryable"])
        self.assertFalse(result["must_stop"])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.session(batch["handle"])["phase"], "active")

    def test_lease_loss_fences_all_further_attachment_calls(self):
        self.install_wire()
        _, batch = self.begin()
        self.response = self.error_envelope("get", "lease_lost")
        request = self.request(batch, "get", attachment_id="att-1")
        self.assertTrue(self.call(request)["must_stop"])
        self.assertEqual(self.call(request)["error"], "lease_lost")
        self.assertEqual(len(self.calls), 1)

    def test_legacy_without_capabilities_keeps_ordinary_reply(self):
        # Exercise an explicitly old package surface even after combination
        # installs a newer public WorkerState in this same test environment.
        original_begin = self.state.begin
        def legacy_begin(*args):
            batch = original_begin(*args)
            batch.pop("capabilities", None)
            for message in batch.get("messages", []):
                message.pop("attachment_refs", None)
                message.pop("reference_context", None)
            return batch
        self.state.begin = legacy_begin
        _, batch = self.begin()
        self.assertNotIn("capabilities", batch)
        result = self.call(self.request(batch, "query"))
        self.assertEqual(result["error"], "attachment_unavailable")
        self.assertFalse(result["must_stop"])
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "legacy"})["ok"])

    def test_capabilities_do_not_limit_legacy_large_text_begin_refresh_or_reply(self):
        self.install_wire()
        long_text = "长正文" * 10000
        self.assertGreater(len(long_text.encode("utf-8")), 65536)
        _, batch = self.begin(text=long_text)
        self.assertEqual(batch["messages"][0]["text"], long_text)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(refreshed["messages"][0]["text"], long_text)
        result = self.call({"action": "reply", "handle": batch["handle"],
                            "revision": refreshed["revision"], "text": "read full body"})
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["delivery_confirmed"])

    def test_many_legal_reference_records_have_no_new_aggregate_batch_limit(self):
        self.install_wire()
        template = self.context["slots"][0]
        self.context["slots"] = [dict(template, ordinal=i, item_index=i - 1, attachment_id="att-" + str(i))
                                 for i in range(1, 17)]
        self.context["total_positions"] = 16
        for name in ("begin", "refresh"):
            original = getattr(self.state, name)
            def all_references(*args, original=original):
                result = original(*args)
                for message in result.get("messages", []):
                    mid = message["message_id"]
                    message["reference_context"] = dict(self.context, text_message_id=mid, snapshot_id="snapshot-" + mid)
                return result
            setattr(self.state, name, all_references)
        for index in range(50):
            self.incoming("message-%03d" % index)
        _, batch = self.begin()
        projection = [message["reference_context"] for message in batch["messages"]]
        self.assertGreater(len(json.dumps(projection).encode("utf-8")), 65536)
        self.assertEqual(batch["owned_count"], 51)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(refreshed["owned_count"], 51)
        self.assertEqual(self.session(batch["handle"])["owned"], [item["message_id"] for item in batch["messages"]])

    def test_incoming_refs_above_reply_limit_preserve_all_positions_and_handle(self):
        self.install_wire()
        template = self.context["slots"][0]
        self.context["slots"] = [dict(template, ordinal=i, item_index=i - 1, attachment_id="att-" + str(i))
                                 for i in range(1, 17)]
        self.context.update(overflow=True, total_positions=17)
        refs = [{"attachment_id": "att-" + str(i), "item_index": i - 1, "kind": "image", "status": "ready"}
                for i in range(1, 18)]
        for name in ("begin", "refresh"):
            original = getattr(self.state, name)
            def incoming_refs(*args, original=original):
                result = original(*args)
                result["messages"][0]["attachment_refs"] = refs
                return result
            setattr(self.state, name, incoming_refs)
        _, batch = self.begin()
        self.assertEqual(batch["messages"][0]["attachment_refs"], refs)
        self.assertEqual(batch["messages"][0]["reference_context"]["total_positions"], 17)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(refreshed["messages"][0]["attachment_refs"], refs)
        self.assertEqual(self.session(batch["handle"])["owned"], ["a"])
        request = self.request(refreshed, "reply", attachment_ids=[ref["attachment_id"] for ref in refs])
        self.assertEqual(self.call(request)["error"], "invalid_request")  # Outgoing cap remains16.
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"],
                                  "revision": refreshed["revision"], "text": "all positions retained"})["ok"])

    def test_single_reference_above_call_budget_preserves_begin_and_refresh(self):
        self.install_wire()
        template = self.context["slots"][0]
        self.context["slots"] = [dict(template, ordinal=i, item_index=i - 1,
                                      attachment_id="att-%d-" % i + "x" * 5000) for i in range(1, 17)]
        self.context["total_positions"] = 16
        adapter._reference_context(self.context, "a")  # All individual fields are legal.
        self.assertGreater(len(json.dumps(self.context).encode("utf-8")), 65536)
        _, batch = self.begin()
        self.assertEqual(batch["messages"][0]["reference_context"], self.context)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(refreshed["messages"][0]["reference_context"], self.context)
        self.assertLess(len(json.dumps(self.session(batch["handle"])).encode("utf-8")), 10000)
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"],
                                  "revision": refreshed["revision"], "text": "reference retained"})["ok"])

    def test_reference_overflow_uses_retained_slots_for_all_policy_widths(self):
        self.install_wire()
        template = self.context["slots"][0]
        # Snapshot width is evidence in the record, not the fixed wire limit or
        # the policy currently used for a different, later snapshot.
        for width in (1, 2, 16, 2):
            slots = [dict(template, ordinal=i, item_index=i - 1, attachment_id="att-" + str(i),
                          status="failed" if i == 1 else "pending") for i in range(1, width + 1)]
            for total in (width, width + 1):
                with self.subTest(width=width, total=total):
                    context = dict(self.context, slots=slots, total_positions=total, overflow=total > width)
                    self.assertEqual(adapter._reference_context(context, "a"), context)

    def test_short_overflow_snapshot_survives_begin_refresh_and_reference(self):
        self.install_wire()
        first = self.context["slots"][0]
        self.context.update(slots=[first, dict(first, ordinal=2, item_index=1,
                                              attachment_id="att-2", status="failed")],
                            total_positions=3, overflow=True)
        _, batch = self.begin()
        self.assertEqual(batch["messages"][0]["reference_context"], self.context)
        self.assertEqual(self.session(batch["handle"])["owned"], ["a"])
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.assertEqual(refreshed["messages"][0]["reference_context"], self.context)
        referenced = self.call(self.request(refreshed, "reference", text_message_id="a",
                                            snapshot_id=self.context["snapshot_id"], ordinals=[1]))
        self.assertTrue(referenced["ok"], referenced)
        self.assertEqual(referenced["result"]["reference_context"], self.context)
        self.assertEqual(referenced["result"]["reference_context"]["slots"][1]["status"], "failed")
        self.assertTrue(self.call(self.request(refreshed, "release", hold_id="read-hold"))["ok"])
        self.assertTrue(self.call({"action": "reply", "handle": batch["handle"],
                                  "revision": refreshed["revision"], "text": "fixed snapshot retained"})["ok"])

    def test_reference_projection_does_not_reindex_retained_original_ordinals(self):
        self.install_wire()
        template = self.context["slots"][0]
        slots = [dict(template, ordinal=i, item_index=i - 1, attachment_id="att-" + str(i))
                 for i in (1, 2, 4)]
        context = dict(self.context, slots=slots, total_positions=4, overflow=True)
        projected = adapter._reference_context(context, "a")
        self.assertEqual(projected, context)
        self.assertEqual([slot["ordinal"] for slot in projected["slots"]], [1, 2, 4])

    def test_short_snapshot_rejects_forged_overflow_types_and_original_order(self):
        self.install_wire()
        first = self.context["slots"][0]
        second = dict(first, ordinal=2, item_index=1, attachment_id="att-2")
        valid = dict(self.context, slots=[first, second], total_positions=3, overflow=True)
        invalid = [dict(valid, overflow=False), dict(valid, total_positions=2),
                   dict(valid, total_positions=1), dict(valid, total_positions=True),
                   dict(valid, overflow=1), dict(valid, slots=[second, first]),
                   dict(valid, slots=[first, first]),
                   dict(valid, slots=[first, dict(second, ordinal=0)]),
                   dict(valid, slots=[first, dict(second, ordinal=17)]),
                   dict(valid, slots=[first] * 17), dict(valid, slots="not-a-list")]
        for index, context in enumerate(invalid):
            with self.subTest(case=index):
                with self.assertRaises(adapter.SessionError):
                    adapter._reference_context(context, "a")

    def test_wire_utf8_limit_is_measured_with_the_stdout_encoding(self):
        # A compact object just below the budget must not grow through pretty
        # printing. Existing message bodies use their separate legacy budget.
        response = {"ok": True, "result": {"x": ""}}
        size = len(json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        response["result"]["x"] = "x" * (65535 - size)
        adapter._attachment_size(response)
        output = io.StringIO()
        with patch.object(adapter, "handle_request", return_value=response), patch.object(adapter.sys, "stdin", io.StringIO('{}')), patch.object(adapter.sys, "stdout", output):
            self.assertEqual(adapter.main(["--base", self.base, "--state-dir", self.state_dir]), 0)
        self.assertEqual(len(output.getvalue().encode("utf-8")), 65536)

    def test_mutating_storage_error_closes_unknown_outcome_without_retry(self):
        self.install_wire()
        _, batch = self.begin()
        self.response = self.error_envelope("continue", "storage_invalid")
        request = self.request(batch, "continue", group_id="delivery-group")
        result = self.call(request)
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertEqual(self.session(batch["handle"])["phase"], "uncertain")
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)

    def test_unknown_mutating_success_shape_cannot_reopen_handle(self):
        self.install_wire()
        _, batch = self.begin()
        raw = self.business_result({"action": "reply", "arguments": {}})
        raw["queued"] = 1
        self.response = {"schema_version": 1, "action": "reply", "ok": True, "error": None, "result": raw}
        request = self.request(batch, "reply", attachment_ids=["att-1"], caption="中" * 16000)
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)

    def test_attachment_inputs_reject_unknowns_bool_duplicates_and_hard_bounds_before_state(self):
        def forbidden(*args):
            self.fail("invalid attachment request reached state")
        batch = {"handle": "a" * 32, "revision": 1}
        cases = [self.request(batch, "query", limit=True), self.request(batch, "query", limit=21),
                 self.request(batch, "query", filters={"scope": "private"}),
                 self.request(batch, "query", filters={"kind": "audio"}),
                 self.request(batch, "query", filters={"status": "prepared"}),
                 self.request(batch, "query", filters={"tag": "字" * 129}),
                 self.request(batch, "get", attachment_id="att-1", path="/private"),
                 self.request(batch, "release", hold_id="hold", lease="private"),
                 self.request(batch, "pin", attachment_id="att-1", pinned=1),
                 self.request(batch, "reply", attachment_ids=["x", "x"]),
                 self.request(batch, "reply", attachment_ids=[str(i) for i in range(17)]),
                 self.request(batch, "reply", attachment_ids=["x"], caption="x" * 16001),
                 self.request(batch, "reference", text_message_id="a", snapshot_id="s", ordinals=[True]),
                 self.request(batch, "reference", text_message_id="a", snapshot_id="s", ordinals=[2, 2]),
                 self.request(batch, "reference", text_message_id="a", snapshot_id="s", ordinals=[17]),
                 self.request(batch, "get", attachment_id="字" * 30000)]
        unknown = self.request(batch, "query")
        unknown["request"]["schema_version"] = 2
        cases.append(unknown)
        for request in cases:
            with self.subTest(request_action=request["request"]["action"]):
                self.assertEqual(self.call(request, forbidden)["error"], "invalid_request")
        raw = json.dumps(self.request(batch, "query")) + " " * 65536
        with self.assertRaises(adapter.InvalidRequest):
            adapter.parse_request(io.StringIO(raw))
        self.assertFalse((Path(self.state_dir) / "muse-adapter").exists())

    def test_unknown_reference_schema_missing_required_and_overflow_do_not_disappear(self):
        self.install_wire()
        for bad in (dict(self.context, schema_version=2),
                    {key: value for key, value in self.context.items() if key != "cutoff_seq"},
                    dict(self.context, slots=self.context["slots"] * 17),
                    dict(self.context, overflow=False, total_positions=17)):
            with self.subTest(keys=list(bad)):
                with self.assertRaises(adapter.SessionError):
                    adapter._reference_context(bad, "a")
        self.caps["schema_version"] = True
        self.assertEqual(self.begin_invalid()["error"], "unsupported_version")

    def begin_invalid(self):
        self.incoming("a")
        return self.call({"action": "begin", "lease": self.state.plan()["payload"]["lease"]})

    def test_result_projection_never_traverses_unknown_sensitive_objects(self):
        self.install_wire()
        _, batch = self.begin()
        class Unreadable:
            def __iter__(self):
                raise AssertionError("sensitive subtree traversed")
        metadata = dict(self.metadata(), context=Unreadable(), object_key=Unreadable())
        metadata["tags"] = [{"value": "known", "source": "user", "message_id": None, "private": Unreadable()}]
        self.response = {"schema_version": 1, "action": "query", "ok": True, "error": None,
                         "result": {"items": [metadata], "next_cursor": None, "complete": True, "private": Unreadable()},
                         "lease": self.session(batch["handle"])["lease"]}
        result = self.call(self.request(batch, "query"))
        self.assertTrue(result["ok"], result)
        encoded = json.dumps(result)
        for forbidden in ("object_key", "context", "private", self.session(batch["handle"])["lease"]):
            self.assertNotIn(forbidden, encoded)

    def test_bad_paths_oversized_results_and_unknown_caption_are_not_success(self):
        self.install_wire()
        _, batch = self.begin()
        raw = self.business_result({"action": "get", "arguments": {}})
        raw["material"]["path"] = "/etc/passwd"
        self.response = {"schema_version": 1, "action": "get", "ok": True, "error": None, "result": raw}
        self.assertEqual(self.call(self.request(batch, "get", attachment_id="att-1"))["error"], "storage_invalid")
        many = [dict(self.metadata(), attachment_id=str(i), original_name="x" * 4096) for i in range(20)]
        self.response = {"schema_version": 1, "action": "query", "ok": True, "error": None,
                         "result": {"items": many, "next_cursor": None, "complete": True}}
        self.assertEqual(self.call(self.request(batch, "query"))["error"], "storage_invalid")
        status = self.status()
        status["units"]["unknown"] = 1  # Unknown caption unit, no unknown attachment.
        status["units"]["pending"] = 1
        self.response = {"schema_version": 1, "action": "status", "ok": True, "error": None,
                         "result": {"group": status}}
        self.assertEqual(self.call(self.request(batch, "status", group_id="delivery-group"))["error"], "storage_invalid")


if __name__ == "__main__":
    unittest.main()
