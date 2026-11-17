"""Public continuity wiring on real synthetic Store/receive/worker paths.

These are implementation-side integration tests. The Muse adapter and final
combined candidate still require their separately owned verification.
"""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

import test_attachment_runtime as runtime_fixture
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge import cloud_context, context_runtime, recent_context
from wechat_bridge.attachments import AttachmentStore
from wechat_bridge.store import atomic_write_json, load_json
from wechat_bridge.worker_state import WORKER_LEASE_SECONDS


class ContextRuntime(unittest.TestCase):
    def setUp(self):
        flags = patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "1", "WECHAT_CLOUD_CONTEXT": "0"})
        flags.start()
        self.addCleanup(flags.stop)
        self.f = runtime_fixture.RuntimeTests(methodName="runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.counter = 0

    @property
    def recent_path(self):
        return self.f.base / "state/continuity/recent.json"

    def batch(self, text="SYNTHETIC user evidence"):
        self.counter += 1
        mid = "context-%03d" % self.counter
        self.f.receive(self.f.message(mid, text=text))
        self.f.now = time.time()
        return self.f.batch()

    def delta(self, batch, **changes):
        result = {"schema_version": 1, "handle": "synthetic-handle", "revision": 1,
            "source_message_ids": list(batch["owned"]), "topic": "SYNTHETIC topic",
            "intent": "SYNTHETIC intent", "bot_action": "SYNTHETIC queued response",
            "open_loop": "SYNTHETIC next step", "historical_refs": []}
        result.update(changes)
        return result

    def reply(self, batch, text="SYNTHETIC bot evidence", basis=None, **changes):
        kwargs = {"handle": "synthetic-handle", "revision": 1, "basis": basis}
        kwargs.update(changes)
        return self.f.worker.reply_with_context(batch["owned"], text, batch["lease"], **kwargs)

    def commit(self, call, batch, delta=None):
        # Model the documented local adapter ordering with a durable queued
        # marker. This does not substitute for tests of the actual Muse adapter.
        self.assertTrue(call.result.get("queued") or call.result.get("ok"), call.result)
        atomic_write_json(str(self.f.home / "synthetic-adapter-session.json"),
                          {"phase": "queued", "business_result": call.result})
        self.assertEqual(load_json(str(self.f.home / "synthetic-adapter-session.json"))["phase"], "queued")
        return self.f.worker.context_commit(call.context_capsule,
                                            self.delta(batch) if delta is None else delta)

    def attachment(self, batch, action, arguments, basis=None):
        request = {"schema_version": 1, "action": action, "arguments": arguments}
        return self.f.worker.attachment_call_with_context(batch["owned"], batch["lease"], request,
            handle="synthetic-handle", revision=1, basis=basis)

    def test_default_off_preserves_plain_result_and_creates_no_context_files(self):
        with patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "0", "WECHAT_CLOUD_CONTEXT": "0"}):
            batch = self.batch()
            self.assertEqual(context_runtime.capabilities(), {"schema_version": 1, "features": []})
            self.assertEqual(self.f.worker.context_for_batch(batch).projection, {})
            call = self.reply(batch)
            self.assertEqual(call.result, {"queued": batch["owned"][-1] + ".json",
                "covers_message_ids": batch["owned"], "completed": batch["owned"]})
            self.assertIsNone(call.context_capsule)
            self.assertEqual(self.commit(call, batch), {"status": "skipped", "reason": "disabled"})
            self.assertFalse((self.f.base / "state/continuity").exists())

    def test_plain_capture_precedes_inbox_deletion_and_commit_follows_queued_save(self):
        batch = self.batch()
        original = AttachmentStore.context_sources
        captured = []
        def inspect(catalog, scope, messages):
            for row in messages:
                self.assertTrue((self.f.base / "inbox" / (row["message_id"] + ".json")).exists())
            captured.extend(row["message_id"] for row in messages)
            return original(catalog, scope, messages)
        with patch.object(AttachmentStore, "context_sources", autospec=True, side_effect=inspect):
            call = self.reply(batch)
        self.assertEqual(captured, batch["owned"])
        self.assertIsNotNone(call.context_capsule)
        self.assertEqual(repr(call.context_capsule), "<private context capsule>")
        with self.assertRaises(TypeError):
            json.dumps(call.context_capsule)
        self.assertFalse(self.recent_path.exists())
        self.assertFalse((self.f.base / "inbox" / (batch["owned"][0] + ".json")).exists())
        self.assertEqual(self.commit(call, batch)["status"], "applied")
        entry = load_json(str(self.recent_path))["entries"][0]
        self.assertEqual(entry["revision"], 1)
        self.assertEqual(entry["admission"]["kind"], "reply")
        self.assertEqual([q["text"] for q in entry["quotes"]],
                         ["SYNTHETIC user evidence", "SYNTHETIC bot evidence"])
        self.assertEqual(self.f.worker.context_commit(call.context_capsule, self.delta(batch)),
                         {"status": "skipped", "reason": "capsule_unavailable"})

    def test_final_business_completion_failure_cannot_return_capsule(self):
        batch = self.batch()
        with patch.object(self.f.worker, "_complete", side_effect=OSError("synthetic receipt failure")):
            with self.assertRaises(OSError):
                self.reply(batch)
        self.assertFalse(self.recent_path.exists())
        self.assertTrue((self.f.base / "inbox" / (batch["owned"][0] + ".json")).exists())
        self.assertTrue((self.f.base / "outbox/pending" / (batch["owned"][0] + ".json")).exists())

    def test_needs_refresh_and_missing_sequence_never_issue_a_capsule(self):
        batch = self.batch()
        self.f.receive(self.f.message("context-arrival", text="SYNTHETIC later arrival"))
        self.f.now = time.time()
        call = self.reply(batch)
        self.assertEqual(call.result["error"], "needs_refresh")
        self.assertIsNone(call.context_capsule)
        fresh = self.f.worker.refresh(batch["lease"])
        self.assertEqual(set(fresh["owned"]), set(batch["owned"] + ["context-arrival"]))
        # Delete only the private test catalog event, preserving the admitted
        # source batch and the real ordinary queue path.
        import sqlite3
        with sqlite3.connect(str(self.f.base / "state/attachments.sqlite3")) as db:
            db.execute("DELETE FROM events WHERE seq=(SELECT min(seq) FROM events)")
        call = self.reply(fresh)
        self.assertTrue(call.result["queued"])
        self.assertIsNone(call.context_capsule)
        self.assertEqual(self.commit(call, fresh)["reason"], "capsule_unavailable")
        self.assertFalse(self.recent_path.exists())

    def test_capsule_construction_failure_does_not_change_queued_business_result(self):
        batch = self.batch()
        with patch("wechat_bridge.context_runtime._Capsule", side_effect=RuntimeError("synthetic capsule fault")):
            call = self.reply(batch)
        self.assertEqual(call.result["completed"], batch["owned"])
        self.assertIsNone(call.context_capsule)
        self.assertEqual(load_json(str(self.f.base / "state/received" / (batch["owned"][0] + ".json")))["status"], "complete")

    def test_bad_optional_candidates_are_absorbed_after_queue_success(self):
        cases = [{"topic": "x" * 49}, {"intent": "x" * 81},
            {"bot_action": "x" * 129}, {"open_loop": "x" * 129},
            {"topic": "x" * 48, "intent": "x" * 80, "bot_action": "x" * 128, "open_loop": "x"},
            {"revision": True}, {"revision": 1.0}, {"revision": 0},
            {"handle": "other"}, {"source_message_ids": ["not-owned"]},
            {"historical_refs": ["0" * 64]}, {"topic": "\ud800"},
            {"topic": "x" * 9000}]
        for changes in cases:
            with self.subTest(fields=sorted(changes)):
                batch = self.batch()
                call = self.reply(batch)
                before = copy.deepcopy(call.result)
                result = self.commit(call, batch, self.delta(batch, **changes))
                self.assertEqual(result["status"], "skipped", result)
                self.assertEqual(call.result, before)
        self.assertFalse(self.recent_path.exists())

    def test_capsule_survives_lease_expiry_but_never_login_scope_change(self):
        batch = self.batch()
        call = self.reply(batch)
        self.f.now += WORKER_LEASE_SECONDS + 1
        self.assertEqual(self.commit(call, batch)["status"], "applied")
        self.f.now = time.time()
        second = self.batch()
        call = self.reply(second)
        session = load_json(self.f.store.session_path)
        session["login_epoch"] = "b" * 32
        atomic_write_json(self.f.store.session_path, session)
        before = self.recent_path.read_bytes()
        self.assertEqual(self.commit(call, second), {"status": "skipped", "reason": "scope_mismatch"})
        self.assertEqual(self.recent_path.read_bytes(), before)

    def test_storage_read_and_write_failures_stay_outside_business_result(self):
        batch = self.batch()
        with patch("wechat_bridge.recent_context.read", side_effect=OSError("synthetic unavailable disk")):
            context = self.f.worker.context_for_batch(batch)
        self.assertNotIn("recent_context_v1", context.projection)
        self.assertIsNone(context.basis)
        call = self.reply(batch)
        original = copy.deepcopy(call.result)
        with patch("wechat_bridge.recent_context.append", side_effect=OSError("synthetic disk full")):
            result = self.commit(call, batch)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(call.result, original)
        self.assertFalse(self.recent_path.exists())

    def test_three_quotes_share_two_kib_budget_without_splitting_unicode(self):
        user = "\U0001f642" * 600
        self.f.receive(self.f.message("context-unicode-one", text=user))
        self.f.receive(self.f.message("context-unicode-two", text=user))
        self.f.now = time.time()
        batch = self.f.batch()
        call = self.reply(batch, text="\U0001f680" * 600)
        self.assertEqual(self.commit(call, batch)["status"], "applied")
        quotes = load_json(str(self.recent_path))["entries"][0]["quotes"]
        self.assertEqual([q["speaker"] for q in quotes], ["user", "user", "bot"])
        self.assertEqual([len(q["text"].encode("utf-8")) for q in quotes], [512, 512, 1024])
        self.assertTrue(all(q["truncated"] for q in quotes))

    def test_expired_unconsumed_capsule_cannot_recreate_deleted_snapshot_after_wall_rollback(self):
        first = self.batch()
        issued = self.f.now
        with patch("wechat_bridge.context_runtime.time.monotonic", return_value=1000.0):
            held = self.reply(first)
            second = self.batch()
            self.assertEqual(self.commit(self.reply(second), second)["status"], "applied")
        self.assertTrue(self.recent_path.exists())
        # Maintenance has physically removed all old contents after 48h.
        self.f.now = issued + context_runtime.TTL + 1
        context_runtime.recent_context_maintain(self.f.worker.store, clock=lambda: self.f.now)
        self.assertFalse(self.recent_path.exists())
        # A wall-clock rollback alone would make the original source look live.
        # The same-process monotonic lifetime still rejects the held capsule.
        self.f.now = issued + 1
        with patch("wechat_bridge.context_runtime.time.monotonic", return_value=1000.0 + context_runtime.TTL + 1):
            self.assertEqual(self.commit(held, first), {"status": "skipped", "reason": "source_expired"})
        self.assertFalse(self.recent_path.exists())

    def test_capsule_rejects_wall_clock_before_its_issue_time(self):
        batch = self.batch()
        call = self.reply(batch)
        self.f.now -= 1
        self.assertEqual(self.commit(call, batch), {"status": "skipped", "reason": "source_expired"})
        self.assertFalse(self.recent_path.exists())

    def test_generated_media_placeholder_is_never_stored_as_user_original_words(self):
        self.f.file(mid="context-media-placeholder")
        self.f.now = time.time()
        batch = self.f.batch()
        self.assertEqual(batch["messages"][0]["text"], "[文件]")
        call = self.reply(batch)
        self.assertEqual(self.commit(call, batch)["status"], "applied")
        entry = load_json(str(self.recent_path))["entries"][0]
        self.assertEqual([quote["speaker"] for quote in entry["quotes"]], ["bot"])
        self.assertEqual(entry["quotes"][0]["text"], "SYNTHETIC bot evidence")
        self.assertNotIn("[文件]", json.dumps(entry["quotes"], ensure_ascii=False))

    def test_safe_projection_and_quote_require_signed_basis_and_a_live_batch(self):
        first = self.batch(text="SYNTHETIC private original user words")
        call = self.reply(first, text="SYNTHETIC private original bot words")
        self.assertEqual(self.commit(call, first)["status"], "applied")
        second = self.batch(text="SYNTHETIC please quote the earlier words")
        context = self.f.worker.context_for_batch(second)
        recent = context.projection["recent_context_v1"]
        self.assertEqual(len(recent["entries"]), 1)
        serialized = json.dumps(context.projection, ensure_ascii=False)
        self.assertNotIn("private original", serialized)
        self.assertNotIn("quotes", serialized)
        eid = recent["entries"][0]["id"]
        quote = self.f.worker.context_quote(second, context.basis, eid, 0)
        self.assertEqual(quote["status"], "ok")
        self.assertEqual(quote["quote"]["text"], "SYNTHETIC private original user words")
        tampered = copy.deepcopy(context.basis)
        tampered["payload"]["entries"].append({"entry_id": "0" * 64, "expires_at": self.f.now + 100})
        self.assertEqual(self.f.worker.context_quote(second, tampered, eid, 0)["status"], "unavailable")
        self.assertEqual(self.f.worker.context_quote(second, context.basis, eid, True)["status"], "unavailable")
        self.assertEqual(self.f.worker.context_quote(second, context.basis, eid, 3)["status"], "unavailable")
        call = self.reply(second, basis=context.basis)
        self.assertEqual(self.commit(call, second, self.delta(second, historical_refs=[eid]))["status"], "applied")
        self.assertEqual(self.f.worker.context_quote(second, context.basis, eid, 0)["status"], "unavailable")
        entries = load_json(str(self.recent_path))["entries"]
        self.assertEqual(entries[1]["historical_refs"], [{"entry_id": eid, "expires_at": entries[0]["expires_at"]}])

    def test_forged_basis_cannot_grant_history_reference_to_valid_capsule(self):
        first = self.batch()
        self.assertEqual(self.commit(self.reply(first), first)["status"], "applied")
        second = self.batch()
        context = self.f.worker.context_for_batch(second)
        eid = context.projection["recent_context_v1"]["entries"][0]["id"]
        forged = copy.deepcopy(context.basis)
        forged["signature"] = "0" * 64
        call = self.reply(second, basis=forged)
        before = self.recent_path.read_bytes()
        self.assertEqual(self.commit(call, second, self.delta(second, historical_refs=[eid]))["status"], "skipped")
        self.assertEqual(self.recent_path.read_bytes(), before)

    def test_slow_begin_read_rechecks_live_batch_without_renewing_lease(self):
        first = self.batch()
        self.assertEqual(self.commit(self.reply(first), first)["status"], "applied")
        current = self.batch()
        claims = copy.deepcopy(self.f.worker._claims())
        read = recent_context.read
        def slow_read(*args, **kwargs):
            result = read(*args, **kwargs)
            self.f.now += WORKER_LEASE_SECONDS + 1
            return result
        with patch.object(recent_context, "read", side_effect=slow_read):
            result = self.f.worker.context_for_batch(current)
        self.assertNotIn("recent_context_v1", result.projection)
        self.assertNotIn("cloud_context_v1", result.projection)
        self.assertIsNone(result.basis)
        self.assertEqual(self.f.worker._claims(), claims)

    def test_slow_quote_read_rechecks_live_batch_without_renewing_lease(self):
        first = self.batch()
        self.assertEqual(self.commit(self.reply(first), first)["status"], "applied")
        current = self.batch()
        context = self.f.worker.context_for_batch(current)
        eid = context.projection["recent_context_v1"]["entries"][0]["id"]
        claims = copy.deepcopy(self.f.worker._claims())
        get_entry = recent_context.get_entry
        def slow_get_entry(*args, **kwargs):
            result = get_entry(*args, **kwargs)
            self.f.now += WORKER_LEASE_SECONDS + 1
            return result
        with patch.object(recent_context, "get_entry", side_effect=slow_get_entry):
            result = self.f.worker.context_quote(current, context.basis, eid, 0)
        self.assertEqual(result["status"], "unavailable")
        self.assertNotIn("quote", result)
        self.assertEqual(self.f.worker._claims(), claims)

    def near_expiry_batch(self):
        first = self.batch()
        self.assertEqual(self.commit(self.reply(first), first)["status"], "applied")
        expires = load_json(str(self.recent_path))["entries"][0]["expires_at"]
        self.f.receive(self.f.message("context-near-expiry", text="SYNTHETIC ask about earlier source"))
        # The new batch lease begins near the old record's expiry, so crossing
        # that record's TTL by one second does not itself expire the live lease.
        self.f.now = expires - 1
        batch = self.f.batch()
        return batch, expires

    def test_begin_read_cannot_return_original_record_after_ttl_crosses_during_io(self):
        batch, expires = self.near_expiry_batch()
        claims = copy.deepcopy(self.f.worker._claims())
        read = recent_context.read
        def cross_ttl(*args, **kwargs):
            result = read(*args, **kwargs)
            self.assertTrue(result["entries"])
            self.f.now = expires
            return result
        with patch.object(recent_context, "read", side_effect=cross_ttl):
            result = self.f.worker.context_for_batch(batch)
        self.assertNotIn("recent_context_v1", result.projection)
        self.assertIsNone(result.basis)
        self.assertTrue(all(self.f.worker._alive(mid, row) for mid, row in claims.items()))
        self.assertEqual(self.f.worker._claims(), claims)

    def test_quote_read_cannot_return_original_words_after_ttl_crosses_during_io(self):
        batch, expires = self.near_expiry_batch()
        context = self.f.worker.context_for_batch(batch)
        eid = context.projection["recent_context_v1"]["entries"][0]["id"]
        claims = copy.deepcopy(self.f.worker._claims())
        get_entry = recent_context.get_entry
        def cross_ttl(*args, **kwargs):
            result = get_entry(*args, **kwargs)
            self.assertIsNotNone(result)
            self.f.now = expires
            return result
        with patch.object(recent_context, "get_entry", side_effect=cross_ttl):
            result = self.f.worker.context_quote(batch, context.basis, eid, 0)
        self.assertEqual(result["status"], "unavailable")
        self.assertNotIn("quote", result)
        self.assertTrue(all(self.f.worker._alive(mid, row) for mid, row in claims.items()))
        self.assertEqual(self.f.worker._claims(), claims)

    def test_attachment_reply_and_continue_keep_exact_wire_and_commit_separately(self):
        aid = self.f.file(mid="context-file")
        self.f.now = time.time()
        batch = self.f.batch()
        state = runtime.ledger(self.f.worker.store)
        state.reserve_units(self.f.scope, batch["context"], ["synthetic-prior:%d" % i for i in range(8)])
        call = self.attachment(batch, "reply", {"attachment_ids": [aid], "caption": "SYNTHETIC caption"})
        envelope = call.result
        self.assertEqual(set(envelope), {"schema_version", "action", "ok", "result", "error"})
        self.assertEqual(set(envelope["result"]), {"queued", "delivery_confirmed", "group_id", "job_id", "status"})
        self.assertTrue(envelope["ok"], envelope)
        self.assertIsNotNone(call.context_capsule)
        self.assertLess(len(json.dumps(envelope).encode()), 65536)
        self.assertNotIn("capsule", json.dumps(envelope))
        self.assertFalse(self.recent_path.exists())
        self.assertEqual(self.commit(call, batch)["status"], "applied")
        self.assertEqual(envelope["result"]["status"]["state"], "awaiting_continue")
        self.f.receive(self.f.message("context-continue", text="继续"))
        self.f.now = time.time()
        followup = self.f.batch()
        context = self.f.worker.context_for_batch(followup)
        continued = self.attachment(followup, "continue", {"group_id": envelope["result"]["group_id"]}, context.basis)
        self.assertTrue(continued.result["ok"], continued.result)
        self.assertIsNotNone(continued.context_capsule)
        self.assertNotEqual(continued.result["result"]["job_id"], envelope["result"]["job_id"])
        self.assertEqual(self.commit(continued, followup)["status"], "applied")
        entries = load_json(str(self.recent_path))["entries"]
        self.assertEqual([e["admission"]["kind"] for e in entries], ["attachment", "attachment"])

    def test_attachment_business_failure_and_optional_capsule_failure_are_separate(self):
        aid = self.f.file(mid="context-attachment-failure")
        self.f.now = time.time()
        batch = self.f.batch()
        with patch.object(self.f.worker, "_complete", side_effect=OSError("synthetic receipt failure")):
            failed = self.attachment(batch, "reply", {"attachment_ids": [aid]})
        self.assertFalse(failed.result["ok"])
        self.assertIsNone(failed.context_capsule)
        self.assertFalse(self.recent_path.exists())

    def test_attachment_capsule_constructor_failure_keeps_queued_wire(self):
        aid = self.f.file(mid="context-attachment-capsule-failure")
        self.f.now = time.time()
        batch = self.f.batch()
        with patch("wechat_bridge.context_runtime._Capsule", side_effect=RuntimeError("synthetic capsule fault")):
            call = self.attachment(batch, "reply", {"attachment_ids": [aid]})
        self.assertTrue(call.result["ok"], call.result)
        self.assertTrue(call.result["result"]["queued"])
        self.assertIsNone(call.context_capsule)
        self.assertFalse(self.recent_path.exists())

    def test_attachment_outer_wire_overflow_cannot_issue_capsule_after_admission(self):
        aid = self.f.file(mid="context-attachment-wire-overflow")
        self.f.now = time.time()
        batch = self.f.batch()
        with patch.object(runtime, "public_status", return_value={"synthetic_padding": "x" * 65536}):
            call = self.attachment(batch, "reply", {"attachment_ids": [aid]})
        self.assertFalse(call.result["ok"], call.result)
        self.assertEqual(call.result["error"]["code"], "response_too_large")
        self.assertIsNone(call.context_capsule)
        self.assertFalse(self.recent_path.exists())
        self.assertEqual(load_json(str(self.f.base / "state/received" / (batch["owned"][0] + ".json")))["status"], "complete")

    def test_default_off_attachment_uses_original_wire_without_private_data(self):
        aid = self.f.file(mid="context-default-off-attachment")
        self.f.now = time.time()
        batch = self.f.batch()
        with patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "0", "WECHAT_CLOUD_CONTEXT": "0"}):
            call = self.attachment(batch, "reply", {"attachment_ids": [aid], "caption": "SYNTHETIC caption"})
        self.assertTrue(call.result["ok"], call.result)
        self.assertTrue(call.result["result"]["queued"])
        self.assertEqual(set(call.result), {"schema_version", "action", "ok", "result", "error"})
        self.assertIsNone(call.context_capsule)
        self.assertFalse((self.f.base / "state/continuity").exists())

    def cli_maintain(self, base, binding=None):
        script = '''import runpy,socket,subprocess,sys
sys.path.insert(0,sys.argv[1])
def deny(*a,**kw): raise AssertionError("maintenance CLI forbids sockets")
socket.socket.connect=deny;socket.socket.connect_ex=deny
socket.socket.bind=deny;socket.socket.listen=deny
socket.create_connection=deny;socket.getaddrinfo=deny
subprocess.Popen=deny
base=sys.argv[2]
extra=sys.argv[3:]
sys.argv=["wechat_bridge.worker_state","--base",base,"recent-context-maintain",*extra]
runpy.run_module("wechat_bridge.worker_state",run_name="__main__")
'''
        args = [sys.executable, "-I", "-B", "-c", script, str(ROOT), str(base)]
        if binding is not None:
            args.extend(["--cloud-source-id", binding.source_id, "--cloud-case-id", binding.case_id,
                         "--cloud-publisher-id", binding.publisher_id])
        proc = subprocess.run(args,
            cwd=str(ROOT), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_maintenance_cli_without_session_cleans_only_owned_content_and_never_starts_serve(self):
        batch = self.batch()
        self.assertEqual(self.commit(self.reply(batch), batch)["status"], "applied")
        directory = self.recent_path.parent
        unknown = directory / "unrelated-private-data.txt"
        unknown.write_text("SYNTHETIC unowned file")
        for name in (".recent.json.tmp", ".cloud.json.tmp"):
            path = directory / name
            path.write_text('{"synthetic":true}')
            path.chmod(0o600)
        Path(self.f.store.session_path).unlink()
        with patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "0", "WECHAT_CLOUD_CONTEXT": "0"}):
            result = self.cli_maintain(self.f.base)
        self.assertEqual(result["recent"]["status"], "cleared")
        self.assertFalse(self.recent_path.exists())
        self.assertFalse((directory / ".recent.json.tmp").exists())
        self.assertFalse((directory / ".cloud.json.tmp").exists())
        self.assertEqual(unknown.read_text(), "SYNTHETIC unowned file")
        self.assertFalse(Path(self.f.store.pid_path).exists())
        self.assertFalse(Path(self.f.store.session_path).exists())

    def test_maintenance_cli_missing_base_creates_nothing(self):
        missing = self.f.home / "does-not-exist"
        result = self.cli_maintain(missing)
        self.assertIn("recent", result)
        self.assertIn("cloud", result)
        self.assertFalse(missing.exists())

    def test_maintenance_cli_needs_relogin_cleans_context_without_rewriting_session(self):
        batch = self.batch()
        self.assertEqual(self.commit(self.reply(batch), batch)["status"], "applied")
        session_bytes = Path(self.f.store.session_path).read_bytes()
        Path(self.f.store.needs_relogin_path).write_text("SYNTHETIC relogin required")
        result = self.cli_maintain(self.f.base)
        self.assertEqual(result["recent"]["status"], "cleared")
        self.assertFalse(self.recent_path.exists())
        self.assertEqual(Path(self.f.store.session_path).read_bytes(), session_bytes)
        self.assertTrue(Path(self.f.store.needs_relogin_path).exists())
        self.assertFalse(Path(self.f.store.pid_path).exists())

    @staticmethod
    def cloud_projection(revision, at, facts="SYNTHETIC first source facts"):
        return {"source_revision": revision, "source_updated_at": at,
            "source_ref": "synthetic://continuous-context/case", "fact_status": "confirmed",
            "title": "SYNTHETIC cloud case", "facts": facts, "change": "SYNTHETIC state change",
            "next_step": "SYNTHETIC next task", "open_loop": "SYNTHETIC open question"}

    def test_real_synthetic_cloud_publish_then_begin_projection_reads_latest_revision(self):
        binding = cloud_context.CloudBinding("synthetic-source", "synthetic-case", "synthetic-publisher")
        at = time.time() - 10
        first = self.cloud_projection(1, at)
        published = cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
                                          first, None, time.time(), enabled=True)
        self.assertEqual(published["status"], "published", published)
        batch = self.batch("SYNTHETIC ask about the cloud case")
        with patch.dict(os.environ, {"WECHAT_CLOUD_CONTEXT": "1"}):
            one = self.f.worker.context_for_batch(batch, binding)
            card = one.projection["cloud_context_v1"]
            self.assertEqual(card["status"], "available", card)
            self.assertEqual(card["projection"]["source_revision"], 1)
            self.assertEqual(card["projection"]["source_updated_at"], at)
            second = self.cloud_projection(2, time.time() - 1, "SYNTHETIC corrected source facts")
            published = cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
                                              second, 1, time.time(), enabled=True)
            self.assertEqual(published["status"], "published", published)
            self.f.now = time.time()
            two = self.f.worker.context_for_batch(batch, binding)
        card = two.projection["cloud_context_v1"]
        self.assertEqual(card["status"], "available", card)
        self.assertEqual(card["projection"]["source_revision"], 2)
        self.assertEqual(card["projection"]["facts"], second["facts"])
        self.assertLessEqual(len(json.dumps(two.projection, ensure_ascii=False,
                                           separators=(",", ":")).encode("utf-8")), 16 * 1024)

    def test_maintenance_cli_applies_new_source_version_and_unchanged_checks_need_no_model(self):
        binding = cloud_context.CloudBinding("synthetic-maintain-source", "synthetic-case", "synthetic-publisher")
        first = self.cloud_projection(1, time.time() - 10)
        self.assertEqual(cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
            first, None, time.time(), enabled=True)["status"], "published")
        # The CLI subprocess forbids every socket and further process launch.
        # It can complete both changed and unchanged checks using local files.
        with patch.dict(os.environ, {"WECHAT_CLOUD_CONTEXT": "1"}):
            result = self.cli_maintain(self.f.base, binding)
            self.assertEqual(result["cloud_refresh"]["status"], "applied", result)
            cache_path = self.f.base / "state/continuity/cloud.json"
            cached = load_json(str(cache_path))
            self.assertEqual(cached["projection"]["source_revision"], 1)
            second = self.cloud_projection(2, time.time() - 1, "SYNTHETIC completion source facts")
            self.assertEqual(cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
                second, 1, time.time(), enabled=True)["status"], "published")
            result = self.cli_maintain(self.f.base, binding)
            self.assertEqual(result["cloud_refresh"]["status"], "applied", result)
            cached = load_json(str(cache_path))
            self.assertEqual(cached["projection"]["source_revision"], 2)
            source_path = self.f.base / "state/continuity-source/current.json"
            source_bytes = source_path.read_bytes()
            applied, checked = cached["bridge_applied_at"], cached["bridge_checked_at"]
            result = self.cli_maintain(self.f.base, binding)
            self.assertEqual(result["cloud_refresh"]["status"], "unchanged", result)
        unchanged = load_json(str(cache_path))
        self.assertGreaterEqual(unchanged["bridge_checked_at"], checked)
        self.assertEqual(unchanged["bridge_applied_at"], applied)
        self.assertEqual(unchanged["projection"]["source_updated_at"], second["source_updated_at"])
        self.assertEqual(source_path.read_bytes(), source_bytes)

    def test_enabled_cloud_without_fixed_binding_clears_bridge_cache_but_keeps_source(self):
        binding = cloud_context.CloudBinding("synthetic-removed-source", "synthetic-case", "synthetic-publisher")
        self.assertEqual(cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
            self.cloud_projection(1, time.time() - 10), None, time.time(), enabled=True)["status"], "published")
        batch = self.batch()
        source_path = self.f.base / "state/continuity-source/current.json"
        source_bytes = source_path.read_bytes()
        cache_path = self.f.base / "state/continuity/cloud.json"
        def assert_text_retired(expected_revision):
            marker = load_json(str(cache_path))
            self.assertEqual(set(marker), {"schema_version", "tombstone", "scope_digest", "binding_digest",
                "clock_high_water", "retired_revision", "retired_digest", "retired_source_updated_at",
                "retired_expires_at", "reason"})
            self.assertTrue(marker["tombstone"])
            self.assertEqual(marker["retired_revision"], expected_revision)
            self.assertGreaterEqual(marker["clock_high_water"], marker["retired_source_updated_at"])
            self.assertEqual(marker["retired_expires_at"], marker["retired_source_updated_at"] + context_runtime.TTL)
            for field in ("scope_digest", "binding_digest", "retired_digest"):
                self.assertRegex(marker[field], r"^[0-9a-f]{64}$")
            self.assertNotIn("projection", marker)
            marker_text = cache_path.read_text()
            self.assertLessEqual(len(marker_text.encode("utf-8")), 4096)
            for private_text in (*self.f.scope.values(), binding.source_id, binding.case_id,
                    binding.publisher_id, "SYNTHETIC cloud case", "SYNTHETIC first source facts",
                    "SYNTHETIC newly confirmed facts", "synthetic://continuous-context/case"):
                self.assertNotIn(private_text, marker_text)
        with patch.dict(os.environ, {"WECHAT_CLOUD_CONTEXT": "1"}):
            populated = self.f.worker.context_for_batch(batch, binding)
            self.assertEqual(populated.projection["cloud_context_v1"]["status"], "available")
            self.assertTrue(cache_path.exists())
            missing = self.f.worker.context_for_batch(batch, None)
            self.assertIsNone(missing.projection["cloud_context_v1"]["projection"])
            self.assertEqual(missing.projection["cloud_context_v1"]["reason"], "source_missing")
            assert_text_retired(1)
            self.assertEqual(source_path.read_bytes(), source_bytes)
            # The hash-only retirement lineage intentionally rejects the old
            # source version. A new authoritative conclusion can be applied.
            self.assertEqual(cloud_context.publish(self.f.worker.store, binding, binding.publisher_id,
                self.cloud_projection(2, time.time(), "SYNTHETIC newly confirmed facts"),
                1, time.time(), enabled=True)["status"], "published")
            source_bytes = source_path.read_bytes()
            self.f.now = time.time()
            repopulated = self.f.worker.context_for_batch(batch, binding)
            self.assertEqual(repopulated.projection["cloud_context_v1"]["status"], "available")
            self.assertEqual(repopulated.projection["cloud_context_v1"]["projection"]["source_revision"], 2)
            self.assertTrue(cache_path.exists())
            maintained = self.cli_maintain(self.f.base)
            self.assertNotIn("cloud_refresh", maintained)
        assert_text_retired(2)
        self.assertEqual(source_path.read_bytes(), source_bytes)


if __name__ == "__main__":
    unittest.main()
