"""Synthetic adapter continuity regressions, without live state or a publisher."""
from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "muse_context_baseline_fixtures", ROOT / "hooks/tests/test_muse_worker.py")
fixtures = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixtures)
adapter = fixtures.adapter


class PrivateCapsule:
    """Its representation must never enter a public result or persisted session."""

    def __repr__(self):
        raise AssertionError("private capsule was inspected for serialization")


class TerminalResult:
    def __init__(self, owner, result, capsule):
        self.owner = owner
        self.result = result
        self.capsule = capsule

    @property
    def context_capsule(self):
        self.owner.assertEqual(self.owner.session(self.owner.handle)["phase"], "queued")
        self.owner.events.append("capsule_read")
        if self.owner.capsule_error:
            raise OSError("SYNTHETIC-PRIVATE-CAPSULE-ERROR")
        return self.capsule


class UnreadableExtra:
    def __repr__(self):
        raise AssertionError("an unknown private subtree was inspected")

    def __iter__(self):
        raise AssertionError("an unknown private subtree was traversed")


class MuseContextTests(unittest.TestCase):
    setUp = fixtures.MuseWorkerTests.setUp
    incoming = fixtures.MuseWorkerTests.incoming
    call = fixtures.MuseWorkerTests.call
    begin = fixtures.MuseWorkerTests.begin
    session = fixtures.MuseWorkerTests.session
    install_wire = fixtures.AttachmentAdapterTests.install_wire
    request = fixtures.AttachmentAdapterTests.request
    metadata = fixtures.AttachmentAdapterTests.metadata
    material = fixtures.AttachmentAdapterTests.material
    status = fixtures.AttachmentAdapterTests.status
    business_result = fixtures.AttachmentAdapterTests.business_result
    error_envelope = fixtures.AttachmentAdapterTests.error_envelope

    def install_context(self):
        self.events = []
        self.context_reads = []
        self.context_commits = []
        self.capsule = PrivateCapsule()
        self.capsule_error = False
        self.commit_error = False
        self.wrapper_error = False
        self.handle = None
        self.projection = {"recent_context_v1": None}
        self.basis = {"synthetic_signed_basis": "PRIVATE-BASIS-NOT-MODEL-DATA"}
        self.context_capabilities = {
            "schema_version": 1, "features": ["recent_context_v1", "memory_delta_v1"]}
        caps = patch.object(adapter, "_context_capabilities", return_value=self.context_capabilities)
        caps.start()
        self.addCleanup(caps.stop)
        # These tests isolate terminal ordering from projection schema coverage.
        # Final projection/quote tests use the fixed public schema separately.
        projection = patch.object(adapter, "_context_projection", side_effect=lambda value, *args: deepcopy(value))
        projection.start()
        self.addCleanup(projection.stop)

        def context_for_batch(batch, cloud_binding=None):
            self.assertTrue(batch["owned"])
            self.context_reads.append(list(batch["owned"]))
            return type("ContextRead", (), {
                "projection": deepcopy(self.projection), "basis": deepcopy(self.basis)})()

        ordinary = self.state.reply
        attachment = self.state.attachment_call

        def reply_with_context(ids, text, lease, media_path=None, *, handle, revision, basis=None):
            self.events.append("ordinary_publish")
            self.handle = handle
            result = ordinary(ids, text, lease, media_path)
            if self.wrapper_error:
                raise OSError("SYNTHETIC-PRIVATE-WRAPPER-ERROR")
            return TerminalResult(self, result, self.capsule)

        def attachment_call_with_context(ids, lease, request, *, handle, revision, basis=None):
            self.events.append("attachment_publish")
            self.handle = handle
            result = attachment(ids, lease, request)
            if self.wrapper_error:
                raise OSError("SYNTHETIC-PRIVATE-WRAPPER-ERROR")
            return TerminalResult(self, result, self.capsule)

        def context_commit(capsule, memory_delta_v1):
            self.assertEqual(self.session(self.handle)["phase"], "queued")
            self.assertIs(capsule, self.capsule)
            self.events.append("memory_commit")
            if self.commit_error:
                raise OSError("SYNTHETIC-PRIVATE-MEMORY-ERROR")
            if not isinstance(memory_delta_v1, dict) or memory_delta_v1.get("schema_version") != 1:
                return {"status": "skipped", "reason": "invalid_memory_delta"}
            self.context_commits.append(deepcopy(memory_delta_v1))
            return {"status": "stored"}

        self.state.context_for_batch = context_for_batch
        self.state.reply_with_context = reply_with_context
        self.state.attachment_call_with_context = attachment_call_with_context
        self.state.context_commit = context_commit

    def delta(self, batch):
        return {"schema_version": 1, "handle": batch["handle"], "revision": batch["revision"],
                "source_message_ids": [message["message_id"] for message in batch["messages"]],
                "topic": "当前合成事项", "intent": "继续核对", "bot_action": "已回复",
                "open_loop": "等待结果", "historical_refs": []}

    def ordinary_request(self, batch, delta=True):
        request = {"action": "reply", "handle": batch["handle"],
                   "revision": batch["revision"], "text": "synthetic final answer"}
        if delta:
            request["memory_delta_v1"] = self.delta(batch)
        return request

    def assert_queued(self, batch, result):
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.session(batch["handle"])["phase"], "queued")
        encoded = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("PRIVATE", encoded)
        self.assertNotIn("context_capsule", encoded)
        self.assertNotIn("context_capsule", self.session(batch["handle"]))

    def test_begin_reads_only_after_owned_and_keeps_basis_private(self):
        self.install_context()
        _, batch = self.begin()
        self.assertEqual(self.context_reads, [["a"]])
        self.assertIn("context_capabilities", batch)
        self.assertNotIn("PRIVATE-BASIS", json.dumps(batch))
        self.assertNotIn("context_capsule", json.dumps(batch))
        private = self.session(batch["handle"])
        self.assertNotIn("messages", private)

    def test_ordinary_terminal_saves_queued_before_consuming_capsule(self):
        self.install_context()
        _, batch = self.begin()
        result = self.call(self.ordinary_request(batch))
        self.assert_queued(batch, result)
        self.assertEqual(self.events, ["ordinary_publish", "capsule_read", "memory_commit"])
        self.assertEqual(self.context_commits, [self.delta(batch)])
        self.assertEqual(len(list((Path(self.base) / "outbox/pending").glob("*.json"))), 1)
        self.assertEqual(self.call(self.ordinary_request(batch))["error"], "session_closed")

    def test_missing_summary_keeps_original_business_path(self):
        self.install_context()
        _, batch = self.begin()
        result = self.call(self.ordinary_request(batch, delta=False))
        self.assert_queued(batch, result)
        self.assertFalse(self.context_commits)

    def test_bad_summary_is_optional_when_business_is_valid(self):
        self.install_context()
        for index, value in enumerate((False, [], "bad summary", {"schema_version": 999})):
            with self.subTest(value=value):
                _, batch = self.begin(mid="bad-%d" % index)
                request = self.ordinary_request(batch)
                request["memory_delta_v1"] = value
                result = self.call(request)
                self.assert_queued(batch, result)
        self.assertFalse(self.context_commits)

    def test_commit_and_capsule_failure_after_queue_preserve_success(self):
        self.install_context()
        for index, field in enumerate(("commit_error", "capsule_error")):
            with self.subTest(field=field):
                _, batch = self.begin(mid="failure-%d" % index)
                setattr(self, field, True)
                result = self.call(self.ordinary_request(batch))
                setattr(self, field, False)
                self.assert_queued(batch, result)
                self.assertEqual(self.call(self.ordinary_request(batch))["error"], "session_closed")

    def test_wrapper_error_after_publication_remains_unknown_without_memory_or_replay(self):
        self.install_context()
        _, batch = self.begin()
        self.wrapper_error = True
        request = self.ordinary_request(batch)
        result = self.call(request)
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertEqual(self.events, ["ordinary_publish"])
        self.assertFalse(self.context_commits)
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(list((Path(self.base) / "outbox/pending").glob("*.json"))), 1)

    def test_queued_save_failure_never_consumes_memory(self):
        self.install_context()
        _, batch = self.begin()
        original_save = adapter._save

        def fail_queued(directory, handle, session):
            if session["phase"] == "queued":
                raise OSError("SYNTHETIC-PRIVATE-QUEUED-SAVE-ERROR")
            return original_save(directory, handle, session)

        with patch.object(adapter, "_save", side_effect=fail_queued):
            result = self.call(self.ordinary_request(batch))
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertEqual(self.events, ["ordinary_publish"])
        self.assertFalse(self.context_commits)

    def test_needs_refresh_discards_old_delta_and_uses_new_batch(self):
        self.install_context()
        _, batch = self.begin()
        self.incoming("b", text="new question")
        old = self.ordinary_request(batch)
        result = self.call(old)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertFalse(self.context_commits)
        self.assertNotIn("capsule_read", self.events)
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"])
        self.assertEqual(self.call(old)["error"], "revision_mismatch")
        final = self.call(self.ordinary_request(refreshed))
        self.assert_queued(refreshed, final)
        self.assertEqual(self.context_commits[-1]["source_message_ids"], ["a", "b"])
        self.assertEqual(self.context_commits[-1]["revision"], refreshed["revision"])

    def assert_attachment_terminal(self, action):
        self.install_wire()
        self.install_context()
        _, batch = self.begin()
        arguments = {"attachment_ids": ["att-1"], "caption": "one caption"} if action == "reply" else {"group_id": "delivery-group"}
        request = self.request(batch, action, **arguments)
        request["memory_delta_v1"] = self.delta(batch)
        result = self.call(request)
        self.assert_queued(batch, result)
        self.assertTrue(result["result"]["queued"])
        self.assertEqual(self.calls[-1][2], request["request"])
        self.assertEqual(self.context_commits[-1], self.delta(batch))
        self.assertEqual(self.events, ["attachment_publish", "capsule_read", "memory_commit"])

    def test_attachment_reply_keeps_capsule_outside_business_wire(self):
        self.assert_attachment_terminal("reply")

    def test_attachment_continue_keeps_capsule_outside_business_wire(self):
        self.assert_attachment_terminal("continue")

    def test_attachment_failure_and_unknown_never_consume_memory(self):
        self.install_wire()
        self.install_context()
        _, batch = self.begin()
        self.response = self.error_envelope("reply", "needs_refresh")
        request = self.request(batch, "reply", attachment_ids=["att-1"])
        request["memory_delta_v1"] = self.delta(batch)
        result = self.call(request)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertNotIn("capsule_read", self.events)
        self.assertFalse(self.context_commits)

    def assert_attachment_optional_error(self, field):
        self.install_wire()
        self.install_context()
        _, batch = self.begin()
        request = self.request(batch, "reply", attachment_ids=["att-1"])
        request["memory_delta_v1"] = self.delta(batch)
        setattr(self, field, True)
        result = self.call(request)
        self.assert_queued(batch, result)
        self.assertEqual(self.call(request)["error"], "session_closed")

    def test_attachment_capsule_error_preserves_queued(self):
        self.assert_attachment_optional_error("capsule_error")

    def test_attachment_commit_error_preserves_queued(self):
        self.assert_attachment_optional_error("commit_error")

    def test_attachment_queued_save_failure_does_not_access_capsule(self):
        self.install_wire()
        self.install_context()
        _, batch = self.begin()
        request = self.request(batch, "continue", group_id="delivery-group")
        request["memory_delta_v1"] = self.delta(batch)
        original_save = adapter._save

        def fail_queued(directory, handle, session):
            if session["phase"] == "queued":
                raise OSError("SYNTHETIC-PRIVATE-ATTACHMENT-SAVE-ERROR")
            return original_save(directory, handle, session)

        with patch.object(adapter, "_save", side_effect=fail_queued):
            result = self.call(request)
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertEqual(self.events, ["attachment_publish"])
        self.assertFalse(self.context_commits)
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)

    def test_attachment_wrapper_unknown_is_not_retried(self):
        self.install_wire()
        self.install_context()
        _, batch = self.begin()
        request = self.request(batch, "reply", attachment_ids=["att-1"])
        request["memory_delta_v1"] = self.delta(batch)
        self.wrapper_error = True
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(self.events, ["attachment_publish"])
        self.assertFalse(self.context_commits)
        self.assertEqual(self.call(request)["error"], "outcome_unknown")
        self.assertEqual(len(self.calls), 1)

    def test_capability_disabled_and_old_core_preserve_current_reply(self):
        self.install_context()
        with patch.object(adapter, "_context_capabilities", return_value=None):
            for name in ("context_for_batch", "reply_with_context", "attachment_call_with_context", "context_commit"):
                delattr(self.state, name)
            _, batch = self.begin()
            self.assertNotIn("context_capabilities", batch)
            result = self.call(self.ordinary_request(batch))
            self.assertTrue(result["ok"], result)
            self.assertEqual(self.session(batch["handle"])["phase"], "queued")
            self.assertFalse(self.context_reads)
            self.assertFalse(self.context_commits)

    def test_malformed_and_oversize_outer_envelopes_retain_input_error(self):
        for raw in ('{"action":"reply","action":"reply"}',
                    '{"action":"reply","text":NaN}', '{"broken":',
                    json.dumps({"action": "reply", "text": "x" * adapter.MAX_INPUT_BYTES})):
            with self.subTest(raw_prefix=raw[:35]), self.assertRaises(adapter.InvalidRequest):
                adapter.parse_request(io.StringIO(raw))
        called = []

        def no_state(base, state_dir):
            called.append(True)
            raise AssertionError("invalid business input constructed state")

        result = self.call({"action": "reply", "handle": "a" * 32, "revision": 1,
                            "text": [], "memory_delta_v1": {}}, factory=no_state)
        self.assertEqual(result["error"], "invalid_request")
        self.assertFalse(called)


class MuseContextProjectionTests(unittest.TestCase):
    setUp = fixtures.MuseWorkerTests.setUp
    incoming = fixtures.MuseWorkerTests.incoming
    call = fixtures.MuseWorkerTests.call
    begin = fixtures.MuseWorkerTests.begin
    session = fixtures.MuseWorkerTests.session

    CAPS = {"schema_version": 1, "features": [
        "recent_context_v1", "memory_delta_v1", "cloud_context_v1"]}

    def entry(self, entry_id="a" * 64, quote_count=3):
        source_at = self.now - 60
        return {"schema_version": 1, "id": entry_id, "revision": 1,
                "topic": "合成旧事项", "intent": "继续查明", "bot_action": "已核记录",
                "open_loop": "等待确认", "sources": [{"message_id": "historical-source",
                    "seq": 11, "received_at": source_at, "fingerprint": "b" * 64}],
                "source_at": source_at, "expires_at": source_at + 172800, "cutoff_seq": 11,
                "historical_refs": [], "admission": {
                    "kind": "reply", "job_id": "synthetic-job.json", "group_id": None},
                "quote_count": quote_count}

    def projection(self, entry=None):
        return {"recent_context_v1": {"schema_version": 1,
                "snapshot_id": "c" * 64, "entries": [entry or self.entry()], "high_water_seq": 11}}

    def basis(self, entry=None):
        entry = entry or self.entry()
        return {"payload": {"schema_version": 1, "scope": {
                    "bot_id": "PRIVATE-SYNTHETIC-BOT", "owner_id": "PRIVATE-SYNTHETIC-OWNER",
                    "conversation_id": "PRIVATE-SYNTHETIC-CONVERSATION", "login_epoch": "PRIVATE-EPOCH"},
                "snapshot_id": "c" * 64, "issued_at": self.now,
                "entries": [{"entry_id": entry["id"], "expires_at": entry["expires_at"]}]},
                "signature": "d" * 64}

    def cloud(self, status="available"):
        projection = {"source_id": "wbridge-synthetic", "case_id": "continuity-smoke",
                      "publisher_id": "wbridge-synthetic-publisher", "source_revision": 1,
                      "source_updated_at": self.now - 30, "source_ref": "synthetic-origin-r1",
                      "fact_status": "confirmed", "title": "合成事项", "facts": "已保存",
                      "change": "首次结论", "next_step": "核对", "open_loop": "待验",
                      "source_digest": "e" * 64, "bridge_checked_at": self.now - 10,
                      "bridge_applied_at": self.now - 10, "observed_source_revision": 1}
        if status in ("empty", "disabled", "paused", "expired", "skipped"):
            projection = None
        return {"schema_version": 1, "status": status, "projection": projection}

    def install_read_context(self, projection=None, basis=None):
        self.read_projection = self.projection() if projection is None else projection
        self.read_basis = self.basis() if basis is None else basis
        self.read_batches = []
        self.read_bindings = []
        self.quote_calls = []
        caps = patch.object(adapter, "_context_capabilities", return_value=deepcopy(self.CAPS))
        caps.start()
        self.addCleanup(caps.stop)

        def context_for_batch(batch, cloud_binding=None):
            self.assertTrue(batch["owned"])
            self.read_batches.append(list(batch["owned"]))
            self.read_bindings.append(cloud_binding)
            return SimpleNamespace(projection=self.read_projection, basis=self.read_basis)

        def context_quote(batch, basis, entry_id, quote_index):
            self.assertEqual(set(batch), {"lease", "owned"})
            self.assertEqual(batch["owned"], ["a"])
            self.assertEqual(basis, self.read_basis)
            private = self.session(self.current_handle)
            self.assertEqual(batch["lease"], private["lease"])
            self.quote_calls.append((entry_id, quote_index))
            entry = self.read_projection["recent_context_v1"]["entries"][0]
            if entry_id != entry["id"] or quote_index >= entry["quote_count"]:
                return {"status": "unavailable", "reason": "entry_unavailable"}
            return {"status": "ok", "entry_id": entry_id, "expires_at": entry["expires_at"],
                    "quote": {"speaker": "bot" if quote_index == 2 else "user",
                              "message_id": "historical-source", "text": "synthetic quote %d" % quote_index,
                              "truncated": quote_index == 2, "private_extra": UnreadableExtra()},
                    "private_extra": UnreadableExtra()}

        self.state.context_for_batch = context_for_batch
        self.state.context_quote = context_quote

    def quote_request(self, batch, index=0, entry_id="a" * 64):
        return {"action": "context_quote", "handle": batch["handle"], "revision": batch["revision"],
                "entry_id": entry_id, "quote_index": index}

    def test_projection_and_private_basis_use_exact_schema_without_quote_text(self):
        projection = self.projection()
        projection["unknown_private"] = UnreadableExtra()
        projection["recent_context_v1"]["unknown_private"] = UnreadableExtra()
        entry = projection["recent_context_v1"]["entries"][0]
        entry["unknown_private"] = UnreadableExtra()
        entry["sources"][0]["unknown_private"] = UnreadableExtra()
        self.install_read_context(projection=projection)
        _, batch = self.begin()
        self.assertEqual(batch["context"], self.projection())
        public = json.dumps(batch)
        self.assertNotIn("PRIVATE", public)
        self.assertNotIn("signature", public)
        self.assertNotIn("quote 0", public)
        private = self.session(batch["handle"])
        self.assertEqual(private["context_basis"], self.read_basis)
        self.assertNotIn("messages", private)

    def test_capabilities_do_not_grant_unknown_features_or_traverse_private_fields(self):
        module = ModuleType("wechat_bridge.context_runtime")
        raw = dict(self.CAPS, private=UnreadableExtra())
        module.capabilities = lambda: raw
        with patch.dict(sys.modules, {"wechat_bridge.context_runtime": module}):
            self.assertEqual(adapter._context_capabilities(), self.CAPS)
            for value in ({"schema_version": True, "features": []},
                          {"schema_version": 1, "features": ["recent_context_v1"] * 2},
                          {"schema_version": 1, "features": "recent_context_v1"}):
                raw = value
                self.assertIsNone(adapter._context_capabilities())
            raw = {"schema_version": 1, "features": ["unknown_future_feature"]}
            result = adapter._context_capabilities()
            self.assertTrue(result is None or not result["features"])

    def test_cloud_only_without_fixed_binding_does_not_read_optional_context(self):
        self.install_read_context(projection={"cloud_context_v1": self.cloud()})
        caps = {"schema_version": 1, "features": ["cloud_context_v1"]}
        with patch.object(adapter, "_context_capabilities", return_value=caps):
            _, batch = self.begin()
        self.assertNotIn("context_capabilities", batch)
        self.assertNotIn("context", batch)
        self.assertFalse(self.read_batches)
        self.assertNotIn("context_basis", self.session(batch["handle"]))

    def test_missing_cloud_binding_keeps_recent_context_only(self):
        projection = self.projection()
        projection["cloud_context_v1"] = self.cloud()
        self.install_read_context(projection=projection)
        _, batch = self.begin()
        self.assertEqual(batch["context"], self.projection())
        self.assertEqual(batch["context_capabilities"], {
            "schema_version": 1, "features": ["recent_context_v1", "memory_delta_v1"]})
        self.assertEqual(self.read_bindings, [None])

    def test_public_adapter_rejects_arbitrary_or_partial_cloud_binding_before_state(self):
        constructed = []

        def no_state(base, state_dir):
            constructed.append(True)
            raise AssertionError("invalid configuration constructed WorkerState")

        bindings = (("wbridge-synthetic", "continuity-smoke", "wbridge-synthetic-publisher"),
                    {"source_id": "wbridge-synthetic"},
                    {"source_id": "wbridge-synthetic", "case_id": "continuity-smoke"},
                    {"source_id": "another-source", "case_id": "continuity-smoke",
                     "publisher_id": "wbridge-synthetic-publisher"})
        for binding in bindings:
            with self.subTest(binding=binding):
                result = adapter.handle_request({"action": "begin", "lease": "synthetic-lease"},
                    self.base, self.state_dir, state_factory=no_state, cloud_binding=binding)
                self.assertEqual(result["error"], "invalid_configuration")
                self.assertTrue(result["must_stop"])
        self.assertFalse(constructed)

    def test_fixed_synthetic_binding_is_private_and_reaches_cloud_read(self):
        projection = self.projection()
        projection["cloud_context_v1"] = self.cloud()
        self.install_read_context(projection=projection)
        self.incoming("a")
        event = self.state.plan()["payload"]
        binding = {"source_id": "wbridge-synthetic", "case_id": "continuity-smoke",
                   "publisher_id": "wbridge-synthetic-publisher"}
        module = ModuleType("wechat_bridge.cloud_context")
        module.CloudBinding = lambda **fields: SimpleNamespace(**fields)
        with patch.dict(sys.modules, {"wechat_bridge.cloud_context": module}):
            batch = adapter.handle_request({"action": "begin", "lease": event["lease"]},
                self.base, self.state_dir, state_factory=lambda base, state_dir: self.state,
                cloud_binding=binding)
        self.assertTrue(batch["ok"], batch)
        self.assertEqual(batch["context"], projection)
        self.assertEqual(vars(self.read_bindings[0]), binding)
        self.assertEqual(self.session(batch["handle"])["context_cloud_binding"], binding)
        self.assertNotIn("context_cloud_binding", batch)

    def test_every_fixed_context_reason_survives_and_unknown_reason_is_neutral(self):
        reasons = ("storage_busy", "storage_invalid", "storage_changed", "storage_too_large",
                   "sequence_unavailable", "scope_mismatch", "epoch_mismatch", "clock_invalid",
                   "clock_anomaly", "too_large", "projection_too_large", "entry_unavailable",
                   "invalid_entry", "entry_too_large", "scope_unavailable", "context_unavailable")
        for reason in reasons:
            with self.subTest(reason=reason):
                raw = {"recent_context_status": reason}
                self.assertEqual(adapter._context_projection(raw, self.CAPS), raw)
        result = adapter._context_projection({"context_status": "PRIVATE-ERROR-TEXT"}, self.CAPS)
        self.assertEqual(result, {"context_status": "context_unavailable"})

    def test_recent_bad_types_or_relationships_degrade_without_losing_cloud_card(self):
        mutations = (("revision", True), ("revision", 1.0), ("quote_count", True),
                     ("quote_count", 4), ("expires_at", self.now + 1),
                     ("topic", "x" * 49), ("intent", "x" * 81))
        for name, value in mutations:
            with self.subTest(field=name, value=value):
                projection = self.projection()
                projection["recent_context_v1"]["entries"][0][name] = value
                projection["cloud_context_v1"] = self.cloud()
                result = adapter._context_projection(projection, self.CAPS)
                self.assertNotIn("recent_context_v1", result)
                self.assertEqual(result["cloud_context_v1"], self.cloud())
                self.assertEqual(result["context_status"], "context_unavailable")
        projection = self.projection()
        entry = projection["recent_context_v1"]["entries"][0]
        entry.update(topic="a" * 48, intent="b" * 80, bot_action="c" * 128, open_loop="d")
        self.assertNotIn("recent_context_v1", adapter._context_projection(projection, self.CAPS))
        projection = self.projection()
        projection["recent_context_v1"]["entries"][0]["sources"][0]["seq"] = True
        self.assertNotIn("recent_context_v1", adapter._context_projection(projection, self.CAPS))

    def test_utf8_limits_apply_to_entry_recent_cloud_and_total(self):
        self.assertEqual(adapter._context_limit({"x": "a" * 16376}, 16384), {"x": "a" * 16376})
        for value in ({"x": "a" * 16377}, {"x": "💡" * 4096}):
            with self.assertRaises(adapter.SessionError):
                adapter._context_limit(value, 16384)
        entry = self.entry()
        entry["sources"] = [dict(entry["sources"][0], message_id="m%d" % i) for i in range(80)]
        result = adapter._context_projection(self.projection(entry), self.CAPS)
        self.assertNotIn("recent_context_v1", result)
        projection = self.projection()
        projection["recent_context_v1"]["entries"] = []
        for index in range(10):
            entry = self.entry(entry_id="%064x" % index)
            entry.update(topic="💡" * 48, intent="💡" * 80, bot_action="💡" * 128, open_loop="")
            projection["recent_context_v1"]["entries"].append(entry)
        self.assertNotIn("recent_context_v1", adapter._context_projection(projection, self.CAPS))
        cloud = self.cloud()
        cloud["projection"].update(source_ref="💡" * 512, source_id="💡" * 128,
                                   case_id="💡" * 128, publisher_id="💡" * 128,
                                   title="💡" * 512, facts="", change="", next_step="", open_loop="")
        self.assertNotIn("cloud_context_v1", adapter._context_projection({"cloud_context_v1": cloud}, self.CAPS))

    def test_cloud_fixed_statuses_preserve_missing_and_stale_semantics(self):
        for status in ("empty", "disabled", "available", "unavailable", "update_pending", "paused", "expired", "skipped"):
            with self.subTest(status=status):
                card = self.cloud(status)
                result = adapter._context_projection({"cloud_context_v1": card}, self.CAPS)
                self.assertEqual(result.get("cloud_context_v1"), card)
        card = self.cloud()
        card["projection"]["source_revision"] = True
        self.assertNotIn("cloud_context_v1", adapter._context_projection({"cloud_context_v1": card}, self.CAPS))

    def test_context_refresh_updates_private_basis_without_changing_message_digest(self):
        self.install_read_context()
        _, batch = self.begin()
        before = self.session(batch["handle"])
        self.read_projection = self.projection()
        self.read_projection["recent_context_v1"]["snapshot_id"] = "f" * 64
        self.read_projection["recent_context_v1"]["entries"][0]["topic"] = "合成新事项"
        self.read_basis = self.basis()
        self.read_basis["payload"]["snapshot_id"] = "f" * 64
        self.read_basis["signature"] = "0" * 64
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        after = self.session(batch["handle"])
        self.assertEqual(after["batch_digest"], before["batch_digest"])
        self.assertEqual(after["reference_digests"], before["reference_digests"])
        self.assertEqual(after["reference_bindings_v2"], before["reference_bindings_v2"])
        self.assertNotEqual(after["context_basis"], before["context_basis"])
        self.assertEqual(refreshed["revision"], batch["revision"] + 1)
        self.assertEqual(refreshed["context"], self.read_projection)

    def test_quote_uses_zero_based_index_and_private_minimal_live_batch(self):
        self.install_read_context()
        _, batch = self.begin()
        self.current_handle = batch["handle"]
        before = self.session(batch["handle"])
        for index in range(3):
            result = self.call(self.quote_request(batch, index))
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["result"]["quote"]["text"], "synthetic quote %d" % index)
            self.assertNotIn("PRIVATE", json.dumps(result))
            self.assertNotIn("private_extra", result["result"])
            self.assertNotIn("private_extra", result["result"]["quote"])
        self.assertEqual(self.quote_calls, [("a" * 64, index) for index in range(3)])
        self.assertEqual(self.session(batch["handle"]), before)
        self.assertEqual(self.call(self.quote_request(batch, 3))["error"], "invalid_request")

    def test_quote_beyond_actual_count_or_wrong_revision_stays_nonterminal(self):
        self.install_read_context(projection=self.projection(self.entry(quote_count=1)))
        _, batch = self.begin()
        self.current_handle = batch["handle"]
        unavailable = self.call(self.quote_request(batch, 1))
        self.assertTrue(unavailable["ok"])
        self.assertEqual(unavailable["result"], {"status": "unavailable", "reason": "entry_unavailable"})
        request = self.quote_request(batch)
        request["revision"] += 1
        self.assertEqual(self.call(request)["error"], "revision_mismatch")
        self.assertEqual(self.session(batch["handle"])["phase"], "active")

    def test_bad_quote_result_and_bad_basis_degrade_without_closing_handle(self):
        self.install_read_context()
        _, batch = self.begin()
        self.current_handle = batch["handle"]
        self.state.context_quote = lambda *args: {
            "status": "ok", "entry_id": "a" * 64, "expires_at": self.entry()["expires_at"],
            "quote": {"speaker": "user", "message_id": "historical-source", "text": "💡" * 513,
                      "truncated": True}}
        result = self.call(self.quote_request(batch))
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["status"], "unavailable")
        self.assertEqual(self.session(batch["handle"])["phase"], "active")
        self.read_basis = dict(self.basis(), unapproved=UnreadableExtra())
        refreshed = self.call({"action": "refresh", "handle": batch["handle"]})
        self.assertTrue(refreshed["ok"])
        self.assertEqual(refreshed["context"], {"context_status": "context_unavailable"})
        self.assertNotIn("context_basis", self.session(batch["handle"]))


if __name__ == "__main__":
    unittest.main()
