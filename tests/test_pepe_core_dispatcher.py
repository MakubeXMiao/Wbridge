"""Offline PEPE public-layer integration; no HTTP, credentials or live Store."""
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from wechat_bridge.backends.registry import validate_config
from wechat_bridge.dispatcher import Dispatcher
from wechat_bridge.serve import handle_incoming
from wechat_bridge.store import Store, atomic_write_json


class FakeDriver:
    """A fake remote peer that uses the same journal boundary as the adapter."""
    def __init__(self, backend, task_id, submit_state):
        self.backend, self.task_id = backend, task_id
        self.submit_state = submit_state
        self.request = None
        self.events, self.wire = [], []
        self.closed, self.cancel_count = False, 0

    def start(self, request):
        self.request = request
        if self.backend != "pepe" or self.submit_state == "prepared":
            return
        snapshot = request["remote_task_journal"].begin_submit()
        self.wire.append(copy.deepcopy(snapshot["submit"]))
        if self.submit_state == "bound":
            request["remote_task_journal"].bind_task(self.task_id)

    def poll(self):
        result, self.events = self.events, []
        return result

    def respond_approval(self, approval_id, decision, action_digest):
        raise AssertionError("PEPE does not implement public approval delivery")

    def cancel(self):
        self.cancel_count += 1

    def close(self):
        self.closed = True

    def event(self, kind, **changes):
        result = {"type": kind, "event_id": "event-1",
                  "execution_id": self.request["execution_id"],
                  "attempt_id": self.request["attempt_id"],
                  "remote_task_id": self.task_id, "session_id": None}
        result.update(changes)
        return result

    def emit(self, kind, **changes):
        event = self.event(kind, **changes)
        self.events.append(event)
        return event


class ForbiddenCredentialEnvironment(dict):
    def __getitem__(self, key):
        if key == "PEPE_TEST_TOKEN":
            raise AssertionError("public core read a provider credential")
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key == "PEPE_TEST_TOKEN":
            raise AssertionError("public core read a provider credential")
        return super().get(key, default)


class PepeCoreDispatcherTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0

    def runtime(self, submit_state="bound", timeout=1800, unverified=False):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        store = Store(temp.name)
        pepe = {"provider": "pepe", "transport": "http", "instance_id": "pepe-synthetic",
                "base_url": "https://dot2api.example.invalid", "credential_env": "PEPE_TEST_TOKEN",
                "tenant": "tenant-synthetic", "principal": "principal-synthetic", "queue": "pepe",
                "queue_ownership": "single_store", "instructions": "Answer from the supplied text.",
                "execution_policy": ({"mode": "text_only", "enforcement": "unverified"} if unverified else
                                     {"mode": "native_delegate", "approval": "platform_native"})}
        config = validate_config({
            "schema_version": 1, "account_scope": "account-synthetic", "default_backend": "pepe",
            "job_timeout_seconds": timeout, "backends": {
                "pepe": pepe,
                "codex": {"instance_id": "cli-synthetic", "command": [sys.executable],
                          "cwd": temp.name,
                          "execution_policy": {"approval": "explicit", "sandbox": "workspace-write"}}}})
        created = []

        def factory(backend, selected):
            driver = FakeDriver(backend, "remote-task-%d" % (len(created) + 1), submit_state)
            created.append(driver)
            return driver

        dispatcher = Dispatcher(store, config, "owner", "bot", factory=factory, clock=lambda: self.now)
        self.addCleanup(dispatcher.close)
        dispatcher.acquire()
        store.managed_inbox_dir = dispatcher.inbox
        return SimpleNamespace(store=store, config=config, created=created,
                               dispatcher=dispatcher, factory=factory)

    def receive(self, rt, mid="input-1", text="hello", **changes):
        record = {"message_id": mid, "from_user_id": "owner", "text": text,
                  "context_token": "synthetic-private-context", "received_at": self.now,
                  "input_provenance": {"schema_version": 1, "single_chat": True, "item_types": [1]}}
        record.update(changes)
        atomic_write_json(str(Path(rt.dispatcher.inbox) / (mid + ".json")), record)

    def rows(self, rt, table):
        with rt.dispatcher.state.transaction() as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table)]

    def jobs(self, rt):
        return [json.loads(row["payload"]) for row in self.rows(rt, "jobs")]

    def outputs(self, rt, kind=None):
        values = [json.loads(row["payload"]) for row in self.rows(rt, "outputs")]
        return values if kind is None else [v for v in values if v["backend_v1"]["output_kind"] == kind]

    def drain(self, rt):
        for path in Path(rt.store.pending_dir).glob("*.json"):
            path.rename(Path(rt.store.sent_dir) / path.name)

    def test_real_receiver_to_frozen_submit_and_single_public_final(self):
        rt = self.runtime()
        msg = {"message_type": 1, "message_id": "actual-input", "from_user_id": "owner",
               "context_token": "synthetic-private-context",
               "item_list": [{"type": 1, "text_item": {"text": "actual words"}}]}
        self.assertTrue(handle_incoming(rt.store, msg, "owner"))
        rt.dispatcher.tick()
        driver = rt.created[0]
        request = driver.request
        self.assertNotIn("cwd", request)
        self.assertIsNone(request["session_id"])
        self.assertEqual(request["transport"], "http")
        self.assertEqual(request["execution_policy"], {
            "mode": "native_delegate", "approval": "platform_native"})
        frozen = request["remote_task_journal"].snapshot()
        self.assertEqual(frozen["state"], "bound")
        self.assertEqual(frozen["task_id"], driver.task_id)
        self.assertEqual(driver.wire, [request["remote_task"]["submit"]])
        self.assertEqual(driver.wire[0]["payload"], {
            "messages": [{"role": "user", "content": "actual words"}]})
        self.assertNotIn("synthetic-private-context", json.dumps(driver.wire))
        self.assertNotIn("context_token", json.dumps(driver.wire))
        event = driver.emit("final", text="remote answer")
        driver.events.append(copy.deepcopy(event))
        rt.dispatcher.tick()
        rt.dispatcher.event(request["execution_id"], event)
        self.assertEqual(self.jobs(rt)[0]["status"], "succeeded")
        self.assertEqual(len(self.outputs(rt, "final")), 1)
        envelope = self.outputs(rt, "final")[0]
        self.assertEqual(envelope["text"], "remote answer")
        self.assertEqual(envelope["backend_v1"]["execution_id"], request["execution_id"])

    def test_unverified_policy_rejects_before_prepare_factory_or_credentials(self):
        rt = self.runtime(unverified=True)
        self.receive(rt)
        with patch("wechat_bridge.dispatcher.prepare_pepe_request",
                   side_effect=AssertionError("policy must precede request/journal preparation")), \
                patch("os.environ", ForbiddenCredentialEnvironment()):
            rt.dispatcher.tick()
        job = self.jobs(rt)[0]
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["side_effect_state"], "none")
        self.assertEqual(job["error_code"], "pepe_execution_policy_unverified")
        self.assertEqual(rt.created, [])
        self.drain(rt)
        self.receive(rt, "route-cli", "/backend codex")
        rt.dispatcher.tick()
        self.drain(rt)
        self.receive(rt, "cli-input", "local work")
        rt.dispatcher.tick()
        self.assertEqual([d.backend for d in rt.created], ["codex"])
        self.assertEqual(rt.created[0].request["execution_policy"]["approval"], "explicit")
        self.assertNotIn("remote_task", rt.created[0].request)

    def test_public_preparation_does_not_read_credential_environment(self):
        rt = self.runtime()
        self.receive(rt)
        with patch("os.environ", ForbiddenCredentialEnvironment()):
            rt.dispatcher.tick()
        self.assertEqual(len(rt.created), 1)
        self.assertEqual(len(rt.created[0].wire), 1)

    def test_old_source_and_media_inputs_fail_before_factory_without_path_reads(self):
        cases = ({"input_provenance": None}, {"media_paths": ["/not/opened/private-file"]},
                 {"media_failed": True}, {"media_failed_count": 1},
                 {"attachment_refs": [{"attachment_id": "synthetic-id", "status": "ready"}]})
        for changes in cases:
            with self.subTest(changes=changes):
                rt = self.runtime()
                self.receive(rt, **changes)
                with patch("wechat_bridge.dispatcher.os.path.isfile",
                           side_effect=AssertionError("PEPE must not inspect attachment paths")):
                    rt.dispatcher.tick()
                self.assertEqual(rt.created, [])
                job = self.jobs(rt)[0]
                self.assertEqual(job["status"], "failed")
                self.assertEqual(job["side_effect_state"], "none")

    def test_too_short_remote_deadline_is_unsent_failure(self):
        from wechat_bridge.backends.contract import BackendError
        from wechat_bridge.pepe_core import prepare_pepe_request
        from wechat_bridge.remote_tasks import RemoteTaskJournal

        rt = self.runtime(timeout=66)
        self.receive(rt)

        def delayed_prepare(state, job, selected, now, clock):
            # Consume real remaining budget after validation, without replacing
            # the policy gate or the helper's deadline/journal logic.
            self.now += 2
            return prepare_pepe_request(state, job, selected, clock(), clock=clock)

        with patch("wechat_bridge.dispatcher.prepare_pepe_request", side_effect=delayed_prepare) as prepare:
            rt.dispatcher.tick()
        prepare.assert_called_once()
        self.assertEqual(rt.created, [])
        job = self.jobs(rt)[0]
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["side_effect_state"], "none")
        self.assertEqual(job["error_code"], "pepe_deadline_too_short")
        journal = RemoteTaskJournal(rt.dispatcher.state, job["execution_id"], job["attempt_id"],
                                    job["backend_instance_ref"], clock=lambda: self.now)
        with self.assertRaisesRegex(BackendError, "remote_task_not_prepared"):
            journal.snapshot()

    def test_minimum_timeout_can_dispatch_after_normal_fractional_elapsed_time(self):
        from wechat_bridge.pepe_core import prepare_pepe_request

        rt = self.runtime(timeout=66)
        self.receive(rt)

        def delayed_prepare(state, job, selected, now, clock):
            self.now += 0.01
            return prepare_pepe_request(state, job, selected, clock(), clock=clock)

        with patch("wechat_bridge.dispatcher.prepare_pepe_request", side_effect=delayed_prepare):
            rt.dispatcher.tick()
        self.assertEqual(len(rt.created), 1)
        self.assertEqual(len(rt.created[0].wire), 1)
        self.assertEqual(self.jobs(rt)[0]["status"], "running")

    def test_pepe_ignores_preexisting_provider_session(self):
        rt = self.runtime()
        self.receive(rt)
        rt.dispatcher.ingest()
        job = self.jobs(rt)[0]
        with rt.dispatcher.state.transaction() as db:
            db.execute("INSERT INTO sessions VALUES (?,?)", (job["conversation_key"], "stale-provider-session"))
        rt.dispatcher.tick()
        self.assertIsNone(rt.created[0].request["session_id"])
        self.assertNotIn("stale-provider-session", json.dumps(rt.created[0].wire))

    def test_wrong_remote_task_cannot_publish_a_valid_looking_final(self):
        rt = self.runtime()
        self.receive(rt)
        rt.dispatcher.tick()
        rt.created[0].emit("final", remote_task_id="another-remote-task", text="must not publish")
        rt.dispatcher.tick()
        self.assertEqual(self.jobs(rt)[0]["status"], "unknown")
        self.assertEqual(self.outputs(rt, "final"), [])

    def test_wrong_attempt_cannot_publish(self):
        rt = self.runtime()
        self.receive(rt)
        rt.dispatcher.tick()
        rt.created[0].emit("final", attempt_id="old-attempt", text="must not publish")
        rt.dispatcher.tick()
        self.assertEqual(self.jobs(rt)[0]["status"], "unknown")
        self.assertEqual(self.outputs(rt, "final"), [])

    def test_pepe_session_or_approval_event_never_establishes_continuation_or_permission(self):
        for kind, changes in (("final", {"session_id": "remote-session", "text": "answer"}),
                              ("approval_requested", {"approval_id": "action-1", "action_summary": "external write",
                                                      "action_digest": "a" * 64})):
            with self.subTest(kind=kind):
                rt = self.runtime()
                self.receive(rt)
                rt.dispatcher.tick()
                rt.created[0].emit(kind, **changes)
                rt.dispatcher.tick()
                self.assertEqual(self.jobs(rt)[0]["status"], "unknown")
                self.assertEqual(self.rows(rt, "approvals"), [])
                self.assertEqual(self.rows(rt, "sessions"), [])
                self.assertEqual(self.outputs(rt, "final"), [])

    def test_submitting_or_bound_remote_failures_retain_unknown_even_with_none_claim(self):
        for state in ("submitting", "bound"):
            for kind in ("failed", "cancelled"):
                with self.subTest(state=state, kind=kind):
                    rt = self.runtime(submit_state=state)
                    self.receive(rt)
                    rt.dispatcher.tick()
                    driver = rt.created[0]
                    driver.emit(kind, error_code="remote_terminal", side_effect_state="none",
                                remote_task_id=driver.task_id if state == "bound" else None)
                    rt.dispatcher.tick()
                    self.assertEqual(self.jobs(rt)[0]["status"], "unknown")
                    self.assertEqual(driver.request["remote_task_journal"].snapshot()["state"], state)
                    self.assertTrue(driver.closed)

    def test_proven_unsent_failure_can_be_terminal(self):
        rt = self.runtime(submit_state="prepared")
        self.receive(rt)
        rt.dispatcher.tick()
        driver = rt.created[0]
        driver.emit("failed", error_code="local_validation", side_effect_state="none", remote_task_id=None)
        rt.dispatcher.tick()
        self.assertEqual(self.jobs(rt)[0]["status"], "failed")
        self.assertEqual(driver.wire, [])
        self.assertEqual(driver.request["remote_task_journal"].snapshot()["state"], "prepared")

    def test_global_busy_and_unknown_fence_cover_another_sender_job(self):
        rt = self.runtime()
        self.receive(rt)
        rt.dispatcher.tick()
        first = self.jobs(rt)[0]
        # The owner-only receiver is unchanged. Seed an independent durable
        # sender job to exercise the Store-wide scheduler invariant directly.
        other = copy.deepcopy(first)
        other.update(execution_id="other-sender-job", attempt_id="other-sender-attempt",
                     sender_key="other-sender", status="queued", events=[])
        with rt.dispatcher.state.transaction() as db:
            rt.dispatcher.state.put_job(db, other)
        rt.dispatcher.tick()
        self.assertEqual(len(rt.created), 1)
        rt.created[0].emit("unknown", error_code="remote_connection_unknown")
        rt.dispatcher.tick()
        rt.dispatcher.tick()
        self.assertEqual(len(rt.created), 1)
        self.assertEqual({j["execution_id"]: j["status"] for j in self.jobs(rt)},
                         {first["execution_id"]: "unknown", "other-sender-job": "queued"})

    def test_restart_keeps_mapping_unknown_and_rejects_late_result_without_replay(self):
        rt = self.runtime()
        self.receive(rt)
        rt.dispatcher.tick()
        driver = rt.created[0]
        snapshot = driver.request["remote_task_journal"].snapshot()
        late = driver.event("final", text="late result")
        self.receive(rt, "input-2", "queued later")
        rt.dispatcher.tick()
        queued = next(j["execution_id"] for j in self.jobs(rt) if j["status"] == "queued")
        rt.dispatcher.close()
        replacement = Dispatcher(rt.store, rt.config, "owner", "bot", factory=rt.factory,
                                 clock=lambda: self.now)
        self.addCleanup(replacement.close)
        replacement.acquire()
        rt.dispatcher = replacement
        replacement.tick()
        replacement.event(snapshot["execution_id"], late)
        self.assertEqual(len(rt.created), 1)
        self.assertEqual(len(driver.wire), 1)
        self.assertEqual({j["execution_id"]: j["status"] for j in self.jobs(rt)},
                         {snapshot["execution_id"]: "unknown", queued: "queued"})
        self.assertEqual(driver.request["remote_task_journal"].snapshot(), snapshot)
        self.assertEqual(self.outputs(rt, "final"), [])


if __name__ == "__main__":
    unittest.main()
