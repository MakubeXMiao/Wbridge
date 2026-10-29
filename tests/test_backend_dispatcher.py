"""Synthetic supervisor regressions; no provider, WeChat or real sockets."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from wechat_bridge.backend_state import digest
from wechat_bridge.dispatcher import Dispatcher, managed_inbox
from wechat_bridge.serve import _is_mergeable_text, _write_inbox_record, handle_incoming
from wechat_bridge.store import Store, atomic_write_json


class FakeDriver:
    def __init__(self):
        self.events, self.decisions = [], []
        self.request = None
        self.cancel_count = 0
        self.closed = False

    def start(self, request):
        self.request = request

    def poll(self):
        result, self.events = self.events, []
        return result

    def respond_approval(self, approval_id, decision, action_digest):
        self.decisions.append((approval_id, decision, action_digest))

    def cancel(self):
        self.cancel_count += 1

    def close(self):
        self.closed = True

    def emit(self, kind, **extra):
        event = {"event_id": "event-%s" % (len(self.events) + 1),
                 "type": kind, "execution_id": self.request["execution_id"],
                 "attempt_id": self.request["attempt_id"]}
        event.update(extra)
        self.events.append(event)
        return event


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(self.temp.name)
        self.now = 1000.0
        self.created = []
        selected = {"instance_id": "instance-a", "command": [sys.executable], "cwd": self.temp.name,
                    "execution_policy": {"approval": "explicit", "sandbox": "workspace-write"}}
        self.config = {"schema_version": 1, "account_scope": "test-account", "default_backend": "codex",
                       "job_timeout_seconds": 1800, "approval_timeout_seconds": 300, "legacy_state_dir": None,
                       "backends": {"codex": selected, "claude_code": dict(selected, instance_id="instance-b")}}
        self.dispatcher = self.make_dispatcher()
        self.dispatcher.acquire()
        self.store.managed_inbox_dir = self.dispatcher.inbox

    def make_dispatcher(self, store=None, config=None, owner="owner", bot="bot"):
        return Dispatcher(store or self.store, config or self.config, owner, bot,
                          factory=self.factory, clock=lambda: self.now)

    def factory(self, backend, config):
        driver = FakeDriver()
        self.created.append(driver)
        return driver

    def tearDown(self):
        self.dispatcher.close()
        self.temp.cleanup()

    def receive(self, mid, text):
        _write_inbox_record(self.store, {"message_id": mid, "context_token": "context-test"}, "owner", text)

    def tick(self):
        self.dispatcher.tick()

    def rows(self, table):
        with self.dispatcher.state.transaction() as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table).fetchall()]

    def job(self, n=0):
        return json.loads(self.rows("jobs")[n]["payload"])

    def approval(self):
        self.receive("1", "work")
        self.tick()
        driver = self.created[0]
        driver.emit("approval_requested", approval_id="provider-action-1", action_summary="write synthetic file",
                    action_digest="a" * 64)
        self.tick()
        return driver, json.loads(self.rows("approvals")[0]["payload"])

    def drain(self):
        for path in Path(self.store.pending_dir).glob("*.json"):
            path.rename(Path(self.store.sent_dir) / path.name)

    def test_inputs_frozen_and_later_messages_serialized(self):
        self.receive("1", "first")
        self.tick()
        self.receive("2", "later")
        self.tick()
        self.assertEqual(len(self.created), 1)
        self.assertEqual([m["message_id"] for m in self.created[0].request["messages"]], ["1"])
        self.created[0].emit("final", text="result", session_id="provider-session")
        self.tick()
        self.assertEqual(len(self.created), 2)
        self.assertEqual(self.created[1].request["session_id"], "provider-session")
        self.assertEqual(self.created[1].request["messages"][0]["message_id"], "2")

    def test_duplicate_receive_and_final_admit_once(self):
        self.receive("1", "work")
        self.tick()
        self.receive("1", "replayed")
        driver = self.created[0]
        event = driver.emit("final", text="final")
        driver.events.append(copy.deepcopy(event))
        self.tick()
        self.assertEqual(len(self.rows("jobs")), 1)
        self.assertEqual(len(self.rows("outputs")), 1)
        self.assertEqual(len(list(Path(self.store.pending_dir).glob("*.json"))), 1)
        self.dispatcher.event(driver.request["execution_id"], dict(event, event_id="late"))
        self.assertEqual(len(self.rows("outputs")), 1)

    def test_backend_switch_only_idle(self):
        self.receive("1", "/backend claude_code")
        self.tick()
        self.assertEqual(self.rows("routes")[0]["backend"], "claude_code")
        self.drain()
        self.receive("2", "work")
        self.tick()
        self.receive("3", "/backend codex")
        self.tick()
        self.assertEqual(self.rows("routes")[0]["backend"], "claude_code")
        self.assertEqual(self.created[0].request["backend_id"], "claude_code")

    def test_pending_control_delivery_blocks_switch(self):
        self.receive("1", "/status")
        self.tick()
        self.receive("2", "/backend claude_code")
        self.tick()
        self.assertEqual(self.rows("routes")[0]["backend"], "codex")

    def test_explicit_approval_binds_digest_and_consumes_once(self):
        driver, approval = self.approval()
        self.receive("2", "好")
        self.tick()
        self.assertFalse(driver.decisions)
        self.receive("3", "/approve %s allow" % approval["id"])
        self.tick()
        self.receive("4", "/approve %s allow" % approval["id"])
        self.tick()
        self.assertEqual(driver.decisions, [("provider-action-1", "allow", "a" * 64)])
        self.assertEqual(self.rows("approvals")[0]["status"], "consumed")
        self.assertEqual(len(self.created), 1)

    def test_expired_approval_never_allows(self):
        driver, approval = self.approval()
        self.now = approval["expires_at"]
        self.receive("2", "/approve %s allow" % approval["id"])
        self.tick()
        self.assertEqual(driver.decisions, [])
        self.assertEqual(driver.cancel_count, 1)
        self.assertEqual(self.job()["status"], "cancelling")
        self.now += 11
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")

    def test_restart_invalidates_approvals_no_restart_or_grant(self):
        driver, approval = self.approval()
        self.dispatcher.close()
        self.dispatcher = self.make_dispatcher()
        self.dispatcher.acquire()
        self.receive("2", "/approve %s allow" % approval["id"])
        self.receive("3", "new work")
        self.tick()
        self.assertEqual(len(self.created), 1)
        self.assertEqual(driver.decisions, [])
        self.assertEqual(self.job()["status"], "unknown")
        self.assertEqual(self.rows("approvals")[0]["status"], "invalidated")

    def test_same_batch_cancel_revokes_collected_allow(self):
        driver, approval = self.approval()
        self.receive("2", "/approve %s allow" % approval["id"])
        self.receive("3", "/cancel " + self.job()["execution_id"])
        self.tick()
        self.assertEqual(driver.decisions, [])
        self.assertEqual(driver.cancel_count, 1)
        self.assertEqual(self.rows("approvals")[0]["status"], "invalidated")

    def test_denial_is_exact_one_time_not_persistent_policy(self):
        driver, approval = self.approval()
        self.receive("2", "/approve %s deny" % approval["id"])
        self.tick()
        self.assertEqual(driver.decisions[0][1], "deny")
        self.assertEqual(self.job()["status"], "running")

    def test_decision_delivery_exception_not_replayed(self):
        driver, approval = self.approval()
        driver.respond_approval = lambda *args: (_ for _ in ()).throw(OSError("disconnect"))
        self.receive("2", "/approve %s allow" % approval["id"])
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")
        self.tick()
        self.assertEqual(len(self.created), 1)

    def test_wrong_attempt_event_causes_unknown_not_output(self):
        self.receive("1", "work")
        self.tick()
        self.created[0].emit("final", text="wrong", attempt_id="stale-attempt")
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")
        self.assertFalse(any(json.loads(row["payload"])["text"] == "wrong" for row in self.rows("outputs")))

    def test_cancel_requires_confirmation_and_blocks_late_final(self):
        self.receive("1", "work")
        self.tick()
        driver = self.created[0]
        self.receive("2", "/cancel " + self.job()["execution_id"])
        self.tick()
        self.assertEqual(self.job()["status"], "cancelling")
        self.assertEqual(driver.cancel_count, 1)
        driver.emit("final", text="late")
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")

    def test_confirmed_cancel_terminal(self):
        self.receive("1", "work")
        self.tick()
        self.created[0].emit("cancelled")
        self.tick()
        self.assertEqual(self.job()["status"], "cancelled")

    def test_cannot_emit_final_while_permission_pending(self):
        driver, approval = self.approval()
        driver.emit("final", event_id="final-2", text="unauthorized")
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")

    def test_auth_owner_filter_no_command_or_private_body_retained(self):
        handle_incoming(self.store, {"message_id": "foreign", "from_user_id": "stranger", "text": "/backend claude_code"}, "owner")
        self.tick()
        self.assertEqual(self.rows("inputs"), [])
        self.assertFalse(list(Path(self.inbox if hasattr(self, 'inbox') else self.dispatcher.inbox).glob("*.json")))

    def test_raw_managed_inbox_wrong_owner_is_rejected(self):
        atomic_write_json(os.path.join(self.dispatcher.inbox, "bad.json"), {"message_id": "bad", "from_user_id": "other", "text": "bad"})
        with self.assertRaises(ValueError):
            self.tick()
        self.assertEqual(self.rows("jobs"), [])

    def test_managed_records_invisible_to_legacy_worker(self):
        self.receive("1", "work")
        self.assertFalse(list(Path(self.store.inbox_dir).glob("*.json")))
        self.assertTrue(list(Path(self.dispatcher.inbox).glob("*.json")))

    def test_same_state_rejects_different_bot_owner_and_config(self):
        for owner, bot in [("different", "bot"), ("owner", "other-bot")]:
            with self.assertRaises(ValueError):
                self.make_dispatcher(owner=owner, bot=bot)
        config = copy.deepcopy(self.config)
        config["backends"]["codex"]["instance_id"] = "other-instance"
        with self.assertRaises(ValueError):
            self.make_dispatcher(config=config)

    def test_independent_accounts_and_instances_do_not_share_sessions(self):
        self.receive("1", "account a")
        self.tick()
        self.created[0].emit("final", text="a", session_id="session-a")
        self.tick()
        with tempfile.TemporaryDirectory() as folder:
            store = Store(folder)
            config = copy.deepcopy(self.config)
            config["account_scope"] = "account-b"
            config["backends"]["codex"]["instance_id"] = "instance-b"
            other = self.make_dispatcher(store, config, owner="owner-b", bot="bot-b")
            other.acquire()
            try:
                atomic_write_json(os.path.join(other.inbox, "1.json"), {"message_id": "1", "from_user_id": "owner-b", "text": "b"})
                other.tick()
                request = self.created[-1].request
                self.assertIsNone(request["session_id"])
                self.assertNotEqual(request["conversation_key"], self.created[0].request["conversation_key"])
                self.assertNotEqual(request["account_scope"], self.created[0].request["account_scope"])
            finally:
                other.close()

    def test_publication_crash_preserves_unknown_without_duplicate(self):
        self.receive("1", "work")
        self.tick()
        self.created[0].emit("final", text="result")
        with patch("wechat_bridge.dispatcher.atomic_write_json", side_effect=OSError("crash")):
            with self.assertRaises(OSError):
                self.tick()
        self.assertEqual(self.rows("outputs")[0]["phase"], "publishing")
        self.dispatcher.close()
        self.dispatcher = self.make_dispatcher()
        self.dispatcher.acquire()
        self.tick()
        self.assertEqual(self.rows("outputs")[0]["phase"], "unknown")
        self.assertFalse(list(Path(self.store.pending_dir).glob("*.json")))

    def test_prepared_output_can_publish_after_restart(self):
        self.receive("1", "/status")
        self.dispatcher.ingest()
        self.dispatcher.close()
        self.dispatcher = self.make_dispatcher()
        self.dispatcher.acquire()
        self.tick()
        self.assertEqual(self.rows("outputs")[0]["phase"], "admitted")

    def test_no_credentials_context_or_shell_in_provider_request(self):
        self.receive("1", "work")
        self.tick()
        request = self.created[0].request
        self.assertNotIn("context_token", json.dumps(request))
        self.assertNotIn("bot_token", json.dumps(request))
        self.assertEqual(request["execution_policy"]["approval"], "explicit")

    def test_output_cannot_merge_even_if_client_key_removed(self):
        self.receive("1", "work")
        self.tick()
        self.created[0].emit("final", text="result")
        self.tick()
        envelope = json.loads(self.rows("outputs")[0]["payload"])
        envelope.pop("client_key")
        self.assertFalse(_is_mergeable_text("v1-output.json", envelope))

    def test_queued_job_survives_restart_without_running_old_job(self):
        self.receive("1", "queued")
        self.dispatcher.ingest()
        self.dispatcher.close()
        self.dispatcher = self.make_dispatcher()
        self.dispatcher.acquire()
        self.tick()
        self.assertEqual(len(self.created), 1)

    def test_same_bot_lock_survives_different_tempdirs(self):
        import wechat_bridge.serve as serve
        start = len(serve._lock_fhs)
        try:
            with patch.object(serve, "_bot_lock_root", return_value=os.path.join(self.temp.name, "bot-locks")):
                self.assertTrue(serve._acquire_bot_lock({"ilink_bot_id": "bot-a"}))
                with patch.dict(os.environ, {"TMPDIR": "/other/temp"}):
                    self.assertFalse(serve._acquire_bot_lock({"ilink_bot_id": "bot-a"}))
                    self.assertTrue(serve._acquire_bot_lock({"ilink_bot_id": "bot-b"}))
        finally:
            for handle in serve._lock_fhs[start:]:
                handle.close()
            del serve._lock_fhs[start:]

    def test_background_scheduler_exit_stops_receiver(self):
        import threading
        from wechat_bridge.serve import _run_backend_worker
        for failure in (None, RuntimeError, SystemExit):
            class Exiting:
                def run(self, stop):
                    if failure:
                        raise failure("synthetic")
            stop = threading.Event()
            _run_backend_worker(Exiting(), stop)
            self.assertTrue(stop.is_set())

    def test_shutdown_fences_before_provider_cleanup(self):
        driver, approval = self.approval()
        observed = []
        def close_probe():
            observed.append((self.job()["status"], self.rows("approvals")[0]["status"]))
        driver.close = close_probe
        self.dispatcher.close()
        self.assertEqual(observed, [("unknown", "invalidated")])

    def test_shutdown_persistence_error_still_closes_driver(self):
        driver, approval = self.approval()
        with patch.object(self.dispatcher.state, "recover", side_effect=OSError("synthetic disk error")):
            with self.assertRaises(OSError):
                self.dispatcher.close()
        self.assertTrue(driver.closed)
        self.assertIsNone(self.dispatcher.lock)

    def test_scheduler_single_owner(self):
        other = self.make_dispatcher()
        with self.assertRaises(RuntimeError):
            other.acquire()

    def test_event_burst_and_empty_final_fail_closed(self):
        self.receive("1", "work")
        self.tick()
        self.created[0].emit("final", text=" ")
        self.tick()
        self.assertEqual(self.job()["status"], "unknown")

    def test_muse_legacy_delivery_preserves_record_and_busy_switch(self):
        self.dispatcher.close()
        with self.dispatcher.state.transaction() as db:
            db.execute("INSERT INTO routes VALUES ('owner','muse',1)")
        # Same config fixture: enablement is separately validated by registry.
        self.dispatcher.acquire()
        self.receive("1", "legacy")
        self.tick()
        original = json.loads((Path(self.store.inbox_dir) / "1.json").read_text())
        self.assertEqual(original["context_token"], "context-test")
        self.receive("2", "/backend claude_code")
        self.tick()
        self.assertEqual(self.rows("routes")[0]["backend"], "muse")
        self.assertEqual(len(self.created), 0)


if __name__ == "__main__":
    unittest.main()
