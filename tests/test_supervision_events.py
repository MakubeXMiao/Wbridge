"""Synthetic supervision identities/signals; no real bridge service is started."""

import json
import math
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import worker_state as ws


class SupervisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wbridge-supervision-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.store = ws.Store(str(self.base))
        self.now = 1900000000.0
        self.events = []
        patcher = mock.patch.object(ws, "append_supervision_event", side_effect=lambda state, event: self.events.append(event))
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(ws, "launch_logged_service", side_effect=AssertionError("must mock synthetic launch"))
        patcher.start()
        self.addCleanup(patcher.stop)

    def identity(self, pid=12345, age=1000):
        return {"pid": pid, "start": self.now - age,
                "tokens": ["/SYNTHETIC_PRIVATE_USERNAME/wechat-bridge", "serve", "SYNTHETIC_SECRET_ARGUMENT"]}

    def setup_process(self, heartbeat=None):
        Path(self.store.pid_path).write_text("12345")
        if heartbeat is not None:
            Path(self.store.flusher_hb_path).write_text(str(heartbeat))

    def run_supervision(self, identity, sleep=lambda _: None):
        return ws.supervise(str(self.base), "/SYNTHETIC_PRIVATE_EXECUTABLE", lambda: self.now, identity, sleep)

    def assert_safe_events(self):
        for event in self.events:
            self.assertEqual(event["event"], "serve_supervision")
            self.assertIn(event["reason"], ws.SUPERVISION_REASONS)
            self.assertIn(event["action"], ws.SUPERVISION_ACTIONS)
            self.assertIn(event["result"], ws.SUPERVISION_RESULTS)
            self.assertEqual(event["exit_code"], "unknown")
            self.assertEqual(event["exit_signal"], "unknown")
            self.assertNotIn("SYNTHETIC_", json.dumps(event, allow_nan=False))
            self.assertNotIn("tokens", event)

    def test_healthy_process_maintains_logs_before_early_return(self):
        self.setup_process(self.now - 120)
        process = self.identity()
        with mock.patch.object(ws, "maintain_runtime_logs", return_value=True) as maintain, mock.patch.object(ws.os, "kill") as kill:
            self.assertEqual(self.run_supervision(lambda pid: process), {"alive": True, "restarted": False})
        maintain.assert_called_once_with(self.store.state_dir)
        kill.assert_not_called()
        self.assertEqual(self.events[-1]["reason"], "heartbeat_fresh")
        self.assert_safe_events()

    def test_previous_heartbeat_retains_180_second_startup_grace(self):
        self.setup_process(self.now - 1000)
        process = self.identity(age=180)
        with mock.patch.object(ws.os, "kill") as kill:
            self.assertTrue(self.run_supervision(lambda pid: process)["alive"])
        kill.assert_not_called()
        self.assertEqual(self.events[-1]["reason"], "startup_grace")

    def test_stale_heartbeat_term_then_kill_and_actual_new_identity(self):
        self.setup_process(self.now - 121)
        old, new = self.identity(), self.identity(pid=54321, age=1)
        state = {"old_alive": True, "new_alive": False}

        def identity(pid):
            return old if pid == 12345 and state["old_alive"] else new if pid == 54321 and state["new_alive"] else None

        def kill(pid, requested):
            self.assertEqual(pid, 12345)
            if requested == signal.SIGKILL:
                state["old_alive"] = False

        def launch(*args):
            state["new_alive"] = True
            Path(self.store.pid_path).write_text("54321")
            return {"started": True, "relay_pid": 90000, "child_pid": 54321}

        with mock.patch.object(ws.os, "kill", side_effect=kill) as killed, mock.patch.object(ws, "launch_logged_service", side_effect=launch):
            result = self.run_supervision(identity)
        self.assertEqual(result, {"alive": True, "restarted": True})
        self.assertEqual([call.args[1] for call in killed.call_args_list], [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual([row["action"] for row in self.events], ["term", "kill", "observe", "launch", "observe"])
        self.assertEqual(self.events[-1]["old_pid"], 12345)
        self.assertEqual(self.events[-1]["new_pid"], 54321)
        self.assertEqual(self.events[-1]["reason"], "service_ready")
        self.assert_safe_events()

    def test_startup_timeout_term_exit_does_not_request_kill(self):
        self.setup_process(self.now - 1000)
        old = self.identity(age=181)
        active = [True]

        def kill(pid, requested):
            active[0] = False

        with mock.patch.object(ws.os, "kill", side_effect=kill) as killed, mock.patch.object(ws, "launch_logged_service", return_value={"started": False, "child_pid": None}):
            self.assertFalse(self.run_supervision(lambda pid: old if active[0] else None)["alive"])
        self.assertEqual(killed.call_count, 1)
        self.assertEqual(killed.call_args.args[1], signal.SIGTERM)
        self.assertEqual(self.events[0]["reason"], "startup_timeout")
        self.assertEqual(self.events[1]["result"], "stopped")
        self.assert_safe_events()

    def test_pid_identity_change_before_signal_defers_without_kill(self):
        self.setup_process(self.now - 121)
        original = self.identity()
        changed = self.identity(age=2)
        calls = iter((original, changed))
        with mock.patch.object(ws.os, "kill") as kill:
            result = self.run_supervision(lambda pid: next(calls))
        kill.assert_not_called()
        self.assertFalse(result["restarted"])
        self.assertEqual(self.events[-1]["reason"], "identity_changed")

    def test_identity_change_after_term_blocks_kill_and_relaunch(self):
        self.setup_process(self.now - 121)
        original = self.identity()
        changed = self.identity(age=2)
        calls = iter((original, original, changed))
        with mock.patch.object(ws.os, "kill") as killed:
            result = self.run_supervision(lambda pid: next(calls))
        self.assertEqual(killed.call_count, 1)
        self.assertFalse(result["restarted"])
        self.assertEqual(self.events[-1]["reason"], "identity_changed")

    def test_kill_request_without_exit_confirmation_is_unknown_not_restart(self):
        self.setup_process(self.now - 121)
        original = self.identity()
        with mock.patch.object(ws.os, "kill"):
            result = self.run_supervision(lambda pid: original)
        self.assertEqual(result, {"alive": True, "restarted": False})
        self.assertEqual(self.events[-1]["reason"], "stop_unconfirmed")
        self.assertEqual(self.events[-1]["result"], "unknown")

    def test_log_failures_do_not_change_healthy_or_signal_decisions(self):
        self.setup_process(self.now - 120)
        with mock.patch.object(ws, "maintain_runtime_logs", side_effect=OSError("SYNTHETIC_PRIVATE_ERROR")), mock.patch.object(ws, "append_supervision_event", side_effect=OSError("SYNTHETIC_PRIVATE_ERROR")), mock.patch.object(ws.os, "kill") as killed:
            result = self.run_supervision(lambda pid: self.identity())
        self.assertEqual(result, {"alive": True, "restarted": False})
        killed.assert_not_called()

    def test_nonfinite_heartbeat_not_treated_as_fresh(self):
        self.setup_process()
        original = self.identity(age=180)
        for value in ("nan", "inf", "-inf"):
            Path(self.store.flusher_hb_path).write_text(value)
            with mock.patch.object(ws.os, "kill") as killed:
                self.assertTrue(self.run_supervision(lambda pid: original)["alive"])
            killed.assert_not_called()
            self.assertEqual(self.events[-1]["reason"], "startup_grace")
            self.assertIsNone(self.events[-1]["heartbeat_at"])
        self.assert_safe_events()

    def test_launch_ack_does_not_prove_pidfile_service_identity(self):
        self.setup_process()
        with mock.patch.object(ws, "launch_logged_service", return_value={"started": True, "child_pid": 54321}):
            result = self.run_supervision(lambda pid: None)
        self.assertFalse(result["alive"])
        self.assertEqual(self.events[-1]["reason"], "spawn_unconfirmed")
        self.assertIsNone(self.events[-1]["new_pid"])

    def test_new_process_requires_ack_matching_pid_and_finite_recent_start(self):
        cases = ((None, self.now), (True, float("nan")), (True, float("inf")),
                 (True, self.now - 30), (True, self.now + 30))
        for acknowledged, started_at in cases:
            self.setup_process()
            calls = [0]

            def identity(pid):
                calls[0] += 1
                if calls[0] == 1:
                    return None
                return {"pid": 54321, "start": started_at, "tokens": ["SYNTHETIC_PRIVATE"]}

            def launch(*args):
                Path(self.store.pid_path).write_text("54321")
                return {"started": acknowledged, "child_pid": 54321}

            with self.subTest(acknowledged=acknowledged, started_at=started_at), mock.patch.object(ws, "launch_logged_service", side_effect=launch):
                self.assertFalse(self.run_supervision(identity)["alive"])
            self.assertEqual(self.events[-1]["reason"], "spawn_unconfirmed")

    def test_observation_schema_and_worker_lease_constants_remain_bounded(self):
        self.assertEqual((ws.WORKER_LEASE_SECONDS, ws.WORKER_MAX_SECONDS), (90, 570))
        ws._supervision_event(self.store, float("nan"), "heartbeat_fresh", "keep", "healthy",
                              old={"pid": 12345, "start": float("inf"), "tokens": ["SYNTHETIC_PRIVATE"]},
                              heartbeat=float("inf"))
        self.assertIsNone(self.events[-1]["at"])
        self.assertIsNone(self.events[-1]["old_start"])
        self.assertIsNone(self.events[-1]["heartbeat_at"])
        count = len(self.events)
        ws._supervision_event(self.store, self.now, "SYNTHETIC_PRIVATE", "keep", "healthy")
        self.assertEqual(len(self.events), count)
        self.assert_safe_events()


if __name__ == "__main__":
    unittest.main(verbosity=2)
