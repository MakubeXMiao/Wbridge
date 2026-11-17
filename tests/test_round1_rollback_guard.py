"""Verify the rollback stop through the supported bridge entry points."""

from contextlib import redirect_stdout
import fcntl
import io
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from wechat_bridge import cli, delivery_failures, ilink, rollback_guard, serve
from wechat_bridge.media_retention import PIN_NAME
from wechat_bridge.store import Store, atomic_write_json
from wechat_bridge.worker_state import WorkerState


class RollbackGuardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.state = self.base / "state"
        self.pin = self.state / PIN_NAME

    def run_guard(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = rollback_guard.main(["--base", str(self.base)])
        return code, json.loads(output.getvalue())

    def snapshot(self):
        return {str(path.relative_to(self.base)): path.read_bytes()
                for path in self.base.rglob("*") if path.is_file()}

    def test_missing_or_unknown_lock_leaves_state_unchanged(self):
        self.assertEqual(self.run_guard(), (1, {"status": "blocked", "reason": "state_lock_unavailable"}))
        self.assertEqual(list(self.base.iterdir()), [])
        self.state.mkdir(mode=0o700)
        original = self.state / "original"
        original.write_bytes(b"synthetic private contents")
        self.pin.symlink_to(original)
        self.assertEqual(self.run_guard(), (1, {"status": "blocked", "reason": "state_lock_unavailable"}))
        self.assertTrue(self.pin.is_symlink())
        self.assertEqual(original.read_bytes(), b"synthetic private contents")

    def test_active_store_or_serve_blocks_guard_without_leaking_a_pin(self):
        store = Store(self.base)
        self.addCleanup(store.close)
        blocked = (1, {"status": "blocked", "reason": "state_lock_busy"})
        self.assertEqual(self.run_guard(), blocked)
        store.close()
        with open(store.lock_path, "a") as serve_lock:
            fcntl.flock(serve_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.run_guard(), blocked)
        state_fd = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(state_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.run_guard(), blocked)
        finally:
            os.close(state_fd)
        with rollback_guard.hold(self.base):
            self.assertEqual(self.run_guard(), blocked)

    def test_foreground_guard_blocks_serve_send_and_worker_without_recovery(self):
        Store(self.base).close()
        records = {
            "outbox/sending/interrupted.json": {"delivery_state": "final_inflight_v1", "chunks_done": 1},
            "outbox/sending/accepted.json": {"delivery_state": "api_accepted_v1", "chunks_done": 2},
            "outbox/sending/duplicate.json": {"delivery_state": "final_inflight_v1"},
            "outbox/pending/duplicate.json": {"delivery_state": "recovered"},
            "outbox/pending/unproven.json": {"client_key": "old"},
            "outbox/failed/closed.json": {"delivery_outcome": "unknown"},
            "state/received/synthetic.json": {"status": "accepted"},
            "state/session.json": {"bot_token": "synthetic", "ilink_user_id": "synthetic-owner"},
        }
        for name, record in records.items():
            atomic_write_json(self.base / name, record)
        planned, _ = delivery_failures.plan_first_notices(self.base, {}, 1)
        self.assertEqual([name for name, _ in planned], ["closed.json"])
        delivery_failures.record_notice_result(self.base, [key for _, key in planned], True)
        status = delivery_failures.status(self.base, "closed.json")
        closed = delivery_failures.reconcile(self.base, "closed.json", status["sha256"], "received", 2)
        before = self.snapshot()
        run_from_source = (
            "import runpy, sys; sys.path.insert(0, sys.argv.pop(1)); "
            "runpy.run_module('wechat_bridge.rollback_guard', run_name='__main__')"
        )
        command = [sys.executable, "-I", "-B", "-c", run_from_source, str(ROOT), "--base", str(self.base)]
        with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as guard:
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(guard.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=5), "rollback guard did not report readiness")
                self.assertEqual(json.loads(guard.stdout.readline()), {"status": "held", "mode": "read_only_stop"})
                with patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": str(self.base)}), \
                        patch.object(ilink, "http_json") as send_http, \
                        patch.object(serve, "http_json") as poll_http, \
                        patch.object(serve, "_recover_sending") as recover:
                    for args in (["serve"], ["send", "--text", "synthetic"]):
                        with self.subTest(args=args), self.assertRaises(BlockingIOError):
                            cli.main(args)
                    with self.assertRaises(BlockingIOError):
                        WorkerState(self.base, self.base / "worker")
                    send_http.assert_not_called()
                    poll_http.assert_not_called()
                    recover.assert_not_called()
                self.assertFalse((self.base / "worker").exists())
                self.assertEqual(delivery_failures.status(self.base, "closed.json"), closed)
                state_fd = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(state_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(state_fd)
                self.assertEqual(self.snapshot(), before)
            finally:
                guard.terminate()
                guard.wait(timeout=5)
        self.assertEqual(self.snapshot(), before)
        with rollback_guard.hold(self.base):
            pass


if __name__ == "__main__":
    unittest.main()
