"""Exercise the real process boundary with a private, synthetic transport."""

import fcntl
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import cdn_process
from wechat_bridge.progress import OperationAborted


TRANSPORT = """\
import os
from pathlib import Path
import sys
import time

class UploadError(RuntimeError):
    def __init__(self, status=None):
        self.http_status = status

class WriteIdleTimeout(TimeoutError):
    pass

def upload(url, body, report, *, write_idle_timeout=None, on_write_state=None):
    assert "private-synthetic" in url and "private-synthetic" not in " ".join(sys.argv)
    assert body == b"synthetic-cipher"
    for fd in os.environ.get("WBR_TEST_CDN_GUARDS", "").split(","):
        if fd:
            os.fstat(int(fd))
    if url.endswith("/reject"):
        raise UploadError(500)
    if url.endswith("/dns_error"):
        raise UploadError()
    if url.endswith("/abnormal"):
        os._exit(9)
    if url.endswith("/internal_error"):
        raise RuntimeError("PRIVATE INTERNAL ERROR")
    if url.endswith("/invalid"):
        report({"raw": url})
    if url.endswith("/duplicate_frame"):
        sys.stdout.buffer.write(b'{"kind":"error","status":500,"status":null}\\n')
        sys.stdout.buffer.flush()
        os._exit(0)
    if url.endswith("/stall"):
        Path(os.environ["WBR_TEST_CDN_MARKER"]).write_text(str(os.getpid()))
        time.sleep(30)
    if url.endswith("/idle_stall"):
        assert write_idle_timeout == 60
        # Accelerate the transport's local clock edges for the real watchdog.
        on_write_state(time.monotonic() + 0.6, time.time() + 10)
        time.sleep(0.25)
        on_write_state(time.monotonic() + 0.7, time.time() + 10)
        time.sleep(0.45)
        Path(os.environ["WBR_TEST_CDN_MARKER"]).write_text(str(os.getpid()))
        time.sleep(30)
    if url.endswith("/idle_wall_stall"):
        on_write_state(time.monotonic() + 10, time.time() + 0.55)
        Path(os.environ["WBR_TEST_CDN_MARKER"]).write_text(str(os.getpid()))
        time.sleep(30)
    if url.endswith("/idle_cleared"):
        on_write_state(time.monotonic() + 0.2, time.time() + 0.2)
        on_write_state(None, None)
        time.sleep(0.35)
    if url.endswith("/idle_socket_timeout"):
        raise WriteIdleTimeout("PRIVATE TLS ERROR")
    report({"phase": "write", "outcome": "success", "elapsed_ms": 1,
            "bytes_written": len(body), "bytes_total": len(body), "error": None})
    return 200, "private-synthetic-response"
"""


class ProcessTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.temp = Path(temp.name)
        package = self.temp / "wechat_bridge"
        shutil.copytree(ROOT / "wechat_bridge", package, ignore=shutil.ignore_patterns("__pycache__"))
        (package / "cdn_transport.py").write_text(TRANSPORT)
        self.marker = self.temp / "worker"
        self.guards = [self.temp / "serve.lock", self.temp / "bot.lock"]
        self.children = []
        spawn = cdn_process._start

        def record_child(*args):
            child = spawn(*args)
            self.children.append(child)
            return child

        patches = [patch.object(cdn_process, "__file__", str(package / "cdn_process.py")),
                   patch.object(cdn_process, "_start", side_effect=record_child),
                   patch.dict(os.environ, {"WBR_TEST_CDN_MARKER": str(self.marker)})]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    @staticmethod
    def wait_until(predicate, timeout=5):
        finish = time.monotonic() + timeout
        while time.monotonic() < finish:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def upload(self, route, **kwargs):
        options = {"check": lambda: None, "deadline": time.monotonic() + 5,
                   "guard_fds": (), "report": lambda row: None}
        options.update(kwargs)
        return cdn_process.upload("https://private-synthetic.invalid/" + route, b"synthetic-cipher", **options)

    def assert_reaped(self):
        child = self.children[-1]
        self.assertIsNotNone(child.returncode)
        with self.assertRaises(ChildProcessError):
            os.waitpid(child.pid, os.WNOHANG)

    def test_result_and_http_error_use_separate_safe_frames(self):
        events = []
        self.assertEqual(self.upload("accept", report=events.append), (200, "private-synthetic-response"))
        self.assertEqual([row["bytes_written"] for row in events], [16])
        self.assertNotIn("private", str(events))
        self.assert_reaped()
        with self.assertRaises(cdn_process.CDNUploadError) as rejected:
            self.upload("reject")
        self.assertEqual(rejected.exception.http_status, 500)
        self.assertEqual(str(rejected.exception), "cdn_upload_failed")
        self.assertTrue(rejected.exception.confirmed)
        self.assert_reaped()
        with self.assertRaises(cdn_process.CDNUploadError) as dns:
            self.upload("dns_error")
        self.assertIsNone(dns.exception.http_status)
        self.assertTrue(dns.exception.confirmed)
        self.assert_reaped()
        for route in ("invalid", "duplicate_frame", "abnormal", "internal_error"):
            with self.subTest(route=route), self.assertRaises(cdn_process.CDNUploadError) as unknown:
                self.upload(route, report=events.append)
            self.assertIsNone(unknown.exception.http_status)
            self.assertFalse(unknown.exception.confirmed)
            self.assert_reaped()
        self.assertEqual(len(events), 1)

    def test_cancel_reaps_worker_and_preserves_caller_lock(self):
        with self.guards[0].open("w") as first, self.guards[1].open("w") as second:
            guard_fds = (first.fileno(), second.fileno())
            for fd in guard_fds:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def cancel():
                if self.marker.exists():
                    raise OperationAborted("lease_lost")

            with patch.dict(os.environ, {"WBR_TEST_CDN_GUARDS": ",".join(map(str, guard_fds))}):
                with self.assertRaises(OperationAborted) as aborted:
                    self.upload("stall", check=cancel, guard_fds=guard_fds)
            self.assertEqual(aborted.exception.code, "lease_lost")
            self.assert_reaped()
            for path in self.guards:
                with path.open() as rival, self.assertRaises(BlockingIOError):
                    fcntl.flock(rival, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_child_enforces_both_deadlines_while_parent_callback_blocks(self):
        for clock in ("monotonic", "wall"):
            with self.subTest(clock=clock):
                if self.marker.exists():
                    self.marker.unlink()
                independently_stopped = []

                def check():
                    if self.marker.exists() and not independently_stopped:
                        independently_stopped.append(self.wait_until(
                            lambda: self.children[-1].poll() is not None, 3))

                options = ({"deadline": time.monotonic() + 0.8} if clock == "monotonic" else
                           {"deadline": time.monotonic() + 10, "wall_deadline": time.time() + 0.8})
                with self.assertRaises(OperationAborted) as aborted:
                    self.upload("stall", check=check, **options)
                self.assertEqual(aborted.exception.code, "expired")
                self.assertEqual(independently_stopped, [True])
                self.assert_reaped()

    def test_trial_attempt_deadline_reaps_child_independently_of_parent_callback(self):
        for clock in ("monotonic", "wall"):
            with self.subTest(clock=clock):
                if self.marker.exists():
                    self.marker.unlink()
                independently_stopped = []

                def check():
                    if self.marker.exists() and not independently_stopped:
                        independently_stopped.append(self.wait_until(
                            lambda: self.children[-1].poll() is not None, 3))

                options = ({"attempt_deadline": time.monotonic() + 0.8,
                            "attempt_wall_deadline": time.time() + 10}
                           if clock == "monotonic" else
                           {"attempt_deadline": time.monotonic() + 10,
                            "attempt_wall_deadline": time.time() + 0.8})
                with self.assertRaises(cdn_process.CDNUploadError) as expired:
                    self.upload("stall", check=check, **options)
                self.assertEqual(expired.exception.reason, "attempt_expired")
                self.assertFalse(expired.exception.confirmed)
                self.assertEqual(independently_stopped, [True])
                self.assertEqual(self.children[-1].returncode, cdn_process._ATTEMPT_REACHED)
                self.assert_reaped()

    def test_trial_worker_writes_renew_idle_until_confirmed_stall(self):
        for route in ("idle_stall", "idle_wall_stall"):
            with self.subTest(route=route):
                if self.marker.exists():
                    self.marker.unlink()
                independently_stopped = []

                def check():
                    if self.marker.exists() and not independently_stopped:
                        independently_stopped.append(self.wait_until(
                            lambda: self.children[-1].poll() is not None, 3))

                with self.assertRaises(cdn_process.CDNUploadError) as expired:
                    self.upload(route, check=check, attempt_deadline=time.monotonic() + 4,
                                write_idle_timeout=60)
                self.assertEqual(expired.exception.reason, "write_idle_timeout")
                self.assertFalse(expired.exception.confirmed)
                self.assertEqual(independently_stopped, [True])
                self.assertEqual(self.children[-1].returncode, cdn_process._WRITE_IDLE_REACHED)
                self.assert_reaped()

    def test_trial_idle_is_disarmed_after_write_and_socket_timeout_is_sanitized(self):
        def trial(route):
            return self.upload(route, attempt_deadline=time.monotonic() + 4, write_idle_timeout=60)

        self.assertEqual(trial("idle_cleared"), (200, "private-synthetic-response"))
        self.assert_reaped()
        with self.assertRaises(cdn_process.CDNUploadError) as expired:
            trial("idle_socket_timeout")
        self.assertEqual(str(expired.exception), "write_idle_timeout")
        self.assertTrue(expired.exception.confirmed)
        self.assertEqual(self.children[-1].returncode, 0)
        self.assert_reaped()

    def test_original_authority_expiry_precedes_trial_attempt_expiry(self):
        past = time.monotonic() - 1
        with self.assertRaises(OperationAborted) as expired:
            self.upload("stall", deadline=past, attempt_deadline=past)
        self.assertEqual(expired.exception.code, "expired")
        self.assertFalse(self.children)

    def test_parent_death_stops_worker_holding_both_guards(self):
        launcher = """\
import fcntl, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from wechat_bridge import cdn_process
guards = tuple(os.open(path, os.O_CREAT | os.O_RDWR, 0o600) for path in sys.argv[2:])
for fd in guards:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.environ["WBR_TEST_CDN_GUARDS"] = ",".join(map(str, guards))
worker = Path(os.environ["WBR_TEST_CDN_MARKER"])
handoff = worker.with_name("parent-dropped-guards")
def check():
    if worker.exists() and not handoff.exists():
        for fd in guards:
            os.close(fd)
        handoff.touch()
cdn_process.upload("https://private-synthetic.invalid/stall", b"synthetic-cipher",
                   check=check, deadline=None, guard_fds=guards, report=lambda row: None)
"""
        command = [sys.executable, "-I", "-B", "-c", launcher, str(self.temp), *map(str, self.guards)]
        with subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as parent:
            try:
                self.assertTrue(self.wait_until(self.marker.with_name("parent-dropped-guards").exists))
                for path in self.guards:
                    with path.open() as rival, self.assertRaises(BlockingIOError):
                        fcntl.flock(rival, fcntl.LOCK_EX | fcntl.LOCK_NB)
                parent.kill()
                parent.wait(timeout=3)

                def both_released():
                    for path in self.guards:
                        with path.open() as rival:
                            try:
                                fcntl.flock(rival, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            except BlockingIOError:
                                return False
                    return True

                self.assertTrue(self.wait_until(both_released))
            finally:
                if parent.poll() is None:
                    parent.kill()
                    parent.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
