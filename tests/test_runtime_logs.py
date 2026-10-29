"""Synthetic-only log retention, open-FD, relay and disk-failure tests."""

import concurrent.futures
import errno
import json
import os
from pathlib import Path
import socket
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import runtime_logs as logs


class RuntimeLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wbridge-log-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "serve.log"
        for target in ("socket.create_connection", "socket.getaddrinfo", "socket.socket.connect"):
            patcher = mock.patch(target, side_effect=AssertionError("real network forbidden"))
            patcher.start()
            self.addCleanup(patcher.stop)

    def retained(self):
        return b"".join(path.read_bytes() for path in (
            Path(str(self.path) + ".2"), Path(str(self.path) + ".1"), self.path) if path.exists())

    def assert_bound(self, maximum, backups=2):
        sizes = []
        for index in range(backups + 1):
            path = self.path if not index else Path(str(self.path) + ".%d" % index)
            if path.exists():
                sizes.append(path.stat().st_size)
                self.assertLessEqual(path.stat().st_size, maximum)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertLessEqual(sum(sizes), maximum * (backups + 1))
        self.assertFalse(list(Path(self.temp.name).glob("*.tmp*")))

    def test_continuous_sink_retains_only_bounded_latest_bytes(self):
        sink = logs.BoundedLogSink(self.path, max_bytes=64, backups=2)
        payload = bytes(range(256)) * 9
        self.assertTrue(sink.write(payload))
        self.assert_bound(64)
        retained = self.retained()
        self.assertEqual(retained, payload[-len(retained):])
        self.assertEqual(len(retained), 192)
        self.assertEqual(Path(str(self.path) + ".lock").stat().st_size, 0)

    def test_atomic_events_do_not_split_json_across_rotations(self):
        sink = logs.BoundedLogSink(self.path, max_bytes=64, backups=2)
        for number in range(20):
            self.assertTrue(sink.write((json.dumps({"synthetic": number}) + "\n").encode(), atomic=True))
        self.assert_bound(64)
        for suffix in ("", ".1", ".2"):
            for line in Path(str(self.path) + suffix).read_text().splitlines():
                self.assertIsInstance(json.loads(line)["synthetic"], int)

    def test_legacy_open_append_fd_keeps_same_inode_after_maintenance(self):
        sink = logs.BoundedLogSink(self.path, max_bytes=32, backups=2)
        with open(self.path, "ab", buffering=0) as old_stdout:
            old_stdout.write(b"old" * 100)
            inode = os.fstat(old_stdout.fileno()).st_ino
            self.assertTrue(sink.maintain())
            self.assertEqual(self.path.stat().st_ino, inode)
            self.assertEqual(self.path.stat().st_size, 32)
            old_stdout.write(b"SYNTHETIC_NEW_FD_DATA")
            self.assertTrue(self.path.read_bytes().endswith(b"SYNTHETIC_NEW_FD_DATA"))
            # An old direct writer can grow again; periodic trim is honest about
            # that limitation and never strands its open FD on a renamed file.
            self.assertGreater(self.path.stat().st_size, 32)
            self.assertTrue(sink.maintain())
            self.assertEqual(self.path.stat().st_ino, inode)
        self.assert_bound(32)

    def test_existing_reserved_slots_are_trimmed_without_temporary_copy(self):
        for suffix in ("", ".1", ".2"):
            Path(str(self.path) + suffix).write_bytes(b"x" * 1000)
        sink = logs.BoundedLogSink(self.path, max_bytes=32, backups=2)
        self.assertTrue(sink.maintain())
        self.assert_bound(32)
        self.assertEqual(len(self.retained()), 96)

    def test_concurrent_writers_are_bounded_and_contention_is_best_effort(self):
        barrier = threading.Barrier(8)

        def write(index):
            barrier.wait(timeout=3)
            sink = logs.BoundedLogSink(self.path, max_bytes=128, backups=2)
            return [sink.write(("SYNTHETIC:%d\n" % index).encode()) for _ in range(30)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(write, range(8)))
        self.assertTrue(any(result for group in results for result in group))
        self.assertTrue(all(type(result) is bool for group in results for result in group))
        self.assert_bound(128)

    def test_log_and_lock_symlinks_or_hardlinks_do_not_write_other_file(self):
        original = Path(self.temp.name) / "synthetic-original"
        original.write_bytes(b"SYNTHETIC: untouched")
        for kind in ("file-symlink", "lock-symlink", "hardlink"):
            with self.subTest(kind=kind):
                for path in (self.path, Path(str(self.path) + ".lock")):
                    if path.exists() or path.is_symlink():
                        path.unlink()
                if kind == "file-symlink":
                    self.path.symlink_to(original)
                elif kind == "lock-symlink":
                    Path(str(self.path) + ".lock").symlink_to(original)
                else:
                    os.link(original, self.path)
                self.assertFalse(logs.BoundedLogSink(self.path).write(b"synthetic attempt"))
                self.assertEqual(original.read_bytes(), b"SYNTHETIC: untouched")

    def test_disk_and_rotation_failures_return_false_without_growth_or_temps(self):
        sink = logs.BoundedLogSink(self.path, max_bytes=64, backups=2)
        self.assertTrue(sink.write(b"x" * 64))
        with mock.patch.object(logs.os, "replace", side_effect=OSError(errno.ENOSPC, "SYNTHETIC disk full")):
            self.assertFalse(sink.write(b"new"))
        self.assert_bound(64)
        with mock.patch.object(logs.os, "write", side_effect=OSError(errno.ENOSPC, "SYNTHETIC disk full")):
            self.assertFalse(sink.write(b"new"))
        self.assert_bound(64)

    def test_relay_real_pipe_bounds_healthy_stdout_and_stderr(self):
        script = """import os,time,stat
os.write(1,('PIPE:'+str(stat.S_ISFIFO(os.fstat(1).st_mode))+'\\n').encode())
for i in range(100):
 os.write(1,b'SYNTHETIC_STDOUT:' + b'x'*800 + b'\\n')
 os.write(2,b'SYNTHETIC_STDERR:' + b'y'*800 + b'\\n')
 time.sleep(0.003)
os.write(1,b'SYNTHETIC_STDOUT_END\\n')
os.write(2,b'SYNTHETIC_STDERR_END\\n')
"""
        outcome = []
        worker = threading.Thread(target=lambda: outcome.append(logs.run_relay(
            [sys.executable, "-I", "-c", script], str(self.path), max_bytes=1024, backups=2)), daemon=True)
        worker.start()
        observations = 0
        deadline = time.monotonic() + 5
        while worker.is_alive() and time.monotonic() < deadline:
            # Files can be atomically renamed between exists/stat; only stable
            # observations count. No file is expected ever to exceed the cap.
            try:
                self.assert_bound(1024)
                observations += 1
            except FileNotFoundError:
                pass
            time.sleep(0.01)
        worker.join(timeout=1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [0])
        self.assertGreater(observations, 2)
        self.assertIn(b"SYNTHETIC_STDOUT_END", self.retained())
        self.assertIn(b"SYNTHETIC_STDERR_END", self.retained())
        self.assert_bound(1024)

    def test_disk_unavailable_does_not_stop_pipe_draining(self):
        marker = Path(self.temp.name) / "synthetic-finished"
        script = "import os,pathlib; os.write(1,b'x'*1048576); os.write(2,b'y'*1048576); pathlib.Path(%r).write_text('done')" % str(marker)
        started = time.monotonic()
        with mock.patch.object(logs.BoundedLogSink, "write", return_value=False) as write:
            self.assertEqual(logs.run_relay([sys.executable, "-I", "-c", script], str(self.path)), 0)
        self.assertTrue(marker.exists())
        self.assertLess(time.monotonic() - started, 5)
        self.assertTrue(all(len(call.args[0]) <= logs.READ_CHUNK for call in write.call_args_list))

    def test_relay_exits_when_child_exited_even_if_descendant_inherited_pipe(self):
        script = "import subprocess,sys; subprocess.Popen([sys.executable,'-I','-c','import time; time.sleep(2)'])"
        started = time.monotonic()
        self.assertEqual(logs.run_relay([sys.executable, "-I", "-c", script], str(self.path)), 0)
        self.assertLess(time.monotonic() - started, 1.8)

    def test_spawn_handshake_identifies_actual_child_with_pipe_fd(self):
        base = Path(self.temp.name) / "synthetic-base"
        (base / "state").mkdir(parents=True)
        script = Path(self.temp.name) / "synthetic-executable"
        script.write_text("#!%s\nimport os,stat\nos.write(1,('SYNTHETIC_PIPE:'+str(stat.S_ISFIFO(os.fstat(1).st_mode))).encode())\n" % sys.executable)
        script.chmod(0o700)
        launched = logs.launch_logged_service(str(script), str(base), dict(os.environ))
        self.assertTrue(launched["started"])
        self.assertNotEqual(launched["relay_pid"], launched["child_pid"])
        target = base / "state" / "serve.log"
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not target.exists():
            time.sleep(0.02)
        self.assertIn(b"SYNTHETIC_PIPE:True", target.read_bytes())

    def test_spawn_failure_is_fixed_ack_and_exit_record_uses_actual_wait(self):
        self.assertEqual(logs.run_relay(["/missing/synthetic-executable"], str(self.path)), 1)
        self.assertEqual(logs.run_relay([sys.executable, "-I", "-c", "raise SystemExit(7)"], str(self.path)), 0)
        events = [json.loads(line) for line in (Path(self.temp.name) / "serve-supervision.jsonl").read_text().splitlines()]
        self.assertEqual(events[-1]["reason"], "child_exit")
        self.assertEqual(events[-1]["exit_code"], 7)
        self.assertEqual(events[-1]["exit_signal"], "unknown")


if __name__ == "__main__":
    unittest.main(verbosity=2)
