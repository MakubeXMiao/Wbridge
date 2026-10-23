"""Hook integration with synthetic state and a fake runtime, never a live bot."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge.worker_state import WorkerState
from wechat_bridge.store import atomic_write_json


class MuseHookTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-muse-hook-")
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.base, self.state = self.home / "bridge", self.home / "state"
        self.env = dict(os.environ, HOME=str(self.home), PYTHONPATH=str(ROOT),
                        PYTHONDONTWRITEBYTECODE="1", WECHAT_BRIDGE_PYTHON=sys.executable,
                        WECHAT_BRIDGE_DIR=str(self.base), WECHAT_HOOK_STATE_DIR=str(self.state),
                        HATCH_HOOK_RUNTIME=str(ROOT / "tests/fixtures/hook_runtime.sh"))
        # A synthetic serve-shaped sleeper satisfies supervision without
        # running receive/send code. Production state is never read or launched.
        self.worker = WorkerState(str(self.base), str(self.state))
        Path(self.worker.store.session_path).write_text("{}")
        fake = self.home / "bin/wechat-bridge"
        fake.parent.mkdir()
        fake.write_text("import time\ntime.sleep(30)\n")
        child = subprocess.Popen([sys.executable, str(fake), "serve"],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        def stop():
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)
        self.addCleanup(stop)
        Path(self.worker.store.pid_path).write_text(str(child.pid))
        Path(self.worker.store.flusher_hb_path).write_text(str(time.time()))
        atomic_write_json(str(self.base / "inbox/synthetic.json"),
                          dict(message_id="synthetic", from_user_id="synthetic-owner",
                               context_token="SYNTHETIC-SECRET", text="synthetic body"))

    def run_hook(self, **extra):
        return subprocess.run(["bash", str(ROOT / "hooks/wechat-inbox-watch.sh")],
                              env=dict(self.env, **extra), capture_output=True,
                              text=True, timeout=15)

    def test_fixed_adapter_and_wake_observations(self):
        result = self.run_hook()
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)["payload"]
        adapter = payload["muse_adapter"]
        expected = ROOT / "hooks/muse-worker.py"
        self.assertEqual(Path(adapter["path"]), expected.resolve())
        self.assertEqual(adapter["sha256"], hashlib.sha256(expected.read_bytes()).hexdigest())
        self.assertEqual(adapter["protocol"], 1)
        phases = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
        called, returned = [p for p in phases if p["stage"].startswith("wake_")]
        self.assertEqual(called["stage"], "wake_called")
        self.assertEqual(returned["stage"], "wake_returned")
        self.assertTrue(returned["runtime_accepted"])
        self.assertFalse(returned["worker_started_confirmed"])
        self.assertGreaterEqual(returned["elapsed_seconds"], 0)
        for value in ("SYNTHETIC-SECRET", "synthetic body", payload["lease"]):
            self.assertNotIn(value, result.stderr)
        # Hook has not consumed the event as if a platform worker had started.
        self.assertTrue(self.worker.begin(payload["lease"])["owned"])

    def test_runtime_failure_is_unknown_dispatch_not_early_release(self):
        result = self.run_hook(FAKE_WAKE_FAIL="1")
        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)["payload"]
        records = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
        returned = next(row for row in records if row["stage"] == "wake_returned")
        self.assertFalse(returned["runtime_accepted"])
        self.assertEqual(returned["exit_code"], 1)
        self.assertFalse(returned["worker_started_confirmed"])
        self.assertEqual(self.worker._claims()["synthetic"]["kind"], "event")
        self.assertEqual(self.worker._claims()["synthetic"]["lease"], payload["lease"])

    def test_missing_adapter_refuses_wake_and_does_not_fallback(self):
        result = self.run_hook(WECHAT_MUSE_ADAPTER=str(self.home / "absent.py"))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn('"stage":"muse_adapter_missing"', result.stderr)
        self.assertEqual(self.worker._claims()["synthetic"]["kind"], "event")

    def test_dry_run_with_missing_adapter_has_no_writes(self):
        before = {str(p): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else None)
                  for p in self.home.rglob("*")}
        result = self.run_hook(HATCH_HOOK_DRY_RUN="1", WECHAT_MUSE_ADAPTER="/absent")
        self.assertEqual(result.returncode, 0, result.stderr)
        after = {str(p): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else None)
                 for p in self.home.rglob("*")}
        self.assertEqual(before, after)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent")


if __name__ == "__main__":
    unittest.main()
