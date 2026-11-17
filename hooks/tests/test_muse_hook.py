"""Hook integration with synthetic state and a fake runtime, never a live bot."""
import hashlib
import fcntl
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge.worker_state import WorkerState
from wechat_bridge.store import atomic_write_json

RUNTIME_NAMES = ("WECHAT_REFERENCE_V2", "WECHAT_CDN_TIER_TRIAL",
                 "WECHAT_RECENT_CONTEXT", "WECHAT_CLOUD_CONTEXT")


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
        self.env["WECHAT_CLOUD_CONTEXT"] = "0"
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
        self.assertNotIn("context_argv", adapter)
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

    def install_command_recorder(self):
        recorder = self.home / "recording-python"
        commands = self.home / "commands.jsonl"
        self.runtime_reads = self.home / "runtime-reads.jsonl"
        recorder.write_text(f'''#!{sys.executable}
import json, os, sys
if "--runtime-environment" in sys.argv[1:]:
    record = {{"locked": os.environ.get("WECHAT_HOOK_LOCKED") == "1"}}
    if os.environ.get("FAKE_CAPTURE_ADAPTER_ROUTE") == "1":
        record["adapter"] = sys.argv[1]
    with open({str(self.runtime_reads)!r}, "a") as stream:
        stream.write(json.dumps(record) + "\\n")
if sys.argv[1:3] == ["-m", "wechat_bridge.worker_state"]:
    for command in ("supervise", "recent-context-maintain", "plan"):
        if command not in sys.argv[3:]:
            continue
        record = {{"command": command,
            "locked": os.environ.get("WECHAT_HOOK_LOCKED") == "1",
            "context_argv": sys.argv[sys.argv.index("--cloud-source-id"):] if "--cloud-source-id" in sys.argv else []}}
        if os.environ.get("FAKE_CAPTURE_RUNTIME_ENV") == "1":
            record["environment"] = {{name: os.environ.get(name) for name in {RUNTIME_NAMES!r}}}
        with open({str(commands)!r}, "a") as stream:
            stream.write(json.dumps(record) + "\\n")
        if command == "supervise":
            print('{{"alive":true}}')
        elif command == "plan":
            if os.environ.get("FAKE_PLAN_WAKE") == "1":
                print('{{"decision":"wake","reason":"new_wechat_message","payload":{{}}}}')
            else:
                print('{{"decision":"silent","reason":"synthetic","payload":{{}}}}')
        elif os.environ.get("FAKE_CONTEXT_MAINTENANCE_FAIL") == "1":
            print("PRIVATE-CONTEXT-TRACEBACK", file=sys.stderr)
            sys.exit(2)
        sys.exit(0)
os.execv({sys.executable!r}, [{sys.executable!r}] + sys.argv[1:])
''')
        recorder.chmod(0o700)
        self.env["WECHAT_BRIDGE_PYTHON"] = str(recorder)
        return commands

    def recorded_commands(self, path):
        return [json.loads(line) for line in path.read_text().splitlines()]

    def write_runtime_config(self, **changes):
        environment = dict(zip(RUNTIME_NAMES, ("1", "1", "1", "0")))
        environment.update(changes)
        directory = self.state / "muse-adapter"
        directory.mkdir(mode=0o700, exist_ok=True)
        directory.chmod(0o700)
        config = directory / "runtime-switches.json"
        atomic_write_json(str(config), {"schema_version": 1, "environment": environment})
        config.chmod(0o600)
        return config, environment

    def test_runtime_configuration_reaches_supervise_and_both_maintenance_gates(self):
        commands = self.install_command_recorder()
        _, environment = self.write_runtime_config()
        for gate in ("valid_session", "no_session", "needs_relogin"):
            with self.subTest(gate=gate):
                if commands.exists():
                    commands.unlink()
                session = Path(self.worker.store.session_path)
                relogin = self.base / "state/NEEDS_RELOGIN"
                session.write_text("{}")
                if relogin.exists():
                    relogin.unlink()
                if gate == "no_session":
                    session.unlink()
                elif gate == "needs_relogin":
                    relogin.write_text("synthetic")
                result = self.run_hook(FAKE_CAPTURE_RUNTIME_ENV="1")
                self.assertEqual(result.returncode, 0, result.stderr)
                rows = self.recorded_commands(commands)
                expected = "supervise" if gate == "valid_session" else "recent-context-maintain"
                self.assertEqual([row["command"] for row in rows], [expected, "plan"])
                self.assertTrue(all(row["locked"] for row in rows))
                self.assertTrue(all(row["environment"] == environment for row in rows))
                self.assertTrue(all(row["context_argv"] == [] for row in rows))
                self.assertEqual(json.loads(result.stdout)["decision"], "silent")

    def test_runtime_configuration_zero_overrides_inherited_enable(self):
        commands = self.install_command_recorder()
        _, environment = self.write_runtime_config(**dict.fromkeys(RUNTIME_NAMES, "0"))
        result = self.run_hook(FAKE_CAPTURE_RUNTIME_ENV="1", **dict.fromkeys(RUNTIME_NAMES, "1"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(row["environment"] == environment for row in self.recorded_commands(commands)))
        self.assertTrue(all(row["context_argv"] == [] for row in self.recorded_commands(commands)))

    def test_runtime_configuration_cloud_enable_drives_fixed_binding(self):
        commands = self.install_command_recorder()
        _, environment = self.write_runtime_config(WECHAT_CLOUD_CONTEXT="1")
        result = self.run_hook(FAKE_CAPTURE_RUNTIME_ENV="1", FAKE_PLAN_WAKE="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.recorded_commands(commands)
        binding = ["--cloud-source-id", "wbridge-synthetic", "--cloud-case-id", "continuity-smoke",
                   "--cloud-publisher-id", "wbridge-synthetic-publisher"]
        self.assertEqual(rows[0]["environment"], environment)
        self.assertEqual(rows[0]["context_argv"], binding)
        self.assertEqual(json.loads(result.stdout)["payload"]["muse_adapter"]["context_argv"], binding)

    def test_absent_runtime_configuration_preserves_inherited_values(self):
        commands = self.install_command_recorder()
        environment = dict(zip(RUNTIME_NAMES, ("1", "1", "0", "0")))
        result = self.run_hook(FAKE_CAPTURE_RUNTIME_ENV="1", **environment)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(row["environment"] == environment for row in self.recorded_commands(commands)))
        self.assertFalse((self.state / "muse-adapter/runtime-switches.json").exists())

    def test_runtime_adapter_override_is_honored_when_configuration_is_absent(self):
        commands = self.install_command_recorder()
        alternate = self.home / "alternate-muse-worker.py"
        alternate.write_bytes((ROOT / "hooks/muse-worker.py").read_bytes())
        runtime = self.home / "runtime-adapter-override.sh"
        runtime.write_text("source " + shlex.quote(str(ROOT / "tests/fixtures/hook_runtime.sh")) + "\n"
                           "export WECHAT_MUSE_ADAPTER=" + shlex.quote(str(alternate)) + "\n")
        result = self.run_hook(HATCH_HOOK_RUNTIME=str(runtime), FAKE_PLAN_WAKE="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        adapter = json.loads(result.stdout)["payload"]["muse_adapter"]
        self.assertEqual(Path(adapter["path"]), alternate.resolve())
        self.assertEqual(adapter["sha256"], hashlib.sha256(alternate.read_bytes()).hexdigest())
        self.assertEqual([row["command"] for row in self.recorded_commands(commands)], ["supervise", "plan"])
        self.assertFalse(self.runtime_reads.exists())

    def test_private_configuration_overrides_runtime_exports_using_runtime_adapter(self):
        commands = self.install_command_recorder()
        _, environment = self.write_runtime_config(**dict.fromkeys(RUNTIME_NAMES, "0"))
        alternate = self.home / "alternate-muse-worker.py"
        alternate.write_bytes((ROOT / "hooks/muse-worker.py").read_bytes())
        runtime = self.home / "runtime-switch-override.sh"
        runtime.write_text("source " + shlex.quote(str(ROOT / "tests/fixtures/hook_runtime.sh")) + "\n"
                           "export WECHAT_MUSE_ADAPTER=" + shlex.quote(str(alternate)) + "\n"
                           + "\n".join("export " + name + "=1" for name in RUNTIME_NAMES) + "\n")
        result = self.run_hook(HATCH_HOOK_RUNTIME=str(runtime), FAKE_PLAN_WAKE="1",
                               FAKE_CAPTURE_RUNTIME_ENV="1", FAKE_CAPTURE_ADAPTER_ROUTE="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.recorded_commands(commands)
        self.assertTrue(all(row["environment"] == environment for row in rows))
        self.assertTrue(all(row["context_argv"] == [] for row in rows))
        self.assertEqual(self.recorded_commands(self.runtime_reads), [
            {"locked": True, "adapter": str(alternate)}])
        payload = json.loads(result.stdout)["payload"]["muse_adapter"]
        self.assertEqual(Path(payload["path"]), alternate.resolve())
        self.assertNotIn("context_argv", payload)

    def test_invalid_runtime_configuration_stops_before_lifecycle(self):
        commands = self.install_command_recorder()
        config, _ = self.write_runtime_config()
        config.write_text('{"schema_version":1,"environment":{"PRIVATE":"PRIVATE-VALUE"}}')
        result = self.run_hook()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(json.loads(result.stderr), {
            "stage": "muse_runtime_environment", "status": "invalid_configuration"})
        self.assertFalse(commands.exists())
        self.assertEqual((self.state / "wechat_inbox_watch.lock").stat().st_size, 0)
        self.assertEqual(self.recorded_commands(self.runtime_reads), [{"locked": True}])
        self.assertNotIn("PRIVATE-VALUE", result.stderr)

    def test_runtime_configuration_helper_output_is_validated_before_export(self):
        commands = self.install_command_recorder()
        self.write_runtime_config()
        fake = self.home / "invalid-environment-adapter.py"
        fake.write_text('print(\'{"schema_version":1,"configured":true,"environment":{"PATH":"PRIVATE-VALUE"}}\')\n')
        result = self.run_hook(WECHAT_MUSE_ADAPTER=str(fake))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(json.loads(result.stderr), {
            "stage": "muse_runtime_environment", "status": "invalid_configuration"})
        self.assertFalse(commands.exists())
        self.assertEqual((self.state / "wechat_inbox_watch.lock").stat().st_size, 0)
        self.assertEqual(self.recorded_commands(self.runtime_reads), [{"locked": True}])
        self.assertNotIn("PRIVATE-VALUE", result.stderr)

    def test_runtime_configuration_is_read_once_under_the_hook_lock(self):
        commands = self.install_command_recorder()
        self.write_runtime_config()
        result = self.run_hook()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.recorded_commands(self.runtime_reads), [{"locked": True}])
        self.assertEqual([row["command"] for row in self.recorded_commands(commands)], ["supervise", "plan"])

    def test_busy_hook_lock_skips_runtime_configuration_and_lifecycle(self):
        commands = self.install_command_recorder()
        config, _ = self.write_runtime_config()
        config.write_text("SYNTHETIC malformed private configuration")
        with (self.state / "wechat_inbox_watch.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.run_hook()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent")
        self.assertFalse(commands.exists())
        self.assertFalse(self.runtime_reads.exists())

    def test_dry_run_does_not_read_malformed_runtime_configuration(self):
        commands = self.install_command_recorder()
        config, _ = self.write_runtime_config()
        config.write_text("SYNTHETIC malformed private configuration")
        before = {str(path): (path.stat().st_mtime_ns, path.read_bytes() if path.is_file() else None)
                  for path in self.home.rglob("*")}
        result = self.run_hook(HATCH_HOOK_DRY_RUN="1", WECHAT_MUSE_ADAPTER="/absent")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent")
        self.assertFalse(commands.exists())
        after = {str(path): (path.stat().st_mtime_ns, path.read_bytes() if path.is_file() else None)
                 for path in self.home.rglob("*")}
        self.assertEqual(before, after)

    def test_login_gate_maintenance_is_once_and_mutually_exclusive(self):
        commands = self.install_command_recorder()
        for gate in ("valid_session", "no_session", "needs_relogin"):
            with self.subTest(gate=gate):
                if commands.exists():
                    commands.unlink()
                session = Path(self.worker.store.session_path)
                relogin = self.base / "state/NEEDS_RELOGIN"
                session.write_text("{}")
                if relogin.exists():
                    relogin.unlink()
                if gate == "no_session":
                    session.unlink()
                elif gate == "needs_relogin":
                    relogin.write_text("synthetic")
                result = self.run_hook(WECHAT_RECENT_CONTEXT="0", WECHAT_CLOUD_CONTEXT="0")
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = "supervise" if gate == "valid_session" else "recent-context-maintain"
                self.assertEqual(self.recorded_commands(commands), [
                    {"command": expected, "locked": True, "context_argv": []},
                    {"command": "plan", "locked": True, "context_argv": []}])
                self.assertEqual(json.loads(result.stdout)["decision"], "silent")
                self.assertNotIn("wake_called", result.stderr)

    def test_old_core_maintenance_failure_is_fixed_and_does_not_block_plan(self):
        commands = self.install_command_recorder()
        Path(self.worker.store.session_path).unlink()
        result = self.run_hook(FAKE_CONTEXT_MAINTENANCE_FAIL="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.recorded_commands(commands), [
            {"command": "recent-context-maintain", "locked": True, "context_argv": []},
            {"command": "plan", "locked": True, "context_argv": []}])
        self.assertEqual(json.loads(result.stderr), {
            "stage": "recent_context_maintenance", "status": "unavailable"})
        self.assertNotIn("PRIVATE-CONTEXT-TRACEBACK", result.stderr)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent")

    def test_dry_run_skips_gate_maintenance_and_locking(self):
        commands = self.install_command_recorder()
        Path(self.worker.store.session_path).unlink()
        before = {str(p): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else None)
                  for p in self.home.rglob("*")}
        result = self.run_hook(HATCH_HOOK_DRY_RUN="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(commands.exists())
        after = {str(p): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else None)
                 for p in self.home.rglob("*")}
        self.assertEqual(before, after)

    def test_enabled_cloud_binding_matches_program_and_worker_argv(self):
        commands = self.install_command_recorder()
        binding = ["--cloud-source-id", "wbridge-synthetic", "--cloud-case-id", "continuity-smoke",
                   "--cloud-publisher-id", "wbridge-synthetic-publisher"]
        for gate in ("valid_session", "no_session", "needs_relogin"):
            with self.subTest(gate=gate):
                if commands.exists():
                    commands.unlink()
                session = Path(self.worker.store.session_path)
                relogin = self.base / "state/NEEDS_RELOGIN"
                session.write_text("{}")
                if relogin.exists():
                    relogin.unlink()
                if gate == "no_session":
                    session.unlink()
                elif gate == "needs_relogin":
                    relogin.write_text("synthetic")
                result = self.run_hook(WECHAT_CLOUD_CONTEXT="1", FAKE_PLAN_WAKE="1")
                self.assertEqual(result.returncode, 0, result.stderr)
                rows = self.recorded_commands(commands)
                expected = "supervise" if gate == "valid_session" else "recent-context-maintain"
                self.assertEqual(rows, [
                    {"command": expected, "locked": True, "context_argv": binding},
                    {"command": "plan", "locked": True, "context_argv": []}])
                adapter = json.loads(result.stdout)["payload"]["muse_adapter"]
                self.assertEqual(adapter["context_argv"], binding)


if __name__ == "__main__":
    unittest.main()
