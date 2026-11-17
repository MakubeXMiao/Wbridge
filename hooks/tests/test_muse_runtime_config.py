"""Private runtime switch configuration and independent synthetic CLI workers."""
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
SPEC = importlib.util.spec_from_file_location("muse_runtime_config_adapter", ROOT / "hooks/muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)

ENVIRONMENT = {
    "WECHAT_REFERENCE_V2": "1",
    "WECHAT_CDN_TIER_TRIAL": "1",
    "WECHAT_RECENT_CONTEXT": "1",
    "WECHAT_CLOUD_CONTEXT": "0",
}

CHILD_GUARD = '''import runpy, socket, sys
def deny(*args, **kwargs):
    raise RuntimeError("synthetic runtime config tests forbid network")
socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
'''


class RuntimeConfigFiles(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-runtime-config-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.state = self.root / "hook-state"
        self.state.mkdir(mode=0o700)
        self.directory = self.state / "muse-adapter"
        self.config = self.directory / "runtime-switches.json"

    def write_config(self, environment=None, raw=None):
        self.directory.mkdir(mode=0o700, exist_ok=True)
        if raw is None:
            value = {"schema_version": 1, "environment": dict(ENVIRONMENT if environment is None else environment)}
            raw = json.dumps(value).encode("utf-8")
        self.config.write_bytes(raw)
        self.config.chmod(0o600)

    def snapshot(self):
        result = {}
        for path in self.root.rglob("*"):
            info = path.lstat()
            content = path.read_bytes() if stat.S_ISREG(info.st_mode) else None
            result[str(path.relative_to(self.root))] = (info.st_mode, info.st_size, info.st_mtime_ns, content)
        return result

    def assert_invalid(self):
        with self.assertRaises(adapter.SessionError) as error:
            adapter._runtime_environment(str(self.state))
        self.assertEqual(error.exception.code, "invalid_configuration")

    def run_cli(self, request=None, arguments=(), environment=None):
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1")
        if environment is not None:
            env.update(environment)
        command = [sys.executable, "-B", "-c", CHILD_GUARD, str(ROOT / "hooks/muse-worker.py"),
                   "--base", str(self.root / "bridge"), "--state-dir", str(self.state), *arguments]
        return subprocess.run(command, cwd=ROOT, env=env,
            input="" if request is None else json.dumps(request), text=True, capture_output=True, timeout=15)

    def test_missing_config_does_not_create_private_directory(self):
        before = self.snapshot()
        self.assertIsNone(adapter._runtime_environment(str(self.state)))
        self.assertEqual(self.snapshot(), before)

        result = self.run_cli(arguments=("--runtime-environment",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "schema_version": 1, "configured": False, "environment": {}})
        self.assertEqual(self.snapshot(), before)

    def test_relative_state_directory_cannot_select_launch_configuration(self):
        with self.assertRaises(adapter.SessionError) as error:
            adapter._runtime_environment("relative-hook-state")
        self.assertEqual(error.exception.code, "invalid_configuration")

    def test_exact_private_configuration_is_detached_and_read_only(self):
        self.write_config()
        before = self.snapshot()
        environment = adapter._runtime_environment(str(self.state))
        self.assertEqual(environment, ENVIRONMENT)
        environment["WECHAT_RECENT_CONTEXT"] = "0"
        self.assertEqual(adapter._runtime_environment(str(self.state)), ENVIRONMENT)
        result = self.run_cli(arguments=("--runtime-environment",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "schema_version": 1, "configured": True, "environment": ENVIRONMENT})
        self.assertNotIn("runtime_configuration ", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_bad_schema_keys_values_and_duplicate_json_are_rejected(self):
        values = [None, [], {"schema_version": True, "environment": ENVIRONMENT},
                  {"schema_version": 2, "environment": ENVIRONMENT},
                  {"schema_version": 1, "environment": ENVIRONMENT, "extra": "private"}]
        for name in ENVIRONMENT:
            missing = dict(ENVIRONMENT)
            missing.pop(name)
            values.append({"schema_version": 1, "environment": missing})
        for value in (True, 1, None, "", "on", "1\n", "PRIVATE-DO-NOT-PRINT"):
            environment = dict(ENVIRONMENT, WECHAT_RECENT_CONTEXT=value)
            values.append({"schema_version": 1, "environment": environment})
        values.append({"schema_version": 1, "environment": dict(ENVIRONMENT, PYTHONPATH="private")})
        for value in values:
            with self.subTest(value=value):
                self.write_config(raw=json.dumps(value).encode("utf-8"))
                self.assert_invalid()
        self.write_config(raw=b'{"schema_version":1,"schema_version":1,"environment":{}}')
        self.assert_invalid()
        encoded = json.dumps({"schema_version": 1, "environment": ENVIRONMENT})
        duplicate = encoded.replace('"WECHAT_RECENT_CONTEXT": "1"',
            '"WECHAT_RECENT_CONTEXT": "1", "WECHAT_RECENT_CONTEXT": "0"')
        self.write_config(raw=duplicate.encode("utf-8"))
        self.assert_invalid()

    def test_invalid_utf8_oversized_and_non_json_content_are_rejected(self):
        for raw in (b"\xff", b"not-json", b"x" * 4097, b'{"schema_version":NaN}'):
            with self.subTest(size=len(raw)):
                self.write_config(raw=raw)
                self.assert_invalid()

    def test_file_and_parent_permissions_are_private(self):
        self.write_config()
        for mode in (0o644, 0o640, 0o660):
            with self.subTest(file_mode=mode):
                self.config.chmod(mode)
                self.assert_invalid()
        self.config.chmod(0o600)
        for mode in (0o750, 0o770):
            with self.subTest(directory_mode=mode):
                self.directory.chmod(mode)
                self.assert_invalid()
        self.directory.chmod(0o700)
        self.assertEqual(adapter._runtime_environment(str(self.state)), ENVIRONMENT)

        # Darwin can silently strip a requested setgid bit. Inject its exact
        # metadata so Linux inherited 02700/02600 remains a tested rejection.
        original_fstat = adapter.os.fstat
        class WithSetgid:
            def __init__(self, info):
                self.info = info
                self.st_mode = info.st_mode | stat.S_ISGID

            def __getattr__(self, name):
                return getattr(self.info, name)

        for predicate in (stat.S_ISREG, stat.S_ISDIR):
            with self.subTest(setgid_file_type=predicate.__name__):
                def inherited_setgid(fd):
                    info = original_fstat(fd)
                    return WithSetgid(info) if predicate(info.st_mode) else info

                with patch.object(adapter.os, "fstat", side_effect=inherited_setgid):
                    self.assert_invalid()

    def test_symlinks_hardlinks_and_special_files_are_rejected_without_following(self):
        self.write_config()
        real = self.root / "external.json"
        self.config.replace(real)
        self.config.symlink_to(real)
        self.assert_invalid()
        self.config.unlink()
        os.link(real, self.config)
        self.assert_invalid()
        self.config.unlink()
        os.mkfifo(self.config, mode=0o600)
        self.assert_invalid()
        self.config.unlink()
        self.config.mkdir(mode=0o700)
        self.assert_invalid()
        self.assertEqual(json.loads(real.read_text())["environment"], ENVIRONMENT)

    def test_private_directory_symlink_is_rejected(self):
        self.write_config()
        alternate = self.root / "alternate"
        self.directory.rename(alternate)
        self.directory.symlink_to(alternate, target_is_directory=True)
        self.assert_invalid()

    def test_config_owner_must_be_the_effective_user(self):
        self.write_config()
        real_fstat = adapter.os.fstat

        class DifferentOwner:
            def __init__(self, info):
                self.info = info
                self.st_uid = os.geteuid() + 1

            def __getattr__(self, name):
                return getattr(self.info, name)

        def wrong_owner(fd):
            info = real_fstat(fd)
            return DifferentOwner(info) if stat.S_ISREG(info.st_mode) else info

        with patch.object(adapter.os, "fstat", side_effect=wrong_owner):
            self.assert_invalid()

    def test_path_replacement_during_read_is_rejected(self):
        self.write_config()
        replacement = self.root / "replacement.json"
        replacement.write_text(json.dumps({"schema_version": 1,
            "environment": dict(ENVIRONMENT, WECHAT_RECENT_CONTEXT="0")}))
        replacement.chmod(0o600)
        original_read = adapter.os.read
        replaced = []

        def replace_after_read(fd, size):
            value = original_read(fd, size)
            if not replaced and size == 4097:
                os.replace(replacement, self.config)
                replaced.append(True)
            return value

        with patch.object(adapter.os, "read", side_effect=replace_after_read):
            self.assert_invalid()
        self.assertEqual(replaced, [True])
        self.assertEqual(adapter._runtime_environment(str(self.state))["WECHAT_RECENT_CONTEXT"], "0")

    def test_invalid_config_stops_before_business_state_or_private_values_escape(self):
        self.write_config(raw=json.dumps({"schema_version": 1,
            "environment": dict(ENVIRONMENT, WECHAT_RECENT_CONTEXT="PRIVATE-DO-NOT-PRINT")}).encode("utf-8"))
        before = self.snapshot()
        result = self.run_cli({"action": "begin", "lease": "synthetic-unowned"})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(json.loads(result.stdout)["error"], "invalid_configuration")
        self.assertNotIn("PRIVATE-DO-NOT-PRINT", result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_embedded_cli_restores_environment_after_request(self):
        self.write_config()
        before = {name: "0" for name in ENVIRONMENT}
        observed = []

        def business(*args, **kwargs):
            observed.append({name: os.environ.get(name) for name in ENVIRONMENT})
            return {"ok": True}

        with patch.dict(os.environ, before), patch.object(adapter, "handle_request", side_effect=business), \
                patch.object(sys, "stdin", io.StringIO('{"action":"begin","lease":"synthetic"}')), \
                patch.object(sys, "stdout", io.StringIO()):
            code = adapter.main(["--base", str(self.root / "bridge"), "--state-dir", str(self.state)])
            self.assertEqual(code, 0)
            self.assertEqual({name: os.environ.get(name) for name in ENVIRONMENT}, before)
        self.assertEqual(observed, [ENVIRONMENT])


class RuntimeConfigCliWorkers(unittest.TestCase):
    def setUp(self):
        import test_attachment_runtime as public_fixture
        self.fixture = public_fixture.RuntimeTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.state = self.fixture.home / "hook"
        self.directory = self.state / "muse-adapter"
        self.directory.mkdir(mode=0o700, exist_ok=True)
        self.config = self.directory / "runtime-switches.json"
        self.counter = 0

    def configure(self, recent):
        environment = dict(ENVIRONMENT, WECHAT_RECENT_CONTEXT=recent)
        self.config.write_text(json.dumps({"schema_version": 1, "environment": environment}))
        self.config.chmod(0o600)

    def cli(self, request, ambient_recent):
        environment = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1",
            WECHAT_RECENT_CONTEXT=ambient_recent, WECHAT_CLOUD_CONTEXT="0",
            SYNTHETIC_RUNTIME_SECRET="PRIVATE-NEVER-TRACE")
        with subprocess.Popen([sys.executable, "-B", "-c", CHILD_GUARD,
            str(ROOT / "hooks/muse-worker.py"), "--base", str(self.fixture.base),
            "--state-dir", str(self.state)], cwd=ROOT, env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
            try:
                stdout, stderr = child.communicate(json.dumps(request), timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate(timeout=5)
                raise
            self.assertEqual(child.returncode, 0, stdout + stderr)
            traces = [json.loads(line[len("runtime_configuration "):]) for line in stderr.splitlines()
                      if line.startswith("runtime_configuration ")]
            if self.config.exists():
                expected = json.loads(self.config.read_text())["environment"]
                self.assertEqual(len(traces), 1, stderr)
                trace = traces[0]
                self.assertEqual(set(trace), {
                    "stage", "pid", "effective_switches_sha256", "effective_switches"})
                self.assertEqual(trace["pid"], child.pid)
                self.assertNotEqual(trace["pid"], os.getpid())
                self.assertEqual(trace["stage"], "muse_runtime_configuration")
                self.assertEqual(trace["effective_switches"], expected)
                encoded = json.dumps(expected, ensure_ascii=False, sort_keys=True,
                    allow_nan=False).encode("utf-8")
                self.assertEqual(trace["effective_switches_sha256"], hashlib.sha256(encoded).hexdigest())
            else:
                self.assertEqual(traces, [])
            self.assertNotIn("PRIVATE-NEVER-TRACE", stdout + stderr)
            return json.loads(stdout)

    def begin(self, ambient_recent):
        self.counter += 1
        self.fixture.receive(self.fixture.message("runtime-mode-%d" % self.counter, text="SYNTHETIC new case"))
        self.fixture.now = time.time()
        wake = self.fixture.worker.plan()
        self.assertEqual(wake["decision"], "wake", wake)
        return self.cli({"action": "begin", "lease": wake["payload"]["lease"]}, ambient_recent)

    def test_independent_worker_processes_apply_private_config_without_hook_environment(self):
        self.configure("1")
        first = self.begin(ambient_recent="0")
        self.assertEqual(first["context_capabilities"]["features"], ["recent_context_v1", "memory_delta_v1"])
        delta = {"schema_version": 1, "handle": first["handle"], "revision": first["revision"],
            "source_message_ids": [message["message_id"] for message in first["messages"]],
            "topic": "SYNTHETIC recent topic", "intent": "verify private mode", "bot_action": "queued reply",
            "open_loop": "await next process", "historical_refs": []}
        reply = self.cli({"action": "reply", "handle": first["handle"], "revision": first["revision"],
            "text": "SYNTHETIC answer", "memory_delta_v1": delta}, ambient_recent="0")
        self.assertTrue(reply["ok"])
        second = self.begin(ambient_recent="0")
        self.assertEqual(second["context"]["recent_context_v1"]["entries"][0]["topic"], delta["topic"])

    def test_private_zero_overrides_platform_worker_inherited_one(self):
        self.configure("0")
        batch = self.begin(ambient_recent="1")
        self.assertNotIn("context_capabilities", batch)
        self.assertNotIn("context", batch)

    def test_no_private_file_preserves_existing_platform_environment(self):
        self.assertFalse(self.config.exists())
        batch = self.begin(ambient_recent="1")
        self.assertEqual(batch["context_capabilities"]["features"], ["recent_context_v1", "memory_delta_v1"])


if __name__ == "__main__":
    unittest.main()
