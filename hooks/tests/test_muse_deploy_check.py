"""Synthetic read-only deployment checks; no live accounts or network."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


TOOL = Path(__file__).resolve().parents[1] / "muse-deploy-check.py"
SPEC = importlib.util.spec_from_file_location("muse_deploy_check", TOOL)
DEPLOY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEPLOY)


class DeploymentCheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-muse-deploy-")
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.source = self.home / "candidate"
        self.live = self.home / "observed"
        (self.source / "hooks").mkdir(parents=True)
        (self.source / "wechat_bridge").mkdir()
        self.live.mkdir()
        (self.source / "hooks/wechat-inbox-watch.sh").write_text("#!/bin/sh\n# synthetic hook\n")
        (self.source / "hooks/muse-worker.py").write_text("# synthetic adapter\n")
        (self.source / "wechat_bridge/__init__.py").write_text("# synthetic package\n")
        (self.source / "wechat_bridge/worker_state.py").write_text("# synthetic worker\n")
        self.hook = {"version": 1, "id": "wechat-inbox-watch", "enabled": True,
                     "prompt": "Synthetic prompt: only the supplied task.", "poll_interval_secs": 5,
                     "script_timeout_secs": 600, "script_path": str(self.live / "hook.sh"),
                     "delivery": {"surface": "side_chat", "to": "<wechat-bridge-chat-id>"}}
        self.write_hook(self.source / "hooks/wechat-inbox-watch.json", self.hook)
        shutil.copyfile(self.source / "hooks/wechat-inbox-watch.sh", self.live / "hook.sh")
        shutil.copyfile(self.source / "hooks/muse-worker.py", self.live / "muse-worker.py")
        shutil.copytree(self.source / "wechat_bridge", self.live / "wechat_bridge")
        exported = dict(self.hook, delivery={"surface": "side_chat", "to": "SYNTHETIC-PRIVATE-DESTINATION"})
        exported["private_export_field"] = "SYNTHETIC-SECRET-NEVER-REPORT"
        self.write_hook(self.live / "export.json", exported)
        self.fake_python = self.live / "python"
        self.set_probe()
        self.git("init", "-q")
        self.git("config", "user.name", "Synthetic Test")
        self.git("config", "user.email", "synthetic@example.invalid")
        self.git("add", ".")
        self.git("commit", "-qm", "synthetic deployment candidate")
        self.head = self.git("rev-parse", "HEAD").strip()
        for name in ("connect", "connect_ex", "bind"):
            blocker = patch.object(socket.socket, name, side_effect=AssertionError("network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        for name in ("getaddrinfo", "create_connection"):
            blocker = patch.object(socket, name, side_effect=AssertionError("network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)

    @staticmethod
    def write_hook(path, value):
        path.write_text(json.dumps(value, ensure_ascii=False))

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.source)] + list(arguments), text=True)

    def set_probe(self, **changes):
        result = {"package_directory": str(self.live / "wechat_bridge"),
                  "worker_state_file": str(self.live / "wechat_bridge/worker_state.py"),
                  "bytecode_disabled": True}
        result.update(changes)
        self.fake_python.write_text("#!/bin/sh\nprintf '%s\\n' " + shlex.quote(json.dumps(result)) + "\n")
        self.fake_python.chmod(0o700)

    def invoke(self, **changes):
        values = {"source": str(self.source), "hook-script": str(self.live / "hook.sh"),
                  "adapter": str(self.live / "muse-worker.py"), "hook-json": str(self.live / "export.json"),
                  "python": str(self.fake_python), "expected-head": self.head}
        values.update(changes)
        argv = [item for name, value in values.items() for item in ("--" + name, value)]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code = DEPLOY.main(argv)
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        report = json.loads(output.getvalue())
        self.assertEqual(code, 0 if report["ok"] else 1)
        self.assertNotIn("SYNTHETIC-PRIVATE-DESTINATION", output.getvalue())
        self.assertNotIn("SYNTHETIC-SECRET-NEVER-REPORT", output.getvalue())
        return report

    @staticmethod
    def failures(report):
        return {entry["name"]: entry.get("failure_code") for entry in report["checks"] if not entry["ok"]}

    def snapshot(self):
        return {str(p.relative_to(self.home)): (p.stat().st_mode, p.stat().st_mtime_ns,
                hashlib.sha256(p.read_bytes()).hexdigest())
                for p in self.home.rglob("*") if p.is_file()}

    def test_matching_files_and_configuration_are_read_only_and_have_explicit_limits(self):
        before = self.snapshot()
        report = self.invoke()
        self.assertTrue(report["ok"], report)
        self.assertEqual(before, self.snapshot())
        self.assertFalse(any(report["limits"].values()))
        self.assertFalse(report["observed_export_source"]["freshness_verified"])
        self.assertEqual(report["runtime_probe"]["fingerprint_scope"], "all_python_sources_in_package")

    def test_dirty_candidate_and_wrong_head_fail(self):
        (self.source / "untracked.txt").write_text("synthetic uncommitted file")
        report = self.invoke(**{"expected-head": "0" * 40})
        self.assertIn("source_head", self.failures(report))
        self.assertEqual(self.failures(report)["source_clean"], "source_dirty")

    def test_live_adapter_and_hook_mismatch_fail(self):
        (self.live / "muse-worker.py").write_text("# wrong synthetic adapter\n")
        (self.live / "hook.sh").write_text("# wrong synthetic hook\n")
        report = self.invoke()
        self.assertEqual(set(self.failures(report)), {"adapter", "hook_script"})

    def test_prompt_is_exact_not_trimmed_and_bad_configuration_fails(self):
        exported = json.loads((self.live / "export.json").read_text())
        exported.update(prompt=self.hook["prompt"] + "\n", enabled=1, poll_interval_secs=True)
        exported["delivery"]["to"] = "<wechat-bridge-chat-id>"
        self.write_hook(self.live / "export.json", exported)
        failures = self.failures(self.invoke())
        self.assertIn("hook_prompt", failures)
        self.assertIn("hook_enabled", failures)
        self.assertIn("hook_poll_interval", failures)
        self.assertIn("hook_delivery_to_non_placeholder", failures)

    def test_configuration_must_point_to_the_script_actually_checked(self):
        exported = json.loads((self.live / "export.json").read_text())
        exported["script_path"] = str(self.live / "another-hook.sh")
        self.write_hook(self.live / "export.json", exported)
        self.assertIn("hook_configured_script_path", self.failures(self.invoke()))

    def test_adapter_elsewhere_cannot_hide_missing_hook_directory_adapter(self):
        elsewhere = self.home / "unrelated-adapter.py"
        shutil.copyfile(self.source / "hooks/muse-worker.py", elsewhere)
        (self.live / "muse-worker.py").unlink()
        failures = self.failures(self.invoke(adapter=str(elsewhere)))
        self.assertEqual(failures["adapter_default_path_binding"], "adapter_must_be_hook_directory_default")

    def test_worker_and_extra_python_source_mismatch_fail(self):
        (self.live / "wechat_bridge/worker_state.py").write_text("# different worker\n")
        (self.live / "wechat_bridge/extra.py").write_text("# unexpected source\n")
        failures = self.failures(self.invoke())
        self.assertIn("runtime_package_python_sources", failures)
        self.assertIn("runtime_worker_state", failures)

    def test_wrong_worker_origin_and_failed_probe_are_sanitized(self):
        self.set_probe(worker_state_file=str(self.live / "outside.py"))
        failures = self.failures(self.invoke())
        self.assertEqual(failures["runtime_probe"], "runtime_worker_outside_package")
        self.fake_python.write_text("#!/bin/sh\necho SYNTHETIC-SECRET-NEVER-REPORT >&2\nexit 1\n")
        failures = self.failures(self.invoke())
        self.assertEqual(failures["runtime_probe"], "runtime_probe_failed")

    def test_multiple_hook_export_is_rejected_without_guessing(self):
        self.write_hook(self.live / "export.json", {"hooks": [self.hook]})
        self.assertEqual(self.failures(self.invoke())["hook_configuration"], "hook_definition_required")

    def test_missing_and_special_file_fail_without_reading_private_contents(self):
        report = self.invoke(adapter=str(self.live / "missing.py"))
        self.assertEqual(self.failures(report)["adapter"], "file_unreadable")
        fifo = self.live / "fifo"
        os.mkfifo(fifo)
        self.assertEqual(self.failures(self.invoke(adapter=str(fifo)))["adapter"], "not_a_regular_file")

    def test_invalid_arguments_still_return_one_sanitized_json(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code = DEPLOY.main([])
        self.assertEqual(code, 1)
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertEqual(json.loads(output.getvalue())["checks"][0]["failure_code"], "invalid_arguments")

    def test_probe_itself_blocks_sockets_and_bytecode(self):
        # Run the actual probe in its own process so its permanent audit hook
        # cannot affect the test runner. Synthetic in-memory modules avoid any
        # package installation or use of the user's deployment.
        bootstrap = r'''
import contextlib, io, json, pathlib, socket, sys, types
package = types.ModuleType("wechat_bridge")
package.__path__ = [sys.argv[1]]
worker = types.ModuleType("wechat_bridge.worker_state")
worker.__file__ = str(pathlib.Path(sys.argv[1]) / "worker_state.py")
sys.modules.update({"wechat_bridge": package, "wechat_bridge.worker_state": worker})
with contextlib.redirect_stdout(io.StringIO()) as output:
    exec(sys.argv[2], {})
result = json.loads(output.getvalue())
for name, call in (("creation", lambda: socket.socket()),
                   ("dns", lambda: socket.getaddrinfo("synthetic.invalid", 443))):
    try:
        call()
    except RuntimeError:
        result[name + "_blocked"] = True
    else:
        raise AssertionError("network operation was not blocked")
print(json.dumps(result))
'''
        proc = subprocess.run([sys.executable, "-I", "-B", "-c", bootstrap,
                               str(self.live / "wechat_bridge"), DEPLOY.PROBE],
                              capture_output=True, text=True, check=True, timeout=10)
        result = json.loads(proc.stdout)
        self.assertTrue(result["bytecode_disabled"])
        self.assertTrue(result["creation_blocked"])
        self.assertTrue(result["dns_blocked"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
