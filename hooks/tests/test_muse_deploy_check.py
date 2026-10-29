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
        return self.git_at(self.source, *arguments)

    @staticmethod
    def git_at(directory, *arguments):
        return subprocess.check_output(["git", "-C", str(directory)] + list(arguments), text=True)

    def set_package_source(self):
        """Two independently clean commits, with deliberately different Hooks."""
        self.package_source = self.home / "package-candidate"
        shutil.copytree(self.source, self.package_source, ignore=shutil.ignore_patterns(".git"))
        (self.package_source / "wechat_bridge/media.py").write_text("# synthetic newer package\n")
        (self.package_source / "hooks/wechat-inbox-watch.sh").write_text("# older unrelated hook\n")
        (self.package_source / "hooks/muse-worker.py").write_text("# older unrelated adapter\n")
        self.write_hook(self.package_source / "hooks/wechat-inbox-watch.json",
                        dict(self.hook, prompt="Older unrelated prompt."))
        for args in (("init", "-q"), ("config", "user.name", "Synthetic Test"),
                     ("config", "user.email", "synthetic@example.invalid"),
                     ("add", "."), ("commit", "-qm", "synthetic separate package")):
            self.git_at(self.package_source, *args)
        self.package_head = self.git_at(self.package_source, "rev-parse", "HEAD").strip()
        shutil.copytree(self.package_source / "wechat_bridge", self.live / "wechat_bridge", dirs_exist_ok=True)
        self.package_args = {"package-source": str(self.package_source),
                             "package-expected-head": self.package_head}

    def set_probe(self, **changes):
        result = {"package_directory": str(self.live / "wechat_bridge"),
                  "worker_state_file": str(self.live / "wechat_bridge/worker_state.py"),
                  "sys_executable": str(self.fake_python),
                  "python_version": [3, 11, 9],
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
        self.assertNotIn("SYNTHETIC-OTHER-PRIVATE-DESTINATION", output.getvalue())
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
        self.assertEqual(report["source_mode"], "single")
        self.assertEqual(report["source_bindings"]["runtime_package"], "source")
        self.assertNotIn("package_source", report)
        self.assertEqual(report["runtime_probe"]["source_binding"], "source")
        self.assertEqual(report["hook_target_preservation"], {
            "requested": False, "performed": False, "status": "not_performed",
            "note": "未进行前后目标保全比较"})

    def preserved_hook(self, **changes):
        preserved = json.loads((self.live / "export.json").read_text())
        preserved.update(changes)
        # Old prompt/settings may differ after a legitimate update; this
        # comparison protects the Hook identity and delivery tuple only.
        preserved["prompt"] = "Previous synthetic prompt."
        preserved["private_before_export_field"] = "SYNTHETIC-SECRET-NEVER-REPORT"
        path = self.live / "before-export.json"
        self.write_hook(path, preserved)
        return path

    def test_preserved_target_matches_without_writes_or_private_export_fields(self):
        preserved = self.preserved_hook()
        before = self.snapshot()
        paths_before = set(self.home.rglob("*"))
        report = self.invoke(**{"preserve-hook-json": str(preserved)})
        self.assertTrue(report["ok"], report)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(paths_before, set(self.home.rglob("*")))
        result = report["hook_target_preservation"]
        self.assertTrue(result["requested"])
        self.assertTrue(result["performed"])
        self.assertEqual(result["status"], "matched")
        self.assertTrue(result["hook_id_equal"])
        self.assertTrue(result["delivery_surface_equal"])
        self.assertTrue(result["delivery_to_equal"])
        self.assertTrue(result["preserved_hook_id_valid"])
        self.assertTrue(result["preserved_delivery_valid"])
        self.assertEqual(result["preserved_export_sha256"], hashlib.sha256(preserved.read_bytes()).hexdigest())
        self.assertEqual(result["current_export_sha256"],
                         hashlib.sha256((self.live / "export.json").read_bytes()).hexdigest())
        self.assertFalse(any(report["limits"].values()))
        self.assertNotIn("private_before_export_field", json.dumps(report))
        self.assertNotIn("private_export_field", json.dumps(report))

    def test_two_non_placeholder_targets_still_fail_preservation(self):
        preserved = self.preserved_hook()
        exported = json.loads((self.live / "export.json").read_text())
        exported["delivery"]["to"] = "SYNTHETIC-OTHER-PRIVATE-DESTINATION"
        self.write_hook(self.live / "export.json", exported)
        report = self.invoke(**{"preserve-hook-json": str(preserved)})
        self.assertEqual(self.failures(report), {
            "preserve_hook_delivery_to_equal": "preserve_hook_delivery_target_mismatch"})
        self.assertFalse(report["hook_target_preservation"]["delivery_to_equal"])

    def test_preserved_export_missing_special_or_invalid_is_sanitized(self):
        missing = self.live / "missing-before.json"
        fifo = self.live / "before-fifo"
        os.mkfifo(fifo)
        for path in (missing, fifo):
            with self.subTest(path=path.name):
                report = self.invoke(**{"preserve-hook-json": str(path)})
                self.assertEqual(self.failures(report), {
                    "preserve_hook_export": "preserve_hook_export_unreadable"})
                self.assertFalse(report["hook_target_preservation"]["performed"])
        invalid = self.live / "invalid-before.json"
        for content in ('{"private":"SYNTHETIC-SECRET-NEVER-REPORT"',
                        '{"hooks":[]}', '[]', 'null', '"SYNTHETIC-SECRET-NEVER-REPORT"'):
            with self.subTest(content=content):
                invalid.write_text(content)
                self.assertEqual(self.failures(self.invoke(**{"preserve-hook-json": str(invalid)})), {
                    "preserve_hook_export": "preserve_hook_export_invalid"})

    def test_preserved_export_requires_non_placeholder_side_chat_and_correct_hook(self):
        for changes, expected in (
                ({"delivery": {"surface": "side_chat", "to": "<wechat-bridge-chat-id>"}},
                 "preserve_hook_delivery_target_missing_or_placeholder"),
                ({"delivery": {"surface": "side_chat"}},
                 "preserve_hook_delivery_target_missing_or_placeholder"),
                ({"delivery": {"surface": "other", "to": "SYNTHETIC-PRIVATE-DESTINATION"}},
                 "preserve_hook_delivery_surface_invalid"),
                ({"delivery": None}, "preserve_hook_delivery_surface_invalid"),
                ({"id": "wrong-synthetic-hook"}, "preserve_hook_id_mismatch"),
                ({"id": None}, "preserve_hook_id_mismatch")):
            with self.subTest(changes=changes):
                preserved = self.preserved_hook(**changes)
                report = self.invoke(**{"preserve-hook-json": str(preserved)})
                self.assertIn(expected, self.failures(report).values())
                self.assertEqual(report["hook_target_preservation"]["status"], "failed")

    def test_preservation_compares_exact_surface_id_and_target_values(self):
        preserved = self.preserved_hook()
        original = json.loads((self.live / "export.json").read_text())
        for mutate, failure in (
                (lambda h: h.update(id="wrong-synthetic-hook"), "preserve_hook_id_mismatch"),
                (lambda h: h["delivery"].update(surface="SIDE_CHAT"), "preserve_hook_delivery_surface_mismatch"),
                (lambda h: h["delivery"].update(to=h["delivery"]["to"].lower()), "preserve_hook_delivery_target_mismatch")):
            with self.subTest(failure=failure):
                exported = json.loads(json.dumps(original))
                mutate(exported)
                self.write_hook(self.live / "export.json", exported)
                report = self.invoke(**{"preserve-hook-json": str(preserved)})
                self.assertIn(failure, self.failures(report).values())

    def test_valid_preserved_export_cannot_hide_invalid_current_export(self):
        preserved = self.preserved_hook()
        (self.live / "export.json").write_text('{"SYNTHETIC-SECRET-NEVER-REPORT"')
        report = self.invoke(**{"preserve-hook-json": str(preserved)})
        self.assertEqual(self.failures(report), {
            "hook_configuration": "hook_json_invalid",
            "preserve_hook_comparison": "preserve_hook_current_unavailable"})
        self.assertFalse(report["hook_target_preservation"]["performed"])

    def test_same_wrong_hook_in_both_exports_still_fails_expected_identity(self):
        preserved = self.preserved_hook(id="wrong-synthetic-hook")
        exported = json.loads((self.live / "export.json").read_text())
        exported["id"] = "wrong-synthetic-hook"
        self.write_hook(self.live / "export.json", exported)
        report = self.invoke(**{"preserve-hook-json": str(preserved)})
        self.assertEqual(self.failures(report), {
            "hook_id": "hook_id_mismatch", "preserve_hook_id": "preserve_hook_id_mismatch"})
        self.assertTrue(report["hook_target_preservation"]["hook_id_equal"])
        self.assertFalse(report["hook_target_preservation"]["preserved_hook_id_valid"])
        self.assertEqual(report["hook_target_preservation"]["status"], "failed")

    def test_preservation_keeps_dual_source_compatibility_and_read_only_behavior(self):
        self.set_package_source()
        preserved = self.preserved_hook()
        before = self.snapshot()
        args = dict(self.package_args, **{"preserve-hook-json": str(preserved)})
        report = self.invoke(**args)
        self.assertTrue(report["ok"], report)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(report["source_mode"], "dual")
        self.assertEqual(report["hook_target_preservation"]["status"], "matched")

    def test_dual_source_binds_package_separately_without_mutating_either_source(self):
        self.set_package_source()
        before = self.snapshot()
        report = self.invoke(**self.package_args)
        self.assertTrue(report["ok"], report)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(report["source_mode"], "dual")
        self.assertEqual(report["source"]["head"], self.head)
        self.assertEqual(report["package_source"]["head"], self.package_head)
        self.assertFalse(report["source"]["dirty"])
        self.assertFalse(report["package_source"]["dirty"])
        checks = {row["name"] for row in report["checks"] if row["ok"]}
        for prefix in ("source", "package_source"):
            for name in ("is_repository_root", "head", "clean"):
                self.assertIn(prefix + "_" + name, checks)
        self.assertEqual(report["source_bindings"], {
            "hook_script": "source", "adapter": "source", "hook_configuration": "source",
            "runtime_package": "package_source"})
        self.assertEqual(report["runtime_probe"]["source_binding"], "package_source")
        self.assertEqual(report["runtime_probe"]["sys_executable"], str(self.fake_python))
        self.assertEqual(report["runtime_probe"]["python_version"], "3.11.9")
        self.assertFalse(any(report["limits"].values()))
        # The old invocation still means one source; a mismatched installed
        # package cannot be silently forgiven when the new options are absent.
        self.assertIn("runtime_package_python_sources", self.failures(self.invoke()))

    def test_dual_source_wrong_package_head_and_dirty_package_fail(self):
        self.set_package_source()
        (self.package_source / "untracked.txt").write_text("synthetic untracked file")
        args = dict(self.package_args, **{"package-expected-head": "0" * 40})
        report = self.invoke(**args)
        self.assertEqual(set(self.failures(report)), {"package_source_head", "package_source_clean"})
        self.assertEqual(self.failures(report)["package_source_clean"], "package_source_dirty")
        (self.package_source / "untracked.txt").unlink()
        (self.package_source / "wechat_bridge/media.py").write_text("# uncommitted package change\n")
        self.assertIn("package_source_clean", self.failures(self.invoke(**self.package_args)))

    def test_dual_source_good_package_does_not_hide_dirty_or_wrong_hook_source(self):
        self.set_package_source()
        (self.source / "untracked.txt").write_text("synthetic untracked file")
        args = dict(self.package_args, **{"expected-head": "0" * 40})
        self.assertEqual(set(self.failures(self.invoke(**args))), {"source_head", "source_clean"})

    def test_dual_source_requires_both_full_package_arguments(self):
        self.set_package_source()
        for option in self.package_args:
            with self.subTest(option=option):
                report = self.invoke(**{option: self.package_args[option]})
                self.assertEqual(self.failures(report), {
                    "arguments": "package_source_and_expected_head_required_together"})
        for bad in (self.package_head[:7], "", "SYNTHETIC-SECRET-NEVER-REPORT"):
            with self.subTest(bad=bad):
                args = dict(self.package_args, **{"package-expected-head": bad})
                self.assertEqual(self.failures(self.invoke(**args)), {
                    "arguments": "full_package_expected_head_required"})

    def test_each_source_must_be_a_repository_root(self):
        self.set_package_source()
        for option, subdirectory, check_name in (
                ("source", self.source / "hooks", "source_is_repository_root"),
                ("package-source", self.package_source / "wechat_bridge", "package_source_is_repository_root")):
            with self.subTest(option=option):
                args = dict(self.package_args, **{option: str(subdirectory)})
                self.assertIn(check_name, self.failures(self.invoke(**args)))

    def test_unavailable_package_git_is_sanitized(self):
        self.set_package_source()
        args = dict(self.package_args, **{"package-source": str(self.live)})
        self.assertEqual(self.failures(self.invoke(**args))["package_source_git"], "package_source_git_unavailable")

    def test_swapped_sources_with_correct_heads_do_not_pass(self):
        self.set_package_source()
        args = {"source": str(self.package_source), "expected-head": self.package_head,
                "package-source": str(self.source), "package-expected-head": self.head}
        failures = self.failures(self.invoke(**args))
        self.assertEqual(set(failures), {"hook_script", "adapter", "hook_prompt", "runtime_package_python_sources"})

    def test_correct_hook_with_wrong_installed_package_does_not_pass(self):
        self.set_package_source()
        (self.live / "wechat_bridge/media.py").unlink()
        self.assertEqual(set(self.failures(self.invoke(**self.package_args))), {"runtime_package_python_sources"})

    def test_good_package_cannot_hide_old_deployed_hooks_or_prompt(self):
        self.set_package_source()
        # These stale files match the package-source commit exactly, but the
        # authoritative Hook source is still --source, never --package-source.
        for source_name, deployed_name in (("wechat-inbox-watch.sh", "hook.sh"), ("muse-worker.py", "muse-worker.py")):
            shutil.copyfile(self.package_source / "hooks" / source_name, self.live / deployed_name)
        exported = json.loads((self.live / "export.json").read_text())
        exported["prompt"] = "Older unrelated prompt."
        self.write_hook(self.live / "export.json", exported)
        self.assertEqual(set(self.failures(self.invoke(**self.package_args))), {"hook_script", "adapter", "hook_prompt"})

    def test_dual_source_package_special_file_still_fails_closed(self):
        self.set_package_source()
        package_file = self.package_source / "wechat_bridge/media.py"
        package_file.unlink()
        os.mkfifo(package_file)
        report = self.invoke(**self.package_args)
        self.assertEqual(self.failures(report)["runtime_probe"], "not_a_regular_file")

    def test_invalid_interpreter_identity_is_rejected_and_error_text_redacted(self):
        for changes in ({"sys_executable": "SYNTHETIC-SECRET-NEVER-REPORT"},
                        {"python_version": "SYNTHETIC-SECRET-NEVER-REPORT"},
                        {"python_version": [True, 11, 9]},
                        {"python_version": [-1, 11, 9]}):
            with self.subTest(changes=changes):
                self.set_probe(**changes)
                self.assertEqual(self.failures(self.invoke())["runtime_probe"], "runtime_probe_invalid")

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
        self.assertEqual(result["sys_executable"], sys.executable)
        self.assertEqual(result["python_version"], list(sys.version_info[:3]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
