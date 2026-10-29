#!/usr/bin/env python3
"""Read-only checks of explicit candidate sources and deployment files.

This examines a supplied Hook export and performs a fresh isolated Python import.
Hook files always come from --source/--expected-head. By default the package does
too; --package-source/--package-expected-head can select a separate package commit.
--preserve-hook-json compares a pre-update single-Hook export with --hook-json;
omitting it leaves target preservation unexamined.
It cannot establish which configuration a platform has activated, which code an
existing worker has loaded, or whether a message reached a phone.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


MAX_FILE_BYTES = 16 * 1024 * 1024
PROBE = r'''
import contextlib, io, json, pathlib, socket, sys
def blocked(*args, **kwargs):
    raise RuntimeError("network is disabled in deployment inspection")
def deny_socket_events(event, args):
    if event.startswith("socket."):
        blocked()
sys.addaudithook(deny_socket_events)
socket.socket.connect = blocked
socket.socket.connect_ex = blocked
socket.socket.bind = blocked
socket.create_connection = blocked
socket.getaddrinfo = blocked
sys.dont_write_bytecode = True
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    import wechat_bridge
    import wechat_bridge.worker_state as worker_state
package_paths = list(wechat_bridge.__path__)
if len(package_paths) != 1:
    raise RuntimeError("expected one package directory")
print(json.dumps({"package_directory": str(pathlib.Path(package_paths[0]).resolve()),
                  "worker_state_file": str(pathlib.Path(worker_state.__file__).resolve()),
                  "sys_executable": sys.executable,
                  "python_version": list(sys.version_info[:3]),
                  "bytecode_disabled": bool(sys.dont_write_bytecode)}))
'''


class CheckFailure(Exception):
    def __init__(self, code):
        self.code = code


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise CheckFailure("invalid_arguments")


def read_file(path):
    """Reject special/oversized files before reading; do not create anything."""
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise CheckFailure("not_a_regular_file")
            if info.st_size > MAX_FILE_BYTES:
                raise CheckFailure("file_too_large")
            content = stream.read(MAX_FILE_BYTES + 1)
            if len(content) > MAX_FILE_BYTES:
                raise CheckFailure("file_too_large")
            return content
    except CheckFailure:
        raise
    except OSError:
        raise CheckFailure("file_unreadable")


def digest(content):
    return hashlib.sha256(content).hexdigest()


def package_digest(directory):
    """Fingerprint all Python sources, including relative names and extra files."""
    directory = Path(directory)
    if not directory.is_dir():
        raise CheckFailure("package_directory_unavailable")
    try:
        paths = sorted(directory.rglob("*.py"), key=lambda p: p.relative_to(directory).as_posix())
        rows = [(path.relative_to(directory).as_posix(), digest(read_file(path))) for path in paths]
    except OSError:
        raise CheckFailure("package_sources_unreadable")
    if not rows or not any(name == "worker_state.py" for name, _ in rows):
        raise CheckFailure("worker_state_source_missing")
    encoded = json.dumps(rows, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return digest(encoded), dict(rows)


def parse_hook(content):
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeError, ValueError):
        raise CheckFailure("hook_json_invalid")
    # A single exported Hook definition is required. Never guess among hooks.
    if not isinstance(value, dict) or not isinstance(value.get("prompt"), str):
        raise CheckFailure("hook_definition_required")
    return value


def non_placeholder_target(value):
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        return False
    lower = value.lower()
    return not (any(c.isspace() for c in value) or "<" in value or ">" in value
                or any(marker in lower for marker in ("placeholder", "wechat-bridge-chat-id", "replace_me", "your-chat")))


def inspect(args):
    report = {
        "schema_version": 1,
        "scope": "read_only_candidate_file_and_configuration_consistency",
        "checks": [],
        "hook_target_preservation": {
            "requested": args.preserve_hook_json is not None,
            "performed": False,
            "status": "not_performed",
            "note": "未进行前后目标保全比较",
        },
        "limits": {
            "existing_serve_or_worker_loaded_code_proven": False,
            "platform_active_configuration_proven": False,
            "delivery_destination_identity_proven": False,
            "runtime_environment_overrides_proven_absent": False,
            "phone_delivery_proven": False,
        },
    }

    def check(name, ok, code=None):
        entry = {"name": name, "ok": bool(ok)}
        if not ok:
            entry["failure_code"] = code or name + "_mismatch"
        report["checks"].append(entry)

    if (args.package_source is None) != (args.package_expected_head is None):
        raise CheckFailure("package_source_and_expected_head_required_together")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.expected_head):
        raise CheckFailure("full_expected_head_required")
    dual_source = args.package_source is not None
    if dual_source and not re.fullmatch(r"[0-9a-fA-F]{40}", args.package_expected_head):
        raise CheckFailure("full_package_expected_head_required")
    source = Path(args.source).expanduser().resolve()
    package_source = Path(args.package_source).expanduser().resolve() if dual_source else source
    report["source_mode"] = "dual" if dual_source else "single"
    report["source_bindings"] = {
        "hook_script": "source", "adapter": "source", "hook_configuration": "source",
        "runtime_package": "package_source" if dual_source else "source",
    }
    git_env = dict(os.environ, GIT_OPTIONAL_LOCKS="0", GIT_TERMINAL_PROMPT="0")

    def inspect_source(directory, expected_head, label):
        report[label] = {"expected_head": expected_head.lower()}

        def git(*arguments):
            result = subprocess.run(["git", "-C", str(directory)] + list(arguments),
                                    stdin=subprocess.DEVNULL, capture_output=True,
                                    env=git_env, timeout=15, check=True)
            return result.stdout.decode("utf-8", "replace").strip()

        try:
            root = Path(git("rev-parse", "--show-toplevel")).resolve()
            head = git("rev-parse", "HEAD")
            dirty = git("status", "--porcelain=v1", "--untracked-files=all")
            report[label].update(head=head, dirty=bool(dirty), dirty_entries=len(dirty.splitlines()))
            check(label + "_is_repository_root", root == directory)
            check(label + "_head", head == expected_head.lower())
            check(label + "_clean", not dirty, label + "_dirty")
        except (OSError, subprocess.SubprocessError):
            check(label + "_git", False, label + "_git_unavailable")

    inspect_source(source, args.expected_head, "source")
    if dual_source:
        inspect_source(package_source, args.package_expected_head, "package_source")

    report["files"] = {}
    # The inspected candidate supports the Hook's same-directory default only.
    # A correct adapter elsewhere must not hide a missing/stale effective one.
    hook_path = Path(args.hook_script).expanduser().absolute()
    effective_adapter = (hook_path.parent / "muse-worker.py").resolve()
    check("adapter_default_path_binding", Path(args.adapter).expanduser().resolve() == effective_adapter,
          "adapter_must_be_hook_directory_default")
    report["adapter_path_policy"] = "same_directory_default_only_environment_overrides_not_inspected"
    for label, relative, observed in (
            ("hook_script", "hooks/wechat-inbox-watch.sh", args.hook_script),
            ("adapter", "hooks/muse-worker.py", args.adapter)):
        try:
            source_hash = digest(read_file(source / relative))
            observed_hash = digest(read_file(Path(observed).expanduser()))
            report["files"][label] = {"source_sha256": source_hash, "observed_sha256": observed_hash}
            check(label, source_hash == observed_hash)
        except CheckFailure as error:
            check(label, False, error.code)

    source_hook = None
    hook = None
    current_export_hash = None
    try:
        source_hook = parse_hook(read_file(source / "hooks/wechat-inbox-watch.json"))
        exported = read_file(Path(args.hook_json).expanduser())
        current_export_hash = digest(exported)
        report["observed_export_source"] = {
            "kind": "explicitly_supplied_single_hook_json_file",
            "sha256": current_export_hash,
            "freshness_verified": False,
            "platform_activation_proven": False,
        }
        hook = parse_hook(exported)
        source_interval = source_hook.get("poll_interval_secs")
        check("source_hook_enabled", source_hook.get("enabled") is True,
              "source_hook_disabled_or_invalid")
        check("source_hook_poll_interval", type(source_interval) in (int, float) and source_interval == 5,
              "source_hook_poll_interval_must_be_five")
        check("hook_id", bool(source_hook.get("id")) and hook.get("id") == source_hook["id"])
        check("hook_prompt", hook["prompt"] == source_hook["prompt"])
        check("hook_enabled", hook.get("enabled") is True, "hook_disabled_or_invalid")
        interval = hook.get("poll_interval_secs")
        check("hook_poll_interval", type(interval) in (int, float) and interval == 5,
              "hook_poll_interval_must_be_five")
        source_timeout = source_hook.get("script_timeout_secs")
        observed_timeout = hook.get("script_timeout_secs")
        check("hook_script_timeout", type(source_timeout) in (int, float) and source_timeout > 0
              and type(observed_timeout) in (int, float) and observed_timeout == source_timeout)
        delivery = hook.get("delivery")
        delivery = delivery if isinstance(delivery, dict) else {}
        source_delivery = source_hook.get("delivery")
        source_delivery = source_delivery if isinstance(source_delivery, dict) else {}
        check("hook_delivery_surface", delivery.get("surface") == "side_chat"
              and delivery.get("surface") == source_delivery.get("surface"))
        check("hook_delivery_to_non_placeholder", non_placeholder_target(delivery.get("to")),
              "hook_delivery_target_missing_or_placeholder")
        configured_script = hook.get("script_path")
        script_matches = (isinstance(configured_script, str) and bool(configured_script)
                          and Path(configured_script).expanduser().is_absolute()
                          and Path(configured_script).expanduser().resolve()
                          == Path(args.hook_script).expanduser().resolve())
        check("hook_configured_script_path", script_matches)
        report["hook_configuration"] = {
            "prompt_sha256": digest(hook["prompt"].encode("utf-8")),
            "delivery_target": "non_placeholder" if non_placeholder_target(delivery.get("to")) else "invalid",
        }
    except (CheckFailure, OSError, TypeError, ValueError) as error:
        check("hook_configuration", False, error.code if isinstance(error, CheckFailure) else "hook_configuration_invalid")

    if args.preserve_hook_json is not None:
        preservation = report["hook_target_preservation"]
        preservation.update(status="failed", note="前后导出比较不证明平台激活、目标身份或 worker 采用")
        try:
            preserved_bytes = read_file(Path(args.preserve_hook_json).expanduser())
            preservation["preserved_export_sha256"] = digest(preserved_bytes)
        except CheckFailure:
            check("preserve_hook_export", False, "preserve_hook_export_unreadable")
        else:
            try:
                preserved = parse_hook(preserved_bytes)
            except CheckFailure:
                check("preserve_hook_export", False, "preserve_hook_export_invalid")
            else:
                # Only compare the identity and delivery tuple. Never copy any
                # exported field, target, path or exception into the report.
                old_delivery = preserved.get("delivery")
                old_delivery = old_delivery if isinstance(old_delivery, dict) else {}
                valid_surface = old_delivery.get("surface") == "side_chat"
                valid_target = non_placeholder_target(old_delivery.get("to"))
                check("preserve_hook_delivery_surface", valid_surface,
                      "preserve_hook_delivery_surface_invalid")
                check("preserve_hook_delivery_to_non_placeholder", valid_target,
                      "preserve_hook_delivery_target_missing_or_placeholder")
                if hook is None or source_hook is None:
                    check("preserve_hook_comparison", False, "preserve_hook_current_unavailable")
                else:
                    new_delivery = hook.get("delivery")
                    new_delivery = new_delivery if isinstance(new_delivery, dict) else {}
                    old_id = preserved.get("id")
                    valid_id = (isinstance(old_id, str) and bool(old_id)
                                and old_id == source_hook.get("id"))
                    id_equal = isinstance(old_id, str) and old_id == hook.get("id")
                    surface_equal = old_delivery.get("surface") == new_delivery.get("surface")
                    target_equal = old_delivery.get("to") == new_delivery.get("to")
                    check("preserve_hook_id", valid_id and id_equal, "preserve_hook_id_mismatch")
                    check("preserve_hook_delivery_surface_equal", surface_equal,
                          "preserve_hook_delivery_surface_mismatch")
                    check("preserve_hook_delivery_to_equal", target_equal,
                          "preserve_hook_delivery_target_mismatch")
                    preservation.update(
                        performed=True, current_export_sha256=current_export_hash,
                        hook_id_equal=id_equal, delivery_surface_equal=surface_equal,
                        delivery_to_equal=target_equal,
                        preserved_hook_id_valid=valid_id,
                        preserved_delivery_valid=valid_surface and valid_target,
                        status="matched" if (valid_surface and valid_target and valid_id and id_equal
                                             and surface_equal and target_equal) else "failed")

    try:
        interpreter = str(Path(args.python).expanduser().absolute())
        probe = subprocess.run([interpreter, "-I", "-B", "-c", PROBE],
                               stdin=subprocess.DEVNULL, capture_output=True,
                               cwd="/", env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
                               timeout=30, check=True)
        if len(probe.stdout) > 65536:
            raise CheckFailure("runtime_probe_invalid")
        try:
            observed = json.loads(probe.stdout)
            package = Path(observed["package_directory"]).resolve()
            worker = Path(observed["worker_state_file"]).resolve()
            if observed.get("bytecode_disabled") is not True:
                raise CheckFailure("runtime_bytecode_not_disabled")
            executable = observed["sys_executable"]
            version = observed["python_version"]
            if (not isinstance(executable, str) or not Path(executable).is_absolute()
                    or not isinstance(version, list) or len(version) != 3
                    or not all(type(part) is int and part >= 0 for part in version)):
                raise CheckFailure("runtime_probe_invalid")
        except (TypeError, KeyError, ValueError):
            raise CheckFailure("runtime_probe_invalid")
        if worker != (package / "worker_state.py").resolve():
            raise CheckFailure("runtime_worker_outside_package")
        source_hash, source_files = package_digest(package_source / "wechat_bridge")
        observed_hash, observed_files = package_digest(package)
        report["runtime_probe"] = {
            "kind": "fresh_isolated_import_with_network_and_bytecode_disabled",
            "requested_python_executable": interpreter,
            "sys_executable": executable,
            "python_version": ".".join(str(part) for part in version),
            "resolved_package_directory": str(package),
            "source_binding": report["source_bindings"]["runtime_package"],
            "fingerprint_scope": "all_python_sources_in_package",
            "source_python_sources_sha256": source_hash,
            "observed_python_sources_sha256": observed_hash,
            "source_python_file_count": len(source_files),
            "observed_python_file_count": len(observed_files),
            "source_worker_state_sha256": source_files["worker_state.py"],
            "observed_worker_state_sha256": observed_files["worker_state.py"],
        }
        check("runtime_package_python_sources", source_files == observed_files)
        check("runtime_worker_state", source_files["worker_state.py"] == observed_files["worker_state.py"])
    except CheckFailure as error:
        check("runtime_probe", False, error.code)
    except subprocess.TimeoutExpired:
        check("runtime_probe", False, "runtime_probe_timeout")
    except subprocess.CalledProcessError:
        # Never include interpreter output: imports/errors may disclose secrets.
        check("runtime_probe", False, "runtime_probe_failed")
    except (OSError, TypeError, ValueError):
        check("runtime_probe", False, "runtime_probe_unavailable")

    report["ok"] = bool(report["checks"]) and all(entry["ok"] for entry in report["checks"])
    return report


def main(argv=None):
    parser = JsonArgumentParser(description=__doc__)
    for name in ("source", "hook-script", "adapter", "hook-json", "python", "expected-head"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--package-source", help="separate clean repository for package Python sources")
    parser.add_argument("--package-expected-head", help="full SHA of --package-source; both options are required together")
    parser.add_argument("--preserve-hook-json", help="pre-update actual single-Hook export; compare id and delivery without reporting targets")
    try:
        report = inspect(parser.parse_args(argv))
    except CheckFailure as error:
        report = {"schema_version": 1, "ok": False,
                  "checks": [{"name": "arguments", "ok": False, "failure_code": error.code}]}
    except Exception:
        # Paths, private export fields and subprocess output stay out of errors.
        report = {"schema_version": 1, "ok": False,
                  "checks": [{"name": "inspection", "ok": False, "failure_code": "inspection_failed"}]}
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
