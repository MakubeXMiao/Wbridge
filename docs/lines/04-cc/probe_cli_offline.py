"""Evidence probe only: no account, model calls, or wbridge adapter implementation.

Requires macOS sandbox-exec. Uses an explicitly supplied official CLI binary,
an empty HOME/cwd, an allowlisted environment, and OS-level network denial.
Do not use this probe to validate authenticated generation or tool approvals.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    if sys.platform != "darwin" or not Path("/usr/bin/sandbox-exec").is_file():
        parser.error("macOS sandbox-exec is required; no unguarded fallback")
    output.mkdir(parents=True, exist_ok=False)
    user_home = str(Path.home())
    home = output / "home"
    cwd = output / "cwd"
    home.mkdir()
    cwd.mkdir()
    # Read only the downloaded evidence within the actual user's home. Writes
    # stay under output. No network connection is permitted, including localhost.
    readable = str(binary.parent)
    profile = (
        '(version 1)(allow default)(deny network*)(deny mach-lookup)'
        '(deny file-read* (require-all (subpath %s) '
        '(require-not (subpath %s))(require-not (subpath %s))))'
        '(deny file-write* (require-all (require-not (subpath %s)) '
        '(require-not (literal "/dev/null"))))'
    ) % tuple(json.dumps(p) for p in (user_home, readable, str(output), str(output)))
    (output / "sandbox.sb").write_text(profile + "\n")
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": str(home),
        "TMPDIR": str(output / "tmp"),
        "CLAUDE_CODE_TMPDIR": str(output / "claude-tmp"),
        "CLAUDE_CONFIG_DIR": str(home / "config"),
        "XDG_CONFIG_HOME": str(home / "xdg-config"),
        "XDG_CACHE_HOME": str(home / "xdg-cache"),
        "DISABLE_AUTOUPDATER": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "TERM": "dumb",
        "LANG": "en_US.UTF-8",
    }
    Path(env["TMPDIR"]).mkdir()
    prefix = ["/usr/bin/sandbox-exec", "-p", profile]
    guard = subprocess.run(
        prefix + ["/usr/bin/python3", "-c",
                  "import errno,json,socket,sys\n"
                  "try:\n socket.socket().connect(('127.0.0.1', 9))\n"
                  "except OSError as e:\n print(json.dumps({'control':'socket-denial','errno':e.errno})); sys.exit(41 if e.errno==errno.EPERM else 42)\n"
                  "sys.exit(0)"],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=10,
    )
    (output / "network-guard.stderr").write_text(guard.stderr)
    (output / "network-guard.stdout").write_text(guard.stdout)
    try:
        control = json.loads(guard.stdout)
    except (ValueError, TypeError):
        control = {}
    if guard.returncode != 41 or control != {"control": "socket-denial", "errno": 1}:
        raise RuntimeError("network-denial control did not establish EPERM")
    base = ["--bare", "-p", "SYNTHETIC_PROBE_ONLY", "--tools", "",
            "--permission-mode", "dontAsk", "--permission-prompts", "none",
            "--setting-sources", "", "--no-session-persistence"]
    cases = [
        ("version", ["--bare", "--version"], None),
        ("help", ["--bare", "--help"], None),
        ("invalid_flag", ["--bare", "--wbridge-synthetic-invalid-flag"], None),
        ("missing_auth_json", base + ["--output-format", "json"], None),
        ("missing_auth_stream", base + ["--output-format", "stream-json", "--verbose"], None),
        ("invalid_schema", base + ["--output-format", "json", "--json-schema", "not-json"], None),
        ("missing_resume", base + ["--resume", "00000000-0000-4000-8000-000000000004",
                                   "--output-format", "json"], None),
        ("idle_sigterm", ["--bare", "-p", "--input-format", "stream-json",
                          "--output-format", "stream-json", "--verbose",
                          "--tools", "", "--no-session-persistence"], signal.SIGTERM),
    ]
    records = []
    script = Path(__file__).read_bytes()
    (output / "probe-script.py").write_bytes(script)
    summary = {"scope": "credential-free offline CLI probes; no model/tool/session acceptance",
               "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
               "script_sha256": hashlib.sha256(script).hexdigest(),
               "network_guard": "EPERM; unique JSON control and exit code verified",
               "env_credentials_cleared": True,
               "mach_lookup": "denied by sandbox profile",
               "keychain_access": "not exercised; not an authentication acceptance test",
               "results": records}
    for name, flags, sent_signal in cases:
        started = time.monotonic()
        proc = subprocess.Popen(prefix + [str(binary)] + flags, cwd=cwd, env=env,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, start_new_session=True)
        timed_out = False
        signal_sent = False
        stdout = stderr = ""
        failure = None
        try:
            if sent_signal:
                time.sleep(2)
                if proc.poll() is None:
                    try:
                        os.killpg(proc.pid, sent_signal)
                        signal_sent = True
                    except ProcessLookupError:
                        pass
            stdout, stderr = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                stdout, stderr = proc.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                stdout, stderr = proc.communicate(timeout=3)
        except BaseException as error:
            failure = error
        finally:
            # Also clean up this probe's process group on interruption/errors.
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    proc.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.communicate(timeout=3)
        (output / (name + ".stdout")).write_text(stdout)
        (output / (name + ".stderr")).write_text(stderr)
        record = {"case": name, "argv": flags, "exit": proc.returncode,
                  "seconds": round(time.monotonic() - started, 3),
                  "timeout": timed_out, "signal_sent": signal_sent,
                  "probe_error": type(failure).__name__ if failure else None}
        records.append(record)
        (output / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(record), flush=True)
        if failure:
            raise failure


if __name__ == "__main__":
    main()
