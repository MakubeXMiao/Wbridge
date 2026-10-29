"""Exercise the adapter with a real pinned CLI, empty HOME and denied network.

Requires macOS sandbox-exec. This proves only handshake/policy/no-auth behavior,
not generation, tool approvals, resumed conversation or WeChat acceptance.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from wechat_bridge.backends.claude_code import ClaudeCodeConfig, ClaudeCodeExecutor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "darwin" or not Path("/usr/bin/sandbox-exec").is_file():
        parser.error("Requires macOS sandbox-exec; no unguarded fallback")
    binary, output = args.binary.resolve(strict=True), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for name in ("home", "cwd", "tmp"):
        (output / name).mkdir()
    profile = (
        '(version 1)(allow default)(deny network*)(deny mach-lookup)'
        '(deny file-read* (require-all (subpath %s) '
        '(require-not (subpath %s))(require-not (subpath %s))))'
        '(deny file-write* (require-all (require-not (subpath %s)) '
        '(require-not (literal "/dev/null"))))'
    ) % tuple(json.dumps(p) for p in (str(Path.home()), str(binary.parent), str(output), str(output)))
    sandbox = output / "sandbox.sb"
    sandbox.write_text(profile + "\n")
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(output / "home"),
           "TMPDIR": str(output / "tmp"), "CLAUDE_CODE_TMPDIR": str(output / "tmp/claude"),
           "CLAUDE_CONFIG_DIR": str(output / "home/config"), "LANG": "en_US.UTF-8",
           "DISABLE_AUTOUPDATER": "1", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"}
    guard = subprocess.run(
        ["/usr/bin/sandbox-exec", "-f", str(sandbox), "/usr/bin/python3", "-c",
         "import socket,errno,json,sys\ntry:\n socket.socket().connect(('127.0.0.1',9))\n"
         "except OSError as e:\n print(json.dumps({'guard':'network','errno':e.errno}));sys.exit(41 if e.errno==errno.EPERM else 42)"],
        env=env, cwd=output / "cwd", stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
    if guard.returncode != 41 or json.loads(guard.stdout) != {"guard": "network", "errno": 1}:
        raise RuntimeError("Network denial not verified")
    wrapper = output / "claude-sandbox"
    wrapper.write_text("#!/bin/sh\nexec /usr/bin/sandbox-exec -f %s %s \"$@\"\n" % (shlex.quote(str(sandbox)), shlex.quote(str(binary))))
    wrapper.chmod(0o700)
    root = Path(__file__).resolve().parents[3]
    import wechat_bridge.backends.claude_code as adapter
    source = Path(adapter.__file__).resolve()
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"], text=True).splitlines()
    run = ClaudeCodeExecutor(ClaudeCodeConfig(
        workdir=str(output / "cwd"), executable=str(wrapper), env=env,
        timeout_seconds=25, startup_timeout_seconds=12,
    )).start("SYNTHETIC_NO_AUTH_PROBE_ONLY", "probe-execution", "probe-attempt")
    outcome = run.wait(35)
    events = run.poll()
    record = {"scope": "real CLI, network-denied, unauthenticated adapter probe only",
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "source_head": head, "dirty": dirty, "loaded_module": str(source),
              "adapter_sha256": source_hash,
              "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "python": sys.executable, "python_version": platform.python_version(),
              "network_control": "EPERM/exit41 verified", "outcome": outcome,
              "event_types": [e["type"] for e in events]}
    if source_hash != hashlib.sha256(source.read_bytes()).hexdigest():
        raise RuntimeError("Adapter source changed during probe")
    (output / "result.json").write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(record, ensure_ascii=False))
    return 0 if outcome["code"] == "not_authenticated" else 1


if __name__ == "__main__":
    raise SystemExit(main())
