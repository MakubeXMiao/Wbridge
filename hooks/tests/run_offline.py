"""Run Muse-only suites under the repository's socket-denying test guard."""
import argparse
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="muse-offline-results-"))
    output.mkdir(parents=True, exist_ok=True)
    guard = runpy.run_path(str(ROOT / "tests/run_offline.py"))["GUARD"]
    results = []
    for suite in sorted((ROOT / "hooks/tests").glob("test_*.py")):
        with tempfile.TemporaryDirectory(prefix="synthetic-muse-state-") as state:
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
            try:
                proc = subprocess.run([sys.executable, "-I", "-B", "-c", guard,
                                       str(ROOT), str(suite), state], cwd=ROOT,
                                      env=env, text=True, capture_output=True, timeout=120)
                log, code = proc.stdout + proc.stderr, proc.returncode
            except subprocess.TimeoutExpired:
                log, code = "suite timeout", "TIMEOUT"
        (output / (suite.name + ".log")).write_text(log)
        result = dict(suite=suite.name, exit=code, status="PASS" if code == 0 else "FAIL")
        results.append(result)
        print(json.dumps(result), flush=True)
        if code != 0:
            print(log[-5000:], flush=True)
    (output / "results.json").write_text(json.dumps(dict(
        scope="offline synthetic only; no platform/phone acceptance",
        network="real Python sockets blocked by shared guard", results=results), indent=2) + "\n")
    return 0 if results and all(row["exit"] == 0 for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
