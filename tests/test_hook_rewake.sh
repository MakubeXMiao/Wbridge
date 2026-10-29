#!/usr/bin/env bash
# Checkout-owned offline contract tests. Fake runtime is not Muse acceptance.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export HOOK_SCRIPT="${HOOK_SCRIPT:-$ROOT/hooks/wechat-inbox-watch.sh}"
export HOOK_RUNTIME="${HOOK_RUNTIME:-$ROOT/tests/fixtures/hook_runtime.sh}"
PYTHON="${PYTHON:-python3}"
if "$PYTHON" "$ROOT/tests/test_worker_state.py" \
  WorkerStateTests.test_rewake_only_adopt_and_old_lease_rejected \
  WorkerStateTests.test_started_worker_expires_after_90_seconds \
  WorkerStateTests.test_three_rewakes_then_stuck_notice \
  WorkerStateTests.test_six_hour_fallback_releases_only_old_lease \
  WorkerStateTests.test_deadletter_does_not_consume_new_message_lease \
  HookScriptTests.test_dry_run_creates_no_paths_or_changes_mtime
then
  echo "== test_hook_rewake: 6 passed, 0 failed =="
else
  status=$?
  echo "== test_hook_rewake: failed (exit $status) =="
  exit "$status"
fi
