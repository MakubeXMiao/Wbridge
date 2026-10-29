#!/usr/bin/env bash
# Checkout-owned offline contract tests. Fake runtime is not Muse acceptance.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export HOOK_SCRIPT="${HOOK_SCRIPT:-$ROOT/hooks/wechat-inbox-watch.sh}"
export HOOK_RUNTIME="${HOOK_RUNTIME:-$ROOT/tests/fixtures/hook_runtime.sh}"
PYTHON="${PYTHON:-python3}"
if "$PYTHON" "$ROOT/tests/test_worker_state.py" \
  WorkerStateTests.test_claim_is_unique_under_concurrent_workers \
  WorkerStateTests.test_hook_and_worker_use_atomic_same_claim \
  WorkerStateTests.test_seen_prune_and_complete_share_lock \
  WorkerStateTests.test_old_heartbeat_does_not_kill_new_process \
  WorkerStateTests.test_process_identity_checks_command_position_not_substrings \
  WorkerStateTests.test_sent_prune_preserves_failed_and_completion_receipts \
  HookScriptTests.test_default_script_is_checkout_not_home_install
then
  echo "== test_hook_audit2fix: 7 passed, 0 failed =="
else
  status=$?
  echo "== test_hook_audit2fix: failed (exit $status) =="
  exit "$status"
fi
