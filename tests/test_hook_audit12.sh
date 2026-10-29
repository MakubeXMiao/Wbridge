#!/usr/bin/env bash
# Checkout-owned offline contract tests. Fake runtime is not Muse acceptance.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export HOOK_SCRIPT="${HOOK_SCRIPT:-$ROOT/hooks/wechat-inbox-watch.sh}"
export HOOK_RUNTIME="${HOOK_RUNTIME:-$ROOT/tests/fixtures/hook_runtime.sh}"
PYTHON="${PYTHON:-python3}"
if "$PYTHON" "$ROOT/tests/test_worker_state.py" \
  WorkerStateTests.test_two_hook_plans_only_emit_one_new_lease \
  WorkerStateTests.test_supplement_gets_started_marker_and_same_owner \
  WorkerStateTests.test_failed_notice_retries_without_false_confirmation \
  WorkerStateTests.test_failed_cover_never_marks_unsent_message_complete \
  WorkerStateTests.test_completed_receipt_blocks_replayed_inbox_after_seen_trim \
  WorkerStateTests.test_legacy_seen_residue_gets_sender_bound_receipt_before_prune \
  HookScriptTests.test_runtime_failure_is_recorded_but_unconfirmed
then
  echo "== test_hook_audit12: 7 passed, 0 failed =="
else
  status=$?
  echo "== test_hook_audit12: failed (exit $status) =="
  exit "$status"
fi
