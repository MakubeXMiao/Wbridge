#!/usr/bin/env bash
# Platform adapter; machine state and lease transitions live in worker_state.
# Runtime wake success is an attempt result, not a delivery acknowledgement.
set -euo pipefail
BASE="${WECHAT_BRIDGE_DIR:-$HOME/workspace/wechat-bridge}"
STATE_DIR="${WECHAT_HOOK_STATE_DIR:-$HOME/hooks/state}"
PYTHON="${WECHAT_BRIDGE_PYTHON:-$HOME/workspace/wechat-bridge-venv/bin/python}"
if [ ! -x "$PYTHON" ]; then PYTHON=python3; fi

# Observation mode must not mkdir/touch/lock/restart or change any timestamp.
if [ "${HATCH_HOOK_DRY_RUN:-0}" = 1 ]; then
  source "$HATCH_HOOK_RUNTIME"
  silent "dry run: no state mutations or serve supervision" '{}'
  exit 0
fi

# A Python parent holds the lock. The descriptor is not inherited by the shell
# or detached serve, so background processes cannot accidentally pin it forever.
if [ "${WECHAT_HOOK_LOCKED:-0}" != 1 ]; then
  exec "$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" \
    lock-run --lock "$STATE_DIR/wechat_inbox_watch.lock" -- bash "$0"
fi

source "$HATCH_HOOK_RUNTIME"

serve_alive=1
if [ -f "$BASE/state/session.json" ] && [ ! -f "$BASE/state/NEEDS_RELOGIN" ]; then
  health="$("$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" \
    supervise --executable "${WECHAT_BRIDGE_EXECUTABLE:-$HOME/workspace/wechat-bridge-venv/bin/wechat-bridge}" 2>/dev/null)" || health='{"alive":false}'
  serve_alive="$("$PYTHON" -c 'import json,sys; print(1 if json.load(sys.stdin).get("alive") else 0)' <<< "$health")"
fi
if [ "$serve_alive" = 1 ]; then
  plan="$("$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" plan)"
else
  plan="$("$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" plan --serve-down)"
fi
decision="$("$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["decision"])' <<< "$plan")"
reason="$("$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["reason"])' <<< "$plan")"
payload="$("$PYTHON" -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["payload"],ensure_ascii=False))' <<< "$plan")"
if [ "$decision" != wake ]; then
  silent "$reason" "$payload"
  exit 0
fi
# Adapter paths come from the installed Hook, never from message contents.
# A missing companion must fail visibly, not fall back to model-written outbox.
ADAPTER="${WECHAT_MUSE_ADAPTER:-$(cd -- "$(dirname -- "$0")" && pwd -P)/muse-worker.py}"
if [ "$reason" = new_wechat_message ] && [ ! -f "$ADAPTER" ]; then
  echo '{"stage":"muse_adapter_missing","must_stop":true}' >&2
  exit 1
fi
payload="$("$PYTHON" -c '
import hashlib,json,pathlib,sys
p=json.load(sys.stdin)
a=pathlib.Path(sys.argv[1]).resolve()
p["muse_adapter"]={"path":str(a),"protocol":1,
    "sha256":hashlib.sha256(a.read_bytes()).hexdigest() if a.is_file() else None}
print(json.dumps(p,ensure_ascii=False))
' "$ADAPTER" <<< "$payload")"
# These timestamps observe the wake call only. A successful return is not
# proof that a platform worker started, adopted a batch, or delivered a reply.
wake_at="$("$PYTHON" -c 'import time; print(time.time())')"
"$PYTHON" -c 'import json,sys; print(json.dumps({"stage":"wake_called","at":float(sys.argv[1]),"reason":sys.argv[2]}),file=sys.stderr)' "$wake_at" "$reason"
accepted=0
wake_exit=0
if wake "$reason" "$payload"; then accepted=1; else wake_exit=$?; fi
"$PYTHON" -c 'import json,sys,time; now=time.time(); print(json.dumps({"stage":"wake_returned","at":now,"elapsed_seconds":round(now-float(sys.argv[1]),6),"exit_code":int(sys.argv[2]),"runtime_accepted":bool(int(sys.argv[3])),"worker_started_confirmed":False}),file=sys.stderr)' "$wake_at" "$wake_exit" "$accepted"
# Keep attempts unconfirmed. Cooling retries never mean 'delivered'.
keys=()
while IFS= read -r key; do
  [ -n "$key" ] && keys+=("$key")
done < <("$PYTHON" -c 'import json,sys; print("\n".join(json.load(sys.stdin).get("notice_keys",[])))' <<< "$payload")
if [ "${#keys[@]}" -gt 0 ]; then
  if [ "$accepted" = 1 ]; then
    "$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" \
      notice-result --keys "${keys[@]}" --accepted >/dev/null
  else
    "$PYTHON" -m wechat_bridge.worker_state --base "$BASE" --state-dir "$STATE_DIR" \
      notice-result --keys "${keys[@]}" >/dev/null
  fi
fi
if [ "$accepted" != 1 ]; then exit 1; fi
