#!/usr/bin/env bash
# hook 层离线测试（全盘审查第 2 / 9 / 10 项）：认领续租语义、死信告警、宕机标记清除。
# 用假 HOME + 假 serve 进程跑真实的 ~/hooks/scripts/wechat-inbox-watch.sh，
# 只读真实运行时 ~/hooks/runtime/hook_runtime.sh，不碰线上数据。
# 运行：bash tests/test_hook_audit12.sh
set -u

REAL_HOME="$HOME"
SCRIPT="$REAL_HOME/hooks/scripts/wechat-inbox-watch.sh"
RUNTIME="$REAL_HOME/hooks/runtime/hook_runtime.sh"
FAKE="$(mktemp -d /tmp/wbhook-XXXXXX)"
PASS=0
FAIL=0

cleanup() {
  [ -n "${SERVE_PID:-}" ] && kill "$SERVE_PID" 2>/dev/null
  rm -rf "$FAKE"
}
trap cleanup EXIT

mkdir -p "$FAKE/workspace/wechat-bridge/state" \
         "$FAKE/workspace/wechat-bridge/inbox" \
         "$FAKE/workspace/wechat-bridge/outbox/failed" \
         "$FAKE/workspace/wechat-bridge/outbox/pending" \
         "$FAKE/workspace/wechat-bridge/outbox/sending" \
         "$FAKE/workspace/wechat-bridge/outbox/sent" \
         "$FAKE/hooks/state"
echo '{}' > "$FAKE/workspace/wechat-bridge/state/session.json"
touch "$FAKE/hooks/state/wechat_inbox_seen.txt" "$FAKE/hooks/state/wechat_inbox_claimed.txt"

# 假 serve：argv[0] 伪装成 wechat-bridge 过 pid_is_serve 的 cmdline 校验
setsid bash -c 'exec -a wechat-bridge sleep 120' &
SERVE_PID=$!
echo "$SERVE_PID" > "$FAKE/workspace/wechat-bridge/state/serve.pid"
sleep 0.3

run_hook() {
  HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" bash "$SCRIPT" 2>/dev/null
}

check() {  # check <name> <0|1>
  if [ "$2" -eq 0 ]; then PASS=$((PASS+1)); echo "PASS $1"; else FAIL=$((FAIL+1)); echo "FAIL $1"; fi
}

now() { date +%s; }
CLAIM="$FAKE/hooks/state/wechat_inbox_claimed.txt"
SEENF="$FAKE/hooks/state/wechat_inbox_seen.txt"
INBOX="$FAKE/workspace/wechat-bridge/inbox"
FAILED_DIR="$FAKE/workspace/wechat-bridge/outbox/failed"

# S1：新消息未认领 → 唤醒并登记认领
echo '{}' > "$INBOX/1001.json"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"new_wechat_message"'; check "hook S1 新消息唤醒" $?
grep -q "^1001 " "$CLAIM"; check "hook S1 认领已登记" $?

# S2：第 2 项核心——认领 1000 秒前（旧语义 900 秒已过期）但文件还在、未 seen
# → 应续租静默，而不是重复唤醒第二个 worker
echo "1001 $(( $(now) - 1000 ))" > "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "hook S2 过期租约被续租不重唤" $?
ts="$(awk '$1==1001 {print $NF}' "$CLAIM" | tail -1)"
[ -n "$ts" ] && [ $(( $(now) - ts )) -lt 60 ]; check "hook S2 认领时间戳已刷新" $?

# S3：认领超过兜底 6 小时且文件还在 → worker 假死，重新唤醒
echo "1001 $(( $(now) - 21601 ))" > "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"new_wechat_message"'; check "hook S3 兜底过期重新唤醒" $?

# S4：已 seen 的消息永不唤醒（即使认领过期）
echo "1001" >> "$SEENF"
echo "1001 $(( $(now) - 21601 ))" > "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "hook S4 seen 压制唤醒" $?

# S5：worker 按 prompt 约定自己补记的认领行（同格式）要被容忍，不重复唤醒
echo '{}' > "$INBOX/1002.json"
echo "1002 $(now)" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "hook S5 worker 补记认领被容忍" $?

# S6：第 9 项——failed/ 出现新死信 → 唤醒一次；再跑不重复报
echo '{"failed_reason":"x"}' > "$FAILED_DIR/2001.json"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"wechat_dead_letter"'; check "hook S6 死信唤醒" $?
echo "$out" | grep -q '2001.json'; check "hook S6 死信 payload 带文件名" $?
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "hook S6 死信只报一次" $?

# S7：死信被人工移走后又同名再进 failed/ → 应重新报告
rm "$FAILED_DIR/2001.json"
run_hook > /dev/null
echo '{"failed_reason":"y"}' > "$FAILED_DIR/2001.json"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"wechat_dead_letter"'; check "hook S7 死信复现重新报告" $?

# S8：第 10 项——serve 活着时 DOWN_NOTIFIED 必须被清掉（旧实现永久残留）
touch "$FAKE/hooks/state/wechat_serve_down_notified"
run_hook > /dev/null
[ ! -f "$FAKE/hooks/state/wechat_serve_down_notified" ]; check "hook S8 serve 活着清除宕机标记" $?

echo "== test_hook_audit12: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ]
