#!/usr/bin/env bash
# hook 层离线测试（2026-10-22 超时重唤醒兜底）：认领满 600 秒未处理 →
# 重唤醒（payload 以 rewoken_files 单列）；节流 600 秒/次、最多 3 次；
# 用尽后恰好一次 wechat_stuck_message 告警；dry run 不改任何状态。
# 用假 HOME + 假 serve 进程跑真实的 ~/hooks/scripts/wechat-inbox-watch.sh，
# 只读真实运行时 ~/hooks/runtime/hook_runtime.sh，不碰线上数据。
# 运行：bash tests/test_hook_rewake.sh
set -u

REAL_HOME="$HOME"
SCRIPT="$REAL_HOME/hooks/scripts/wechat-inbox-watch.sh"
RUNTIME="$REAL_HOME/hooks/runtime/hook_runtime.sh"
FAKE="$(mktemp -d /tmp/wbrewake-XXXXXX)"
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
run_hook_dry() {
  HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" HATCH_HOOK_DRY_RUN=1 bash "$SCRIPT" 2>/dev/null
}

check() {  # check <name> <0|1>
  if [ "$2" -eq 0 ]; then PASS=$((PASS+1)); echo "PASS $1"; else FAIL=$((FAIL+1)); echo "FAIL $1"; fi
}

now() { date +%s; }
CLAIM="$FAKE/hooks/state/wechat_inbox_claimed.txt"
SEENF="$FAKE/hooks/state/wechat_inbox_seen.txt"
REWAKE="$FAKE/hooks/state/wechat_inbox_rewake.txt"
INBOX="$FAKE/workspace/wechat-bridge/inbox"

# R1：已认领 700 秒、未 seen → 重唤醒，payload 单列 rewoken_files，状态计数=1
echo '{}' > "$INBOX/4001.json"
echo "4001 $(( $(now) - 700 )) $(( $(now) - 700 ))" > "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"new_wechat_message"'; check "rewake R1 超时认领被重唤醒" $?
echo "$out" | grep -q '"rewoken_files":\[".*/4001.json"\]'; check "rewake R1 payload 单列 rewoken_files" $?
echo "$out" | grep -q '"files":\[\]'; check "rewake R1 files 为空（不是新消息）" $?
grep -q "^4001 1 " "$REWAKE"; check "rewake R1 状态计数记为 1" $?

# R2：节流——紧接着再跑一轮（距上次重唤醒 <600 秒）→ 不再重唤醒
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R2 节流内不重复重唤醒" $?
grep -q "^4001 1 " "$REWAKE"; check "rewake R2 计数仍为 1" $?

# R3：新鲜认领（300 秒）→ 不重唤醒、不建状态条目
echo '{}' > "$INBOX/4002.json"
echo "4002 $(( $(now) - 300 )) $(( $(now) - 300 ))" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R3 新鲜认领不重唤醒" $?
! grep -q "^4002 " "$REWAKE"; check "rewake R3 无状态条目" $?

# R4：已 seen 的消息即使认领超时也不重唤醒
echo '{}' > "$INBOX/4003.json"
echo "4003 $(( $(now) - 5000 )) $(( $(now) - 5000 ))" >> "$CLAIM"
echo "4003" >> "$SEENF"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R4 seen 消息不重唤醒" $?
! grep -q "^4003 " "$REWAKE"; check "rewake R4 seen 无状态条目" $?

# R5：重唤醒用尽（3 次）+ 距上次 >600 秒 → 恰好一次 wechat_stuck_message 告警
echo '{}' > "$INBOX/4004.json"
echo "4004 $(( $(now) - 5000 )) $(( $(now) - 5000 ))" >> "$CLAIM"
echo "4004 3 $(( $(now) - 700 )) 0" >> "$REWAKE"
out="$(run_hook)"
echo "$out" | grep -q '"reason":"wechat_stuck_message"'; check "rewake R5 用尽后触发卡死告警" $?
echo "$out" | grep -q '"stuck_files":\["4004.json"\]'; check "rewake R5 告警 payload 带文件名" $?
grep -q "^4004 3 .* 1$" "$REWAKE"; check "rewake R5 已告警标记落盘" $?
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R5 告警只报一次" $?

# R6：计数未满但节流未到（2 次、上次 60 秒前）→ 不重唤醒也不告警
echo '{}' > "$INBOX/4005.json"
echo "4005 $(( $(now) - 2000 )) $(( $(now) - 2000 ))" >> "$CLAIM"
echo "4005 2 $(( $(now) - 60 )) 0" >> "$REWAKE"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R6 节流期静默" $?
grep -q "^4005 2 " "$REWAKE"; check "rewake R6 计数未变" $?

# R7：状态清理——条目对应的 inbox 文件已消失 → 下一轮被清掉
echo "4999 1 $(( $(now) - 700 )) 0" >> "$REWAKE"
run_hook > /dev/null
! grep -q "^4999 " "$REWAKE"; check "rewake R7 孤儿条目被清理" $?

# R8：dry run——报告将重唤醒什么，但不改重唤醒状态、不动认领
echo '{}' > "$INBOX/4006.json"
echo "4006 $(( $(now) - 700 )) $(( $(now) - 700 ))" >> "$CLAIM"
claim_before="$(cat "$CLAIM")"
out="$(run_hook_dry)"
echo "$out" | grep -q '"rewoken_files":\[".*/4006.json"\]'; check "rewake R8 dry run 报告将重唤醒" $?
! grep -q "^4006 " "$REWAKE"; check "rewake R8 dry run 不写状态" $?
[ "$(cat "$CLAIM")" = "$claim_before" ]; check "rewake R8 dry run 不动认领文件" $?

echo "== test_hook_rewake: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ]
