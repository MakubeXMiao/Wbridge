#!/usr/bin/env bash
# hook 层离线测试（2026-10-22 超时重唤醒兜底 + 同日调参：阈值拆分 +
# 已开工标记）：无标记的已认领消息满 180 秒未处理 → 快速重唤醒
# （rule no-marker-180）；有已开工标记则标记未满 900 秒绝不重唤醒、
# 满 900 秒才按 worker 中途死亡重唤醒（rule started-stale-900）；
# 两次重唤醒间隔 ≥600 秒、合计最多 3 次；用尽后恰好一次
# wechat_stuck_message 告警；dry run 不改任何状态（含标记目录）。
# 用假 HOME + 假 serve 进程跑真实的 ~/hooks/scripts/wechat-inbox-watch.sh，
# 只读真实运行时 ~/hooks/runtime/hook_runtime.sh，不碰线上数据。
# 运行：bash tests/test_hook_rewake.sh
#
# 变更注记（调参适配）：旧 R3「新鲜认领 300 秒不重唤醒」在新阈值下
# 不再成立（300 ≥ 180 会走快速重唤醒），改为 100 秒验证同一语义
# （低于首次阈值不触发）；300 秒档的行为由 S1（200 秒触发）覆盖。
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
  HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" bash "$SCRIPT" 2>&1
}
run_hook_dry() {
  HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" HATCH_HOOK_DRY_RUN=1 bash "$SCRIPT" 2>&1
}

check() {  # check <name> <0|1>
  if [ "$2" -eq 0 ]; then PASS=$((PASS+1)); echo "PASS $1"; else FAIL=$((FAIL+1)); echo "FAIL $1"; fi
}

now() { date +%s; }
CLAIM="$FAKE/hooks/state/wechat_inbox_claimed.txt"
SEENF="$FAKE/hooks/state/wechat_inbox_seen.txt"
REWAKE="$FAKE/hooks/state/wechat_inbox_rewake.txt"
INBOX="$FAKE/workspace/wechat-bridge/inbox"
STARTED="$FAKE/hooks/state/wechat_worker_started"
mkdir -p "$STARTED"

# R1：已认领 700 秒、无标记、未 seen → 重唤醒（新语义下走 no-marker
# 快速通道，700≥180），payload 单列 rewoken_files，状态计数=1
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

# R3（已按新阈值适配，见文件头注记）：新鲜认领（100 秒 < 180 秒）
# → 不重唤醒、不建状态条目
echo '{}' > "$INBOX/4002.json"
echo "4002 $(( $(now) - 100 )) $(( $(now) - 100 ))" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake R3 新鲜认领（100s）不重唤醒" $?
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
rm -f "$INBOX/4006.json"  # 隔离后续 S 用例：4006 已验证完，移出扫描面

# S1：无标记 + 认领满 200 秒（≥180）→ 快速重唤醒，日志规则为 no-marker-180
echo '{}' > "$INBOX/4010.json"
echo "4010 $(( $(now) - 200 )) $(( $(now) - 200 ))" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"rewoken_files":\[".*/4010.json"\]'; check "rewake S1 无标记 200s 触发快速重唤醒" $?
echo "$out" | grep -q '"rewake":"4010:no-marker-180:1"'; check "rewake S1 日志规则为 no-marker-180" $?
grep -q "^4010 1 " "$REWAKE"; check "rewake S1 状态计数记为 1" $?

# S2：无标记 + 认领仅 60 秒（<180）→ 不重唤醒
echo '{}' > "$INBOX/4011.json"
echo "4011 $(( $(now) - 60 )) $(( $(now) - 60 ))" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"decision":"silent"'; check "rewake S2 无标记 60s 不重唤醒" $?
! grep -q "^4011 " "$REWAKE"; check "rewake S2 无状态条目" $?

# S3：新鲜标记（刚写）+ 认领已 1000 秒 → 不重唤醒（worker 正在干活）
echo '{}' > "$INBOX/4012.json"
echo "4012 $(( $(now) - 1000 )) $(( $(now) - 1000 ))" >> "$CLAIM"
date +%s > "$STARTED/4012.started"
out="$(run_hook)"
! echo "$out" | grep -q '4012'; check "rewake S3 新鲜标记压制重唤醒" $?
! grep -q "^4012 " "$REWAKE"; check "rewake S3 无状态条目" $?
[ -f "$STARTED/4012.started" ]; check "rewake S3 有效标记不被孤儿清理误删" $?

# S4：陈旧标记（1000 秒前）+ 认领 2000 秒 → 按中途死亡重唤醒，规则 started-stale-900
echo '{}' > "$INBOX/4013.json"
echo "4013 $(( $(now) - 2000 )) $(( $(now) - 2000 ))" >> "$CLAIM"
date +%s > "$STARTED/4013.started"
touch -d "@$(( $(now) - 1000 ))" "$STARTED/4013.started"
out="$(run_hook)"
echo "$out" | grep -q '"rewoken_files":\[".*/4013.json"\]'; check "rewake S4 陈旧标记触发重唤醒" $?
echo "$out" | grep -q '"rewake":"4013:started-stale-900:1"'; check "rewake S4 日志规则为 started-stale-900" $?
grep -q "^4013 1 " "$REWAKE"; check "rewake S4 状态计数记为 1" $?

# S5：标记在两轮之间出现 → 压制一笔本已满足间隔的重唤醒
# （4014 已重唤醒 1 次、上次在 700 秒前，无标记时本轮必触发第 2 次）
echo '{}' > "$INBOX/4014.json"
echo "4014 $(( $(now) - 5000 )) $(( $(now) - 5000 ))" >> "$CLAIM"
echo "4014 1 $(( $(now) - 700 )) 0" >> "$REWAKE"
date +%s > "$STARTED/4014.started"
out="$(run_hook)"
! echo "$out" | grep -q '4014'; check "rewake S5 中途出现的标记压制待发重唤醒" $?
grep -q "^4014 1 " "$REWAKE"; check "rewake S5 计数未被消耗" $?

# S6：快速首唤之后的间隔仍按 600 秒：4015 首唤刚触发，把上次时间
# 改到 200 秒前再跑一轮 → 不得第二次重唤醒
echo '{}' > "$INBOX/4015.json"
echo "4015 $(( $(now) - 200 )) $(( $(now) - 200 ))" >> "$CLAIM"
out="$(run_hook)"
echo "$out" | grep -q '"rewoken_files":\[".*/4015.json"\]'; check "rewake S6 快速首唤触发" $?
sed -i "s/^4015 1 .*/4015 1 $(( $(now) - 200 )) 0/" "$REWAKE"
out="$(run_hook)"
! echo "$out" | grep -q '4015'; check "rewake S6 首唤后 200s 间隔未满不二唤" $?
grep -q "^4015 1 " "$REWAKE"; check "rewake S6 计数仍为 1" $?

# S7：孤儿标记清理——无对应 inbox 文件的标记被删（4012 的有效标记保留已在 S3 验过）
date +%s > "$STARTED/4998.started"
run_hook > /dev/null
[ ! -e "$STARTED/4998.started" ]; check "rewake S7 孤儿标记被清理" $?

# S8：seen 消息在「陈旧标记」规则下同样永不重唤醒
echo '{}' > "$INBOX/4016.json"
echo "4016 $(( $(now) - 5000 )) $(( $(now) - 5000 ))" >> "$CLAIM"
echo "4016" >> "$SEENF"
date +%s > "$STARTED/4016.started"
touch -d "@$(( $(now) - 2000 ))" "$STARTED/4016.started"
out="$(run_hook)"
! echo "$out" | grep -q '4016'; check "rewake S8 seen+陈旧标记不重唤醒" $?
! grep -q "^4016 " "$REWAKE"; check "rewake S8 无状态条目" $?

# S9：dry run 纯净性（含标记目录）——报告将重唤醒 4017，但 CLAIM /
# REWAKE 内容与标记目录分文不动，连孤儿标记也不得在 dry run 里清
echo '{}' > "$INBOX/4017.json"
echo "4017 $(( $(now) - 200 )) $(( $(now) - 200 ))" >> "$CLAIM"
date +%s > "$STARTED/4997.started"
claim_md5="$(md5sum "$CLAIM" | awk '{print $1}')"
rw_md5="$(md5sum "$REWAKE" | awk '{print $1}')"
started_list="$(ls "$STARTED")"
out="$(run_hook_dry)"
echo "$out" | grep -q '"rewoken_files":\[".*/4017.json"\]'; check "rewake S9 dry run 报告将重唤醒 4017" $?
[ "$(md5sum "$CLAIM" | awk '{print $1}')" = "$claim_md5" ]; check "rewake S9 dry run 不动认领" $?
[ "$(md5sum "$REWAKE" | awk '{print $1}')" = "$rw_md5" ]; check "rewake S9 dry run 不动重唤醒状态" $?
[ "$(ls "$STARTED")" = "$started_list" ]; check "rewake S9 dry run 不动标记目录" $?
! grep -q "^4017 " "$REWAKE"; check "rewake S9 dry run 不写 4017 状态" $?

echo "== test_hook_rewake: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ]
