#!/usr/bin/env bash
# hook 层离线测试（二次全盘审查修复）：P2-1 脚本级整程锁 + 认领维护
# 锁内读-改-写；P1-1 心跳过期杀进程重拉；P3-1 sent/ 30 天清理；
# P3-2 seen.txt 上限裁剪。
# 用假 HOME + 假 serve 进程跑真实的 ~/hooks/scripts/wechat-inbox-watch.sh，
# 只读真实运行时 ~/hooks/runtime/hook_runtime.sh，不碰线上数据。
# 运行：bash tests/test_hook_audit2fix.sh
set -u

REAL_HOME="$HOME"
SCRIPT="$REAL_HOME/hooks/scripts/wechat-inbox-watch.sh"
RUNTIME="$REAL_HOME/hooks/runtime/hook_runtime.sh"
FAKE="$(mktemp -d /tmp/wbhook2-XXXXXX)"
PASS=0
FAIL=0

cleanup() {
  pkill -f "$FAKE" 2>/dev/null
  [ -n "${SERVE_PID:-}" ] && kill "$SERVE_PID" 2>/dev/null
  rm -rf "$FAKE"
}
trap cleanup EXIT

BRIDGE="$FAKE/workspace/wechat-bridge"
STATE_DIR="$FAKE/hooks/state"
mkdir -p "$BRIDGE/state" "$BRIDGE/inbox" "$BRIDGE/outbox/failed" \
         "$BRIDGE/outbox/pending" "$BRIDGE/outbox/sending" \
         "$BRIDGE/outbox/sent" "$STATE_DIR" \
         "$FAKE/workspace/wechat-bridge-venv/bin"
echo '{}' > "$BRIDGE/state/session.json"
touch "$STATE_DIR/wechat_inbox_seen.txt" "$STATE_DIR/wechat_inbox_claimed.txt"
CLAIM="$STATE_DIR/wechat_inbox_claimed.txt"
CLAIM_LOCK="$STATE_DIR/wechat_inbox_claimed.lock"
SEENF="$STATE_DIR/wechat_inbox_seen.txt"
INBOX="$BRIDGE/inbox"

# 假 serve 二进制（供 hook 拉起路径调用）：写 pid + 持续写心跳 + 长睡，
# argv 伪装成 wechat-bridge 过 pid_is_serve 的 cmdline 校验。
cat > "$FAKE/workspace/wechat-bridge-venv/bin/wechat-bridge" <<'EOS'
#!/usr/bin/env bash
echo $$ > "$WECHAT_BRIDGE_DIR/state/serve.pid"
( for i in $(seq 1 120); do date +%s > "$WECHAT_BRIDGE_DIR/state/flusher_heartbeat"; sleep 1; done ) &
exec -a wechat-bridge sleep 300
EOS
chmod +x "$FAKE/workspace/wechat-bridge-venv/bin/wechat-bridge"

# 初始假 serve：普通长睡进程（不写心跳；进程龄 <180s 时缺心跳不判死）
setsid bash -c 'exec -a wechat-bridge sleep 300' &
SERVE_PID=$!
echo "$SERVE_PID" > "$BRIDGE/state/serve.pid"
sleep 0.3

run_hook() {
  HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" bash "$SCRIPT" 2>/dev/null
}

check() {  # check <name> <0|1>
  if [ "$2" -eq 0 ]; then PASS=$((PASS+1)); echo "PASS $1"; else FAIL=$((FAIL+1)); echo "FAIL $1"; fi
}

now() { date +%s; }

# ---------- H1（P2-1）：两轮并发执行 → 只有一轮唤醒，认领只登记一次 ----------
echo '{}' > "$INBOX/3001.json"
out1="$FAKE/out1.txt"; out2="$FAKE/out2.txt"
HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" bash "$SCRIPT" > "$out1" 2>/dev/null &
p1=$!
HATCH_HOOK_RUNTIME="$RUNTIME" HOME="$FAKE" bash "$SCRIPT" > "$out2" 2>/dev/null &
p2=$!
wait "$p1" "$p2"
wakes=$(( $(grep -c '"reason":"new_wechat_message"' "$out1") + $(grep -c '"reason":"new_wechat_message"' "$out2") ))
[ "$wakes" -eq 1 ]; check "hook H1 并发两轮只有一轮唤醒" $?
grep -q "another hook instance" "$out1" "$out2"; check "hook H1 另一轮静默跳过" $?
[ "$(grep -c '^3001 ' "$CLAIM")" -eq 1 ]; check "hook H1 认领只登记一次" $?
rm -f "$INBOX/3001.json"

# ---------- H2（P2-1）：worker 锁内补记与认领维护并发 → 补记行不丢 ----------
# 800 条已认领（inbox 文件都在、未 seen），维护会逐行读-改-写；
# 期间 worker 按 prompt 约定带锁追加一条新认领。最终两边都必须在。
for i in $(seq 1 800); do
  id=$((100000 + i))
  echo '{}' > "$INBOX/$id.json"
  echo "$id $(now) $(now)" >> "$CLAIM"
done
echo '{}' > "$INBOX/999999.json"
( sleep 0.05
  exec 9>>"$CLAIM_LOCK"
  flock -x 9
  echo "999999 $(now)" >> "$CLAIM"
  flock -u 9
  exec 9>&- ) &
appender=$!
run_hook > /dev/null
wait "$appender"
grep -q '^999999 ' "$CLAIM"; check "hook H2 维护期间 worker 补记未被冲掉" $?
[ "$(wc -l < "$CLAIM")" -ge 800 ]; check "hook H2 既有认领行完整保留" $?
# 清理 H2 现场
rm -f "$INBOX"/*.json
: > "$CLAIM"

# ---------- H3（P3-2）：seen 超 5000 行 → 裁到最近 2000 行 + 节流戳 ----------
seq 1 5001 | sed 's/^/s/' > "$SEENF"
rm -f "$STATE_DIR/wechat_sent_prune.stamp"
run_hook > /dev/null
[ "$(wc -l < "$SEENF")" -eq 2000 ]; check "hook H3 seen 裁到 2000 行" $?
[ "$(head -1 "$SEENF")" = "s3002" ] && [ "$(tail -1 "$SEENF")" = "s5001" ]
check "hook H3 seen 保留的是最近 2000 行" $?
[ -f "$STATE_DIR/wechat_sent_prune.stamp" ]; check "hook H3 节流戳已写" $?

# ---------- H4（P3-1）：sent/ 30 天清理；failed/ 不动 ----------
echo '{}' > "$BRIDGE/outbox/sent/old.json"
touch -d '31 days ago' "$BRIDGE/outbox/sent/old.json"
echo '{}' > "$BRIDGE/outbox/sent/new.json"
echo '{}' > "$BRIDGE/outbox/failed/dead.json"
touch -d '31 days ago' "$BRIDGE/outbox/failed/dead.json"
rm -f "$STATE_DIR/wechat_sent_prune.stamp"
run_hook > /dev/null
[ ! -e "$BRIDGE/outbox/sent/old.json" ]; check "hook H4 31 天前的 sent 已清" $?
[ -e "$BRIDGE/outbox/sent/new.json" ]; check "hook H4 新 sent 保留" $?
[ -e "$BRIDGE/outbox/failed/dead.json" ]; check "hook H4 failed 永不动" $?

# ---------- H5（P1-1）：心跳过期 → 杀旧 serve 并重新拉起 ----------
echo $(( $(now) - 1000 )) > "$BRIDGE/state/flusher_heartbeat"
out="$(run_hook)"
old_alive=1; kill -0 "$SERVE_PID" 2>/dev/null || old_alive=0
[ "$old_alive" -eq 0 ]; check "hook H5 心跳过期旧 serve 被杀" $?
newpid="$(cat "$BRIDGE/state/serve.pid")"
[ -n "$newpid" ] && [ "$newpid" != "$SERVE_PID" ] && kill -0 "$newpid" 2>/dev/null
check "hook H5 新 serve 已被拉起" $?
echo "$out" | grep -q '"decision":"silent"'; check "hook H5 拉起成功不打扰（静默）" $?
hb_age=$(( $(now) - $(cat "$BRIDGE/state/flusher_heartbeat") ))
[ "$hb_age" -lt 120 ]; check "hook H5 新 serve 心跳已恢复新鲜" $?
SERVE_PID="$newpid"

echo "== test_hook_audit2fix: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ]
