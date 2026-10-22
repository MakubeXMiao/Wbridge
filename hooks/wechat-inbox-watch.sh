#!/usr/bin/env bash
# 微信桥监听：serve 保活 + 新消息/登录失效/新死信时唤醒 agent。
# 认领语义（2026-10-21 全盘审查第 2 项改）：认领 = 「这条消息正在被处理」
# 的租约。认领行格式 "id 首次认领ts 最近续租ts"（旧的两字段行按
# 首次=续租解析）。有效性看**首次认领**时间：文件还在、未 seen、且首次
# 认领未超过 CLAIM_STALE_SECS（6 小时），就视为有 worker 在处理，hook
# 每轮只刷新续租时间戳、绝不重复唤醒——worker 慢不再被第二个 worker
# 抢活。兜底：首次认领超过 6 小时文件还在，视为原 worker 已死，允许
# 重新唤醒，避免消息永久没人管。worker 若把 wake payload 之外的消息
# 并入同一条合并回复，必须先自己往 CLAIM 里原子补记该消息（见 worker
# prompt），hook 容忍 worker 追加的行（两字段或三字段皆可）。
# 超时重唤醒（2026-10-22 事故驱动加）：认领只是租约，唤醒的 worker
# 执行本身可能丢失（2026-10-22 09:44/09:56 两条消息被认领、唤醒决定
# 已做出，但执行未落地，消息在 inbox 静默滞留到人工发现）。首次认领
# 满 REWAKE_STALE_SECS（600 秒）文件还在、未 seen，就当原唤醒已丢，
# 重新唤醒一次（payload 以 rewoken_files 单列，worker 须先查
# sent/pending 有无同名任务、防重复回复）；同一消息两次重唤醒至少
# 间隔 600 秒、最多 3 次，状态记 wechat_inbox_rewake.txt；3 次用尽
# 再过 600 秒仍没处理，唤醒一次 wechat_stuck_message 让 worker 向
# 用户报告（同一条只报一次，文件消失后条目自动清、重置）。6 小时
# 兜底排在重唤醒之后，语义不变。
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BASE="$HOME/workspace/wechat-bridge"
STATE_DIR="$HOME/hooks/state"
SEEN="$STATE_DIR/wechat_inbox_seen.txt"
CLAIM="$STATE_DIR/wechat_inbox_claimed.txt"
CLAIM_LOCK="$STATE_DIR/wechat_inbox_claimed.lock"
NOTIFIED="$STATE_DIR/wechat_relogin_notified"
DOWN_NOTIFIED="$STATE_DIR/wechat_serve_down_notified"
FAILED_NOTIFIED="$STATE_DIR/wechat_failed_notified.txt"
CLAIM_STALE_SECS=21600   # 6 小时：认领兜底过期（worker 假死时重新唤醒）
REWAKE="$STATE_DIR/wechat_inbox_rewake.txt"
REWAKE_STALE_SECS=600    # 首次认领满 10 分钟未处理：视为唤醒丢失，重唤醒
REWAKE_MAX=3             # 同一消息最多重唤醒次数，用尽后转卡死告警
mkdir -p "$STATE_DIR"
touch "$SEEN" "$CLAIM" "$FAILED_NOTIFIED"

DRY="${HATCH_HOOK_DRY_RUN:-0}"

# 二次审查 P2-1：脚本级整程锁。轮询改到 5 秒后，两轮 hook 执行可能
# 重叠（尤其 serve 拉起路径带 sleep 2），旧结构有两处 check-then-act
# 窗口：认领检查在锁外、认领维护的「读-改-写」只锁住了写。现在整个
# 脚本主体套一把独立锁（与认领锁不同文件，避免和 claim 函数的 fd 9
# 锁同文件自死锁）；拿不到锁 = 上一轮还在跑，本轮直接静默跳过，
# 5 秒后下一轮自然补上，绝不两轮并发。锁文件句柄在拉起 serve 时要
# 显式关掉（见保活段 8>&-），否则子进程继承句柄会把锁永久占住。
exec 8>>"$STATE_DIR/wechat_inbox_watch.lock"
if ! flock -n 8; then
  silent "another hook instance is running" '{}'
  exit 0
fi

# 带 flock 的认领文件修改（审查第 10 项 + 二次审查 P2-1）：认领文件
# 的一切修改都走 CLAIM_LOCK 这把锁——追加走 claim_append；维护段的
# 「读-改-写」整环持锁完成（见下方认领维护段），worker 在维护期间的
# 补记追加会等锁，不会被整体替换冲掉。（二次审查后原 claim_rewrite
# 整体替换函数已删：它的无锁读窗口正是 P2-1 要关的窗口。）

claim_append() {
  # $1 = id, $2 = ts。worker 补记认领也走这个格式约定。
  exec 9>>"$CLAIM_LOCK"
  flock -x 9
  echo "$1 $2" >> "$CLAIM"
  flock -u 9
  exec 9>&-
}

# 登录失效：只唤醒一次，直到标记被清除（重新登录后 NEEDS_RELOGIN 消失）
if [ -f "$BASE/state/NEEDS_RELOGIN" ]; then
  if [ -f "$NOTIFIED" ]; then
    silent "relogin already notified" '{}'
    exit 0
  fi
  if [ "$DRY" != "1" ]; then
    touch "$NOTIFIED"
  fi
  wake "wechat_relogin_needed" '{"reason":"NEEDS_RELOGIN flag present"}'
  exit 0
else
  if [ "$DRY" != "1" ]; then
    rm -f "$NOTIFIED"
  fi
fi

# 未登录：静默
if [ ! -f "$BASE/state/session.json" ]; then
  silent "not logged in yet" '{}'
  exit 0
fi

# serve 保活：进程不在就拉起；拉起失败才唤醒人工处理（只唤醒一次）
# P7：存活判断除 kill -0 外还要验 /proc/<pid>/cmdline 含 wechat-bridge，防 pid 复用误判
pid_is_serve() {
  local p="$1"
  [ -n "$p" ] && kill -0 "$p" 2>/dev/null || return 1
  if [ -r "/proc/$p/cmdline" ]; then
    grep -q "wechat-bridge" "/proc/$p/cmdline" 2>/dev/null || return 1
  fi
  return 0
}
serve_alive=0
if [ -f "$BASE/state/serve.pid" ]; then
  pid="$(cat "$BASE/state/serve.pid" 2>/dev/null || true)"
  if pid_is_serve "$pid"; then
    serve_alive=1
  fi
fi
if [ "$serve_alive" -eq 1 ]; then
  # 审查第 10 项：只要观测到 serve 活着就清掉宕机已通知标记。
  # 旧实现只在 hook 自己拉起成功时清，serve 被人工/其他路径恢复时标记
  # 永久留着，压制下一次真正的宕机告警。
  if [ "$DRY" != "1" ]; then
    rm -f "$DOWN_NOTIFIED"
  fi
fi
# flusher 心跳监督（二次审查 P1-1）：进程活着不等于发送线程活着。
# flusher 持续把 epoch 写进 state/flusher_heartbeat（每轮 + 每个发送
# 单元 + 长操作内部都刷新，健康时间隔远小于阈值）；>120 秒没更新 =
# 线程已死或卡死，按宕机处理：杀掉进程，让下面既有的保活段重新拉起
# （拉起后启动回收会把 sending/ 的在途任务移回 pending，不丢回复）。
# 心跳文件不存在时用进程存活时长兜底：健康 flusher 启动约 3 秒内
# 必写心跳，进程已活过 180 秒却还没有心跳文件，同样判死。
if [ "$serve_alive" -eq 1 ] && [ "$DRY" != "1" ]; then
  hb_file="$BASE/state/flusher_heartbeat"
  hb_stale=0
  if [ -f "$hb_file" ]; then
    hb_ts="$(cat "$hb_file" 2>/dev/null || echo 0)"
    case "$hb_ts" in ''|*[!0-9]*) hb_ts=0 ;; esac
    if [ $(( $(date +%s) - hb_ts )) -gt 120 ]; then
      hb_stale=1
    fi
  else
    proc_start="$(stat -c %Y "/proc/$pid" 2>/dev/null || echo 0)"
    if [ "$proc_start" -gt 0 ] && [ $(( $(date +%s) - proc_start )) -gt 180 ]; then
      hb_stale=1
    fi
  fi
  if [ "$hb_stale" -eq 1 ]; then
    log "flusher heartbeat stale, killing serve for relaunch" "{\"pid\":$pid}"
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
    serve_alive=0
  fi
fi
if [ "$serve_alive" -eq 0 ]; then
  if [ "$DRY" = "1" ]; then
    serve_alive=1  # dry run 不实际拉起，按存活处理避免误判
  else
    # P6：serve.log 轮转（仅在 serve 未运行、即将拉起时做，避免与打开的 fd 错位）
    if [ -f "$BASE/state/serve.log" ]; then
      logsize="$(stat -c%s "$BASE/state/serve.log" 2>/dev/null || echo 0)"
      if [ "$logsize" -gt 5242880 ]; then
        mv -f "$BASE/state/serve.log" "$BASE/state/serve.log.1"
      fi
    fi
    # P3-3/4：带 TZ=Asia/Shanghai 拉起，serve.log 时间戳从此是北京
    # 时间（此前 hook 环境无 TZ，日志记的是 UTC，排障要手动 +8；
    # 本次改动之前的日志仍是 UTC，勿混读）。8>&- 关掉脚本锁句柄，
    # 防子进程继承后永久占锁（P2-1）。
    WECHAT_BRIDGE_DIR="$BASE" TZ=Asia/Shanghai setsid nohup "$HOME/workspace/wechat-bridge-venv/bin/wechat-bridge" serve >> "$BASE/state/serve.log" 2>&1 < /dev/null 8>&- &
    sleep 2
    if [ -f "$BASE/state/serve.pid" ]; then
      newpid="$(cat "$BASE/state/serve.pid" 2>/dev/null || true)"
      if pid_is_serve "$newpid"; then
        serve_alive=1
        log "serve restarted by hook" "{\"pid\":$newpid}"
        rm -f "$DOWN_NOTIFIED"
      fi
    fi
  fi
  if [ "$serve_alive" -eq 0 ]; then
    if [ -f "$DOWN_NOTIFIED" ]; then
      silent "serve down, already notified" '{}'
      exit 0
    fi
    touch "$DOWN_NOTIFIED"
    wake "wechat_serve_down" '{"reason":"serve process dead and hook restart failed"}'
    exit 0
  fi
fi

# 日常保留期维护（二次审查 P3-1/P3-2，时间戳节流、每天至多跑一次）：
# - outbox/sent/ 只保留 30 天：更老的任务文件删除。sent 文件含回复
#   全文与 context_token，无限累积既占盘也是无谓的凭据留存面。
#   failed/ 永不动——死信是待人工处置的证据。
# - wechat_inbox_seen.txt 只增不减，hook 每轮还要逐个 grep 它：超过
#   5000 行时只留最近 2000 行（老 ID 对应的 inbox 文件早已删除，不会
#   再被扫描到，裁掉不影响防重复语义）。就地重写不换 inode，worker
#   的追加（>>）不会因为换文件而落到旧文件上。
PRUNE_STAMP="$STATE_DIR/wechat_sent_prune.stamp"
if [ "$DRY" != "1" ]; then
  maint_now="$(date +%s)"
  last_prune=0
  if [ -f "$PRUNE_STAMP" ]; then
    last_prune="$(cat "$PRUNE_STAMP" 2>/dev/null || echo 0)"
  fi
  case "$last_prune" in ''|*[!0-9]*) last_prune=0 ;; esac
  if [ $((maint_now - last_prune)) -ge 86400 ]; then
    pruned="$(find "$BASE/outbox/sent" -maxdepth 1 -name '*.json' -mtime +30 -print -delete 2>/dev/null | wc -l)"
    if [ "$pruned" -gt 0 ]; then
      log "pruned old sent jobs" "{\"count\":$pruned}"
    fi
    if [ "$(wc -l < "$SEEN")" -gt 5000 ]; then
      tail -n 2000 "$SEEN" > "$SEEN.trimtmp"
      cat "$SEEN.trimtmp" > "$SEEN"
      rm -f "$SEEN.trimtmp"
      log "trimmed seen file to recent 2000 lines" '{}'
    fi
    echo "$maint_now" > "$PRUNE_STAMP"
  fi
fi

# 死信告警（审查第 9 项）：outbox/failed/ 出现新文件时唤醒一次。
# 回复进了死信意味着用户永远收不到、而消息早已被标 seen，必须有人
# 告诉用户。用独立的已通知清单（每行一个文件名），同一个死信只报一次；
# 人工把任务移回 pending 或删掉后，它不在 failed/ 里了，清单条目随之
# 失效——若同一文件日后再进 failed/ 会重新报一次，符合预期。
new_failed=()
if [ -d "$BASE/outbox/failed" ]; then
  for f in "$BASE"/outbox/failed/*.json; do
    [ -e "$f" ] || continue
    fname="$(basename "$f")"
    if ! grep -qxF "$fname" "$FAILED_NOTIFIED" 2>/dev/null; then
      new_failed+=("$fname")
      if [ "$DRY" != "1" ]; then
        echo "$fname" >> "$FAILED_NOTIFIED"
      fi
    fi
  done
  # 清理已消失死信的通知条目，让同名文件将来重新死信时能再报
  if [ "$DRY" != "1" ] && [ -s "$FAILED_NOTIFIED" ]; then
    tmp_fail="$(mktemp)"
    while read -r fname; do
      [ -n "$fname" ] || continue
      if [ -f "$BASE/outbox/failed/$fname" ]; then
        echo "$fname" >> "$tmp_fail"
      fi
    done < "$FAILED_NOTIFIED"
    mv "$tmp_fail" "$FAILED_NOTIFIED"
  fi
fi
if [ "${#new_failed[@]}" -gt 0 ]; then
  payload="$(printf '%s\n' "${new_failed[@]}" | python3 -c '
import json, sys
files = [l.strip() for l in sys.stdin if l.strip()]
print(json.dumps({"failed_files": files}, ensure_ascii=False))')"
  log "new dead-letter jobs in failed/" "$payload"
  wake "wechat_dead_letter" "$payload"
  exit 0
fi

# 认领维护：清理已处理/已消失消息的认领记录；对「文件还在、未 seen」
# 的认领刷新续租时间戳（首次认领时间保留——有效性按首次认领算，
# 续租不能把 6 小时兜底无限往后推，审查第 2 项）。
now="$(date +%s)"
if [ "$DRY" != "1" ]; then
  tmp_claim="$(mktemp)"
  # 二次审查 P2-1：读-改-写整环在认领锁内完成。旧实现先无锁读 CLAIM
  # 生成新内容、最后才在锁内整体替换——读取与替换之间，worker 按约
  # 定补记的认领行会被替换静默冲掉，那条消息随即失去认领、被下一轮
  # 当新消息重复唤醒。现在从读第一行起就持锁，worker 的追加会等本
  # 轮维护结束，行不会丢。（脚本级整程锁是另一个文件，不冲突。）
  exec 9>>"$CLAIM_LOCK"
  flock -x 9
  while read -r cid cfirst clast _rest; do
    [ -n "$cid" ] || continue
    case "$cfirst" in ''|*[!0-9]*) cfirst="$now" ;; esac
    if [ -f "$BASE/inbox/$cid.json" ] && ! grep -qx "$cid" "$SEEN" 2>/dev/null; then
      echo "$cid $cfirst $now" >> "$tmp_claim"   # 续租：首次不变、续租刷新
    fi
  done < "$CLAIM"
  cat "$tmp_claim" > "$CLAIM"
  flock -u 9
  exec 9>&-
  rm -f "$tmp_claim"
fi

# 超时重唤醒状态装载：行格式 "id 已重唤醒次数 上次重唤醒ts 已告警0/1"。
# 装载时顺带清理：inbox 文件已消失（=已处理完）或已 seen 的条目丢弃，
# 卡死告警状态随之重置。该文件只由本脚本读写，脚本整程锁已串行化，
# 不需要额外加锁。dry run 只读不落盘。
declare -A RW_CNT RW_LAST RW_AL RW_LISTED
rw_ids=()
if [ -f "$REWAKE" ]; then
  while read -r rid rcnt rlast ral _rx; do
    [ -n "$rid" ] || continue
    case "$rcnt" in ''|*[!0-9]*) rcnt=0 ;; esac
    case "$rlast" in ''|*[!0-9]*) rlast=0 ;; esac
    case "$ral" in ''|*[!0-9]*) ral=0 ;; esac
    if [ -f "$BASE/inbox/$rid.json" ] && ! grep -qx "$rid" "$SEEN" 2>/dev/null; then
      RW_CNT[$rid]="$rcnt"; RW_LAST[$rid]="$rlast"; RW_AL[$rid]="$ral"
      RW_LISTED[$rid]=1; rw_ids+=("$rid")
    fi
  done < "$REWAKE"
fi

# 新消息扫描（含超时重唤醒判定）
wake_files=()
rewake_files=()
stuck_files=()
declare -A RW_FIRE RW_ALERT RW_DROP
for f in "$BASE"/inbox/*.json; do
  [ -e "$f" ] || continue
  id="$(basename "$f" .json)"
  if grep -qx "$id" "$SEEN" 2>/dev/null; then
    continue  # 已处理（含已被并入合并回复并标 seen 的）
  fi
  claim_line="$(awk -v id="$id" '$1==id {print}' "$CLAIM" | tail -1)"
  if [ -n "$claim_line" ]; then
    first_ts="$(echo "$claim_line" | awk '{print $2}')"
    case "$first_ts" in
      ''|*[!0-9]*)
        continue ;;  # 认领行格式异常：保守按「已认领」处理，不重复唤醒
      *)
        claim_age=$((now - first_ts))
        if [ "$claim_age" -ge "$CLAIM_STALE_SECS" ]; then
          : # 首次认领超 6 小时兜底：落到下方按新消息重新唤醒（原逻辑）
          if [ -n "${RW_LISTED[$id]:-}" ]; then
            RW_DROP[$id]=1  # 既然按新消息重走，重唤醒状态一并清掉
          fi
        elif [ "$claim_age" -ge "$REWAKE_STALE_SECS" ]; then
          # 已认领未满 6 小时但超过重唤醒阈值：原唤醒大概率已丢失
          rcnt="${RW_CNT[$id]:-0}"; rlast="${RW_LAST[$id]:-0}"; ral="${RW_AL[$id]:-0}"
          if [ "$rcnt" -lt "$REWAKE_MAX" ]; then
            if [ "$rlast" -eq 0 ] || [ $((now - rlast)) -ge "$REWAKE_STALE_SECS" ]; then
              rewake_files+=("$f")
              RW_FIRE[$id]=1
            fi
          elif [ "$ral" -eq 0 ] && [ $((now - rlast)) -ge "$REWAKE_STALE_SECS" ]; then
            stuck_files+=("$(basename "$f")")
            RW_ALERT[$id]=1
          fi
          continue
        else
          continue  # 已有 worker 在处理（租约有效），不重复唤醒
        fi
        ;;
    esac
  fi
  wake_files+=("$f")
  if [ "$DRY" != "1" ]; then
    claim_append "$id" "$now"
  fi
done

# 决定本轮唤醒：新消息/重唤醒优先（卡死告警等没有其他唤醒的轮次再发，
# 反正下一轮 5 秒后就到）；状态落盘只在决定做出后、且仅非 dry 时进行。
fire_msg=0; fire_stuck=0
if [ "${#wake_files[@]}" -gt 0 ] || [ "${#rewake_files[@]}" -gt 0 ]; then
  fire_msg=1
elif [ "${#stuck_files[@]}" -gt 0 ]; then
  fire_stuck=1
fi
if [ "$DRY" != "1" ]; then
  tmp_rw="$(mktemp)"
  for rid in "${rw_ids[@]}"; do
    if [ "$fire_msg" -eq 1 ] && [ -n "${RW_DROP[$rid]:-}" ]; then
      continue
    fi
    rcnt="${RW_CNT[$rid]}"; rlast="${RW_LAST[$rid]}"; ral="${RW_AL[$rid]}"
    if [ "$fire_msg" -eq 1 ] && [ -n "${RW_FIRE[$rid]:-}" ]; then
      rcnt=$((rcnt + 1)); rlast="$now"
    fi
    if [ "$fire_stuck" -eq 1 ] && [ -n "${RW_ALERT[$rid]:-}" ]; then
      ral=1
    fi
    echo "$rid $rcnt $rlast $ral" >> "$tmp_rw"
  done
  if [ "$fire_msg" -eq 1 ]; then
    for rid in "${!RW_FIRE[@]}"; do
      if [ -z "${RW_LISTED[$rid]:-}" ]; then
        echo "$rid 1 $now 0" >> "$tmp_rw"  # 此前无条目：首次重唤醒新建
      fi
    done
  fi
  cat "$tmp_rw" > "$REWAKE"
  rm -f "$tmp_rw"
fi

if [ "$fire_stuck" -eq 1 ]; then
  payload="$(printf '%s\n' "${stuck_files[@]}" | python3 -c '
import json, sys
files = [l.strip() for l in sys.stdin if l.strip()]
print(json.dumps({"stuck_files": files}, ensure_ascii=False))')"
  log "wechat messages stuck unprocessed after rewake cap" "$payload"
  wake "wechat_stuck_message" "$payload"
  exit 0
fi

if [ "$fire_msg" -eq 0 ]; then
  silent "no new wechat messages" '{}'
  exit 0
fi

all_files=("${wake_files[@]}" "${rewake_files[@]}")
payload="$(python3 - "${#wake_files[@]}" "${all_files[@]}" <<'PYEOF'
import json, sys
n = int(sys.argv[1])
files = sys.argv[2:]
print(json.dumps({"files": files[:n], "rewoken_files": files[n:]}, ensure_ascii=False))
PYEOF
)"
if [ "${#rewake_files[@]}" -gt 0 ]; then
  log "stale claimed wechat messages re-woken" "$payload"
fi
log "new wechat inbox messages" "$payload"
wake "new_wechat_message" "$payload"
