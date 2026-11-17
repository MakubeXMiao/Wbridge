# Worker / Hook 状态协议

本文件描述本仓库的机器接口。离线测试与假运行时只能验证桥接状态转换，不能证明 Muse 平台投递、调度、600 秒 worker 上限或微信送达；实机验收见 [MUSE-TESTING](../docs/MUSE-TESTING.md)。

## 一次 begin，批量处理，同一发送者一个 owner

Hook 输出 `files`、`rewoken_files`、`message_ids`、实际解释器 `python` 和一次性事件 `lease`。文件列表是唤醒线索，当前可处理批次由 `begin` 返回；不能因 `files=[]` 跳过重唤醒。优先使用 payload.python/base/state_dir，以下示例是默认部署路径：

```bash
P=~/workspace/wechat-bridge-venv/bin/python
B=~/workspace/wechat-bridge
S=~/hooks/state
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  begin --lease EVENT_LEASE
```

`begin` 在同一锁内消费事件 lease、换成新的私有 worker lease、收集同一发送者当前可处理的消息并返回：

- `lease`：之后全部命令使用此私有 lease，不能继续使用事件 lease。
- `owned`、`messages`：完整处理集合和按时间排序的消息内容；无需再 cat inbox 或查 context。
- `to`、`context`：本批接收人与最新上下文；`reply` 会自行取得并校验它们。
- `expires_at`、`hard_deadline`、`renew_after_seconds`：软期限、硬期限与建议续租间隔。

只处理实际返回的 `owned/messages`。同一事件不能消费两次；`owned=[]` 或 `lease_lost` 就退出，迟到 worker 不能重新 claim。`covered` 的 pending/sending/sent/queued 表示已有回复入队，helper 会完成收件状态，不能再回；failed/uncertain 不当成功，须报告并人工核对。

同一发送者有活跃事件或 worker 时，后到消息归其批次，不另起 sibling owner；不同发送者绝不合并。后到消息不会延长租约。已经进入回复入队临界区之后才收到的消息，留给后续批次，不能承诺无限延后已经完成的回复。

## 简单消息的两次调用快路径

问候、确认和“只回复 X”不强制等待 5/8 秒，不为此读取历史或写临时正文文件。正常流程为 `begin` 后直接生成简短回复，再调用 `reply`；“只回复 X”应仅返回 X。复杂事务仍按需查询历史与工具，不能为了快路径漏掉金额、日期、安排等必要核对。

```bash
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  reply --lease PRIVATE_LEASE --ids 201 202 --text '收到，我会按这两条信息一起处理。'
```

使用最近一次返回的全部 `owned`，顺序从旧到新。正文作为一个参数传入，动态内容应使用 subprocess 参数数组等安全传参方式，不能拼接为 shell 代码。helper 从消息中派生接收人与最新 context，worker 不再填写 `--to`/`--context`。

若新消息已到达、但不在本次 `--ids` 内，`reply` 返回 `error=needs_refresh` 和当前批次，**不入队、不完成原消息**。worker 读取新增消息，调用 `refresh` 续租并取得完整批次，重新组织完整回复后再次 `reply`；不能只机械扩大 ID 却沿用遗漏新问题的旧正文。

成功后 helper 保留不可变批次登记、`covers_message_ids` 和固定 `client_key`，原子入队，再逐条写 complete receipt、标 seen、删除 inbox/started。`queued` 只表示入队；serve/API 接受与手机收到分别验收。正常 `reply` 后无需 `complete` 或另改状态文件。

媒体沿用发送器能力，通过同一入口提交。helper 不读取媒体内容、不上传；路径须指向已有普通文件，发送期间保留不变：

```bash
# 媒体加配文
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  reply --lease PRIVATE_LEASE --ids 204 --text '配文' --media-path /tmp/picture.png
# 纯媒体可省略 --text
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  reply --lease PRIVATE_LEASE --ids 205 --media-path /tmp/document.pdf
```

正文和媒体均为空、媒体不存在或不是普通文件时拒绝入队，保留 inbox。原任务正文、覆盖 ID、路径和 client_key 同时保存在 prepared 登记中。

## 90 秒软租约与 570 秒硬上限

| 状态 | 期限与恢复行为 |
| --- | --- |
| Hook 已计划，尚未 begin | 事件创建后 180 秒到期，后到消息不延长 |
| worker 已 begin | begin 或最后一次有效 refresh 后 90 秒到期 |
| worker 持续取得真实进展 | 建议约 30 秒 refresh；最迟 begin 起 570 秒仍失效 |
| 主动放弃 | release 后下一轮 Hook 可重新计划 |
| 已过期或已换 owner | begin/refresh/reply 拒绝旧 lease，不允许迟到写入 |

复杂任务在真实进展之后及时续租，同时收集同一发送者的后到消息：

```bash
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  refresh --lease PRIVATE_LEASE
# 明确无法继续且尚未入队时主动释放
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" \
  release --lease PRIVATE_LEASE
```

必须在 `expires_at` 前续租；到期之后不能靠 refresh 恢复旧身份。不得使用脱离 worker 的后台心跳或无进展续租，否则无法发现 owner 沉默。阻塞工具应拆为短步骤；当前协议不保证超过 90 秒无可观测进展的单次调用仍能保有 lease。`refresh` 不重置开始时间，也不能越过 570 秒硬上限。

Hook 每 5 秒轮询时，静默 owner 最晚在最后一次有效 begin/refresh 后约 **90+5 秒**可被重新计划；这是状态恢复界限，平台调度延迟另计。平台杀掉 worker 不会直接通知状态机，实际失效依赖该租约机制。新 worker 不再使用旧版“started 后等 700 秒”或“重唤醒后再等 600 秒”；旧 700 秒仅用于 legacy 行的迁移上限。每条消息的恢复预算仍为 3 次，耗尽则进入卡住告警，不因新消息或换 lease 重置预算。

`adopt/claim/enqueue-reply/complete` 是兼容维护入口；显式传入 `claim --lease` 时只允许仍活跃的私有 worker lease，未知或过期 token 均拒绝；维护者无 lease 的手动 claim 才创建新身份。新平台 prompt 必须使用 begin/refresh/reply/release，不得再用 claim 补扫或直接改 claimed/seen/started/outbox。

## 崩溃、恢复与时间记录

- `state/reply_batches/*.json` 保留原任务正文、覆盖 ID 和固定身份。
- `phase=prepared` 明确尚未写发布意图；维护者可执行 `recover-prepared TASK.json` 按原任务恢复。
- `phase=publishing`、内容缺失或结果不明时，拒绝自动重建，需人工核对；不制造新正文或 client_key。
- `state/received/` 的 complete receipt 是永久收件防重账本，不跟随 seen 保留期裁剪；删除/迁移须保留此语义。
- `prune-seen` 与普通 plan 一样，在同一锁内先把 legacy 已 seen 残留 inbox 迁移为 sender-bound complete receipt，再裁剪 seen。
- receipt 锁在前、CLAIM 锁在后；seen 追加和裁剪共享 CLAIM 锁。

`latency` 记录消息 ID、阶段和时间，不包含正文、context_token 或完整 lease。worker 的阶段记录写 stderr，stdout 保持单个 JSON；serve 记录发送开始与 API 接受。用消息 ID 串联收件时间、wake_planned、adopted、renewed、enqueued、send_started、api_accepted，区分调度、生成、排队、发送耗时。wake_planned 不代表平台已投递，API 接受也不代表手机收到；实测应另记手机首回时间。当前证据不足以将此前约 6 分半的首回延迟归因于某一个固定计时器。

隐私保留限制：`state/reply_batches/` 包含完整原任务正文和 context_token，以支持 prepared 恢复及不确定状态核对，**没有自动 TTL**；`state/quarantine/` 可能保存被拒绝的原始输入，也没有自动 TTL。这些是私有运行数据，不得提交 Git 或转给协作者。sent/ 的 30 天清理不会清除上述副本或全部凭据；不要删除未知结果证据。

## 告警和进程监督

未知真实运行时的送达确认协议。告警记录始终标为未确认；wake 成功只记 runtime 接受尝试。告警默认每 600 秒尝试，每 3 次后冷却至少 1 小时，再周期重试；此节的 600 秒是告警节奏，**不是 worker 租约恢复等待**。故障解除清理对应状态；可显式设置 `WECHAT_NOTICE_RETRY_SECONDS`、`WECHAT_NOTICE_RETRY_BUDGET`。

进程监督验证命令位置和进程启动时间。旧进程心跳不立即判死新进程；新 serve 保留 180 秒启动宽限，已有本进程心跳则用 120 秒陈旧阈值。仅验证过的同一进程身份可被停止；serve 健康阈值与 worker 租约分开。

## 离线验证与配套部署

```bash
python tests/run_offline.py
```

Shell 套件默认测试当前 checkout 的 Hook，使用 `tests/fixtures/hook_runtime.sh`，可显式设置 `HOOK_SCRIPT` / `HOOK_RUNTIME`。它们包装相同 Python 测试的子集，不能重复累计为独立场景。多 wake 并发、owner 死亡与过期接管必须另有离线回归；平台响应速度和手机效果仍待实测。

`HATCH_HOOK_DRY_RUN=1` 只观察，不创建目录、修改状态或监督进程。应用本版必须先停止旧 Hook 调度并确认旧 worker 已结束，再配套更新 Python 包、Hook 脚本和真实平台 prompt，保留登录、游标、收件与发送账本。只换 prompt 或只换脚本会破坏协议；不可同时运行两版 worker。本提交不部署、不合并。
