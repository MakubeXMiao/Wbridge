# Worker / Hook 状态协议

本文件描述本仓库的机器接口。离线测试和假运行时不证明 Muse 平台投递、调度、600 秒 worker 上限或微信送达；需由所有者在隔离平台环境验收。默认180/600/700秒阈值未变。

## 处理一批消息

Hook 输出 `files`、`rewoken_files`、`message_ids`、实际解释器 `python` 和一次性事件 `lease`；处理集合是两种文件列表的并集，不能因 `files=[]` 跳过重唤醒。优先使用 payload.python/base/state_dir；以下是旧payload缺字段时的默认参数：

```bash
P=~/workspace/wechat-bridge-venv/bin/python
B=~/workspace/wechat-bridge
S=~/hooks/state
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" adopt 201 202 --lease EVENT_LEASE
```

仅处理输出 `owned`。输出的 `lease` 是新的私有 worker lease，后续命令用它；同一个事件再投递不能再次消费原事件 lease。`covered` 的 pending/sending/sent/queued 表示已有回复入队，helper 会完成收件状态；failed/uncertain 不当成功，须报告并人工核对。

宽限补扫发现203时，必须先原子认领，只合并 `claimed` 或同lease的 `owned`：

```bash
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" claim 203 --lease PRIVATE_LEASE
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" enqueue-reply \
  --ids 201 202 203 --to OWNER_ID --context=CONTEXT_TOKEN \
  --text-file /tmp/reply.txt --lease PRIVATE_LEASE
```

IDs按时间从旧到新，最后ID作为队列任务名。命令写不可变批次登记、`covers_message_ids` 和固定 `client_key`，原子入队后为每个ID写complete receipt，再标seen、删除inbox/started。**queued仅指入队**；发送成功、失败、服务端去重仍由发送组件和平台验收确认。

媒体沿用发送器原有图片/文件等能力，也必须通过同一lease入口。配文和媒体登记在同一个私有任务，helper不读取媒体内容、不上传；媒体路径必须指向已有普通文件，发送期间应保留该文件不变：

```bash
# 媒体加配文
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" enqueue-reply \
  --ids 204 --to OWNER_ID --context=CONTEXT_TOKEN --lease PRIVATE_LEASE \
  --text-file /tmp/caption.txt --media-path /tmp/picture.png
# 纯媒体不需要text-file
"$P" -m wechat_bridge.worker_state --base "$B" --state-dir "$S" enqueue-reply \
  --ids 205 --to OWNER_ID --context=CONTEXT_TOKEN --lease PRIVATE_LEASE \
  --media-path /tmp/document.pdf
```

正文和媒体都为空、路径不存在或不是普通文件时拒绝入队，保留原inbox。媒体任务的覆盖ID、正文、路径和client_key也一起保留在prepared登记中，恢复仍使用原任务。

不直接改claimed/seen/started或outbox JSON。收尾重试可用 `complete ID... --lease PRIVATE_LEASE`，它仅接受已入队覆盖或已有complete receipt的消息。

## 崩溃和恢复

- `state/reply_batches/*.json` 保留原任务正文、覆盖ID和固定身份。
- `phase=prepared` 明确尚未写发布意图；维护者可执行 `recover-prepared TASK.json` 按原任务恢复。
- `phase=publishing`、内容缺失或结果不明时，命令拒绝自动重建，需人工核对；不制造新正文或client_key。
- `state/received/` 的complete receipt是永久收件防重账本，不跟随seen保留期裁剪；删除/迁移须保留此语义。
- `prune-seen` 与普通plan一样，在同一锁内先把legacy已seen残留inbox迁移为sender-bound complete receipt，再裁剪seen；维护入口不能跳过迁移。
- receipt锁在前、CLAIM锁在后；所有seen追加和裁剪共享CLAIM锁。

隐私保留限制：`state/reply_batches/` 当前包含完整原任务正文与context_token，以支持prepared恢复及不确定状态核对，**没有自动TTL**；`state/quarantine/` 可能保存被拒绝的原始输入，也没有自动TTL。这些是私有运行数据，不得提交Git或转给协作者。sent/的30天清理只清sent任务，不会清除上述副本或全部凭据。今后若压缩admitted登记为hash/覆盖关系，须先另行验证恢复和查重，不应直接删除未知结果的证据。

## 告警和进程监督

未知真实运行时的送达确认协议。告警记录始终标为未确认；wake返回成功只记runtime接受尝试，不能当作用户收到。默认每600秒尝试，每3次后冷却至少1小时，再周期重试；故障解除清理对应状态。可显式设置 `WECHAT_NOTICE_RETRY_SECONDS`、`WECHAT_NOTICE_RETRY_BUDGET` 调整节奏。

进程监督验证命令位置和进程启动时间。旧进程的心跳不立即判死新进程；新进程保留180秒启动宽限，已有本进程心跳则用120秒陈旧阈值。仅验证过的同一进程身份可被停止。

## 离线验证与部署边界

```bash
python tests/test_worker_state.py
PYTHON=python bash tests/test_hook_audit12.sh
PYTHON=python bash tests/test_hook_audit2fix.sh
PYTHON=python bash tests/test_hook_rewake.sh
```

Shell套件默认测试当前checkout的Hook，使用 `tests/fixtures/hook_runtime.sh`，可显式设置 `HOOK_SCRIPT` / `HOOK_RUNTIME`。它们包装的是同一组Python测试的子集，不能重复累计为独立场景。

`HATCH_HOOK_DRY_RUN=1` 仅输出静默观察结果，既不运行状态计划，也不创建目录、修改时间戳、加锁或监督进程。应用新版本必须同步Python包、Hook脚本与prompt；只换prompt或只换脚本会破坏协议。本提交不部署、不合并。
