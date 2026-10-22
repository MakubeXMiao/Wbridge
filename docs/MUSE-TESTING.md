# 给 Muse：拉取修复分支并做实机测试

仓库：https://github.com/MakubeXMiao/wechat-bridge-muse
分支：`fix/qa-reliability-20261022`
原始基线：`e1e72168ca8e1d2819df439cd3a1119aa4078eca`

本轮针对已部署版本的连发退步：5 条消息被多个 worker 拆分，首条约 6 分半回复，其余消息无 reply_batches 且长期占位。新版本改为同一发送者单 owner 批量处理，begin/reply 快路径，以及 90 秒软租约、570 秒硬上限。本文件列的是**待实机验收项**，不代表已修复线上效果。

所有者已授权将修复推到此分支，尚未批准合并 main 或由 Codex 直接部署。本文件供所有者另行让 Muse 安装测试。先记录当前运行源码/提交、解释器和真实 Hook 定义，确认独立测试账号或受控切换同一账号。

## 1. 拉取到独立源码目录

```bash
git clone --single-branch --branch fix/qa-reliability-20261022 \
  https://github.com/MakubeXMiao/wechat-bridge-muse.git \
  ~/workspace/wechat-bridge-qa-src
cd ~/workspace/wechat-bridge-qa-src
git rev-parse HEAD
python3 -m venv ~/workspace/wechat-bridge-qa-venv
~/workspace/wechat-bridge-qa-venv/bin/python -m pip install -e .
~/workspace/wechat-bridge-qa-venv/bin/python tests/run_offline.py
```

目录已存在时，先检查 git status，禁止覆盖未提交工作；在干净的测试副本中 fetch 此分支并 fast-forward。记录实际拉取 SHA，不沿用上次已部署的 SHA 当作本次测试版本。安装和离线测试不需要真实 bot token。

离线回归至少覆盖三条路径：多 wake 并发 claim/begin 仍只有一个 owner；owner 中途死亡或沉默后过期；新 worker 接管后旧 worker 被 fencing 拒绝。另核对到期边界、续租硬上限、后到消息重组与发送账本保留。离线 PASS 不能替代以下平台验收。

## 2. 停旧调度，再切换配套版本

- 记录旧服务源码、解释器、启动命令、Hook 脚本、真实 prompt/投递目标和返回旧代码的方法。
- 在云主机本地备份代码/配置；运行状态先停写再备份。凭据、消息、完整备份不得上传 GitHub 或发给协作者。
- 优先使用独立测试微信账号及独立数据/Hook 状态。若同一机器人受控切换，先停旧 Hook 调度，确认旧 worker 已结束，再停止已验证身份的旧 serve；不能让旧 worker 与新协议同时操作租约，也不能两套服务抢收。
- 不覆盖或清空 session、游标、队列、seen/claim/started、received 账本；测试账号不能复用旧账号数据目录。退出 worker 与删除状态不是同一件事。

## 3. 包、脚本和真实平台 prompt 一起更新

- Python 服务与 worker 使用测试 venv 的本包，确认 `python -c "import wechat_bridge; print(wechat_bridge.__file__)"` 指向本分支源码。
- Hook 脚本使用本分支 `hooks/wechat-inbox-watch.sh`。配置 `WECHAT_BRIDGE_PYTHON`、`WECHAT_BRIDGE_EXECUTABLE`、`WECHAT_BRIDGE_DIR`、`WECHAT_HOOK_STATE_DIR` 与实际测试路径一致；平台不支持环境配置时，复制到既有默认路径也须保持同一版本。
- 将 `hooks/wechat-inbox-watch.json` 的 **prompt** 和 5 秒轮询设置同步到真实平台定义；保留真实 delivery.to、聊天绑定、平台字段与运行时，不把仓库占位符覆盖线上目标。只拉 Git 不会自动更新平台保存的 Hook prompt。
- `HATCH_HOOK_RUNTIME` 由平台提供，不在仓库；不要将 `tests/fixtures/hook_runtime.sh` 安装为生产运行时。
- Worker 按 [WORKER-STATE](../hooks/WORKER-STATE.md) 使用 **begin→reply**；复杂任务按实际进展 refresh，明确放弃时 release。确认真实 prompt 已移除 5/8 秒强制等待、claim 补扫、手读 inbox/context、手写 outbox 和旧 700 秒开工保护。
- 首次新版本 plan 会迁移 legacy 状态；保留备份，不混跑旧 worker。观察模式 `HATCH_HOOK_DRY_RUN=1` 不验证真实租约执行或平台投递。

## 4. 本轮连发与恢复验收

每项记录 SHA、消息 ID、平台 worker ID、动作、脱敏阶段日志和手机结果。不要把命令 exit 0、queued、API 接受或回复正文写着“成功”当作手机验收。

| 编号 | 操作 | 必须观察的结果 |
| --- | --- | --- |
| B1 单条快路径 | 分别发送问候、确认和“只回复：单条测试” | begin 一次取得消息，正常仅一次 reply；无强制 sleep、临时正文文件或无关历史查询；手机收到真实回复，“只回复”严格遵循正文要求。记录端到端时间 |
| B2 连发 5 条 | 同一账号快速连续发送 5 条带不同序号的消息，并确保 5 条在首次 reply 入队前已被桥接收到 | 同一活跃 owner 批量取得全部 5 条并合并回复；reply_batches 的 covers_message_ids 覆盖全部 5 条；无多个 sibling 各占一部分、无剩余 inbox 卡住、无重复回复。手机确认回复覆盖全部内容 |
| B3 后到消息 | worker 已 begin、尚未 reply 时继续发消息，跨越至少一轮 Hook 轮询 | 后到消息归同一 owner；refresh 获取它们，或 reply 返回 needs_refresh 后重组；不能遗漏内容、不能仅补 ID。新到发生在入队临界区之后的消息应进入下一批次，不算丢失 |
| B4 重复/并发 wake | 在隔离测试环境让同事件并发/重复到达，并在 owner 活跃时发生下一次轮询 | 同事件只有一个 begin 成功；迟到 sibling 退出；无分裂所有权或重复 reply_batches。不同发送者仍各自处理，不混入一条回复 |
| B5 owner 中途死亡 | 隔离测试中确认 begin 已成功、尚无 reply_batches，然后让该平台 worker 结束，保持 Hook/serve 正常 | 从最后一次有效 begin/refresh 起不超过约 90+5 秒即可重新计划；新 worker 接管全部未处理消息。另记平台实际启动新 worker 的延迟；不再因旧 700/600 秒阈值占位十几分钟 |
| B6 静默与硬上限 | 隔离测试 owner 不再取得进展，或仅真实进展间隔续租 | 无续租到 90 秒失效；有续租也最迟 begin 起 570 秒失效。平台的 600 秒可能从更早的开工时刻计时；若提前结束，仍须由 90 秒静默租约恢复。后到消息和旧 started 文件不能续命；没有脱离 worker 的后台心跳 |
| B7 过期 fencing | 保留旧私有 lease，在其过期后、新 owner 接管前后分别尝试 refresh/reply | 均拒绝旧身份；无旧批次入队，不影响新 owner；新 owner 正常回复。不能只以旧 adopt 返回 lease_lost 判定整条恢复路径通过 |
| B8 未启动与预算 | 事件已计划但不 begin；另用隔离状态重复触发失败接管 | 事件在 180 秒到期后下一轮可重计划；每条消息恢复 3 次后进入可见卡住告警，添加新消息或更换 lease 不重置预算 |

B2 建议至少重复 3 轮，并包含“全部先到再 begin”和“begin 后跨轮询继续到达”两种时序。测试期间记录实际开始、首次回复和最后一条完成时间。合并只覆盖入队前已收到的消息；不能为等待未来消息无限推迟回复。

简单消息应对比同环境旧版“几十秒”的实测表现。若仍到分钟量级，即使最终有回复也应标记延迟问题未通过；根据下一节阶段时间定位，不以租约能恢复代替响应速度验收。

## 5. 定位首回延迟，保留原功能验收

按相同消息 ID 收集时间：

1. 收件记录 received_at → wake_planned：桥接收件到 Hook 计划。
2. wake_planned → adopted：Hook 投递及平台排队/启动，另保留平台真实 worker 开始时间。
3. adopted → enqueued：worker 生成回复/调用工具/协议步骤；查看中间 renewed 和 needs_refresh。
4. enqueued → send_started：发送队列等待。
5. send_started → api_accepted：接口发送耗时；失败时保留错误种类与重试时间。
6. api_accepted → 手机可见回复：由所有者确认，无法从 API 成功自动推断。

worker 的 `latency` 写 stderr，stdout 为 JSON；serve 的阶段日志只应含阶段、时间、ID 等元数据。对外交回时仍需脱敏，不附正文、context_token、完整 lease、登录凭据或整个 reply_batches 文件。若某阶段没记录，标记缺失，不能凭猜测补时间。前次约 6 分半首回尚无足够证据确认某个单一原因。

以下原功能仍须回归：

- 同一消息的文字+附件、多附件；无转写语音保留原音，不假称已有自动 ASR。
- 图片/文件回复及配文：配文先于媒体；失败恢复后不重复配文；纯媒体入口可用。
- 发送中断/超时、serve 重启：任务身份与进度保留；手机实际收到几次，不预设 iLink 必定按 client_id 去重。
- 同用户回复 FIFO、token 预算耗尽后的续发、failed/隔离告警；prepared 与未知发送结果按不同恢复流程处理。
- 涉及安排/金额/日期等事务仍按需读历史与工具，不因快路径漏记或假报完成。
- 云主机保活、真实 runtime/Hook 投递、实际 worker 上限和重启后状态保留。

## 6. 回滚和交回结果

测试不通过先停止新调度和服务，报告具体提交、失败步骤和脱敏日志，不合并、不清空队列、不自动重发未知结果任务。

代码回滚不等于运行状态回滚。新版已经发送的消息不可撤回；恢复旧游标/队列/完成账本可能重复发送。保留新旧恢复证据，使用已核对的旧代码及配套旧 Hook/prompt。若有新 lease/批次或未确认发送状态，先由维护者判断兼容处置，不能声称一条 git 命令即可安全回退。

请交回：实际拉取 SHA、安装路径和真实 prompt 版本、离线结果、B1–B8 各项 PASS/FAIL/未测、5 条连发逐阶段时间表、手机确认及尚未验证项。所有者实机确认通过后再决定合并 main。
