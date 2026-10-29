# Muse 适配器候选与手机测试

任务 `WBR-02-20261023-002`，2026-10-23。用户已授权本线自动修改、自测、独立 QA、上传候选及让 Muse 更新后手机测试；强调效果优先、必要可重构，Muse 建议必须二次评审。本候选不合并旧上游 main，不改变公共协议、账号绑定和运行账本。

## 改善机制

以前模型须保管私有 lease、逐条抄完整 owned ID 并拼接正文参数。现在 `muse-worker.py` 在私有目录保存这些机器状态，模型只接收任务、handle 与 revision，提交答案。后到消息触发 `needs_refresh`，必须读取完整新任务、显式 refresh 并重写答案；旧 revision 不能提交。私有文件权限 0600，不进 Git。

适配器仍调用公共 `WorkerState.begin/refresh/reply/release`；不手写 outbox、不提前 begin、不自动心跳、不延长 180/90/570 秒。reply 的结果不明会封住该会话，等待核对；不能自动重发或把未知当作失败。外部提醒/工具动作的业务幂等仍未实现，不能以回复防重代替。

Hook 增加实际 adapter 路径/协议/文件哈希，以及脱敏 `wake_called/wake_returned` 时间、返回值。返回成功仅为 runtime 接受，不能算平台 worker 开工。

本候选针对协议走样、版本混用和定位困难。正常仍两次调用，不能承诺缩短 Muse 排队时间。没有证据支持个人 Muse 公开调用 API、固定上下文 token 数或亚秒回复；不采用没有生命周期证据的自动续租。

## 本地验证

使用隔离临时目录和合成消息，无真实账号、消息、网络或部署操作：

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/run_offline.py --output-dir qa-results/core
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python hooks/tests/run_offline.py --output-dir qa-results/muse
```

独立 QA 必须绑定最终候选 SHA。特别验证特殊字符原样入队、重复 begin、过期 fencing、不同发送者、后到消息与旧 revision、同 handle 并发 reply、未知发布结果禁止继续、缺文件/错版本明确失败。脚本 dry run 仍不得创建路径、锁或缓存。

## 更新到 Muse 环境

1. 记录旧源码完整 SHA、dirty、实际解释器/包路径、serve 启动身份、脚本/adapter 哈希、真实平台 Hook prompt。保留真实 delivery.to，不上传凭据、完整消息或状态。
2. 停旧 Hook 调度并确认旧 worker 已结束，再停身份已核对的旧 serve；不要两套服务抢同一 bot。保留账号、游标、收据、队列和所有发送/恢复账本。回滚代码不能回滚这些状态。
3. 拉取指定候选 SHA 到干净源码目录，使用对应 venv 安装本包。离线测试通过后，将 `wechat-inbox-watch.sh`、`muse-worker.py` 安装到同一 Hook 脚本目录，或由维护者配置绝对路径 `WECHAT_MUSE_ADAPTER`。不要把测试 runtime 安装为平台 runtime。
4. 将候选 JSON 的 prompt 同步到平台实际定义，poll=5、enabled=true，保留现有真实聊天绑定。仅 git pull 不会更新平台保存的 prompt；不要复制仓库占位 delivery。
5. 导出平台实际配置到本地私有文件，以 `muse-deploy-check.py --help` 的参数核对源码 SHA、live脚本/adapter、prompt、解释器包路径/核心文件哈希。检查器只认证所提供的文件和配置导出；另需平台保存回读、新 worker 使用本入口的证据。
6. 启动单一新版 serve 和新版 Hook，保存启动时间/身份与首次 adapter 使用证据。旧 worker 未结束、检查失败或平台真实 prompt 未确认时不要通知“已可测试”。缺证据须明示限制。

## 外出手机测试

先由用户发送“只回复：WBR测试1”，确认原文准确且只收到一次；再做普通小查询、5条连续消息和 begin 后后到消息，各保留批次与时间。使用测试专用正文，暂不创建真实提醒或其他有副作用事务。媒体仍是公共链路的独立待验项。

记录 received→plan、wake调用→返回、返回→begin、begin→queued、queued→API接受、手机实际可见。手机阶段由用户确认；人工补发单列，不能算自动成功。候选只有离线 PASS 或已上传时，不应通知手机验收通过。若协议错误消失但开工前仍反复等分钟，继续缩 prompt 或放宽 lease 不足以达到快助手目标，需评估正式可控的调用后端。

## 回滚

停新调度并等 worker 退出，核对发送结果后恢复成套旧源码/脚本/prompt。新增 adapter 会话文件是私有运行证据，保留核对；不要清除来解锁未知结果。恢复旧账号、游标、队列或收据可能重复发送，不属于代码回滚步骤。
