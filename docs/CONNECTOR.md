# WeChat Bridge：当前运行约定

该桥接程序使用 iLink 接收消息，保存 inbox，由 Muse worker 处理并入队，独立 flusher 发送回复。云主机的真实安装、平台唤醒与微信送达须由所有者验收，仓库测试不等同于实机通过。

依赖由 `pyproject.toml` 声明：`qrcode[pil]`、`pycryptodome`。只改 GitHub 不会自动更新云主机。

## 本分支的协议入口

Worker 和 Hook 使用 [`hooks/WORKER-STATE.md`](../hooks/WORKER-STATE.md) 中的机器状态接口。不得再直接追加 claimed/seen/started 或手写 outbox 来绕过 lease。`files` 与 `rewoken_files` 必须合并处理，真实处理集合由 helper 返回的所有权决定。

部署与验收按 [Muse 拉取测试说明](MUSE-TESTING.md)。Python 包、Hook 脚本和 prompt 要同步更新；仓库定义中的 delivery.to 是占位符，保留线上真实投递目标。

## 数据与服务

默认数据目录 `~/workspace/wechat-bridge`，可用 `WECHAT_BRIDGE_DIR` 指定独立目录；默认Hook状态目录 `~/hooks/state`，隔离测试用 `WECHAT_HOOK_STATE_DIR`。

- `state/session.json`：私有凭据，禁止提交或发给协作者。
- `state/get_updates_buf.txt`、`state/last_context.json`：游标与最新上下文。
- `state/received/`：持久收件/完成账本，不随 seen 缓存裁剪。
- `state/quarantine/`、`state/QUARANTINE_NOTICE`：异常输入的私有隔离证据与可见提示，不能当作正常已处理回复。
- `state/reply_batches/`：不可变回复批次与发布状态，含正文/上下文，当前没有自动TTL。
- `inbox/`、`outbox/pending/`、`outbox/sending/`、`outbox/sent/`、`outbox/failed/`：处理与投递队列。
- `state/serve.pid`、`state/serve.lock`、`state/flusher_heartbeat`：服务身份、单实例和线程监督。

`sent/` 的30天维护只清除sent副本；不会清除reply_batches或quarantine。failed、received及其它恢复证据不应随意删。新日志不输出正文/上下文，但旧日志与备份仍需按私有资料处理。

## 发送与恢复

- 类型错误任务会隔离，不能堵住正常回复。
- 同一收件人按FIFO；前一条退避时后续等待，前一条死信后可放行；不同收件人不互相阻塞。
- 合并正文、成员覆盖和client_key在首次网络请求前持久化。错误或超时后保持整个任务重试，不拆成新身份逐条重发。
- sending遗留在串行发送周期之间安全回收；已有pending进度、次数和退避不能被旧副本覆盖。
- 分片按已确认进度继续，超过真实片数的元数据拒绝；内部每context预算8，耗尽后等待新上下文。8是客户端策略，未声称已验证服务端额度。
- client_id保持稳定有助于服务端去重，但服务端语义尚待实机确认，不能承诺网络故障下严格只投递一次。
- 配文先发，配文待重试则媒体等待，配文死信则媒体停止。
- 次数上限5、任务龄30分钟、退避30/120/600秒；超限进入failed，不擅自从死信重发。

切换账号不能复用旧状态目录；命令会拒绝覆盖，保留原资料。请选择新数据目录，并在旧服务停止后绑定。活动服务期间不能重绑凭据。

## 运维与告警

`wechat-bridge status` 显示队列和隔离提示。Hook负责保活；其输出接受仅代表一次通知尝试，未确认送达的故障按有限预算/冷却重试。180/600/700秒恢复阈值未改，平台worker实际时限另行验收。

重启、测试或回滚前先停对应Hook调度并确认worker/serve不再处理。只对已验证的本项目进程操作，禁止按模糊名字杀系统进程。保留运行状态，不使用git clean/reset或同步删除覆盖队列。prepared恢复及结果不明批次按机器协议文档处理，不制造新身份重发。

## 离线检查

```bash
python -m pip install -e .
python tests/run_offline.py
```

该入口分进程运行所有离线套件、阻断真实socket并隔离状态。Shell子套件采用当前checkout与假runtime；不等同于Muse/微信集成验收。旧版操作与事故说明保留在Git历史e1e7216，不能拿旧手写状态流程混用新lease协议。
