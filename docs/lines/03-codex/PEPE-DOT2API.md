# 原版 Dot2API / 当前 PEPE 文字适配候选

WBR-03-20261111-DOT2API-INTAKE。基线 `0d11261b34c649ffc37365a2fbce9cb4d5116b64`；原版 Dot2API 固定 `66a761505ff73c5dca730260efcaeb5697db81ad`。只在独立副本组合01公共差额与03适配器，Muse原仓库、Hook、运行状态、账号和进程不变。本文不是运行配置或上线记录。

## 行为

微信文字由公共收件层登记，01冻结完整原版 Submit 并通过受限 journal 交给03。03调用 `begin_submit()`，成功持久化后只发送一次 `POST /v1/tasks`，保存返回的原 task ID，再有界查询 `GET /v1/tasks/{id}`。只在同一原任务明确 completed 且返回合法非空纯文字时产生一次 final，微信发送、分块、去重和收据仍由公共层处理。

03不生成新的TTL/幂等键，不重POST未知提交，不写公共DB/outbox，不用CLI/API模型补答。原版异步请求没有顶层 model；回答来自该队列连接的当前PEPE，使用PEPE默认模型。task ID不是会话ID，首期 session_id=null，无附件或真实会话续聊。

HTTP在短独立stdlib子进程中执行，以父进程总超时限制DNS/connect/read；敏感正文和凭据仅经stdin，不进argv、子进程环境或诊断。关闭/超时只回收本地HTTP资源，绝不证明PEPE停止。默认不跟随重定向，也不继承代理环境；HTTPS保留证书校验，HTTP仅允许loopback。

## 公共接口 v1 + v1.1

01配置字段为 provider、transport、instance_id、base_url、credential_env、tenant、principal、queue、queue_ownership、instructions、execution_policy；HTTP分支不伪造command/cwd。03只读取 `credential_env` 指定的环境值，不在日志或文件交接中输出它。

两种政策声明必须分别理解：

- `{"mode":"text_only","enforcement":"unverified"}`：默认未就绪；在读取凭据/启动HTTP前拒绝，不能靠prompt证明工具隔离。
- `{"mode":"native_delegate","approval":"platform_native"}`：原生委派候选；PEPE沿用自己的工具、已有权限和原生确认。桥不额外授予权限，也不提供逐工具微信审批；不能声称工具全部禁用或所有操作一定经过本人确认。

用户已在00确认采用“先沿用PEPE自身权限和确认，微信负责文字收发”的语义，无需重复询问这一选择。此确认不代表已授权恢复服务、签发新凭据、建立运行配置或发真实请求。其他provider的explicit审批与Muse路径不变。

request含 `transport=http`、原v1执行/attempt/account/sender/instance/deadline/messages、`remote_task`冻结快照及 `remote_task_journal` 对象。03只用 `begin_submit()`、`bind_task(task_id)`、`snapshot()`；public job、状态持久化和输入来源校验由01负责。

回包按原版 Service.public 的真实字段验证：id、queue、instructions、payload、max_attempts、状态及结果。原版不返回tenant/principal/worker/idempotency_key，不能要求这些虚构字段。认证service/tenant/principal/queue与原冻结快照绑定；错绑定不发布。

unknown提交、HTTP断连、远端failed/cancelled/expired、租约失效、close以及cancel请求均不当作PEPE停止确认。保持原task映射及公共unknown占位，首期不自动恢复发布、跨Store抢占或切模型重跑。同一真实队列仅一个Store/提交拥有者。

和公共journal的保守门槛一致：begin_submit成功后，连同POST的401/403错误也交回unknown，不因HTTP错误自动释放submitting意图或重投；只有确定在提交意图前拒绝的输入/凭据/政策错误才报告failed/none。

## 本地验证

`tests/test_pepe_backend.py`使用原版JSON结构、fake HTTP及受限FakeJournal验证边界；另有一个固定本地sleeper子进程验证HTTP超时后的实际kill/wait。不会连接PEPE/微信、读取真实token或打开真实socket。FakeJournal验证不能替代与01真实RemoteTaskJournal的组合检查。最终组合由00以固定候选SHA完成独立验收。

## 原实例现状与最小恢复窗口

2026-11-11只读取证：既有已授权的独立宿主上 `<DOT2API_SOURCE_ROOT>` 固定66a7615且tracked clean；18792没有监听/该目录进程，HTTPS根404。原数据元信息显示旧api/dot身份disabled、两token revoked、task.available订阅inactive。未读取token、加密key、回调URL或任务正文，未调用会推进maintenance的任务API。当前PEPE与旧订阅的精确对应仍未知。证据保存在本任务共享03协调目录的 `ops-readonly/READONLY-OPS.json`。

恢复前先由00核对独立微信bot/state、目标队列归属和具体操作窗口；用户的原生委派语义选择已确认。以下是待执行步骤，本文没有恢复服务或生成凭据：

1. 保留原66a7615源码、现有数据目录和加密key。先核实际环境引用、tenant/queue、端口、HTTPS路由与安装方式，不重跑init/setup，不编辑数据库复活disabled身份。
2. 仅按原版CLI新建指定dot队列的API调用身份（submit/read）及Dot身份（read/work/subscribe），签发新的受限到期凭据。分开记录稳定principal与token轮换；不把新principal当旧未知任务的同一幂等域。
3. 在原目录/数据上启动一份原版服务，loopback端口18792；只恢复 `dot2api.example.invalid` 的必要HTTPS入口，保留其他服务/VPN。具体启动命令沿用原项目部署方式，本候选不新建自动重启/发布系统。
4. 当前PEPE通过原版MCP认证重新连接并订阅task.available，采用原版领取/文字作答/完成流程；核实当前PEPE独占这条队列。按实际refreshBefore续订，不把默认24h当不可变上限或永久有效。恢复后按原指引补扫待处理任务，事件无历史回放。
5. 先以固定无隐私文字验证唯一任务→当前PEPE→合法结果，再部署00已验的公共+适配完整候选到独立微信状态。只有实际准备就绪才邀请用户必要的扫码和手机文字收发；/readyz或API接受不替代手机收到。

真实恢复需要对应授权，当前本地实现授权不包含新凭据、订阅、服务启用或微信绑定。本阶段不发Muse微版本QA。

## 回退

停止新增PEPE派发，保留journal、task ID、输入和收据及unknown状态；不删库、队列或游标，不把待处理输入送回Muse。回退候选代码不等于取消远端任务。若后续已恢复服务/新身份，按批准窗口撤销新凭据及停止该独立服务/路由，并如实保留未确认的远端执行状态。
