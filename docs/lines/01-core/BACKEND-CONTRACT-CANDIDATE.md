# 最小后端契约候选 v0

任务：`WBR-01-20261023-001`。**讨论稿，非已实现 API，字段/方法名均待跨线补充与用户确认。** 本文不绑定具体 CLI、SDK 或云服务；任何能力都须由对应线用实际版本验证。

## 1. 分层与责任

| 层 | 唯一职责 |
| --- | --- |
| 微信传输 / Store / sender（01） | 接收过滤、附件下载、凭据/上下文、持久账本、出站入队与发送 |
| 消息协调 / 路由 / 准入（01） | sender 所有权、批次版本、路由冻结、session 映射、执行身份/回调 fencing、取消/审批授权与结果准入 |
| 后端适配（02/03/04） | 将抽象请求映射到真实平台；取得真实执行/session 身份、状态/最终结果及取消/审批反馈；声明限制 |
| 后端执行器 | 处理已授权任务与工具；正文不能改变路由、权限、收件范围或公共状态协议 |

建议先保留当前 Muse legacy 路径，再增加版本化抽象边界；不在本轮移动函数、创建公共新 API 或并跑两个收件服务。

## 2. 身份与边界

候选版本名为 `wbridge.backend.v0`。消息及控制事件均使用显式字段，动态内容按数据传递，不拼接 shell。

| 标识 | 含义 / 稳定性 |
| --- | --- |
| `account_scope` | 单个微信账号/数据环境的本地非秘密标识；不使用 bot token 充当标识 |
| `sender_key` | 已通过接收层鉴权的发送者，不信任模型自报；当前只处理绑定机主 |
| `message_id` | 传输收件 ID；唯一键为 account_scope + message_id，检查同 ID 的发送者一致性 |
| `execution_id` | 一次逻辑任务的持久身份；重启/重复 dispatch 不改成新身份重做副作用 |
| `attempt_id` / `fence` | 当前可授权 attempt 的身份；仅公共协调层签发/验证，旧 attempt 迟到结果拒绝 |
| `output_id` | 一次逻辑输出的持久身份，按 account/execution/output_kind/ordinal 命名隔离；重传事件或新 attempt 不自动换新输出身份 |
| `input_revision` | 同一任务所含消息/附件的版本；后到消息导致变更时不能用旧 revision 发布遗漏内容 |
| `backend_id` | 显式选择的后端标识；`muse`、`codex`、`claude-code` 为讨论候选，不等于已有注册表 |
| `backend_instance_ref` | 配置中已授权的后端账号/安装/宿主实例命名空间；非秘密引用，不在不同安装间复用 session |
| `backend_refs` | adapter 实际返回的 run/session/thread/turn/item 等不透明 ID，允许缺项；不合成不存在的 provider ID |
| `route_revision` / `session_epoch` | 固定路由与会话边界，包含后端安装实例；切换后端/实例或重置会话需新 epoch，不能串接旧后端会话 |

现有 lease 仍按当前 worker_state 验证；新增 fence/attempt 字段尚未实现，不能仅写在 JSON 就声称能拦截回放。未来执行 fence 与现有私有 lease 的映射由 01 实施、各线复核，不直接复用微信 context 或任意 PID。

## 3. 入站消息和附件

现有 inbox 字段为 message_id、text、from_user_id、context_token、create_time_ms、received_at，可附 media_paths/media_failed/media_failed_count（`serve.py:109`）。候选不就地改写旧 inbox；由公共协调层生成受限请求视图：

```json
{
  "contract_version": "wbridge.backend.v0",
  "account_scope": "synthetic-account",
  "sender_key": "synthetic-owner",
  "execution_id": "synthetic-exec-1",
  "attempt_id": "synthetic-attempt-1",
  "input_revision": 1,
  "backend_id": "codex",
  "backend_instance_ref": "synthetic-codex-instance-1",
  "route_revision": 1,
  "conversation_key": "synthetic-conversation-1",
  "session_epoch": 1,
  "runtime_profile_ref": "synthetic-bounded-runtime",
  "execution_policy_ref": "synthetic-readonly-policy",
  "messages": [{
    "message_id": "synthetic-msg-1",
    "received_at": "2026-10-23T16:00:00+08:00",
    "source_created_at": null,
    "text": "合成测试：总结附件",
    "attachments": [{"attachment_ref": "synthetic-att-1", "kind": "file", "availability": "local", "mime_type": null, "sha256": null}]
  }],
  "limits": {"expires_at": "2026-10-23T16:01:30+08:00", "hard_deadline": "2026-10-23T16:09:30+08:00"}
}
```

以上均为合成示例，不包含真实 message/context/lease 或宿主路径。时间统一用带时区的 RFC3339 字符串；公共层转换旧 epoch/create_time_ms，来源时间未知则为 null，不倒填。

3.1 附件按消息内顺序保存，允许文本+多附件。新附件对象至少有 reference、类型/未知、可达状态；MIME/hash/大小只有已核实才填写。旧 `media_paths` 无法恢复全部类型/逐项失败归属时保留 unknown 和原聚合失败数，不能把下载失败变成“没有附件”。语音原文件与已存在的转写分开，不声称后端自动支持 ASR。

3.2 附件引用由 01 的受限 resolver 管理，后端不能据任意路径读取其他用户文件。`local` 只表示公共宿主可读，不保证远程 Muse/云执行器可访问；上传、临时文件保留与删除策略由各线提出并经授权。无可用传递通道则明确返回 unavailable/unsupported，不静默丢附件。

3.3 多文件入站与多文件出站分别声明能力。现有 reply 只有单个 `media_path`，候选允许未来结果用数组表达意图，但未确定公共准入/失败恢复策略前，不把多附件出站标为已支持，也不由适配器绕过 sender 分别发送。

## 4. 后端选择与会话映射

4.1 推荐最小策略为**配置选择默认后端，每个逻辑任务只派一个后端**。是否支持微信内切换命令、按会话固定或按任务选择，由用户决定；不先做多后端广播。用户文本中的“忽略规则”等内容不能绕过既定路由鉴权。

4.2 创建 execution 前校验 backend 已配置、能力满足并持久冻结 route。未知/不可用后端显式拒绝或保持待处理，不默认切到另一家、换账号或复制旧 session 试跑。活跃 sender/批次不允许因配置刷新拆成多个 owner。

4.3 推荐持久映射键为 `(account_scope, sender_key, conversation_key, backend_id, backend_instance_ref, session_epoch)`；值包括 adapter 声明的 backend_session_ref/backend_refs、状态、版本与更新时间。会话的实际创建/续聊由适配器负责，公共层只存不透明引用并校验归属。并发更新需要原子 compare-and-set 或同一协调锁，不能最后写入者覆盖别家映射；不从桌面“最近会话”或全局 continue 猜测续聊目标。

4.4 `state/session.json`、微信 context_token、event/worker lease、backend_session_ref 是四种不同事物。公共层保留微信凭据及 reply context，普通新后端请求不传这些原值；回复目标由原收件记录派生，模型不能任意指定联系人。

4.5 Muse 当前可信 helper 路径会返回私有 lease 和带微信 context 的原记录。旧路径继续受现有权限边界约束；收窄请求视图是后续兼容迁移，不可只改 prompt 而遗漏 helper/包/Hook 配套。Codex/CC 不照抄这一历史暴露模式。

4.6 会话映射隔离还必须落实到**批次选择**。候选保留单 sender owner 锁，但后到消息只有匹配当前冻结 backend、backend_instance_ref、conversation_key、session_epoch 才能加入其 input_revision；不同 route/实例/会话/epoch 的消息保持待处理或明确拒绝，策略由用户确认，不能抢起第二个 sender owner。相同 backend 更换安装实例也不得加入旧批次或复用旧 session。现有 `_collect/plan` 仅按 sender 归并，不具备上述判断，不能直接复用为多会话调度器。控制消息的鉴权/分类、路由解析时点和持久化也须在实施前明确；审批/取消/切换指令不能仅合入普通正文让旧执行自行处理。

## 5. 逻辑操作与能力声明

这些是候选语义操作，**不是某家 SDK 的真实方法名或现有 CLI 命令**。

| 操作 | 输入 / 输出责任 |
| --- | --- |
| `describe_capabilities` | adapter 返回已核版本、宿主和支持/不支持/未知；不能由模型猜支持 |
| `start` | 接受带固定 execution/attempt/revision 的请求；返回 accepted 或明确失败，并提供真实 backend_run_ref（未知单列） |
| `observe` | 取得按序状态/事件/最终结果；轮询、流或回调由各线选择，但不能只拿“提交成功”当最终答案 |
| `update_input`（可选） | 将完整新 revision 纳入同一执行；必须说明能否避免重复工具副作用。未核实则不支持 |
| `cancel`（可选） | 针对固定 execution/attempt 发取消请求，区分 requested/confirmed/unsupported/unknown |
| `respond_approval`（可选） | 仅接受已鉴权、未过期、绑定当前 execution/attempt/approval 的决定；重复决定幂等 |
| `respond_input`（可选） | 绑定独立 question_id 与原 execution/attempt 的问答/选项响应；不将用户补充问题强行转换为允许/拒绝 |
| `resume_or_reconcile`（可选） | 重启后按已有 run/session 引用核对，区分可续、已结束和未知，不生成新任务替代核对 |

能力矩阵至少覆盖：纯文本、各类附件可达性、最终结果、session 续接、事件/状态、输入更新、取消确认、审批往返、重启核对、执行幂等/副作用可见性、最大无进展阻塞。每项附实际平台/CLI/SDK 版本、命令/证据与限制；未知不能当 true。

### 宿主与执行策略

runtime_profile_ref 由配置解析为已核的 executable/版本/摘要、允许 cwd、宿主语言/版本及环境变量白名单；execution_policy_ref 指向明确的 sandbox、工具与审批策略。用户文本不能扩展路径、注入命令或关闭审批。只传本次运行需要的环境，不继承整个宿主秘密环境。

公共包目前仍声明 Python >=3.8。可选 SDK 有更高或不同语言要求时，优先评估独立宿主进程边界；不为某后端未经确认提高公共包门槛，也不把某台 Mac 的 CLI 路径硬编码成公共默认。具体宿主与消息 framing 由各线提出后冻结。

除 lease 的 expires_at/hard_deadline 外，正式实现前还需确定总 execution deadline、finalize 预留、cancel grace、审批/问题有效期、最大无进展阻塞、消息/事件字节数与积压上限。adapter 使用单调计时处理本次剩余预算，校验最终结果与入队也要留预算；这些不是改变现有 90/570 秒。旧原型 600 秒超时不能原样套用当前 worker。

## 6. 执行状态与事件

候选事件最小字段为 contract_version、execution_id、attempt_id、input_revision、backend_id、event_id、sequence、observed_at、kind、payload。公共层校验 fence/归属；同 event_id 重放只处理一次，序号缺口触发核对，冲突的终态不能采用最后写覆盖。

| 执行状态 | 可确认的事实 |
| --- | --- |
| created / dispatching | 公共层已建任务 / 正尝试投递，未证明后端启动 |
| accepted | 真实适配层接受；不等于运行、完成或微信收到 |
| running | 有后端运行证据；进展需要可观察依据，不是后台心跳 |
| waiting_approval | 真实审批请求已持久登记，未获准执行受保护动作 |
| waiting_input | 等待真实问题/选项的用户响应；不伪造用户回答，不等同权限审批 |
| cancel_requested | 已尝试请求取消，不能提前报 cancelled |
| succeeded / failed / cancelled / timed_out | 仅根据对应后端的可验证结果进入；cancelled 不表示已经撤销工具副作用 |
| unknown | 连接断开、宿主退出或恢复无法判定；保留原身份并核对，不自动改成失败后重跑 |

状态不能随意线性覆盖：最终结果与取消竞态需唯一终态仲裁；本地超时后仍可能有后端副作用。适配器应同时提供 `side_effect_state=none/possible/confirmed/unknown` 或明确未知，不能承诺工具副作用严格一次。

### 后到消息与 input_revision

现有 reply 的 `needs_refresh` 保护必须保留。入队前如果同冻结路由/会话的 owned 集合变化，旧 revision 的成功结果不能静默覆盖新消息；不同路由/会话的消息按 4.6 隔离，不用增加当前 revision 的办法把它们纳入旧执行。

候选策略：支持 update_input 的后端先取得其确认，再生成覆盖新 revision 的完整结果；不支持的后端不能靠仅扩大 message IDs 掩盖遗漏，也不能无条件重跑整个任务（可能重复外部副作用）。它们需要选择“安全重组回答的受限模式”或新的 durable job/批次切分方案，经用户确认并经回归验证后使用；在此之前对该场景返回待处理/不支持，并阻止重复自动执行。该项是首轮跨线阻塞，尚未设计成已可运行的调度算法。

## 7. 结果与发送准入

候选最终结果包含 execution/attempt/revision、output_id/output_kind、覆盖的 message_ids、backend_session_ref（适用时）、终态、text、artifact_refs、error 和证据摘要。正文或产物必须真实存在；只有退出码 0、空 stdout、提交回执或文本“完成了”都不足以确认最终结果。

7.1 只有 01 能将合法结果交给公共 sender。准入依次检查：当前身份/lease/fence、账号与 sender、冻结 route、最新 input_revision 与完整 owned、内容/附件可读性及能力、固定输出身份。失败不写 complete receipt。

7.2 新后端不得填写微信 to/context 或手写 outbox。未来结果事件由公共层转换为当前 `reply`/批次准入；采用这一映射前，须明确 lease/fence/批次身份的持久关系并测试崩溃窗口。

7.3 单独记录三个维度：backend execution、queue admission、transport delivery。现有 `completed` 表示收件已经交给队列，`queued` 表示已入队；`sent/` 目录本身也不能直接当 API 接受：合并成员可能预先以 `covered` alias 放入 sent，必须沿 `merged_into` 查询 canonical envelope 的实际发送状态，不能给成员生成独立 api_accepted。API 接受仍不是手机可见回执；只有独立手机确认或明确可验证的接收确认才能标 recipient_confirmed。

7.4 发送器拥有分片、稳定 client_key、配文/媒体顺序、同接收者 FIFO 与退避。后端错误可带 retry_hint，但不能命令公共层换身份重发 uncertain/failed 或重新运行有副作用的工具。

7.5 当前 sender 的自动文本合并没有 backend/session 语义。候选先保守限制新输出只在 `(account_scope, recipient, backend_id, backend_instance_ref, conversation_key, session_epoch, execution_id, output_kind)` 全部相同时参与允许的合并；不同执行、审批请求、进展通知和最终答复默认不合并。缺上述元数据的 legacy 任务单独走旧 Muse 兼容域，不能与新任务混合。迁移不重算或替换旧 client_key、合并正文/成员与已确认分片进度；正常 sender 仍按旧协议持久更新 chunks_done、attempts 和退避状态。相同 output_id 的内容冲突要核对，不能最后写覆盖原内容。

7.6 legacy helper 有 stdout 业务 JSON 返回，也有 nonzero + stderr 结构化错误（并可能混有阶段日志）；兼容层必须区分 lease_lost、needs_refresh、校验失败与进程异常，不只检查退出码或把全部 stderr 当一个 JSON。新抽象错误格式可统一，但不能在迁移中丢掉原准入拒绝信息。

## 8. 取消与审批

8.1 当前 `release` 只释放消息 owner，**不调用后端取消**。未来取消请求需绑定原 sender/账号/execution/attempt，由公共层鉴权并去重；过期 worker 不能取消别人的新 attempt。需要取消的动作若无法核验，应报告 unknown，不声称已停。

8.2 排队前、准入后、API 接受后的取消语义不同。准入后的既有回复不能通过取消后端自动撤回；已发送微信和已发生工具动作不可声称回滚。队列撤销若要支持，必须另设与 sender 共享锁/状态的契约，不能删队列文件冒充取消。

8.3 审批对象至少绑定 approval_id、execution_id、attempt_id、sender_key、动作摘要、到期时间、决定版本；拒绝/过期/重复或跨会话决定不能放行。普通聊天里的“好”不能未经规则判别就变成审批授权，日志不保存秘密参数。

8.4 等人审批、问题响应或长任务可能超过现有 90/570 秒，waiting_input 与 waiting_approval 遵守同一闸门。推荐首阶段仅开放已验证能在现租约内完成的能力；不能保证的能力明确不支持/待确认。若需等待人类响应后自动续作，应先设计独立于短 worker 的持久 job/checkpoint、过期请求处理、重新取得 owner 与副作用核对；不能用无进展 refresh、放宽租约或简单 release→重执行替代。没有该方案前不发布审批/问题等待后的自动续作能力。

8.5 普通问题响应与权限审批分别绑定 question_id/approval_id 及后端请求 ID、动作/输入摘要哈希。一次允许不能自动变成永久权限变更；不默认 bypass/关闭 sandbox 来绕过不支持的审批通道。取消要覆盖当前执行和待执行输入，记录是否仍有未消费的输入/工具请求；仅中断当前轮次不能推断整个后端队列已清空。

## 9. 去重、超时和恢复

9.1 区分四个去重域：微信收件 `(account_scope,message_id)`；后端投递 execution/attempt；事件 event_id；出站不可变 batch/client_key。保持某一层身份不意味着端到端严格一次。

9.2 当前事件未 begin 的 180 秒、worker 90 秒软/570 秒硬、进展后建议 30 秒 refresh、每消息 3 次恢复预算原样保留。serve 健康检查与告警节奏是别的期限；不据平台摘要改变这些值。

9.3 旧 attempt 超时/失联后先核对 run 和副作用，再决定接管/取消；新 lease 不代表旧平台 worker 已死亡。后端无恢复查询时标 unknown 并阻断可能重复的动作，不能把下一次 wake 当作安全重做证明。

当前 fencing 主要保护消息/回复准入，不证明旧 worker 已停止操作外部工具；恢复预算和租约到期也不是后端工具幂等证明。候选执行层需要补上这层核对，不能声称已有代码已解决。

9.4 保留 `prepared → publishing → admitted` 的不可变登记与原消息覆盖。只有证明未开始发布的 prepared 才按原正文/路径/client_key 恢复；publishing、缺内容或结果未知需人工核对，不从临时会话记忆重造答复。

9.5 新增执行/路由/session/审批持久记录的文件名、锁顺序、schema 版本、原子性、保留期与迁移工具尚未确定。它们是候选契约实施前置项，不在本轮向真实 state 写新字段。

## 10. 首轮必须补齐的输入

| 来源 | 待补内容 | 未齐时的限制 |
| --- | --- | --- |
| 02 Muse | 真实 runtime/prompt/包版本及测试归属；worker/session/run、取消/审批、附件和输入更新能力 | 不判定 09:38 摘要根因，不改租约，不声称 Muse 满足新契约 |
| 03 Codex | 实际 CLI/SDK 版本、最终结果和 session 恢复、事件、取消/审批、输入更新、执行副作用核对 | 不把命令启动/提交回执当最终结果；不冻结其适配 API |
| 04 CC | 实际版本和模式、最终答案提取、session、事件、取消/审批、附件与重启核对 | 不把 CLI、SDK、云 session 的能力视为相同 |
| 用户 / 00 归口 | 路由选择方式、切换规则、审批/长任务首版范围、附件传递与保留策略 | 未确认前保持候选，不开始跨线重构或生产切换 |

## 11. 已读跨线输入快照

以下是本轮已读取的文档/报告输入，**不是 01 重新执行的真实后端验证**。原始私有证据保留在各线协调目录；这里只列脱敏要求和提交来源。

| 来源候选 | 已纳入的要求 | 仍未证明的能力 |
| --- | --- | --- |
| 03：`34d12228cf09a8b777dde35648627a286150384f` 的 `docs/lines/03-codex/INTERFACE-REQUIREMENTS.md`；报告给出独立文档/证据 QA PASS | task/run/session/thread/turn 分开；显式 cwd/沙箱/审批/环境白名单；唯一运行产物和成功终态同时校验；取消 requested/confirmed/unknown；执行预算给准入预留；旧 600 秒与 570 秒冲突 | 真实 final/续聊/事件/取消/审批未验；CLI/协议存在与合成探针不能当真实后端通过 |
| 04：`dcc58295e4f0a3932efa91c28d25d6f9cb142211` 的 `docs/lines/04-cc/INTERFACE-REQUESTS.md`；报告给出独立文档/证据 QA PASS | 独立宿主与公共 Python 门槛；安装实例会话命名空间；审批与问题响应区分；真实 error flag/终态/产物校验；取消涵盖待执行输入；消息/积压/期限上限 | 隔离认证错误/参数/SIGTERM 探针不证明真实生成、审批、续聊或执行中取消；SDK 类型声明不等于端到端实现 |
| 02：共享报告状态为“取证阻塞；报告索取尚无发送成功证据”（报告标注 2026-10-23 16:10:34+08:00） | 继续保留 legacy Muse 协议与版本取证闸门；不依据当前磁盘版本倒填今早运行事实 | 没有绑定版本的完整实测报告，不能确定今早根因或声称租约修复已实机通过 |

字段映射仍需各线确认：本候选 execution_id 对应逻辑 task 身份，attempt_id 对应桥内的一次 run 尝试；provider run/thread/session/turn 则进入 backend_refs。它们与本仓库开发工单 `WBR-*` 编号不混用。03/04 的原字段名不是另建一套独立公共 API 的许可。

适配器最终结果必须同时核对本 attempt 的终态、provider error 标志/停止原因、有效且新鲜的 final 产物，以及退出/信号证据；不能只看到 subtype/success 字样就通过。终态后迟到数据、断流和退出不一致进入核对；业务动作是否完成仍需要工具证据，不由模型正文自证。
