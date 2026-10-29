# Attachment inbox v1 · 003 公共契约候选

任务 WBR-00-20261027-003；唯一源码基线 e804be792ef08e985f94d7536c2926a597391150。
本文具体化已批准的 R2 语义，交 00 转 02 复核接入；接口冻结回执前不做跨线接线。
R2 SHA256 876db6df163e2a71b260a873f79ef222c4cbf53711dda6eebf706bf9eebb26be；
共识 SHA256 2cb1638ce6db3cf4caecc85450e062e7bf89df80c8d29acf43a934b92bdf0a39。
02 架构 → 05 公共实现 → 非实现者 06；00 负责最终组合 QA、Muse 复核及已授权发布。

## 1. 身份、版本和策略

1.1 `scope` 只有 `bot_id, owner_id, conversation_id, login_epoch` 四个非空字符串。
公共 helper 从当前有效 session 和仍活着的本人 worker batch 推导，不接受模型传入身份、token 或路径。
当前只支持已登录本人对话，conversation_id 为本人 ID。bot ID 缺失时用绑定本目录的独立 runtime ID，不能用 token 或其 hash 代替。
login_epoch 是登录 session 中独立随机 UUID：成功重登录更换，普通进程重启复用；旧合法 session 在受控持锁启动时只补一次。
附件长期归属为 bot/owner/conversation，接收 epoch 只作来源；同本人重登后仍可显式查目录。
即时快照、worker 活权和发送授权必须匹配当前 epoch；旧 epoch 的快照与发送任务拒绝继续。

1.2 所有新增结果带 `schema_version:1`。新附件组使用独立目录/数据库，不写入旧 sender 扫描的普通 pending 队列。
旧 helper/adapter 不支持新能力时明确 unavailable；禁止旧 sender 忽略附件字段、只发文字却报组完成。
新能力探测返回 `attachments_v1, references_v1, attachment_groups_v1`，旧 begin/refresh/reply/release 形状兼容。

1.3 默认策略可配置，都是本地策略：单件 104857600 字节、普通保留 30 天、总量 5GiB（含 pin），
最近组最长 24h、模糊单图关联约 15min、最多 16 原位置。阈值 90/570、120/180、发送 cap8、单 job 1800s 不变。
私人失败留样 OFF，生产 GC apply/delete OFF；不执行文件/宏、不解压或联网扫描，不自动导入历史不明归属材料。

## 2. A：原件与可恢复目录

2.1 `wechat_bridge.attachments.AttachmentStore(base, policy=None, clock=time.time, fault=None)` 持有新目录。
原件位于受控 `attachments/objects/`，暂存 `attachments/staging/`，索引为 `state/attachments.sqlite3`。
磁盘键不取原名；目录只记相对对象键，无 key、可用下载 URL、context_token、lease 或历史回复权限。
必须拒绝根/对象/索引链接及越界路径；读实体重新核对普通文件、size/hash。只读打开不建目录、锁或 schema。

2.2 记录至少包含 `attachment_id, bot_id, owner_id, conversation_id, received_epoch, message_id,
item_index, received_at, kind, original_name|null, name_source, declared_type|null, detected_type|null,
size, sha256, object_key|null, status, decryption_state, preview_state, tags[], expires_at, pinned`。
tag 为 `{value, source, message_id|null}`；source 只允许用户说明、同批文字、原名或明确标记的推断，推断不冒充确认。
unknown 原名为 null，不把 image.jpg 兜底名当原名。同 source message/item 重放同 ID；不同消息同字节仍是不同接收记录。

2.3 内部接口（仅公共受信接收层传 scope）：

```python
register_event(scope, message_id, items, text='', received_at=None) -> event
# items: [{item_index:int, kind, original_name:null|str, declared_type:null|str}]
# event: {attachment_refs:[{attachment_id,item_index,status,stage_path}], reference_context:null|dict}
publish(scope, attachment_id, staged_path, decryption_state='protocol_decoded',
        detected_type=None, preview_state='unavailable', tags=[]) -> metadata
fail(scope, attachment_id, reason) -> metadata
query(scope, filters=None, limit=20, cursor=None) -> {items,next_cursor}
acquire(scope, attachment_id, holder, ttl_seconds=1800) -> {path,metadata,hold_id}
release(scope, hold_id) -> None
pin(scope, attachment_id, pinned=True) -> metadata
resolve_reference(scope, snapshot_id, ordinals=None, holder=None) -> reference_context
preview_gc() -> {dry_run:True,...}
recover() -> bounded reconciliation result
```

2.4 register 在下载前预留最坏单件预算并登记 pending；额度不足不下载正文，登记明确 quota 拒收位置，文字/控制仍继续。
发布先安全写入/fsync 文件及目录，再在同一 SQLite 事务公布索引 ready 与图片槽 ready。
阶段 pending→prepared→ready 可幂等恢复：正式文件有而事务未提交时核原预备元数据后完成；ready 缺文件则 missing，绝不发失效路径。
同一消息重放不生成新 ID、不重复任务；索引损坏/未知对象不猜测恢复或删除。预算包含已用、预留及受控暂存，pin 不豁免计量。
所有 fault 点仅构造器测试注入，不能由模型/CLI 请求开启。SQL 事务与文件系统分别留故障证据，不称跨两者天然原子。

2.5 检索是本 bot 本人的有界日期/原名/标签/种类/状态查询；返回 ID 和元数据，不返回任意路径。
acquire 才返回可读受控 path，并持久建立在用保护。发送排队/在途/unknown 使用独立稳定 holder，不因 worker 完成失去保护。
到期只进待清理，实体尚在时仍可本人获取并建立保护；pin、有效快照、在用、待发、unknown 保留。
GC 只预览，认识版本化目录/索引；不因索引自己的对象键永久 pin 全部附件，不绕过旧 unknown-root 保护。
实际删除后仅允许 ID/状态/删除时间的最小墓碑，去除原名、标签及正文；墓碑默认再保留 30 天（可配置），实际删除/清退属于后置 C。

## 3. B1：原位置与固定引用

3.1 同本人、同 epoch 的连续图片事件构成一组；文字/非图片事件关闭当前组，之后新图片建立下一组。
同事件带图和文字时，先登记该事件全部图片原位置，再为文字建快照。跨进程重启复用未关闭组，最长不超过原 24h。
每个图片槽保留 `ordinal`（从1开始）、message_id/item_index、attachment_id 和 pending/ready/failed/quota/missing 状态。
失败不重排。超过 16 原位置明确 `overflow/total_positions`，不默默截断成完整小组；原件仍按容量边界逐件处理。

3.2 文字进入时持久固定 cutoff 接收序列和 snapshot_id；快照成员/顺序不可变，其版本与最新组 revision 分开。
后续 pending→ready 可更新同 ID 的可读状态，正常新图/新组不能否定仍有效的旧快照或让其指向另一张。
快照自身 expiry 不晚于该组原 24h 截止点；重登、越界、缺实体、过期分别返回明确状态，不退到更旧组猜图。
`reference_context` 至少有 schema_version/snapshot_id/cutoff_seq/group_id/created_at/expires_at/slots/overflow。
旧引用不加入 owned/claim/receipt，不重新执行或完成历史任务。

3.3 公共层只提供事实与候选，不按“这个呢”强行选图。02 用明确指代或短窗口内明显需看图且唯一的候选决定是否申请指定原序号；不清楚先澄清。
02 每次用 snapshot_id 向公共 helper 重读指定子集，再实际打开返回的可读图片；拿到 ID/hash/path 不算模型看过。
未知/HEIC无已验证解码器仍可保存原件并按文件回传，不能报已读图。模型真读图/Hook采用由02及最终组合另验。

## 4. B2：一个逻辑回复、物理单元与预算

4.1 `wechat_bridge.attachment_delivery.DeliveryLedger(base, clock=time.time, fault=None)` 使用独立 `state/attachment-delivery.sqlite3`。
组固定 schema/type=`attachment_group_v1`、group_id、原请求 ID/digest、本人归属、epoch、有序 attachment_id 清单和 caption。
文本按实际 split_text 分块；每个 chunk、附件、状态说明都是稳定 unit_id/client_key。文件名用元数据原名的安全展示名，不能靠磁盘对象名还原。
附件先在 catalog 建稳定 group holder 再登记组；中断恢复保守保护，无可证归属的 orphan hold 不自动删除。

4.2 内部公共接口：

```python
register_context(scope, context_token, fresh=False) -> context_ref
create_group(scope, request_id, attachment_ids, caption='') -> group
prepare_page(scope, group_id, request_id, context_token, explicit_continue=False) -> job
next_unit(scope, job_id) -> unit|null
begin_unit(scope, job_id, unit_id) -> unit
settle_unit(scope, job_id, unit_id, outcome) -> status
group_status(scope, group_id) -> status
cancel_group(scope, group_id) -> status
reserve_units(scope, context_token, unit_ids) -> reservation
begin_reserved(scope, context_token, unit_id) -> state
settle_reserved(scope, context_token, unit_id, outcome) -> state
recover() -> status
```

4.3 共享预算覆盖 serve 普通文本/配文/媒体、新组和 CLI send，保持 cap8。额度 key 是私有 scope+context 摘要，仅用于计账，不是登录代次或授权。
预留→inflight 必须在可见发送前持久；accepted 消耗、proven_unsent 才返还，unknown 仍占用且不自动重发。
只上传未调用 send 的可证失败可返还；超时/断连/无法解释的响应不得用普通失败退款。inflight 重启进入 unknown。
同稳定 ID 重放不重复扣账或发送；并发原子预留。旧组/正文不能靠重启归零获得8次新机会。

4.4 升级创建账本时，已有 last_context/inbox/旧 pending/sending/failed 中不能证明余额的 context 标 legacy_unknown，保守占满8。
只有账本启用后 receiver 登记的新当前 context 才可开新预算；已知 legacy_unknown 同 token 不因新消息重复出现而重置。
普通旧任务可沿旧兼容规则等后续新 context；新组只能在明确继续选定组后建新 job，不能被 serve919–926 自动升级接管。
未登记的任意 CLI context 默认 unknown，不默认为0；旧能力仍可发送，但必须受共享预算和当前本人权限限制。

4.5 prepare_page 先计算可发页并预留实际物理单元，包括必要的本页进度说明；无余额不额外发送说明。
新组 job 与旧普通队列隔离，独立 fence/owner/epoch/取消状态；入队后不再依赖已经 complete 的 worker lease。
组进度跨 job 保留；每个 job 仍1800s，过期不复活。之后用户明确选组“继续”才以当前新请求/context 建关联 job；多待发组先澄清。
已接受不重发，unknown 先核查，只有可证未成功项可续；换话题不续发。保留有限重试，不无限刷新或重试。

## 5. Adapter 调用与文件 owner

5.1 01 在 WorkerState 增加受活 lease/owned IDs 保护的统一 `attachment_call(ids, lease, request)`；
wire/CLI 与成功/失败字段以附录 A 为准，不要求02直接调用 AttachmentStore 或自行构造 scope。
与现 reply 一样，提交前收集新到消息，owned 集变化返回 needs_refresh，不提前 complete。
请求只传 attachment_id/snapshot_id/原序号/有限过滤/选定 group_id；公共层核当前本人、epoch 和实体。
get/reference 返回受控 path+保护，02 实际读取后释放；多件回复一次提交 ordered IDs+caption，不能循环关闭旧句柄。
reference_context 随 begin/refresh 的 messages 传递，旧消息字段保持；02 需显式保留新字段/能力，不复制目录真相。

5.2 01 owner：attachments.py、attachment_delivery.py、新公共测试、store/serve/worker_state/cli 接线、media/crypto/image_validation 的必要兼容修改、media_retention及打包。
02 owner：hooks/muse-worker.py、Hook测试、提示及部署检查；收件箱真实投递目标保护。01不写Hook业务、不直接给02派单。
同公共文件仅本体接线；内部agent只写指定新模块/对应测试；非实现者06不写源码/仓库测试。

## 6. 解密、稳定与阶段退出标准

6.1 保留协议明确 hex/base64+ECB 快路径；按协议字段生成可接受候选，只有同一明文字节才可合并。
正常可读图正常ready；协议解出的未知格式为 saved/preview_unavailable；padding-only 不记 content_verified。
不同可接受候选冲突为 decrypt_ambiguous，不把评分最高者当原件，不默认保存密文/key/多个失败候选。
旧 download_media 默认仍返 path，新增结构化元数据通过可选输出/新内部函数；旧成功来源顺序不无故改变。

6.2 下载读取、保存/索引提交、上传分段、微信调用前后真实阶段进展才刷新服务心跳；有取消/失权检查。
失权/取消信号须穿透广泛异常重试，在最终可见 send 前复查；不能用无条件后台报活掩盖卡死。
worker refresh 仍须实际进展且不超过原硬期限；长工具平台边界由02核验，不假定CLI接口能保证平台不睡眠。
正式 reply 占位不实现。收到、保存、索引可查、Muse接手、API接受、手机收到分别留证。

6.3 A 退出：单件原字节、来源/原名、同名隔离、quota预留竞争、文件/索引故障恢复、跨批本人取回、dry-run保护。
B1 退出：下载前pending、九图原第2张/首图失败、文字固定截止、新图不换旧快照、重启/epoch隔离及02实际读所指图。
B2 退出：一次多件提交、顺序/部分/unknown/恢复、物理预算全入口一致、旧余额迁移、明确继续/30min新job、旧sender不误完成。
A+B1+B2+稳定链、最终固定组合全离线/故障矩阵/安装核对、独立QA及Muse复核共同构成完整首版；A不是整单交付。
任何原FAIL保留，新SHA按影响复验；第三同类FAIL交00。生产删除后置，真实手机验收待人类明日测试，01不代00发布云端。

## 附录 A · 可直接接线的公共 wire surface

A.1 唯一 CLI 为 `python -m wechat_bridge.worker_state`，全局 `--base BASE --state-dir STATE` 位置沿旧命令。
`attachment-capabilities` 不需要 lease，不构造可写 Store，不探测账号/真实服务；输出下列固定能力对象：

```json
{"schema_version":1,"protocol":"attachment_inbox_v1","version":1,
 "features":["attachments_v1","references_v1","attachment_groups_v1"],
 "limits":{"request_bytes":65536,"response_bytes":65536,"query_limit":20,"attachment_ids":16,"reference_ordinals":16}}
```

新版 begin/refresh 在**最外层**增加 `capabilities`，值为同一对象；原 owned/messages/lease 等位置不变。
每个 `messages[i]` 可有 `attachment_refs` 和 `reference_context`，旧消息缺字段按 `[]`/`null`。
attachment_refs 白名单为 `attachment_id,item_index,kind,status`；不把 stage_path/object_key/scope/token 传给02模型。
reference_context 白名单恰为 `schema_version,snapshot_id,text_message_id,cutoff_seq,group_id,login_epoch,
created_at,expires_at,slots,overflow,total_positions`；slots 白名单为
`ordinal,message_id,item_index,attachment_id,received_at,status`。ordinal从1，item_index为协议0起原位置；overflow为bool，total_positions为int。
slot.status 是事实观察，pending/ready/failed/quota/missing；附加 unknown/version 不受支持时不静默剥掉再猜图。
`saved`/`preview_unavailable` 是展示含义，目录机器状态用 ready，另以 preview_state 表示能力。

A.2 02 从自己的现 handle/revision 取**私有** lease 和 owned（有序字符串 ID 数组），调用：

```text
python -m wechat_bridge.worker_state --base BASE --state-dir STATE \
  attachment-call --ids MID1 MID2 --lease LEASE --request-file PRIVATE_JSON
```

Python 等价调用为 `state.attachment_call(ids, lease, request)`。request只有
`{"schema_version":1,"action":ACTION,"arguments":{...}}` 三字段；拒绝未知键、bool当int、未知版本和大于64KiB输入。
所有动作先核活worker及当前epoch，重新收集已到达的同本人消息，但**不自动续租**；owned集合变化返回 needs_refresh。
只有旧 refresh 显式续租，02在真实进展后调用并重读整批/revision。query/get/reference/status/pin/release 都不完成本批。
reply/continue只有成功入队后才完成当前全部owned；若未知结果，沿旧adapter replying/关闭句柄规则，不自动重复提交。

成功统一 `{"schema_version":1,"action":ACTION,"ok":true,"result":{...},"error":null}`；
失败统一 `{"schema_version":1,"action":ACTION,"ok":false,"result":null,
"error":{"code":CODE,"retryable":false,"refresh_required":false}}`。
needs_refresh时refresh_required=true，02调用refresh，不采用失效结果；storage_busy/processing可retryable=true，仍不延原硬期限。
错误码为固定短码，无异常原文、SQL/路径/凭据：`unsupported_version,unsupported_action,invalid_request,
response_too_large,lease_lost,needs_refresh,epoch_mismatch,not_found,scope_mismatch,attachment_unavailable,
snapshot_expired,processing,quota_exceeded,storage_busy,storage_invalid,hold_forbidden,group_unknown,
group_busy,continue_required,budget_exhausted,cancelled,expired`。未知底层错误归storage_invalid，不假装空结果。

A.3 动作与精确业务字段（除标optional者均必需；所有ID为非空有限字符串）：

| action | arguments | result |
| --- | --- | --- |
| query | optional filters/cursor/limit；limit默认20、1..20。filters仅 received_after/received_before（有限epoch秒）、name_contains/tag（最多128字符）、kind/status（受支持枚举） | items为下述公开metadata数组；next_cursor为null或不透明页游标；complete为bool。游标绑定本人和规范化filters，不能换条件/越界使用 |
| get | attachment_id | attachment为公开metadata；material为 `{attachment_id,path,hold_id,expires_at}`，expires_at为该普通读hold原有界截止；未ready/缺失/越界不返path |
| reference | text_message_id,snapshot_id；optional ordinals（1..16互异int数组，缺省请求快照内所有原位置） | reference_context为A.1形状；materials数组，每件为 `{ordinal,attachment_id,path,hold_id,expires_at,preview_state,detected_type,size,sha256}`；unavailable数组为 `{ordinal,attachment_id,status}`；complete为所有所选槽可用bool |
| release | hold_id | `{released:true}`；仅释放当前活批自己derive holder的hold，永不调用旧worker release或关闭本批 |
| pin | attachment_id,pinned（bool） | `{attachment:metadata}` |
| reply | attachment_ids（1..16互异有序ID），optional caption（默认空，最多16000字符） | queued:true,delivery_confirmed:false,group_id,job_id,status（见A.4） |
| continue | group_id | queued:true,delivery_confirmed:false,group_id,job_id,status；只用当前新请求/context，不接受客户端传旧token或旧job |
| status | optional group_id/cursor/limit（1..20、默认20） | group_id指定则 `{group:status}`；不指定则 `{groups:[status],next_cursor}`，仅列本人未完成/unknown组，供一个可续或多个澄清，不靠adapter缓存组真相 |

公开metadata白名单：`attachment_id,message_id,item_index,received_at,kind,original_name,name_source,
declared_type,detected_type,size,sha256,status,decryption_state,preview_state,tags,expires_at,pinned`。
tags白名单 `value,source,message_id`。名称最多4096 UTF8字节、标签最多32项且每值128字符；原名null表示未知。
path仅在get/reference成功material中返回，为公共核对过的本机受控文件，不是任意文件读取授权。
64KiB响应预算优先缩小query/status页并给next_cursor，不静默截掉某附件的必要字段；单件仍超限则response_too_large。

A.4 group status白名单：`schema_version,group_id,created_at,state,attachment_count,
accepted_attachment_ids,pending_attachment_ids,unknown_attachment_ids,units,can_continue,requires_explicit_continue`。
state为queued/sending/partial/awaiting_continue/unknown/complete/cancelled/expired之一；units为
`{total,accepted,pending,unknown}`整数计数，附件数与物理单元数分开。caption未知也令can_continue=false，不因unknown附件数组空而允许重发。
requires_explicit_continue恒true；仅无活动job、无unknown且尚有可证明未成功单元时can_continue=true。
continue须有当前新owned请求，且公共层核到文字明确要求继续/发剩余，含否定/取消或不明意图时continue_required；不能仅靠一个客户端布尔值授权。
group_id由本次明确选择或唯一候选确定，多个必须澄清；status发现列表不包含旧正文、context或接收者权限。

A.5 hold归属绑定 `scope + 当前epoch + 当前worker lease` 的不可变派生holder，不接受模型传holder。
同活批重复get/ref可复用同hold；release必须匹配此holder。同批已释放/已过期hold重复release幂等 `{released:true}`，
别的批/越界hold返回hold_forbidden，即使知道hold_id也不能释放。失权/epoch改变先拒绝，不以release偷续命；普通hold至原期限自然失保护。
group持有者另有受信发送器命名空间，不允许worker release释放；queued/inflight/unknown持久保护不因1800秒job到期自行消失。
底层AttachmentStore.release可返回None，与上述公开JSON封套无冲突。

A.6 reference必须校验text_message_id在当前owned，且该inbox记录的snapshot_id/cutoff/固定成员与目录持久快照吻合。
adapter revision/digest绑定当前owned/text及不可变引用身份（schema,snapshot_id,text_mid,cutoff,group_id,epoch,成员ID/原序号）；
不拿“最新group revision”作权限，不因同ID pending变ready就重新挑对象。新增消息导致owned变化照旧needs_refresh。
过期、失权、未知schema、快照不吻合均失败，不悄悄改成另一个快照。reference成功materials才可供02实际读图，接口返回本身不证明模型已读。

A.7 可选内部真实进度回调统一为 `progress(event)`，event白名单为
`{"schema_version":1,"stage":STAGE,"completed":N,"total":null|N}`；
stage为download/read/decrypt/persist/index/upload/send_before/send_after，数字非负且单阶段单调。
回调先执行check_cancel/owner/epoch/fence/期限检查，可抛固定OperationAborted；取消/失权异常穿透重试，最终send前再查。
服务记录真实阶段变化或字节推进，不能无条件定时伪造进展；该callback不自动续worker lease。
02实际工具若会阻塞，须分短步骤，在完成真实工作后显式refresh；本契约不声称Muse可在任意长工具内部获得调度保障。

## 附录 B · 已请求的有效短窗口增量

B.1 `capabilities` 同一对象增加 `policy:{ambiguity_seconds:N}`，N为非负整数秒，来自**当前实际有效配置**。
receiver/helper共同从 `state/attachment-policy.json` 读取受支持的AttachmentStore策略；无文件取其默认，坏配置明确不可用，不冒充默认已生效。
能力探测仍只读，不为此创建Store/库/目录；begin/refresh返回同源有效policy，不能各写一个900。
该字段在本批新实现必有；旧6a能力没有policy或没有合法ambiguity_seconds时，02仅对模糊短句保守澄清，明确点名原位置照旧可用，不推定900秒。
0表示关闭模糊自动关联。数字是可配置本地策略，不是微信/Muse平台事实。

B.2 短窗口起点取所选slot.received_at（图片接收时间），不是新文字的snapshot.created_at。
02还须满足唯一、明显需看图、无否定/话题切换等原规则；拥有窗口数字不等于允许强行猜对象。
snapshot原expires_at/epoch/cutoff约束继续独立检查。64KiB response_bytes只约束attachment-call封套，不截断旧begin/refresh完整messages。
progress计数按一次实际操作的阶段单调，多个附件分独立操作；下载换源累计实际读取量，不把同阶段从0复位当新进展。
kind为image/video/file/voice；公开status为pending/ready/failed/quota/missing（内部prepared观察为pending）。
decryption_state为pending/unavailable/protocol_decoded/content_verified/plaintext_verified；preview_state为readable/unavailable/invalid/resource_limited；
tag.source为user/batch_text/original_name/inferred。结构可读验证仍不是来源认证。

## 附录 C · 用户明确标签的受控补记候选

C.1 本增量补R2“先发图，下一条文字明确说这是房东图，晚上按标签找回”的实际链路，交00转02核对。
能力features增加 `attachment_labels_v1`；旧helper没有该feature时02不假称已经标注，不把普通单件reply当标注入口。
统一attachment-call新增action=`label`，仍遵守同一活lease/owned/epoch/needs_refresh、64KiB封套和不自动续租规则。

C.2 arguments必有 `text_message_id,label`。label为1..128字符，必须逐字出现在该当前owned文字原文中；
否定/取消或不能证明用户明确标注时拒绝invalid_request并由02澄清，不从图像猜“房东/房客”。
对象选择为以下两种互斥形状之一：

- `snapshot_id,ordinals`：ordinals为1..16互异原序号，必须和text_message_id的固定快照绑定相符；公共层从快照选attachment IDs，不接受最新组替代。
- `attachment_ids`：1..16互异有序ID，用于已明确选定的本人历史目录结果；逐件复核归属，不接受路径或第三方目标。

result精确为 `{"attachments":[公开metadata]}`。索引原子补记 `{value:label,source:"user",message_id:text_message_id}`，同标签来源重复幂等；
不修改原名、顺序、快照、正文、receipt，不重做旧任务，也不声称模型看过图。待处理原件可先标记，随后ready发布保留该明确标签。
标签最多32项；超过上限整次拒绝quota_exceeded，不半批改写。已有明确标签不被后来的publish覆盖。

C.3 同事件短文字可作为有来源batch_text标签；长于128字符的配文不整体塞进单标签，也不截成虚构标签。
可不生成自动标签，**原件保存不得依赖自动标签是否生成或已满**。需要明确标签时用C.2当前用户原文入口。
本增量只存有依据的有限标签及来源消息ID，不建近期全文档案、不推断人物角色；晚上query filters.tag即可找回。

## 附录 D · 已入队组的明确取消入口候选

D.1 模块已有cancel_group及发送前失权检查；本增量使R2取消保护可由当前本人新请求实际到达，不让02直接调用账本。
features增加attachment_cancel_v1；attachment-call增加action=`cancel`，arguments精确为 `text_message_id,group_id`。
text_message_id必须当前owned，文字必须明确要求取消/停止/别再发所选组；多组仍先status并明确选择，不由最新组猜测。
能力缺失时不假称已取消。该动作仍检查活lease/epoch/归属，不自动续租、不complete当前批；02可随后用当前普通reply诚实说明。

D.2 result精确为 `{"group":A.4状态}`。原子停止尚未开始的单元并返还其可证未消耗预留；inflight保留unknown占用。
同本人重复取消幂等；旧worker或foreign组拒绝。下载/上传/最终send前重查取消状态，取消信号不得被内部重试吞掉。
不能承诺撤回已交给网络的调用、已accepted或手机已收消息；这种竞态保留accepted/unknown事实与对应保护，不伪装全未发。
新明确“继续”不能复活cancelled/unknown旧job，另行请求的边界仍依原契约。此为003取消接线具体化，交00转02核对，不改变账号、平台权限或其他开发线。
