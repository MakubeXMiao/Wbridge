# Muse 附件适配 · 003

本批从固定 e804be792ef08e985f94d7536c2926a597391150 开始。公共接口以固定契约 a42349f34369e53aa8876fe555fdff20c1801edd 的 `docs/lines/01-core/ATTACHMENT-INBOX-V1.md` 及附录 A/B/C/D 为准；具体安装版本以最终组合 manifest 为准。本文说明适配流程，不是独立 QA、平台采用或手机验收回执。

## 1. 兼容与权限

1.1 Hook 仍提供解释器、base、state-dir 和同目录 adapter 的绝对路径，`muse_adapter.protocol=1`；不能由消息正文覆盖这些值。旧 begin/refresh/reply/release 请求和普通回复保持兼容。公共包没有新能力时，新附件操作明确不可用，不能假装旧单件回复已支持多件。

1.2 新公共包在 begin/refresh 最外层提供 `capabilities`；每条当前消息可有 `attachment_refs` 和 `reference_context`。adapter 仅输出固定字段和类型，保留原顺序，拒绝未知版本/无效必需字段；未知敏感子树不遍历或回显。持久会话只留当批身份、引用摘要和调用时间，不保存另一套附件目录、历史消息或完整图片引用真相。

1.3 历史图只作为当前请求的材料，永远不加入本批 owned/claim/receipt，不重新执行旧任务。begin 仅使用 Hook 给出的 lease；之后附件扩展请求不能自带 lease、ids、收件人、context、holder 或任意磁盘路径。公共 helper 从当前活权推导本人身份、epoch 和 holder。

## 2. 扩展请求

2.1 模型仍通过已部署 adapter 的 JSON stdin 调用。外层增加一种 `action=attachment`，内部只映射冻结的 `WorkerState.attachment_call(owned, lease, request)`，不复制公共目录、发送器或预算实现：

```json
{
  "action": "attachment",
  "handle": "begin返回的句柄",
  "revision": 1,
  "request": {
    "schema_version": 1,
    "action": "query",
    "arguments": {"limit": 5}
  }
}
```

以上句柄、revision 仅作形状说明，实际必须来自本批返回。内部动作是 query/get/reference/release/pin/reply/continue/status，以及具备 `attachment_labels_v1` 时的 label、具备 `attachment_cancel_v1` 时的 cancel；参数精确按公共契约，拒绝未知键，不自行构造内部路径或旧发送上下文。

2.2 成功返回 `ok/handle/revision/result`，其中 result 为公共业务结果的安全投影。新操作错误返回固定短码、must_stop、retryable、requires_refresh 等标志；不附原始异常、SQL、私有输出。needs_refresh 或 revision_mismatch 后必须显式 refresh，重读完整本批并重组答案。

2.3 读取操作不关闭当前任务、不隐式续租。reply/continue 入队前先持久记录 replying；异常或未知结果闭锁，不能换句柄或重试。成功入队后当前句柄关闭，`delivery_confirmed=false`，查询后续进度必须使用后续合法新批；不能在已经关闭的句柄上调用 status。

2.4 64KiB 只限附件调用的新封套，不能套给 begin/refresh 的完整文字或合法引用记录。回复清单的16项上限不限制入件 attachment_refs；保留公共返回的全部引用和明确 overflow，不通过过滤失败位置、重排或静默删字段伪造完整结果。字段类型/值域、快照槽数、查询页及实际调用清单仍按公共契约验证；不添加新的入件数量或整批字节帽。原文长期归档不在本批。

2.5 引用卡最多16槽是硬上界，公共有效策略可以只保留1..16中的较少槽位。净化按该卡的实际slots与total_positions验证overflow：总原位置数大于已保留槽数即为true，不能硬写总数大于16。原序号仍须在1..16、严格升序且不重复，总数不得小于所含原序号；不重排或滤掉失败/pending位置，不从别批或新策略反推改写已有卡。

2.6 普通旧 reply 的 media_path 在现有会话阶段、revision、needs_refresh 检查后、写 replying 和调用公共 reply 前，复用 `wechat_bridge.attachment_runtime.is_catalog_original_path` 的只读判定。确定属于受管原件时，用既有非终止 invalid_request 封套拒绝，不调用公共 reply、不改会话或回复登记；同一仍有效的 handle 可由模型显式改用正确 attachment ID/group。活权、本人和 epoch 仍由真实公共调用核验，预检不续租或建立新 handle。canonical、符号链接和目录身份识别使用同一公共实现，不在适配层复制。

只有旧公共包确实缺整个 attachment_runtime 模块且当批没有附件 capabilities 时，普通旧媒体维持兼容。已有附件能力却缺 guard、模块导入损坏或 guard 出错均固定错误停手，不降级调用公共 reply、不回显私有路径或异常。此路证明尚未调用公共 reply，不改已处于 replying/uncertain 的会话；真实公共 reply 调用后的未知 ValueError、异常或结果仍 uncertain/must_stop，不能据错误字样猜测未发布。普通生成 PDF 等非受管文件仍经原回复队列和公共计账。

## 3. 固定引用与真实读图

3.1 明确指图时使用当前文字绑定的 text_message_id、snapshot_id 和原 ordinal 调 reference。每次使用前重新向公共层取这份固定快照，不能改查最新图片组。失败或 pending 的第一张仍占第一张；“第二张”始终指原第二张。新图不替换已绑定文字的对象；缺失、过期、代次变化和处理中分别说明。

3.2 模糊短句的有效窗口来自 `capabilities.policy.ambiguity_seconds`，不是硬写 900 秒。0 关闭模糊自动关联；旧能力缺 policy 或值非法时先澄清，明确点名仍可按有效快照取用。窗口起点用 slot.received_at，不能拿文字快照 created_at 当图片时间。仍须原位置唯一、明显需看图、没有其他指代或话题切换；不能过滤 failed/pending 后制造“唯一”。

3.3 reference/get 返回的受控 path 仅表示取得了候选材料和读保护。2026-10-27 Muse 主聊天只读回报其可见工具定义为 `muse.read({"path":"文件路径"})`，声明支持 png/jpeg/gif/webp，limit/offset 为文本分页；这不是唤醒 worker 的可用性证明，也没有新目录成功读图证据。模板要求当前 worker 确有该工具时按真实定义读取，成功返回图片上下文后才回答细节；不可用或失败就说明，不能改用未授权第三方识图服务。

ID、路径、哈希、Pillow 可打开或离线 reader 替身均不证明模型看过图。真实 worker 工具可用性、格式/路径可达、实际读到指定像素与 prompt 采用，仍应由对应现场回执/合成输入单独绑定；当前代码交付不能冒称这些已通过。

3.4 成功读完或确定无法读取后，在活权有效时逐件调用内部 attachment release 释放 hold_id。它不等于外层 action=release：后者会关闭整个任务。不得释放别批或发送器的保护；失权后停止，不能通过释放操作偷续租。未知 hold 结果不自动补动作。

## 4. 检索、回传与真实进展

4.1 已有本批 attachment_refs 时直接按需 get 指定 ID，不为单图先遍历历史。跨批取回先 query 有界目录，按当前本人、日期、原名或有来源标签定位；同名/多批歧义先澄清。原名未知不编造，未知格式可保存/取回与模型可预览分别说明。

4.2 多件只在一次内部 reply 中提交有序 attachment_ids 和一条 caption，不循环旧 reply，不用 media_path 绕过组权限。公共层负责稳定发送单元、共享持久预算与部分进度；一个逻辑回复不等于一个物理发送单位。API accepted、明确未成功和 unknown 分开，不能把 unknown 当未发重试。

4.3 status 只查当前本人组。只有当前新请求明确继续并选定组，才调用 continue；多个候选先澄清，换话题不借新上下文偷续旧文件，不复活旧 job 或重置预算。

4.4 query/get/reference 和工具调用分短步骤，只有实际推进后才显式 refresh，并重读 messages/revision。保持 90/570 与服务 120/180 阈值；公共进度回调和服务心跳不是 worker 续租。正式 reply 不用作“稍等”占位，没有平台调度证据不能承诺任意长工具不失联。

4.5 “这是房东图”等明确用户标签可通过 label 补记。请求带当前 owned 的 text_message_id 与字面 label，并选择该文字固定快照/原序号或明确目录 ID 清单，两种形状互斥。不能从图像推测身份，疑问、否定或意图不清先澄清。公共层核原文字面来源、本人归属、原子32项上限和幂等；adapter 验净化结果，成功后才说已标注。该动作不关闭任务或续租，不修改原名/顺序/快照或建立近期全文档案。旧 helper 没有 label feature 时明确不可用；pending 的明确标签必须在后续发布保留，普通原件保存不能依赖自动标签生成。

4.6 取消已入队组需要当前新批明确取消文字的 text_message_id 和选定 group_id，使用 cancel，公共层核当前本人、epoch 与意图。它不关闭本批、不续租，可随后普通 reply 如实说明；外层 release 不能替代它。只取消尚未开始的发送单元，已 accepted 或在飞 unknown 保留事实和保护，不能承诺远程撤回。未知、取消或过期旧 job 不能靠 continue 复活；缺 cancel feature 就说明能力不可用，不假报成功。

4.7 对原名/类型已有证据识别的可执行文件、安装包或带宏文件，仅在用户明确取回/下载时于配文给简短来源确认提醒。不执行、解压、自动预览或用外部程序打开不能预览的文件，不通过联网扫描私人文件来判断类型，不承诺无病毒。保存成功或原件哈希一致不证明安全或发送源身份；普通文件不堆警告。该提示沿用公共配文和共享发送预算，不增加动作或 schema。

## 5. 验证与更新

5.1 适配自测、固定 SHA 的非实现者 QA、公共组合/构建安装检查、Muse 对实际收到固定源码的复核分别记录；离线替身通过不升级为真实读图或手机通过。原失败记录保留，修复绑定新候选后按影响复验。

5.2 本批实现、检查及通过后的正常受控更新已有条件化授权；仍须满足上述门槛，不再重复要求同一批准。现场旧号、测试模式及“微信需求收件箱”投递点保留，目标保全检查见 `MUSE-ATTACHMENT-003-DEPLOYMENT.md`。更新前实际配置/依赖/加载文件和回退材料另行绑定，不能拿旧 pre-012 快照覆盖后来配置。

5.3 私人失败留样 OFF、生产 GC 仅 dry-run 零删除。不清空或恢复旧队列、receipt、游标、账本或新附件索引来做回滚。存在新版未完成/unknown 发送组时，先冻结并核对兼容门槛，不能让旧发送器盲接。
