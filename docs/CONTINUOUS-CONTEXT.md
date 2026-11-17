# 连续上下文公共接线 v1

任务 WBR-00-20261108-CONTINUITY-IMPLEMENT，基线 d797bd38ca414604b9aabb17a73e98837d2f9914。
本说明冻结 01/02 的本地接口。默认关闭，当前交付只含代码和离线合成证据。

## 1. 能力与读取

`wechat_bridge.context_runtime.capabilities()` 返回独立能力对象。`WECHAT_RECENT_CONTEXT=1`
启用 `recent_context_v1` 和 `memory_delta_v1`，`WECHAT_CLOUD_CONTEXT=1` 启用
`cloud_context_v1`。不修改原附件 capabilities、batch/reference digest 或图片快照。

在原 begin/refresh 成功、拥有权已核后调用
`state.context_for_batch(batch, cloud_binding=None)`，返回本地 `ContextRead`：

- `projection` 是可供 adapter 净化的字典，按能力包含 `recent_context_v1`、
  `cloud_context_v1` 及有限降级原因。近期项包含版本、条目 ID、四个摘要字段、来源
  序号/时间/引用及原期限，不含原话。近期最多 12 KiB，云端最多 4 KiB，总计 16 KiB。
- `basis` 是 adapter 私有会话保存的签名快照依据，仅供终结/回查传回，不向模型投影。
  绑定 scope、读取时快照 ID、当时可见的记录 ID 和期限。refresh 丢弃旧候选并替换 basis。

`AttachmentStore.context_sources(scope, owned_records)` 内部只读查询真实 events.seq，
核本批权威 inbox 身份、login_epoch、message_id、received_at 和事件指纹。无真实序号
就降级 `sequence_unavailable`。不 register_event、不初始化附件、不新增计数器。

## 2. 终结与可选摘要

普通终结调用 `state.reply_with_context(ids, text, lease, media_path=None, *, handle,
revision, basis=None)`。附件终结调用
`state.attachment_call_with_context(ids, lease, request, *, handle, revision, basis=None)`。
两者返回本地 `ContextResult(result, context_capsule)`，其中 `result` 与原调用返回完全一致。
旧 `reply`、`attachment_call` 继续保持原形态。附件 request 不增加记忆字段。

adapter 依次执行原 result 校验、保存原 queued，再在独立异常边界调用
`state.context_commit(context_capsule, memory_delta_v1)`。会话保存不明、业务 unknown、
needs_refresh、附件失败或外层 wire 校验失败均不得消费记忆。无需再请求活租约。
capsule 是同进程一次性对象，绑定 Store 实例、最终 owned/scope/cutoff、handle/revision
及原 job/group，只在队列接纳和处理完成已证后签发。不可 JSON 序列化，不进附件 result、
原 64 KiB 校验对象、日志或模型响应。

`memory_delta_v1` 位于 Muse 终结外包络，是可选字典：

```json
{"schema_version":1,"handle":"adapter-handle","revision":1,
 "source_message_ids":["owned-message-id"],"topic":"","intent":"",
 "bot_action":"","open_loop":"","historical_refs":[]}
```

来源集合必须精确覆盖当前 capsule 的本批消息。`historical_refs` 仅可填 begin/refresh
已核可见条目 ID，旧期限保持不变。四字段分别最多 48/80/128/128 Unicode 字符且
合计最多 256。坏摘要只返回固定记忆降级状态，不抛到业务 unknown 分支。
JSON 整体错误、原业务字段无效或外包络超过原硬上限仍由 adapter 按原输入错误处理。

## 3. 留存、回查与维护

近期固定 10 条/48h，按本批最早可信来源时间到期。每条完整存储 8 KiB，快照 96 KiB。
同批去重，晚到 cutoff 不覆盖新话题。追加非阻塞锁后重核当前 scope。
原话仅由公共层已核 inbox/真实回复截取，最多两处用户和一处 Bot，总 2 KiB UTF-8，
标注截断和出处，不自动注入。

用户有具体追问时，adapter 可在活 batch 中调用
`state.context_quote(batch, basis, entry_id, quote_index)`，一次只返回一处已核片段及
原期限/出处。basis 不能扩张可见条目，缺失/过期/不可证就返回固定不可用状态。

`recent_context_maintain(base, clock=time.time)` 仅处理桥私有目录
`state/continuity/` 内的 `recent.json`、`.recent.json.tmp`、`cloud.json`、`.cloud.json.tmp`。
各类共用该目录的非阻塞发布锁，不新增内容文件，不递归扫描未知文件。
CLI 为 `python -m wechat_bridge.worker_state --base BASE recent-context-maintain`。
supervise 的本地维护分支调用同 helper。02 在无 session/NEEDS_RELOGIN 的既有 Hook
门关闭路径调用专用 CLI，每次 Hook 最多一次，不改变 serve 生命周期、不唤醒模型。
关闭能力仍可维护到期残留，注销/换范围先清旧内容才能写新范围。

## 4. 合成云端事项

`cloud_context.CloudBinding(source_id, case_id, publisher_id)` 来自固定可信本地配置，
当前只准合成事项。`publish(store, binding, publisher_id, projection,
expected_previous_revision, now, *, enabled=False)` 是新增独立源发布步骤，不复用微信
begin/reply/附件动作。调用者身份由可信本地程序固定配置，当前没有平台认证接口。

projection 包含 `source_revision`、`source_updated_at`、`source_ref`、`fact_status`
（confirmed/provisional/corrected/completed/revoked）及 `title/facts/change/next_step/open_loop`。
五个文本合计最多 512 Unicode 字符，完整投影最多 4 KiB。首版固定 scope/binding/publisher，
后续比较 expected_previous_revision 原子替换，冲突不覆盖、不重做业务副作用。

`refresh(store, binding, scope, now, *, enabled=False)` 在合法 begin 或既有程序维护机会
核同宿主源版本。`read(store, scope, now, *, enabled=False)` 返回安全状态卡。
缓存分别记录 source_updated_at/bridge_checked_at/bridge_applied_at。未变仅更新 checked；
源不可达保留上次 checked；已见新版本未能应用标 update_pending，旧缓存不称最新。
TTL 从源更新时间起 48h，检查不能续期。`maintain(store, scope_or_None, now)` 清桥缓存
及固定临时文件，不删除 `state/continuity-source/current.json` 权威源。

真实来源、异地主机传输、真实平台加载、微信实发、手机体验和发布均由后续门槛验收。
token 不可观测，只报告真实 UTF-8 字节与可测时间。

## 5. v1.1 类型与签名澄清

revision 沿当前 adapter 使用正整数，拒绝 bool，示例中的字符串不是接口类型。
basis 的本地 HMAC-SHA256 校验复用现有 session.bot_token 派生的域隔离密钥，不新增
外部凭据或独立密钥文件。签名仅用于防止模型伪造已读快照依据，scope/TTL/当前存储和
活 batch 仍分别复核，不代表 Muse 平台身份认证。v1 已转发副本保持原字节。

精确字段和恢复补充见 [公共 schema](CONTINUOUS-CONTEXT-SCHEMA.md)。摘要四字段是模型候选，
结构校验能证明来源ID、旧引用期限和原话出处，无法语义审计改写后的每句话。02提示词和
连续场景验收仍须验证只摘要本批新增事实、旧事实只连旧引用；入队也不表示手机实收。
