# 公共接线 v1.2 精确 schema

沿 v1、v1.1，旧副本原字节保留。实现为 context_runtime、recent_context、cloud_context、context_files。
下文 number 均为有限非负 int/float，拒 bool；hex 均为 64 位小写 SHA256。

## 1. 能力、本地对象与投影

`capabilities()` 精确返回 `{schema_version:1,features:list[str]}`，features 仅可出现
recent_context_v1、memory_delta_v1、cloud_context_v1，分别由 WECHAT_RECENT_CONTEXT=1
和 WECHAT_CLOUD_CONTEXT=1 控制，默认空。不改变附件 capabilities。

ContextRead 仅含 projection:dict、basis:dict|null。ContextResult 仅含 result:原业务dict、
context_capsule:私有对象|null。不可把整个本地对象作 JSON 输出。

projection 的可选键为 recent_context_v1:RecentProjection、recent_context_status:ContextReason、
cloud_context_v1:CloudRead、context_status:ContextReason。关闭时为 {}。近期含字段名最多
12288B、云端卡最多4096B、整个projection最多16384B，均按紧凑UTF-8 JSON核。
总预算超限返回 `{context_status:too_large}`。

RecentProjection 精确字段：schema_version:1、snapshot_id:hex、entries:list[EntryProjection]
（0..10）、high_water_seq:非负int。EntryProjection 精确字段：

- schema_version:1、id:hex、revision:严格正int，拒bool/float。
- topic/intent/bot_action/open_loop:str，各48/80/128/128 Unicode字符，合计最多256。
- sources:list[{message_id:safe str至多180,seq:正int,received_at:number,fingerprint:hex}]，
  1..1000项，并受完整条目8KiB限制。
- source_at:最早来源时间、expires_at:source_at+172800、cutoff_seq:最大真实seq。
- historical_refs:list[{entry_id:hex,expires_at:原期限}]，最多10，过滤过期引用。
- admission:{kind:reply|attachment,job_id:非空str至多512,group_id:null|非空str至多512}。
- quote_count:int 0..3。回查索引0基，范围0至quote_count-1。投影无quotes原话正文。

## 2. 私有 basis、候选与回查

basis 仅在adapter私有session保存，不给模型，紧凑UTF-8最多16384B。精确字段为
payload 和 signature。payload 精确字段 schema_version:1、scope:{bot_id,owner_id,
conversation_id,login_epoch}、snapshot_id:hex、issued_at:number、entries:list[{entry_id:hex,
expires_at:number}]（最多10）。signature 为 HMAC-SHA256 hex，key由既有session.bot_token
作域隔离派生，不新增凭据或密钥文件，不表示平台认证。basis不含历史正文。

context_for_batch 与 context_quote 均支持最小batch `{lease:私有lease,owned:已有ID列表}`。
公共层重新核claims/inbox的活batch，不要求adapter保存正文，不自动refresh或续租。

`context_quote(batch,basis,entry_id,quote_index)` 成功精确返回
`{status:ok,entry_id:hex,expires_at:number,quote:{speaker:user|bot,message_id:来源ID,
text:str,truncated:bool}}`。一次只回一处，总存储原话最多2048B，用户最多2处、Bot最多1处。
来源无法核验就不留，生成的媒体/失败占位不冒充用户原话。失败精确返回
`{status:unavailable,reason:disabled|ContextReason}`，不终结原业务。

memory_delta_v1 位于Muse终结外包络，精确字段为 schema_version:1、handle:str（1..128）、
revision:严格正int、source_message_ids:无重复str列表（集合精确等于本批所有owned）、
topic/intent/bot_action/open_loop:str、historical_refs:begin已核可见entry ID列表（最多10）。
候选整体最多8192B，外包络仍受原硬上限。坏候选只丢记忆。

context_commit 成功精确返回 `{status:applied,entry_id:hex,snapshot_id:hex}`，跳过精确返回
`{status:skipped,reason:枚举}`。reason 为 disabled、capsule_unavailable、no_candidate、
invalid_candidate、source_expired、scope_unavailable、scope_mismatch、duplicate_batch、
late_batch、historical_reference_unavailable、snapshot_too_large 或 ContextReason。

ContextReason 固定集合为 storage_busy、storage_invalid、storage_changed、storage_too_large、
sequence_unavailable、scope_mismatch、epoch_mismatch、clock_invalid、clock_anomaly、too_large、
projection_too_large、entry_unavailable、invalid_entry、entry_too_large、scope_unavailable、
context_unavailable。不返回原始异常文字。

capsule只能在原业务队列接纳、处理完成、附件wire检查全成功后签发；构造失败仍保住原result，
capsule为空。02可靠保存原queued后才调用commit，保存不明禁止消费。capsule还绑定签发时的
wall/monotonic期限，防止挂起旧批因时钟回拨重入库。记忆异常独立吸收，不改业务unknown。

## 3. 合成云端事项

CloudBinding(source_id,case_id,publisher_id) 三字段均为非空str至多128Unicode字符，来自
可信固定本地配置。source_revision 为严格int 1..2^63-1，expected_previous_revision 首版null、
后版同类型。新版本必须递增，源时间不得倒退或大于now。同版同内容幂等，同版异内容拒绝。
CAS冲突、倒退版本和倒退时间均不覆盖。

源projection精确字段为 source_revision:int、source_updated_at:number、source_ref:非空str
至多512、fact_status:confirmed|provisional|corrected|completed|revoked，以及 title/facts/change/
next_step/open_loop:str。五文本合计至多512Unicode字符，含元数据总计最多4096B。

CloudRead 必有 schema_version:1、status:str、projection:dict|null，可选reason:str。status为
empty、disabled、available、unavailable、update_pending、paused、expired、skipped。
paused/expired/disabled无投影。unavailable/update_pending可带旧可核投影，不能称最新。
非空projection精确含binding三项、源projection九项，以及 source_digest:hex、
bridge_checked_at:number、bridge_applied_at:number、observed_source_revision:正int|null。
read可接受binding，context_for_batch把固定binding传入以拒旧来源。

publish 返回 status+reason，可选source_revision/source_digest/active，status为 published、
unchanged、conflict、disabled、skipped。refresh 返回status+reason，可选source_revision/
source_digest，status为 applied、unchanged、unavailable、update_pending、revoked、paused、
expired、disabled、skipped。maintain 返回status+reason，可选removed:固定文件名list，status为
cleaned、unchanged、skipped。pause 是独立可信源操作，比较expected版，暂停缓存且不续源时间。

云端reason固定集合为 applied、binding_mismatch、cache_missing、cache_too_large、cleaned、
clock_invalid、disabled、invalid_request、older_source、published、publisher_mismatch、
revision_conflict、scope_mismatch、source_expired、source_invalid、source_missing、source_paused、
source_revoked、source_too_large、source_unavailable、storage_busy、storage_changed、storage_invalid、
storage_too_large、unchanged。

## 4. 维护与实际补漏

`recent_context_maintain(base,clock=time.time,cloud_binding=None)` 先维护固定桥文件，再在
cloud开关开启、scope有效、固定binding已给定时调用真正的cloud_context.refresh。仅清理时
不得称完成补漏。无session/NEEDS_RELOGIN仍清理，关闭能力停止新增与注入，维护仍有效。

CLI recent-context-maintain 与 supervise 均接受同组三个受控参数 `--cloud-source-id ID
--cloud-case-id ID --cloud-publisher-id ID`，必须同时提供或全部省略。02将同一固定合成配置
传给begin的CloudBinding和维护CLI，不接受模型临时选择。supervise已内置一次维护；只有
门关闭路径另调专用CLI，每次原Hook至多一次，不新增模型定时查信。

维护更新checked_at用于检测下次回拨，源时间与TTL保持。桥目录固定recent/cloud正式文件及
各自tmp，共最多4个含内容文件；独立源current及tmp不被桥清理。当前只验同宿主合成来源，
真实来源、平台加载、微信发送与发布仍待后续门槛。

## 5. v1.3 恢复与再核澄清

begin/quote 在可选磁盘读取后，再核活batch、当前scope和原期限，不自动续租。
云端开关开启但没有当前固定binding时，不注入旧来源，清除桥缓存正文，权威源保留。
实际合成绑定固定为 wbridge-synthetic / continuity-smoke / wbridge-synthetic-publisher。

云端缓存正文到期、撤销、注销或范围改变后，在同一个cloud.json内原子替换为有界控制水位，
不新增内容文件，不保留旧正文、原账号或事项文本。私有字段为schema_version:1、tombstone:true、
scope_digest:hex、binding_digest:hex、clock_high_water:number、retired_revision:int|null、
retired_digest:hex|null、retired_source_updated_at:number|null、retired_expires_at:number|null、
reason:有限恢复原因。四个retired字段全空或全有效，expiry保持原source_at+172800。
同范围旧版本不能在跨进程时钟回拨后重新进入；新可核版本及更晚源时间可前进。

因此maintain的cleaned表示正文已清除，removed可以是空列表；它只列真实unlink的临时文件。
私有控制水位不进入CloudRead，公开schema和4KiB上限保持。维护可以更新私有时钟水位，
只有真正核对源版本才更新bridge_checked_at。关闭能力仍维护到期正文。
