# Muse 连续上下文接入

任务 WBR-00-20261108-CONTINUITY-IMPLEMENT，固定公共基线 `d797bd38ca414604b9aabb17a73e98837d2f9914`。本文描述本批 Muse 接线、合成验证及后续现场验收。公共接口以经 00 转发的接线 v1、v1.1、v1.2 为准。源端发布入口和组合代码完成情况由固定候选及原始测试回执证明。

## 启用和可信参数

近期文字与云端状态分别由 `WECHAT_RECENT_CONTEXT=1`、`WECHAT_CLOUD_CONTEXT=1` 开启。默认均关闭。先安装匹配公共包、adapter、Hook 和实际保存的提示词，再在受控环境启用对应阶段。旧公共包、缺少协商能力或关闭时沿用当前批回复，摘要和原话回查均可省略。

首个云端来源固定为合成三元组。source_id 为 `wbridge-synthetic`，case_id 为 `continuity-smoke`，publisher_id 为 `wbridge-synthetic-publisher`。adapter CLI 只接受这组三项同时给定，或全部省略。Hook 将可信配置形成 `payload.muse_adapter.context_argv` 数组。Worker 原样追加各 argv 元素，用户消息、文字摘要和模型都不能自行更换来源。维护命令与合法 begin 使用同一个固定绑定。

```text
--cloud-source-id wbridge-synthetic
--cloud-case-id continuity-smoke
--cloud-publisher-id wbridge-synthetic-publisher
```

这三个选项用于 `hooks/muse-worker.py`，也用于公共 `worker_state` 的 supervise 和 recent-context-maintain 命令。缺少固定配置的云端阶段不能被说成已接通。环境开关、实际 Hook 保存读回、adapter 采用和运行进程证据分别记录。

## Worker 内的读取和终结

合法 begin 或 refresh 核对本批拥有权后，adapter 才读取附加上下文。模型响应使用独立 `context_capabilities` 和 `context`。能力对象 schema_version 为 1，features 仅协商 recent_context_v1、memory_delta_v1、cloud_context_v1。原附件能力、batch digest、revision 以及图片快照的身份继续由原协议负责。

近期投影的 entries 含摘要、可见条目 ID、来源与原期限，不自动带原话。需要核实具体原话时，当前活 Worker 可在终结前调用 adapter 包装动作。示例中的 handle、revision 和 entry_id 均替换为本次返回值。

```json
{"action":"context_quote","handle":"CURRENT_HANDLE","revision":1,"entry_id":"VISIBLE_ENTRY_ID","quote_index":0}
```

quote_index 从 0 开始，必须小于该条目的 quote_count。一次只回一处有出处、原期限及截断标记的片段。quote_count 为 0、已过期、不可证或来源缺失时，说明当前无法核实。回查不续租、不终结原业务。不能从摘要恢复未保存的逐字原文，也不能直接读取私有快照。

同一个 Worker 可以在普通 reply 或 attachment 的 reply/continue 终结外包络顶层提供可选 memory_delta_v1。附件 request 内不增加记忆字段。摘要绑定当前 handle 和整数 revision，source_message_ids 的集合精确覆盖本批全部 owned。只记本批新增话题、意图、真实已核动作和未完事项，旧事实仅通过 begin/refresh 已核可见条目引用，沿用其旧期限。简单问候、确认、测试和“只回复X”不要求摘要。

adapter 先校验原业务结果并可靠保存 queued，再于同一次本地调用的独立异常边界消费私有 capsule。坏摘要、capsule 缺失或记忆写入失败只降级附加上下文。业务 unknown、会话保存不明、needs_refresh 和非终结附件动作均不写记忆。queued 后没有第二次模型动作，也不单独调用记忆写口。needs_refresh 或 revision 变化后丢弃旧摘要候选，重读全部当前 messages 再生成。

摘要辅助承接已知案件，当前微信原文优先。明确换题时处理新话题，歧义先澄清。图片仍由该条文字的固定快照、原位置和实际读图结果决定，文字摘要不能选另一张旧图。普通聊天自然回答，用户问进展或来源离线、过期、更新待应用时再解释相关更新时间和核对状态。

## 独立源端发布流程

云端 source agent 在原任务形成一项新结论后，增加一次受信的状态发布步骤。它使用固定 CloudBinding 和固定 publisher_id 调用公共 cloud_context.publish，核验发布结果后才记录“状态已发布”。发布失败、冲突或同版异内容不会重做原任务副作用。下一次正常源任务先核对当前版本，再决定新的 expected_previous_revision。

这个步骤独立于微信 begin、reply 和附件动作。现有微信动作不具备源端发布权。当前发布者身份由可信同宿主测试程序固定，不能把它当成已验证的平台认证。真实来源和生产调用工作流仍需指定并验收。

下面是受控合成来源任务中的函数调用。store 由合成夹具建立并绑定权威范围，now 是这次新合成结论的真实生成时间。首版 expected_previous_revision 为 None，后续必须给当前已知版本并递增 source_revision。

```python
import time
from wechat_bridge.cloud_context import CloudBinding, publish

binding = CloudBinding(
    source_id="wbridge-synthetic",
    case_id="continuity-smoke",
    publisher_id="wbridge-synthetic-publisher",
)
now = time.time()
result = publish(
    store,
    binding,
    binding.publisher_id,
    {
        "source_revision": 1,
        "source_updated_at": now,
        "source_ref": "synthetic:continuity-smoke/revision-1",
        "fact_status": "confirmed",
        "title": "合成事项",
        "facts": "合成结论已形成",
        "change": "首次发布",
        "next_step": "在下一批合法 begin 核对该版本",
        "open_loop": "",
    },
    expected_previous_revision=None,
    now=now,
    enabled=True,
)
```

enabled=True 只用于已授权的受控合成调用。函数默认关闭，不因 import 或普通微信请求自动发布。不能为了让旧事实继续有效而刷新 source_updated_at。源数据在 `state/continuity-source/current.json`，与桥缓存分开管理。

合法 begin 及既有 Hook 程序维护机会核同宿主源版本并更新桥缓存。正常登录路径复用 supervise 的一次维护，无 session 或 NEEDS_RELOGIN 时调用 recent-context-maintain；每次 Hook 最多维护一次。无变化只核版本，不唤醒模型。源版本变化且应用成功后，下一条合法 begin 应读取新版本。已见新版本而未应用时为 update_pending，旧可核投影不能声称最新。源不可达保留上次核对时间。

桥缓存分别保存 source_updated_at、bridge_checked_at、bridge_applied_at。同版检查只能更新 checked_at，不能给源事实续期。源端权威文件不属于桥维护删除对象。

## 容量、量测和分阶段验收

所有硬预算按紧凑 UTF-8 JSON 的实际字节核对。近期四个摘要字段分别最多 48、80、128、128 个 Unicode 字符，合计最多 256，候选整体最多 8192 字节。完整条目最多 8 KiB，近期快照最多 96 KiB，最多 10 条，按最早可信来源时间到 48 小时即时拒读。原话总计最多 2 KiB，最多两处用户和一处 Bot 原话，截断必须标注。

近期自动投影最多 12 KiB，云端卡最多 4 KiB，两者合计最多 16 KiB，包含字段名和元数据。云端五个文本字段合计最多 512 Unicode 字符，完整源投影最多 4 KiB，缓存从源更新时间起最多 48 小时。反复核对和重述都不能重置期限。

目前平台模型 ID、匹配计数器及整批 input/output/cached token 不可观测，保留这个结论。只记录实际注入字节、adapter 各调用起止、Hook 唤醒往返、业务入队、调用返回、Worker 收尾、源发布到桥应用和新版本被下一条答复采用的可观测时间。记录样本数、配对差 p50/p90 和最大样本值，不能把本机耗时或样本最大值解释为平台保证。

离线先验正常及附件终结、坏摘要、缺 capsule、会话保存不明、needs_refresh、unknown、锁忙、过期、范围变化、默认关闭和旧端兼容。合成云端闭环使用真实公共 publish、版本比较、缓存维护和合法 begin 读取，包含更正、冲突、离线和 update_pending。预备的测试入口为 `hooks/tests/test_muse_context_runtime.py`，是否已通过以固定 SHA 的原始回执为准。

Mac 合成测试只证明本地代码路径。真实 Muse 服务加载、实际 Hook 执行维护、可见平台配置下的开启/关闭配对、微信 API 接纳及手机实收仍按固定候选独立登记。没有真实来源时，不能给云端连续体验的整体验收分数。源端状态发布成功与下一条微信已采用新版本也分别验收。

## 关闭与回退

先关闭 WECHAT_RECENT_CONTEXT 和 WECHAT_CLOUD_CONTEXT，停止新增与注入，当前批业务回复继续走原协议。维护入口仍清理固定 recent/cloud 正式文件及各自临时文件的过期内容。VM 停机或 Hook 暂停期间物理清理顺延，恢复首次维护先清过期内容。维护不递归删除未知文件，也不删除独立源端权威文件。

回退使用能识别现有业务状态的已验候选，按项目原停门与单实例流程执行。上下文文件不参与业务账本恢复，不清空或回滚队列、回执、游标和附件账本。主动立即清除内容、接入真实来源、读取云端聊天、现场重启或微信实发分别依对应授权与验收门槛。
