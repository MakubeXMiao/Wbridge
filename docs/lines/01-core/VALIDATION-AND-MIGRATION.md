# 兼容、迁移约束与验证清单

任务：`WBR-01-20261023-001`。本轮仅静态审阅与候选设计；下列是未来需要执行的验证，不是本轮测试通过清单。

## 1. 兼容与隔离约束

1.1 保持当前包名 `wechat-bridge`、模块 `wechat_bridge`、CLI、路径与既有 begin/refresh/reply/release 协议；当前为 POSIX/fcntl，实现不等于原生 Windows 已适配。命令/函数移动、字段删除或包名更改须单独提兼容方案。

1.2 不直接重写旧 session、游标、inbox、receipt、claim/seen/started、reply_batches 或 outbox。新 route/session/job 等候选记录应有独立 schema/version、受限权限、原子更新和明确锁顺序；旧记录缺字段不得臆造 backend_session 或当前运行版本。

1.3 所有测试使用独立 base、Hook state、解释器/executable 与假 runtime/transport；真实账号验证还需隔离账号或已授权的受控停旧切换。同 bot 不并行收件，不把三个新后端各当一套桥接服务。

1.4 不把测试 fixture 部署为真实 runtime，不把仓库 delivery.to 占位符覆盖线上绑定。Muse 包、Hook 脚本、平台保存的 prompt 必须配套核验；只拉 Git 或改 JSON 不等于平台生效。

1.5 传输 BOT_AGENT 元信息、旧路径常量、Linux/macOS 进程监督是兼容项，不在提取 backend 接口时顺手清理。当前 `CONNECTOR.md` 中旧阈值描述需另提文档候选，不能成为继续使用旧 worker 期限的依据。

1.6 诊断日志仅记录脱敏阶段、版本、ID 与耗时；凭据、context、完整 lease、消息正文/文件及状态数据库不进仓库。reply_batches/quarantine 当前没有自动 TTL，sent 清理不等于全部副本删除；后续保留策略由用户确认，不能以“脱敏/迁移”删除恢复证据。

## 2. 推荐迁移顺序（未执行）

| 阶段 | 工作与 owner | 进入下一步的条件 |
| --- | --- | --- |
| M0 冻结实测版本 | 02 取证，01 接收公共问题；00 汇总 | 实际运行代码/dirty、包路径/进程重启、脚本 hash、真实 prompt、消息/worker ID 与时间对应；无法核实明确未知 |
| M1 契约收敛 | 01 整合 02/03/04 能力矩阵 | 用户确认路由、后到输入、session、取消/审批/长任务范围；细化存储/锁/迁移/保留期 |
| M2 无行为变化的适配边界 | 01 公共 facade，02 Muse shim；各自任务分支 | 单一路径/单一 owner，不改变租约和发送身份；旧 Muse 合成回放及离线回归通过 |
| M3 单个新后端隔离接入 | 03 或 04 实现已确认适配；01 只改公共 owner 文件 | synthetic contract tests、故障注入/恢复、session 隔离与副作用核对；未支持能力显式关闭 |
| M4 独立验收和受控切换 | 非实现者 QA；02/03/04 各自真实环境；00 组织发布 | 针对最终候选 SHA 与配套配置的 QA、范围内实测和用户发布授权 |

M2 是未来候选步骤，不是本轮启动重构的许可。候选的阶段名不替代具体任务、目录和文件修改授权。

### 回滚与不确定状态

代码回退只换代码，不恢复旧游标、队列或完成账本。先停对应调度/已验证进程，保存正在使用的代码与配置，以及停写后的必要状态证据；不能让新旧协议的 worker 并行操作同一 state。

只有不改变运行数据语义的边界提取，才可能直接退回原代码/配套 Hook。引入新 schema/审批/job 状态后，必须先证明旧版可读或提供受控兼容路径；否则只停止新路径、保留状态待人工核对。已经发送的微信/发生的工具副作用不因 Git 回退消失。

## 3. 验证矩阵

“已有入口”表示本次读到的测试代码/文档，均未在本任务执行。

| 编号 | 范围与可观察通过条件 | 已有入口 / 待补 |
| --- | --- | --- |
| V01 收件与游标 | 绑定机主之外不处理；群/非入站过滤；无效输入可见隔离；持久化失败不前移游标 | `test_receiver_hardening.py`、`test_p1p7.py` |
| V02 收件防重 | 同消息重放/并发/重启不重建完成任务；同 ID sender 不冲突；seen 裁剪保留永久 receipt | `test_receiver_hardening.py`、`test_worker_state.py` |
| V03 媒体入站 | 文本+多附件、部分失败、语音原件/已有转写、文件名清理；不覆盖同消息不同附件；下载/解密边界 | `test_receiver_hardening.py`、`test_media_login_hardening.py`、`test_crypto.py` |
| V04 凭据与隔离 | 缺机主/token 拒绝；运行中不重绑；换账号不复用旧状态；新后端不拿微信凭据/任意读文件 | 旧登录见 `test_media_login_hardening.py`；新后端权限待补 |
| V05 单 owner 与批次 | 重复/并发 wake 只一个 owner；同 sender 后到归同 owner；不同 sender 不合并 | `test_burst_ownership.py`、`test_worker_state.py` |
| V06 期限与 fencing | 180/90/570 及边界时刻、begin 与 refresh、旧身份迟到、恢复预算 3 次；接收新消息不续命 | `test_burst_ownership.py`、`test_worker_state.py`；实机 B5–B8 另验 |
| V07 后到输入准入 | needs_refresh 时不入队/不 complete；新 revision 全量覆盖；不靠重跑有副作用工具补消息 | 旧 reply 覆盖见 `test_burst_ownership.py`；后端 update_input/安全降级待补 |
| V08 发布崩溃窗口 | prepared 可原身份恢复；publishing/未知发送不自动重造；旧 receipt/覆盖/client_key 保留 | `test_worker_state.py`、`test_delivery_hardening.py` |
| V09 发送/FIFO | 同接收者退避阻止后续超车；不同接收者不阻塞；分片进度/稳定身份/预算/媒体配文正确；上限进死信；合并 anchor 仍 pending/失败时 covered 成员不报独立成功 | `test_delivery_hardening.py`、`test_merge.py`、`test_p1p7.py` |
| V10 宿主与 Hook | 命令位置+启动时间核对才停进程；flusher 监督、锁不被子进程长期继承；dry-run 无状态变更 | `test_worker_state.py`、`test_audit12.py`、`test_audit2fix.py`、`test_hook_*.sh` |
| V11 路由与合并 | 一个任务一个 backend；不存在/未配置时拒绝；同 sender 不按后端分裂 owner；配置/重启不漂移活跃 route；活跃时切 route/epoch 或同 backend 切安装实例不吸入旧批次；跨 backend/session/execution/output_kind 不混合发送，legacy 域隔离 | 新 contract tests，待补 |
| V12 session 映射 | 跨账号、sender、backend、后端安装实例、epoch 不串线；同账号/sender/backend/实例/epoch 的不同 conversation_key 也隔离；重复创建/并发更新幂等；只恢复已确认的后端会话 | 新 contract tests + 02/03/04 版本能力证据，待补 |
| V13 执行/事件 | accepted 与 running/最终结果分开；重复/乱序/缺号/冲突终态处理；旧 attempt 回调拒绝；CLI 退出不伪造成功 | 新 adapter fixtures 和事件存储故障注入，待补 |
| V14 取消 | request 与 confirmed 分开；取消/成功竞态；已准入/发送回复不误称撤回；unknown 不自动重做副作用 | 各线真实取消能力 + 公共仲裁 tests，待补 |
| V15 审批/长任务 | 绑定身份/动作/期限；拒绝/过期/重复/跨会话无效；重启恢复原待审状态；不无进展续租保活 | 先确认是否首版开放，再设计 durable job/tests；当前阻塞 |
| V16 附件跨宿主 | local 不等于远程可读；不可达显式失败；引用过期/路径越界/类型限制/保留期；多出站能力独立验证 | 01 resolver + 各线传递机制，待补且需范围授权 |
| V17 超时/副作用 | 工具阻塞超过 lease、平台先杀 worker、结果丢失、旧任务仍活；核对旧 run 后才恢复；无核对能力时保留 unknown | 旧租约 tests + 新后端故障矩阵，待补 |
| V18 迁移/回退 | 旧输入/账本不丢；新字段 version 和旧可读性验证；锁顺序无反转；代码回退不回滚运行状态 | 先冻结 schema/兼容路径，再补 migration fixtures |
| V19 阶段追踪 | 同一消息收件→wake→adopt→enqueue→send→API；后端 run/session/attempt 可关联；正文/秘密不入日志 | `test_latency_trace.py`；新增事件标识待补 |
| V20 实机 | 对应真实 SHA/dirty/解释器/包/脚本/prompt；B1–B8、五连发首回、媒体、重启；手机可见结果单列 | 平台现场取证与[当前验收范围](../../PUBLIC-UPDATE-20261029.md)；离线测试不代做 |
| V21 多实例与日志 | 同 bot 跨 base 启动冲突可见；监督不跨实例误停；合成 URL/query/token 异常不出现在媒体等下层日志 | 当前单 Store 锁/命令识别不能保证全局互斥；`media.py:79` 错误路径需补日志回归 |

现有统一离线入口为 `tests/run_offline.py`，分进程阻断真实 Python socket 并隔离状态；Shell 子套件是部分 Python 场景的包装，不重复累计覆盖。后续获准执行时使用独立 venv/base；macOS 系统 Python 可设置 `PYTHONDONTWRITEBYTECODE=1` 排除字节码缓存对 dry-run 路径检查的影响。该环境处理不能替代代码/平台验收。

已有 sender 测试中的假服务端会按 client_id 主动去重，不能据此证明真实 iLink 服务端 exactly-once。历史 ACK 丢失类离线断言只证明该模拟假设下的客户端行为，服务端语义仍需版本绑定的实测。

## 4. 阻塞与决定清单

| 编号 | 待补 / 待决定 | 责任 / 解除标准 |
| --- | --- | --- |
| D01 | 09:38 摘要缺实际运行版本归属与完整阶段证据，不能判断新修复是否运行或确定公共根因 | 02 提交绑定 SHA/dirty/运行包/脚本/prompt/时间的报告；01 据证据另提公共修复候选 |
| D02 | 03/04 首轮文档接口要求已纳入候选第 11 节，但真实 session/final/取消/审批等未验；02 绑定版本输入仍缺 | 保留各线文档/证据 QA 与真实后端验证的区别；各线补未测能力和字段映射确认，01 再收敛正式契约 |
| D03 | 后端选择/切换是否允许、默认后端、活跃任务遇切换的行为，以及消息路由解析/持久化时点和批次准入边界 | 用户确认；推荐任务路由冻结、不自动 fallback，跨 route/conversation/epoch 后到消息不进入旧执行 |
| D04 | 后到消息与不支持 update_input 的后端如何保持全批回复且避免重复工具副作用 | 01/各线给可验证方案；用户确认前限制该场景，不靠重跑或扩大 ID 掩盖 |
| D05 | 审批/长任务首版是否开放；是否引入持久 job 与恢复仲裁 | 用户决定范围；01 冻结生命周期/schema/锁顺序，不能放宽现租约替代设计 |
| D06 | 跨宿主附件传递、多附件出站、引用/保留期及私有数据边界 | 各线提供可用通道与限制；用户确认后才传递真实附件 |
| D07 | 新 route/session/execution/approval 的存储格式、原子更新、迁移与回退兼容 | 01 在能力/范围收敛后补正式契约；通过 synthetic crash/upgrade/restart 用例 |
| D08 | 新输出身份/合并域、legacy 队列兼容域、多实例身份与下层日志暴露路径 | 01 冻结合并/实例边界并提独立修复候选；不把静态风险点写成已发生的实机事故 |
| D09 | 可选 SDK 宿主、有效 cwd/权限策略/环境白名单、deadline 预留、事件与输出上限、问题响应和待执行输入取消 | 01 与 03/04 确定最小可验证的宿主和事件边界；公共 Python 门槛、现有租约与首版能力不能被顺手放宽 |

这些项目不阻止本轮候选交付，但阻止把讨论稿当正式公共 API 开始跨线适配重构。没有新的实测证据前，不根据等待时间摘要修改 `worker_state.py` 或放宽租约。
