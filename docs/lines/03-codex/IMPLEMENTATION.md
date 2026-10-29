# Codex backend v1

任务 `WBR-03-20261024-001`；2026-10-24。03 只拥有 `backends/codex.py`、`backends/_codex_protocol.py`、`tests/test_codex_backend.py` 和本目录。公共注册、CLI、持久作业、审批鉴权、微信收发与投递仍由 01 持有。

## 接口与协议

遵循 01 v1 的 `create_backend(config)` 与单 execution 的 `start/poll/respond_approval/cancel/close`。初始契约为 `4555329439ebf2e1d7a102022c5d04d35926b732`；当前修复按固定集成 `cad9aeec2db30d594d8fdca184360c0e8499e7e4` 的真实 registry/dispatcher 验收，包含单项待决审批、final 64,000 字符、session 1,024 字符边界。公共包未同步到本分支时，协议测试仍能独立运行；由 01 注册 factory。

每次创建独立 stdio app-server 进程组；初始化、配置预检、显式 thread/start 或 thread/resume，再 turn/start。不会连接桌面共享 daemon，不使用 recent/last 或自动重试。配置 `command` 只允许一个绝对可执行路径，其他任意启动参数不接受。可选 `model` 显式指定模型；缺省由本机 Codex 配置选择。

新公共 `session_id` 是 `wbc2.` 开头的适配器不透明引用，以紧凑数组封装原始 provider thread/session ID、cwd 的 SHA256，以及账号/发送者/后端/实例/会话的摘要。只将可重新计算的 cwd 改为摘要，不截断 provider 身份。合法的两个 256 字符 ASCII ID 配合长 cwd 可落入公共 1,024 字符上限；Unicode 等编码后仍超限时返回明确 `codex_session_ref_too_large` failed。生成和恢复均执行 1,024 字符上限，并校验 provider ID；兼容未超限的旧 `wbc1.` 引用。它不是鉴权令牌；01 必须仅从受控会话映射读取。任何作用域、目录、返回 thread/session 不一致或非 idle thread 均拒绝续接。未成功的 attempt 不产生可续接 final。

输入冻结。图片魔数已识别且本地路径获准时使用 localImage；其他本地附件以路径交给模型。`attachment_roots` 必须由操作者配置，默认仅 cwd；路径解析后仍需位于获准目录，文件上限 100 MiB。不可用附件只传明确失败标记。此边界检查不是文件系统只读访问沙箱。

## 最终结果与停止

只接受当前 thread/turn 的 agentMessage 完成项，并等待对应 `turn/completed`、status=completed、无 terminal error、非空 final。优先 final_answer；兼容 phase=null 的最后一条已完成 agentMessage，绝不把 commentary/partial_answer/delta 作为最终结果。历史 resume items、错身份、冲突 item、断流、无 final、非零退出已被观察到时均不会发布成功。

最终文本最多 64,000 字符，包含合并多项 final 时的分隔符。超出时输出 `failed / codex_final_too_large / side_effect_state=possible`，不截断、不发布 session，也不让公共事件校验把它变成 unknown；这只表示无法交付结果，不表示 provider 动作未发生。

错误仅输出固定代码，stderr 只统计字节数、不写入用户结果或日志。队列、帧、结果、审批数量均设上限。清理只作用于本 attempt 创建的进程组，TERM 后有界 KILL；终态前的进程退出码与清理退出码分开保存。

取消先撤销待答审批、发 turn/interrupt；ACK 不等于 interrupted。已请求取消/超时后丢弃迟到 final。公共 cancelled 仅在已确认 owned process group 停止时输出；副作用仍可能发生。超时停止明确则 failed，停止或协议状态不明则 unknown。01 负责持久 fencing、重启后 unknown 和审批失效，适配器不恢复失联 attempt。

## 审批范围

支持 commandExecution 与 fileChange 的明确一次 `accept/decline`。RPC ID 保留原始整数/字符串类型；摘要与 SHA256 绑定实际请求及文件改动，匹配 execution、attempt、scope、thread、turn、item。拒绝摘要不完整/过长/含疑似秘密、重复回答、摘要不符、过期、错身份与已关闭请求。审批默认最多 120 秒，并受 job deadline 和公共审批期限的较小值约束。

同时到达的请求在适配器内排队，公共层一次仅呈现一项；用户答复成功后，下次 poll 才呈现下一项。排队保留原 RPC ID、动作摘要/摘要哈希及原到期时间，不在呈现时续期。公共审批 ID 最多 256 字符，过长时仅对公共 ID 使用哈希别名，回复仍映射回原始 wire ID。任一审批到期会停止整个 attempt；provider 撤回未回答请求也停止 attempt，因为 v1 没有撤回公共审批的事件。取消、关闭、错误和终态清空全部待决/排队项，并尽力向 provider 发 cancel；进程停止证据才是清理的最终依据，不声称撤销已发生副作用。

文件审批必须有同 item 的具体 changes；首版仅在完整安全摘要可在 1200 字符内呈现时允许。目录 grantRoot、额外 permissions、网络审批上下文、writeStdin、会话/整轮权限 grant、缺少 accept/decline 的 availableDecisions、普通用户问题与 MCP elicitation 都明确失败/unknown，不转成批准。首版不支持长期放行、规则修订或会话授权。常规 sandbox 内动作依 Codex on-request 策略执行，并非每条 shell 命令都弹审批。

## 启动隔离与能力边界

本机 app-server 不支持 exec 的 ignore-user-config/ignore-rules。预检因此采用两阶段配置读取，并每次显式传 cwd：先只读发现继承 MCP 名称，再重启 owned server，逐项 `mcp_servers.NAME.enabled=false`，复核全禁用后才新建 thread。空表覆盖不会清空继承配置；仅支持已验证的字母/数字/下划线/横线 server 名称，其他名称失败关闭。

关闭 hooks/plugins/apps/memories/shell_snapshot/multi_agent/browser_use/computer_use/skill_mcp_dependency_install，关闭通知程序和网页搜索；project_doc_max_bytes=0。子进程环境只保留基本系统变量及既有 CODEX_HOME，不继承 API keys 或桌面 thread ID；额外 shell_environment_policy.set 非空则失败。只由 Codex 本身使用本机登录，不读取或复制 token。

有效配置层的非空 `.rules` 文件可能隐式 allow 越出 sandbox，因此首版发现它们即拒绝启动；不会改写用户规则。未知托管规则源或不可确认的隔离配置也拒绝。配置预检与 thread/turn 之间并非系统级原子锁，运行期间不应并发改配置或规则。`workspace-write` 限制写目录和网络，不保证其他本地文件不可读；未实测的读隔离不得宣传为已验证。真正认证运行时仍需验证本机 sandbox 生效与登录可用性。

## 验证与证据

离线入口：`PYTHONDONTWRITEBYTECODE=1 python3 tests/test_codex_backend.py`。fake 可执行子进程覆盖正常/空/错误终态、部分帧/断流、错会话、显式续聊、作用域、审批往返/拒绝/失效、超时/取消/迟到 final、配置/MCP/规则预检和附件路径边界。原 core 12 套单独运行 `tests/run_offline.py`；本线不修改公共测试入口。

集成退回项另用固定集成的私有 archive 加候选适配器复验真实 registry → dispatcher：双审批串行及取消/过期/错误、64,000/64,001 字符、长 ID/cwd 续聊与越界失败、首次 QR 绑定后配置顺序。该局部覆盖不替代 00 合并各线最终版本后的整体验收。证据在本任务 `integration-repair/` 子目录。

当前取证 CLI 为 `0.163.0-alpha.11.2`，318 个本机稳定 schema 与此前 .11.1 导出一致。只读登录状态为 ChatGPT；真实 initialize/initialized/config/read 已验证，含两阶段禁用 14 个 MCP 配置。未发起真实 thread/turn、模型生成或微信收发。候选固定 SHA、自测、独立 QA、远端回读记录保存在 `<git-common-dir>/wbridge-coordination/evidence/03/WBR-03-20261024-001/`。

协议参考：[OpenAI App Server](https://learn.chatgpt.com/docs/app-server)、[Rules](https://learn.chatgpt.com/docs/agent-configuration/rules)、[Sandbox](https://learn.chatgpt.com/docs/sandboxing)。官方滚动文档、安装版 schema、只读协议探针和真实模型验收分别记录。
