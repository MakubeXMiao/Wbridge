# Codex 能力矩阵

任务：`WBR-03-20261023-001`。观测时间：2026-10-23（Asia/Shanghai）。本表严格区分帮助/协议声明、源码事实、合成测试和真实后端实测。

## 1. 本机安装与证据版本

| 项目 | 本轮观察 |
| --- | --- |
| PATH 中的 CLI | `codex-cli 0.163.0-alpha.11.1` |
| 二进制 | `/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex` |
| 二进制 SHA256 | `9394a943e3fce54f998ecc150eb0b7d6e22ce0da48e5063c59b4883e80aeaecd` |
| Node / npm | `v24.19.0` / `11.17.0` |
| 独立 TS SDK | 全局 npm 清单、本工作树及旧 relay 的指定 manifest 路径未发现 `@openai/codex-sdk` 或 `@openai/codex`；没有安装 SDK |
| 独立 Python SDK | 本线系统 Python 3.9.6 的 distribution 清单未发现 `openai-codex`、`openai` 或 `codex` |
| 盘点边界 | 只查上述可复核位置，不推断全盘不存在其他解释器或 SDK |
| 本线版本 | 从 `5bc172585e9005df536281152a9ce1510f899c5b` 建立 `codex/codex`；业务代码与上游修复 `462337c34f70175e2ee2ddd2bd2a7b1ace402c57` 一致 |

官方仍分别提供 SDK 和 app-server 路线。TS SDK 提供 `startThread()`、`run()`、`resumeThread(id)` 与 `finalResponse`；当前另有基于本地 app-server 的 Python SDK。SDK 默认携带或选择的 runtime 不应视为本机这个 CLI 版本，后续需分别锁定包版本和实际二进制。上述属于文档支持，均未做 SDK 运行验证。[官方 SDK](https://learn.chatgpt.com/docs/codex-sdk)

## 2. 能力矩阵

| 能力 | 本机 CLI 帮助/协议与官方依据 | 旧原型源码事实 | 本轮验证及缺口 |
| --- | --- | --- | --- |
| 启动 | `exec` 支持 stdin prompt、`-C`、sandbox、`--json`、`--output-last-message`、`--output-schema`；app-server 提供 initialize/thread/start/turn/start | 直接启动 CLI，prompt 经 stdin；可执行路径硬编码，继承宿主环境 | 帮助与离线 schema 已核；真实模型启动未测 |
| 最终结果 | CLI JSONL 有完成的 `agent_message.text` 与 `turn.completed`；`-o` 保存最终消息；app-server 需关联完成的 agentMessage 与 turn 终态 | 清除旧答案后读取结果文件，但空白答案会替换成提示文字再发送 | 已读源码；真实 final、schema 校验与非空校验未测，不能用退出 0 或发送接受当作最终结果成功 |
| 续聊/session | CLI 有 `exec resume <SESSION_ID>`；app-server 有 thread/resume。本机 Thread schema 同时存在 `id` 与 `sessionId` | 每次 `--ephemeral`，不保存会话、不维护 sender→session 映射 | 命令/字段已核；跨进程与多轮续接未测。不得用 `--last` 代替显式归属映射 |
| 结构化事件 | CLI JSONL 的 `thread.started`、`turn.*`、`item.*`、`error` 与 app-server 的 `thread/…`、`turn/…`、`item/…` 是不同事件形状 | 未启用 `--json`，stdout/stderr 合流写日志，只取最后文字 | 帮助/schema 已核；真实事件流、断流和未知事件兼容未测 |
| 取消 | CLI 进程信号没有本轮已核的统一结构化取消终态保证；app-server `turn/interrupt` 要求 threadId+turnId，后续应收到 interrupted 终态 | 内部 stop callback，relay 整体 SIGINT/SIGTERM 停当前 worker；没有按微信任务取消入口 | 旧进程管理器合成立即取消/进程组清理通过；真实 CLI 与 app-server 取消未测，请求成功不等于停止确认 |
| 失败/超时 | CLI 参数解析可先失败；app-server TurnStatus 为 completed/interrupted/failed/inProgress，Turn 有 error；无已验证的通用 turn 超时保证 | 默认 600000ms 外层计时；超时 CODEX_TIMEOUT，非零退出统一 CODEX_EXIT，未保留数值退出码；不确定状态转复核 | 两个参数拒绝探针均 exit 2；旧管理器合成超时、spawn 失败、exit 7 及子进程清理通过。模型/网络/额度故障未测 |
| 审批 | CLI 帮助提供 on-request/never 政策，但 exec 不是本轮已验证的双向审批通道；app-server schema 有 requestApproval/decision/关联 ID | 未显式设置审批策略，没有 request ID、批准/拒绝回传 | 仅帮助/schema 支持；审批产生、回答、超时、恢复、取消竞态均未测，不声明 CLI 交互审批可用 |

CLI 官方非交互说明支持 JSONL 与显式 resume；CLI reference 说明输出文件和隔离参数。[非交互模式](https://learn.chatgpt.com/docs/non-interactive-mode)、[CLI reference](https://learn.chatgpt.com/docs/cli/reference)。app-server 的生命周期、终态、interrupt、审批和用量字段见[官方协议](https://learn.chatgpt.com/docs/app-server)，本轮以本机生成的 schema 核对字段存在性；schema 不是运行回执。

### 本机协议的具体限制

- `turn/completed` 是通知名称，仍须检查 `turn.status`；它可以是 failed 或 interrupted。
- `agentMessage.phase` 可空。本机描述明确说 provider 不总是一致输出 phase；不能只靠 `final_answer` 一个字段决定成功，也不能把 commentary 当最终答案。缺失时的兼容提取策略须在契约中明定并单测。
- `thread.id`、`thread.sessionId`、`turn.id` 分开保存，不从一个 ID 推导另一个。
- `turn/interrupt` 要求 `threadId` 和 `turnId`。返回 `{}` 是取消请求被接受；运行结果以之后的终态或明确的停止证据为准。
- 审批 JSON-RPC request ID 与 thread/turn/item ID 分开。`serverRequest/resolved` 表示请求已回答或清理，不能单独证明受限动作执行成功。
- `--ignore-user-config` 跳过用户配置，认证仍使用现有 Codex home；`--ephemeral` 不保存 rollout。它们不能合起来证明认证隔离，也不适合作为持久会话续接验收方式。

旧 MCP server 不是建议的新集成入口；官方迁移文档指向 app-server。本轮没有启动这些服务，也没有把滚动网页内容当作安装版运行结果。[官方迁移说明](https://learn.chatgpt.com/docs/mcp-server)

## 3. 旧原型盘点

已只读比较 `~/Library/Application Support/Codex-WeChat-Relay` 与原工作目录 `~/Documents/Codex/2026-10-20/new-chat/work/wechat-integrations` 的代码；两处 `relay.mjs`、`worker-process.mjs` 相同。没有读取账号、队列、运行日志或完整消息。

| 源码位置 | 事实与影响 |
| --- | --- |
| `relay.mjs:17`、`:141–170` | 硬编码本机 ChatGPT.app CLI 路径；`exec --ephemeral --sandbox workspace-write --skip-git-repo-check`，禁用旧 wechat-ilink MCP，最终文件经 `--output-last-message` 返回 |
| `relay.mjs:122–130` | 空结果替换提示文字，发送 API 接受后可以记录为 api_accepted；这是发送处理逻辑，不证明有效模型答案已取回 |
| `relay.mjs:151–162` | 每条消息独立入口，不继承桌面聊天；没有 session 持久映射或 resume |
| `worker-process.mjs:117–175` | detached 进程组、默认 600 秒、SIGTERM→宽限→SIGKILL、组消失检查；退出 0 后也清理剩余辅助进程；取消、超时或非零退出失败 |
| `relay.mjs:83–90` | 重启时 running/sending 转复核，不根据不确定状态盲目重跑 |

`relay.mjs` SHA256：`e0728669e89a02605b54ff22a861c983144cbb9721c17678defe324349d7200c`。

`worker-process.mjs` SHA256：`e198e97ef44464236c28962abdea114c70a309f81f82545953e65710e32796f5`。

本轮只执行旧 `checks/worker-lease.test.mjs` 的 9 项 Node 合成子进程测试，全部通过。测试不调用 Codex 或微信，不替代真实模型结果、续聊、审批或手机收件验收。旧整套 20/20 仅为历史记录，本轮没有重新执行整套 relay 测试。
