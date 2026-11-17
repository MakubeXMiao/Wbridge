# 04 CC 桥能力矩阵与首轮取证

任务：`WBR-04-20261023-001`。日期：2026-10-23（Asia/Shanghai）。责任线：04 CC 桥。采用中枢角色：02 架构；取证脚本由独立 06 复核。

## 1. 结论与版本范围

Claude Code 有执行、结构化结果、指定 session 续接和 SDK 双向审批/取消的接口基础；**当前还不能把本机登记为已验收的 Claude 执行后端**。本轮只验证了无凭据的 CLI 行为及固定发行包的静态类型，未调用真实模型、未验证真实审批或执行中取消。

| 对象 | 本轮实际状态 |
| --- | --- |
| 工作树起点 | `5bc172585e9005df536281152a9ce1510f899c5b`，接收时干净、detached HEAD；现绑定 `codex/claude-code` |
| 规则 | 读取主工作区 `b225563680f5acfe383bba9c08bf63d16a22dfe0` 的 AGENTS、DEVELOPMENT-START 和 COLLABORATION-DRAFT；未将治理提交合入本工作树 |
| 本机已有安装 | PATH、npm 全局清单及本次列出的标准位置未发现 Claude Code；当前系统 Python 未发现 `claude_agent_sdk`/`claude_code_sdk`。不是对整个磁盘的穷尽结论 |
| 主机 | macOS 27.2，arm64；Node `v24.19.0`，npm `11.17.0`，系统 Python `3.9.6` |
| 隔离 CLI 发行包 | 官方 npm `@anthropic-ai/claude-code-darwin-arm64@2.1.309`；下载包 SHA512 与 registry integrity 一致，实际 `--bare --version` 返回 `2.1.309 (Claude Code)`；不是原本预装版本 |
| TypeScript SDK | 官方 npm `@anthropic-ai/claude-agent-sdk@0.3.309`；tarball 校验通过，仅解包核对 `sdk.d.ts` 和元数据；未安装 native optional dependency，未执行 SDK query |
| Python SDK | 未安装、未绑定精确发行版。官方仓库要求 Python 3.10+；本机系统 Python 3.9.6 不满足，不能直接塞进现有 Python >=3.8 公共包。[官方 README](https://github.com/anthropics/claude-agent-sdk-python) |

本轮没有新建账号、凭据或云资源，没有连接微信 bot、队列或 Muse Hook。探针配置使用空 HOME/cwd、环境白名单、`--bare`、禁用工具，macOS sandbox 拒绝网络、Mach lookup、真实用户 HOME 的其他路径读取及输出目录外写入。未查询 Keychain；不将该过程写成认证能力验收。

## 2. 能力矩阵

证据等级：**本机实测**＝隔离 CLI 的实际输出；**固定版本静态核对**＝0.3.309 发行包类型；**官方资料**＝检索当日文档/官方源码，不能代替指定版本实测。

| 能力 | CLI / SDK 入口 | 本轮证据与判断 | 缺口 |
| --- | --- | --- | --- |
| 启动 | CLI `-p`；TS `query()`；Python `query()` / `ClaudeSDKClient` | CLI 版本/帮助通过，具备 print/json/stream-json/resume/permission 等参数；TS query 类型存在。SDK 宿主需单独验证 | 真实认证和生成未测 |
| 最终结果 | CLI JSON/NDJSON 的 `type=result`；SDK ResultMessage | 无凭据 JSON 和流式场景均实际得到结果事件；`subtype=success`、`is_error=true`、`terminal_reason=api_error`、exit 1 同时出现 | 成功生成与附件回传未测；不能只看 subtype |
| 流式进展 | `--output-format stream-json --verbose`；SDK partial messages | 本机收到 system init、synthetic assistant 认证失败消息及最终错误 result；TS 类型有结构化事件 | 模型 token 增量和真实工具事件未测；增量不是终态 |
| session 续接 | `--resume <id>`；SDK `resume`；Python client 同一连接 | 不存在的合成 session 实测 stderr 报不存在、exit 1；固定 TS 类型含 `resume?: string` | 两轮真实续聊、重启恢复、持久化未测；不使用“最近会话”做多用户路由 |
| 工具审批 | CLI permission host；SDK `canUseTool` / `can_use_tool` | 官方文档与固定 TS 类型核对；TS 回调有 `signal`、`toolUseID`，返回 allow/deny。不会因读取 stdout 自动形成双向审批 | 真实 allow/deny、超时/过期/重复响应、撤销、AskUserQuestion 未测 |
| 取消 | TS `Query.interrupt()`、`close()`、`Options.abortController`；Python client `interrupt()` | 固定类型核对；无输入进程启动 2 秒后发送 SIGTERM，V2 exit 0、V3 exit 143，均无最终 result；不能保证当时初始化阶段完全相同 | 生成中/工具中/等待审批时取消、子进程清理未测；不能据这些退出记录证明执行取消或固定退出码 |
| 错误输出 | 无效参数/schema、无认证、session 不存在 | 本机验证：参数/schema/session 错误在 stderr；认证失败在 stdout JSON/result，stderr 可为空；均 exit 1 | 真实网络错误、限流、崩溃重启未测 |
| 超时 | 宿主总 deadline；模型 turns/budget 另行约束 | 探针宿主有界超时；本轮正常用例均未触发超时路径。官方 API_TIMEOUT_MS 不是整次任务总时限 | 超时/强杀异常路径未做故障注入；不能宣称恢复已验收 |
| cloud session | `--cloud <session-id> -p` | 仅官方资料核对：返回发送状态后退出；stream-json 不用于此入口 | 本轮不创建/调用云 session，最终回答获取链路未验收 |

对应官方资料：[headless](https://code.claude.com/docs/en/headless)、[CLI reference](https://code.claude.com/docs/en/cli-reference)、[SDK Python](https://code.claude.com/docs/en/agent-sdk/python)、[SDK 会话](https://code.claude.com/docs/en/agent-sdk/sessions)、[审批](https://code.claude.com/docs/en/agent-sdk/user-input)、[流式输出](https://code.claude.com/docs/en/agent-sdk/streaming-output)、[cloud 输出](https://code.claude.com/docs/en/claude-code-on-the-web#output)。

## 3. 固定 SDK 类型核对

发行包来源：[0.3.309 元数据](https://registry.npmjs.org/@anthropic-ai/claude-agent-sdk/0.3.309)。这是静态证据，不是 SDK 执行结果。

- `CanUseTool` 含 `AbortSignal` 与 `toolUseID`；`PermissionResult` 区分允许/拒绝。
- `Options` 含 `abortController`、`canUseTool`、`permissionPrompts`、`resume`；没有理由把它们直接变成微信权限或永久许可。
- `Query.interrupt()` 返回 receipt 或 undefined，取决于 CLI 宣告的 capability；receipt 可能还有仍在队列中的用户消息。`close()` 结束消息读取；abort/关闭与轮次 interrupt 不等同。
- Result 的 success/error 分支都保留错误与终态相关字段；一轮 result 后仍可能有系统事件，streaming input 也可能还有后续轮次，不能把一个 result 等同于进程和全部任务结束。

Python 的取消回调实现另有官方 `main` 源码线索，但本轮未固定其发行版，因此不据它宣告 Python 审批撤销已验证。Python 文档目前将 callback 的 signal 标为预留；与 TS 的 AbortSignal 应分别适配。

## 4. 可复现本地取证与限制

仅使用已校验的官方 native binary，运行本目录 [probe_cli_offline.py](probe_cli_offline.py)。它不是 CC 适配器，也不读取 wbridge 运行状态。

```bash
python3 docs/lines/04-cc/probe_cli_offline.py \
  --binary /path/to/verified/claude \
  --output /path/to/new-private-probe-directory
```

脚本拒绝复用已有输出目录。原始 stdout/stderr、环境发现、发行包元数据、二进制/SDK 类型哈希、脚本快照与检查结果存本机 `<git-common-dir>/wbridge-coordination/evidence/04/WBR-04-20261023-001/`；不进入代码仓库。共享 `reports/04.md` 给出实际路径和当前提交。

取证迭代保留：

1. `cli-probes/`：版本/帮助/参数检查成功；运行入口被 sandbox 阻止写默认 `/tmp/claude-<uid>`。不能据此判断认证或 session 能力。
2. `cli-probes-v2/`：设定取证专用 `CLAUDE_CODE_TMPDIR` 后到达无认证/不存在 session 错误路径；空闲 SIGTERM 实测退出。该变量作为本次版本的隔离手段，不宣称所有 CLI 版本均支持。
3. `cli-probes-v3/`：最终脚本保留唯一 socket-denial JSON 控制、脚本快照/hash及逐用例落盘；8 项执行记录均需按上述结果分类，不能写成“8 项后端能力 PASS”。

无认证 JSON 的脱敏要点（真实 CLI 产生的错误事件，非人工伪造成功 fixture）：

```json
{"type":"result","subtype":"success","is_error":true,
 "terminal_reason":"api_error","result":"Not logged in · Please run /login"}
```

没有真实授权认证前，本轮不继续模型生成、审批往返、真实会话恢复或云执行验证；这些是后续验收前置条件，不阻碍本轮提交能力差距和候选接口要求。

## 5. 交给 01 的决定点

见 [INTERFACE-REQUESTS.md](INTERFACE-REQUESTS.md)：独立运行宿主/版本门槛、精确 session 映射、分层终态、审批双向相关性与有效期、取消确认和有界清理、总 deadline、与现有租约的关系、结果不明及迟到事件处理。公共契约经用户确认后，再进入本线适配实现。
