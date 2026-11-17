# 03 Codex 首轮证据索引

任务：`WBR-03-20261023-001`。日期：2026-10-23（Asia/Shanghai）。

## 1. 本地证据位置

私有原始输出保存在所有工作树共用的 Git 目录内，不进入版本库：

```text
<git-common-dir>/wbridge-coordination/evidence/03/WBR-03-20261023-001/
```

用 `git rev-parse --path-format=absolute --git-common-dir` 定位。注册与进展分别为协调目录的 `registrations/03.json`、`reports/03.md`；03 不写总 registry 或他线记录。

| 证据 | 内容/结果 |
| --- | --- |
| `cli-help-index.json`、`00-version.txt`、`01-…07-*.txt` | 当前可执行文件、SHA256、8 次 version/help 的 argv/exit/输出摘要；CLI 0.163.0-alpha.11.1 |
| `local-install-inventory.json`、`python-sdk-inventory.json` | 已检查的二进制/SDK 路径和系统 Python distribution；没有全盘扫描或安装 SDK |
| `platform-inventory.json`、`legacy-source-inventory.json` | Node/npm/Python 与全局 npm 清单；两处旧原型代码路径及 SHA256，不包含运行状态 |
| `app-server-schema/`、`schema-generation.log` | 本机 generate-json-schema 成功，输出 318 份 JSON；未启动服务器 |
| `schema-capability-summary.json` | ThreadStart/Resume、TurnStart/Interrupt/Completed、审批 response 等字段抽取 |
| `parser-rejections.json`、`parser-rejection-0.log`、`parser-rejection-1.log` | 无效 sandbox 与未知 flag 均在解析阶段 exit 2、stdout 为空；不是模型/运行时失败验证 |
| `legacy-worker-isolated-test.json`、`.log` | 旧 worker-process 在 Node 合成子进程/临时 lease 上 9/9 PASS；无 Codex/微信调用 |

## 2. 可复核命令与边界

```bash
codex --version
codex --help
codex exec --help
codex exec resume --help
codex exec fork --help
codex app-server --help
codex app-server generate-json-schema --help
codex app-server generate-ts --help
npm list --global --depth=0
```

本轮使用当前 PATH 解析后的绝对二进制，摘要已固定。schema 生成命令为 `codex app-server generate-json-schema --out <本线证据目录>/app-server-schema`；这是写入隔离证据目录的协议导出，没有调用 thread/start 或模型。

参数失败命令分别为：

```bash
codex exec --sandbox __wbridge_invalid_sandbox__ --json --ephemeral --ignore-user-config --skip-git-repo-check 'Only a parser probe'
codex exec --wbridge-nonexistent-flag
```

两者都在参数检查时结束，不曾进入模型运行。不能据此声称已验证认证错误、provider 错误或模型超时。

旧测试使用 Node `--test <旧原型源码>/checks/worker-lease.test.mjs`，cwd 设为本线证据目录。测试文件使用 `mkdtemp` 创建并清理临时 lease，只启动自身创建的 Node 子进程。未运行 relay 主入口、旧整套 relay 测试或任何登录/发送命令。

测试 SHA256：`5f923fdf402713014ef65d087941886ae0346352bb5cc2efc0839f618795fe41`；被测 worker 源码 SHA256：`e198e97ef44464236c28962abdea114c70a309f81f82545953e65710e32796f5`。

## 3. 官方来源与版本差异

以下官方页面在本轮检索并打开核对；网页为滚动文档，未绑定安装版运行行为：

- [非交互模式](https://learn.chatgpt.com/docs/non-interactive-mode)：CLI JSONL、final 与显式 resume。
- [CLI reference](https://learn.chatgpt.com/docs/cli/reference)：exec 输出、配置/认证边界、ephemeral。
- [SDK](https://learn.chatgpt.com/docs/codex-sdk)：TS/Python SDK 路线及使用入口；本机未做 SDK 运行验证。
- [App Server](https://learn.chatgpt.com/docs/app-server)：initialize、thread/turn、终态、interrupt、审批和 usage；本机协议导出用于字段核对。
- [MCP server 迁移](https://learn.chatgpt.com/docs/mcp-server)：旧 server 入口迁移方向；未启动旧 server。

本机 `exec --help` 与 app-server schema 的 flag/字段存在性是本地证据；官方示例的最终消息、usage、取消/审批流程仍需要本机真实后端验证，二者不得合并成“实测通过”。

## 4. 未测与下一阶段条件

真实 Codex 最终结果、多轮 session、重启恢复、真实事件与 usage、运行中取消/超时、网络/额度错误、审批往返、附件处理均未测。没有读取认证内容、修改登录态、新建 API key、启动云资源、连接 bot、处理真实队列或改变 Hook。

后续先由 01 确认公共契约与隔离策略，再为所选路线安排真实后端验证。测试记录绑定 CLI/SDK/runtime、输入用例、run/session/turn ID 与终态；真实微信发送/手机收件属于另外的验收层。
