# CC 协作者安装与验收

任务 `WBR-04-20261024-001`，2026-10-24。当前是实验性候选：fake 协议测试及真实 CLI 空 HOME 未认证诊断，与成功生成/续聊/审批/微信手机闭环分开。本机没有已认证 CC，本轮没有联系协作者或使用他人账号。

## 1. 获取与安装

1.1 从本公开仓库取得**完整固定提交**，包含 CC 模块和公共调度。下方文档记录固定 CLI 版本与当时验证边界，当前整体范围见[更新说明](../../PUBLIC-UPDATE-20261029.md)。

```bash
git clone https://github.com/MakubeXMiao/wechat-bridge-muse.git
cd wechat-bridge-muse
git checkout --detach <本公开仓库的完整提交号>
git rev-parse HEAD
git status --short
python3 -m venv .venv
.venv/bin/python -m pip install -e .
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/test_claude_code_backend.py
```

Python >=3.8，macOS/Linux 等 POSIX。不安装 Python Agent SDK，不提高公共包最低 Python。macOS Python 缓存写入导致历史回归失败时，保留上例环境变量。原生 Windows 此适配器不支持，不能仅凭 CLI 支持 Windows 宣称适配器支持。

1.2 按 [Claude 官方安装文档](https://code.claude.com/docs/en/setup#install-a-specific-version) 安装 **2.1.309**，例如已安装 native launcher 后：

```bash
claude install 2.1.309
claude --version
command -v claude
```

记录实际可执行文件绝对路径和 SHA256。适配器会在每次运行前核验版本，版本不符明确失败；不要用修改版本字符串绕过检查。CLI 后续升级应由 04 先核对协议并重新 QA。

1.3 协作者自行在自己的终端登录自己的 Claude 账号：

```bash
claude auth login
claude auth status
```

`auth status` 默认 JSON，退出 0/1 表示登录/未登录；只回报状态、认证方式、CLI 版本，不复制 token、账号邮箱、OAuth 文件或系统钥匙串内容。无账号/无权限时保留待测，不借用 Codex 认证或其他人的登录。官方参数依据：[CLI 参考](https://code.claude.com/docs/en/cli-reference)。

## 2. 配置和启动

2.1 建立专用 Claude 工作目录、独立测试微信/bot 和全新 state。不要复用正在运行的 Muse bot/state、放宽机主过滤或启动竞争收件服务。CLI 是协作者本机登录；微信是独立 bot 的登录；两者分别验证。

2.2 用 01 公共入口按最终集成版 README 完成该 bot 的绑定与配置预检。把下例写入该 state 的 `backends.json`（替换绝对路径、instance/account，不存密钥）：

```json
{
  "schema_version": 1,
  "account_scope": "cc-test-bot",
  "default_backend": "claude_code",
  "legacy_state_dir": null,
  "job_timeout_seconds": 1800,
  "approval_timeout_seconds": 300,
  "backends": {
    "claude_code": {
      "instance_id": "collaborator-cc-a",
      "command": ["/absolute/path/to/claude"],
      "cwd": "/absolute/isolated/cc-workspace",
      "execution_policy": {"approval": "explicit"}
    }
  }
}
```

公共启动命令以 **01 的最终集成 README/`wechat-bridge --help`** 为准，不用旧的 Muse Hook 启动命令替代；本线不复制收发服务。记录启动命令（去掉凭据）、解释器路径、实际加载 `wechat_bridge.__file__`、候选 SHA、dirty patch hash、启动时间。先预检成功，再在独立 bot 启动唯一收件服务。记录此时后端配置文件 hash，不上传原始配置/状态。

2.3 若后端回答超过公共上限 64000 字符，将收到 `final_text_too_large` 失败，不会收到被截断而冒充完整的回答；该任务不自动重跑，后续不同任务可以继续。配置中启动等待不得超过 15 秒、取消宽限不得超过 5 秒，关闭等待预算为 20 秒。

2.4 在微信先发 `/backend` 确认当前 CC；多后端集成测试需由 01 配好另一测试 backend。只在空闲时 `/backend claude_code`；任务运行中尝试切换应被拒绝，路由不会随晚到消息改变。普通“好”不能批准；必须使用 `/approve <public-approval-id> allow` 或 `deny`。`/cancel <execution-id>` 是请求，等待明确取消结果再继续。

## 3. 逐项真实验收

只在专用目录使用合成内容/文件。每项登记 PASS/FAIL/未测，附时间、源码 SHA、CLI 版本、session/execution/approval 的脱敏标识与实际手机结果。

| 用例 | 操作 | 可观察通过条件 |
| --- | --- | --- |
| 缺失环境 | 配置不存在 CLI；清空测试 HOME 的离线探针 | 分别 cli_not_found / not_authenticated，无生成或工具运行 |
| 生成 | 微信“只回答 SYNTHETIC-CC-OK，不用工具” | 结构化 final 成功且手机收到；记录 API 接受与手机收到两个事实 |
| 指定续聊 | 第一轮给合成暗号，第二轮问刚才暗号 | 两轮同一明确 session；重启后续聊也须记录实际 session |
| 允许一次 | 请求在专用 cwd 新建 acceptance.txt，固定内容 | 创建前出现完整动作+cwd；显式 allow 后文件和最终回答吻合 |
| 拒绝 | 请求新建 denied.txt，显式 deny | 文件始终不存在；拒绝不写永久权限，模型可说明拒绝结果 |
| 重复/晚到 | 对已处理 ID 再 allow；等待过期后再 allow | 核心拒绝、CLI 无重复工具动作；过期任务不重放 |
| 身份 | 其他发送者回复同一审批 ID | 无权批准，无工具动作，不把普通文本当允许 |
| 取消 | 待审批时取消；生成中取消；合成工具执行中取消 | 审批失效；进程/受管子进程停止；无晚到 final 投递，不承诺撤销副作用 |
| 超时/重启 | 缩短测试 deadline；审批等待时停止/重启服务 | 不自动批准；持久任务 unknown/超时，不能自动重做 |
| 隔离/切换 | 独立账号/实例会话，运行时切换/排队输入 | 不串 session；本轮冻结路由；迟到结果不重复入队 |
| 本地文件 | 发送合成文本附件/图片，观察 Read 审批 | 公共层记录可读本地路径、工具获准后处理；实际读图能力另记 |

工具执行应只改合成文件；不批准安装常驻服务、访问私密目录或永久授权。`safe-mode` 和人工审批不是 OS sandbox。失败、断流、取消后的 side_effect_state=possible/unknown 不得通过重放任务“验证”。先由操作者核对文件/进程，再按公共恢复流程解除 unknown。

## 4. 脱敏回报模板

```text
任务: WBR-04-20261024-001
集成完整SHA / CC候选SHA:
dirty: clean 或 patch SHA256（原patch留本机）
OS/架构/Python版本:
解释器/加载包: 用 <HOME> 替代用户名
Claude版本/二进制SHA256:
认证: 已登录/未登录；方式；不含邮箱/token
bot/account/instance: 使用稳定别名
config hash / cwd别名:
服务启动/重启时间和实际加载证据:
用例编号、起止时间、PASS/FAIL/未测:
session/execution/attempt/approval: 稳定脱敏别名
最终事件类型 / error_code / side_effect_state / exit:
API接受时间 / 手机实收时间 / 人工补发: 分别填写
文件/进程验证: 合成文件名、存在与否、受管进程已退出与否
复现步骤与最小脱敏片段:
未测项/限制:
```

不提交完整私聊、stdout/stderr、account 信息、session transcripts、state/队列/租约、token 或认证目录。源码 SHA 只证明磁盘代码；需要加载路径和重启证据才可将运行结果归给该版本。修改源码或配置后重新记录版本与回归，不能沿用上一轮实机 PASS。
