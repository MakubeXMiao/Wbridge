# 首版隔离启动与预检

任务 `WBR-01-20261024-001`。公共接口见 [BACKEND-CONTRACT-V1.md](BACKEND-CONTRACT-V1.md)。本页是本地候选操作说明；离线通过不代表真实模型或手机收发验收通过。

## 1. 准备独立实例

使用审查通过的完整集成版本，包含公共代码及需要启用的 `backends/codex.py`、`backends/claude_code.py`。适配模块未装入会明确预检失败。Python 最低版本仍为 3.8；平台 SDK 的额外依赖按对应开发线说明安装。

为新微信账号使用新的绝对 `WECHAT_BRIDGE_DIR`，为模型使用独立的绝对工作目录。不要用正在手测的 Muse 状态目录或同一个 bot 再启动第二个收件服务。下方名称是示例，应替换为自己的路径；它们不指向现有线上实例。

```sh
export WECHAT_BRIDGE_DIR="$HOME/workspace/wbridge-isolated-test"
mkdir -p "$HOME/workspace/wbridge-isolated-code"
python3 -m wechat_bridge login
python3 -m wechat_bridge wait-login
```

先完成扫码，再创建 `state/backends.json`。现有登录防护会拒绝把已有无法确认归属的数据目录绑定给新账号；提前创建后端配置也会被视作已有数据。登录凭据只保存在本地，不能复制到仓库、配置示例或协作者报告。新独立目录登录只清理本实例重登录通知；历史默认目录保留旧通知路径，显式启用 Muse 时使用 legacy_state_dir 对应的 Hook 通知。

## 2. 启用后端

在该实例的 `state/backends.json` 手动创建配置，权限设为仅自己可读写。这里的绝对路径是占位符；必须替换为已安装可执行文件、现有独立工作目录。`command` 是 argv 数组，不通过 shell；允许平台私有配置字段，但本例不填认证信息。

```json
{
  "schema_version": 1,
  "account_scope": "wechat-test-account-a",
  "default_backend": "codex",
  "legacy_state_dir": null,
  "job_timeout_seconds": 1800,
  "approval_timeout_seconds": 300,
  "backends": {
    "codex": {
      "instance_id": "local-codex-test-a",
      "command": ["/absolute/path/to/codex"],
      "cwd": "/absolute/isolated/workspace",
      "execution_policy": {"approval": "explicit", "sandbox": "workspace-write"}
    }
  }
}
```

`schema_version` 必须是整数 1。`account_scope` 是本地实例标识，不能代替登录身份；公共调度还绑定实际 bot 和机主。`instance_id` 标识后端安装/账号/运行实例。执行中路由固定，账号、实例、会话互相隔离。已有受管状态后不要直接改身份、工作目录、命令或权限配置；需要新的独立实例或人工核对恢复计划。

允许的默认后端是配置中启用的 `codex`、`claude_code` 或 `muse`。CC 采用相同字段，`execution_policy` 至少是 `{"approval":"explicit"}`；真实认证由运行它的协作者完成。权限策略不接受绕过审批/无限制沙箱，也不能通过命令参数覆盖公共权限策略。

如确实需要启用 Muse，加入 `"muse": {}`，并将 `legacy_state_dir` 设置为该 Muse Hook 的实际绝对状态目录，例如已确认的 `/absolute/hooks/state`。这是 Hook worker 状态目录，不等于桥接实例根目录。目录存在只证明本地路径可用；仍需 02 核对 Hook、真实 prompt、运行包和同一 bot 单收件服务。该配置不会自动安装/更新 Hook。

## 3. 预检与运行

```sh
chmod 600 "$WECHAT_BRIDGE_DIR/state/backends.json"
python3 -m wechat_bridge backend-check
python3 -m wechat_bridge backends status
python3 -m wechat_bridge serve
```

两个诊断命令只读取配置、本地身份是否齐全和持久 job 的状态数量；不会创建状态目录/数据库、启动模型或联网验证 token。输出仅含允许显示的诊断字段，不显示 token、模型环境变量、原始消息、job/审批 payload 或 provider 私有配置。DB 存在但无法读取/版本不匹配时退出码为 1。`serve` 在启用配置时也先做同一预检。

预检 `ok: true` 代表配置、路径、适配工厂及本地身份检查通过。`provider_auth: not_checked` 与 `wechat_token_validity: not_checked` 必须继续保留：它们不证明平台登录有效、真实生成可用或手机已收到消息。平台各自的认证只读诊断与真实闭环另行执行并记录版本。

无 `state/backends.json` 的新目录沿用 legacy 行为。文件损坏、字段不合法、不可读或失效链接都会明确失败，不静默退回 legacy。已有 `state/backend_jobs.sqlite3` 或 `backend_inbox`（即使为空）时，缺失配置会被拒绝；预检也会核对已有数据库绑定的账号、实例根目录及完整配置摘要，防止换配置后误接旧任务。

需要停用受管模式时，先停止调度与收发，保留原配置、DB、managed 输入和回执，再核对所有活动/unknown 任务及未发送输出。删除配置不是迁移操作；使用新的独立 legacy 实例或经审核的恢复方案，不把旧数据重放给 Muse。

## 4. 微信控制与恢复

- `/backend` 查看当前与可选后端；`/backend codex`、`/backend claude_code`、`/backend muse` 仅在空闲时切换。
- `/status` 查询执行状态；后到普通消息排下一轮，不改变正在执行任务的输入或路由。
- `/approve <提示中的公共审批ID> allow` 或 `deny` 只回答那一个绑定动作。普通“好”不是授权；一次允许不会变成永久允许。
- `/cancel <执行ID>` 请求取消；请求已接收不等于已经确认停止。

审批有效期最多 300 秒，并受 job 截止时间限制；job 默认 1800 秒，可配置 1～86400 秒。长期任务和审批使用持久 job，不延长 Muse 的 90/570 租约，不用虚假 progress 续租。重启/断连时不能把先前等待批准或正在执行的任务自动批准、重做或当成成功。

`unknown` 表示结果或副作用尚未确认，会阻止同一发送者继续调度/切换。保留数据库、收件、输出和回执，先核对平台进程、已发生动作与发送状态，再由操作者处理。首版没有自动“清空 unknown 重试”命令。不要删除数据库、清空队列、回滚游标或把 managed 输入移进 legacy inbox。

真实验收至少分别记录：代码 SHA/dirty、加载包和解释器、平台版本、后端实例与工作目录、进程启动时间、输入/执行/审批 ID、API 接受与手机收到；报告移除凭据和消息正文。Muse 还需 02 提供真实 Hook 与 prompt 的版本/哈希。配置样例、离线测试和预检不能替代这些证据。
