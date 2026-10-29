# Claude Code adapter v1

任务 `WBR-04-20261024-001`，2026-10-24。04 owns `wechat_bridge/backends/claude_code.py`、本线 tests/docs；公共注册、CLI、打包、会话与审批持久状态归 01。

契约：01 `4555329439ebf2e1d7a102022c5d04d35926b732` 的 `docs/lines/01-core/BACKEND-CONTRACT-V1.md`。本实现导出 `create_backend(config)`，无公共内部模块依赖。可由 00 cherry-pick 本线候选，再合并 01 公共入口；本分支单独不提供微信调度入口。

## 执行与审批

使用 Python >=3.8 标准库直接驱动 **Claude Code 2.1.309** 的双向 NDJSON 控制协议；不额外安装 Python/TS SDK。协议对照官方 TS SDK **0.3.309** 发行包 `sdk.mjs` 和 `sdk.d.ts`。目前仅支持 POSIX 进程组，原生 Windows 返回 `unsupported_platform`。版本不同在发 prompt 前失败，不能静默升级协议。

每个 driver 只接收一个 execution/attempt、一个冻结输入批次。新会话传新 UUID `--session-id`，续聊只传指定 `--resume`，禁止最近会话查找。公共层负责在账号/发送人/实例/会话范围保存真实 session；适配器校验当前 config 与 request 的实例、cwd、policy，检查所有携带 session 的返回帧。恢复失败不改成新开。

启动：有界 `--version` → `initialize` 校验版本及 default 权限 → `list_permission_rules` 核对实际 ask 规则、原 cwd、无额外目录、无被管理策略禁用的规则 → 单个 `user`。不是只检查 CLI 参数字符串。子进程使用 argv，用户内容通过 stdin JSON 的 `client_composed:true` 发送，禁用 `/command`、`@path` 自动展开。附件仅传公共层验证后的本地文件路径，访问仍须工具审批；远程 URL 不下载。

`--safe-mode` 保留协作者自己的认证，跳过个人/项目定制；空 setting sources、空 strict MCP 配置、禁用 hooks、固定工具 `Read/Glob/Grep/Bash/Write/Edit`，并给启用工具设置 ask 规则。配置可缩小这六种工具；不能添加未知工具或 bypass 参数。子进程禁用自动更新和后台任务。管理策略导致规则不符合预期时拒绝运行。

`can_use_tool` 请求保留 `request_id` 与 `tool_use_id`；hash 绑定 execution、attempt、session、cwd、审批 ID、工具名及完整 input。公共侧再次绑定账号、发送者、实例、有效期和一次性审批。允许原样回传 `updatedInput` 与 `toolUseID`；拒绝只发 deny，不写 `updatedPermissions`。展示完整 JSON 动作（含 cwd），超过 2000 字或出现常见凭据字段/令牌格式则失败，不显示删节动作让人批准。该检测不能代替操作者避免把秘密写入工具命令。

同一进程的多个权限请求保持原始 ID/digest/deadline，公共 facade 依次展示；首项答复后才展示下一项，以匹配 v1 的单项 waiting_approval 状态。排队等待不重置审批有效期。

不支持 AskUserQuestion、MCP 对话、`requires_user_interaction` 专用交互、cloud、永久授权；它们不会被转换为 allow。撤回审批会终止为 `unknown/approval_withdrawn`，使核心终结旧审批；晚到回答不能恢复执行。

上述模式及协议的依据：[官方 headless](https://code.claude.com/docs/en/headless)、[审批回调](https://code.claude.com/docs/en/agent-sdk/user-input)、[权限顺序](https://code.claude.com/docs/en/agent-sdk/permissions)、[固定 SDK 发行元数据](https://registry.npmjs.org/@anthropic-ai/claude-agent-sdk/0.3.309)。`--bare` 不读取订阅 OAuth，因此此运行器使用 safe mode；empty HOME 的离线探针与正常协作者认证运行分开。

## 终态、资源和失败

只有匹配当前会话的单个 `result`，`subtype=success`、`is_error=false`、无错误正文、完成的 terminal/stop reason、非空 final、无 queued/deferred work，并在关闭 stdin 后正常 exit 0 才转换公共 `final`。assistant、增量、退出 0、审批返回均不当作 final。错误消息只输出受控错误码，原始 stderr/account 数据不进入公共事件。公共 v1 的回答上限是 64000 个 Python 字符：恰好 64000 原样提交，超出时发 `failed/final_text_too_large`（副作用状态保留 possible），不发 final、不截断、不重试；该已知交付失败不会把调度任务变成 unknown，也不需要为此解除 unknown 冻结。后续不同输入可正常处理。

取消清除待发输入/允许，作废审批，发送 `interrupt(cancel_queued=true)`，等待有界宽限，再 TERM/KILL 自己的进程组并 wait；只有清理后发终态。超时/审批到期不自动批准或重试。过程断线、最终结果后进程不退出、残留子进程持有管道、未知交互、续聊不明均失败或 unknown。进度队列和字节数有上限，溢出仍保留一个终态。

版本探针和主进程的回收异常均覆盖为 `unknown/cleanup_failed`；管道仍会关闭，保留受管句柄供有界重试，不继续提交 prompt 或假报已回收。`close()` 的监督线程等待与残留回收共用一个 20 秒绝对截止时间。独立故障注入用于验证此路径；历史一次 subprocess ResourceWarning 缺少对应 trace，未归因为该缺口，也不据此宣称真实残留进程已复现。

这是进程组清理与审批控制，**不提供操作系统 sandbox**；公共 policy 要求非 none sandbox 时拒绝启动。已经发生的工具副作用不会回滚，脱离进程组的外部服务也不因取消变成撤销。首版应在专用、最小权限工作目录内运行，不批准启动常驻服务或后台进程。

配置（公共 `backends.json` 中单个 backend）：

```json
{
  "instance_id": "collaborator-cc-a",
  "command": ["/absolute/path/to/claude"],
  "cwd": "/absolute/isolated/workspace",
  "execution_policy": {"approval": "explicit"},
  "timeout_seconds": 1800,
  "approval_timeout_seconds": 300,
  "startup_timeout_seconds": 15,
  "cancel_grace_seconds": 1,
  "max_output_bytes": 4194304,
  "tools": ["Read", "Glob", "Grep", "Bash", "Write", "Edit"]
}
```

`command` 只能含可执行文件，无追加 flags。私有 executor 的 `env` 可用完整环境字典替换继承环境，仅供受控测试/宿主配置；不要把密钥放进仓库或 backends.json。一般协作者配置省略 env，CLI 使用协作者自己的正常认证。总 deadline 取公共 job 与配置较短者；审批最多 300 秒；配置在工厂和私有执行器中校验：总任务最多 86400 秒、审批最多 300 秒、启动等待最多 15 秒、取消宽限最多 5 秒，均拒绝布尔值、非有限数和超限值。`close()` 的等待预算独立固定为 20 秒；清理异常仍抛出，不假报取消成功。driver poll 每次最多 100 事件。

## 验证与验收范围

```bash
PYTHONDONTWRITEBYTECODE=1 python3 tests/test_claude_code_backend.py
```

测试用临时目录、替换环境、Python fake CLI，不连接模型、微信或真实账号。覆盖真实 wire 形状与状态机的正常/失败路径、审批篡改和过期、终态/退出/串线、超时/取消、版本/策略审计、资源上限、公共 v1 映射。此测试不证明 Claude 真正执行了工具。

可选真实 CLI 无认证探针（macOS 专用，输出目录必须未存在）：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 docs/lines/04-cc/probe_backend_offline.py \
  --binary /absolute/path/to/claude-2.1.309 \
  --output /absolute/private/new-probe-directory
```

探针空 HOME、替换环境，sandbox 禁网络、Mach lookup、其他真实 home 读取及证据目录外写入；先校验 socket EPERM。成功只表示真实 CLI 的版本、初始化、实际 ask 规则与未认证诊断跑通。它不装认证、不启动微信，也不是成功生成、真实权限工具或真实续聊验收。

固定 SHA、自测与非实现者 QA 结论记录在本线共享 `reports/04.md`；交付时 Git commit 是代码和测试绑定点。协作者真实测试及脱敏报告见 [COLLABORATOR-ACCEPTANCE.md](COLLABORATOR-ACCEPTANCE.md)。公共集成、安装与手机验收必须绑定最终集成 SHA，单支通过不能替代。

## 集成 QA 退回修复验证

2026-10-24，00 在固定集成 `cad9aeec2db30d594d8fdca184360c0e8499e7e4` 复现两项：64001 字符 final 违反公共 64000 上限并冻结后续队列；取消宽限 31 秒时 close 在 32 秒仍未返回。上述限制由本线实现，不放宽公共契约。

[接线回归脚本](test_dispatcher_integration.py) 从指定本地 Git 集成 SHA 创建临时源码快照，叠加本线 adapter/fixture，在子进程中运行真实 registry → Dispatcher → facade；只替换外部 Claude 为 fake，微信 sender 使用合成回执。测试不写他线工作树、运行账号或真实队列。固定集成基底与本线候选必须一起记录；此离线接线通过不代替新集成 SHA 的最终 QA 或真实手机验收。

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p test_claude_code_backend.py -v
PYTHONDONTWRITEBYTECODE=1 python3 docs/lines/04-cc/test_dispatcher_integration.py
```

接线脚本需要本地 Git 对象库已有上述固定集成提交；输出记录基底 SHA、叠加文件 hash 和六个 ASCII/中文边界场景。实际 sender 的分片预算保持不变，合成 `/status` 输入续接 context 后核对完整分片回执，明确区分数据库 admitted、文件 sent 和真实手机未测。
