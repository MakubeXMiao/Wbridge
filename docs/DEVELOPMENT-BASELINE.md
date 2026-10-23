# wbridge 开发基线

建立日期：2026 年 10 月 23 日（Asia/Shanghai）。本次只导入代码并准备本地开发，不部署服务、不操作微信账号。

## 1. 来源与历史

- 上游仓库：[MakubeXMiao/wechat-bridge-muse](https://github.com/MakubeXMiao/wechat-bridge-muse)。
- 导入分支：`fix/qa-reliability-20261022`。
- 导入提交：`462337c34f70175e2ee2ddd2bd2a7b1ace402c57`。
- 导入前源工作树干净。目标使用独立 Git 对象库，不依赖原目录的 worktree 元数据或对象引用。
- 源副本是浅仓库：保留其已有的 13 个可达提交，历史边界为 `e1e72168ca8e1d2819df439cd3a1119aa4078eca`；不声称导入了更早的上游历史。
- 未导入源仓库其他实验分支、未跟踪文件、虚拟环境、账号凭据、消息、队列、日志或 Hook 运行状态。
- `main` 是 wbridge 的本地开发起点；不改变原仓库或其远端 `main`。本轮未配置远端、未推送。

导入后的提交只补充 README、本文与本地文件忽略规则。Python 包、Hook、测试及许可证保留导入版本，包名和 CLI 暂不改名。

## 2. 工作树准备

本次基线以 `wbridge-baseline-20261023` 标签固定。本轮不创建三个工作树；请以包含该标签的 wbridge 当前 `main` 为共同起点创建 Muse、Codex 和 Claude Code 工作树。建议分支名分别为：

- `codex/muse`
- `codex/codex`
- `codex/claude-code`

三个工作树创建后，先由总控确认消息、附件、会话、执行状态、结果、取消和审批接口及文件归属，再开始适配实现。现阶段不假设三个适配器已经具备独立目录或稳定公共契约。

工作树只隔离代码。各线使用独立虚拟环境；需要运行集成测试时，另外指定独立的 `WECHAT_BRIDGE_DIR`、`WECHAT_HOOK_STATE_DIR`、`WECHAT_BRIDGE_PYTHON` 和 `WECHAT_BRIDGE_EXECUTABLE`。同一个 bot 只由一个收件服务轮询；不得直接复用现有 Muse 的账号状态或启动三套服务抢收。

## 3. 已知验收边界

现有实现针对同发送者单 owner 批处理、90 秒软租约、从 begin 起 570 秒硬上限、续租与过期身份隔离做了修复。首次事件未 begin 的恢复窗口仍为 180 秒。

五条连发、首回延迟、owner 死亡后恢复、重复消息与媒体须按 [Muse 验收说明](MUSE-TESTING.md) 实测。导入、安装、离线测试或 API 接受均不能代替真实手机收件和云平台验收。

`docs/CONNECTOR.md` 的运维段仍保留旧的 180/600/700 秒阈值表述；当前 worker 租约期限以 [WORKER-STATE.md](../hooks/WORKER-STATE.md)、Hook prompt 和源码为准。此处记录已有文档差异，导入阶段不修改运行协议。

本地离线检查命令：

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/run_offline.py --output-dir qa-results/import-baseline-20261023-no-bytecode
```

测试报告保存在忽略目录 `qa-results/`，不包含真实账号或消息。macOS 系统 Python 可能在临时 HOME 下写入字节码缓存，影响 worker dry-run 的无写入断言；上面的环境变量用于排除该解释器行为，未修改业务或测试代码。各线自测后由非实现者复核；真实环境验收与后续合并、发布另行按授权执行。

导入当日由独立 QA 在 Python 3.9.6 下执行：首次 11/12 套件通过，失败项确认为解释器字节码缓存写入；设置上述变量后 12/12 套件通过。首轮记录保留于 `qa-results/import-baseline-20261023/`，通过记录位于 `qa-results/import-baseline-20261023-no-bytecode/`。本地 editable 安装与 CLI 帮助检查通过，安装指向本工作区；凭据/运行状态扫描、代码及权限模式比对、Git 对象完整性检查均通过。
