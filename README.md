# Wbridge

通过微信 iLink / ClawBot 把消息交给助理，再把回复送回微信。项目目前按两条产品路线组织：

1. **第一桥：Muse**，使用 Muse 平台的 Hook 和 worker。
2. **第二桥：Codex**，本分支当前接的是 **PEPE／原版 dot2api**；本机 Codex CLI 是另一种接法，不能混为同一运行环境。

本分支 `codex/bridge-02-codex` 是独立的 **PEPE 文字测试版本**。它不会自动修改、升级或切换已有 Muse 实例。开发对话编号仍为 01 公共底座、02 Muse、03 Codex、04 CC。

## 已验证的范围

2026-11-16，固定研发源 `705232adbb8d6efad24b3a11979d93ae3bad6e7d` 完成了一次 PEPE 原生自动处理验证，随后用独立微信账号跑通真实文字收发。后台两项任务成功、两封回复获微信 API 接纳，用户明确确认手机收到至少一条回复。API 接纳和手机实收是分开的证据。

本轮公开前集中 QA 的结果与限制见 [QA 说明](docs/QA-CODEX-BRIDGE-20261117.md)。公开提交是源码快照，提交号不同于研发源；生产模块和 Hook 字节保持审核版本，不附私有研发历史或运行资料。

| 能力 | 本分支当前边界 |
| --- | --- |
| 微信文字 → PEPE → 微信 | 已完成首轮真实手机收发，继续作为测试版本使用 |
| 图片、语音、视频、文件交给 PEPE | 当前 PEPE 路线不支持；不会冒充已看图或已读文件 |
| 权限与工具 | PEPE 沿用自身平台权限和确认；微信桥不提供逐工具审批，不保证每次工具调用都需要本人确认 |
| 电脑主聊与微信任务记忆 | 任务设置为沿用同一任务聊天；不等于共享电脑主聊全部历史，记忆共享未实测 |
| 超时、取消、重启 | 保留原 task 映射及待核对状态；不会自动重投或换另一个助理。关闭本地请求不证明 PEPE 已停止 |
| Muse／PEPE 在一个微信入口互切 | 仅有公共路由基础，当前不启用；跨主机接线、状态迁移、附件兼容仍须补齐 |
| 多日常驻、断线恢复 | 尚未完成持续验收，不宣称全天稳定或自动恢复 |

**已知边界问题：** 在多后端配置中，回复发送结果不确定时，现有切换检查与状态展示仍有缺口。这不触发自动重发，但在修复前不得把同一入口多助理切换作为已验功能。详见 [已知问题与规划](docs/TWO-BRIDGES-ROADMAP.md)。

## 运行链路与前置条件

微信 ClawBot → 独立桥进程 → 原版 dot2api 专用任务队列 → PEPE 原生事件任务 → 结果 → 微信。

需要能够持续运行桥和 dot2api 的 POSIX 宿主，以及已经配置好的 PEPE MCP 连接和原生事件处理任务。只创建 MCP 连接、只显示“订阅启用”都不代表可以自动接单；必须先验证真实通知、领取和完成链路。原生处理任务须允许本专用队列的领取、续租、完成或释放所需的协议状态写回，同时沿用 PEPE 自身其他权限。

本仓库不包含现有服务器设置、凭据或用户的 PEPE 任务配置。dot2api 使用 [Pluviobyte/dot2api](https://github.com/Pluviobyte/dot2api) 固定源 `66a761505ff73c5dca730260efcaeb5697db81ad`，需按其文档单独安装和配置；不是把本项目改个模型名称即可接上所有助理。

## 安装与检查

声明 Python >= 3.8，使用 POSIX 文件锁，面向 macOS／Linux；本轮完整离线回归实际运行于 macOS Python 3.12.13／Pillow 13.0.0，最低 Python／依赖组合和原生 Windows 未全验。

```bash
git clone --branch codex/bridge-02-codex https://github.com/MakubeXMiao/Wbridge.git
cd Wbridge
python3 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python tests/run_offline.py --output-dir qa-results/public
.venv/bin/python hooks/tests/run_offline.py --output-dir qa-results/muse
```

[PEPE 独立接入步骤与配置示例](docs/lines/03-codex/PEPE-QUICKSTART.md)。完整 Git 源码包含 Muse Hook；wheel 只安装 Python 包，不会自动安装 Hook 或设置平台投递目标。

## 两个桥如何一起用

推荐先保留两个独立入口，再做同一入口的文字切换。一个微信机器人只能有一个收件服务；不把 Muse 现有状态复制给 PEPE，也不直接在现有绑定数据库上加后端或清空状态。

完整的阶段、分工、切换体验与验收条件见 [两桥功能规划](docs/TWO-BRIDGES-ROADMAP.md)。这是待确认的规划，尚未实施。

旧技术文档保留其日期与当时背景；本分支当前边界以本页、QA 说明与 PEPE 快速接入文档为准。

[MIT](LICENSE)。
