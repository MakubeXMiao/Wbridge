# wbridge

微信内的轻量私人 AI 助手。项目将基于现有 iLink / ClawBot 桥接，逐步支持 Muse、Codex 和 Claude Code 三种可选后端。

## 当前开发基线

代码导入自 `MakubeXMiao/wechat-bridge-muse` 的 `fix/qa-reliability-20261022` 分支，提交 `462337c34f70175e2ee2ddd2bd2a7b1ace402c57`。

当前已有微信收发、媒体处理、持久队列及 Muse Hook / worker 租约实现。统一后端接口、Codex 和 Claude Code 适配尚未实现。现有 Muse 修复仍需真实微信及云环境验收，离线检查不代表手机已收到回复。

包名 `wechat-bridge`、Python 模块 `wechat_bridge` 和命令 `wechat-bridge` 暂时沿用，避免在导入阶段改变兼容性。

## 本地准备与离线检查

依赖 Python 3.8 或更新版本；当前实现使用 `fcntl`，原生 Windows 支持尚未适配。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/run_offline.py --output-dir qa-results/offline
```

离线入口隔离状态、阻断真实 Python socket，并使用测试 Hook runtime，不需要微信登录。禁写字节码的环境变量用于避免系统 Python 缓存干扰临时目录的无写入检查。`tests/fixtures/hook_runtime.sh` 仅用于测试。

## 开发与交接

- [开发基线与工作树约定](docs/DEVELOPMENT-BASELINE.md)
- [当前桥接运行约定](docs/CONNECTOR.md)
- [Worker 状态协议](hooks/WORKER-STATE.md)
- [Muse 真实环境验收](docs/MUSE-TESTING.md)

公共底座由总控协调，三个后端分别在独立工作树开发。并行实施前先确定公共接口与文件归属；工作树不会自动隔离机器人、服务、日志或运行状态。
