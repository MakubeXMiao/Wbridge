# wbridge · 微信多助理桥

通过微信 iLink / ClawBot 接收消息，把请求交给 Muse、Codex 或 Claude Code，再把结果发回微信。Muse 使用 Hook 与 worker；Codex 和 Claude Code 使用宿主上的命令行程序。三种接入方式需要分别安装和认证。

这是 [MakubeXMiao/wechat-bridge-muse](https://github.com/MakubeXMiao/wechat-bridge-muse) 的完整源码更新。包名、模块名和命令仍为 `wechat-bridge` / `wechat_bridge`，保留原有兼容性。

## 这一版做了什么

- 增加附件箱：图片、语音、视频和文件统一保存原始解密字节，记录索引，可按日期、原名、类型或标签查找；附件不执行、不解压。
- 保留图片原顺序和失败位置，支持明确引用、取回及分批发送；结果不确定时保留记录，避免盲目重发。
- 加强收件防重、队列恢复、进程监督、权限和目录检查，提供部署前后 Hook 投递目标保全检查。
- 集成后端切换、单次显式审批、取消，以及 Codex / Claude Code 适配代码。

默认附件策略为单件 **100 MiB（104857600 字节）**、普通保留期 30 天、总量 5 GiB（含钉住的附件）。当前附件清理只预览，**不会因满 30 天自动删除**；达到容量边界会拒收新附件正文。私人失败留样默认关闭。详见[附件箱实现与限制](docs/lines/01-core/ATTACHMENT-INBOX-IMPLEMENTATION.md)。

## 已验证到哪里

本次生产源码对应已完成独立离线检查的研发基线 `1f0160bc42e016cb47c273c091575f296c3d9634`。Muse 受控云端测试实例已加载该版，用户确认两条文字回复在手机收到。**真实图片的模型读取、附件找回与续发/取消，以及连续多日稳定性仍待实测。** Codex / Claude Code 的真实认证、模型执行、微信审批和手机闭环也须在各自环境验收。

公开仓库采用完整源码快照，保留本仓库历史，不包含私有开发历史或运行资料；公开提交号与研发基线不同。生产模块及 Hook 程序保持该基线字节，测试补充自带的历史输入样本，说明文档按公开仓库调整。参见[本次更新、验证与回退说明](docs/PUBLIC-UPDATE-20261029.md)。

## 安装与离线检查

声明支持 Python >= 3.8，程序使用 `fcntl`，需 macOS / Linux 等 POSIX 环境；未适配原生 Windows。当前实测包括 macOS Python 3.9.6 / Pillow 11.3 和 Muse 云端 Linux Python 3.12.3 / Pillow 13.0，最低依赖组合未全部实测。

```bash
git clone https://github.com/MakubeXMiao/wechat-bridge-muse.git
cd wechat-bridge-muse
python3 -m venv .venv
.venv/bin/python -m pip install -e .
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/run_offline.py --output-dir qa-results/offline
```

离线入口使用隔离状态、模拟平台与网络，不需要微信登录。Hook 专项入口为 `.venv/bin/python hooks/tests/run_offline.py`。它们不证明真实微信送达。测试结果输出目录、虚拟环境与运行状态不应提交。

## 启动

按[隔离启动指南](docs/lines/01-core/QUICKSTART-V1.md)建立实例：先 `login`、`wait-login` 扫码成功，再写 `state/backends.json`，然后 `backend-check`、`backends status`、`serve`。使用安装环境中的 `wechat-bridge` 或 `python -m wechat_bridge`。

已有 Muse 实例更新须先备份可恢复的代码、依赖和配置，保全当前 Hook 的真实投递目标及运行状态，不要重新绑定原账号。同一个 bot 只运行一个收件服务；新增测试实例使用独立账号、目录和工作区。模型平台登录与微信登录分别完成，不复制 token。

| 接入 | 文档 |
| --- | --- |
| 公共收发与恢复 | [运行约定](docs/CONNECTOR.md) · [后端契约](docs/lines/01-core/BACKEND-CONTRACT-V1.md) |
| Muse | [适配交接](docs/lines/02-muse/MUSE-ADAPTER-HANDOFF.md) · [附件适配](docs/lines/02-muse/MUSE-ATTACHMENT-003-ADAPTER.md) · [部署检查](docs/lines/02-muse/MUSE-ATTACHMENT-003-DEPLOYMENT.md) |
| Codex | [接入说明](docs/lines/03-codex/README.md) · [本机测试](docs/lines/03-codex/LOCAL-TESTING.md) |
| Claude Code | [协作者安装与验收](docs/lines/04-cc/COLLABORATOR-ACCEPTANCE.md) |

旧技术文档保留其日期与当时接口背景，当前公开版本的验证范围和入口以本页及[更新说明](docs/PUBLIC-UPDATE-20261029.md)为准。仓库更新不会自动部署到任何主机。

## 许可

[MIT](LICENSE)。
