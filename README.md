# Wbridge · 微信助理桥

用微信与 Muse 或 Codex 助理对话。

**两个版本入口：[微信 · Muse](https://github.com/MakubeXMiao/Wbridge/tree/wechat/muse) · [微信 · Codex](https://github.com/MakubeXMiao/Wbridge/tree/wechat/codex)**

当前查看：**`wechat/muse` · Muse 公开版**。保留 2026-10-29 的公开源码基线（业务源 `1f0160b`）；这不代表最新云端运行版本。

## 按编号找功能

| 编号 | 板块 | 从这里进入 |
| --- | --- | --- |
| 00 | 项目总览：分工、版本与后续安排 | [项目总览](docs/00-overview/README.md) |
| 01 | 公共底座：微信收发、附件、排队与状态 | [公共底座](docs/lines/01-core/README.md) |
| 02 | Muse：平台接线与使用说明 | [Muse 桥](docs/lines/02-muse/README.md) |
| 03 | Codex：PEPE 文字接入与本机接入说明 | [Codex 桥](docs/lines/03-codex/README.md) |

00～03 是开发分工；两个版本入口分别服务 Muse 和 Codex。两种编号不互相替代。

## 代码放在哪里

| 目录 | 内容 |
| --- | --- |
| [wechat_bridge/](wechat_bridge/) | 公共运行代码及各助理适配器 |
| [hooks/](hooks/) | Muse 平台 Hook 与 worker |
| [tests/](tests/) | 离线检查与合成测试 |
| [docs/](docs/) | 安装、边界、QA 与历史记录 |

## 使用与边界

先看 [Muse 接入说明](docs/lines/02-muse/README.md) 和 [隔离启动指南](docs/lines/01-core/QUICKSTART-V1.md)。

Codex／PEPE 的新文字测试版请进入上方 Codex 分支；本分支未升级为该版。同一微信入口切换仍属后续规划。

一个微信机器人只运行一个收件服务。源码整理不会自动更新云端实例。

[MIT 许可](LICENSE) · [源码来源与版本记录](SOURCE-SNAPSHOT.json)
