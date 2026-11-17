# 00 · 项目总览

[回仓库首页](../../README.md)

Wbridge 把微信消息交给助理处理，再将回复发回微信。目前只把两条路线作为产品入口：

| 产品入口 | 用途 | 当前公开范围 |
| --- | --- | --- |
| [wechat/muse](https://github.com/MakubeXMiao/Wbridge/tree/wechat/muse) | 第一桥：Muse | 保留 2026-10-29 的公开 Muse 基线；业务源为 `1f0160b`，不能据此判断最新云机版本 |
| [wechat/codex](https://github.com/MakubeXMiao/Wbridge/tree/wechat/codex) | 第二桥：Codex | 当前接 PEPE／dot2api，业务源 `705232a`，首轮真实文字收发已通；原互审包为 `36807de` |

分支是可选择的代码版本；下列编号是开发分工。进入任一分支后，都可以按同一套编号找说明：

| 编号 | 谁负责什么 | 入口 |
| --- | --- | --- |
| 00 项目总览 | 收需求、汇总进度、组织 QA 和发布 | 本页 |
| 01 公共底座 | 微信收发、附件、队列、状态和公共契约 | [公共底座](../lines/01-core/README.md) |
| 02 Muse | Muse 平台接线、Hook、会话和真实验收 | [Muse 桥](../lines/02-muse/README.md) |
| 03 Codex | PEPE／Codex 适配、远端任务与结果 | [Codex 桥](../lines/03-codex/README.md) |

当前未将 CC 列为第三个已交付产品；已有代码和历史材料继续保留。

## 一条消息怎么走

- Muse：微信 → 公共收件 → Muse Hook／worker → 公共回复队列 → 微信。
- Codex 当前路线：微信 → 公共收件 → dot2api 专用队列 → PEPE 原生任务 → 公共回复队列 → 微信。

本机 Codex CLI 是另一种接法，不能把它的会话和权限视为当前 PEPE 的会话和权限。

## 接下来怎么推进

1.1 先收齐当前整包互审反馈，集中处理问题；原固定源码包不因导航整理而换审对象。

1.2 优先验证掉线后的原任务核对、可读的进度状态和微信连续追问。

1.3 再做双入口联合验收和同一入口切换。切换需要统一收件、正确的任务归属、状态迁移和 Muse 附件保全。

当前 PEPE 媒体、长期稳定运行、断线恢复及电脑主聊记忆共享未验收。发送结果未知时的切换检查有已知 P2，开放多后端切换前必须修复；不能把两个分支存在当作已经可以互切。

详细方案和 QA 请从 [Codex 分支](https://github.com/MakubeXMiao/Wbridge/tree/wechat/codex) 的当前文档进入。

## 目录怎么看

程序继续放在 `wechat_bridge/`，Muse 平台程序放在 `hooks/`，测试放在 `tests/`；本次仅按编号整理文档入口，不移动或复制这些程序目录。

带日期的历史文档保留当时背景。判断当前可用范围，以所选分支首页、对应 QA 与实际部署证据为准。
