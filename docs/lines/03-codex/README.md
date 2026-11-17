# 03 · Codex 桥

[回项目总览](../../00-overview/README.md) · [回仓库首页](../../../README.md) · [打开 Codex 分支](https://github.com/MakubeXMiao/Wbridge/tree/wechat/codex)

第二桥的产品名称是 Codex；当前已跑通的路线是 **PEPE／原版 dot2api 文字接入**。本机 Codex CLI 仍有独立适配代码，两种接法的会话、权限和实际验收分别记录。

## 先选要看的路线

| 路线 | 使用说明 |
| --- | --- |
| 当前 PEPE／dot2api | [独立接入步骤](PEPE-QUICKSTART.md) · [配置示例](backends.pepe.example.json) · [适配机制与约束](PEPE-DOT2API.md) |
| 本机 Codex CLI | [独立微信测试](LOCAL-TESTING.md) · [实现与限制](IMPLEMENTATION.md) · [配置示例](backends.example.json) |

## 当前 PEPE 文字版的边界

- 业务源 `705232a`，公开互审包为 `36807de`；2026-11-16 首轮真实微信文字收发已通，手机明确确认至少一条回复。
- 已有 61 套公共、8 套 Muse Hook 回归和 9 项独立组合检查；这些检查不等于媒体、长期稳定或网络恢复已通过。
- PEPE 沿用自身平台的工具权限和确认，微信桥不提供逐工具审批。
- 每次只提交当前文字；原生任务沿用同一聊天的设置，不证明共享电脑主聊完整历史。
- 本地取消、超时或 HTTP 资源退出不等于 PEPE 已停止；结果不明时保留原任务，不自动重投。
- 多后端模式中，发送结果未知时的切换检查与状态展示仍有 P2，归 01 修复；开放 Muse／PEPE 单入口切换前必须处理。

具体检查与未测范围见 [本轮 QA](../../QA-CODEX-BRIDGE-20261117.md)，后续阶段见 [两桥规划](../../TWO-BRIDGES-ROADMAP.md)。

## 后续工作的分工

03 优先核实原任务查询、订阅恢复和连续追问的实际能力；公共恢复、状态与切换由 01 牵头，Muse 接线涉及 02。新功能方案确认后再实施。

本次只整理入口，现有业务源码与已发送的互审包保持原样。
