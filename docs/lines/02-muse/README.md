# 02 · Muse 桥

[回项目总览](../../00-overview/README.md) · [回仓库首页](../../../README.md) · [打开 Muse 分支](https://github.com/MakubeXMiao/Wbridge/tree/wechat/muse)

Muse 使用平台 Hook 与 worker：微信消息进入公共底座，再由 Muse 处理，回复交回公共发送层。平台接线、真实运行配置和验收由 02 负责。

本分支包含 Muse 接线代码并通过了相关离线回归；这不表示该分支已更新到 Muse 现网，Muse 的当前公开入口仍在独立 Muse 分支。

| 想了解什么 | 入口 |
| --- | --- |
| Muse 接线和会话适配 | [适配交接](MUSE-ADAPTER-HANDOFF.md) |
| 附件保存与回发适配 | [附件适配说明](MUSE-ATTACHMENT-003-ADAPTER.md) |
| 更新、核版与回退检查 | [部署检查](MUSE-ATTACHMENT-003-DEPLOYMENT.md) |
| Hook 与 worker 程序 | [hooks/](../../../hooks/) |
| 公共收发与附件 | [01 公共底座](../01-core/README.md) |

运行包、Hook、实际平台提示词和手机结果需要对应同一版本。仓库里的 Hook 定义是模板，不能覆盖线上真实投递目标。

Python wheel 不包含 Hook 和平台配置；需要 Muse 接线时使用相同版本的完整源码。当前整理不修改现有微信绑定、云端服务或投递目标。
