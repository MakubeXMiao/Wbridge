# 01 · 公共底座

[回项目总览](../../00-overview/README.md) · [回仓库首页](../../../README.md)

公共底座负责微信收发、消息去重、附件收存、排队、回复状态和后端调度；由 01 维护。各助理共用这一层。

| 想了解或修改什么 | 入口 |
| --- | --- |
| 独立安装、扫码与启动顺序 | [启动指南](QUICKSTART-V1.md) |
| 后端接入、消息和结果规则 | [公共契约](BACKEND-CONTRACT-V1.md) |
| 附件保存、索引与限制 | [附件箱说明](ATTACHMENT-INBOX-IMPLEMENTATION.md) |
| 版本核对、迁移和回退 | [验证与迁移](VALIDATION-AND-MIGRATION.md) |
| 微信接口与收发循环 | [ilink.py](../../../wechat_bridge/ilink.py) · [serve.py](../../../wechat_bridge/serve.py) |
| 本地存储与任务调度 | [store.py](../../../wechat_bridge/store.py) · [dispatcher.py](../../../wechat_bridge/dispatcher.py) |
| 命令行与公共测试 | [cli.py](../../../wechat_bridge/cli.py) · [tests/](../../../tests/) |

涉及两个助理的路由、状态或附件变更，先由 00 归口，再交 01 设计；适配线不各自复制收发代码。

文档存在、离线检查通过和手机实际收到分别核对。具体版本边界以当前分支首页及 QA 记录为准。
