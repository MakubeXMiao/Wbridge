# 03 Codex 桥

当前任务：`WBR-03-20261024-001`。首版使用 **app-server 双向协议**，提供显式会话、结果校验、一次性命令/文件修改审批与取消/超时处理，接入 01 v1 契约；当前修复按固定集成 `cad9aeec2db30d594d8fdca184360c0e8499e7e4` 的实际边界复验。

## 当前使用入口

1. 从新私有库 `MakubeXMiao/wbridge` 选择 00 已完成集成 QA 的固定 SHA，按[公共启动指南](../01-core/QUICKSTART-V1.md)确认安装及公共 CLI。03 单支的协议测试不能替代完整集成版本。
2. 按[独立微信账号的本机测试准备](LOCAL-TESTING.md)准备模板、扫码绑定、保存配置、预检和启动。**首次绑定前，配置模板放在运行根之外；扫码成功后才写入 `state/backends.json`。**
3. 查看[实现、限制与验证](IMPLEMENTATION.md)与[配置模板](backends.example.json)。目标如为 OpenAI Codex Dot 的 PEPE，先完成[部署前置核对](PEPE-DEPLOYMENT-PREFLIGHT.md)，不能直接套用本机安装路径。

实现已有离线协议测试；候选 SHA、自测、独立 QA、上传及集成复验结果见共享协调目录 `reports/03.md`。真实模型生成、审批往返、sandbox 和微信手机收发仍须在选定主机上验收，配置预检或离线 PASS 不代表已接通。

## 首轮历史取证（2026-10-23）

以下文档属于已结束的 `WBR-03-20261023-001` 能力调查，保留当时版本与原型证据。其 exec 路线建议及“尚未实现”状态不适用于当前首版，不作为安装或审批接线指南。

- [能力矩阵、版本与原型盘点](CAPABILITY-MATRIX.md)
- [提交给 01 的最小接口要求](INTERFACE-REQUIREMENTS.md)
- [证据索引、复现命令与未测项](EVIDENCE.md)
