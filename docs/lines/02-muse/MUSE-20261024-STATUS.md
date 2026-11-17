# Muse 版本与手测核对

任务 `WBR-02-20261024-001`；2026-10-24，Asia/Shanghai。本轮采用 05 实现部署核验工具，06 独立检查固定候选。运行中的 worker、Hook、prompt 和公共包不在本次修改范围。

## 1. 已读回执和证据限度

02 已直接读取 Muse「微信桥」旁聊中的媒体部署回执。以下部署事实仍是 **Muse 自报**，没有直接读取云端原日志、平台配置导出或存量进程：

- 公共包来自 `codex/media-receive-review` 的 `b7c47be`；Muse 报告安装包与来源 `diff -r` 零差异。
- Hook/adapter/prompt 保持 `codex/muse` 的 `2b3e537`；脚本、adapter、prompt 哈希前缀分别为 `6310ab1b`、`b6a5c5b8`、`9fea6910`，prompt 为 1936 字，平台保存定义回读一致。
- 旧 serve PID 965 停止，新 PID 9426 于 **2026-10-23 22:48:59** 启动；Python 3.12；Hook 恢复 5 秒轮询；单一服务，账本、游标、队列、claims 保留。
- core 12 套、组合 hooks 3 套通过；原单源部署检查器 **17/18**，唯一失败为 `runtime_package_python_sources`，另做了包的逐文件比较。

这些哈希前缀与本机固定候选吻合。完整候选 SHA、逐文件 SHA-256、源地址和迁移步骤见 [迁移清单](MUSE-WBRIDGE-MIGRATION.md)。本次完整解释器/包路径、双源源码 dirty、原安装与启动记录、当时实际加载身份、真实 prompt 导出、worker 使用证据仍需补齐。现在的文件哈希或新解释器导入不能倒填历史加载状态。

Muse 另提到 Pillow 13.0.0 在云端 venv 中。02 二评发现仓库已声明 `qrcode[pil]`，本机 qrcode 安装元数据明确含 Pillow 传递依赖，故“未直接声明 Pillow”不单独构成依赖缺失结论；云端版本仍为自报。

## 2. 当前手测矩阵

截至本轮读取，旁聊尚无下列用例的新结果。此表的 unknown 表示未取得对应证据，不表示失败，也不表示用户没有做过。

| 用例 | 测试窗口及版本绑定 | 自动/人工 | queued/API 接受 | 手机确认 |
| --- | --- | --- | --- | --- |
| 单条 | unknown | unknown | unknown | unknown |
| 5 条连发 | unknown | unknown | unknown | unknown |
| begin 后到输入 | unknown | unknown | unknown | unknown |
| 图片 | unknown | unknown | unknown | unknown |
| 图片后紧跟文字 | unknown | unknown | unknown | unknown |

需要逐项关联脱敏消息 ID、received/wake/begin/queued/API 接受时间及用户手机确认。先前 18:31 的文字探针早于媒体部署，不能作为当前组合的图片或五项验收。补充取证请求准备期间输入框出现用户草稿，02 停止操作，没有发出请求或草稿；待空闲时再补证。

## 3. 本线优化及依赖

本轮仅一项已有证据的优化：**部署检查器显式支持公共包与 Hook 的不同来源**。双源均检查完整 SHA、仓库根与 dirty；Hook/adapter/prompt 只比 Hook 来源，包全部 Python 文件只比公共包来源；保留原单源命令。输出标明来源绑定，并记录本次新探针解释器身份。

这个工具仍只验证所提供的文件和导出配置，不证明存量进程加载、平台配置激活、聊天目标身份、环境覆盖或手机送达。新增能力只在本地候选中，不安装到当前手测环境。

尚无充分证据支持调整 worker 租约、prompt 或延迟策略。公共问题交 01；01 新调度候选的 legacy 兼容复核，等待其固定 SHA。Muse 本轮维持旧协议，没有实现新后端的微信权限审批能力，不将其包装成已支持。

新库由 00 创建并确认固定 refs。02 收到就绪通知、核验来源且本轮手测窗口结束后，再交接 Muse 来源迁移。当前候选不推旧库，不中途替换运行代码。
