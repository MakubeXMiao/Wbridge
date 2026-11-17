# PEPE 公共接线候选 v1.1

PEPE 使用独立 bot/Store 配置中的 HTTP provider；本候选没有配置任何真实运行实例。配置归公共层，HTTP adapter 归03；原 Muse Hook 和默认 legacy 流程保留。

`backends.pepe` 必须显式包含以下字段，不接受假 `command/cwd` 或内联凭据：

```json
{
  "provider": "pepe",
  "transport": "http",
  "instance_id": "pepe-instance",
  "base_url": "https://dot2api.example.invalid",
  "credential_env": "DOT2API_TOKEN",
  "tenant": "configured-tenant",
  "principal": "configured-principal",
  "queue": "pepe",
  "queue_ownership": "single_store",
  "instructions": "Answer from the supplied text.",
  "execution_policy": {"mode": "text_only", "enforcement": "unverified"}
}
```

未核模式可以校验配置，但会在创建 driver、读取凭据和提交之前拒绝派发。显式原生委派模式为 `execution_policy={"mode":"native_delegate","approval":"platform_native"}`：文字只限制微信输入/输出，PEPE沿用其已有工具/权限及平台确认，桥不提供微信逐工具审批，不声称平台策略或“所有动作必经本人确认”已经验证。现 Codex/CC 的 `approval=explicit` 保持原义。本任务人类已确认原生委派语义；实际配置/目标和恢复窗口仍需另行确认，本地候选通过不授予上线权限。

公共层从真实收件来源摘要验证单聊文字，拒绝语音转写、媒体/引用、未知/旧来源和不够原版最小TTL的任务；PEPE不读取附件文件，`session_id` 始终为 null。来源摘要只加到 managed 记录，legacy 记录形状不变。配置使用根URL及至多128字符wire queue ID；启用PEPE的任务超时至少66秒，运行中还会重新核剩余预算，不能靠改TTL重提。

`remote_tasks` 私有表随现有 BackendState 初始化添加，不清队列/游标/账本；原状态 binding 仍限制 bot/owner/config/Store。同一真实队列首期只支持一个 Store 提交拥有者，现有全Store串行及 unknown 阻塞保留。

公共层先冻结原版完整 Submit、幂等键、输入digest、service/tenant/principal/queue及 execution/attempt/instance；adapter拿到范围受限 journal：

- `begin_submit()`：一次性把 prepared 提交为 submitting，事务成功后才允许首次 POST；冻结TTL必须仍装入原deadline。
- `bind_task(task_id)`：保存原任务ID，重复同ID幂等，拒绝改绑和同scope跨执行冲突；迟到绑定只留证。
- `snapshot()`：独立副本，不提供重跑或发布权限。

原版回包不返回 tenant/principal/idempotency_key/worker；认证身份为本地冻结绑定，adapter按实际 id/queue/instructions/payload及状态核对，不伪造回包字段。公共层核原绑定 task、execution/attempt与 session 约束后才接 final；发送仍由既有公共 outbox/sender 完成。

提交响应丢失、断连、远端取消/失租/到期均不能证明PEPE停止。提交后失败/取消保守进入 unknown，保留映射和队列占位；重启仍走现有 unknown/人工核验，不自动重POST、恢复发布或切换模型。回滚代码不回滚/删除运行状态；出现remote_tasks的状态应保留由本版本人工核对，不把旧版本当作安全重跑入口。

新增四套测试已注册 `tests/run_offline.py`。最终组合验收仍须包含03真实driver、真实公共 sender 及网络边缘替身；本线自测不证明云端/微信API或手机实收，不进行打包。
