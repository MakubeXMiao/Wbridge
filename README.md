# wechat-bridge（Muse 定制版）

基于 [Liu-zhongxian/wechat-bridge](https://github.com/Liu-zhongxian/wechat-bridge)（MIT License，见 `LICENSE`）的个人定制版本：走腾讯官方 iLink / 微信 ClawBot Bot 协议，把个人微信一对一私聊接进 Muse（桥只做消息管道，回复由 Muse 本人产生）。仅连接 `ilinkai.weixin.qq.com` 与 `novac2c.cdn.weixin.qq.com` 两个腾讯域名，无第三方中转。

> 个人维护的 fork。运行时数据（登录态 `session.json`、inbox/outbox、媒体、日志）一律不入库，见 `.gitignore`。现行运维语义以 `docs/CONNECTOR.md` 为准（已脱敏）。

## 本 fork 在原版基础上的改动（2026-10-21，六轮）

1. **P1–P7 加固**：发送失败退避（30s→2m→10m）+ 死信隔离；逐任务异常隔离防 serve 崩溃；收消息先落盘再推进游标；配文拆独立任务只发一次；单 context_token 发送上限 8 条 + 发送间隔 1.5s；发送侧 -14 先探活再判失效；CDN 上传 60s 超时 + full_url 404 回退；日志带时间戳/任务名/attempt；serve flock 单实例。
2. **resp_ok 实测修正**：iLink 的 sendmessage 成功响应是 `{"message_id": ...}`、没有 `ret`——成功判据改为「有 ret 以 ret==0 为准、无 ret 看成功标志字段」。
3. **合并回复**：worker 约定（同批消息合并成一条回复，见 `hooks/wechat-inbox-watch.json` 的 prompt 与 `docs/CONNECTOR.md`）+ serve 代码兜底（同批未尝试的纯文本任务自动拼单发送、合并失败降级逐条）。
4. **全盘审查 12 项**：`sending/` 启动回收；hook 认领改首次认领计时 + flock；长文本分片进度异常路径不丢 + client_id 稳定派生；配文先行门控；合并分组被非文本任务截断、额度按实际分片计、片间 pacing；getupdates 异常响应退避不崩；收消息全程兜底占位；CLI send 判据与 wait-login 落盘修正；死信 hook 告警；宕机告警标记自愈；损坏任务文件隔离进死信。
5. **提速（ABC）**：flush 拆为 serve 内独立线程（2.5s 一轮），不再等长轮询返回才发送，闲置后首条回复从 1–2 分钟降到典型 20–35 秒；hook 轮询间隔 30s→10s；worker 合并宽限改自适应（连发等约 15s 补扫、单条旧消息只等约 5 秒）。测试见 `tests/test_speed.py`。
6. **二次全盘审查修复（P1–P3）**：flusher 线程监督双保险（主循环查活重启 + 心跳文件，hook 心跳过期杀进程重拉）；hook 脚本整程 flock + 认领维护锁内读-改-写；快路径分流加硬规则（含时间/金额/人名/变更动词一律走事务路径）；sent/ 30 天保留期 + 移入时 chmod 0600；seen 记录上限裁剪；serve.log 改北京时间；wait-login 非 dict 响应防护；媒体 client_id 稳定派生；sendmessage/getuploadurl 关传输层自动重试（任务层带稳定 client_id 接管）。测试见 `tests/test_audit2fix.py`、`tests/test_hook_audit2fix.sh`。

## 布局

- `wechat_bridge/` — 桥本体（serve / media / ilink / cli / store / crypto）
- `tests/` — 离线测试套件（假网络、真实响应形状）：`test_crypto.py`、`test_p1p7.py`、`test_merge.py`、`test_audit12.py`、`test_hook_audit12.sh`、`test_speed.py`
- `hooks/` — 收件箱轮询 hook 脚本与定义（保活 serve、唤醒 agent 回复、死信/掉线告警）

现行参数（hook 轮询 5 秒、合并宽限 8 秒等）以 `docs/CONNECTOR.md` 为准。
- `docs/CONNECTOR.md` — 运维手册（脱敏版）：布局、常用操作、发送语义、各轮回滚步骤

## 恢复要点

代码恢复后还需运行时数据目录（含 `state/session.json` 登录态）才能免扫码运行；登录态不在本仓库内，单独保管。若登录态丢失，重新扫码登录即可（见运维手册）。
