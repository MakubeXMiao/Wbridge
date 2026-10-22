# WeChat Bridge（自建集成）

用户自建的微信接入：第三方开源项目 wechat-bridge（GitHub: Liu-zhongxian/wechat-bridge，MIT），
走腾讯官方 iLink / ClawBot Bot 协议。2026-10-21 经用户批准安装在 Muse 的云 VM 上，代码已逐行审过：
只连 ilinkai.weixin.qq.com 与 novac2c.cdn.weixin.qq.com 两个腾讯域名，无第三方上报；
依赖仅 qrcode、pycryptodome、pillow（pillow 是作者漏写的依赖）。

注意：这不是 Muse 官方微信连接（官方仍只支持 WhatsApp；微信接入需求反馈已于 2026-10-20 提交）。
本桥由 Muse 维护，出问题按下面手册处理。

## 布局

- 程序（venv）：`~/workspace/wechat-bridge-venv/bin/wechat-bridge`（login / wait-login / serve / send / status）
- 数据目录：`~/workspace/wechat-bridge`（环境变量 WECHAT_BRIDGE_DIR 指向它）
  - `state/session.json`：登录会话（0600，含 bot_token，不要外传、不要打印）
  - `state/serve.pid`、`state/serve.log`（带时间戳日志，>5MB 时 hook 在重启 serve 前轮转为 serve.log.1，仅留一份）、`state/serve.lock`（serve 单实例 flock）、`state/NEEDS_RELOGIN`（token 失效标记）
  - `inbox/*.json`：收到的消息（字段 text / from_user_id / context_token / message_id / media_paths? / media_failed?）
  - `outbox/pending/*.json`：待发回复；serve 认领后先移 `outbox/sending/`，成功移 `outbox/sent/`，
    可重试失败连同 attempts/next_attempt_at 写回 pending/，到顶移 `outbox/failed/`（死信，含 failed_reason）
- 审计源码副本：`~/workspace/wechat-bridge-audit`
- 回复回路 hook：`wechat-inbox-watch`（脚本 `~/hooks/scripts/wechat-inbox-watch.sh`，5 秒轮询；
  间隔是 hook 定义 `~/hooks/definitions/wechat-inbox-watch.json` 里的 `poll_interval_secs`，
  平台每-hook 可配、最小 5 秒。2026-10-21 晚提速 B 从 30 秒改到 10 秒，当晚稍后用户
  再拍板改到平台下限 5 秒）
  - 职责：serve 保活（进程死了自动 setsid 拉起，拉起失败才唤醒 agent）、
    新 inbox 消息唤醒 agent 回复、NEEDS_RELOGIN 时唤醒 agent 提醒用户重新扫码（只提醒一次）。
  - 已处理消息 ID 记在 `~/hooks/state/wechat_inbox_seen.txt`。
  - 认领机制（2026-10-21 全盘审查第 2 项改）：`~/hooks/state/wechat_inbox_claimed.txt`，
    行格式 `id 首次认领ts 续租ts`。有效性按**首次认领**算：消息文件还在、未 seen、
    首次认领未满 6 小时就视为有 worker 在处理，hook 每轮只续租、绝不重复唤醒
    （旧版固定 15 分钟过期，慢 worker 会被第二个 worker 抢活、可能重复回复）；
    超过 6 小时文件还在才重新唤醒（原 worker 假死兜底）。认领文件的读写都走
    同目录 `.lock` 的 flock。worker 把 wake 名单之外的消息并入合并回复时，
    必须先按 prompt 里的命令自己补记认领，否则会被下一轮 hook 当新消息再唤醒。
  - 超时重唤醒（2026-10-22 加，事故驱动；同日调参：阈值拆分 + 已开工标记）：
    认领只是租约，唤醒的 worker 执行本身可能丢失。文件还在、未 seen 时，hook
    按「已开工标记」区分两种超时重新唤醒，payload 以 `rewoken_files` 与
    `files` 单列区分，判定规则名（no-marker-180 / started-stale-900 /
    interval-600）记入 hook 日志便于对账：
    - **已开工标记**：worker 按 prompt 第 1 步，开工前为每条消息写
      `~/hooks/state/wechat_worker_started/<id>.started`（内容为开工 epoch），
      处理完按第 4 步连同 inbox 文件一起删；hook 对无对应 inbox 文件的孤儿
      标记兜底清理。
    - **无标记**（唤醒大概率没送达、worker 根本没启动）：首次认领满
      **180 秒**即快速重唤醒（`REWAKE_FIRST_STALE_SECS`，只管首次）；之后
      与上次重唤醒间隔满 **600 秒**（`REWAKE_INTERVAL_SECS`）才再唤。
    - **有标记**（worker 确实在干活）：标记未满 **900 秒**
      （`STARTED_STALE_SECS`）绝不重唤醒，防两程并行、重复回复；满 900 秒
      认定它干活途中已死，可重唤醒（同样受间隔与次数闸约束）。
    - 节流与上限：两次重唤醒至少间隔 600 秒、两条路径合计最多 **3 次**
      （`REWAKE_MAX`），状态记 `~/hooks/state/wechat_inbox_rewake.txt`（行格式
      `id 次数 上次重唤醒ts 已告警`）。用尽后又过 600 秒仍未处理 → 唤醒一次
      `wechat_stuck_message` 让 worker 向用户报告（同一条只报一次；文件消失
      后条目自动清、告警状态重置）。worker 处理 rewoken 消息前必须先查
      `outbox/sent/` 与 `outbox/pending/` 有无 `<message_id>.json` 或
      `<message_id>_*.json` 任务（已有回复发出或在排队就只标 seen、删文件，
      不重复回复）。6 小时兜底排在重唤醒之后，语义不变。
    - 恢复时间线（典型）：唤醒丢失 → 约 **3 分钟**自愈；worker 开工后死亡
      → 约 **15 分钟**被接管；最坏情况（多次重唤醒接连丢失）→ 约 **33
      分钟**发出卡死告警（3 分钟首唤 + 两次 10 分钟间隔重唤 + 10 分钟后告警）。
  - 事故注记（2026-10-22）：09:44 与 09:56 两条消息被认领、唤醒决定已做出，
    但 worker 执行未落地，消息在 inbox 静默滞留；因认领每轮续租，后续检查
    一直判「处理中」，直到 10:12 人工排查发现后手动补处理（合并回复已发送、
    标 seen、删文件）。根因在唤醒投递环节丢失，桥侧无痕迹可查。教训：认领 ≠
    送达，必须配超时重唤醒兜底——即上一条。
  - 死信告警（2026-10-21 全盘审查第 9 项）：hook 每轮扫 `outbox/failed/`，
    出现没报过的新死信文件就唤醒一次（reason `wechat_dead_letter`，已报清单在
    `~/hooks/state/wechat_failed_notified.txt`；文件被人工移走后条目自动失效，
    同名再进死信会再报）。worker 指令含「不要无声卡住」条款（办不了的事几分钟内必须回话）。
  - flusher 心跳监督（2026-10-21 深夜二次审查 P1-1）：serve 的发送线程持续把 epoch
    写进 `state/flusher_heartbeat`。hook 每轮检查：serve 进程活着但心跳 >120 秒
    未更新（或进程已活过 180 秒却从未有心跳文件）→ 判定发送线程死/卡，按宕机
    杀进程重拉（拉起后启动回收会接管 sending/ 的在途任务）。进程存活检查发现
    不了线程级故障，这条是专门补的盲区；serve 内部另有主循环查活+进程内重启
    的第一道保险（见「二次全盘审查修复」节）。
  - 脚本整程锁（二次审查 P2-1）：hook 脚本全程持 `~/hooks/state/wechat_inbox_watch.lock`
    的 flock，上一轮没跑完时下一轮直接静默跳过，杜绝 5 秒间隔下两轮重叠执行；
    认领维护的读-改-写整环另在认领锁内完成，worker 的补记追加不会被冲掉。
  - 保留期维护（二次审查 P3-1/P3-2，时间戳节流、每天至多一次）：`outbox/sent/`
    只保留 30 天（更老的删；`failed/` 永不动）；`wechat_inbox_seen.txt` 超过
    5000 行时裁到最近 2000 行。
  - 投递面（2026-10-21 改）：worker 在侧边聊天「微信桥」（chat_id <wechat-bridge-chat-id>）里
    运行并汇报，不再占用主聊天——此前 worker 在主聊天运行时会让用户 Mac 上的 Muse 客户端频繁闪动。
- serve 由 hook 以 setsid nohup 方式拉起常驻（输出追加到 `state/serve.log`）；VM 更换后靠 hook 的保活逻辑重新拉起（手动拉起命令见「serve 管理」节）。

## Worker 回复约定：消息分流（快路径，2026-10-21 晚起）

worker 被唤醒后先给消息分流（hook 的 worker 提示词里已同步写入，以本节为准）：

- **简单消息走快路径**：招呼、测试、确认、闲聊、不涉及事实查询的，直接回复，
  不查记忆、不读文件（合并、宽限、认领、标记等发送规矩照常执行）。
- **事务消息走完整路径**：记录新安排、问待办/到期、涉及租房/日期/金额/承诺、
  需要核对历史的，先按需检索记忆、读相关文件，再回复。判不准按事务消息处理。
- **硬规则（二次审查 P2-2，优先于语气判断）**：消息只要含时间/日期/金额/人名，
  或带变更动词（改、取消、提前、推迟、记一下、别提醒了），或是在汇报一件已记
  事项的进展/完成（如「发票开好了」），一律走事务路径——这类短陈述句读起来
  像随口确认，实则要落账，快路径的框定会诱导漏判，所以单列硬规则钉死。

## Worker 回复约定：合并回复（2026-10-21，用户批准「两个都改」之一）

用户连发多条消息时，逐条回复会刷屏。约定（hook 的 worker 提示词里已同步写入，以本节为准）：

- **同批合并**：同一次 hook 唤醒（wake payload 的 files 列表）里的多条消息，只写**一条**合并回复：
  一个 outbox 任务文件，命名用本批最新一条的 message_id，context_token 用最新一条的，
  正文里简短逐个回应每个小问题（每个 1–3 句）。
- **等漏网（自适应宽限，2026-10-21 晚提速 C 改、当晚用户拍板 15 秒缩为 8 秒）**：不再一律等满。看本批最新一条的
  create_time_ms 与条数：本批 ≥2 条、或最新一条距现在 <10 秒（用户可能还在连发）→ 等约
  8 秒补扫一次 inbox，有同波新文件就并进同一条合并回复（补扫最多一次）；本批只有 1 条
  且距现在已 ≥10 秒 → 只等约 5 秒静默确认，无新文件就立刻落笔。旧版一律等 15 秒，
  闲置后的单条消息也陪等，是首条回复慢的来源之一。
- **补记认领**：并入合并回复、但不在本次 wake 名单里的文件，必须先按 hook prompt 第 3 步的命令
  （带 flock 往 `wechat_inbox_claimed.txt` 追加一行）给它们补记认领，再写回复、标 seen；
  否则下一轮 hook 会把它们当新消息再唤醒一次、造成重复回复（全盘审查第 2 项）。
- **不跨批**：上一批的回复已经发出、或几分钟后另起话题的消息，单独回复，不要硬凑。
- **媒体不并入文本**：带媒体的回复单独成任务，不并进文本合并（serve 侧也不会把媒体任务并入合并组）。
- serve 侧另有机械合并兜底（见发送语义），worker 偶尔没合并也不会刷屏，但语义合并优先、效果更好。

## 发送语义（2026-10-21 P1–P7 改造，用户批准）

代码在原作者 0.2.0 基础上由 Muse 打了本地补丁（venv 已装包与 `wechat-bridge-audit/wechat_bridge/` 保持一致，改代码改审计树后同步过去）：

- **发送架构（2026-10-21 晚提速 A）**：flush 不在 getupdates 轮询主循环里跑了，
  serve 内有独立 flusher 线程每 2.5 秒扫一轮 outbox（`FLUSH_INTERVAL_SECONDS`，
  调用被锁串行化，认领仍靠 pending→sending 原子 rename，全进程只有一个认领者）。
  旧架构下闲置时长轮询一轮最长挂 ~120s，回复写好也要等轮询返回才发得出——那是
  「闲置后第一条回复慢 1–2 分钟」的主因；现在回复落盘后 ~3 秒内即被发走。
  主循环只管收消息。发送侧确认 token 失效时 flusher 置停止事件、主循环据
  NEEDS_RELOGIN 标记协同退出。下面的重试/死信/合并/额度语义全部不变，
  只是执行者从主循环换成了 flusher 线程。

- **重试有上限**：任务 JSON 带 `attempts/next_attempt_at/enqueued_at`；失败退避 30s→2m→10m；
  attempts≥5 或任务龄 >30 分钟进 `outbox/failed/`（含 failed_reason），不再无限空转。人工处置 failed/ 里
  的任务：修好原因后把文件改名移回 pending/ 并删掉 attempts/next_attempt_at 即可重发。
- **崩溃安全**：每条任务独立异常隔离，发送异常不杀 serve；任务先认领进 sending/ 再发，
  pending 文件中途被移走只跳过不崩；收消息整批落 inbox 后才保存 get_updates_buf 游标；
  媒体下载/解密失败的消息写 inbox 占位记录（text=`[媒体接收失败…]`、media_failed=true），不再静默丢。
- **配文只发一次、且先于媒体**：媒体任务的 text 配文被拆成独立的 `<任务名>_caption.json` 文本任务，
  在同一次发送流程里先于媒体本体送达（全盘审查第 4 项修正——旧实现只是写进 pending，
  配文实际比图晚一轮）。配文若排了退避重试，媒体顺延到配文送达之后；配文若进了死信，
  媒体跟着进死信等人工处置（不发无说明的图、配文也不会被无限单独重发）。
  worker 排队时仍按旧格式写 {text+media_path} 即可，serve 自动拆。
- **速率与 token**：发送成功间隔 1.5s；同一 context_token 在 serve 生命周期内最多发 8 条，
  余量留队等用户下一条消息（新 token）；未尝试过的任务入队后若有更新的同用户 token 会自动升级；
  长文本分片失败时从失败片续发，已发片不重发。
- **ret 判据（2026-10-21 实测修正）**：sendmessage 成功响应是 `{"message_id": ...}`、根本没有 ret；getuploadurl 成功带 upload_full_url/upload_param，也可能无 ret。判据：有 ret 以 ret==0 为准；无 ret 看成功标志字段（message_id / upload_full_url / upload_param）；无 ret 又无标志、或非 JSON 响应才按可重试失败。（初版误按「必须 ret==0」，把已送达消息当失败重试造成重复发送，已于当日修复，resp_ok 在 ilink.py。）
  发送侧遇 -14 先 getupdates 探活，探活也 -14 才写 NEEDS_RELOGIN（不再误判掉线惊动重扫码）。
- **媒体上传**：超时 60s、每地址最多 2 次；full_url 吃 404 时自动新 filekey 重新取址回退 param 地址；
  日志记录所用地址形式（form=full_url/param）与 HTTP 状态。
- **连发机械合并（兜底，2026-10-21 加）**：flush 时，pending 里 attempts==0 的纯文本任务
  （配文任务、无分片进度的除外）若同收件人且 enqueued_at 跨度 ≤120s，会被合并成一条发出
  （正文以空行拼接、用最新任务的 token、token 额度按实际送达的分片数计——全盘审查
  第 5 项修正，旧版按任务数低估）；任何非文本任务都会截断正在攒的文本组，发送顺序
  不被打乱；组内最新任务入队未满 20s 时整组推迟到下一轮，等 worker 把这波写完。
  合并整条未送达时**降级逐条发送**（各自记 attempts/退避），一条毒任务不拖全组；
  长文本分片部分送达时未达部分收拢为续发任务、已达分片不重发。日志会有「合并发送 N 条任务为一条」一行列出全部文件名。
- **单实例**：serve 启动 flock `state/serve.lock`，重复实例直接退出；status 与 hook 的存活判断
  都校验 /proc/<pid>/cmdline 含 wechat-bridge，防 pid 复用误判。

## 全盘审查 12 项（2026-10-21 晚，用户批准全修）

在 P1–P7 与合并兜底之上再修一轮，离线套件在 `wechat-bridge-audit/tests/`
（test_p1p7 / test_merge / test_audit12 / test_hook_audit12.sh，全部持久化）：

- **sending/ 启动回收（第 1 项）**：serve 拿到 flock 后先把 sending/ 里滞留的任务全量
  移回 pending（保留 attempts/分片进度、清退避），serve 中途被杀不再有回复永久卡死；
  status 现在也显示 `outbox/sending` 计数，滞留不再隐形。
- **分片进度（第 3 项）**：长文本每片送达即落盘 chunks_done；发送抛异常时进度挂在异常
  对象上带回、serve 照样落盘，重试绝不重发已达片；client_id 改为「任务名+片号」稳定
  派生（md5），同任务重试复用同一组 client_id。
- **片间节奏（第 5 项）**：ilink.send_message 片与片之间也等 1.5s（serve 传 PACE_SECONDS）。
- **轮询健壮性（第 6 项）**：getupdates 先判 dict 再取字段；非 dict、带 _raw、或带非零
  ret/errcode 的响应一律按轮询失败处理（连续失败退避 2s→30s、不存游标、不 flush），
  不再有非 -14 错误空转打服务器。-14 仍走原确认流程。
- **收消息不丢（第 7 项）**：handle_incoming 任何环节失败都至少写一条占位 inbox 记录
  （aeskey 非 hex 走多格式解析、不再整条抛掉）；记录实在落不了盘时返回 False，
  本批 get_updates_buf 游标不推进，消息下轮重新投递。
- **CLI 判据（第 8 项）**：`wechat-bridge send` 用 resp_ok 判最终响应与配文响应，
  失败返回退出码 1（旧版回 -2 也报成功）；wait-login 的 binded_redirect 若带
  bot_token，与 confirmed 一样落盘保存会话。
- **坏文件隔离（第 12 项）**：pending 里读不出的任务（JSON 损坏、不是 dict、名为
  *.json 的目录）在文件龄 >10 秒后改名移入 failed/（`<名>.corrupt-<时间戳>.json`），
  不再每轮堵住整个发送通道；太新的文件先跳过一轮，防误杀 worker 正在写的任务。

## serve 管理

- **查状态**：`~/workspace/wechat-bridge-venv/bin/wechat-bridge status`（登录态、serve pid、
  inbox/pending/sending/failed 计数）。线程级健康看心跳：`cat ~/workspace/wechat-bridge/state/flusher_heartbeat`
  与当前 epoch 差应 <120 秒；`ps -o nlwp -p $(cat ~/workspace/wechat-bridge/state/serve.pid)` 应为 2（主循环 + flusher）。
- **手动重启 serve**（hook 也会保活自动拉起，手动只在需要立刻重启时用；命令与 hook 脚本里的拉起行逐字一致）：

  ```bash
  kill $(cat ~/workspace/wechat-bridge/state/serve.pid)   # 先停旧的（单实例 flock 不退让会让新实例直接退出）
  WECHAT_BRIDGE_DIR="$HOME/workspace/wechat-bridge" TZ=Asia/Shanghai setsid nohup \
    "$HOME/workspace/wechat-bridge-venv/bin/wechat-bridge" serve \
    >> "$HOME/workspace/wechat-bridge/state/serve.log" 2>&1 < /dev/null &
  ```

  TZ=Asia/Shanghai 让 serve.log 时间戳记北京时间（2026-10-21 深夜起；此前的日志是 UTC，排障勿混读）。
  worker 提示词里 wechat_serve_down 的处置指的就是本节：先按上面命令手动拉起并确认 pid 与进程存活，
  成功在摘要说明已恢复，失败才报告用户。
- **venv 重建 runbook（二次审查 P2-4）**：venv 死绑系统 Python（`pyvenv.cfg` 指向 `/usr/bin/python3.12`）。
  VM 更换后若该解释器不存在，venv 立坏、serve 起不来、hook 拉起必失败（唤醒 wechat_serve_down）。
  重建步骤（数据与登录态都在 `~/workspace/wechat-bridge`，随盘持久，不受影响、无需重新扫码）：

  ```bash
  python3 -m venv ~/workspace/wechat-bridge-venv --clear   # 用新镜像里可用的 python3
  ~/workspace/wechat-bridge-venv/bin/pip install ~/workspace/wechat-bridge-audit
  ~/workspace/wechat-bridge-venv/bin/wechat-bridge status  # 确认已登录，再等 hook 拉起 serve
  ```

- **已知平台项：hook 日志增长（二次审查 P1-2，未修复、平台侧）**：平台给每轮 hook 执行
  往 `~/hooks/logs/wechat-inbox-watch.jsonl` 追加一行（典型 366 B）。5 秒间隔下约
  15,500 行/天 ≈ **5.7 MB/天 ≈ 170 MB/月 ≈ 2 GB/年**，是全桥其他文件增长总和的约 100 倍。
  已核查：hook 定义字段（poll_interval_secs / script_timeout_secs 等）与本地文档都**没有**
  暴露 hook 日志的保留/轮转开关，我方也没有安全的截断点（文件由平台进程持续追加，
  贸然截断可能丢行或错位）。待向平台确认保留策略；若将来磁盘吃紧，临时缓解是把
  poll_interval_secs 调大（翻倍即减半增长）或请平台侧轮转。

## 二次全盘审查修复（2026-10-21 深夜，用户批准 P1–P3 全修）

备份在 `~/workspace/wechat-bridge-backups/pre-audit2fix-20261021/`（含 README 回滚步骤）。
离线套件新增 `tests/test_audit2fix.py`（17 项）与 `tests/test_hook_audit2fix.sh`（15 项），旧套件全数回归。

- **P1-1 flusher 监督双保险**：(1) serve 主循环每轮查 flusher 线程存活，死了就先回收
  sending/ 遗留任务、再进程内重启线程并记日志；(2) flusher 每轮、每个发送单元、
  长操作内部（分片回调、媒体上传每次尝试前）刷新 `state/flusher_heartbeat`，
  hook 据心跳 >120 秒未更新判线程死/卡并杀进程重拉（见布局节 hook 职责）。
- **P1-2 hook 平台日志**：无我方修复点，量化后记为已知平台项（见「serve 管理」节）。
- **P2-1 hook 并发**：脚本整程 flock（上一轮未完下一轮静默跳过）；认领维护的
  读-改-写整环移入认领锁内，worker 补记不再被冲掉；原 claim_rewrite 整体替换函数已删。
- **P2-2 快路径硬规则**：见「消息分流」节，含时间/金额/人名/变更动词/事项进展汇报的消息一律走事务路径。
- **P2-3 手册补全**：新增本「serve 管理」节（完整手动重启命令）；布局节轮询间隔统一为 5 秒。
- **P2-4 venv runbook**：见「serve 管理」节。
- **P3-1 sent/ 保留期与权限**：hook 每天至多一次清理 sent/ 中 >30 天的任务文件
  （本轮上线时 sent/ 共 54 个文件、全部 <30 天，首跑删除 0 个）；serve 把任务移入
  sent/ 时统一 chmod 0600（此前 45 个是 worker 的 umask 0660）。
- **P3-2 seen.txt 上限**：hook 维护段超过 5000 行裁到最近 2000 行（就地重写不换 inode）。
- **P3-3/4 日志时区**：hook 拉起 serve 时带 TZ=Asia/Shanghai，serve.log 从此北京时间（旧日志 UTC）。
- **P3-5 登录流与媒体幂等**：wait-login（及取二维码）遇非 dict 响应按轮询失败处理、不再
  AttributeError 打崩；媒体 sendmessage 的 client_id 改为按任务名稳定派生（md5(任务名#0)，
  与文本同规则），响应丢失后的任务层重试复用同一 client_id，不再必然重复发图。
- **P3-6 传输层重试收口**：sendmessage 与 getuploadurl 改为单次尝试（retries=1）——旧默认
  传输层连打 3 次，「服务端已收下、连接才断」时会重复投递，去重与否全靠未验证的服务端
  行为；现在连接错误立刻抛给任务层，由带稳定 client_id 的退避重试接管。getupdates/探活
  保持默认重试（幂等无投递副作用）。
- **P3-7 杂项**：审计树里被 URL 编码弄坏文件名的旧提示词文档改名为 `PROMPT-optimized.md`；
  审计树移除 egg-info 与 __pycache__（运行时会再生成，后者属正常产物）。

## 回滚（补丁版出问题时，约 2 分钟）

备份在 `~/workspace/wechat-bridge-backups/pre-p1p7-20261021/`（补丁前的已装包、审计源码、hook、本文档）。

1. 杀 serve：`kill $(cat ~/workspace/wechat-bridge/state/serve.pid)`（hook 会在约 5 秒内尝试拉起，
   所以下面第 2 步要快，或先临时把 pending 清空防旧代码空转——旧代码会按原样处理 pending 任务，
   attempts 等新字段会被忽略，无数据风险）
2. 还原代码：`cp ~/workspace/wechat-bridge-backups/pre-p1p7-20261021/wechat_bridge_pkg/*.py ~/workspace/wechat-bridge-venv/lib/python3.12/site-packages/wechat_bridge/`，
   审计树同理用 `wechat_bridge_audit_src/`；hook 用备份里的 `wechat-inbox-watch.sh` 覆回 `~/hooks/scripts/`
3. 清 pycache 后等 hook 拉起或手动 setsid nohup 拉起（同 hook 命令），`wechat-bridge status` 确认已登录+serve 运行中。
   登录态不受回滚影响，不需要重新扫码。outbox/failed/、sending/ 是新版才用的目录，旧代码会忽略。

## 回滚（合并改造出问题时，约 2 分钟）

备份在 `~/workspace/wechat-bridge-backups/pre-merge-20261021/`（合并改造前的已装包、审计源码、
hook 脚本、本文档，内含 README.txt 写了完整步骤）。步骤与上一节同构：杀 serve → 用备份的
`wechat_bridge_pkg/*.py` 覆回已装包、`wechat_bridge_audit_src/` 覆回审计树、hook 脚本覆回
`~/hooks/scripts/` → 清 pycache → 等 hook 拉起 → status 确认。另需手动一步：合并约定还写进了
hook 的 worker 提示词（`~/hooks/definitions/wechat-inbox-watch.json` 的 prompt 第 3 步），
回滚代码时把那段改回「每条消息回一条」或整段删掉合并表述。登录态不受影响，不需要重新扫码。

## 回滚（全盘审查 12 项出问题时，约 2 分钟）

备份在 `~/workspace/wechat-bridge-backups/pre-audit12-20261021/`（本轮修复前的已装包、
审计源码、hook 脚本与定义、本文档，内含 README.txt 完整步骤）。步骤与前两节同构：
杀 serve → 用备份的 `wechat_bridge_pkg/*.py` 覆回已装包、`wechat_bridge_audit_src/`
覆回审计树、hook 脚本与定义 JSON 覆回 `~/hooks/` 对应位置 → 清 pycache → 等 hook
拉起 → status 确认。注意：本轮删掉的审计树 `build/` 与 `watchdog.sh` 是死代码，
回滚不需要恢复它们（watchdog.sh 备份里有一份，仅备查）。登录态不受影响，
不需要重新扫码。

## 回滚（提速改造出问题时，约 2 分钟）

备份在 `~/workspace/wechat-bridge-backups/pre-speed-20261021/`（提速前的已装包、
审计源码、hook 脚本与定义、本文档，内含 README.txt 完整步骤）。步骤与前几节同构：
杀 serve → 用备份的 `wechat_bridge_pkg/*.py` 覆回已装包、`wechat_bridge_audit_src/`
覆回审计树 → 清 pycache → 等 hook 拉起 → status 确认。hook 定义用 hooks 工具把
`poll_interval_secs` 改回 30、prompt 第 3 步的「自适应宽限」段改回原来的一律
等约 15 秒（备份 hooks/wechat-inbox-watch.json 里是旧版全文，或覆文件后
hooks.view 核对）。登录态不受影响，不需要重新扫码。

## 回滚（二次全盘审查修复出问题时，约 2 分钟）

备份在 `~/workspace/wechat-bridge-backups/pre-audit2fix-20261021/`（改前已装包、审计源码、
测试、hook 脚本与定义、本文档，内含 README.txt 完整步骤）。步骤与前几节同构：
杀 serve → 用备份的 `wechat_bridge_pkg/*.py` 覆回已装包、`wechat_bridge_audit_src/`
覆回审计树、hook 脚本与定义覆回 `~/hooks/` 对应位置、文档覆回 → 清 pycache →
等 hook 拉起 → status 确认。新增的 `state/flusher_heartbeat` 文件旧代码不读，
留着无害；hook 侧的 sent/ 清理与 seen 裁剪随脚本覆回一并撤销（已裁/已清的
文件不可恢复，但都是 >30 天的 sent 任务与过期 seen 行，无业务影响）。
登录态不受影响，不需要重新扫码。

## 常用操作

- 查状态：`~/workspace/wechat-bridge-venv/bin/wechat-bridge status`
- 重新登录（token 失效时）：`wechat-bridge login` 生成 `~/workspace/wechat-bridge/qrcode.png` 发给用户扫，
  同时后台跑 `wechat-bridge wait-login` 等确认；成功后确认 serve 在跑（hook 会自动拉起）。
- 手动发一条：用户必须先发过消息（要有 context_token）；优先走 outbox 机制，`wechat-bridge send` 亦可。

## 限制（微信官方）

- 仅一对一私聊，不支持群聊；serve 只处理机主本人的消息。
- Bot 不能主动发起会话：用户先发消息才有 context_token。因此晨报等主动推送不保证送达，
  晨报仍走 Muse App + WhatsApp，不要承诺微信推送。
- 连续约 10 条不回复会被限流；token 失效（ret/errcode -14）需重新扫码。
- 回复延迟构成（2026-10-21 晚提速后）：hook 5 秒轮询发现消息 + worker 处理
  （含自适应合并宽限，连发场景最多等约 8 秒补扫）+ flusher 线程 2.5 秒内扫到并发出；
  闲置后第一条典型 ~15–30 秒。改造前是 hook 30 秒 + 一律等 15 秒 + 发送等长轮询
  返回（最长 ~120 秒），第一条常要 1–2 分钟。
