# Muse 私有运行开关

这是 WBR-00-20261108-CONTINUITY-ROLLOUT 的接线修正。固定公共包保持 b1a7ee7 的 37 个模块。Hook 与平台 Worker 分别读取同一份私有配置，解决平台 Worker 无法继承 Hook 脚本 export 的问题。

## 文件与优先级

配置路径固定为实际 `--state-dir` 下的 `muse-adapter/runtime-switches.json`。Hook 使用现有 `WECHAT_HOOK_STATE_DIR` 定位同一目录，adapter 使用原有可信 CLI 参数。路径不能来自微信正文或请求 JSON。

文件必须为当前运行用户所有、精确 0600、单硬链接的普通文件，父目录 `muse-adapter` 必须为同用户所有、精确 0700 的真实目录。拒绝符号链接、特殊文件、超过 4096 字节、重复键、未知键、缺字段和读取期间的对象替换或修改。读取不创建目录、改权限或写入文件。新建私有目录后必须核实实际模式，setgid 父目录可能改变继承结果。

完整格式如下。这组值对应此次现场已核的 v2/CDN 开启、新两项关闭状态，属于本次迁移示例。

```json
{
  "schema_version": 1,
  "environment": {
    "WECHAT_REFERENCE_V2": "1",
    "WECHAT_CDN_TIER_TRIAL": "1",
    "WECHAT_RECENT_CONTEXT": "0",
    "WECHAT_CLOUD_CONTEXT": "0"
  }
}
```

四项值均只接受字符串 `"0"` 或 `"1"`。文件存在时完整覆盖这四项继承值，文件中的 `"0"` 可压住平台环境中的 `"1"`。其它环境变量沿用原入口。文件确实缺失时保持原继承环境，原有两个新能力仍按公共代码默认关闭。坏文件返回固定 `invalid_configuration`，不静默改用默认值。

配置不接受密钥、路径、命令或额外环境项。`memory_delta_v1`、微信请求、Hook payload 和模型动作均不能修改开关。

## 两个实际入口

Hook 在 dry-run 之后进入既有 Hook 互斥锁，沿原顺序加载受信平台 runtime，再确定实际 adapter 并读取一次配置，之后才运行 supervise、plan 或 wake。runtime 提供的原 adapter 路径继续有效，配置中的四项值优先于 runtime 的继承环境。坏配置可以创建或触及固定互斥文件的元数据，但不会领取消息、监督启停、规划或唤醒。锁被其它 Hook 占用时不读取配置。锁内重入不会二次读取配置，也不靠额外环境标记跳过校验。

Hook 通过已部署 adapter 的只读入口取得四项白名单值，逐项 export。它不 source 或 eval 配置内容。只读入口可由维护者核对。

```sh
"$LIVE_PYTHON" "$LIVE_ADAPTER" \
  --base "$LIVE_BASE" --state-dir "$LIVE_STATE_DIR" \
  --runtime-environment
```

成功输出 `schema_version`、`configured` 和 `environment`。缺文件输出 `configured=false` 和空 environment。该命令不读取请求 stdin、不构造 WorkerState/Store，不领取或发送消息。

平台 Worker 每次正常运行 adapter CLI 时，自行读取同一文件并在调用公共层前应用四项值。该步骤独立于 Hook 的环境继承。普通回复、附件回复、继续和原话回查仍走原业务入口。原有进程内 `handle_request` 保留其既有调用约定，私有运行配置属于 CLI 启动边界。

正常 adapter CLI 在应用有效配置后向 stderr 记录 `runtime_configuration`。记录仅含固定 stage、实际 PID、四项有效值和 `effective_switches_sha256`。这个哈希使用现有 `_digest` 协议，对四项值按 `json.dumps(environment, ensure_ascii=False, sort_keys=True).encode("utf-8")` 取 SHA256，原始配置文件哈希另记。没有原始路径、正文、token、lease 或任意环境值。记录失败不会改变业务状态。

`/proc/PID/environ` 通常反映 exec 时的初始环境，不能替代 CLI 读取配置后的有效值记录。合法 begin 的能力返回和此 PID 记录合用，证明实际 Worker 采用。Hook 子进程/serve/relay 则按真实启动环境、进程身份与日志分别绑定。

## 受控原子更新

配置由原维护者在已核实际状态根下维护。先保存旧配置或“原来缺失”的事实及其 hash，与当前代码、依赖、Hook、prompt 和恢复步骤一起保存在原机私有备份中。

1. 使用已经核实的实际用户、绝对状态目录及私有父目录。父目录若新建，核实际 0700；已有目录异常先定位，不递归改权限。
2. 在同一私有父目录内创建独占临时普通文件，权限 0600。写完整 JSON，校验四项严格 schema，flush 并 fsync 文件。
3. 以同目录原子 replace 发布到固定名，再 fsync 父目录。拒绝现有目标的链接、硬链接或不当归属，失败保留原可用配置，不把半写内容作为正式文件。
4. 用只读入口读回并独立保存实际文件字节 hash。再以真实 Hook 和独立平台 Worker 的实际记录核有效值。

每次读取只消费一份完整配置。原子 replace 不保证不同时刻的 Hook 和 Worker 天然采用同一版本，也不会修改已运行 serve 的环境。切换仍需原有静止窗口、身份核实与单 receiver 停起纪律。

此次先安装配套新 Hook/adapter，配置保持 v2=1、CDN=1、recent=0、cloud=0，验证默认关闭及真实加载。旧 owned 和旧收件按原流程自然完成后，再在已核静止窗口仅把 recent 改为 1，cloud 继续为 0。需要重启的 actor 按受控流程重启，不改公共账本或重放消息。

## 验证与回退

新增测试必须覆盖文件存在、缺失、严格坏值、重复/未知/缺字段、过大、目录和文件模式、链接及替换竞态；分别通过真实 Hook 和独立 adapter CLI 证明同一配置生效，包含继承环境为 1、文件为 0，以及反向组合。原普通和附件终结、真实 context 链路按受影响范围复验。

新 Hook/adapter 使用后继源码和独立 QA 证据，b1 原 Hook 字节的 PASS 不自动覆盖。公共 37 模块逐字节未改时，可复用其已核公共与安装证据。

停用近期能力时，配置中的 recent 置 0，cloud 保持 0，核实际 actor 生效，过期维护继续。若恢复旧 Hook/adapter，须连同旧代码和实际环境承载方式恢复；旧代码不会读取本配置，不能仅留下文件就认为旧 v2/CDN 仍启用。回退保留当前队列、回执、游标和附件账本。
