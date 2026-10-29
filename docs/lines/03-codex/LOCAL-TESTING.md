# 独立账号测试准备

本说明在候选上传后供 00 集成与用户配合测试。本轮没有执行扫码、启动 serve、发起真实 Codex turn 或放宽 Muse 机主过滤。

## 版本及目录

从本公开仓库取完整固定提交；安装须包含适配器及公共 v1 CLI/调度。先记录源码 SHA/dirty、解释器、实际 `wechat_bridge.__file__`、Codex 绝对路径/`--version`、启动时间；不要把当前磁盘 HEAD 倒填到旧进程。

另建独立运行根（例 `/absolute/wbridge-codex-test`）及独立工作目录（例 `/absolute/wbridge-codex-workspace`），均放在 Git 仓库外，权限 0700。首次登录使用全新空运行根，不能在其中预放配置、模板或其他数据。所有测试命令显式设置 `WECHAT_BRIDGE_DIR` 为新运行根。该根下的 `state/`、`media/`、backend DB 与会话不复用 Muse 或旧 Codex relay。`CODEX_HOME` 维持本机已登录配置，不复制 auth 文件，也不另建 key。

仅先执行 `<Codex绝对路径> --version` 与 `<Codex绝对路径> login status`。预期显示 ChatGPT 已登录；如果未登录，暂停真实模型测试，由用户自行完成同账号登录，不读取认证文件寻找 token。记录状态类别，不记录输出中的账号/凭据片段。

先将本目录 `backends.example.json` 的路径与实例名替换，**仅保存到运行根以外**，例如 `/absolute/wbridge-codex-prep/backends.json`，权限 0600。此时不要创建新运行根中的 `state/backends.json`。`account_scope` 使用独立测试标识；公共层同时绑定实际登录 bot 和 owner。`attachment_roots` 只包含测试工作目录与该新运行根的 media。模型可选，不随意扩大其他配置。

## 首次扫码、配置与启动次序

按[公共启动指南](../01-core/QUICKSTART-V1.md)使用完整集成版本；以下命令中的绝对路径均须替换。`WBR_PYTHON` 必须指向已安装该固定版本的同一个解释器，后续不要混用系统 Python、另一 venv 或旧 relay 的包。先创建空运行根并扫码：

```sh
export WBR_PYTHON=/absolute/wbridge-venv/bin/python
export WECHAT_BRIDGE_DIR=/absolute/wbridge-codex-test
mkdir -m 700 "$WECHAT_BRIDGE_DIR"
mkdir -m 700 /absolute/wbridge-codex-workspace
"$WBR_PYTHON" -m wechat_bridge login
"$WBR_PYTHON" -m wechat_bridge wait-login
```

`mkdir` 若提示运行根已存在，先核实目录归属，不删除旧状态或在未知目录继续绑定。由用户用**另一微信账号**扫码，待 `wait-login` 明确成功，确认得到新的 bot/owner 绑定；同一个 bot 只运行一个收件服务。若失败，或扫描结果仍是 Muse/旧 relay 所用 bot，停止本次启动并保留状态排查，不放宽过滤、不竞争轮询。

仅在上述绑定成功且归属核对通过后，将准备好的模板放入运行根，再检查配置和启动：

```sh
cp /absolute/wbridge-codex-prep/backends.json "$WECHAT_BRIDGE_DIR/state/backends.json"
chmod 600 "$WECHAT_BRIDGE_DIR/state/backends.json"
"$WBR_PYTHON" -m wechat_bridge backend-check
"$WBR_PYTHON" -m wechat_bridge backends status
"$WBR_PYTHON" -m wechat_bridge serve
```

逐条执行；任一预检失败先修正，两个预检均成功后才运行 `serve`。首次扫码前放入配置会触发公共登录的未知归属数据保护，不能为此移除保护或清空状态。公共预检仅检查配置、路径、适配工厂及本地身份；`provider_auth: not_checked` 和 `wechat_token_validity: not_checked` 不证明真实认证、模型生成或手机收件成功。

## 实测记录

先完成代码上传和集成 QA，再由用户在新 bot 发合成测试消息：

1. 新任务正常回复；第二轮明确引用首轮合成标记，确认仅续接本测试 thread。
2. `/backend` 查看路由；仅空闲切换。未配置 CC/Muse 时拒绝不可用目标，不临时复用其他 bot/state。
3. 以测试目录内无敏感内容的动作触发一次命令/小型文件修改审批。核对动作摘要、公共审批 ID；分别测试 `/approve <id> deny` 和新请求 `/approve <id> allow`。普通“好”不得批准；过期/重复/错发送者应拒绝。
4. `/cancel <execution-id>`、任务超时与审批期间断开后重启；确认不会发布迟到答案或自动重做副作用。中断回执与“副作用已撤销”分别判断。
5. 可选测试该独立 media 内的合成图片；文件失败标记必须明确，不能声称看到了未下载图片。

每项记录完整代码 SHA、dirty、解释器/包路径、Codex 版本、重启时间、脱敏 execution/attempt/thread/turn 关联、终态、微信 API 接受时间和手机实际收到。真实 final、续聊、审批往返、取消、sandbox、微信闭环在取得记录前均为**未测**。

停止测试时只停止测试根对应的服务和 owned 执行器，保留 DB/游标/收据；未决 unknown 按 01 对账流程处理，不删除状态后重试。Muse 的现有运行目录、Hook、机主绑定和生产代码不随此测试修改。
