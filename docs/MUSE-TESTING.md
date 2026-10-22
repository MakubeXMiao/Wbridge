# 给 Muse：拉取修复分支并做实机测试

仓库：https://github.com/MakubeXMiao/wechat-bridge-muse
分支：`fix/qa-reliability-20261022`
基线：`e1e72168ca8e1d2819df439cd3a1119aa4078eca`

所有者只批准把修复推到此分支，**尚未批准合并main或替他直接部署**。本文件用于所有者另行让Muse执行测试。请先报告计划、当前运行源码路径/提交和实际Hook定义，确认使用测试账号还是受控切换同一账号。

## 1. 先拉取到独立源码目录

```bash
git clone --single-branch --branch fix/qa-reliability-20261022 \
  https://github.com/MakubeXMiao/wechat-bridge-muse.git \
  ~/workspace/wechat-bridge-qa-src
cd ~/workspace/wechat-bridge-qa-src
git rev-parse HEAD
python3 -m venv ~/workspace/wechat-bridge-qa-venv
~/workspace/wechat-bridge-qa-venv/bin/python -m pip install -e .
~/workspace/wechat-bridge-qa-venv/bin/python tests/run_offline.py
```

若目录已存在，先检查git status，禁止覆盖未提交工作；在干净的测试副本中fetch此分支并fast-forward。安装和离线测试不需要真实bot token。

## 2. 切换前的必要核对

- 记录旧服务源码、解释器、启动命令、Hook脚本、实际prompt/投递目标和返回旧代码的方法。
- 在云主机本地备份上述代码/配置；运行状态先停写再备份。凭据、消息、完整备份不得上传GitHub或发给协作者。
- 优先使用独立测试微信账号及独立数据/Hook状态。若使用同一个机器人，暂停旧Hook、等待旧worker结束、停止已验证身份的旧serve后才切换；不能两套服务抢收。
- 不覆盖或清空现有session、游标、队列、seen/claim/started、received账本。测试账号不能复用旧账号的数据目录。

## 3. 包、脚本、prompt作为一个版本更新

- Python服务与worker都使用测试venv里的本包，先确认 `python -c "import wechat_bridge; print(wechat_bridge.__file__)"` 指向此分支源码。
- Hook脚本使用此分支的 `hooks/wechat-inbox-watch.sh`。配置 `WECHAT_BRIDGE_PYTHON`、`WECHAT_BRIDGE_EXECUTABLE`、`WECHAT_BRIDGE_DIR`、`WECHAT_HOOK_STATE_DIR` 与实际测试路径一致；平台若不支持环境配置，复制到既有默认路径时也须保持同一版本。
- 把 `hooks/wechat-inbox-watch.json` 的prompt及兼容的轮询设置同步到真实平台定义；保留原真实delivery.to、聊天绑定、平台字段和所需运行时，不把仓库占位符覆盖线上目标。
- `HATCH_HOOK_RUNTIME` 是平台提供文件，不在仓库；不要把 tests/fixtures/hook_runtime.sh 装成生产运行时。
- Worker按 [WORKER-STATE](../hooks/WORKER-STATE.md) 执行adopt/claim/enqueue-reply；不能继续旧版直接改文件的处理方式。
- 首次新版本plan会迁移仍在inbox中的legacy seen消息和旧重唤醒预算；运行前保留备份，不混跑旧worker。

## 4. 所有者实机验收清单

逐项记录版本、动作、预期和脱敏证据，不能只看命令exit0：

1. 单条消息、连续两条消息：确实收件、worker取得lease、手机收到相应回复且无额外重复。
2. 同一message的文字+附件、多附件：如API实际提供该形状，所有内容保留；无转写语音保留原音，不假称已有自动ASR。
3. 图片/文件回复与配文：配文先于媒体；失败恢复后不重复配文。确认新helper媒体入口可用。
4. 发送中断/超时、serve重启：稳定任务身份、进度保留；记录手机实际收到几次，不预设iLink必定按client_id去重。
5. 超时/重复投递：同事件不能两次adopt；迟到旧lease不能写回复；只有rewoken_files的payload也能处理。
6. 同用户回复FIFO、token预算耗尽后续发、failed/隔离告警和恢复状态可见。
7. 原有事务路径：涉及安排/金额/日期等仍按需读取历史与工具，不因新快路径漏记。
8. 云主机进程保活、实际runtime、真实Hook投递/worker上限和重启后状态保留。

## 5. 回滚和交回结果

测试不通过先停止新调度和服务，报告具体提交/失败步骤/脱敏日志，不合并、不清空队列或自动重发未知结果任务。

代码回滚不等于运行状态回滚。新版测试期间已经发送的消息不可撤回；盲目恢复旧游标/队列/完成账本会导致重复。保留新旧恢复证据，使用已核对的旧代码+配套旧Hook/prompt；如存在新lease/批次或未确认发送状态，先由维护者判断兼容处置，不能声称一条git命令即可安全回退。

请交回：实际拉取SHA、安装路径、离线结果、每项实机PASS/FAIL、尚未验证项及手机确认。所有者确认通过后再决定合并main。
