# 文件审批离线夹具启动修复

任务：WBR-00-20261029-001。基线：`b6efe0529ccb48fc3d898a8119713b0c2d75b05f`。

## 改动与理由

只调整`FakeCodexCase`的合成Python子进程启动方式：精确匹配当前fixture的`fake-codex`路径时，将实际启动argv从`[fake, ...]`变为`[sys.executable, fake, ...]`。其他路径原样转发，仍调用真实Popen，原cwd、环境、stdio管道、独立进程组、时钟、超时和清理逻辑不变。子脚本的`__file__`及`sys.argv[1:]`不变；不伪造任何协议帧或结果。

该macOS/Python3.9.6环境中的一次运输层诊断：新临时shebang脚本首个initialize响应约875ms，同解释器显式启动新合成脚本约123ms，后续config/read和thread/start均不足1ms。纯解释器导入profile约67ms，不支持把875ms归因site初始化。两次运输探针都只到thread/start，未调用CodexRun.start、turn/start或原失败方法；共用原1秒绝对截止，没有放大预算。

历史原方法两次startup_timeout都发生在取得thread_id之前，文件审批代码尚未执行。原fixture请求日志被unittest清理，不能恢复具体RPC卡点，也未取得操作系统内部延迟来源。上述观察支持消除夹具临时脚本执行路径的额外成本，不证明历史每次超时的精确原因，也不是产品启动性能修复。

## 不变条件与验收边界

- 所有原测试方法、断言、FAKE协议脚本和make_run的1秒启动预算保持不变；产品公开适配器的3.5秒预算与两个产品文件完全不变。
- 原full及受控FAIL证据保留；新候选只补受影响的合成启动、环境/argv隔离、配置前置、审批决定及失败/取消清理链路。完整组合全验由00收口，不能拼成全36套PASS。
- 不再覆盖内核直接执行临时FAKE shebang这条路径。真实Codex冷启动、账号认证、sandbox实际强制、微信/手机和生产部署仍未验证。
- 本单任何第三次同类正式FAIL立即停报00，不按新SHA重置计数或自动返修重跑。
