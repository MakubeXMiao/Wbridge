# 公共监督事件 V1 / WBR-00-20261024-012

01公共模块自己持久记录完整监督信息，02 Hook无需另存重复stderr/原命令。`worker_state supervise` stdout继续只有 `{alive:bool,restarted:bool}` JSON，既有Hook只取alive仍兼容；事件不混入stdout，不改变120/180、90/570。

位置：运行根 `state/serve-supervision.jsonl`，备份 `.1`，各≤256KiB、合计≤512KiB；文件0600，状态目录0700；锁 `.lock` 为空。非可靠审计账本：争锁/磁盘/轮转失败可丢该事件，不能因为诊断失败改监督决定，不输出完整异常。serve.log独立5MiB×3；新受控启动经pipe relay连续有界，旧健康FD仅巡检裁剪，正常受控重启后生效，不为日志主动重启。

每行固定字段：event=`serve_supervision`、schema_version=1、at、reason、action、result、old_pid/old_start、new_pid/new_start、heartbeat_at/heartbeat_age_seconds、exit_code、exit_signal。无tokens/命令/URL/账号/context/正文。PID为>1真整数或null；时间为有限非负数字或null，不能填NaN/Inf。未知心跳/启动时间用null。没有直接wait证据的退出码/信号为字符串unknown；relay只可对自己实际wait的亲生child写真实整数退出码或信号，不能从PID消失猜退出原因。

reason有限枚举：identity_unverified、pid_missing_or_invalid、clock_unavailable、heartbeat_stale、heartbeat_fresh、startup_timeout、startup_grace、identity_changed、stop_unconfirmed、signal_failed、spawn_failed、spawn_unconfirmed、service_ready、child_exit。

action有限枚举：keep/defer/term/kill/observe/launch。result有限枚举：healthy/unknown/requested/stopped/failed/started。TERM/KILL请求记录requested，后续独立身份观察才可记stopped；身份变更或停止未确认时defer/unknown，不继续向复用PID发信号。新启动须真实relay ACK、childPID与独立PID/start核对，ACK本身不代表serve ready。事件保留最初触发reason和各步动作，不把十次启动写成十次已证崩溃。

supervise返回、持久诊断、云端实际加载与手机送达分别验收。该契约已在既有有界监督修复授权内；若02不新增消费者，无需Hook增量。最终实现候选仍需本线非实现者QA及00的公共+Muse组合QA，本文不是现场部署或历史根因证明。
