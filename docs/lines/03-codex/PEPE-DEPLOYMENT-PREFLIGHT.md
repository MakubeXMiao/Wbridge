# PEPE 部署前置核对

目标为用户指定的 OpenAI Codex Dot 环境 **PEPE**。本页是 `WBR-03-20261024-001` 的准备清单，未访问 PEPE、未部署或迁移登录。本机已有协议取证，PEPE 的系统、执行入口、认证、持久运行和网络能力仍为 **unknown**。

02 已取得 Muse 的实答，回读完成上界为 2026-10-24 01:21:57（Asia/Shanghai）；私有来源为 `<git-common-dir>/wbridge-coordination/evidence/02-muse/WBR-00-20261024-003/muse-reply-readback.md`。下述环境及运行方式是 **Muse 自报**，02 未直接读取其云端命令、日志或历史证据，不等于独立实机验收。以下核对项不阻塞本线明确的代码及文档修复。

## Muse 实答与复用边界

- 自报在其平台 VM 运行 Ubuntu 24.04.5 LTS / x86_64 / Python 3.12.3，以 venv 安装 Python 包；shell 身份为 root，“用户级安装”不能解释为非 root 权限。home 持久性与 VM 更换后的恢复仍只有自报，不能推广到 PEPE。
- 自报用 `setsid nohup` 后台启动 `serve`；每 5 秒由平台 Hook 检查并在失败后重新拉起。后台化和自动重启是两个环节，`nohup` 本身不提供监管，现有记录也未证明系统开机自启或休眠恢复。
- 平台 Hook、专属 runtime wake、旁聊投递与 agent 执行环境属于 Muse 平台能力。Muse agent 身份由平台托管，没有其管理的可迁移登录文件；它没有验证 Codex 登录机制。
- Muse 明确表示从未在 OpenAI Codex Dot / PEPE 部署 Codex CLI 或微信桥，没有该机器的访问或验证记录。

可参考普通 Python/venv 安装、文件状态、POSIX 锁与后台进程经验；每项仍需目标机实证。不得直接复制 Muse Hook、runtime wake 或平台身份方案。`serve.lock` 只保护相同数据目录，不证明不同目录/主机对同一 bot 全局互斥。

## 两条接入路线

| 路线 | 执行方式与依赖 | 当前结论 |
| --- | --- | --- |
| A：PEPE 宿主运行 wbridge + Codex CLI | 在确认可访问的 PEPE 执行环境安装固定版本，使用目标机 Codex CLI app-server、目标机认证、独立 cwd/state 和进程监管。 | 优先评估复用现有 driver；本机可运行不代表 PEPE 有二进制、认证、沙箱或常驻能力。所有目标机条件仍待核。 |
| B：直接唤醒 PEPE / Dot agent | 由平台提供明确的 agent 触发 API、身份与会话归属、结构化事件/final、双向审批、取消/超时、恢复及生命周期契约。 | 现有 driver 启动 CLI app-server，不能等同唤醒 Dot agent。需 01/00 确认平台契约与归属后再评估适配；Muse wake 机制不构成该 API 的证据。 |

本页以路线 A 列部署准备；路线 B 不应通过更改命令路径或复制 Hook 冒充已支持。只有核实访问入口与真实平台能力后，才由 00 收敛路线。

## 最小只读能力探针

03 已只读核对本对话当前入口：工具清单未暴露 PEPE 远程 shell/API；项目列表返回的 7 个项目均为 `hostId=local`；浏览器清单只有 `mcpapps` 和 `iab`，没有 `dot` surface；当前 shell 为 `MakubeXMac-mini.local` / Darwin / arm64，cwd 是本机 `bfdf/wbridge` 工作树。这仅证明**本对话当前没有可调用的 PEPE 执行入口**，不证明 PEPE 不存在或不支持部署。本机 shell 不视为云端，以下探针均未在 PEPE 执行。

缺少的下一步由 00 协调：请用户打开/连接 PEPE 自己的终端，或提供已配置的受管远程执行入口；随后在该入口运行只读身份和 runtime 探针。不猜 SSH 地址、不扫描网络、不读取连接配置或凭据，也不通过向 PEPE agent 发消息试探权限。本次未执行远程命令、登录或部署。

取得明确入口后，首轮仅采集必要元数据：

1. 核对工具/主机展示的目标身份、实际 OS/架构、用户、cwd 与时钟，区分本机和 PEPE；仅核验已知入口，不据名字推断宿主能力。
2. 查询可执行路径及 Python、Codex 版本；如已安装 wbridge，仅查询实际包路径。使用目标机 `codex login status` 记录状态类别，不打开 auth/token 文件、不输出账号或凭据。若尚未安装，记录缺失，留待后续安装步骤。
3. 查询是否具备 Python `venv`、POSIX `flock`/进程组能力，以及平台公开的常驻、监管、存储、出网限制；现有配置只记录是否满足约束，不采集凭据或完整私有配置。仅凭 API/命令存在不能判定真实停止、沙箱或持久运行 PASS。
4. 若选择路线 B，读取平台明确提供的 API/schema 或能力说明，逐项核对触发、thread/session、审批、cancel、事件终态和生命周期；没有文档/入口即保持未知，不用 Muse 专属接口代替。

跨任务常驻、重启持久化、出网、取消、沙箱、扫码与真实模型需要后续受控测试；只读首轮不启动服务、创建线程/模型任务、安装软件或修改 state。随后按下表逐项留证。

在**已核对身份的 PEPE 终端**执行以下元数据探针；本轮未执行它。若 `python3` 不存在，仅记录缺失，不在探针中安装。它不展开环境变量、不导入 bridge 包、不读取配置/认证文件，也不运行模型。

```sh
python3 - <<'PY'
import datetime, importlib.util, json, os, platform, shutil, sys
package = importlib.util.find_spec("wechat_bridge")
print(json.dumps({
    "hostname": platform.node(), "os": platform.system(),
    "release": platform.release(), "architecture": platform.machine(),
    "uid": os.getuid() if hasattr(os, "getuid") else None,
    "cwd": os.getcwd(), "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "python": sys.executable, "python_version": platform.python_version(),
    "codex_executable": shutil.which("codex"),
    "bridge_module": package.origin if package else None,
    "venv_module": importlib.util.find_spec("venv") is not None,
    "fcntl_module": importlib.util.find_spec("fcntl") is not None,
    "process_group_api": all(hasattr(os, name) for name in ("setsid", "killpg")),
}, ensure_ascii=False, indent=2))
PY
```

若找到 Codex，再由目标机操作者用返回的绝对路径执行 `--version` 和 `login status`；回执只保留版本与登录状态类别，不粘贴原始登录输出。元数据中 `true` 仅表示 API/模块存在，不能代替后续能力测试。

## 主机与运行条件

| 核对项 | PEPE 需提供的证据与通过条件 | 当前状态 |
| --- | --- | --- |
| 主机身份与访问 | 操作者确认目标就是 PEPE、可用访问方式和执行权限；若使用 SSH，核验主机身份、连接入口与实际登录用户。不能假定 PEPE 提供 SSH。 | unknown |
| OS 与架构 | 记录 OS/版本、CPU 架构、实际用户及工作目录；确认能运行所选 Python、Codex 与当前适配器所需的进程组操作。不能沿用 macOS 应用内路径。 | unknown |
| Python 与安装 | 记录解释器绝对路径/版本、实际加载的 `wechat_bridge.__file__`、安装方式；对固定集成 SHA 构建/安装并运行离线测试。 | unknown |
| Codex 二进制与协议 | 记录绝对可执行路径、`--version`；核验该安装版 app-server 的初始化、配置读取、显式 thread/turn、审批与 interrupt 协议。 | unknown |
| 登录状态 | 由 PEPE 操作者仅执行该主机 `codex login status`，记录状态类别。需登录时由用户在目标环境完成；不读取、复制本机 token/auth 文件，不新建 key 或把认证打入镜像/仓库。 | unknown |
| 出网与重连 | 在目标主机核验 Codex 服务、微信 iLink/CDN、DNS/TLS 和必要代理路径；记录断连、会话结束后的进程/网络行为。离线测试不覆盖此项。 | unknown |
| 独立 cwd 与磁盘 | 为模型建立仓库外独立目录，确认权限、磁盘配额与重启后持久性；附件仅用合成数据及显式允许目录。目标主机路径变更须使用新的受管实例或审核后的恢复流程。 | unknown |
| 沙箱与本机配置 | 验证 workspace-write、审批、MCP/插件/规则预检及环境过滤在该安装版生效。未验证前不得声称只读隔离或能阻止读取主机其他文件。 | unknown |
| 进程组与停止 | 验证 owned app-server 的进程组、TERM/KILL、取消/超时和残留子进程清理；停止回执不等于撤销已发生副作用。 | unknown |
| 服务监管与持久运行 | 确认目标支持的服务监管方式、日志位置/权限、单实例约束、退出和重启行为；不能假定终端会话结束后仍运行。配置不记录 token、正文或审批 payload。 | unknown |

本机取证仅证明本机已安装版本和已记录的只读探针结果；准确版本及边界见 [IMPLEMENTATION.md](IMPLEMENTATION.md) 与对应固定 SHA 的私有证据。目标版本不一致时重新核验，不用本机 PASS 填写 PEPE 的状态。

## 独立账号与可恢复交付

1. 选择 00 集成并经独立 QA 的完整 SHA，从新私有库 `MakubeXMiao/wbridge` 安装。记录源码 SHA/dirty、解释器和实际包路径、Codex 版本、配置摘要及启动时间；取得服务重启及实际加载版本证据后才标记部署完成。
2. 为 PEPE 使用全新 bridge 运行根、唯一 `account_scope`/`instance_id` 和独立 cwd。模板放在运行根外，按照 [LOCAL-TESTING.md](LOCAL-TESTING.md) 先由用户用另一微信号扫码，绑定成功且确认与 Muse 的 bot/owner 不同后再保存配置、预检和启动。目标一次只运行一个对应 bot 的收件服务。
3. 不同步本机或 Muse 的认证、会话、DB、队列、游标、收据或 Hook。既有 session 绑定账号、后端实例及 cwd，不能将本机 session token 当作可携带的跨主机恢复授权。需要迁移受管状态时另由 01/00 审核归属与恢复方案。
4. 发布前保留可回退的已审核安装版本、配置摘要和停止/启动命令。回退先停目标服务并核对活动/unknown 执行与投递；保留现场状态，由 01 核对状态兼容性。不得通过删除 DB、回滚游标、清空队列或重放 unknown 实现回退，也不改变 Muse 线上实例。

## 真实验收及回执

在代码上传、集成 QA、目标主机核对完成后安排用户测试；每项都绑定 PEPE 实际加载的完整 SHA、解释器/包路径、Codex 版本和重启时间：

1. 真实 final 与连续两轮显式续聊，确认线程归属和无跨账号/实例串线；明确超限/失败结果。
2. 两个同时到来的权限请求按序呈现，逐项允许/拒绝；过期、取消、断连与重启清理，以及普通“好”不构成批准。
3. 空闲后端切换、取消与超时、迟到结果、sandbox 生效与所有 owned 子进程停止。
4. 微信 API 接受与用户手机收到分别留证；确认同 bot 只有本实例收件且 Muse 原实例运行未受影响。

回执逐项填 `PASS / FAIL / unknown`，附脱敏时间、execution/attempt/thread/turn 关联及复现步骤；不上传私聊正文、身份凭据或原始配置。取得这些证据前，PEPE 部署、模型和手机验收均保持未验证。
