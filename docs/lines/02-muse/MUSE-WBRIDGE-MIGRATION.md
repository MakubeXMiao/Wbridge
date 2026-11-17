# Muse 迁往 wbridge：版本与操作清单

任务：`WBR-02-20261024-001`。记录日期：2026-10-24，Asia/Shanghai。责任：02 准备并核对 Muse 迁移；00 创建新库、发布已审 refs、收口旧库；01 接纳公共媒体及公共集成。

**本文件是迁移准备清单。新库 refs 就绪和 Muse 来源切换仍待回执；当前双源部署已有 Muse 自报，实际加载证据仍未完整。当前用户手测期间只读取、记录，不重启、不换包、不改真实 prompt、不另发测试消息。** 新库就绪和手测窗口结束两项都满足后，02 才交接切换来源；运行代码升级另记新测试窗口。后续上传目标是 `MakubeXMiao/wbridge`，停止向旧库上传。

## 1. 本次已核与证据来源

1.1 本机只读核对起点为 `<MUSE_WORKTREE>`，分支 `codex/muse`，HEAD `2b3e53791d49962418bc4333be7cbee0a55fa265`；开始时工作区干净，`git remote -v` 为空。本机无 remote 不证明新库不存在，也不证明 Muse 云端没有 remote。

1.2 规则读取自主工作区的 `AGENTS.md`、`docs/FIRST-FULL-VERSION-20261024.md`，而非假设本工作树有最新任务单。本文件涉及 07 发布角色的交接准备，不执行发布。当前项目未依赖 `.ai/` 模板，沿用已有项目资料。

1.3 以下哈希于本轮直接从本地 Git 固定提交的 blob 计算；适配文件另与当前工作树逐字节比较一致。公共媒体的三个变更文件与当前 `codex/muse` 不同，这是两个来源的预期差异。没有把本机哈希当作云端读取结果。

1.4 既有私有证据位于共享协调目录 `evidence/02-muse/`，不随仓库迁移公开：

| 相对证据路径 | 能证明的内容 | 本轮状态 |
| --- | --- | --- |
| `WBR-02-20261023-002/muse-deployment-receipt-review.json` | 2026-10-23 18:10:39 读取的 Muse 部署自报；本地只核对了其三个哈希前缀 | 历史回执，本轮待刷新 |
| `WBR-01-20261023/final-candidate-publication.json` | `b7c47be` 已推旧库并回读 ref 一致；交接已发送 | 不能证明云端已安装或重启 |
| `WBR-01-20261023/independent-qa-b7c47be.json` | 固定候选离线独立 QA | 核心 12/12、媒体 45/45（含 1024 ECB 样本） |
| `WBR-01-20261023/dual-source-manifest-b7c47be-2b3e537.json` | 公共代码/测试取 `b7c47be`、Hook 取 `2b3e537` 的合成组合清单 | 核心 12 套、Muse 3 套离线通过；没有单一组合 Git SHA，没有部署证明 |

1.5 本轮 02 主执行者已直接读取 Muse“微信桥”旁聊，向本文编写者转交最新部署摘要：Muse 自报公共包已装 `b7c47be`、`diff -r` 零差异，Hook 保持 `2b3e537`；脚本/adapter/prompt 哈希前缀为 `6310ab1b` / `b6a5c5b8` / `9fea6910`，真实平台定义已回读。自报旧 serve PID 965 已停、新 PID 9426 于 **2026-10-23 22:48:59** 启动，单实例、账本保留；旧检查器 17/18，仅 `runtime_package_python_sources` 失败。这与单源检查器的预期限制吻合，但不是本线直接读取云端原日志或导出文件的独立部署证明。尚无本轮新用例记录和手机验收；该摘要不补齐完整解释器/包路径、完整 hash 和运行证据。

1.6 Muse 另自报 venv 的 Pillow 为 13.0.0。二评已核 `pyproject.toml` 声明 `qrcode[pil]`，本地 qrcode metadata 的 `pil` extra 包含 `pillow>=9.1.0`。因此“未单独直接列 Pillow”不等于“完全未声明安装依赖”，不据此认定部署缺陷；云端版本仍为自报，需实际环境证据。

## 2. 固定双来源版本

分支可继续前移；核对、安装、验收均绑定完整 SHA，不使用“最新分支”代替。

| 用途 | 原来源仓库 / ref | 固定 SHA | 新库待核内容 |
| --- | --- | --- | --- |
| Muse Hook、adapter、prompt 基准 | `MakubeXMiao/wechat-bridge-muse` / `refs/heads/codex/muse` | `2b3e53791d49962418bc4333be7cbee0a55fa265` | 在 `MakubeXMiao/wbridge` 可 fetch 的保存 ref、完整对象及历史 |
| 公共包、媒体接收与公共测试 | 同旧库 / `refs/heads/codex/media-receive-review` | `b7c47bedf22c957b2df6980eefcca787b556436d` | 在新库可 fetch 的保存 ref、完整对象及历史 |
| 导入基线与历史回退依据 | 同旧库 / `fix/qa-reliability-20261022` | `462337c34f70175e2ee2ddd2bd2a7b1ace402c57` | 00 保存历史，不将其当新版本运行目标 |

新目标地址：`https://github.com/MakubeXMiao/wbridge.git`；使用既有 SSH 认证时为 `git@github.com:MakubeXMiao/wbridge.git`。这些是目标地址，本文没有联网核验仓库存在或 refs 已发布。00 应交回具体 ref→完整 SHA、访问验证和迁移记录，02 收到后再对 Muse 发实际切换要求。

如果 01/00 形成新的集成提交，用该新 SHA 重新生成清单及 QA；不能直接沿用本节的双源哈希或测试结论。不能整支部署 `b7c47be` 内的旧 Hook 来覆盖 `2b3e537` 的 adapter/prompt。

### 2.1 Muse 适配基准哈希

下表 SHA-256 均来自 `2b3e53791d49962418bc4333be7cbee0a55fa265`：

| 内容 | SHA-256 |
| --- | --- |
| `hooks/wechat-inbox-watch.sh` | `6310ab1b876c6dea8abd90ff7f3c5cbda5ed9c3d05f6e1a3a5837299e598fcf0` |
| `hooks/muse-worker.py` | `b6a5c5b8a120171f01cf36b0bbe1b5023045c81e9cbacb8651e4bc4301866869` |
| 仓库 `hooks/wechat-inbox-watch.json` 整文件 | `17af8b0b652d2988b0f1213279940480b385adb94a90db24a71f7eb1551b4164` |
| 上述 JSON 解码后 `prompt` 字符串的 UTF-8 字节 | `9fea6910fc4f850bb089a47906c2a823c93cf922b2f2506e342b602eaec2f088` |
| `hooks/muse-deploy-check.py` | `12001298e1188ba44efb90a11037c8c4ffbd8d409c409e83db20b534073a8db9` |

真实平台 JSON 包含实际 `delivery.to`、绝对脚本路径和平台字段，整文件哈希不应强制等于仓库占位定义。应另记导出整文件哈希、导出时间、真实 prompt 哈希和平台保存回读证据。保持原聊天绑定，仓库占位符不是部署配置。

### 2.2 公共包基准哈希

下表均来自 `b7c47bedf22c957b2df6980eefcca787b556436d` 的 `wechat_bridge/`：

| Python 相对文件 | SHA-256 |
| --- | --- |
| `__init__.py` | `2bda7f7e35746977e19bf51147aeb2482febb82e5b7d2766fa26133cdfa977a8` |
| `__main__.py` | `e0930aeba5a3e2e2d94ca6c5fac4f72c0c4eb2be9bba283becf98e909091471c` |
| `cli.py` | `11d36c894bfa5df997b3b25ab729596babb6b61997efbe2a2e362c65f4c94433` |
| `crypto.py` | `c835b1130bb6ecdd1a474f2a0b25b92df1f4eeb5ad94fcbd4be080b808235178` |
| `ilink.py` | `cbd95fcecd07458676d35ee9e9753e92e00f614bcc9086b85700dceb03256778` |
| `media.py` | `dee5f15a73b49f594af78bff3b531e6d241c28b93951fa09750e1670221f3fb1` |
| `serve.py` | `90a455ea196dc2bd7a36b27cbb5682357ca6b6625c40536b9398d3671f02faa7` |
| `store.py` | `e78d2b68cbb6fc9928f42cc6823cffdcae29403046aff6738af309408d4471c3` |
| `worker_state.py` | `2ad315d5b15d4be5cccf048e2f2d01b48d8e9275eff1d5709d5f892a84e8a955` |

`muse-deploy-check.py` 的 `package_digest` 算法为：按包内 POSIX 相对文件名排序所有 `*.py`，逐文件计算 SHA-256，形成 `[相对名称, 哈希]` 列表，用 `json.dumps(rows, ensure_ascii=True, separators=(",", ":"))` 编码 ASCII，再取 SHA-256。文件集合也必须相同，不能忽略额外 `.py`。

| 9 文件包基准 | 按上述算法的集合 SHA-256 |
| --- | --- |
| `b7c47be` 公共包 | `1edb65a18ef5a8d3c8d3cc8aa2c3f864779f846b0c658aacee203d723db7d425` |
| `2b3e537` 原公共包，用于识别尚未更新媒体的状态 | `b229e258c6fb00c99e53dda49e4211ee8fa59b9255af6de39076dcbc3efb427e` |

集合摘要只覆盖 Python 源码；解释器、依赖版本、实际加载与启停证据另记。不能与既有 manifest 的其他编码算法摘要混用。

## 3. 现有地址与路径清单

### 3.1 仓库旧引用

本轮检索范围是当前 `codex/muse` 的 `hooks/`、`docs/`、`README.md`、`pyproject.toml`，没有扫描云端：

| 位置 | 现状 | 迁移处理 / owner |
| --- | --- | --- |
| `docs/MUSE-TESTING.md` 开头和 clone 示例 | 指向旧库及 `fix/qa-reliability-20261022` | 新运行入口就绪后由 01/00 更新公共说明；02 在交接中给完整新 ref/SHA，避免照旧示例安装 |
| `README.md`、`docs/DEVELOPMENT-BASELINE.md` | 旧库是代码来源和导入历史 | 保留来源事实；新增当前新库入口由其 owner 处理，不全局抹去旧来源 |
| `docs/lines/02-muse/MUSE-ADAPTER-HANDOFF.md` | 部署流程按单候选编写，未指定新远端 | 本文补充双源、迁移门槛；旧测试/历史说明不倒改 |
| `hooks/wechat-inbox-watch.sh`、`hooks/muse-worker.py` | 没有硬编码旧 GitHub URL | 源仓库切换无需改 Hook 业务代码 |
| `pyproject.toml` | 包名/CLI 为 `wechat-bridge`，导入名 `wechat_bridge`，无旧 GitHub URL | 仅迁移仓库不要求改包名/CLI；公共打包变更归 01 |

### 3.2 Muse 运行路径与来源

以下 `~` 属于 Muse 云端执行用户，绝不是本机用户路径。没有收到本轮刷新回执前，不将它们作为已核当前绝对路径使用。

| 项目 | 历史回执或源码默认值 | 证据与待补项 |
| --- | --- | --- |
| Muse 源码目录 | `~/workspace/wechat-bridge-qa-muse` | 2026-10-23 18:10:39 Muse 自报；当前 remote/ref/dirty、媒体源码目录 unknown |
| Python | `~/workspace/wechat-bridge-venv/bin/python`，Python 3.12 | 同次 Muse 自报；需当前绝对 executable、版本及 serve 使用关联 |
| 实际安装包 | 历史仅写 venv `site-packages`；本轮 Muse 自报同路径已装 `b7c47be`，`diff -r` 零差异 | 本次完整包路径仍缺；须补绝对路径及上述逐文件哈希，不能只凭摘要认定实际加载 |
| serve | 历史适配更新：1190 → 10851，2026-10-23 18:07:52；本轮媒体部署自报：965 → 9426，2026-10-23 22:48:59 | 两次分开记录，不推断中间启停过程；本轮仍缺原始启动日志与进程身份核对 |
| Hook 脚本 | 仓库模板 `~/hooks/scripts/wechat-inbox-watch.sh` | 源码模板，当前平台实际配置路径需回读 |
| adapter | 默认与实际 Hook 脚本同目录的 `muse-worker.py` | Hook 允许 `WECHAT_MUSE_ADAPTER` 覆盖；历史自报无覆盖，当前有效环境待核 |
| bot BASE / Hook 状态 | 源码默认 `~/workspace/wechat-bridge` / `~/hooks/state` | 仅默认值；以当前 `WECHAT_BRIDGE_DIR`、`WECHAT_HOOK_STATE_DIR` 实际值为准 |
| 备份 | 适配前 `~/workspace/wechat-bridge-backups/pre-codex-muse-20261023/`；媒体前 `~/workspace/wechat-bridge-backups/pre-media-fix-20261023/` | 分别来自历史和本轮 Muse 自报；存在性、内容、与当前回退版本关系待核 |

另外核对 `WECHAT_BRIDGE_EXECUTABLE`、`WECHAT_BRIDGE_PYTHON` 的实际路径，以及平台提供的 `HATCH_HOOK_RUNTIME`。只回报所需非敏感路径、哈希、版本与时间，不导出整个环境。`tests/fixtures/hook_runtime.sh` 不进入真实平台。

## 4. 分阶段迁移与验收

### 4.1 A：冻结当前测试对象（现在只读）

02 收集当前两来源的完整 SHA、dirty/补丁哈希、解释器/包路径、包完整文件清单及哈希、Hook/adapter/真实 prompt 哈希、平台回读时间、serve/worker 启动身份、媒体更新后的重启证据。历史信息保持历史标签；缺失项写 `unknown`。

记录单条、连发 5 条、begin 后到输入、图片、图文后跟文本每轮的起止时间、匿名化消息关联、worker/batch 关联及阶段耗时。`queued`、API 接受、自动回复、人工补发、手机确认分别列出。用户正在手测，不能以采证名义重启、改 prompt、变更 active 源码或新发测试。

保留账号绑定、token、游标、队列、seen/claim/started、reply_batches、发送/恢复账本、adapter 私有会话及日志。内容和备份留在各自私有环境，不能上传仓库或转发完整消息/context/lease。保持同一 bot 单一 serve。

### 4.2 B：00 发布新库 refs，02 核对可取

00 回交新库地址/权限、保存两来源的可 fetch refs、完整 SHA、迁入历史与回退对象清单。目标仓库为空或只有默认分支不算就绪。02 从新库的独立干净验证目录读取两提交，核对对象及本文件哈希；保护现有未提交改动。不得用旧库对象的本地存在冒充新库可 fetch 成功。

阶段验收：新库实际 refs 与完整 SHA 对应；Muse 当前认证能读取新私有库，且未传递令牌；两个固定对象及必要回退历史可取。新库创建、refs 保存与 Muse 可访问分别留回执。来源切换前不得删除旧库，旧库收口由 00 完成。

### 4.3 C：手测结束后切换开发/拉取来源

条件：B 完成、用户当前测试窗口结束已记录、02 已保存 A 的版本结果。02 向 Muse 交接新地址与已验证固定 refs，不发空仓库或浮动“拉最新”。Muse 先在独立源码副本取两个完整 SHA；记录 fetch 来源、HEAD/dirty 与哈希。

先迁开发副本及部署配方的来源记录。现有运行包、实际 Hook、真实 prompt、状态路径在这一步仍保留。若后续要将已有源码目录的 `origin` 指向新库，先确认其用途、dirty、editable 安装关系、定时拉取/自动部署触发点；只改地址也可能触发自动流程，须确认不会越过既定测试窗口。更新后回读 fetch/push URL，后续推送只向新库；不得顺手 pull 到浮动 main。

阶段验收：Muse 开发副本和部署配方不再使用旧库作为获取/上传目标；回读来源和固定 SHA 一致；旧库地址仅作为历史出处/回退记录保留。地址迁移完成不代表运行升级完成。

### 4.4 D：需要升级运行代码时另开窗口

仅在旧版本测试封存、实际候选逐文件 QA 零 P1、新运行窗口和部署版本已协调后执行。若本轮证明实际已加载目标双源且无需升级，就保持运行，不为改仓库地址做无意义重装。

确需更新时：按已核身份停 Hook 调度、等待旧 worker 结束、受控停止唯一 serve；维护者保存本地代码/配置回退材料，运行状态只在停写时备份。公共包从固定 `b7c47be`（或新经审集成 SHA）安装，Hook/adapter/prompt 使用对应已审核来源。逐项核对真实目标、环境覆盖及文件哈希，再启动唯一 serve/Hook；保存新的启动身份、时间与首个 adapter 使用证据。

部署验收需三类材料：固定源码一致性、真实配置保存回读、进程加载/启动证据。新解释器的一次 import 只能证明该解释器此刻会找到的包，不能单独证明原 serve 已换包。重启后重新绑定测试窗口；离线 QA 和旧手机结果不自动转给新进程。

### 4.5 E：回退与旧库收口

回退只使用已核的上一套代码、Hook 和真实 prompt，在核对未知发送结果后受控启停。禁止用旧状态目录覆盖新状态、清队列/游标或删除 adapter 会话“解锁”；这些动作可能丢失事实或重复发送。

02 将新源码/部署引用、实际运行版本、回退来源和剩余旧库引用清单交 00。00 确认新库历史/refs、部署引用与回退路径齐全后再收口旧库。02 不自行删除旧库。

## 5. 单源部署检查器的评估与建议

5.1 **02 已安排显式双源检查的本地实现；本文件成稿时尚待候选完成和独立 QA，不用于现场。** 读取 `2b3e537` 的 `hooks/muse-deploy-check.py` 可见：唯一 `--source` / `--expected-head` 同时用于 Hook 脚本、adapter、prompt 与整个 Python 包；源码还必须干净。传 `2b3e537` 会使 `b7c47be` 安装包比较失败，传 `b7c47be` 又无法提供目标 adapter/Hook。拼一个未提交混合目录绕不过 source_clean；不能因此删检查或把两次失败报告拼成“18/18 PASS”。本轮 Muse 自报的 17/18 不等同于双源部署完整通过。

5.2 已对齐的候选参数方案：原 `--source` / `--expected-head` 持有 Hook/adapter/prompt；增加成对可选的 `--package-source` / `--package-expected-head`，两者都省略则公共包沿用原 source，保持单源兼容；只给半组必须拒绝。每个源分别验证仓库根、完整 SHA、dirty、文件归属。完整包文件集合对公共源，检查器版本也单列。报告提供两套 expected/observed SHA 与文件哈希，禁止虚构组合单一 SHA。具体可用性以最终候选 SHA 的 `--help` 和独立 QA 为准；本文没有运行新参数对现场作检查。

5.3 保留现有只读、文件大小限制、错误脱敏、隔离 import/禁网络和字节码、同目录 adapter 默认绑定策略。对于 `WECHAT_MUSE_ADAPTER` override 继续明确“不支持/未核”，除非新增专项有效路径校验；不能扩大为接受任意传入 adapter 路径。继续保留五项限制为 false：旧进程加载、平台激活、投递目标身份、有效环境 override、手机送达均不由检查器独自认证。

5.4 必要离线测试：合法双源；任一源错 SHA/dirty；公共包多/少 `.py`、某文件错 hash；真实 prompt 漂移；同目录 adapter 错配；单源旧命令兼容；双源半参数拒绝；输出不泄露配置正文/secret；所有检查失败均非零退出。测试必须用合成路径/配置，不从用户运行状态取样。

5.5 检查器升级前，可以由维护者按两独立干净快照生成显式逐文件对照清单，并保存原始结果。该清单只认证提供的文件与导出配置，仍需平台回读和实际进程证据。若后续改为经审单一集成 SHA，现单源模型可重新适用，但须在那个 SHA 上复验，不追认历史组合。

## 6. 回执模板

交回时填值或 `unknown`，时间一律注明 Asia/Shanghai；敏感明细仅留私有证据。

| 项目 | 需回交 |
| --- | --- |
| 新库就绪 | 00 通知时间、repo、ref→完整 SHA、可访问验证与历史保存结果 |
| 本轮测试冻结 | 起止时间、双源 SHA/dirty、实际进程身份、各用例自动/API/人工/手机状态 |
| 来源迁移 | Muse 新源码位置、fetch/push 来源、固定 SHA/dirty、引用检查范围与剩余项 |
| 实际运行 | 包解释器/路径/文件集合哈希、Hook/adapter/真实 prompt 哈希、平台回读、启动/重启证据 |
| QA | 固定版本、组合方式、自测及非实现者 QA；单列真实未测项 |
| 回退 | 旧代码/配置来源、私有备份核对；不回滚状态的处置说明 |
| 结论 | 分别写“新库 refs 已核”“来源切换完成”“运行版本已核”“手机验收通过/未测”，不合并为一个笼统完成状态 |
