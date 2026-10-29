# 003 公共实现与验收说明

本批以e804be792ef08e985f94d7536c2926a597391150为源码基线，新增附件目录、固定图片引用和多件回传。
公共接口见同目录ATTACHMENT-INBOX-V1.md。01不更改Muse Hook/adapter/prompt、不操作云端或现有账号；
最终还需00组合固定公共与02适配候选、非实现者验收、Muse复核。离线HTTP替身不证明平台已读图或手机收到。

## 原件和目录

附件先登记pending及原位置、预留单件最坏字节，再下载到此ID专属stage。
媒体保存协议解出的字节，不转码、缩放或解压；文件名作为元数据，磁盘对象用独立ID。
文件与目录分别持久：prepared阶段保存size/hash/目标键，fsync后发布，重启核同一记录完成原发布。
目录ready和图片槽ready同一SQLite事务；目录损坏或实体缺失明确失败，不返一个失效path。
同消息重放修复原记录，不重复原件；不同消息的相同字节仍分别保留来源。
恢复逐件处理prepared：通过私有普通文件及大小上限校验后，正式对象或stage大小/hash不合时标missing及prepared_file_invalid，保留原字节，不用stage覆盖已有坏对象。
已缺失正式对象且stage有效时继续原发布；两处均缺失记prepared_file_missing。
单件权限、链接、特殊文件或读取身份校验失败时保留prepared，recover以blocked计数并继续独立健康项及后续admission对账；不放行该材料。
目录/索引/SQLite损坏、写入失败或未知I/O仍明确失败并回滚，不能把所有异常当成可跳过的坏附件。
首次初始化必须可证明附件存储为空。缺索引而原件、stage或任意sidecar仍在，及有索引却缺必需目录时，
查询、接收和预览均明确阻断；不重建空来源目录，不移动或删除遗留内容。合法空布局与旧state的其它文件仍允许首次初始化。
attachments/catalog.json持久记录私有catalog_id，正文尚未下载的pending历史也不能因实体目录为空被重建。
旧无标记目录只在受控写/恢复时补同CID证据；RO只校验、不迁移。缺索引时还检查固定收件/journal中的明确附件引用，普通旧文字或media_paths不据此误判。

正常完整可读图标ready/readable；仅协议解出而无可读能力标ready/unavailable，内容校验程度另记。
损坏与解码资源不足区分，真正不同解密候选冲突拒绝，不按padding/评分选一个冒充原件。
HEIC原件按文件保存/取回；本批没有增加或假设HEIC编解码器。语音有媒体原件时按文件回传，不承诺原生语音气泡。
历史AES失败根因没有因此被证明。

原件暂存不对已有inode执行O_TRUNC或正文写入：先无截断核既有私有单链接partial，再用O_EXCL新建inode。
已有partial重试先创建同目录、按stage ID归属的私有0B辅助项，核名字/FD身份后替换固定.part，关闭旧FD，再写正文。
辅助项在切换前无明文；仅已登记pending/prepared的0B辅助项按暂存计量识别，非空/无归属仍记unknown，不自动清理。本stage重试拒绝非空/异常辅助项，不作失败留样。
切换前失败保留旧partial，切换后退出留下新空/partial供重试；文件fsync后复核固定stage与新FD及大小，再fsync持有的目录FD。
硬/软链接、特殊文件、非私有权限或显式身份替换在写入前拒绝，不通过chmod或截断来修复。

依据 [Apple open(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/open.2.html)、
[Python3.9 os.rename/replace](https://docs.python.org/3.9/library/os.html#os.replace)、
[Linux rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)。rename的原子性只指目录项切换，
不把fstat和rename组合成原子条件操作，也不隔离同UID主动攻击者；不承诺阻止其在检查后给新inode另建别名。

## 图片引用和标签

连续图片组在下载前就有原位置，失败第一张不会让第二张改号。
新文字固定snapshot、接收截止序列和成员；后来新图不能换掉对象。引用资料不加入历史任务的owned/receipt。
无效代次、过期、pending、丢失分别说明，不能退到旧图猜测。
每个新图片组在私有目录记录创建时position_limit，配置上/下调只影响之后新建的组；旧快照按协议硬上限16及原成员验证。
已overflow的旧组不补洞、不重排。无此私有字段的旧组保守保留已有布局：已overflow按现槽数锁定，未overflow按硬上限16；不声称恢复未知旧配置值。
02须用受控reference/get拿到所指材料，再调用实际可用的读图工具；公共返回ID/path/hash本身不算模型看过。

短同批文字可成有来源batch_text标签，长配文不会被硬塞入单个128字标签。
图片后另一条“这是房东图”的明确标签，通过活批label入口绑定该文字固定快照；不猜人物身份、不存近期全文。
目录保留有限标签及来源ID，后续本人新请求可query tag找回。明确标签在pending→ready过程中保留。
删除/移除/取消标签及后续分句的否定标记请求拒绝invalid_request，不增加删除接口。
“这张房东图是假的”这类描述不授予标注；“这是房东图”“这张是房东图”和明确标记命令仍可用。
明确命名命令内带引号的操作词或否定词可作为标签字面量（如标记为「不要的东西」）；只遮罩实际匹配的完整命名字面值，所有意图扫描共用该视图，引号外删除、否定或疑问仍拒绝。

## 回传与恢复

一次reply提交有序附件ID及配文，公共层只登记一个逻辑组。
配文每片、每附件、页状态说明均是独立稳定物理单元；新组放独立SQLite队列，旧sender不会忽略列表后误报完成。
全入口共享持久cap8：普通旧回复、长文片、配文、媒体和CLI send。该数字是项目本地策略。
本store的attachments/objects与attachments/staging均是受管原件命名空间，普通worker reply及CLI所选image/file/video不接收这些路径或指向它们的符号链接；原件必须走既有附件ID/group入口。
已有普通pending/sending及准备态重发也不能消费该原件；拆配文、认领、实际媒体调用/计账前重查，已拆配文通过父任务或reply_batches原记录一并阻断。
被阻断的普通任务/journal保持原样待核，不删队列、不猜新组、不迁移身份或自动继续，不把不确定发送改为proven_unsent；既有账本和hold不因此释放。
生成的合法普通PDF等仍用旧媒体接口。此检查只认识受管路径/符号链接目标，不声称防同UID硬链接、复制别名或任意本机文件读取。
纯路径预检先保留原始路径中的symlink/..顺序再resolve，结合两目录的祖先身份识别当前文件系统的大小写/目录别名；不构造catalog、不读正文。CLI在initialize、配文、预算及上传前拒绝，Muse前置直接复用同helper，不改Hook。
首次升级无法证明余额的旧context保守占满；等receiver登记新context，不能把未知旧余额初始化为0。
CLI send首次使用也先做同一受控迁移；任意手填context不能取得新额度。重复观察同一token不重置账本。
receiver观察到从未登记的token才可创建本地新预算；上游未承诺每条入件token必然不同，
cap8及上述准入都是本地政策，不能据此声称平台额度已刷新。
新组永不采用普通旧任务的“换最新context”分支，换话题不会续发旧附件。

reserved只预留；最终可见send前持久inflight。accepted不重复，unknown仍占预算且不得自动重发。
上传前/上传阶段的可证未发送失败可以返还；网络调用后超时或不明响应保留unknown，不猜平台是否已消耗。
重启把inflight收敛为unknown；prepared admission仅恢复既定发表，不重新执行业务。
原job仍1800秒；组保留进度，之后明确选择继续才用新请求与当前context建新job。
额度为0时登记parked job及组进度，不额外发超预算说明；多组先澄清，unknown组先核查。
组版本/type在实际消费、维护、恢复和缓存实例中均验证；未知值在claim、退款、原件获取和HTTP前拒绝，不能只依赖只读检查器。
组、job、reservation状态与payload/job-unit kind都按v1正向集合校验，终态释放另须正向安全job状态。
已知unknown是合法不确定状态，继续保护；未来或损坏枚举不能被归一化、误作安全终态或回退成普通文本。
当前文字只说“继续／取消这组”时，必须只有一个未完成候选；多组须逐字提供完整组ID或唯一ag_短码（至少8位十六进制）。
公共层查整个本人候选集，不从最近一页、模型传入ID或未固定的“第几组”猜选组。
取消前及事务提交前均核验当前lease/epoch，失权时取消状态与可证未发退款一起回滚；状态探查不先做维护提交。
原件hold的hash后提交、建组和分页也使用同一活权检查；分页过期维护纳入同一受保护事务。
跨库先前已获授权提交的hold/组保留journal保护，失权记录blocked不自动续发，不承诺多个SQLite和JSON全局原子。

已识别执行文件、安装包或宏类型的名称/MIME只触发一句固定下载提醒，附在原有配文中，重试不重复添加。
新组、旧媒体回复和CLI均使用实际文本/附件物理单元共享计账；无配文的风险文件也先占一条提醒额度，额度不足不越过提醒发文件。
此为有限元数据规则，不打开、执行、解包或作病毒检测；未命中规则不表示文件安全。

正常cancel或complete后的发送hold由唯一sender安全点补偿释放，重启后下一轮sender也会补扫已知终态。
发送器先核当前scope/session/epoch、全局pending/job与catalog/私有目录结构，再预读本轮pending_jobs已枚举集合的每个admission，缓存各自的context或已知v1局部阻断结果；本轮集合的全部格式/文件安全校验完成前，不允许其中任何job进入next/上传/发送。沿用原列表返回上限，不宣称一次扫描无限范围的队列。
单job journal缺件、未严格admitted/phase完成或request/job/group/scope/context连接不符时，仅记录阻断并跳过本轮。
不调用被阻断job的next/begin/settle，不更改其状态、预留、unknown、hold或journal；后续健康job仍须独立完整核验。
context连接只派生既有context_ref，不为坏journal注册新context。私有文件/根目录、JSON/schema、数据库结构、扫描权限及未知I/O错误仍上抛，不按单job故障吞掉。
未知journal schema/type、坏JSON/UTF8/非对象、bool schema都属于全局拒绝；admitted=1不等于True。新隔离动作不改坏job状态，不抹掉原有正常expiry maintenance。
活pending的canonical journal不作为终态hold释放依据；原可信startup recover_admissions可补完原intent，本轮不做live自动恢复或坏journal修复，不称被阻断组已送达。
须有匹配的原admission，账本证明无活job及reserved/inflight/unknown；只释放精确组的既有hold元数据，不读/新建hold或删除原件。
取消worker不直接释放正在上传读取中的组；sender退出该读取后再核终态。unknown、待续、orphan、错epoch继续保守保护，worker/snapshot/pin不受此释放影响。

## 容量与维护

实际有效配置来自私有state/attachment-policy.json，无配置取公共默认；坏配置不冒充默认生效。
默认单件100MiB、普通30天、总5GiB含pin、最近24h、模糊窗口900秒、最多16原位置。
capabilities.policy.ambiguity_seconds输出同源实际值；0关闭模糊关联，旧能力缺字段时保守澄清。
这些是可配置本地策略，不是微信或Muse限制。窗口以图片接收时间计，不以新文字时间重置。

接收准入使用实际fstatvfs可用字节，扣除所有未写预留和控制空间余量后再收正文。
默认control_reserve_bytes为129MiB，按两库各64MiB本地元数据预算加一次1MiB控制JSON计算，可配置。
它是防本应用继续吃尽空间的准入余量，不保证其他进程/文件系统损坏或真实硬满时仍可写；未新核云端可用容量。
quota不足只拒新附件正文，文字/控制仍走收件；数据库或I/O错误保留故障状态，不虚报已存。
若持久发送账本不可用，继续接收文字但暂停无法计账的新发送，不回退到内存0额度。
旧低级serve.run嵌入调用在从无磁盘session且从无新附件状态时保留兼容，附件能力不启用。
已存在session（包括坏session）或任一新库、启用标记、附件目录时，不能选此兼容分支。

GC认识两个新库和catalog相对对象键，附件候选/保护/预算单列；自有对象路径不把全部附件永远pin。
pin、有效快照、在用、待发和unknown保护。到期先列候选，尚存的本人原件可取并建立新hold。
新能力存在时apply固定拒绝attachment_delete_disabled。本批只有只读预览，无生产删除、权限修改、游标/队列/receipt清理或私人失败留样。
旧未知root/DB/sidecar仍阻断，不能用新目录名绕过安全判断。

## 证据边界与回退

全套入口为tests/run_offline.py及hooks/tests/run_offline.py；后者只验证本分支现有配套的兼容性，02最新适配由00组合后另验。
新增测试包含临时合成原件、真实SQLite、受控子进程硬退出与合成HTTP边缘；ENOSPC/decoder资源分类部分为注入，不能写成实盘耗尽或真实OOM。
Python3.8仅语法检查，实际解释器与每轮SHA在私有证据记录。失败原日志保留，不因后续PASS覆盖。
实际收件、模型已读、API接受、手机收到、持续多日运行分别验收，不能用离线PASS替代。

回退保留原件、新目录和账本，不恢复旧状态快照；存在新组未完成/unknown时须先冻结核对，旧sender不能接管它们。
原90/570、120/180、cap8、30min未改。原版、03/04、旧号、新收件箱目标、011/PEPE成果和主区历史dirty均保护。

## 第二轮集中修复：新请求前置与既有意图

新reply/continue先安全读取admission，只把真实缺件且无同request稳定hold/group/job视为fresh；坏JSON、未知格式/不安全路径或遗留发表证据不能当空记录覆盖。预检只读已存在ledger/catalog，不构造空库来证明fresh。
fresh整批先核所有ID/本人三元归属/状态，再nofollow-stat ready实体；不创建hold或读全量内容。本人历史received_epoch原件仍允许，当前worker/session/intent的epoch仍按原规则核验。continue预检保留busy/unknown与原expiry后果：过期inflight不能变确定未发，过期reserved可沿明确新continue续；只核未accepted附件的状态/实体，已accepted原件后来缺失不影响剩余材料继续；零余额仍合法parked，不新增提示例外。
预检完成后再次核authority，随后才尝试写prepared。真实acquire仍按原路径每原件hash一次；stat后变化或同长hash坏只能在该阶段确认，不承诺无race，也不为预检再全读一遍大文件。
从intent写入尝试开始，普通业务错误不再作为无副作用证据：非lease/epoch失败映射既有storage_invalid，由现Muse publishing规则转outcome_unknown/must_stop；可能已提交的journal/hold/group/page及预算全部保留，不删、覆写或盲返。lease/epoch沿原硬停与未admitted的blocked保护，不用旧worker失权破坏已admitted的原job。
既有valid同参意图不走fresh纠正：原group/job/context/不可变发表输入正向绑定后可返回同原job，无需因complete、原件后来missing或余额变化再acquire/hash；未完成阶段仍沿可信恢复，原authorized_at不刷新。异参/损坏或丢journal有持久证据保持闭锁，旧污染记录不自动修成fresh。显式startup恢复仍受scope/epoch及原normal expiry维护约束。
旧blocked ordinary原件任务可能继续留队供人工核对；本批不引入TTL、轮次、自动failed、队列/账本清理或新后台维护。
