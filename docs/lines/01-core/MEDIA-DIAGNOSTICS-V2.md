# 公共媒体诊断与失败样本取证

本文记录005/A历史候选的范围。012新增完整图片校验、关闭态留样与维护机制，当前行为及资源/激活边界见 [012公共修复](MUSE-PUBLIC-REPAIR-012.md)；不以新代码倒填旧版本已具备留样能力。

任务 `WBR-00-20261024-005/A`。01 负责公共下载诊断；02 负责 Muse 云端取证与真实验收；00 负责最终集成及部署安排。

## 1. 已知现象与本次范围

02 回读的历史组合为公共 `b7c47bedf22c957b2df6980eefcca787b556436d` 加 Hook `2b3e53791d49962418bc4333be7cbee0a55fa265`，历史进程加载链仍不完整。三图两败一成：其中 50,380 字节响应不整块、没有进入图片解密策略；61,072 字节响应整块但未命中；107,072 字节成功对照命中 `media:b64-hex32-ECB`。失败原体与 HTTP 头没有保存，真实加密根因仍为 **unknown**。

本次补足逐来源的 HTTP 元信息、失败分类和落盘结果。下载顺序、AES 候选及选择、明文回退、大小上限、120 秒 HTTP 超时和原子替换方式保持原有行为。没有自动解压、额外下载、重试或原体留存；不改 Hook、90/570 租约、队列或游标。

合成回归可证明诊断字段和边界行为；不能重建已丢失的历史响应，也不能把新诊断通过写成两张失败图片已修好。

## 2. 日志语义

`media_receive` 的 `diagnostic_version=2`；一次 `download_media` 生成一个随机 `transfer_id`，两源共享，不含消息、账号或 URL 的派生信息。`source` 为固定 `param/full_url`，`source_attempt` 从 1 开始，`source_count` 为可用源数。

| 字段 | 含义与边界 |
| --- | --- |
| `http_status` | 成功或 HTTPError 的整数状态，限 100～599；缺失或不合法为 null |
| `content_type` | 固定 MIME 类别，或 missing/invalid/other；参数和任意原值不输出 |
| `content_encoding` | identity/gzip/deflate/br，或 missing/invalid/other；仅分类，不解压 |
| `content_length` / `content_length_state` | 有界非负整数或 null；状态 valid/missing/invalid，拒绝超长、非 ASCII 数字和超过 2^63−1 的值 |
| `final_host_relation` | 最终与请求主机比较为 same/different/unknown；不输出主机、路径、query 或摘要 |
| `final_url_changed` | 最终 URL 是否变化，无法读取时 null；不代表已记录重定向次数或中间地址 |
| `bytes` / `block_remainder` | 实际 `read(limit+1)` 返回的字节数和除 16 余数；读取抛错时 null，不检查异常中的私有 partial 内容 |
| `body_length_matches_content_length` | 声明长度合法时与已读长度比较，否则 null；不一致本身不证明截断原因 |
| `magic` / `magic_scope` | HTTP 原体固定魔数分类，scope 恒为 http_response_body；**不是逐策略解密后的魔数** |
| `plaintext_magic` | 成功候选明文的固定魔数分类，与原体 magic 分别记录 |
| `attempted` / `attempted_count` | 进入的固定本地策略名称；列表最多 128 项，另有 attempted_truncated；进入策略不等于完整解密或结构验证成功 |
| `stage` / `failure_reason` | 保留 http/decrypt 阶段，并用固定原因区分请求/HTTP、读体、密文过大、空体、非整块、未命中、歧义和明文过大 |
| `elapsed_ms` | 每源单调计时，非负且最多 3,600,000 毫秒 |

元信息缺失、getter 抛错或恶意/超长 header 不应使本可成功的下载失败。HTTPError 仅提取同样的安全类别并关闭响应，不读取错误正文或打印异常。日志不含 URL、key 值/摘要、原体/明文前缀、任意 header 值或完整异常。

`media_receive.outcome_scope=download_decrypt`：success 只表示该源下载、解密和大小检查通过。随后独立 `media_persist` 以同一 transfer_id 记录原子落盘 success/failed；失败仅输出固定 `write_failed`，不输出路径/异常。原子替换前的失败保持旧文件并尝试清理临时文件，调用仍抛出原错误。附件落盘不等于模型读取、微信回复 API 接受或手机收到。

## 3. 给 02 的最小云端取样步骤

以下是下一次受控验证的取样要求，当前候选**没有自动保存原体、密钥或下载地址的开关**。历史失败文件不存在，不能用重新下载的结果倒填原案。

1. 先由 00/02 安排验证窗口及经审候选，记录完整 SHA/dirty、解释器与实际包路径、唯一 serve 的新启动/加载证据；Hook/prompt 单独绑定版本。不要用整支 01 覆盖 Muse 部署，不启动第二个同 bot 收件服务。
2. 首选由用户发送不含私人信息的合成图片。先用新日志记录两源的 transfer_id、来源顺序、安全 HTTP 类别、长度、余数及结果。仅诊断元数据即可解释的问题不收集原始图片。
3. 确有失败且需要原体时，取样插桩须由 01/06 先审查并由 02 在该窗口执行；从**同一次实际下载响应**复制，不为取样另发请求。当前版本不具备该插桩，实施前不得声称原体已取得。限定最多 3 次传输、每次最多 2 源、每源 1 MiB，总计最多 6 MiB；超过上限只标 `truncated/not_replayable`，不能把截取体当完整失败样本。
4. 原体及回放所需的两处 key、source、expected_type 只保存在云端仓库外的专用私有目录（0700，文件0600），用随机样本名。无需保存签名 URL、bot token、context、完整消息或队列。原体与 key 分开存，维护者在本机核对权限；不以聊天、Git、公开日志或命令行参数传递原值。
5. 私有 manifest 可记录原体 SHA256、实际长度、是否完整、采集时间与版本以及安全 HTTP 字段，仅供原地完整性校验。离线回放使用已保存体和原 key，mock HTTP、禁用真实 socket，不重新下载。聊天汇报仅给随机样本 ID、安全字段和结论；原始 manifest、体与 key 保留在原宿主。
6. 默认关闭取样；窗口结束立即停用，由 02 在完成核验后 24 小时内按约定清理取样副本。若需跨宿主传递、扩大件数/大小、私人图片或延长留存，应先取得该具体范围授权。取样失败、来源不明或仍缺原体，结论继续 unknown。

02 回交：新候选/启动绑定、合成用例时间、逐源安全日志、样本是否真实取得/完整/可回放、原地回放结果，以及 API 接受和手机实收各自状态。原案旧证据、新诊断验证和真实图片修复验收分开记录。

## 4. 本地合成验证

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/test_media_http_diagnostics.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python tests/run_offline.py --output-dir qa-results/media-diagnostics
```

覆盖：200 响应分类、缺失/恶意元信息、读体失败、HTTPError、长度与大小边界、50380/61072 合成无命中、加密 PNG 的两种 magic、双源关联与回退、落盘失败和敏感值不泄漏。真实 HTTP/失败密文、模型及手机验收不在离线结果内。
