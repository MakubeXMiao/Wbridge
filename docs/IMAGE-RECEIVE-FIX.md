# 图片接收诊断与兼容候选

工单：`WBR-01-20261023`。基线：`462337c34f70175e2ee2ddd2bd2a7b1ace402c57`。候选分支：`codex/media-receive-fix`。

本次用户在当前对话明确指定公共接收专项，实施另建分支，保留 `codex/codex`、`codex/core` 和 `codex/muse`。Muse 转述的历史失败次数与日志结论未在本机独立复核；旧样本参数缺失，本候选不能证明历史根因，也不能代替部署后的新图片验收。

## 改动范围

- `crypto.py`：新增固定图片魔数分类；补原字符串 MD5、raw 截断、CBC 前置 IV、CBC key IV 等派生策略。图片候选全部同时验证 PKCS7 和 JPEG/PNG/GIF/WEBP 魔数；头部不符时跳过全量解密，防止每种猜测重复处理大文件。
- `media.py`：显式图片模式、明文图片快速通道、每源结构化诊断、限幅字段名树；通用文件的既有 padding 直解行为保留。图片读取上界预留 PKCS7 与可能的 16 字节前置 IV，最终明文仍受原 100MB 上限约束，仍临时文件写入后原子替换。
- `serve.py`：只在图片接收分支记录字段树，向下载器传原始 `image_item.aeskey` 及显式图片模式。原始钥匙只用于当前内存中的派生，不入日志/账本；其他媒体仍沿用原调用。
- 原有媒体套件新增 18 项合成测试；接收套件假下载函数接受可选图片类型。发送函数、加密发送与原 key parser 的 AST 保持一致；队列、收据、游标、Hook、prompt 和 worker 协议未改。

派生属于待实测的兼容探测，不声明 CBC 或 raw-MD5 是微信官方协议。官方当前文档描述的常规上传格式为 AES-128-ECB/PKCS7；不能凭某个字段类型推定所有服务端行为。[腾讯协议](https://github.com/Tencent/openclaw-weixin/blob/main/docs/protocol.md)

## 新诊断

`media_receive` 后为单行 JSON。每个下载源结束时记录：

- `source` 仅 `param` / `full_url`，不含真实地址或 query。
- `stage` 为 `http` / `decrypt`；`bytes`、`block_aligned`、`magic`，HTTP 未取得数据时 bytes/alignment 为 null。每源独立初始化，后源不会沿用前源字节。
- `keys` 仅来源名、存在性/类型/长度/字符集类别；不输出钥匙、摘要或前缀。
- `attempted` 为固定派生/模式名；成功时另有 `strategy`、`plaintext_magic`。图片明文通道策略为 `plaintext-image`。
- `outcome` 只描述该源的下载/解密与大小检查，不证明文件已原子发布；后续 `已接收附件` 才说明接收函数已返回。异常只保留固定类别和可选 HTTP 状态码，不输出异常原文。

`image_item_fields` 只记录字段名路径和 key 字段的形态元数据。最多 64 个字段、5 层递归，数组只采首项，触限标 `truncated`。保留最长 64 字符的 ASCII 标识符字段名，包括新出现的合法字段；URL、控制字符、超长/非法名称及与已收集标量私有值同名的名称遮蔽。不记录字段值、正文、完整 URL、钥匙或原始密文。

## 验证与交付边界

新增用例覆盖：四种明文/加密图片头、padding 与魔数双条件拒绝、两种新增 CBC IV、AES-256 CBC、原字符串 MD5/raw 截断、两来源回退、HTTP 诊断独立性及脱敏、普通未知字段名可见/恶意名遮蔽、图片大小边界（含前置 IV）、原子失败保留旧文件与清临时文件、原始 item key 到接收落盘的路径。

使用原 `tests/run_offline.py` 分进程、阻断真实 socket、隔离状态运行全部 12 套；新增用例已纳入现有媒体套件，不增加另一套遗漏在入口外的测试。macOS 测试使用 `PYTHONDONTWRITEBYTECODE=1` 避免系统 Python 缓存干扰目录无写入断言。实际结果及候选 SHA 以工单交付回执为准。

本次只推送候选分支，不合并 main、不部署。交给 Muse 按最终完整 SHA 逐文件 QA，零 P1 后由授权部署方执行。部署时另记实际包/解释器、serve 加载的源码 SHA/重启证据与时间；保留现有登录、队列、账本、游标和 Hook。随后由用户发一张新测试图，核对上述诊断、真实落盘和手机侧结果。日志足以判定阶段和策略时再提出根因，缺证据保持待核，不将合成 PASS 写成线上已修复。
