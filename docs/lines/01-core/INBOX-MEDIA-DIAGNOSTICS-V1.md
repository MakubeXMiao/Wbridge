# 可选 inbox 媒体诊断 V1

任务 WBR-00-20261024-012，固定 b43 基线的加法契约。01 产生与净化；02 在 begin/refresh/needs_refresh 各入口独立净化嵌套对象，并按净化结果计算 digest。只在内部 inbox/适配消息中传递，不自动写入普通聊天或私有 session 正文。

可选顶层字段 `media_diagnostics`：

```json
{"version":1,"items":[{"item_index":0,"source":"param","outcome":"failed","downloaded_bytes":50380,"reason":"ciphertext_not_block_aligned"}],"truncated":false}
```

1.1 对象只输出 version/items/truncated；version 必须为真整数 1，items 必须是列表，truncated 必须是真布尔。字段缺失、null、类型错误或未知版本：丢弃整个可选诊断，保留 text/media_paths/media_failed/count 等原业务字段。旧记录没有该字段时行为不变。

1.2 items 最多保留 16 条，每个原 item_list 的 item_index 最多保留 2 个不同 source；只检查输入列表前 32 条，保持原顺序。超过条数、来源重复、坏子项或字节预算时丢弃相应子项并将 truncated 设 true。空列表允许。序列化采用 UTF-8 紧凑 JSON，整个对象至多 4096 字节。

1.3 每条只输出 item_index/source/outcome/downloaded_bytes/reason，未知属性全部剥离，不遍历未知嵌套值。item_index 为原 item_list 索引，真整数 0..999；downloaded_bytes 为实际一次 read 返回长度，真整数 0..104857633 或 null，拒绝 bool/float/负数/越界。它不是像素、解密后尺寸或下载完成证明。未知长度用 null，不猜。

1.4 source：param/full_url/unknown；outcome：success/failed；reason：ok/request_or_http_error/http_body_read_failed/ciphertext_too_large/empty_body/ciphertext_not_block_aligned/decrypt_no_candidate/decrypt_ambiguous/plaintext_too_large/image_invalid/image_resource_limit/image_validation_timeout/write_failed/unknown。成功须 reason=ok，失败须 reason 非 ok；无法分类时用 unknown，不放异常原文。source=unknown 只作无法确定来源的本地诊断，不表示第三次下载。

1.5 首源失败、第二源成功保留两条，附件计为成功；两个源均失败时 media_failed_count 仅增加一个附件失败。不同附件的成功与失败可共存。诊断截断/丢弃不能改变原附件失败总数或消息顺序。落盘失败将对应最后成功来源的可选记录改为 failed/write_failed，不表示已保存附件。

1.6 只收公共 downloader 的本地固定字段，不从上游消息透传同名字段，不复制完整 media_receive 日志。禁止 URL、host、query、key/摘要、原体、路径、账号、context、正文、HTTP 原始属性或任意异常进入此结构。02 白名单不能只加入字段名；必须按此嵌套规则净化后再 snapshot/digest。

验证：旧记录；容器 null/坏类型/未知版本；坏子项与恶意敏感扩展；int/bool/float/边界；扫描/字节/同 item 两源界限；混合成功失败及首败次成。该结构提供有界定位信息，不证明真实 AES 根因、模型读取或手机送达。回退旧版时可忽略该可选字段，不迁移队列、receipt 或游标。
