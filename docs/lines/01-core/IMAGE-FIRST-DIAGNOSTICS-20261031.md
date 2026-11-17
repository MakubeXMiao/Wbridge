# 001-IMAGE-FIRST 诊断有限增量（01/02同批）

固定基础运行源码1f0160bc42e016cb47c273c091575f296c3d9634；本分支父9ab987b260b9bb3a6b958c7ad7c17aa4943f8af1仅额外带公开测试夹具。用户已批准图片代码/合成测试，不含部署许可。

## Wire冻结

只向既有REASONS新增 `http_body_incomplete`。其余枚举、V1形状、类型和预算不变：顶层 `{version:1,items:list,truncated:bool}`；每项 `{item_index:int,source:str,outcome:str,downloaded_bytes:int|null,reason:str}`。item_index为0..999真整数；source沿用param/full_url/unknown；success当且仅当reason=ok；新增reason只能配failed。

新增原因指HTTP framing已证不完整：在无Transfer-Encoding、Content-Encoding缺失/identity且存在单一合法Content-Length的可比原始body域内，实收短于声明；或客户端明确抛IncompleteRead。缺长度、普通超时、AES/格式失败不凭猜测归此类。已证可比超长用既有http_body_read_failed，超过本地密文界仍ciphertext_too_large。

每item每source只输出**最终attempt摘要一条**，最多2source；最终成功为ok，失败为最后可证类别。downloaded_bytes是最终attempt实收长度，非多次累计；未知null，合法范围0..104857633。全体仍最多16项/扫描32项/JSON4096B，重复source拒绝/标truncated。保存失败仍将该source原success摘要回写write_failed，不能让前一次失败挤掉最终结果。

公共media.collect、serve收集/写入、media_diagnostics净化与02独立净化必须同批对齐；02 begin、refresh、reply-needs_refresh三个snapshot入口覆盖。旧V1记录保持可读；旧reader遇新reason会丢该项并truncated，不会自动映射unknown。此次推荐成套升级，不增加attempt_count、完整性/stage等wire字段。

## Stage仅为本地安全诊断

media_receive现有stage=http/decrypt、outcome=success/failed保留，scope=download_decrypt不表示文件已保存；新增本地request_attempt/retry_planned不进入wire。media_persist只在真实写/fsync后成功。接收索引阶段catalog_publish/index_acquire，发送阶段file_read/getuploadurl/cdn_upload/sendmessage/settle，以独立本地事件记录success/failed；业务回包失败、传输异常与settle异常分开，accepted仅API层。

所有stage/reason为固定有限值，状态/字节受限，不含URL/query/host/key/token、原响应/异常repr或正文。局部日志不授予重试、活权或发送权限；POST/sendmessage未知不自动重试，留样OFF、账本/unknown及V1全部上限不变。此冻结件是兼容增量，不是图片闭环已通过或现场已更新。
