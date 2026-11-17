# 图片首包实施边界

用户授权WBR-00-20261030-001 /001-IMAGE-FIRST。直接基底9ab987b（运行实现同1f0160b；只另有已验公开测试夹具），原件/catalog/身份/账本不重建。719020a先冻结唯一新wire原因http_body_incomplete，02经00并行镜像，本文不增wire字段或放宽预算。

## 下载恢复策略及理由

仅expected_type=image启用可信长度门。严格单个原始Content-Length（0..2^63-1）、无Transfer-Encoding、Content-Encoding缺失或严格identity时比较；不使用诊断层的宽松头分类当可信证据。短体在AES/原样图片fallback/落盘前失败；可观测超长归现有HTTP读失败，本地密文超限仍原类别。缺/坏/重复/编码不可比不谎报short，沿原内容验证兼容行为；不宣称未知长度的EOF就证明资源完整。

每来源最多2次GET，只对已证短体/IncompleteRead补一次fresh GET。2是“让一次不完整响应后至少有一次恢复机会”的最小本地尝试数，不是平台限额或统计最优。原来源顺序保留；不拼接旧短体，不对普通AES/格式/超限/取消错误再试。原唯一transfer与validator对象/累计限额保留，capture只对终局失败一次，默认OFF。

每来源全部尝试共用原该来源的urlopen时长设置（原件60秒、旧调用120秒）作为monotonic软预算；重试只在剩余>0时启动，urlopen接剩余值，读循环在检查点核预算，**不另加sleep或新时间常量**。这些沿用既有配置限制额外工作，不证明DNS/慢滴漏有硬总截止；最多两来源各预算，不把receiver发布inbox前下载套上worker90/570或Hook期限。OperationAborted/epoch在真实工作检查点透传，重试不靠timer续租。

每source只collect最终结果，downloaded_bytes为最后attempt而非重下累计；本地V2另有request_attempt/retry_planned。source_attempt仍表示来源序号；保存失败仍回写共享摘要write_failed。成功下载解密与文件持久化、catalog发布、索引acquire分别记录。

## 发送与状态保护

新增有限本地file_read/getuploadurl/cdn_upload/sendmessage/settle诊断，异常转换为安全MediaSendError，不携带原响应、URL、query、token/key或异常repr。404回退读机器HTTP状态，保持原上传尝试/回退次数、取址POST和sendmessage单次；不增加结果未知的发送重试。失败响应只保留有限整型ret/errcode供token状态与现有账本判定，结果回调仍恰一次；API接受和durable settle分开，异常不擦除已accepted，不给unknown退款。

receive沿现register→download→stage/fsync→publish→acquire；当前本人沿query/get/reply→admission→ledger→sender。仅增加安全阶段定位；prepared恢复、ready重放不重下、scope/epoch/lease、holds、原字节与100MiB界、唯一receiver均保留。

新3套定点测试登记到离线入口；本线只运行受影响选择集，旧fixture/未改模块的有效证据按字节范围复用。自测不等于非作者组合QA或手机收到；最终固定SHA、内容hash和运行证据见共享协调交接。未授权构包/推送/部署/手机试验，未做GC、fullbegin、launcher或监督重构。回滚保留原件、索引、cursor/queue/holds/ledger；新reason旧reader会丢项并truncated，需匹配adapter版本，不清状态迁就回滚。
