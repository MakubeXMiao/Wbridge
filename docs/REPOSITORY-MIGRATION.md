# wbridge 独立仓库迁移

目标：`https://github.com/MakubeXMiao/wbridge`（私有）。日期：2026-10-24。

新库保留原源码历史与独立开发线，主工作区及现有工作树继续使用，不通过复制文件重建历史。原 shallow 历史已经补全至 `dfafff8`；本地运行状态、协调目录、凭据、登录信息、测试原文及虚拟环境不入库。

## 1. 首批版本映射

| 新库分支/标签 | 固定来源 | 用途 |
| --- | --- | --- |
| main | wbridge 导入基线及 00 协调文档 | 初始主线，不宣称多后端首版已完成 |
| codex/core | 2284d073201b6f26fd6714c18b62156a7d282b6c | 公共契约首轮候选，后续由 01 推进 |
| codex/muse | 2b3e53791d49962418bc4333be7cbee0a55fa265 | Muse 适配候选 |
| codex/codex | 34d12228cf09a8b777dde35648627a286150384f | Codex 能力取证，后续由 03 推进 |
| codex/claude-code | dcc58295e4f0a3932efa91c28d25d6f9cb142211 | CC 能力取证，后续由 04 推进 |
| codex/media-receive-review | b7c47bedf22c957b2df6980eefcca787b556436d | 媒体最终修复候选 |
| archive/media-receive-fix | bfa9995eba94e0733460c09a20eaeeade50f16ed | 原媒体候选，已被 b7c47be 复核修正，不作当前部署入口 |
| archive/muse-main | e1e72168ca8e1d2819df439cd3a1119aa4078eca | 原库 main 历史引用，不等于实机稳定版本 |
| archive/qa-reliability-20261022 | 462337c34f70175e2ee2ddd2bd2a7b1ace402c57 | wbridge 导入的修复起点 |
| wbridge-baseline-20261023 | 指向 5bc172585e9005df536281152a9ce1510f899c5b 的原 annotated tag | 保留初始基线标签对象 |

此表定义迁移对象；实际上传完成与否由 00 的远端 SHA 核对回执记录。开发线实施中的未提交文件和后续新提交不混入首批固定快照。

## 2. Muse 迁移责任与顺序

1. 00 创建新库、上传指定 refs、核对远端完整 SHA 后通知 02。
2. 02 将新库 URL 与固定版本交给 Muse，确认拉取权限和开发/部署脚本中的旧库引用；私有库权限缺失单列阻塞，不通过复制本机 token 解决。
3. 保留本轮手测的运行包、Hook、真实 prompt、状态目录和单 bot 服务。切换代码来源与替换运行版本分开记录，不在手测中途静默重启。
4. 后续首版集成形成单一可复现候选。对双来源部署，公共包与 Muse Hook/prompt 分别核对，不能用单源检查器假报全通过。
5. 新库历史/refs、Muse 拉取与部署引用、回退路径均核验后，再处理用户提出的旧库删除；此前保留旧库，不持续向旧库开发分支推送。

## 3. 交付口径

分支迁移、候选 QA、运行部署、真实手机验收分别记录。新库中保留一个分支不代表该分支已集成，也不代表新版本已部署。CC 的本机认证和微信真实验收仍待协作者环境。
