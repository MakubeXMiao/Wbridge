# 附件收件箱 003：Muse Hook 投递目标保全检查

本说明只覆盖 02 的只读 checker 增量。R2 本批实施、独立 QA/Muse 代码复核及其后的私库/既有云端受控更新已获条件化授权；仍须按最终固定候选满足对应 QA、复核和现场部署 gate。本文不是运行验收回执。

1.1 本批实际部署和回滚检查必须传 `--preserve-hook-json PATH`，指向**更新前实际导出的单个 Hook JSON**。导出保存在云端受控私有路径，不提交 Git、不作为普通附件发回；不要从仓库占位模板构造，也不要覆盖该更新前副本。`--hook-json` 指向更新/回退后实际导出的同一 Hook。

1.2 新选项精确比较 Hook `id`、`delivery.surface`、`delivery.to`。更新前导出必须是单 Hook 定义，surface 为 `side_chat`，目标非空且非占位；缺失/损坏/错 Hook/不合法目标或任一字段不同均失败。即使两个目标都不是占位值，目标不同仍失败。合法更新改变 prompt 或其他设置不等于可以改变投递点。

1.3 命令仍要求既有参数；双源用法保持不变。以下路径和 SHA 是变量占位，现场使用经过核对的真实路径和完整 SHA，不含任何 target 值：

```sh
python3 -I -B hooks/muse-deploy-check.py \
  --source "$HOOK_SOURCE" --expected-head "$HOOK_SHA" \
  --package-source "$PACKAGE_SOURCE" --package-expected-head "$PACKAGE_SHA" \
  --hook-script "$EFFECTIVE_HOOK_SCRIPT" --adapter "$EFFECTIVE_ADAPTER" \
  --hook-json "$AFTER_HOOK_EXPORT" --preserve-hook-json "$BEFORE_HOOK_EXPORT" \
  --python "$EFFECTIVE_PYTHON"
```

1.4 单源时省去成对的 `--package-source/--package-expected-head`。旧调用不传保全选项仍能运行，但输出 `hook_target_preservation.requested=false`、`performed=false` 和“未进行前后目标保全比较”；这种结果不足以满足本批投递目标保全 gate。

2.1 checker 不输出真实 target、Hook id、导出内私有字段或原始错误文本；新增结果仅含固定状态/失败类别、文件 SHA256 与比较布尔值。它只读指定文件，不写导出、不创建缓存、不修正配置；不修改模板 `delivery.to`，不猜当前目标 ID。

2.2 文件前后一致只说明给定两个导出中的目标元组保持一致。更新前导出是否真实/足够新、当前目标名称确为“微信需求收件箱”、平台当前激活哪个定义、worker 实际采用哪个 prompt/adapter，必须在现场分别核对。checker 的非占位检查和 hash 不能证明这些事实，合成套通过也不能替代现场证据。

2.3 保全当前有效目标及 override；禁止把旧开发旁聊目标或仓库占位 JSON 整份导回现网。回滚也使用本窗口更新前保全的实际定义、实际加载文件/依赖及可恢复配置，不能用 pre-012 的历史快照代替当前回退材料。队列、游标、receipt、账本、会话和附件索引/原件不得以回滚名义清空或恢复旧状态。

3.1 对应合成验证入口：`python3 -B -m unittest discover -s hooks/tests -p 'test_muse_deploy_check.py' -v`。用例覆盖相同目标、两个不同非占位目标、更新前导出缺失/损坏/占位/错 surface/id、任意私有字段不回显、单源/双源兼容以及只读无缓存。实现者只交自测；独立 QA 由非实现者针对最终候选完成。
