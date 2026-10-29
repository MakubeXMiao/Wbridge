"""Explicit quoted naming literals and outside vetoes; real public calls only."""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime


class QuotedLabelTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RuntimeTests("runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.aid = self.f.file(mid="synthetic-quoted-label-original")

    def label(self, text, value):
        f = self.f
        row = f.receive(f.message(text=text))
        batch = f.batch()
        result = f.call(batch, "label", {"text_message_id": row["message_id"],
            "attachment_ids": [self.aid], "label": value})
        return row, batch, result

    def metadata(self):
        return runtime.catalog(self.f.store).query(self.f.scope)["items"][0]

    def test_actual_explicit_quoted_negative_words_are_literal_source_tags(self):
        examples = (("标记为「不要的东西」", "不要的东西"),
                    ("标注为“不是房东图”", "不是房东图"),
                    ('命名为"don\'t disturb"', "don't disturb"),
                    ("标记为「不要，\n删除」", "不要，\n删除"))
        for text, value in examples:
            with self.subTest(text=text):
                row, batch, result = self.label(text, value)
                metadata = self.f.success(result)["attachments"][0]
                self.assertIn({"value": value, "source": "user", "message_id": row["message_id"]}, metadata["tags"])
                self.assertEqual(metadata["original_name"], "SYNTHETIC-file.pdf")
                self.f.complete_plain(batch)

    def test_outside_negation_deletion_question_and_ambiguous_unquoted_stay_rejected(self):
        value = "不要的东西"
        examples = ("不要标记为「不要的东西」", "标记为「不要的东西」，不要标记",
                    "标记为「不要的东西」；取消", "删除「不要的东西」这个标签",
                    "标记为「不要的东西」？", "标记为不要的东西", "刚才提到「不要的东西」")
        for text in examples:
            with self.subTest(text=text):
                before = self.metadata()
                _, batch, result = self.label(text, value)
                self.f.error(result, "invalid_request")
                self.assertEqual(self.metadata(), before)
                self.f.complete_plain(batch)
        before = self.metadata()
        _, batch, result = self.label("标记为「不要，\n删除」，不要标记", "不要，\n删除")
        self.f.error(result, "invalid_request")
        self.assertEqual(self.metadata(), before)
        self.f.complete_plain(batch)

    def test_no_placeholder_or_selected_substring_can_forge_naming_authority(self):
        examples = (("标记为<literal-label>，房东图只是描述", "房东图"),
                    ("标记为\x00不要的东西", "不要的东西"),
                    ("标记为「不要的东西」", "不要"),
                    ("这张房东图是假的", "房东图"))
        for text, value in examples:
            with self.subTest(text=text):
                before = self.metadata()
                _, batch, result = self.label(text, value)
                self.f.error(result, "invalid_request")
                self.assertEqual(self.metadata(), before)
                self.f.complete_plain(batch)
        for text in ("这是房东图", "这张是房东图", "请把这张图标记为房东图"):
            _, batch, result = self.label(text, "房东图")
            self.assertTrue(result["ok"], result)
            self.f.complete_plain(batch)


if __name__ == "__main__":
    unittest.main()
