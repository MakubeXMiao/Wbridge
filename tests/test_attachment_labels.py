"""003 literal-user labels through the real public helper, offline synthetic data.

Only fixture methods are reused from the frozen runtime suite.  Its test class
is not inherited or exposed to discovery, so this file executes only label cases.
"""
import copy
import json
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime, media, serve, worker_state
from wechat_bridge.store import atomic_write_json, load_json


class LabelTests(unittest.TestCase):
    setUp = fixtures.RuntimeTests.setUp
    message = fixtures.RuntimeTests.message
    transport = fixtures.RuntimeTests.transport
    receive = fixtures.RuntimeTests.receive
    file = fixtures.RuntimeTests.file
    batch = fixtures.RuntimeTests.batch
    call = fixtures.RuntimeTests.call
    success = fixtures.RuntimeTests.success
    error = fixtures.RuntimeTests.error
    complete_plain = fixtures.RuntimeTests.complete_plain

    def images_then_text(self, text="这是房东图", count=1, prefix="synthetic-label"):
        image = self.receive(self.message(prefix + "-images",
            attachments=[("image", fixtures.png(), None) for _ in range(count)]))
        message = self.receive(self.message(prefix + "-text", text=text))
        batch = self.batch()
        return image, message, batch

    def label_args(self, message, label="房东图", ordinals=None):
        return {"text_message_id": message["message_id"], "label": label,
                "snapshot_id": message["reference_context"]["snapshot_id"],
                "ordinals": [1] if ordinals is None else ordinals}

    def metadata(self, attachment_id):
        rows = runtime.catalog(self.store).query(self.scope, limit=100)["items"]
        return next(row for row in rows if row["attachment_id"] == attachment_id)

    def test_label_feature_is_advertised_by_actual_capabilities(self):
        self.assertIn("attachment_labels_v1", runtime.capabilities(str(self.base))["features"])

    def test_later_literal_text_labels_fixed_snapshot_and_evening_query_finds_original(self):
        image, text, batch = self.images_then_text()
        attachment_id = image["attachment_refs"][0]["attachment_id"]
        claims_before = copy.deepcopy(self.worker._claims())
        result = self.success(self.call(batch, "label", self.label_args(text)))
        self.assertEqual([row["attachment_id"] for row in result["attachments"]], [attachment_id])
        self.assertIn({"value": "房东图", "source": "user", "message_id": text["message_id"]},
                      result["attachments"][0]["tags"])
        self.assertEqual(self.worker._claims(), claims_before)
        self.assertTrue((self.base / "inbox" / (text["message_id"] + ".json")).exists())
        self.complete_plain(batch)
        self.assertFalse((self.base / "inbox" / (image["message_id"] + ".json")).exists())
        self.assertFalse((self.base / "inbox" / (text["message_id"] + ".json")).exists())
        # The evening request is a new worker/batch.  Its ability to retrieve
        # the original cannot depend on the already-deleted old inbox body.
        self.now += 6 * 3600
        with patch.object(serve.time, "time", return_value=self.now):
            evening = self.receive(self.message("synthetic-evening", text="把中午房东的附件发我"))
        self.worker = worker_state.WorkerState(str(self.base), str(self.home / "hook"), lambda: self.now)
        self.addCleanup(self.worker.store.close)
        current = self.batch()
        self.assertEqual(current["owned"], [evening["message_id"]])
        found = self.success(self.call(current, "query", {"filters": {"tag": "房东"}}))
        self.assertEqual([row["attachment_id"] for row in found["items"]], [attachment_id])
        material = self.success(self.call(current, "get", {"attachment_id": attachment_id}))["material"]
        self.assertEqual(Path(material["path"]).read_bytes(), fixtures.png())
        self.assertEqual(load_json(str(self.base / "state/received" / (image["message_id"] + ".json")))["status"], "complete")

    def test_long_caption_does_not_become_truncated_tag_or_prevent_save(self):
        caption = "SYNTHETIC 配文 " + "这是长配文。" * 500
        self.assertGreater(len(caption), 3000)
        message = self.receive(self.message("synthetic-long-caption", text=caption,
            attachments=[("image", fixtures.png(), None)]))
        attachment_id = message["attachment_refs"][0]["attachment_id"]
        metadata = self.metadata(attachment_id)
        self.assertEqual(metadata["status"], "ready")
        self.assertEqual(metadata["preview_state"], "readable")
        self.assertFalse(any(tag["source"] == "batch_text" for tag in metadata["tags"]))
        self.assertEqual(Path(message["media_paths"][0]).read_bytes(), fixtures.png())

    def test_pending_image_can_be_labeled_then_publish_preserves_user_tag(self):
        pending = self.message("synthetic-pending-label-image", attachments=[("image", fixtures.png(), None)])
        entered, release = threading.Event(), threading.Event()
        result = []
        def blocked(request, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError("synthetic label barrier timeout")
            return self.transport(request, **kwargs)
        def download_pending():
            try:
                result.append(serve.handle_incoming(self.store, pending, fixtures.OWNER))
            except BaseException as error:
                result.append(error)
        with patch.object(media.urllib.request, "urlopen", side_effect=blocked):
            thread = threading.Thread(target=download_pending)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                text = self.receive(self.message("synthetic-label-before-publish", text="这是房东图"))
                batch = self.batch()
                applied = self.success(self.call(batch, "label", self.label_args(text)))
                self.assertEqual(applied["attachments"][0]["status"], "pending")
                attachment_id = applied["attachments"][0]["attachment_id"]
                self.assertNotIn(pending["message_id"], batch["owned"])
                self.assertIn({"value": "房东图", "source": "user", "message_id": text["message_id"]},
                              applied["attachments"][0]["tags"])
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [True])
        refreshed = self.worker.refresh(batch["lease"])
        query = self.success(self.call(refreshed, "query", {"filters": {"tag": "房东"}}))
        self.assertEqual(query["items"][0]["attachment_id"], attachment_id)
        self.assertEqual(query["items"][0]["status"], "ready")
        self.assertEqual(query["items"][0]["tags"].count(
            {"value": "房东图", "source": "user", "message_id": text["message_id"]}), 1)

    def test_repeated_literal_label_is_idempotent_and_has_actual_user_source(self):
        image, text, batch = self.images_then_text()
        attachment_id = image["attachment_refs"][0]["attachment_id"]
        first = self.success(self.call(batch, "label", self.label_args(text)))
        second = self.success(self.call(batch, "label", self.label_args(text)))
        self.assertEqual(first, second)
        tags = self.metadata(attachment_id)["tags"]
        self.assertEqual(tags.count({"value": "房东图", "source": "user", "message_id": text["message_id"]}), 1)
        self.assertEqual(self.worker._receipt(text["message_id"])["status"], "received")

    def test_full_user_tags_do_not_make_pending_original_fail_automatic_caption_tag(self):
        pending = self.message("synthetic-full-tags-image", text="SYNTHETIC caption",
            attachments=[("image", fixtures.png(), None)])
        entered, release = threading.Event(), threading.Event()
        answers = []
        def blocked(request, **kwargs):
            entered.set()
            if not release.wait(15):
                raise AssertionError("synthetic full-tag barrier timeout")
            return self.transport(request, **kwargs)
        def download_pending():
            try:
                answers.append(serve.handle_incoming(self.store, pending, fixtures.OWNER))
            except BaseException as error:
                answers.append(error)
        with patch.object(media.urllib.request, "urlopen", side_effect=blocked):
            thread = threading.Thread(target=download_pending)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                labels = ["标签%02d" % value for value in range(32)]
                text = self.receive(self.message("synthetic-fill-pending-labels",
                    text="请把选定的图片标记为 " + "、".join(labels)))
                batch = self.batch()
                for label in labels:
                    result = self.success(self.call(batch, "label", self.label_args(text, label=label)))
                aid = result["attachments"][0]["attachment_id"]
                self.assertEqual(result["attachments"][0]["status"], "pending")
                self.assertEqual(len(result["attachments"][0]["tags"]), 32)
            finally:
                release.set()
                thread.join(15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(answers, [True])
        metadata = self.metadata(aid)
        self.assertEqual(metadata["status"], "ready")
        self.assertEqual(len(metadata["tags"]), 32)
        self.assertTrue(all(tag["source"] == "user" for tag in metadata["tags"]))
        material = runtime.catalog(self.store).acquire(self.scope, aid, "worker:synthetic-check-original")
        self.assertEqual(Path(material["path"]).read_bytes(), fixtures.png())

    def test_negated_or_question_text_is_not_clear_label_authority(self):
        image = self.receive(self.message("synthetic-denial-image", attachments=[("image", fixtures.png(), None)]))
        attachment_id = image["attachment_refs"][0]["attachment_id"]
        for index, words in enumerate(("这不是房东图", "不要标记为房东图", "这是房东图吗？", "房东图？")):
            text = self.receive(self.message("synthetic-denial-%d" % index, text=words))
            batch = self.batch()
            self.error(self.call(batch, "label", self.label_args(text)), "invalid_request")
            self.assertFalse(any(tag["source"] == "user" for tag in self.metadata(attachment_id)["tags"]))
            self.complete_plain(batch)

    def test_removal_or_cancel_label_language_never_appends_a_tag(self):
        image = self.receive(self.message("synthetic-remove-image", attachments=[("image", fixtures.png(), None)]))
        aid = image["attachment_refs"][0]["attachment_id"]
        words = ("去掉房东图这个标签", "删除房东图这个标签", "移除房东图这个标签",
                 "清除房东图这个标签", "取消房东图这个标签", "撤销房东图这个标记",
                 "房东图这个标签，去掉", "房东图这个标签。取消", "标记为房东图，然后删除标签",
                 "这是房东图，不要标记", "这是房东图，别加标签", "给这张图加上房东图标签，不要",
                 "remove 房东图标签", "delete 房东图标签", "clear 房东图标签", "cancel 房东图标签")
        for index, phrase in enumerate(words):
            with self.subTest(phrase=phrase):
                text = self.receive(self.message("synthetic-remove-%d" % index, text=phrase))
                batch = self.batch()
                claims = copy.deepcopy(self.worker._claims())
                before = self.metadata(aid)
                self.error(self.call(batch, "label", self.label_args(text)), "invalid_request")
                self.assertEqual(self.metadata(aid), before)
                self.assertEqual(self.worker._claims(), claims)
                self.assertEqual(self.worker._receipt(text["message_id"])["status"], "received")
                self.complete_plain(batch)

    def test_description_with_label_before_is_does_not_authorize_label(self):
        image = self.receive(self.message("synthetic-description-image", attachments=[("image", fixtures.png(), None)]))
        aid = image["attachment_refs"][0]["attachment_id"]
        for index, phrase in enumerate(("这张房东图是假的", "房东图的标记是错的", "房东图标签是旧的", "房东图叫错了")):
            with self.subTest(phrase=phrase):
                text = self.receive(self.message("synthetic-description-%d" % index, text=phrase))
                batch = self.batch()
                self.error(self.call(batch, "label", self.label_args(text)), "invalid_request")
                self.assertEqual(self.metadata(aid)["tags"], [])
                self.complete_plain(batch)

    def test_positive_identification_and_quoted_action_words_are_literal_labels(self):
        image = self.receive(self.message("synthetic-positive-label-image", attachments=[("image", fixtures.png(), None)]))
        aid = image["attachment_refs"][0]["attachment_id"]
        examples = (("这是房东图", "房东图"), ("这张是房东图", "房东图"),
                    ("这不是房客图，这是房东图", "房东图"), ("请把这张图标记为房东图", "房东图"),
                    ("给这张图加上房东图标签", "房东图"), ("请添加房东图这个标签", "房东图"),
                    ("给这张图打上房东图标签", "房东图"), ("请贴上“房东图”标签", "房东图"),
                    ("请标记为「取消订单」", "取消订单"),
                    ("标注为“删除记录”", "删除记录"), ('命名为"移除记录"', "移除记录"))
        for index, (phrase, label) in enumerate(examples):
            with self.subTest(phrase=phrase):
                text = self.receive(self.message("synthetic-positive-label-%d" % index, text=phrase))
                batch = self.batch()
                result = self.success(self.call(batch, "label", self.label_args(text, label=label)))
                self.assertIn({"value": label, "source": "user", "message_id": text["message_id"]},
                              result["attachments"][0]["tags"])
                self.complete_plain(batch)
        for index, phrase in enumerate(("去掉「取消订单」这个标签", "删除“删除记录”这个标签",
                                        "请标记为「取消订单」，取消", "不要标记为「取消订单」")):
            with self.subTest(phrase=phrase):
                label = "删除记录" if "删除记录" in phrase else "取消订单"
                text = self.receive(self.message("synthetic-quoted-remove-%d" % index, text=phrase))
                batch = self.batch()
                before = self.metadata(aid)
                self.error(self.call(batch, "label", self.label_args(text, label=label)), "invalid_request")
                self.assertEqual(self.metadata(aid), before)
                self.complete_plain(batch)

    def test_label_must_be_literal_and_cannot_supply_a_forged_source(self):
        _, text, batch = self.images_then_text("这是房客图")
        self.error(self.call(batch, "label", self.label_args(text, label="房东图")), "invalid_request")
        wrong = self.label_args(text, label="房客图")
        wrong["source"] = "user"
        self.error(self.call(batch, "label", wrong), "invalid_request")
        for value in ("", "x" * 129, True):
            self.error(self.call(batch, "label", self.label_args(text, label=value)), "invalid_request")

    def test_target_modes_are_exclusive_and_ordinals_are_original_integers(self):
        image, text, batch = self.images_then_text(count=2)
        both = self.label_args(text)
        both["attachment_ids"] = [image["attachment_refs"][0]["attachment_id"]]
        self.error(self.call(batch, "label", both), "invalid_request")
        for ordinals in ([True], [1, 1], [], [17]):
            self.error(self.call(batch, "label", self.label_args(text, ordinals=ordinals)), "invalid_request")
        self.assertFalse(any(tag["source"] == "user" for tag in self.metadata(image["attachment_refs"][0]["attachment_id"])["tags"]))

    def test_new_arrival_requires_refresh_before_any_label_mutation(self):
        image, text, batch = self.images_then_text()
        aid = image["attachment_refs"][0]["attachment_id"]
        self.receive(self.message("synthetic-extra-after-label-request", text="SYNTHETIC more"))
        rejected = self.call(batch, "label", self.label_args(text))
        self.error(rejected, "needs_refresh")
        self.assertTrue(rejected["error"]["refresh_required"])
        self.assertEqual(self.metadata(aid)["tags"], [])
        current = self.worker.refresh(batch["lease"])
        self.success(self.call(current, "label", self.label_args(text)))
        self.assertTrue(self.metadata(aid)["tags"])

    def test_expired_worker_and_changed_epoch_cannot_label(self):
        image, text, batch = self.images_then_text()
        aid = image["attachment_refs"][0]["attachment_id"]
        session = load_json(self.store.session_path)
        session["login_epoch"] = "d" * 32
        atomic_write_json(self.store.session_path, session)
        self.error(self.call(batch, "label", self.label_args(text)), "epoch_mismatch")
        self.assertEqual(self.metadata(aid)["tags"], [])
        self.now += worker_state.WORKER_LEASE_SECONDS
        self.error(self.call(batch, "label", self.label_args(text)), "lease_lost")
        self.assertEqual(self.metadata(aid)["tags"], [])

    def test_fixed_snapshot_cannot_silently_label_a_later_image(self):
        original, text, _ = self.images_then_text()
        later = self.receive(self.message("synthetic-later-label-image", attachments=[("image", fixtures.png("blue"), None)]))
        batch = self.worker.refresh(self.worker._claims()[text["message_id"]]["lease"])
        result = self.success(self.call(batch, "label", self.label_args(text)))
        original_id = original["attachment_refs"][0]["attachment_id"]
        later_id = later["attachment_refs"][0]["attachment_id"]
        self.assertEqual([item["attachment_id"] for item in result["attachments"]], [original_id])
        self.assertEqual(self.metadata(later_id)["tags"], [])

    def test_explicit_selected_historical_id_can_receive_current_literal_label(self):
        aid = self.file(mid="synthetic-historical-file")
        old = self.batch()
        self.complete_plain(old)
        text = self.receive(self.message("synthetic-label-history", text="请把选中的旧文件标记为房东"))
        current = self.batch()
        self.assertNotIn("synthetic-historical-file", current["owned"])
        labeled = self.success(self.call(current, "label", {"text_message_id": text["message_id"],
            "label": "房东", "attachment_ids": [aid]}))
        self.assertEqual(labeled["attachments"][0]["attachment_id"], aid)
        self.assertIn({"value": "房东", "source": "user", "message_id": text["message_id"]}, labeled["attachments"][0]["tags"])
        found = self.success(self.call(current, "query", {"filters": {"tag": "房东"}}))
        self.assertEqual(found["items"][0]["attachment_id"], aid)
        self.assertEqual(self.worker._receipt("synthetic-historical-file")["status"], "complete")

    def test_foreign_attachment_in_batch_rejects_without_half_labeling_own_file(self):
        aid = self.file()
        foreign = dict(self.scope, bot_id="synthetic-foreign-bot")
        files = runtime.catalog(self.store)
        event = files.register_event(foreign, "synthetic-foreign-original", [{"item_index": 0, "kind": "file",
            "original_name": None, "declared_type": None}])
        ref = event["attachment_refs"][0]
        Path(ref["stage_path"]).write_bytes(b"SYNTHETIC foreign original")
        Path(ref["stage_path"]).chmod(0o600)
        files.publish(foreign, ref["attachment_id"], ref["stage_path"])
        text = self.receive(self.message("synthetic-label-cross-scope", text="请把选中的文件标记为房东"))
        batch = self.batch()
        rejected = self.call(batch, "label", {"text_message_id": text["message_id"], "label": "房东",
            "attachment_ids": [aid, ref["attachment_id"]]})
        self.error(rejected, "scope_mismatch")
        self.assertEqual(self.metadata(aid)["tags"], [])
        self.assertEqual(files.query(foreign)["items"][0]["tags"], [])

    def test_thirty_two_tag_limit_is_atomic_for_the_whole_selected_batch(self):
        labels = ["标签%02d" % number for number in range(33)]
        text_body = "请把选定的图片标记为 " + "、".join(labels)
        image, text, batch = self.images_then_text(text_body, count=2, prefix="synthetic-tag-cap")
        first, second = [ref["attachment_id"] for ref in image["attachment_refs"]]
        for label in labels[:32]:
            self.success(self.call(batch, "label", {"text_message_id": text["message_id"],
                "label": label, "attachment_ids": [first]}))
        before_first, before_second = copy.deepcopy(self.metadata(first)), copy.deepcopy(self.metadata(second))
        self.assertEqual(len(before_first["tags"]), 32)
        self.assertEqual(before_second["tags"], [])
        self.error(self.call(batch, "label", {"text_message_id": text["message_id"],
            "label": labels[32], "attachment_ids": [first, second]}), "quota_exceeded")
        self.assertEqual(self.metadata(first), before_first)
        self.assertEqual(self.metadata(second), before_second)
        for aid in (first, second):
            material = self.success(self.call(batch, "get", {"attachment_id": aid}))["material"]
            self.assertEqual(Path(material["path"]).read_bytes(), fixtures.png())


if __name__ == "__main__":
    unittest.main(verbosity=2)
