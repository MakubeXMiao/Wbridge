"""Unified references through real receive, worker resolution, and file reply."""
from pathlib import Path
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_attachment_runtime as harness
from wechat_bridge import attachment_runtime as runtime, media, serve
from wechat_bridge.store import Store, atomic_write_json, load_json


class UnifiedReferenceRuntimeTests(unittest.TestCase):
    setUp = harness.RuntimeTests.setUp
    message = harness.RuntimeTests.message
    transport = harness.RuntimeTests.transport
    receive = harness.RuntimeTests.receive
    batch = harness.RuntimeTests.batch
    call = harness.RuntimeTests.call
    success = harness.RuntimeTests.success
    error = harness.RuntimeTests.error
    send_spy = harness.RuntimeTests.send_spy
    flush = harness.RuntimeTests.flush

    def test_mixed_message_freezes_positions_and_returns_file_photo_as_original(self):
        original = harness.png("blue")
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            row = self.receive(self.message("mixed", text="SYNTHETIC latest photo",
                attachments=[("image", harness.png(), None),
                             ("file", original, "camera-original.bin"),
                             ("file", b"SYNTHETIC plain text", "notes.txt")]))
        frozen = row["reference_context_v2"]
        self.assertEqual(frozen["confirmed_photo_count"], 0)
        self.assertEqual(len(row["reference_context"]["slots"]), 1)
        batch = self.batch()
        self.assertIn("references_v2", batch["capabilities"]["features"])
        args = {"text_message_id": "mixed", "snapshot_id": frozen["snapshot_id"]}
        metadata = self.success(self.call(batch, "reference_v2", args))
        self.assertEqual(metadata["materials"], [])
        self.assertEqual(metadata["unavailable"], [])
        self.assertTrue(metadata["complete"])
        result = self.success(self.call(batch, "reference_v2", dict(args, ordinals=[1, 2, 3])))
        current = result["reference_context_v2"]
        self.assertEqual(metadata["reference_context_v2"], current)
        self.assertEqual(current["membership_digest"], frozen["membership_digest"])
        self.assertEqual((current["attachment_count"], current["candidate_position_count"],
                          current["confirmed_photo_count"]), (3, 3, 2))
        self.assertEqual([(item["ordinal"], item["kind"], item["photo_state"])
                          for item in current["slots"]],
                         [(1, "image", "confirmed_photo"), (2, "file", "confirmed_photo"),
                          (3, "file", "not_photo")])
        latest = current["latest_candidate"]
        self.assertEqual(latest["ordinal"], 2)
        self.assertEqual([item["ordinal"] for item in result["unavailable"]], [3])
        self.assertFalse(result["complete"])
        item = next(item for item in result["materials"] if item["ordinal"] == 2)
        self.assertEqual(item["attachment_id"], latest["attachment_id"])
        self.assertEqual(Path(item["path"]).read_bytes(), original)
        for material in result["materials"]:
            self.success(self.call(batch, "release", {"hold_id": material["hold_id"]}))
        group = self.success(self.call(batch, "reply", {"attachment_ids": [latest["attachment_id"]]}))
        self.assertTrue(group["queued"])
        messages, originals = self.send_spy()
        self.flush()
        self.assertEqual(originals, [original])
        sent = messages[0]["item_list"][0]
        self.assertEqual(sent["type"], media.ITEM_FILE)
        self.assertEqual(sent["file_item"]["file_name"], "camera-original.bin")

    def test_v2_empty_window_cannot_masquerade_as_v1_reference(self):
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "0"}):
            old = self.receive(self.message("old", attachments=[("image", harness.png(), None)]))
        self.assertNotIn("reference_context_v2", old)
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            row = self.receive(self.message("text", text="SYNTHETIC latest photo"))
        self.assertIsNone(row["reference_context_v2"])
        legacy = row["reference_context"]
        batch = self.batch()
        args = {"text_message_id": "text", "snapshot_id": legacy["snapshot_id"], "ordinals": [1]}
        self.error(self.call(batch, "reference_v2", args), "scope_mismatch")
        result = self.success(self.call(batch, "reference", args))
        self.assertEqual(result["materials"][0]["attachment_id"],
                         old["attachment_refs"][0]["attachment_id"])

    def test_failed_catalog_registration_breaks_reference_continuity_after_restart(self):
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            old = self.receive(self.message("old", attachments=[("image", harness.png(), None)]))
        # A compatible receiver with its v2 writer temporarily disabled still
        # fences off an uncatalogued arrival from the next enabled window.
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "0"}), patch.object(
                runtime, "receive_event", side_effect=runtime.AttachmentCallError("storage_busy")):
            missed = self.receive(self.message("missed", attachments=[("file", harness.png(), "new.png")]))
        self.assertTrue(missed["media_failed"])
        self.assertNotIn("reference_context_v2", missed)
        self.store = Store(str(self.base))
        self.addCleanup(self.store.close)
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            text = self.receive(self.message("after-gap", text="SYNTHETIC latest photo"))
            self.assertIsNone(text["reference_context_v2"])
            self.assertEqual(text["reference_context"]["slots"][0]["attachment_id"],
                             old["attachment_refs"][0]["attachment_id"])
            arrived = self.receive(self.message("new", attachments=[("file", harness.png(), "new.png")]))
            text = self.receive(self.message("next-text", text="SYNTHETIC latest photo"))
        self.assertEqual(text["reference_context_v2"]["attachment_count"], 1)
        self.assertEqual(text["reference_context_v2"]["latest_candidate"]["attachment_id"],
                         arrived["attachment_refs"][0]["attachment_id"])

    def test_quarantined_batch_cuts_once_even_when_replayed(self):
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            self.receive(self.message("old", attachments=[("image", harness.png(), None)]))
            bad = {"msgs": self.message("malformed-batch", text="SYNTHETIC"),
                   "get_updates_buf": "bad"}
            following = self.message("new-file", attachments=[("file", harness.png(), "new.png")])
            text = self.message("after-replay", text="SYNTHETIC latest photo")
            polls = [bad, {"msgs": [following], "get_updates_buf": "new"}, bad,
                     {"msgs": [text], "get_updates_buf": "text"}, KeyboardInterrupt()]
            with patch.object(serve, "http_json", side_effect=polls), patch.object(
                    media.urllib.request, "urlopen", side_effect=self.transport), patch.object(
                    serve, "_acquire_lock", return_value=True), patch.object(
                    serve, "_acquire_bot_lock", return_value=True), patch.object(
                    serve, "FLUSH_INTERVAL_SECONDS", 60):
                self.assertEqual(serve.run(self.store, self.session["baseurl"],
                                           self.session["bot_token"], harness.OWNER), 0)
        first = load_json(str(self.base / "inbox" / "new-file.json"))
        result = load_json(str(self.base / "inbox" / "after-replay.json"))["reference_context_v2"]
        self.assertEqual(result["attachment_count"], 1)
        self.assertEqual(result["latest_candidate"]["attachment_id"],
                         first["attachment_refs"][0]["attachment_id"])

    def test_gap_record_failure_retains_unacknowledged_message(self):
        message = self.message("uncatalogued", text="SYNTHETIC original")
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}), patch.object(
                runtime, "receive_event", side_effect=runtime.AttachmentCallError("storage_busy")), patch.object(
                runtime, "break_reference_group", side_effect=OSError("synthetic storage error")):
            self.assertFalse(serve.handle_incoming(self.store, message, harness.OWNER))
        self.assertFalse((self.base / "inbox" / "uncatalogued.json").exists())

    def test_request_cannot_substitute_inbox_membership_identity(self):
        with patch.dict(os.environ, {"WECHAT_REFERENCE_V2": "1"}):
            row = self.receive(self.message("mixed", text="SYNTHETIC photo",
                attachments=[("file", harness.png(), "camera.bin")]))
        context = row["reference_context_v2"]
        context["membership_digest"] = "0" * 64
        atomic_write_json(str(self.base / "inbox" / "mixed.json"), row)
        batch = self.batch()
        self.error(self.call(batch, "reference_v2",
            {"text_message_id": "mixed", "snapshot_id": context["snapshot_id"]}), "scope_mismatch")


if __name__ == "__main__":
    unittest.main()
