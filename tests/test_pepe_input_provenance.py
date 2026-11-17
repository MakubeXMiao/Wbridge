"""Receiver provenance regressions; temporary Stores and no network calls."""
import json
from pathlib import Path
import tempfile
import unittest

from wechat_bridge.ilink import ITEM_VOICE
from wechat_bridge.serve import _write_inbox_record, handle_incoming
from wechat_bridge.store import Store, atomic_write_json


class ManagedInputProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name)
        self.store.managed_inbox_dir = str(Path(self.store.base) / "backend_inbox")
        Path(self.store.managed_inbox_dir).mkdir()

    def message(self, items=None, **changes):
        result = {"message_id": "input-1", "from_user_id": "owner",
                  "message_type": 1, "context_token": "synthetic-context",
                  "item_list": [{"type": 1, "text_item": {"text": "hello"}}]
                  if items is None else items}
        result.update(changes)
        return result

    def record(self, message_id="input-1"):
        path = Path(self.store.managed_inbox_dir) / (message_id + ".json")
        return json.loads(path.read_text())

    def test_real_text_receiver_preserves_plain_text_provenance(self):
        self.assertTrue(handle_incoming(self.store, self.message(), "owner"))
        record = self.record()
        self.assertEqual(record["text"], "hello")
        self.assertEqual(record["input_provenance"], {
            "schema_version": 1, "single_chat": True, "item_types": [1]})
        self.assertNotIn("context_token", record["input_provenance"])

    def test_media_free_voice_transcript_stays_voice(self):
        msg = self.message(items=[{"type": ITEM_VOICE,
                                  "voice_item": {"text": "voice words"}}])
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        record = self.record()
        self.assertEqual(record["text"], "[语音转写]voice words")
        self.assertFalse(record.get("media_paths"))
        self.assertFalse(record.get("media_failed_count"))
        self.assertEqual(record["input_provenance"]["item_types"], [ITEM_VOICE])
        self.assertNotEqual(ITEM_VOICE, 1)

    def test_unknown_item_mixed_with_text_keeps_unsupported_type(self):
        msg = self.message(items=[{"type": 1, "text_item": {"text": "caption"}},
                                  {"type": 999}])
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        record = self.record()
        self.assertEqual(record["text"], "caption")
        self.assertEqual(record["input_provenance"]["item_types"], [1, 999])

    def test_missing_and_empty_item_list_do_not_gain_plain_text_origin(self):
        msg = self.message()
        msg.pop("item_list")
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        self.assertIsNone(self.record()["input_provenance"]["item_types"])
        msg = self.message(items=[], message_id="empty")
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        self.assertEqual(self.record("empty")["input_provenance"]["item_types"], [])

    def test_group_message_is_filtered_before_managed_record(self):
        msg = self.message(group_id="group-1")
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        self.assertFalse(list(Path(self.store.managed_inbox_dir).glob("*.json")))
        self.assertFalse(list(Path(self.store.receipts_dir).glob("*.json")))

    def test_invalid_type_is_quarantined_instead_of_managed_admission(self):
        msg = self.message(items=[{"type": True, "text_item": {"text": "bad"}}])
        self.assertTrue(handle_incoming(self.store, msg, "owner"))
        self.assertFalse(list(Path(self.store.managed_inbox_dir).glob("*.json")))

    def test_direct_managed_writer_retains_unknown_malformed_source(self):
        cases = (("not-an-array", None), ([None], [None]),
                 ([{"type": True}], [True]), ([{"type": "1"}], ["1"]),
                 ([{"type": 1.0}], [1.0]))
        for n, (items, expected) in enumerate(cases):
            with self.subTest(items=items):
                msg = self.message(items=items, message_id="malformed-%d" % n)
                self.assertTrue(_write_inbox_record(self.store, msg, "owner", "flattened"))
                actual = self.record(msg["message_id"])["input_provenance"]["item_types"]
                self.assertEqual(actual, expected)
                if expected is not None:
                    self.assertIs(type(actual[0]), type(expected[0]))

    def test_malformed_chat_source_does_not_gain_single_chat(self):
        for n, changes in enumerate(({"message_type": True}, {"group_id": 0},
                                     {"group_id": False}, {"group_id": []})):
            with self.subTest(changes=changes):
                msg = self.message(message_id="chat-%d" % n, **changes)
                self.assertTrue(_write_inbox_record(self.store, msg, "owner", "hello"))
                self.assertIs(self.record(msg["message_id"])["input_provenance"]["single_chat"], False)

    def test_legacy_writer_keeps_existing_record_shape(self):
        del self.store.managed_inbox_dir
        self.assertTrue(handle_incoming(self.store, self.message(), "owner"))
        record = json.loads((Path(self.store.inbox_dir) / "input-1.json").read_text())
        self.assertNotIn("input_provenance", record)

    def test_replay_does_not_upgrade_a_historical_managed_record(self):
        # Receiving a replay must not backfill unverifiable historical input as
        # purportedly verified plain text.
        old = {"message_id": "historical", "from_user_id": "owner", "text": "old"}
        path = Path(self.store.managed_inbox_dir) / "historical.json"
        atomic_write_json(str(path), old)
        self.assertTrue(handle_incoming(self.store, self.message(message_id="historical"), "owner"))
        self.assertEqual(json.loads(path.read_text()), old)


if __name__ == "__main__":
    unittest.main()
