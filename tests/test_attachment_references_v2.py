"""Public reference v2 storage tests with isolated synthetic originals."""
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from wechat_bridge.attachments import AttachmentError, AttachmentStore


SCOPE = {"bot_id": "synthetic-bot", "owner_id": "synthetic-owner",
         "conversation_id": "synthetic-conversation", "login_epoch": "synthetic-epoch"}


def item(index, kind="file"):
    return {"item_index": index, "kind": kind, "original_name": "synthetic.jpg",
            "declared_type": "image/jpeg"}


class ReferencesV2(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="reference-v2-synthetic-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name) / "runtime"
        self.now = 1000.0
        self.policy = {"max_bytes": 64, "quota_bytes": 32768, "recent_seconds": 60,
                       "retention_seconds": 30, "control_reserve_bytes": 0}
        self.catalog = self.reopen()

    def reopen(self, **policy):
        return AttachmentStore(str(self.base), {**self.policy, **policy}, clock=lambda: self.now)

    def event(self, mid, items=(), text="", *, enabled=True, scope=SCOPE):
        return self.catalog.register_event(scope, mid, list(items), text=text, reference_v2=enabled)

    def photo(self, ref, *, state="content_verified"):
        return self.publish(ref, "JPEG", preview="readable", state=state)

    def publish(self, ref, detected, preview="unavailable", state="protocol_decoded"):
        # The receiver supplies validated content metadata. This fixture
        # exercises registration and resolution rather than media decoding.
        path = ref["stage_path"]
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as original:
            original.write(b"SYNTHETIC ORIGINAL")
        return self.catalog.publish(SCOPE, ref["attachment_id"], path, decryption_state=state,
                                    detected_type=detected, preview_state=preview)

    def resolve(self, context, ordinals=None, *, holder=None, scope=SCOPE):
        return self.catalog.resolve_reference_v2(scope, context["text_message_id"],
            context["snapshot_id"], ordinals=ordinals, holder=holder)

    def counts(self, context, expected):
        self.assertEqual(expected, tuple(context[key] for key in
            ("attachment_count", "candidate_position_count", "confirmed_photo_count")))

    def test_same_event_text_includes_ordered_file_photo_and_non_candidate(self):
        event = self.event("mixed", [item(8, "image"), item(5, "voice"), item(2)], "这组")
        snapshot = event["reference_context_v2"]
        self.counts(snapshot, (3, 2, 0))
        self.assertEqual([2, 8], [row["item_index"] for row in snapshot["slots"]])
        self.assertEqual(snapshot["cutoff_seq"], snapshot["first_event_seq"])
        refs = event["attachment_refs"]
        self.photo(refs[0])
        self.photo(refs[2], state="plaintext_verified")
        found = self.resolve(snapshot, [1, 2], holder="worker:synthetic")
        self.assertTrue(found["complete"])
        self.counts(found["reference_context_v2"], (3, 2, 2))
        self.assertEqual(["file", "image"], [row["kind"] for row in found["reference_context_v2"]["slots"]])
        self.assertEqual(snapshot["membership_digest"], found["reference_context_v2"]["membership_digest"])
        self.assertEqual([refs[0]["attachment_id"], refs[2]["attachment_id"]],
                         [row["attachment_id"] for row in found["materials"]])
        self.assertEqual([b"SYNTHETIC ORIGINAL"] * 2,
                         [Path(row["path"]).read_bytes() for row in found["materials"]])
        self.assertEqual([refs[2]["attachment_id"]],
                         [row["attachment_id"] for row in event["reference_context"]["slots"]])

    def test_cross_event_order_cutoff_and_replay_survive_restart(self):
        older = self.event("older", [item(9, "image")])
        self.now -= 1
        newer = self.event("newer", [item(0)])
        self.assertIsNone(newer["reference_context_v2"])
        snapshot = self.event("text", text="最新")["reference_context_v2"]
        self.assertEqual(["older", "newer"], [row["message_id"] for row in snapshot["slots"]])
        self.assertLess(snapshot["last_member_seq"], snapshot["cutoff_seq"])
        self.photo(newer["attachment_refs"][0])
        self.catalog = self.reopen()
        observed = self.resolve(snapshot)["reference_context_v2"]
        self.assertEqual(newer["attachment_refs"][0]["attachment_id"],
                         observed["latest_candidate"]["attachment_id"])
        self.counts(observed, (2, 2, 1))
        repeated_text = self.event("followup", text="刚才那张")["reference_context_v2"]
        self.assertEqual(snapshot["group_id"], repeated_text["group_id"])
        self.assertEqual(snapshot["last_member_seq"], repeated_text["last_member_seq"])
        self.assertGreater(repeated_text["cutoff_seq"], snapshot["cutoff_seq"])
        newest = self.event("new-group", [item(1)])
        new_snapshot = self.event("new-group-text", text="这张")["reference_context_v2"]
        self.assertNotEqual(snapshot["group_id"], new_snapshot["group_id"])
        self.assertEqual(newest["attachment_refs"][0]["attachment_id"],
                         new_snapshot["latest_candidate"]["attachment_id"])
        replay = self.event("text", text="最新", enabled=False)["reference_context_v2"]
        self.assertEqual(observed, replay)
        self.assertEqual(older["attachment_refs"][0]["attachment_id"], replay["slots"][0]["attachment_id"])

    def test_overflow_and_holes_stay_at_original_positions(self):
        refs = self.event("seventeen", [item(index) for index in range(17)])["attachment_refs"]
        snapshot = self.event("text", text="最新")["reference_context_v2"]
        self.publish(refs[0], "text/plain")
        self.publish(refs[1], "HEIC")
        self.publish(refs[2], "JPEG", preview="resource_limited")
        self.catalog.fail(SCOPE, refs[3]["attachment_id"], "receive_failed")
        self.photo(refs[-1])
        found = self.resolve(snapshot, [1, 2, 3, 4], holder="worker:synthetic")
        result = found["reference_context_v2"]
        self.counts(result, (17, 17, 1))
        self.assertEqual(snapshot["membership_digest"], result["membership_digest"])
        self.assertTrue(result["overflow"])
        self.assertEqual(list(range(1, 17)), [row["ordinal"] for row in result["slots"]])
        self.assertEqual(17, result["latest_candidate"]["ordinal"])
        self.assertEqual(refs[16]["attachment_id"], result["latest_candidate"]["attachment_id"])
        self.assertFalse(found["complete"])
        self.assertEqual([], found["materials"])
        self.assertEqual(["not_photo", "unverified", "unverified", "unverified"],
                         [row["photo_state"] for row in found["unavailable"]])
        self.assertEqual("failed", found["unavailable"][-1]["status"])
        with self.assertRaisesRegex(AttachmentError, "^invalid_request$"):
            self.resolve(snapshot, [17])

    def test_legacy_event_and_policy_limit_survive_cutover(self):
        old_photo = self.event("old-photo", [item(0, "image")], enabled=False)
        old_text = self.event("old-text", text="旧图", enabled=False)
        self.assertNotIn("reference_context_v2", self.event("old-photo", [item(0, "image")]))
        self.assertNotIn("reference_context_v2", self.event("old-text", text="旧图"))
        self.catalog = self.reopen(max_positions=1)
        files = self.event("file-1", [item(1)])
        self.catalog = self.reopen()
        self.event("file-2", [item(3)])
        current = self.event("current", text="新组")["reference_context_v2"]
        self.counts(current, (2, 2, 0))
        self.assertEqual(1, current["position_limit"])
        self.assertEqual([files["attachment_refs"][0]["attachment_id"]],
                         [slot["attachment_id"] for slot in current["slots"]])
        original = self.catalog.resolve_reference(SCOPE, old_text["reference_context"]["snapshot_id"])
        self.assertEqual(old_text["reference_context"], original["reference_context"])
        self.assertEqual(old_photo["attachment_refs"][0]["attachment_id"],
                         original["reference_context"]["slots"][0]["attachment_id"])
        self.event("v1-gap", [item(5)], enabled=False)
        self.assertIsNone(self.event("after-gap", text="刚才那张")["reference_context_v2"])
        self.assertEqual(current, self.resolve(current)["reference_context_v2"])

    def test_epoch_expiry_and_wrong_text_do_not_resolve(self):
        self.event("attachment", [item(0)])
        snapshot = self.event("text", text="这张")["reference_context_v2"]
        other_epoch = dict(SCOPE, login_epoch="next-synthetic-epoch")
        other_scope = dict(SCOPE, conversation_id="other-synthetic-conversation")
        for scope, code in ((other_epoch, "epoch_mismatch"), (other_scope, "not_found")):
            with self.assertRaisesRegex(AttachmentError, "^" + code + "$"):
                self.resolve(snapshot, scope=scope)
        with self.assertRaisesRegex(AttachmentError, "^not_found$"):
            self.catalog.resolve_reference_v2(SCOPE, "attachment", snapshot["snapshot_id"])
        self.assertIsNone(self.event("new-epoch", text="这张", scope=other_epoch)["reference_context_v2"])
        self.now = snapshot["expires_at"]
        with self.assertRaisesRegex(AttachmentError, "^snapshot_expired$"):
            self.resolve(snapshot)

    def test_registration_failure_leaves_no_partial_frozen_range(self):
        self.event("old", [item(0)])

        def fault(point):
            if point == "register_before_commit":
                raise ValueError("synthetic transaction failure")

        self.catalog.fault = fault
        with self.assertRaisesRegex(AttachmentError, "^storage_invalid$"):
            self.event("mixed", [item(1)], text="两张")
        self.catalog = self.reopen()
        self.assertEqual(["old"], [row["message_id"] for row in self.catalog.query(SCOPE)["items"]])
        retry = self.event("mixed", [item(1)], text="两张")["reference_context_v2"]
        self.counts(retry, (2, 2, 0))
        self.assertEqual(["old", "mixed"], [row["message_id"] for row in retry["slots"]])

    def test_missing_original_remains_confirmed_but_cannot_return_a_hold(self):
        ref = self.event("attachment", [item(0)])["attachment_refs"][0]
        self.photo(ref)
        snapshot = self.event("text", text="这张")["reference_context_v2"]
        held = self.resolve(snapshot, [1], holder="worker:synthetic")
        original = Path(held["materials"][0]["path"])
        original.write_bytes(b"SYNTHETIC CHANGED ")
        observed = self.resolve(snapshot, [1], holder="worker:synthetic")
        self.assertFalse(observed["complete"])
        self.assertEqual([], observed["materials"])
        context = observed["reference_context_v2"]
        self.counts(context, (1, 1, 1))
        self.assertEqual(snapshot["membership_digest"], context["membership_digest"])
        self.assertEqual("missing", context["latest_candidate"]["status"])
        self.assertEqual("confirmed_photo", observed["unavailable"][0]["photo_state"])

    def test_preview_protects_live_range_and_invalid_extension_fails_closed(self):
        ref = self.event("attachment", [item(0)])["attachment_refs"][0]
        self.photo(ref)
        snapshot = self.event("text", text="这张")["reference_context_v2"]
        self.event("text-again", text="还是这张")
        self.now += 35
        preview = self.catalog.preview_gc()
        self.assertEqual(0, preview["deleted_files"])
        self.assertEqual("in_use", preview["items"][0]["reason"])
        self.now += 26
        self.assertEqual("expired", self.catalog.preview_gc()["items"][0]["reason"])
        self.now -= 26
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            raw = db.execute("SELECT payload FROM events WHERE seq=?", (snapshot["cutoff_seq"],)).fetchone()[0]
            event = json.loads(raw)
            event["reference_v2"]["group"]["member_chain"] = "0" * 64
            db.execute("UPDATE events SET payload=? WHERE seq=?", (json.dumps(event), snapshot["cutoff_seq"]))
        for action in (lambda: self.resolve(snapshot), self.catalog.preview_gc):
            with self.assertRaisesRegex(AttachmentError, "^storage_invalid$"):
                action()


if __name__ == "__main__":
    unittest.main()
