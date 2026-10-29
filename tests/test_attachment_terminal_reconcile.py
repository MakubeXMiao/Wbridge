"""Normal terminal delivery protection recovery; synthetic public paths only."""
import copy
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.store import Store, atomic_write_json, load_json


class TerminalReconcileTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RuntimeTests("runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)

    def queued(self, caption="", count=1):
        f = self.f
        aids = [f.file(mid="synthetic-terminal-%d" % i) for i in range(count)]
        batch = f.batch()
        group = f.success(f.call(batch, "reply", {"attachment_ids": aids, "caption": caption}))
        return aids, group

    def cancel(self, group):
        f = self.f
        row = f.receive(f.message("synthetic-terminal-cancel", text="取消 " + group["group_id"]))
        batch = f.batch()
        f.success(f.call(batch, "cancel", {"text_message_id": row["message_id"],
                                           "group_id": group["group_id"]}))
        return batch

    def holds(self):
        with sqlite3.connect(str(self.f.base / "state/attachments.sqlite3")) as db:
            return {hid: dict(json.loads(raw), attachment_id=aid)
                    for hid, aid, raw in db.execute("SELECT id,attachment_id,payload FROM holds")}

    def group_holds(self):
        return {hid: value for hid, value in self.holds().items()
                if value["holder"].startswith("group:")}

    def original_journal(self, group):
        state = runtime.ledger(self.f.store)
        info = state.terminal_materials(self.f.scope, group["group_id"])
        return self.f.base / "state/attachment_admissions" / (info["request_id"] + ".json")

    def test_normal_unstarted_cancel_releases_at_sender_safe_point_not_preview(self):
        f = self.f
        aids, group = self.queued()
        self.cancel(group)
        before = self.holds()
        self.assertTrue(self.group_holds())
        self.assertTrue(all(v["released_at"] is None for v in self.group_holds().values()))
        original_files = {str(p): (p.read_bytes(), p.stat().st_mtime_ns)
                          for p in (f.base / "attachments/objects").iterdir()}
        runtime.catalog(f.store).preview_gc()
        self.assertEqual(before, self.holds())
        messages, originals = f.send_spy()
        f.flush()
        self.assertEqual((messages, originals), ([], []))
        released = self.group_holds()
        self.assertEqual(set(released), {hid for hid, value in before.items()
                                        if value["holder"].startswith("group:")})
        self.assertTrue(all(v["released_at"] is not None for v in released.values()))
        f.flush()
        self.assertEqual(released, self.group_holds())
        self.assertEqual(original_files, {str(p): (p.read_bytes(), p.stat().st_mtime_ns)
                                         for p in (f.base / "attachments/objects").iterdir()})
        self.assertEqual(runtime.catalog(f.store).query(f.scope)["items"][0]["attachment_id"], aids[0])

    def test_completion_interruption_before_release_is_reconciled_after_restart(self):
        f = self.f
        _, group = self.queued()
        messages, originals = f.send_spy()
        with patch.object(runtime, "_release_group_holds", side_effect=RuntimeError("synthetic cleanup interruption")):
            with self.assertRaises(RuntimeError):
                f.flush()
        self.assertEqual(runtime.ledger(f.store).group_status(f.scope, group["group_id"])["state"], "complete")
        self.assertEqual((len(messages), len(originals)), (1, 1))
        self.assertEqual(len(self.group_holds()), 2)
        self.assertTrue(all(v["released_at"] is None for v in self.group_holds().values()))
        restarted = Store(str(f.base))
        self.addCleanup(restarted.close)
        runtime.initialize(restarted)
        runtime.ledger(restarted).recover()
        runtime.recover_admissions(restarted)
        f.flush(restarted)
        self.assertTrue(all(v["released_at"] is not None for v in self.group_holds().values()))
        self.assertEqual((len(messages), len(originals)), (1, 1))

    def test_cancel_during_upload_keeps_holds_until_sender_unwinds(self):
        f = self.f
        _, group = self.queued()
        observed = []
        def cancel_during_upload():
            self.cancel(group)
            observed.append(copy.deepcopy(self.group_holds()))
            self.assertEqual(len(observed[-1]), 2)
            self.assertTrue(all(v["released_at"] is None for v in observed[-1].values()))
        messages, originals = f.send_spy(after_upload=cancel_during_upload)
        f.flush()
        self.assertEqual((len(observed), len(messages), len(originals)), (1, 0, 1))
        self.assertTrue(all(v["released_at"] is not None for v in self.group_holds().values()))

    def test_unknown_cancel_preserves_both_delivery_holders(self):
        f = self.f
        _, group = self.queued()
        messages, _ = f.send_spy(final_result=TimeoutError("synthetic outcome unknown"))
        f.flush()
        self.cancel(group)
        before = self.group_holds()
        f.flush()
        f.flush()
        self.assertEqual(before, self.group_holds())
        self.assertTrue(all(v["released_at"] is None for v in before.values()))
        self.assertIsNone(runtime.ledger(f.store).terminal_materials(f.scope, group["group_id"]))
        self.assertEqual(len(messages), 1)

    def test_awaiting_continue_is_not_a_terminal_release(self):
        f = self.f
        _, group = self.queued(caption="C" * 14000, count=2)
        f.send_spy()
        f.flush()
        state = runtime.ledger(f.store)
        self.assertEqual(state.group_status(f.scope, group["group_id"])["state"], "awaiting_continue")
        before = self.group_holds()
        f.flush()
        self.assertEqual(before, self.group_holds())
        self.assertTrue(all(v["released_at"] is None for v in before.values()))

    def test_missing_original_journal_preserves_orphan_until_proof_restored(self):
        f = self.f
        _, group = self.queued()
        self.cancel(group)
        path = self.original_journal(group)
        retained = f.home / "synthetic-retained-journal.json"
        path.rename(retained)
        before = self.group_holds()
        f.send_spy()
        f.flush()
        self.assertEqual(before, self.group_holds())
        retained.rename(path)
        f.flush()
        self.assertTrue(all(v["released_at"] is not None for v in self.group_holds().values()))

    def test_changed_journal_material_binding_cannot_release(self):
        f = self.f
        _, group = self.queued()
        self.cancel(group)
        path = self.original_journal(group)
        record = load_json(str(path))
        record["attachment_ids"] = ["0" * 32]
        atomic_write_json(str(path), record)
        before = self.group_holds()
        f.send_spy()
        with self.assertRaises(runtime.AttachmentCallError) as failure:
            f.flush()
        self.assertEqual(failure.exception.code, "storage_invalid")
        self.assertEqual(before, self.group_holds())

    def test_terminal_reconcile_preserves_active_worker_hold_and_pin(self):
        f = self.f
        aids, group = self.queued()
        row = f.receive(f.message("synthetic-terminal-cancel", text="取消 " + group["group_id"]))
        batch = f.batch()
        material = f.success(f.call(batch, "get", {"attachment_id": aids[0]}))["material"]
        f.success(f.call(batch, "pin", {"attachment_id": aids[0], "pinned": True}))
        f.success(f.call(batch, "cancel", {"text_message_id": row["message_id"], "group_id": group["group_id"]}))
        before = self.holds()[material["hold_id"]]
        f.send_spy()
        f.flush()
        self.assertEqual(before, self.holds()[material["hold_id"]])
        self.assertTrue(runtime.catalog(f.store).query(f.scope)["items"][0]["pinned"])
        self.assertTrue(all(v["released_at"] is not None for v in self.group_holds().values()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
