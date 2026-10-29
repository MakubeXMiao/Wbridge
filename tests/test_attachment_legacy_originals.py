"""Reserved catalog originals cannot use the ordinary media queue; synthetic only."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime, serve
from wechat_bridge.store import atomic_write_json, load_json


class LegacyOriginalTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RuntimeTests("runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)

    def original(self):
        aid = self.f.file()
        return aid, next((self.f.base / "attachments/objects").glob(aid + ".*"))

    def generated(self):
        path = self.f.home / "SYNTHETIC-generated.pdf"
        path.write_bytes(b"SYNTHETIC generated ordinary file")
        return path

    def old_pending(self, original, caption="SYNTHETIC ordinary caption"):
        """Inject a historical job after using the real ordinary producer."""
        f = self.f
        if not list((f.base / "inbox").glob("*.json")):
            f.receive(f.message(text="SYNTHETIC ordinary request"))
        batch = f.batch()
        result = f.worker.reply(batch["owned"], caption, batch["lease"], str(self.generated()))
        path = f.base / "outbox/pending" / result["queued"]
        job = load_json(str(path))
        job["media_path"] = str(original)
        atomic_write_json(str(path), job)
        return path, job

    def ledger_rows(self):
        path = self.f.base / "state/attachment-delivery.sqlite3"
        self.assertTrue(path.exists())
        with sqlite3.connect(str(path)) as db:
            names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return {name: db.execute('SELECT * FROM "' + name + '" ORDER BY rowid').fetchall() for name in names}

    def test_original_and_symlink_alias_refused_before_reply_publication(self):
        f = self.f
        _, original = self.original()
        alias = f.home / "SYNTHETIC-alias.pdf"
        alias.symlink_to(original)
        batch = f.batch()
        before_claims = copy.deepcopy(f.worker._claims())
        before = self.ledger_rows()
        paths = [original, alias]
        case_alias = original.parent.with_name("OBJECTS") / original.name
        if case_alias.exists() and case_alias.samefile(original):
            paths.append(case_alias)
        for path in paths:
            with self.assertRaisesRegex(ValueError, "require attachment ID"):
                f.worker.reply(batch["owned"], "SYNTHETIC caption", batch["lease"], str(path))
        self.assertEqual(f.worker._claims(), before_claims)
        self.assertEqual(self.ledger_rows(), before)
        self.assertEqual(list((f.base / "outbox/pending").glob("*.json")), [])
        self.assertEqual(list(f.worker.batch_dir.glob("*.json")), [])
        self.assertFalse(runtime.is_catalog_original_path(f.store, f.home / "objects-generated.pdf"))

    def test_existing_original_pending_keeps_bytes_and_never_splits_or_accounts(self):
        f = self.f
        _, original = self.original()
        path, job = self.old_pending(original)
        before, accounting = path.read_bytes(), self.ledger_rows()
        messages, bodies = f.send_spy()
        f.flush()
        f.flush()
        self.assertEqual((messages, bodies), ([], []))
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.ledger_rows(), accounting)
        for directory in (f.store.sending_dir, f.store.sent_dir, f.store.failed_dir):
            self.assertEqual(list(Path(directory).glob("*.json")), [])
        self.assertFalse(list((f.base / "outbox/pending").glob("*_caption.json")))
        with self.assertRaisesRegex(ValueError, "require attachment ID"):
            serve._split_caption(f.store, path.name, job)

    def test_existing_sending_and_split_caption_remain_uncertain_and_unchanged(self):
        f = self.f
        _, original = self.original()
        pending, job = self.old_pending(original)
        job.update(delivery_state="inflight", attempts=1, caption_split=True)
        sending = f.base / "outbox/sending" / pending.name
        atomic_write_json(str(sending), job)
        pending.unlink()
        caption = f.base / "outbox/pending" / (pending.stem + "_caption.json")
        atomic_write_json(str(caption), dict(to_user_id=job["to_user_id"], text="SYNTHETIC old caption",
            context_token=job["context_token"], kind="caption", login_epoch=job["login_epoch"],
            enqueued_at=job["enqueued_at"], delivery_state="inflight", attempts=1))
        budget = runtime.send_budget(f.store, job["to_user_id"], f.session["bot_token"],
                                     job["context_token"], job["client_key"])
        self.assertTrue(budget.before(0, 1))
        budget.result(0, 1, {"ret": -1})
        before = {p: p.read_bytes() for p in (sending, caption)}
        accounting = self.ledger_rows()
        self.assertIn("unknown", json.dumps(accounting))
        messages, bodies = f.send_spy()
        serve._recover_sending(f.store)
        f.flush()
        self.assertEqual({p: p.read_bytes() for p in before}, before)
        self.assertEqual(self.ledger_rows(), accounting)
        self.assertEqual((messages, bodies), ([], []))
        self.assertFalse(pending.exists())

    def test_prepared_ordinary_republication_refused_without_changing_journal(self):
        f = self.f
        _, original = self.original()
        pending, job = self.old_pending(original)
        record_path = f.worker.batch_dir / pending.name
        record = load_json(str(record_path))
        record.update(phase="prepared", admitted=False, job=job)
        atomic_write_json(str(record_path), record)
        pending.unlink()
        before = record_path.read_bytes()
        with self.assertRaisesRegex(ValueError, "require attachment ID"):
            f.worker.recover_prepared(pending.name)
        self.assertEqual(record_path.read_bytes(), before)
        self.assertFalse(pending.exists())

    def test_existing_caption_is_blocked_by_original_registry_if_parent_is_absent(self):
        f = self.f
        _, original = self.original()
        pending, job = self.old_pending(original)
        registry = f.worker.batch_dir / pending.name
        record = load_json(str(registry))
        record["job"] = job
        atomic_write_json(str(registry), record)
        pending.unlink()
        caption = f.base / "outbox/pending" / (pending.stem + "_caption.json")
        atomic_write_json(str(caption), dict(to_user_id=job["to_user_id"], text="SYNTHETIC orphan caption",
            context_token=job["context_token"], kind="caption", enqueued_at=job["enqueued_at"]))
        before = caption.read_bytes(), registry.read_bytes(), self.ledger_rows()
        messages, bodies = f.send_spy()
        f.flush()
        self.assertEqual((messages, bodies), ([], []))
        self.assertEqual((caption.read_bytes(), registry.read_bytes(), self.ledger_rows()), before)

    def test_claimed_original_cannot_start_caption_upload_or_budget(self):
        f = self.f
        _, original = self.original()
        pending, job = self.old_pending(original)
        sending = f.base / "outbox/sending" / pending.name
        os.replace(str(pending), str(sending))
        before = sending.read_bytes()
        accounting, counts = self.ledger_rows(), {}
        messages, bodies = f.send_spy()
        self.assertTrue(serve._deliver_claimed(f.store, "https://synthetic.invalid", f.session["bot_token"],
            counts, sending.name, job, str(sending), str(pending), job["enqueued_at"], 0))
        self.assertEqual((counts, messages, bodies), ({}, [], []))
        self.assertEqual(sending.read_bytes(), before)
        self.assertEqual(self.ledger_rows(), accounting)

    def test_cancelled_or_unknown_group_cannot_resend_through_ordinary_reply(self):
        for state in ("cancelled", "unknown"):
            with self.subTest(state=state):
                f = fixtures.RuntimeTests("runTest")
                f.setUp()
                try:
                    aid = f.file()
                    original = next((f.base / "attachments/objects").glob(aid + ".*"))
                    group = f.success(f.call(f.batch(), "reply", {"attachment_ids": [aid]}))
                    if state == "unknown":
                        f.send_spy(final_result=TimeoutError("SYNTHETIC ambiguous acknowledgement"))
                        f.flush()
                    f.receive(f.message(text=("取消 " + group["group_id"]) if state == "cancelled" else "SYNTHETIC unrelated"))
                    batch = f.batch()
                    if state == "cancelled":
                        f.success(f.call(batch, "cancel", {"group_id": group["group_id"], "text_message_id": batch["owned"][-1]}))
                    with self.assertRaisesRegex(ValueError, "require attachment ID"):
                        f.worker.reply(batch["owned"], "", batch["lease"], str(original))
                    self.assertEqual(runtime.ledger(f.store).group_status(f.scope, group["group_id"])["state"], state)
                    self.assertEqual(list((f.base / "outbox/pending").glob("*.json")), [])
                finally:
                    f.doCleanups()

    def test_awaiting_continue_cannot_send_remaining_original_via_ordinary_reply(self):
        f = self.f
        aids = [f.file(mid="synthetic-wait-%d" % i) for i in range(9)]
        group = f.success(f.call(f.batch(), "reply", {"attachment_ids": aids}))
        messages, bodies = f.send_spy()
        f.flush()
        self.assertEqual(len(messages), 8)
        f.receive(f.message(text="SYNTHETIC unrelated request"))
        batch = f.batch()
        f.error(f.call(batch, "continue", {"group_id": group["group_id"]}), "continue_required")
        path = next((f.base / "attachments/objects").glob(aids[-1] + ".*"))
        with self.assertRaisesRegex(ValueError, "require attachment ID"):
            f.worker.reply(batch["owned"], "", batch["lease"], str(path))
        f.flush()
        self.assertEqual(len(messages), 8)
        status = runtime.ledger(f.store).group_status(f.scope, group["group_id"])
        self.assertEqual(status["state"], "awaiting_continue")
        self.assertIn(aids[-1], status["pending_attachment_ids"])

    def test_generated_pdf_still_sends_and_old_worker_epoch_still_rejected(self):
        f = self.f
        f.receive(f.message(text="SYNTHETIC generate PDF"))
        batch = f.batch()
        path = self.generated()
        f.worker.reply(batch["owned"], "", batch["lease"], str(path))
        messages, bodies = f.send_spy()
        f.flush()
        self.assertEqual((len(messages), bodies), (1, [path.read_bytes()]))
        f.receive(f.message(text="SYNTHETIC second request"))
        batch = f.batch()
        session = load_json(f.store.session_path)
        session["login_epoch"] = "e" * 32
        atomic_write_json(f.store.session_path, session)
        with self.assertRaisesRegex(ValueError, "epoch"):
            f.worker.reply(batch["owned"], "", batch["lease"], str(path))


if __name__ == "__main__":
    unittest.main()
