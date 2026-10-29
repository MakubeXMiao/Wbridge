"""003 finishing controls: public cancel, projection, real local bounds and faults.

Everything is a synthetic temporary fixture. Sparse boundary cases really hash
100 MiB locally; they do not download/upload 100 MiB or fill the disk.
"""
import copy
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime, ilink, media, serve, worker_state
from wechat_bridge import attachments as catalog_module
from wechat_bridge.attachments import AttachmentError, AttachmentStore
from wechat_bridge.store import atomic_write_json, load_json


class ControlTests(unittest.TestCase):
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
    send_spy = fixtures.RuntimeTests.send_spy
    flush = fixtures.RuntimeTests.flush

    def queued_group(self, count=1):
        aids = [self.file(mid="synthetic-control-file-%d" % i) for i in range(count)]
        batch = self.batch()
        result = self.success(self.call(batch, "reply", {"attachment_ids": aids}))
        return aids, result, batch["context"]

    def cancel_request(self, words="取消这组附件", mid="synthetic-cancel"):
        text = self.receive(self.message(mid, text=words))
        return text, self.batch()

    def cancel_args(self, text, group):
        return {"text_message_id": text["message_id"], "group_id": group["group_id"]}

    def record(self, aid):
        return next(row for row in runtime.catalog(self.store).query(self.scope, limit=100)["items"]
                    if row["attachment_id"] == aid)

    def holds(self):
        path = self.base / "state/attachments.sqlite3"
        with sqlite3.connect(str(path)) as db:
            return db.execute("SELECT id,attachment_id,holder,payload FROM holds ORDER BY id").fetchall()

    def test_cancel_capability_and_queued_cancel_are_public_idempotent_without_closing_batch(self):
        self.assertIn("attachment_cancel_v1", runtime.capabilities(str(self.base))["features"])
        aids, group, context = self.queued_group(count=2)
        text, batch = self.cancel_request()
        state = runtime.ledger(self.worker.store)
        before = copy.deepcopy(self.worker._claims())
        first = self.success(self.call(batch, "cancel", self.cancel_args(text, group)))
        self.assertEqual(set(first), {"group"})
        self.assertEqual(first["group"]["state"], "cancelled")
        self.assertEqual(first["group"]["accepted_attachment_ids"], [])
        self.assertEqual(first["group"]["unknown_attachment_ids"], [])
        self.assertEqual(self.success(self.call(batch, "cancel", self.cancel_args(text, group))), first)
        self.assertEqual(self.worker._claims(), before)
        self.assertEqual(self.worker._receipt(text["message_id"])["status"], "received")
        self.assertEqual(state.pending_jobs(self.scope)["job_ids"], [])
        self.assertEqual(state.register_context(self.scope, context)["remaining"], 8)
        self.assertTrue(all(self.record(aid)["status"] == "ready" for aid in aids))

    def test_denial_question_or_unrelated_text_cannot_cancel(self):
        _, group, _ = self.queued_group()
        for index, words in enumerate(("不要取消这组附件", "取消这组附件吗？", "SYNTHETIC another topic")):
            text, batch = self.cancel_request(words, "synthetic-cancel-denial-%d" % index)
            self.error(self.call(batch, "cancel", self.cancel_args(text, group)), "invalid_request")
            self.assertEqual(runtime.ledger(self.worker.store).group_status(self.scope, group["group_id"])["state"], "queued")
            self.complete_plain(batch)

    def test_cancel_rejects_unowned_text_and_foreign_group(self):
        _, group, _ = self.queued_group()
        text, batch = self.cancel_request()
        bad = self.cancel_args(text, group)
        bad["text_message_id"] = "synthetic-not-owned"
        self.error(self.call(batch, "cancel", bad), "invalid_request")
        state = runtime.ledger(self.worker.store)
        foreign = dict(self.scope, bot_id="synthetic-control-foreign")
        other = state.create_group(foreign, "synthetic-foreign-request", ["synthetic-foreign-file"])
        self.error(self.call(batch, "cancel", self.cancel_args(text, other)), "scope_mismatch")
        self.assertEqual(state.group_status(self.scope, group["group_id"])["state"], "queued")
        self.assertEqual(state.group_status(foreign, other["group_id"])["state"], "queued")

    def test_expired_worker_cannot_cancel_a_queued_group(self):
        _, group, _ = self.queued_group()
        text, batch = self.cancel_request()
        self.now += worker_state.WORKER_LEASE_SECONDS
        self.error(self.call(batch, "cancel", self.cancel_args(text, group)), "lease_lost")
        self.assertEqual(runtime.ledger(self.worker.store).group_status(self.scope, group["group_id"])["state"], "queued")

    def test_changed_login_epoch_cannot_use_old_cancel_worker(self):
        _, group, _ = self.queued_group()
        text, batch = self.cancel_request()
        session = load_json(self.store.session_path)
        session["login_epoch"] = "e" * 32
        atomic_write_json(self.store.session_path, session)
        self.error(self.call(batch, "cancel", self.cancel_args(text, group)), "epoch_mismatch")
        self.assertEqual(runtime.ledger(self.worker.store).group_status(self.scope, group["group_id"])["state"], "queued")

    def test_actual_public_cancel_after_upload_stops_final_send_and_refunds_only_unsent(self):
        _, group, context = self.queued_group()
        text, batch = self.cancel_request()
        cancellations = []
        def cancel_after_upload():
            cancellations.append(self.success(self.call(batch, "cancel", self.cancel_args(text, group))))
        messages, originals = self.send_spy(after_upload=cancel_after_upload)
        self.flush()
        self.assertEqual(len(originals), 1)
        self.assertEqual(messages, [])
        self.assertEqual(cancellations[0]["group"]["state"], "cancelled")
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, context)["remaining"], 8)
        self.assertEqual(self.worker._receipt(text["message_id"])["status"], "received")

    def test_public_cancel_of_real_inflight_keeps_unknown_budget_and_never_replays(self):
        aids, group, context = self.queued_group()
        text, batch = self.cancel_request()
        messages, _ = self.send_spy(final_result=TimeoutError("synthetic final outcome unknown"))
        edge = media.http_json
        actual_edge = edge.side_effect
        cancellations = []
        def inflight_edge(method, url, headers, body, **kwargs):
            if url.endswith("/sendmessage"):
                cancellations.append(self.success(self.call(batch, "cancel", self.cancel_args(text, group))))
            return actual_edge(method, url, headers, body, **kwargs)
        edge.side_effect = inflight_edge
        self.flush()
        self.assertEqual(len(messages), 1)
        self.assertEqual(cancellations[0]["group"]["state"], "unknown")
        self.assertEqual(cancellations[0]["group"]["unknown_attachment_ids"], aids)
        state = runtime.ledger(self.store)
        self.assertEqual(state.register_context(self.scope, context)["remaining"], 7)
        repeated = self.success(self.call(batch, "cancel", self.cancel_args(text, group)))
        self.assertEqual(repeated["group"]["state"], "unknown")
        self.assertEqual(state.register_context(self.scope, context)["remaining"], 7)
        self.flush()
        self.assertEqual(len(messages), 1)

    def test_seventeen_received_refs_remain_seventeen_while_reference_slots_stop_at_sixteen(self):
        images = [("image", fixtures.png(), None) for _ in range(17)]
        incoming = self.receive(self.message("synthetic-seventeen-images", attachments=images))
        text = self.receive(self.message("synthetic-seventeen-question", text="上一组第16张是什么"))
        self.assertEqual(len(incoming["attachment_refs"]), 17)
        self.assertEqual([item["item_index"] for item in incoming["attachment_refs"]], list(range(17)))
        self.assertEqual(len({item["attachment_id"] for item in incoming["attachment_refs"]}), 17)
        context = text["reference_context"]
        self.assertEqual(len(context["slots"]), 16)
        self.assertTrue(context["overflow"])
        self.assertEqual(context["total_positions"], 17)
        batch = self.batch()
        for value in (batch, self.worker.refresh(batch["lease"])):
            picture = next(row for row in value["messages"] if row["message_id"] == incoming["message_id"])
            question = next(row for row in value["messages"] if row["message_id"] == text["message_id"])
            self.assertEqual(len(picture["attachment_refs"]), 17)
            self.assertEqual(question["reference_context"]["total_positions"], 17)
            self.assertEqual(len(question["reference_context"]["slots"]), 16)
        selected = self.success(self.call(batch, "reference", {"text_message_id": text["message_id"],
            "snapshot_id": context["snapshot_id"], "ordinals": [16]}))
        self.assertEqual(selected["materials"][0]["attachment_id"], incoming["attachment_refs"][15]["attachment_id"])
        self.error(self.call(batch, "reply", {"attachment_ids": [row["attachment_id"] for row in incoming["attachment_refs"]]}), "invalid_request")

    def test_begin_and_refresh_preserve_large_legal_text_and_reference_above_call_limit(self):
        self.receive(self.message("synthetic-large-ref-image", attachments=[("image", fixtures.png(), None)]))
        body = "SYNTHETIC " + "L" * 70000
        text = self.receive(self.message("synthetic-large-ref-text", text=body))
        batch = self.batch()
        self.assertGreater(len(json.dumps(batch).encode("utf-8")), 65536)
        refreshed = self.worker.refresh(batch["lease"])
        for value in (batch, refreshed):
            record = next(row for row in value["messages"] if row["message_id"] == text["message_id"])
            self.assertEqual(record["text"], body)
            self.assertEqual(record["reference_context"], text["reference_context"])
        result = self.success(self.call(refreshed, "reference", {"text_message_id": text["message_id"],
            "snapshot_id": text["reference_context"]["snapshot_id"], "ordinals": [1]}))
        self.assertEqual(len(result["materials"]), 1)

    def test_real_100mib_sparse_stage_is_hashed_and_max_plus_one_is_rejected(self):
        maximum = ilink.MAX_MEDIA_BYTES
        self.assertEqual(maximum, 104857600)
        files = runtime.catalog(self.store)
        self.assertEqual(files.policy["max_bytes"], maximum)
        item = {"item_index": 0, "kind": "file", "original_name": None, "declared_type": None}
        event = files.register_event(self.scope, "synthetic-real-100mib", [item])
        ref = event["attachment_refs"][0]
        fd = os.open(ref["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.ftruncate(fd, maximum)
        finally:
            os.close(fd)
        stage_stat = Path(ref["stage_path"]).stat()
        self.assertLess(stage_stat.st_blocks * 512, maximum)
        expected = hashlib.sha256()
        block = b"\x00" * (1 << 20)
        for _ in range(maximum // len(block)):
            expected.update(block)
        read_sizes = []
        actual_read = catalog_module.os.read
        def measured_read(file_descriptor, count):
            if os.fstat(file_descriptor).st_size == maximum:
                read_sizes.append(count)
            return actual_read(file_descriptor, count)
        with patch.object(catalog_module.os, "read", side_effect=measured_read):
            metadata = files.publish(self.scope, ref["attachment_id"], ref["stage_path"])
            material = files.acquire(self.scope, ref["attachment_id"], "worker:synthetic-100mib")
        self.assertEqual(metadata["status"], "ready")
        self.assertEqual(metadata["size"], maximum)
        self.assertEqual(metadata["sha256"], expected.hexdigest())
        self.assertEqual(Path(material["path"]).stat().st_size, maximum)
        self.assertTrue(read_sizes)
        self.assertTrue(all(0 < size <= (1 << 20) for size in read_sizes))
        too_large = files.register_event(self.scope, "synthetic-max-plus-one", [item])["attachment_refs"][0]
        fd = os.open(too_large["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.ftruncate(fd, maximum + 1)
        finally:
            os.close(fd)
        with self.assertRaises(AttachmentError) as caught:
            files.publish(self.scope, too_large["attachment_id"], too_large["stage_path"])
        self.assertEqual(caught.exception.code, "storage_invalid")
        self.assertEqual(self.record(too_large["attachment_id"])["status"], "pending")
        self.assertEqual(self.record(ref["attachment_id"])["status"], "ready")
        print("synthetic_sparse_boundary " + json.dumps({"bytes": maximum,
            "stage_allocated_bytes": stage_stat.st_blocks * 512, "max_plus_one_rejected": True,
            "max_hash_read_bytes": max(read_sizes), "network_100mib": False}))

    def test_get_hash_crossing_worker_expiry_rolls_back_hold_and_returns_no_path(self):
        aid = self.file()
        batch = self.batch()
        before = self.holds()
        original_file = AttachmentStore._file
        def slow_hash(catalog, *args, **kwargs):
            result = original_file(catalog, *args, **kwargs)
            self.now += worker_state.WORKER_LEASE_SECONDS
            return result
        with patch.object(AttachmentStore, "_file", slow_hash):
            result = self.call(batch, "get", {"attachment_id": aid})
        self.error(result, "lease_lost")
        self.assertNotIn("path", json.dumps(result))
        self.assertEqual(self.holds(), before)
        self.assertEqual(self.record(aid)["status"], "ready")

    def _expire_before_commit(self, action, arguments):
        batch = self.batch()
        original_fault = AttachmentStore._fault
        observed = []
        def expiry(catalog, point):
            if point == "transaction_before_commit":
                observed.append(point)
                self.now += worker_state.WORKER_LEASE_SECONDS
            return original_fault(catalog, point)
        with patch.object(AttachmentStore, "_fault", expiry):
            result = self.call(batch, action, arguments)
        self.error(result, "lease_lost")
        self.assertEqual(observed, ["transaction_before_commit"])

    def test_pin_before_commit_expiry_rolls_back_mutation(self):
        aid = self.file()
        self._expire_before_commit("pin", {"attachment_id": aid, "pinned": True})
        self.assertFalse(self.record(aid)["pinned"])

    def test_label_before_commit_expiry_rolls_back_mutation(self):
        aid = self.file()
        text = self.receive(self.message("synthetic-expiring-label", text="这是房东文件"))
        self._expire_before_commit("label", {"text_message_id": text["message_id"], "label": "房东",
            "attachment_ids": [aid]})
        self.assertEqual(self.record(aid)["tags"], [])

    def _media_enospc(self, which):
        body = b"SYNTHETIC ENOSPC original"
        message = self.message("synthetic-enospc-" + which,
            attachments=[("file", body, "SYNTHETIC.bin")])
        injected = []
        actual_fsync, actual_fdopen = media.os.fsync, media.os.fdopen
        def no_space_fsync(fd):
            info = os.fstat(fd)
            if stat.S_ISREG(info.st_mode) and info.st_size == len(body) and not injected:
                injected.append("fsync")
                raise OSError(errno.ENOSPC, "synthetic disk-full at stage fsync")
            return actual_fsync(fd)
        class NoSpaceWriter:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.stream.close()
            def write(self, data):
                injected.append("write")
                raise OSError(errno.ENOSPC, "synthetic disk-full at stage write")
        def no_space_fdopen(fd, mode, *args, **kwargs):
            stream = actual_fdopen(fd, mode, *args, **kwargs)
            return NoSpaceWriter(stream) if mode == "wb" else stream
        target, effect = ("fsync", no_space_fsync) if which == "fsync" else ("fdopen", no_space_fdopen)
        with patch.object(media.os, target, side_effect=effect):
            record = self.receive(message)
        self.assertEqual(injected, [which])
        self.assertEqual(record["media_failed_count"], 1)
        self.assertNotIn("media_paths", record)
        metadata = runtime.catalog(self.store).query(self.scope)["items"][0]
        self.assertNotEqual(metadata["status"], "ready")
        self.assertEqual(metadata["status"], "failed")
        self.assertIsNone(metadata["object_key"])
        self.assertEqual(list((self.base / "attachments/objects").iterdir()), [])

    def test_enospc_during_actual_stage_write_does_not_publish_ready(self):
        self._media_enospc("write")

    def test_enospc_during_actual_stage_fsync_does_not_publish_ready(self):
        self._media_enospc("fsync")


if __name__ == "__main__":
    unittest.main(verbosity=2)
