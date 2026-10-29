"""Actual synthetic catalog/ledger integration with conservative GC preview."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import stat
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def deny(*args, **kwargs):
    raise AssertionError("attachment GC tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.create_connection = deny
socket.getaddrinfo = deny
socket.socket.bind = deny
socket.socket.listen = deny

from wechat_bridge.attachments import AttachmentStore
from wechat_bridge.attachment_delivery import DeliveryLedger
from wechat_bridge.media_retention import MediaRetention
from wechat_bridge.store import atomic_write_json


SCOPE = {"bot_id": "synthetic-bot", "owner_id": "synthetic-owner",
         "conversation_id": "synthetic-owner", "login_epoch": "synthetic-epoch"}


class Clock:
    def __init__(self):
        self.value = 1900000000.0

    def __call__(self):
        return self.value


class AttachmentGCTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="attachment-gc-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name) / "runtime"
        self.base.mkdir(mode=0o700)
        (self.base / "state").mkdir(mode=0o700)
        (self.base / "media").mkdir(mode=0o700)
        self.clock = Clock()
        self.policy = {"max_bytes": 1024, "quota_bytes": 32768,
                       "retention_seconds": 30, "recent_seconds": 24,
                       "ambiguity_seconds": 15, "tombstone_seconds": 30,
                       "control_reserve_bytes": 0}
        atomic_write_json(str(self.base / "state/attachment-policy.json"), self.policy)
        self.catalog = AttachmentStore(str(self.base), policy=self.policy, clock=self.clock)
        self.old = self.base / "media/legacy-old.bin"
        self.old.write_bytes(b"SYNTHETIC: legacy bytes")
        os.utime(str(self.old), (self.clock.value - 10000, self.clock.value - 10000))

    def gc(self, base=None, **kwargs):
        return MediaRetention(str(base or self.base), ttl_seconds=60, min_age_seconds=10,
                              clock=self.clock, **kwargs)

    def ready(self, mid, data=b"SYNTHETIC", kind="file", catalog=None):
        catalog = catalog or self.catalog
        event = catalog.register_event(SCOPE, mid, [{"item_index": 0, "kind": kind,
                                                   "original_name": None, "declared_type": None}])
        ref = event["attachment_refs"][0]
        fd = os.open(ref["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
        return catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])

    @staticmethod
    def snapshot(base):
        result = {}
        base = Path(base)
        for path in [base] + sorted(base.rglob("*")):
            info = path.lstat()
            value = hashlib.sha256(path.read_bytes()).hexdigest() if stat.S_ISREG(info.st_mode) else None
            if stat.S_ISLNK(info.st_mode):
                value = os.readlink(str(path))
            result[str(path.relative_to(base))] = (info.st_dev, info.st_ino, info.st_mode,
                info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns, value)
        return result

    def preview_unchanged(self, base=None, **kwargs):
        base = Path(base or self.base)
        before = self.snapshot(base)
        with mock.patch("wechat_bridge.store.Store.__init__", side_effect=AssertionError("Store constructor forbidden")):
            report = self.gc(base, **kwargs).run()
        self.assertEqual(before, self.snapshot(base))
        self.assertTrue(report["dry_run"])
        self.assertEqual(0, report["deleted_files"])
        self.assertEqual(0, report["deleted_bytes"])
        return report

    def test_missing_feature_is_unavailable_without_any_initialization(self):
        report = self.preview_unchanged()
        self.assertEqual("planned", report["status"])
        self.assertEqual(1, report["files"])
        self.assertEqual(1, report["eligible_files"])
        self.assertEqual("unavailable", report["attachment_inbox"]["status"])
        for relative in ("attachments", "state/attachments.sqlite3", "state/attachment-delivery.sqlite3",
                         "state/media_usage.lock", "state/serve.lock"):
            self.assertFalse((self.base / relative).exists())

    def test_catalog_expiry_pin_worker_and_unknown_group_hold_are_separate(self):
        expired = self.ready("synthetic-expired", b"expired")
        pinned = self.ready("synthetic-pinned", b"pinned")
        worker = self.ready("synthetic-worker", b"worker")
        unknown = self.ready("synthetic-unknown", b"unknown")
        self.catalog.pin(SCOPE, pinned["attachment_id"])
        self.catalog.acquire(SCOPE, worker["attachment_id"], "worker:synthetic", ttl_seconds=600)
        ledger = DeliveryLedger(self.base, clock=self.clock)
        ledger.register_context(SCOPE, "synthetic-current-context", fresh=True)
        group = ledger.create_group(SCOPE, "synthetic-return", [unknown["attachment_id"]])
        self.catalog.acquire(SCOPE, unknown["attachment_id"], "group:" + group["group_id"])
        job = ledger.prepare_page(SCOPE, group["group_id"], "synthetic-return", "synthetic-current-context")
        unit = ledger.next_unit(SCOPE, job["job_id"])
        ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        ledger.settle_unit(SCOPE, job["job_id"], unit["unit_id"], "unknown")
        self.clock.value += 31
        report = self.preview_unchanged()
        self.assertEqual("planned", report["status"])
        self.assertEqual((1, len(self.old.read_bytes()), 1), (report["files"], report["bytes"], report["eligible_files"]))
        inbox = report["attachment_inbox"]
        self.assertEqual("planned", inbox["status"])
        self.assertFalse(inbox["delete_enabled"])
        catalog = inbox["catalog"]
        reasons = {item["attachment_id"]: item["reason"] for item in catalog["items"]}
        self.assertEqual("expired", reasons[expired["attachment_id"]])
        self.assertEqual("pinned", reasons[pinned["attachment_id"]])
        self.assertEqual("in_use", reasons[worker["attachment_id"]])
        self.assertEqual("in_use", reasons[unknown["attachment_id"]])
        self.assertEqual(len(b"expired"), catalog["eligible_bytes"])
        self.assertEqual(32768, catalog["quota_bytes"])
        self.assertEqual(3, catalog["protected_files"])
        self.assertEqual(1, inbox["delivery"]["counts"]["groups"]["unknown"])
        self.assertEqual(sum(map(len, (b"expired", b"pinned", b"worker", b"unknown"))), catalog["physical_bytes"])

    def test_attachment_json_and_sqlite_originals_are_not_reference_metadata(self):
        source_json = json.dumps({"media_path": str(self.old)}).encode("utf-8")
        self.ready("synthetic-json", source_json)
        self.ready("synthetic-sqlite", b"SQLite format 3\x00" + b"SYNTHETIC original")
        self.clock.value += 31
        report = self.preview_unchanged()
        self.assertEqual("planned", report["status"])
        self.assertEqual(1, report["eligible_files"])
        self.assertEqual(2, sum(item["eligible"] for item in report["attachment_inbox"]["catalog"]["items"]))
        self.assertEqual(0, report["attachment_inbox"]["catalog"]["protected_files"])

    def test_unknown_objects_and_unsafe_catalog_storage_block_whole_preview(self):
        self.ready("synthetic-one")
        for directory in ("objects", "staging"):
            unknown = self.base / "attachments" / directory / "synthetic-unknown.bin"
            unknown.write_bytes(b"SYNTHETIC: unknown origin")
            unknown.chmod(0o600)
            report = self.preview_unchanged()
            self.assertEqual("blocked", report["status"])
            self.assertEqual("blocked", report["attachment_inbox"]["status"])
            unknown.unlink()
        (self.base / "attachments/unknown-layout").mkdir(mode=0o700)
        self.assertEqual("blocked", self.preview_unchanged()["status"])

    def test_catalog_and_delivery_sidecars_or_unknown_schema_are_never_skipped(self):
        self.ready("synthetic-one")
        DeliveryLedger(self.base, clock=self.clock)
        changes = [(database, suffix) for database in ("attachments.sqlite3", "attachment-delivery.sqlite3")
                   for suffix in ("-wal", "-shm", "-journal")]
        for database, suffix in changes:
            with self.subTest(database=database, suffix=suffix):
                path = self.base / "state" / (database + suffix)
                path.write_bytes(b"")
                self.assertEqual("blocked", self.preview_unchanged()["status"])
                path.unlink()
        for database in ("attachments.sqlite3", "attachment-delivery.sqlite3"):
            clone = Path(self.temp.name) / ("bad-" + database)
            shutil.copytree(str(self.base), str(clone))
            path = clone / "state" / database
            with sqlite3.connect(str(path)) as db:
                db.execute("CREATE TABLE synthetic_unknown(value TEXT)")
            self.assertEqual("blocked", self.preview_unchanged(clone)["status"])

    def test_other_same_named_or_unrecognized_databases_keep_root_blocked(self):
        self.ready("synthetic-one")
        DeliveryLedger(self.base, clock=self.clock)
        for relative in ("state/worker.sqlite3", "other/attachments.sqlite3", "other/attachment-delivery.sqlite3"):
            with self.subTest(relative=relative):
                path = self.base / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(str(path)) as db:
                    db.execute("CREATE TABLE synthetic_unknown(value TEXT)")
                self.assertEqual("blocked", self.preview_unchanged()["status"])
                path.unlink()

    def test_git_root_stays_blocked_even_when_catalog_is_valid(self):
        self.ready("synthetic-one")
        (self.base / ".git").mkdir(mode=0o700)
        self.assertEqual("planned", self.catalog.preview_gc()["status"])
        self.assertEqual("blocked", self.preview_unchanged()["status"])

    def test_apply_is_disabled_for_marker_catalog_delivery_or_partial_storage(self):
        for kind in ("marker", "catalog", "delivery", "partial"):
            with self.subTest(kind=kind):
                base = Path(self.temp.name) / ("apply-" + kind)
                shutil.copytree(str(self.base), str(base))
                if kind == "marker":
                    atomic_write_json(str(base / "state/attachments_enabled.json"), {"schema_version": 1, "enabled_at": self.clock.value})
                elif kind == "catalog":
                    self.ready("synthetic-apply", catalog=AttachmentStore(str(base), policy=self.policy, clock=self.clock))
                elif kind == "delivery":
                    DeliveryLedger(base, clock=self.clock)
                else:
                    (base / "attachments").mkdir(mode=0o700)
                before = self.snapshot(base)
                for maintenance in (False, True):
                    report = self.gc(base).run(apply=True, offline_maintenance=maintenance)
                    self.assertEqual("attachment_delete_disabled", report["reason"])
                    self.assertEqual(0, report["deleted_files"])
                    self.assertEqual(before, self.snapshot(base))

    def test_missing_index_with_orphan_storage_is_blocked_without_initializing(self):
        (self.base / "attachments/objects").mkdir(parents=True, mode=0o700)
        (self.base / "attachments/staging").mkdir(mode=0o700)
        self.assertEqual("blocked", self.preview_unchanged()["status"])
        self.assertFalse((self.base / "state/attachments.sqlite3").exists())

    def test_catalog_identity_marker_is_recognized_by_zero_write_preview(self):
        self.ready("synthetic-catalog-marker")
        marker = self.base / "attachments/catalog.json"
        self.assertTrue(marker.is_file())
        original = marker.read_bytes()
        report = self.preview_unchanged()
        self.assertEqual(report["status"], "planned")
        self.assertEqual(report["attachment_inbox"]["catalog"]["status"], "planned")
        self.assertEqual(original, marker.read_bytes())
        self.assertEqual(self.gc().run(apply=True, offline_maintenance=True)["reason"],
                         "attachment_delete_disabled")

    def test_catalog_identity_mismatch_is_not_blessed_by_layout_recognition(self):
        self.ready("synthetic-catalog-mismatch")
        marker = self.base / "attachments/catalog.json"
        atomic_write_json(str(marker), {"schema_version": 1, "catalog_id": "0" * 32})
        report = self.preview_unchanged()
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["attachment_inbox"]["status"], "blocked")

    def test_pending_history_identity_without_body_blocks_missing_index_preview(self):
        event = self.catalog.register_event(SCOPE, "synthetic-pending-catalog", [
            {"item_index": 0, "kind": "file", "original_name": None, "declared_type": None}])
        self.assertEqual(event["attachment_refs"][0]["status"], "pending")
        self.assertEqual(list((self.base / "attachments/objects").iterdir()), [])
        self.assertEqual(list((self.base / "attachments/staging").iterdir()), [])
        marker = self.base / "attachments/catalog.json"
        original = marker.read_bytes()
        database = self.base / "state/attachments.sqlite3"
        database.unlink()  # Deliberate synthetic index-loss bootstrap only.
        self.assertEqual(self.preview_unchanged()["status"], "blocked")
        self.assertFalse(database.exists())
        self.assertEqual(original, marker.read_bytes())

    def test_known_external_worker_catalog_is_separate_and_forbids_apply(self):
        external = Path(self.temp.name) / "synthetic-external-worker"
        external.mkdir(mode=0o700)
        catalog = AttachmentStore(str(external), policy=self.policy, clock=self.clock)
        self.ready("synthetic-external", catalog=catalog)
        atomic_write_json(str(external / "state/attachment-policy.json"), self.policy)
        before = self.snapshot(external)
        report = self.preview_unchanged(worker_roots=[str(external)])
        self.assertEqual("planned", report["status"])
        self.assertEqual("unavailable", report["attachment_inbox"]["catalog"]["status"])
        self.assertEqual("planned", report["attachment_inbox"]["other_roots"][0]["catalog"]["status"])
        self.assertEqual(before, self.snapshot(external))
        self.assertEqual("attachment_delete_disabled", self.gc(worker_roots=[str(external)]).run(apply=True, offline_maintenance=True)["reason"])
        atomic_write_json(str(self.base / "state/backends.json"), {"legacy_state_dir": str(external)})
        self.assertEqual("planned", self.preview_unchanged()["status"])
        self.assertEqual("attachment_delete_disabled", self.gc().run(apply=True, offline_maintenance=True)["reason"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
