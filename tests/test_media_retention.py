"""Synthetic offline retention: no real runtime paths or live deletions."""
import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import cli, serve
from wechat_bridge.backend_state import BackendState
from wechat_bridge.media_retention import MediaRetention
from wechat_bridge.store import Store, atomic_write_json


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="media-retention-synthetic-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name) / "runtime"
        self.store = Store(self.base)
        self.addCleanup(self.store.close)
        self.media = self.base / "media"
        self.now = 2000000

    def file(self, name, age=1000000):
        path = self.media / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic" * 10)
        os.utime(path, (self.now-age, self.now-age))
        return path

    def gc(self, **kw):
        return MediaRetention(str(self.base), clock=lambda: self.now, **kw)

    def apply(self, collector=None):
        return (collector or self.gc()).run(apply=True, offline_maintenance=True)

    def snapshot(self):
        return {str(p.relative_to(self.base)): (p.stat().st_mode, p.stat().st_size,
            p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else None)
            for p in self.base.rglob("*")}

    def test_constructor_and_default_dry_run_do_not_create_missing_root(self):
        missing = Path(self.tmp.name) / "missing"
        collector = MediaRetention(str(missing))
        self.assertFalse(missing.exists())
        self.assertEqual(collector.run()["status"], "blocked")
        self.assertFalse(missing.exists())

    def test_dry_run_zero_tree_mutations_and_cli_avoids_store(self):
        self.file("old.bin")
        before = self.snapshot()
        with patch.object(cli, "Store", side_effect=AssertionError("dry-run Store")), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["media-gc", "--base", str(self.base)]), 0)
        self.assertEqual(before, self.snapshot())

    def test_live_store_pin_blocks_apply_even_before_inbox_publication(self):
        path = self.file("unpublished.bin")
        self.assertEqual(self.apply()["status"], "blocked")
        self.assertTrue(path.exists())
        self.store.close()
        self.assertEqual(self.apply()["status"], "applied")
        self.assertFalse(path.exists())

    def test_replacing_global_lock_does_not_hide_live_store_client(self):
        path = self.file("unpublished.bin")
        global_lock = self.base / "state" / "media_usage.lock"
        global_lock.unlink()
        global_lock.write_bytes(b"")
        self.assertEqual(self.apply()["status"], "blocked")
        self.assertTrue(path.exists())
        self.store.close()
        self.assertEqual(self.apply()["deleted_files"], 1)

    def test_default_never_deletes_and_explicit_maintenance_required(self):
        path = self.file("orphan.bin")
        self.store.close()
        self.assertEqual(self.gc().run()["eligible_files"], 1)
        self.assertEqual(self.gc().run(apply=True)["reason"], "maintenance_required")
        self.assertTrue(path.exists())

    def test_all_legacy_managed_worker_and_retry_references_are_pins(self):
        locations = ["inbox/a.json", "backend_inbox/b.json", "outbox/pending/c.json",
                     "outbox/sending/d.json", "outbox/failed/e.json", "outbox/sent/f.json",
                     "state/reply_batches/g.json", "state/worker/h.json"]
        for i, location in enumerate(locations):
            path = self.file("ref%d.bin" % i)
            atomic_write_json(str(self.base / location), {"nested": [{"media_path": str(path)}]})
        staging = self.file(".media-staging")
        orphan = self.file("old-orphan.bin")
        self.store.close()
        report = self.apply()
        self.assertEqual(report["deleted_files"], 1, report)
        self.assertFalse(orphan.exists())
        self.assertTrue(staging.exists())
        self.assertEqual(len(list(self.media.iterdir())), len(locations)+1)
        self.assertTrue((self.base / "state" / "received").is_dir())

    def test_managed_db_and_explicit_external_worker_roots_protect_media(self):
        a, b, orphan = [self.file(n) for n in ("db.bin", "worker.bin", "orphan.bin")]
        dbpath = self.base / "state" / "backend_jobs.sqlite3"
        BackendState(self.store, {"synthetic": True}, "owner", "bot")
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("INSERT INTO inputs VALUES (?,?,?,?,?)", ("synthetic1", "owner", "message", None,
                       json.dumps({"media_paths": [str(a)]})))
        external = Path(self.tmp.name) / "synthetic-worker"
        external.mkdir()
        atomic_write_json(str(external / "session.json"), {"attachments": [str(b)]})
        self.store.close()
        before = self.snapshot()
        collector = self.gc(worker_roots=[str(external)])
        self.assertEqual(collector.run()["eligible_files"], 1)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.apply(collector)["deleted_files"], 1)
        self.assertTrue(a.exists() and b.exists())
        self.assertFalse(orphan.exists())

    def test_wal_or_bad_reference_or_scan_limit_blocks_all(self):
        path = self.file("old.bin")
        self.store.close()
        wal = self.base / "state" / "backend_jobs.sqlite3-wal"
        wal.write_bytes(b"synthetic")
        self.assertEqual(self.apply()["status"], "blocked")
        wal.unlink()
        bad = self.base / "outbox" / "failed" / "bad.json"
        bad.write_text("{broken")
        self.assertEqual(self.apply()["status"], "blocked")
        bad.unlink()
        with patch("wechat_bridge.media_retention.MAX_FILES", 2):
            self.assertEqual(self.apply()["status"], "blocked")
        self.assertTrue(path.exists())

    def test_unknown_db_schema_and_sqlite_magic_any_extension_block_all(self):
        path = self.file("old.bin")
        self.store.close()
        dbpath = self.base / "state" / "backend_jobs.sqlite3"
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("CREATE TABLE jobs(payload TEXT, attachment TEXT)")
            db.execute("INSERT INTO jobs VALUES (?,?)", ("{}", str(path)))
        self.assertEqual(self.apply()["status"], "blocked")
        dbpath.unlink()
        BackendState(self.store, {"synthetic": True}, "owner", "bot")
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("ALTER TABLE jobs ADD COLUMN attachment TEXT")
            db.execute("INSERT INTO jobs VALUES (?,?,?,?,?)", ("synthetic", "owner", "unknown", "{}", str(path)))
        self.assertEqual(self.apply()["status"], "blocked")
        dbpath.unlink()
        for name in ("worker.sqlite", "worker-no-extension", "worker.data"):
            other = self.base / "state" / name
            with sqlite3.connect(str(other)) as db:
                db.execute("CREATE TABLE attachments(path TEXT)")
                db.execute("INSERT INTO attachments VALUES (?)", (str(path),))
            self.assertEqual(self.apply()["status"], "blocked")
            self.assertTrue(path.exists())
            other.unlink()

    def test_symlink_and_hardlink_block_without_touching_external_target(self):
        external = Path(self.tmp.name) / "outside.bin"
        external.write_bytes(b"do-not-touch")
        link = self.media / "link.bin"
        link.symlink_to(external)
        self.store.close()
        self.assertEqual(self.apply()["status"], "blocked")
        link.unlink()
        os.link(external, link)
        self.assertEqual(self.apply()["status"], "blocked")
        self.assertEqual(external.read_bytes(), b"do-not-touch")

    def test_quota_never_overrides_references_recent_files_or_staging(self):
        pinned, recent, staging = self.file("pinned.bin"), self.file("recent.bin", 1), self.file(".stage")
        atomic_write_json(str(self.base / "inbox" / "a.json"), {"media_paths": [str(pinned)]})
        self.store.close()
        report = self.apply(self.gc(quota_bytes=0))
        self.assertEqual(report["deleted_files"], 0)
        self.assertGreater(report["quota_unreclaimable_bytes"], 0)
        self.assertTrue(all(p.exists() for p in (pinned, recent, staging)))

    def test_ambiguous_relative_media_reference_blocks_all(self):
        path = self.file("old.bin")
        atomic_write_json(str(self.base / "outbox" / "failed" / "retry.json"), {"media_path": "old.bin"})
        self.store.close()
        self.assertEqual(self.apply()["status"], "blocked")
        self.assertTrue(path.exists())

    def test_replaced_lock_and_changed_reference_fail_closed(self):
        path = self.file("old.bin")
        self.store.close()
        collector = self.gc()
        original = collector._scan
        count = [0]
        def changed():
            result = original()
            count[0] += 1
            if count[0] == 2:
                pin = self.base / "state" / "media_usage.lock"
                pin.unlink()
                pin.write_bytes(b"")
            return result
        with patch.object(collector, "_scan", changed):
            self.assertEqual(self.apply(collector)["status"], "blocked")
        self.assertTrue(path.exists())

    def test_download_to_publication_gap_is_guarded(self):
        ready, finish = threading.Event(), threading.Event()
        def download(media, key, dest, **kw):
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(b"synthetic")
            os.utime(dest, (1, 1))
            ready.set()
            finish.wait(3)
        message = {"message_id": "synthetic1", "message_type": 1, "from_user_id": "owner",
            "item_list": [{"type": 2, "image_item": {"media": {}}}]}
        with patch.object(serve, "download_media", download), contextlib.redirect_stdout(io.StringIO()):
            worker = threading.Thread(target=serve.handle_incoming, args=(self.store, message, "owner"))
            worker.start()
            self.assertTrue(ready.wait(2))
            self.assertEqual(self.apply()["status"], "blocked")
            finish.set()
            worker.join(3)
        self.store.close()
        self.assertEqual(self.apply()["deleted_files"], 0)


if __name__ == "__main__":
    unittest.main()
