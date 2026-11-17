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

    def _retention_schema_fixture(self, *, legacy=False, allow_null_json=False):
        """Temporary scan fixtures, not executable PEPE attachment tasks."""
        dbpath = self.base / "state" / "backend_jobs.sqlite3"
        if dbpath.exists():
            dbpath.unlink()
        BackendState(self.store, {"synthetic": True}, "owner", "bot")
        with sqlite3.connect(str(dbpath)) as db:
            if legacy or allow_null_json:
                db.execute("DROP TABLE remote_tasks")
            if allow_null_json:
                # Keep the known names/types/order but permit SQL NULL so the
                # reference reader, rather than SQLite's writer, faces it.
                db.execute("CREATE TABLE remote_tasks (execution_id TEXT, scope_id TEXT, "
                           "request_key TEXT, task_id TEXT, identity TEXT, payload TEXT)")
        return dbpath

    def _retention_close_for_offline_scan(self):
        # Apply may open these existing coordination files; precreate them so
        # complete before/after tree equality also detects unintended writes.
        for name in ("media_usage.lock", "serve.lock", "message_receipts.lock", "backend-worker.lock"):
            path = self.base / "state" / name
            if not path.exists():
                path.touch(mode=0o600)
        self.store.close()

    def _assert_retention_blocked_without_mutation(self, collector=None):
        collector = collector or self.gc()
        before = self.snapshot()
        preview = collector.run()
        self.assertEqual(preview["status"], "blocked", preview)
        self.assertEqual(preview["deleted_files"], 0, preview)
        self.assertEqual(self.snapshot(), before)
        applied = self.apply(collector)
        self.assertEqual(applied["status"], "blocked", applied)
        self.assertEqual(applied["deleted_files"], 0, applied)
        self.assertEqual(self.snapshot(), before)

    def test_known_eight_table_empty_remote_journal_allows_offline_gc(self):
        orphan = self.file("empty-journal-orphan.bin")
        dbpath = self._retention_schema_fixture()
        self._retention_close_for_offline_scan()
        database_before = dbpath.read_bytes()
        before = self.snapshot()
        preview = self.gc().run()
        self.assertEqual(preview["status"], "planned", preview)
        self.assertEqual(preview["eligible_files"], 1, preview)
        self.assertEqual(self.snapshot(), before)
        applied = self.apply()
        self.assertEqual(applied["status"], "applied", applied)
        self.assertEqual(applied["deleted_files"], 1, applied)
        self.assertFalse(orphan.exists())
        self.assertEqual(dbpath.read_bytes(), database_before)

    def test_legacy_seven_table_database_is_scanned_without_migration(self):
        pin, orphan = self.file("legacy-db-pin.bin"), self.file("legacy-db-orphan.bin")
        dbpath = self._retention_schema_fixture(legacy=True)
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("INSERT INTO inputs VALUES (?,?,?,?,?)", (
                "legacy-input", "owner", "message", None,
                json.dumps({"nested": [{"text": str(pin)}]})))
            before_tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(len(before_tables), 7)
        self.assertNotIn("remote_tasks", before_tables)
        self._retention_close_for_offline_scan()
        database_before = dbpath.read_bytes()
        before = self.snapshot()
        preview = self.gc().run()
        self.assertEqual(preview["status"], "planned", preview)
        self.assertEqual(preview["eligible_files"], 1, preview)
        self.assertEqual(self.snapshot(), before)
        applied = self.apply()
        self.assertEqual(applied["status"], "applied", applied)
        self.assertEqual(applied["deleted_files"], 1, applied)
        self.assertTrue(pin.exists())
        self.assertFalse(orphan.exists())
        self.assertEqual(dbpath.read_bytes(), database_before)
        with sqlite3.connect(str(dbpath)) as db:
            self.assertEqual({row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}, before_tables)

    def test_all_remote_journal_stages_and_existing_db_json_columns_pin_media(self):
        dbpath = self._retention_schema_fixture()
        pins = []
        with sqlite3.connect(str(dbpath)) as db:
            for stage in ("prepared", "submitting", "bound"):
                payload_pin = self.file(stage + "-payload-text-pin.bin")
                identity_pin = self.file(stage + "-identity-only-pin.bin")
                pins.extend((payload_pin, identity_pin))
                # The arbitrary nested strings deliberately test conservative
                # scanning, without claiming PEPE supports attachments.
                identity = {"nested": [{"identity_only": {"text": str(identity_pin)}}]}
                payload = {"state": stage, "nested": {"messages": [{"content": str(payload_pin)}]}}
                db.execute("INSERT INTO remote_tasks VALUES (?,?,?,?,?,?)", (
                    "fixture-" + stage, "synthetic-scope", "request-" + stage,
                    "task-bound" if stage == "bound" else None,
                    json.dumps(identity), json.dumps(payload)))
            for table in ("jobs", "inputs", "outputs"):
                pin = self.file(table + "-unpublished-reference.bin")
                pins.append(pin)
                raw = json.dumps({"nested": [{"text": str(pin)}]})
                if table == "jobs":
                    db.execute("INSERT INTO jobs VALUES (?,?,?,?)", ("fixture-job", "owner", "queued", raw))
                elif table == "inputs":
                    db.execute("INSERT INTO inputs VALUES (?,?,?,?,?)", (
                        "fixture-input", "owner", "message", None, raw))
                else:
                    db.execute("INSERT INTO outputs VALUES (?,?,?)", ("fixture-output", "prepared", raw))
        orphan = self.file("remote-db-orphan.bin")
        self._retention_close_for_offline_scan()
        database_before = dbpath.read_bytes()
        before = self.snapshot()
        preview = self.gc().run()
        self.assertEqual(preview["status"], "planned", preview)
        self.assertEqual(preview["eligible_files"], 1, preview)
        self.assertEqual(self.snapshot(), before)
        applied = self.apply()
        self.assertEqual(applied["status"], "applied", applied)
        self.assertEqual(applied["deleted_files"], 1, applied)
        self.assertFalse(orphan.exists())
        self.assertTrue(all(path.exists() for path in pins))
        self.assertEqual(len(list(self.media.iterdir())), len(pins))
        self.assertEqual(dbpath.read_bytes(), database_before)

    def test_remote_json_columns_reject_null_bad_objects_duplicates_and_nonfinite(self):
        self.file("bad-remote-json-orphan.bin")
        dbpath = self._retention_schema_fixture(allow_null_json=True)
        self._retention_close_for_offline_scan()
        bad_json = [
            None, "{broken", "[]", '"scalar"', "1", "true", "null",
            '{"same":1,"same":2}', '{"nested":{"same":1,"same":2}}',
            '{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}',
            '{"nested":[{"value":1e999}]}',
        ]
        for column in ("identity", "payload"):
            for raw in bad_json:
                with self.subTest(column=column, raw=raw):
                    values = {"identity": "{}", "payload": "{}"}
                    values[column] = raw
                    with sqlite3.connect(str(dbpath)) as db:
                        db.execute("DELETE FROM remote_tasks")
                        db.execute("INSERT INTO remote_tasks VALUES (?,?,?,?,?,?)", (
                            "invalid-json", "synthetic-scope", "invalid-request", None,
                            values["identity"], values["payload"]))
                    self._assert_retention_blocked_without_mutation()

    def test_known_profiles_still_reject_unknown_schema_order_types_views_and_triggers(self):
        self.file("unknown-schema-orphan.bin")
        generated_pin = self.file("generated-column-only-reference.bin")
        generated_literal = str(generated_pin).replace("'", "''")
        self._retention_close_for_offline_scan()
        changes = [
            ("unknown-table", "CREATE TABLE future_state (value TEXT)"),
            ("unknown-column", "ALTER TABLE jobs ADD COLUMN future_reference TEXT"),
            ("wrong-type", "DROP TABLE jobs; CREATE TABLE jobs "
                           "(id INTEGER, sender TEXT, status TEXT, payload TEXT)"),
            ("wrong-order", "DROP TABLE jobs; CREATE TABLE jobs "
                            "(sender TEXT, id TEXT, status TEXT, payload TEXT)"),
            ("view", "CREATE VIEW future_view AS SELECT 1 AS value"),
            ("trigger", "CREATE TRIGGER future_trigger AFTER INSERT ON jobs BEGIN SELECT 1; END"),
            # table_info omits VIRTUAL generated columns, although SELECT *
            # exposes this sole reference after the known ordinary columns.
            ("generated-job-column", "ALTER TABLE jobs ADD COLUMN future_reference TEXT "
                                     "GENERATED ALWAYS AS ('%s') VIRTUAL; "
                                     "INSERT INTO jobs (id,sender,status,payload) "
                                     "VALUES ('generated-job','owner','queued','{}')" % generated_literal),
        ]
        for legacy in (True, False):
            for label, script in changes:
                with self.subTest(legacy=legacy, change=label):
                    dbpath = self._retention_schema_fixture(legacy=legacy)
                    with sqlite3.connect(str(dbpath)) as db:
                        db.executescript(script)
                    self._assert_retention_blocked_without_mutation()
        remote_changes = [
            ("remote-column", "ALTER TABLE remote_tasks ADD COLUMN future_reference TEXT"),
            ("remote-type", "DROP TABLE remote_tasks; CREATE TABLE remote_tasks "
                            "(execution_id TEXT, scope_id TEXT, request_key TEXT, task_id TEXT, "
                            "identity BLOB, payload TEXT)"),
            ("remote-order", "DROP TABLE remote_tasks; CREATE TABLE remote_tasks "
                             "(execution_id TEXT, scope_id TEXT, request_key TEXT, identity TEXT, "
                             "task_id TEXT, payload TEXT)"),
            ("remote-generated-column", "ALTER TABLE remote_tasks ADD COLUMN future_reference TEXT "
                                        "GENERATED ALWAYS AS ('%s') VIRTUAL; "
                                        "INSERT INTO remote_tasks "
                                        "(execution_id,scope_id,request_key,task_id,identity,payload) "
                                        "VALUES ('generated-remote','synthetic-scope','generated-request',"
                                        "NULL,'{}','{}')" % generated_literal),
        ]
        for label, script in remote_changes:
            with self.subTest(change=label):
                dbpath = self._retention_schema_fixture()
                with sqlite3.connect(str(dbpath)) as db:
                    db.executescript(script)
                self._assert_retention_blocked_without_mutation()

    def test_remote_profile_keeps_wal_value_budget_and_depth_fail_closed(self):
        self.file("remote-budget-orphan.bin")
        dbpath = self._retention_schema_fixture()
        self._retention_close_for_offline_scan()
        wal = Path(str(dbpath) + "-wal")
        wal.write_bytes(b"synthetic-not-a-live-wal")
        self._assert_retention_blocked_without_mutation()
        wal.unlink()
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("INSERT INTO remote_tasks VALUES (?,?,?,?,?,?)", (
                "budget-fixture", "synthetic-scope", "budget-request", None, "{}",
                json.dumps({"nested": list(range(100))})))
        with patch("wechat_bridge.media_retention.MAX_VALUES", 50):
            self._assert_retention_blocked_without_mutation()
        deep = {"text": "synthetic"}
        for _ in range(18):
            deep = {"nested": deep}
        with sqlite3.connect(str(dbpath)) as db:
            db.execute("UPDATE remote_tasks SET payload=?", (json.dumps(deep),))
        self._assert_retention_blocked_without_mutation()


if __name__ == "__main__":
    unittest.main()
