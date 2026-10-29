"""Synthetic public attachment catalog tests; no provider or real state."""
import copy
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from wechat_bridge.attachments import (AttachmentError, AttachmentStore, DEFAULT_POLICY,
                                       CATALOG_MARKER_NAME, MAX_CATALOG_MARKER_BYTES)


SCOPE = {"bot_id": "synthetic-bot", "owner_id": "synthetic-owner",
         "conversation_id": "synthetic-owner", "login_epoch": "synthetic-epoch"}
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))


class Clock:
    def __init__(self):
        self.value = 1000.0
    def __call__(self):
        return self.value


def item(index=0, kind="file", name=None):
    return {"item_index": index, "kind": kind, "original_name": name, "declared_type": None}


class Attachments(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="attachments-synthetic-")
        self.base = Path(self.temp.name) / "runtime"
        self.clock = Clock()
        self.policy = {"max_bytes": 16, "quota_bytes": 1024, "retention_seconds": 30,
                       "recent_seconds": 24, "ambiguity_seconds": 15, "tombstone_seconds": 30}
        self.catalog = AttachmentStore(str(self.base), self.policy, self.clock)

    def tearDown(self):
        self.temp.cleanup()

    def reject(self, fn, code=None):
        with self.assertRaises(AttachmentError) as error:
            fn()
        if code:
            self.assertEqual(error.exception.code, code)
        return error.exception.code

    def event(self, mid="synthetic-message", items=None, text=""):
        return self.catalog.register_event(SCOPE, mid, [item()] if items is None else items, text=text)

    @staticmethod
    def write(ref, data=b"SYNTHETIC"):
        fd = os.open(ref["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)

    def ready(self, mid="synthetic-message", kind="file", data=b"SYNTHETIC", name=None):
        ref = self.event(mid, [item(kind=kind, name=name)])["attachment_refs"][0]
        self.write(ref, data)
        return self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])

    @staticmethod
    def tree_snapshot(base):
        if not base.exists():
            return None
        result = {}
        for path in [base] + sorted(base.rglob("*")):
            info = path.lstat()
            payload = (os.readlink(str(path)) if stat.S_ISLNK(info.st_mode) else
                       path.read_bytes() if stat.S_ISREG(info.st_mode) else None)
            result[str(path.relative_to(base))] = (info.st_mode, info.st_uid, info.st_nlink,
                                                   info.st_size, info.st_mtime_ns, payload)
        return result

    def hold_rows(self):
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            return {row[0]: (row[1], row[2], row[3], json.loads(row[4]))
                    for row in db.execute("SELECT id,attachment_id,scope,holder,payload FROM holds")}

    def test_constructor_and_empty_read_methods_create_nothing(self):
        self.assertFalse(self.base.exists())
        self.assertFalse(self.catalog.query(SCOPE)["available"])
        self.assertEqual(self.catalog.preview_gc()["status"], "unavailable")
        self.assertFalse(self.base.exists())

    def test_missing_catalog_with_originals_fails_before_sqlite_and_preserves_storage(self):
        self.ready()
        (self.base / "state/attachments.sqlite3").unlink()
        before = self.tree_snapshot(self.base)
        with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=AssertionError("must not open SQLite")):
            for action in (lambda: self.catalog.query(SCOPE), self.catalog.preview_gc,
                           self.event, self.catalog.recover):
                self.reject(action, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_missing_catalog_with_partial_staging_is_not_reported_empty(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        (self.base / "state/attachments.sqlite3").unlink()
        (self.base / "attachments/objects").rmdir()
        before = self.tree_snapshot(self.base)
        with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=AssertionError("must not open SQLite")):
            for action in (lambda: self.catalog.query(SCOPE), self.catalog.preview_gc, self.event):
                self.reject(action, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_existing_catalog_with_partial_directories_is_not_repaired_as_empty(self):
        for missing in ("attachments", "objects", "staging"):
            with self.subTest(missing=missing):
                base = Path(self.temp.name) / ("partial-" + missing)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                catalog.register_event(SCOPE, "synthetic-reservation", [item()])
                if missing == "attachments":
                    (base / "attachments/objects").rmdir()
                    (base / "attachments/staging").rmdir()
                    (base / "attachments" / CATALOG_MARKER_NAME).unlink()
                    (base / "attachments").rmdir()
                else:
                    (base / "attachments" / missing).rmdir()
                before = self.tree_snapshot(base)
                with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=AssertionError("must not open SQLite")):
                    for action in (lambda: catalog.query(SCOPE), catalog.preview_gc,
                                   lambda: catalog.register_event(SCOPE, "synthetic-new", [item()])):
                        self.reject(action, "storage_invalid")
                self.assertEqual(self.tree_snapshot(base), before)

    def test_missing_state_directory_does_not_hide_existing_originals(self):
        self.ready()
        (self.base / "state/attachments.sqlite3").unlink()
        (self.base / "state").rmdir()
        before = self.tree_snapshot(self.base)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.reject(self.catalog.preview_gc, "storage_invalid")
        self.reject(self.event, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_missing_catalog_sidecars_block_without_creating_primary_or_directories(self):
        for suffix in ("-journal", "-wal", "-shm"):
            with self.subTest(suffix=suffix):
                base = Path(self.temp.name) / ("orphan" + suffix)
                (base / "state").mkdir(mode=0o700, parents=True)
                sidecar = base / "state" / ("attachments.sqlite3" + suffix)
                sidecar.write_bytes(b"")
                sidecar.chmod(0o600)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                before = self.tree_snapshot(base)
                with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=AssertionError("must not open SQLite")):
                    for action in (lambda: catalog.query(SCOPE), catalog.preview_gc,
                                   lambda: catalog.register_event(SCOPE, "synthetic-new", [item()])):
                        self.reject(action, "storage_invalid")
                self.assertEqual(self.tree_snapshot(base), before)

    def test_legal_empty_layouts_with_git_and_legacy_state_remain_fresh(self):
        layouts = ((), ("attachments",), ("attachments/objects",),
                   ("attachments/staging",), ("attachments/objects", "attachments/staging"))
        for index, layout in enumerate(layouts):
            with self.subTest(layout=layout):
                base = Path(self.temp.name) / ("empty-%d" % index)
                base.mkdir(mode=0o700)
                (base / ".git").mkdir(mode=0o700)
                (base / "state").mkdir(mode=0o700)
                legacy = base / "state/legacy-state.json"
                legacy.write_bytes(b'{"synthetic":true}')
                legacy.chmod(0o600)
                if layout:
                    (base / "attachments").mkdir(mode=0o700)
                for name in layout:
                    (base / name).mkdir(mode=0o700, parents=True, exist_ok=True)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                before = self.tree_snapshot(base)
                self.assertFalse(catalog.query(SCOPE)["available"])
                self.assertEqual(catalog.preview_gc()["status"], "unavailable")
                self.assertEqual(self.tree_snapshot(base), before)
                event = catalog.register_event(SCOPE, "synthetic-first", [item()])
                self.assertEqual(event["attachment_refs"][0]["status"], "pending")
                self.assertEqual(legacy.read_bytes(), b'{"synthetic":true}')
                self.assertTrue((base / ".git").is_dir())

    def test_missing_catalog_unknown_attachment_root_entry_is_not_fresh(self):
        self.base.mkdir(mode=0o700)
        (self.base / "attachments").mkdir(mode=0o700)
        (self.base / "attachments/synthetic-unknown-empty-dir").mkdir(mode=0o700)
        before = self.tree_snapshot(self.base)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.reject(self.catalog.preview_gc, "storage_invalid")
        self.reject(self.event, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_catalog_disappearance_after_preflight_cannot_rebuild_over_originals(self):
        self.ready()
        original_directories = self.catalog._directories
        def disappear(create):
            handles = original_directories(create)
            (self.base / "state/attachments.sqlite3").unlink()
            return handles
        with patch.object(self.catalog, "_directories", side_effect=disappear):
            self.reject(lambda: self.event("synthetic-late-new"), "storage_invalid")
        self.assertFalse((self.base / "state/attachments.sqlite3").exists())
        self.assertEqual([p.read_bytes() for p in (self.base / "attachments/objects").iterdir()], [b"SYNTHETIC"])

    def test_pending_catalog_has_private_independent_cid_marker_before_any_body(self):
        ref = self.event(items=[item(kind="image")])["attachment_refs"][0]
        self.assertEqual(ref["status"], "pending")
        self.assertEqual(list((self.base / "attachments/objects").iterdir()), [])
        self.assertEqual(list((self.base / "attachments/staging").iterdir()), [])
        marker = self.base / "attachments" / CATALOG_MARKER_NAME
        info = marker.stat()
        self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
        self.assertEqual(info.st_nlink, 1)
        self.assertEqual(info.st_uid, os.geteuid())
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            cid = db.execute("SELECT value FROM metadata WHERE key='catalog_id'").fetchone()[0]
        self.assertEqual(json.loads(marker.read_text()), {"schema_version": 1, "catalog_id": cid})
        self.assertLessEqual(info.st_size, MAX_CATALOG_MARKER_BYTES)

    def test_pending_owned_inbox_snapshot_survives_no_body_index_loss_as_explicit_error(self):
        import importlib
        fixture_module = importlib.import_module("test_attachment_runtime")
        fixture = fixture_module.RuntimeTests(methodName="setUp")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        message = fixture.message("synthetic-before-body")
        scope, event = fixture_module.runtime.receive_event(fixture.store, message,
            fixture_module.OWNER, [item(kind="image")], "", fixture.now)
        self.assertTrue(fixture_module.serve._write_inbox_record(fixture.store, message,
            fixture_module.OWNER, "", attachment_event=event, login_epoch=scope["login_epoch"],
            received_at=fixture.now))
        text = fixture.receive(fixture.message(text="SYNTHETIC first image"))
        self.assertEqual(text["reference_context"]["slots"][0]["attachment_id"], event["attachment_refs"][0]["attachment_id"])
        batch = fixture.batch()
        database = fixture.base / "state/attachments.sqlite3"
        database.unlink()
        self.assertEqual(list((fixture.base / "attachments/objects").iterdir()), [])
        self.assertEqual(list((fixture.base / "attachments/staging").iterdir()), [])
        before = self.tree_snapshot(fixture.base)
        fixture.error(fixture.call(batch, "query"), "storage_invalid")
        restarted = AttachmentStore(str(fixture.base))
        self.reject(lambda: restarted.register_event(scope, "synthetic-after-loss", [item()]), "storage_invalid")
        self.assertFalse(database.exists())
        self.assertEqual(self.tree_snapshot(fixture.base), before)

    def test_empty_catalog_marker_blocks_index_loss_after_restart_and_missing_state(self):
        self.event(items=[])
        (self.base / "state/attachments.sqlite3").unlink()
        (self.base / "state").rmdir()
        before = self.tree_snapshot(self.base)
        restarted = AttachmentStore(str(self.base), self.policy, self.clock)
        for action in (lambda: restarted.query(SCOPE), restarted.preview_gc,
                       lambda: restarted.register_event(SCOPE, "synthetic-after-loss", [])):
            self.reject(action, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)
        self.assertFalse((self.base / "state").exists())

    def test_zero_byte_original_index_loss_keeps_marker_and_original_unchanged(self):
        meta = self.ready(data=b"")
        (self.base / "state/attachments.sqlite3").unlink()
        before = self.tree_snapshot(self.base)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.reject(self.event, "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)
        self.assertEqual((self.base / "attachments/objects" / meta["object_key"]).read_bytes(), b"")

    def test_legacy_catalog_read_does_not_migrate_but_write_adds_same_cid_evidence(self):
        pending = self.event(items=[item(kind="image")])
        context = self.catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC image")["reference_context"]
        marker = self.base / "attachments" / CATALOG_MARKER_NAME
        original = marker.read_bytes()
        marker.unlink()
        before = self.tree_snapshot(self.base)
        legacy = AttachmentStore(str(self.base), self.policy, self.clock)
        self.assertEqual(legacy.query(SCOPE)["items"][0]["attachment_id"], pending["attachment_refs"][0]["attachment_id"])
        self.assertEqual(legacy.resolve_reference(SCOPE, context["snapshot_id"])["reference_context"], context)
        self.assertEqual(legacy.preview_gc()["status"], "planned")
        self.assertEqual(self.tree_snapshot(self.base), before)
        legacy.register_event(SCOPE, "synthetic-next", [])
        self.assertEqual(marker.read_bytes(), original)
        self.assertEqual(legacy.resolve_reference(SCOPE, context["snapshot_id"])["reference_context"], context)

    def test_legacy_known_consumer_refs_block_fresh_even_without_marker_or_body(self):
        cases = (("inbox", {"attachment_refs": [{"attachment_id": "a" * 32, "status": "pending"}]}),
                 ("backend_inbox", {"reference_context": {"snapshot_id": "b" * 32}}),
                 ("state/attachment_admissions", {"attachment_ids": ["c" * 32]}))
        for index, (name, row) in enumerate(cases):
            with self.subTest(consumer=name):
                base = Path(self.temp.name) / ("legacy-evidence-%d" % index)
                base.mkdir(mode=0o700)
                if name.startswith("state/"):
                    (base / "state").mkdir(mode=0o700)
                directory = base / name
                directory.mkdir(mode=0o700)
                path = directory / "synthetic-owned.json"
                fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w") as stream:
                    json.dump(row, stream)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                before = self.tree_snapshot(base)
                with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=AssertionError("must not rebuild SQLite")):
                    for action in (lambda: catalog.query(SCOPE), catalog.preview_gc,
                                   lambda: catalog.register_event(SCOPE, "synthetic-new", [item()])):
                        self.reject(action, "storage_invalid")
                self.assertEqual(self.tree_snapshot(base), before)

    def test_legacy_plain_text_and_media_paths_do_not_claim_catalog_existence(self):
        self.base.mkdir(mode=0o700)
        (self.base / "inbox").mkdir(mode=0o700)
        path = self.base / "inbox/synthetic-legacy.json"
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump({"text": "SYNTHETIC ordinary legacy text", "media_paths": ["/do/not/read/body"],
                       "attachment_refs": [], "reference_context": None}, stream)
        before = self.tree_snapshot(self.base)
        self.assertFalse(self.catalog.query(SCOPE)["available"])
        self.assertEqual(self.tree_snapshot(self.base), before)
        self.assertEqual(self.event()["attachment_refs"][0]["status"], "pending")
        self.assertEqual(path.read_text(), before["inbox/synthetic-legacy.json"][-1].decode())

    def test_catalog_marker_anomalies_are_preserved_and_never_rebound(self):
        cases = (b"", b"not JSON", b'{"schema_version":true,"catalog_id":"x"}',
                 b'{"schema_version":2,"catalog_id":"x"}',
                 b'{"schema_version":1,"catalog_id":"ffffffffffffffffffffffffffffffff"}',
                 b"x" * (MAX_CATALOG_MARKER_BYTES + 1), "permissions", "symlink", "hardlink")
        for index, data in enumerate(cases):
            with self.subTest(anomaly=index):
                base = Path(self.temp.name) / ("bad-marker-%d" % index)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                catalog.register_event(SCOPE, "synthetic-pending", [item()])
                marker = base / "attachments" / CATALOG_MARKER_NAME
                if isinstance(data, bytes):
                    marker.write_bytes(data)
                elif data == "permissions":
                    marker.chmod(0o644)
                else:
                    foreign = Path(self.temp.name) / ("synthetic-foreign-marker-%d" % index)
                    foreign.write_bytes(marker.read_bytes())
                    foreign.chmod(0o600)
                    marker.unlink()
                    if data == "symlink":
                        marker.symlink_to(foreign)
                    else:
                        os.link(str(foreign), str(marker))
                before = self.tree_snapshot(base)
                for action in (lambda: catalog.query(SCOPE), catalog.preview_gc,
                               lambda: catalog.register_event(SCOPE, "synthetic-after-bad-marker", [])):
                    self.reject(action, "storage_invalid")
                self.assertEqual(self.tree_snapshot(base), before)

    def test_marker_write_enospc_precedes_business_and_leaves_no_false_pending(self):
        with patch("wechat_bridge.attachments.os.write", side_effect=OSError(errno.ENOSPC, "SYNTHETIC marker full")):
            self.reject(self.event, "quota_exceeded")
        marker = self.base / "attachments" / CATALOG_MARKER_NAME
        self.assertEqual(marker.read_bytes(), b"")
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM attachments").fetchone()[0], 0)
        before = self.tree_snapshot(self.base)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_real_exit_after_marker_fsync_leaves_durable_evidence_before_pending(self):
        script = """import json,os,sys
sys.path.insert(0,sys.argv[1])
from wechat_bridge.attachments import AttachmentStore
def fault(point):
    if point=='catalog_marker_persisted':os._exit(74)
cat=AttachmentStore(sys.argv[2],json.loads(sys.argv[3]),clock=lambda:1000.0,fault=fault)
cat.register_event(json.loads(sys.argv[4]),'synthetic-before-pending',[{'item_index':0,'kind':'image','original_name':None,'declared_type':None}])
"""
        process = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(ROOT), str(self.base),
            json.dumps(self.policy), json.dumps(SCOPE)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(process.returncode, 74, process.stderr)
        restarted = AttachmentStore(str(self.base), self.policy, self.clock)
        self.assertEqual(restarted.query(SCOPE)["items"], [])
        self.assertTrue((self.base / "attachments" / CATALOG_MARKER_NAME).is_file())
        (self.base / "state/attachments.sqlite3").unlink()
        self.reject(lambda: restarted.query(SCOPE), "storage_invalid")

    def test_legacy_evidence_uncertainty_or_budget_limit_cannot_claim_fresh(self):
        for index, data in enumerate((b"not JSON", b"x" * ((1 << 20) + 1), b'{"attachment_refs":null}')):
            with self.subTest(case=index):
                base = Path(self.temp.name) / ("uncertain-legacy-%d" % index)
                base.mkdir(mode=0o700)
                (base / "inbox").mkdir(mode=0o700)
                path = base / "inbox/synthetic.json"
                path.write_bytes(data)
                path.chmod(0o600)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                before = self.tree_snapshot(base)
                self.reject(lambda: catalog.query(SCOPE), "storage_invalid")
                self.reject(lambda: catalog.register_event(SCOPE, "synthetic-new", []), "storage_invalid")
                self.assertEqual(self.tree_snapshot(base), before)

    def test_zero_ambiguity_policy_and_explicit_zero_headroom_are_valid(self):
        cat = AttachmentStore(str(self.base), {"ambiguity_seconds": 0, "control_reserve_bytes": 0}, self.clock)
        self.assertEqual(cat.policy["ambiguity_seconds"], 0)
        self.assertEqual(cat.policy["control_reserve_bytes"], 0)
        self.assertFalse(self.base.exists())
        for key in ("ambiguity_seconds", "control_reserve_bytes"):
            for value in (True, -1, 1.5):
                self.reject(lambda: AttachmentStore(str(self.base), {key: value}), "invalid_request")

    def test_source_and_runtime_can_share_private_root_with_git_marker(self):
        self.base.mkdir(mode=0o700)
        (self.base / ".git").mkdir(mode=0o700)
        self.assertEqual(self.ready()["status"], "ready")
        self.assertTrue((self.base / ".git").is_dir())

    def test_original_bytes_name_provenance_and_safe_object_key(self):
        name = "../../SYNTHETIC/\n$(not-executed).pdf"
        meta = self.ready(name=name)
        self.assertEqual(meta["original_name"], name)
        self.assertEqual(meta["name_source"], "original_name")
        self.assertEqual(meta["sha256"], hashlib.sha256(b"SYNTHETIC").hexdigest())
        material = self.catalog.acquire(SCOPE, meta["attachment_id"], "worker:synthetic")
        self.assertEqual(Path(material["path"]).read_bytes(), b"SYNTHETIC")
        self.assertEqual(Path(material["path"]).parent, (self.base / "attachments/objects").resolve())
        self.assertNotIn("SYNTHETIC", Path(material["path"]).name)
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "ready")

    def test_missing_original_name_is_null_and_zero_byte_file_is_retained(self):
        meta = self.ready(data=b"")
        self.assertIsNone(meta["original_name"])
        self.assertEqual(meta["name_source"], "unknown")
        self.assertEqual(meta["size"], 0)
        self.assertEqual(meta["sha256"], hashlib.sha256(b"").hexdigest())
        self.assertEqual(Path(self.catalog.acquire(SCOPE, meta["attachment_id"], "worker:zero")["path"]).read_bytes(), b"")

    def test_replay_is_same_id_but_different_messages_are_separate_records(self):
        first = self.event()
        self.assertEqual(self.event(), first)
        self.write(first["attachment_refs"][0])
        a = self.catalog.publish(SCOPE, first["attachment_refs"][0]["attachment_id"], first["attachment_refs"][0]["stage_path"])
        again = self.event()
        self.assertEqual(again["attachment_refs"][0]["attachment_id"], a["attachment_id"])
        self.assertEqual(again["attachment_refs"][0]["status"], "ready")
        b = self.ready("synthetic-second")
        self.assertNotEqual(a["attachment_id"], b["attachment_id"])
        self.assertEqual(a["sha256"], b["sha256"])
        self.reject(lambda: self.event(items=[item(name="changed")]), "invalid_request")

    def test_quota_reserves_before_download_and_failure_releases_reservation(self):
        self.catalog = AttachmentStore(str(self.base), {"max_bytes": 8, "quota_bytes": 16}, self.clock)
        refs = self.event(items=[item(i) for i in range(3)])["attachment_refs"]
        self.assertEqual([r["status"] for r in refs], ["pending", "pending", "quota"])
        self.assertFalse(any(Path(r["stage_path"]).exists() for r in refs))
        self.assertEqual(self.catalog.preview_gc()["reserved_bytes"], 16)
        self.catalog.fail(SCOPE, refs[0]["attachment_id"], "request_or_http_error")
        self.assertEqual(self.catalog.preview_gc()["reserved_bytes"], 8)
        self.assertEqual(self.event("synthetic-next")["attachment_refs"][0]["status"], "pending")

    def test_concurrent_reservations_cannot_both_spend_last_capacity(self):
        policy = {"max_bytes": 16, "quota_bytes": 16}
        self.catalog = AttachmentStore(str(self.base), policy, self.clock)
        self.catalog.register_event(SCOPE, "synthetic-bootstrap", [], text="SYNTHETIC bootstrap")
        def reserve(index):
            cat = AttachmentStore(str(self.base), policy, self.clock)
            return cat.register_event(SCOPE, "synthetic-%d" % index, [item()])["attachment_refs"][0]["status"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, range(2)))
        self.assertEqual(sorted(results), ["pending", "quota"])
        self.assertEqual(self.catalog.preview_gc()["reserved_bytes"], 16)

    def test_pin_does_not_exempt_bytes_from_capacity(self):
        self.catalog = AttachmentStore(str(self.base), {"max_bytes": 16, "quota_bytes": 16}, self.clock)
        meta = self.ready(data=b"x" * 16)
        self.catalog.pin(SCOPE, meta["attachment_id"])
        self.assertEqual(self.event("synthetic-full")["attachment_refs"][0]["status"], "quota")
        self.assertEqual(self.catalog.preview_gc()["physical_bytes"], 16)

    def test_disk_free_space_also_bounds_reservation(self):
        class SmallDisk:
            f_bavail, f_frsize = 1, 1
        with patch("wechat_bridge.attachments.os.fstatvfs", return_value=SmallDisk()):
            self.assertEqual(self.event()["attachment_refs"][0]["status"], "quota")
        self.assertTrue(self.catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC query")["schema_version"])

    def test_control_metadata_headroom_exact_boundary(self):
        reserve = DEFAULT_POLICY["control_reserve_bytes"]
        self.assertEqual(reserve, 129 << 20)
        class Disk:
            f_bavail, f_frsize = reserve + 15, 1
        with patch("wechat_bridge.attachments.os.fstatvfs", return_value=Disk()):
            self.assertEqual(self.event("synthetic-short")["attachment_refs"][0]["status"], "quota")
        Disk.f_bavail += 1
        with patch("wechat_bridge.attachments.os.fstatvfs", return_value=Disk()):
            self.assertEqual(self.event("synthetic-exact")["attachment_refs"][0]["status"], "pending")
            self.assertEqual(self.event("synthetic-next")["attachment_refs"][0]["status"], "quota")
            self.assertTrue(self.catalog.register_event(SCOPE, "synthetic-control", [], text="SYNTHETIC control")["schema_version"])

    def test_concurrent_disk_reservations_include_unwritten_bytes(self):
        self.catalog.register_event(SCOPE, "synthetic-bootstrap", [], text="SYNTHETIC bootstrap")
        class Disk:
            f_bavail, f_frsize = DEFAULT_POLICY["control_reserve_bytes"] + 16, 1
        def reserve(index):
            other = AttachmentStore(str(self.base), self.policy, self.clock)
            return other.register_event(SCOPE, "synthetic-disk-%d" % index, [item()])["attachment_refs"][0]["status"]
        with patch("wechat_bridge.attachments.os.fstatvfs", return_value=Disk()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                statuses = list(pool.map(reserve, range(2)))
        self.assertEqual(sorted(statuses), ["pending", "quota"])
        self.assertEqual(self.catalog.preview_gc()["reserved_bytes"], 16)

    def test_stage_consumer_enospc_keeps_reservation_and_never_publishes(self):
        ref = self.event()["attachment_refs"][0]
        fd = os.open(ref["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            # The catalog gives the receiver this stage path. This synthetic
            # consumer failure is not a test of media.py's integrated writer.
            with patch("os.write", side_effect=OSError(errno.ENOSPC, "SYNTHETIC stage full")):
                with self.assertRaises(OSError):
                    os.write(fd, b"SYNTHETIC")
        finally:
            os.close(fd)
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.preview_gc()["reserved_bytes"], 16)
        self.reject(lambda: self.catalog.acquire(SCOPE, ref["attachment_id"], "worker:unsaved"), "processing")
        self.assertIsNone(self.catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC text")["reference_context"])

    def test_stage_file_fsync_enospc_does_not_commit_prepared_or_free_reservation(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        original_fsync = os.fsync
        def full_file(fd):
            if stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(errno.ENOSPC, "SYNTHETIC fsync full")
            return original_fsync(fd)
        with patch("wechat_bridge.attachments.os.fsync", side_effect=full_file):
            self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.preview_gc()["accounted_bytes"], 16)
        self.assertEqual(self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])["status"], "ready")

    def test_atomic_publication_enospc_leaves_known_prepared_for_recovery(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        with patch("wechat_bridge.attachments.os.rename", side_effect=OSError(errno.ENOSPC, "SYNTHETIC rename full")):
            self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.preview_gc()["accounted_bytes"], 9)
        self.reject(lambda: self.catalog.acquire(SCOPE, ref["attachment_id"], "worker:prepared"), "processing")
        self.assertEqual(self.catalog.recover()["recovered"], 1)
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "ready")

    def test_sqlite_commit_full_rolls_back_without_false_ready(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        original_connect = sqlite3.connect
        class FailingCommit:
            def __init__(self, native):
                self.native = native
            def __getattr__(self, name):
                return getattr(self.native, name)
            def commit(self):
                raise sqlite3.OperationalError("database or disk is full")
        with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=lambda *a, **k: FailingCommit(original_connect(*a, **k))):
            self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.preview_gc()["accounted_bytes"], 16)
        self.assertEqual(self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])["status"], "ready")

    def test_directory_fsync_full_after_rename_recovers_same_object(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        objects_inode = (self.base / "attachments/objects").stat().st_ino
        original_fsync = os.fsync
        def fail_object_directory(fd):
            info = os.fstat(fd)
            if stat.S_ISDIR(info.st_mode) and info.st_ino == objects_inode:
                raise OSError(errno.ENOSPC, "SYNTHETIC directory fsync full")
            return original_fsync(fd)
        with patch("wechat_bridge.attachments.os.fsync", side_effect=fail_object_directory):
            self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.recover()["recovered"], 1)
        self.assertEqual(Path(self.catalog.acquire(SCOPE, ref["attachment_id"], "worker:recovered")["path"]).read_bytes(), b"SYNTHETIC")

    def test_final_sqlite_commit_full_keeps_prepared_object_for_recovery(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        original_connect, count = sqlite3.connect, [0]
        class FailingCommit:
            def __init__(self, native):
                self.native = native
            def __getattr__(self, name):
                return getattr(self.native, name)
            def commit(self):
                count[0] += 1
                if count[0] == 2:
                    raise sqlite3.OperationalError("database or disk is full")
                return self.native.commit()
        with patch("wechat_bridge.attachments.sqlite3.connect", side_effect=lambda *a, **k: FailingCommit(original_connect(*a, **k))):
            self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.recover()["recovered"], 1)
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "ready")

    def test_exact_size_boundary_and_oversize_rejection_preserves_pending(self):
        self.ready(data=b"x" * 16)
        ref = self.event("synthetic-large")["attachment_refs"][0]
        self.write(ref, b"x" * 17)
        self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "storage_invalid")
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-large"})["items"][0]["status"], "pending")

    def test_pending_first_failure_keeps_second_original_ordinal(self):
        e = self.event(items=[item(i, "image") for i in range(9)])
        refs = e["attachment_refs"]
        text = self.catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC second image")
        ctx = text["reference_context"]
        self.assertEqual([s["ordinal"] for s in ctx["slots"]], list(range(1, 10)))
        self.assertTrue(all(s["status"] == "pending" for s in ctx["slots"]))
        self.catalog.fail(SCOPE, refs[0]["attachment_id"], "decrypt_ambiguous")
        self.write(refs[1])
        self.catalog.publish(SCOPE, refs[1]["attachment_id"], refs[1]["stage_path"], preview_state="readable", detected_type="image/png")
        result = self.catalog.resolve_reference(SCOPE, ctx["snapshot_id"], [1, 2], holder="worker:images")
        self.assertEqual(result["unavailable"][0]["ordinal"], 1)
        self.assertEqual(result["materials"][0]["ordinal"], 2)
        self.assertEqual(result["materials"][0]["attachment_id"], refs[1]["attachment_id"])
        self.assertFalse(result["complete"])

    def test_text_cutoff_snapshot_does_not_follow_later_group(self):
        first = self.event(items=[item(kind="image")])
        bound = self.catalog.register_event(SCOPE, "synthetic-question", [], text="SYNTHETIC image")["reference_context"]
        self.clock.value += 1
        second = self.event("synthetic-later", [item(kind="image")])
        later = self.catalog.register_event(SCOPE, "synthetic-question2", [], text="SYNTHETIC later")["reference_context"]
        self.assertNotEqual(bound["group_id"], later["group_id"])
        old = self.catalog.resolve_reference(SCOPE, bound["snapshot_id"])["reference_context"]
        self.assertEqual(old["slots"][0]["attachment_id"], first["attachment_refs"][0]["attachment_id"])
        self.assertNotEqual(old["slots"][0]["attachment_id"], second["attachment_refs"][0]["attachment_id"])
        self.assertEqual(old["cutoff_seq"], bound["cutoff_seq"])

    def test_group_position_limit_survives_increase_and_restart_without_filling_overflow(self):
        catalog = AttachmentStore(str(self.base), dict(self.policy, max_positions=2), self.clock)
        first = catalog.register_event(SCOPE, "synthetic-first", [item(i, "image") for i in range(3)])
        catalog.policy["max_positions"] = 16
        catalog.register_event(SCOPE, "synthetic-fourth", [item(kind="image")])
        restarted = AttachmentStore(str(self.base), dict(self.policy, max_positions=16), self.clock)
        restarted.register_event(SCOPE, "synthetic-fifth", [item(kind="image")])
        context = restarted.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC images")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in context["slots"]], [1, 2])
        self.assertEqual([slot["attachment_id"] for slot in context["slots"]],
                         [ref["attachment_id"] for ref in first["attachment_refs"][:2]])
        self.assertEqual(context["total_positions"], 5)
        self.assertTrue(context["overflow"])
        self.assertNotIn("position_limit", context)
        self.reject(lambda: restarted.resolve_reference(SCOPE, context["snapshot_id"], [3]), "not_found")
        new = restarted.register_event(SCOPE, "synthetic-next-images", [item(i, "image") for i in range(3)],
                                        text="SYNTHETIC new group")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in new["slots"]], [1, 2, 3])
        self.assertFalse(new["overflow"])
        self.assertEqual(restarted.resolve_reference(SCOPE, context["snapshot_id"])["reference_context"], context)

    def test_group_position_limit_survives_decrease_until_new_group(self):
        catalog = AttachmentStore(str(self.base), dict(self.policy, max_positions=16), self.clock)
        catalog.register_event(SCOPE, "synthetic-first", [item(i, "image") for i in range(3)])
        catalog.policy["max_positions"] = 1
        catalog.register_event(SCOPE, "synthetic-fourth", [item(kind="image")])
        restarted = AttachmentStore(str(self.base), dict(self.policy, max_positions=1), self.clock)
        context = restarted.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC old group")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in context["slots"]], [1, 2, 3, 4])
        self.assertFalse(context["overflow"])
        result = restarted.resolve_reference(SCOPE, context["snapshot_id"], [1, 4])
        self.assertEqual([slot["ordinal"] for slot in result["unavailable"]], [1, 4])
        new = restarted.register_event(SCOPE, "synthetic-next-images", [item(i, "image") for i in range(2)],
                                        text="SYNTHETIC new group")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in new["slots"]], [1])
        self.assertTrue(new["overflow"])
        self.assertEqual(restarted.resolve_reference(SCOPE, context["snapshot_id"])["reference_context"], context)

    def test_old_snapshot_pending_to_ready_survives_lower_policy_and_later_group(self):
        catalog = AttachmentStore(str(self.base), self.policy, self.clock)
        refs = catalog.register_event(SCOPE, "synthetic-first", [item(i, "image") for i in range(3)])["attachment_refs"]
        context = catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC third image")["reference_context"]
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            frozen = db.execute("SELECT payload FROM snapshots WHERE id=?", (context["snapshot_id"],)).fetchone()[0]
        restarted = AttachmentStore(str(self.base), dict(self.policy, max_positions=1), self.clock)
        self.write(refs[2])
        restarted.publish(SCOPE, refs[2]["attachment_id"], refs[2]["stage_path"])
        result = restarted.resolve_reference(SCOPE, context["snapshot_id"], [3], holder="worker:synthetic")
        self.assertEqual(result["materials"][0]["attachment_id"], refs[2]["attachment_id"])
        self.assertEqual(result["materials"][0]["ordinal"], 3)
        self.assertEqual([slot["status"] for slot in result["reference_context"]["slots"]], ["pending", "pending", "ready"])
        restarted.register_event(SCOPE, "synthetic-new", [item(i, "image") for i in range(2)], text="SYNTHETIC later")
        self.assertEqual(restarted.resolve_reference(SCOPE, context["snapshot_id"], [3])["reference_context"], result["reference_context"])
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT payload FROM snapshots WHERE id=?", (context["snapshot_id"],)).fetchone()[0], frozen)

    def test_legacy_overflow_group_keeps_original_prefix_after_reconfiguration(self):
        catalog = AttachmentStore(str(self.base), dict(self.policy, max_positions=2), self.clock)
        catalog.register_event(SCOPE, "synthetic-first", [item(i, "image") for i in range(3)])
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            gid, raw = db.execute("SELECT id,payload FROM image_groups").fetchone()
            legacy = json.loads(raw)
            legacy.pop("position_limit")
            db.execute("UPDATE image_groups SET payload=? WHERE id=?", (json.dumps(legacy), gid))
        restarted = AttachmentStore(str(self.base), dict(self.policy, max_positions=16), self.clock)
        restarted.register_event(SCOPE, "synthetic-fourth", [item(kind="image")])
        context = restarted.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC legacy")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in context["slots"]], [1, 2])
        self.assertEqual(context["total_positions"], 4)
        self.assertTrue(context["overflow"])
        lowered = AttachmentStore(str(self.base), dict(self.policy, max_positions=1), self.clock)
        self.assertEqual(lowered.resolve_reference(SCOPE, context["snapshot_id"], [2])["unavailable"][0]["ordinal"], 2)
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            self.assertEqual(json.loads(db.execute("SELECT payload FROM image_groups WHERE id=?", (gid,)).fetchone()[0])["position_limit"], 2)

    def test_legacy_snapshot_read_is_nonmutating_and_uses_protocol_bound(self):
        catalog = AttachmentStore(str(self.base), self.policy, self.clock)
        context = catalog.register_event(SCOPE, "synthetic-images", [item(i, "image") for i in range(3)],
                                           text="SYNTHETIC legacy")["reference_context"]
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            group = json.loads(db.execute("SELECT payload FROM image_groups").fetchone()[0])
            group.pop("position_limit")
            db.execute("UPDATE image_groups SET payload=?", (json.dumps(group),))
        before = self.tree_snapshot(self.base)
        lowered = AttachmentStore(str(self.base), dict(self.policy, max_positions=1), self.clock)
        self.assertEqual([slot["ordinal"] for slot in lowered.resolve_reference(SCOPE, context["snapshot_id"], [1, 3])["unavailable"]], [1, 3])
        self.assertEqual(lowered.preview_gc()["status"], "planned")
        self.assertEqual(self.tree_snapshot(self.base), before)
        self.reject(lambda: lowered.resolve_reference(SCOPE, context["snapshot_id"], [17]), "invalid_request")
        self.reject(lambda: lowered.resolve_reference(SCOPE, context["snapshot_id"], [4]), "not_found")

    def test_legacy_active_nonoverflow_group_cannot_recover_unknown_old_policy(self):
        catalog = AttachmentStore(str(self.base), dict(self.policy, max_positions=2), self.clock)
        catalog.register_event(SCOPE, "synthetic-first", [item(kind="image")])
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            group = json.loads(db.execute("SELECT payload FROM image_groups").fetchone()[0])
            group.pop("position_limit")
            db.execute("UPDATE image_groups SET payload=?", (json.dumps(group),))
        lowered = AttachmentStore(str(self.base), dict(self.policy, max_positions=1), self.clock)
        context = lowered.register_event(SCOPE, "synthetic-more", [item(i, "image") for i in range(3)],
                                         text="SYNTHETIC legacy")["reference_context"]
        self.assertEqual([slot["ordinal"] for slot in context["slots"]], [1, 2, 3, 4])
        self.assertFalse(context["overflow"])

    def test_private_position_limit_rejects_invalid_type_bounds_or_layout(self):
        for index, limit in enumerate((True, 0, 17, 1)):
            with self.subTest(limit=limit):
                base = Path(self.temp.name) / ("invalid-limit-%d" % index)
                catalog = AttachmentStore(str(base), self.policy, self.clock)
                catalog.register_event(SCOPE, "synthetic-images", [item(i, "image") for i in range(2)])
                with sqlite3.connect(str(base / "state/attachments.sqlite3")) as db:
                    group = json.loads(db.execute("SELECT payload FROM image_groups").fetchone()[0])
                    group["position_limit"] = limit
                    db.execute("UPDATE image_groups SET payload=?", (json.dumps(group),))
                self.reject(catalog.preview_gc, "storage_invalid")

    def test_event_with_text_binds_its_own_images_before_closing_group(self):
        result = self.event(items=[item(2, "image"), item(5, "file"), item(6, "image")], text="SYNTHETIC these")
        ctx = result["reference_context"]
        self.assertEqual([s["item_index"] for s in ctx["slots"]], [2, 6])
        self.assertEqual(ctx["text_message_id"], "synthetic-message")
        self.assertEqual(ctx["login_epoch"], SCOPE["login_epoch"])
        later = self.event("synthetic-later", [item(kind="image")], text="SYNTHETIC next")["reference_context"]
        self.assertNotEqual(later["group_id"], ctx["group_id"])

    def test_consecutive_image_events_and_restart_preserve_group(self):
        self.event(items=[item(kind="image")])
        self.catalog = AttachmentStore(str(self.base), self.policy, self.clock)
        self.event("synthetic-two", [item(3, "image")])
        ctx = self.catalog.register_event(SCOPE, "synthetic-question", [], text="SYNTHETIC both")["reference_context"]
        self.assertEqual(ctx["total_positions"], 2)
        self.assertEqual([s["message_id"] for s in ctx["slots"]], ["synthetic-message", "synthetic-two"])

    def test_overflow_is_explicit_and_originals_remain_queryable(self):
        event = self.event(items=[item(i, "image") for i in range(18)], text="SYNTHETIC too many")
        ctx = event["reference_context"]
        self.assertEqual(len(event["attachment_refs"]), 18)
        self.assertEqual(len(ctx["slots"]), 16)
        self.assertEqual(ctx["total_positions"], 18)
        self.assertTrue(ctx["overflow"])
        self.assertEqual(len(self.catalog.query(SCOPE, limit=100)["items"]), 18)
        self.reject(lambda: self.catalog.resolve_reference(SCOPE, ctx["snapshot_id"], [17]), "invalid_request")

    def test_reference_expiry_and_epoch_are_separate_from_longterm_ownership(self):
        ready = self.ready(kind="image")
        ctx = self.catalog.register_event(SCOPE, "synthetic-question", [], text="SYNTHETIC image")["reference_context"]
        other_epoch = dict(SCOPE, login_epoch="synthetic-new-epoch")
        self.reject(lambda: self.catalog.resolve_reference(other_epoch, ctx["snapshot_id"]), "epoch_mismatch")
        self.assertEqual(self.catalog.query(other_epoch)["items"][0]["attachment_id"], ready["attachment_id"])
        self.assertTrue(self.catalog.acquire(other_epoch, ready["attachment_id"], "worker:new")["path"])
        self.clock.value = ctx["expires_at"]
        self.reject(lambda: self.catalog.resolve_reference(SCOPE, ctx["snapshot_id"]), "snapshot_expired")

    def test_scope_isolation_and_paths_only_after_acquire(self):
        ready = self.ready()
        query = self.catalog.query(SCOPE)["items"][0]
        self.assertNotIn("path", query)
        foreign = dict(SCOPE, bot_id="synthetic-foreign")
        self.assertEqual(self.catalog.query(foreign)["items"], [])
        self.reject(lambda: self.catalog.acquire(foreign, ready["attachment_id"], "worker:foreign"), "scope_mismatch")
        self.reject(lambda: self.catalog.pin(foreign, ready["attachment_id"]), "scope_mismatch")

    def test_paging_and_cursor_filter_binding(self):
        for index in range(5):
            self.ready("synthetic-%d" % index, name="SYNTHETIC.pdf")
        page = self.catalog.query(SCOPE, {"name": "synthetic"}, limit=2)
        self.assertFalse(page["complete"])
        page2 = self.catalog.query(SCOPE, {"name": "synthetic"}, limit=2, cursor=page["next_cursor"])
        self.assertTrue(set(i["attachment_id"] for i in page["items"]).isdisjoint(i["attachment_id"] for i in page2["items"]))
        self.reject(lambda: self.catalog.query(SCOPE, {"name": "other"}, cursor=page["next_cursor"]), "invalid_request")
        self.reject(lambda: self.catalog.query(dict(SCOPE, owner_id="other"), {"name": "synthetic"}, cursor=page["next_cursor"]), "invalid_request")

    def test_paging_cutoff_does_not_repeat_rows_when_new_events_arrive(self):
        original = {self.ready("synthetic-%d" % index)["attachment_id"] for index in range(4)}
        first = self.catalog.query(SCOPE, limit=2)
        self.ready("synthetic-newer")
        second = self.catalog.query(SCOPE, limit=2, cursor=first["next_cursor"])
        ids = [row["attachment_id"] for row in first["items"] + second["items"]]
        self.assertEqual(set(ids), original)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(second["complete"])

    def test_tags_have_sources_and_limits(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        tags = [{"value": "SYNTHETIC landlord", "source": "user", "message_id": "synthetic-message"}]
        meta = self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=tags)
        self.assertEqual(meta["tags"], tags)
        self.assertEqual(len(self.catalog.query(SCOPE, {"tag": "LANDLORD"})["items"]), 1)
        bad = [{"value": "SYNTHETIC", "source": "confirmed_by_model", "message_id": None}]
        self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=bad), "invalid_request")
        self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=[dict(tags[0], value="x" * 129)]), "invalid_request")

    def test_pending_explicit_label_survives_publish_and_does_not_change_reference(self):
        ref = self.event(items=[item(kind="image")])["attachment_refs"][0]
        ctx = self.catalog.register_event(SCOPE, "synthetic-label-text", [], text="SYNTHETIC 这是房东图")["reference_context"]
        labelled = self.catalog.label(SCOPE, [ref["attachment_id"]], "房东", "synthetic-label-text")
        tag = {"value": "房东", "source": "user", "message_id": "synthetic-label-text"}
        self.assertEqual(labelled[0]["status"], "pending")
        self.assertEqual(labelled[0]["tags"], [tag])
        self.assertEqual(self.catalog.resolve_reference(SCOPE, ctx["snapshot_id"])["reference_context"], ctx)
        self.write(ref)
        automatic = {"value": "SYNTHETIC caption", "source": "batch_text", "message_id": "synthetic-message"}
        ready = self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=[automatic])
        self.assertEqual(ready["tags"], [tag, automatic])
        found = self.catalog.query(SCOPE, {"tag": "房东"})["items"]
        self.assertEqual([row["attachment_id"] for row in found], [ref["attachment_id"]])
        self.assertEqual(found[0]["status"], "ready")

    def test_explicit_label_historical_epoch_and_atomic_foreign_rejection(self):
        original = self.ready()
        current = dict(SCOPE, login_epoch="synthetic-new-login")
        result = self.catalog.label(current, [original["attachment_id"]], "SYNTHETIC historical", "synthetic-current-text")
        self.assertEqual(result[0]["received_epoch"], SCOPE["login_epoch"])
        self.assertEqual(result[0]["tags"][0]["message_id"], "synthetic-current-text")
        foreign = dict(SCOPE, owner_id="synthetic-foreign", conversation_id="synthetic-foreign")
        other = self.catalog.register_event(foreign, "synthetic-foreign-message", [item()])["attachment_refs"][0]
        self.reject(lambda: self.catalog.label(current, [original["attachment_id"], other["attachment_id"]],
            "SYNTHETIC forbidden", "synthetic-current-text"), "scope_mismatch")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["tags"], result[0]["tags"])
        self.assertEqual(self.catalog.query(foreign)["items"][0]["tags"], [])

    def test_explicit_label_idempotency_and_capacity_are_whole_batch(self):
        first = self.event("synthetic-empty")["attachment_refs"][0]
        full = self.event("synthetic-full")["attachment_refs"][0]
        self.write(full)
        existing = [{"value": "SYNTHETIC-%d" % i, "source": "user", "message_id": "synthetic-source"} for i in range(32)]
        self.catalog.publish(SCOPE, full["attachment_id"], full["stage_path"], tags=existing)
        same = self.catalog.label(SCOPE, [full["attachment_id"]], "SYNTHETIC-0", "synthetic-source")
        self.assertEqual(same[0]["tags"], existing)
        self.reject(lambda: self.catalog.label(SCOPE, [first["attachment_id"], full["attachment_id"]],
            "SYNTHETIC extra", "synthetic-new-source"), "quota_exceeded")
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-empty"})["items"][0]["tags"], [])
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-full"})["items"][0]["tags"], existing)

    def test_explicit_label_response_budget_rolls_back_every_selected_record(self):
        refs = self.event(items=[item(i, name="n" * 4096) for i in range(16)])["attachment_refs"]
        ids = [ref["attachment_id"] for ref in refs]
        self.reject(lambda: self.catalog.label(SCOPE, ids, "SYNTHETIC label", "synthetic-label-source",
            response_bytes=64000), "response_too_large")
        self.assertTrue(all(not row["tags"] for row in self.catalog.query(SCOPE, limit=100)["items"]))
        self.assertEqual(len(self.catalog.label(SCOPE, ids[:1], "SYNTHETIC label", "synthetic-label-source", response_bytes=64000)), 1)

    def test_optional_long_or_absent_batch_caption_does_not_gate_original(self):
        first = self.event()["attachment_refs"][0]
        self.write(first)
        supplied = [{"value": "x" * 129, "source": "batch_text", "message_id": "synthetic-message"},
                    {"value": "", "source": "batch_text", "message_id": "synthetic-message"},
                    {"value": "SYNTHETIC bounded", "source": "batch_text", "message_id": "synthetic-message"}]
        saved = self.catalog.publish(SCOPE, first["attachment_id"], first["stage_path"], tags=supplied)
        self.assertEqual(saved["status"], "ready")
        self.assertEqual(saved["tags"], supplied[2:])
        second = self.event("synthetic-no-caption")["attachment_refs"][0]
        self.write(second)
        self.assertEqual(self.catalog.publish(SCOPE, second["attachment_id"], second["stage_path"], tags=None)["tags"], [])

    def test_optional_batch_tags_never_replace_full_explicit_provenance(self):
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        explicit = [{"value": "SYNTHETIC-%d" % i, "source": "user", "message_id": "synthetic-source"} for i in range(32)]
        automatic = {"value": "SYNTHETIC optional", "source": "batch_text", "message_id": "synthetic-message"}
        result = self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=[automatic] + explicit)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["tags"], explicit)
        again = self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"], tags=[explicit[0], automatic])
        self.assertEqual(again["tags"], explicit)

    def test_explicit_label_strict_types_and_bounds(self):
        ref = self.event()["attachment_refs"][0]
        aid = ref["attachment_id"]
        invalid = (([], "SYNTHETIC"), ([aid, aid], "SYNTHETIC"), ([aid] * 17, "SYNTHETIC"),
                   ([True], "SYNTHETIC"), ([aid], ""), ([aid], " " * 3), ([aid], "x" * 129), ([aid], True))
        for ids, label in invalid:
            self.reject(lambda: self.catalog.label(SCOPE, ids, label, "synthetic-source"), "invalid_request")
        self.reject(lambda: self.catalog.label(SCOPE, [aid], "SYNTHETIC", "synthetic-source", response_bytes=True), "invalid_request")
        self.assertEqual(self.catalog.label(SCOPE, [aid], "x" * 128, "synthetic-source")[0]["tags"][0]["value"], "x" * 128)

    def test_explicit_label_fault_before_commit_is_not_partial(self):
        refs = self.event(items=[item(i) for i in range(2)])["attachment_refs"]
        def stop(point):
            if point == "label_before_commit":
                raise RuntimeError("SYNTHETIC label transaction interruption")
        failing = AttachmentStore(str(self.base), self.policy, self.clock, fault=stop)
        with self.assertRaises(RuntimeError):
            failing.label(SCOPE, [ref["attachment_id"] for ref in refs], "SYNTHETIC label", "synthetic-source")
        self.assertTrue(all(not row["tags"] for row in self.catalog.query(SCOPE)["items"]))

    def test_hold_identity_release_tombstone_and_fixed_expiry(self):
        ready = self.ready()
        one = self.catalog.acquire(SCOPE, ready["attachment_id"], "worker:one", ttl_seconds=10)
        self.clock.value += 2
        again = self.catalog.acquire(SCOPE, ready["attachment_id"], "worker:one", ttl_seconds=100)
        self.assertEqual(one["hold_id"], again["hold_id"])
        self.assertEqual(one["expires_at"], again["expires_at"])
        self.reject(lambda: self.catalog.release(SCOPE, one["hold_id"], holder="worker:two"), "hold_forbidden")
        self.catalog.release(SCOPE, one["hold_id"], holder="worker:one")
        self.catalog.release(SCOPE, one["hold_id"], holder="worker:one")
        self.reject(lambda: self.catalog.release(SCOPE, one["hold_id"], holder="worker:two"), "hold_forbidden")
        self.reject(lambda: self.catalog.release(SCOPE, "f" * 32, holder="worker:one"), "hold_forbidden")

    def test_expired_ordinary_hold_is_not_protection_but_release_keeps_owner(self):
        ready = self.ready()
        hold = self.catalog.acquire(SCOPE, ready["attachment_id"], "worker:one", ttl_seconds=1)
        self.clock.value += 40
        self.assertEqual(self.catalog.preview_gc()["items"][0]["reason"], "expired")
        self.reject(lambda: self.catalog.release(SCOPE, hold["hold_id"], holder="worker:other"), "hold_forbidden")
        self.catalog.release(SCOPE, hold["hold_id"], holder="worker:one")
        self.catalog.release(SCOPE, hold["hold_id"], holder="worker:one")
        self.reject(lambda: self.catalog.release(dict(SCOPE, login_epoch="synthetic-other"), hold["hold_id"], holder="worker:one"), "hold_forbidden")

    def test_delivery_release_is_exact_idempotent_and_does_not_open_originals(self):
        a, b, other = (self.ready("synthetic-a"), self.ready("synthetic-b"),
                       self.ready("synthetic-other"))
        group, request = "ag_synthetic", "synthetic-request"
        targets = []
        for meta in (a, b):
            for holder in ("group:request:" + request, "group:" + group):
                targets.append(self.catalog.acquire(SCOPE, meta["attachment_id"], holder)["hold_id"])
        for holder in ("worker:synthetic", "group:other", "group:request:synthetic-request-extra"):
            self.catalog.acquire(SCOPE, a["attachment_id"], holder)
        self.catalog.acquire(SCOPE, other["attachment_id"], "group:" + group)
        self.catalog.pin(SCOPE, a["attachment_id"])
        (self.base / "attachments/objects" / a["object_key"]).unlink()
        before = self.hold_rows()
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            metadata = list(db.execute("SELECT * FROM attachments ORDER BY id"))
        with patch.object(self.catalog, "_file", side_effect=AssertionError("must not hash originals")), \
                patch.object(self.catalog, "acquire", side_effect=AssertionError("must not acquire")), \
                patch.object(self.catalog, "_acquire", side_effect=AssertionError("must not create holds")):
            result = self.catalog.release_delivery_holds(SCOPE, group, request,
                                                       [a["attachment_id"], b["attachment_id"]])
            self.assertEqual(result, {"released_count": 4})
            released = self.hold_rows()
            self.clock.value += 1
            self.assertEqual(self.catalog.release_delivery_holds(SCOPE, group, request,
                [a["attachment_id"], b["attachment_id"]]), {"released_count": 0})
        self.assertEqual(self.hold_rows(), released)
        self.assertEqual(set(before), set(released))
        for hid, row in before.items():
            if hid in targets:
                expected = copy.deepcopy(row)
                expected[3]["released_at"] = 1000.0
                self.assertEqual(released[hid], expected)
            else:
                self.assertEqual(released[hid], row)
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            self.assertEqual(list(db.execute("SELECT * FROM attachments ORDER BY id")), metadata)
        self.assertEqual((self.base / "attachments/objects" / b["object_key"]).read_bytes(), b"SYNTHETIC")

    def test_delivery_release_matches_original_hold_epoch(self):
        meta = self.ready()
        later = dict(SCOPE, login_epoch="synthetic-later")
        for scope in (SCOPE, later):
            for holder in ("group:request:synthetic-request", "group:ag_synthetic"):
                self.catalog.acquire(scope, meta["attachment_id"], holder)
        self.assertEqual(self.catalog.release_delivery_holds(later, "ag_synthetic", "synthetic-request",
            [meta["attachment_id"]]), {"released_count": 2})
        rows = self.hold_rows()
        self.assertTrue(all((row[3]["released_at"] is None) ==
                            (row[3]["login_epoch"] == SCOPE["login_epoch"]) for row in rows.values()))
        self.assertEqual(self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            [meta["attachment_id"]]), {"released_count": 2})

    def test_delivery_release_strict_inputs_and_cross_owner_are_atomic(self):
        own = self.ready()
        foreign = dict(SCOPE, owner_id="synthetic-other-owner", conversation_id="synthetic-other-owner")
        ref = self.catalog.register_event(foreign, "synthetic-foreign", [item()])["attachment_refs"][0]
        self.write(ref)
        self.catalog.publish(foreign, ref["attachment_id"], ref["stage_path"])
        for scope, aid in ((SCOPE, own["attachment_id"]), (foreign, ref["attachment_id"])):
            self.catalog.acquire(scope, aid, "group:ag_synthetic")
        before = self.hold_rows()
        invalid_ids = ([], (), [True], [own["attachment_id"]] * 2, ["f" * 32] * 17)
        for ids in invalid_ids:
            self.reject(lambda: self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request", ids), "invalid_request")
        for value in (None, True, "", "with space", "../bad", "x" * 257):
            self.reject(lambda: self.catalog.release_delivery_holds(SCOPE, value, "synthetic-request", [own["attachment_id"]]), "invalid_request")
            self.reject(lambda: self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", value, [own["attachment_id"]]), "invalid_request")
        self.reject(lambda: self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            [own["attachment_id"], ref["attachment_id"]]), "scope_mismatch")
        self.reject(lambda: self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            [own["attachment_id"], "f" * 32]), "not_found")
        self.assertEqual(self.hold_rows(), before)

    def test_delivery_release_preserves_pin_worker_and_snapshot_protection(self):
        policy = dict(self.policy, recent_seconds=240)
        catalog = AttachmentStore(str(self.base), policy, self.clock)
        refs = catalog.register_event(SCOPE, "synthetic-images", [item(i, "image") for i in range(3)])["attachment_refs"]
        for ref in refs:
            self.write(ref)
            catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])
        catalog.register_event(SCOPE, "synthetic-text", [], text="SYNTHETIC look at these")
        catalog.pin(SCOPE, refs[0]["attachment_id"])
        worker = catalog.acquire(SCOPE, refs[1]["attachment_id"], "worker:synthetic")
        for ref in refs:
            catalog.acquire(SCOPE, ref["attachment_id"], "group:request:synthetic-request")
            catalog.acquire(SCOPE, ref["attachment_id"], "group:ag_synthetic")
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            snapshots = list(db.execute("SELECT * FROM snapshots"))
        self.clock.value += 40
        self.assertEqual(catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            [ref["attachment_id"] for ref in refs]), {"released_count": 6})
        self.assertIsNone(self.hold_rows()[worker["hold_id"]][3]["released_at"])
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            self.assertEqual(list(db.execute("SELECT * FROM snapshots")), snapshots)
        reasons = {row["attachment_id"]: row["reason"] for row in catalog.preview_gc()["items"]}
        self.assertEqual(reasons, {refs[0]["attachment_id"]: "pinned", refs[1]["attachment_id"]: "in_use",
                                   refs[2]["attachment_id"]: "in_use"})

    def test_delivery_release_empty_store_does_not_initialize_storage(self):
        self.assertEqual(self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            ["f" * 32]), {"released_count": 0})
        self.assertFalse(self.base.exists())
        (self.base / "state").mkdir(mode=0o700, parents=True)
        before = self.tree_snapshot(self.base)
        self.assertEqual(self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            ["f" * 32]), {"released_count": 0})
        self.assertEqual(self.tree_snapshot(self.base), before)

    def test_delivery_release_commit_fault_rolls_back_all_selected_holds(self):
        a, b = self.ready("synthetic-a"), self.ready("synthetic-b")
        for meta in (a, b):
            for holder in ("group:request:synthetic-request", "group:ag_synthetic"):
                self.catalog.acquire(SCOPE, meta["attachment_id"], holder)
        before = self.hold_rows()
        def stop(point):
            if point == "transaction_before_commit":
                raise RuntimeError("SYNTHETIC release commit interruption")
        failing = AttachmentStore(str(self.base), self.policy, self.clock, fault=stop)
        with self.assertRaises(RuntimeError):
            failing.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
                [a["attachment_id"], b["attachment_id"]])
        self.assertEqual(self.hold_rows(), before)
        self.assertEqual(self.catalog.release_delivery_holds(SCOPE, "ag_synthetic", "synthetic-request",
            [a["attachment_id"], b["attachment_id"]]), {"released_count": 4})

    def test_gc_preview_no_write_pin_inuse_expiry_and_persistent_group_hold(self):
        a, b, c = self.ready("synthetic-a"), self.ready("synthetic-b"), self.ready("synthetic-c")
        self.catalog.pin(SCOPE, a["attachment_id"])
        self.catalog.acquire(SCOPE, b["attachment_id"], "group:synthetic-group", ttl_seconds=1)
        self.clock.value += 100
        before = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in self.base.rglob("*") if p.is_file()}
        preview = self.catalog.preview_gc()
        after = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in self.base.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        reasons = {r["attachment_id"]: r["reason"] for r in preview["items"]}
        self.assertEqual(reasons, {a["attachment_id"]: "pinned", b["attachment_id"]: "in_use", c["attachment_id"]: "expired"})
        self.assertEqual(preview["deleted_files"], 0)
        self.assertEqual(preview["eligible_bytes"], c["size"])

    def test_expired_entity_remains_acquirable_and_gains_protection(self):
        meta = self.ready()
        self.clock.value += 40
        self.assertTrue(self.catalog.preview_gc()["items"][0]["eligible"])
        acquired = self.catalog.acquire(SCOPE, meta["attachment_id"], "worker:expired-original")
        self.assertTrue(Path(acquired["path"]).exists())
        self.assertEqual(self.catalog.preview_gc()["items"][0]["reason"], "in_use")

    def test_missing_and_same_size_changed_originals_are_not_returned(self):
        a = self.ready("synthetic-a")
        first = self.catalog.acquire(SCOPE, a["attachment_id"], "worker:read")
        Path(first["path"]).write_bytes(b"DIFFERENT")
        self.reject(lambda: self.catalog.acquire(SCOPE, a["attachment_id"], "worker:changed"), "attachment_unavailable")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "missing")
        b = self.ready("synthetic-b")
        second = self.catalog.acquire(SCOPE, b["attachment_id"], "worker:read")
        Path(second["path"]).unlink()
        self.reject(lambda: self.catalog.acquire(SCOPE, b["attachment_id"], "worker:missing"), "attachment_unavailable")
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-b"})["items"][0]["status"], "missing")

    def test_root_stage_object_and_database_links_are_rejected(self):
        self.base.symlink_to(Path(self.temp.name))
        self.reject(self.event, "storage_invalid")
        self.base.unlink()
        ref = self.event()["attachment_refs"][0]
        foreign = Path(self.temp.name) / "synthetic-foreign"
        foreign.write_bytes(b"SYNTHETIC"); foreign.chmod(0o600)
        Path(ref["stage_path"]).symlink_to(foreign)
        self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "storage_invalid")
        Path(ref["stage_path"]).unlink()
        os.link(str(foreign), ref["stage_path"])
        self.reject(lambda: self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"]), "storage_invalid")
        Path(ref["stage_path"]).unlink()
        self.write(ref)
        meta = self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])
        obj = self.base / "attachments/objects" / meta["object_key"]
        obj.unlink(); obj.symlink_to(foreign)
        self.reject(lambda: self.catalog.acquire(SCOPE, ref["attachment_id"], "worker:link"), "storage_invalid")
        obj.unlink()
        database = self.base / "state/attachments.sqlite3"
        dbdata = database.read_bytes(); database.unlink(); database.symlink_to(foreign)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.assertEqual(foreign.read_bytes(), b"SYNTHETIC")
        self.assertTrue(dbdata)

    def test_unknown_objects_count_toward_quota_and_block_gc_without_deletion(self):
        self.event()
        unknown = self.base / "attachments/objects/synthetic-unknown"
        unknown.write_bytes(b"SYNTHETIC"); unknown.chmod(0o600)
        report = self.catalog.preview_gc()
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["physical_bytes"], 9)
        self.assertEqual(unknown.read_bytes(), b"SYNTHETIC")

    def test_journal_links_and_unexpected_wal_fail_before_sqlite_open(self):
        self.event()
        external = Path(self.temp.name) / "synthetic-protected"
        external.write_bytes(b"SYNTHETIC"); external.chmod(0o600)
        journal = self.base / "state/attachments.sqlite3-journal"
        journal.symlink_to(external)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.assertEqual(external.read_bytes(), b"SYNTHETIC")
        journal.unlink()
        wal = self.base / "state/attachments.sqlite3-wal"
        wal.write_bytes(b""); wal.chmod(0o600)
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")
        self.assertTrue(wal.exists())

    def test_schema_and_input_types_fail_closed(self):
        self.reject(lambda: self.event(items=[item(True)]), "invalid_request")
        self.reject(lambda: self.catalog.query(SCOPE, limit=True), "invalid_request")
        self.reject(lambda: self.catalog.register_event(dict(SCOPE, unexpected="x"), "synthetic", []), "invalid_request")
        self.reject(lambda: self.catalog.register_event(SCOPE, "synthetic", [item(name="x" * 4097)]), "invalid_request")
        self.event()
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            db.execute("CREATE TABLE unknown(payload TEXT)")
        self.reject(lambda: self.catalog.query(SCOPE), "storage_invalid")

    def test_snapshot_version_and_bool_ordinal_are_not_coerced(self):
        for index, field, value in ((0, "schema_version", 2), (1, "ordinal", True)):
            self.event("synthetic-image-%d" % index, [item(kind="image")])
            ctx = self.catalog.register_event(SCOPE, "synthetic-text-%d" % index, [], text="SYNTHETIC image")["reference_context"]
            with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
                raw = json.loads(db.execute("SELECT payload FROM snapshots WHERE id=?", (ctx["snapshot_id"],)).fetchone()[0])
                if field == "ordinal":
                    raw["slots"][0][field] = value
                else:
                    raw[field] = value
                db.execute("UPDATE snapshots SET payload=? WHERE id=?", (json.dumps(raw), ctx["snapshot_id"]))
            self.reject(lambda: self.catalog.resolve_reference(SCOPE, ctx["snapshot_id"]), "storage_invalid")

    def test_readonly_query_preserves_catalog_and_object_bytes_and_mtime(self):
        self.ready()
        before = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in self.base.rglob("*") if p.is_file()}
        self.assertTrue(self.catalog.query(SCOPE)["items"])
        after = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in self.base.rglob("*") if p.is_file()}
        self.assertEqual(before, after)

    def test_prepared_faults_recover_and_do_not_reexecute_source(self):
        for index, point in enumerate(("publish_prepared", "publish_after_replace", "publish_before_commit", "publish_ready")):
            with self.subTest(point=point):
                base = Path(self.temp.name) / ("crash-%d" % index)
                cat = AttachmentStore(str(base), self.policy, self.clock)
                ref = cat.register_event(SCOPE, "synthetic-crash", [item()])["attachment_refs"][0]
                self.write(ref)
                script = """import json,os,sys
sys.path.insert(0,sys.argv[1])
from wechat_bridge.attachments import AttachmentStore
def fault(point):
    if point==sys.argv[5]:os._exit(73)
cat=AttachmentStore(sys.argv[2],json.loads(sys.argv[6]),clock=lambda:1000.0,fault=fault)
cat.publish(json.loads(sys.argv[3]),sys.argv[4],sys.argv[7])
"""
                process = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(ROOT), str(base),
                    json.dumps(SCOPE), ref["attachment_id"], point, json.dumps(self.policy), ref["stage_path"]],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                self.assertEqual(process.returncode, 73, process.stderr)
                reopened = AttachmentStore(str(base), self.policy, self.clock)
                reopened.recover()
                values = reopened.query(SCOPE)["items"]
                self.assertEqual(len(values), 1)
                self.assertEqual(values[0]["attachment_id"], ref["attachment_id"])
                self.assertEqual(values[0]["status"], "ready")
                self.assertEqual(Path(reopened.acquire(SCOPE, ref["attachment_id"], "worker:reopen")["path"]).read_bytes(), b"SYNTHETIC")

    def prepared(self, mid):
        ref = self.event(mid)["attachment_refs"][0]
        self.write(ref)
        def interrupt(point):
            if point == "publish_prepared":
                raise RuntimeError("SYNTHETIC committed prepare interruption")
        interrupted = AttachmentStore(str(self.base), self.policy, self.clock, interrupt)
        with self.assertRaisesRegex(RuntimeError, "committed prepare"):
            interrupted.publish(SCOPE, ref["attachment_id"], ref["stage_path"])
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            raw = db.execute("SELECT payload FROM attachments WHERE id=?", (ref["attachment_id"],)).fetchone()[0]
        record = json.loads(raw)
        self.assertEqual(record["status"], "prepared")
        return ref, self.base / "attachments/objects" / record["object_key"]

    def test_corrupt_prepared_objects_are_missing_and_do_not_block_healthy_recovery(self):
        for suffix, data in (("size", b"BAD"), ("hash", b"CORRUPTED")):
            bad, obj = self.prepared("synthetic-corrupt-" + suffix)
            obj.write_bytes(data)
            obj.chmod(0o600)
            # Even an intact stage is not authority to overwrite a bad object.
            original = (obj.read_bytes(), obj.stat())
            good, _ = self.prepared("synthetic-healthy-" + suffix)
            result = self.catalog.recover()
            self.assertEqual((result["recovered"], result["missing"], result["blocked"]), (1, 1, 0))
            rows = {r["attachment_id"]: r for r in self.catalog.query(SCOPE)["items"]}
            self.assertEqual(rows[bad["attachment_id"]]["status"], "missing")
            self.assertEqual(rows[bad["attachment_id"]]["failure_reason"], "prepared_file_invalid")
            self.assertEqual(rows[good["attachment_id"]]["status"], "ready")
            self.reject(lambda: self.catalog.acquire(SCOPE, bad["attachment_id"], "worker:bad"), "attachment_unavailable")
            self.assertEqual(obj.read_bytes(), original[0])
            self.assertEqual((obj.stat().st_ino, obj.stat().st_mtime_ns),
                             (original[1].st_ino, original[1].st_mtime_ns))
            self.assertEqual(Path(bad["stage_path"]).read_bytes(), b"SYNTHETIC")
            self.assertEqual(self.catalog.recover()["recovered"], 0)

    def test_missing_or_corrupt_prepared_stage_has_nonready_terminal_outcome(self):
        for suffix, data in (("missing", None), ("invalid", b"BAD")):
            bad, _ = self.prepared("synthetic-stage-" + suffix)
            stage = Path(bad["stage_path"])
            if data is None:
                stage.unlink()
            else:
                stage.write_bytes(data)
            good, _ = self.prepared("synthetic-stage-good-" + suffix)
            result = self.catalog.recover()
            self.assertEqual((result["recovered"], result["missing"]), (1, 1))
            row = self.catalog.query(SCOPE, {"message_id": "synthetic-stage-" + suffix})["items"][0]
            self.assertEqual((row["status"], row["failure_reason"]), ("missing", "prepared_file_" + suffix))
            if data is not None:
                self.assertEqual(stage.read_bytes(), data)

    def test_unsafe_prepared_object_is_retained_and_independent_recovery_progresses(self):
        for kind in ("mode", "hardlink", "symlink", "directory", "fifo"):
            with self.subTest(kind=kind):
                bad, obj = self.prepared("synthetic-unsafe-" + kind)
                good, _ = self.prepared("synthetic-safe-" + kind)
                sentinel = self.base / ("synthetic-sentinel-" + kind)
                sentinel.write_bytes(b"SYNTHETIC")
                sentinel.chmod(0o600)
                if kind == "hardlink":
                    os.link(str(sentinel), str(obj))
                elif kind == "symlink":
                    obj.symlink_to(sentinel)
                elif kind == "directory":
                    obj.mkdir(mode=0o700)
                elif kind == "fifo":
                    os.mkfifo(str(obj), 0o600)
                else:
                    obj.write_bytes(b"SYNTHETIC")
                    obj.chmod(0o644)
                before, outside = obj.lstat(), sentinel.stat()
                result = self.catalog.recover()
                # Previous unsafe records remain prepared; none become ready.
                self.assertEqual(result["recovered"], 1)
                self.assertGreaterEqual(result["blocked"], 1)
                rows = {r["attachment_id"]: r for r in self.catalog.query(SCOPE)["items"]}
                self.assertEqual(rows[bad["attachment_id"]]["status"], "pending")
                self.assertEqual(rows[good["attachment_id"]]["status"], "ready")
                self.reject(lambda: self.catalog.acquire(SCOPE, bad["attachment_id"], "worker:unsafe"), "processing")
                after = obj.lstat()
                self.assertEqual((before.st_ino, before.st_mode, before.st_nlink, before.st_mtime_ns),
                                 (after.st_ino, after.st_mode, after.st_nlink, after.st_mtime_ns))
                self.assertEqual((sentinel.read_bytes(), sentinel.stat().st_ino, sentinel.stat().st_mtime_ns),
                                 (b"SYNTHETIC", outside.st_ino, outside.st_mtime_ns))
                # Remove only this test's unsafe entry before creating a new
                # reservation; normal intake still conservatively rejects it.
                if kind == "directory":
                    obj.rmdir()
                else:
                    obj.unlink()
                self.catalog.recover()

    def test_prepared_identity_validation_is_local_but_unknown_io_aborts(self):
        bad, _ = self.prepared("synthetic-identity")
        good, _ = self.prepared("synthetic-after-identity")
        real_file = self.catalog._file
        def unstable(directory, name, *args, **kwargs):
            if name == self.catalog._stage_name(bad["attachment_id"]):
                raise AttachmentError("storage_invalid")
            return real_file(directory, name, *args, **kwargs)
        with patch.object(self.catalog, "_file", side_effect=unstable):
            self.assertEqual(self.catalog.recover()["blocked"], 1)
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-after-identity"})["items"][0]["status"], "ready")
        with patch.object(self.catalog, "_file", side_effect=OSError(errno.EIO, "SYNTHETIC I/O failure")):
            self.reject(self.catalog.recover, "storage_invalid")
        self.assertEqual(self.catalog.query(SCOPE, {"message_id": "synthetic-identity"})["items"][0]["status"], "pending")

    def test_prepared_catalog_and_commit_errors_are_not_file_local(self):
        self.prepared("synthetic-commit-error")
        with patch.object(self.catalog, "_put", side_effect=AttachmentError("storage_invalid")):
            self.reject(self.catalog.recover, "storage_invalid")
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            raw = json.loads(db.execute("SELECT payload FROM attachments").fetchone()[0])
            raw["schema_version"] = 99
            db.execute("UPDATE attachments SET payload=?", (json.dumps(raw),))
        self.reject(self.catalog.recover, "storage_invalid")

    def test_register_and_uncommitted_prepare_rollback_is_visible_pending(self):
        def crash(point):
            if point == "register_before_commit":
                raise RuntimeError("SYNTHETIC local failure")
        broken = AttachmentStore(str(self.base), self.policy, self.clock, crash)
        with self.assertRaises(RuntimeError):
            broken.register_event(SCOPE, "synthetic-crash", [item()])
        self.assertEqual(self.catalog.query(SCOPE)["items"], [])
        ref = self.event()["attachment_refs"][0]
        self.write(ref)
        def prepare_fault(point):
            if point == "publish_prepare_before_commit":
                raise RuntimeError("SYNTHETIC before metadata commit")
        broken.fault = prepare_fault
        with self.assertRaises(RuntimeError):
            broken.publish(SCOPE, ref["attachment_id"], ref["stage_path"])
        self.assertEqual(self.catalog.query(SCOPE)["items"][0]["status"], "pending")
        self.assertEqual(self.catalog.publish(SCOPE, ref["attachment_id"], ref["stage_path"])["status"], "ready")

    def test_recovery_pages_have_progress_cursor(self):
        for index in range(3):
            self.ready("synthetic-%d" % index)
        first = self.catalog.recover(limit=1)
        second = self.catalog.recover(limit=1, cursor=first["next_cursor"])
        third = self.catalog.recover(limit=1, cursor=second["next_cursor"])
        self.assertTrue(first["more"] and second["more"])
        self.assertFalse(third["more"])
        self.assertGreater(second["next_cursor"], first["next_cursor"])


if __name__ == "__main__":
    unittest.main()
