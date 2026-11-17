"""Existing event provenance is read-only and bound to owned inbox records."""
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.attachments import AttachmentError, AttachmentStore
from wechat_bridge.store import Store, atomic_write_json


def deny(*args, **kwargs):
    raise AssertionError("sequence tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny

SCOPE = {"bot_id": "synthetic-bot", "owner_id": "synthetic-owner",
         "conversation_id": "synthetic-owner", "login_epoch": "a" * 32}


class ContextSequences(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="recent-sequence-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve() / "bridge"
        self.catalog = AttachmentStore(str(self.base))

    @staticmethod
    def record(mid, at, scope=SCOPE):
        return {"message_id": mid, "received_at": at,
                "from_user_id": scope["owner_id"], "login_epoch": scope["login_epoch"],
                "text": "SYNTHETIC"}

    def event(self, mid="m1", at=1000.0, scope=SCOPE, text="SYNTHETIC", **kwargs):
        self.catalog.register_event(scope, mid, [], text=text, received_at=at, **kwargs)
        return self.record(mid, at, scope)

    def reject(self, records, scope=SCOPE, code="sequence_unavailable"):
        with self.assertRaises(AttachmentError) as error:
            self.catalog.context_sources(scope, records)
        self.assertEqual(error.exception.code, code)

    def snapshot(self):
        if not self.base.exists():
            return None
        snapshot = {}
        for path in [self.base, *sorted(self.base.rglob("*"))]:
            info = path.lstat()
            body = (os.readlink(path) if stat.S_ISLNK(info.st_mode) else
                    path.read_bytes() if stat.S_ISREG(info.st_mode) else None)
            snapshot[str(path.relative_to(self.base))] = (
                info.st_mode, info.st_nlink, info.st_size, info.st_mtime_ns, body)
        return snapshot

    def counts(self):
        uri = (self.base / "state/attachments.sqlite3").as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True) as db:
            return tuple(db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                         for name in ("events", "attachments", "snapshots", "holds"))

    def mutate(self, **fields):
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            seq, raw = db.execute("SELECT seq,payload FROM events ORDER BY seq LIMIT 1").fetchone()
            event = json.loads(raw)
            event.update(fields)
            db.execute("UPDATE events SET payload=? WHERE seq=?", (json.dumps(event), seq))

    def test_actual_receive_text_events_have_real_order_without_catalog_writes(self):
        store = Store(str(self.base))
        self.addCleanup(store.close)
        atomic_write_json(store.session_path, {"bot_token": "synthetic-only",
            "ilink_user_id": SCOPE["owner_id"], "ilink_bot_id": SCOPE["bot_id"]})
        scope = runtime.initialize(store)
        # Deliberately receive an earlier timestamp second. Order comes from seq.
        for mid, at in (("m2", 1002.0), ("m1", 1001.0)):
            runtime.receive_event(store, {"message_id": mid, "context_token": "synthetic-only"},
                                  scope["owner_id"], [], "SYNTHETIC", at)
        records = [self.record("m1", 1001.0, scope), self.record("m2", 1002.0, scope)]
        before = self.snapshot()
        for _ in range(3):
            result = self.catalog.context_sources(scope, records)
            self.assertEqual([row["message_id"] for row in result], ["m2", "m1"])
            self.assertEqual([row["seq"] for row in result], [1, 2])
            self.assertEqual([row["received_at"] for row in result], [1002.0, 1001.0])
            expected = hashlib.sha256(b'[[],"SYNTHETIC"]').hexdigest()
            self.assertTrue(all(row["fingerprint"] == expected for row in result))
            self.assertEqual(self.counts(), (2, 0, 0, 0))
        self.assertEqual(self.snapshot(), before)

    def test_missing_catalog_lookup_creates_no_directories_or_files(self):
        self.assertFalse(self.base.exists())
        self.reject([self.record("missing", 1000.0)])
        self.assertFalse(self.base.exists())
        self.base.mkdir(mode=0o700)
        before = self.snapshot()
        self.reject([self.record("missing", 1000.0)])
        self.assertEqual(self.snapshot(), before)

    def test_owned_record_scope_epoch_time_and_id_are_checked(self):
        record = self.event()
        before = self.snapshot()
        for field, value in (("login_epoch", "b" * 32), ("from_user_id", "other"),
                             ("received_at", 1000.01), ("received_at", True),
                             ("received_at", float("nan")), ("message_id", "missing"),
                             ("message_id", 1)):
            with self.subTest(field=field, value=value):
                bad = dict(record, **{field: value})
                self.reject([bad])
        self.reject([record], dict(SCOPE, bot_id="other"))
        self.assertEqual(self.snapshot(), before)

    def test_new_epoch_cannot_reuse_old_receive_time_or_v2_epoch(self):
        self.event()
        new = dict(SCOPE, login_epoch="b" * 32)
        self.reject([self.record("m1", 1001.0, new)], new)
        self.event("v2", 1001.0, reference_v2=True)
        self.reject([self.record("v2", 1001.0, new)], new)

    def test_missing_member_does_not_return_partial_sources(self):
        record = self.event()
        before = self.snapshot()
        self.reject([record, self.record("missing", 1001.0)])
        self.assertEqual(self.snapshot(), before)

    def test_exhausted_event_quota_can_still_query_without_new_rows(self):
        records = [self.event("m1", 1000.0), self.event("m2", 1001.0)]
        with patch("wechat_bridge.attachments.MAX_RECORDS", 2):
            with self.assertRaises(AttachmentError) as error:
                self.event("m3", 1002.0)
            self.assertEqual(error.exception.code, "quota_exceeded")
            before = self.snapshot()
            self.assertEqual(len(self.catalog.context_sources(SCOPE, records)), 2)
            self.assertEqual(self.snapshot(), before)
            self.assertEqual(self.counts(), (2, 0, 0, 0))

    def test_lookup_is_parameterized_and_batch_bound_is_strict(self):
        record = self.event()
        before = self.snapshot()
        self.reject([dict(record, message_id="' OR 1=1 --")])
        for records in ([], {}, (record,), [record, record], [record] * 1001):
            with self.subTest(kind=type(records).__name__, size=len(records)):
                self.reject(records)
        self.assertEqual(self.snapshot(), before)

    def test_corrupt_event_sequence_timestamp_and_fingerprint_fail_closed(self):
        record = self.event()
        for field, value in (("schema_version", True), ("seq", True), ("seq", 999),
                             ("received_at", True), ("fingerprint", "x" * 64),
                             ("fingerprint", "a" * 63), ("message_id", "other")):
            with self.subTest(field=field, value=value):
                with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
                    saved = db.execute("SELECT payload FROM events WHERE seq=1").fetchone()[0]
                self.mutate(**{field: value})
                self.reject([record])
                with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
                    db.execute("UPDATE events SET payload=? WHERE seq=1", (saved,))

    def test_one_thousand_record_boundary_is_accepted_before_missing_lookup(self):
        # No catalog exists. Valid input reaches the read-only lookup without
        # initializing it, even at the maximum batch bound.
        records = [self.record("m" + str(index), 1000.0) for index in range(1000)]
        with patch.object(self.catalog, "_db", wraps=self.catalog._db) as reader:
            self.reject(records)
            reader.assert_called_once_with(False, initialize=False)
        self.assertFalse(self.base.exists())


if __name__ == "__main__":
    unittest.main()
