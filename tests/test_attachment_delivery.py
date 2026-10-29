"""Synthetic implementation tests for the actual local delivery ledger."""
import concurrent.futures
import contextlib
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def deny(*args, **kwargs):
    raise AssertionError("synthetic ledger tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.create_connection = deny
socket.getaddrinfo = deny
socket.socket.bind = deny
socket.socket.listen = deny

from wechat_bridge.attachment_delivery import CAP, DeliveryError, DeliveryLedger


SCOPE = {"bot_id": "synthetic-bot", "owner_id": "synthetic-owner",
         "conversation_id": "synthetic-owner", "login_epoch": "synthetic-epoch-a"}
TOKEN = "synthetic-private-current-context"


class Clock:
    def __init__(self):
        self.value = 1900000000.0

    def __call__(self):
        return self.value


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-delivery-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.clock = Clock()
        self.ledger = DeliveryLedger(self.base, clock=self.clock)
        self.ledger.register_context(SCOPE, TOKEN, fresh=True)

    def reject(self, code, function, *args, **kwargs):
        with self.assertRaises(DeliveryError) as caught:
            function(*args, **kwargs)
        self.assertEqual(code, caught.exception.code)

    def scalar(self, query, parameters=()):
        with sqlite3.connect(str(self.ledger.path)) as db:
            return db.execute(query, parameters).fetchone()[0]

    def database_snapshot(self):
        with sqlite3.connect(str(self.ledger.path)) as db:
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            return {name: db.execute("SELECT * FROM " + name + " ORDER BY rowid").fetchall()
                    for (name,) in tables}

    def worker_authority(self, name):
        from wechat_bridge.attachment_runtime import current_scope
        from wechat_bridge.worker_state import WorkerState
        worker = WorkerState(str(self.base / (name + "-worker")),
                             str(self.base / (name + "-hook")), clock=self.clock)
        session = {"ilink_bot_id": SCOPE["bot_id"], "ilink_user_id": SCOPE["owner_id"],
                   "bot_token": "synthetic-bot-token", "login_epoch": "e" * 32}
        Path(worker.store.session_path).write_text(json.dumps(session))
        scope = current_scope(worker.store)
        self.ledger.register_context(scope, TOKEN, fresh=True)
        record = {"message_id": "7002", "from_user_id": scope["owner_id"],
                  "context_token": TOKEN, "text": "SYNTHETIC request"}
        (Path(worker.store.inbox_dir) / "7002.json").write_text(json.dumps(record))
        lease = worker.claim(["7002"])["lease"]
        checks = []
        def authority():
            checks.append(self.clock())
            claim = worker._claims()["7002"]
            if claim["lease"] != lease or not worker._alive("7002", claim):
                raise DeliveryError("lease_lost")
            if current_scope(worker.store) != scope:
                raise DeliveryError("epoch_mismatch")
        def lose(kind):
            if kind == "lease":
                self.clock.value = worker._deadline("7002", worker._claims()["7002"])
            else:
                Path(worker.store.session_path).write_text(json.dumps(dict(session, login_epoch="f" * 32)))
        return scope, authority, lose, checks

    def group(self, count=2, caption="", request="synthetic-request"):
        ids = ["synthetic-attachment-%02d" % i for i in range(count)]
        group = self.ledger.create_group(SCOPE, request, ids, caption)
        return group, self.ledger.prepare_page(SCOPE, group["group_id"], request, TOKEN)

    def send_page(self, job, ledger=None, scope=None):
        ledger, scope = ledger or self.ledger, scope or SCOPE
        sent = []
        while True:
            unit = ledger.next_unit(scope, job["job_id"])
            if unit is None:
                return sent
            self.assertEqual("reserved", unit["state"])
            self.assertTrue(ledger.begin_unit(scope, job["job_id"], unit["unit_id"])["send_allowed"])
            sent.append(unit)
            ledger.settle_unit(scope, job["job_id"], unit["unit_id"], "accepted")

    def accept_generic(self, unit_id, token=TOKEN, scope=None, ledger=None):
        scope, ledger = scope or SCOPE, ledger or self.ledger
        ledger.reserve_units(scope, token, [unit_id])
        self.assertTrue(ledger.begin_reserved(scope, token, unit_id)["send_allowed"])
        ledger.settle_reserved(scope, token, unit_id, "accepted")

    def terminal_catalog_fixture(self):
        from wechat_bridge.attachments import AttachmentStore
        from wechat_bridge.store import atomic_write_json
        files = AttachmentStore(str(self.ledger.base), clock=self.clock,
                                policy={"max_bytes": 1024, "quota_bytes": 16384, "control_reserve_bytes": 0})
        event = files.register_event(SCOPE, "synthetic-enum-source", [{"item_index": 0,
            "kind": "file", "original_name": "SYNTHETIC.bin", "declared_type": None}])
        ref = event["attachment_refs"][0]
        fd = os.open(ref["stage_path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(b"SYNTHETIC ORIGINAL ENUM TEST")
        files.publish(SCOPE, ref["attachment_id"], ref["stage_path"])
        request = "synthetic:enum-request"
        files.acquire(SCOPE, ref["attachment_id"], "group:request:" + request)
        group = self.ledger.create_group(SCOPE, request, [ref["attachment_id"]], "SYNTHETIC caption")
        job = self.ledger.prepare_page(SCOPE, group["group_id"], request, TOKEN)
        self.ledger.cancel_group(SCOPE, group["group_id"])
        directory = self.ledger.base / "state/attachment_admissions"
        directory.mkdir(mode=0o700)
        atomic_write_json(str(directory / (request + ".json")), {
            "schema_version": 1, "type": "attachment_group_v1", "group_id": group["group_id"],
            "job_id": job["job_id"], "request_id": request, "scope": SCOPE,
            "attachment_ids": [ref["attachment_id"]], "admitted": True, "phase": "admitted"})
        store = mock.Mock(state_dir=str(self.ledger.base / "state"),
                          _attachment_runtime_blocked=False, _attachment_terminal_cursor="")
        return files, group, job, store

    def flush_without_http(self, files, store, error=None):
        from wechat_bridge import attachment_runtime as runtime
        with mock.patch.object(runtime, "active", return_value=True), \
                mock.patch.object(runtime, "current_scope", return_value=dict(SCOPE)), \
                mock.patch.object(runtime, "_session", return_value={"bot_token": "synthetic-token"}), \
                mock.patch.object(runtime, "ledger", return_value=self.ledger), \
                mock.patch.object(runtime, "catalog", return_value=files), \
                mock.patch("wechat_bridge.ilink.send_message") as text_send, \
                mock.patch("wechat_bridge.media.send_media_message") as media_send:
            if error is None:
                runtime.flush_groups(store, "https://synthetic.invalid", "synthetic-token")
            else:
                self.reject(error, runtime.flush_groups, store, "https://synthetic.invalid", "synthetic-token")
            self.assertEqual(0, text_send.call_count)
            self.assertEqual(0, media_send.call_count)

    def test_legacy_context_never_becomes_fresh_by_replay(self):
        first = self.ledger.register_context(SCOPE, "synthetic-old")
        again = self.ledger.register_context(SCOPE, "synthetic-old", fresh=True)
        self.assertEqual(first, again)
        self.assertTrue(first["legacy_unknown"])
        self.assertEqual((8, 0), (first["used"], first["remaining"]))
        self.reject("budget_exhausted", self.ledger.reserve_units, SCOPE, "synthetic-old", ["ordinary:0"])
        self.assertEqual(0, self.ledger.register_context(SCOPE, "", fresh=True)["remaining"])

    def test_strict_scope_lists_and_outcome_types(self):
        for bad in ({}, dict(SCOPE, extra="x"), dict(SCOPE, login_epoch=True), dict(SCOPE, bot_id="")):
            self.reject("invalid_request", self.ledger.register_context, bad, TOKEN)
        self.reject("invalid_request", self.ledger.register_context, SCOPE, TOKEN, fresh=1)
        self.reject("invalid_request", self.ledger.reserve_units, SCOPE, TOKEN, ["x", "x"])
        self.reject("invalid_request", self.ledger.reserve_units, SCOPE, TOKEN, ["../../x"])
        self.reject("invalid_request", self.ledger.settle_reserved, SCOPE, TOKEN, "x", {"accepted": True})

    def test_atomic_cap_and_reservation_replay(self):
        reservation = self.ledger.reserve_units(SCOPE, TOKEN, ["legacy:%d" % i for i in range(7)])
        self.assertEqual(1, reservation["remaining"])
        self.reject("budget_exhausted", self.ledger.reserve_units, SCOPE, TOKEN, ["extra:0", "extra:1"])
        self.assertEqual(0, self.scalar("SELECT count(*) FROM reservations WHERE unit_id LIKE 'extra:%'"))
        replay = self.ledger.reserve_units(SCOPE, TOKEN, ["legacy:%d" % i for i in range(7)])
        self.assertEqual(7, replay["used"])
        self.ledger.reserve_units(SCOPE, TOKEN, ["last"])
        self.assertEqual(0, self.ledger.register_context(SCOPE, TOKEN)["remaining"])

    def test_accepted_identity_survives_new_context_and_epoch(self):
        self.accept_generic("stable:accepted")
        second = "synthetic-next-context"
        self.ledger.register_context(SCOPE, second, fresh=True)
        replay = self.ledger.reserve_units(SCOPE, second, ["stable:accepted"])
        self.assertEqual("accepted", replay["units"][0]["state"])
        self.assertEqual(0, replay["used"])
        self.assertFalse(self.ledger.begin_reserved(SCOPE, second, "stable:accepted")["send_allowed"])
        fresh_scope = dict(SCOPE, login_epoch="synthetic-epoch-b")
        current = self.ledger.register_context(fresh_scope, TOKEN, fresh=True)
        self.assertEqual(1, current["used"])
        again = self.ledger.reserve_units(fresh_scope, second, ["stable:accepted"])
        self.assertEqual("accepted", again["units"][0]["state"])
        self.assertFalse(self.ledger.begin_reserved(fresh_scope, second, "stable:accepted")["send_allowed"])
        self.reject("epoch_mismatch", self.ledger.register_context, SCOPE, TOKEN, fresh=True)

    def test_unknown_identity_cannot_escape_through_new_token_or_login(self):
        self.ledger.reserve_units(SCOPE, TOKEN, ["stable:unknown"])
        self.ledger.begin_reserved(SCOPE, TOKEN, "stable:unknown")
        self.ledger.settle_reserved(SCOPE, TOKEN, "stable:unknown", "unknown")
        self.ledger.register_context(SCOPE, "synthetic-next", fresh=True)
        self.reject("group_unknown", self.ledger.reserve_units, SCOPE, "synthetic-next", ["stable:unknown"])
        current = dict(SCOPE, login_epoch="synthetic-epoch-b")
        self.assertEqual(1, self.ledger.register_context(current, TOKEN, fresh=True)["used"])
        self.reject("group_unknown", self.ledger.reserve_units, current, "synthetic-next", ["stable:unknown"])
        self.reject("group_unknown", self.ledger.settle_reserved, current, TOKEN, "stable:unknown", "proven_unsent")

    def test_proven_unsent_refunds_and_conflicts_are_rejected(self):
        self.ledger.reserve_units(SCOPE, TOKEN, ["preflight"])
        self.reject("group_busy", self.ledger.settle_reserved, SCOPE, TOKEN, "preflight", "accepted")
        self.ledger.settle_reserved(SCOPE, TOKEN, "preflight", "proven_unsent")
        self.assertEqual(8, self.ledger.register_context(SCOPE, TOKEN)["remaining"])
        self.ledger.reserve_units(SCOPE, TOKEN, ["preflight"])
        self.ledger.begin_reserved(SCOPE, TOKEN, "preflight")
        self.ledger.settle_reserved(SCOPE, TOKEN, "preflight", "accepted")
        self.ledger.settle_reserved(SCOPE, TOKEN, "preflight", "accepted")
        self.reject("group_busy", self.ledger.settle_reserved, SCOPE, TOKEN, "preflight", "proven_unsent")
        self.assertEqual(1, self.ledger.register_context(SCOPE, TOKEN)["used"])

    def test_concurrent_reservations_cannot_exceed_cap(self):
        def reserve(index):
            ledger = DeliveryLedger(self.base, clock=self.clock)
            try:
                ledger.reserve_units(SCOPE, TOKEN, ["thread:%d:%d" % (index, i) for i in range(5)])
                return "reserved"
            except DeliveryError as error:
                return error.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, [0, 1]))
        self.assertEqual(["budget_exhausted", "reserved"], sorted(results))
        self.assertEqual(5, self.ledger.register_context(SCOPE, TOKEN)["used"])

    def test_only_one_begin_can_authorize_a_network_call(self):
        self.ledger.reserve_units(SCOPE, TOKEN, ["cas:unit"])
        ledgers = [DeliveryLedger(self.base, clock=self.clock), DeliveryLedger(self.base, clock=self.clock)]
        def begin(ledger):
            try:
                return ledger.begin_reserved(SCOPE, TOKEN, "cas:unit")["send_allowed"]
            except DeliveryError as error:
                return error.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(begin, ledgers))
        self.assertEqual(1, results.count(True))
        self.assertEqual(1, results.count("group_busy"))

    def test_sqlite_writer_busy_is_bounded_and_no_partial_reservation(self):
        with sqlite3.connect(str(self.ledger.path), isolation_level=None) as db:
            db.execute("BEGIN IMMEDIATE")
            started = time.monotonic()
            self.reject("storage_busy", self.ledger.reserve_units, SCOPE, TOKEN, ["busy"])
            self.assertLess(time.monotonic() - started, 2)
            db.rollback()
        self.assertEqual(0, self.scalar("SELECT count(*) FROM reservations WHERE unit_id='busy'"))

    def test_create_group_idempotency_order_and_bounds(self):
        ids = ["attachment:%d" % i for i in range(16)]
        group = self.ledger.create_group(SCOPE, "request", ids, "x" * 16000)
        self.assertEqual(group["group_id"], self.ledger.create_group(SCOPE, "request", ids, "x" * 16000)["group_id"])
        self.assertEqual(24, group["units"]["total"])
        self.assertEqual(ids, group["pending_attachment_ids"])
        self.reject("invalid_request", self.ledger.create_group, SCOPE, "request", ids[::-1], "x" * 16000)
        for bad in ([], ["a"] * 2, ids + ["extra"], ["/arbitrary/path"]):
            self.reject("invalid_request", self.ledger.create_group, SCOPE, "bad", bad)
        self.reject("invalid_request", self.ledger.create_group, SCOPE, "bad-caption", ["a"], "x" * 16001)

    def test_trusted_caution_extends_only_internal_payload_and_remains_budgeted(self):
        caption, caution = "x" * 16000, "\nSYNTHETIC CAUTION: " + "!" * 108
        self.assertEqual(128, len(caution))
        group = self.ledger.create_group(SCOPE, "caution:max", ["a"], caption, trusted_caution=caution)
        stored = self.scalar("SELECT caption FROM groups WHERE group_id=?", (group["group_id"],))
        self.assertEqual(caption + caution, stored)
        self.assertEqual(16128, len(stored))
        self.assertEqual(10, group["units"]["total"])
        job = self.ledger.prepare_page(SCOPE, group["group_id"], "caution:max", TOKEN)
        self.assertEqual(8, len(job["units"]))
        self.assertEqual(["text"] * 7 + ["status"], [u["kind"] for u in job["units"]])
        self.send_page(job)
        self.assertEqual(CAP, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.ledger.register_context(SCOPE, "synthetic-caution-next", fresh=True)
        rest = self.ledger.prepare_page(SCOPE, group["group_id"], "caution:continue", "synthetic-caution-next", explicit_continue=True)
        sent = self.send_page(rest)
        self.assertEqual(["text", "text", "attachment"], [u["kind"] for u in sent])
        self.assertEqual(caption + caution, "".join(u["text"] for u in job["units"] + sent if u["kind"] == "text"))
        self.assertEqual("complete", self.ledger.group_status(SCOPE, group["group_id"])["state"])

    def test_trusted_caution_bounds_and_idempotency_cannot_override_request(self):
        caution = "\nSYNTHETIC: verify generated download before use."
        group = self.ledger.create_group(SCOPE, "caution:replay", ["a"], "caption", trusted_caution=caution)
        replay = self.ledger.create_group(SCOPE, "caution:replay", ["a"], "caption", trusted_caution=caution)
        self.assertEqual(group, replay)
        self.reject("invalid_request", self.ledger.create_group, SCOPE, "caution:replay", ["a"], "caption")
        self.reject("invalid_request", self.ledger.create_group, SCOPE, "caution:replay", ["a"], "caption", trusted_caution=caution + "!")
        for bad in ("x" * 129, None, True, ["synthetic"]):
            self.reject("invalid_request", self.ledger.create_group, SCOPE, "caution:bad", ["a"], trusted_caution=bad)
        self.reject("invalid_request", self.ledger.create_group, SCOPE, "caution:public", ["a"], "x" * 16001, trusted_caution=caution)

    def test_group_creation_rolls_back_real_sql_when_worker_lease_or_epoch_changes(self):
        for kind, code in (("lease", "lease_lost"), ("epoch", "epoch_mismatch")):
            with self.subTest(kind=kind):
                self.clock.value = 1900000000.0
                self.ledger = DeliveryLedger(self.base / ("create-" + kind), clock=self.clock)
                scope, authority, lose, checks = self.worker_authority("create-" + kind)
                before = self.database_snapshot()
                active_db, sql_seen = [], []
                transaction = self.ledger._transaction
                @contextlib.contextmanager
                def observe(*args, **kwargs):
                    with transaction(*args, **kwargs) as db:
                        active_db[:] = [db]
                        yield db
                def fault(point):
                    if point == "group_before_commit":
                        db = active_db[0]
                        self.assertEqual(1, db.execute("SELECT count(*) FROM groups").fetchone()[0])
                        self.assertEqual(2, db.execute("SELECT count(*) FROM payload_units").fetchone()[0])
                        sql_seen.append(True)
                        lose(kind)
                self.ledger.fault = fault
                with mock.patch.object(self.ledger, "_transaction", observe):
                    self.reject(code, self.ledger.create_group, scope, "guard:create", ["synthetic:a"],
                                "SYNTHETIC caption", check_authority=authority)
                self.ledger.fault = None
                self.assertEqual([True], sql_seen)
                self.assertEqual(2, len(checks))
                self.assertEqual(before, self.database_snapshot())

    def test_page_precommit_loss_rolls_back_new_page_and_expiry_maintenance_together(self):
        for kind, code in (("lease", "lease_lost"), ("epoch", "epoch_mismatch")):
            with self.subTest(kind=kind):
                self.clock.value = 1900000000.0
                self.ledger = DeliveryLedger(self.base / ("page-" + kind), clock=self.clock)
                scope = dict(SCOPE, login_epoch="e" * 32)
                self.ledger.register_context(scope, TOKEN, fresh=True)
                old = self.ledger.create_group(scope, "guard:old", ["old:a", "old:b", "old:c"])
                old_job = self.ledger.prepare_page(scope, old["group_id"], "guard:old", TOKEN)
                accepted = self.ledger.next_unit(scope, old_job["job_id"])
                self.ledger.begin_unit(scope, old_job["job_id"], accepted["unit_id"])
                self.ledger.settle_unit(scope, old_job["job_id"], accepted["unit_id"], "accepted")
                inflight = self.ledger.next_unit(scope, old_job["job_id"])
                self.ledger.begin_unit(scope, old_job["job_id"], inflight["unit_id"])
                self.clock.value = old_job["expires_at"]
                scope, authority, lose, checks = self.worker_authority("page-" + kind)
                new = self.ledger.create_group(scope, "guard:new", ["new:a", "new:b"])
                before = self.database_snapshot()
                active_db, sql_seen = [], []
                transaction = self.ledger._transaction
                @contextlib.contextmanager
                def observe(*args, **kwargs):
                    with transaction(*args, **kwargs) as db:
                        active_db[:] = [db]
                        yield db
                def fault(point):
                    if point == "page_before_commit":
                        db = active_db[0]
                        self.assertEqual("expired", db.execute("SELECT state FROM jobs WHERE job_id=?", (old_job["job_id"],)).fetchone()[0])
                        self.assertEqual(["accepted", "unknown", "proven_unsent"], [row[0] for row in db.execute(
                            "SELECT state FROM reservations WHERE job_id=? ORDER BY unit_id", (old_job["job_id"],))])
                        self.assertEqual(1, db.execute("SELECT count(*) FROM jobs WHERE group_id=?", (new["group_id"],)).fetchone()[0])
                        sql_seen.append(True)
                        lose(kind)
                self.ledger.fault = fault
                with mock.patch.object(self.ledger, "_transaction", observe):
                    self.reject(code, self.ledger.prepare_page, scope, new["group_id"], "guard:new", TOKEN,
                                check_authority=authority)
                self.ledger.fault = None
                self.assertEqual([True], sql_seen)
                self.assertEqual(2, len(checks))
                self.assertEqual(before, self.database_snapshot())

    def test_group_and_page_authority_prechecks_fail_before_any_database_mutation(self):
        group = self.ledger.create_group(SCOPE, "guard:preexisting", ["synthetic:a"])
        before = self.database_snapshot()
        for function, args in ((self.ledger.create_group, (SCOPE, "guard:denied", ["synthetic:b"])),
                               (self.ledger.prepare_page, (SCOPE, group["group_id"], "guard:preexisting", TOKEN))):
            guard = mock.Mock(side_effect=DeliveryError("lease_lost"))
            self.reject("lease_lost", function, *args, check_authority=guard)
            self.assertEqual(1, guard.call_count)
            self.assertEqual(before, self.database_snapshot())
            self.reject("invalid_request", function, *args, check_authority=True)
            self.assertEqual(before, self.database_snapshot())

    def test_group_and_page_idempotent_returns_still_check_final_authority(self):
        guard = mock.Mock()
        group = self.ledger.create_group(SCOPE, "guard:replay", ["synthetic:a"], check_authority=guard)
        self.assertEqual(2, guard.call_count)
        guard.reset_mock()
        self.assertEqual(group, self.ledger.create_group(SCOPE, "guard:replay", ["synthetic:a"], check_authority=guard))
        self.assertEqual(2, guard.call_count)
        guard.reset_mock()
        job = self.ledger.prepare_page(SCOPE, group["group_id"], "guard:replay", TOKEN, check_authority=guard)
        self.assertEqual(2, guard.call_count)
        guard.reset_mock()
        self.assertEqual(job, self.ledger.prepare_page(SCOPE, group["group_id"], "guard:replay", TOKEN, check_authority=guard))
        self.assertEqual(2, guard.call_count)
        before = self.database_snapshot()
        for function, args in ((self.ledger.create_group, (SCOPE, "guard:replay", ["synthetic:a"])),
                               (self.ledger.prepare_page, (SCOPE, group["group_id"], "guard:replay", TOKEN))):
            guard = mock.Mock(side_effect=[None, DeliveryError("epoch_mismatch")])
            self.reject("epoch_mismatch", function, *args, check_authority=guard)
            self.assertEqual(2, guard.call_count)
            self.assertEqual(before, self.database_snapshot())

    def test_caption_chunks_status_and_multiple_pages_share_physical_cap(self):
        group, job = self.group(16, caption="x" * 4001)
        self.assertEqual(8, len(job["units"]))
        self.assertEqual(["text"] * 3 + ["attachment"] * 4 + ["status"], [u["kind"] for u in job["units"]])
        seen, pages = [], 0
        while True:
            seen.extend(self.send_page(job))
            pages += 1
            status = self.ledger.group_status(SCOPE, group["group_id"])
            if status["state"] == "complete":
                break
            self.assertTrue(status["can_continue"])
            request, token = "synthetic-continue:%d" % pages, "synthetic-context:%d" % pages
            self.ledger.register_context(SCOPE, token, fresh=True)
            self.reject("continue_required", self.ledger.prepare_page, SCOPE, group["group_id"], request, token)
            job = self.ledger.prepare_page(SCOPE, group["group_id"], request, token, explicit_continue=True)
            self.assertLessEqual(len(job["units"]), 8)
        self.assertEqual(3, pages)
        attachments = [u["attachment_id"] for u in seen if u["kind"] == "attachment"]
        self.assertEqual(["synthetic-attachment-%02d" % i for i in range(16)], attachments)
        self.assertEqual(len(seen), len({u["unit_id"] for u in seen}))
        self.assertEqual(21, status["units"]["accepted"])
        self.assertEqual({"total": 21, "accepted": 21, "pending": 0, "unknown": 0}, status["units"])

    def test_partial_failure_preserves_prefix_and_requires_explicit_new_request(self):
        group, job = self.group(3, caption="caption")
        first = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], first["unit_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], first["unit_id"], "accepted")
        failed = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], failed["unit_id"], "proven_unsent")
        status = self.ledger.group_status(SCOPE, group["group_id"])
        self.assertEqual("partial", status["state"])
        self.assertEqual(1, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.assertIsNone(self.ledger.next_unit(SCOPE, job["job_id"]))
        self.ledger.register_context(SCOPE, "synthetic-next", fresh=True)
        self.reject("continue_required", self.ledger.prepare_page, SCOPE, group["group_id"], "different-topic", "synthetic-next")
        continued = self.ledger.prepare_page(SCOPE, group["group_id"], "explicit-continue", "synthetic-next", explicit_continue=True)
        units = self.send_page(continued)
        self.assertEqual(["attachment"] * 3, [u["kind"] for u in units])
        self.assertEqual(failed["unit_id"], units[0]["unit_id"])
        self.assertEqual("complete", self.ledger.group_status(SCOPE, group["group_id"])["state"])

    def test_unknown_caption_blocks_continue_even_with_no_unknown_attachment(self):
        group, job = self.group(2, caption="caption")
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        status = self.ledger.settle_unit(SCOPE, job["job_id"], unit["unit_id"], "unknown")
        self.assertEqual([], status["unknown_attachment_ids"])
        self.assertEqual(1, status["units"]["unknown"])
        self.assertFalse(status["can_continue"])
        self.ledger.register_context(SCOPE, "synthetic-next", fresh=True)
        self.reject("group_unknown", self.ledger.prepare_page, SCOPE, group["group_id"], "continue", "synthetic-next", explicit_continue=True)

    def test_unknown_status_notice_also_blocks_continue(self):
        group, job = self.group(9)
        for _ in range(7):
            unit = self.ledger.next_unit(SCOPE, job["job_id"])
            self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
            self.ledger.settle_unit(SCOPE, job["job_id"], unit["unit_id"], "accepted")
        notice = self.ledger.next_unit(SCOPE, job["job_id"])
        self.assertEqual("status", notice["kind"])
        self.ledger.begin_unit(SCOPE, job["job_id"], notice["unit_id"])
        result = self.ledger.settle_unit(SCOPE, job["job_id"], notice["unit_id"], "unknown")
        self.assertEqual(7, len(result["accepted_attachment_ids"]))
        self.assertEqual([], result["unknown_attachment_ids"])
        self.assertEqual("unknown", result["state"])
        self.assertFalse(result["can_continue"])

    def test_exhausted_budget_parks_zero_unit_job_without_sending_notice(self):
        for i in range(8):
            self.accept_generic("old-text:%d" % i)
        group, job = self.group()
        self.assertEqual("budget_exhausted", job["state"])
        self.assertEqual([], job["units"])
        self.assertNotIn(job["job_id"], self.ledger.pending_jobs(SCOPE)["job_ids"])
        self.assertIsNone(self.ledger.next_unit(SCOPE, job["job_id"]))
        status = self.ledger.group_status(SCOPE, group["group_id"])
        self.assertEqual("awaiting_continue", status["state"])
        self.assertTrue(status["can_continue"])
        self.assertEqual(job, self.ledger.prepare_page(SCOPE, group["group_id"], "synthetic-request", TOKEN))

    def test_single_remaining_unit_can_only_send_budgeted_notice(self):
        for i in range(7):
            self.accept_generic("old-text:%d" % i)
        group, job = self.group()
        self.assertEqual(["status"], [u["kind"] for u in job["units"]])
        self.send_page(job)
        self.assertEqual(8, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.assertEqual([], self.ledger.group_status(SCOPE, group["group_id"])["accepted_attachment_ids"])

    def test_original_authorization_deadline_and_old_request_never_resurrect(self):
        group = self.ledger.create_group(SCOPE, "request", ["a", "b"])
        original = self.clock.value
        self.clock.value += 100
        job = self.ledger.prepare_page(SCOPE, group["group_id"], "request", TOKEN, authorized_at=original)
        self.assertEqual(original + 1800, job["expires_at"])
        self.clock.value = original + 1800
        self.assertEqual([], self.ledger.pending_jobs(SCOPE)["job_ids"])
        replay = self.ledger.prepare_page(SCOPE, group["group_id"], "request", TOKEN)
        self.assertEqual((job["job_id"], job["expires_at"], "expired"), (replay["job_id"], replay["expires_at"], replay["state"]))
        self.reject("expired", self.ledger.prepare_page, SCOPE, group["group_id"], "new-request", TOKEN, explicit_continue=True, authorized_at=original)
        self.ledger.register_context(SCOPE, "synthetic-next", fresh=True)
        new = self.ledger.prepare_page(SCOPE, group["group_id"], "new-request", "synthetic-next", explicit_continue=True)
        self.assertNotEqual(job["job_id"], new["job_id"])
        self.assertEqual(self.clock.value + 1800, new["expires_at"])

    def test_cancel_reserved_releases_budget_and_revokes_claim(self):
        group, job = self.group()
        first = self.ledger.next_unit(SCOPE, job["job_id"])
        status = self.ledger.cancel_group(SCOPE, group["group_id"])
        self.assertEqual("cancelled", status["state"])
        self.assertFalse(status["can_continue"])
        self.assertEqual(0, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.reject("cancelled", self.ledger.begin_unit, SCOPE, job["job_id"], first["unit_id"])
        self.assertEqual(status, self.ledger.cancel_group(SCOPE, group["group_id"]))

    def test_cancel_checks_real_worker_expiry_after_state_calculation_and_rolls_back(self):
        from wechat_bridge.worker_state import WorkerState
        worker = WorkerState(str(self.base / "synthetic-worker"),
                             str(self.base / "synthetic-hook"), clock=self.clock)
        record = {"message_id": "7001", "from_user_id": SCOPE["owner_id"],
                  "context_token": TOKEN, "text": "SYNTHETIC cancel"}
        (Path(worker.store.inbox_dir) / "7001.json").write_text(json.dumps(record))
        lease = worker.claim(["7001"])["lease"]
        claim = worker._claims()["7001"]
        self.assertTrue(worker._alive("7001", claim))
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        before = self.database_snapshot()
        original_status, calculated, checks = self.ledger._status, [], []

        def calculate(db, row):
            result = original_status(db, row)
            if row["cancelled"]:
                calculated.append((result["state"], result["units"]["unknown"]))
            return result

        def cross(point):
            if point == "cancel_before_commit":
                self.assertTrue(calculated)
                self.assertEqual(("unknown", 1), calculated[-1])
                self.clock.value = worker._deadline("7001", claim)

        def authority():
            checks.append(self.clock())
            current = worker._claims()["7001"]
            if current["lease"] != lease or not worker._alive("7001", current):
                raise ValueError("synthetic lease_lost")

        self.ledger.fault = cross
        with mock.patch.object(self.ledger, "_status", side_effect=calculate):
            with self.assertRaisesRegex(ValueError, "synthetic lease_lost"):
                self.ledger.cancel_group(SCOPE, group["group_id"], check_authority=authority)
        self.ledger.fault = None
        self.assertEqual(2, len(checks))  # Entry plus final transaction guard.
        self.assertEqual(before, self.database_snapshot())
        self.assertEqual(2, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.assertEqual("sending", self.ledger.group_status(SCOPE, group["group_id"])["state"])

    def test_cancel_before_commit_fault_and_authority_errors_are_not_retried_or_mapped(self):
        group, job = self.group()
        self.ledger.next_unit(SCOPE, job["job_id"])
        before = self.database_snapshot()
        errors = [ValueError("synthetic authority"), OSError(errno.ENOSPC, "synthetic authority"),
                  sqlite3.OperationalError("synthetic authority busy")]
        for error in errors:
            calls = []
            def authority():
                calls.append(True)
                if len(calls) == 2:
                    raise error
            with self.assertRaises(type(error)) as caught:
                self.ledger.cancel_group(SCOPE, group["group_id"], check_authority=authority)
            self.assertIs(error, caught.exception)
            self.assertEqual([True, True], calls)
            self.assertEqual(before, self.database_snapshot())
        fault = ValueError("synthetic cancel_before_commit")
        def fail(point):
            if point == "cancel_before_commit":
                raise fault
        self.ledger.fault = fail
        checks = []
        with self.assertRaises(ValueError) as caught:
            self.ledger.cancel_group(SCOPE, group["group_id"], check_authority=lambda: checks.append(True))
        self.assertIs(fault, caught.exception)
        self.assertEqual([True], checks)  # Fault precedes only the final guard.
        self.ledger.fault = None
        self.assertEqual(before, self.database_snapshot())
        self.reject("invalid_request", self.ledger.cancel_group, SCOPE, group["group_id"], check_authority=True)
        self.assertEqual(before, self.database_snapshot())

    def test_cancel_authority_failure_does_not_commit_separate_expiry_maintenance(self):
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        before = self.database_snapshot()
        self.clock.value = job["expires_at"]
        def deny_authority():
            raise ValueError("synthetic expired authority")
        with self.assertRaisesRegex(ValueError, "synthetic expired authority"):
            self.ledger.cancel_group(SCOPE, group["group_id"], check_authority=deny_authority)
        self.assertEqual(before, self.database_snapshot())

    def test_cancel_inflight_keeps_unknown_and_rejects_late_acceptance(self):
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        status = self.ledger.cancel_group(SCOPE, group["group_id"])
        self.assertEqual("unknown", status["state"])
        self.assertEqual(1, status["units"]["unknown"])
        self.assertEqual(1, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.reject("cancelled", self.ledger.settle_unit, SCOPE, job["job_id"], unit["unit_id"], "accepted")

    def test_completed_group_cancellation_cannot_erase_accepted_history(self):
        group, job = self.group()
        self.send_page(job)
        status = self.ledger.cancel_group(SCOPE, group["group_id"])
        self.assertEqual("complete", status["state"])
        self.assertEqual(2, len(status["accepted_attachment_ids"]))
        self.assertEqual(2, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.assertEqual([], self.ledger.list_groups(SCOPE)["groups"])

    def test_expiry_of_inflight_job_is_unknown_and_rejects_late_settle(self):
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        self.clock.value = job["expires_at"]
        status = self.ledger.group_status(SCOPE, group["group_id"])
        self.assertEqual("unknown", status["state"])
        self.assertFalse(status["can_continue"])
        self.assertEqual(1, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.reject("expired", self.ledger.settle_unit, SCOPE, job["job_id"], unit["unit_id"], "accepted")

    def test_unstarted_generic_reservation_can_move_but_old_context_cannot_begin(self):
        self.ledger.reserve_units(SCOPE, TOKEN, ["not-yet-sent"])
        self.ledger.register_context(SCOPE, "synthetic-next", fresh=True)
        moved = self.ledger.reserve_units(SCOPE, "synthetic-next", ["not-yet-sent"])
        self.assertEqual(1, moved["used"])
        self.assertEqual(0, self.ledger.register_context(SCOPE, TOKEN)["used"])
        self.reject("group_busy", self.ledger.begin_reserved, SCOPE, TOKEN, "not-yet-sent")
        self.assertTrue(self.ledger.begin_reserved(SCOPE, "synthetic-next", "not-yet-sent")["send_allowed"])

    def test_epoch_change_revokes_jobs_and_does_not_recreate_request(self):
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        new_scope = dict(SCOPE, login_epoch="synthetic-epoch-b")
        self.ledger.register_context(new_scope, "synthetic-new-login-context", fresh=True)
        self.reject("epoch_mismatch", self.ledger.next_unit, SCOPE, job["job_id"])
        self.reject("epoch_mismatch", self.ledger.next_unit, new_scope, job["job_id"])
        self.reject("epoch_mismatch", self.ledger.create_group, new_scope, "synthetic-request", ["synthetic-attachment-00", "synthetic-attachment-01"])
        history = self.ledger.group_status(new_scope, group["group_id"])
        self.assertEqual("unknown", history["state"])
        self.assertFalse(history["can_continue"])
        self.reject("epoch_mismatch", self.ledger.prepare_page, new_scope, group["group_id"], "continue", "synthetic-new-login-context", explicit_continue=True)

    def test_scope_isolation_and_discovery_cursor_boundaries(self):
        for i in range(5):
            self.ledger.create_group(SCOPE, "request:%d" % i, ["file:%d" % i], "synthetic-private-caption")
        first = self.ledger.list_groups(SCOPE, limit=2)
        second = self.ledger.list_groups(SCOPE, limit=2, cursor=first["next_cursor"])
        third = self.ledger.list_groups(SCOPE, limit=2, cursor=second["next_cursor"])
        self.assertEqual(5, len({g["group_id"] for page in (first, second, third) for g in page["groups"]}))
        self.assertIsNone(third["next_cursor"])
        self.assertNotIn("synthetic-private-caption", json.dumps(first))
        self.assertNotIn(TOKEN, json.dumps(first))
        other = dict(SCOPE, owner_id="synthetic-other", conversation_id="synthetic-other")
        self.ledger.register_context(other, "synthetic-other-context", fresh=True)
        self.reject("invalid_request", self.ledger.list_groups, other, cursor=first["next_cursor"])
        self.reject("scope_mismatch", self.ledger.group_status, other, first["groups"][0]["group_id"])
        self.reject("invalid_request", self.ledger.list_groups, SCOPE, limit=True)
        self.reject("invalid_request", self.ledger.list_groups, SCOPE, cursor=first["next_cursor"][:-1] + "!")

    def test_group_selector_requires_a_unique_pending_candidate_and_never_uses_latest(self):
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, None)
        first = self.ledger.create_group(SCOPE, "selector:first", ["synthetic:a"])
        group_id = first["group_id"]
        before = self.tree_snapshot(self.base)
        for selector in (None, "", group_id[:11], group_id):
            self.assertEqual(group_id, self.ledger.resolve_group_selector(SCOPE, selector))
        self.assertEqual(before, self.tree_snapshot(self.base))
        self.clock.value += 1
        second = self.ledger.create_group(SCOPE, "selector:second", ["synthetic:b"])
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, None)
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, "")
        self.assertEqual(group_id, self.ledger.resolve_group_selector(SCOPE, group_id))
        self.assertEqual(second["group_id"], self.ledger.resolve_group_selector(SCOPE, second["group_id"]))
        for bad in ("latest", "1", 1, True, [], {}, "ag_1234567", "ag_" + "f" * 33,
                    group_id.upper(), "continue " + group_id, group_id + "!", "ag_zzzzzzzz"):
            self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, bad)
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, "ag_" + "0" * 32)

    def test_group_selector_checks_full_database_beyond_first_discovery_page(self):
        groups = []
        for index in range(25):
            group_hex = ("a1b2c3d4" + "%024x" % index) if index < 2 else "%032x" % index
            with mock.patch("wechat_bridge.attachment_delivery.uuid.uuid4", return_value=mock.Mock(hex=group_hex)):
                groups.append(self.ledger.create_group(SCOPE, "selector:%d" % index, ["synthetic:%d" % index]))
            self.clock.value += 1
        first_page = self.ledger.list_groups(SCOPE)["groups"]
        self.assertEqual(20, len(first_page))
        self.assertNotIn(groups[0]["group_id"], [g["group_id"] for g in first_page])
        before = self.tree_snapshot(self.base)
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, "ag_a1b2c3d4")
        self.assertEqual(groups[2]["group_id"], self.ledger.resolve_group_selector(SCOPE, groups[2]["group_id"]))
        self.assertEqual(before, self.tree_snapshot(self.base))

    def test_group_selector_isolates_principal_and_excludes_complete_cancelled_but_keeps_unknown(self):
        complete, job = self.group(1, request="selector:complete")
        self.send_page(job)
        cancelled = self.ledger.create_group(SCOPE, "selector:cancelled", ["synthetic:b"])
        status = self.ledger.cancel_group(SCOPE, cancelled["group_id"])
        self.assertEqual("cancelled", status["state"])
        self.assertEqual(status, self.ledger.cancel_group(SCOPE, cancelled["group_id"]))
        for selector in (None, complete["group_id"], cancelled["group_id"]):
            self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, selector)
        unknown, job = self.group(1, request="selector:unknown")
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        self.ledger.cancel_group(SCOPE, unknown["group_id"])
        self.assertEqual("unknown", self.ledger.group_status(SCOPE, unknown["group_id"])["state"])
        other = dict(SCOPE, owner_id="synthetic-other", conversation_id="synthetic-other")
        self.ledger.register_context(other, "synthetic-other-context", fresh=True)
        foreign = self.ledger.create_group(other, "selector:foreign", ["synthetic:c"])
        before = self.tree_snapshot(self.base)
        self.assertEqual(unknown["group_id"], self.ledger.resolve_group_selector(SCOPE, None))
        self.assertEqual(foreign["group_id"], self.ledger.resolve_group_selector(other, None))
        self.reject("continue_required", self.ledger.resolve_group_selector, other, unknown["group_id"])
        self.reject("continue_required", self.ledger.resolve_group_selector, SCOPE, foreign["group_id"])
        self.reject("epoch_mismatch", self.ledger.resolve_group_selector, dict(SCOPE, login_epoch="untrusted-epoch"), None)
        self.assertEqual(before, self.tree_snapshot(self.base))

    def test_recover_reserved_page_advances_fence_without_extending_deadline(self):
        group, job = self.group()
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        another = DeliveryLedger(self.base, clock=self.clock)
        self.assertEqual("sending", another.group_status(SCOPE, group["group_id"])["state"])
        self.reject("group_busy", another.next_unit, SCOPE, job["job_id"])
        result = another.recover()
        self.assertEqual(0, result["inflight_to_unknown"])
        self.assertEqual(2, another.register_context(SCOPE, TOKEN)["used"])
        claimed = another.next_unit(SCOPE, job["job_id"])
        self.assertGreater(claimed["fence"], unit["fence"])
        self.assertEqual(job["expires_at"], claimed["expires_at"])
        self.reject("group_busy", self.ledger.begin_unit, SCOPE, job["job_id"], unit["unit_id"])
        self.assertEqual(2, len(self.send_page(job, ledger=another)))

    def test_recover_inflight_preserves_prefix_and_marks_unknown(self):
        group, job = self.group(3)
        first = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], first["unit_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], first["unit_id"], "accepted")
        second = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], second["unit_id"])
        recovered = DeliveryLedger(self.base, clock=self.clock)
        self.assertEqual(1, recovered.recover()["inflight_to_unknown"])
        status = recovered.group_status(SCOPE, group["group_id"])
        self.assertEqual(["synthetic-attachment-00"], status["accepted_attachment_ids"])
        self.assertEqual(["synthetic-attachment-01"], status["unknown_attachment_ids"])
        self.assertEqual(["synthetic-attachment-02"], status["pending_attachment_ids"])
        self.assertEqual(2, recovered.register_context(SCOPE, TOKEN)["used"])
        self.assertFalse(status["can_continue"])

    def test_regular_file_database_and_parent_link_guards(self):
        cases = []
        outside = self.base / "outside"
        outside.mkdir()
        symlink_base = self.base / "base-link"
        symlink_base.symlink_to(outside, target_is_directory=True)
        cases.append(symlink_base)
        linked_state = self.base / "linked-state"
        linked_state.mkdir()
        (linked_state / "state").symlink_to(outside, target_is_directory=True)
        cases.append(linked_state)
        sentinel = outside / "sentinel"
        sentinel.write_bytes(b"SYNTHETIC: unchanged")
        linked_db = self.base / "linked-db"
        (linked_db / "state").mkdir(parents=True)
        (linked_db / "state/attachment-delivery.sqlite3").symlink_to(sentinel)
        cases.append(linked_db)
        hard_db = self.base / "hard-db"
        (hard_db / "state").mkdir(parents=True)
        os.link(str(sentinel), str(hard_db / "state/attachment-delivery.sqlite3"))
        cases.append(hard_db)
        for base in cases:
            self.reject("storage_invalid", DeliveryLedger, base)
        self.assertEqual(b"SYNTHETIC: unchanged", sentinel.read_bytes())

    def test_missing_opened_ledger_never_recreates_or_resets_accounting(self):
        group, job = self.group()
        self.ledger.path.unlink()
        before = self.tree_snapshot(self.base)
        for function, args in ((self.ledger.register_context, (SCOPE, TOKEN)),
                               (self.ledger.reserve_units, (SCOPE, TOKEN, ["synthetic:lost"])),
                               (self.ledger.cancel_group, (SCOPE, group["group_id"])),
                               (self.ledger.next_unit, (SCOPE, job["job_id"])),
                               (self.ledger.resolve_group_selector, (SCOPE, None))):
            self.reject("storage_invalid", function, *args)
            self.assertFalse(self.ledger.path.exists())
            self.assertEqual(before, self.tree_snapshot(self.base))

    def test_ledger_lost_between_check_and_connect_is_not_recreated(self):
        self.group()
        check_files = self.ledger._check_files
        def lose_after_check():
            check_files()
            self.ledger.path.unlink()
        with mock.patch.object(self.ledger, "_check_files", side_effect=lose_after_check):
            self.reject("storage_invalid", self.ledger.reserve_units, SCOPE, TOKEN, ["synthetic:lost-race"])
        self.assertFalse(self.ledger.path.exists())
        self.assertEqual([], list((self.base / "state").iterdir()))

    @staticmethod
    def tree_snapshot(base):
        base = Path(base)
        if not base.exists():
            return None
        result = {}
        for path in [base] + sorted(base.rglob("*")):
            info = path.lstat()
            content = hashlib.sha256(path.read_bytes()).hexdigest() if stat.S_ISREG(info.st_mode) else None
            if stat.S_ISLNK(info.st_mode):
                content = os.readlink(str(path))
            result[str(path.relative_to(base))] = (
                info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                info.st_size, info.st_mtime_ns, info.st_ctime_ns, content)
        return result

    def test_readonly_inspector_missing_storage_never_constructs_or_writes(self):
        cases = [self.base / "missing", self.base / "empty", self.base / "empty-state"]
        cases[1].mkdir(mode=0o700)
        (cases[2] / "state").mkdir(mode=0o700, parents=True)
        for base in cases:
            before = self.tree_snapshot(base)
            with mock.patch.object(DeliveryLedger, "__init__", side_effect=AssertionError("constructor forbidden")):
                self.assertEqual({"schema_version": 1, "status": "unavailable"}, DeliveryLedger.inspect_readonly(base))
            self.assertEqual(before, self.tree_snapshot(base))

    def test_readonly_inspector_known_summary_zero_writes_and_no_private_material(self):
        group, job = self.group(2, caption="SYNTHETIC: PRIVATE CAPTION /not/a/path")
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], unit["unit_id"], "unknown")
        before = self.tree_snapshot(self.base)
        with mock.patch.object(DeliveryLedger, "__init__", side_effect=AssertionError("constructor forbidden")):
            result = DeliveryLedger.inspect_readonly(self.base)
        self.assertEqual(before, self.tree_snapshot(self.base))
        self.assertEqual("known", result["status"])
        self.assertTrue(result["read_only"])
        self.assertEqual(2, result["protected_attachment_count"])
        self.assertEqual(1, result["counts"]["groups"]["unknown"])
        self.assertEqual(1, result["counts"]["reservations"]["unknown"])
        encoded = json.dumps(result)
        for private in (TOKEN, "PRIVATE CAPTION", "/not/a/path", str(self.base), group["group_id"]):
            self.assertNotIn(private, encoded)
        self.assertTrue(result["catalog_holds_required"])

    def test_readonly_inspector_rejects_all_sidecars_without_touching_them(self):
        for suffix in ("-journal", "-wal", "-shm"):
            with self.subTest(suffix=suffix):
                sidecar = Path(str(self.ledger.path) + suffix)
                sidecar.write_bytes(b"SYNTHETIC: preserve unknown sidecar")
                before = self.tree_snapshot(self.base)
                self.reject("storage_invalid", DeliveryLedger.inspect_readonly, self.base)
                self.assertEqual(before, self.tree_snapshot(self.base))
                sidecar.unlink()
        with sqlite3.connect(str(self.ledger.path), isolation_level=None) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO metadata VALUES('synthetic-uncommitted','x')")
            before = self.tree_snapshot(self.base)
            self.reject("storage_invalid", DeliveryLedger.inspect_readonly, self.base)
            self.assertEqual(before, self.tree_snapshot(self.base))
            db.rollback()
        self.assertEqual("known", DeliveryLedger.inspect_readonly(self.base)["status"])

    def test_readonly_inspector_rejects_unknown_schema_metadata_and_corruption(self):
        self.ledger.create_group(SCOPE, "readonly:source", ["synthetic-file"])
        changes = (
            "CREATE TABLE unknown_data(value TEXT)",
            "CREATE VIEW unknown_view AS SELECT 1",
            "CREATE TRIGGER unknown_trigger AFTER INSERT ON contexts BEGIN SELECT 1; END",
            "CREATE INDEX unknown_index ON jobs(state)",
            "ALTER TABLE contexts ADD COLUMN unknown_column TEXT",
            "UPDATE metadata SET value='2' WHERE key='schema_version'",
            "UPDATE metadata SET value='invalid' WHERE key='cursor_secret'",
            "INSERT INTO metadata VALUES('unknown_metadata','x')",
            "UPDATE groups SET schema_version=2",
            "UPDATE groups SET state='unknown_future_state'",
            "PRAGMA user_version=99",
            "PRAGMA journal_mode=WAL",
        )
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                base = self.base / ("schema:%d" % index)
                (base / "state").mkdir(parents=True, mode=0o700)
                path = base / "state/attachment-delivery.sqlite3"
                shutil.copy2(str(self.ledger.path), str(path))
                with sqlite3.connect(str(path)) as db:
                    db.execute(change)
                before = self.tree_snapshot(base)
                self.reject("storage_invalid", DeliveryLedger.inspect_readonly, base)
                self.assertEqual(before, self.tree_snapshot(base))
        corrupt = self.base / "corrupt-inspector"
        (corrupt / "state").mkdir(parents=True, mode=0o700)
        path = corrupt / "state/attachment-delivery.sqlite3"
        path.write_bytes(b"SYNTHETIC: not a database" * 40)
        path.chmod(0o600)
        before = self.tree_snapshot(corrupt)
        self.reject("storage_invalid", DeliveryLedger.inspect_readonly, corrupt)
        self.assertEqual(before, self.tree_snapshot(corrupt))

    def test_readonly_inspector_links_private_permissions_and_size_budget(self):
        linked = self.base / "inspector-link"
        (linked / "state").mkdir(parents=True, mode=0o700)
        target = linked / "state/attachment-delivery.sqlite3"
        target.symlink_to(self.ledger.path)
        before = self.tree_snapshot(linked)
        self.reject("storage_invalid", DeliveryLedger.inspect_readonly, linked)
        self.assertEqual(before, self.tree_snapshot(linked))
        target.unlink()
        os.link(str(self.ledger.path), str(target))
        before = self.tree_snapshot(linked)
        self.reject("storage_invalid", DeliveryLedger.inspect_readonly, linked)
        self.assertEqual(before, self.tree_snapshot(linked))
        target.unlink()
        for path in (self.ledger.path, self.base / "state"):
            original = stat.S_IMODE(path.stat().st_mode)
            path.chmod(0o644 if path.is_file() else 0o755)
            before = self.tree_snapshot(self.base)
            self.reject("storage_invalid", DeliveryLedger.inspect_readonly, self.base)
            self.assertEqual(before, self.tree_snapshot(self.base))
            path.chmod(original)
        before = self.tree_snapshot(self.base)
        with mock.patch("wechat_bridge.attachment_delivery._INSPECT_MAX_BYTES", self.ledger.path.stat().st_size - 1):
            self.reject("storage_invalid", DeliveryLedger.inspect_readonly, self.base)
        self.assertEqual(before, self.tree_snapshot(self.base))

    def test_database_page_budget_bounds_growth_and_rolls_back_full_transaction(self):
        with sqlite3.connect(str(self.ledger.path)) as db:
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
        size = self.ledger.path.stat().st_size
        maximum = size + page_size
        before = self.scalar("SELECT count(*) FROM groups")
        with mock.patch("wechat_bridge.attachment_delivery.MAX_DATABASE_BYTES", maximum):
            self.reject("quota_exceeded", self.ledger.create_group, SCOPE, "budget:large", ["synthetic:a"], "x" * 16000)
            self.assertLessEqual(self.ledger.path.stat().st_size, maximum)
            self.assertEqual(before, self.scalar("SELECT count(*) FROM groups"))
        with mock.patch("wechat_bridge.attachment_delivery.MAX_DATABASE_BYTES", size - 1):
            self.reject("quota_exceeded", DeliveryLedger, self.base)
            self.reject("quota_exceeded", DeliveryLedger.inspect_readonly, self.base)
        self.assertEqual("queued", self.ledger.create_group(SCOPE, "budget:after", ["synthetic:a"])["state"])

    def test_unknown_group_row_blocks_cached_constructor_sender_and_all_public_mutations(self):
        from wechat_bridge import attachment_runtime as runtime
        cases = (("schema_version", 2), ("type", "attachment_group_v2"),
                 ("schema_version", "future"), ("schema_version", 1.5),
                 ("schema_version", sqlite3.Binary(b"1")), ("type", sqlite3.Binary(b"attachment_group_v1")))
        for index, (column, value) in enumerate(cases):
            with self.subTest(column=column, value=value):
                self.ledger = DeliveryLedger(self.base / ("unknown-%d" % index), clock=self.clock)
                self.ledger.register_context(SCOPE, TOKEN, fresh=True)
                group, job = self.group(1, caption="SYNTHETIC text")
                with sqlite3.connect(str(self.ledger.path)) as db:
                    db.execute("UPDATE groups SET " + column + "=? WHERE group_id=?", (value, group["group_id"]))
                before, tree = self.database_snapshot(), self.tree_snapshot(self.ledger.base)
                unit_id = job["units"][0]["unit_id"]
                calls = ((self.ledger.group_status, (SCOPE, group["group_id"])),
                         (self.ledger.job_status, (SCOPE, job["job_id"])),
                         (self.ledger.pending_jobs, (SCOPE,)),
                         (self.ledger.next_unit, (SCOPE, job["job_id"])),
                         (self.ledger.begin_unit, (SCOPE, job["job_id"], unit_id)),
                         (self.ledger.settle_unit, (SCOPE, job["job_id"], unit_id, "proven_unsent")),
                         (self.ledger.cancel_group, (SCOPE, group["group_id"])),
                         (self.ledger.create_group, (SCOPE, "synthetic-request", ["synthetic-attachment-00"], "SYNTHETIC text")),
                         (self.ledger.prepare_page, (SCOPE, group["group_id"], "synthetic-request", TOKEN)),
                         (self.ledger.register_context, (SCOPE, TOKEN)),
                         (self.ledger.reserve_units, (SCOPE, TOKEN, ["synthetic:extra"])),
                         (self.ledger.list_groups, (SCOPE,)),
                         (self.ledger.resolve_group_selector, (SCOPE, None)),
                         (self.ledger.terminal_materials, (SCOPE, group["group_id"])),
                         (self.ledger.recover, ()))
                for function, args in calls:
                    self.reject("storage_invalid", function, *args)
                    self.assertEqual(before, self.database_snapshot())
                self.reject("storage_invalid", DeliveryLedger, self.ledger.base, clock=self.clock)
                store = mock.Mock(state_dir=str(self.ledger.base / "state"),
                                  _attachment_runtime_blocked=False, _attachment_terminal_cursor="")
                with mock.patch.object(runtime, "active", return_value=True), \
                        mock.patch.object(runtime, "current_scope", return_value=dict(SCOPE)), \
                        mock.patch.object(runtime, "_session", return_value={"bot_token": "synthetic-token"}), \
                        mock.patch.object(runtime, "ledger", return_value=self.ledger), \
                        mock.patch.object(runtime, "catalog"), \
                        mock.patch("wechat_bridge.ilink.send_message") as text_send, \
                        mock.patch("wechat_bridge.media.send_media_message") as media_send:
                    self.reject("storage_invalid", runtime.flush_groups, store, "https://synthetic.invalid", "synthetic-token")
                    self.assertEqual(0, text_send.call_count)
                    self.assertEqual(0, media_send.call_count)
                self.assertEqual(before, self.database_snapshot())
                self.assertEqual(tree, self.tree_snapshot(self.ledger.base))

    def test_unknown_group_row_blocks_expiry_recovery_settle_and_epoch_rotation_without_refund(self):
        group, job = self.group(3)
        first = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], first["unit_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], first["unit_id"], "accepted")
        inflight = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], inflight["unit_id"])
        self.clock.value = job["expires_at"]
        for column, value in (("schema_version", 2), ("type", "unknown")):
            with self.subTest(column=column):
                with sqlite3.connect(str(self.ledger.path)) as db:
                    db.execute("UPDATE groups SET schema_version=1,type='attachment_group_v1'")
                    db.execute("UPDATE groups SET " + column + "=?", (value,))
                before, claims = self.database_snapshot(), dict(self.ledger._claims)
                for function, args in ((self.ledger.pending_jobs, (SCOPE,)), (self.ledger.recover, ()),
                                       (self.ledger._maintenance, ()),
                                       (self.ledger.settle_unit, (SCOPE, job["job_id"], inflight["unit_id"], "proven_unsent"))):
                    self.reject("storage_invalid", function, *args)
                    self.assertEqual(before, self.database_snapshot())
                    self.assertEqual(claims, self.ledger._claims)
                self.reject("storage_invalid", self.ledger.register_context,
                            dict(SCOPE, login_epoch="synthetic-new"), "synthetic-new-token", fresh=True)
                self.assertEqual(before, self.database_snapshot())
                self.assertEqual(3, self.scalar("SELECT count(*) FROM reservations WHERE state IN ('reserved','inflight','accepted','unknown')"))

    def test_orphan_job_cannot_be_published_as_v1_or_begin_a_send(self):
        group, job = self.group(1)
        with sqlite3.connect(str(self.ledger.path)) as db:
            db.execute("DELETE FROM groups WHERE group_id=?", (group["group_id"],))
        before = self.database_snapshot()
        for function, args in ((self.ledger.pending_jobs, (SCOPE,)),
                               (self.ledger.job_status, (SCOPE, job["job_id"])),
                               (self.ledger.next_unit, (SCOPE, job["job_id"])),
                               (self.ledger.begin_unit, (SCOPE, job["job_id"], job["units"][0]["unit_id"]))):
            self.reject("storage_invalid", function, *args)
            self.assertEqual(before, self.database_snapshot())
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))

        self.clock.value = job["expires_at"]
        for function in (self.ledger._maintenance, self.ledger.recover):
            self.reject("storage_invalid", function)
            self.assertEqual(before, self.database_snapshot())

    def test_terminal_materials_complete_cancelled_and_settle_exit_are_readonly(self):
        cancelled, cancel_job = self.group(3, request="terminal:cancel")
        self.ledger.cancel_group(SCOPE, cancelled["group_id"])
        complete, complete_job = self.group(2, request="terminal:complete")
        self.send_page(complete_job)
        exited, exit_job = self.group(1, request="terminal:exit")
        unit = self.ledger.next_unit(SCOPE, exit_job["job_id"])
        self.ledger.begin_unit(SCOPE, exit_job["job_id"], unit["unit_id"])
        def exit_after_commit(point):
            if point == "unit_settle_committed":
                raise ValueError("synthetic accepted commit then exit")
        self.ledger.fault = exit_after_commit
        with self.assertRaisesRegex(ValueError, "synthetic accepted commit then exit"):
            self.ledger.settle_unit(SCOPE, exit_job["job_id"], unit["unit_id"], "accepted")
        self.ledger = DeliveryLedger(self.ledger.base, clock=self.clock)
        before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
        with mock.patch.object(self.ledger, "_maintenance", side_effect=AssertionError("terminal lookup must be observational")):
            for group, state, request in ((cancelled, "cancelled", "terminal:cancel"),
                                          (complete, "complete", "terminal:complete"),
                                          (exited, "complete", "terminal:exit")):
                value = self.ledger.terminal_materials(SCOPE, group["group_id"])
                self.assertEqual({"group_id": group["group_id"], "request_id": request,
                                  "attachment_ids": group["pending_attachment_ids"], "state": state}, value)
                self.assertEqual(value, self.ledger.terminal_materials(SCOPE, group["group_id"]))
            self.assertIsNone(self.ledger.terminal_materials(SCOPE, "ag_" + "0" * 32))
        self.assertEqual(before, self.database_snapshot())
        self.assertEqual(tree, self.tree_snapshot(self.base))
        self.assertEqual(3, self.ledger.register_context(SCOPE, TOKEN)["used"])

    def test_terminal_materials_rejects_unknown_partial_expired_parked_and_create_only_orphan(self):
        orphan = self.ledger.create_group(SCOPE, "terminal:orphan", ["synthetic:orphan"])
        self.ledger.cancel_group(SCOPE, orphan["group_id"])
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, orphan["group_id"]))
        group, job = self.group(2, request="terminal:unknown")
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
        unit = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], unit["unit_id"])
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
        self.ledger.cancel_group(SCOPE, group["group_id"])
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
        partial, page = self.group(2, request="terminal:partial")
        unit = self.ledger.next_unit(SCOPE, page["job_id"])
        self.ledger.settle_unit(SCOPE, page["job_id"], unit["unit_id"], "proven_unsent")
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, partial["group_id"]))
        expired, page = self.group(2, request="terminal:expired")
        self.clock.value = page["expires_at"]
        before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, expired["group_id"]))
        self.assertEqual(before, self.database_snapshot())
        self.assertEqual(tree, self.tree_snapshot(self.base))
        self.assertEqual("expired", self.ledger.group_status(SCOPE, expired["group_id"])["state"])
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, expired["group_id"]))
        self.ledger.register_context(SCOPE, "synthetic-old-full")
        parked = self.ledger.create_group(SCOPE, "terminal:parked", ["synthetic:parked"])
        self.ledger.prepare_page(SCOPE, parked["group_id"], "terminal:parked", "synthetic-old-full")
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, parked["group_id"]))

    def test_terminal_materials_never_ignores_live_job_or_unresolved_group_reservation(self):
        group, job = self.group(2)
        self.ledger.cancel_group(SCOPE, group["group_id"])
        for state in ("queued", "sending", "unknown"):
            with sqlite3.connect(str(self.ledger.path)) as db:
                db.execute("UPDATE jobs SET state=? WHERE job_id=?", (state, job["job_id"]))
            before = self.database_snapshot()
            self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
            self.assertEqual(before, self.database_snapshot())
        with sqlite3.connect(str(self.ledger.path)) as db:
            db.execute("UPDATE jobs SET state='cancelled' WHERE job_id=?", (job["job_id"],))
        for state in ("reserved", "inflight", "unknown"):
            with sqlite3.connect(str(self.ledger.path)) as db:
                db.execute("UPDATE reservations SET state=? WHERE unit_id=?", (state, job["units"][0]["unit_id"]))
            before = self.database_snapshot()
            self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
            self.assertEqual(before, self.database_snapshot())
        with sqlite3.connect(str(self.ledger.path)) as db:
            db.execute("UPDATE reservations SET state='proven_unsent',job_id='synthetic-orphan'")
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))

    def test_terminal_materials_strict_scope_epoch_and_ordered_payload_binding(self):
        group, job = self.group(2)
        self.ledger.cancel_group(SCOPE, group["group_id"])
        other = dict(SCOPE, owner_id="synthetic-other", conversation_id="synthetic-other")
        self.ledger.register_context(other, "synthetic-other-token", fresh=True)
        before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
        self.reject("scope_mismatch", self.ledger.terminal_materials, other, group["group_id"])
        self.reject("epoch_mismatch", self.ledger.terminal_materials, dict(SCOPE, login_epoch="synthetic-wrong"), group["group_id"])
        self.reject("invalid_request", self.ledger.terminal_materials, dict(SCOPE, extra="x"), group["group_id"])
        self.assertEqual(before, self.database_snapshot())
        self.assertEqual(tree, self.tree_snapshot(self.base))
        with sqlite3.connect(str(self.ledger.path)) as db:
            db.execute("UPDATE groups SET attachment_ids=? WHERE group_id=?", (json.dumps(group["pending_attachment_ids"][::-1]), group["group_id"]))
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))

    def test_positive_v1_enum_family_blocks_terminal_proof_release_constructor_and_consumption(self):
        files, group, job, store = self.terminal_catalog_fixture()
        self.assertIsNotNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
        cases = (("groups", "state"), ("jobs", "state"), ("reservations", "state"),
                 ("payload_units", "kind"), ("job_units", "kind"))
        for table, column in cases:
            for value in ("future_v2", "", sqlite3.Binary(b"cancelled")):
                with self.subTest(table=table, column=column, value=value):
                    with sqlite3.connect(str(self.ledger.path)) as db:
                        rowid, original = db.execute("SELECT rowid," + column + " FROM " + table + " ORDER BY rowid LIMIT 1").fetchone()
                        db.execute("UPDATE " + table + " SET " + column + "=? WHERE rowid=?", (value, rowid))
                    before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
                    for function, args in ((self.ledger.terminal_materials, (SCOPE, group["group_id"])),
                                           (self.ledger.group_status, (SCOPE, group["group_id"])),
                                           (self.ledger.job_status, (SCOPE, job["job_id"])),
                                           (self.ledger.pending_jobs, (SCOPE,)),
                                           (self.ledger.next_unit, (SCOPE, job["job_id"])),
                                           (self.ledger.cancel_group, (SCOPE, group["group_id"])),
                                           (self.ledger.reserve_units, (SCOPE, TOKEN, ["synthetic:extra"])),
                                           (self.ledger.register_context, (SCOPE, TOKEN)),
                                           (self.ledger._maintenance, ()), (self.ledger.recover, ())):
                        self.reject("storage_invalid", function, *args)
                        self.assertEqual(before, self.database_snapshot())
                    self.reject("storage_invalid", DeliveryLedger, self.ledger.base, clock=self.clock)
                    self.reject("storage_invalid", DeliveryLedger.inspect_readonly, self.ledger.base)
                    self.flush_without_http(files, store, "storage_invalid")
                    self.assertEqual(before, self.database_snapshot())
                    self.assertEqual(tree, self.tree_snapshot(self.base))
                    self.assertEqual("in_use", files.preview_gc()["items"][0]["reason"])
                    with sqlite3.connect(str(self.ledger.path)) as db:
                        db.execute("UPDATE " + table + " SET " + column + "=? WHERE rowid=?", (original, rowid))
        for table, column in (("groups", "cancelled"), ("contexts", "legacy_unknown")):
            with sqlite3.connect(str(self.ledger.path)) as db:
                rowid, original = db.execute("SELECT rowid," + column + " FROM " + table + " LIMIT 1").fetchone()
                db.execute("UPDATE " + table + " SET " + column + "=2 WHERE rowid=?", (rowid,))
            before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
            self.reject("storage_invalid", self.ledger.terminal_materials, SCOPE, group["group_id"])
            self.flush_without_http(files, store, "storage_invalid")
            self.assertEqual(before, self.database_snapshot())
            self.assertEqual(tree, self.tree_snapshot(self.base))
            with sqlite3.connect(str(self.ledger.path)) as db:
                db.execute("UPDATE " + table + " SET " + column + "=? WHERE rowid=?", (original, rowid))

    def test_known_unknown_is_supported_but_never_positive_terminal_hold_release_proof(self):
        files, group, job, store = self.terminal_catalog_fixture()
        for table in ("groups", "jobs", "reservations"):
            with self.subTest(table=table):
                with sqlite3.connect(str(self.ledger.path)) as db:
                    rowid, original = db.execute("SELECT rowid,state FROM " + table + " ORDER BY rowid LIMIT 1").fetchone()
                    db.execute("UPDATE " + table + " SET state='unknown' WHERE rowid=?", (rowid,))
                self.ledger = DeliveryLedger(self.ledger.base, clock=self.clock)
                before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
                self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
                self.assertEqual("known", DeliveryLedger.inspect_readonly(self.ledger.base)["status"])
                self.flush_without_http(files, store)
                self.assertEqual(before, self.database_snapshot())
                self.assertEqual(tree, self.tree_snapshot(self.base))
                self.assertEqual("in_use", files.preview_gc()["items"][0]["reason"])
                with sqlite3.connect(str(self.ledger.path)) as db:
                    db.execute("UPDATE " + table + " SET state=? WHERE rowid=?", (original, rowid))
        # Each supported safe historical terminal page may accompany an overall
        # cancelled group, provided all material reservations are proven safe.
        for state in ("complete", "partial", "cancelled", "expired", "budget_exhausted"):
            with sqlite3.connect(str(self.ledger.path)) as db:
                db.execute("UPDATE jobs SET state=? WHERE job_id=?", (state, job["job_id"]))
            value = self.ledger.terminal_materials(SCOPE, group["group_id"])
            self.assertEqual("cancelled", value["state"])
        store._attachment_terminal_cursor = ""
        self.flush_without_http(files, store)
        with sqlite3.connect(str(files.base / "state/attachments.sqlite3")) as db:
            holds = [json.loads(row[0]) for row in db.execute("SELECT payload FROM holds")]
        self.assertTrue(holds)
        self.assertTrue(all(hold["released_at"] is not None for hold in holds))
        self.assertEqual(0, self.ledger.register_context(SCOPE, TOKEN)["used"])

    def test_enum_validation_sees_orphans_instead_of_losing_them_in_parent_joins(self):
        files, group, job, store = self.terminal_catalog_fixture()
        with sqlite3.connect(str(self.ledger.path)) as db:
            db.execute("DELETE FROM groups WHERE group_id=?", (group["group_id"],))
        self.assertIsNone(self.ledger.terminal_materials(SCOPE, group["group_id"]))
        self.flush_without_http(files, store)
        self.assertEqual("in_use", files.preview_gc()["items"][0]["reason"])
        for table, column in (("jobs", "state"), ("reservations", "state"),
                              ("payload_units", "kind"), ("job_units", "kind")):
            with self.subTest(table=table):
                with sqlite3.connect(str(self.ledger.path)) as db:
                    rowid, original = db.execute("SELECT rowid," + column + " FROM " + table + " LIMIT 1").fetchone()
                    db.execute("UPDATE " + table + " SET " + column + "='future_orphan_v2' WHERE rowid=?", (rowid,))
                before, tree = self.database_snapshot(), self.tree_snapshot(self.base)
                self.reject("storage_invalid", DeliveryLedger, self.ledger.base, clock=self.clock)
                self.reject("storage_invalid", self.ledger.terminal_materials, SCOPE, group["group_id"])
                self.reject("storage_invalid", self.ledger.recover)
                self.flush_without_http(files, store, "storage_invalid")
                self.assertEqual(before, self.database_snapshot())
                self.assertEqual(tree, self.tree_snapshot(self.base))
                self.assertEqual("in_use", files.preview_gc()["items"][0]["reason"])
                with sqlite3.connect(str(self.ledger.path)) as db:
                    db.execute("UPDATE " + table + " SET " + column + "=? WHERE rowid=?", (original, rowid))

    def test_unknown_enum_cannot_normalize_expiry_or_refund_inflight_or_reserved_budget(self):
        group, job = self.group(3)
        accepted = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], accepted["unit_id"])
        self.ledger.settle_unit(SCOPE, job["job_id"], accepted["unit_id"], "accepted")
        inflight = self.ledger.next_unit(SCOPE, job["job_id"])
        self.ledger.begin_unit(SCOPE, job["job_id"], inflight["unit_id"])
        self.clock.value = job["expires_at"]
        for table, column in (("groups", "state"), ("jobs", "state"), ("reservations", "state"),
                              ("payload_units", "kind"), ("job_units", "kind")):
            with self.subTest(table=table):
                with sqlite3.connect(str(self.ledger.path)) as db:
                    rowid, original = db.execute("SELECT rowid," + column + " FROM " + table + " ORDER BY rowid LIMIT 1").fetchone()
                    db.execute("UPDATE " + table + " SET " + column + "='future_expiry_v2' WHERE rowid=?", (rowid,))
                before, claims = self.database_snapshot(), dict(self.ledger._claims)
                for function, args in ((self.ledger._maintenance, ()), (self.ledger.recover, ()),
                                       (self.ledger.pending_jobs, (SCOPE,)),
                                       (self.ledger.settle_unit, (SCOPE, job["job_id"], inflight["unit_id"], "proven_unsent")),
                                       (self.ledger.register_context, (dict(SCOPE, login_epoch="new-epoch"), "new-context"))):
                    self.reject("storage_invalid", function, *args)
                    self.assertEqual(before, self.database_snapshot())
                    self.assertEqual(claims, self.ledger._claims)
                with sqlite3.connect(str(self.ledger.path)) as db:
                    db.execute("UPDATE " + table + " SET " + column + "=? WHERE rowid=?", (original, rowid))
                self.assertEqual(3, self.scalar("SELECT count(*) FROM reservations WHERE state IN ('reserved','inflight','accepted','unknown')"))

    def test_real_process_exit_reservation_inflight_and_commit_windows(self):
        cases = ("reservation_before_commit", "reservation_committed", "unit_inflight_committed",
                 "unit_settle_before_commit", "unit_settle_committed", "page_before_commit",
                 "page_committed", "group_before_commit", "recovery_before_commit")
        for point in cases:
            with self.subTest(point=point):
                base = self.base / point
                result = subprocess.run([sys.executable, "-I", "-B", str(Path(__file__)), "--crash", point, str(base)],
                                        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                self.assertEqual(73, result.returncode, result.stdout + result.stderr)
                ledger = DeliveryLedger(base, clock=self.clock)
                ledger.recover()
                budget = ledger.register_context(SCOPE, TOKEN)
                if point in ("reservation_before_commit", "page_before_commit", "group_before_commit"):
                    self.assertEqual(0, budget["used"])
                elif point in ("reservation_committed", "page_committed"):
                    self.assertEqual(2, budget["used"])
                else:
                    self.assertEqual(1, budget["used"])
                    expected = "accepted" if point == "unit_settle_committed" else "unknown"
                    with sqlite3.connect(str(ledger.path)) as db:
                        self.assertEqual(expected, db.execute("SELECT state FROM reservations WHERE unit_id='crash:unit'").fetchone()[0])
                    if expected == "unknown":
                        self.reject("group_unknown", ledger.reserve_units, SCOPE, TOKEN, ["crash:unit"])
                self.assertLessEqual(budget["used"], CAP)


def crash_child(point, base):
    clock = Clock()
    def fault(name):
        if name == point:
            os._exit(73)
    ledger = DeliveryLedger(base, clock=clock, fault=fault)
    ledger.register_context(SCOPE, TOKEN, fresh=True)
    if point in ("group_before_commit", "page_before_commit", "page_committed"):
        group = ledger.create_group(SCOPE, "crash:request", ["crash:a", "crash:b"])
        ledger.prepare_page(SCOPE, group["group_id"], "crash:request", TOKEN)
    elif point.startswith("reservation_"):
        ledger.reserve_units(SCOPE, TOKEN, ["crash:a", "crash:b"])
    else:
        ledger.reserve_units(SCOPE, TOKEN, ["crash:unit"])
        ledger.begin_reserved(SCOPE, TOKEN, "crash:unit")
        if point == "recovery_before_commit":
            ledger.recover()
        else:
            ledger.settle_reserved(SCOPE, TOKEN, "crash:unit", "accepted")
    raise AssertionError("fault checkpoint not reached")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--crash":
        crash_child(sys.argv[2], sys.argv[3])
    else:
        unittest.main(verbosity=2)
