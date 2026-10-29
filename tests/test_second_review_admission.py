"""Consolidated fresh-admission/replay boundaries on real public/Muse code.

Only short synthetic files and local state are used. Transport is not needed
for these queue/intent tests; all real socket operations are denied.
"""
from contextlib import contextmanager, ExitStack
import errno
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("second_review_muse_fixture", ROOT / "hooks/tests/test_muse_prequeue.py")
muse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(muse)
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.attachment_delivery import DeliveryLedger, DeliveryError, JOB_SECONDS
from wechat_bridge.attachments import AttachmentStore, AttachmentError
from wechat_bridge.store import atomic_write_json, load_json


def deny(*args, **kwargs):
    raise AssertionError("second-review admission tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny


@contextmanager
def case():
    value = muse.MusePrequeueTests("runTest")
    value.setUp()
    try:
        yield value
    finally:
        value.doCleanups()
        value.worker.store.close()


class SecondReviewAdmissionTests(unittest.TestCase):
    @staticmethod
    def request(c, action="reply"):
        return action + "-" + runtime._digest(c.session()["owned"])

    @classmethod
    def journal(cls, c, action="reply"):
        return Path(c.store.state_dir) / "attachment_admissions" / (cls.request(c, action) + ".json")

    @staticmethod
    def tables(path):
        if not path.exists():
            return None
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return {name: db.execute('SELECT * FROM "' + name + '" ORDER BY rowid').fetchall() for name in names}

    @classmethod
    def publication(cls, c):
        directory = Path(c.store.state_dir) / "attachment_admissions"
        return {
            "catalog": cls.tables(Path(c.store.state_dir) / "attachments.sqlite3"),
            "ledger": cls.tables(Path(c.store.state_dir) / "attachment-delivery.sqlite3"),
            "journals": {p.name: p.read_bytes() for p in directory.glob("*.json")},
            "ordinary": {str(p.relative_to(c.base)): p.read_bytes()
                for directory in (c.store.pending_dir, c.store.sending_dir, c.store.sent_dir, c.store.failed_dir)
                for p in Path(directory).glob("*.json")},
        }

    @staticmethod
    def counts(c):
        state = runtime.ledger(c.store)
        with sqlite3.connect(str(state.path)) as db:
            return tuple(db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                         for table in ("groups", "jobs"))

    @classmethod
    def request_holds(cls, c):
        wanted = "group:request:" + cls.request(c)
        with sqlite3.connect(str(Path(c.store.state_dir) / "attachments.sqlite3")) as db:
            return [row for row in db.execute("SELECT id,attachment_id,holder,payload FROM holds") if row[2] == wanted]

    @staticmethod
    def extra(c, name, status="ready", foreign=False):
        scope = dict(c.scope)
        if foreign:
            scope.update(owner_id="synthetic-foreign", conversation_id="synthetic-foreign")
        files = runtime.catalog(c.store)
        ref = files.register_event(scope, "synthetic-extra-" + name, [{"item_index": 0,
            "kind": "file", "original_name": "SYNTHETIC-" + name + ".bin", "declared_type": None}])["attachment_refs"][0]
        if status == "failed":
            files.fail(scope, ref["attachment_id"], "synthetic_failure")
        elif status in ("ready", "prepared", "missing", "wrong_size"):
            stage = Path(ref["stage_path"])
            stage.write_bytes(b"SYNTHETIC EXTRA " + name.encode("ascii"))
            stage.chmod(0o600)
            if status == "prepared":
                def stop(point):
                    if point == "publish_prepared":
                        raise RuntimeError("SYNTHETIC original prepare boundary")
                interrupted = AttachmentStore(c.store.base, policy=runtime.effective_policy(c.store.base), fault=stop)
                with unittest.TestCase().assertRaisesRegex(RuntimeError, "prepare boundary"):
                    interrupted.publish(scope, ref["attachment_id"], str(stage))
            else:
                files.publish(scope, ref["attachment_id"], str(stage))
                original = next((Path(c.store.base) / "attachments/objects").glob(ref["attachment_id"] + ".*"))
                if status == "missing":
                    original.unlink()
                elif status == "wrong_size":
                    original.write_bytes(b"BAD")
        return ref["attachment_id"]

    @staticmethod
    def raw(c, ids=None):
        private = c.session()
        wire = {"schema_version": 1, "action": "reply",
                "arguments": {"attachment_ids": ids or [c.attachment_id]}}
        return c.worker.attachment_call(private["owned"], private["lease"], wire)

    def failure(self, result, code, stop=False):
        self.assertFalse(result["ok"], result)
        self.assertEqual(result["error"], code)
        self.assertEqual(result["must_stop"], stop)

    def test_fresh_deterministic_failures_keep_handle_correctable_and_no_publication(self):
        variants = (("missing_id", "not_found"), ("invalid_id", "invalid_request"),
            ("foreign", "scope_mismatch"), ("pending", "processing"), ("prepared", "processing"),
            ("failed", "attachment_unavailable"), ("missing", "attachment_unavailable"),
            ("wrong_size", "attachment_unavailable"))
        for variant, code in variants:
            with self.subTest(variant=variant), case() as c:
                if variant == "missing_id":
                    aid = "0" * 32 if c.attachment_id != "0" * 32 else "1" * 32
                elif variant == "invalid_id":
                    aid = "not-an-attachment-id"
                else:
                    aid = self.extra(c, variant, foreign=variant == "foreign",
                                     status="ready" if variant == "foreign" else variant)
                before = self.publication(c)
                refused = c.attachment("reply", attachment_ids=[aid])
                self.failure(refused, code)
                self.assertEqual(c.session()["phase"], "active")
                self.assertEqual(refused["handle"], c.batch["handle"])
                self.assertEqual(refused["revision"], c.batch["revision"])
                self.assertEqual(self.publication(c), before)
                corrected = c.attachment("reply", attachment_ids=[c.attachment_id])
                self.assertTrue(corrected["ok"], corrected)
                self.assertEqual(c.session()["phase"], "queued")
                self.assertEqual(self.counts(c), (1, 1))

    def test_mixed_batch_checks_every_metadata_record_before_any_request_hold(self):
        for variant, code in (("missing", "not_found"), ("foreign", "scope_mismatch"), ("pending", "processing")):
            with self.subTest(variant=variant), case() as c:
                bad = "0" * 32 if variant == "missing" else self.extra(c, variant,
                    foreign=variant == "foreign", status="ready" if variant == "foreign" else "pending")
                before = self.publication(c)
                original = AttachmentStore._file
                hashes = []
                def observe(files, *args, **kwargs):
                    hashes.append(True)
                    return original(files, *args, **kwargs)
                with patch.object(AttachmentStore, "_file", observe):
                    self.failure(c.attachment("reply", attachment_ids=[c.attachment_id, bad]), code)
                self.assertEqual(hashes, [])
                self.assertEqual(self.request_holds(c), [])
                self.assertEqual(self.publication(c), before)
                self.assertTrue(c.attachment("reply", attachment_ids=[c.attachment_id])["ok"])

    def test_fresh_preflight_does_not_add_a_second_whole_file_hash(self):
        with case() as c:
            original = AttachmentStore._file
            calls = []
            def observe(files, *args, **kwargs):
                calls.append(self.journal(c).exists())
                return original(files, *args, **kwargs)
            with patch.object(AttachmentStore, "_file", observe):
                result = c.attachment("reply", attachment_ids=[c.attachment_id])
            self.assertTrue(result["ok"], result)
            self.assertEqual(calls, [True])

    @contextmanager
    def fault(self, c, stage):
        with ExitStack() as stack:
            if stage == "prepared":
                stack.enter_context(patch.object(runtime, "publish_admission",
                    side_effect=AttachmentError("not_found")))
            elif stage == "hold":
                original = AttachmentStore.acquire
                def held(files, *args, **kwargs):
                    original(files, *args, **kwargs)
                    raise AttachmentError("not_found")
                stack.enter_context(patch.object(AttachmentStore, "acquire", held))
            elif stage in ("group", "page"):
                method = "create_group" if stage == "group" else "prepare_page"
                original = getattr(DeliveryLedger, method)
                def committed(state, *args, **kwargs):
                    original(state, *args, **kwargs)
                    raise DeliveryError("group_busy")
                stack.enter_context(patch.object(DeliveryLedger, method, committed))
            elif stage == "admitted":
                original = runtime.atomic_write_json
                def interrupted(path, value):
                    if Path(path).parent.name == "attachment_admissions" and value.get("phase") == "admitted":
                        raise OSError(errno.EIO, "SYNTHETIC admitted write interruption")
                    return original(path, value)
                stack.enter_context(patch.object(runtime, "atomic_write_json", interrupted))
            elif stage == "complete":
                stack.enter_context(patch.object(c.worker, "_complete",
                    side_effect=OSError(errno.EIO, "SYNTHETIC receipt completion interruption")))
            else:
                raise AssertionError("unknown synthetic fault stage")
            yield

    def test_errors_after_intent_remain_uncertain_even_with_correctable_error_codes(self):
        for stage, expected in (("prepared", (0, 0)), ("hold", (0, 0)),
                                ("group", (1, 0)), ("page", (1, 1)),
                                ("admitted", (1, 1)), ("complete", (1, 1))):
            with self.subTest(stage=stage), case() as c:
                with self.fault(c, stage):
                    result = c.attachment("reply", attachment_ids=[c.attachment_id])
                self.failure(result, "outcome_unknown", stop=True)
                self.assertEqual(c.session()["phase"], "uncertain")
                self.assertTrue(self.journal(c).exists())
                self.assertEqual(self.counts(c), expected)
                self.assertEqual(len(self.request_holds(c)), 0 if stage == "prepared" else 1)
                before = self.publication(c)
                self.failure(c.attachment("reply", attachment_ids=[c.attachment_id]), "outcome_unknown", stop=True)
                self.assertEqual(self.publication(c), before)

    def test_existing_same_parameter_intent_replays_without_fresh_rejection_or_time_reset(self):
        for phase in ("prepared", "publishing", "admitted", "complete"):
            with self.subTest(phase=phase), case() as c:
                stage = {"prepared": "prepared", "publishing": "page", "admitted": "complete", "complete": "complete"}[phase]
                with self.fault(c, stage):
                    self.failure(c.attachment("reply", attachment_ids=[c.attachment_id]), "outcome_unknown", stop=True)
                path = self.journal(c)
                old = load_json(str(path))
                original_time = old["authorized_at"]
                state = runtime.ledger(c.store)
                if phase == "complete":
                    while True:
                        unit = state.next_unit(c.scope, old["job_id"])
                        if unit is None:
                            break
                        state.begin_unit(c.scope, old["job_id"], unit["unit_id"])
                        state.settle_unit(c.scope, old["job_id"], unit["unit_id"], "accepted")
                c.now += 0.25
                if phase in ("admitted", "complete"):
                    c.original.unlink()
                    with patch.object(AttachmentStore, "acquire") as acquire:
                        result = self.raw(c)
                    acquire.assert_not_called()
                else:
                    result = self.raw(c)
                self.assertTrue(result["ok"], result)
                self.assertEqual(self.counts(c), (1, 1))
                saved = load_json(str(path))
                self.assertEqual(saved["authorized_at"], original_time)
                if old.get("job_id"):
                    self.assertEqual(result["result"]["job_id"], old["job_id"])
                if phase == "complete":
                    self.assertEqual(result["result"]["status"]["state"], "complete")

    def test_existing_different_payload_and_corrupt_journal_are_never_overwritten(self):
        with case() as c:
            other = self.extra(c, "different")
            with self.fault(c, "prepared"):
                self.failure(c.attachment("reply", attachment_ids=[c.attachment_id]), "outcome_unknown", stop=True)
            before = self.publication(c)
            refused = self.raw(c, [other])
            self.assertFalse(refused["ok"], refused)
            self.assertEqual(refused["error"]["code"], "storage_invalid")
            self.assertEqual(self.publication(c), before)
        for variant in ("json", "type", "null"):
            with self.subTest(variant=variant), case() as c:
                path = self.journal(c)
                path.parent.mkdir(mode=0o700, exist_ok=True)
                if variant == "json":
                    path.write_bytes(b"{SYNTHETIC BROKEN JSON")
                elif variant == "null":
                    path.write_bytes(b"null")
                else:
                    path.write_text(json.dumps({"schema_version": 1, "type": "attachment_group_future"}))
                path.chmod(0o600)
                before = self.publication(c)
                self.failure(c.attachment("reply", attachment_ids=[c.attachment_id]), "outcome_unknown", stop=True)
                self.assertEqual(self.publication(c), before)

    def test_missing_journal_with_request_holder_group_or_job_is_not_fresh(self):
        for artifact in ("holder", "group", "job"):
            with self.subTest(artifact=artifact), case() as c:
                request = self.request(c)
                files, state = runtime.catalog(c.store), runtime.ledger(c.store)
                if artifact == "holder":
                    files.acquire(c.scope, c.attachment_id, "group:request:" + request)
                else:
                    group = state.create_group(c.scope, request, [c.attachment_id])
                    if artifact == "job":
                        state.prepare_page(c.scope, group["group_id"], request, "SYNTHETIC_CONTEXT_original")
                self.assertFalse(self.journal(c).exists())
                before = self.publication(c)
                self.failure(c.attachment("reply", attachment_ids=[c.attachment_id]), "outcome_unknown", stop=True)
                self.assertEqual(self.publication(c), before)

    @staticmethod
    def start_continue(c, group):
        c.incoming("synthetic-continue", "继续 " + group["group_id"])
        plan = c.worker.plan()
        c.assertEqual(plan["decision"], "wake", plan)
        c.batch = c.api({"action": "begin", "lease": plan["payload"]["lease"]})
        c.assertTrue(c.batch["ok"], c.batch)

    @staticmethod
    def finish_page(state, scope, job_id, outcome):
        while True:
            unit = state.next_unit(scope, job_id)
            if unit is None:
                break
            state.begin_unit(scope, job_id, unit["unit_id"])
            state.settle_unit(scope, job_id, unit["unit_id"], outcome)
            if outcome != "accepted":
                break

    def test_continue_known_rejections_do_not_create_a_new_intent(self):
        for status, expected in (("busy", "group_busy"), ("unknown", "group_unknown"),
                                 ("cancelled", "continue_required"), ("complete", "continue_required")):
            with self.subTest(status=status), case() as c:
                first = c.attachment("reply", attachment_ids=[c.attachment_id])["result"]
                state = runtime.ledger(c.store)
                if status == "unknown":
                    self.finish_page(state, c.scope, first["job_id"], "unknown")
                elif status == "cancelled":
                    state.cancel_group(c.scope, first["group_id"])
                elif status == "complete":
                    self.finish_page(state, c.scope, first["job_id"], "accepted")
                self.start_continue(c, first)
                before = self.publication(c)
                result = c.attachment("continue", group_id=first["group_id"])
                self.failure(result, expected)
                self.assertEqual(c.session()["phase"], "active")
                self.assertEqual(self.publication(c), before)

    def test_continue_remaining_material_rejections_preserve_same_correctable_handle(self):
        variants = (("missing_record", "not_found"), ("failed", "attachment_unavailable"),
                    ("missing", "attachment_unavailable"), ("ready_missing", "attachment_unavailable"))
        for variant, code in variants:
            with self.subTest(variant=variant), case() as c:
                other = self.extra(c, "continue-" + variant)
                queued = c.attachment("reply", attachment_ids=[c.attachment_id, other])
                self.assertTrue(queued["ok"], queued)
                first = queued["result"]
                state = runtime.ledger(c.store)
                self.finish_page(state, c.scope, first["job_id"], "proven_unsent")
                self.start_continue(c, first)
                database = Path(c.store.state_dir) / "attachments.sqlite3"
                with sqlite3.connect(str(database)) as db:
                    saved = db.execute("SELECT * FROM attachments WHERE id=?", (other,)).fetchone()
                    metadata = json.loads(saved[-1])
                    if variant == "missing_record":
                        db.execute("DELETE FROM attachments WHERE id=?", (other,))
                    elif variant in ("failed", "missing"):
                        # Simulate a later catalog observation, then restore the
                        # exact synthetic row; this is not a recovery API.
                        metadata.update(status=variant, failure_reason="synthetic_failure")
                        db.execute("UPDATE attachments SET payload=? WHERE id=?", (json.dumps(metadata), other))
                original = Path(c.base) / "attachments/objects" / metadata["object_key"]
                if variant == "ready_missing":
                    content = original.read_bytes()
                    original.unlink()
                before = self.publication(c)
                with patch.object(AttachmentStore, "_file") as whole_file:
                    refused = c.attachment("continue", group_id=first["group_id"])
                whole_file.assert_not_called()
                self.failure(refused, code)
                self.assertEqual(c.session()["phase"], "active")
                self.assertEqual((refused["handle"], refused["revision"]),
                                 (c.batch["handle"], c.batch["revision"]))
                self.assertFalse(self.journal(c, "continue").exists())
                self.assertEqual(self.publication(c), before)
                if variant == "ready_missing":
                    original.write_bytes(content)
                    original.chmod(0o600)
                else:
                    with sqlite3.connect(str(database)) as db:
                        if variant == "missing_record":
                            db.execute("INSERT INTO attachments VALUES(?,?,?,?,?)", saved)
                        else:
                            db.execute("UPDATE attachments SET payload=? WHERE id=?", (saved[-1], other))
                corrected = c.attachment("continue", group_id=first["group_id"])
                self.assertTrue(corrected["ok"], corrected)
                self.assertEqual(c.session()["phase"], "queued")
                self.assertEqual(self.counts(c), (1, 2))
                self.assertNotEqual(corrected["result"]["job_id"], first["job_id"])

    def test_continue_checks_only_remaining_materials_when_accepted_original_is_missing(self):
        with case() as c:
            other = self.extra(c, "continue-ready")
            queued = c.attachment("reply", attachment_ids=[c.attachment_id, other])
            self.assertTrue(queued["ok"], queued)
            first = queued["result"]
            state = runtime.ledger(c.store)
            unit = state.next_unit(c.scope, first["job_id"])
            self.assertEqual(unit["attachment_id"], c.attachment_id)
            state.begin_unit(c.scope, first["job_id"], unit["unit_id"])
            state.settle_unit(c.scope, first["job_id"], unit["unit_id"], "accepted")
            self.finish_page(state, c.scope, first["job_id"], "proven_unsent")
            c.original.unlink()
            self.start_continue(c, first)
            checked = []
            preflight = AttachmentStore.preflight_admission
            def observe(files, scope, request_id, attachment_ids=None):
                checked.append(None if attachment_ids is None else list(attachment_ids))
                return preflight(files, scope, request_id, attachment_ids)
            with patch.object(AttachmentStore, "preflight_admission", observe), \
                    patch.object(AttachmentStore, "_file") as whole_file:
                result = c.attachment("continue", group_id=first["group_id"])
            whole_file.assert_not_called()
            self.assertEqual(checked, [None, [other]])
            self.assertTrue(result["ok"], result)
            job = state.job_status(c.scope, result["result"]["job_id"])
            self.assertEqual([unit["attachment_id"] for unit in job["units"]
                              if unit["kind"] == "attachment"], [other])
            self.assertEqual(result["result"]["status"]["accepted_attachment_ids"], [c.attachment_id])
            self.assertFalse(c.original.exists())
            self.assertEqual(self.counts(c), (1, 2))

    def test_expired_reserved_continues_but_expired_inflight_is_unknown(self):
        for inflight in (False, True):
            with self.subTest(inflight=inflight), case() as c:
                state = runtime.ledger(c.store)
                state.clock = lambda: c.now
                # The adapter publishes through a distinct Store instance.
                runtime.ledger(c.worker.store).clock = lambda: c.now
                first = c.attachment("reply", attachment_ids=[c.attachment_id])["result"]
                if inflight:
                    unit = state.next_unit(c.scope, first["job_id"])
                    state.begin_unit(c.scope, first["job_id"], unit["unit_id"])
                c.now += JOB_SECONDS + 1
                self.start_continue(c, first)
                before = self.publication(c)
                result = c.attachment("continue", group_id=first["group_id"])
                if inflight:
                    self.failure(result, "group_unknown")
                    self.assertEqual(self.publication(c), before)
                else:
                    self.assertTrue(result["ok"], result)
                    self.assertNotEqual(result["result"]["job_id"], first["job_id"])
                    self.assertEqual(self.counts(c), (1, 2))

    def test_zero_budget_is_a_valid_parked_reply_and_continue(self):
        for continuing in (False, True):
            with self.subTest(continuing=continuing), case() as c:
                state = runtime.ledger(c.store)
                if continuing:
                    first = c.attachment("reply", attachment_ids=[c.attachment_id])["result"]
                    self.finish_page(state, c.scope, first["job_id"], "proven_unsent")
                    self.start_continue(c, first)
                    context = "SYNTHETIC_CONTEXT_synthetic-continue"
                else:
                    context = "SYNTHETIC_CONTEXT_original"
                state.reserve_units(c.scope, context, ["synthetic-budget:%d" % i for i in range(8)])
                result = c.attachment("continue", group_id=first["group_id"]) if continuing else \
                         c.attachment("reply", attachment_ids=[c.attachment_id])
                self.assertTrue(result["ok"], result)
                job = state.job_status(c.scope, result["result"]["job_id"])
                self.assertEqual((job["state"], job["units"]), ("budget_exhausted", []))
                self.assertEqual(c.session()["phase"], "queued")

    def test_lease_and_epoch_loss_after_acquire_keep_original_blocked_protection(self):
        for loss in ("lease", "epoch"):
            with self.subTest(loss=loss), case() as c:
                original = AttachmentStore.acquire
                def acquired(files, *args, **kwargs):
                    value = original(files, *args, **kwargs)
                    if loss == "lease":
                        c.now += 91
                    else:
                        session = load_json(c.store.session_path)
                        session["login_epoch"] = "e" * 32
                        atomic_write_json(c.store.session_path, session)
                    return value
                with patch.object(AttachmentStore, "acquire", acquired):
                    result = c.attachment("reply", attachment_ids=[c.attachment_id])
                self.failure(result, "lease_lost" if loss == "lease" else "epoch_mismatch", stop=True)
                self.assertEqual(len(self.request_holds(c)), 1)
                self.assertEqual(self.counts(c), (0, 0))
                self.assertEqual(load_json(str(self.journal(c)))["phase"], "blocked")

    def test_global_precondition_io_is_not_a_correctable_business_rejection(self):
        with case() as c:
            before = self.publication(c)
            with patch.object(AttachmentStore, "_record", side_effect=OSError(errno.EIO, "SYNTHETIC metadata IO")):
                result = c.attachment("reply", attachment_ids=[c.attachment_id])
            self.failure(result, "outcome_unknown", stop=True)
            self.assertEqual(self.publication(c), before)


if __name__ == "__main__":
    unittest.main()
