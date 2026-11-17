"""Offline remote intent regressions using only a temporary synthetic Store."""
import copy
import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from wechat_bridge.backend_state import BackendState, encode
from wechat_bridge.remote_tasks import RemoteTaskError, RemoteTaskJournal
from wechat_bridge.store import Store


class RemoteTaskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(self.temp.name)
        self.config = {"schema_version": 1, "account_scope": "synthetic-account"}
        self.state = BackendState(self.store, self.config, "owner", "bot")
        self.now = 1000.0
        self.binding = {"service": "http://127.0.0.1:9999", "tenant": "synthetic-tenant",
                        "principal": "synthetic-principal", "queue": "fixed-test-queue"}
        self.submit = {"queue": "fixed-test-queue", "instructions": "Reply to this test text.",
                       "payload": {"messages": [{"role": "user", "content": "synthetic input"}]},
                       "max_attempts": 1, "ttl_seconds": 600,
                       "idempotency_key": "wbr-" + "a" * 64}
        self.input_digest = hashlib.sha256(b"synthetic input").hexdigest()
        self.put_job("job-a")
        self.journal = self.make_journal("job-a")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def put_job(self, execution, **extra):
        job = {"execution_id": execution, "attempt_id": "attempt-a", "backend_instance_ref": "remote-a",
               "account_scope": "synthetic-account", "sender_key": "synthetic-sender",
               "backend_id": "codex", "conversation_key": "synthetic-conversation", "route_revision": 1,
               "status": "running", "deadline_at": 2000.0, "events": []}
        job.update(extra)
        with self.state.transaction() as db:
            self.state.put_job(db, job)
        return job

    def mutate_job(self, **extra):
        with self.state.transaction() as db:
            job = self.state.get_job(db, "job-a")
            job.update(extra)
            self.state.put_job(db, job)

    def make_journal(self, execution="job-a", state=None, attempt="attempt-a", instance="remote-a"):
        return RemoteTaskJournal(state or self.state, execution, attempt, instance, clock=lambda: self.now)

    def prepare(self, journal=None, binding=None, submit=None, input_digest=None):
        return (journal or self.journal).prepare(binding or self.binding, submit or self.submit,
                                                 input_digest or self.input_digest)

    def count(self):
        with self.state.transaction() as db:
            return db.execute("SELECT COUNT(*) FROM remote_tasks").fetchone()[0]

    def assert_code(self, code, operation):
        with self.assertRaises(RemoteTaskError) as caught:
            operation()
        self.assertEqual(str(caught.exception), code)
        self.assertEqual(caught.exception.code, code)

    def test_full_frozen_round_trip_has_exact_snapshot_and_stable_digest(self):
        prepared = self.prepare()
        self.assertEqual(set(prepared), {"schema_version", "execution_id", "attempt_id", "backend_instance_ref",
                                        "binding", "submit", "submit_digest", "input_digest", "state", "task_id"})
        self.assertEqual(prepared["submit"], self.submit)
        self.assertEqual(prepared["submit_digest"], hashlib.sha256(encode(self.submit).encode()).hexdigest())
        self.assertEqual(self.journal.begin_submit()["state"], "submitting")
        bound = self.journal.bind_task("remote-original-id")
        self.assertEqual(bound["state"], "bound")
        self.assertEqual(self.journal.bind_task("remote-original-id"), bound)
        self.assertEqual(self.prepare(), bound)
        self.assert_code("remote_task_mapping_conflict", lambda: self.journal.bind_task("replacement-id"))
        self.assertEqual(self.journal.snapshot(), bound)

    def test_begin_is_committed_before_return_and_never_regrants_after_response_loss(self):
        self.prepare()
        self.journal.begin_submit()
        # Independent connection represents a driver/process that lost the POST response.
        with sqlite3.connect(self.state.path) as db:
            row = db.execute("SELECT payload FROM remote_tasks").fetchone()
        self.assertEqual(json.loads(row[0])["state"], "submitting")
        self.assert_code("remote_task_submit_already_begun", self.make_journal().begin_submit)
        self.assertIsNone(self.journal.snapshot()["task_id"])

    def test_prepared_or_submitting_restart_keeps_intent_but_cannot_submit(self):
        for phase in ("prepared", "submitting"):
            execution = "restart-" + phase
            self.put_job(execution)
            journal = self.make_journal(execution)
            submit = dict(self.submit, idempotency_key="wbr-" + hashlib.sha256(execution.encode()).hexdigest())
            frozen = self.prepare(journal=journal, submit=submit)
            if phase == "submitting":
                frozen = journal.begin_submit()
            self.state.recover(self.now)
            reopened = BackendState(self.store, self.config, "owner", "bot")
            fresh = self.make_journal(execution, state=reopened)
            self.assertEqual(fresh.snapshot(), frozen)
            self.assert_code("remote_task_not_running", fresh.begin_submit)
            self.assert_code("remote_task_not_running", lambda: self.prepare(journal=fresh, submit=submit))

    def test_bind_after_unknown_or_cancelling_changes_only_mapping(self):
        self.prepare()
        self.journal.begin_submit()
        for status in ("unknown", "cancelling", "cancelled"):
            self.mutate_job(status=status)
            with self.state.transaction() as db:
                before = db.execute("SELECT * FROM jobs").fetchall()
                before = [tuple(row) for row in before]
                self.assertEqual(db.execute("SELECT COUNT(*) FROM outputs").fetchone()[0], 0)
            self.assertEqual(self.journal.bind_task("late-original-id")["task_id"], "late-original-id")
            with self.state.transaction() as db:
                self.assertEqual([tuple(row) for row in db.execute("SELECT * FROM jobs")], before)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM outputs").fetchone()[0], 0)
            self.assert_code("remote_task_not_running", self.journal.begin_submit)

    def test_changed_identity_with_same_key_cannot_reuse_or_bind(self):
        self.prepare()
        self.journal.begin_submit()
        fields = {"attempt_id": "new-attempt", "backend_instance_ref": "new-instance",
                  "account_scope": "other-account", "sender_key": "other-sender",
                  "backend_id": "other-backend", "conversation_key": "other-conversation", "deadline_at": 9000.0}
        for key, value in fields.items():
            with self.subTest(key=key):
                with self.state.transaction() as db:
                    original = self.state.get_job(db, "job-a")
                self.mutate_job(**{key: value})
                journal = self.make_journal(attempt=value if key == "attempt_id" else "attempt-a",
                                            instance=value if key == "backend_instance_ref" else "remote-a")
                self.assert_code("remote_task_identity_changed", journal.snapshot)
                self.assert_code("remote_task_identity_changed", lambda: journal.bind_task("late-id"))
                self.assert_code("remote_task_identity_changed", lambda: self.prepare(journal=journal))
                with self.state.transaction() as db:
                    self.state.put_job(db, original)
        self.assertEqual(self.count(), 1)

    def test_metadata_identity_fences_existing_journal(self):
        self.prepare()
        with self.state.transaction() as db:
            binding = dict(self.state.binding, owner="different-owner")
            db.execute("UPDATE metadata SET value=? WHERE key='binding'", (encode(binding),))
        self.assert_code("remote_task_identity_changed", self.journal.snapshot)
        self.assert_code("remote_task_identity_changed", self.journal.begin_submit)

    def test_same_scope_same_request_key_cannot_belong_to_another_execution(self):
        self.prepare()
        self.put_job("job-b")
        self.assert_code("remote_task_request_conflict", lambda: self.prepare(journal=self.make_journal("job-b")))
        self.assertEqual(self.count(), 1)

    def test_simultaneous_binding_cannot_assign_one_remote_task_to_two_executions(self):
        self.prepare()
        self.journal.begin_submit()
        self.put_job("job-b")
        other = self.make_journal("job-b")
        self.prepare(journal=other, submit=dict(self.submit, idempotency_key="wbr-" + "b" * 64))
        other.begin_submit()
        barrier = threading.Barrier(2)

        def bind(journal):
            barrier.wait(timeout=3)
            try:
                return journal.bind_task("single-remote-task")["state"]
            except RemoteTaskError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(bind, (self.journal, other)))
        self.assertCountEqual(results, ["bound", "remote_task_mapping_conflict"])
        self.assertEqual(sorted(j.snapshot()["state"] for j in (self.journal, other)), ["bound", "submitting"])

    def test_same_remote_id_in_distinct_scope_is_independent(self):
        self.prepare()
        self.journal.begin_submit()
        self.journal.bind_task("tenant-local-id")
        self.put_job("job-b", account_scope="other-local-account")
        other = self.make_journal("job-b")
        self.prepare(journal=other, binding=dict(self.binding, tenant="other-tenant"))
        other.begin_submit()
        self.assertEqual(other.bind_task("tenant-local-id")["task_id"], "tenant-local-id")

    def test_simultaneous_begin_grants_exactly_one_permit(self):
        self.prepare()
        barrier = threading.Barrier(2)

        def begin(_):
            barrier.wait(timeout=3)
            try:
                return self.make_journal().begin_submit()["state"]
            except RemoteTaskError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(begin, range(2)))
        self.assertCountEqual(result, ["submitting", "remote_task_submit_already_begun"])

    def test_arguments_and_every_return_value_are_independent(self):
        binding, submit = copy.deepcopy(self.binding), copy.deepcopy(self.submit)
        frozen = self.prepare(binding=binding, submit=submit)
        expected = copy.deepcopy(frozen)
        binding["tenant"] = "mutated"
        submit["payload"]["messages"][0]["content"] = "changed"
        frozen["binding"]["principal"] = "changed"
        frozen["submit"]["payload"]["messages"][0]["content"] = "changed"
        self.assertEqual(self.journal.snapshot(), expected)
        begun = self.journal.begin_submit()
        begun["state"] = "prepared"
        begun["submit"]["ttl_seconds"] = 86400
        snapshot = self.journal.snapshot()
        snapshot["submit"].clear()
        self.assertEqual(self.journal.snapshot()["submit"], self.submit)
        bound = self.journal.bind_task("original")
        bound["task_id"] = "replacement"
        self.assertEqual(self.journal.snapshot()["task_id"], "original")

    def test_frozen_prepare_rejects_any_rewrite_without_overwriting(self):
        expected = self.prepare()
        variants = [(dict(self.binding, principal="other"), self.submit, self.input_digest),
                    (self.binding, dict(self.submit, instructions="new instructions"), self.input_digest),
                    (self.binding, dict(self.submit, idempotency_key="wbr-" + "f" * 64), self.input_digest),
                    (self.binding, self.submit, "e" * 64)]
        for binding, submit, input_digest in variants:
            self.assert_code("remote_task_frozen_conflict",
                             lambda: self.prepare(binding=binding, submit=submit, input_digest=input_digest))
            self.assertEqual(self.journal.snapshot(), expected)

    def test_strict_schema_rejects_bad_or_credential_fields_without_new_rows(self):
        cases = []
        for key, value in (("max_attempts", True), ("max_attempts", 2), ("ttl_seconds", True),
                           ("ttl_seconds", 60.0), ("ttl_seconds", 59), ("ttl_seconds", 86401),
                           ("instructions", " "), ("instructions", "x" * 16001),
                           ("idempotency_key", "secret-bearer-value"), ("queue", "different")):
            cases.append((self.binding, dict(self.submit, **{key: value}), self.input_digest))
        for payload in ({"messages": []}, {"messages": [{"role": "assistant", "content": "text"}]},
                        {"messages": [{"role": "user", "content": " "}]},
                        {"messages": [{"role": "user", "content": "x" * 64001}]},
                        {"messages": [{"role": "user", "content": "\ud800"}]},
                        {"messages": [{"role": "user", "content": "ok", "attachment": "private-path"}]}):
            cases.append((self.binding, dict(self.submit, payload=payload), self.input_digest))
        cases.extend([(dict(self.binding, token="secret-bearer-value"), self.submit, self.input_digest),
                      (self.binding, dict(self.submit, token="secret-bearer-value"), self.input_digest),
                      (self.binding, self.submit, "F" * 64),
                      (self.binding, self.submit, float("nan"))])
        for binding, submit, input_digest in cases:
            with self.subTest(submit_keys=list(submit)):
                self.assert_code("remote_task_invalid",
                                 lambda: self.journal.prepare(binding, submit, input_digest))
                self.assertEqual(self.count(), 0)

    def test_prepare_requires_running_current_job_and_bind_requires_begin(self):
        self.assert_code("remote_task_not_prepared", self.journal.snapshot)
        self.assert_code("remote_task_not_prepared", self.journal.begin_submit)
        self.assert_code("remote_task_submit_not_begun", lambda: self.journal.bind_task("id"))
        self.mutate_job(status="unknown")
        self.assert_code("remote_task_not_running", self.prepare)
        self.assertEqual(self.count(), 0)
        self.mutate_job(status="running")
        self.prepare()
        self.assert_code("remote_task_submit_not_begun", lambda: self.journal.bind_task("id"))

    def test_deadline_clock_is_strict_and_expiry_does_not_consume_permit(self):
        self.prepare()
        for value in (2000.0, float("nan"), float("inf"), True, 10 ** 1000):
            self.now = value
            self.assert_code("remote_task_deadline_expired", self.journal.begin_submit)
            self.assertEqual(self.journal.snapshot()["state"], "prepared")
        self.now = 1399.0
        self.assertEqual(self.journal.begin_submit()["state"], "submitting")

    def test_late_begin_cannot_shorten_frozen_ttl_to_fit_deadline(self):
        frozen = self.prepare()
        self.now = 1400.0
        # Still before job deadline, but the full frozen TTL plus I/O margin no longer fits.
        self.assert_code("remote_task_deadline_expired", self.journal.begin_submit)
        self.assertEqual(self.journal.snapshot(), frozen)
        self.assert_code("remote_task_frozen_conflict",
                         lambda: self.prepare(submit=dict(self.submit, ttl_seconds=60)))
        self.assertEqual(self.journal.snapshot()["submit"]["ttl_seconds"], 600)

    def test_sql_constraints_backstop_duplicate_request_and_task_ids(self):
        self.prepare()
        self.journal.begin_submit()
        self.journal.bind_task("only-task")
        with self.assertRaises(sqlite3.IntegrityError):
            with self.state.transaction() as db:
                row = tuple(db.execute("SELECT * FROM remote_tasks").fetchone())
                db.execute("INSERT INTO remote_tasks VALUES (?,?,?,?,?,?)", ("other-job",) + row[1:])
        with self.assertRaises(sqlite3.IntegrityError):
            with self.state.transaction() as db:
                db.execute("INSERT INTO remote_tasks VALUES (?,?,?,?,?,?)",
                           ("other-job", row[1], "other-key", row[3], row[4], row[5]))

    def test_commit_failure_returns_no_permit_and_rolls_back(self):
        self.prepare()
        original = self.state.transaction

        from contextlib import contextmanager
        @contextmanager
        def fail_commit():
            with original() as db:
                yield db
                raise sqlite3.OperationalError("private-request-content-must-not-escape")

        with patch.object(self.state, "transaction", fail_commit):
            self.assert_code("remote_task_storage_failed", self.journal.begin_submit)
        self.assertEqual(self.journal.snapshot()["state"], "prepared")

    def test_legacy_schema_upgrade_preserves_existing_records_and_binding(self):
        self.prepare()
        with self.state.transaction() as db:
            job_before = db.execute("SELECT payload FROM jobs").fetchone()[0]
            binding_before = db.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()[0]
            db.execute("DROP TABLE remote_tasks")
            db.execute("INSERT INTO routes VALUES ('legacy-sender','codex',2)")
        upgraded = BackendState(self.store, self.config, "owner", "bot")
        with upgraded.transaction() as db:
            self.assertEqual(db.execute("SELECT payload FROM jobs").fetchone()[0], job_before)
            self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()[0], binding_before)
            self.assertEqual(tuple(db.execute("SELECT * FROM routes").fetchone()), ("legacy-sender", "codex", 2))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM remote_tasks").fetchone()[0], 0)
        self.prepare(journal=self.make_journal(state=upgraded))
        # A second startup is an idempotent migration and preserves the journal.
        again = BackendState(self.store, self.config, "owner", "bot")
        self.assertEqual(self.make_journal(state=again).snapshot()["state"], "prepared")

    def test_corrupt_persisted_payload_is_safe_and_never_grants_begin(self):
        self.prepare()
        with self.state.transaction() as db:
            db.execute("UPDATE remote_tasks SET payload=?", ('{"schema_version":1,"schema_version":2}',))
        self.assert_code("remote_task_corrupt", self.journal.snapshot)
        self.assert_code("remote_task_corrupt", self.journal.begin_submit)


if __name__ == "__main__":
    unittest.main()
