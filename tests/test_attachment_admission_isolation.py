"""Per-job admission isolation and global safety failures; synthetic only."""
from contextlib import contextmanager
import errno
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.attachment_delivery import DeliveryError
from wechat_bridge.attachments import AttachmentError
from wechat_bridge.store import atomic_write_json, load_json


@contextmanager
def synthetic_fixture():
    fixture = fixtures.RuntimeTests("runTest")
    fixture.setUp()
    try:
        yield fixture
    finally:
        fixture.doCleanups()


class AdmissionIsolationTests(unittest.TestCase):
    @staticmethod
    def queued(f, suffix):
        aid = f.file(mid="synthetic-admission-" + suffix,
                     body=b"SYNTHETIC original " + suffix.encode("ascii"))
        batch = f.batch()
        group = f.success(f.call(batch, "reply", {"attachment_ids": [aid]}))
        return aid, group

    @staticmethod
    def journal(f, group):
        job = runtime.ledger(f.store).job_status(f.scope, group["job_id"])
        return f.base / "state/attachment_admissions" / (job["request_id"] + ".json")

    @staticmethod
    def journal_bytes(path):
        if not path.exists():
            return None
        info = path.stat()
        return (path.read_bytes(), info.st_mode, info.st_ino, info.st_mtime_ns)

    @staticmethod
    def snapshot(f, group):
        state = runtime.ledger(f.store)
        with sqlite3.connect(str(state.path)) as db:
            db.row_factory = sqlite3.Row
            job = dict(db.execute("SELECT * FROM jobs WHERE job_id=?", (group["job_id"],)).fetchone())
            parent = dict(db.execute("SELECT * FROM groups WHERE group_id=?", (group["group_id"],)).fetchone())
            reservations = [dict(row) for row in db.execute(
                "SELECT * FROM reservations WHERE job_id=? ORDER BY unit_id", (group["job_id"],))]
        with sqlite3.connect(str(f.base / "state/attachments.sqlite3")) as db:
            holds = {hid: raw for hid, holder, raw in db.execute("SELECT id,holder,payload FROM holds")
                     if holder in ("group:" + group["group_id"], "group:request:" + job["request_id"])}
        return {"job": job, "group": parent, "reservations": reservations, "holds": holds}

    @staticmethod
    def original_bytes(f):
        return {path.name: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in (f.base / "attachments/objects").iterdir()}

    @contextmanager
    def forbid_bad_job_consumption(self, state, bad_job):
        patches, forbidden = [], []
        for method in ("next_unit", "begin_unit", "settle_unit"):
            original = getattr(state, method)
            def check(scope, job_id, *args, _original=original, _name=method, **kwargs):
                if job_id == bad_job:
                    forbidden.append(_name)
                self.assertNotEqual(job_id, bad_job, "blocked job must not be consumed or settled")
                return _original(scope, job_id, *args, **kwargs)
            wrapped = patch.object(state, method, side_effect=check)
            wrapped.start()
            patches.append(wrapped)
        try:
            yield
        finally:
            for wrapped in reversed(patches):
                wrapped.stop()
            self.assertEqual(forbidden, [], "even a caught call must not consume or settle a blocked job")

    def assert_only_healthy_sends(self, f, bad, good, path):
        state = runtime.ledger(f.store)
        before, source = self.snapshot(f, bad), self.original_bytes(f)
        with sqlite3.connect(str(state.path)) as db:
            contexts = list(db.execute("SELECT * FROM contexts ORDER BY context_ref"))
        journal = self.journal_bytes(path)
        messages, originals = f.send_spy()
        with self.forbid_bad_job_consumption(state, bad["job_id"]):
            f.flush()
            f.flush()  # A blocked retry cannot refund, rewrite, or replay either job.
        self.assertEqual((len(messages), len(originals)), (1, 1))
        self.assertEqual(state.job_status(f.scope, good["job_id"])["state"], "complete")
        self.assertEqual(self.snapshot(f, bad), before)
        self.assertEqual(self.journal_bytes(path), journal)
        self.assertEqual(self.original_bytes(f), source)
        with sqlite3.connect(str(state.path)) as db:
            self.assertEqual(list(db.execute("SELECT * FROM contexts ORDER BY context_ref")), contexts)
        self.assertTrue(before["holds"])
        self.assertTrue(all(row["state"] == "reserved" for row in before["reservations"]))

    def test_missing_unadmitted_and_mismatched_job_admissions_are_individually_blocked(self):
        variants = ("missing", "unadmitted", "integer_admitted", "request", "job", "group", "scope", "context")
        for variant in variants:
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, bad = self.queued(f, variant + "-bad")
                _, good = self.queued(f, variant + "-good")
                path = self.journal(f, bad)
                value = load_json(str(path))
                if variant == "missing":
                    path.unlink()
                else:
                    if variant == "unadmitted":
                        value["admitted"] = False
                        value["phase"] = "publishing"
                    elif variant == "integer_admitted":
                        value["admitted"] = 1
                    elif variant == "request":
                        value["request_id"] = "synthetic-wrong-request"
                    elif variant == "job":
                        value["job_id"] = "aj_" + "0" * 32
                    elif variant == "group":
                        value["group_id"] = "ag_" + "0" * 32
                    elif variant == "scope":
                        value["scope"]["owner_id"] = "synthetic-other-owner"
                    elif variant == "context":
                        value["context_token"] = "synthetic-unregistered-wrong-context"
                    atomic_write_json(str(path), value)
                self.assert_only_healthy_sends(f, bad, good, path)

    def test_public_helper_exit_after_page_commit_is_isolated_without_live_recovery(self):
        with synthetic_fixture() as f:
            aid = f.file(mid="synthetic-public-crash", body=b"SYNTHETIC crash original")
            batch = f.batch()
            script = r'''import hashlib,json,os,socket,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
def deny(*a,**kw):raise AssertionError('synthetic child forbids real sockets')
socket.socket.connect=deny;socket.socket.connect_ex=deny;socket.socket.bind=deny
socket.socket.listen=deny;socket.create_connection=deny;socket.getaddrinfo=deny
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.worker_state import WorkerState
worker=WorkerState(sys.argv[2],sys.argv[3],lambda:float(sys.argv[4]))
runtime.ledger(worker.store)
modules=[]
for name,module in sorted(sys.modules.items()):
 path=getattr(module,'__file__',None)
 if name.startswith('wechat_bridge') and path and Path(path).is_file():
  modules.append(dict(module=name,path=str(Path(path).resolve()),sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()))
print(json.dumps(dict(executable=sys.executable,version=sys.version,modules=modules)),flush=True)
real_write=runtime.atomic_write_json
def crash(path,value):
 if Path(path).parent.name=='attachment_admissions' and value.get('phase')=='admitted':os._exit(77)
 return real_write(path,value)
runtime.atomic_write_json=crash
batch=json.loads(sys.argv[5])
worker.attachment_call(batch['owned'],batch['lease'],dict(schema_version=1,action='reply',arguments=dict(attachment_ids=[sys.argv[6]])))
raise SystemExit(91)
'''
            process = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(ROOT), str(f.base),
                str(f.home / "hook"), str(f.now), json.dumps(batch), aid],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
            self.assertEqual(process.returncode, 77, process.stderr)
            loaded = json.loads(process.stdout.strip())
            for module in loaded["modules"]:
                self.assertEqual(module["sha256"], hashlib.sha256(Path(module["path"]).read_bytes()).hexdigest())
            print("synthetic_admission_child " + json.dumps(dict(exit=77, loaded=loaded)), flush=True)
            state = runtime.ledger(f.store)
            first_job = state.pending_jobs(f.scope)["job_ids"][0]
            job = state.job_status(f.scope, first_job)
            bad = {"job_id": first_job, "group_id": job["group_id"]}
            path = self.journal(f, bad)
            partial = load_json(str(path))
            self.assertEqual((partial["phase"], partial["admitted"]), ("publishing", False))
            self.assertNotIn("job_id", partial)
            f.worker.release(batch["lease"])
            f.receive(f.message("synthetic-ordinary-after-crash", text="SYNTHETIC ordinary request"))
            f.complete_plain(f.batch())
            _, good = self.queued(f, "after-crash")
            before, source, journal = self.snapshot(f, bad), self.original_bytes(f), self.journal_bytes(path)
            messages, originals = f.send_spy()
            with self.forbid_bad_job_consumption(state, first_job):
                f.flush()
                f.flush()
            self.assertEqual((len(messages), len(originals)), (2, 1))  # ordinary + healthy attachment
            self.assertEqual(state.job_status(f.scope, good["job_id"])["state"], "complete")
            self.assertEqual(self.snapshot(f, bad), before)
            self.assertEqual(self.journal_bytes(path), journal)
            self.assertEqual(self.original_bytes(f), source)
            # Only the explicit existing startup-recovery operation may finish
            # this trusted intent. The new flusher must not repair it in place.
            self.assertEqual(runtime.recover_admissions(f.store), 1)
            self.assertEqual(load_json(str(path))["job_id"], first_job)
            f.flush()
            self.assertEqual((len(messages), len(originals)), (3, 2))
            self.assertEqual(state.job_status(f.scope, first_job)["state"], "complete")

    def test_normal_unknown_is_not_pending_and_keeps_its_protection(self):
        with synthetic_fixture() as f:
            _, first = self.queued(f, "unknown-first")
            first_messages, _ = f.send_spy(final_result=TimeoutError("SYNTHETIC uncertain response"))
            f.flush()
            state = runtime.ledger(f.store)
            self.assertEqual(state.job_status(f.scope, first["job_id"])["state"], "unknown")
            self.assertNotIn(first["job_id"], state.pending_jobs(f.scope)["job_ids"])
            before = self.snapshot(f, first)
            _, second = self.queued(f, "unknown-second")
            messages, originals = f.send_spy()
            f.flush()
            self.assertEqual((len(first_messages), len(messages), len(originals)), (1, 1, 1))
            self.assertEqual(self.snapshot(f, first), before)
            self.assertEqual(state.job_status(f.scope, second["job_id"])["state"], "complete")

    def assert_global_failure(self, f, group, exception_type=Exception, code=None):
        before = self.snapshot(f, group)
        messages, originals = f.send_spy()
        with self.assertRaises(exception_type) as error:
            runtime.flush_groups(f.store, "https://synthetic.invalid", f.session["bot_token"])
        if code is not None:
            self.assertEqual(getattr(error.exception, "code", None), code)
        self.assertEqual((messages, originals), ([], []))
        self.assertEqual(self.snapshot(f, group), before)

    def test_global_session_scope_and_epoch_errors_still_propagate(self):
        for variant in ("session", "scope", "epoch"):
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, group = self.queued(f, "global-" + variant)
                current = load_json(f.store.session_path)
                if variant == "session":
                    del current["bot_token"]
                elif variant == "scope":
                    current["ilink_user_id"] = "synthetic-other-owner"
                else:
                    current["login_epoch"] = "f" * 32
                atomic_write_json(f.store.session_path, current)
                self.assert_global_failure(f, group, (runtime.AttachmentCallError, DeliveryError))

    def test_global_unknown_pending_enum_and_database_structure_still_propagate(self):
        for variant in ("pending_enum", "database_structure"):
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, group = self.queued(f, "global-" + variant)
                state = runtime.ledger(f.store)
                with sqlite3.connect(str(state.path)) as db:
                    if variant == "pending_enum":
                        db.execute("UPDATE jobs SET state='future_v9' WHERE job_id=?", (group["job_id"],))
                    else:
                        db.execute("DROP TABLE job_units")
                self.assert_global_failure(f, group, DeliveryError, "storage_invalid")

    def test_global_private_root_permission_error_is_not_a_local_job_failure(self):
        # The existing BASE policy permits owner-controlled 0755; its write
        # protection forbids group/other writable bits. The attachment namespace
        # is separately private and rejects all group/other permissions.
        for name, invalid_mode in (("base", 0o777), ("attachments", 0o755)):
            with self.subTest(root=name), synthetic_fixture() as f:
                _, group = self.queued(f, "global-root-permission-" + name)
                directory = f.base if name == "base" else f.base / "attachments"
                mode = directory.stat().st_mode & 0o777
                try:
                    directory.chmod(invalid_mode)
                    self.assert_global_failure(f, group, Exception, "storage_invalid")
                finally:
                    directory.chmod(mode)

    def test_late_invalid_request_is_rejected_before_any_healthy_consumption(self):
        for value in (".", ".."):
            with self.subTest(request_id=value), synthetic_fixture() as f:
                _, healthy = self.queued(f, "ordered-healthy")
                _, bad = self.queued(f, "ordered-invalid-request")
                state = runtime.ledger(f.store)
                journal = self.journal(f, bad)
                original_journal = self.journal_bytes(journal)
                with sqlite3.connect(str(state.path)) as db:
                    db.execute("UPDATE jobs SET request_id=? WHERE job_id=?", (value, bad["job_id"]))
                before = self.snapshot(f, bad)
                with patch.object(state, "next_unit") as next_unit, \
                        patch.object(state, "begin_unit") as begin_unit, \
                        patch.object(state, "settle_unit") as settle_unit:
                    self.assert_global_failure(f, healthy, DeliveryError, "storage_invalid")
                    next_unit.assert_not_called()
                    begin_unit.assert_not_called()
                    settle_unit.assert_not_called()
                self.assertEqual(self.snapshot(f, bad), before)
                self.assertEqual(self.journal_bytes(journal), original_journal)

    def test_missing_or_unsafe_admission_directory_is_not_a_missing_local_file(self):
        for variant in ("missing", "permissions"):
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, group = self.queued(f, "global-admission-root-" + variant)
                directory = f.base / "state/attachment_admissions"
                if variant == "missing":
                    directory.rename(directory.with_name("synthetic-retained-admissions"))
                else:
                    directory.chmod(0o755)
                self.assert_global_failure(f, group, (runtime.AttachmentCallError, AttachmentError), "storage_invalid")

    def test_late_journal_global_errors_precede_all_unit_consumption(self):
        for variant in ("schema", "bool_schema", "type", "file_mode"):
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, healthy = self.queued(f, "late-format-healthy")
                _, bad = self.queued(f, "late-format-bad")
                path = self.journal(f, bad)
                record = load_json(str(path))
                if variant == "file_mode":
                    path.chmod(0o644)
                else:
                    record["type" if variant == "type" else "schema_version"] = (
                        "attachment_group_v2" if variant == "type" else True if variant == "bool_schema" else 2)
                    atomic_write_json(str(path), record)
                # Terminal maintenance is cursor-bounded and cannot substitute
                # for global preflight of every current pending journal.
                f.store._attachment_terminal_cursor = "z" * 256
                state = runtime.ledger(f.store)
                before = self.snapshot(f, bad), self.journal_bytes(path)
                with patch.object(state, "next_unit", side_effect=AssertionError(
                        "global admission preflight must precede unit consumption")) as next_unit, \
                        patch.object(state, "begin_unit") as begin_unit, \
                        patch.object(state, "settle_unit") as settle_unit:
                    self.assert_global_failure(f, healthy, runtime.AttachmentCallError, "storage_invalid")
                    next_unit.assert_not_called()
                    begin_unit.assert_not_called()
                    settle_unit.assert_not_called()
                self.assertEqual((self.snapshot(f, bad), self.journal_bytes(path)), before)

    def test_global_unknown_io_and_programming_errors_still_propagate(self):
        for variant in ("io", "programming", "scan_permission", "scan_io"):
            with self.subTest(variant=variant), synthetic_fixture() as f:
                _, group = self.queued(f, "global-" + variant)
                if variant == "io":
                    operation = patch.object(runtime.os, "read", side_effect=OSError(errno.EIO, "SYNTHETIC IO"))
                    exception = (runtime.AttachmentCallError, AttachmentError, OSError)
                elif variant == "programming":
                    operation = patch.object(runtime.ledger(f.store), "pending_jobs",
                        side_effect=RuntimeError("SYNTHETIC unexpected programming failure"))
                    exception = RuntimeError
                else:
                    real_scandir = runtime.os.scandir
                    directory = f.base / "state/attachment_admissions"
                    fault = (PermissionError(errno.EACCES, "SYNTHETIC scan permission")
                             if variant == "scan_permission" else OSError(errno.EIO, "SYNTHETIC scan IO"))
                    def fail_scan(path):
                        if isinstance(path, (str, Path)) and Path(path) == directory:
                            raise fault
                        return real_scandir(path)
                    operation = patch.object(runtime.os, "scandir", side_effect=fail_scan)
                    exception = OSError
                with operation:
                    self.assert_global_failure(f, group, exception)


if __name__ == "__main__":
    unittest.main()
