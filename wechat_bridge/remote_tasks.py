"""Durable remote submit intent and task mapping; never a retry or publish API.

Only a committed ``begin_submit`` grants the caller one POST.  A lost response,
process restart, or unknown job must be reconciled outside this journal without
calling begin_submit again.  No provider operation occurs inside transactions.
"""
import contextlib
import hashlib
import json
import math
import re
import sqlite3
import time

from .backends.contract import BackendError


class RemoteTaskError(BackendError):
    """A fixed, public error code without request text or private identities."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _fail(code="remote_task_invalid"):
    raise RemoteTaskError(code) from None


def _text(value, limit, identifier=False):
    if type(value) is not str or not value.strip() or len(value) > limit:
        _fail()
    if identifier and (value != value.strip() or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        _fail()
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        _fail()
    if "\x00" in value:
        _fail()
    return value


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        _fail()


def _encode(value):
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False,
                             sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > 400000:
            _fail()
        return encoded
    except (TypeError, ValueError, UnicodeError, RecursionError):
        _fail()


def _decode(value):
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                _fail("remote_task_corrupt")
            result[key] = item
        return result

    try:
        if type(value) is not str or len(value.encode("utf-8")) > 400000:
            _fail("remote_task_corrupt")
        return json.loads(value, object_pairs_hook=pairs,
                          parse_constant=lambda value: _fail("remote_task_corrupt"))
    except (TypeError, ValueError, UnicodeError, RecursionError):
        _fail("remote_task_corrupt")


def _digest(value):
    return hashlib.sha256(_encode(value).encode("utf-8")).hexdigest()


def _sha(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        _fail()
    return value


def _request(binding, submit, input_digest):
    _keys(binding, ("service", "tenant", "principal", "queue"))
    for name, value in binding.items():
        _text(value, 2048 if name == "service" else 256, identifier=True)
    _keys(submit, ("queue", "instructions", "payload", "max_attempts", "ttl_seconds", "idempotency_key"))
    _text(submit["queue"], 256, identifier=True)
    if submit["queue"] != binding["queue"]:
        _fail()
    _text(submit["instructions"], 16000)
    if type(submit["max_attempts"]) is not int or submit["max_attempts"] != 1:
        _fail()
    if type(submit["ttl_seconds"]) is not int or not 60 <= submit["ttl_seconds"] <= 86400:
        _fail()
    key = submit["idempotency_key"]
    if type(key) is not str or re.fullmatch(r"wbr-[a-f0-9]{64}", key) is None:
        _fail()
    _keys(submit["payload"], ("messages",))
    messages = submit["payload"]["messages"]
    if type(messages) is not list or len(messages) != 1:
        _fail()
    _keys(messages[0], ("role", "content"))
    if messages[0]["role"] != "user" or type(messages[0]["role"]) is not str:
        _fail()
    _text(messages[0]["content"], 64000)
    _sha(input_digest)
    # Copy the entire validated original request before opening the transaction.
    return _decode(_encode(binding)), _decode(_encode(submit))


class RemoteTaskJournal:
    def __init__(self, state, execution_id, attempt_id, instance_id, clock=time.time):
        self._state = state
        self.execution_id = _text(execution_id, 256, identifier=True)
        self.attempt_id = _text(attempt_id, 256, identifier=True)
        self.instance_id = _text(instance_id, 256, identifier=True)
        self.clock = clock
        self._state_binding = _decode(_encode(state.binding))

    @contextlib.contextmanager
    def _transaction(self):
        try:
            with self._state.transaction() as db:
                yield db
        except sqlite3.Error:
            _fail("remote_task_storage_failed")

    def _identity(self, db, running=False):
        row = db.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()
        if (not row or _decode(row[0]) != self._state_binding
                or self._state.binding != self._state_binding):
            _fail("remote_task_identity_changed")
        row = db.execute("SELECT sender,status,payload FROM jobs WHERE id=?", (self.execution_id,)).fetchone()
        if not row:
            _fail("remote_task_job_missing")
        job = _decode(row["payload"])
        if type(job) is not dict:
            _fail("remote_task_corrupt")
        if (job.get("execution_id") != self.execution_id or job.get("attempt_id") != self.attempt_id
                or job.get("backend_instance_ref") != self.instance_id
                or job.get("sender_key") != row["sender"] or job.get("status") != row["status"]):
            _fail("remote_task_identity_changed")
        if running and job.get("status") != "running":
            _fail("remote_task_not_running")
        identity = {"state_binding": self._state_binding}
        for name in ("execution_id", "attempt_id", "backend_instance_ref", "account_scope",
                     "sender_key", "backend_id", "conversation_key"):
            identity[name] = _text(job.get(name), 512, identifier=True)
        deadline = job.get("deadline_at")
        if type(deadline) not in (int, float) or not 0 < deadline < 1e15 or not math.isfinite(deadline):
            _fail("remote_task_invalid_deadline")
        identity["deadline_at"] = deadline
        return identity

    def _load(self, db, identity):
        row = db.execute("SELECT * FROM remote_tasks WHERE execution_id=?", (self.execution_id,)).fetchone()
        if row is None:
            return None
        if _decode(row["identity"]) != identity:
            _fail("remote_task_identity_changed")
        result = _decode(row["payload"])
        _keys(result, ("schema_version", "execution_id", "attempt_id", "backend_instance_ref",
                       "binding", "submit", "submit_digest", "input_digest", "state", "task_id"))
        if (type(result["schema_version"]) is not int or result["schema_version"] != 1
                or result["execution_id"] != self.execution_id or result["attempt_id"] != self.attempt_id
                or result["backend_instance_ref"] != self.instance_id):
            _fail("remote_task_corrupt")
        _request(result["binding"], result["submit"], result["input_digest"])
        if (result["submit_digest"] != _digest(result["submit"])
                or row["scope_id"] != _digest(result["binding"])
                or row["request_key"] != result["submit"]["idempotency_key"]
                or row["task_id"] != result["task_id"]):
            _fail("remote_task_corrupt")
        if result["state"] == "bound":
            _text(result["task_id"], 512, identifier=True)
        elif result["state"] not in ("prepared", "submitting") or result["task_id"] is not None:
            _fail("remote_task_corrupt")
        return result

    def prepare(self, binding, submit, input_digest):
        binding, submit = _request(binding, submit, input_digest)
        result = {"schema_version": 1, "execution_id": self.execution_id, "attempt_id": self.attempt_id,
                  "backend_instance_ref": self.instance_id, "binding": binding, "submit": submit,
                  "submit_digest": _digest(submit), "input_digest": input_digest,
                  "state": "prepared", "task_id": None}
        with self._transaction() as db:
            identity = self._identity(db, running=True)
            previous = self._load(db, identity)
            if previous is not None:
                if any(previous[name] != result[name] for name in result if name not in ("state", "task_id")):
                    _fail("remote_task_frozen_conflict")
                result = previous
            else:
                if db.execute("SELECT 1 FROM remote_tasks WHERE scope_id=? AND request_key=?",
                              (_digest(binding), submit["idempotency_key"])).fetchone():
                    _fail("remote_task_request_conflict")
                db.execute("INSERT INTO remote_tasks VALUES (?,?,?,NULL,?,?)",
                           (self.execution_id, _digest(binding), submit["idempotency_key"],
                            _encode(identity), _encode(result)))
        return result

    def begin_submit(self):
        with self._transaction() as db:
            identity = self._identity(db, running=True)
            result = self._load(db, identity)
            if result is None:
                _fail("remote_task_not_prepared")
            if result["state"] != "prepared":
                _fail("remote_task_submit_already_begun")
            now = self.clock()
            if (type(now) not in (int, float) or not 0 <= now < 1e15 or not math.isfinite(now)
                    or now + result["submit"]["ttl_seconds"] + 1 > identity["deadline_at"]):
                _fail("remote_task_deadline_expired")
            result["state"] = "submitting"
            db.execute("UPDATE remote_tasks SET payload=? WHERE execution_id=?",
                       (_encode(result), self.execution_id))
        # A committed state change is the one-use submission permit.
        return result

    def bind_task(self, task_id):
        task_id = _text(task_id, 512, identifier=True)
        with self._transaction() as db:
            identity = self._identity(db)
            result = self._load(db, identity)
            if result is None or result["state"] == "prepared":
                _fail("remote_task_submit_not_begun")
            if result["task_id"] is not None and result["task_id"] != task_id:
                _fail("remote_task_mapping_conflict")
            conflict = db.execute("SELECT execution_id FROM remote_tasks WHERE scope_id=? AND task_id=?",
                                  (_digest(result["binding"]), task_id)).fetchone()
            if conflict and conflict[0] != self.execution_id:
                _fail("remote_task_mapping_conflict")
            result.update(state="bound", task_id=task_id)
            db.execute("UPDATE remote_tasks SET task_id=?,payload=? WHERE execution_id=?",
                       (task_id, _encode(result), self.execution_id))
        return result

    def snapshot(self):
        with self._transaction() as db:
            result = self._load(db, self._identity(db))
            if result is None:
                _fail("remote_task_not_prepared")
        return result
