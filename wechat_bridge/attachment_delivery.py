"""Durable, local attachment groups and shared physical-send accounting.

Construct only at controlled writable entry points. This is deliberately separate
from the legacy JSON outbox: an older sender cannot accidentally finish a group.
The module has no network, path-resolution, catalog, or worker-lease authority.
"""
from contextlib import contextmanager
import base64
import errno
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid

from .ilink import split_text


CAP = 8
JOB_SECONDS = 1800
MAX_DATABASE_BYTES = 64 << 20  # Local metadata budget, not a platform limit.
_SCOPE = frozenset(("bot_id", "owner_id", "conversation_id", "login_epoch"))
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
_OCCUPIED = ("reserved", "inflight", "accepted", "unknown")
_TERMINAL_JOBS = ("complete", "partial", "unknown", "cancelled", "expired", "budget_exhausted")
_GROUP_STATES = ("queued", "sending", "partial", "awaiting_continue", "unknown", "complete", "cancelled", "expired")
_JOB_STATES = ("queued", "sending") + _TERMINAL_JOBS
_RESERVATION_STATES = _OCCUPIED + ("proven_unsent",)
_PAYLOAD_KINDS = ("text", "attachment")
_JOB_UNIT_KINDS = _PAYLOAD_KINDS + ("status",)
_SAFE_TERMINAL_JOBS = ("complete", "partial", "cancelled", "expired", "budget_exhausted")
_INSPECT_MAX_BYTES = 256 << 20  # Read-only inspection budget, not a platform limit.


class DeliveryError(ValueError):
    """Fixed, non-sensitive category suitable for the public wrapper."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _identifier(value):
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise DeliveryError("invalid_request")
    return value


def _scope(value):
    if (type(value) is not dict or set(value) != _SCOPE or
            any(type(v) is not str or not v.strip() or len(v) > 512 or "\x00" in v
                for v in value.values())):
        raise DeliveryError("invalid_request")
    return dict(value)


class DeliveryLedger:
    """One instance owns a claimed page until settle, cancellation, or recovery.

    ``next_unit`` claims a job for this instance; ``begin_unit`` and
    ``settle_unit`` must use that same instance. ``recover`` is an explicit sole
    sender startup operation, never a constructor side effect.
    """
    def __init__(self, base, clock=time.time, fault=None):
        self.base = Path(os.path.abspath(os.fspath(base)))
        self.clock, self.fault = clock, fault
        self.owner = uuid.uuid4().hex
        self._claims = {}
        for directory in (self.base, self.base / "state"):
            if directory.is_symlink():
                raise DeliveryError("storage_invalid")
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not directory.is_dir():
                raise DeliveryError("storage_invalid")
        self.path = self.base / "state" / "attachment-delivery.sqlite3"
        created = False
        try:
            fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR |
                         getattr(os, "O_NOFOLLOW", 0), 0o600)
        except FileExistsError:
            pass
        except OSError as error:
            raise DeliveryError("quota_exceeded" if error.errno in (errno.ENOSPC, errno.EDQUOT) else "storage_invalid") from None
        else:
            os.close(fd)
            created = True
        self._check_files()
        try:
            with self._transaction(initialize=created) as db:
                if created:
                    self._schema(db)
                self._validate_group_rows(db)
                row = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
                if row is None or row[0] != "1":
                    raise DeliveryError("storage_invalid")
                key = db.execute("SELECT value FROM metadata WHERE key='cursor_secret'").fetchone()
                if key is None or not re.fullmatch(r"[0-9a-f]{64}", key[0]):
                    raise DeliveryError("storage_invalid")
                self._cursor_secret = bytes.fromhex(key[0])
        except DeliveryError:
            raise

    @classmethod
    def open_existing(cls, base, clock=time.time):
        """Open an initialized ledger without mkdir, creation or a write txn."""
        value = object.__new__(cls)
        value.base = Path(os.path.abspath(os.fspath(base)))
        value.path = value.base / "state" / "attachment-delivery.sqlite3"
        value.clock, value.fault, value.owner, value._claims = clock, None, uuid.uuid4().hex, {}
        try:
            info = value.path.lstat()
        except FileNotFoundError:
            raise DeliveryError("storage_invalid") from None
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise DeliveryError("storage_invalid")
        with value._readonly() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
            key = db.execute("SELECT value FROM metadata WHERE key='cursor_secret'").fetchone()
            if (row is None or row[0] != "1" or key is None
                    or type(key[0]) is not str or not re.fullmatch(r"[0-9a-f]{64}", key[0])):
                raise DeliveryError("storage_invalid")
            value._cursor_secret = bytes.fromhex(key[0])
        return value

    def admission_job(self, scope, request_id, expected_ids=None, expected_caption=None):
        """Read an existing job identity for a journal, without creating it."""
        scope, request_id = _scope(scope), _identifier(request_id)
        with self._readonly() as db:
            self._guard(db, scope)
            rows = db.execute("SELECT * FROM jobs WHERE scope_key=? AND request_id=? LIMIT 2",
                              (self._scope_key(scope), request_id)).fetchall()
            if len(rows) > 1:
                raise DeliveryError("storage_invalid")
            if rows and expected_ids is not None:
                group = self._group(db, scope, rows[0]["group_id"])
                if (group["request_id"] != request_id
                        or json.loads(group["attachment_ids"]) != expected_ids
                        or group["caption"] != expected_caption
                        or group["request_digest"] != _digest([expected_ids, expected_caption])):
                    raise DeliveryError("storage_invalid")
            return self._job_view(db, self._job(db, scope, rows[0]["job_id"])) if rows else None

    def preflight_admission(self, scope, request_id, context_token, group_id=None, check_materials=None,
                            journal_directory_exists=True, check_remaining_materials=None):
        """Reject known fresh-request failures without intent or ledger writes.

        The real page transaction remains authoritative. Expired reservations
        retain the existing maintenance semantics; budget zero is legal parked
        admission, never a reason to send an unbudgeted notice or reject here.
        """
        scope, request_id = _scope(scope), _identifier(request_id)
        self.context_reference(scope, context_token)
        with self._readonly() as db:
            self._guard(db, scope)
            key = self._scope_key(scope)
            if not journal_directory_exists and (
                    db.execute("SELECT 1 FROM groups LIMIT 1").fetchone()
                    or db.execute("SELECT 1 FROM jobs LIMIT 1").fetchone()):
                raise DeliveryError("storage_invalid")
            if (db.execute("SELECT 1 FROM groups WHERE scope_key=? AND request_id=? LIMIT 1", (key, request_id)).fetchone()
                    or db.execute("SELECT 1 FROM jobs WHERE scope_key=? AND request_id=? LIMIT 1", (key, request_id)).fetchone()):
                raise DeliveryError("storage_invalid")
            metadata = check_materials() if check_materials is not None else []
            if group_id is None:
                return metadata
            group = self._group(db, scope, group_id)
            status = self._status(db, group)
            if group["cancelled"]:
                raise DeliveryError("cancelled")
            jobs = db.execute("SELECT * FROM jobs WHERE group_id=? AND state IN ('queued','sending')",
                              (group_id,)).fetchall()
            for job in jobs:
                self._job_parent(db, job)
            expired_inflight = db.execute("SELECT 1 FROM reservations r JOIN jobs j ON r.job_id=j.job_id "
                "WHERE j.group_id=? AND j.state IN ('queued','sending') AND j.expires_at<=? "
                "AND r.state='inflight' LIMIT 1", (group_id, self._now())).fetchone()
            if status["state"] == "unknown" or expired_inflight:
                raise DeliveryError("group_unknown")
            if any(job["expires_at"] > self._now() for job in jobs):
                raise DeliveryError("group_busy")
            remaining = [row for row in self._payload(db, group_id) if row["reservation_state"] != "accepted"]
            if not remaining:
                raise DeliveryError("invalid_request")
            if any(row["reservation_state"] in ("inflight", "unknown") for row in remaining):
                raise DeliveryError("group_unknown")
            remaining_ids = list(dict.fromkeys(row["attachment_id"] for row in remaining
                                               if row["kind"] == "attachment"))
            if remaining_ids and check_remaining_materials is not None:
                check_remaining_materials(remaining_ids)
            return metadata

    def _check_files(self):
        for path in (self.path, Path(str(self.path) + "-journal"),
                     Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm")):
            try:
                info = path.lstat()
            except FileNotFoundError:
                if path == self.path:
                    raise DeliveryError("storage_invalid") from None
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise DeliveryError("storage_invalid")
            if path == self.path and info.st_size > MAX_DATABASE_BYTES:
                raise DeliveryError("quota_exceeded")
        if self.base.is_symlink() or (self.base / "state").is_symlink():
            raise DeliveryError("storage_invalid")

    @contextmanager
    def _transaction(self, check_authority=None, initialize=False):
        self._check_files()
        db = None
        authority_failed = False
        try:
            # The constructor exclusively creates a new file. Later operations
            # must not recreate a lost ledger between validation and connect.
            db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True,
                                 timeout=0.25, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            page_limit = MAX_DATABASE_BYTES // page_size
            if page_limit < 1 or db.execute("PRAGMA max_page_count=%d" % page_limit).fetchone()[0] > page_limit:
                raise DeliveryError("quota_exceeded")
            db.execute("BEGIN IMMEDIATE")
            if not initialize:
                self._validate_group_rows(db)
            yield db
            self._validate_group_rows(db)
            if check_authority is not None:
                authority_failed = True
                check_authority()
                authority_failed = False
            db.commit()
        except sqlite3.Error as error:
            if db is not None:
                db.rollback()
            if authority_failed:
                raise
            if isinstance(error, sqlite3.OperationalError) and (
                    "locked" in str(error).lower() or "busy" in str(error).lower()):
                raise DeliveryError("storage_busy") from None
            if "full" in str(error).lower():
                raise DeliveryError("quota_exceeded") from None
            raise DeliveryError("storage_invalid") from None
        except OSError as error:
            if db is not None:
                db.rollback()
            if authority_failed:
                raise
            raise DeliveryError("quota_exceeded" if error.errno in (errno.ENOSPC, errno.EDQUOT) else "storage_invalid") from None
        except BaseException:
            if db is not None:
                db.rollback()
            raise
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _validate_group_rows(db):
        # Defaults and negative state tests cannot prove a v1 row is understood.
        # These positive sets also cover orphan rows, not only successful joins.
        if db.execute("SELECT 1 FROM groups WHERE typeof(schema_version)!='integer' OR schema_version!=1 OR typeof(type)!='text' OR type!='attachment_group_v1' LIMIT 1").fetchone():
            raise DeliveryError("storage_invalid")
        enums = (("groups", "state", _GROUP_STATES), ("jobs", "state", _JOB_STATES),
                 ("reservations", "state", _RESERVATION_STATES),
                 ("payload_units", "kind", _PAYLOAD_KINDS), ("job_units", "kind", _JOB_UNIT_KINDS))
        for table, column, allowed in enums:
            placeholders = ",".join("?" for _ in allowed)
            if db.execute("SELECT 1 FROM " + table + " WHERE typeof(" + column + ")!='text' OR " + column + " NOT IN (" + placeholders + ") LIMIT 1", allowed).fetchone():
                raise DeliveryError("storage_invalid")
        for table, column in (("groups", "cancelled"), ("contexts", "legacy_unknown")):
            if db.execute("SELECT 1 FROM " + table + " WHERE typeof(" + column + ")!='integer' OR " + column + " NOT IN (0,1) LIMIT 1").fetchone():
                raise DeliveryError("storage_invalid")

    @staticmethod
    def _enum(value, allowed):
        if type(value) is not str or value not in allowed:
            raise DeliveryError("storage_invalid")

    @staticmethod
    def _group_row(row):
        if (row is None or type(row["schema_version"]) is not int or row["schema_version"] != 1
                or type(row["type"]) is not str or row["type"] != "attachment_group_v1"
                or type(row["cancelled"]) is not int or row["cancelled"] not in (0, 1)):
            raise DeliveryError("storage_invalid")
        DeliveryLedger._enum(row["state"], _GROUP_STATES)
        return row

    @contextmanager
    def _readonly(self):
        self._check_files()
        db = None
        try:
            db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True,
                                 timeout=0.25, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("BEGIN")
            self._validate_group_rows(db)
            yield db
        except sqlite3.Error as error:
            if isinstance(error, sqlite3.OperationalError) and (
                    "locked" in str(error).lower() or "busy" in str(error).lower()):
                raise DeliveryError("storage_busy") from None
            raise DeliveryError("storage_invalid") from None
        except OSError as error:
            raise DeliveryError("quota_exceeded" if error.errno in (errno.ENOSPC, errno.EDQUOT) else "storage_invalid") from None
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _schema_statements():
        return (
            "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
            "CREATE TABLE identities(identity_key TEXT PRIMARY KEY,active_epoch TEXT NOT NULL)",
            "CREATE TABLE epochs(identity_key TEXT,epoch TEXT,PRIMARY KEY(identity_key,epoch))",
            "CREATE TABLE contexts(context_ref TEXT PRIMARY KEY,scope_key TEXT NOT NULL,legacy_unknown INTEGER NOT NULL,created_at REAL NOT NULL,origin_epoch TEXT NOT NULL)",
            "CREATE TABLE groups(group_id TEXT PRIMARY KEY,scope_key TEXT NOT NULL,scope_json TEXT NOT NULL,request_id TEXT NOT NULL,request_digest TEXT NOT NULL,attachment_ids TEXT NOT NULL,caption TEXT NOT NULL,created_at REAL NOT NULL,cancelled INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL,schema_version INTEGER NOT NULL DEFAULT 1,type TEXT NOT NULL DEFAULT 'attachment_group_v1',UNIQUE(scope_key,request_id))",
            "CREATE TABLE payload_units(unit_id TEXT PRIMARY KEY,group_id TEXT NOT NULL,position INTEGER NOT NULL,kind TEXT NOT NULL,text TEXT,attachment_id TEXT,ordinal INTEGER,UNIQUE(group_id,position))",
            "CREATE TABLE jobs(job_id TEXT PRIMARY KEY,group_id TEXT NOT NULL,scope_key TEXT NOT NULL,request_id TEXT NOT NULL,context_ref TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,state TEXT NOT NULL,owner TEXT,fence INTEGER NOT NULL DEFAULT 0,error TEXT,login_epoch TEXT NOT NULL,UNIQUE(group_id,request_id))",
            "CREATE TABLE job_units(job_id TEXT NOT NULL,position INTEGER NOT NULL,unit_id TEXT NOT NULL,kind TEXT NOT NULL,text TEXT,attachment_id TEXT,ordinal INTEGER,PRIMARY KEY(job_id,position),UNIQUE(job_id,unit_id))",
            "CREATE TABLE reservations(scope_key TEXT NOT NULL,unit_id TEXT NOT NULL,context_ref TEXT NOT NULL,state TEXT NOT NULL,group_id TEXT,job_id TEXT,owner TEXT,updated_at REAL NOT NULL,origin_epoch TEXT NOT NULL,PRIMARY KEY(scope_key,unit_id))",
            "CREATE INDEX reservation_context ON reservations(context_ref,state)",
            "CREATE INDEX reservation_group ON reservations(group_id,state)",
            "CREATE INDEX group_scope ON groups(scope_key,created_at,group_id)",
            "CREATE INDEX job_group ON jobs(group_id,state)",
        )
    @staticmethod
    def _schema(db):
        for statement in DeliveryLedger._schema_statements():
            db.execute(statement)
        db.execute("INSERT INTO metadata VALUES('schema_version','1')")
        db.execute("INSERT INTO metadata VALUES('cursor_secret',?)", (os.urandom(32).hex(),))

    @staticmethod
    def inspect_readonly(base):
        """Recognize only our exact, quiescent ledger, without constructing it.

        Missing storage is unavailable, not initialized. Sidecars, unknown
        schema/data versions, links, corruption, or a changing file fail closed.
        Catalog holds remain the authority for protecting original bytes; the
        summary contains counts only and never exposes captions or credentials.
        """
        handles, db, expected = [], None, None
        try:
            root = Path(os.path.abspath(os.fspath(base)))
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                root_fd = os.open(str(root), directory_flags)
            except FileNotFoundError:
                return {"schema_version": 1, "status": "unavailable"}
            handles.append(root_fd)
            root_info = os.fstat(root_fd)
            if not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.geteuid() or root_info.st_mode & 0o022:
                raise DeliveryError("storage_invalid")
            try:
                state_fd = os.open("state", directory_flags, dir_fd=root_fd)
            except FileNotFoundError:
                return {"schema_version": 1, "status": "unavailable"}
            handles.append(state_fd)
            state_info = os.fstat(state_fd)
            if not stat.S_ISDIR(state_info.st_mode) or state_info.st_uid != os.geteuid() or state_info.st_mode & 0o077:
                raise DeliveryError("storage_invalid")
            name = "attachment-delivery.sqlite3"
            try:
                file_fd = os.open(name, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) |
                                  getattr(os, "O_NOFOLLOW", 0), dir_fd=state_fd)
            except FileNotFoundError:
                return {"schema_version": 1, "status": "unavailable"}
            handles.append(file_fd)
            info = os.fstat(file_fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or not 512 <= info.st_size <= _INSPECT_MAX_BYTES):
                raise DeliveryError("storage_invalid")
            if info.st_size > MAX_DATABASE_BYTES:
                raise DeliveryError("quota_exceeded")

            def sidecars_absent():
                for suffix in ("-journal", "-wal", "-shm"):
                    try:
                        os.stat(name + suffix, dir_fd=state_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    raise DeliveryError("storage_invalid")

            def signature(value):
                return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
                        value.st_size, value.st_mtime_ns, value.st_ctime_ns)

            sidecars_absent()
            header = os.pread(file_fd, 100, 0)
            if len(header) != 100 or header[:16] != b"SQLite format 3\x00" or header[18:20] != b"\x01\x01":
                raise DeliveryError("storage_invalid")
            page_size = int.from_bytes(header[16:18], "big")
            page_size = 65536 if page_size == 1 else page_size
            if page_size < 512 or page_size > 65536 or page_size & (page_size - 1) or info.st_size % page_size:
                raise DeliveryError("storage_invalid")
            # The already-open descriptor is pinned with O_NOFOLLOW. immutable
            # prevents journal recovery, lock files, or any SQLite disk writes.
            db = sqlite3.connect("file:/dev/fd/%d?mode=ro&immutable=1" % file_fd, uri=True, timeout=0)
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA temp_store=MEMORY")
            ticks = [0]
            def bounded():
                ticks[0] += 1
                return int(ticks[0] > 20000)
            db.set_progress_handler(bounded, 1000)
            expected = sqlite3.connect(":memory:")
            expected.execute("PRAGMA temp_store=MEMORY")
            DeliveryLedger._schema(expected)
            query = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            if db.execute(query).fetchall() != expected.execute(query).fetchall():
                raise DeliveryError("storage_invalid")
            tables = [row[0] for row in expected.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            for table in tables:  # Controlled names come from our own in-memory schema.
                if db.execute("PRAGMA table_xinfo(" + table + ")").fetchall() != expected.execute("PRAGMA table_xinfo(" + table + ")").fetchall():
                    raise DeliveryError("storage_invalid")
            if (db.execute("PRAGMA user_version").fetchone() != (0,) or
                    db.execute("PRAGMA application_id").fetchone() != (0,) or
                    db.execute("PRAGMA quick_check(1)").fetchall() != [("ok",)]):
                raise DeliveryError("storage_invalid")
            metadata = dict(db.execute("SELECT key,value FROM metadata").fetchall())
            if (set(metadata) != {"schema_version", "cursor_secret"} or metadata["schema_version"] != "1"
                    or type(metadata["cursor_secret"]) is not str
                    or not re.fullmatch(r"[0-9a-f]{64}", metadata["cursor_secret"])):
                raise DeliveryError("storage_invalid")
            DeliveryLedger._validate_group_rows(db)
            valid_states = {
                "groups": _GROUP_STATES, "jobs": _JOB_STATES,
                "reservations": _RESERVATION_STATES,
            }
            counts = {}
            for table, allowed in valid_states.items():
                rows = db.execute("SELECT state,count(*) FROM " + table + " GROUP BY state").fetchall()
                if any(state not in allowed for state, count in rows):
                    raise DeliveryError("storage_invalid")
                counts[table] = dict(rows)
            protected = db.execute("SELECT count(DISTINCT p.attachment_id) FROM payload_units p JOIN groups g ON g.group_id=p.group_id WHERE p.kind='attachment' AND g.state!='complete'").fetchone()[0]
            sidecars_absent()
            if (signature(info) != signature(os.fstat(file_fd)) or
                    signature(info) != signature(os.stat(name, dir_fd=state_fd, follow_symlinks=False)) or
                    signature(state_info) != signature(os.fstat(state_fd)) or
                    signature(state_info) != signature(os.stat("state", dir_fd=root_fd, follow_symlinks=False)) or
                    signature(root_info) != signature(os.fstat(root_fd)) or
                    signature(root_info) != signature(root.lstat())):
                raise DeliveryError("storage_invalid")
            return {"schema_version": 1, "status": "known", "read_only": True,
                    "database": name, "counts": counts,
                    "protected_attachment_count": protected, "catalog_holds_required": True}
        except DeliveryError:
            raise
        except (OSError, sqlite3.Error, ValueError, TypeError, OverflowError):
            raise DeliveryError("storage_invalid") from None
        finally:
            if expected is not None:
                expected.close()
            if db is not None:
                db.close()
            for handle in reversed(handles):
                os.close(handle)

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise DeliveryError("invalid_request")
        return float(value)

    def _fault(self, point):
        if self.fault is not None:
            self.fault(point)

    @staticmethod
    def _scope_key(scope):
        # A new login revokes authority; it must never reset accounting or an
        # accepted/unknown unit belonging to the same bot and person.
        return _digest({k: v for k, v in scope.items() if k != "login_epoch"})

    @staticmethod
    def _identity(scope):
        return _digest({k: v for k, v in scope.items() if k != "login_epoch"})

    def _guard(self, db, scope, initialize=False, activate=False):
        identity, epoch = self._identity(scope), scope["login_epoch"]
        row = db.execute("SELECT active_epoch FROM identities WHERE identity_key=?", (identity,)).fetchone()
        if row is None:
            if not initialize:
                raise DeliveryError("epoch_mismatch")
            db.execute("INSERT INTO identities VALUES(?,?)", (identity, epoch))
            db.execute("INSERT INTO epochs VALUES(?,?)", (identity, epoch))
        elif row[0] != epoch:
            seen = db.execute("SELECT 1 FROM epochs WHERE identity_key=? AND epoch=?", (identity, epoch)).fetchone()
            if not activate or seen is not None:
                raise DeliveryError("epoch_mismatch")
            old = dict(scope, login_epoch=row[0])
            old_key = self._scope_key(old)
            for job in db.execute("SELECT * FROM jobs WHERE scope_key=? AND login_epoch=? AND state IN ('queued','sending')", (old_key, row[0])).fetchall():
                self._stop_job(db, job, "cancelled", "epoch_mismatch")
            db.execute("UPDATE reservations SET state=CASE state WHEN 'inflight' THEN 'unknown' ELSE 'proven_unsent' END,owner=NULL,updated_at=? WHERE scope_key=? AND origin_epoch=? AND group_id IS NULL AND state IN ('reserved','inflight')", (self._now(), old_key, row[0]))
            db.execute("UPDATE groups SET cancelled=1 WHERE scope_key=?", (old_key,))
            for group in db.execute("SELECT group_id FROM groups WHERE scope_key=?", (old_key,)).fetchall():
                self._update_group(db, group[0])
            db.execute("UPDATE identities SET active_epoch=? WHERE identity_key=?", (epoch, identity))
            db.execute("INSERT INTO epochs VALUES(?,?)", (identity, epoch))

    def context_reference(self, scope, token):
        """Derive the existing context identity without registering or mutating it."""
        scope = _scope(scope)
        if type(token) is not str or len(token) > 65536 or "\x00" in token:
            raise DeliveryError("invalid_request")
        return "ctx:" + _digest([self._identity(scope), token])

    def _context(self, db, scope, token, fresh=False):
        ref = self.context_reference(scope, token)
        row = db.execute("SELECT * FROM contexts WHERE context_ref=?", (ref,)).fetchone()
        if row is None:
            db.execute("INSERT INTO contexts VALUES(?,?,?,?,?)", (
                ref, self._scope_key(scope), int(not fresh or not token), self._now(), scope["login_epoch"]))
            row = db.execute("SELECT * FROM contexts WHERE context_ref=?", (ref,)).fetchone()
        return row

    @staticmethod
    def _used(db, context):
        return (CAP if context["legacy_unknown"] else 0) + db.execute(
            "SELECT count(*) FROM reservations WHERE context_ref=? AND state IN ('reserved','inflight','accepted','unknown')",
            (context["context_ref"],)).fetchone()[0]

    def _budget(self, db, context):
        used = self._used(db, context)
        return {"schema_version": 1, "context_ref": context["context_ref"],
                "legacy_unknown": bool(context["legacy_unknown"]), "used": used,
                "remaining": max(0, CAP - used), "cap": CAP}

    def register_context(self, scope, context_token, fresh=False):
        scope = _scope(scope)
        if type(fresh) is not bool:
            raise DeliveryError("invalid_request")
        with self._transaction() as db:
            self._guard(db, scope, initialize=True, activate=fresh)
            context = self._context(db, scope, context_token, fresh)
            result = self._budget(db, context)
        return result

    def _reserve(self, db, scope_key, context, unit_ids, group_id=None, job_id=None, epoch=None):
        rows = {}
        needed = 0
        for unit_id in unit_ids:
            row = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (scope_key, unit_id)).fetchone()
            rows[unit_id] = row
            if row is not None and row["group_id"] != group_id:
                raise DeliveryError("invalid_request")
            if row is not None and row["state"] == "unknown":
                raise DeliveryError("group_unknown")
            if row is None or row["state"] == "proven_unsent" or (
                    row["state"] == "reserved" and row["context_ref"] != context["context_ref"]):
                needed += 1
        if needed > max(0, CAP - self._used(db, context)):
            raise DeliveryError("budget_exhausted")
        for unit_id, old in rows.items():
            if old is None:
                db.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?,?)", (
                    scope_key, unit_id, context["context_ref"], "reserved", group_id, job_id, None, self._now(), epoch))
            elif old["state"] == "proven_unsent" or (old["state"] == "reserved" and old["context_ref"] != context["context_ref"]):
                db.execute("UPDATE reservations SET context_ref=?,state='reserved',job_id=?,owner=NULL,updated_at=?,origin_epoch=? WHERE scope_key=? AND unit_id=?", (
                    context["context_ref"], job_id, self._now(), epoch, scope_key, unit_id))
        self._fault("reservation_before_commit")

    @staticmethod
    def _reservation_view(row, send_allowed=False):
        DeliveryLedger._enum(row["state"], _RESERVATION_STATES)
        return {"schema_version": 1, "unit_id": row["unit_id"], "state": row["state"],
                "context_ref": row["context_ref"], "send_allowed": send_allowed}

    def reserve_units(self, scope, context_token, unit_ids):
        scope = _scope(scope)
        if (type(unit_ids) is not list or len(unit_ids) > CAP or
                len(set(_identifier(x) for x in unit_ids)) != len(unit_ids)):
            raise DeliveryError("invalid_request")
        with self._transaction() as db:
            self._guard(db, scope, initialize=True)
            context = self._context(db, scope, context_token)
            self._reserve(db, self._scope_key(scope), context, unit_ids, epoch=scope["login_epoch"])
            units = [self._reservation_view(db.execute(
                "SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (self._scope_key(scope), unit)).fetchone()) for unit in unit_ids]
            result = dict(self._budget(db, context), units=units)
        self._fault("reservation_committed")
        return result

    def _get_reservation(self, db, scope, token, unit_id, group=False):
        _identifier(unit_id)
        context = self._context(db, scope, token)
        row = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (self._scope_key(scope), unit_id)).fetchone()
        if row is None or (row["group_id"] is not None and not group):
            raise DeliveryError("not_found")
        if row["state"] == "accepted":
            return row
        if row["context_ref"] != context["context_ref"]:
            raise DeliveryError("group_unknown" if row["state"] == "unknown" else "group_busy")
        return row

    def begin_reserved(self, scope, context_token, unit_id):
        scope = _scope(scope)
        with self._transaction() as db:
            self._guard(db, scope)
            row = self._get_reservation(db, scope, context_token, unit_id)
            allowed = self._begin(db, row)
            current = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (self._scope_key(scope), unit_id)).fetchone()
            result = self._reservation_view(current, allowed)
        if allowed:
            self._fault("unit_inflight_committed")
        return result

    def _begin(self, db, row):
        self._enum(row["state"], _RESERVATION_STATES)
        if row["state"] == "accepted":
            return False
        if row["state"] == "unknown":
            raise DeliveryError("group_unknown")
        if row["state"] != "reserved":
            raise DeliveryError("group_busy")
        db.execute("UPDATE reservations SET state='inflight',owner=?,updated_at=? WHERE scope_key=? AND unit_id=? AND state='reserved'", (
            self.owner, self._now(), row["scope_key"], row["unit_id"]))
        return True

    @staticmethod
    def _outcome(outcome):
        if type(outcome) is not str or outcome not in ("accepted", "proven_unsent", "unknown"):
            raise DeliveryError("invalid_request")

    def _settle(self, db, row, outcome):
        self._enum(row["state"], _RESERVATION_STATES)
        if row["state"] in ("accepted", "proven_unsent", "unknown"):
            if row["state"] != outcome:
                raise DeliveryError("group_unknown" if row["state"] == "unknown" else "group_busy")
            return
        if row["state"] == "inflight" and row["owner"] != self.owner:
            raise DeliveryError("group_busy")
        if outcome == "accepted" and row["state"] != "inflight":
            raise DeliveryError("group_busy")
        db.execute("UPDATE reservations SET state=?,owner=NULL,updated_at=? WHERE scope_key=? AND unit_id=?", (
            outcome, self._now(), row["scope_key"], row["unit_id"]))
        self._fault("unit_settle_before_commit")

    def settle_reserved(self, scope, context_token, unit_id, outcome):
        scope = _scope(scope)
        self._outcome(outcome)
        with self._transaction() as db:
            self._guard(db, scope)
            row = self._get_reservation(db, scope, context_token, unit_id)
            self._settle(db, row, outcome)
            current = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (self._scope_key(scope), unit_id)).fetchone()
            result = self._reservation_view(current)
        self._fault("unit_settle_committed")
        return result

    def create_group(self, scope, request_id, attachment_ids, caption="", *, trusted_caution="", check_authority=None):
        """Accept a public caption plus a bounded, source-generated caution.

        Only the public layer may supply ``trusted_caution``; it is not a model
        request field. Both parts form immutable, fully budgeted text payload.
        """
        scope = _scope(scope)
        _identifier(request_id)
        if (type(attachment_ids) is not list or not 1 <= len(attachment_ids) <= 16 or
                len(set(_identifier(x) for x in attachment_ids)) != len(attachment_ids) or
                type(caption) is not str or len(caption) > 16000 or
                type(trusted_caution) is not str or len(trusted_caution) > 128):
            raise DeliveryError("invalid_request")
        if check_authority is not None:
            if not callable(check_authority):
                raise DeliveryError("invalid_request")
            check_authority()
        caption += trusted_caution
        key = self._scope_key(scope)
        digest = _digest([attachment_ids, caption])
        with self._transaction(check_authority=check_authority) as db:
            self._guard(db, scope, initialize=True)
            old = db.execute("SELECT * FROM groups WHERE scope_key=? AND request_id=?", (key, request_id)).fetchone()
            if old is not None:
                if json.loads(old["scope_json"])["login_epoch"] != scope["login_epoch"]:
                    raise DeliveryError("epoch_mismatch")
                if old["request_digest"] != digest:
                    raise DeliveryError("invalid_request")
                return self._status(db, old)
            group_id = "ag_" + uuid.uuid4().hex
            db.execute("INSERT INTO groups(group_id,scope_key,scope_json,request_id,request_digest,attachment_ids,caption,created_at,cancelled,state) VALUES(?,?,?,?,?,?,?,?,0,'queued')", (
                group_id, key, _json(scope), request_id, digest, _json(attachment_ids), caption, self._now()))
            payload = []
            if caption:
                payload.extend(("text", text, None, None) for text in split_text(caption))
            payload.extend(("attachment", None, attachment, index + 1) for index, attachment in enumerate(attachment_ids))
            for index, (kind, text, attachment, ordinal) in enumerate(payload):
                db.execute("INSERT INTO payload_units VALUES(?,?,?,?,?,?,?)", (
                    "%s:payload:%03d" % (group_id, index), group_id, index, kind, text, attachment, ordinal))
            self._fault("group_before_commit")
            result = self._status(db, db.execute("SELECT * FROM groups WHERE group_id=?", (group_id,)).fetchone())
        return result

    def _group(self, db, scope, group_id, historical=False):
        _identifier(group_id)
        row = db.execute("SELECT * FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if row is None:
            raise DeliveryError("not_found")
        self._group_row(row)
        try:
            stored_scope = _scope(json.loads(row["scope_json"]))
        except (ValueError, TypeError, KeyError):
            raise DeliveryError("storage_invalid") from None
        if row["scope_key"] != self._scope_key(stored_scope):
            raise DeliveryError("storage_invalid")
        if row["scope_key"] != self._scope_key(scope):
            raise DeliveryError("scope_mismatch")
        if not historical and stored_scope["login_epoch"] != scope["login_epoch"]:
            raise DeliveryError("epoch_mismatch")
        return row

    def _maintain(self, db):
        groups = set()
        for job in db.execute("SELECT * FROM jobs WHERE state IN ('queued','sending') AND expires_at<=? LIMIT 1000", (self._now(),)).fetchall():
            self._stop_job(db, job, "expired", "expired")
            groups.add(job["group_id"])
        for group_id in groups:
            self._update_group(db, group_id)

    def _maintenance(self):
        with self._transaction() as db:
            self._maintain(db)

    def _stop_job(self, db, job, state, error=None):
        self._job_parent(db, job)
        self._enum(state, _JOB_STATES)
        db.execute("UPDATE reservations SET state=CASE state WHEN 'inflight' THEN 'unknown' ELSE 'proven_unsent' END,owner=NULL,updated_at=? WHERE job_id=? AND state IN ('reserved','inflight')", (self._now(), job["job_id"]))
        db.execute("UPDATE jobs SET state=?,owner=NULL,fence=fence+1,error=? WHERE job_id=?", (state, error, job["job_id"]))

    def _payload(self, db, group_id):
        rows = db.execute("SELECT p.*,r.state AS reservation_state FROM payload_units p LEFT JOIN reservations r ON r.unit_id=p.unit_id WHERE p.group_id=? ORDER BY p.position", (group_id,)).fetchall()
        for row in rows:
            self._enum(row["kind"], _PAYLOAD_KINDS)
            if row["reservation_state"] is not None:
                self._enum(row["reservation_state"], _RESERVATION_STATES)
        return rows

    def _status(self, db, group):
        self._group_row(group)
        rows = self._payload(db, group["group_id"])
        accepted, pending, unknown = [], [], []
        accepted_units = unknown_units = 0
        for row in rows:
            state = row["reservation_state"] or "planned"
            accepted_units += state == "accepted"
            unknown_units += state == "unknown"
            if row["kind"] == "attachment":
                (accepted if state == "accepted" else unknown if state == "unknown" else pending).append(row["attachment_id"])
        # A page's status is a physical send too. Obsolete, proven-unsent
        # status notices are excluded because they will never be continued.
        notices = db.execute("SELECT r.state FROM reservations r JOIN job_units u ON u.unit_id=r.unit_id AND u.job_id=r.job_id WHERE r.group_id=? AND u.kind='status' AND r.state!='proven_unsent'", (group["group_id"],)).fetchall()
        accepted_units += sum(row[0] == "accepted" for row in notices)
        unknown_units += sum(row[0] == "unknown" for row in notices)
        total_units = len(rows) + len(notices)
        jobs = db.execute("SELECT * FROM jobs WHERE group_id=? ORDER BY rowid DESC", (group["group_id"],)).fetchall()
        active = [row for row in jobs if row["state"] in ("queued", "sending") and row["expires_at"] > self._now()]
        if unknown_units:
            state = "unknown"
        elif all((row["reservation_state"] == "accepted") for row in rows):
            state = "complete"
        elif group["cancelled"]:
            state = "cancelled"
        elif active:
            state = "sending" if any(row["state"] == "sending" for row in active) else "queued"
        elif jobs:
            last = jobs[0]["state"]
            state = "expired" if last == "expired" else "partial" if last == "partial" else "awaiting_continue"
        else:
            state = "queued"
        return {"schema_version": 1, "group_id": group["group_id"], "created_at": group["created_at"],
                "state": state, "attachment_count": len(json.loads(group["attachment_ids"])),
                "accepted_attachment_ids": accepted, "pending_attachment_ids": pending,
                "unknown_attachment_ids": unknown,
                "units": {"total": total_units, "accepted": accepted_units,
                          "pending": total_units - accepted_units - unknown_units, "unknown": unknown_units},
                "can_continue": not active and not unknown_units and not group["cancelled"] and any(row["reservation_state"] != "accepted" for row in rows),
                "requires_explicit_continue": True}

    def _update_group(self, db, group_id):
        group = db.execute("SELECT * FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if group is not None:
            db.execute("UPDATE groups SET state=? WHERE group_id=?", (self._status(db, group)["state"], group_id))

    def _job_view(self, db, job):
        self._job_parent(db, job)
        try:
            _identifier(job["request_id"])
            if (job["request_id"] in (".", "..") or type(job["context_ref"]) is not str
                    or not re.fullmatch(r"ctx:[0-9a-f]{64}", job["context_ref"])):
                raise DeliveryError("storage_invalid")
        except DeliveryError:
            raise DeliveryError("storage_invalid") from None
        context = db.execute("SELECT scope_key FROM contexts WHERE context_ref=?", (job["context_ref"],)).fetchone()
        if context is None or context["scope_key"] != job["scope_key"]:
            raise DeliveryError("storage_invalid")
        units = []
        for row in db.execute("SELECT u.*,r.state AS reservation_state FROM job_units u JOIN reservations r ON r.scope_key=? AND r.unit_id=u.unit_id WHERE u.job_id=? ORDER BY u.position", (job["scope_key"], job["job_id"])).fetchall():
            units.append(self._unit_view(job, row))
        return {"schema_version": 1, "type": "attachment_group_v1", "group_id": job["group_id"],
                "job_id": job["job_id"], "request_id": job["request_id"], "context_ref": job["context_ref"],
                "state": job["state"], "created_at": job["created_at"], "expires_at": job["expires_at"],
                "authorized_at": job["expires_at"] - JOB_SECONDS,
                "fence": job["fence"], "units": units, "reason": job["error"]}

    @staticmethod
    def _unit_view(job, unit):
        DeliveryLedger._enum(job["state"], _JOB_STATES)
        DeliveryLedger._enum(unit["kind"], _JOB_UNIT_KINDS)
        DeliveryLedger._enum(unit["reservation_state"], _RESERVATION_STATES)
        return {"schema_version": 1, "group_id": job["group_id"], "job_id": job["job_id"],
                "unit_id": unit["unit_id"], "client_key": "attachment:" + unit["unit_id"],
                "kind": unit["kind"], "text": unit["text"], "attachment_id": unit["attachment_id"],
                "ordinal": unit["ordinal"], "state": unit["reservation_state"],
                "context_ref": job["context_ref"], "fence": job["fence"], "expires_at": job["expires_at"]}

    def prepare_page(self, scope, group_id, request_id, context_token, explicit_continue=False, *, authorized_at=None, check_authority=None):
        scope = _scope(scope)
        _identifier(request_id)
        if type(explicit_continue) is not bool:
            raise DeliveryError("invalid_request")
        authorized_now = self._now()
        if authorized_at is None:
            authorized_at = authorized_now
        if (type(authorized_at) not in (int, float) or not math.isfinite(authorized_at)
                or authorized_at < 0 or authorized_at > authorized_now):
            raise DeliveryError("invalid_request")
        if check_authority is not None:
            if not callable(check_authority):
                raise DeliveryError("invalid_request")
            check_authority()
        with self._transaction(check_authority=check_authority) as db:
            self._guard(db, scope)
            self._maintain(db)
            group = self._group(db, scope, group_id)
            context = self._context(db, scope, context_token)
            old = db.execute("SELECT * FROM jobs WHERE group_id=? AND request_id=?", (group_id, request_id)).fetchone()
            if old is not None:
                if old["context_ref"] != context["context_ref"]:
                    raise DeliveryError("invalid_request")
                return self._job_view(db, old)
            if authorized_at + JOB_SECONDS <= self._now():
                raise DeliveryError("expired")
            status = self._status(db, group)
            if group["cancelled"]:
                raise DeliveryError("cancelled")
            if status["state"] == "unknown":
                raise DeliveryError("group_unknown")
            if db.execute("SELECT 1 FROM jobs WHERE group_id=? AND state IN ('queued','sending')", (group_id,)).fetchone():
                raise DeliveryError("group_busy")
            previous = db.execute("SELECT 1 FROM jobs WHERE group_id=?", (group_id,)).fetchone()
            if previous is not None:
                if not explicit_continue or request_id == group["request_id"]:
                    raise DeliveryError("continue_required")
            elif request_id != group["request_id"] and not explicit_continue:
                raise DeliveryError("continue_required")
            remaining = [row for row in self._payload(db, group_id) if row["reservation_state"] != "accepted"]
            if not remaining:
                raise DeliveryError("invalid_request")
            available = max(0, CAP - self._used(db, context))
            count = min(len(remaining), available)
            needs_notice = len(remaining) > available
            if needs_notice and available:
                count = max(0, available - 1)
            chosen = remaining[:count]
            job_id = "aj_" + uuid.uuid4().hex
            now = self._now()
            state = "queued" if available else "budget_exhausted"
            db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,NULL,0,?,?)", (
                job_id, group_id, group["scope_key"], request_id, context["context_ref"], now, authorized_at + JOB_SECONDS,
                state, None if available else "budget_exhausted", scope["login_epoch"]))
            descriptors = [dict(row) for row in chosen]
            if needs_notice and available:
                descriptors.append({"unit_id": job_id + ":status", "kind": "status",
                                    "text": "本页内容已发送；还有内容待发，请明确要求继续此组。" if count else "当前发送额度不足，附件尚待发；请明确要求继续此组。",
                                    "attachment_id": None, "ordinal": None})
            self._reserve(db, group["scope_key"], context, [row["unit_id"] for row in descriptors], group_id, job_id, scope["login_epoch"])
            for position, row in enumerate(descriptors):
                db.execute("INSERT INTO job_units VALUES(?,?,?,?,?,?,?)", (
                    job_id, position, row["unit_id"], row["kind"], row["text"], row["attachment_id"], row["ordinal"]))
            self._update_group(db, group_id)
            self._fault("page_before_commit")
            result = self._job_view(db, db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())
        self._fault("page_committed")
        return result

    def _job(self, db, scope, job_id):
        _identifier(job_id)
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise DeliveryError("not_found")
        self._job_parent(db, row)
        if row["scope_key"] != self._scope_key(scope):
            raise DeliveryError("scope_mismatch")
        if row["login_epoch"] != scope["login_epoch"]:
            raise DeliveryError("epoch_mismatch")
        return row

    def _job_parent(self, db, job):
        self._enum(job["state"], _JOB_STATES)
        group = self._group_row(db.execute("SELECT * FROM groups WHERE group_id=?", (job["group_id"],)).fetchone())
        try:
            stored_scope = _scope(json.loads(group["scope_json"]))
        except (ValueError, TypeError, KeyError):
            raise DeliveryError("storage_invalid") from None
        if (job["scope_key"] != group["scope_key"] or group["scope_key"] != self._scope_key(stored_scope)
                or job["login_epoch"] != stored_scope["login_epoch"]):
            raise DeliveryError("storage_invalid")
        return group

    def next_unit(self, scope, job_id):
        scope = _scope(scope)
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            job = self._job(db, scope, job_id)
            if job["state"] in _TERMINAL_JOBS:
                return None
            if job["owner"] not in (None, self.owner):
                raise DeliveryError("group_busy")
            if job["owner"] is None:
                db.execute("UPDATE jobs SET owner=?,fence=fence+1,state='sending' WHERE job_id=?", (self.owner, job_id))
                job = self._job(db, scope, job_id)
            self._claims[job_id] = job["fence"]
            for unit in db.execute("SELECT u.*,r.state AS reservation_state FROM job_units u JOIN reservations r ON r.scope_key=? AND r.unit_id=u.unit_id WHERE u.job_id=? ORDER BY u.position", (job["scope_key"], job_id)).fetchall():
                if unit["reservation_state"] == "accepted":
                    continue
                if unit["reservation_state"] == "unknown":
                    raise DeliveryError("group_unknown")
                if unit["reservation_state"] != "reserved":
                    raise DeliveryError("group_busy")
                self._update_group(db, job["group_id"])
                return self._unit_view(job, unit)
            db.execute("UPDATE jobs SET state='complete',owner=NULL WHERE job_id=?", (job_id,))
            self._update_group(db, job["group_id"])
            return None

    def _claimed(self, db, scope, job_id):
        job = self._job(db, scope, job_id)
        if job["state"] == "expired" or job["expires_at"] <= self._now():
            raise DeliveryError("expired")
        if job["state"] == "cancelled":
            raise DeliveryError("cancelled")
        if job["state"] == "unknown":
            raise DeliveryError("group_unknown")
        if (job["state"] != "sending" or job["owner"] != self.owner or
                self._claims.get(job_id) != job["fence"]):
            raise DeliveryError("group_busy")
        return job

    def begin_unit(self, scope, job_id, unit_id):
        scope = _scope(scope)
        _identifier(unit_id)
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            job = self._claimed(db, scope, job_id)
            unit = db.execute("SELECT u.*,r.state AS reservation_state FROM job_units u JOIN reservations r ON r.scope_key=? AND r.unit_id=u.unit_id WHERE u.job_id=? AND u.unit_id=?", (job["scope_key"], job_id, unit_id)).fetchone()
            if unit is None:
                raise DeliveryError("not_found")
            first = db.execute("SELECT u.unit_id FROM job_units u JOIN reservations r ON r.scope_key=? AND r.unit_id=u.unit_id WHERE u.job_id=? AND r.state!='accepted' ORDER BY u.position LIMIT 1", (job["scope_key"], job_id)).fetchone()
            if first is not None and first[0] != unit_id:
                raise DeliveryError("group_busy")
            row = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=?", (job["scope_key"], unit_id)).fetchone()
            allowed = self._begin(db, row)
            result = dict(self._unit_view(job, unit), state="inflight" if allowed else "accepted", send_allowed=allowed)
        if allowed:
            self._fault("unit_inflight_committed")
        return result

    def settle_unit(self, scope, job_id, unit_id, outcome):
        scope = _scope(scope)
        _identifier(unit_id)
        self._outcome(outcome)
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            job = self._job(db, scope, job_id)
            row = db.execute("SELECT * FROM reservations WHERE scope_key=? AND unit_id=? AND job_id=?", (job["scope_key"], unit_id, job_id)).fetchone()
            if row is None:
                raise DeliveryError("not_found")
            if row["state"] == outcome and outcome in ("accepted", "proven_unsent", "unknown"):
                group = self._group(db, scope, job["group_id"])
                return dict(self._status(db, group), job_id=job_id, job_state=job["state"])
            self._claimed(db, scope, job_id)
            self._settle(db, row, outcome)
            if outcome in ("unknown", "proven_unsent"):
                db.execute("UPDATE reservations SET state='proven_unsent',owner=NULL,updated_at=? WHERE job_id=? AND state='reserved'", (self._now(), job_id))
                db.execute("UPDATE jobs SET state=?,owner=NULL,fence=fence+1 WHERE job_id=?", ("unknown" if outcome == "unknown" else "partial", job_id))
            elif not db.execute("SELECT 1 FROM reservations WHERE job_id=? AND state!='accepted'", (job_id,)).fetchone():
                db.execute("UPDATE jobs SET state='complete',owner=NULL WHERE job_id=?", (job_id,))
            self._update_group(db, job["group_id"])
            status = self._status(db, self._group(db, scope, job["group_id"]))
            result = dict(status, job_id=job_id, job_state=self._job(db, scope, job_id)["state"])
        self._fault("unit_settle_committed")
        return result

    def group_status(self, scope, group_id, *, maintenance=True):
        scope = _scope(scope)
        if maintenance:
            self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            return self._status(db, self._group(db, scope, group_id, historical=True))

    def job_status(self, scope, job_id):
        scope = _scope(scope)
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            return self._job_view(db, self._job(db, scope, job_id))

    def pending_jobs(self, scope, limit=100):
        scope = _scope(scope)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise DeliveryError("invalid_request")
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            jobs = db.execute("SELECT * FROM jobs WHERE scope_key=? AND login_epoch=? AND state IN ('queued','sending') AND expires_at>? ORDER BY created_at,rowid LIMIT ?", (
                self._scope_key(scope), scope["login_epoch"], self._now(), limit)).fetchall()
            for job in jobs:
                self._job_parent(db, job)
            return {"schema_version": 1, "job_ids": [row["job_id"] for row in jobs]}

    def terminal_materials(self, scope, group_id):
        """Read proven terminal material IDs for trusted hold reconciliation.

        This never expires work, reserves budget, releases a hold, or reads an
        original. Missing/create-only orphan groups remain protected.
        """
        scope = _scope(scope)
        _identifier(group_id)
        with self._readonly() as db:
            self._guard(db, scope)
            if db.execute("SELECT 1 FROM groups WHERE group_id=?", (group_id,)).fetchone() is None:
                return None
            group = self._group(db, scope, group_id)
            if group["state"] not in ("complete", "cancelled"):
                return None
            jobs = db.execute("SELECT * FROM jobs WHERE group_id=?", (group_id,)).fetchall()
            if not any(job["request_id"] == group["request_id"] for job in jobs):
                return None
            for job in jobs:
                self._job_parent(db, job)
                if job["state"] not in _SAFE_TERMINAL_JOBS:
                    return None
            if db.execute("SELECT 1 FROM reservations WHERE group_id=? AND state NOT IN ('accepted','proven_unsent') LIMIT 1", (group_id,)).fetchone():
                return None
            if db.execute("SELECT 1 FROM reservations r LEFT JOIN jobs j ON j.job_id=r.job_id WHERE r.group_id=? AND (j.job_id IS NULL OR j.group_id!=r.group_id OR j.scope_key!=r.scope_key OR j.login_epoch!=r.origin_epoch) LIMIT 1", (group_id,)).fetchone():
                return None
            if self._status(db, group)["state"] != group["state"]:
                return None
            try:
                ids = json.loads(group["attachment_ids"])
                if (type(ids) is not list or not 1 <= len(ids) <= 16
                        or len(set(_identifier(aid) for aid in ids)) != len(ids)):
                    raise ValueError()
                request_id = _identifier(group["request_id"])
            except (ValueError, TypeError, KeyError):
                raise DeliveryError("storage_invalid") from None
            payload = [row[0] for row in db.execute("SELECT attachment_id FROM payload_units WHERE group_id=? AND kind='attachment' ORDER BY position", (group_id,))]
            if payload != ids:
                return None
            return {"group_id": group_id, "request_id": request_id,
                    "attachment_ids": ids, "state": group["state"]}

    def cancel_group(self, scope, group_id, check_authority=None):
        """Roll back the entire cancellation if the trusted final guard fails."""
        scope = _scope(scope)
        if check_authority is not None and not callable(check_authority):
            raise DeliveryError("invalid_request")
        if check_authority is not None:
            check_authority()
        # No separately committed maintenance may precede this authorization.
        with self._transaction(check_authority=check_authority) as db:
            self._guard(db, scope)
            group = self._group(db, scope, group_id)
            for job in db.execute("SELECT * FROM jobs WHERE group_id=? AND state IN ('queued','sending')", (group_id,)).fetchall():
                self._stop_job(db, job, "cancelled", "cancelled")
            db.execute("UPDATE groups SET cancelled=1 WHERE group_id=?", (group_id,))
            self._update_group(db, group_id)
            result = self._status(db, self._group(db, scope, group_id))
            self._fault("cancel_before_commit")
        return result

    def resolve_group_selector(self, scope, selector):
        """Read the whole principal's pending set; never infer a latest group.

        ``None`` or an empty string requires exactly one candidate. Otherwise
        only an exact ``ag_`` plus 8..32 lowercase hex prefix is accepted. This
        lookup does not authorize intent, resume work, expire jobs, or cancel.
        """
        scope = _scope(scope)
        if selector is None or selector == "":
            prefix = None
        elif type(selector) is str and re.fullmatch(r"ag_[0-9a-f]{8,32}", selector):
            prefix = selector
        else:
            raise DeliveryError("continue_required")
        with self._readonly() as db:
            self._guard(db, scope)
            where = "scope_key=? AND state NOT IN ('complete','cancelled')"
            params = [self._scope_key(scope)]
            if prefix is not None:
                where += " AND group_id GLOB ?"
                params.append(prefix + "*")
            rows = db.execute("SELECT group_id FROM groups WHERE " + where + " LIMIT 2", params).fetchall()
            if len(rows) != 1:
                raise DeliveryError("continue_required")
            return rows[0][0]

    def _cursor(self, scope, after):
        raw = _json({"version": 1, "scope": self._scope_key(scope), "after": after}).encode("utf-8")
        sig = hmac.new(self._cursor_secret, raw, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(sig + raw).decode("ascii")

    def _uncursor(self, scope, cursor):
        if type(cursor) is not str or len(cursor) > 2048:
            raise DeliveryError("invalid_request")
        try:
            raw = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True)
            sig, body = raw[:32], raw[32:]
            if not hmac.compare_digest(sig, hmac.new(self._cursor_secret, body, hashlib.sha256).digest()):
                raise ValueError()
            value = json.loads(body.decode("utf-8"))
            if set(value) != {"version", "scope", "after"} or value["version"] != 1 or value["scope"] != self._scope_key(scope):
                raise ValueError()
            at, group_id = value["after"]
            if type(at) not in (float, int) or not math.isfinite(at):
                raise ValueError()
            return float(at), _identifier(group_id)
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise DeliveryError("invalid_request") from None

    def list_groups(self, scope, limit=20, cursor=None):
        scope = _scope(scope)
        if type(limit) is not int or not 1 <= limit <= 20:
            raise DeliveryError("invalid_request")
        after = self._uncursor(scope, cursor) if cursor is not None else None
        self._maintenance()
        with self._transaction() as db:
            self._guard(db, scope)
            params = [self._scope_key(scope)]
            where = "scope_key=? AND state NOT IN ('complete','cancelled')"
            if after is not None:
                where += " AND (created_at<? OR (created_at=? AND group_id<?))"
                params.extend((after[0], after[0], after[1]))
            rows = db.execute("SELECT * FROM groups WHERE " + where + " ORDER BY created_at DESC,group_id DESC LIMIT ?", params + [limit + 1]).fetchall()
            selected = rows[:limit]
            next_cursor = self._cursor(scope, [selected[-1]["created_at"], selected[-1]["group_id"]]) if len(rows) > limit else None
            return {"schema_version": 1, "groups": [self._status(db, row) for row in selected], "next_cursor": next_cursor}

    def recover(self):
        """Call once while holding the sole sender's service lock."""
        with self._transaction() as db:
            jobs = db.execute("SELECT * FROM jobs WHERE state IN ('queued','sending')").fetchall()
            for job in jobs:
                self._job_parent(db, job)
            inflight = db.execute("SELECT count(*) FROM reservations WHERE state='inflight'").fetchone()[0]
            db.execute("UPDATE reservations SET state='unknown',owner=NULL,updated_at=? WHERE state='inflight'", (self._now(),))
            groups = set()
            for job in jobs:
                groups.add(job["group_id"])
                if job["expires_at"] <= self._now():
                    self._stop_job(db, job, "expired", "expired")
                elif db.execute("SELECT 1 FROM reservations WHERE job_id=? AND state='unknown'", (job["job_id"],)).fetchone():
                    db.execute("UPDATE reservations SET state='proven_unsent',owner=NULL,updated_at=? WHERE job_id=? AND state='reserved'", (self._now(), job["job_id"]))
                    db.execute("UPDATE jobs SET state='unknown',owner=NULL,fence=fence+1 WHERE job_id=?", (job["job_id"],))
                else:
                    db.execute("UPDATE jobs SET state='queued',owner=NULL,fence=fence+1 WHERE job_id=?", (job["job_id"],))
            for group_id in groups:
                self._update_group(db, group_id)
            self._fault("recovery_before_commit")
        self._claims.clear()
        return {"schema_version": 1, "inflight_to_unknown": inflight,
                "jobs_reconciled": len(jobs), "groups_reconciled": len(groups)}
