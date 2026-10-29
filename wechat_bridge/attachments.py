"""Private attachment originals, recoverable catalog and fixed image references.

Policy values in this module are local resource/retention policies.  They are
not statements about WeChat limits.  No provider, decoder or external program
is called here.  Construction and read-only methods never initialize storage.
"""
import base64
from contextlib import contextmanager
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import urllib.parse
import uuid


MAX_JSON_BYTES = 1 << 20
MAX_DATABASE_BYTES = 64 << 20
CATALOG_MARKER_NAME = "catalog.json"
MAX_CATALOG_MARKER_BYTES = 1024
DEFAULT_POLICY = {
    "max_bytes": 100 << 20, "quota_bytes": 5 << 30,
    "retention_seconds": 30 * 86400, "recent_seconds": 86400,
    "ambiguity_seconds": 900, "max_positions": 16,
    "tombstone_seconds": 30 * 86400,
    # Two bounded SQLite catalogs (attachments + delivery), plus one maximum
    # control JSON. This reserve is local headroom, not filesystem ownership.
    "control_reserve_bytes": 2 * MAX_DATABASE_BYTES + MAX_JSON_BYTES,
}
KINDS = frozenset(("image", "video", "voice", "file"))
STATUSES = frozenset(("pending", "prepared", "ready", "failed", "quota", "missing"))
TAG_SOURCES = frozenset(("user", "batch_text", "original_name", "inferred"))
DECRYPTION_STATES = frozenset(("pending", "unavailable", "protocol_decoded", "content_verified", "plaintext_verified"))
PREVIEW_STATES = frozenset(("readable", "unavailable", "invalid", "resource_limited"))
MAX_RECORDS = 10000
MAX_EVENT_ITEMS = 1024
ID_RE = re.compile(r"[0-9a-f]{32}\Z")
KEY_RE = re.compile(r"[0-9a-f]{32}(?:\.(?:bin|jpg|png|gif|webp|bmp|mp4|mov|pdf|heic|heif))?\Z")
REASON_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
SCHEMA = {
    "metadata": (("key", "TEXT"), ("value", "TEXT")),
    "events": (("seq", "INTEGER"), ("event_key", "TEXT"), ("scope", "TEXT"), ("payload", "TEXT")),
    "attachments": (("id", "TEXT"), ("event_seq", "INTEGER"), ("item_index", "INTEGER"), ("scope", "TEXT"), ("payload", "TEXT")),
    "image_groups": (("id", "TEXT"), ("scope_epoch", "TEXT"), ("payload", "TEXT")),
    "snapshots": (("id", "TEXT"), ("scope_epoch", "TEXT"), ("payload", "TEXT")),
    "holds": (("id", "TEXT"), ("attachment_id", "TEXT"), ("scope", "TEXT"), ("holder", "TEXT"), ("payload", "TEXT")),
}


class AttachmentError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class _PreparedFileBlocked(AttachmentError):
    """A single prepared file failed safety checks, not catalog validation."""
    def __init__(self):
        super().__init__("storage_invalid")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _parse(raw):
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_JSON_BYTES:
        raise AttachmentError("storage_invalid")
    try:
        def unique(pairs):
            answer = {}
            for key, value in pairs:
                if key in answer:
                    raise ValueError("duplicate")
                answer[key] = value
            return answer
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError("constant")))
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise AttachmentError("storage_invalid") from None


def _text(value, maximum=512, nullable=False, empty=False):
    if value is None and nullable:
        return value
    if type(value) is not str or (not empty and not value):
        raise AttachmentError("invalid_request")
    try:
        if len(value.encode("utf-8")) > maximum or "\x00" in value:
            raise AttachmentError("invalid_request")
    except UnicodeError:
        raise AttachmentError("invalid_request") from None
    return value


def _number(value, positive=False):
    if type(value) not in (int, float):
        raise AttachmentError("invalid_request")
    try:
        valid = math.isfinite(value) and value >= 0 and (not positive or value > 0)
    except OverflowError:
        valid = False
    if not valid:
        raise AttachmentError("invalid_request")
    return float(value)


def _scope(value):
    if type(value) is not dict or set(value) != {"bot_id", "owner_id", "conversation_id", "login_epoch"}:
        raise AttachmentError("invalid_request")
    return {key: _text(value[key]) for key in ("bot_id", "owner_id", "conversation_id", "login_epoch")}


def _scope_key(scope):
    return _json([scope[key] for key in ("bot_id", "owner_id", "conversation_id")])


def _epoch_key(scope):
    return _json([_scope_key(scope), scope["login_epoch"]])


def _id(value):
    if type(value) is not str or not ID_RE.fullmatch(value):
        raise AttachmentError("invalid_request")
    return value


def _tags(value):
    if not isinstance(value, (list, tuple)) or len(value) > 32:
        raise AttachmentError("invalid_request")
    result = []
    for item in value:
        if type(item) is not dict or set(item) != {"value", "source", "message_id"}:
            raise AttachmentError("invalid_request")
        val = _text(item["value"], 512)
        if len(val) > 128 or type(item["source"]) is not str or item["source"] not in TAG_SOURCES:
            raise AttachmentError("invalid_request")
        result.append({"value": val, "source": item["source"],
                       "message_id": _text(item["message_id"], nullable=True)})
    return result


def _publication_tags(value):
    """Validate trusted metadata without making optional captions a save gate.

    A batch_text tag is optional, bounded provenance.  Do not truncate a long
    caption into a made-up tag, and do not persist malformed optional fields.
    Other supplied provenance remains strict because it is not an automatic
    caption fallback.
    """
    if value is None:
        return []
    if not isinstance(value, (list, tuple)) or len(value) > MAX_EVENT_ITEMS:
        raise AttachmentError("invalid_request")
    result = []
    for item in value:
        optional = type(item) is dict and item.get("source") == "batch_text"
        try:
            tag = _tags([item])[0]
            if not tag["value"].strip():
                raise AttachmentError("invalid_request")
        except AttachmentError:
            if optional:
                continue
            raise
        if tag not in result:
            result.append(tag)
    return result


def _merge_publication_tags(existing, incoming):
    result = _tags(existing)
    # Preserve every existing explicit tag. Optional automatic captions must
    # not take the last slot ahead of non-optional supplied provenance.
    ordered = [tag for tag in incoming if tag["source"] != "batch_text"]
    ordered.extend(tag for tag in incoming if tag["source"] == "batch_text")
    for tag in ordered:
        if tag in result:
            continue
        if len(result) == 32:
            if tag["source"] == "batch_text":
                continue
            raise AttachmentError("quota_exceeded")
        result.append(tag)
    return result


class AttachmentStore:
    def __init__(self, base, policy=None, clock=time.time, fault=None):
        raw = os.fspath(base)
        if not isinstance(raw, str) or not os.path.isabs(raw) or "\x00" in raw:
            raise AttachmentError("storage_invalid")
        root = Path(os.path.normpath(raw))
        for alias in ("/var", "/tmp"):
            if os.path.islink(alias) and (str(root) == alias or str(root).startswith(alias + "/")):
                root = Path(os.path.realpath(alias) + str(root)[len(alias):])
        if root in (Path("/"), Path.home()) or len(root.parts) < 3:
            raise AttachmentError("storage_invalid")
        self.base = root
        self.policy = dict(DEFAULT_POLICY)
        if policy is not None:
            if type(policy) is not dict or set(policy) - set(DEFAULT_POLICY):
                raise AttachmentError("invalid_request")
            self.policy.update(policy)
        for key, value in self.policy.items():
            minimum = 0 if key in ("ambiguity_seconds", "control_reserve_bytes") else 1
            if type(value) is not int or value < minimum:
                raise AttachmentError("invalid_request")
        if self.policy["max_bytes"] > (100 << 20) or self.policy["max_positions"] > 16:
            raise AttachmentError("invalid_request")
        self.clock, self.fault = clock, fault
        self._mutex = threading.RLock()

    def _now(self):
        return _number(self.clock())

    def _fault(self, point):
        if self.fault is not None:
            self.fault(point)

    @staticmethod
    def _private_file(info):
        return (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                and info.st_uid == os.geteuid() and not info.st_mode & 0o077)

    @staticmethod
    def _directory(parent, name, create):
        if create:
            try:
                os.mkdir(name, 0o700, dir_fd=parent)
            except FileExistsError:
                pass
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            os.close(fd)
            raise AttachmentError("storage_invalid")
        return fd

    def _legacy_catalog_evidence(self, handles):
        """Inspect only known control JSON, never attachment body paths."""
        count, total = 0, 0
        for name, parent in (("inbox", "root"), ("backend_inbox", "root"),
                             ("attachment_admissions", "state")):
            if parent not in handles:
                continue
            try:
                directory = self._directory(handles[parent], name, False)
            except FileNotFoundError:
                continue
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        count += 1
                        if count > MAX_RECORDS:
                            raise AttachmentError("storage_invalid")
                        if not entry.name.endswith(".json"):
                            continue
                        fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW |
                                     getattr(os, "O_NONBLOCK", 0), dir_fd=directory)
                        try:
                            before = os.fstat(fd)
                            if not self._private_file(before) or before.st_size > MAX_JSON_BYTES:
                                raise AttachmentError("storage_invalid")
                            raw = os.read(fd, MAX_JSON_BYTES + 1)
                            total += len(raw)
                            after = os.fstat(fd)
                            if (len(raw) != before.st_size or total > MAX_DATABASE_BYTES
                                    or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                                    (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                                raise AttachmentError("storage_invalid")
                            row = _parse(raw.decode("utf-8"))
                        finally:
                            os.close(fd)
                        if type(row) is not dict:
                            raise AttachmentError("storage_invalid")
                        for key in ("attachment_refs", "attachment_ids"):
                            if key in row:
                                if type(row[key]) is not list:
                                    raise AttachmentError("storage_invalid")
                                if row[key]:
                                    return True
                        if row.get("reference_context") is not None:
                            return True
            finally:
                os.close(directory)
        return False

    def _assert_fresh_storage(self, handles):
        """Absent catalogs are fresh only when no attachment evidence remains."""
        if "state" in handles:
            for suffix in ("-journal", "-wal", "-shm"):
                try:
                    os.stat("attachments.sqlite3" + suffix, dir_fd=handles["state"],
                            follow_symlinks=False)
                except FileNotFoundError:
                    continue
                raise AttachmentError("storage_invalid")
        if "attachments" in handles:
            with os.scandir(handles["attachments"]) as entries:
                if any(entry.name not in ("objects", "staging") for entry in entries):
                    raise AttachmentError("storage_invalid")
        for name in ("objects", "staging"):
            if name in handles:
                with os.scandir(handles[name]) as entries:
                    if next(entries, None) is not None:
                        raise AttachmentError("storage_invalid")
        if self._legacy_catalog_evidence(handles):
            raise AttachmentError("storage_invalid")

    def _catalog_marker(self, dirs, catalog_id, create):
        """Bind a durable, independent existence witness to this catalog."""
        try:
            fd = os.open(CATALOG_MARKER_NAME, os.O_RDONLY | os.O_NOFOLLOW |
                         getattr(os, "O_NONBLOCK", 0), dir_fd=dirs["attachments"])
        except FileNotFoundError:
            if not create:
                return  # Legacy read-only access does not perform migration.
            try:
                fd = os.open(CATALOG_MARKER_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                             os.O_NOFOLLOW, 0o600, dir_fd=dirs["attachments"])
            except FileExistsError:
                return self._catalog_marker(dirs, catalog_id, False)
            try:
                raw = _json({"schema_version": 1, "catalog_id": catalog_id}).encode("utf-8")
                offset = 0
                while offset < len(raw):
                    written = os.write(fd, raw[offset:])
                    if written <= 0:
                        raise AttachmentError("storage_invalid")
                    offset += written
                os.fsync(fd)
            finally:
                os.close(fd)
            os.fsync(dirs["attachments"])
            self._fault("catalog_marker_persisted")
            return
        try:
            before = os.fstat(fd)
            if not self._private_file(before) or before.st_size > MAX_CATALOG_MARKER_BYTES:
                raise AttachmentError("storage_invalid")
            raw = os.read(fd, MAX_CATALOG_MARKER_BYTES + 1)
            after = os.fstat(fd)
            if (len(raw) != before.st_size or
                    (before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                    (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise AttachmentError("storage_invalid")
            marker = _parse(raw.decode("utf-8"))
            if (type(marker) is not dict or set(marker) != {"schema_version", "catalog_id"}
                    or type(marker["schema_version"]) is not int or marker["schema_version"] != 1
                    or marker["catalog_id"] != catalog_id):
                raise AttachmentError("storage_invalid")
        finally:
            os.close(fd)

    def _directories(self, create):
        for part in (self.base,) + tuple(self.base.parents)[:-1]:
            if part.is_symlink():
                raise AttachmentError("storage_invalid")
        if create and not self.base.exists():
            self.base.mkdir(mode=0o700)
        if not self.base.exists():
            return None
        fd = os.open(str(self.base), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        handles, keep = {"root": fd}, False
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                raise AttachmentError("storage_invalid")
            for name, parent, child in (("attachments", "root", "attachments"),
                                        ("objects", "attachments", "objects"),
                                        ("staging", "attachments", "staging"),
                                        ("state", "root", "state")):
                if parent not in handles:
                    continue
                try:
                    handles[name] = self._directory(handles[parent], child, False)
                except FileNotFoundError:
                    pass
            catalog_exists = False
            if "state" in handles:
                try:
                    catalog = os.stat("attachments.sqlite3", dir_fd=handles["state"],
                                      follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    if not self._private_file(catalog) or catalog.st_size > MAX_DATABASE_BYTES:
                        raise AttachmentError("storage_invalid")
                    catalog_exists = True
            if catalog_exists:
                if set(handles) != {"root", "attachments", "objects", "staging", "state"}:
                    raise AttachmentError("storage_invalid")
            else:
                self._assert_fresh_storage(handles)
                if not create:
                    return None
                for name, parent in (("attachments", "root"), ("objects", "attachments"),
                                     ("staging", "attachments"), ("state", "root")):
                    if name not in handles:
                        handles[name] = self._directory(handles[parent], name, True)
            keep = True
            return handles
        finally:
            if not keep:
                for value in reversed(list(handles.values())):
                    os.close(value)

    @staticmethod
    def _check_schema(db):
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != set(SCHEMA) or db.execute("SELECT 1 FROM sqlite_master WHERE type IN ('view','trigger') LIMIT 1").fetchone():
            raise AttachmentError("storage_invalid")
        for table, expected in SCHEMA.items():
            actual = tuple((row[1], row[2]) for row in db.execute('PRAGMA table_info("%s")' % table))
            if actual != expected:
                raise AttachmentError("storage_invalid")
        metadata = dict(db.execute("SELECT key,value FROM metadata"))
        if (set(metadata) != {"schema_version", "catalog_id"} or metadata["schema_version"] != "1"
                or not ID_RE.fullmatch(metadata["catalog_id"])):
            raise AttachmentError("storage_invalid")
        return metadata["catalog_id"]

    @contextmanager
    def _db(self, write=False, initialize=True):
        if not self._mutex.acquire(timeout=0.1):
            raise AttachmentError("storage_busy")
        dirs, db = None, None
        try:
            dirs = self._directories(write and initialize)
            if dirs is None:
                yield None, None
                return
            name, created = "attachments.sqlite3", False
            try:
                check = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dirs["state"])
            except FileNotFoundError:
                # Recheck after directory acquisition, before either reporting
                # an empty store or creating a new catalog.
                self._assert_fresh_storage(dirs)
                if not write or not initialize:
                    yield None, dirs
                    return
                try:
                    check = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                    0o600, dir_fd=dirs["state"])
                    created = True
                except FileExistsError:
                    check = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dirs["state"])
            try:
                info = os.fstat(check)
                if not self._private_file(info) or info.st_size > MAX_DATABASE_BYTES:
                    raise AttachmentError("storage_invalid")
            finally:
                os.close(check)
            path = str(self.base / "state" / name)
            for suffix in ("-journal", "-wal", "-shm"):
                try:
                    sidecar = os.stat(name + suffix, dir_fd=dirs["state"], follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (suffix != "-journal" or not self._private_file(sidecar)
                        or sidecar.st_size > 2 * MAX_DATABASE_BYTES):
                    raise AttachmentError("storage_invalid")
            uri = "file:%s?mode=%s" % (urllib.parse.quote(path, safe="/"), "rw" if write else "ro")
            db = sqlite3.connect(uri, uri=True, timeout=0.1, isolation_level=None)
            after = os.stat(name, dir_fd=dirs["state"], follow_symlinks=False)
            if (after.st_dev, after.st_ino) != (info.st_dev, info.st_ino) or not self._private_file(after):
                raise AttachmentError("storage_invalid")
            if created:
                db.execute("PRAGMA journal_mode=DELETE")
                db.execute("PRAGMA synchronous=FULL")
                db.executescript("""BEGIN IMMEDIATE;
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE events(seq INTEGER PRIMARY KEY,event_key TEXT UNIQUE NOT NULL,scope TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE attachments(id TEXT PRIMARY KEY,event_seq INTEGER NOT NULL,item_index INTEGER NOT NULL,scope TEXT NOT NULL,payload TEXT NOT NULL,UNIQUE(event_seq,item_index));
CREATE TABLE image_groups(id TEXT PRIMARY KEY,scope_epoch TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE snapshots(id TEXT PRIMARY KEY,scope_epoch TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE holds(id TEXT PRIMARY KEY,attachment_id TEXT NOT NULL,scope TEXT NOT NULL,holder TEXT NOT NULL,payload TEXT NOT NULL);
""")
                db.execute("INSERT INTO metadata VALUES ('schema_version','1')")
                db.execute("INSERT INTO metadata VALUES ('catalog_id',?)", (uuid.uuid4().hex,))
                db.commit()
                os.fsync(dirs["state"])
                self._fault("catalog_initialized")
            catalog_id = self._check_schema(db)
            if db.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
                raise AttachmentError("storage_invalid")
            if write:
                db.execute("PRAGMA synchronous=FULL")
                page_size = db.execute("PRAGMA page_size").fetchone()[0]
                db.execute("PRAGMA max_page_count=%d" % (MAX_DATABASE_BYTES // page_size))
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            self._catalog_marker(dirs, catalog_id, write)
            yield db, dirs
            if write:
                self._fault("transaction_before_commit")
            db.commit()
        except AttachmentError:
            if db is not None:
                db.rollback()
            raise
        except sqlite3.Error as error:
            if db is not None:
                db.rollback()
            text = str(error).lower()
            raise AttachmentError("storage_busy" if "locked" in text or "busy" in text else
                                  "quota_exceeded" if "full" in text else "storage_invalid") from None
        except OSError as error:
            if db is not None:
                db.rollback()
            raise AttachmentError("quota_exceeded" if error.errno in (errno.ENOSPC, errno.EDQUOT) else "storage_invalid") from None
        except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError,
                KeyError, IndexError, AttributeError):
            if db is not None:
                db.rollback()
            raise AttachmentError("storage_invalid") from None
        finally:
            if db is not None:
                db.close()
            if dirs:
                for value in reversed(list(dirs.values())):
                    os.close(value)
            self._mutex.release()

    @staticmethod
    def _put(db, record):
        db.execute("UPDATE attachments SET payload=? WHERE id=?", (_json(record), record["attachment_id"]))

    def _record(self, db, attachment_id, scope=None):
        row = db.execute("SELECT scope,payload FROM attachments WHERE id=?", (_id(attachment_id),)).fetchone()
        if not row:
            raise AttachmentError("not_found")
        if scope is not None and row[0] != _scope_key(scope):
            raise AttachmentError("scope_mismatch")
        rec = _parse(row[1])
        required = {"schema_version", "attachment_id", "bot_id", "owner_id", "conversation_id", "received_epoch",
                    "message_id", "item_index", "received_at", "kind", "original_name", "name_source",
                    "declared_type", "detected_type", "size", "sha256", "object_key", "status", "decryption_state",
                    "preview_state", "tags", "expires_at", "pinned", "failure_reason"}
        try:
            if type(rec) is not dict or set(rec) != required or type(rec["schema_version"]) is not int or rec["schema_version"] != 1:
                raise ValueError()
            if rec["attachment_id"] != attachment_id or rec["kind"] not in KINDS or rec["status"] not in STATUSES:
                raise ValueError()
            rec_scope = {key: rec[key] for key in ("bot_id", "owner_id", "conversation_id")}
            rec_scope["login_epoch"] = rec["received_epoch"]
            _scope(rec_scope)
            if _scope_key(rec_scope) != row[0]:
                raise ValueError()
            _text(rec["message_id"]); _text(rec["original_name"], 4096, nullable=True, empty=True)
            _text(rec["declared_type"], 256, nullable=True); _text(rec["detected_type"], 256, nullable=True)
            _text(rec["decryption_state"], 64); _text(rec["preview_state"], 64)
            if rec["decryption_state"] not in DECRYPTION_STATES or rec["preview_state"] not in PREVIEW_STATES:
                raise ValueError()
            if type(rec["item_index"]) is not int or rec["item_index"] < 0 or type(rec["pinned"]) is not bool:
                raise ValueError()
            _number(rec["received_at"]); _number(rec["expires_at"]); _tags(rec["tags"])
            if rec["name_source"] != ("original_name" if rec["original_name"] is not None else "unknown"):
                raise ValueError()
            if rec["failure_reason"] is not None and not REASON_RE.fullmatch(rec["failure_reason"]):
                raise ValueError()
            if rec["object_key"] is not None:
                if not KEY_RE.fullmatch(rec["object_key"]) or not rec["object_key"].startswith(attachment_id):
                    raise ValueError()
                if type(rec["size"]) is not int or not 0 <= rec["size"] <= self.policy["max_bytes"] or not re.fullmatch(r"[0-9a-f]{64}", rec["sha256"]):
                    raise ValueError()
                if rec["decryption_state"] not in ("protocol_decoded", "content_verified", "plaintext_verified"):
                    raise ValueError()
            elif rec["size"] is not None or rec["sha256"] is not None or rec["status"] in ("prepared", "ready", "missing"):
                raise ValueError()
        except (AttachmentError, KeyError, TypeError, ValueError):
            raise AttachmentError("storage_invalid") from None
        return rec

    @staticmethod
    def _stage_name(attachment_id):
        return attachment_id + ".part"

    def _stage_path(self, attachment_id):
        return str(self.base / "attachments" / "staging" / self._stage_name(attachment_id))

    def _file(self, directory, name, expected=None, fsync=False):
        # Nonblocking open lets the regular-file check reject a substituted
        # FIFO instead of waiting indefinitely for a writer.
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(fd)
            if not self._private_file(before) or before.st_size > self.policy["max_bytes"]:
                raise AttachmentError("storage_invalid")
            size, hasher = 0, hashlib.sha256()
            while True:
                data = os.read(fd, min(1 << 20, self.policy["max_bytes"] + 1 - size))
                if not data:
                    break
                size += len(data)
                if size > self.policy["max_bytes"]:
                    raise AttachmentError("storage_invalid")
                hasher.update(data)
            after = os.fstat(fd)
            def identity(info):
                return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
                        info.st_uid, info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            if identity(before) != identity(after) or size != before.st_size:
                raise AttachmentError("storage_invalid")
            digest = hasher.hexdigest()
            if expected is not None and (size, digest) != expected:
                raise AttachmentError("attachment_unavailable")
            if fsync:
                os.fsync(fd)
            return size, digest
        finally:
            os.close(fd)

    def _observed_status(self, dirs, rec):
        if rec["status"] != "ready":
            return "pending" if rec["status"] == "prepared" else rec["status"]
        try:
            info = os.stat(rec["object_key"], dir_fd=dirs["objects"], follow_symlinks=False)
            return "ready" if self._private_file(info) and info.st_size == rec["size"] else "missing"
        except FileNotFoundError:
            return "missing"

    def _metadata(self, dirs, rec):
        result = json.loads(_json(rec))
        result["status"] = self._observed_status(dirs, rec)
        return result

    def _usage(self, db, dirs):
        physical, stage_sizes, object_sizes, unknown = 0, {}, {}, []
        for kind, sizes in (("objects", object_sizes), ("staging", stage_sizes)):
            with os.scandir(dirs[kind]) as entries:
                for count, entry in enumerate(entries, 1):
                    if count > MAX_RECORDS * 2:
                        raise AttachmentError("storage_invalid")
                    info = entry.stat(follow_symlinks=False)
                    if not self._private_file(info):
                        raise AttachmentError("storage_invalid")
                    physical += info.st_size
                    sizes[entry.name] = info.st_size
        reserved, logical_missing, known_objects, known_staging = 0, 0, set(), set()
        for (attachment_id,) in db.execute("SELECT id FROM attachments"):
            rec = self._record(db, attachment_id)
            stage = self._stage_name(attachment_id)
            known_staging.add(stage)
            # Retry helpers contain no body until moved to the reserved .part.
            empty_stage = "." + stage + ".empty"
            if rec["status"] in ("pending", "prepared") and stage_sizes.get(empty_stage) == 0:
                known_staging.add(empty_stage)
            if rec["object_key"]:
                known_objects.add(rec["object_key"])
            if rec["status"] == "pending":
                reserved += max(0, self.policy["max_bytes"] - stage_sizes.get(stage, 0))
            elif rec["object_key"] and rec["object_key"] not in object_sizes and stage not in stage_sizes:
                logical_missing += rec["size"]
        unknown.extend("objects:" + key for key in object_sizes if key not in known_objects)
        unknown.extend("staging:" + key for key in stage_sizes if key not in known_staging)
        return {"physical_bytes": physical, "reserved_bytes": reserved,
                "missing_accounted_bytes": logical_missing, "accounted_bytes": physical + reserved + logical_missing,
                "unknown_files": unknown}

    def _group(self, db, group_id):
        row = db.execute("SELECT payload FROM image_groups WHERE id=?", (group_id,)).fetchone()
        if not row:
            raise AttachmentError("storage_invalid")
        group = _parse(row[0])
        fields = {"schema_version", "group_id", "created_at", "expires_at", "closed", "slots", "total_positions"}
        if (type(group) is not dict or set(group) not in (fields, fields | {"position_limit"})
                or type(group["schema_version"]) is not int or group["schema_version"] != 1
                or group.get("group_id") != group_id or type(group.get("slots")) is not list
                or len(group["slots"]) > 16
                or type(group.get("total_positions")) is not int or group["total_positions"] < len(group["slots"])
                or type(group.get("closed")) is not bool):
            raise AttachmentError("storage_invalid")
        # Legacy groups did not persist their policy. An overflow proves their
        # retained prefix is already full; otherwise only the protocol cap is
        # known. Neither case may invent positions which were not retained.
        limit = group.get("position_limit", len(group["slots"]) if
                          group["total_positions"] > len(group["slots"]) else 16)
        if type(limit) is not int or not 1 <= limit <= 16 or len(group["slots"]) > limit:
            raise AttachmentError("storage_invalid")
        for ordinal, slot in enumerate(group["slots"], 1):
            if (type(slot) is not dict or type(slot.get("ordinal")) is not int
                    or slot["ordinal"] != ordinal):
                raise AttachmentError("storage_invalid")
        group["position_limit"] = limit
        _number(group["created_at"]); _number(group["expires_at"])
        return group

    def _context(self, db, dirs, snapshot, scope):
        if (type(snapshot) is not dict or set(snapshot) != {"schema_version", "snapshot_id", "text_message_id", "cutoff_seq", "group_id", "login_epoch", "created_at", "expires_at", "slots", "overflow", "total_positions"}
                or type(snapshot["schema_version"]) is not int or snapshot["schema_version"] != 1
                or type(snapshot["slots"]) is not list or len(snapshot["slots"]) > 16
                or type(snapshot["overflow"]) is not bool or type(snapshot["total_positions"]) is not int):
            raise AttachmentError("storage_invalid")
        _id(snapshot["snapshot_id"]); _id(snapshot["group_id"]); _text(snapshot["text_message_id"])
        if (type(snapshot["cutoff_seq"]) is not int or snapshot["cutoff_seq"] < 1
                or snapshot["login_epoch"] != scope["login_epoch"]
                or snapshot["total_positions"] < len(snapshot["slots"])
                or snapshot["overflow"] != (snapshot["total_positions"] > len(snapshot["slots"]))):
            raise AttachmentError("storage_invalid")
        _number(snapshot["created_at"]); _number(snapshot["expires_at"])
        origin = db.execute("SELECT scope,payload FROM events WHERE seq=?", (snapshot["cutoff_seq"],)).fetchone()
        group_scope = db.execute("SELECT scope_epoch FROM image_groups WHERE id=?", (snapshot["group_id"],)).fetchone()
        if not origin or origin[0] != _scope_key(scope) or not group_scope or group_scope[0] != _epoch_key(scope):
            raise AttachmentError("storage_invalid")
        event, group = _parse(origin[1]), self._group(db, snapshot["group_id"])
        if (event["message_id"] != snapshot["text_message_id"] or event["snapshot_id"] != snapshot["snapshot_id"]
                or event["received_at"] != snapshot["created_at"] or group["expires_at"] != snapshot["expires_at"]
                or group["total_positions"] != snapshot["total_positions"]):
            raise AttachmentError("storage_invalid")
        result = json.loads(_json(snapshot))
        for ordinal, slot in enumerate(result["slots"], 1):
            if type(slot) is not dict or set(slot) != {"ordinal", "message_id", "item_index", "attachment_id", "received_at", "status"}:
                raise AttachmentError("storage_invalid")
            if type(slot["ordinal"]) is not int or slot["ordinal"] != ordinal:
                raise AttachmentError("storage_invalid")
            if (type(slot["item_index"]) is not int or slot["item_index"] < 0
                    or slot["status"] not in STATUSES - {"prepared"}):
                raise AttachmentError("storage_invalid")
            _number(slot["received_at"])
            rec = self._record(db, slot["attachment_id"], scope)
            event_seq = db.execute("SELECT event_seq FROM attachments WHERE id=?", (slot["attachment_id"],)).fetchone()[0]
            if event_seq > snapshot["cutoff_seq"] or rec["kind"] != "image" or rec["received_epoch"] != scope["login_epoch"]:
                raise AttachmentError("storage_invalid")
            if (rec["message_id"], rec["item_index"], rec["received_at"]) != (slot["message_id"], slot["item_index"], slot["received_at"]):
                raise AttachmentError("storage_invalid")
            group_slot = group["slots"][ordinal - 1]
            if any(group_slot[key] != slot[key] for key in ("ordinal", "message_id", "item_index", "attachment_id", "received_at")):
                raise AttachmentError("storage_invalid")
            slot["status"] = self._observed_status(dirs, rec)
        return result

    def _event_result(self, db, dirs, event, scope):
        refs = []
        for aid in event["attachment_ids"]:
            rec = self._record(db, aid, scope)
            refs.append({"attachment_id": aid, "item_index": rec["item_index"], "kind": rec["kind"],
                         "status": self._observed_status(dirs, rec), "stage_path": self._stage_path(aid)})
        context = None
        if event["snapshot_id"]:
            row = db.execute("SELECT payload FROM snapshots WHERE id=?", (event["snapshot_id"],)).fetchone()
            if not row:
                raise AttachmentError("storage_invalid")
            context = self._context(db, dirs, _parse(row[0]), scope)
        return {"schema_version": 1, "attachment_refs": refs, "reference_context": context}

    def register_event(self, scope, message_id, items, text="", received_at=None):
        scope, message_id = _scope(scope), _text(message_id)
        if type(items) is not list or len(items) > MAX_EVENT_ITEMS or type(text) is not str:
            raise AttachmentError("invalid_request")
        normalized, indices = [], set()
        for item in items:
            if type(item) is not dict or set(item) != {"item_index", "kind", "original_name", "declared_type"}:
                raise AttachmentError("invalid_request")
            index = item["item_index"]
            if (type(index) is not int or not 0 <= index <= 1000000 or index in indices
                    or type(item["kind"]) is not str or item["kind"] not in KINDS):
                raise AttachmentError("invalid_request")
            indices.add(index)
            normalized.append({"item_index": index, "kind": item["kind"],
                "original_name": _text(item["original_name"], 4096, nullable=True, empty=True) or None,
                "declared_type": _text(item["declared_type"], 256, nullable=True)})
        normalized.sort(key=lambda item: item["item_index"])
        at = self._now() if received_at is None else _number(received_at)
        _text(text, MAX_JSON_BYTES, empty=True)
        fingerprint = hashlib.sha256(_json([normalized, text]).encode()).hexdigest()
        key = hashlib.sha256(_json([_scope_key(scope), message_id]).encode()).hexdigest()
        with self._db(True) as (db, dirs):
            previous = db.execute("SELECT payload FROM events WHERE event_key=?", (key,)).fetchone()
            if previous:
                event = _parse(previous[0])
                if event["fingerprint"] != fingerprint:
                    raise AttachmentError("invalid_request")
                for aid in event["attachment_ids"]:
                    rec = self._record(db, aid, scope)
                    if rec["status"] == "prepared":
                        self._finish_prepared(db, dirs, rec)
                return self._event_result(db, dirs, event, scope)
            count = db.execute("SELECT count(*) FROM attachments").fetchone()[0]
            if count + len(normalized) > MAX_RECORDS or db.execute("SELECT count(*) FROM events").fetchone()[0] >= MAX_RECORDS:
                raise AttachmentError("quota_exceeded")
            usage_record = self._usage(db, dirs)
            usage = usage_record["accounted_bytes"]
            filesystem = os.fstatvfs(dirs["staging"])
            free = max(0, filesystem.f_bavail * filesystem.f_frsize - usage_record["reserved_bytes"]
                       - self.policy["control_reserve_bytes"])
            seq = db.execute("INSERT INTO events(event_key,scope,payload) VALUES (?,?,'{}')", (key, _scope_key(scope))).lastrowid
            event = {"schema_version": 1, "message_id": message_id, "received_at": at,
                     "seq": seq, "fingerprint": fingerprint, "attachment_ids": [], "snapshot_id": None}
            current = db.execute("SELECT id FROM image_groups WHERE scope_epoch=? ORDER BY rowid DESC LIMIT 1", (_epoch_key(scope),)).fetchone()
            group = self._group(db, current[0]) if current else None
            images = [item for item in normalized if item["kind"] == "image"]
            if images and (group is None or group["closed"] or at >= group["expires_at"]):
                group = {"schema_version": 1, "group_id": uuid.uuid4().hex, "created_at": at,
                         "expires_at": at + self.policy["recent_seconds"], "closed": False,
                         "slots": [], "total_positions": 0, "position_limit": self.policy["max_positions"]}
                db.execute("INSERT INTO image_groups VALUES (?,?,?)", (group["group_id"], _epoch_key(scope), _json(group)))
            for item in normalized:
                aid = uuid.uuid4().hex
                accepted = (usage + self.policy["max_bytes"] <= self.policy["quota_bytes"]
                            and free >= self.policy["max_bytes"])
                if accepted:
                    usage += self.policy["max_bytes"]
                    free -= self.policy["max_bytes"]
                rec = dict(schema_version=1, attachment_id=aid, bot_id=scope["bot_id"], owner_id=scope["owner_id"],
                    conversation_id=scope["conversation_id"], received_epoch=scope["login_epoch"], message_id=message_id,
                    item_index=item["item_index"], received_at=at, kind=item["kind"], original_name=item["original_name"],
                    name_source="original_name" if item["original_name"] is not None else "unknown",
                    declared_type=item["declared_type"], detected_type=None, size=None, sha256=None, object_key=None,
                    status="pending" if accepted else "quota", decryption_state="pending" if accepted else "unavailable",
                    preview_state="unavailable", tags=[], expires_at=at + self.policy["retention_seconds"], pinned=False,
                    failure_reason=None if accepted else "quota_exceeded")
                db.execute("INSERT INTO attachments VALUES (?,?,?,?,?)", (aid, seq, item["item_index"], _scope_key(scope), _json(rec)))
                event["attachment_ids"].append(aid)
                if item["kind"] == "image":
                    retained_prefix = group["total_positions"] == len(group["slots"])
                    group["total_positions"] += 1
                    if retained_prefix and len(group["slots"]) < group["position_limit"]:
                        group["slots"].append({"ordinal": group["total_positions"], "message_id": message_id,
                            "item_index": item["item_index"], "attachment_id": aid, "received_at": at, "status": rec["status"]})
            closes = bool(text.strip()) or any(item["kind"] != "image" for item in normalized) or not images
            if group is not None:
                if closes:
                    group["closed"] = True
                db.execute("UPDATE image_groups SET payload=? WHERE id=?", (_json(group), group["group_id"]))
                if text.strip() and at < group["expires_at"]:
                    sid = uuid.uuid4().hex
                    snapshot = dict(schema_version=1, snapshot_id=sid, text_message_id=message_id,
                        cutoff_seq=seq, group_id=group["group_id"], login_epoch=scope["login_epoch"], created_at=at,
                        expires_at=group["expires_at"], slots=group["slots"],
                        overflow=group["total_positions"] > len(group["slots"]), total_positions=group["total_positions"])
                    db.execute("INSERT INTO snapshots VALUES (?,?,?)", (sid, _epoch_key(scope), _json(snapshot)))
                    event["snapshot_id"] = sid
            db.execute("UPDATE events SET payload=? WHERE seq=?", (_json(event), seq))
            self._fault("register_before_commit")
            return self._event_result(db, dirs, event, scope)

    @staticmethod
    def _object_key(attachment_id, detected_type):
        extensions = {"image/jpeg": "jpg", "jpeg": "jpg", "jpg": "jpg", "image/png": "png", "png": "png",
                      "image/gif": "gif", "gif": "gif", "image/webp": "webp", "webp": "webp", "image/bmp": "bmp",
                      "bmp": "bmp", "video/mp4": "mp4", "mp4": "mp4", "video/quicktime": "mov", "mov": "mov",
                      "application/pdf": "pdf", "pdf": "pdf", "image/heic": "heic", "heic": "heic", "image/heif": "heif", "heif": "heif"}
        return attachment_id + "." + extensions.get((detected_type or "").lower(), "bin")

    def _prepared_file(self, directory, name, expected, fsync=False):
        # Only file-local, classified validation failures are recoverable here.
        # Catalog/SQLite failures and unexpected I/O still abort the transaction.
        try:
            self._file(directory, name, expected, fsync=fsync)
        except FileNotFoundError:
            return "missing"
        except AttachmentError as error:
            if error.code == "attachment_unavailable":
                return "invalid"
            if error.code == "storage_invalid":
                raise _PreparedFileBlocked() from None
            raise
        except OSError as error:
            if error.errno in (errno.ELOOP, errno.ENOTDIR, errno.EACCES, errno.EPERM):
                raise _PreparedFileBlocked() from None
            raise
        return "valid"

    def _finish_prepared(self, db, dirs, rec):
        expected = (rec["size"], rec["sha256"])
        state = self._prepared_file(dirs["objects"], rec["object_key"], expected)
        if state == "invalid":
            # An existing mismatched object is retained verbatim. Recovery must
            # not overwrite it with a stage or publish either untrusted body.
            rec["status"], rec["failure_reason"] = "missing", "prepared_file_invalid"
            self._put(db, rec)
            return rec
        if state == "missing":
            state = self._prepared_file(dirs["staging"], self._stage_name(rec["attachment_id"]), expected, fsync=True)
            if state != "valid":
                rec["status"], rec["failure_reason"] = "missing", "prepared_file_" + state
                self._put(db, rec)
                return rec
            os.rename(self._stage_name(rec["attachment_id"]), rec["object_key"],
                      src_dir_fd=dirs["staging"], dst_dir_fd=dirs["objects"])
            os.fsync(dirs["objects"]); os.fsync(dirs["staging"])
            self._fault("publish_after_replace")
        rec["status"], rec["failure_reason"] = "ready", None
        self._put(db, rec)
        self._fault("publish_before_commit")
        return rec

    def publish(self, scope, attachment_id, staged_path, decryption_state="protocol_decoded",
                detected_type=None, preview_state="unavailable", tags=()):
        scope, attachment_id = _scope(scope), _id(attachment_id)
        if os.fspath(staged_path) != self._stage_path(attachment_id):
            raise AttachmentError("storage_invalid")
        decryption_state = _text(decryption_state, 64)
        detected_type = _text(detected_type, 256, nullable=True)
        preview_state, tags = _text(preview_state, 64), _publication_tags(tags)
        if decryption_state not in ("protocol_decoded", "content_verified", "plaintext_verified"):
            raise AttachmentError("invalid_request")
        if preview_state not in PREVIEW_STATES:
            raise AttachmentError("invalid_request")
        with self._db(True) as (db, dirs):
            rec = self._record(db, attachment_id, scope)
            combined_tags = _merge_publication_tags(rec["tags"], tags)
            if rec["status"] == "ready":
                self._file(dirs["objects"], rec["object_key"], (rec["size"], rec["sha256"]))
                if combined_tags != rec["tags"]:
                    rec["tags"] = combined_tags
                    self._put(db, rec)
                return self._metadata(dirs, rec)
            if rec["status"] == "prepared":
                rec["tags"] = combined_tags
                return self._metadata(dirs, self._finish_prepared(db, dirs, rec))
            if rec["status"] != "pending":
                raise AttachmentError("attachment_unavailable")
            size, digest = self._file(dirs["staging"], self._stage_name(attachment_id), fsync=True)
            os.fsync(dirs["staging"])
            rec.update(status="prepared", size=size, sha256=digest,
                       object_key=self._object_key(attachment_id, detected_type),
                       decryption_state=decryption_state, detected_type=detected_type,
                       preview_state=preview_state, tags=combined_tags)
            self._put(db, rec)
            self._fault("publish_prepare_before_commit")
        self._fault("publish_prepared")
        with self._db(True) as (db, dirs):
            rec = self._record(db, attachment_id, scope)
            if rec["status"] == "prepared":
                self._finish_prepared(db, dirs, rec)
            elif rec["status"] != "ready":
                raise AttachmentError("attachment_unavailable")
            result = self._metadata(dirs, rec)
        self._fault("publish_ready")
        return result

    def fail(self, scope, attachment_id, reason):
        scope, attachment_id = _scope(scope), _id(attachment_id)
        if type(reason) is not str or not REASON_RE.fullmatch(reason):
            raise AttachmentError("invalid_request")
        with self._db(True) as (db, dirs):
            rec = self._record(db, attachment_id, scope)
            if rec["status"] == "prepared":
                self._finish_prepared(db, dirs, rec)
            if rec["status"] == "pending":
                rec.update(status="failed", failure_reason=reason, decryption_state="unavailable")
                self._put(db, rec)
            return self._metadata(dirs, rec)

    def query(self, scope, filters=None, limit=20, cursor=None):
        scope = _scope(scope)
        if type(limit) is not int or not 1 <= limit <= 100 or (filters is not None and type(filters) is not dict):
            raise AttachmentError("invalid_request")
        filters = {} if filters is None else dict(filters)
        if set(filters) - {"received_after", "received_before", "name", "tag", "kind", "status", "message_id"}:
            raise AttachmentError("invalid_request")
        for key in ("received_after", "received_before"):
            if key in filters:
                filters[key] = _number(filters[key])
        for key in ("name", "tag", "message_id"):
            if key in filters:
                _text(filters[key], 512)
        if ("kind" in filters and (type(filters["kind"]) is not str or filters["kind"] not in KINDS)
                or "status" in filters and (type(filters["status"]) is not str or filters["status"] not in STATUSES - {"prepared"})
                or "received_after" in filters and "received_before" in filters and filters["received_after"] > filters["received_before"]):
            raise AttachmentError("invalid_request")
        binding = hashlib.sha256(_json([_scope_key(scope), filters]).encode()).hexdigest()
        after_seq, after_index, cutoff = None, None, None
        if cursor is not None:
            try:
                if type(cursor) is not str or len(cursor) > 512:
                    raise ValueError()
                decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if (set(decoded) != {"v", "binding", "after_seq", "after_index", "cutoff"}
                        or type(decoded["v"]) is not int or decoded["v"] != 1 or decoded["binding"] != binding
                        or any(type(decoded[key]) is not int or decoded[key] < 0 for key in ("after_seq", "after_index", "cutoff"))
                        or decoded["after_seq"] > decoded["cutoff"]):
                    raise ValueError()
                after_seq, after_index, cutoff = decoded["after_seq"], decoded["after_index"], decoded["cutoff"]
            except (ValueError, TypeError, UnicodeError, KeyError):
                raise AttachmentError("invalid_request") from None
        found, more, anchor = [], False, None
        with self._db() as (db, dirs):
            if db is None:
                return {"schema_version": 1, "items": [], "next_cursor": None, "complete": True, "available": False}
            if cutoff is None:
                cutoff = db.execute("SELECT coalesce(max(seq),0) FROM events WHERE scope=?", (_scope_key(scope),)).fetchone()[0]
            sql = "SELECT id,event_seq,item_index FROM attachments WHERE scope=? AND event_seq<=?"
            parameters = [_scope_key(scope), cutoff]
            if after_seq is not None:
                sql += " AND (event_seq<? OR (event_seq=? AND item_index>?))"
                parameters.extend((after_seq, after_seq, after_index))
            sql += " ORDER BY event_seq DESC,item_index ASC"
            for count, row in enumerate(db.execute(sql, parameters), 1):
                if count > MAX_RECORDS:
                    raise AttachmentError("storage_invalid")
                rec = self._metadata(dirs, self._record(db, row[0], scope))
                if ("received_after" in filters and rec["received_at"] < filters["received_after"]
                        or "received_before" in filters and rec["received_at"] > filters["received_before"]
                        or "name" in filters and filters["name"].casefold() not in (rec["original_name"] or "").casefold()
                        or "tag" in filters and not any(filters["tag"].casefold() in tag["value"].casefold() for tag in rec["tags"])
                        or any(key in filters and rec[key] != filters[key] for key in ("kind", "status", "message_id"))):
                    continue
                if len(found) >= limit:
                    more = True
                    break
                found.append(rec)
                anchor = (row[1], row[2])
        next_cursor = base64.urlsafe_b64encode(_json({"v": 1, "binding": binding,
            "after_seq": anchor[0], "after_index": anchor[1], "cutoff": cutoff}).encode()).decode() if more else None
        return {"schema_version": 1, "items": found, "next_cursor": next_cursor, "complete": not more, "available": True}

    @staticmethod
    def _hold(raw, attachment_id=None, holder=None):
        hold = _parse(raw)
        if (type(hold) is not dict or set(hold) != {"schema_version", "attachment_id", "login_epoch", "holder", "created_at", "expires_at", "released_at"}
                or type(hold["schema_version"]) is not int or hold["schema_version"] != 1
                or attachment_id is not None and hold["attachment_id"] != attachment_id
                or holder is not None and hold["holder"] != holder):
            raise AttachmentError("storage_invalid")
        try:
            _id(hold["attachment_id"]); _text(hold["login_epoch"]); _text(hold["holder"])
            _number(hold["created_at"])
            if hold["expires_at"] is None:
                if not hold["holder"].startswith("group:"):
                    raise AttachmentError("storage_invalid")
            elif _number(hold["expires_at"]) < hold["created_at"] or hold["holder"].startswith("group:"):
                raise AttachmentError("storage_invalid")
            if hold["released_at"] is not None:
                _number(hold["released_at"])
        except AttachmentError:
            raise AttachmentError("storage_invalid") from None
        return hold

    def admission_metadata(self, scope, attachment_ids):
        """Read original naming metadata for an existing intent, not a hold."""
        scope = _scope(scope)
        with self._db(False) as (db, _):
            if db is None:
                raise AttachmentError("storage_invalid")
            records = [self._record(db, aid, scope) for aid in attachment_ids]
            return [{"original_name": rec["original_name"], "declared_type": rec["declared_type"]}
                    for rec in records]

    def preflight_admission(self, scope, request_id, attachment_ids=None):
        """Read-only request/material preconditions; never hash or acquire.

        Presence of an old stable request hold is evidence of possible prior
        publication, even if its journal disappeared. It is not a fresh retry.
        Historical same-owner originals retain the normal three-part scope.
        """
        scope = _scope(scope)
        _text(request_id, 256)
        if attachment_ids is not None:
            if (type(attachment_ids) is not list or not 1 <= len(attachment_ids) <= 16
                    or len(set(_id(aid) for aid in attachment_ids)) != len(attachment_ids)):
                raise AttachmentError("invalid_request")
        with self._db(False) as (db, dirs):
            if db is None:
                raise AttachmentError("not_found" if attachment_ids is not None else "storage_invalid")
            if db.execute("SELECT 1 FROM holds WHERE scope=? AND holder=? LIMIT 1",
                    (_scope_key(scope), "group:request:" + request_id)).fetchone():
                raise AttachmentError("storage_invalid")
            if attachment_ids is None:
                return []
            records = [self._record(db, aid, scope) for aid in attachment_ids]
            for rec in records:
                if rec["status"] != "ready":
                    raise AttachmentError("processing" if rec["status"] in ("pending", "prepared")
                                          else "attachment_unavailable")
            # Check the whole batch's metadata before touching any entity.
            for rec in records:
                try:
                    info = os.stat(rec["object_key"], dir_fd=dirs["objects"], follow_symlinks=False)
                except FileNotFoundError:
                    raise AttachmentError("attachment_unavailable") from None
                if not self._private_file(info):
                    raise AttachmentError("storage_invalid")
                if info.st_size != rec["size"]:
                    raise AttachmentError("attachment_unavailable")
            return [{"original_name": rec["original_name"], "declared_type": rec["declared_type"]}
                    for rec in records]

    def _acquire(self, db, dirs, scope, attachment_id, holder, ttl_seconds, now):
        rec = self._record(db, attachment_id, scope)
        if rec["status"] != "ready":
            raise AttachmentError("processing" if rec["status"] in ("pending", "prepared") else "attachment_unavailable")
        try:
            self._file(dirs["objects"], rec["object_key"], (rec["size"], rec["sha256"]))
        except FileNotFoundError:
            rec.update(status="missing", failure_reason="object_missing")
            self._put(db, rec)
            raise AttachmentError("attachment_unavailable") from None
        except AttachmentError as error:
            if error.code == "attachment_unavailable":
                rec.update(status="missing", failure_reason="object_changed")
                self._put(db, rec)
            raise
        hold, hold_id = None, None
        for hid, raw in db.execute("SELECT id,payload FROM holds WHERE attachment_id=? AND scope=? AND holder=?", (attachment_id, _scope_key(scope), holder)):
            candidate = self._hold(raw, attachment_id, holder)
            if (candidate["login_epoch"] == scope["login_epoch"] and candidate["released_at"] is None
                    and (candidate["expires_at"] is None or now < candidate["expires_at"])):
                hold, hold_id = candidate, hid
                break
        if hold is None:
            hold_id = uuid.uuid4().hex
            hold = {"schema_version": 1, "attachment_id": attachment_id, "login_epoch": scope["login_epoch"],
                    "holder": holder, "created_at": now, "expires_at": None if holder.startswith("group:") else now + ttl_seconds,
                    "released_at": None}
            db.execute("INSERT INTO holds VALUES (?,?,?,?,?)", (hold_id, attachment_id, _scope_key(scope), holder, _json(hold)))
        return {"schema_version": 1, "path": str(self.base / "attachments" / "objects" / rec["object_key"]),
                "metadata": self._metadata(dirs, rec), "hold_id": hold_id, "holder": holder, "expires_at": hold["expires_at"]}

    def acquire(self, scope, attachment_id, holder, ttl_seconds=1800):
        scope, attachment_id, holder = _scope(scope), _id(attachment_id), _text(holder)
        ttl_seconds = _number(ttl_seconds, positive=True)
        error, result = None, None
        with self._db(True) as (db, dirs):
            try:
                result = self._acquire(db, dirs, scope, attachment_id, holder, ttl_seconds, self._now())
            except AttachmentError as caught:
                if caught.code != "attachment_unavailable":
                    raise
                error = caught
        if error is not None:
            raise error
        return result

    def release(self, scope, hold_id, holder=None):
        scope, hold_id = _scope(scope), _id(hold_id)
        if holder is not None:
            holder = _text(holder)
        with self._db(True) as (db, _):
            row = db.execute("SELECT scope,holder,payload,attachment_id FROM holds WHERE id=?", (hold_id,)).fetchone()
            if not row or row[0] != _scope_key(scope) or holder is not None and row[1] != holder:
                raise AttachmentError("hold_forbidden")
            hold = self._hold(row[2], row[3], row[1])
            self._record(db, row[3], scope)
            if hold["login_epoch"] != scope["login_epoch"]:
                raise AttachmentError("hold_forbidden")
            if hold["released_at"] is None:
                hold["released_at"] = self._now()
                db.execute("UPDATE holds SET payload=? WHERE id=?", (_json(hold), hold_id))

    def release_delivery_holds(self, scope, group_id, request_id, attachment_ids):
        """Release only proven-terminal delivery holders; trusted sender only.

        The caller proves the ledger terminal state and matching journal first.
        This method neither establishes that proof nor grants worker authority.
        Original bytes are deliberately not opened, including missing originals.
        """
        scope = _scope(scope)
        for value in (group_id, request_id):
            if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", value):
                raise AttachmentError("invalid_request")
        if (type(attachment_ids) is not list or not 1 <= len(attachment_ids) <= 16
                or any(type(aid) is not str or not ID_RE.fullmatch(aid) for aid in attachment_ids)
                or len(set(attachment_ids)) != len(attachment_ids)):
            raise AttachmentError("invalid_request")
        holders = ("group:request:" + request_id, "group:" + group_id)
        with self._db(True, initialize=False) as (db, _):
            if db is None:
                return {"released_count": 0}
            for aid in attachment_ids:
                self._record(db, aid, scope)
            selected, scanned = [], 0
            for aid in attachment_ids:
                for hid, holder, raw in db.execute(
                        "SELECT id,holder,payload FROM holds WHERE attachment_id=? AND scope=? AND holder IN (?,?)",
                        (aid, _scope_key(scope), *holders)):
                    scanned += 1
                    if scanned > MAX_RECORDS:
                        raise AttachmentError("storage_invalid")
                    hold = self._hold(raw, aid, holder)
                    if hold["login_epoch"] == scope["login_epoch"] and hold["released_at"] is None:
                        selected.append((hid, hold))
            now = self._now()
            for hid, hold in selected:
                hold["released_at"] = now
                db.execute("UPDATE holds SET payload=? WHERE id=?", (_json(hold), hid))
            return {"released_count": len(selected)}

    def pin(self, scope, attachment_id, pinned=True):
        scope, attachment_id = _scope(scope), _id(attachment_id)
        if type(pinned) is not bool:
            raise AttachmentError("invalid_request")
        with self._db(True) as (db, dirs):
            rec = self._record(db, attachment_id, scope)
            rec["pinned"] = pinned
            self._put(db, rec)
            return self._metadata(dirs, rec)

    def label(self, scope, attachment_ids, value, message_id, response_bytes=None):
        """Atomically add bounded explicit user provenance to selected records.

        The public wrapper verifies the current owned text and literal label;
        this internal method accepts identity only from that trusted wrapper.
        Long-term record ownership intentionally excludes the receive epoch.
        """
        scope, value, message_id = _scope(scope), _text(value, 512), _text(message_id)
        if len(value) > 128 or not value.strip():
            raise AttachmentError("invalid_request")
        if (type(attachment_ids) is not list or not 1 <= len(attachment_ids) <= 16
                or any(type(aid) is not str or not ID_RE.fullmatch(aid) for aid in attachment_ids)
                or len(set(attachment_ids)) != len(attachment_ids)):
            raise AttachmentError("invalid_request")
        if response_bytes is not None and (type(response_bytes) is not int or response_bytes < 1):
            raise AttachmentError("invalid_request")
        tag = {"value": value, "source": "user", "message_id": message_id}
        with self._db(True) as (db, dirs):
            # Validate every object and the complete result before modifying
            # any row. The transaction also protects against concurrent tags.
            records = [self._record(db, aid, scope) for aid in attachment_ids]
            for rec in records:
                if tag not in rec["tags"]:
                    if len(rec["tags"]) == 32:
                        raise AttachmentError("quota_exceeded")
                    rec["tags"].append(dict(tag))
            result = [self._metadata(dirs, rec) for rec in records]
            if response_bytes is not None and len(_json(result).encode("utf-8")) > response_bytes:
                raise AttachmentError("response_too_large")
            for rec in records:
                self._put(db, rec)
            self._fault("label_before_commit")
            return result

    def resolve_reference(self, scope, snapshot_id, ordinals=None, holder=None):
        scope, snapshot_id = _scope(scope), _id(snapshot_id)
        if ordinals is not None and (type(ordinals) is not list or not ordinals or len(ordinals) > 16
                or any(type(i) is not int or not 1 <= i <= 16 for i in ordinals) or len(set(ordinals)) != len(ordinals)):
            raise AttachmentError("invalid_request")
        if holder is not None:
            holder = _text(holder)
        with self._db(holder is not None) as (db, dirs):
            if db is None:
                raise AttachmentError("not_found")
            row = db.execute("SELECT scope_epoch,payload FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
            if not row:
                raise AttachmentError("not_found")
            expected = _parse(row[0])
            if expected[0] != _scope_key(scope):
                raise AttachmentError("scope_mismatch")
            if expected[1] != scope["login_epoch"]:
                raise AttachmentError("epoch_mismatch")
            context = self._context(db, dirs, _parse(row[1]), scope)
            now = self._now()
            if now >= context["expires_at"]:
                raise AttachmentError("snapshot_expired")
            selected = context["slots"] if ordinals is None else [slot for ordinal in ordinals for slot in context["slots"] if slot["ordinal"] == ordinal]
            if ordinals is not None and len(selected) != len(ordinals):
                raise AttachmentError("not_found")
            materials, unavailable = [], []
            for slot in selected:
                if slot["status"] != "ready":
                    unavailable.append({key: slot[key] for key in ("ordinal", "attachment_id", "status")})
                elif holder is not None:
                    try:
                        acquired = self._acquire(db, dirs, scope, slot["attachment_id"], holder, 1800, now)
                    except AttachmentError as error:
                        if error.code != "attachment_unavailable":
                            raise
                        unavailable.append({"ordinal": slot["ordinal"], "attachment_id": slot["attachment_id"], "status": "missing"})
                        continue
                    meta = acquired["metadata"]
                    materials.append(dict(ordinal=slot["ordinal"], attachment_id=slot["attachment_id"], path=acquired["path"],
                        hold_id=acquired["hold_id"], expires_at=acquired["expires_at"], preview_state=meta["preview_state"],
                        detected_type=meta["detected_type"], size=meta["size"], sha256=meta["sha256"]))
            return {"schema_version": 1, "reference_context": context, "materials": materials,
                    "unavailable": unavailable, "complete": not unavailable}

    def preview_gc(self):
        now = self._now()
        with self._db() as (db, dirs):
            if db is None:
                return {"schema_version": 1, "dry_run": True, "status": "unavailable", "items": [],
                        "physical_bytes": 0, "reserved_bytes": 0, "accounted_bytes": 0, "deleted_files": 0, "deleted_bytes": 0}
            usage = self._usage(db, dirs)
            protected = set()
            for row in db.execute("SELECT attachment_id,payload FROM holds"):
                hold = self._hold(row[1], row[0])
                if hold["released_at"] is None and (hold["expires_at"] is None or now < hold["expires_at"]):
                    protected.add(row[0])
            for scope_epoch, raw in db.execute("SELECT scope_epoch,payload FROM snapshots"):
                snap = _parse(raw)
                encoded_scope, epoch = _parse(scope_epoch)
                identity = _parse(encoded_scope)
                snapshot_scope = _scope(dict(zip(("bot_id", "owner_id", "conversation_id", "login_epoch"), identity + [epoch])))
                self._context(db, dirs, snap, snapshot_scope)
                if now < snap["expires_at"]:
                    protected.update(slot["attachment_id"] for slot in snap["slots"])
            for (gid,) in db.execute("SELECT id FROM image_groups WHERE rowid IN (SELECT max(rowid) FROM image_groups GROUP BY scope_epoch)"):
                group = self._group(db, gid)
                if now < group["expires_at"]:
                    protected.update(slot["attachment_id"] for slot in group["slots"])
            items, eligible, protected_count = [], 0, 0
            for (aid,) in db.execute("SELECT id FROM attachments ORDER BY event_seq,item_index"):
                rec = self._record(db, aid)
                if rec["status"] not in ("ready", "missing"):
                    continue
                reason = ("pinned" if rec["pinned"] else "in_use" if aid in protected else
                          "missing" if self._observed_status(dirs, rec) == "missing" else
                          "expired" if now >= rec["expires_at"] else "retained")
                if reason == "expired":
                    eligible += rec["size"]
                elif reason in ("pinned", "in_use"):
                    protected_count += 1
                items.append({"attachment_id": aid, "status": self._observed_status(dirs, rec),
                              "size": rec["size"], "reason": reason, "eligible": reason == "expired"})
            return dict(schema_version=1, dry_run=True, status="blocked" if usage["unknown_files"] else "planned",
                        items=items, eligible_bytes=eligible, protected_files=protected_count,
                        quota_bytes=self.policy["quota_bytes"], deleted_files=0, deleted_bytes=0, **usage)

    def recover(self, limit=1000, cursor=None):
        if type(limit) is not int or not 1 <= limit <= MAX_RECORDS:
            raise AttachmentError("invalid_request")
        if cursor is not None and (type(cursor) is not int or cursor < 0):
            raise AttachmentError("invalid_request")
        recovered, missing, scanned, blocked = 0, 0, 0, 0
        with self._db(True) as (db, dirs):
            ids = list(db.execute("SELECT rowid,id FROM attachments WHERE rowid>? ORDER BY rowid LIMIT ?", (cursor or 0, limit + 1)))
            for _, aid in ids[:limit]:
                scanned += 1
                rec = self._record(db, aid)
                if rec["status"] == "prepared":
                    try:
                        self._finish_prepared(db, dirs, rec)
                    except _PreparedFileBlocked:
                        # Keep unsafe files and their prepared record intact;
                        # independent healthy entries must still make progress.
                        blocked += 1
                        continue
                    recovered += rec["status"] == "ready"
                    missing += rec["status"] == "missing"
                elif rec["status"] == "ready" and self._observed_status(dirs, rec) != "ready":
                    rec.update(status="missing", failure_reason="object_missing")
                    self._put(db, rec)
                    missing += 1
            return {"schema_version": 1, "scanned": scanned, "recovered": recovered,
                    "missing": missing, "blocked": blocked, "more": len(ids) > limit,
                    "next_cursor": ids[limit-1][0] if len(ids) > limit else None}
