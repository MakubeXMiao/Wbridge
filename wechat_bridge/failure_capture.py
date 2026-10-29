"""Explicit, default-OFF, same-response capture for a single bounded window.

No network/replay implementation. The owner must call cleanup_expired after the
24 h window (and after process restart); capture itself also expires closed.
"""
import base64
import contextlib
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time
import uuid

from .media_diagnostics import REASONS, SOURCES

MAX_TRANSFERS = 3
MAX_SOURCES = 2
MAX_SOURCE_BYTES = 1 << 20
MAX_TOTAL_BYTES = 6 << 20
METADATA_RESERVE = 128 << 10
# Includes marker (2 KiB), journal (16 KiB), six keys (72 KiB), six
# manifests (24 KiB), and the sole pending temp (at most 12 KiB): 126 KiB.
MAX_BODY_BYTES = MAX_TOTAL_BYTES - METADATA_RESERVE
RETENTION_SECONDS = 86400
_MARKER = ".failure-capture-v1.json"
_LOCK = ".failure-capture.lock"
_JOURNAL = ".failure-capture-journal-v1.jsonl"
_MAX_MARKER_BYTES = 2048
_MAX_JOURNAL_BYTES = 16384
_MAX_JOURNAL_RECORDS = 8  # bootstrap, six reservations, one cleanup


class _CaptureBusy(Exception):
    pass


class _CleanupFailure(Exception):
    def __init__(self, deleted):
        self.deleted = deleted


def _json_bytes(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _private(info, directory=False):
    return (info.st_uid == os.geteuid()
            and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600)
            and (stat.S_ISDIR(info.st_mode) if directory else
                 stat.S_ISREG(info.st_mode) and info.st_nlink == 1))


def _root_path(value, repository_roots):
    path = os.path.abspath(os.fspath(value))
    # macOS standard aliases are normalized; caller-created links are rejected.
    for alias in ("/var", "/tmp"):
        if os.path.islink(alias) and (path == alias or path.startswith(alias + "/")):
            path = os.path.realpath(alias) + path[len(alias):]
    current = Path(path)
    for node in (current,) + tuple(current.parents):
        if node.is_symlink() or (node / ".git").exists():
            raise ValueError("failure_capture_configuration_invalid")
    local_repo = Path(__file__).resolve().parent.parent
    for root in (local_repo,) + tuple(repository_roots):
        candidate = os.path.realpath(os.fspath(root))
        if os.path.commonpath((path, candidate)) == candidate:
            raise ValueError("failure_capture_configuration_invalid")
    return path


def _bounded_names(directory, limit):
    names = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(names) >= limit:
                raise ValueError()
            names.add(entry.name)
    return names


class FailureCapture:
    def __init__(self, root=None, *, enabled=False, clock=time.time, repository_roots=()):
        self.enabled = False
        self.cleanup_enabled = False
        self.root = None
        self._fd = None
        self.clock = clock
        if enabled is not True:
            return  # OFF does not stat/create/chmod/clean any path.
        try:
            now = self._now()
            path = _root_path(root, repository_roots)
            if not os.path.exists(path):
                os.mkdir(path, 0o700)
            if not _private(os.lstat(path), True):
                raise ValueError()
            _bounded_names(path, 7)
            self._fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.root = path
            with self._locked():
                names = _bounded_names(self._fd, 7)
                if _JOURNAL in names:
                    info = os.stat(_JOURNAL, dir_fd=self._fd, follow_symlinks=False)
                    if not _private(info):
                        raise ValueError()
                    if self._recover_bootstrap(names, info.st_size):
                        os.unlink(_JOURNAL, dir_fd=self._fd)
                        os.fsync(self._fd)
                        names.remove(_JOURNAL)
                if _JOURNAL not in names:
                    if _MARKER in names:
                        # A validated old window keeps its original budget and
                        # expiry; an old random .tmp is not migration evidence.
                        window = self._validate_window(self._read_json(_MARKER, self._fd))
                        self._validate_files(window, [], legacy=True)
                    else:
                        self._validate_empty_bootstrap(names)
                        window = {"version": 1, "window_id": uuid.uuid4().hex,
                            "created_at": now, "expires_at": now + RETENTION_SECONDS,
                            "transfers": {}, "body_bytes": 0, "cleaned": False}
                    self._append_window(window, [], initial=True)
                for directory in ("bodies", "keys", "manifests"):
                    try:
                        os.mkdir(directory, 0o700, dir_fd=self._fd)
                    except FileExistsError:
                        pass
                    info = os.stat(directory, dir_fd=self._fd, follow_symlinks=False)
                    if not _private(info, True):
                        raise ValueError()
                _, _, torn, _ = self._recover()
            self.enabled = not torn
            self.cleanup_enabled = True
        except Exception:
            self.close()
            raise ValueError("failure_capture_configuration_invalid") from None

    def _recover_bootstrap(self, names, size):
        if _MARKER not in names:
            if size == 0:
                self._validate_empty_bootstrap(names - {_JOURNAL})
                return True
            return False
        # A legacy ledger remains authoritative while revision zero is being
        # written. Only its exact initial-record prefix is provably ours; never
        # truncate a journal with even one complete record or unknown bytes.
        if size > 2048:
            return False
        fd = os.open(_JOURNAL, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self._fd)
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(2049)
        if b"\n" in raw:
            return False
        window = self._validate_window(self._read_json(_MARKER, self._fd))
        self._validate_files(window, [], legacy=True, bootstrap_journal=True)
        _, expected = self._journal_line(window, [])
        if not expected.startswith(raw):
            raise ValueError()
        return True

    def _validate_empty_bootstrap(self, names):
        if names - {_LOCK, "bodies", "keys", "manifests"}:
            raise ValueError()
        for name in names:
            if name == _LOCK:
                info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
                if not _private(info) or info.st_size:
                    raise ValueError()
            else:
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
                try:
                    if not _private(os.fstat(fd), True) or _bounded_names(fd, 0):
                        raise ValueError()
                finally:
                    os.close(fd)

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError()
        return value

    @contextlib.contextmanager
    def _locked(self):
        info = os.stat(self.root, follow_symlinks=False)
        own = os.fstat(self._fd)
        if not _private(info, True) or (info.st_dev, info.st_ino) != (own.st_dev, own.st_ino):
            raise ValueError()
        fd = os.open(_LOCK, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=self._fd)
        try:
            if not _private(os.fstat(fd)):
                raise ValueError()
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise _CaptureBusy() from None
            yield
        finally:
            os.close(fd)

    def _read_json(self, name, directory):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        with os.fdopen(fd, "rb") as stream:
            if not _private(os.fstat(stream.fileno())):
                raise ValueError()
            raw = stream.read(32769)
        if len(raw) > 32768:
            raise ValueError()
        return json.loads(raw)

    def _write_bytes(self, name, data, directory):
        fd = os.open(name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())

    def _write_json(self, name, value, directory, temporary):
        raw = _json_bytes(value)
        if len(raw) > (_MAX_MARKER_BYTES if name == _MARKER else 12288):
            raise ValueError()
        try:
            self._write_bytes(temporary, raw, directory)
            os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass

    @staticmethod
    def _journal_line(window, records):
        revision = len(records)
        record = {"version": 1, "revision": revision,
                  "previous": records[-1]["hash"] if records else None, "window": window}
        raw = _json_bytes(record)
        digest = hashlib.sha256(raw).hexdigest()
        line = digest.encode("ascii") + b" " + raw + b"\n"
        return digest, line

    def _append_window(self, window, records, initial=False):
        self._validate_window(window)
        if records:
            self._validate_transition(records[-1]["window"], window)
        revision = len(records)
        if revision >= _MAX_JOURNAL_RECORDS:
            raise ValueError()
        digest, line = self._journal_line(window, records)
        if len(line) > 2048:
            raise ValueError()
        flags = os.O_WRONLY | os.O_NOFOLLOW | (os.O_CREAT | os.O_EXCL if initial else os.O_APPEND)
        fd = os.open(_JOURNAL, flags, 0o600, dir_fd=self._fd)
        with os.fdopen(fd, "ab" if not initial else "wb") as stream:
            info = os.fstat(stream.fileno())
            if not _private(info) or info.st_size + len(line) > _MAX_JOURNAL_BYTES:
                raise ValueError()
            stream.write(line)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(self._fd)
        # Only a durable complete journal entry authorizes later sample/temp IO.
        records.append({"revision": revision, "hash": digest, "window": copy.deepcopy(window)})

    def _read_journal(self):
        fd = os.open(_JOURNAL, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self._fd)
        with os.fdopen(fd, "rb") as stream:
            if not _private(os.fstat(stream.fileno())):
                raise ValueError()
            raw = stream.read(_MAX_JOURNAL_BYTES + 1)
        if len(raw) > _MAX_JOURNAL_BYTES:
            raise ValueError()
        records, torn = [], False
        for line in raw.splitlines(keepends=True):
            if not line.endswith(b"\n"):
                if len(line) > 2048:
                    raise ValueError()
                torn = True
                break
            if len(records) >= _MAX_JOURNAL_RECORDS or len(line) > 2048:
                raise ValueError()
            digest, separator, content = line[:-1].partition(b" ")
            if (not separator or not re.fullmatch(b"[0-9a-f]{64}", digest)
                    or hashlib.sha256(content).hexdigest().encode("ascii") != digest):
                raise ValueError()
            record = json.loads(content)
            if (type(record) is not dict
                    or set(record) != {"version", "revision", "previous", "window"}
                    or type(record["version"]) is not int or record["version"] != 1
                    or type(record["revision"]) is not int or record["revision"] != len(records)
                    or record["previous"] != (records[-1]["hash"] if records else None)):
                raise ValueError()
            window = self._validate_window(record["window"])
            if records:
                self._validate_transition(records[-1]["window"], window)
            records.append({"revision": record["revision"], "hash": digest.decode("ascii"), "window": window})
        if not records:
            raise ValueError()  # No durable bootstrap: no sample IO was allowed.
        return records, torn

    def _validate_transition(self, old, new):
        if (old["cleaned"] or any(old[name] != new[name]
                for name in ("version", "window_id", "created_at", "expires_at"))
                or new["body_bytes"] < old["body_bytes"]):
            raise ValueError()
        for transfer, sources in old["transfers"].items():
            if any(new["transfers"].get(transfer, {}).get(source) != sample
                   for source, sample in sources.items()):
                raise ValueError()
        before = sum(map(len, old["transfers"].values()))
        after = sum(map(len, new["transfers"].values()))
        if new["cleaned"]:
            if old["transfers"] != new["transfers"] or old["body_bytes"] != new["body_bytes"]:
                raise ValueError()
        elif after != before + 1 or new["body_bytes"] - old["body_bytes"] > MAX_SOURCE_BYTES:
            raise ValueError()

    @staticmethod
    def _temporary(window, revision, kind, sample=None):
        suffix = "-" + sample if sample is not None else ""
        return ".tmp-capture-%s-%d-%s%s.json" % (window["window_id"], revision, kind, suffix)

    def _write_marker(self, window, records):
        self._write_json(_MARKER, window, self._fd,
                         self._temporary(window, records[-1]["revision"], "marker"))

    def _recover(self):
        records, torn = self._read_journal()
        window = copy.deepcopy(records[-1]["window"])
        pending, marker = self._validate_files(window, records, torn=torn)
        latch = dict(window, cleaned=True)
        if torn and marker == latch:
            window = latch
        # Validate the entire bounded tree before unlinking any proven-owned temp.
        deleted = 0
        try:
            for directory, filename in pending:
                fd = self._fd if directory is None else os.open(directory,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
                try:
                    if directory is None and torn:
                        try:
                            temporary = self._read_json(filename, fd)
                        except (ValueError, UnicodeError):
                            temporary = None
                        else:
                            temporary = self._validate_window(temporary)
                            if temporary != records[-1]["window"] and temporary != latch:
                                raise ValueError()
                        if temporary == latch:
                            # Finish the durable cleaned-only intent directly;
                            # recovery may die again without losing the latch.
                            os.replace(filename, _MARKER, src_dir_fd=fd, dst_dir_fd=fd)
                            os.fsync(fd)
                            marker = window = latch
                            continue
                    os.unlink(filename, dir_fd=fd)
                    deleted += 1
                    os.fsync(fd)
                finally:
                    if directory is not None:
                        os.close(fd)
            if not torn and marker != window:
                self._write_marker(window, records)
        except Exception:
            if deleted:
                raise _CleanupFailure(deleted) from None
            raise
        return window, records, torn, deleted

    def _validate_window(self, value):
        if (type(value) is not dict
                or set(value) != {"version", "window_id", "created_at", "expires_at", "transfers", "body_bytes", "cleaned"}
                or type(value.get("version")) is not int or value["version"] != 1
                or type(value.get("cleaned")) is not bool
                or not re.fullmatch(r"[0-9a-f]{32}", value.get("window_id", ""))
                or type(value.get("transfers")) is not dict
                or type(value.get("body_bytes")) is not int
                or not 0 <= value["body_bytes"] <= MAX_BODY_BYTES
                or len(value["transfers"]) > MAX_TRANSFERS):
            raise ValueError()
        for field in ("created_at", "expires_at"):
            number = value.get(field)
            if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
                raise ValueError()
        if value["expires_at"] - value["created_at"] != RETENTION_SECONDS:
            raise ValueError()
        for transfer, sources in value["transfers"].items():
            if (type(transfer) is not str or not re.fullmatch(r"[0-9a-f]{32}", transfer)
                    or type(sources) is not dict or not 1 <= len(sources) <= MAX_SOURCES):
                raise ValueError()
            for source, sample in sources.items():
                if (type(source) is not str or source not in SOURCES or type(sample) is not str
                        or not re.fullmatch(r"[0-9a-f]{32}", sample)):
                    raise ValueError()
        samples = [sample for sources in value["transfers"].values() for sample in sources.values()]
        if len(set(samples)) != len(samples) or len(_json_bytes(value)) > _MAX_MARKER_BYTES:
            raise ValueError()
        return value

    @staticmethod
    def _key(value):
        if type(value) is str:
            if len(value) > 4096:
                return {"type": "oversized"}
            # Preserve only explicit key-shaped representations. The crypto
            # fallback can derive keys from arbitrary text; evidence capture
            # deliberately does NOT persist that broader/private text domain.
            if not value.isascii():
                return {"type": "invalid"}
            supported = bool(re.fullmatch(r"[A-Za-z0-9_-]{16}|[A-Za-z0-9_-]{32}|[0-9a-fA-F]{64}", value))
            if not supported and len(value) in (22, 24, 43, 44, 86, 88):
                raw_value = value.encode("ascii")
                for alt, encoder in ((None, base64.b64encode), (b"-_", base64.urlsafe_b64encode)):
                    try:
                        decoded = base64.b64decode(raw_value + b"=" * ((-len(raw_value)) % 4),
                                                   altchars=alt, validate=True)
                        canonical = encoder(decoded).rstrip(b"=") == raw_value.rstrip(b"=")
                        if canonical and (len(decoded) in (16, 32)
                                or len(decoded) == 64 and re.fullmatch(b"[0-9a-fA-F]{64}", decoded)):
                            supported = True
                    except (ValueError, TypeError):
                        pass
            if not supported:
                return {"type": "invalid"}
            raw, kind = value.encode("utf-8"), "str"
        elif type(value) in (bytes, bytearray):
            if len(value) > 4096:
                return {"type": "oversized"}
            if len(value) not in (16, 32):
                return {"type": "invalid"}
            raw, kind = bytes(value), "bytes"
        elif value is None:
            return {"type": "null"}
        else:
            return {"type": "invalid"}
        if len(raw) > 4096:
            return {"type": "oversized"}
        return {"type": kind, "base64": base64.b64encode(raw).decode("ascii")}

    def _validate_files(self, window, records, legacy=False, bootstrap_journal=False, torn=False):
        """Bounded whole-tree check; an intent proves exact temp ownership."""
        names = _bounded_names(self._fd, 7)
        fixed = {_MARKER, _LOCK, "bodies", "keys", "manifests"}
        expected_root = fixed if legacy and not bootstrap_journal else fixed | {_JOURNAL}
        root_temp = None if legacy else self._temporary(window, records[-1]["revision"], "marker")
        if (names - expected_root - ({root_temp} if root_temp else set())
                or expected_root - names - ({_MARKER} if not legacy else set())):
            raise ValueError()
        samples = {sample for sources in window["transfers"].values() for sample in sources.values()}
        owners = {}
        for record in records:
            for sources in record["window"]["transfers"].values():
                for sample in sources.values():
                    owners.setdefault(sample, record["revision"])
        total, body_total, pending = 0, 0, []
        marker = None
        for name in names - {"bodies", "keys", "manifests"}:
            info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if not _private(info):
                raise ValueError()
            cap = _MAX_JOURNAL_BYTES if name == _JOURNAL else 0 if name == _LOCK else _MAX_MARKER_BYTES
            if info.st_size > cap:
                raise ValueError()
            total += info.st_size
            if name == root_temp:
                pending.append((None, name))
            elif name == _MARKER:
                marker = self._validate_window(self._read_json(_MARKER, self._fd))
                if (not legacy and not any(marker == record["window"] for record in records)
                        and not (torn and marker == dict(window, cleaned=True))):
                    raise ValueError()
        for name, extension in (("bodies", ".bin"), ("keys", ".json"), ("manifests", ".json")):
            directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
            try:
                if not _private(os.fstat(directory), True):
                    raise ValueError()
                expected = {sample + extension for sample in samples}
                temps = {} if name == "bodies" or legacy else {
                    self._temporary(window, owners[sample], name, sample): sample for sample in samples}
                for filename in _bounded_names(directory, len(expected) + (1 if temps else 0)):
                    if filename not in expected and filename not in temps:
                        raise ValueError()
                    info = os.stat(filename, dir_fd=directory, follow_symlinks=False)
                    if not _private(info):
                        raise ValueError()
                    if name == "bodies":
                        if info.st_size > MAX_SOURCE_BYTES:
                            raise ValueError()
                        body_total += info.st_size
                    elif info.st_size > (12288 if name == "keys" else 4096):
                        raise ValueError()
                    if filename in temps:
                        pending.append((name, filename))
                    total += info.st_size
                    if total > MAX_TOTAL_BYTES:
                        raise ValueError()
            finally:
                os.close(directory)
        if total > MAX_TOTAL_BYTES or body_total > window["body_bytes"] or len(pending) > 1:
            raise ValueError()
        return pending, marker

    def capture(self, transfer_id, source, body, item_key=None, media_key=None,
                expected_type=None, reason="unknown"):
        if not self.enabled:
            return {"status": "disabled"}
        try:
            if (type(transfer_id) is not str or not re.fullmatch(r"[0-9a-f]{32}", transfer_id)
                    or type(body) is not bytes):
                return {"status": "not_captured"}
            source = source if type(source) is str and source in SOURCES else "unknown"
            reason = reason if type(reason) is str and reason in REASONS and reason != "ok" else "unknown"
            with self._locked():
                window, records, torn, _ = self._recover()
                if torn:
                    self.enabled = False
                    return {"status": "capture_failed"}
                now = self._now()
                if window["cleaned"] or now >= window["expires_at"]:
                    self._cleanup_locked(window, records, torn)
                    return {"status": "expired"}
                if now < window["created_at"]:
                    return {"status": "capture_failed"}
                transfers = window["transfers"]
                if transfer_id not in transfers and len(transfers) >= MAX_TRANSFERS:
                    return {"status": "budget_exhausted"}
                sources = transfers.setdefault(transfer_id, {})
                if source in sources:
                    # A spent slot is not proof that crash-interrupted files
                    # are complete or replayable. Never refill or refund it.
                    return {"status": "already_captured", "sample_id": sources[source]}
                if len(sources) >= MAX_SOURCES:
                    return {"status": "budget_exhausted"}
                sample = uuid.uuid4().hex
                sources[source] = sample
                allowance = min(MAX_SOURCE_BYTES, MAX_BODY_BYTES - window["body_bytes"])
                sample_body = body[:allowance]
                window["body_bytes"] += len(sample_body)
                # Reserve durably BEFORE writing. A failed capture still spends
                # this slot; a new object/process cannot bypass the budget.
                self._append_window(window, records)
                self._write_marker(window, records)
                truncated = len(body) > len(sample_body)
                keys = {"item_key": self._key(item_key), "media_key": self._key(media_key)}
                replayable = not truncated and all(value["type"] not in ("invalid", "oversized")
                                                   for value in keys.values())
                directories = []
                try:
                    for name in ("bodies", "keys", "manifests"):
                        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
                        if not _private(os.fstat(fd), True):
                            os.close(fd)
                            raise ValueError()
                        directories.append(fd)
                    self._write_bytes(sample + ".bin", sample_body, directories[0])
                    self._write_json(sample + ".json", keys, directories[1],
                        self._temporary(window, records[-1]["revision"], "keys", sample))
                    self._write_json(sample + ".json", {"version": 1, "sample_id": sample,
                        "source": source, "expected_type": "image" if expected_type == "image" else "generic",
                        "reason": reason, "captured_at": now, "expires_at": window["expires_at"],
                        "downloaded_bytes": len(body), "saved_bytes": len(sample_body),
                        "truncated": truncated, "replayable": replayable,
                        "body_sha256": hashlib.sha256(sample_body).hexdigest()}, directories[2],
                        self._temporary(window, records[-1]["revision"], "manifests", sample))
                    self._validate_files(window, records)
                finally:
                    for fd in directories:
                        os.close(fd)
                return {"status": "captured", "sample_id": sample,
                        "truncated": truncated, "replayable": replayable}
        except _CaptureBusy:
            return {"status": "capture_busy"}
        except Exception:
            self.enabled = False  # Fail closed; a repeated error cannot grow temps.
            return {"status": "capture_failed"}

    def _cleanup_locked(self, window, records, torn):
        deleted = 0
        try:
            if not window["cleaned"]:
                window["cleaned"] = True
                if torn:
                    # The broken journal is immutable. An exact cleaned-only
                    # cache transition latches cleanup before any sample unlink.
                    self._write_marker(window, records)
                else:
                    self._append_window(window, records)
            for name, extension in (("bodies", ".bin"), ("keys", ".json"), ("manifests", ".json")):
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
                try:
                    if not _private(os.fstat(fd), True):
                        raise ValueError()
                    for sources in window["transfers"].values():
                        for sample in sources.values():
                            try:
                                os.unlink(sample + extension, dir_fd=fd)
                                deleted += 1
                            except FileNotFoundError:
                                pass
                    os.fsync(fd)
                finally:
                    os.close(fd)
            if not torn and self._read_json(_MARKER, self._fd) != window:
                self._write_marker(window, records)
        except Exception:
            raise _CleanupFailure(deleted) from None
        return deleted

    def cleanup_expired(self, now=None):
        """deleted counts this call's sample and recovered-temp unlinks only."""
        if not self.cleanup_enabled:
            return {"status": "disabled", "deleted": 0}
        recovered = 0
        try:
            current = self._now() if now is None else now
            if type(current) not in (int, float) or not math.isfinite(current):
                return {"status": "cleanup_failed", "deleted": 0}
            with self._locked():
                window, records, torn, recovered = self._recover()
                if torn:
                    self.enabled = False
                if not window["cleaned"] and current < window["expires_at"]:
                    return {"status": "not_expired", "deleted": recovered}
                deleted = self._cleanup_locked(window, records, torn)
                return {"status": "cleaned", "deleted": deleted + recovered}
        except _CaptureBusy:
            return {"status": "cleanup_busy", "deleted": 0}
        except _CleanupFailure as error:
            self.enabled = False
            deleted = error.deleted + recovered
            return {"status": "cleanup_partial" if deleted else "cleanup_failed", "deleted": deleted}
        except Exception:
            self.enabled = False
            return {"status": "cleanup_failed", "deleted": 0}

    def close(self):
        self.enabled = False
        self.cleanup_enabled = False
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
