"""Explicit, default-OFF, same-response capture for a single bounded window.

No network/replay implementation. The owner must call cleanup_expired after the
24 h window (and after process restart); capture itself also expires closed.
"""
import base64
import contextlib
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
MAX_BODY_BYTES = MAX_TOTAL_BYTES - METADATA_RESERVE
RETENTION_SECONDS = 86400
_MARKER = ".failure-capture-v1.json"
_LOCK = ".failure-capture.lock"


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
            names = _bounded_names(path, 5)
            if names and _MARKER not in names:
                raise ValueError()
            self._fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.root = path
            with self._locked():
                names = _bounded_names(self._fd, 5)
                if names - {_MARKER, _LOCK, "bodies", "keys", "manifests"}:
                    raise ValueError()
                for directory in ("bodies", "keys", "manifests"):
                    try:
                        os.mkdir(directory, 0o700, dir_fd=self._fd)
                    except FileExistsError:
                        pass
                    info = os.stat(directory, dir_fd=self._fd, follow_symlinks=False)
                    if not _private(info, True):
                        raise ValueError()
                if _MARKER in names:
                    window = self._validate_window(self._read_json(_MARKER, self._fd))
                else:
                    window = {"version": 1, "window_id": uuid.uuid4().hex,
                        "created_at": now, "expires_at": now + RETENTION_SECONDS,
                        "transfers": {}, "body_bytes": 0, "cleaned": False}
                    self._write_json(_MARKER, window, self._fd)
                self._validate_files(window)
            self.enabled = True
        except Exception:
            self.close()
            raise ValueError("failure_capture_configuration_invalid") from None

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
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
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

    def _write_json(self, name, value, directory):
        raw = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        if len(raw) > 32768:
            raise ValueError()
        temporary = ".tmp-" + uuid.uuid4().hex
        try:
            self._write_bytes(temporary, raw, directory)
            os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass

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
            if (not re.fullmatch(r"[0-9a-f]{32}", transfer) or type(sources) is not dict
                    or len(sources) > MAX_SOURCES):
                raise ValueError()
            for source, sample in sources.items():
                if source not in SOURCES or not re.fullmatch(r"[0-9a-f]{32}", sample):
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

    def _validate_files(self, window):
        """At most 18 registered files; missing reserved crash slots are safe."""
        names = _bounded_names(self._fd, 5)
        if names != {_MARKER, _LOCK, "bodies", "keys", "manifests"}:
            raise ValueError()
        samples = {sample for sources in window["transfers"].values() for sample in sources.values()}
        total, body_total = 0, 0
        for name in (_MARKER, _LOCK):
            info = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if not _private(info):
                raise ValueError()
            if info.st_size > (8192 if name == _MARKER else 0):
                raise ValueError()
            total += info.st_size
        for name, extension in (("bodies", ".bin"), ("keys", ".json"), ("manifests", ".json")):
            directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
            try:
                if not _private(os.fstat(directory), True):
                    raise ValueError()
                expected = {sample + extension for sample in samples}
                for filename in _bounded_names(directory, len(expected)):
                    if filename not in expected:
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
                    total += info.st_size
                    if total > MAX_TOTAL_BYTES:
                        raise ValueError()
            finally:
                os.close(directory)
        if total > MAX_TOTAL_BYTES or body_total > window["body_bytes"]:
            raise ValueError()

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
                window = self._validate_window(self._read_json(_MARKER, self._fd))
                self._validate_files(window)
                now = self._now()
                if now < window["created_at"]:
                    return {"status": "capture_failed"}
                if now >= window["expires_at"]:
                    self._cleanup_locked(window)
                    return {"status": "expired"}
                if window["cleaned"]:
                    return {"status": "expired"}
                transfers = window["transfers"]
                if transfer_id not in transfers and len(transfers) >= MAX_TRANSFERS:
                    return {"status": "budget_exhausted"}
                sources = transfers.setdefault(transfer_id, {})
                if source in sources:
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
                self._write_json(_MARKER, window, self._fd)
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
                    self._write_json(sample + ".json", keys, directories[1])
                    self._write_json(sample + ".json", {"version": 1, "sample_id": sample,
                        "source": source, "expected_type": "image" if expected_type == "image" else "generic",
                        "reason": reason, "captured_at": now, "expires_at": window["expires_at"],
                        "downloaded_bytes": len(body), "saved_bytes": len(sample_body),
                        "truncated": truncated, "replayable": replayable,
                        "body_sha256": hashlib.sha256(sample_body).hexdigest()}, directories[2])
                    self._validate_files(window)
                finally:
                    for fd in directories:
                        os.close(fd)
                return {"status": "captured", "sample_id": sample,
                        "truncated": truncated, "replayable": replayable}
        except Exception:
            self.enabled = False  # Fail closed; a repeated error cannot grow temps.
            return {"status": "capture_failed"}

    def _cleanup_locked(self, window):
        deleted = 0
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
            finally:
                os.close(fd)
        window["cleaned"] = True
        self._write_json(_MARKER, window, self._fd)
        return deleted

    def cleanup_expired(self, now=None):
        if not self.enabled:
            return {"status": "disabled", "deleted": 0}
        try:
            current = self._now() if now is None else now
            if type(current) not in (int, float) or not math.isfinite(current):
                return {"status": "cleanup_failed", "deleted": 0}
            with self._locked():
                window = self._validate_window(self._read_json(_MARKER, self._fd))
                if current < window["expires_at"]:
                    return {"status": "not_expired", "deleted": 0}
                deleted = self._cleanup_locked(window)
                return {"status": "cleaned", "deleted": deleted}
        except Exception:
            self.enabled = False
            return {"status": "cleanup_failed", "deleted": 0}

    def close(self):
        self.enabled = False
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
