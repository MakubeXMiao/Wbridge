"""Conservative offline media retention. Planning never creates state or locks.

Updated Store users hold a shared lifetime pin. Applying requires an explicit
offline maintenance assertion, an exclusive pin and the existing serve lock.
Unknown references, unsupported state or races fail closed. Staging is retained.
"""
import contextlib
import fcntl
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import urllib.parse
import uuid
import weakref

PIN_NAME = "media_usage.lock"
CLIENT_PINS = "media_usage_clients"
MAX_FILES = 10000
MAX_METADATA_BYTES = 16 << 20
MAX_JSON_BYTES = 1 << 20
MAX_VALUES = 100000
DB_COLUMNS = {
    "metadata": (("key", "TEXT"), ("value", "TEXT")),
    "jobs": (("id", "TEXT"), ("sender", "TEXT"), ("status", "TEXT"), ("payload", "TEXT")),
    "inputs": (("id", "TEXT"), ("sender", "TEXT"), ("kind", "TEXT"), ("job_id", "TEXT"), ("record", "TEXT")),
    "approvals": (("id", "TEXT"), ("status", "TEXT"), ("payload", "TEXT")),
    "outputs": (("id", "TEXT"), ("phase", "TEXT"), ("payload", "TEXT")),
    "routes": (("sender", "TEXT"), ("backend", "TEXT"), ("revision", "INTEGER")),
    "sessions": (("key", "TEXT"), ("session_id", "TEXT")),
}


def _regular(fd):
    info = os.fstat(fd)
    return stat.S_ISREG(info.st_mode) and info.st_nlink == 1


def pin_store(store):
    """Pin even before the first media download, until Store.close()/GC."""
    path = os.path.join(store.state_dir, PIN_NAME)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    directory, client = None, None
    name = uuid.uuid4().hex + ".lock"
    try:
        if not _regular(fd):
            raise ValueError("media_usage_lock_invalid")
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        if os.stat(path, follow_symlinks=False).st_ino != os.fstat(fd).st_ino:
            raise ValueError("media_usage_lock_replaced")
        pins = os.path.join(store.state_dir, CLIENT_PINS)
        os.makedirs(pins, mode=0o700, exist_ok=True)
        directory = os.open(pins, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        info = os.fstat(directory)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("media_client_pins_invalid")
        client = os.open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        fcntl.flock(client, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        if client is not None:
            os.close(client)
        if directory is not None:
            try:
                os.unlink(name, dir_fd=directory)
            except OSError:
                pass
            os.close(directory)
        os.close(fd)
        raise
    store._media_pin = weakref.finalize(store, _release_pin, fd, directory, client, name)


def _release_pin(global_fd, directory, client, name):
    try:
        current = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if (current.st_dev, current.st_ino) == (os.fstat(client).st_dev, os.fstat(client).st_ino):
            os.unlink(name, dir_fd=directory)
    except OSError:
        pass
    finally:
        for fd in (client, directory, global_fd):
            os.close(fd)


def client_pins_are_control_files(path):
    """Fresh-login exemption for our own zero-byte coordination files only."""
    try:
        directory = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(directory)
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                return False
            with os.scandir(directory) as entries:
                for count, entry in enumerate(entries, 1):
                    info = entry.stat(follow_symlinks=False)
                    if (count > MAX_FILES or not re.fullmatch(r"[0-9a-f]{32}\.lock", entry.name)
                            or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_size or info.st_uid != os.geteuid() or info.st_mode & 0o077):
                        return False
            return True
        finally:
            os.close(directory)
    except OSError:
        return False


def _check_client_pins(state):
    path = state / CLIENT_PINS
    if not path.exists():
        return  # Legacy roots require the explicit offline maintenance assertion.
    if not client_pins_are_control_files(path):
        raise ValueError("media_client_pins_unknown")
    directory = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with os.scandir(directory) as entries:
            for count, entry in enumerate(entries, 1):
                if count > MAX_FILES:
                    raise ValueError("reference_budget_exceeded")
                fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
                try:
                    if not _regular(fd):
                        raise ValueError("media_client_pin_invalid")
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(fd)
    finally:
        os.close(directory)


def _checked_root(path):
    if not isinstance(path, (str, os.PathLike)) or not os.path.isabs(path):
        raise ValueError("root_invalid")
    root = Path(os.path.normpath(os.fspath(path)))
    # Only normalize the standard aliases when they really are OS links.
    for alias in ("/var", "/tmp"):
        if os.path.islink(alias) and (str(root) == alias or str(root).startswith(alias + "/")):
            root = Path(os.path.realpath(alias) + str(root)[len(alias):])
    if root in (Path("/"), Path.home()) or len(root.parts) < 3:
        raise ValueError("root_invalid")
    for part in (root,) + tuple(root.parents)[:-1]:
        if part.is_symlink():
            raise ValueError("root_symlink")
    if not root.is_dir() or (root / ".git").exists():
        raise ValueError("root_invalid")
    if root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o022:
        raise ValueError("root_permissions_invalid")
    return root


class _AttachmentBlocked(ValueError):
    def __init__(self, summary):
        self.summary = summary
        super().__init__("attachment_state_unknown")


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _attachment_present(base):
    """Presence alone forbids this release from applying any media deletion."""
    root = Path(base)
    return any(os.path.lexists(str(path)) for path in (
        root / "attachments", root / "state/attachments_enabled.json",
        root / "state/attachments.sqlite3", root / "state/attachment-delivery.sqlite3"))


def _attachment_preview(root, clock):
    """Read only the two explicitly owned databases; never construct Store."""
    empty = {"schema_version": 1, "status": "unavailable"}
    summary = {"schema_version": 1, "dry_run": True, "status": "unavailable",
               "active": _attachment_present(root), "delete_enabled": False,
               "catalog": dict(empty), "delivery": dict(empty)}
    known, skipped = {}, {}
    if not summary["active"]:
        return summary, known, skipped
    try:
        from .attachments import AttachmentStore
        from .attachment_delivery import DeliveryLedger
        from .attachment_runtime import effective_policy
        catalog_path = root / "state/attachments.sqlite3"
        delivery_path = root / "state/attachment-delivery.sqlite3"
        before = {}
        for path in (catalog_path, delivery_path):
            for suffix in ("-journal", "-wal", "-shm"):
                if os.path.lexists(str(path) + suffix):
                    raise ValueError("attachment_sidecar")
            if os.path.lexists(str(path)):
                before[path] = _signature(path.lstat())
        catalog_marker = root / "attachments/catalog.json"
        if os.path.lexists(str(catalog_marker)):
            before[catalog_marker] = _signature(catalog_marker.lstat())
        marker = root / "state/attachments_enabled.json"
        if os.path.lexists(str(marker)):
            fd = os.open(str(marker), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
            try:
                info = os.fstat(fd)
                if (not _regular(fd) or info.st_uid != os.geteuid() or info.st_mode & 0o077
                        or info.st_size > 65536):
                    raise ValueError("attachment_marker_invalid")
                value = json.loads(os.read(fd, 65537).decode("utf-8"))
            finally:
                os.close(fd)
            if (type(value) is not dict or set(value) != {"schema_version", "enabled_at"}
                    or type(value["schema_version"]) is not int or value["schema_version"] != 1
                    or type(value["enabled_at"]) not in (int, float)
                    or not math.isfinite(value["enabled_at"]) or value["enabled_at"] < 0):
                raise ValueError("attachment_marker_invalid")
        catalog = AttachmentStore(str(root), policy=effective_policy(str(root)), clock=clock).preview_gc()
        delivery = DeliveryLedger.inspect_readonly(root)
        summary.update(catalog=catalog, delivery=delivery)
        if catalog.get("schema_version") != 1 or catalog.get("status") not in ("planned", "unavailable"):
            raise ValueError("attachment_catalog_unknown")
        if catalog.get("unknown_files"):
            raise ValueError("attachment_catalog_unknown")
        if catalog_path in before:
            if catalog["status"] != "planned":
                raise ValueError("attachment_catalog_unavailable")
            directory = root / "attachments"
            fd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                found = set()
                with os.scandir(fd) as entries:
                    for entry in entries:
                        info = entry.stat(follow_symlinks=False)
                        if entry.name == "catalog.json":
                            # AttachmentStore.preview_gc has already checked
                            # this private identity against the actual catalog.
                            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                                    or info.st_uid != os.geteuid() or info.st_mode & 0o077
                                    or catalog_marker not in before
                                    or _signature(info) != before[catalog_marker]):
                                raise ValueError("attachment_state_changed")
                            known[catalog_marker] = _signature(info)
                            continue
                        if (entry.name not in ("objects", "staging") or not stat.S_ISDIR(info.st_mode)
                                or info.st_uid != os.geteuid() or info.st_mode & 0o077):
                            raise ValueError("attachment_layout_unknown")
                        found.add(entry.name)
                        skipped[directory / entry.name] = _signature(info)
                if found != {"objects", "staging"}:
                    raise ValueError("attachment_layout_unknown")
            finally:
                os.close(fd)
            known[catalog_path] = _signature(catalog_path.lstat())
        elif os.path.lexists(str(root / "attachments")):
            raise ValueError("attachment_catalog_unavailable")
        if delivery_path in before:
            if delivery.get("schema_version") != 1 or delivery.get("status") != "known":
                raise ValueError("attachment_delivery_unknown")
            known[delivery_path] = _signature(delivery_path.lstat())
        for path, original in before.items():
            if _signature(path.lstat()) != original:
                raise ValueError("attachment_state_changed")
            for suffix in ("-journal", "-wal", "-shm"):
                if os.path.lexists(str(path) + suffix):
                    raise ValueError("attachment_sidecar")
        summary["status"] = "planned"
        return summary, known, skipped
    except (OSError, ValueError, TypeError, sqlite3.Error, RecursionError):
        summary.update(status="blocked", reason="attachment_state_unknown")
        raise _AttachmentBlocked(summary) from None


def _attachment_apply_guard(base, worker_roots):
    roots = [Path(base)] + [Path(path) for path in worker_roots]
    if any(_attachment_present(root) for root in roots):
        return True
    config = Path(base) / "state/backends.json"
    if os.path.lexists(str(config)):
        fd = os.open(str(config), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
        try:
            if not _regular(fd) or os.fstat(fd).st_size > MAX_JSON_BYTES:
                raise ValueError("reference_file_invalid")
            value = json.loads(os.read(fd, MAX_JSON_BYTES + 1))
        finally:
            os.close(fd)
        if not isinstance(value, dict):
            raise ValueError("reference_file_invalid")
        external = value.get("legacy_state_dir")
        if external:
            if not isinstance(external, str) or not os.path.isabs(external):
                raise ValueError("reference_file_invalid")
            if _attachment_present(external):
                return True
    return False


class MediaRetention:
    def __init__(self, base, *, ttl_seconds=7*86400, quota_bytes=512 << 20,
                 min_age_seconds=86400, worker_roots=(), clock=time.time):
        # Pure configuration: not even stat/mkdir/open in the constructor.
        for value in (ttl_seconds, quota_bytes, min_age_seconds):
            if type(value) is not int or value < 0:
                raise ValueError("retention_limit_invalid")
        if ttl_seconds < min_age_seconds:
            raise ValueError("retention_limit_invalid")
        self.base, self.ttl, self.quota = base, ttl_seconds, quota_bytes
        self.min_age, self.worker_roots, self.clock = min_age_seconds, tuple(worker_roots), clock

    @staticmethod
    def _blocked(reason, dry_run):
        return {"dry_run": dry_run, "status": "blocked", "reason": reason,
                "files": None, "bytes": None, "eligible_files": None,
                "eligible_bytes": None, "deleted_files": 0, "deleted_bytes": 0}

    def _scan(self):
        root = _checked_root(self.base)
        media = root / "media"
        if not media.is_dir() or media.is_symlink():
            raise ValueError("media_root_invalid")
        now = self.clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            raise ValueError("clock_invalid")
        refs, metadata, candidates = set(), [0, 0, 0], []

        def values(obj, depth=0, field=None):
            metadata[2] += 1
            if metadata[2] > MAX_VALUES or depth > 16:
                raise ValueError("reference_budget_exceeded")
            if isinstance(obj, dict):
                for key, value in obj.items():
                    values(value, depth + 1, key)
            elif isinstance(obj, list):
                for value in obj:
                    values(value, depth + 1, field)
            elif isinstance(obj, str):
                if obj.startswith(("/", "~")):
                    refs.add(os.path.realpath(os.path.expanduser(obj)))
                elif obj.startswith(("media/", "./media/")):
                    refs.add(os.path.realpath(str(root / obj)))
                elif field in ("media_path", "media_paths", "path"):
                    # Relative paths depend on a caller's cwd. Do not guess
                    # which runtime media file an old/private writer meant.
                    raise ValueError("reference_path_ambiguous")

        def read_json(path):
            fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
            try:
                if not _regular(fd) or os.fstat(fd).st_size > MAX_JSON_BYTES:
                    raise ValueError("reference_file_invalid")
                raw = os.read(fd, MAX_JSON_BYTES + 1)
            finally:
                os.close(fd)
            metadata[1] += len(raw)
            if len(raw) > MAX_JSON_BYTES or metadata[1] > MAX_METADATA_BYTES:
                raise ValueError("reference_budget_exceeded")
            obj = json.loads(raw)
            values(obj)
            return obj

        roots = [root]
        config = root / "state" / "backends.json"
        if config.exists():
            obj = read_json(config)
            if not isinstance(obj, dict):
                raise ValueError("reference_file_invalid")
            external = obj.get("legacy_state_dir")
            if external:
                roots.append(_checked_root(external))
        roots.extend(_checked_root(path) for path in self.worker_roots)
        attachment_sources, recognized, attachment_directories = [], {}, {}
        for source_root in dict.fromkeys(roots):
            summary, known, skipped = _attachment_preview(source_root, self.clock)
            attachment_sources.append(summary)
            recognized.update(known)
            attachment_directories.update(skipped)
        attachment_report = dict(attachment_sources[0], other_roots=attachment_sources[1:])
        visited = set()
        database = root / "state" / "backend_jobs.sqlite3"

        def bounded_files(directory, exclude_media=False, parent=None, name=None, depth=0):
            if depth > 64:
                raise ValueError("reference_budget_exceeded")
            fd = os.open(str(directory) if parent is None else name,
                         os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                with os.scandir(fd) as entries:
                    for entry in entries:
                        # Stop while enumerating, before any whole-directory
                        # list/sort can allocate beyond the configured budget.
                        metadata[0] += 1
                        if metadata[0] > MAX_FILES:
                            raise ValueError("reference_budget_exceeded")
                        path = directory / entry.name
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            if exclude_media and path == media:
                                continue
                            if path in attachment_directories:
                                if _signature(info) != attachment_directories[path]:
                                    raise ValueError("attachment_state_changed")
                                continue
                            if entry.name == ".git":
                                raise ValueError("reference_repository_unknown")
                            yield from bounded_files(path, exclude_media, fd, entry.name, depth+1)
                        elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                            yield path, info
                        else:
                            raise ValueError("reference_file_invalid")
            finally:
                os.close(fd)

        for source_root in roots:
            for path, info in bounded_files(source_root, exclude_media=True):
                name = path.name
                if path in visited:
                    continue
                visited.add(path)
                if path in recognized:
                    if _signature(info) != recognized[path]:
                        raise ValueError("attachment_state_changed")
                    continue
                fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    prefix = os.read(fd, 16)
                finally:
                    os.close(fd)
                if (prefix == b"SQLite format 3\x00" or name.endswith((".sqlite3", ".sqlite", ".db"))) and path != database:
                    raise ValueError("unknown_reference_database")
                if name.endswith(".json"):
                    read_json(path)
                elif name.endswith(("-wal", "-shm", "-journal")):
                    raise ValueError("database_active")
                elif name.startswith((".tmp-", ".media-")):
                    raise ValueError("reference_staging")
        if database.exists():
            if database.is_symlink() or database.stat().st_size > MAX_METADATA_BYTES * 4:
                raise ValueError("reference_symlink")
            # immutable+ro does not create journal/WAL/SHM. Existing sidecars
            # were rejected above rather than silently ignoring their records.
            uri = "file:%s?mode=ro&immutable=1" % urllib.parse.quote(str(database), safe="/")
            with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=0.05)) as db:
                tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables != set(DB_COLUMNS) or db.execute("SELECT 1 FROM sqlite_master WHERE type IN ('view','trigger') LIMIT 1").fetchone():
                    raise ValueError("unknown_reference_database")
                for table, columns in DB_COLUMNS.items():
                    actual = tuple((row[1], row[2]) for row in db.execute('PRAGMA table_info("%s")' % table))
                    if actual != columns:
                        raise ValueError("unknown_reference_database")
                    for row in db.execute('SELECT * FROM "%s"' % table):
                        metadata[0] += 1
                        if metadata[0] > MAX_FILES:
                            raise ValueError("reference_budget_exceeded")
                        for (column, _), raw in zip(columns, row):
                            if raw is None:
                                continue
                            if type(raw) is int:
                                continue
                            if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_JSON_BYTES:
                                raise ValueError("reference_file_invalid")
                            metadata[1] += len(raw.encode("utf-8"))
                            if metadata[1] > MAX_METADATA_BYTES:
                                raise ValueError("reference_budget_exceeded")
                            if column in ("payload", "record") or table == "metadata" and column == "value":
                                values(json.loads(raw))
                            else:
                                values(raw)
        total, media_count = 0, 0
        for path, info in bounded_files(media):
            total += info.st_size
            media_count += 1
            # All dot/staging files are pins, regardless of age or quota.
            if any(part.startswith(".") for part in path.relative_to(media).parts):
                continue
            if str(path) in refs or os.path.realpath(str(path)) in refs:
                continue
            age = now - info.st_mtime
            if age >= self.min_age:
                candidates.append((path, info, age >= self.ttl))
        chosen, reclaim = [], 0
        for path, info, expired in sorted(candidates, key=lambda row: (row[1].st_mtime, str(row[0]))):
            if expired or total - reclaim > self.quota:
                chosen.append((path, info))
                reclaim += info.st_size
        report = {"dry_run": True, "status": "planned", "reason": "ok",
                  "files": media_count, "scanned_entries": metadata[0], "bytes": total, "eligible_files": len(chosen),
                  "eligible_bytes": reclaim, "deleted_files": 0, "deleted_bytes": 0,
                  "remaining_bytes": total, "quota_bytes": self.quota,
                  "quota_unreclaimable_bytes": max(0, total - reclaim - self.quota),
                  "attachment_inbox": attachment_report}
        return root, media, chosen, report

    def run(self, *, apply=False, offline_maintenance=False):
        if type(apply) is not bool or type(offline_maintenance) is not bool:
            return self._blocked("maintenance_required", True)
        if not apply:
            try:
                return self._scan()[3]
            except _AttachmentBlocked as error:
                return dict(self._blocked("reference_or_root_unknown", True), attachment_inbox=error.summary)
            except (OSError, ValueError, TypeError, sqlite3.Error, RecursionError):
                return self._blocked("reference_or_root_unknown", True)
        try:
            if _attachment_apply_guard(self.base, self.worker_roots):
                return self._blocked("attachment_delete_disabled", False)
        except (OSError, ValueError, TypeError, RecursionError):
            return self._blocked("reference_or_root_unknown", False)
        if not offline_maintenance:
            return self._blocked("maintenance_required", False)
        handles, names, report = [], [], None
        try:
            root = _checked_root(self.base)
            state = root / "state"
            if not state.is_dir() or state.is_symlink():
                raise ValueError("state_invalid")
            for name in (PIN_NAME, "serve.lock", "message_receipts.lock", "backend-worker.lock"):
                fd = os.open(str(state / name), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                handles.append(fd)
                names.append(state / name)
                if not _regular(fd):
                    raise ValueError("lock_invalid")
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _check_client_pins(state)
            if _attachment_apply_guard(self.base, self.worker_roots):
                return self._blocked("attachment_delete_disabled", False)
            _, media, selected, report = self._scan()
            report["dry_run"] = False
            # Re-read all references and identities under the same exclusion.
            _, _, confirmed, _ = self._scan()
            expected = {str(p): s for p, s in selected}
            actual = {str(p): s for p, s in confirmed}
            if expected != actual:
                raise ValueError("reference_race")
            if _attachment_apply_guard(self.base, self.worker_roots):
                return self._blocked("attachment_delete_disabled", False)
            for path, info in selected:
                if _attachment_apply_guard(self.base, self.worker_roots):
                    if report["deleted_files"]:
                        report.update(status="partial", reason="attachment_delete_disabled",
                                      remaining_bytes=report["bytes"]-report["deleted_bytes"])
                        return report
                    return self._blocked("attachment_delete_disabled", False)
                if any(os.stat(str(p), follow_symlinks=False).st_ino != os.fstat(fd).st_ino
                       for p, fd in zip(names, handles)):
                    raise ValueError("lock_replaced")
                # Open every component relative to a checked directory FD.
                parent = os.open(str(media), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    for part in path.relative_to(media).parts[:-1]:
                        child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                        os.close(parent)
                        parent = child
                    current = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
                    if current != info or not stat.S_ISREG(current.st_mode) or current.st_nlink != 1:
                        raise ValueError("media_race")
                    os.unlink(path.name, dir_fd=parent)
                    report["deleted_files"] += 1
                    report["deleted_bytes"] += current.st_size
                finally:
                    os.close(parent)
            report.update(status="applied", remaining_bytes=report["bytes"]-report["deleted_bytes"])
            return report
        except (OSError, ValueError, TypeError, sqlite3.Error, RecursionError):
            if report is not None and report["deleted_files"]:
                report.update(status="partial", reason="busy_or_reference_unknown",
                              remaining_bytes=report["bytes"]-report["deleted_bytes"])
                return report
            return self._blocked("busy_or_reference_unknown", False)
        finally:
            for fd in reversed(handles):
                os.close(fd)
