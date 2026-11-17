"""Private maintenance records for outbox dead letters."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
import re
import stat
import uuid

from .store import safe_message_id


_NOTICE_PREFIX = "dead-letter:"
_JOURNAL = "dead_letter_reviews.json"
_MAX_JOURNAL_BYTES = 16 << 20
_MAX_FAILURE_METADATA_BYTES = 1 << 20
_SHA256 = re.compile(r"[0-9a-f]{64}")


@contextmanager
def _directory(path, parent=None, private=False):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & (0o077 if private else 0o022):
            raise ValueError("unsafe_dead_letter_directory")
        yield fd
    finally:
        os.close(fd)


def _file_stamp(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


@contextmanager
def _private_file(directory, name):
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    fd = os.open(name, flags, dir_fd=directory)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or info.st_mode & 0o077):
            raise ValueError("unsafe_dead_letter_file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            yield stream, info
        try:
            current = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            raise ValueError("dead_letter_file_changed") from None
        if _file_stamp(current) != _file_stamp(info):
            raise ValueError("dead_letter_file_changed")
    finally:
        os.close(fd)


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("invalid_dead_letter_journal")
        result[key] = value
    return result


def _load_journal(state):
    try:
        with _private_file(state, _JOURNAL) as (stream, info):
            if info.st_size > _MAX_JOURNAL_BYTES:
                raise ValueError("dead_letter_journal_full")
            data = stream.read(_MAX_JOURNAL_BYTES + 1)
    except FileNotFoundError:
        return {}
    document = json.loads(data, object_pairs_hook=_unique_pairs)
    if (not isinstance(document, dict) or type(document.get("schema_version")) is not int
            or document["schema_version"] != 1 or not isinstance(document.get("records"), dict)):
        raise ValueError("invalid_dead_letter_journal")
    return document["records"]


def _save_journal(state, rows):
    data = json.dumps({"schema_version": 1, "records": rows}, allow_nan=False).encode("utf-8")
    if len(data) > _MAX_JOURNAL_BYTES:
        raise ValueError("dead_letter_journal_full")
    name = ".dead-letter-" + uuid.uuid4().hex
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=state)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, _JOURNAL, src_dir_fd=state, dst_dir_fd=state)
        os.fsync(state)
    finally:
        try:
            os.unlink(name, dir_fd=state)
        except FileNotFoundError:
            pass


@contextmanager
def _journal(base, *, write=False):
    # A lock on the pinned state directory serializes the Hook and the local
    # maintenance CLI, including callers that use different Hook state dirs.
    with _directory(base) as root, _directory("state", root, private=True) as state:
        fcntl.flock(state, fcntl.LOCK_EX if write else fcntl.LOCK_SH)
        yield root, state, _load_journal(state)


@contextmanager
def _failed(root):
    with _directory("outbox", root) as outbox, _directory("failed", outbox, private=True) as failed:
        yield failed


def _failure_snapshot(failed, name, *, stamps=None, read_outcome=False):
    if (type(name) is not str or not name.endswith(".json")
            or safe_message_id(name[:-5]) + ".json" != name):
        raise ValueError("invalid_dead_letter_name")
    with _private_file(failed, name) as (stream, info):
        stamp = _file_stamp(info)
        # Only first-notice planning uses the index of previously hashed files.
        # The open private fd and its current directory entry are still checked.
        if not read_outcome and stamps is not None and (name, stamp) in stamps:
            return stamps[(name, stamp)], stamp, None
        digest = hashlib.sha256()
        remaining = info.st_size
        data = bytearray() if read_outcome and remaining <= _MAX_FAILURE_METADATA_BYTES else None
        while remaining:
            chunk = stream.read(min(remaining, 65536))
            if not chunk:
                raise ValueError("dead_letter_file_changed")
            digest.update(chunk)
            if data is not None:
                data.extend(chunk)
            remaining -= len(chunk)
    return digest.hexdigest(), stamp, _sender_evidence(data)


def _reject_nonfinite(_):
    raise ValueError("invalid_dead_letter_metadata")


def _sender_evidence(data):
    if data is not None:
        try:
            body = json.loads(data, object_pairs_hook=_unique_pairs, parse_constant=_reject_nonfinite)
            if isinstance(body, dict) and body.get("delivery_outcome") in ("proven_unsent", "unknown"):
                prefix = body.get("chunks_done")
                if type(prefix) is not int or prefix < 0 or body.get("media_path"):
                    prefix = None
                return body["delivery_outcome"], prefix
        except (ValueError, RecursionError):
            pass
    return None


def _known_stamps(rows):
    stamps, conflicts = {}, set()
    for identity, row in rows.items():
        if not isinstance(row, dict):
            continue
        name, stamp, sha256 = row.get("file"), row.get("file_stamp"), row.get("sha256")
        if (not isinstance(name, str) or not name.isascii()
                or not isinstance(stamp, list) or len(stamp) != 5
                or any(type(value) is not int for value in stamp)
                or not isinstance(sha256, str) or not _SHA256.fullmatch(sha256)
                or identity != _key(name, sha256)):
            continue
        key = (name, tuple(stamp))
        if key in stamps and stamps[key] != sha256:
            conflicts.add(key)
        stamps[key] = sha256
    for key in conflicts:
        del stamps[key]
    return stamps


def _key(name, sha256):
    return hashlib.sha256((name + "\0" + sha256).encode("ascii")).hexdigest()


def _record(rows, name, sha256):
    key = _key(name, sha256)
    if key not in rows:
        return {"file": name, "sha256": sha256, "notice_state": "unplanned"}
    row = rows[key]
    if (not isinstance(row, dict) or row.get("file") != name or row.get("sha256") != sha256
            or row.get("notice_state") not in ("unplanned", "planned", "legacy_attempted")):
        raise ValueError("invalid_dead_letter_journal")
    if ("runtime_accepted" in row and type(row["runtime_accepted"]) is not bool
            or ("decision" in row) != ("decided_at" in row)
            or ("decision" in row and row["decision"] not in ("received", "discarded"))):
        raise ValueError("invalid_dead_letter_journal")
    for name in ("planned_at", "decided_at"):
        if name in row and (type(row[name]) not in (int, float)
                            or not math.isfinite(row[name]) or row[name] < 0):
            raise ValueError("invalid_dead_letter_journal")
    return row


def _view(row, sender_evidence):
    sender_outcome, prefix = sender_evidence if sender_evidence is not None else (None, None)
    if row.get("decision") == "received":
        outcome, evidence = "delivered", "human_confirmation"
    elif sender_outcome is not None:
        outcome, evidence = sender_outcome, "sender_failure_record"
    else:
        outcome, evidence = "unknown", "unverified"
    return {
        "schema_version": 1,
        "file": row["file"],
        "sha256": row["sha256"],
        "delivery_outcome": outcome,
        "delivery_evidence_source": evidence,
        "accepted_text_prefix_chunks": prefix,
        "delivery_outcome_scope": "unresolved_suffix" if prefix and outcome != "delivered" else "letter",
        "notice_state": row["notice_state"],
        "planned_at": row.get("planned_at"),
        "runtime_accepted": row.get("runtime_accepted"),
        "review_state": "closed" if "decision" in row else "open",
        "decision": row.get("decision"),
        "decided_at": row.get("decided_at"),
    }


def plan_first_notices(base, legacy, now):
    """Record each first plan before returning its existing Hook payload keys."""
    eligible, migrated = [], set()
    with _journal(base, write=True) as (root, state, rows), _failed(root) as failed:
        migrated_names = {row["file"] for row in rows.values()
                          if isinstance(row, dict) and row.get("notice_state") == "legacy_attempted"
                          and isinstance(row.get("file"), str)}
        changed = False
        stamps = _known_stamps(rows)
        for name in sorted(os.listdir(failed)):
            try:
                sha256, stamp, _ = _failure_snapshot(failed, name, stamps=stamps)
            except (OSError, ValueError):
                continue
            row = _record(rows, name, sha256)
            key, old_key = _key(name, sha256), "failed:" + name
            if old_key in legacy:
                migrated.add(old_key)
            if row.get("file_stamp") != list(stamp):
                row["file_stamp"] = list(stamp)
                rows[key] = row
                changed = True
            if row["notice_state"] != "unplanned" or "decision" in row:
                continue
            old_row = legacy.get(old_key)
            old_attempts = old_row.get("attempts") if isinstance(old_row, dict) else None
            if type(old_attempts) is int and old_attempts > 0 and name not in migrated_names:
                row["notice_state"] = "legacy_attempted"
            else:
                row.update(notice_state="planned", planned_at=now)
                eligible.append((name, _NOTICE_PREFIX + key))
            rows[key] = row
            changed = True
        if changed:
            _save_journal(state, rows)
    return eligible, migrated


def record_notice_result(base, keys, accepted):
    """Record the first observed wake response separately from the review."""
    keys = [key[len(_NOTICE_PREFIX):] for key in keys if key.startswith(_NOTICE_PREFIX)]
    if not keys:
        return
    with _journal(base, write=True) as (_, state, rows):
        changed = False
        for key in keys:
            row = rows.get(key)
            if (isinstance(row, dict) and row.get("notice_state") == "planned"
                    and "runtime_accepted" not in row):
                row["runtime_accepted"] = bool(accepted)
                changed = True
        if changed:
            _save_journal(state, rows)


def status(base, name):
    with _journal(base) as (root, _, rows), _failed(root) as failed:
        sha256, _, sender_evidence = _failure_snapshot(failed, name, read_outcome=True)
        return _view(_record(rows, name, sha256), sender_evidence)


def reconcile(base, name, sha256, decision, now):
    if type(sha256) is not str or not _SHA256.fullmatch(sha256):
        raise ValueError("invalid_dead_letter_sha256")
    if decision not in ("received", "discarded"):
        raise ValueError("invalid_dead_letter_decision")
    with _journal(base, write=True) as (root, state, rows), _failed(root) as failed:
        actual, stamp, sender_evidence = _failure_snapshot(failed, name, read_outcome=True)
        if actual != sha256:
            raise ValueError("dead_letter_digest_mismatch")
        row = _record(rows, name, sha256)
        if "decision" in row:
            if row["decision"] != decision:
                raise ValueError("dead_letter_already_resolved")
        else:
            row.update(decision=decision, decided_at=now, file_stamp=list(stamp))
            rows[_key(name, sha256)] = row
            _save_journal(state, rows)
        return _view(row, sender_evidence)
