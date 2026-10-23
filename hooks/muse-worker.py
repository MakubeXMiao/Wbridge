#!/usr/bin/env python3
"""Muse-only structured adapter for the existing WorkerState protocol.

Use the deployed bridge interpreter and explicit --base / --state-dir paths.
JSON stdin: begin(action, lease); refresh/release(action, handle);
reply(action, handle, revision, text?, media_path?). Paths cannot come from JSON.
The model receives a handle and batch revision, never the private worker lease.
There is no implicit renewal, retry, external-tool execution or cancellation.
An unknown reply result closes only this adapter handle. It does not change
the core recovery contract or guarantee action idempotency for a new worker;
external reminders/tools need their own durable action identities and results.

Exit codes: 0 success, 2 invalid input/configuration, 3 must stop,
4 needs_refresh/revision mismatch. stdout is one JSON object; errors are fixed
categories and never contain exception text. Requires Python 3.9 or later.
"""
import argparse
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid

try:
    from wechat_bridge.worker_state import WorkerState
    from wechat_bridge.store import safe_message_id
except Exception:
    # A missing/mismatched deployment must fail as a fixed category instead of
    # printing an import traceback or falling back to another package location.
    WorkerState = None
    safe_message_id = None


MAX_INPUT_BYTES = 1024 * 1024
HANDLE_RE = re.compile(r"[0-9a-f]{32}\Z")
LEASE_RE = re.compile(r"[A-Za-z0-9_-]{1,256}\Z")
SCHEMA = 1
MESSAGE_FIELDS = ("message_id", "text", "media_paths", "media_failed",
                  "media_failed_count", "create_time_ms", "received_at")
TIME_FIELDS = ("expires_at", "hard_deadline", "renew_after_seconds")


class InvalidRequest(Exception):
    pass


class SessionError(Exception):
    def __init__(self, code):
        self.code = code


def failure(code, **fields):
    return dict(ok=False, error=code,
                must_stop=code not in ("needs_refresh", "revision_mismatch"),
                **fields)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidRequest()
        result[key] = value
    return result


def _reject_constant(value):
    raise InvalidRequest()


def parse_request(stream):
    raw = stream.read(MAX_INPUT_BYTES + 1)
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_INPUT_BYTES:
        raise InvalidRequest()
    try:
        return json.loads(raw, object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
    except (ValueError, TypeError, RecursionError):
        raise InvalidRequest() from None


def validate_request(request):
    if not isinstance(request, dict):
        raise InvalidRequest()
    action = request.get("action")
    if action not in ("begin", "refresh", "reply", "release"):
        raise InvalidRequest()
    required = {"action", "lease"} if action == "begin" else {"action", "handle"}
    allowed = set(required)
    if action == "reply":
        required.add("revision")
        allowed.update(("revision", "text", "media_path"))
    if not required.issubset(request) or set(request) - allowed:
        raise InvalidRequest()
    if action == "begin":
        if not isinstance(request["lease"], str) or not LEASE_RE.fullmatch(request["lease"]):
            raise InvalidRequest()
    elif not isinstance(request["handle"], str) or not HANDLE_RE.fullmatch(request["handle"]):
        raise InvalidRequest()
    if action == "reply":
        revision = request["revision"]
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise InvalidRequest()
        text = request.get("text", "")
        media = request.get("media_path")
        if not isinstance(text, str):
            raise InvalidRequest()
        if "media_path" in request and (not isinstance(media, str) or not media or not Path(media).is_absolute()):
            raise InvalidRequest()
        if not text.strip() and not media:
            raise InvalidRequest()
    return action


def _trusted_paths(base, state_dir):
    if (not isinstance(base, str) or not isinstance(state_dir, str)
            or not Path(base).is_absolute() or not Path(state_dir).is_absolute()):
        raise SessionError("invalid_configuration")
    return str(Path(base).resolve()), str(Path(state_dir).resolve())


def _adapter_dir(state_dir):
    directory = Path(state_dir) / "muse-adapter"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or not directory.is_dir():
        raise SessionError("invalid_configuration")
    directory.chmod(0o700)
    return directory


@contextmanager
def _session_lock(directory, handle):
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(str(directory / (handle + ".lock")), flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _save(directory, handle, session):
    fd, name = tempfile.mkstemp(prefix="." + handle + "-", dir=str(directory))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(session, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, str(directory / (handle + ".json")))
        directory_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


def _load(directory, handle, base, state_dir):
    path = directory / (handle + ".json")
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        raise SessionError("unknown_handle") from None
    try:
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            session = json.load(stream)
        if (not isinstance(session, dict) or session.get("schema") != SCHEMA
                or session.get("base") != base or session.get("state_dir") != state_dir
                or not isinstance(session.get("lease"), str)
                or not LEASE_RE.fullmatch(session["lease"])
                or not isinstance(session.get("revision"), int)
                or isinstance(session["revision"], bool) or session["revision"] < 1
                or not isinstance(session.get("owned"), list)
                or not session["owned"]
                or len(set(session["owned"])) != len(session["owned"])
                or any(not isinstance(mid, str) or safe_message_id(mid) != mid
                       for mid in session["owned"])):
            raise ValueError()
        return session
    except (ValueError, TypeError, KeyError, RecursionError):
        raise SessionError("invalid_session") from None


def _emit_safe_traces(raw, stream):
    # Preserve phase observability while discarding all arbitrary helper output.
    for line in raw.splitlines():
        if not line.startswith("latency "):
            continue
        try:
            record = json.loads(line[8:])
            if record.get("stage") not in ("adopted", "renewed", "enqueued"):
                continue
            clean = {"stage": record["stage"]}
            for key in ("at", "count", "worker_started_at", "wake_planned_at",
                        "expires_at", "enqueued_at", "received_at"):
                value = record.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                    clean[key] = value
            ids = record.get("message_ids")
            if isinstance(ids, list) and all(isinstance(mid, str) and safe_message_id(mid) == mid for mid in ids):
                clean["message_ids"] = ids
            print("latency " + json.dumps(clean, ensure_ascii=False), file=stream)
        except (ValueError, TypeError, AttributeError):
            continue


def _invoke(method, *args):
    errors, original_stderr = io.StringIO(), sys.stderr
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(errors):
            return method(*args)
    finally:
        _emit_safe_traces(errors.getvalue(), original_stderr)


def _snapshot(session, batch):
    owned, messages = batch.get("owned"), batch.get("messages")
    if (batch.get("lease") != session["lease"] or not isinstance(owned, list)
            or not owned or len(set(owned)) != len(owned)
            or not isinstance(messages, list)
            or any(not isinstance(mid, str) or safe_message_id(mid) != mid for mid in owned)
            or any(not isinstance(message, dict) for message in messages)
            or [message.get("message_id") for message in messages] != owned):
        raise SessionError("invalid_state_result")
    clean = []
    for message in messages:
        if ("text" in message and not isinstance(message["text"], str)
                or "media_paths" in message and
                (not isinstance(message["media_paths"], list)
                 or any(not isinstance(path, str) for path in message["media_paths"]))):
            raise SessionError("invalid_state_result")
        item = {"message_id": message["message_id"]}
        for key in MESSAGE_FIELDS[1:]:
            if key not in message:
                continue
            value = message[key]
            if key in ("create_time_ms", "received_at") and value is None:
                item[key] = None
                continue
            if key not in ("text", "media_paths") and (
                    not isinstance(value, (int, float, bool)) or not math.isfinite(value)):
                raise SessionError("invalid_state_result")
            item[key] = value
        clean.append(item)
    session["owned"] = owned
    session["revision"] = session.get("revision", 0) + 1
    session["batch_digest"] = hashlib.sha256(json.dumps(clean, ensure_ascii=False,
                                                        sort_keys=True).encode("utf-8")).hexdigest()
    for key in TIME_FIELDS:
        value = batch.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise SessionError("invalid_state_result")
        session[key] = value
    return dict(messages=clean, owned_count=len(owned), revision=session["revision"],
                **{key: session[key] for key in TIME_FIELDS})


def _uncertain(directory, handle, session):
    session["phase"] = "uncertain"
    try:
        _save(directory, handle, session)
    except OSError:
        pass  # The pre-call 'replying' marker also forbids automatic replay.
    return failure("outcome_unknown")


def _dispatch(request, base, state_dir, state_factory):
    action = request["action"]
    directory = _adapter_dir(state_dir)
    handle = uuid.uuid4().hex if action == "begin" else request["handle"]
    with _session_lock(directory, handle):
        if action == "begin":
            state = state_factory(base, state_dir)
            batch = _invoke(state.begin, request["lease"])
            if not batch.get("owned"):
                return failure("lease_lost")
            session = {"schema": SCHEMA, "base": base, "state_dir": state_dir,
                       "lease": batch["lease"], "phase": "active"}
            fields = _snapshot(session, batch)
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, **fields)

        session = _load(directory, handle, base, state_dir)
        phase = session.get("phase")
        if phase in ("replying", "uncertain", "releasing"):
            return failure("outcome_unknown")
        if phase == "lease_lost":
            return failure("lease_lost")
        if phase not in ("active", "needs_refresh"):
            return failure("session_closed")
        if action == "reply" and request["revision"] != session["revision"]:
            return failure("revision_mismatch", revision=session["revision"], requires_refresh=True)
        if action == "reply" and phase == "needs_refresh":
            return failure("needs_refresh", revision=session["revision"], requires_refresh=True)
        state = state_factory(base, state_dir)
        if action == "refresh":
            batch = _invoke(state.refresh, session["lease"])
            if batch.get("error") == "lease_lost" or not batch.get("owned"):
                session["phase"] = "lease_lost"
                _save(directory, handle, session)
                return failure("lease_lost")
            fields = _snapshot(session, batch)
            session["phase"] = "active"
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, **fields)
        if action == "release":
            session["phase"] = "releasing"
            _save(directory, handle, session)
            _invoke(state.release, session["lease"])
            session["phase"] = "released"
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, released=True, external_tools_cancelled=False)

        # Persist the attempt BEFORE calling core. A crash or unknown result
        # must never silently retry or release this session on the next call.
        session["phase"] = "replying"
        _save(directory, handle, session)
        try:
            result = _invoke(state.reply, session["owned"], request.get("text", ""),
                             session["lease"], request.get("media_path"))
            if result.get("error") == "needs_refresh":
                fields = _snapshot(session, result)
                session["phase"] = "needs_refresh"
                _save(directory, handle, session)
                return dict(failure("needs_refresh", handle=handle, requires_refresh=True), **fields)
            if (not isinstance(result.get("queued"), str)
                    or result.get("covers_message_ids") != session["owned"]):
                return _uncertain(directory, handle, session)
            session["phase"] = "queued"
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, revision=session["revision"],
                        queued=result["queued"], owned_count=len(session["owned"]),
                        delivery_confirmed=False)
        except ValueError as error:
            # These exact core errors occur before publication. Never emit the
            # source text, which could include identifiers or future secrets.
            if str(error).startswith("lease lost for ") or str(error) == "lease lost before publication":
                session["phase"] = "lease_lost"
                _save(directory, handle, session)
                return failure("lease_lost")
            return _uncertain(directory, handle, session)
        except Exception:
            return _uncertain(directory, handle, session)


def handle_request(request, base, state_dir, state_factory=None):
    try:
        validate_request(request)
        if WorkerState is None or safe_message_id is None:
            return failure("invalid_configuration")
        base, state_dir = _trusted_paths(base, state_dir)
        return _dispatch(request, base, state_dir, state_factory or WorkerState)
    except InvalidRequest:
        return failure("invalid_request")
    except SessionError as error:
        return failure(error.code)
    except Exception:
        return failure("adapter_state_error")


class QuietParser(argparse.ArgumentParser):
    def error(self, message):
        raise InvalidRequest()


def main(argv=None):
    parser = QuietParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--state-dir", required=True)
    try:
        args = parser.parse_args(argv)
        request = parse_request(sys.stdin)
        result = handle_request(request, args.base, args.state_dir)
    except (InvalidRequest, UnicodeError):
        result = failure("invalid_request")
    except Exception:
        result = failure("adapter_state_error")
    print(json.dumps(result, ensure_ascii=False))
    if result["ok"]:
        return 0
    if result["error"] in ("invalid_request", "invalid_configuration"):
        return 2
    if not result["must_stop"]:
        return 4
    return 3


if __name__ == "__main__":
    sys.exit(main())
