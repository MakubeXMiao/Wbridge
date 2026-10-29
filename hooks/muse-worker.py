#!/usr/bin/env python3
"""Muse-only structured adapter for the existing WorkerState protocol.

Use the deployed bridge interpreter and explicit --base / --state-dir paths.
JSON stdin: begin(action, lease); refresh/release(action, handle);
reply(action, handle, revision, text?, media_path?). Deployment paths are trusted CLI inputs.
attachment(action, handle, revision, request) wraps the frozen attachment wire
surface. A material path is a read candidate, never proof the model read pixels.
The model receives a handle and batch revision, never the private worker lease.
There is no implicit renewal, retry, external-tool execution or cancellation.
An unknown reply result closes only this adapter handle. It does not change
the core recovery contract or guarantee action idempotency for a new worker;
external reminders/tools need their own durable action identities and results.

Exit codes: 0 success, 2 invalid input/configuration, 3 must stop,
4 nonterminal business errors/needs_refresh/revision mismatch. stdout is one JSON object; errors are fixed
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
import time
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
                  "media_failed_count", "media_diagnostics", "create_time_ms", "received_at")
TIME_FIELDS = ("expires_at", "hard_deadline", "renew_after_seconds")
# This adapter also runs with older public packages. Keep validation of the
# optional V1 transport metadata here instead of importing a new core module.
MEDIA_DIAGNOSTIC_SOURCES = frozenset(("param", "full_url", "unknown"))
MEDIA_DIAGNOSTIC_REASONS = frozenset((
    "ok", "request_or_http_error", "http_body_read_failed", "ciphertext_too_large",
    "empty_body", "ciphertext_not_block_aligned", "decrypt_no_candidate",
    "decrypt_ambiguous", "plaintext_too_large", "image_invalid",
    "image_resource_limit", "image_validation_timeout", "write_failed", "unknown",
))
MEDIA_DIAGNOSTIC_MAX_ITEMS = 16
MEDIA_DIAGNOSTIC_MAX_SCAN = 32
MEDIA_DIAGNOSTIC_MAX_BYTES = 4096
MEDIA_DIAGNOSTIC_MAX_DOWNLOADED = (100 << 20) + 33
ATTACHMENT_BYTES = 65536
ATTACHMENT_ACTIONS = frozenset(("query", "get", "reference", "release", "pin", "label", "cancel",
                                "reply", "continue", "status"))
ATTACHMENT_ERRORS = frozenset((
    "unsupported_version", "unsupported_action", "invalid_request", "response_too_large",
    "lease_lost", "needs_refresh", "epoch_mismatch", "not_found", "scope_mismatch",
    "attachment_unavailable", "snapshot_expired", "processing", "quota_exceeded",
    "storage_busy", "storage_invalid", "hold_forbidden", "group_unknown", "group_busy",
    "continue_required", "budget_exhausted", "cancelled", "expired"))
SLOT_STATES = frozenset(("pending", "ready", "failed", "quota", "missing"))
GROUP_STATES = frozenset(("queued", "sending", "partial", "awaiting_continue", "unknown",
                          "complete", "cancelled", "expired"))
ATTACHMENT_KINDS = frozenset(("image", "video", "file", "voice"))
DECRYPTION_STATES = frozenset(("pending", "unavailable", "protocol_decoded", "content_verified", "plaintext_verified"))
PREVIEW_STATES = frozenset(("readable", "unavailable", "invalid", "resource_limited"))
TAG_SOURCES = frozenset(("user", "batch_text", "original_name", "inferred"))
CAPABILITY_FEATURES = ("attachments_v1", "references_v1", "attachment_groups_v1")
LABEL_FEATURE = "attachment_labels_v1"
CANCEL_FEATURE = "attachment_cancel_v1"
CAPABILITY_LIMITS = {"request_bytes": ATTACHMENT_BYTES, "response_bytes": ATTACHMENT_BYTES,
                     "query_limit": 20, "attachment_ids": 16, "reference_ordinals": 16}


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
        request = json.loads(raw, object_pairs_hook=_unique_object,
                             parse_constant=_reject_constant)
        if (type(request) is dict and request.get("action") == "attachment"
                and len(raw.encode("utf-8")) > ATTACHMENT_BYTES):
            raise InvalidRequest()
        return request
    except (ValueError, TypeError, RecursionError):
        raise InvalidRequest() from None


def validate_request(request):
    if not isinstance(request, dict):
        raise InvalidRequest()
    action = request.get("action")
    if action not in ("begin", "refresh", "reply", "release", "attachment"):
        raise InvalidRequest()
    required = {"action", "lease"} if action == "begin" else {"action", "handle"}
    allowed = set(required)
    if action == "reply":
        required.add("revision")
        allowed.update(("revision", "text", "media_path"))
    if action == "attachment":
        required.update(("revision", "request"))
        allowed.update(("revision", "request"))
    if not required.issubset(request) or set(request) - allowed:
        raise InvalidRequest()
    if action == "begin":
        if not isinstance(request["lease"], str) or not LEASE_RE.fullmatch(request["lease"]):
            raise InvalidRequest()
    elif not isinstance(request["handle"], str) or not HANDLE_RE.fullmatch(request["handle"]):
        raise InvalidRequest()
    if action in ("reply", "attachment"):
        revision = request["revision"]
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise InvalidRequest()
    if action == "attachment":
        try:
            _attachment_request(request["request"])
            _attachment_size(request)
        except (SessionError, ValueError, TypeError, RecursionError):
            raise InvalidRequest() from None
    if action == "reply":
        text = request.get("text", "")
        media = request.get("media_path")
        if not isinstance(text, str):
            raise InvalidRequest()
        if "media_path" in request and (not isinstance(media, str) or not media or not Path(media).is_absolute()):
            raise InvalidRequest()
        if not text.strip() and not media:
            raise InvalidRequest()
    return action


def _attachment_size(value):
    if len(json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")) > ATTACHMENT_BYTES:
        raise SessionError("response_too_large")


def _fields(value, names):
    # Select first; never serialize or traverse unknown sensitive subtrees.
    if type(value) is not dict or any(name not in value for name in names):
        raise SessionError("invalid_state_result")
    return {name: value[name] for name in names}


def _string(value, nullable=False, chars=ATTACHMENT_BYTES, nonempty=True, byte_limit=ATTACHMENT_BYTES):
    if value is None and nullable:
        return value
    if (type(value) is not str or len(value) > chars or nonempty and not value
            or len(value.encode("utf-8")) > byte_limit or "\x00" in value):
        raise SessionError("invalid_state_result")
    return value


def _integer(value, low=0, high=(1 << 63) - 1):
    if type(value) is not int or not low <= value <= high:
        raise SessionError("invalid_state_result")
    return value


def _number(value, nullable=False):
    if value is None and nullable:
        return value
    if type(value) not in (int, float) or not math.isfinite(value):
        raise SessionError("invalid_state_result")
    return value


def _boolean(value):
    if type(value) is not bool:
        raise SessionError("invalid_state_result")
    return value


def _list(value, limit, convert, minimum=0):
    if type(value) is not list or not minimum <= len(value) <= limit:
        raise SessionError("invalid_state_result")
    return [convert(item) for item in value]


def _ids(value, limit=16, minimum=0):
    result = _list(value, limit, _string, minimum)
    if len(set(result)) != len(result):
        raise SessionError("invalid_state_result")
    return result


def _version(value):
    if type(value) is not int or value != 1:
        raise SessionError("unsupported_version")
    return value


def _capabilities(value):
    result = _fields(value, ("schema_version", "protocol", "version", "features", "limits"))
    _version(result["schema_version"])
    _version(result["version"])
    if type(result["protocol"]) is not str or result["protocol"] != "attachment_inbox_v1":
        raise SessionError("unsupported_version")
    features = _ids(result["features"], limit=16)
    if not set(CAPABILITY_FEATURES).issubset(features):
        raise SessionError("attachment_unavailable")
    result["features"] = list(CAPABILITY_FEATURES)
    for feature in (LABEL_FEATURE, CANCEL_FEATURE):
        if feature in features:
            result["features"].append(feature)
    result["limits"] = _fields(result["limits"], CAPABILITY_LIMITS)
    for key, expected in CAPABILITY_LIMITS.items():
        if type(result["limits"][key]) is not int or result["limits"][key] != expected:
            raise SessionError("unsupported_version")
    # This policy is evidence from the effective public configuration, never
    # an adapter default. Old/invalid policy disables only ambiguous inference.
    policy = value.get("policy")
    if type(policy) is dict:
        seconds = policy.get("ambiguity_seconds")
        if type(seconds) is int and seconds >= 0:
            try:
                _attachment_size(dict(result, policy={"ambiguity_seconds": seconds}))
            except (SessionError, ValueError):
                pass
            else:
                result["policy"] = {"ambiguity_seconds": seconds}
    return result


def _attachment_request(value):
    result = _fields(value, ("schema_version", "action", "arguments"))
    if set(value) != set(result):
        raise SessionError("invalid_request")
    _version(result["schema_version"])
    action = _string(result["action"])
    if action not in ATTACHMENT_ACTIONS or type(result["arguments"]) is not dict:
        raise SessionError("invalid_request")
    required = {"get": {"attachment_id"}, "reference": {"text_message_id", "snapshot_id"},
                "release": {"hold_id"}, "pin": {"attachment_id", "pinned"},
                "label": {"text_message_id", "label"},
                "cancel": {"text_message_id", "group_id"},
                "reply": {"attachment_ids"}, "continue": {"group_id"}}.get(action, set())
    optional = {"query": {"filters", "cursor", "limit"}, "reference": {"ordinals"},
                "label": {"snapshot_id", "ordinals", "attachment_ids"},
                "reply": {"caption"}, "status": {"group_id", "cursor", "limit"}}.get(action, set())
    args = result["arguments"]
    if not required.issubset(args) or set(args) - required - optional:
        raise SessionError("invalid_request")
    if action == "label" and (set(args) & optional) not in ({"snapshot_id", "ordinals"}, {"attachment_ids"}):
        raise SessionError("invalid_request")
    clean = {}
    for key, item in args.items():
        if key in ("attachment_id", "text_message_id", "snapshot_id", "hold_id", "group_id"):
            clean[key] = _string(item)
        elif key == "cursor":
            clean[key] = _string(item, nullable=True)
        elif key == "limit":
            clean[key] = _integer(item, 1, 20)
        elif key == "pinned":
            clean[key] = _boolean(item)
        elif key == "caption":
            clean[key] = _string(item, chars=16000, nonempty=False)
        elif key == "label":
            clean[key] = _string(item, chars=128)
        elif key == "attachment_ids":
            clean[key] = _ids(item, minimum=1)
        elif key == "ordinals":
            clean[key] = _list(item, 16, lambda n: _integer(n, 1, 16), minimum=1)
            if len(set(clean[key])) != len(clean[key]):
                raise SessionError("invalid_request")
        elif key == "filters":
            if type(item) is not dict or set(item) - {"received_after", "received_before", "name_contains", "tag", "kind", "status"}:
                raise SessionError("invalid_request")
            clean[key] = {name: (_number(field) if name.startswith("received_") else
                                 _string(field, chars=128, nonempty=False))
                          for name, field in item.items()}
            if ("kind" in clean[key] and clean[key]["kind"] not in ATTACHMENT_KINDS or
                    "status" in clean[key] and clean[key]["status"] not in SLOT_STATES):
                raise SessionError("invalid_request")
    result["arguments"] = clean
    _attachment_size(result)
    return result


def _attachment_ref(value):
    result = _fields(value, ("attachment_id", "item_index", "kind", "status"))
    for key in ("attachment_id", "kind", "status"):
        _string(result[key])
    _integer(result["item_index"])
    if result["status"] not in SLOT_STATES or result["kind"] not in ATTACHMENT_KINDS:
        raise SessionError("invalid_state_result")
    return result


def _reference_context(value, message_id=None):
    if value is None:
        return None
    result = _fields(value, ("schema_version", "snapshot_id", "text_message_id", "cutoff_seq",
                             "group_id", "login_epoch", "created_at", "expires_at", "slots",
                             "overflow", "total_positions"))
    _version(result["schema_version"])
    for key in ("snapshot_id", "text_message_id", "group_id", "login_epoch"):
        _string(result[key])
    if message_id is not None and result["text_message_id"] != message_id:
        raise SessionError("invalid_state_result")
    _integer(result["cutoff_seq"])
    _number(result["created_at"])
    _number(result["expires_at"])
    if result["expires_at"] < result["created_at"]:
        raise SessionError("invalid_state_result")
    _boolean(result["overflow"])
    _integer(result["total_positions"])
    def slot(raw):
        item = _fields(raw, ("ordinal", "message_id", "item_index", "attachment_id", "received_at", "status"))
        _integer(item["ordinal"], 1, 16)
        _integer(item["item_index"])
        _number(item["received_at"])
        for key in ("message_id", "attachment_id", "status"):
            _string(item[key])
        if item["status"] not in SLOT_STATES:
            raise SessionError("invalid_state_result")
        return item
    result["slots"] = _list(result["slots"], 16, slot)
    ordinals = [item["ordinal"] for item in result["slots"]]
    if (len(set(ordinals)) != len(ordinals) or ordinals != sorted(ordinals)
            or result["total_positions"] < max(ordinals, default=0)
            # Public policy can retain fewer than the hard maximum of 16 slots.
            or result["overflow"] != (result["total_positions"] > len(result["slots"]))):
        raise SessionError("invalid_state_result")
    return result


def _reference_identity(value):
    if value is None:
        return None
    result = {key: value[key] for key in ("schema_version", "snapshot_id", "text_message_id",
                                         "cutoff_seq", "group_id", "login_epoch", "total_positions", "overflow")}
    result["slots"] = [{key: item[key] for key in ("ordinal", "message_id", "item_index", "attachment_id")}
                       for item in value["slots"]]
    return result


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _metadata(value):
    result = _fields(value, ("attachment_id", "message_id", "item_index", "received_at", "kind",
                             "original_name", "name_source", "declared_type", "detected_type", "size",
                             "sha256", "status", "decryption_state", "preview_state", "tags", "expires_at", "pinned"))
    for key in ("attachment_id", "message_id", "kind", "name_source", "status", "decryption_state", "preview_state"):
        _string(result[key])
    if (result["kind"] not in ATTACHMENT_KINDS or result["status"] not in SLOT_STATES or
            result["decryption_state"] not in DECRYPTION_STATES or result["preview_state"] not in PREVIEW_STATES):
        raise SessionError("invalid_state_result")
    for key in ("original_name", "declared_type", "detected_type"):
        _string(result[key], nullable=True, byte_limit=4096)
    _integer(result["item_index"])
    _number(result["received_at"])
    _number(result["expires_at"], nullable=True)
    if result["size"] is not None:
        _integer(result["size"], 0, 100 << 20)
    if result["sha256"] is not None and (type(result["sha256"]) is not str or
                                         not re.fullmatch(r"[0-9a-f]{64}", result["sha256"])):
        raise SessionError("invalid_state_result")
    if result["status"] == "ready" and (result["size"] is None or result["sha256"] is None):
        raise SessionError("invalid_state_result")
    _boolean(result["pinned"])
    def tag(raw):
        item = _fields(raw, ("value", "source", "message_id"))
        _string(item["value"], chars=128)
        _string(item["source"])
        if item["source"] not in TAG_SOURCES:
            raise SessionError("invalid_state_result")
        _string(item["message_id"], nullable=True)
        return item
    result["tags"] = _list(result["tags"], 32, tag)
    return result


def _material(value, base, reference=False):
    keys = ("attachment_id", "path", "hold_id", "expires_at")
    if reference:
        keys += ("ordinal", "preview_state", "detected_type", "size", "sha256")
    result = _fields(value, keys)
    for key in ("attachment_id", "path", "hold_id"):
        _string(result[key])
    path = Path(result["path"])
    if not path.is_absolute():
        raise SessionError("invalid_state_result")
    try:
        path.resolve().relative_to(Path(base) / "attachments" / "objects")
    except ValueError:
        raise SessionError("invalid_state_result") from None
    _number(result["expires_at"])
    if reference:
        _integer(result["ordinal"], 1, 16)
        _string(result["preview_state"])
        if result["preview_state"] not in PREVIEW_STATES:
            raise SessionError("invalid_state_result")
        _string(result["detected_type"], nullable=True, byte_limit=4096)
        _integer(result["size"], 0, 100 << 20)
        if type(result["sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", result["sha256"]):
            raise SessionError("invalid_state_result")
    return result


def _group_status(value):
    result = _fields(value, ("schema_version", "group_id", "created_at", "state", "attachment_count",
                             "accepted_attachment_ids", "pending_attachment_ids", "unknown_attachment_ids",
                             "units", "can_continue", "requires_explicit_continue"))
    _version(result["schema_version"])
    _string(result["group_id"])
    _number(result["created_at"])
    if type(result["state"]) is not str or result["state"] not in GROUP_STATES:
        raise SessionError("invalid_state_result")
    _integer(result["attachment_count"], 1, 16)
    ids = []
    for key in ("accepted_attachment_ids", "pending_attachment_ids", "unknown_attachment_ids"):
        result[key] = _ids(result[key])
        ids += result[key]
    if len(ids) != len(set(ids)) or len(ids) > result["attachment_count"]:
        raise SessionError("invalid_state_result")
    result["units"] = _fields(result["units"], ("total", "accepted", "pending", "unknown"))
    for count in result["units"].values():
        _integer(count)
    if sum(result["units"][key] for key in ("accepted", "pending", "unknown")) > result["units"]["total"]:
        raise SessionError("invalid_state_result")
    _boolean(result["can_continue"])
    if result["requires_explicit_continue"] is not True or result["can_continue"] and (result["units"]["unknown"] or result["unknown_attachment_ids"]):
        raise SessionError("invalid_state_result")
    return result


def _attachment_result(value, request, session, base):
    action, args = request["action"], request["arguments"]
    result = _fields(value, ("schema_version", "action", "ok", "result", "error"))
    _version(result["schema_version"])
    if type(result["action"]) is not str or result["action"] != action:
        raise SessionError("invalid_state_result")
    _boolean(result["ok"])
    if not result["ok"]:
        error = _fields(result["error"], ("code", "retryable", "refresh_required"))
        if result["result"] is not None or type(error["code"]) is not str or error["code"] not in ATTACHMENT_ERRORS:
            raise SessionError("invalid_state_result")
        _boolean(error["retryable"])
        _boolean(error["refresh_required"])
        if (error["refresh_required"] != (error["code"] == "needs_refresh") or
                error["retryable"] and error["code"] not in ("processing", "storage_busy")):
            raise SessionError("invalid_state_result")
        return dict(result, error=error)
    if result["error"] is not None:
        raise SessionError("invalid_state_result")
    raw = result["result"]
    if action == "query":
        clean = _fields(raw, ("items", "next_cursor", "complete"))
        clean["items"] = _list(clean["items"], args.get("limit", 20), _metadata)
        _string(clean["next_cursor"], nullable=True)
        _boolean(clean["complete"])
    elif action == "get":
        clean = _fields(raw, ("attachment", "material"))
        clean["attachment"] = _metadata(clean["attachment"])
        clean["material"] = _material(clean["material"], base)
        if (clean["attachment"]["status"] != "ready" or clean["attachment"]["attachment_id"] != args["attachment_id"]
                or clean["material"]["attachment_id"] != args["attachment_id"]):
            raise SessionError("invalid_state_result")
    elif action == "reference":
        clean = _fields(raw, ("reference_context", "materials", "unavailable", "complete"))
        clean["reference_context"] = _reference_context(clean["reference_context"], args["text_message_id"])
        context = clean["reference_context"]
        if (context is None or context["snapshot_id"] != args["snapshot_id"] or
                session.get("reference_digests", {}).get(args["text_message_id"]) != _digest(_reference_identity(context))):
            raise SessionError("invalid_state_result")
        clean["materials"] = _list(clean["materials"], 16, lambda item: _material(item, base, True))
        def unavailable(item):
            item = _fields(item, ("ordinal", "attachment_id", "status"))
            _integer(item["ordinal"], 1, 16)
            _string(item["attachment_id"])
            if type(item["status"]) is not str or item["status"] not in SLOT_STATES:
                raise SessionError("invalid_state_result")
            return item
        clean["unavailable"] = _list(clean["unavailable"], 16, unavailable)
        _boolean(clean["complete"])
        selected = args.get("ordinals", [item["ordinal"] for item in context["slots"]])
        slots = {item["ordinal"]: item for item in context["slots"]}
        returned = clean["materials"] + clean["unavailable"]
        if (len(returned) != len(selected) or {item["ordinal"] for item in returned} != set(selected) or
                any(slots.get(item["ordinal"], {}).get("attachment_id") != item["attachment_id"] for item in returned) or
                any(slots[item["ordinal"]]["status"] != "ready" for item in clean["materials"]) or
                clean["complete"] != (not clean["unavailable"])):
            raise SessionError("invalid_state_result")
    elif action == "release":
        clean = _fields(raw, ("released",))
        if clean["released"] is not True:
            raise SessionError("invalid_state_result")
    elif action == "pin":
        clean = {"attachment": _metadata(_fields(raw, ("attachment",))["attachment"])}
        if clean["attachment"]["attachment_id"] != args["attachment_id"] or clean["attachment"]["pinned"] != args["pinned"]:
            raise SessionError("invalid_state_result")
    elif action == "label":
        expected = len(args.get("attachment_ids", args.get("ordinals", [])))
        clean = {"attachments": _list(_fields(raw, ("attachments",))["attachments"], expected, _metadata, expected)}
        ids = _ids([item["attachment_id"] for item in clean["attachments"]])
        if "attachment_ids" in args and set(ids) != set(args["attachment_ids"]):
            raise SessionError("invalid_state_result")
        if any(not any(tag == {"value": args["label"], "source": "user", "message_id": args["text_message_id"]}
                       for tag in item["tags"]) for item in clean["attachments"]):
            raise SessionError("invalid_state_result")
    elif action == "cancel":
        clean = {"group": _group_status(_fields(raw, ("group",))["group"])}
        if (clean["group"]["group_id"] != args["group_id"] or
                clean["group"]["state"] == "cancelled" and clean["group"]["can_continue"]):
            raise SessionError("invalid_state_result")
    elif action in ("reply", "continue"):
        clean = _fields(raw, ("queued", "delivery_confirmed", "group_id", "job_id", "status"))
        if clean["queued"] is not True or clean["delivery_confirmed"] is not False:
            raise SessionError("invalid_state_result")
        _string(clean["group_id"])
        _string(clean["job_id"])
        clean["status"] = _group_status(clean["status"])
        if clean["status"]["group_id"] != clean["group_id"] or action == "continue" and clean["group_id"] != args["group_id"]:
            raise SessionError("invalid_state_result")
    elif "group_id" in args:
        clean = {"group": _group_status(_fields(raw, ("group",))["group"])}
        if clean["group"]["group_id"] != args["group_id"]:
            raise SessionError("invalid_state_result")
    else:
        clean = _fields(raw, ("groups", "next_cursor"))
        clean["groups"] = _list(clean["groups"], args.get("limit", 20), _group_status)
        _string(clean["next_cursor"], nullable=True)
    result["result"] = clean
    _attachment_size(result)
    return result


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


def _invoke_timed(session, action, method, *args):
    # Private adapter call times only: these are not inferred from lease
    # deadlines and do not establish platform wakeup or message delivery time.
    timing = {"started_at": None, "returned_at": None,
              "result": "outcome_unknown"}
    session.setdefault("call_timings", {})[action] = timing
    def call():
        timing["started_at"] = time.time()
        try:
            result = method(*args)
        except Exception:
            timing["returned_at"] = time.time()
            raise
        timing["returned_at"] = time.time()
        return result
    return _invoke(call)


def _safe_media_diagnostics(value):
    """Bound optional metadata; never inspect unapproved nested properties."""
    if (type(value) is not dict or type(value.get("version")) is not int
            or value["version"] != 1 or type(value.get("items")) is not list
            or type(value.get("truncated")) is not bool):
        return None
    incoming = value["items"]
    clean = {"version": 1, "items": [],
             "truncated": value["truncated"] or len(incoming) > MEDIA_DIAGNOSTIC_MAX_SCAN}
    accepted_sources = {}
    for raw in incoming[:MEDIA_DIAGNOSTIC_MAX_SCAN]:
        if type(raw) is not dict:
            clean["truncated"] = True
            continue
        index = raw.get("item_index")
        source, outcome, reason = raw.get("source"), raw.get("outcome"), raw.get("reason")
        size = raw.get("downloaded_bytes")
        if (type(index) is not int or not 0 <= index <= 999
                or type(source) is not str or source not in MEDIA_DIAGNOSTIC_SOURCES
                or type(outcome) is not str or outcome not in ("success", "failed")
                or type(reason) is not str or reason not in MEDIA_DIAGNOSTIC_REASONS
                or (outcome == "success") != (reason == "ok")
                or "downloaded_bytes" not in raw
                or size is not None and (type(size) is not int
                                         or not 0 <= size <= MEDIA_DIAGNOSTIC_MAX_DOWNLOADED)):
            clean["truncated"] = True
            continue
        sources = accepted_sources.get(index, ())
        if (len(clean["items"]) >= MEDIA_DIAGNOSTIC_MAX_ITEMS
                or len(sources) >= 2 or source in sources):
            clean["truncated"] = True
            continue
        item = {"item_index": index, "source": source, "outcome": outcome,
                "downloaded_bytes": size, "reason": reason}
        clean["items"].append(item)
        if len(json.dumps(clean, separators=(",", ":")).encode("utf-8")) > MEDIA_DIAGNOSTIC_MAX_BYTES:
            clean["items"].pop()
            clean["truncated"] = True
            continue
        accepted_sources.setdefault(index, set()).add(source)
    return clean


def _snapshot(session, batch):
    owned, messages = batch.get("owned"), batch.get("messages")
    if (batch.get("lease") != session["lease"] or not isinstance(owned, list)
            or not owned or len(set(owned)) != len(owned)
            or not isinstance(messages, list)
            or any(not isinstance(mid, str) or safe_message_id(mid) != mid for mid in owned)
            or any(not isinstance(message, dict) for message in messages)
            or [message.get("message_id") for message in messages] != owned):
        raise SessionError("invalid_state_result")
    caps = _capabilities(batch["capabilities"]) if "capabilities" in batch else None
    clean, reference_digests = [], {}
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
            if key == "media_diagnostics":
                diagnostic = _safe_media_diagnostics(value)
                if diagnostic is not None:
                    item[key] = diagnostic
                continue
            if key in ("create_time_ms", "received_at") and value is None:
                item[key] = None
                continue
            if key not in ("text", "media_paths") and (
                    not isinstance(value, (int, float, bool)) or not math.isfinite(value)):
                raise SessionError("invalid_state_result")
            item[key] = value
        if caps is not None:
            refs = message.get("attachment_refs", [])
            if type(refs) is not list:
                raise SessionError("invalid_state_result")
            # Incoming records are the public receiver's complete list. The
            # outgoing reply/selection limit is not an incoming message limit.
            item["attachment_refs"] = [_attachment_ref(ref) for ref in refs]
            item["reference_context"] = _reference_context(message.get("reference_context"), item["message_id"])
            identity = _reference_identity(item["reference_context"])
            reference_digests[item["message_id"]] = _digest(identity) if identity is not None else None
            old_bindings = session.get("reference_digests", {})
            if item["message_id"] in old_bindings and old_bindings[item["message_id"]] != reference_digests[item["message_id"]]:
                raise SessionError("invalid_state_result")
        elif "attachment_refs" in message or "reference_context" in message:
            raise SessionError("attachment_unavailable")
        clean.append(item)
    fields = dict(messages=clean, owned_count=len(owned), revision=session.get("revision", 0) + 1)
    if caps is not None:
        fields["capabilities"] = caps
        # Attachment-call has its own envelope budget. No such byte budget is
        # imposed on begin/refresh message records, individually or in a batch.
        _attachment_size(caps)
    session["owned"] = owned
    session["revision"] = session.get("revision", 0) + 1
    digest_messages = []
    for item in clean:
        digest_item = dict(item)
        if caps is not None:
            digest_item["reference_context"] = _reference_identity(item["reference_context"])
            digest_item["attachment_refs"] = [{key: ref[key] for key in ("attachment_id", "item_index", "kind")}
                                               for ref in item["attachment_refs"]]
        digest_messages.append(digest_item)
    session["batch_digest"] = _digest(digest_messages)
    session["capabilities"] = caps
    # Hashes bind the current batch's immutable references, not a history cache.
    session["reference_digests"] = reference_digests
    for key in TIME_FIELDS:
        value = batch.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise SessionError("invalid_state_result")
        session[key] = value
    fields.update({key: session[key] for key in TIME_FIELDS})
    return fields


def _uncertain(directory, handle, session):
    session["phase"] = "uncertain"
    session["call_timings"]["reply"]["result"] = "outcome_unknown"
    try:
        _save(directory, handle, session)
    except OSError:
        pass  # The pre-call 'replying' marker also forbids automatic replay.
    return failure("outcome_unknown")


def _attachment_failure(handle, session, code, retryable=False):
    return dict(ok=False, error=code, handle=handle, revision=session["revision"],
                must_stop=code in ("outcome_unknown", "lease_lost", "epoch_mismatch",
                                   "invalid_state_result", "unsupported_version", "storage_invalid", "session_closed"),
                retryable=retryable, requires_refresh=code in ("needs_refresh", "revision_mismatch"))


def _legacy_media_preflight(request, handle, session, state):
    media = request.get("media_path")
    if media is None:
        return None
    try:
        from wechat_bridge.attachment_runtime import is_catalog_original_path
    except ModuleNotFoundError as error:
        # Only a genuinely old package without attachment capabilities may
        # retain its original media path. Broken modern imports fail closed.
        if error.name == "wechat_bridge.attachment_runtime" and session.get("capabilities") is None:
            return None
        raise SessionError("invalid_configuration") from None
    # Import/guard errors propagate to the fixed must-stop adapter error. They
    # never downgrade to calling reply, and no publication attempt was made.
    if _invoke(is_catalog_original_path, state.store, media):
        return _attachment_failure(handle, session, "invalid_request")
    return None


def _dispatch_attachment(request, directory, handle, session, state, base):
    wire = _attachment_request(request["request"])
    action = wire["action"]
    if session.get("capabilities") is None or not callable(getattr(state, "attachment_call", None)):
        return _attachment_failure(handle, session, "attachment_unavailable")
    _capabilities(session["capabilities"])
    if action in ("label", "cancel"):
        feature = LABEL_FEATURE if action == "label" else CANCEL_FEATURE
        if feature not in session["capabilities"]["features"]:
            return _attachment_failure(handle, session, "attachment_unavailable")
        if wire["arguments"]["text_message_id"] not in session["owned"]:
            return _attachment_failure(handle, session, "invalid_request")
        if action == "label" and "snapshot_id" in wire["arguments"] and not session.get("reference_digests", {}).get(wire["arguments"]["text_message_id"]):
            return _attachment_failure(handle, session, "attachment_unavailable")
    if action == "reference" and (wire["arguments"]["text_message_id"] not in session["owned"] or
                                   not session.get("reference_digests", {}).get(wire["arguments"]["text_message_id"])):
        return _attachment_failure(handle, session, "attachment_unavailable")
    publishing = action in ("reply", "continue")
    timing_key = "reply" if publishing else "attachment_" + action
    if publishing:
        session["phase"] = "replying"
        session.setdefault("call_timings", {})["reply"] = {
            "started_at": None, "returned_at": None, "result": "outcome_unknown"}
        _save(directory, handle, session)
    try:
        raw = _invoke_timed(session, timing_key, state.attachment_call,
                            list(session["owned"]), session["lease"], wire)
        envelope = _attachment_result(raw, wire, session, base)
        if not envelope["ok"]:
            error = envelope["error"]
            code = error["code"]
            # A broken storage/response boundary may occur after publication.
            if publishing and code in ("storage_invalid", "response_too_large"):
                _uncertain(directory, handle, session)
                return _attachment_failure(handle, session, "outcome_unknown")
            session["phase"] = ("needs_refresh" if code == "needs_refresh" else
                                "lease_lost" if code in ("lease_lost", "epoch_mismatch") else "active")
            session["call_timings"][timing_key]["result"] = code
            _save(directory, handle, session)
            return _attachment_failure(handle, session, code, error["retryable"])
        response = dict(ok=True, handle=handle, revision=session["revision"], result=envelope["result"])
        _attachment_size(response)
        session["phase"] = "queued" if publishing else "active"
        session["call_timings"][timing_key]["result"] = "queued" if publishing else "ok"
        _save(directory, handle, session)
        return response
    except Exception:
        if publishing:
            _uncertain(directory, handle, session)
            return _attachment_failure(handle, session, "outcome_unknown")
        # No automatic release, refresh or retry, including an uncertain hold.
        _save(directory, handle, session)
        return _attachment_failure(handle, session, "storage_invalid")


def _dispatch(request, base, state_dir, state_factory):
    action = request["action"]
    directory = _adapter_dir(state_dir)
    handle = uuid.uuid4().hex if action == "begin" else request["handle"]
    with _session_lock(directory, handle):
        if action == "begin":
            state = state_factory(base, state_dir)
            session = {"schema": SCHEMA, "base": base, "state_dir": state_dir,
                       "phase": "active"}
            batch = _invoke_timed(session, "begin", state.begin, request["lease"])
            if not batch.get("owned"):
                return failure("lease_lost")
            session["lease"] = batch["lease"]
            fields = _snapshot(session, batch)
            session["call_timings"]["begin"]["result"] = "ok"
            _save(directory, handle, session)
            response = dict(ok=True, handle=handle, **fields)
            return response

        session = _load(directory, handle, base, state_dir)
        phase = session.get("phase")
        if phase in ("replying", "uncertain", "releasing"):
            if action == "attachment":
                return _attachment_failure(handle, session, "outcome_unknown")
            return failure("outcome_unknown")
        if phase == "lease_lost":
            if action == "attachment":
                return _attachment_failure(handle, session, "lease_lost")
            return failure("lease_lost")
        if phase not in ("active", "needs_refresh"):
            if action == "attachment":
                return _attachment_failure(handle, session, "session_closed")
            return failure("session_closed")
        if action in ("reply", "attachment") and request["revision"] != session["revision"]:
            if action == "attachment":
                return _attachment_failure(handle, session, "revision_mismatch")
            return failure("revision_mismatch", revision=session["revision"], requires_refresh=True)
        if action in ("reply", "attachment") and phase == "needs_refresh":
            if action == "attachment":
                return _attachment_failure(handle, session, "needs_refresh")
            return failure("needs_refresh", revision=session["revision"], requires_refresh=True)
        state = state_factory(base, state_dir)
        if action == "attachment":
            return _dispatch_attachment(request, directory, handle, session, state, base)
        if action == "refresh":
            try:
                batch = _invoke_timed(session, "refresh", state.refresh, session["lease"])
            except Exception:
                # Record only the observed exception boundary; keep the
                # existing refresh failure/state semantics and fixed error.
                _save(directory, handle, session)
                raise
            if batch.get("error") == "lease_lost" or not batch.get("owned"):
                session["phase"] = "lease_lost"
                session["call_timings"]["refresh"]["result"] = "lease_lost"
                _save(directory, handle, session)
                return failure("lease_lost")
            fields = _snapshot(session, batch)
            session["phase"] = "active"
            session["call_timings"]["refresh"]["result"] = "ok"
            _save(directory, handle, session)
            response = dict(ok=True, handle=handle, **fields)
            return response
        if action == "release":
            session["phase"] = "releasing"
            _save(directory, handle, session)
            _invoke(state.release, session["lease"])
            session["phase"] = "released"
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, released=True, external_tools_cancelled=False)

        rejected = _legacy_media_preflight(request, handle, session, state)
        if rejected is not None:
            return rejected

        # Persist the attempt BEFORE calling core. A crash or unknown result
        # must never silently retry or release this session on the next call.
        session["phase"] = "replying"
        # Do not retain a previous needs_refresh attempt as if it were this
        # one. A crash leaves unknown times, never a fabricated return time.
        session.setdefault("call_timings", {})["reply"] = {
            "started_at": None, "returned_at": None, "result": "outcome_unknown"}
        _save(directory, handle, session)
        try:
            result = _invoke_timed(session, "reply", state.reply, session["owned"],
                                   request.get("text", ""), session["lease"],
                                   request.get("media_path"))
            if result.get("error") == "needs_refresh":
                fields = _snapshot(session, result)
                session["phase"] = "needs_refresh"
                session["call_timings"]["reply"]["result"] = "needs_refresh"
                _save(directory, handle, session)
                return dict(failure("needs_refresh", handle=handle, requires_refresh=True), **fields)
            if (not isinstance(result.get("queued"), str)
                    or result.get("covers_message_ids") != session["owned"]):
                return _uncertain(directory, handle, session)
            session["phase"] = "queued"
            session["call_timings"]["reply"]["result"] = "queued"
            _save(directory, handle, session)
            return dict(ok=True, handle=handle, revision=session["revision"],
                        queued=result["queued"], owned_count=len(session["owned"]),
                        delivery_confirmed=False)
        except ValueError as error:
            # These exact core errors occur before publication. Never emit the
            # source text, which could include identifiers or future secrets.
            if str(error).startswith("lease lost for ") or str(error) == "lease lost before publication":
                session["phase"] = "lease_lost"
                session["call_timings"]["reply"]["result"] = "lease_lost"
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
    # Match the measured attachment wire size, including UTF-8 text.
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    if result["ok"]:
        return 0
    if result["error"] in ("invalid_request", "invalid_configuration"):
        return 2
    if not result["must_stop"]:
        return 4
    return 3


if __name__ == "__main__":
    sys.exit(main())
