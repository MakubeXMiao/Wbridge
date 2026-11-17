"""Optional context, separated from queue admission and public reply results."""
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from types import SimpleNamespace
import time


_SEAL = object()
TTL = 48 * 3600
CONTEXT_REASONS = frozenset({"storage_busy", "storage_invalid", "storage_changed",
    "storage_too_large", "sequence_unavailable", "scope_mismatch", "epoch_mismatch",
    "clock_invalid", "clock_anomaly", "too_large", "projection_too_large",
    "entry_unavailable", "invalid_entry", "entry_too_large", "scope_unavailable",
    "context_unavailable"})


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _enabled(name):
    return os.environ.get(name) == "1"


def capabilities():
    features = []
    if _enabled("WECHAT_RECENT_CONTEXT"):
        features += ["recent_context_v1", "memory_delta_v1"]
    if _enabled("WECHAT_CLOUD_CONTEXT"):
        features.append("cloud_context_v1")
    return {"schema_version": 1, "features": features}


@dataclass
class ContextRead:
    projection: dict
    basis: object = None


@dataclass
class ContextResult:
    result: dict
    context_capsule: object = None


class _Capsule:
    __slots__ = ("_seal", "_store", "_draft", "_used", "_issued_at", "_mono_deadline")

    def __init__(self, store, draft, now):
        self._seal, self._store, self._draft, self._used = _SEAL, store, draft, False
        self._issued_at = now
        remaining = min(s["received_at"] for s in draft["sources"]) + TTL - now
        self._mono_deadline = time.monotonic() + max(0, remaining)

    def __repr__(self):
        return "<private context capsule>"


def _reason(error):
    code = getattr(error, "code", "context_unavailable")
    return code if code in CONTEXT_REASONS else "context_unavailable"


def _current_scope(store):
    from .attachment_runtime import current_scope
    return current_scope(store)


def _bound_batch(worker, batch):
    """Read-only ownership check. Caller holds the original receipt/claim locks."""
    from .store import safe_message_id
    if type(batch) is not dict or type(batch.get("lease")) is not str:
        raise ValueError("invalid batch")
    ids = batch.get("owned")
    if type(ids) is not list or not ids or len(ids) > 1000:
        raise ValueError("invalid batch")
    ids = [safe_message_id(mid) for mid in ids]
    if len(set(ids)) != len(ids):
        raise ValueError("invalid batch")
    claims, records = worker._claims(), worker._records()
    owned = {mid for mid, row in claims.items() if row.get("lease") == batch["lease"]}
    if owned != set(ids):
        raise ValueError("batch changed")
    scope = _current_scope(worker.store)
    for mid in ids:
        row, record = claims[mid], records.get(mid, {})
        if (row.get("kind") != "worker" or not worker._alive(mid, row)
                or record.get("from_user_id") != scope["owner_id"]
                or record.get("login_epoch") != scope["login_epoch"]):
            raise ValueError("batch unavailable")
    return scope, [records[mid] for mid in ids]


def _basis_key(store):
    from .attachment_runtime import _session
    token = _session(store).get("bot_token")
    if type(token) is not str or not token:
        raise ValueError("identity unavailable")
    return hashlib.sha256(b"wbridge-context-basis-v1\0" + token.encode("utf-8")).digest()


def _sign_basis(store, scope, recent, now):
    payload = {"schema_version": 1, "scope": scope, "snapshot_id": recent["snapshot_id"],
        "issued_at": now, "entries": [{"entry_id": e["id"], "expires_at": e["expires_at"]}
                                      for e in recent["entries"]]}
    basis = {"payload": payload, "signature": hmac.new(_basis_key(store), _json(payload),
                                                       hashlib.sha256).hexdigest()}
    if len(_json(basis)) > 16384:
        raise ValueError("basis too large")
    return basis


def _verify_basis(store, basis, scope, now):
    if type(basis) is not dict or set(basis) != {"payload", "signature"} or len(_json(basis)) > 16384:
        raise ValueError("invalid basis")
    payload = basis["payload"]
    if (type(payload) is not dict or set(payload) != {"schema_version", "scope", "snapshot_id", "issued_at", "entries"}
            or type(payload["schema_version"]) is not int or payload["schema_version"] != 1
            or payload["scope"] != scope or type(basis["signature"]) is not str):
        raise ValueError("invalid basis")
    expected = hmac.new(_basis_key(store), _json(payload), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, basis["signature"]):
        raise ValueError("invalid basis")
    at, entries = payload["issued_at"], payload["entries"]
    if (type(at) not in (int, float) or not math.isfinite(at) or not at <= now < at + TTL
            or type(entries) is not list or len(entries) > 10):
        raise ValueError("expired basis")
    permitted = {}
    for entry in entries:
        if type(entry) is not dict or set(entry) != {"entry_id", "expires_at"}:
            raise ValueError("invalid basis")
        eid, expires = entry["entry_id"], entry["expires_at"]
        if (type(eid) is not str or len(eid) != 64 or eid in permitted
                or type(expires) not in (int, float) or not math.isfinite(expires)):
            raise ValueError("invalid basis")
        if now < expires:
            permitted[eid] = expires
    return permitted


def context_for_batch(worker, batch, cloud_binding=None):
    flags = capabilities()["features"]
    if not flags:
        return ContextRead({})
    try:
        with worker.locked():
            scope, _ = _bound_batch(worker, batch)
        now, projection, basis = worker.clock(), {}, None
        recent_context_maintain(worker.store, clock=lambda: now, cloud_binding=cloud_binding)
        if "recent_context_v1" in flags:
            from . import recent_context
            try:
                recent = recent_context.read(worker.store, scope, now)
                projection["recent_context_v1"] = recent
                basis = _sign_basis(worker.store, scope, recent, now)
            except Exception as error:
                projection["recent_context_status"] = _reason(error)
        if "cloud_context_v1" in flags:
            from . import cloud_context
            try:
                cloud = (cloud_context.read(worker.store, scope, now, enabled=True, binding=cloud_binding)
                         if cloud_binding is not None else
                         {"schema_version": 1, "status": "empty", "projection": None, "reason": "source_missing"})
                projection["cloud_context_v1"] = cloud
            except Exception as error:
                projection["cloud_context_v1"] = {"schema_version": 1, "status": "skipped",
                    "projection": None, "reason": _reason(error)}
        # Optional disk I/O must not outlive batch ownership or original TTL.
        with worker.locked():
            final_scope, _ = _bound_batch(worker, batch)
        final_now = worker.clock()
        if final_scope != scope:
            return ContextRead({"context_status": "scope_mismatch"})
        if final_now < now:
            return ContextRead({"context_status": "clock_anomaly"})
        if any(final_now >= e["expires_at"] for e in projection.get("recent_context_v1", {}).get("entries", [])):
            return ContextRead({"context_status": "context_unavailable"})
        card = projection.get("cloud_context_v1", {}).get("projection")
        if card is not None and final_now >= card["source_updated_at"] + TTL:
            projection["cloud_context_v1"] = {"schema_version": 1, "status": "expired",
                                             "projection": None, "reason": "source_expired"}
        if len(_json(projection.get("recent_context_v1", {}))) > 12 * 1024:
            projection.pop("recent_context_v1", None)
            basis = None
        if len(_json(projection.get("cloud_context_v1", {}))) > 4 * 1024:
            projection.pop("cloud_context_v1", None)
        if len(_json(projection)) > 16 * 1024:
            return ContextRead({"context_status": "too_large"})
        return ContextRead(projection, basis)
    except Exception as error:
        return ContextRead({"context_status": _reason(error)})


def _clip(text, size):
    raw = text.encode("utf-8")
    return raw[:size].decode("utf-8", errors="ignore"), len(raw) > size


def _capture(worker, handle, revision, basis, drafts):
    def capture(scope, messages, admission, reply_text):
        # Optional source lookup or malformed local metadata cannot affect the queue.
        try:
            from .attachment_runtime import catalog
            if (not _enabled("WECHAT_RECENT_CONTEXT") or scope is None
                    or type(handle) is not str or not 1 <= len(handle) <= 128
                    or type(revision) is not int or revision < 1):
                return
            sources = catalog(worker.store).context_sources(scope, messages)
            if not sources:
                return
            quotes = []
            by_id = {row["message_id"]: row for row in messages}
            for source in sources:
                text = by_id[source["message_id"]].get("text")
                # Generated image/failure placeholders are not user quotations.
                # A pure-text event can be rechecked against its original digest.
                if (type(text) is str and text and len(quotes) < 2
                        and _digest([[], text]) == source["fingerprint"]):
                    excerpt, clipped = _clip(text, 512)
                    quotes.append({"speaker": "user", "message_id": source["message_id"],
                                   "text": excerpt, "truncated": clipped})
            if type(reply_text) is str and reply_text:
                excerpt, clipped = _clip(reply_text, 1024)
                quotes.append({"speaker": "bot", "message_id": sources[-1]["message_id"],
                               "text": excerpt, "truncated": clipped})
            drafts.append({"scope": dict(scope), "sources": sources, "quotes": quotes,
                "admission": dict(admission), "handle": handle, "revision": revision,
                "basis": basis})
        except Exception as error:
            worker._trace("context_skipped", [row.get("message_id") for row in messages],
                          reason=_reason(error))
            return
    return capture


def reply_with_context(worker, ids, text, lease, media_path=None, *, handle, revision, basis=None):
    drafts = []
    result = worker.enqueue_reply(ids, None, None, text, lease, media_path, collect_batch=True,
        _context_capture=_capture(worker, handle, revision, basis, drafts))
    capsule = None
    try:
        if drafts and type(result) is dict and type(result.get("queued")) is str:
            draft = drafts[-1]
            covered = {v["message_id"] for v in draft["sources"]}
            if covered == set(result.get("covers_message_ids", [])) == set(result.get("completed", [])):
                capsule = _Capsule(worker.store, draft, worker.clock())
    except Exception:
        pass
    return ContextResult(result, capsule)


def attachment_call_with_context(worker, ids, lease, request, *, handle, revision, basis=None):
    from .attachment_runtime import worker_call
    drafts = []
    result = worker_call(worker, ids, lease, request,
        _context_capture=_capture(worker, handle, revision, basis, drafts))
    capsule = None
    try:
        if (drafts and result.get("ok") is True and result.get("action") in ("reply", "continue")
                and type(result.get("result")) is dict and result["result"].get("queued") is True
                and result["result"].get("delivery_confirmed") is False):
            draft = drafts[-1]
            if all(result["result"].get(k) == draft["admission"][k] for k in ("job_id", "group_id")):
                capsule = _Capsule(worker.store, draft, worker.clock())
    except Exception:
        pass
    return ContextResult(result, capsule)


def context_commit(worker, capsule, delta):
    """Call only after the adapter has durably saved the original queued result."""
    try:
        if not _enabled("WECHAT_RECENT_CONTEXT"):
            return {"status": "skipped", "reason": "disabled"}
        if (type(capsule) is not _Capsule or capsule._seal is not _SEAL
                or capsule._store is not worker.store or capsule._used):
            return {"status": "skipped", "reason": "capsule_unavailable"}
        capsule._used = True
        draft, now = capsule._draft, worker.clock()
        if now < capsule._issued_at or time.monotonic() >= capsule._mono_deadline:
            return {"status": "skipped", "reason": "source_expired"}
        if delta is None:
            return {"status": "skipped", "reason": "no_candidate"}
        fields = {"schema_version", "handle", "revision", "source_message_ids",
                  "topic", "intent", "bot_action", "open_loop", "historical_refs"}
        if (type(delta) is not dict or set(delta) != fields or len(_json(delta)) > 8192
                or type(delta["schema_version"]) is not int or delta["schema_version"] != 1
                or type(delta["handle"]) is not str or type(delta["revision"]) is not int
                or delta["revision"] < 1
                or delta["handle"] != draft["handle"] or delta["revision"] != draft["revision"]):
            raise ValueError("invalid candidate")
        ids = delta["source_message_ids"]
        if (type(ids) is not list or any(type(v) is not str for v in ids)
                or len(set(ids)) != len(ids) or set(ids) != {s["message_id"] for s in draft["sources"]}):
            raise ValueError("invalid sources")
        summary = {k: delta[k] for k in ("topic", "intent", "bot_action", "open_loop")}
        if (any(type(v) is not str for v in summary.values()) or sum(map(len, summary.values())) > 256
                or any(len(summary[k]) > n for k, n in zip(summary, (48, 80, 128, 128)))):
            raise ValueError("invalid summary")
        refs = delta["historical_refs"]
        if type(refs) is not list or len(refs) > 10 or any(type(v) is not str for v in refs) or len(set(refs)) != len(refs):
            raise ValueError("invalid references")
        permitted = _verify_basis(worker.store, draft["basis"], draft["scope"], now) if refs else {}
        if any(v not in permitted for v in refs):
            raise ValueError("invalid references")
        sources = draft["sources"]
        source_at, cutoff = min(s["received_at"] for s in sources), max(s["seq"] for s in sources)
        entry = {"schema_version": 1, "id": _digest([draft["scope"], sources]),
            "revision": draft["revision"], **summary, "sources": sources,
            "source_at": source_at, "expires_at": source_at + TTL, "cutoff_seq": cutoff,
            "historical_refs": [{"entry_id": v, "expires_at": permitted[v]} for v in refs],
            "admission": draft["admission"], "quotes": draft["quotes"]}
        from . import recent_context
        return recent_context.append(worker.store, draft["scope"], entry, now)
    except Exception as error:
        reason = _reason(error) if getattr(error, "code", None) else "invalid_candidate"
        return {"status": "skipped", "reason": reason}


def context_quote(worker, batch, basis, entry_id, quote_index):
    try:
        if not _enabled("WECHAT_RECENT_CONTEXT"):
            return {"status": "unavailable", "reason": "disabled"}
        with worker.locked():
            scope, _ = _bound_batch(worker, batch)
        now = worker.clock()
        permitted = _verify_basis(worker.store, basis, scope, now)
        if (type(entry_id) is not str or entry_id not in permitted
                or type(quote_index) is not int or not 0 <= quote_index < 3):
            raise ValueError("invalid quote")
        from . import recent_context
        entry = recent_context.get_entry(worker.store, scope, entry_id, now)
        if (not entry or entry["expires_at"] != permitted[entry_id]
                or quote_index >= len(entry["quotes"])):
            raise ValueError("quote unavailable")
        with worker.locked():
            final_scope, _ = _bound_batch(worker, batch)
        final_now = worker.clock()
        if (final_scope != scope or entry_id not in _verify_basis(worker.store, basis, scope, final_now)
                or not entry["source_at"] <= final_now < entry["expires_at"]):
            raise ValueError("quote unavailable")
        return {"status": "ok", "entry_id": entry_id, "expires_at": entry["expires_at"],
                "quote": entry["quotes"][quote_index]}
    except Exception as error:
        return {"status": "unavailable", "reason": _reason(error)}


def recent_context_maintain(base, clock=time.time, cloud_binding=None):
    """No Store creation, service start, login bootstrap, recursive scan or model call."""
    from . import recent_context, cloud_context
    if isinstance(base, (str, os.PathLike)):
        root = os.path.abspath(base)
        store = SimpleNamespace(base=root, state_dir=os.path.join(root, "state"),
            session_path=os.path.join(root, "state", "session.json"),
            needs_relogin_path=os.path.join(root, "state", "NEEDS_RELOGIN"))
    else:
        store = base
    try:
        scope = _current_scope(store)
    except Exception:
        scope = None
    now, result = clock(), {}
    for name, module in (("recent", recent_context), ("cloud", cloud_context)):
        try:
            effective_scope = (None if name == "cloud" and _enabled("WECHAT_CLOUD_CONTEXT")
                               and cloud_binding is None else scope)
            result[name] = module.maintain(store, effective_scope, now)
        except Exception as error:
            result[name] = {"status": "skipped", "reason": _reason(error)}
    if scope is not None and cloud_binding is not None and _enabled("WECHAT_CLOUD_CONTEXT"):
        try:
            result["cloud_refresh"] = cloud_context.refresh(store, cloud_binding, scope, now, enabled=True)
        except Exception as error:
            result["cloud_refresh"] = {"status": "skipped", "reason": _reason(error)}
    return result
