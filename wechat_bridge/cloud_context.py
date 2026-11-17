"""Opt-in, same-host publication and bounded projection of one cloud case.

The publisher is a trusted local integration configured with a fixed binding.
This is an independent state-publication call, not a WeChat worker action or
platform identity/authentication claim. Bridge maintenance never deletes the
source authority. No function performs network I/O or a business side effect.
"""
from dataclasses import dataclass
import hashlib
import math
import re

from . import context_files as files


MAX_BYTES = 4096
TTL_SECONDS = 48 * 60 * 60
_SOURCE = "current.json"
_CACHE = "cloud.json"
_TEMP = ".cloud.json.tmp"
_FIELDS = ("title", "facts", "change", "next_step", "open_loop")
_PROJECTION = frozenset(("source_revision", "source_updated_at", "source_ref",
                         "fact_status", *_FIELDS))
_FACT_STATUSES = frozenset(("confirmed", "provisional", "corrected", "completed", "revoked"))
_SYNC_STATUSES = frozenset(("applied", "unavailable", "update_pending", "paused"))
_TOMBSTONE_REASONS = frozenset(("source_expired", "source_revoked", "scope_mismatch",
                                "scope_unavailable", "clock_invalid", "storage_invalid"))
_SCOPE = frozenset(("bot_id", "owner_id", "conversation_id", "login_epoch"))
_REASONS = frozenset(("disabled", "invalid_request", "publisher_mismatch", "scope_mismatch",
    "binding_mismatch", "revision_conflict", "older_source", "source_missing", "source_invalid",
    "source_expired", "source_revoked", "source_paused", "source_unavailable", "cache_missing",
    "clock_invalid", "storage_busy", "storage_invalid", "storage_changed", "storage_too_large",
    "source_too_large", "cache_too_large", "cleaned", "unchanged", "applied", "published"))


def _text(value, maximum=128, *, empty=False, multiline=False):
    if (type(value) is not str or len(value) > maximum or (not value and not empty)
            or any(ord(char) < 32 and not (multiline and char in "\t\r\n") for char in value)):
        raise files.ContextError("invalid_request")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise files.ContextError("invalid_request") from None
    return value


def _time(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise files.ContextError("clock_invalid")
    return value


def _revision(value):
    if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
        raise files.ContextError("invalid_request")
    return value


@dataclass(frozen=True)
class CloudBinding:
    source_id: str
    case_id: str
    publisher_id: str

    def __post_init__(self):
        for value in (self.source_id, self.case_id, self.publisher_id):
            _text(value)


def _binding(value):
    if type(value) is CloudBinding:
        return dict(source_id=value.source_id, case_id=value.case_id, publisher_id=value.publisher_id)
    if type(value) is not dict or set(value) != {"source_id", "case_id", "publisher_id"}:
        raise files.ContextError("binding_mismatch")
    return _binding(CloudBinding(**value))


def _scope(value):
    if type(value) is not dict or set(value) != _SCOPE:
        raise files.ContextError("scope_mismatch")
    result = {key: _text(value[key], 512) for key in sorted(_SCOPE)}
    if not re.fullmatch(r"[0-9a-f]{32}", result["login_epoch"]):
        raise files.ContextError("scope_mismatch")
    return result


def _authority(store, expected=None):
    from .attachment_runtime import current_scope
    try:
        scope = _scope(current_scope(store))
    except Exception:
        raise files.ContextError("scope_mismatch") from None
    if expected is not None and scope != _scope(expected):
        raise files.ContextError("scope_mismatch")
    return scope


def _projection(value):
    if type(value) is not dict or set(value) != _PROJECTION:
        raise files.ContextError("invalid_request")
    # The five text fields have one collective Unicode-character budget.
    if any(type(value[field]) is not str for field in _FIELDS):
        raise files.ContextError("invalid_request")
    if sum(len(value[field]) for field in _FIELDS) > 512:
        raise files.ContextError("source_too_large")
    result = {field: _text(value[field], 512, empty=True, multiline=True) for field in _FIELDS}
    result.update(source_revision=_revision(value["source_revision"]),
                  source_updated_at=_time(value["source_updated_at"]),
                  source_ref=_text(value["source_ref"], 512),
                  fact_status=value["fact_status"])
    if type(result["fact_status"]) is not str or result["fact_status"] not in _FACT_STATUSES:
        raise files.ContextError("invalid_request")
    if len(files.encode(result)) > MAX_BYTES:
        raise files.ContextError("source_too_large")
    return result


def _digest(value):
    return hashlib.sha256(files.encode(value)).hexdigest()


def _source_record(value):
    fields = {"schema_version", "scope", "binding", "projection", "digest", "published_at", "active"}
    if (type(value) is not dict or set(value) != fields
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or type(value["active"]) is not bool):
        raise files.ContextError("source_invalid")
    scope, binding = _scope(value["scope"]), _binding(value["binding"])
    projection = _projection(value["projection"])
    published = _time(value["published_at"])
    if projection["source_updated_at"] > published or value["digest"] != _digest(projection):
        raise files.ContextError("source_invalid")
    result = dict(value, scope=scope, binding=binding, projection=projection)
    if len(files.encode(result)) > MAX_BYTES:
        raise files.ContextError("source_too_large")
    return result


def _cache_record(value):
    fields = {"schema_version", "scope", "binding", "projection", "source_digest",
              "bridge_checked_at", "bridge_applied_at", "sync_status", "observed_source_revision",
              "clock_high_water"}
    if (type(value) is not dict or set(value) != fields
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or type(value["sync_status"]) is not str or value["sync_status"] not in _SYNC_STATUSES):
        raise files.ContextError("storage_invalid")
    scope, binding, projection = _scope(value["scope"]), _binding(value["binding"]), _projection(value["projection"])
    checked, applied = _time(value["bridge_checked_at"]), _time(value["bridge_applied_at"])
    clock_high_water = _time(value["clock_high_water"])
    observed = value["observed_source_revision"]
    if observed is not None:
        _revision(observed)
    if (projection["source_updated_at"] > applied or applied > checked or checked > clock_high_water
            or value["source_digest"] != _digest(projection)
            or projection["fact_status"] == "revoked"):
        raise files.ContextError("storage_invalid")
    result = dict(value, scope=scope, binding=binding, projection=projection)
    if len(files.encode(result)) > MAX_BYTES:
        raise files.ContextError("cache_too_large")
    return result


def _tombstone_record(value):
    """One bounded hash-only control record. No source or account text survives."""
    fields = {"schema_version", "tombstone", "scope_digest", "binding_digest", "clock_high_water",
              "retired_revision", "retired_digest", "retired_source_updated_at", "retired_expires_at", "reason"}
    if (type(value) is not dict or set(value) != fields or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["tombstone"] is not True
            or type(value["reason"]) is not str or value["reason"] not in _TOMBSTONE_REASONS):
        raise files.ContextError("storage_invalid")
    for key in ("scope_digest", "binding_digest"):
        if type(value[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", value[key]) is None:
            raise files.ContextError("storage_invalid")
    _time(value["clock_high_water"])
    retired = [value[key] for key in ("retired_revision", "retired_digest", "retired_source_updated_at", "retired_expires_at")]
    if any(item is not None for item in retired):
        _revision(value["retired_revision"])
        if (type(value["retired_digest"]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", value["retired_digest"]) is None
                or _time(value["retired_source_updated_at"]) > value["clock_high_water"]
                or _time(value["retired_expires_at"]) != value["retired_source_updated_at"] + TTL_SECONDS):
            raise files.ContextError("storage_invalid")
    if len(files.encode(value)) > MAX_BYTES:
        raise files.ContextError("storage_too_large")
    return dict(value)


def _retire(record, now, reason, previous=None, scope=None):
    """Replace text with lineage hashes, retaining a monotonic observed wall high water."""
    marker = dict(schema_version=1, tombstone=True, scope_digest=_digest(scope) if scope else "0" * 64,
                  binding_digest="0" * 64, clock_high_water=now, retired_revision=None,
                  retired_digest=None, retired_source_updated_at=None, retired_expires_at=None, reason=reason)
    if record is not None:
        projection = record["projection"]
        marker.update(scope_digest=_digest(record["scope"]), binding_digest=_digest(record["binding"]),
                      clock_high_water=max(now, record.get("clock_high_water", record.get("published_at", now))),
                      retired_revision=projection["source_revision"],
                      retired_digest=record.get("source_digest", record.get("digest")),
                      retired_source_updated_at=projection["source_updated_at"],
                      retired_expires_at=projection["source_updated_at"] + TTL_SECONDS)
    if previous is not None:
        previous = _tombstone_record(previous)
        marker["clock_high_water"] = max(marker["clock_high_water"], previous["clock_high_water"])
        same = (marker["scope_digest"], marker["binding_digest"]) == (previous["scope_digest"], previous["binding_digest"])
        if record is None or (same and previous["retired_revision"] is not None
                and previous["retired_revision"] >= marker["retired_revision"]):
            for key in ("scope_digest", "binding_digest", "retired_revision", "retired_digest",
                        "retired_source_updated_at", "retired_expires_at"):
                marker[key] = previous[key]
    return _tombstone_record(marker)


def _retired_source(marker, source):
    """Same lineage requires both a new version and a newly sourced conclusion."""
    return (marker["retired_revision"] is not None
            and marker["scope_digest"] == _digest(source["scope"])
            and marker["binding_digest"] == _digest(source["binding"])
            and (source["projection"]["source_revision"] <= marker["retired_revision"]
                 or source["projection"]["source_updated_at"] <= marker["retired_source_updated_at"]))


def _tombstone_card(marker, now):
    if now < marker["clock_high_water"]:
        raise files.ContextError("clock_invalid")
    reason = marker["reason"]
    if reason == "source_expired":
        return dict(schema_version=1, status="expired", projection=None, reason=reason)
    return dict(schema_version=1, status="empty", projection=None,
                reason=reason if reason in _REASONS else "scope_mismatch")


def _result(status, reason, **fields):
    return dict(status=status, reason=reason if reason in _REASONS else "storage_invalid", **fields)


def _failure(error, status="skipped"):
    return _result(status, getattr(error, "code", "storage_invalid"))


def _live(record, now):
    _time(now)
    if now < max(record["clock_high_water"], record["bridge_checked_at"], record["bridge_applied_at"],
                 record["projection"]["source_updated_at"]):
        raise files.ContextError("clock_invalid")
    return now < record["projection"]["source_updated_at"] + TTL_SECONDS


def publish(store, binding, publisher_id, projection, expected_previous_revision, now, *, enabled=False):
    """CAS-publish one source conclusion from the separately configured publisher.

    `None` is the expected predecessor only for the first publication. A repeat
    of identical bytes/revision is idempotent. A revision cannot change meaning.
    """
    if enabled is not True:
        return _result("disabled", "disabled")
    try:
        now, binding = _time(now), _binding(binding)
        if _text(publisher_id) != binding["publisher_id"]:
            raise files.ContextError("publisher_mismatch")
        if expected_previous_revision is not None:
            _revision(expected_previous_revision)
        projection = _projection(projection)
        if projection["source_updated_at"] > now:
            raise files.ContextError("clock_invalid")
        scope = _authority(store)
        with files.locked(store, create=True, source=True) as directory:
            _authority(store, scope)
            raw = files.read(directory, _SOURCE, MAX_BYTES)
            old = _source_record(raw) if raw is not None else None
            if old is not None:
                if old["scope"] != scope:
                    raise files.ContextError("scope_mismatch")
                if old["binding"] != binding:
                    raise files.ContextError("binding_mismatch")
                current = old["projection"]["source_revision"]
                if projection["source_revision"] == current:
                    if projection != old["projection"]:
                        return _result("conflict", "revision_conflict", source_revision=current)
                    # Idempotency cannot silently resume an explicitly paused source.
                    return _result("unchanged", "unchanged", source_revision=current,
                                   source_digest=old["digest"], active=old["active"])
                if expected_previous_revision != current:
                    return _result("conflict", "revision_conflict", source_revision=current)
                if projection["source_revision"] < current:
                    return _result("conflict", "revision_conflict", source_revision=current)
                if projection["source_updated_at"] < old["projection"]["source_updated_at"]:
                    return _result("conflict", "older_source", source_revision=current)
                if now < old["published_at"]:
                    raise files.ContextError("clock_invalid")
            elif expected_previous_revision is not None:
                return _result("conflict", "revision_conflict", source_revision=None)
            record = dict(schema_version=1, scope=scope, binding=binding, projection=projection,
                          digest=_digest(projection), published_at=now, active=True)
            _source_record(record)
            _authority(store, scope)
            files.write(directory, _SOURCE, record, MAX_BYTES)
        return _result("published", "published", source_revision=projection["source_revision"],
                       source_digest=record["digest"])
    except Exception as error:
        return _failure(error)


def pause(store, binding, publisher_id, expected_previous_revision, now, *, enabled=False):
    """Explicitly pause a configured source without renewing its conclusion/TTL."""
    if enabled is not True:
        return _result("disabled", "disabled")
    try:
        now, binding = _time(now), _binding(binding)
        if _text(publisher_id) != binding["publisher_id"]:
            raise files.ContextError("publisher_mismatch")
        expected_previous_revision = _revision(expected_previous_revision)
        scope = _authority(store)
        with files.locked(store, source=True) as directory:
            _authority(store, scope)
            raw = files.read(directory, _SOURCE, MAX_BYTES)
            if raw is None:
                return _result("unavailable", "source_missing")
            old = _source_record(raw)
            if old["scope"] != scope:
                raise files.ContextError("scope_mismatch")
            if old["binding"] != binding:
                raise files.ContextError("binding_mismatch")
            if old["projection"]["source_revision"] != expected_previous_revision:
                return _result("conflict", "revision_conflict", source_revision=old["projection"]["source_revision"])
            if now < old["published_at"]:
                raise files.ContextError("clock_invalid")
            if old["active"]:
                _authority(store, scope)
                files.write(directory, _SOURCE, dict(old, active=False), MAX_BYTES)
        return _result("paused", "source_paused", source_revision=expected_previous_revision)
    except Exception as error:
        return _failure(error)


def _source_snapshot(store):
    try:
        with files.locked(store, source=True) as directory:
            raw = files.read(directory, _SOURCE, MAX_BYTES)
        if raw is None:
            return None, "source_missing"
        return _source_record(raw), None
    except Exception as error:
        code = getattr(error, "code", "storage_invalid")
        if code == "storage_busy":
            return None, "storage_busy"
        return None, "source_invalid"


def _public(record, status="available", reason=None):
    projection = dict(record["binding"], **record["projection"],
                      source_digest=record["source_digest"],
                      bridge_checked_at=record["bridge_checked_at"],
                      bridge_applied_at=record["bridge_applied_at"],
                      observed_source_revision=record["observed_source_revision"])
    result = dict(schema_version=1, status=status, projection=projection)
    if reason is not None:
        result["reason"] = reason
    if len(files.encode(result)) > MAX_BYTES:
        raise files.ContextError("cache_too_large")
    return result


def _refresh(store, binding, scope, now, *, enabled=False):
    """Check one fixed same-host source, then atomically apply its safe cache.

    Failed checks retain the last successful `bridge_checked_at`. Known bad or
    unapplied source data is explicitly pending, never presented as up to date.
    """
    if enabled is not True:
        return _result("disabled", "disabled")
    revision = None
    source = None
    try:
        now, binding, scope = _time(now), _binding(binding), _scope(scope)
        _authority(store, scope)
        source, source_error = _source_snapshot(store)
        if source_error == "storage_busy":
            return _result("skipped", "storage_busy")
        # Fix the already-observed source facts before acquiring the separate
        # cache lock. Cache contention cannot erase a known revocation, pause
        # or version waiting to be applied. Validate ownership before exposing
        # even this limited observation.
        if source is not None:
            if source["scope"] != scope:
                raise files.ContextError("scope_mismatch")
            if source["binding"] != binding:
                raise files.ContextError("binding_mismatch")
            projection = source["projection"]
            if now < source["published_at"] or now < projection["source_updated_at"]:
                raise files.ContextError("clock_invalid")
            revision = projection["source_revision"]
        with files.locked(store, create=True) as directory:
            _authority(store, scope)
            raw = files.read(directory, _CACHE, MAX_BYTES)
            marker = _tombstone_record(raw) if raw is not None and raw.get("tombstone") is True else None
            old = _cache_record(raw) if raw is not None and marker is None else None
            if marker is not None and now < marker["clock_high_water"]:
                raise files.ContextError("clock_invalid")
            if old is not None:
                if old["scope"] != scope:
                    raise files.ContextError("scope_mismatch")
                if old["binding"] != binding:
                    raise files.ContextError("binding_mismatch")
                if not _live(old, now):
                    marker = _retire(old, now, "source_expired")
                    files.write(directory, _CACHE, marker, MAX_BYTES)
                    old = None
            if source_error:
                status = "update_pending" if source_error == "source_invalid" else "unavailable"
                if old is not None:
                    changed = dict(old, sync_status=status, observed_source_revision=None)
                    _public(changed, status, source_error)
                    files.write(directory, _CACHE, changed, MAX_BYTES)
                return _result(status, source_error)
            projection, revision = source["projection"], source["projection"]["source_revision"]
            if projection["fact_status"] == "revoked":
                files.write(directory, _CACHE, _retire(source, now, "source_revoked", marker), MAX_BYTES)
                return _result("revoked", "source_revoked", source_revision=revision)
            if now >= projection["source_updated_at"] + TTL_SECONDS:
                files.write(directory, _CACHE, _retire(source, now, "source_expired", marker), MAX_BYTES)
                return _result("expired", "source_expired", source_revision=revision)
            if marker is not None and _retired_source(marker, source):
                return _result("expired", "source_expired", source_revision=revision)
            if not source["active"]:
                if old is not None:
                    files.write(directory, _CACHE, dict(old, sync_status="paused",
                                observed_source_revision=revision), MAX_BYTES)
                return _result("paused", "source_paused", source_revision=revision)
            # An older source file must not roll back an already applied cache.
            if old is not None and (revision < old["projection"]["source_revision"]
                    or projection["source_updated_at"] < old["projection"]["source_updated_at"]):
                files.write(directory, _CACHE, dict(old, sync_status="update_pending",
                            observed_source_revision=revision), MAX_BYTES)
                return _result("update_pending", "older_source", source_revision=revision)
            if (old is not None and revision == old["projection"]["source_revision"]
                    and source["digest"] != old["source_digest"]):
                files.write(directory, _CACHE, dict(old, sync_status="update_pending",
                            observed_source_revision=revision), MAX_BYTES)
                return _result("update_pending", "revision_conflict", source_revision=revision)
            same = old is not None and source["digest"] == old["source_digest"]
            cache = dict(schema_version=1, scope=scope, binding=binding, projection=projection,
                         source_digest=source["digest"], bridge_checked_at=now,
                         bridge_applied_at=old["bridge_applied_at"] if same else now,
                         sync_status="applied", observed_source_revision=revision, clock_high_water=now)
            try:
                _cache_record(cache)
                _public(cache)
                _authority(store, scope)
                files.write(directory, _CACHE, cache, MAX_BYTES)
            except files.ContextError as error:
                if old is not None and error.code in ("cache_too_large", "storage_too_large"):
                    files.write(directory, _CACHE, dict(old, sync_status="update_pending",
                                observed_source_revision=revision), MAX_BYTES)
                    return _result("update_pending", "cache_too_large", source_revision=revision)
                raise
        return _result("unchanged" if same else "applied", "unchanged" if same else "applied",
                       source_revision=revision, source_digest=source["digest"])
    except Exception as error:
        if revision is not None:
            # Revocation/pause/expiry is already authoritative even when its
            # bridge marker or removal cannot be written. Never expose the
            # previous text through the same-Store observation in that case.
            if source["projection"]["fact_status"] == "revoked":
                return _result("revoked", "source_revoked", source_revision=revision)
            if now >= source["projection"]["source_updated_at"] + TTL_SECONDS:
                return _result("expired", "source_expired", source_revision=revision)
            if not source["active"]:
                return _result("paused", "source_paused", source_revision=revision)
            return _result("update_pending", getattr(error, "code", "storage_invalid"), source_revision=revision)
        return _failure(error)


def refresh(store, binding, scope, now, *, enabled=False):
    """Check the source and retain this call's finite result for same-Store read.

    In particular, inability to write an application/pending marker must not
    make the immediately following read present the old cache as newly checked.
    This in-memory observation contains no source text and creates no file.
    A new process must perform its own refresh to observe current source state.
    """
    result = _refresh(store, binding, scope, now, enabled=enabled)
    if enabled is True:
        try:
            store._cloud_context_observation = dict(scope=_scope(scope), binding=_binding(binding),
                                                     at=_time(now), result=dict(result))
        except Exception:
            pass
    return result


def _observation(store, scope, binding, now):
    observation = getattr(store, "_cloud_context_observation", None)
    if (type(observation) is not dict or observation.get("scope") != scope
            or binding is not None and observation.get("binding") != _binding(binding)):
        return None
    if now < observation["at"]:
        raise files.ContextError("clock_invalid")
    result = observation["result"]
    status = result["status"]
    if status in ("applied", "unchanged", "disabled"):
        return None
    return dict(status={"revoked": "empty", "skipped": "unavailable"}.get(status, status),
                reason=result["reason"], source_revision=result.get("source_revision"))


def read(store, scope, now, *, enabled=False, binding=None):
    """Return a bounded model-safe projection, with no source body or credentials."""
    empty = dict(schema_version=1, status="empty", projection=None)
    if enabled is not True:
        return dict(empty, status="disabled", reason="disabled")
    observation = None
    try:
        now, scope = _time(now), _scope(scope)
        _authority(store, scope)
        observation = _observation(store, scope, binding, now)
        if observation is not None and observation["status"] in ("paused", "expired", "empty"):
            return dict(empty, status=observation["status"], reason=observation["reason"])
        with files.locked(store) as directory:
            _authority(store, scope)
            raw = files.read(directory, _CACHE, MAX_BYTES)
            if raw is None:
                if observation is not None:
                    return dict(empty, status=observation["status"], reason=observation["reason"])
                return dict(empty, reason="cache_missing")
            if raw.get("tombstone") is True:
                card = _tombstone_card(_tombstone_record(raw), now)
                if observation is not None:
                    return dict(empty, status=observation["status"], reason=observation["reason"])
                return card
            cache = _cache_record(raw)
            if cache["scope"] != scope:
                raise files.ContextError("scope_mismatch")
            if binding is not None and cache["binding"] != _binding(binding):
                raise files.ContextError("binding_mismatch")
            if not _live(cache, now):
                return dict(empty, status="expired", reason="source_expired")
            if observation is not None:
                if observation["source_revision"] is not None:
                    cache = dict(cache, observed_source_revision=observation["source_revision"])
                return _public(cache, observation["status"], observation["reason"])
            status = cache["sync_status"]
            if status == "paused":
                return dict(empty, status="paused", reason="source_paused")
            reason = {"unavailable": "source_unavailable", "update_pending": "source_invalid"}.get(status)
            return _public(cache, "available" if status == "applied" else status, reason)
    except Exception as error:
        if observation is not None:
            return dict(empty, status=observation["status"],
                        reason=getattr(error, "code", "storage_invalid"))
        result = _failure(error)
        return dict(empty, **result)


def maintain(store, scope, now):
    """Clear bridge text, retaining bounded hash-only anti-rollback controls."""
    try:
        now = _time(now)
        if scope is not None:
            scope = _scope(scope)
            try:
                _authority(store, scope)
            except files.ContextError:
                scope = None
        removed, changed = [], False
        with files.locked(store) as directory:
            if directory is None:
                return _result("unchanged", "unchanged", removed=[])
            if scope is not None:
                try:
                    _authority(store, scope)
                except files.ContextError:
                    scope = None
            if files.remove(directory, _TEMP):
                removed.append(_TEMP)
            updated = None
            try:
                raw = files.read(directory, _CACHE, MAX_BYTES)
                if raw is not None and raw.get("tombstone") is True:
                    marker = _tombstone_record(raw)
                    updated = dict(marker, clock_high_water=max(now, marker["clock_high_water"]))
                    if now < marker["clock_high_water"]:
                        updated["reason"] = "clock_invalid"
                    if updated == marker:
                        updated = None
                elif raw is not None:
                    cache = _cache_record(raw)
                    reason = None
                    if scope is None:
                        reason = "scope_unavailable"
                    elif cache["scope"] != scope:
                        reason = "scope_mismatch"
                    else:
                        try:
                            if not _live(cache, now):
                                reason = "source_expired"
                        except files.ContextError:
                            reason = "clock_invalid"
                    if reason is not None:
                        updated = _retire(cache, now, reason)
                        changed = True
                    elif now > cache["clock_high_water"]:
                        updated = dict(cache, clock_high_water=now)
            except files.ContextError:
                # Parsing/size failure can retire private canonical content
                # without loading any more bytes. A write failure below must
                # propagate, never replace a known high water with a lower one.
                updated = _retire(None, now, "storage_invalid", scope=scope)
                changed = True
            if updated is not None:
                files.write(directory, _CACHE, updated, MAX_BYTES)
            # Only scratch inodes are physically unlinked. Canonical text is
            # atomically replaced with the hash-only marker in the same file.
            if files.remove(directory, _TEMP):
                removed.append(_TEMP)
        cleaned = bool(changed or removed)
        return _result("cleaned" if cleaned else "unchanged", "cleaned" if cleaned else "unchanged", removed=removed)
    except Exception as error:
        return _failure(error)
