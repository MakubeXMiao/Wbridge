"""Small validation helpers for the dict-based backend v1 boundary."""

import copy
import math
import re


class BackendError(ValueError):
    """A public, credential-free backend configuration or protocol failure."""


EVENT_TYPES = frozenset(("started", "progress", "approval_requested", "final",
                         "failed", "cancelled", "unknown"))
SIDE_EFFECT_STATES = frozenset(("none", "possible", "confirmed", "unknown"))


def _text(value, limit=256):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= limit


def validate_event(event, execution_id, attempt_id):
    """Return an independent copy, rejecting malformed or misrouted events.

    This validates shape only. Deduplication, final arbitration, action binding
    and expiry are durable scheduler responsibilities.
    """
    if not isinstance(event, dict):
        raise BackendError("backend event must be an object")
    if not _text(event.get("event_id")):
        raise BackendError("backend event requires a bounded event_id")
    if (not _text(execution_id) or not _text(attempt_id)
            or event.get("execution_id") != execution_id
            or event.get("attempt_id") != attempt_id):
        raise BackendError("backend event execution/attempt mismatch")
    kind = event.get("type")
    if not isinstance(kind, str) or kind not in EVENT_TYPES:
        raise BackendError("unsupported backend event type")
    if "session_id" in event and event["session_id"] is not None:
        if not _text(event["session_id"], 1024):
            raise BackendError("backend event has invalid session_id")
    if "text" in event and (not isinstance(event["text"], str)
                             or len(event["text"]) > 64000):
        raise BackendError("backend event text is invalid or too long")
    if kind == "final" and not _text(event.get("text"), 64000):
        raise BackendError("backend final requires nonempty text")
    if kind == "approval_requested":
        if not _text(event.get("approval_id")) or not _text(event.get("action_summary"), 2000):
            raise BackendError("backend approval requires an ID and bounded action summary")
        digest = event.get("action_digest")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise BackendError("backend approval requires a SHA256 action digest")
        if "expires_at" in event:
            expiry = event["expires_at"]
            if (isinstance(expiry, bool) or not isinstance(expiry, (int, float))
                    or not math.isfinite(expiry) or expiry <= 0):
                raise BackendError("backend approval expiry is invalid")
    if kind == "failed":
        if not _text(event.get("error_code"), 200):
            raise BackendError("backend failure requires a bounded error code")
        if (not isinstance(event.get("side_effect_state"), str)
                or event["side_effect_state"] not in SIDE_EFFECT_STATES):
            raise BackendError("backend failure requires explicit side-effect state")
    return copy.deepcopy(event)
