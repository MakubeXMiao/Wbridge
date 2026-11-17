"""Public PEPE input/config boundary; contains no HTTP or credential access.

An unverified policy stops dispatch. Explicit native delegation is a different
contract: PEPE retains its configured tools/permissions and native confirmations;
the bridge neither certifies those controls nor supplies WeChat tool approvals.
"""
import copy
import math
import re
import time
import urllib.parse

from .backend_state import digest
from .backends.contract import BackendError


_KEYS = {"provider", "transport", "instance_id", "base_url", "credential_env",
         "tenant", "principal", "queue", "queue_ownership", "instructions",
         "execution_policy"}
_POLICY = {"mode": "text_only", "enforcement": "unverified"}
_NATIVE_POLICY = {"mode": "native_delegate", "approval": "platform_native"}


def _text(value, maximum):
    if (not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > maximum
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        return False
    try:
        value.encode("utf-8")
    except UnicodeError:
        return False
    return True


def validate_pepe_config(selected):
    """Validate declarations only; never mistake them for remote enforcement."""
    if not isinstance(selected, dict) or set(selected) != _KEYS:
        raise BackendError("pepe requires the supported HTTP configuration fields")
    if selected["provider"] != "pepe" or selected["transport"] != "http":
        raise BackendError("pepe provider/transport must be pepe/http")
    for field in ("instance_id", "tenant", "principal", "queue"):
        if not _text(selected[field], 200):
            raise BackendError("pepe %s must be a bounded identity" % field)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", selected["queue"]):
        raise BackendError("pepe queue must be a bounded wire identifier")
    if (not isinstance(selected["credential_env"], str)
            or not re.fullmatch(r"[A-Z_][A-Z0-9_]{0,127}", selected["credential_env"])):
        raise BackendError("pepe credential_env must name an environment variable")
    if selected["queue_ownership"] != "single_store":
        raise BackendError("pepe requires a single Store queue owner")
    if (not isinstance(selected["instructions"], str)
            or not selected["instructions"].strip()
            or len(selected["instructions"]) > 16000
            or "\x00" in selected["instructions"]):
        raise BackendError("pepe instructions must be bounded text")
    if selected["execution_policy"] not in (_POLICY, _NATIVE_POLICY):
        raise BackendError("pepe execution_policy requires unverified text_only or explicit native delegation")
    try:
        selected["instructions"].encode("utf-8")
    except UnicodeError:
        raise BackendError("pepe instructions must be valid Unicode") from None
    url = selected["base_url"]
    if not _text(url, 2048) or any(c.isspace() for c in url) or "\\" in url:
        raise BackendError("pepe base_url must be a bounded HTTP service URL")
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or port == 0 or parsed.path not in ("", "/")
                or parsed.scheme not in ("https", "http")
                or (parsed.scheme == "http" and parsed.hostname not in
                    ("localhost", "127.0.0.1", "::1"))):
            raise ValueError()
    except ValueError:
        raise BackendError("pepe base_url requires HTTPS or loopback HTTP, without credentials/query/fragment") from None
    result = copy.deepcopy(selected)
    result["base_url"] = urllib.parse.urlunsplit(parsed).rstrip("/")
    return result


def require_pepe_dispatch_policy(selected):
    """Validate explicit delegation, without claiming platform policy proof.

    The native mode requires a separately authorized operator opt-in. This
    function does not enable it, obtain credentials, or grant PEPE permissions.
    """
    validate_pepe_config(selected)
    if selected["execution_policy"] != _NATIVE_POLICY:
        raise BackendError("pepe_execution_policy_unverified")


def validate_pepe_input(job):
    records = job.get("records")
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict):
        raise BackendError("pepe_single_chat_text_required")
    record = records[0]
    provenance = record.get("input_provenance")
    if (not isinstance(provenance, dict)
            or type(provenance.get("schema_version")) is not int
            or provenance["schema_version"] != 1
            or provenance.get("single_chat") is not True):
        raise BackendError("pepe_single_chat_text_required")
    kinds = provenance.get("item_types")
    if not isinstance(kinds, list) or not kinds or any(type(k) is not int or k != 1 for k in kinds):
        raise BackendError("pepe_single_chat_text_required")
    # Reject references as well as download outcomes; never inspect local paths.
    if any(record.get(field) for field in (
            "media_paths", "media_failed", "media_failed_count", "attachment_refs")):
        raise BackendError("pepe_attachments_unsupported")
    text = record.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > 64000 or "\x00" in text:
        raise BackendError("pepe_text_invalid")
    try:
        text.encode("utf-8")
    except UnicodeError:
        raise BackendError("pepe_text_invalid") from None
    return [{"message_id": str(record["message_id"]), "text": text, "attachments": []}]


def prepare_pepe_request(state, job, selected, now, clock=time.time):
    """Freeze one text task before handing any I/O to the adapter."""
    selected = validate_pepe_config(selected)
    messages = validate_pepe_input(job)
    deadline = job.get("deadline_at")
    if (isinstance(deadline, bool) or not isinstance(deadline, (int, float))
            or not math.isfinite(deadline) or not math.isfinite(now)):
        raise BackendError("pepe_deadline_invalid")
    ttl = min(86400, math.floor(deadline - now) - 5)
    if ttl < 60:
        raise BackendError("pepe_deadline_too_short")
    input_digest = digest(messages)
    key = "wbr-" + digest([job[field] for field in (
        "account_scope", "sender_key", "backend_instance_ref", "execution_id", "attempt_id")]
        + [input_digest])
    binding = {"service": selected["base_url"], "tenant": selected["tenant"],
               "principal": selected["principal"], "queue": selected["queue"]}
    submit = {"queue": selected["queue"], "instructions": selected["instructions"],
              "payload": {"messages": [{"role": "user", "content": messages[0]["text"]}]},
              "max_attempts": 1, "ttl_seconds": ttl, "idempotency_key": key}
    from .remote_tasks import RemoteTaskJournal
    journal = RemoteTaskJournal(state, job["execution_id"], job["attempt_id"],
                                job["backend_instance_ref"], clock=clock)
    snapshot = journal.prepare(binding, submit, input_digest)
    return {"transport": "http", "execution_policy": copy.deepcopy(selected["execution_policy"]),
            "session_id": None, "messages": messages,
            "remote_task": snapshot, "remote_task_journal": journal}
