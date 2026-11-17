"""Current-PEPE adapter for the unmodified Dot2API asynchronous task API.

This module never sends WeChat, writes the public job database, or falls back
to another model. Provider task IDs are not conversation/session IDs.
"""

import copy
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse

from .contract import BackendError


OPERATION_LIMIT = 128 * 1024  # Dot2API models.Input.bounded, ensure_ascii=True.
FINAL_LIMIT = 64000
TASK_STATES = frozenset(("queued", "running", "completed", "failed", "cancelled", "expired"))
POLL_SECONDS = 1.0


def _identifier(value, name, limit=256):
    if (not isinstance(value, str) or not value.strip() or len(value) > limit
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise BackendError("pepe_invalid_" + name)
    return value


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except (ValueError, OverflowError):
        return False


def _canonical(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False,
                      sort_keys=True, separators=(",", ":"))


def _wire_id(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise BackendError("pepe_invalid_" + name)
    return value


def validate_snapshot(request, snapshot, config, expected_state=None):
    """Validate 01's frozen submission without generating a new key or TTL."""
    if (not isinstance(request, dict) or type(request.get("schema_version")) is not int
            or request["schema_version"] != 1):
        raise BackendError("pepe_invalid_request")
    if (request.get("backend_id") != "pepe" or request.get("session_id") is not None
            or request.get("transport") != "http"):
        raise BackendError("pepe_unsupported_session_or_backend")
    for key in (
            "account_scope", "sender_key", "backend_instance_ref", "execution_id", "attempt_id"):
        _identifier(request.get(key), key)
    if request["backend_instance_ref"] != config.get("instance_id"):
        raise BackendError("pepe_instance_mismatch")
    if not _finite(request.get("deadline_at")):
        raise BackendError("pepe_invalid_deadline")
    records = request.get("messages")
    if not isinstance(records, list) or len(records) != 1:
        raise BackendError("pepe_single_text_input_required")
    messages = []
    for record in records:
        if not isinstance(record, dict):
            raise BackendError("pepe_invalid_input")
        if record.get("attachments", []) != []:
            raise BackendError("pepe_attachments_unsupported")
        text = record.get("text")
        if not isinstance(text, str) or not text.strip():
            raise BackendError("pepe_empty_input")
        try:
            text.encode("utf-8")
        except UnicodeError:
            raise BackendError("pepe_invalid_input") from None
        _identifier(record.get("message_id"), "message_id", 1024)
        messages.append({"role": "user", "content": text})
    keys = {"schema_version", "execution_id", "attempt_id", "backend_instance_ref",
            "binding", "submit", "submit_digest", "input_digest", "state", "task_id"}
    if (not isinstance(snapshot, dict) or set(snapshot) != keys
            or type(snapshot["schema_version"]) is not int or snapshot["schema_version"] != 1):
        raise BackendError("pepe_invalid_snapshot")
    for key in ("execution_id", "attempt_id", "backend_instance_ref"):
        if snapshot[key] != request[key]:
            raise BackendError("pepe_snapshot_identity_mismatch")
    binding = {"service": _endpoint(config.get("base_url")),
               "tenant": _identifier(config.get("tenant"), "tenant"),
               "principal": _identifier(config.get("principal"), "principal"),
               "queue": _wire_id(config.get("queue"), "queue")}
    if snapshot["binding"] != binding:
        raise BackendError("pepe_snapshot_binding_mismatch")
    if snapshot["state"] not in ("prepared", "submitting", "bound"):
        raise BackendError("pepe_invalid_snapshot_state")
    if expected_state is not None and snapshot["state"] != expected_state:
        raise BackendError("pepe_snapshot_not_prepared")
    if snapshot["state"] == "bound":
        _wire_id(snapshot["task_id"], "task_id")
    elif snapshot["task_id"] is not None:
        raise BackendError("pepe_invalid_snapshot_task")
    for key in ("submit_digest", "input_digest"):
        if not isinstance(snapshot[key], str) or not re.fullmatch(r"[0-9a-f]{64}", snapshot[key]):
            raise BackendError("pepe_invalid_snapshot_digest")
    body = snapshot["submit"]
    if (not isinstance(body, dict) or set(body) != {"queue", "instructions", "payload",
            "idempotency_key", "max_attempts", "ttl_seconds"}
            or body["queue"] != binding["queue"]
            or body["instructions"] != config.get("instructions")
            or not isinstance(body["instructions"], str) or not body["instructions"].strip()
            or body["payload"] != {"messages": messages}
            or type(body["max_attempts"]) is not int or body["max_attempts"] != 1
            or type(body["ttl_seconds"]) is not int or not 60 <= body["ttl_seconds"] <= 86400
            or not isinstance(body["idempotency_key"], str)
            or not re.fullmatch(r"wbr-[0-9a-f]{64}", body["idempotency_key"])):
        raise BackendError("pepe_invalid_frozen_submission")
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != snapshot["submit_digest"]:
        raise BackendError("pepe_snapshot_digest_mismatch")
    # Match upstream validation, which counts escaped Unicode, including its
    # default separators; a small UTF-8 HTTP body can still exceed this limit.
    if len(json.dumps(body, allow_nan=False).encode("utf-8")) > OPERATION_LIMIT:
        raise BackendError("pepe_input_too_large")
    return copy.deepcopy(snapshot)


def extract_text(result):
    """Accept plain text, never serialize arbitrary JSON/tool output as final."""
    if isinstance(result, dict) and set(result) == {"content"}:
        result = result["content"]
    if not isinstance(result, str) or not result.strip() or len(result) > FINAL_LIMIT:
        raise BackendError("pepe_invalid_final")
    try:
        result.encode("utf-8")
    except UnicodeError:
        raise BackendError("pepe_invalid_final") from None
    return result


def parse_task(response, expected_task_id=None, expected_queue=None, validate_result=True):
    """Validate the original Service.public envelope; no principal is echoed.

    The caller must additionally compare instructions/payload/max_attempts to
    its journal and retain the configured service/tenant/principal binding.
    """
    if not isinstance(response, dict) or not isinstance(response.get("task"), dict):
        raise BackendError("pepe_invalid_task_response")
    task = response["task"]
    task_id = task.get("id")
    _wire_id(task_id, "task_id")
    if expected_task_id is not None and task_id != expected_task_id:
        raise BackendError("pepe_task_mismatch")
    _wire_id(task.get("queue"), "queue")
    if expected_queue is not None and task["queue"] != expected_queue:
        raise BackendError("pepe_queue_mismatch")
    if type(task.get("status")) is not str or task["status"] not in TASK_STATES:
        raise BackendError("pepe_invalid_task_state")
    for key in ("attempts", "max_attempts", "revision"):
        if type(task.get(key)) is not int or task[key] < (0 if key == "attempts" else 1):
            raise BackendError("pepe_invalid_task_metadata")
    if task["attempts"] > task["max_attempts"]:
        raise BackendError("pepe_invalid_task_metadata")
    if not isinstance(task.get("instructions"), str) or not isinstance(task.get("payload"), dict):
        raise BackendError("pepe_invalid_task_echo")
    if validate_result and task["status"] == "completed":
        extract_text(task.get("result"))
    return copy.deepcopy(task)


# An isolated, short-lived stdlib HTTP worker makes DNS/connect/read bounded
# by the parent's actual wait. Closing it says nothing about PEPE execution.
# Credentials and body travel through stdin, never argv, env or diagnostics.
_HTTP_PROGRAM = r'''
import json, sys, urllib.error, urllib.request
LIMIT = 524288
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None
def pairs(values):
    out = {}
    for key, value in values:
        if key in out:
            raise ValueError('duplicate')
        out[key] = value
    return out
def constant(value):
    raise ValueError('constant')
try:
    raw = sys.stdin.buffer.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise ValueError('size')
    args = json.loads(raw)
    body = args['body']
    data = None if body is None else json.dumps(body, ensure_ascii=True, allow_nan=False,
                 sort_keys=True, separators=(',', ':')).encode('ascii')
    req = urllib.request.Request(args['url'], data=data, method=args['method'],
              headers={'Authorization': 'Bearer ' + args['credential'],
                       'Content-Type': 'application/json', 'Accept': 'application/json'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(req, timeout=args['timeout']) as reply:
            payload = reply.read(LIMIT + 1)
            if len(payload) > LIMIT:
                raise ValueError('size')
            value = json.loads(payload, object_pairs_hook=pairs, parse_constant=constant)
            answer = {'status': reply.status, 'data': value}
    except urllib.error.HTTPError as exc:
        answer = {'status': exc.code, 'data': None}
    encoded = json.dumps(answer, ensure_ascii=True, allow_nan=False).encode('ascii')
    if len(encoded) > 4 * LIMIT:
        raise ValueError('size')
    sys.stdout.buffer.write(encoded)
except Exception:
    sys.stdout.buffer.write(b'{"status":0,"data":null}')
'''


class TransportError(BackendError):
    def __init__(self, code, status=0):
        self.code, self.status = code, status
        super().__init__(code)


def _endpoint(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise BackendError("pepe_invalid_endpoint")
    try:
        parts = urllib.parse.urlsplit(value)
        valid = (parts.scheme == "https" or parts.scheme == "http" and
                 parts.hostname in ("127.0.0.1", "::1", "localhost"))
        if (not valid or not parts.hostname or parts.username is not None
                or parts.password is not None or parts.query or parts.fragment
                or parts.path not in ("", "/") or parts.port == 0
                or any(ord(c) <= 32 or ord(c) == 127 for c in value)):
            raise ValueError()
    except (ValueError, TypeError):
        raise BackendError("pepe_invalid_endpoint") from None
    return value.rstrip("/")


def _credential_env(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name):
        raise BackendError("pepe_invalid_credential_ref")
    value = os.environ.get(name)
    if (not isinstance(value, str) or not 1 <= len(value) <= 4096
            or any(ord(c) < 33 or ord(c) > 126 for c in value)):
        raise BackendError("pepe_invalid_credential_ref")
    return value


def _require_execution_policy(config):
    # This is the v1.1 candidate contract, not evidence of a remote sandbox.
    # Native delegation preserves PEPE's own permissions/confirmations; it
    # does not implement per-tool approval through WeChat.
    policy = config.get("execution_policy") if isinstance(config, dict) else None
    if policy == {"mode": "native_delegate", "approval": "platform_native"}:
        return
    if policy == {"mode": "text_only", "enforcement": "unverified"}:
        raise BackendError("pepe_execution_policy_unverified")
    raise BackendError("pepe_invalid_execution_policy")


class HTTPTransport:
    """One held HTTP child at a time; no retry, redirect, proxy or shell."""

    def __init__(self, base_url, credential, clock=time.monotonic):
        self.base_url, self._credential = _endpoint(base_url), credential
        self._clock = clock
        self._lock = threading.Lock()
        self._closed, self._child = False, None

    def request(self, method, path, body=None, timeout=5):
        if (method not in ("GET", "POST") or not isinstance(path, str)
                or not re.fullmatch(r"/v1/(tasks(?:/[A-Za-z0-9_.:%-]+)?|tools/cancel_task)", path)
                or not _finite(timeout) or not 0 < timeout <= 30):
            raise TransportError("pepe_invalid_http_operation")
        args = {"url": self.base_url + path, "method": method, "body": body,
                "credential": self._credential, "timeout": timeout}
        payload = _canonical(args).encode("ascii")
        if len(payload) > 524288:
            raise TransportError("pepe_input_too_large")
        deadline = self._clock() + timeout
        child = None
        try:
            with self._lock:
                if self._closed or self._child is not None:
                    raise TransportError("pepe_http_closed_or_busy")
                child = subprocess.Popen([sys.executable, "-I", "-S", "-c", _HTTP_PROGRAM],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    env={"PATH": os.defpath, "LANG": "C", "LC_ALL": "C"})
                self._child = child
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise TransportError("pepe_http_timeout")
            output, _ = child.communicate(payload, timeout=remaining)
            if child.returncode != 0 or len(output) > 2097152:
                raise TransportError("pepe_http_unknown")
            answer = json.loads(output)
            status = answer.get("status") if isinstance(answer, dict) else None
            if type(status) is not int or status < 100 or status > 599:
                raise TransportError("pepe_http_unknown")
            if not 200 <= status < 300:
                raise TransportError("pepe_http_status", status)
            return answer["data"]
        except TransportError:
            raise
        except subprocess.TimeoutExpired:
            raise TransportError("pepe_http_timeout") from None
        except (OSError, ValueError, KeyError):
            raise TransportError("pepe_http_unknown") from None
        finally:
            if child is not None:
                try:
                    if child.poll() is None:
                        child.kill()
                    child.wait(timeout=2)
                finally:
                    for stream in (child.stdin, child.stdout):
                        if stream is not None:
                            stream.close()
                    with self._lock:
                        if child.returncode is not None and self._child is child:
                            self._child = None

    def close(self):
        with self._lock:
            self._closed = True
            child = self._child
        if child is not None:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=2)


class PepeBackend:
    """One frozen execution, one POST, bounded GETs, no automatic recovery."""

    def __init__(self, config, transport=None, clock=time.time, monotonic=time.monotonic):
        if not isinstance(config, dict):
            raise BackendError("pepe_invalid_config")
        self.config = copy.deepcopy(config)
        self._transport = transport
        self._clock, self._monotonic = clock, monotonic
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._events, self._seq = [], 0
        self._started = self._terminal = self._closed = False
        self._thread = self._request = self._journal = None
        self._task_id = None
        self._deadline_mono = None

    def _emit(self, kind, **fields):
        with self._lock:
            if self._terminal or self._request is None:
                return
            if kind == "final" and (self._closed or self._stop.is_set()):
                return
            self._seq += 1
            identity = self._request["execution_id"] + ":" + self._request["attempt_id"]
            event = {"event_id": "pepe-" + hashlib.sha256((identity + ":" + str(self._seq)).encode()).hexdigest(),
                     "execution_id": self._request["execution_id"],
                     "attempt_id": self._request["attempt_id"], "type": kind,
                     "remote_task_id": self._task_id, "session_id": None}
            event.update(fields)
            self._events.append(event)
            if kind in ("final", "failed", "cancelled", "unknown"):
                self._terminal = True

    def _remaining(self):
        now, mono = self._clock(), self._monotonic()
        if not _finite(now) or not _finite(mono):
            return -1
        return min(self._request["deadline_at"] - now, self._deadline_mono - mono)

    def start(self, request):
        with self._lock:
            if self._started or self._closed:
                raise BackendError("pepe_already_started_or_closed")
            self._started = True
        # Preserve only the required event identity before validation. A bad
        # prepared request is a definite pre-submission failure, not a retry.
        if not isinstance(request, dict):
            raise BackendError("pepe_invalid_request")
        self._request = {key: copy.deepcopy(value) for key, value in request.items()
                         if key != "remote_task_journal"}
        if not all(isinstance(request.get(key), str) and request[key] for key in ("execution_id", "attempt_id")):
            raise BackendError("pepe_invalid_request_identity")
        try:
            _require_execution_policy(self.config)
            if (self.config.get("provider") != "pepe" or self.config.get("transport") != "http"
                    or self.config.get("queue_ownership") != "single_store"
                    or request.get("execution_policy") != self.config.get("execution_policy")):
                raise BackendError("pepe_invalid_config_or_policy")
            frozen = validate_snapshot(request, request.get("remote_task"), self.config, "prepared")
            journal = request.get("remote_task_journal")
            if any(not callable(getattr(journal, name, None)) for name in ("begin_submit", "bind_task", "snapshot")):
                raise BackendError("pepe_missing_journal")
            if journal.snapshot() != frozen:
                raise BackendError("pepe_journal_binding_changed")
            now, mono = self._clock(), self._monotonic()
            if not _finite(now) or not _finite(mono):
                raise BackendError("pepe_invalid_clock")
            self._deadline_mono = mono + request["deadline_at"] - now
            if self._remaining() <= frozen["submit"]["ttl_seconds"]:
                raise BackendError("pepe_frozen_ttl_no_longer_fits")
            if self._transport is None:
                self._transport = HTTPTransport(self.config["base_url"],
                                                _credential_env(self.config.get("credential_env")))
            self._journal = journal
            self._thread = threading.Thread(target=self._run, args=(frozen,), name="pepe-http-execution")
            self._thread.start()
        except BackendError as exc:
            # Only this local pre-dispatch code path may establish none.
            code = str(exc)
            if not re.fullmatch(r"pepe_[a-z_]+", code):
                code = "pepe_invalid_prepared_request"
            self._emit("failed", error_code=code, side_effect_state="none")

    def _match_frozen(self, snapshot, frozen, state):
        value = validate_snapshot(self._request, snapshot, self.config, state)
        wanted = copy.deepcopy(frozen)
        wanted.update(state=state, task_id=value["task_id"])
        if value != wanted:
            raise BackendError("pepe_journal_binding_changed")
        return value

    def _match_task(self, response, frozen, expected_id=None):
        # First retain a verified task identity even if its final text is bad.
        # Result validation belongs after durable bind, before the only final.
        task = parse_task(response, expected_id, frozen["binding"]["queue"], validate_result=False)
        submit = frozen["submit"]
        if any(task[key] != submit[key] for key in ("instructions", "payload", "max_attempts")):
            raise BackendError("pepe_task_echo_mismatch")
        return task

    def _run(self, frozen):
        intent = False
        try:
            if self._stop.is_set():
                return
            if self._remaining() <= frozen["submit"]["ttl_seconds"]:
                self._emit("failed", error_code="pepe_frozen_ttl_no_longer_fits", side_effect_state="none")
                return
            # This durable transition must succeed before the only POST.
            submitted = self._journal.begin_submit()
            intent = True
            self._match_frozen(submitted, frozen, "submitting")
            timeout = min(5.0, self._remaining() - frozen["submit"]["ttl_seconds"])
            if self._stop.is_set() or timeout <= 0:
                self._emit("unknown", error_code="pepe_submission_not_confirmed")
                return
            try:
                response = self._transport.request("POST", "/v1/tasks", submitted["submit"], timeout=timeout)
            except TransportError as exc:
                # Match the public journal fence: after begin_submit no HTTP
                # status automatically releases the durable submitting intent.
                self._emit("unknown", error_code="pepe_auth_rejected_after_intent" if exc.status in (401, 403)
                           else "pepe_submission_unknown")
                return
            task = self._match_task(response, frozen)
            # Even if cancellation/close won the event race, keep a late valid
            # task mapping durably. Binding never reopens the public job.
            bound = self._journal.bind_task(task["id"])
            self._match_frozen(bound, frozen, "bound")
            if bound["task_id"] != task["id"]:
                raise BackendError("pepe_task_mismatch")
            with self._lock:
                self._task_id = task["id"]
            if self._stop.is_set():
                return
            self._emit("started")
            while not self._stop.is_set():
                if self._remaining() <= 0:
                    self._emit("unknown", error_code="pepe_deadline_unknown")
                    return
                if task["status"] == "completed":
                    self._emit("final", text=extract_text(task["result"]))
                    return
                if task["status"] in ("failed", "cancelled", "expired"):
                    self._emit("unknown", error_code="pepe_remote_terminal_not_stop_proof")
                    return
                if self._stop.wait(min(POLL_SECONDS, self._remaining())):
                    return
                timeout = min(5.0, self._remaining())
                if timeout <= 0:
                    self._emit("unknown", error_code="pepe_deadline_unknown")
                    return
                response = self._transport.request("GET", "/v1/tasks/" + urllib.parse.quote(task["id"], safe=""),
                                                   timeout=timeout)
                task = self._match_task(response, frozen, self._task_id)
        except Exception:
            # A journal exception can itself be an ambiguous commit. Do not
            # guess that the transition or provider execution did not happen.
            self._emit("unknown", error_code="pepe_execution_unknown" if intent else "pepe_journal_unknown")

    def poll(self):
        with self._lock:
            result, self._events = self._events[:100], self._events[100:]
            return result

    def respond_approval(self, approval_id, decision, action_digest):
        raise BackendError("pepe_approval_unsupported")

    def cancel(self):
        self._stop.set()
        self._emit("unknown", error_code="pepe_cancel_not_stop_proof")

    def close(self):
        with self._lock:
            self._closed = True
            thread, transport = self._thread, self._transport
        self._stop.set()
        self._emit("unknown", error_code="pepe_close_not_stop_proof")
        if transport is not None:
            transport.close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=7)
            if thread.is_alive():
                raise BackendError("pepe_http_cleanup_unknown")


def create_backend(config):
    _require_execution_policy(config)
    return PepeBackend(config)
