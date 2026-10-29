"""Codex adapter for the public backend v1 dict contract.

Uses the locally authenticated Codex app-server over owned stdio. No transport,
Store, outbox, account credentials, or worker lease access belongs here.
"""
import base64
import copy
import hashlib
import json
import math
import os
import time
from collections import deque
from pathlib import Path

from ._codex_protocol import CodexConfig, CodexRun, RunRequest, SessionRef, _identifier


MAX_FINAL_CHARS = 64000
MAX_SESSION_CHARS = 1024


CAPABILITIES = {
    "protocol": "app-server-stdio-v2",
    "session": "explicit_scoped_thread",
    "approval": ["command_once", "file_change_once"],
    "unsupported_interactions": ["permissions_turn_or_session", "user_questions", "mcp_elicitation", "directory_grant"],
    "live_generation_verified": False,
    "wechat_delivery_verified": False,
}


def _scope(request):
    fields = ("account_scope", "sender_key", "backend_id", "backend_instance_ref", "conversation_key")
    values = [request.get(name) for name in fields]
    if any(not isinstance(v, str) or not v.strip() or len(v) > 512 for v in values):
        raise ValueError("codex_invalid_scope")
    return hashlib.sha256(json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _session_token(scope, thread, cwd, session):
    if not _identifier(thread) or session is not None and not _identifier(session):
        raise ValueError("codex_session_identity_invalid")
    # Keep both provider IDs verbatim. Only the already-known cwd is digested.
    data = json.dumps([scope, hashlib.sha256(cwd.encode()).hexdigest(), thread, session],
                      separators=(",", ":"), ensure_ascii=False).encode()
    token = "wbc2." + base64.urlsafe_b64encode(data).decode().rstrip("=")
    if len(token) > MAX_SESSION_CHARS:
        raise ValueError("codex_session_ref_too_large")
    return token


def _read_session(value, scope, cwd):
    if value is None:
        return None
    try:
        if not isinstance(value, str) or value[:5] not in ("wbc1.", "wbc2.") or len(value) > MAX_SESSION_CHARS:
            raise ValueError()
        data = json.loads(base64.b64decode(value[5:] + "=" * (-len(value[5:]) % 4), altchars=b"-_", validate=True))
        if value.startswith("wbc2."):
            if not isinstance(data, list) or len(data) != 4:
                raise ValueError()
            owner, cwd_digest, thread, session = data
            if owner != scope or cwd_digest != hashlib.sha256(cwd.encode()).hexdigest():
                raise ValueError()
        else:
            if data["v"] != 1 or data["scope"] != scope or data["cwd"] != cwd:
                raise ValueError()
            thread, session = data["thread"], data.get("session")
        if not _identifier(thread) or session is not None and not _identifier(session):
            raise ValueError()
        return SessionRef(scope, thread, cwd, session)
    except (KeyError, ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("codex_session_scope_mismatch") from None


class CodexBackend:
    def __init__(self, config):
        self.config = copy.deepcopy(config)
        command = config.get("command")
        if not isinstance(command, list) or len(command) != 1 or not isinstance(command[0], str):
            raise ValueError("codex_command_requires_one_absolute_executable")
        if not Path(command[0]).is_absolute() or not Path(command[0]).is_file() or not os.access(command[0], os.X_OK):
            raise ValueError("codex_executable_unavailable")
        if config.get("model") is not None and (not isinstance(config["model"], str) or
                not config["model"].strip() or len(config["model"]) > 128):
            raise ValueError("codex_invalid_model")
        if config.get("execution_policy") != {"approval": "explicit", "sandbox": "workspace-write"}:
            raise ValueError("codex_explicit_workspace_policy_required")
        cwd = config.get("cwd")
        if not isinstance(cwd, str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
            raise ValueError("codex_workspace_unavailable")
        self.cwd = str(Path(cwd).resolve())
        roots = config.get("attachment_roots", [self.cwd])
        if not isinstance(roots, list) or not all(isinstance(p, str) and Path(p).is_absolute() for p in roots):
            raise ValueError("codex_attachment_roots_invalid")
        self.attachment_roots = [Path(p).resolve() for p in roots]
        if any(p == Path(p.anchor) for p in self.attachment_roots):
            raise ValueError("codex_attachment_root_too_broad")
        self.protocol_config = CodexConfig(command[0], self.cwd, startup_timeout=3.5, stop_grace=0.15,
            approval_timeout=120.0, model=config.get("model"))
        self.run = None
        self.request = None
        self._seq = 0
        self._approval_ids = {}
        self._approval_queue = deque()
        self._stopping = False
        self._terminal = False

    def _input(self, messages):
        if not isinstance(messages, list) or not 1 <= len(messages) <= 100:
            raise ValueError("codex_invalid_messages")
        texts, images = [], []
        for message in messages:
            if not isinstance(message, dict) or not isinstance(message.get("text", ""), str):
                raise ValueError("codex_invalid_message")
            text = message.get("text", "")
            attachments = message.get("attachments", [])
            if not isinstance(attachments, list) or len(attachments) > 32:
                raise ValueError("codex_invalid_attachments")
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    raise ValueError("codex_invalid_attachment")
                if attachment.get("status") == "failed":
                    text += "\n[Attachment unavailable; do not infer its contents.]"
                    continue
                name = attachment.get("path")
                if attachment.get("status") != "available" or not isinstance(name, str) or not Path(name).is_absolute():
                    raise ValueError("codex_invalid_attachment")
                path = Path(name).resolve()
                if not path.is_file() or not any(root == path or root in path.parents for root in self.attachment_roots):
                    raise ValueError("codex_attachment_outside_allowed_roots")
                if path.stat().st_size > 100 * 1024 * 1024:
                    raise ValueError("codex_attachment_too_large")
                with path.open("rb") as stream:
                    head = stream.read(12)
                is_image = (head.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a")) or
                            head.startswith(b"RIFF") and head[8:12] == b"WEBP")
                if is_image:
                    images.append(str(path))
                else:
                    text += "\n[Local attachment path: " + json.dumps(str(path), ensure_ascii=False) + "]"
            texts.append(text)
        text = "\n\n".join(texts)
        if not text.strip() and images:
            text = "Please inspect the attached image(s)."
        if len(text) > 1000000 or len(images) > 32:
            raise ValueError("codex_input_too_large")
        return text, tuple(images)

    def start(self, request):
        if self.request is not None:
            raise ValueError("codex_driver_already_used")
        request = copy.deepcopy(request)
        scope = _scope(request)
        if request.get("schema_version") != 1 or request.get("backend_id") != "codex":
            raise ValueError("codex_invalid_request_version_or_backend")
        if request.get("backend_instance_ref") != self.config.get("instance_id"):
            raise ValueError("codex_instance_mismatch")
        if request.get("execution_policy") != self.config["execution_policy"]:
            raise ValueError("codex_policy_mismatch")
        if request.get("cwd") != self.cwd:
            raise ValueError("codex_workspace_mismatch")
        deadline = request.get("deadline_at")
        if isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
            raise ValueError("codex_invalid_deadline")
        timeout = deadline - time.time()
        if not 0 < timeout <= 86400:
            raise ValueError("codex_expired_or_excessive_deadline")
        text, images = self._input(request.get("messages"))
        session = _read_session(request.get("session_id"), scope, self.cwd)
        run = CodexRun(self.protocol_config, RunRequest(scope, request.get("execution_id"),
            request.get("attempt_id"), text, self.cwd, timeout, session, images))
        self.request = request
        self.run = run
        run.start()

    def _event(self, kind, **fields):
        self._seq += 1
        return dict(event_id="codex-%d" % self._seq, execution_id=self.request["execution_id"],
                    attempt_id=self.request["attempt_id"], type=kind, **fields)

    def poll(self):
        if self.run is None:
            raise ValueError("codex_not_started")
        events = []
        until = time.monotonic() + 0.2
        for _ in range(32):
            incoming = self.run.poll(0)
            for event in incoming:
                kind = event["kind"]
                if self._terminal:
                    continue
                if kind == "started":
                    events.append(self._event("started"))
                elif kind == "progress":
                    events.append(self._event("progress", text=event["source"]))
                elif kind == "approval_required":
                    approval = event["approval"]
                    if not self._stopping:
                        self._approval_queue.append(approval)
                elif kind == "approval_expired":
                    # There is no v1 event that can withdraw an exposed approval.
                    # Stop the attempt instead of promoting a queued request while
                    # the public scheduler still considers the first one pending.
                    self.cancel()
                elif kind == "terminal":
                    self._terminal = True
                    self._clear_approvals()
                    result = event["result"]
                    status = result["status"]
                    if status == "completed":
                        try:
                            if len(result["text"]) > MAX_FINAL_CHARS:
                                raise ValueError("codex_final_too_large")
                            token = _session_token(self.run.request.scope, result["thread_id"], self.cwd, result["session_id"])
                            events.append(self._event("final", text=result["text"], session_id=token))
                        except ValueError as exc:
                            # Provider completion is known, but its result cannot
                            # be represented by v1. Never publish a truncated final.
                            events.append(self._event("failed", error_code=str(exc), side_effect_state="possible"))
                    elif status == "cancelled" and result["stop_confirmed"]:
                        events.append(self._event("cancelled", side_effect_state="possible"))
                    elif status == "failed" or status == "timed_out" and result["stop_confirmed"]:
                        events.append(self._event("failed", error_code="codex_" + (result["error"] or status),
                            side_effect_state="possible" if result["thread_id"] else "none"))
                    else:
                        events.append(self._event("unknown", error_code="codex_" + (result["error"] or status),
                                                  side_effect_state="unknown"))
            if self._terminal or len(events) >= 64 or time.monotonic() >= until:
                break
        if not self._terminal and not self._stopping and not self._approval_ids:
            while self._approval_queue:
                approval = self._approval_queue.popleft()
                rpc = approval["approval_id"]
                if not self.run.is_approval_pending(rpc):
                    continue
                provider_id = ("i:" if isinstance(rpc, int) else "s:") + str(rpc)
                if len(provider_id) > 256:
                    provider_id = "h:" + hashlib.sha256(json.dumps(rpc).encode()).hexdigest()
                self._approval_ids[provider_id] = rpc
                events.append(self._event("approval_requested", approval_id=provider_id,
                    action_summary=approval["summary"], action_digest=approval["action_digest"],
                    expires_at=time.time() + max(0, approval["deadline"] - time.monotonic())))
                break
        return events

    def respond_approval(self, approval_id, decision, action_digest):
        if self.run is None or decision not in ("allow", "deny") or approval_id not in self._approval_ids:
            raise ValueError("codex_invalid_approval_response")
        accepted = self.run.answer(self._approval_ids[approval_id], "allow_once" if decision == "allow" else "deny",
            scope=self.run.request.scope, execution_id=self.request["execution_id"],
            attempt_id=self.request["attempt_id"], action_digest=action_digest)
        if not accepted:
            raise ValueError("codex_stale_or_mismatched_approval")
        del self._approval_ids[approval_id]

    def cancel(self):
        self._stopping = True
        self._clear_approvals()
        if self.run:
            self.run.cancel()

    def _clear_approvals(self):
        self._approval_ids.clear()
        self._approval_queue.clear()

    def close(self):
        self._stopping = True
        self._clear_approvals()
        if self.run:
            self.run.close()


def create_backend(config):
    return CodexBackend(config)
