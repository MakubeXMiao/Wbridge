"""Owned, stdio-only Codex app-server client. No WeChat state or delivery access.

The public adapter drives poll() while a durable job is active. This module never
resumes a recent session, retries a turn, persists credentials, or auto-approves.
"""
import hashlib
import json
import os
import queue
import re
import select
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ProtocolError(Exception):
    """Fixed diagnostic code only; never include child stderr or prompt text."""


@dataclass(frozen=True)
class CodexConfig:
    executable: str
    workspace_root: str
    startup_timeout: float = 4.0
    stop_grace: float = 2.0
    approval_timeout: float = 120.0
    max_frame_bytes: int = 2 * 1024 * 1024
    model: Optional[str] = None


@dataclass(frozen=True)
class SessionRef:
    scope: str
    thread_id: str
    cwd: str
    session_id: Optional[str] = None


@dataclass(frozen=True)
class RunRequest:
    scope: str
    execution_id: str
    attempt_id: str
    text: str
    cwd: str
    timeout: float
    session: Optional[SessionRef] = None
    local_images: tuple = ()


def _identifier(value):
    return isinstance(value, str) and 0 < len(value) <= 256 and not any(ord(c) < 32 for c in value)


def _safe_summary(value):
    """Approval display only, not a log. Full action is bound by SHA256."""
    text = str(value or "")
    text = re.sub(r"(?i)(bearer\s+)[^\s'\"]+", r"\1[redacted]", text)
    text = re.sub(r"(?i)((?:token|password|secret|api[_-]?key)\s*[=:]\s*)[^\s'\"]+", r"\1[redacted]", text)
    text = re.sub(r"https?://[^\s'\"]+", "[url]", text)
    return "".join(c if ord(c) >= 32 else " " for c in text)[:1200]


def child_environment():
    # Existing local auth is used by Codex itself; no key/token is read here.
    names = ("HOME", "PATH", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "CODEX_HOME", "SYSTEMROOT")
    return {name: os.environ[name] for name in names if name in os.environ}


class StdioTransport:
    def __init__(self, config, cwd, overrides=()):
        self.config = config
        self.frames = queue.Queue(maxsize=256)
        self.closed = threading.Event()
        self.overflow = threading.Event()
        self.stderr_bytes = 0
        self.write_deadline = None
        argv = [config.executable, "app-server", "--listen", "stdio://"]
        for value in overrides:
            argv += ["-c", value]
        try:
            self.process = subprocess.Popen(argv, cwd=cwd, env=child_environment(),
                                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, start_new_session=True)
        except (OSError, ValueError):
            raise ProtocolError("launch_failed") from None
        self.readers = [threading.Thread(target=self._read_stdout, daemon=True),
                        threading.Thread(target=self._drain_stderr, daemon=True)]
        for reader in self.readers:
            reader.start()
        os.set_blocking(self.process.stdin.fileno(), False)

    def _enqueue(self, frame):
        try:
            self.frames.put_nowait(frame)
        except queue.Full:
            self.overflow.set()

    def _read_stdout(self):
        try:
            while not self.closed.is_set():
                line = self.process.stdout.readline(self.config.max_frame_bytes + 1)
                if not line:
                    self._enqueue(ProtocolError("stream_closed"))
                    return
                if len(line) > self.config.max_frame_bytes or not line.endswith(b"\n"):
                    self._enqueue(ProtocolError("invalid_frame"))
                    return
                try:
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError()
                except (ValueError, UnicodeError):
                    self._enqueue(ProtocolError("invalid_json"))
                    return
                self._enqueue(value)
        except (OSError, ValueError):
            if not self.closed.is_set():
                self._enqueue(ProtocolError("read_failed"))

    def _drain_stderr(self):
        try:
            while not self.closed.is_set():
                chunk = self.process.stderr.read(4096)
                if not chunk:
                    return
                self.stderr_bytes += len(chunk)
        except (OSError, ValueError):
            pass

    def send(self, frame):
        try:
            data = json.dumps(frame, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
            if len(data) > self.config.max_frame_bytes:
                raise ProtocolError("outgoing_frame_too_large")
            fd = self.process.stdin.fileno()
            deadline = time.monotonic() + self.config.startup_timeout
            if self.write_deadline is not None:
                deadline = min(deadline, self.write_deadline)
            while data:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([], [fd], [], remaining)[1]:
                    raise ProtocolError("write_timeout")
                try:
                    sent = os.write(fd, data)
                    data = data[sent:]
                except BlockingIOError:
                    continue
        except (OSError, ValueError):
            raise ProtocolError("write_failed") from None

    def receive(self, timeout):
        if self.overflow.is_set():
            raise ProtocolError("event_overflow")
        try:
            frame = self.frames.get(timeout=max(0, timeout))
        except queue.Empty:
            return None
        if isinstance(frame, ProtocolError):
            raise frame
        return frame

    def close(self):
        """Stop only our owned process group, including descendants after root exit."""
        self.closed.set()
        proc = self.process
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                break
            except OSError:
                break
            deadline = time.monotonic() + self.config.stop_grace
            while time.monotonic() < deadline:
                proc.poll()
                try:
                    os.killpg(proc.pid, 0)
                except ProcessLookupError:
                    break
                except OSError:
                    break
                time.sleep(0.01)
            else:
                continue
            break
        try:
            proc.wait(timeout=self.config.stop_grace)
        except subprocess.TimeoutExpired:
            pass
        for reader in self.readers:
            reader.join(timeout=0.1)
        pipes = [proc.stdin]
        if not self.readers[0].is_alive():
            pipes.append(proc.stdout)
        if not self.readers[1].is_alive():
            pipes.append(proc.stderr)
        for pipe in pipes:
            try:
                pipe.close()
            except (OSError, ValueError):
                pass
        try:
            os.killpg(proc.pid, 0)
            return False
        except ProcessLookupError:
            return True
        except OSError:
            return False


class CodexRun:
    """Single-owner driver; call start/poll/answer/cancel from one supervisor.

    SessionRef.scope must be the full account+instance+conversation isolation key
    supplied by the public scheduler. Approval answers require the frozen binding.
    """
    APPROVALS = {
        "item/commandExecution/requestApproval": "command",
        "item/fileChange/requestApproval": "file_change",
    }

    def __init__(self, config, request):
        self.config = config
        self.request = request
        self._validate()
        self.transport = None
        self.thread_id = None
        self.session_id = None
        self.turn_id = None
        self.result = None
        self.events = []
        self.approvals = {}
        self._answered = set()
        self._items = {}
        self._file_changes = {}
        self._backlog = []
        self._rpc = 0
        self._ignored_responses = set()
        self._started = False
        self._stop_reason = None
        self._stop_deadline = None
        self._deadline = None

    def _validate(self):
        r, c = self.request, self.config
        if not all(_identifier(v) for v in (r.scope, r.execution_id, r.attempt_id)):
            raise ValueError("invalid_identity")
        if not isinstance(r.text, str) or not r.text.strip() or not 0 < r.timeout <= 86400:
            raise ValueError("invalid_input_or_timeout")
        root, cwd = Path(c.workspace_root), Path(r.cwd)
        if not root.is_absolute() or not cwd.is_absolute() or not cwd.is_dir():
            raise ValueError("invalid_workspace")
        root, cwd = root.resolve(), cwd.resolve()
        if root == Path(root.anchor) or (cwd != root and root not in cwd.parents):
            raise ValueError("workspace_outside_root")
        self.cwd = str(cwd)
        if not Path(c.executable).is_absolute() or not os.access(c.executable, os.X_OK):
            raise ValueError("executable_unavailable")
        if min(c.startup_timeout, c.stop_grace, c.approval_timeout) <= 0:
            raise ValueError("invalid_limits")
        if r.session and (r.session.scope != r.scope or r.session.cwd != self.cwd or
                          not _identifier(r.session.thread_id)):
            raise ValueError("session_scope_mismatch")

    def _emit(self, kind, **data):
        self.events.append(dict(kind=kind, execution_id=self.request.execution_id,
                                attempt_id=self.request.attempt_id, **data))

    def _send_request(self, method, params):
        self._rpc += 1
        rpc_id = "wbridge-%d" % self._rpc
        self.transport.send({"id": rpc_id, "method": method, "params": params})
        return rpc_id

    def _call(self, method, params):
        rpc_id = self._send_request(method, params)
        deadline = min(self._deadline, self._startup_deadline)
        while time.monotonic() < deadline:
            frame = self.transport.receive(min(0.1, deadline - time.monotonic()))
            if frame is None:
                continue
            if frame.get("id") == rpc_id and "method" not in frame:
                if "error" in frame or not isinstance(frame.get("result"), dict):
                    raise ProtocolError("rpc_failed")
                return frame["result"]
            if len(self._backlog) >= 256:
                raise ProtocolError("event_overflow")
            self._backlog.append(frame)
        raise ProtocolError("startup_timeout")

    def start(self):
        if self._started:
            raise ValueError("already_started")
        self._started = True
        self._deadline = time.monotonic() + self.request.timeout
        self._startup_deadline = time.monotonic() + self.config.startup_timeout
        try:
            self._start_transport()
            config_response = self._call("config/read", {"includeLayers": True, "cwd": self.cwd})
            self._verify_rules(config_response)
            effective = config_response.get("config", {})
            servers = effective.get("mcp_servers", {})
            if not isinstance(servers, dict):
                raise ProtocolError("invalid_runtime_config")
            if any(not isinstance(v, dict) or v.get("enabled", True) for v in servers.values()):
                names = list(servers)
                if not self.transport.close():
                    raise ProtocolError("process_stop_unconfirmed")
                self._backlog.clear()
                self._start_transport(names)
                config_response = self._call("config/read", {"includeLayers": True, "cwd": self.cwd})
                self._verify_rules(config_response)
                effective = config_response.get("config", {})
            self._verify_config(effective)
            params = {"cwd": self.cwd, "sandbox": "workspace-write",
                      "approvalPolicy": "on-request", "approvalsReviewer": "user"}
            if self.config.model:
                params["model"] = self.config.model
            if self.request.session:
                params.update(threadId=self.request.session.thread_id, excludeTurns=True)
                response = self._call("thread/resume", params)
            else:
                params["ephemeral"] = False
                response = self._call("thread/start", params)
            thread = response.get("thread", {})
            self.thread_id = thread.get("id")
            self.session_id = thread.get("sessionId")
            if self.session_id is not None and not _identifier(self.session_id):
                raise ProtocolError("invalid_session_id")
            if not _identifier(self.thread_id) or (self.request.session and
                    self.thread_id != self.request.session.thread_id):
                raise ProtocolError("thread_mismatch")
            if thread.get("cwd") != self.cwd:
                raise ProtocolError("thread_cwd_mismatch")
            if thread.get("status", {}).get("type") != "idle":
                raise ProtocolError("thread_not_idle")
            if self.request.session and self.request.session.session_id and self.session_id != self.request.session.session_id:
                raise ProtocolError("session_mismatch")
            if response.get("approvalPolicy") != "on-request" or response.get("approvalsReviewer") != "user":
                raise ProtocolError("approval_policy_mismatch")
            sandbox = response.get("sandbox", {})
            if sandbox.get("type") != "workspaceWrite" or sandbox.get("networkAccess", False) is not False:
                raise ProtocolError("sandbox_mismatch")
            self._emit("session", thread_id=self.thread_id, session_id=self.session_id, cwd=self.cwd)
            inputs = [{"type": "text", "text": self.request.text, "text_elements": []}]
            inputs.extend({"type": "localImage", "path": path} for path in self.request.local_images)
            response = self._call("turn/start", {"threadId": self.thread_id,
                "input": inputs,
                "cwd": self.cwd, "approvalPolicy": "on-request", "approvalsReviewer": "user",
                "sandboxPolicy": {"type": "workspaceWrite", "writableRoots": [self.cwd],
                    "networkAccess": False, "excludeTmpdirEnvVar": True, "excludeSlashTmp": True}})
            turn = response.get("turn", {})
            self.turn_id = turn.get("id")
            if not _identifier(self.turn_id) or turn.get("status") != "inProgress":
                raise ProtocolError("invalid_turn_start")
            self._emit("started", thread_id=self.thread_id, turn_id=self.turn_id)
            self.transport.write_deadline = self._deadline
        except (ProtocolError, KeyError, TypeError, ValueError, OSError, AttributeError) as exc:
            code = str(exc) if isinstance(exc, ProtocolError) else "invalid_start_response"
            self._finish("outcome_unknown" if self.thread_id else "failed", code)
        return self

    ISOLATION_FEATURES = ("hooks", "plugins", "apps", "memories", "shell_snapshot", "multi_agent",
                          "browser_use", "computer_use", "skill_mcp_dependency_install")

    def _start_transport(self, servers=()):
        self.transport = StdioTransport(self.config, self.cwd, self._config_overrides(servers))
        self.transport.write_deadline = self._startup_deadline
        self._call("initialize", {"clientInfo": {"name": "wbridge_codex", "version": "1"},
                                  "capabilities": {"experimentalApi": True}})
        self.transport.send({"method": "initialized", "params": {}})

    @classmethod
    def _config_overrides(cls, servers=()):
        values = ['approval_policy="on-request"', 'approvals_reviewer="user"',
                'sandbox_mode="workspace-write"', 'sandbox_workspace_write.network_access=false',
                'sandbox_workspace_write.exclude_slash_tmp=true',
                'sandbox_workspace_write.exclude_tmpdir_env_var=true',
                'shell_environment_policy.inherit="none"', 'project_doc_max_bytes=0',
                'project_doc_fallback_filenames=[]', 'notify=[]', 'web_search="disabled"']
        values += ['features.%s=false' % feature for feature in cls.ISOLATION_FEATURES]
        for name in servers:
            if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", name):
                raise ProtocolError("invalid_mcp_name")
            values.append('mcp_servers.%s.enabled=false' % name)
        return values

    @classmethod
    def _verify_config(cls, effective):
        if not isinstance(effective, dict):
            raise ProtocolError("invalid_runtime_config")
        features = effective.get("features", {})
        servers = effective.get("mcp_servers", {})
        if (any(features.get(name) is not False for name in cls.ISOLATION_FEATURES) or
                not isinstance(servers, dict) or
                any(not isinstance(v, dict) or v.get("enabled", True) for v in servers.values()) or
                effective.get("project_doc_max_bytes") != 0 or
                effective.get("approval_policy") != "on-request" or
                effective.get("approvals_reviewer") != "user" or
                effective.get("sandbox_mode") != "workspace-write" or
                effective.get("notify") or
                effective.get("shell_environment_policy", {}).get("set")):
            raise ProtocolError("runtime_isolation_unverified")

    def _verify_rules(self, response):
        # Existing allow rules can bypass sandbox approval. Do not interpret a
        # Starlark rules file or silently inherit it. Inspect metadata only.
        layers = response.get("layers")
        if not isinstance(layers, list):
            raise ProtocolError("config_layers_unknown")
        directories = {Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "rules",
                       Path("/etc/codex/rules")}
        for layer in layers:
            if layer.get("disabledReason"):
                continue
            source = layer.get("name", {})
            if source.get("file"):
                directories.add(Path(source["file"]).parent / "rules")
            elif source.get("dotCodexFolder"):
                directories.add(Path(source["dotCodexFolder"]) / "rules")
            elif source.get("type") not in ("sessionFlags",):
                raise ProtocolError("managed_rules_unverifiable")
        for directory in directories:
            pending, scanned = [directory], 0
            while pending:
                scanned += 1
                if scanned > 128:
                    raise ProtocolError("rules_scan_limit")
                try:
                    with os.scandir(pending.pop()) as entries:
                        for entry in entries:
                            if entry.name.endswith(".rules") and entry.is_file() and entry.stat().st_size:
                                raise ProtocolError("inherited_rules_present")
                            if entry.is_dir():
                                if entry.is_symlink():
                                    raise ProtocolError("rules_symlink_unverifiable")
                                pending.append(Path(entry.path))
                except FileNotFoundError:
                    continue
                except OSError:
                    raise ProtocolError("rules_unreadable") from None

    def poll(self, timeout=0.1):
        if not self._started:
            raise ValueError("not_started")
        if self.result is None:
            try:
                self._check_deadlines()
                if self.result is None:
                    frame = self._backlog.pop(0) if self._backlog else self.transport.receive(min(max(timeout, 0), 0.1))
                    if frame is not None:
                        self._handle(frame)
                    self._check_deadlines()
            except (ProtocolError, KeyError, TypeError, ValueError, AttributeError) as exc:
                code = str(exc) if isinstance(exc, ProtocolError) else "invalid_event"
                self._finish("outcome_unknown", code)
        events, self.events = self.events, []
        return events

    def _check_deadlines(self):
        now = time.monotonic()
        if self._stop_reason:
            if now >= self._stop_deadline:
                self._finish(self._stop_reason, "interrupt_unconfirmed")
            return
        if now >= self._deadline:
            self.cancel("timed_out")
        elif any(now >= a["deadline"] for a in self.approvals.values()):
            self.cancel("timed_out")

    def _bound(self, params):
        return params.get("threadId") == self.thread_id and params.get("turnId") == self.turn_id

    def _handle(self, frame):
        method = frame.get("method")
        if method is None:
            if frame.get("id") in self._ignored_responses:
                self._ignored_responses.remove(frame["id"])
                return
            raise ProtocolError("unexpected_rpc_response")
        params = frame.get("params", {})
        if not isinstance(params, dict):
            raise ProtocolError("invalid_event")
        if "id" in frame:
            self._approval(frame)
            return
        if method == "turn/completed":
            turn = params.get("turn", {})
            if params.get("threadId") != self.thread_id or turn.get("id") != self.turn_id:
                raise ProtocolError("turn_mismatch")
            if self._stop_reason:
                self._finish(self._stop_reason, None if turn.get("status") == "interrupted" else "cancel_raced_terminal",
                             interrupted=turn.get("status") == "interrupted")
                return
            if turn.get("status") != "completed" or turn.get("error") is not None:
                self._finish("failed", "turn_failed")
                return
            if self.approvals:
                raise ProtocolError("unresolved_approval")
            for item in turn.get("items", []):
                self._accept_item(item)
            finals = [v["text"] for v in self._items.values() if v.get("phase") == "final_answer"]
            unphased = [v["text"] for v in self._items.values() if v.get("phase") is None]
            final = "\n\n".join(finals) if finals else (unphased[-1] if unphased else "")
            if not final.strip():
                self._finish("failed", "empty_final")
            else:
                self._finish("completed", None, text=final)
        elif method in ("item/completed", "item/started", "error", "item/agentMessage/delta"):
            if not self._bound(params):
                raise ProtocolError("event_scope_mismatch")
            if method == "item/completed":
                self._accept_item(params.get("item", {}))
            elif method == "item/started" and params.get("item", {}).get("type") == "fileChange":
                item = params["item"]
                changes = item.get("changes")
                if _identifier(item.get("id")) and isinstance(changes, list) and 0 < len(changes) <= 128:
                    self._file_changes[item["id"]] = changes
                    if len(self._file_changes) > 128 or len(json.dumps(self._file_changes)) > self.config.max_frame_bytes:
                        raise ProtocolError("too_many_file_changes")
                self._emit("progress", source=method)
            elif method == "error" and not params.get("willRetry", False):
                self._finish("failed", "backend_error")
            else:
                # Do not forward raw tool output, reasoning, or unvalidated text.
                self._emit("progress", source=method)
        elif method == "serverRequest/resolved":
            rpc_id = params.get("requestId")
            if rpc_id in self.approvals:
                # Server clearing an unanswered request invalidates its UI token.
                self.approvals.pop(rpc_id)
                self._answered.add(rpc_id)
                self._emit("approval_expired", approval_id=str(rpc_id))

    def _accept_item(self, item):
        if not isinstance(item, dict):
            raise ProtocolError("invalid_item")
        if item.get("type") != "agentMessage":
            return
        item_id, text = item.get("id"), item.get("text")
        if not _identifier(item_id) or not isinstance(text, str):
            raise ProtocolError("invalid_final")
        value = {"text": text, "phase": item.get("phase")}
        if item_id in self._items and self._items[item_id] != value:
            raise ProtocolError("conflicting_item")
        self._items[item_id] = value
        if len(self._items) > 128 or sum(len(v["text"]) for v in self._items.values()) > self.config.max_frame_bytes:
            raise ProtocolError("result_too_large")

    def _approval(self, frame):
        method, params, rpc_id = frame["method"], frame.get("params", {}), frame["id"]
        if not isinstance(rpc_id, (str, int)) or isinstance(rpc_id, bool):
            raise ProtocolError("invalid_approval_id")
        if not self._bound(params) or not _identifier(params.get("itemId")):
            raise ProtocolError("approval_scope_mismatch")
        if method not in self.APPROVALS:
            self.transport.send({"id": rpc_id, "error": {"code": -32601, "message": "unsupported interaction"}})
            raise ProtocolError("unsupported_interaction")
        decisions = params.get("availableDecisions")
        if (params.get("additionalPermissions") or params.get("networkApprovalContext") or
                params.get("kind", "command") != "command" or
                decisions is not None and (not isinstance(decisions, list) or
                    "accept" not in decisions or "decline" not in decisions)):
            self.transport.send({"id": rpc_id, "error": {"code": -32602, "message": "unsupported permission scope"}})
            raise ProtocolError("unsupported_permission_scope")
        if method == "item/fileChange/requestApproval":
            if params.get("grantRoot") or not self._file_changes.get(params["itemId"]):
                self.transport.send({"id": rpc_id, "result": {"decision": "decline"}})
                raise ProtocolError("unsupported_file_permission")
        if method == "item/commandExecution/requestApproval" and (
                not isinstance(params.get("command"), str) or not params["command"].strip() or
                not isinstance(params.get("cwd"), str) or not Path(params["cwd"]).is_absolute()):
            self.transport.send({"id": rpc_id, "result": {"decision": "decline"}})
            raise ProtocolError("missing_action_details")
        if self._stop_reason:
            self.transport.send({"id": rpc_id, "result": {"decision": "cancel"}})
            return
        action = {"request": frame, "changes": self._file_changes.get(params["itemId"])}
        digest = hashlib.sha256(json.dumps(action, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if rpc_id in self._answered:
            raise ProtocolError("replayed_approval")
        if rpc_id in self.approvals:
            if self.approvals[rpc_id]["action_digest"] != digest:
                raise ProtocolError("changed_approval")
            return
        if len(self.approvals) >= 16:
            raise ProtocolError("too_many_approvals")
        deadline = min(self._deadline, time.monotonic() + self.config.approval_timeout)
        summary = params.get("command") if self.APPROVALS[method] == "command" else params.get("reason")
        if method == "item/fileChange/requestApproval":
            summary = json.dumps(self._file_changes[params["itemId"]], ensure_ascii=False, sort_keys=True)
        else:
            summary = "%s: %s (cwd: %s)" % (params.get("kind", "command"), summary, params.get("cwd", ""))
        if not summary or _safe_summary(summary) != summary or params.get("environmentId"):
            self.transport.send({"id": rpc_id, "result": {"decision": "decline"}})
            raise ProtocolError("unsafe_action_summary")
        approval = dict(approval_id=rpc_id, scope=self.request.scope, execution_id=self.request.execution_id,
                        attempt_id=self.request.attempt_id, thread_id=self.thread_id, turn_id=self.turn_id,
                        item_id=params["itemId"], action_digest=digest, deadline=deadline,
                        kind=self.APPROVALS[method], summary=_safe_summary(summary),
                        command_kind=params.get("kind", "command"),
                        cwd=_safe_summary(params.get("cwd")), grant_root=_safe_summary(params.get("grantRoot")))
        self.approvals[rpc_id] = approval
        self._emit("approval_required", approval=dict(approval))

    def is_approval_pending(self, approval_id):
        approval = self.approvals.get(approval_id)
        return bool(self.result is None and not self._stop_reason and approval is not None
                    and time.monotonic() < approval["deadline"])

    def answer(self, approval_id, decision, *, scope, execution_id, attempt_id, action_digest):
        approval = self.approvals.get(approval_id)
        if self.result is not None or self._stop_reason or approval is None:
            return False
        if (scope, execution_id, attempt_id, action_digest) != (self.request.scope, self.request.execution_id,
                self.request.attempt_id, approval["action_digest"]):
            return False
        if time.monotonic() >= approval["deadline"] or decision not in ("allow_once", "deny"):
            return False
        try:
            self.transport.send({"id": approval_id, "result": {"decision": "accept" if decision == "allow_once" else "decline"}})
        except ProtocolError:
            self._finish("outcome_unknown", "approval_write_failed")
            return False
        self.approvals.pop(approval_id)
        self._answered.add(approval_id)
        self._emit("approval_answered", approval_id=str(approval_id), decision=decision)
        return True

    def cancel(self, reason="cancelled"):
        if reason not in ("cancelled", "timed_out"):
            raise ValueError("invalid_cancel_reason")
        if self.result is not None or self._stop_reason:
            return
        self._stop_reason = reason
        self._stop_deadline = time.monotonic() + self.config.stop_grace
        if self.transport:
            self.transport.write_deadline = self._stop_deadline
        try:
            for rpc_id in self.approvals:
                self.transport.send({"id": rpc_id, "result": {"decision": "cancel"}})
            self.approvals.clear()
            if self.turn_id:
                self._ignored_responses.add(self._send_request("turn/interrupt", {
                    "threadId": self.thread_id, "turnId": self.turn_id}))
            else:
                self._finish(reason, "no_turn_to_interrupt")
        except ProtocolError:
            self._finish(reason, "interrupt_write_failed")

    def _finish(self, status, error, text=None, interrupted=False):
        if self.result is not None:
            return
        if self.transport and self.approvals:
            # Best-effort explicit denial of both exposed and queued requests.
            # One shared bounded budget; process cleanup remains authoritative.
            self.transport.write_deadline = time.monotonic() + self.config.stop_grace
            for rpc_id in self.approvals:
                try:
                    self.transport.send({"id": rpc_id, "result": {"decision": "cancel"}})
                except ProtocolError:
                    break
        exit_before_cleanup = self.transport.process.poll() if self.transport else None
        if status == "completed" and exit_before_cleanup not in (None, 0):
            status, error, text = "outcome_unknown", "unexpected_exit", None
        stopped = self.transport.close() if self.transport else True
        if not stopped and status == "completed":
            status, error, text = "outcome_unknown", "process_stop_unconfirmed", None
        self.approvals.clear()
        self.result = dict(status=status, error=error, text=text if status == "completed" else None,
                           scope=self.request.scope, execution_id=self.request.execution_id,
                           attempt_id=self.request.attempt_id, thread_id=self.thread_id,
                           session_id=self.session_id, turn_id=self.turn_id, cwd=self.cwd,
                           stop_confirmed=stopped, interrupt_confirmed=interrupted,
                           exit_code=exit_before_cleanup,
                           cleanup_exit_code=self.transport.process.returncode if self.transport else None)
        self._emit("terminal", result=dict(self.result))

    def close(self):
        if self.result is None:
            self._finish("outcome_unknown", "driver_closed")
