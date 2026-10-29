"""Claude Code's SDK stdio control protocol, one explicit session turn per process.

Only execution lives here. The core owns identity, durable approvals, routing and
WeChat delivery. CLI protocol compatibility is pinned in docs/lines/04-cc/.
"""
from dataclasses import dataclass
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid


_SAFE_TOOLS = ("Read", "Glob", "Grep", "Bash", "Write", "Edit")
# Frozen public v1 uses Python character counts, not UTF-8 byte lengths.
_PUBLIC_TEXT_LIMIT = 64000
_TIMEOUT_LIMITS = {"timeout_seconds": 86400, "approval_timeout_seconds": 300,
                   "startup_timeout_seconds": 15, "cancel_grace_seconds": 5}
_CLOSE_TIMEOUT_SECONDS = 20


def _check_timeout(name, value):
    maximum = _TIMEOUT_LIMITS[name]
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not 0 < value <= maximum or not math.isfinite(value)):
        raise ValueError("%s must be finite, positive and <= %s" % (name, maximum))



@dataclass(frozen=True)
class ClaudeCodeConfig:
    workdir: str
    executable: str = "claude"
    env: dict = None
    timeout_seconds: float = 300
    approval_timeout_seconds: float = 120
    startup_timeout_seconds: float = 15
    cancel_grace_seconds: float = 1
    max_output_bytes: int = 4 * 1024 * 1024
    tools: tuple = _SAFE_TOOLS

    def __post_init__(self):
        if not Path(self.workdir).is_absolute() or not Path(self.workdir).is_dir():
            raise ValueError("Claude workdir must be an existing absolute directory")
        if not isinstance(self.executable, str) or not self.executable:
            raise ValueError("Claude executable must be a path or executable name")
        for name in _TIMEOUT_LIMITS:
            _check_timeout(name, getattr(self, name))
        if not isinstance(self.max_output_bytes, int) or self.max_output_bytes < 256:
            raise ValueError("max_output_bytes must be >= 256")
        if not isinstance(self.tools, tuple) or any(t not in _SAFE_TOOLS for t in self.tools):
            raise ValueError("Only the reviewed built-in tool set is supported")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _uuid(value):
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


class ClaudeCodeExecutor:
    """Private executor; config and child environment are trusted local settings."""

    def __init__(self, config):
        self.config = config

    def start(self, prompt, execution_id, attempt_id, session_id=None):
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("A nonempty text prompt is required")
        if len(prompt.encode("utf-8")) > 256 * 1024:
            raise ValueError("Prompt exceeds 256 KiB")
        if not all(isinstance(v, str) and 0 < len(v) <= 256 for v in (execution_id, attempt_id)):
            raise ValueError("Explicit execution_id and attempt_id are required")
        if session_id is not None and not _uuid(session_id):
            raise ValueError("Resume requires an explicit canonical UUID session")
        return ClaudeCodeRun(self.config, prompt, execution_id, attempt_id, session_id)


class ClaudeCodeRun:
    """Nonblocking polling and once-only approvals; owns its entire process group."""

    def __init__(self, config, prompt, execution_id, attempt_id, session_id):
        self.config = config
        self.execution_id, self.attempt_id = execution_id, attempt_id
        self.session_id = session_id or str(uuid.uuid4())
        self._resume = session_id is not None
        self._prompt = prompt
        self._lock = threading.RLock()
        self._done = threading.Event()
        self._events = []
        self._sequence = 0
        self._approvals = {}
        self._seen_requests = set()
        self._tx = bytearray()
        self._final = None
        self._assistant_error = None
        self._outcome = None
        self._stop = None
        self._stop_at = None
        self._initialized = False
        self._session_seen = False
        self._input_sent = False
        self._stdin_closed = False
        self._init_id = str(uuid.uuid4())
        self._rules_id = str(uuid.uuid4())
        self._user_uuid = str(uuid.uuid4())
        self._deadline = time.monotonic() + config.timeout_seconds
        self._interrupt_id = None
        self._process = None
        self._version_probe = None
        self._stderr = bytearray()
        self._worker = threading.Thread(target=self._supervise, name="wbridge-claude", daemon=True)
        self._worker.start()

    def poll(self):
        with self._lock:
            events, self._events = copy.deepcopy(self._events), []
            return events

    def wait(self, timeout=None):
        if not self._done.wait(timeout):
            raise TimeoutError("Claude execution is still running")
        with self._lock:
            return dict(self._outcome)

    def cancel(self):
        with self._lock:
            if self._outcome is not None or self._stop is not None:
                return False
            self._request_stop("cancelled", "cancelled")
            return True

    def approve(self, approval_id, decision, action_hash):
        with self._lock:
            approval = self._approvals.get(approval_id)
            if (decision not in ("allow", "deny") or approval is None or
                    self._stop is not None or self._final is not None or
                    time.monotonic() >= approval["deadline"] or
                    action_hash != approval["action_hash"]):
                return False
            self._approvals.pop(approval_id)
            response = {"behavior": decision, "toolUseID": approval["tool_use_id"]}
            if decision == "allow":
                response["updatedInput"] = approval["tool_input"]
            else:
                response.update(message="User denied this tool call", interrupt=False)
            self._send({"type": "control_response", "response": {
                "subtype": "success", "request_id": approval_id, "response": response}})
            self._emit("approval_closed", approval_id=approval_id, reason=decision)
            return True

    def _emit(self, kind, **data):
        if len(self._events) >= 2048:
            if kind == "result":
                # Completion must remain observable even when progress exhausts the queue.
                self._events = self._events[-2047:]
            else:
                if self._stop is None:
                    self._request_stop("failed", "event_overflow")
                return
        self._sequence += 1
        self._events.append(dict(type=kind, sequence=self._sequence,
                                 execution_id=self.execution_id, attempt_id=self.attempt_id,
                                 session_id=self.session_id, **data))

    def _send(self, message):
        if not self._stdin_closed:
            self._tx.extend((_json(message) + "\n").encode("utf-8"))

    def _close_approvals(self, reason):
        pending = list(self._approvals)
        self._approvals.clear()
        for approval_id in pending:
            self._emit("approval_closed", approval_id=approval_id, reason=reason)

    def _request_stop(self, status, code):
        if self._stop is not None:
            return
        self._stop = (status, code)
        self._stop_at = time.monotonic()
        # Discard queued allows/input before interrupting; never revive stale approval.
        self._tx.clear()
        self._close_approvals(code)
        self._interrupt_id = str(uuid.uuid4())
        self._send({"type": "control_request", "request_id": self._interrupt_id,
                    "request": {"subtype": "interrupt", "cancel_queued": True}})

    def _command(self, executable):
        # Empty sources, safe mode and no MCP avoid loading user/project customizations.
        # Explicit ask rules apply even to normally automatic read tools.
        settings = {"disableAllHooks": True, "permissions": {"ask": list(self.config.tools)}}
        return [executable, "--safe-mode", "--print", "--verbose", "--input-format", "stream-json",
                "--output-format", "stream-json", "--permission-prompt-tool", "stdio",
                "--permission-mode", "default", "--permission-prompts", "host", "--setting-sources", "",
                "--settings", _json(settings), "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}',
                "--tools", ",".join(self.config.tools),
                "--resume" if self._resume else "--session-id", self.session_id]

    def _message(self, message):
        if not isinstance(message, dict) or not isinstance(message.get("type"), str):
            self._request_stop("failed", "protocol_error")
            return
        kind = message["type"]
        incoming_session = message.get("session_id")
        if incoming_session is not None and incoming_session != self.session_id:
            self._request_stop("failed", "session_mismatch")
            return
        if self._stop is not None:
            return  # Late final/permission frames never change a stopped run.
        if kind == "control_response":
            response = message.get("response")
            if not isinstance(response, dict):
                self._request_stop("failed", "protocol_error")
            elif response.get("request_id") == self._init_id:
                if self._initialized or response.get("subtype") != "success":
                    self._request_stop("failed", "initialize_failed")
                    return
                # Do not adopt in-flight permissions from a previous process.
                if response.get("pending_permission_requests") or response.get("pending_user_dialog_requests"):
                    self._request_stop("unknown", "pending_previous_interaction")
                    return
                metadata = response.get("response", {})
                if (not isinstance(metadata, dict) or metadata.get("claude_code_version") != "2.1.309" or
                        metadata.get("current_permission_mode") != "default"):
                    self._request_stop("failed", "unsupported_permission_protocol")
                    return
                if response.get("session_state", metadata.get("session_state")) not in (None, "idle"):
                    self._request_stop("unknown", "pending_previous_interaction")
                    return
                self._initialized = True
                self._send({"type": "control_request", "request_id": self._rules_id,
                            "request": {"subtype": "list_permission_rules"}})
            elif response.get("request_id") == self._rules_id:
                if not self._initialized or self._input_sent or response.get("subtype") != "success":
                    self._request_stop("failed", "permission_policy_mismatch")
                    return
                payload = response.get("response", {})
                state = payload.get("state", {}) if isinstance(payload, dict) else {}
                if not isinstance(state, dict):
                    self._request_stop("failed", "permission_policy_mismatch")
                    return
                rules = state.get("rules", [])
                asks = {r.get("rule") for r in rules if isinstance(r, dict) and
                        r.get("behavior") == "ask" and not r.get("notInEffect")} if isinstance(rules, list) else set()
                directories = state.get("workspaceDirectories")
                if (not isinstance(directories, list) or
                        any(not isinstance(d, dict) or d.get("path") != str(Path(self.config.workdir).resolve()) for d in directories) or
                        state.get("managedOnly") is not False or state.get("errors") or
                        state.get("originalCwd") != str(Path(self.config.workdir).resolve()) or
                        not set(self.config.tools).issubset(asks)):
                    self._request_stop("failed", "permission_policy_mismatch")
                    return
                self._send({"type": "user", "session_id": self.session_id,
                            "uuid": self._user_uuid, "parent_tool_use_id": None, "client_composed": True,
                            "message": {"role": "user", "content": self._prompt}})
                self._input_sent = True
            return
        if kind == "control_cancel_request":
            request_id = message.get("request_id")
            if request_id in self._approvals:
                self._approvals.pop(request_id)
                self._emit("approval_closed", approval_id=request_id, reason="withdrawn")
                self._request_stop("unknown", "approval_withdrawn")
            return
        if kind == "control_request":
            self._permission(message)
            return
        if kind == "system" and message.get("subtype") == "init":
            if incoming_session != self.session_id:
                self._request_stop("failed", "session_mismatch")
                return
            self._session_seen = True
            self._emit("session")
        elif kind == "assistant":
            if message.get("error"):
                # A later success string must not hide an API/auth error frame.
                self._assistant_error = "not_authenticated" if message.get("error") == "authentication_failed" else "backend_error"
            else:
                self._emit("progress", stage="assistant")
        elif kind == "result":
            if self._final is not None or not self._input_sent or incoming_session != self.session_id:
                self._request_stop("failed", "invalid_final")
                return
            if (message.get("user_message_uuid", self._user_uuid) != self._user_uuid or
                    message.get("user_message_uuids", [self._user_uuid]) != [self._user_uuid]):
                self._request_stop("failed", "turn_mismatch")
                return
            if self._approvals:
                self._request_stop("unknown", "final_with_pending_approval")
                return
            self._final = message
            self._final_at = time.monotonic()
        # Unknown non-control frames are ignored, never interpreted as completion.

    def _permission(self, message):
        request_id, request = message.get("request_id"), message.get("request")
        if (not isinstance(request_id, str) or not request_id or len(request_id) > 256 or
                not isinstance(request, dict) or not self._input_sent or self._final is not None):
            self._request_stop("failed", "protocol_error")
            return
        if request_id in self._seen_requests:
            self._request_stop("failed", "duplicate_permission")
            return
        self._seen_requests.add(request_id)
        if (request.get("subtype") != "can_use_tool" or
                request.get("tool_name") not in self.config.tools or request.get("requires_user_interaction")):
            self._request_stop("failed", "unsupported_interaction")
            return
        tool_input = request.get("input")
        if not isinstance(tool_input, dict) or not isinstance(request.get("tool_use_id"), str):
            self._request_stop("failed", "protocol_error")
            return
        # Includes exact execution and session identity, not just the displayed text.
        identity = dict(execution_id=self.execution_id, attempt_id=self.attempt_id,
                        session_id=self.session_id, approval_id=request_id,
                        tool_name=request["tool_name"], tool_use_id=request["tool_use_id"], tool_input=tool_input, cwd=self.config.workdir)
        action_hash = hashlib.sha256(_json(identity).encode("utf-8")).hexdigest()
        ttl = min(self.config.approval_timeout_seconds, max(0, self._deadline - time.monotonic()))
        self._approvals[request_id] = dict(tool_input=copy.deepcopy(tool_input), tool_use_id=request["tool_use_id"], action_hash=action_hash,
                                           deadline=time.monotonic() + ttl)
        self._emit("approval_required", approval_id=request_id, tool_name=request["tool_name"],
                   tool_input=tool_input, tool_use_id=request["tool_use_id"], action_hash=action_hash,
                   expires_at=time.time() + ttl)

    def _finish(self, status, code, exit_code=None, text=""):
        self._close_approvals(code)
        self._outcome = dict(status=status, code=code, text=text, session_id=self.session_id,
                             execution_id=self.execution_id, attempt_id=self.attempt_id,
                             exit_code=exit_code, side_effects_possible=self._input_sent)

    def _resolve(self, exit_code):
        if self._stop is not None:
            self._finish(*self._stop, exit_code=exit_code)
            return
        result = self._final
        if self._assistant_error is not None:
            self._finish("failed", self._assistant_error, exit_code)
            return
        if result is None:
            diagnostic = self._stderr.decode("utf-8", "replace").lower()
            code = "not_authenticated" if "not logged in" in diagnostic else (
                "session_not_found" if "no conversation found" in diagnostic else "missing_final")
            self._finish("unknown" if self._input_sent else "failed", code, exit_code)
            return
        # Do not expose raw backend error messages, which may contain credentials.
        error_text = str(result.get("result", "")) + str(result.get("errors", ""))
        error_result = result.get("is_error") is not False or result.get("subtype") != "success" or bool(result.get("errors"))
        if result.get("api_error_status") == 401 or (error_result and "not logged in" in error_text.lower()):
            self._finish("failed", "not_authenticated", exit_code)
        elif error_result or result.get("api_error_status"):
            self._finish("failed", "backend_error", exit_code)
        elif result.get("terminal_reason") not in (None, "completed") or result.get("stop_reason") not in ("end_turn", "stop_sequence"):
            self._finish("failed", "incomplete_turn", exit_code)
        elif result.get("queued_turn_count", 0) != 0 or result.get("deferred_tool_use"):
            self._finish("unknown", "unfinished_work", exit_code)
        elif exit_code != 0:
            self._finish("unknown", "process_exit", exit_code)
        elif not isinstance(result.get("result"), str) or not result["result"].strip():
            self._finish("failed", "empty_final", exit_code)
        else:
            self._finish("succeeded", "ok", exit_code, result["result"])

    def _signal_group(self, sig):
        if self._process is not None:
            try:
                os.killpg(self._process.pid, sig)
            except ProcessLookupError:
                pass

    def _check_version(self, executable, env):
        """Bound version bytes, time and cancellation before submitting any prompt."""
        probe = subprocess.Popen([executable, "--version"], cwd=self.config.workdir, env=env,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        self._version_probe = probe
        selector = None
        try:
            selector = selectors.DefaultSelector()
            os.set_blocking(probe.stdout.fileno(), False)
            selector.register(probe.stdout, selectors.EVENT_READ)
            end = min(self._deadline, time.monotonic() + self.config.startup_timeout_seconds)
            output = bytearray()
            eof = False
            while not eof or probe.poll() is None:
                with self._lock:
                    if self._stop:
                        self._finish(*self._stop)
                        return False
                    if time.monotonic() >= end:
                        self._finish("failed", "version_timeout")
                        return False
                for _, _ in selector.select(0.02):
                    data = os.read(probe.stdout.fileno(), 4096)
                    if not data:
                        eof = True
                        selector.unregister(probe.stdout)
                    output.extend(data)
                    if len(output) > 4096:
                        with self._lock:
                            self._finish("failed", "version_output_limit")
                        return False
            if probe.returncode != 0 or re.fullmatch(rb"2\.1\.309 \(Claude Code\)\s*", output) is None:
                with self._lock:
                    self._finish("failed", "unsupported_cli_version", probe.returncode)
                return False
            return True
        finally:
            cleaned = self._cleanup_version_probe()
            try:
                if selector is not None:
                    selector.close()
            except (OSError, ValueError):
                cleaned = False
            if not cleaned:
                # A known startup diagnostic does not prove its child was reaped.
                with self._lock:
                    self._finish("unknown", "cleanup_failed", probe.returncode)
                return False

    @staticmethod
    def _cleanup_child(process, streams, timeout):
        """Always close pipes; retain ownership if termination or reaping is uncertain."""
        if process is None:
            return True
        cleaned = True
        try:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                cleaned = False
            try:
                process.wait(timeout=timeout)
            except (OSError, subprocess.TimeoutExpired):
                cleaned = False
        finally:
            for stream in streams:
                if stream is not None:
                    try:
                        stream.close()
                    except (OSError, ValueError):
                        cleaned = False
        return cleaned

    def _cleanup_version_probe(self, timeout=1):
        probe = self._version_probe
        if probe is None:
            return True
        cleaned = self._cleanup_child(probe, (probe.stdout,), timeout)
        if cleaned:
            self._version_probe = None
        return cleaned

    def _cleanup_process(self, timeout=1):
        process = self._process
        if process is None:
            return True
        cleaned = self._cleanup_child(process, (process.stdin, process.stdout, process.stderr), timeout)
        if cleaned:
            self._process = None
        return cleaned

    def _supervise(self):
        selector = None
        process = None
        try:
            selector = selectors.DefaultSelector()
            with self._lock:
                if self._stop:
                    self._finish(*self._stop)
                    return
            if os.name != "posix":
                with self._lock:
                    self._finish("failed", "unsupported_platform")
                return
            env = dict(os.environ if self.config.env is None else self.config.env)
            env["DISABLE_AUTOUPDATER"] = "1"
            env["CLAUDE_CODE_DISABLE_BACKGROUND_TASKS"] = "1"
            executable = shutil.which(self.config.executable, path=env.get("PATH", os.defpath))
            if executable is None:
                with self._lock:
                    self._finish("failed", "cli_not_found")
                return
            if not self._check_version(executable, env):
                return
            with self._lock:
                if self._stop:
                    self._finish(*self._stop)
                    return
            process = subprocess.Popen(self._command(executable), cwd=self.config.workdir, env=env,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True, bufsize=0)
            self._process = process
            for stream in (process.stdout, process.stderr, process.stdin):
                os.set_blocking(stream.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            start = time.monotonic()
            with self._lock:
                self._emit("started")
                self._send({"type": "control_request", "request_id": self._init_id,
                            "request": {"subtype": "initialize", "hooks": {}, "supportedDialogKinds": []}})
            buffer = bytearray()
            total_bytes = 0
            termination_sent = False
            killed_at = None
            stdin_registered = False
            exit_seen_at = None
            while True:
                with self._lock:
                    now = time.monotonic()
                    if self._stop is None:
                        if now >= self._deadline:
                            self._request_stop("timed_out", "timeout")
                        elif not self._input_sent and now - start >= self.config.startup_timeout_seconds:
                            self._request_stop("failed", "initialize_timeout")
                        elif any(now >= a["deadline"] for a in self._approvals.values()):
                            self._request_stop("failed", "approval_expired")
                        elif self._final is not None and now - self._final_at >= self.config.cancel_grace_seconds:
                            self._request_stop("unknown", "final_process_still_running")
                    if self._stop is not None:
                        elapsed = now - self._stop_at
                        if elapsed >= self.config.cancel_grace_seconds and not termination_sent:
                            self._signal_group(signal.SIGTERM)
                            termination_sent = True
                        if elapsed >= self.config.cancel_grace_seconds * 2 and killed_at is None:
                            self._signal_group(signal.SIGKILL)
                            killed_at = now
                        if killed_at is not None and now - killed_at >= 1:
                            break
                    if self._final is not None and not self._stdin_closed and not self._tx:
                        if stdin_registered:
                            selector.unregister(process.stdin)
                            stdin_registered = False
                        process.stdin.close()
                        self._stdin_closed = True
                    if self._tx and not self._stdin_closed and not stdin_registered:
                        selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
                        stdin_registered = True
                ready = selector.select(0.02)
                for key, _ in ready:
                    with self._lock:
                        if key.data == "stdin":
                            try:
                                count = os.write(process.stdin.fileno(), self._tx[:65536])
                                del self._tx[:count]
                            except (BrokenPipeError, OSError):
                                self._stdin_closed = True
                                self._tx.clear()
                            if not self._tx or self._stdin_closed:
                                selector.unregister(process.stdin)
                                stdin_registered = False
                            continue
                        try:
                            chunk = os.read(key.fileobj.fileno(), 65536)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            selector.unregister(key.fileobj)
                            if key.data == "stdout" and buffer and self._stop is None:
                                self._request_stop("failed", "truncated_protocol")
                            continue
                        total_bytes += len(chunk)
                        if total_bytes > self.config.max_output_bytes:
                            self._request_stop("failed", "output_limit")
                            continue
                        if key.data == "stderr":
                            self._stderr.extend(chunk[:max(0, 8192 - len(self._stderr))])
                            continue
                        buffer.extend(chunk)
                        while b"\n" in buffer:
                            line, _, remaining = buffer.partition(b"\n")
                            buffer = bytearray(remaining)
                            if not line.strip():
                                continue
                            try:
                                message = json.loads(line.decode("utf-8"), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
                                self._message(message)
                            except (UnicodeError, ValueError, TypeError, RecursionError):
                                self._request_stop("failed", "protocol_error")
                        if len(buffer) > min(self.config.max_output_bytes, 1024 * 1024):
                            self._request_stop("failed", "output_limit")
                # Drain stdout/stderr to EOF even after the leader exits.
                readers = [k for k in selector.get_map().values() if k.data != "stdin"]
                if process.poll() is not None and not readers:
                    break
                if process.poll() is not None:
                    if exit_seen_at is None:
                        exit_seen_at = time.monotonic()
                    elif time.monotonic() - exit_seen_at >= self.config.cancel_grace_seconds:
                        with self._lock:
                            self._request_stop("unknown", "descendants_still_running")
            # Also stop descendants whose leader exited normally; never leave a tool behind.
            self._signal_group(signal.SIGKILL)
            try:
                exit_code = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                with self._lock:
                    self._finish("unknown", "cleanup_failed")
                return
            with self._lock:
                self._resolve(exit_code)
        except Exception:
            with self._lock:
                if self._outcome is None:
                    self._finish("unknown" if self._input_sent else "failed", "executor_error")
        finally:
            if not self._cleanup_version_probe():
                with self._lock:
                    self._finish("unknown", "cleanup_failed")
            if not self._cleanup_process():
                with self._lock:
                    self._finish("unknown", "cleanup_failed")
            try:
                if selector is not None:
                    selector.close()
            except (OSError, ValueError):
                with self._lock:
                    self._finish("unknown", "cleanup_failed")
            with self._lock:
                if self._outcome is None:
                    self._finish("unknown", "executor_error")
                self._emit("result", outcome=dict(self._outcome))
            self._done.set()


class ClaudeCodeBackend:
    """v1 dictionary adapter. Identity authentication remains a core responsibility."""
    capabilities = {
        "text": True, "explicit_session": True, "approval": True,
        "cancel": True, "timeout": True, "user_questions": False,
        "cloud": False, "protocol_cli_version": "2.1.309",
        "authenticated_acceptance": "unverified",
    }

    def __init__(self, config):
        self.config = copy.deepcopy(config)
        # Reject oversized lifecycle budgets at factory time, before a job starts.
        for name, default in (("timeout_seconds", 1800), ("approval_timeout_seconds", 300),
                              ("startup_timeout_seconds", 15), ("cancel_grace_seconds", 1)):
            _check_timeout(name, self.config.get(name, default))
        self._run = None
        self._pending = []
        self._deferred_approvals = []
        self._public_approval = None
        self._terminal = False
        self._started = False
        self._closed = False

    def start(self, request):
        if self._started or self._closed:
            raise RuntimeError("Each backend object supports one execution only")
        self._started = True
        request = copy.deepcopy(request)
        if request.get("schema_version") != 1 or request.get("backend_id") != "claude_code":
            raise ValueError("Invalid Claude backend v1 request")
        for field in ("account_scope", "sender_key", "conversation_key", "backend_instance_ref"):
            if not isinstance(request.get(field), str) or not request[field]:
                raise ValueError("Missing backend request identity: " + field)
        if request["backend_instance_ref"] != self.config.get("instance_id"):
            raise ValueError("Claude instance mismatch")
        if request.get("cwd") != self.config.get("cwd"):
            raise ValueError("Claude workspace mismatch")
        policy = request.get("execution_policy", {})
        configured_policy = self.config.get("execution_policy", {})
        if policy != configured_policy or policy.get("approval") != "explicit":
            raise ValueError("Explicit approval policy is required")
        if policy.get("sandbox") not in (None, "none"):
            raise ValueError("Claude adapter does not provide the requested OS sandbox")
        command = self.config.get("command", ["claude"])
        if not isinstance(command, list) or len(command) != 1 or not isinstance(command[0], str):
            raise ValueError("Claude command must contain exactly the executable, without flags")
        deadline = request.get("deadline_at")
        if not isinstance(deadline, (int, float)) or not math.isfinite(deadline) or deadline <= time.time():
            raise ValueError("A future finite execution deadline is required")
        messages = request.get("messages")
        if not isinstance(messages, list) or not messages:
            raise ValueError("Frozen messages are required")
        inputs = []
        for message in messages:
            if not isinstance(message, dict) or not isinstance(message.get("text", ""), str):
                raise ValueError("Invalid message")
            files = []
            attachments = message.get("attachments", [])
            if not isinstance(attachments, list):
                raise ValueError("Invalid attachments")
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    raise ValueError("Invalid attachment")
                if attachment.get("status") == "available":
                    path = attachment.get("path")
                    if not isinstance(path, str) or not Path(path).is_absolute() or not Path(path).is_file():
                        raise ValueError("Attachment must reference an existing absolute local file")
                    files.append({"path": path, "status": "available"})
                elif attachment.get("status") == "failed":
                    count = attachment.get("count", 1)
                    if type(count) is not int or count <= 0:
                        raise ValueError("Failed attachment count must be a positive integer")
                    files.append({"status": "failed", "count": count})
                else:
                    raise ValueError("Unsupported attachment status")
            inputs.append({"text": message.get("text", ""), "attachments": files})
        # Keep user text and file references as data; no path/shell/command expansion.
        prompt = "Respond to this frozen message batch; local attachments may be read with tools.\n" + _json(inputs)
        config = ClaudeCodeConfig(
            workdir=self.config["cwd"], executable=command[0],
            timeout_seconds=min(self.config.get("timeout_seconds", 1800), deadline - time.time()),
            approval_timeout_seconds=min(self.config.get("approval_timeout_seconds", 300), 300),
            startup_timeout_seconds=self.config.get("startup_timeout_seconds", 15),
            cancel_grace_seconds=self.config.get("cancel_grace_seconds", 1),
            max_output_bytes=self.config.get("max_output_bytes", 4 * 1024 * 1024),
            tools=tuple(self.config.get("tools", _SAFE_TOOLS)),
            env=self.config.get("env"),
        )
        self._run = ClaudeCodeExecutor(config).start(prompt, request.get("execution_id"),
                                                     request.get("attempt_id"), request.get("session_id"))

    def poll(self):
        if self._run is None:
            return []
        for event in self._run.poll():
            if self._terminal:
                continue
            base = dict(event_id="claude-%s" % event["sequence"], execution_id=event["execution_id"],
                        attempt_id=event["attempt_id"], session_id=event["session_id"])
            kind = event["type"]
            if kind in ("started", "progress", "session"):
                base.update(type="started" if kind == "started" else "progress")
            elif kind == "approval_required":
                summary = _approval_summary(event["tool_name"], event["tool_input"], self.config["cwd"])
                if summary is None:
                    with self._run._lock:
                        self._run._request_stop("failed", "approval_not_displayable")
                    continue
                base.update(type="approval_requested", approval_id=event["approval_id"],
                            action_summary=summary, action_digest=event["action_hash"],
                            expires_at=event["expires_at"])
                self._deferred_approvals.append(base)
                continue
            elif kind == "approval_closed":
                if self._public_approval == event["approval_id"]:
                    self._public_approval = None
                self._deferred_approvals = [a for a in self._deferred_approvals if a["approval_id"] != event["approval_id"]]
                continue
            elif kind == "result":
                result = event["outcome"]
                status = result["status"]
                if status == "succeeded" and len(result["text"]) > _PUBLIC_TEXT_LIMIT:
                    # The provider completed, but v1 cannot deliver its full answer.
                    # Emit a valid failure, never a truncated successful answer or
                    # an invalid final that makes the core fence the whole queue.
                    base.update(type="failed", error_code="final_text_too_large",
                                side_effect_state="possible" if result["side_effects_possible"] else "none")
                elif status == "succeeded":
                    base.update(type="final", text=result["text"])
                else:
                    base.update(type={"cancelled": "cancelled", "unknown": "unknown"}.get(status, "failed"),
                                error_code=result["code"],
                                side_effect_state="possible" if result["side_effects_possible"] else "none")
                self._terminal = True
                self._deferred_approvals.clear()
                self._public_approval = None
            else:
                continue
            self._pending.append(base)
        # Core v1 represents one visible pending approval. Other provider requests
        # remain pending with their original deadlines, never automatically allowed.
        if not self._terminal and self._run._stop is None and self._public_approval is None and self._deferred_approvals:
            approval = self._deferred_approvals.pop(0)
            self._public_approval = approval["approval_id"]
            self._pending.append(approval)
        result, self._pending = self._pending[:100], self._pending[100:]
        return result

    def respond_approval(self, approval_id, decision, action_digest):
        if self._run is None or approval_id != self._public_approval or not self._run.approve(approval_id, decision, action_digest):
            raise ValueError("Approval is invalid, expired, consumed, or does not match this action")

    def cancel(self):
        if self._run is not None:
            self._run.cancel()

    def close(self):
        self._closed = True
        if self._run is not None:
            deadline = time.monotonic() + _CLOSE_TIMEOUT_SECONDS
            self._run.cancel()
            # One absolute budget includes the worker and any retained child.
            # Retry only after the worker is done, avoiding concurrent reaping.
            self._run.wait(max(0, deadline - time.monotonic()))
            for cleanup in (self._run._cleanup_version_probe, self._run._cleanup_process):
                if not cleanup(timeout=min(1, max(0, deadline - time.monotonic()))):
                    raise RuntimeError("Claude child cleanup is incomplete")


def _approval_summary(tool, tool_input, cwd):
    """Display the complete reviewed action, or fail closed; never redact then allow."""
    text = _json({"tool": tool, "input": tool_input, "cwd": cwd})
    # Reject common credential forms entirely. No raw error/logs enter core events.
    suspicious = (r"(?i)(api[_-]?key|access[_-]?token|refresh[_-]?token|password|passwd|authorization|cookie|secret)"
                  r"|bearer\s+\S+|sk-[A-Za-z0-9_-]{10,}|-----BEGIN.*PRIVATE KEY"
                  r"|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]+|glpat-[A-Za-z0-9_-]+"
                  r"|(?:AKIA|ASIA)[A-Z0-9]{16}|://[^/\s:@]+:[^/\s@]+@|(?:^|\s)(?:-u|--user)(?:\s|=)")
    if len(text) > 2000 or re.search(suspicious, text):
        return None
    return text


def create_backend(config):
    return ClaudeCodeBackend(config)
