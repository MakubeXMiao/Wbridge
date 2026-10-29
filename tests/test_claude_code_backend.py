"""Offline Claude Code protocol tests: the child is a synthetic NDJSON peer.

These tests do not invoke Claude, authentication, a model, WeChat, or a network.
They establish executor behavior against fault injection, not real-client acceptance.
Run: python3 -m unittest discover -s tests -p test_claude_code_backend.py -v
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge.backends.claude_code import ClaudeCodeConfig, ClaudeCodeExecutor, create_backend


TOOL_INPUT = {
    "command": "printf '%s' 'SYNTHETIC_ONLY; $(touch never-run)'",
    "description": "synthetic approval",
    "nested": {"values": [0, False, None, "中文字"], "retain": True},
}
SECOND_TOOL_INPUT = {"command": "printf '%s' 'SYNTHETIC_SECOND'", "description": "second synthetic action"}

# A real subprocess exercises pipe framing, process exits, deadlines and cancellation.
# The script never runs a requested tool: every tool/result is synthetic protocol data.
FAKE_CLI = r'''
import json, os, signal, sys, time

mode = os.environ.get("WBR_FAKE_MODE", "success")
if sys.argv[1:] == ["--version"]:
    if mode == "version_hang":
        time.sleep(20)
    if mode == "version_flood":
        print("x" * 16384, flush=True)
        time.sleep(20)
    print("2.1.1 (Claude Code)" if mode == "unsupported_version" else "2.1.309 (Claude Code)")
    sys.exit(0)

trace_path = os.environ["WBR_FAKE_TRACE"]
tool_input = json.loads(os.environ["WBR_FAKE_INPUT"])

def trace(value):
    with open(trace_path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")

def emit(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False) + "\n")
    sys.stdout.flush()

def recv():
    line = sys.stdin.readline()
    if not line:
        return None
    value = json.loads(line)
    trace({"stdin": value})
    return value

def finish(value=None, exit_code=0):
    if value is not None:
        emit(value)
    # Require the host to close its input after the one final result.
    for line in sys.stdin:
        if line.strip():
            trace({"after_final": json.loads(line)})
    trace({"stdin_eof": True})
    sys.exit(exit_code)

trace({"argv": sys.argv[1:], "cwd": os.getcwd(), "pid": os.getpid(),
       "env_sentinel": os.environ.get("WBR_SHOULD_NOT_INHERIT")})
if mode == "startup_hang":
    trace({"startup_hang_ready": True})
    time.sleep(20)
    sys.exit(92)
if mode == "startup_malformed":
    print("not-json", flush=True)
    sys.exit(0)
initialize = recv()
if not initialize or initialize.get("type") != "control_request" or initialize.get("request", {}).get("subtype") != "initialize":
    trace({"error": "user arrived before initialize"})
    sys.exit(91)
if mode == "initialize_error":
    emit({"type": "control_response", "response": {"subtype": "error", "request_id": initialize["request_id"], "error": "synthetic initialization failure"}})
    sys.exit(2)
init_body = {"claude_code_version": "2.1.309", "current_permission_mode": "default"}
if mode == "initialize_wrong_version":
    init_body["claude_code_version"] = "2.1.1"
if mode == "initialize_missing_version":
    init_body.pop("claude_code_version")
if mode == "initialize_requires_action":
    init_body["session_state"] = "requires_action"
if mode == "initialize_wrong_permission_mode":
    init_body["current_permission_mode"] = "bypassPermissions"
init_response = {"subtype": "success", "request_id": initialize["request_id"], "response": init_body}
if mode == "initialize_pending_approval":
    init_response["pending_permission_requests"] = [{"request_id": "from-previous-process"}]
if mode == "initialize_pending_dialog":
    init_response["pending_user_dialog_requests"] = [{"request_id": "from-previous-process"}]
emit({"type": "control_response", "response": init_response})
permission_audit = recv()
if not permission_audit or permission_audit.get("type") != "control_request" or permission_audit.get("request", {}).get("subtype") != "list_permission_rules":
    trace({"error": "no permission audit before prompt"})
    sys.exit(78)
args = sys.argv[1:]
tools = args[args.index("--tools") + 1].split(",")
rules = [{"behavior": "ask", "source": "flagSettings", "rule": tool, "editability": "readonly"} for tool in tools if tool]
state = {"rules": rules, "originalCwd": os.getcwd(), "managedOnly": False, "workspaceDirectories": []}
if mode == "permission_no_ask":
    state["rules"] = []
if mode == "permission_managed_only":
    state["managedOnly"] = True
if mode == "permission_wrong_cwd":
    state["originalCwd"] = "/synthetic-wrong-cwd"
if mode == "permission_extra_directory":
    state["workspaceDirectories"] = ["/synthetic-extra-directory"]
if mode == "permission_inactive_ask":
    for rule in rules:
        rule["notInEffect"] = True
if mode == "permission_state_errors":
    state["errors"] = ["synthetic policy error"]
emit({"type": "control_response", "response": {"subtype": "success", "request_id": permission_audit["request_id"], "response": {"state": state}}})
user = recv()
if not user or user.get("type") != "user":
    trace({"error": "no user message after initialize"})
    sys.exit(90)
args = sys.argv[1:]
session_flag = "--resume" if "--resume" in args else "--session-id"
if session_flag not in args:
    trace({"error": "missing explicit session"})
    sys.exit(89)
session = args[args.index(session_flag) + 1]
emit({"type": "system", "subtype": "init", "session_id": session, "tools": ["Bash"], "apiKeySource": "none"})
result = {"type": "result", "subtype": "success", "is_error": False,
          "stop_reason": "end_turn", "terminal_reason": "completed",
          "queued_turn_count": 0, "session_id": session, "result": "synthetic final"}

if mode.startswith("size_"):
    # A later ordinary message uses the same config/session and returns normally.
    # This lets the actual Dispatcher prove that an oversized answer does not
    # freeze the conversation or cause a replay of the original task.
    if "SYNTHETIC_FOLLOWUP" not in user["message"]["content"]:
        _, alphabet, count = mode.split("_")
        result["result"] = ("界" if alphabet == "unicode" else "X") * int(count)
    finish(result)

if mode == "hang":
    trace({"hang_ready": True})
    time.sleep(60)
    sys.exit(88)
if mode == "ignore_term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    trace({"ignoring_sigterm": True})
    time.sleep(60)
    sys.exit(87)
if mode == "misleading_auth_error":
    result.update(is_error=True, terminal_reason="api_error", result="Not logged in · Please run /login")
    finish(result, 1)
if mode == "success_explains_auth_error":
    result["result"] = "The message Not logged in means the CLI needs authentication; this is a synthetic explanation."
    finish(result)
if mode == "error_exit_zero":
    result.update(is_error=True, terminal_reason="api_error", result="synthetic backend error")
    finish(result)
if mode == "error_subtype":
    result.update(subtype="error_during_execution", is_error=True, result="", errors=["synthetic failure"])
    finish(result)
if mode == "empty_final":
    result["result"] = ""
    finish(result)
if mode == "whitespace_final":
    result["result"] = "  \n\t"
    finish(result)
if mode == "missing_final":
    emit({"type": "assistant", "session_id": session, "message": {"content": [{"type": "text", "text": "partial is not final"}]}})
    sys.exit(0)
if mode == "duplicate_final":
    emit(result)
    result["result"] = "second synthetic final"
    finish(result)
if mode == "wrong_session":
    result["session_id"] = "00000000-0000-4000-8000-000000000099"
    finish(result)
if mode == "missing_session":
    result.pop("session_id")
    finish(result)
if mode == "wrong_turn":
    result["user_message_uuid"] = "00000000-0000-4000-8000-000000000098"
    finish(result)
if mode == "nonzero_final":
    finish(result, 7)
if mode == "bad_terminal_reason":
    result["terminal_reason"] = "api_error"
    finish(result)
if mode == "bad_stop_reason":
    result["stop_reason"] = "max_tokens"
    finish(result)
if mode == "queued_turns":
    result["queued_turn_count"] = 1
    finish(result)
if mode == "malformed":
    print("{not json", flush=True)
    sys.exit(0)
if mode == "partial_ndjson":
    sys.stdout.write('{"type":"result"')
    sys.stdout.flush()
    sys.exit(0)
if mode == "invalid_utf8":
    sys.stdout.buffer.write(b"\xff\n")
    sys.stdout.buffer.flush()
    sys.exit(0)
if mode == "malformed_after_final":
    emit(result)
    print("{not json", flush=True)
    sys.exit(0)
if mode in ("output_flood", "stderr_flood"):
    trace({"output_flood_ready": True})
    release_deadline = time.monotonic() + 10
    while not os.path.exists(trace_path + ".release"):
        if time.monotonic() >= release_deadline:
            sys.exit(72)
        time.sleep(0.005)
    trace({"output_flood_released": True})
if mode == "output_flood":
    sys.stdout.write("x" * 65536)
    sys.stdout.flush()
    time.sleep(20)
    sys.exit(86)
if mode == "stderr_flood":
    sys.stderr.write("x" * 65536)
    sys.stderr.flush()
    time.sleep(20)
    sys.exit(85)
if mode == "event_overflow":
    for index in range(2100):
        emit({"type": "assistant", "session_id": session, "message": {"content": [{"type": "text", "text": "synthetic progress %d" % index}]}})
    finish(result)
if mode == "unknown_control":
    emit({"type": "control_request", "request_id": "unknown-1", "request": {"subtype": "unknown_future_request"}})
    time.sleep(20)
    sys.exit(84)
if mode.startswith("approval"):
    request = {"type": "control_request", "request_id": "permission-1", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": tool_input, "tool_use_id": "tool-1"}}
    if mode == "approval_user_interaction":
        request["request"]["requires_user_interaction"] = True
    emit(request)
    if mode == "approval_parallel":
        second_input = {"command": "printf '%s' 'SYNTHETIC_SECOND'", "description": "second synthetic action"}
        emit({"type": "control_request", "request_id": "permission-2", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": second_input, "tool_use_id": "tool-2"}})
        trace({"both_permissions_emitted": True})
        seen = set()
        for index in range(2):
            response = recv()
            if not response or response.get("type") != "control_response":
                sys.exit(77)
            body = response.get("response", {})
            provider_id = body.get("request_id")
            decision = body.get("response", {})
            if provider_id not in ("permission-1", "permission-2") or provider_id in seen:
                sys.exit(76)
            seen.add(provider_id)
            expected = tool_input if provider_id == "permission-1" else second_input
            if decision.get("behavior") == "allow" and decision.get("updatedInput") != expected:
                sys.exit(75)
            if decision.get("behavior") not in ("allow", "deny") or decision.get("updatedPermissions"):
                sys.exit(74)
            trace({"permission_response": response})
        result["result"] = "synthetic two approvals complete"
        finish(result)
    if mode == "approval_withdrawn":
        time.sleep(0.15)
        emit({"type": "control_cancel_request", "request_id": "permission-1"})
        time.sleep(0.1)
        result["result"] = "synthetic permission withdrawn"
        finish(result)
    if mode == "approval_duplicate_request":
        emit(request)
    if mode == "approval_pending_final":
        finish(result)
    response = recv()
    if response is None:
        sys.exit(83)
    decision = response.get("response", {}).get("response", {})
    if response.get("type") != "control_response" or response.get("response", {}).get("request_id") != "permission-1":
        sys.exit(82)
    trace({"permission_response": response})
    if decision.get("behavior") == "allow":
        if decision.get("updatedInput") != tool_input:
            trace({"error": "allow changed original input"})
            sys.exit(81)
        if decision.get("updatedPermissions"):
            trace({"error": "allow permanently changed permissions"})
            sys.exit(80)
        result["result"] = "synthetic allowed"
    elif decision.get("behavior") == "deny":
        result["result"] = "synthetic denied"
    else:
        sys.exit(79)
    finish(result)
if mode == "delayed_exit":
    emit(result)
    trace({"final_emitted": True})
    release_deadline = time.monotonic() + 10
    while not os.path.exists(trace_path + ".release"):
        if time.monotonic() >= release_deadline:
            sys.exit(73)
        time.sleep(0.005)
    finish()
if mode == "fragmented":
    result["result"] = "synthetic 中文 final"
    encoded = (json.dumps(result, ensure_ascii=False) + "\n").encode("utf-8")
    for byte in encoded:
        sys.stdout.buffer.write(bytes([byte]))
        sys.stdout.buffer.flush()
    finish()
finish(result)
'''


class ClaudeCodeProtocolTests(unittest.TestCase):
    """Assertions concern the private executor; they do not mock that executor."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wbridge-cc-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.script = self.root / "fake claude"
        self.script.write_text("#!" + sys.executable + "\n" + FAKE_CLI, encoding="utf-8")
        self.script.chmod(0o700)
        self.counter = 0
        self.runs = []
        self.addCleanup(self.stop_runs)

    def stop_runs(self):
        for run in self.runs:
            run.cancel()
            # A cleanup failure is a test error, not a reason to hide a child.
            run.wait(20)

    def start(self, mode="success", prompt="synthetic prompt", session_id=None, **overrides):
        self.counter += 1
        trace = self.root / ("trace-%d.jsonl" % self.counter)
        config_values = {
            "executable": str(self.script), "workdir": str(self.root),
            "env": {"WBR_FAKE_MODE": mode, "WBR_FAKE_TRACE": str(trace),
                    "WBR_FAKE_INPUT": json.dumps(TOOL_INPUT)},
            "timeout_seconds": 15, "startup_timeout_seconds": 5,
            "approval_timeout_seconds": 10, "cancel_grace_seconds": 0.1,
            "max_output_bytes": 32768, "tools": ("Bash",),
        }
        config_values.update(overrides)
        run = ClaudeCodeExecutor(ClaudeCodeConfig(**config_values)).start(
            prompt=prompt, execution_id="execution-%d" % self.counter,
            attempt_id="attempt-%d" % self.counter, session_id=session_id)
        self.runs.append(run)
        return run, trace

    def trace(self, path):
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def assert_child_exited(self, trace_path):
        pid = self.trace(trace_path)[0]["pid"]
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def start_backend(self, mode="success", tool_input=None, request_updates=None, config_updates=None):
        self.counter += 1
        trace = self.root / ("facade-trace-%d.jsonl" % self.counter)
        policy = {"approval": "explicit", "sandbox": "none"}
        config = {
            "instance_id": "synthetic-instance", "cwd": str(self.root),
            "command": [str(self.script)], "execution_policy": policy,
            "env": {"WBR_FAKE_MODE": mode, "WBR_FAKE_TRACE": str(trace),
                    "WBR_FAKE_INPUT": json.dumps(TOOL_INPUT if tool_input is None else tool_input)},
            "timeout_seconds": 15, "startup_timeout_seconds": 5,
            "approval_timeout_seconds": 10, "cancel_grace_seconds": 0.1,
            "max_output_bytes": 32768, "tools": ["Bash"],
        }
        config.update(config_updates or {})
        request = {
            "schema_version": 1, "backend_id": "claude_code", "backend_instance_ref": "synthetic-instance",
            "account_scope": "synthetic-account", "sender_key": "synthetic-sender", "conversation_key": "synthetic-conversation",
            "cwd": str(self.root), "execution_id": "facade-execution-%d" % self.counter,
            "attempt_id": "facade-attempt-%d" % self.counter, "session_id": None,
            "execution_policy": policy, "deadline_at": time.time() + 15,
            "messages": [{"message_id": "synthetic-message", "text": "frozen synthetic text", "attachments": []}],
        }
        request.update(request_updates or {})
        backend = create_backend(config)
        self.addCleanup(backend.close)
        backend.start(request)
        return backend, trace, request

    def backend_events_until(self, backend, kinds, timeout=20):
        events = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            events.extend(backend.poll())
            matches = [event for event in events if event["type"] in kinds]
            if matches:
                return matches[0], events
            time.sleep(0.005)
        self.fail("No %r backend event; observed %r" % (kinds, events))

    def events_until(self, run, event_type, timeout=12):
        events = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            events.extend(run.poll())
            found = [event for event in events if event["type"] == event_type]
            if found:
                return found[0], events
            time.sleep(0.005)
        self.fail("No %s event; observed %r" % (event_type, events))

    def assert_rejected(self, mode, **overrides):
        run, trace = self.start(mode, **overrides)
        outcome = run.wait(20)
        self.assertIn(outcome["status"], ("failed", "unknown", "timed_out"), outcome)
        self.assertTrue(outcome.get("code"), outcome)
        self.assertNotEqual(outcome["text"], "synthetic final", outcome)
        records = self.trace(trace)
        # Reaching an unrelated startup error cannot satisfy a protocol test.
        inputs = [record["stdin"] for record in records if "stdin" in record]
        if mode.startswith("version_") or mode == "unsupported_version":
            self.assertFalse(any(message.get("type") == "user" for message in inputs))
        elif mode.startswith("initialize_"):
            self.assertTrue(any(message.get("request", {}).get("subtype") == "initialize" for message in inputs), outcome)
        elif mode.startswith("permission_"):
            self.assertTrue(any(message.get("request", {}).get("subtype") == "list_permission_rules" for message in inputs), outcome)
        elif mode == "startup_hang":
            self.assertTrue(any(record.get("startup_hang_ready") for record in records), outcome)
        elif mode == "startup_malformed":
            self.assertTrue(any("argv" in record for record in records), outcome)
        else:
            self.assertTrue(any(message.get("type") == "user" for message in inputs), outcome)
        return outcome, records

    def test_success_waits_for_exit_and_uses_explicit_new_session(self):
        run, trace = self.start()
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        self.assertEqual(outcome["text"], "synthetic final")
        self.assertEqual(outcome["exit_code"], 0)
        self.assertEqual(outcome["execution_id"], "execution-1")
        self.assertEqual(outcome["attempt_id"], "attempt-1")
        uuid.UUID(outcome["session_id"])
        records = self.trace(trace)
        args = records[0]["argv"]
        self.assertIn("--session-id", args)
        self.assertEqual(args[args.index("--session-id") + 1], outcome["session_id"])
        self.assertNotIn("--resume", args)
        self.assertIn("--permission-prompt-tool", args)
        self.assertEqual(args[args.index("--permission-prompt-tool") + 1], "stdio")
        self.assertNotIn("--dangerously-skip-permissions", args)
        self.assertNotIn("bypassPermissions", args)
        self.assertIn("--safe-mode", args)
        self.assertNotIn("--bare", args)
        self.assertEqual(args[args.index("--permission-prompts") + 1], "host")
        settings = json.loads(args[args.index("--settings") + 1])
        self.assertIs(settings["disableAllHooks"], True)
        self.assertEqual(settings["permissions"]["ask"], ["Bash"])
        self.assertIn("--strict-mcp-config", args)
        self.assertTrue(any(record.get("stdin_eof") for record in records))
        events = run.poll()
        self.assertEqual(len([event for event in events if event["type"] == "result"]), 1)
        sequences = [event["sequence"] for event in events]
        self.assertEqual(sequences, sorted(set(sequences)))
        for event in events:
            self.assertEqual(event["execution_id"], "execution-1")
            self.assertEqual(event["attempt_id"], "attempt-1")
        self.assertEqual(run.poll(), [])

    def test_resume_requires_and_preserves_exact_session(self):
        session = "d9648244-4c7d-436e-9ee9-95aa300f6571"
        run, trace = self.start(session_id=session)
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        self.assertEqual(outcome["session_id"], session)
        args = self.trace(trace)[0]["argv"]
        self.assertEqual(args[args.index("--resume") + 1], session)
        self.assertNotIn("--continue", args)
        self.assertNotIn("--session-id", args)

    def test_prompt_is_stdin_data_not_shell_or_argv(self):
        marker = self.root / "should-not-exist"
        prompt = "$(touch %s); `touch %s`\n--resume malicious\n中文" % (marker, marker)
        run, trace = self.start(prompt=prompt)
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        records = self.trace(trace)
        self.assertFalse(marker.exists())
        self.assertNotIn(prompt, records[0]["argv"])
        inputs = [record["stdin"] for record in records if "stdin" in record]
        self.assertEqual(inputs[0]["request"]["subtype"], "initialize")
        self.assertEqual(inputs[1]["request"]["subtype"], "list_permission_rules")
        self.assertEqual(inputs[2]["message"]["content"], prompt)
        self.assertIs(inputs[2]["client_composed"], True)

    def test_replacement_environment_does_not_inherit_parent_secret(self):
        with patch.dict(os.environ, {"WBR_SHOULD_NOT_INHERIT": "synthetic-secret"}):
            run, trace = self.start()
            self.assertEqual(run.wait(20)["status"], "succeeded")
        self.assertIsNone(self.trace(trace)[0]["env_sentinel"])

    def test_success_event_not_published_before_process_exit(self):
        run, trace = self.start("delayed_exit", cancel_grace_seconds=5)
        deadline = time.monotonic() + 12
        while not any(record.get("final_emitted") for record in self.trace(trace)):
            self.assertLess(time.monotonic(), deadline, "Fake peer never emitted its final")
            time.sleep(0.01)
        with self.assertRaises(TimeoutError):
            run.wait(0.08)
        self.assertFalse(any(event["type"] == "result" for event in run.poll()))
        Path(str(trace) + ".release").touch()
        self.assertEqual(run.wait(20)["status"], "succeeded")

    def test_fragmented_valid_ndjson_is_assembled(self):
        run, _ = self.start("fragmented")
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded")
        self.assertEqual(outcome["text"], "synthetic 中文 final")

    def test_success_subtype_with_auth_error_is_not_success(self):
        outcome, _ = self.assert_rejected("misleading_auth_error")
        self.assertEqual(outcome["exit_code"], 1)
        self.assertTrue("auth" in outcome["code"].lower() or "login" in outcome["text"].lower())

    def test_success_explaining_auth_error_text_remains_success(self):
        run, _ = self.start("success_explains_auth_error")
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        self.assertEqual(outcome["code"], "ok")
        self.assertEqual(outcome["exit_code"], 0)
        self.assertIn("The message Not logged in means", outcome["text"])

    def test_error_flag_is_checked_even_with_exit_zero(self):
        outcome, _ = self.assert_rejected("error_exit_zero")
        self.assertEqual(outcome["exit_code"], 0)

    def test_error_subtype_is_not_success(self):
        self.assert_rejected("error_subtype")

    def test_empty_or_whitespace_final_is_rejected(self):
        for mode in ("empty_final", "whitespace_final"):
            with self.subTest(mode=mode):
                self.assert_rejected(mode)

    def test_partial_assistant_text_cannot_replace_missing_final(self):
        outcome, _ = self.assert_rejected("missing_final")
        self.assertNotEqual(outcome["text"], "partial is not final")

    def test_duplicate_final_is_rejected(self):
        self.assert_rejected("duplicate_final")

    def test_wrong_or_missing_final_session_is_rejected(self):
        for mode in ("wrong_session", "missing_session"):
            with self.subTest(mode=mode):
                self.assert_rejected(mode)

    def test_wrong_turn_id_in_same_session_is_rejected(self):
        self.assert_rejected("wrong_turn")

    def test_initialize_requires_action_is_not_resumed_as_new_input(self):
        outcome, records = self.assert_rejected("initialize_requires_action")
        self.assertEqual(outcome["code"], "pending_previous_interaction")
        self.assertFalse(any(r.get("stdin", {}).get("type") == "user" for r in records))

    def test_nonzero_exit_overrides_valid_final(self):
        outcome, _ = self.assert_rejected("nonzero_final")
        self.assertEqual(outcome["exit_code"], 7)

    def test_error_terminal_or_incomplete_stop_reason_is_rejected(self):
        for mode in ("bad_terminal_reason", "bad_stop_reason", "queued_turns"):
            with self.subTest(mode=mode):
                self.assert_rejected(mode)

    def test_malformed_partial_or_invalid_utf8_is_rejected(self):
        for mode in ("malformed", "partial_ndjson", "invalid_utf8", "malformed_after_final"):
            with self.subTest(mode=mode):
                self.assert_rejected(mode)

    def test_unknown_control_request_fails_closed(self):
        self.assert_rejected("unknown_control")

    def test_initialize_failure_does_not_send_user_prompt(self):
        _, records = self.assert_rejected("initialize_error")
        inputs = [record["stdin"] for record in records if "stdin" in record]
        self.assertFalse(any(value.get("type") == "user" for value in inputs))

    def test_runtime_version_and_effective_permissions_are_audited_before_prompt(self):
        modes = ("initialize_wrong_version", "initialize_missing_version", "initialize_wrong_permission_mode",
                 "initialize_pending_approval", "initialize_pending_dialog", "permission_no_ask",
                 "permission_managed_only", "permission_wrong_cwd", "permission_extra_directory",
                 "permission_inactive_ask", "permission_state_errors")
        for mode in modes:
            with self.subTest(mode=mode):
                _, records = self.assert_rejected(mode)
                self.assertFalse(any(record.get("stdin", {}).get("type") == "user" for record in records))

    def test_required_user_interaction_cannot_be_turned_into_tool_allow(self):
        outcome, records = self.assert_rejected("approval_user_interaction")
        self.assertFalse(any("permission_response" in record for record in records))
        self.assertIn(outcome["status"], ("failed", "unknown"))

    def test_startup_timeout_is_bounded(self):
        run, trace = self.start("startup_hang", startup_timeout_seconds=5)
        ready_deadline = time.monotonic() + 12
        while not any(record.get("startup_hang_ready") for record in self.trace(trace)):
            self.assertLess(time.monotonic(), ready_deadline, "Fake peer never reached the initialization hang")
            time.sleep(0.005)
        observed_ready_at = time.monotonic()
        outcome = run.wait(8)
        self.assertEqual(outcome["code"], "initialize_timeout")
        self.assertFalse(outcome["side_effects_possible"])
        self.assertLess(time.monotonic() - observed_ready_at, 6)
        self.assert_child_exited(trace)

    def test_malformed_initial_frame_fails_before_prompt(self):
        _, records = self.assert_rejected("startup_malformed")
        self.assertFalse(any(record.get("stdin", {}).get("type") == "user" for record in records))

    def test_allow_returns_original_complete_input_once(self):
        run, trace = self.start("approval")
        approval, _ = self.events_until(run, "approval_required")
        self.assertEqual(approval["tool_name"], "Bash")
        self.assertEqual(approval["tool_input"], TOOL_INPUT)
        self.assertTrue(approval["action_hash"])
        self.assertTrue(approval["expires_at"])
        self.assertTrue(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        self.assertFalse(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        self.assertEqual(outcome["text"], "synthetic allowed")
        responses = [record["permission_response"] for record in self.trace(trace) if "permission_response" in record]
        self.assertEqual(len(responses), 1)
        body = responses[0]["response"]["response"]
        self.assertEqual(body["updatedInput"], TOOL_INPUT)
        self.assertFalse(body.get("updatedPermissions"))

    def test_deny_is_explicit_and_does_not_run_tool(self):
        run, trace = self.start("approval")
        approval, _ = self.events_until(run, "approval_required")
        self.assertTrue(run.approve(approval["approval_id"], decision="deny", action_hash=approval["action_hash"]))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        self.assertEqual(outcome["text"], "synthetic denied")
        response = next(record["permission_response"] for record in self.trace(trace) if "permission_response" in record)
        self.assertEqual(response["response"]["response"]["behavior"], "deny")

    def test_stale_id_wrong_hash_or_invalid_decision_cannot_approve(self):
        run, _ = self.start("approval")
        approval, _ = self.events_until(run, "approval_required")
        self.assertFalse(run.approve("stale-id", decision="allow", action_hash=approval["action_hash"]))
        self.assertFalse(run.approve(approval["approval_id"], decision="allow", action_hash="incorrect-hash"))
        self.assertFalse(run.approve(approval["approval_id"], decision="always_allow", action_hash=approval["action_hash"]))
        self.assertTrue(run.approve(approval["approval_id"], decision="deny", action_hash=approval["action_hash"]))
        self.assertEqual(run.wait(20)["text"], "synthetic denied")

    def test_approval_action_hash_cannot_cross_execution_or_session(self):
        first, _ = self.start("approval")
        second, _ = self.start("approval")
        first_approval, _ = self.events_until(first, "approval_required")
        second_approval, _ = self.events_until(second, "approval_required")
        self.assertEqual(first_approval["approval_id"], second_approval["approval_id"])
        self.assertNotEqual(first_approval["action_hash"], second_approval["action_hash"])
        self.assertFalse(second.approve(second_approval["approval_id"], decision="allow", action_hash=first_approval["action_hash"]))
        self.assertTrue(second.approve(second_approval["approval_id"], decision="deny", action_hash=second_approval["action_hash"]))
        self.assertTrue(first.approve(first_approval["approval_id"], decision="deny", action_hash=first_approval["action_hash"]))
        self.assertEqual(first.wait(20)["text"], "synthetic denied")
        self.assertEqual(second.wait(20)["text"], "synthetic denied")

    def test_private_run_supports_parallel_provider_approvals(self):
        run, trace = self.start("approval_parallel")
        approvals = []
        deadline = time.monotonic() + 12
        while len(approvals) < 2:
            approvals.extend(event for event in run.poll() if event["type"] == "approval_required")
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        self.assertEqual([event["approval_id"] for event in approvals], ["permission-1", "permission-2"])
        self.assertNotEqual(approvals[0]["action_hash"], approvals[1]["action_hash"])
        self.assertEqual(approvals[1]["tool_input"], SECOND_TOOL_INPUT)
        for approval in reversed(approvals):
            self.assertTrue(run.approve(approval["approval_id"], "allow", approval["action_hash"]))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        responses = [record["permission_response"]["response"] for record in self.trace(trace) if "permission_response" in record]
        self.assertEqual([response["request_id"] for response in responses], ["permission-2", "permission-1"])

    def test_duplicate_permission_request_fails_closed(self):
        self.assert_rejected("approval_duplicate_request")

    def test_final_with_unresolved_permission_cannot_succeed(self):
        self.assert_rejected("approval_pending_final")

    def test_mutating_display_input_cannot_change_approved_action(self):
        run, trace = self.start("approval")
        approval, _ = self.events_until(run, "approval_required")
        approval["tool_input"]["nested"]["retain"] = False
        approval["tool_input"]["command"] = "MUTATED"
        self.assertTrue(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "succeeded", outcome)
        response = next(record["permission_response"] for record in self.trace(trace) if "permission_response" in record)
        self.assertEqual(response["response"]["response"]["updatedInput"], TOOL_INPUT)

    def test_expired_approval_never_allows(self):
        run, trace = self.start("approval", approval_timeout_seconds=0.12)
        approval, _ = self.events_until(run, "approval_required")
        self.events_until(run, "approval_closed", timeout=2)
        self.assertFalse(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        run.wait(20)
        responses = [record["permission_response"] for record in self.trace(trace) if "permission_response" in record]
        self.assertFalse(any(response.get("response", {}).get("response", {}).get("behavior") == "allow" for response in responses))

    def test_withdrawn_approval_is_closed_and_cannot_be_answered(self):
        run, trace = self.start("approval_withdrawn")
        approval, _ = self.events_until(run, "approval_required")
        closed, _ = self.events_until(run, "approval_closed")
        self.assertEqual(closed["approval_id"], approval["approval_id"])
        self.assertFalse(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "unknown", outcome)
        self.assertEqual(outcome["code"], "approval_withdrawn")
        self.assertFalse(any("permission_response" in record for record in self.trace(trace)))

    def test_cancel_during_approval_closes_request_and_is_idempotent(self):
        run, _ = self.start("approval")
        approval, _ = self.events_until(run, "approval_required")
        self.assertTrue(run.cancel())
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "cancelled", outcome)
        self.assertFalse(run.approve(approval["approval_id"], decision="allow", action_hash=approval["action_hash"]))
        self.assertFalse(run.cancel())
        events = run.poll()
        self.assertTrue(any(event["type"] == "approval_closed" for event in events))
        self.assertEqual(len([event for event in events if event["type"] == "result"]), 1)

    def test_deadline_terminates_hung_child(self):
        started = time.monotonic()
        # Version discovery and initialization must finish before this test can
        # claim it exercised an execution deadline. Their separate timeout
        # tests keep their own short budgets.
        run, trace = self.start("hang", timeout_seconds=12, startup_timeout_seconds=5)
        ready_deadline = started + 10
        while not any(record.get("hang_ready") for record in self.trace(trace)):
            self.assertLess(time.monotonic(), ready_deadline, "Fake peer never reached the post-user hang phase")
            time.sleep(0.01)
        self.assertTrue(any(record.get("stdin", {}).get("type") == "user" for record in self.trace(trace)))
        outcome = run.wait(14)
        self.assertEqual(outcome["status"], "timed_out", outcome)
        self.assertEqual(outcome["code"], "timeout", outcome)
        self.assertTrue(outcome["side_effects_possible"])
        self.assertLess(time.monotonic() - started, 15)
        self.assertIsNotNone(outcome["exit_code"])
        self.assert_child_exited(trace)

    def test_cancel_escalates_when_child_ignores_sigterm(self):
        run, trace = self.start("ignore_term")
        deadline = time.monotonic() + 12
        while not any(record.get("ignoring_sigterm") for record in self.trace(trace)):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        started = time.monotonic()
        self.assertTrue(run.cancel())
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "cancelled", outcome)
        self.assertLess(time.monotonic() - started, 2)
        self.assertIsNotNone(outcome["exit_code"])
        self.assert_child_exited(trace)

    def test_stdout_and_stderr_are_bounded(self):
        for mode in ("output_flood", "stderr_flood"):
            with self.subTest(mode=mode):
                run, trace = self.start(mode, max_output_bytes=1024)
                ready_deadline = time.monotonic() + 12
                while not any(record.get("output_flood_ready") for record in self.trace(trace)):
                    self.assertLess(time.monotonic(), ready_deadline, "Fake peer never reached the output-limit phase")
                    time.sleep(0.005)
                self.assertTrue(any(record.get("stdin", {}).get("type") == "user" for record in self.trace(trace)))
                self.assertFalse(run._done.is_set(), "Output limit must not fire before the fake is released")
                self.assertIsNone(run._stop, "An unrelated stop cannot satisfy the output-limit test")
                started = time.monotonic()
                Path(str(trace) + ".release").touch()
                outcome = run.wait(5)
                self.assertEqual(outcome["status"], "failed", outcome)
                self.assertEqual(outcome["code"], "output_limit")
                self.assertLess(time.monotonic() - started, 2)
                self.assertTrue(any(record.get("output_flood_released") for record in self.trace(trace)))
                self.assert_child_exited(trace)

    def test_event_queue_overflow_preserves_one_terminal_event(self):
        run, _ = self.start("event_overflow", max_output_bytes=4 * 1024 * 1024)
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "failed", outcome)
        self.assertEqual(outcome["code"], "event_overflow")
        events = run.poll()
        results = [event for event in events if event["type"] == "result"]
        self.assertEqual(len(results), 1)
        self.assertEqual(events[-1]["type"], "result")
        self.assertEqual(results[0]["outcome"], outcome)
        self.assertLessEqual(len(events), 2049)
        self.assertEqual(run.poll(), [])

    def test_missing_cli_has_actionable_failure(self):
        run, _ = self.start(executable=str(self.root / "not-installed"))
        outcome = run.wait(20)
        self.assertEqual(outcome["status"], "failed", outcome)
        self.assertTrue(outcome.get("code"))
        self.assertIn("cli", outcome["code"].lower())
        self.assertFalse(outcome["side_effects_possible"])

    def test_unsupported_cli_version_does_not_send_a_prompt(self):
        outcome, records = self.assert_rejected("unsupported_version")
        self.assertEqual(outcome["code"], "unsupported_cli_version")
        self.assertFalse(outcome["side_effects_possible"])
        self.assertEqual(records, [])

    def test_version_probe_timeout_is_bounded_without_a_prompt(self):
        started = time.monotonic()
        outcome, records = self.assert_rejected("version_hang", startup_timeout_seconds=0.15)
        self.assertIn("timeout", outcome["code"].lower())
        self.assertFalse(outcome["side_effects_possible"])
        self.assertEqual(records, [])
        self.assertLess(time.monotonic() - started, 2)

    def test_version_output_is_bounded_before_a_prompt(self):
        started = time.monotonic()
        outcome, records = self.assert_rejected("version_flood")
        self.assertEqual(outcome["code"], "version_output_limit")
        self.assertFalse(outcome["side_effects_possible"])
        self.assertEqual(records, [])
        self.assertLess(time.monotonic() - started, 2)

    def test_cancellation_during_version_probe_is_bounded(self):
        run, trace = self.start("version_hang", startup_timeout_seconds=3)
        time.sleep(0.1)
        started = time.monotonic()
        self.assertTrue(run.cancel())
        outcome = run.wait(2)
        self.assertEqual(outcome["status"], "cancelled", outcome)
        self.assertFalse(outcome["side_effects_possible"])
        self.assertEqual(self.trace(trace), [])
        self.assertLess(time.monotonic() - started, 1)

    def test_execution_deadline_also_bounds_version_probe(self):
        started = time.monotonic()
        outcome, _ = self.assert_rejected("version_hang", startup_timeout_seconds=3, timeout_seconds=0.2)
        self.assertIn("timeout", outcome["code"].lower())
        self.assertFalse(outcome["side_effects_possible"])
        self.assertLess(time.monotonic() - started, 2)

    def cleanup_fault_backend(self):
        """Build a facade without registering cleanup outside the mock scope."""
        policy = {"approval": "explicit", "sandbox": "none"}
        backend = create_backend({
            "instance_id": "cleanup-fault", "cwd": str(self.root),
            "command": ["synthetic-cleanup-peer"], "execution_policy": policy,
            "env": {}, "startup_timeout_seconds": 0.05, "timeout_seconds": 1,
            "cancel_grace_seconds": 0.05, "tools": ["Bash"],
        })
        request = {
            "schema_version": 1, "backend_id": "claude_code", "backend_instance_ref": "cleanup-fault",
            "account_scope": "synthetic-account", "sender_key": "synthetic-sender",
            "conversation_key": "synthetic-conversation", "cwd": str(self.root),
            "execution_id": "cleanup-execution", "attempt_id": "cleanup-attempt", "session_id": None,
            "execution_policy": policy, "deadline_at": time.time() + 5,
            "messages": [{"message_id": "synthetic-message", "text": "must remain unsent", "attachments": []}],
        }
        return backend, request

    def assert_cleanup_terminal(self, backend):
        outcome = backend._run.wait(0)
        self.assertTrue(backend._run._done.is_set())
        self.assertEqual(outcome["status"], "unknown", outcome)
        self.assertEqual(outcome["code"], "cleanup_failed", outcome)
        events = backend.poll()
        terminal = [event for event in events if event["type"] in ("final", "failed", "unknown", "cancelled")]
        self.assertEqual(len(terminal), 1, events)
        self.assertEqual(terminal[0]["type"], "unknown")
        self.assertEqual(terminal[0]["error_code"], "cleanup_failed")
        self.assertEqual(backend.poll(), [])
        return outcome

    def exercise_version_cleanup_fault(self, fault):
        # Real descriptors exercise selector/pipe cleanup. Popen and every
        # signal are mocked; the real supervisor runs synchronously so no fake
        # PID can outlive the signal mock, including when an assertion fails.
        reader_fd, writer_fd = os.pipe()
        with os.fdopen(reader_fd, "rb", buffering=0) as stdout, os.fdopen(writer_fd, "wb", buffering=0):
            probe = Mock(pid=2_000_000_000, returncode=None, stdout=stdout)
            probe.poll.side_effect = lambda: probe.returncode
            attempts = []

            def wait_probe(timeout):
                attempts.append(timeout)
                if fault == "persistent_wait" or (fault == "transient_wait" and len(attempts) == 1):
                    raise subprocess.TimeoutExpired("synthetic-version-probe", timeout)
                probe.returncode = -signal.SIGKILL
                return probe.returncode

            probe.wait.side_effect = wait_probe
            signal_effect = [OSError("synthetic killpg failure"), None] if fault == "killpg" else None
            module = "wechat_bridge.backends.claude_code."
            with patch(module + "threading.Thread.start"), \
                    patch(module + "shutil.which", return_value="/synthetic/claude"), \
                    patch(module + "subprocess.Popen", return_value=probe) as popen, \
                    patch(module + "os.killpg", side_effect=signal_effect) as killpg:
                backend, request = self.cleanup_fault_backend()
                backend.start(request)
                backend._run._supervise()
                outcome = self.assert_cleanup_terminal(backend)
                self.assertFalse(outcome["side_effects_possible"])
                self.assertFalse(backend._run._input_sent)
                self.assertEqual(popen.call_count, 1, "Cleanup failure must not launch the prompt process")
                self.assertEqual(popen.call_args.args[0], ["/synthetic/claude", "--version"])
                self.assertTrue(stdout.closed)
                self.assertEqual(probe.wait.call_count, 2, "Supervisor finalization must retry owned probe cleanup")
                if fault == "persistent_wait":
                    self.assertIs(backend._run._version_probe, probe)
                    with self.assertRaisesRegex(RuntimeError, "cleanup"):
                        backend.close()
                    self.assertEqual(probe.wait.call_count, 3)
                    self.assertIs(backend._run._version_probe, probe)
                else:
                    self.assertIsNone(backend._run._version_probe)
                    backend.close()
                    self.assertEqual(probe.wait.call_count, 2)
                self.assertTrue(all(0 < timeout <= 1 for timeout in attempts), attempts)
                self.assertTrue(all(call.args == (probe.pid, signal.SIGKILL) for call in killpg.call_args_list))
                self.assertEqual(backend.poll(), [])
                # Only a synthetic owner remains; all real descriptors are
                # closed. Detach it before the signal patch leaves scope.
                backend._run._version_probe = None

    def test_version_cleanup_retries_after_first_reap_timeout(self):
        self.exercise_version_cleanup_fault("transient_wait")

    def test_version_cleanup_reaps_even_after_killpg_error(self):
        self.exercise_version_cleanup_fault("killpg")

    def test_persistent_version_cleanup_failure_is_unknown_and_close_raises(self):
        self.exercise_version_cleanup_fault("persistent_wait")

    def test_main_process_cleanup_failure_keeps_terminal_observable_and_close_raises(self):
        for fault in ("wait", "pipe_close"):
            with self.subTest(fault=fault):
                stdin_reader, stdin_writer = os.pipe()
                stdout_reader, stdout_writer = os.pipe()
                stderr_reader, stderr_writer = os.pipe()
                os.write(stdout_writer, b"not-json\n")
                os.close(stdout_writer)
                os.close(stderr_writer)
                with os.fdopen(stdin_reader, "rb", buffering=0), \
                        os.fdopen(stdin_writer, "wb", buffering=0) as stdin, \
                        os.fdopen(stdout_reader, "rb", buffering=0) as stdout, \
                        os.fdopen(stderr_reader, "rb", buffering=0) as stderr:
                    wrapped_stdout = Mock(wraps=stdout)
                    if fault == "pipe_close":
                        def failed_close():
                            stdout.close()
                            raise OSError("synthetic pipe close failure")
                        wrapped_stdout.close.side_effect = failed_close
                    process = Mock(pid=2_000_000_001, returncode=0, stdin=stdin,
                                   stdout=wrapped_stdout, stderr=stderr)
                    process.poll.return_value = 0
                    process.wait.return_value = 0
                    if fault == "wait":
                        process.wait.side_effect = subprocess.TimeoutExpired("synthetic-main-peer", 1)
                    module = "wechat_bridge.backends.claude_code."
                    with patch(module + "threading.Thread.start"), \
                            patch(module + "shutil.which", return_value="/synthetic/claude"), \
                            patch(module + "ClaudeCodeRun._check_version", return_value=True), \
                            patch(module + "subprocess.Popen", return_value=process) as popen, \
                            patch(module + "os.killpg") as killpg:
                        backend, request = self.cleanup_fault_backend()
                        backend.start(request)
                        backend._run._supervise()
                        self.assert_cleanup_terminal(backend)
                        self.assertEqual(popen.call_count, 1)
                        self.assertNotIn("--version", popen.call_args.args[0])
                        self.assertIs(backend._run._process, process)
                        self.assertTrue(all(stream.closed for stream in (stdin, stdout, stderr)))
                        self.assertGreaterEqual(process.wait.call_count, 2)
                        with self.assertRaisesRegex(RuntimeError, "cleanup"):
                            backend.close()
                        self.assertGreaterEqual(process.wait.call_count, 3)
                        self.assertGreaterEqual(wrapped_stdout.close.call_count, 2)
                        self.assertIs(backend._run._process, process)
                        self.assertTrue(all(call.args[0] == process.pid for call in killpg.call_args_list))
                        self.assertEqual(backend.poll(), [])
                        # Retention was asserted above; do not leave a fake PID
                        # attached after the signal patch leaves scope.
                        backend._run._process = None

    def test_invalid_session_is_rejected_without_spawning(self):
        with self.assertRaises(ValueError):
            self.start(session_id="--continue; synthetic")
        self.assertFalse(any(self.root.glob("trace-*.jsonl")))

    def test_public_facade_maps_identity_session_and_final_once(self):
        backend, trace, request = self.start_backend()
        final, events = self.backend_events_until(backend, ("final", "failed", "unknown"))
        self.assertEqual(final["type"], "final", final)
        self.assertEqual(final["text"], "synthetic final")
        self.assertEqual(final["execution_id"], request["execution_id"])
        self.assertEqual(final["attempt_id"], request["attempt_id"])
        uuid.UUID(final["session_id"])
        self.assertEqual(len({event["event_id"] for event in events}), len(events))
        self.assertTrue(any(event["type"] == "started" for event in events))
        self.assertEqual(backend.poll(), [])
        with self.assertRaises(RuntimeError):
            backend.start(request)
        inputs = [record["stdin"] for record in self.trace(trace) if record.get("stdin", {}).get("type") == "user"]
        batch = json.loads(inputs[0]["message"]["content"].split("\n", 1)[1])
        self.assertEqual(batch, [{"text": "frozen synthetic text", "attachments": []}])

    def test_public_facade_preserves_exact_64000_character_finals(self):
        for alphabet, character in (("ascii", "X"), ("unicode", "界")):
            with self.subTest(alphabet=alphabet):
                backend, _, _ = self.start_backend("size_%s_64000" % alphabet,
                    config_updates={"max_output_bytes": 4 * 1024 * 1024, "startup_timeout_seconds": 15,
                                    "timeout_seconds": 30, "cancel_grace_seconds": 1},
                    request_updates={"deadline_at": time.time() + 30})
                final, events = self.backend_events_until(backend, ("final", "failed", "unknown"), timeout=30)
                self.assertEqual(final["type"], "final", final)
                self.assertEqual(final["text"], character * 64000)
                self.assertEqual(len([event for event in events if event["type"] == "final"]), 1)

    def test_public_facade_rejects_oversized_final_without_truncation_or_unknown(self):
        for alphabet in ("ascii", "unicode"):
            for count in (64001, 200000):
                with self.subTest(alphabet=alphabet, count=count):
                    backend, _, _ = self.start_backend("size_%s_%d" % (alphabet, count),
                        config_updates={"max_output_bytes": 4 * 1024 * 1024, "startup_timeout_seconds": 15,
                                        "timeout_seconds": 30, "cancel_grace_seconds": 1},
                        request_updates={"deadline_at": time.time() + 30})
                    final, events = self.backend_events_until(backend, ("final", "failed", "unknown"), timeout=30)
                    self.assertEqual(final["type"], "failed", final)
                    self.assertEqual(final["error_code"], "final_text_too_large")
                    self.assertEqual(final["side_effect_state"], "possible")
                    self.assertFalse(any(event["type"] in ("final", "unknown") for event in events))
                    self.assertFalse(any(event.get("text") for event in events))
                    self.assertEqual(backend.poll(), [])

    def test_private_config_rejects_unbounded_nonfinite_or_boolean_timeouts(self):
        for field in ("timeout_seconds", "approval_timeout_seconds", "startup_timeout_seconds", "cancel_grace_seconds"):
            for value in (0, -1, float("inf"), float("nan"), True, False, 10 ** 1000):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        ClaudeCodeConfig(workdir=str(self.root), **{field: value})
        for field, value in (("timeout_seconds", 86400.001), ("approval_timeout_seconds", 300.001),
                             ("startup_timeout_seconds", 15.001), ("cancel_grace_seconds", 5.001),
                             ("cancel_grace_seconds", 31)):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    ClaudeCodeConfig(workdir=str(self.root), **{field: value})
        config = ClaudeCodeConfig(workdir=str(self.root), startup_timeout_seconds=15, cancel_grace_seconds=5)
        self.assertEqual((config.startup_timeout_seconds, config.cancel_grace_seconds), (15, 5))

    def test_factory_rejects_invalid_budgets_before_start_or_subprocess(self):
        config = {"instance_id": "synthetic-instance", "cwd": str(self.root), "command": [str(self.script)],
                  "execution_policy": {"approval": "explicit"}}
        for field in ("timeout_seconds", "approval_timeout_seconds", "startup_timeout_seconds", "cancel_grace_seconds"):
            for value in (0, -1, float("inf"), float("nan"), True, False, 10 ** 1000):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        create_backend(dict(config, **{field: value}))
        for field, value in (("timeout_seconds", 86400.001), ("approval_timeout_seconds", 300.001),
                             ("startup_timeout_seconds", 15.001), ("cancel_grace_seconds", 5.001),
                             ("cancel_grace_seconds", 31)):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    create_backend(dict(config, **{field: value}))
        backend = create_backend(dict(config, startup_timeout_seconds=15, cancel_grace_seconds=5))
        backend.close()
        self.assertFalse(any(self.root.glob("*trace-*.jsonl")))

    def test_close_is_bounded_with_maximum_grace_and_sigterm_ignoring_child(self):
        backend, trace, _ = self.start_backend("ignore_term", config_updates={"cancel_grace_seconds": 5, "timeout_seconds": 60,
                                                                            "startup_timeout_seconds": 15},
                                              request_updates={"deadline_at": time.time() + 60})
        deadline = time.monotonic() + 30
        while not any(record.get("ignoring_sigterm") for record in self.trace(trace)):
            self.assertLess(time.monotonic(), deadline, "Fake peer never reached the SIGTERM-ignoring execution phase")
            time.sleep(0.01)
        self.assertTrue(any(record.get("stdin", {}).get("type") == "user" for record in self.trace(trace)))
        started = time.monotonic()
        backend.close()
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 20.5)
        self.assert_child_exited(trace)
        final, events = self.backend_events_until(backend, ("cancelled", "failed", "unknown"))
        self.assertEqual(final["type"], "cancelled", final)
        self.assertEqual(len([event for event in events if event["type"] in ("final", "cancelled", "failed", "unknown")]), 1)
        self.assertEqual(backend.poll(), [])

    def test_public_facade_preserves_frozen_batch_and_attachment_references(self):
        attachment = self.root / "synthetic attachment.txt"
        attachment.write_text("synthetic local file", encoding="utf-8")
        messages = [{"text": "first", "attachments": [{"status": "available", "path": str(attachment)}]},
                    {"text": "second", "attachments": [{"status": "failed", "count": 3}]}]
        backend, trace, _ = self.start_backend(request_updates={"messages": messages})
        messages[0]["text"] = "later mutation"
        final, _ = self.backend_events_until(backend, ("final", "failed", "unknown"))
        self.assertEqual(final["type"], "final", final)
        user = next(record["stdin"] for record in self.trace(trace) if record.get("stdin", {}).get("type") == "user")
        batch = json.loads(user["message"]["content"].split("\n", 1)[1])
        self.assertEqual([message["text"] for message in batch], ["first", "second"])
        self.assertEqual(batch[0]["attachments"], [{"status": "available", "path": str(attachment)}])
        self.assertEqual(batch[1]["attachments"], [{"status": "failed", "count": 3}])

    def test_public_facade_defaults_failed_attachment_count_to_one(self):
        backend, trace, _ = self.start_backend(request_updates={"messages": [{"text": "synthetic", "attachments": [{"status": "failed"}]}]})
        final, _ = self.backend_events_until(backend, ("final", "failed", "unknown"))
        self.assertEqual(final["type"], "final", final)
        user = next(record["stdin"] for record in self.trace(trace) if record.get("stdin", {}).get("type") == "user")
        batch = json.loads(user["message"]["content"].split("\n", 1)[1])
        self.assertEqual(batch[0]["attachments"], [{"status": "failed", "count": 1}])

    def test_public_facade_rejects_invalid_failed_attachment_count(self):
        for count in (0, -1, "3", 3.0, True, None):
            with self.subTest(count=count):
                with self.assertRaises(ValueError):
                    self.start_backend(request_updates={"messages": [{"text": "synthetic", "attachments": [{"status": "failed", "count": count}]}]})
        self.assertFalse(any(self.root.glob("facade-trace-*.jsonl")))

    def test_public_facade_maps_bound_one_time_approval(self):
        backend, trace, _ = self.start_backend("approval")
        approval, _ = self.backend_events_until(backend, ("approval_requested", "failed", "unknown"))
        self.assertEqual(approval["type"], "approval_requested", approval)
        summary = json.loads(approval["action_summary"])
        self.assertEqual(summary, {"tool": "Bash", "input": TOOL_INPUT, "cwd": str(self.root)})
        self.assertTrue(approval["action_digest"])
        with self.assertRaises(ValueError):
            backend.respond_approval(approval["approval_id"], "allow", "wrong-digest")
        backend.respond_approval(approval["approval_id"], "allow", approval["action_digest"])
        with self.assertRaises(ValueError):
            backend.respond_approval(approval["approval_id"], "allow", approval["action_digest"])
        final, _ = self.backend_events_until(backend, ("final", "failed", "unknown"))
        self.assertEqual(final["type"], "final", final)
        self.assertEqual(final["text"], "synthetic allowed")
        responses = [record for record in self.trace(trace) if "permission_response" in record]
        self.assertEqual(len(responses), 1)

    def test_public_facade_refuses_secret_or_oversize_approval_display(self):
        for tool_input in ({"command": "synthetic --api_key synthetic-sensitive-value"},
                           {"command": "x" * 3000}):
            with self.subTest(size=len(json.dumps(tool_input))):
                backend, trace, _ = self.start_backend("approval", tool_input=tool_input)
                final, events = self.backend_events_until(backend, ("final", "failed", "unknown"))
                self.assertEqual(final["type"], "failed", final)
                self.assertEqual(final["error_code"], "approval_not_displayable")
                self.assertFalse(any(event["type"] == "approval_requested" for event in events))
                self.assertNotIn("synthetic-sensitive-value", json.dumps(events))
                self.assertFalse(any("permission_response" in record for record in self.trace(trace)))

    def test_public_facade_rejects_synthetic_tokens_and_basic_auth_without_leaks(self):
        synthetic_commands = (
            "printf '%s' 'ghp_" + "A" * 36 + "'",
            "printf '%s' 'github_pat_" + "A" * 22 + "_" + "B" * 59 + "'",
            "printf '%s' 'glpat-" + "C" * 20 + "'",
            "printf '%s' 'AKIA" + "D" * 16 + "'",
            "curl https://synthetic-user:synthetic-pass@example.invalid/",
            "curl -u synthetic-user:synthetic-pass https://example.invalid/",
            "curl --user synthetic-user:synthetic-pass https://example.invalid/",
        )
        for index, command in enumerate(synthetic_commands):
            with self.subTest(synthetic_case=index):
                backend, trace, _ = self.start_backend("approval", tool_input={"command": command})
                final, events = self.backend_events_until(backend, ("final", "failed", "unknown"))
                self.assertEqual(final["type"], "failed", final)
                self.assertEqual(final["error_code"], "approval_not_displayable")
                self.assertFalse(any(event["type"] == "approval_requested" for event in events))
                rendered = json.dumps(events)
                for token in ("ghp_", "github_pat_", "glpat-", "AKIA", "synthetic-pass"):
                    self.assertNotIn(token, rendered)
                self.assertFalse(any("permission_response" in record for record in self.trace(trace)))

    def test_public_facade_serializes_parallel_approvals_after_allow_or_deny(self):
        for first_decision in ("allow", "deny"):
            with self.subTest(first_decision=first_decision):
                backend, trace, _ = self.start_backend("approval_parallel")
                first, first_events = self.backend_events_until(backend, ("approval_requested", "failed", "unknown"))
                self.assertEqual(first["type"], "approval_requested", first)
                self.assertEqual(first["approval_id"], "permission-1")
                self.assertEqual(len([event for event in first_events if event["type"] == "approval_requested"]), 1)
                deadline = time.monotonic() + 2
                while not any(record.get("both_permissions_emitted") for record in self.trace(trace)):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                for _ in range(3):
                    time.sleep(0.01)
                    self.assertFalse(any(event["type"] == "approval_requested" for event in backend.poll()))
                with self.assertRaises(ValueError):
                    backend.respond_approval("permission-2", "allow", first["action_digest"])
                backend.respond_approval(first["approval_id"], first_decision, first["action_digest"])
                second, second_events = self.backend_events_until(backend, ("approval_requested", "failed", "unknown"))
                self.assertEqual(second["type"], "approval_requested", second)
                self.assertEqual(second["approval_id"], "permission-2")
                self.assertEqual(len([event for event in second_events if event["type"] == "approval_requested"]), 1)
                self.assertNotEqual(first["action_digest"], second["action_digest"])
                self.assertEqual(json.loads(second["action_summary"])["input"], SECOND_TOOL_INPUT)
                with self.assertRaises(ValueError):
                    backend.respond_approval(second["approval_id"], "allow", first["action_digest"])
                second_decision = "deny" if first_decision == "allow" else "allow"
                backend.respond_approval(second["approval_id"], second_decision, second["action_digest"])
                final, _ = self.backend_events_until(backend, ("final", "failed", "unknown"))
                self.assertEqual(final["type"], "final", final)
                self.assertEqual(final["text"], "synthetic two approvals complete")
                responses = [record["permission_response"]["response"] for record in self.trace(trace) if "permission_response" in record]
                self.assertEqual([response["request_id"] for response in responses], ["permission-1", "permission-2"])
                self.assertEqual([response["response"]["behavior"] for response in responses], [first_decision, second_decision])

    def test_public_facade_rejects_mismatched_instance_workspace_and_policy(self):
        invalid = (
            {"backend_instance_ref": "different-instance"},
            {"cwd": "/synthetic-other-workspace"},
            {"execution_policy": {"approval": "always", "sandbox": "none"}},
            {"schema_version": 2}, {"backend_id": "codex"},
            {"account_scope": ""}, {"sender_key": ""}, {"conversation_key": ""},
            {"deadline_at": 0}, {"deadline_at": float("nan")},
        )
        for updates in invalid:
            with self.subTest(updates=updates):
                with self.assertRaises(ValueError):
                    self.start_backend(request_updates=updates)
        self.assertFalse(any(self.root.glob("facade-trace-*.jsonl")))

    def test_public_facade_rejects_unsupported_sandbox_and_extra_cli_flags(self):
        sandbox = {"approval": "explicit", "sandbox": "workspace-write"}
        with self.assertRaises(ValueError):
            self.start_backend(request_updates={"execution_policy": sandbox},
                               config_updates={"execution_policy": sandbox})
        with self.assertRaises(ValueError):
            self.start_backend(config_updates={"command": [str(self.script), "--dangerously-skip-permissions"]})
        self.assertFalse(any(self.root.glob("facade-trace-*.jsonl")))

    def test_public_facade_rejects_invalid_attachment_paths(self):
        for path in ("relative.txt", str(self.root / "nonexistent")):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    self.start_backend(request_updates={"messages": [{"text": "synthetic", "attachments": [{"status": "available", "path": path}]}]})

    def test_public_facade_auth_failure_exposes_code_not_raw_backend_text(self):
        backend, _, _ = self.start_backend("misleading_auth_error")
        final, events = self.backend_events_until(backend, ("final", "failed", "unknown"))
        self.assertEqual(final["type"], "failed", final)
        self.assertEqual(final["error_code"], "not_authenticated")
        self.assertNotIn("Not logged in", json.dumps(events))
        self.assertEqual(final["side_effect_state"], "possible")

    def test_public_facade_cancel_maps_to_terminal_and_revokes_approval(self):
        backend, _, _ = self.start_backend("approval")
        approval, _ = self.backend_events_until(backend, ("approval_requested", "failed", "unknown"))
        self.assertEqual(approval["type"], "approval_requested", approval)
        backend.cancel()
        final, _ = self.backend_events_until(backend, ("cancelled", "final", "failed", "unknown"))
        self.assertEqual(final["type"], "cancelled", final)
        self.assertEqual(final["side_effect_state"], "possible")
        with self.assertRaises(ValueError):
            backend.respond_approval(approval["approval_id"], "allow", approval["action_digest"])
        self.assertEqual(backend.poll(), [])


if __name__ == "__main__":
    unittest.main()
