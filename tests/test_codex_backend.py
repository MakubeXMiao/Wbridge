"""Codex app-server contract tests using only owned, synthetic stdio children.

No installed Codex binary, account, network, WeChat state, or auth token is used.
The fake is a real Python child so framing, EOF, signals, and environment
isolation exercise the production process boundary. Only its launch argv is
adapted to use this interpreter, rather than exec a new temporary shebang.
"""
import base64
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechat_bridge.backends._codex_protocol import (
    CodexConfig, CodexRun, RunRequest, SessionRef,
)
from wechat_bridge.backends.codex import create_backend


FAKE_SERVER = r'''
import json
import os
from pathlib import Path
import sys

options = OPTIONS
mode = options.get("mode", "final")
root = Path(__file__).parent
log = root / "requests.jsonl"
thread_id = options.get("thread_id", "synthetic-thread-a")
turn_id = "synthetic-turn-a"
approval = None
parallel_ids = options.get("approval_ids", [17, "17"])
parallel_decisions = []

def send(value):
    print(json.dumps(value), flush=True)

def reply(frame, value):
    send({"id": frame["id"], "result": value})

def event(method, params):
    send({"method": method, "params": params})

def item(text="Synthetic final answer", phase="final_answer"):
    value = {"id": "answer-a", "type": "agentMessage", "text": text}
    if phase is not None:
        value["phase"] = phase
    return value

def terminal(status="completed", text="Synthetic final answer", phase="final_answer"):
    event("turn/completed", {"threadId": thread_id, "turn": {
        "id": turn_id, "status": status,
        "items": [item(text, phase)] if text is not None else [],
        "error": {"message": "synthetic failure"} if status == "failed" else None}})

def request_approval():
    global approval
    method = options.get("approval_method", "item/commandExecution/requestApproval")
    params = {"threadId": thread_id, "turnId": turn_id, "itemId": "tool-a",
              "cwd": str(Path.cwd()), "command": "echo synthetic", "reason": "synthetic edit"}
    params.update(options.get("approval_params", {}))
    if mode.startswith("parallel_"):
        for number, rpc_id in enumerate(parallel_ids):
            send({"id": rpc_id, "method": method, "params": dict(params,
                itemId="parallel-tool-%d" % number, command="echo synthetic-%d" % number)})
        return
    if method == "item/fileChange/requestApproval" and options.get("file_changes", True):
        event("item/started", {"threadId": thread_id, "turnId": turn_id, "item": {
            "id": "tool-a", "type": "fileChange", "changes": [{"path": "synthetic.txt",
            "kind": {"type": "add"}, "diff": "+synthetic"}]}})
    approval = {"id": "approval-a", "method": method, "params": params}
    send(approval)
    if mode == "duplicate_approval":
        send(approval)
    elif mode == "changed_approval":
        changed = dict(approval, params=dict(params, command="echo changed-action"))
        send(changed)
    elif mode == "resolved_approval":
        event("serverRequest/resolved", {"requestId": "approval-a"})

(root / "child-environment.json").write_text(json.dumps({name: os.environ.get(name)
    for name in ["WBRIDGE_FAKE_SECRET", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
                 "GITHUB_TOKEN", "CODEX_THREAD_ID", "HOME", "PATH"]}))
(root / "argv.json").write_text(json.dumps(sys.argv[1:]))

for line in sys.stdin:
    frame = json.loads(line)
    with log.open("a") as stream:
        stream.write(json.dumps(frame) + "\n")
    method = frame.get("method")
    if method == "initialize":
        reply(frame, {"userAgent": "synthetic-app-server"})
    elif method == "initialized":
        pass
    elif method == "config/read":
        features = {name: False for name in ["hooks", "plugins", "apps", "memories",
            "shell_snapshot", "multi_agent", "browser_use", "computer_use",
            "skill_mcp_dependency_install"]}
        config = {"features": features, "project_doc_max_bytes": 0, "mcp_servers": {},
            "approval_policy": "on-request", "approvals_reviewer": "user",
            "sandbox_mode": "workspace-write"}
        config.update(options.get("config", {}))
        if options.get("honor_mcp_disable", True):
            for flag in sys.argv[1:]:
                if flag.startswith("mcp_servers.") and flag.endswith(".enabled=false"):
                    name = flag[len("mcp_servers."):-len(".enabled=false")]
                    if name.startswith('"'):
                        name = json.loads(name)
                    if name in config["mcp_servers"]:
                        config["mcp_servers"][name]["enabled"] = False
        reply(frame, {"config": config, "layers": options.get("layers", [])})
    elif method in ("thread/start", "thread/resume"):
        if mode == "blocked_stdin":
            import time
            time.sleep(2.8)
        if method == "thread/resume":
            thread_id = frame["params"]["threadId"]
        thread = {"id": options.get("response_thread", thread_id),
                  "sessionId": options.get("response_session", "synthetic-session-a"),
                  "status": {"type": options.get("thread_status", "idle")},
                  "cwd": options.get("response_cwd", str(Path.cwd()))}
        reply(frame, {"thread": thread,
            "approvalPolicy": options.get("response_policy", "on-request"),
            "approvalsReviewer": options.get("response_reviewer", "user"),
            "sandbox": options.get("sandbox", {"type": "workspaceWrite", "networkAccess": False})})
        if mode == "blocked_stdin":
            while True:
                time.sleep(1)
    elif method == "turn/start":
        reply(frame, {"turn": {"id": turn_id, "status": "inProgress"}})
        if "approval" in mode:
            request_approval()
        elif mode == "final":
            text = "x" * options["final_length"] if "final_length" in options else "Synthetic final answer"
            event("item/completed", {"threadId": thread_id, "turnId": turn_id,
                                      "item": item(text)})
            terminal(text=text)
        elif mode == "unphased":
            terminal(phase=None)
        elif mode == "empty":
            terminal(text="   ")
        elif mode == "commentary":
            terminal(text="Working on it", phase="commentary")
        elif mode == "failed":
            terminal(status="failed")
        elif mode == "event_error":
            event("error", {"threadId": thread_id, "turnId": turn_id,
                            "willRetry": False, "error": {"message": "synthetic"}})
        elif mode in ("wrong_thread", "wrong_turn"):
            event("item/completed", {"threadId": "unrelated-thread" if mode == "wrong_thread" else thread_id,
                "turnId": "unrelated-turn" if mode == "wrong_turn" else turn_id, "item": item()})
            terminal()
        elif mode == "wrong_terminal":
            event("turn/completed", {"threadId": thread_id,
                "turn": {"id": "unrelated-turn", "status": "completed", "items": [item()]}})
        elif mode == "conflicting_item":
            event("item/completed", {"threadId": thread_id, "turnId": turn_id, "item": item("original")})
            terminal(text="changed")
        elif mode == "disconnect":
            sys.exit(0)
        elif mode == "nonzero":
            sys.exit(7)
        elif mode == "nonzero_final":
            terminal()
            sys.exit(7)
        elif mode == "malformed":
            print("{not-json}", flush=True)
        elif mode == "malformed_terminal":
            event("turn/completed", {"threadId": thread_id, "turn": None})
        elif mode == "oversized_frame":
            terminal(text="x" * 8192)
        elif mode == "unknown_request":
            send({"id": "unknown-a", "method": "item/newCapability/requestApproval",
                  "params": {"threadId": thread_id, "turnId": turn_id, "itemId": "tool-a"}})
    elif method == "turn/interrupt":
        reply(frame, {})
        if mode == "late_final":
            terminal(text="Too late")
        elif mode != "interrupt_ack_only":
            terminal(status="interrupted", text=None)
    elif mode.startswith("parallel_") and frame.get("id") in parallel_ids and "result" in frame:
        parallel_decisions.append(frame["result"]["decision"])
        if len(parallel_decisions) == 1 and mode == "parallel_error_approval":
            event("error", {"threadId": thread_id, "turnId": turn_id,
                "willRetry": False, "error": {"message": "synthetic parallel failure"}})
            (root / "parallel-followup-ready").write_text("error")
        elif len(parallel_decisions) == 1 and mode == "parallel_resolved_approval":
            event("serverRequest/resolved", {"requestId": parallel_ids[1]})
            (root / "parallel-followup-ready").write_text("resolved")
        elif len(parallel_decisions) == 1 and mode == "parallel_terminal_approval":
            terminal(text="Synthetic early completion")
            (root / "parallel-followup-ready").write_text("terminal")
        elif len(parallel_decisions) == 2:
            terminal(text="Decisions: " + ",".join(parallel_decisions))
    elif frame.get("id") == "approval-a" and "result" in frame:
        if mode == "replayed_approval":
            send(approval)
        elif mode != "resolved_approval":
            terminal(text="Decision: " + frame["result"]["decision"])
'''


class FakeCodexCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wbridge-codex-fake-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.fake = self.root / "fake-codex"
        self.events = []
        self.codex_home = self.root / "synthetic-codex-home"
        self.codex_home.mkdir()
        isolated_environment = patch.dict(os.environ, {"CODEX_HOME": str(self.codex_home)})
        isolated_environment.start()
        self.addCleanup(isolated_environment.stop)
        real_popen = subprocess.Popen

        def launch_python_fake(argv, *args, **kwargs):
            # A newly executable script can spend the protocol budget before
            # its first Python instruction. The fake needs this interpreter,
            # not a kernel shebang launch; all real stdio/process options stay.
            if isinstance(argv, (list, tuple)) and argv and argv[0] == str(self.fake):
                argv = [sys.executable, *argv]
            return real_popen(argv, *args, **kwargs)

        fake_launcher = patch("wechat_bridge.backends._codex_protocol.subprocess.Popen",
                              side_effect=launch_python_fake)
        fake_launcher.start()
        self.addCleanup(fake_launcher.stop)

    def write_fake(self, mode="final", options=None):
        settings = dict(options or {}, mode=mode)
        source = FAKE_SERVER.replace("OPTIONS", ascii(settings), 1)
        self.fake.write_text("#!" + sys.executable + "\n" + source)
        self.fake.chmod(0o700)

    def make_run(self, mode="final", *, options=None, session=None, cwd=None,
                 timeout=2.0, approval_timeout=0.5, max_frame_bytes=2 * 1024 * 1024):
        self.write_fake(mode, options)
        config = CodexConfig(str(self.fake), str(self.workspace), startup_timeout=1.0,
                             stop_grace=0.2, approval_timeout=approval_timeout, max_frame_bytes=max_frame_bytes)
        request = RunRequest("account-a/instance-a/conversation-a", "exec-a", "attempt-a",
                             "Synthetic user input", str(cwd or self.workspace), timeout, session)
        run = CodexRun(config, request)
        self.addCleanup(run.close)
        return run

    def start(self, *args, **kwargs):
        return self.make_run(*args, **kwargs).start()

    def drain(self, run, predicate=None):
        deadline = time.monotonic() + 4
        while run.result is None and not (predicate and predicate()) and time.monotonic() < deadline:
            self.events.extend(run.poll(0.02))
        self.events.extend(run.poll(0))
        if predicate:
            self.assertTrue(predicate(), "expected protocol event did not arrive: %r" % run.result)
        else:
            self.assertIsNotNone(run.result, "synthetic child did not reach terminal")
        return run.result

    def requests(self):
        path = self.root / "requests.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def answer(self, run, decision="allow_once", **override):
        approval = run.approvals["approval-a"]
        binding = {name: approval[name] for name in ("scope", "execution_id", "attempt_id", "action_digest")}
        binding.update(override)
        return run.answer("approval-a", decision, **binding)


class CodexProtocolTests(FakeCodexCase):
    def test_command_requires_concrete_command_and_absolute_cwd(self):
        for params in ({"cwd": None}, {"cwd": "relative"}, {"command": 7}, {"command": "   "}):
            with self.subTest(params=params):
                self.events.clear()
                result = self.drain(self.start("approval", options={"approval_params": params}))
                self.assertEqual(result["error"], "missing_action_details")
                self.assertFalse(any(e["kind"] == "approval_required" for e in self.events))

    def test_valid_final_is_bound_to_current_execution(self):
        run = self.start()
        result = self.drain(run)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["text"], "Synthetic final answer")
        self.assertEqual(result["execution_id"], "exec-a")
        self.assertEqual(result["attempt_id"], "attempt-a")
        self.assertEqual(result["thread_id"], "synthetic-thread-a")
        self.assertEqual(result["session_id"], "synthetic-session-a")
        self.assertTrue(result["stop_confirmed"])
        self.assertEqual(sum(e["kind"] == "terminal" for e in self.events), 1)

    def test_provider_without_phase_requires_completed_turn(self):
        result = self.drain(self.start("unphased"))
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["text"], "Synthetic final answer")

    def test_empty_and_commentary_only_are_not_final_answers(self):
        for mode in ("empty", "commentary"):
            with self.subTest(mode=mode):
                result = self.drain(self.start(mode))
                self.assertEqual((result["status"], result["error"]), ("failed", "empty_final"))
                self.assertIsNone(result["text"])

    def test_failure_terminal_and_error_event_discard_text(self):
        for mode in ("failed", "event_error"):
            with self.subTest(mode=mode):
                result = self.drain(self.start(mode))
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])

    def test_unrelated_thread_or_turn_is_never_published(self):
        for mode in ("wrong_thread", "wrong_turn", "wrong_terminal"):
            with self.subTest(mode=mode):
                result = self.drain(self.start(mode))
                self.assertEqual(result["status"], "outcome_unknown")
                self.assertIsNone(result["text"])

    def test_conflicting_completed_item_is_rejected(self):
        result = self.drain(self.start("conflicting_item"))
        self.assertEqual(result["error"], "conflicting_item")
        self.assertIsNone(result["text"])

    def test_disconnect_malformed_and_nonzero_do_not_create_success(self):
        for mode in ("disconnect", "malformed", "nonzero"):
            with self.subTest(mode=mode):
                result = self.drain(self.start(mode))
                self.assertEqual(result["status"], "outcome_unknown")
                self.assertIsNone(result["text"])

    def test_nonzero_exit_invalidates_buffered_final(self):
        run = self.start("nonzero_final")
        run.transport.process.wait(timeout=1)
        result = self.drain(run)
        self.assertEqual((result["status"], result["exit_code"]), ("outcome_unknown", 7))
        self.assertIsNone(result["text"])

    def test_oversized_frame_never_becomes_a_partial_success(self):
        result = self.drain(self.start("oversized_frame", max_frame_bytes=4096))
        self.assertEqual((result["status"], result["error"]), ("outcome_unknown", "invalid_frame"))
        self.assertIsNone(result["text"])

    def test_malformed_terminal_has_failure_instead_of_escaping_driver(self):
        result = self.drain(self.start("malformed_terminal"))
        self.assertEqual(result["status"], "outcome_unknown")
        self.assertIsNone(result["text"])

    def test_explicit_resume_uses_only_owned_thread_and_cwd(self):
        owned = SessionRef("account-a/instance-a/conversation-a", "owned-thread", str(self.workspace.resolve()),
                           "synthetic-session-a")
        result = self.drain(self.start(session=owned))
        self.assertEqual(result["thread_id"], "owned-thread")
        resumes = [r for r in self.requests() if r.get("method") == "thread/resume"]
        self.assertEqual(len(resumes), 1)
        self.assertEqual(resumes[0]["params"]["threadId"], "owned-thread")
        self.assertEqual(resumes[0]["params"]["cwd"], str(self.workspace.resolve()))
        self.assertFalse(any(r.get("method") == "thread/start" for r in self.requests()))

    def test_resume_scope_cwd_and_response_mismatch_are_rejected(self):
        valid = dict(scope="account-a/instance-a/conversation-a", thread_id="owned-thread", cwd=str(self.workspace.resolve()))
        for field, bad in (("scope", "other-account"), ("cwd", str(self.root))):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "session_scope_mismatch"):
                self.make_run(session=SessionRef(**dict(valid, **{field: bad})))
        result = self.drain(self.start(session=SessionRef(**valid), options={"response_thread": "wrong-thread"}))
        self.assertNotEqual(result["status"], "completed")
        self.assertEqual(result["error"], "thread_mismatch")
        self.assertFalse(any(r.get("method") == "turn/start" for r in self.requests()))

    def test_resume_session_id_mismatch_does_not_start_turn(self):
        owned = SessionRef("account-a/instance-a/conversation-a", "owned-thread", str(self.workspace.resolve()),
                           "owned-session")
        result = self.drain(self.start(session=owned))
        self.assertEqual(result["error"], "session_mismatch")
        self.assertFalse(any(r.get("method") == "turn/start" for r in self.requests()))

    def test_restricted_turn_and_user_approval_policy_are_explicit(self):
        self.drain(self.start())
        turn = next(r["params"] for r in self.requests() if r.get("method") == "turn/start")
        self.assertEqual(turn["approvalPolicy"], "on-request")
        self.assertEqual(turn["approvalsReviewer"], "user")
        self.assertEqual(turn["sandboxPolicy"], {"type": "workspaceWrite", "writableRoots": [str(self.workspace.resolve())],
            "networkAccess": False, "excludeTmpdirEnvVar": True, "excludeSlashTmp": True})

    def test_isolation_preflight_precedes_thread_creation(self):
        self.drain(self.start())
        requests = self.requests()
        methods = [r.get("method") for r in requests]
        self.assertEqual(methods[:4], ["initialize", "initialized", "config/read", "thread/start"])
        self.assertTrue(requests[2]["params"]["includeLayers"])
        self.assertEqual(requests[2]["params"]["cwd"], str(self.workspace.resolve()))
        argv = json.loads((self.root / "argv.json").read_text())
        for required in ('notify=[]', 'web_search="disabled"', 'project_doc_fallback_filenames=[]'):
            self.assertIn(required, argv)

    def test_unverified_config_is_rejected_before_thread_creation(self):
        for options in ({"layers": None}, {"config": {"features": {}}},
                        {"config": {"project_doc_max_bytes": 1024}},
                        {"config": {"approval_policy": "never"}}):
            with self.subTest(options=options):
                result = self.drain(self.start(options=options))
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["thread_id"])
        self.assertFalse(any(r.get("method") == "thread/start" for r in self.requests()))

    def test_malformed_nested_config_has_terminal_failure(self):
        for options in ({"config": {"features": None}}, {"layers": [None]},
                        {"layers": [{"name": None}]}):
            with self.subTest(options=options):
                result = self.drain(self.start(options=options))
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])
        self.assertFalse(any(r.get("method") == "thread/start" for r in self.requests()))

    def test_notify_and_injected_shell_environment_are_rejected(self):
        for config in ({"notify": ["synthetic-notifier"]},
                       {"shell_environment_policy": {"set": {"SYNTHETIC_SECRET": "synthetic-value"}}}):
            with self.subTest(config=config):
                result = self.drain(self.start(options={"config": config}))
                self.assertEqual((result["status"], result["error"]), ("failed", "runtime_isolation_unverified"))
                self.assertIsNone(result["thread_id"])
        self.assertFalse(any(r.get("method") == "thread/start" for r in self.requests()))

    def test_enabled_mcp_requires_verified_restart_before_thread_start(self):
        result = self.drain(self.start(options={"config": {
            "mcp_servers": {"synthetic_reader": {"enabled": True}}}}))
        self.assertEqual(result["status"], "completed")
        methods = [r.get("method") for r in self.requests()]
        self.assertEqual(methods.count("initialize"), 2)
        self.assertEqual(methods.count("config/read"), 2)
        for frame in self.requests():
            if frame.get("method") == "config/read":
                self.assertEqual(frame["params"], {"includeLayers": True, "cwd": str(self.workspace.resolve())})
        self.assertEqual(methods.count("thread/start"), 1)
        self.assertLess(max(i for i, value in enumerate(methods) if value == "config/read"),
                        methods.index("thread/start"))
        argv = json.loads((self.root / "argv.json").read_text())
        self.assertTrue(any(value.startswith("mcp_servers.") and value.endswith(".enabled=false") for value in argv))

    def test_uncooperative_enabled_mcp_fails_closed(self):
        result = self.drain(self.start(options={"honor_mcp_disable": False,
            "config": {"mcp_servers": {"synthetic_reader": {"enabled": True}}}}))
        self.assertEqual(result["status"], "failed")
        self.assertFalse(any(r.get("method") == "thread/start" for r in self.requests()))

    def test_inherited_allow_rules_fail_before_any_turn(self):
        rules = self.codex_home / "rules"
        rules.mkdir()
        (rules / "synthetic.rules").write_text('prefix_rule(pattern=["synthetic"], decision="allow")\n')
        result = self.drain(self.start())
        self.assertEqual((result["status"], result["error"]), ("failed", "inherited_rules_present"))
        self.assertFalse(any(r.get("method") in ("thread/start", "turn/start") for r in self.requests()))

    def test_active_thread_and_network_enabled_sandbox_are_rejected(self):
        for options, error in (({"thread_status": "active"}, "thread_not_idle"),
                ({"sandbox": {"type": "workspaceWrite", "networkAccess": True}}, "sandbox_mismatch")):
            with self.subTest(options=options):
                result = self.drain(self.start(options=options))
                self.assertEqual(result["error"], error)
                self.assertIsNone(result["text"])
        self.assertFalse(any(r.get("method") == "turn/start" for r in self.requests()))

    def test_backend_cannot_silently_downgrade_approval_policy(self):
        for option in ({"response_policy": "never"}, {"response_reviewer": "auto"}):
            with self.subTest(option=option):
                result = self.drain(self.start(options=option))
                self.assertEqual(result["error"], "approval_policy_mismatch")
                self.assertIsNone(result["text"])

    def test_allow_once_and_deny_are_protocol_replies(self):
        for decision, wire in (("allow_once", "accept"), ("deny", "decline")):
            with self.subTest(decision=decision):
                run = self.start("approval")
                self.drain(run, lambda: bool(run.approvals))
                self.assertTrue(self.answer(run, decision))
                result = self.drain(run)
                self.assertEqual(result["text"], "Decision: " + wire)
                replies = [r for r in self.requests() if r.get("id") == "approval-a" and "result" in r]
                self.assertEqual(replies[-1]["result"], {"decision": wire})

    def test_file_change_approval_uses_explicit_single_decision(self):
        run = self.start("approval", options={"approval_method": "item/fileChange/requestApproval"})
        self.drain(run, lambda: bool(run.approvals))
        self.assertEqual(run.approvals["approval-a"]["kind"], "file_change")
        self.assertTrue(self.answer(run, "deny"))
        self.assertEqual(self.drain(run)["text"], "Decision: decline")

    def test_file_permission_without_changes_or_with_grant_root_is_rejected(self):
        for options in ({"file_changes": False}, {"approval_params": {"grantRoot": "/synthetic"}}):
            with self.subTest(options=options):
                result = self.drain(self.start("approval", options=dict(options,
                    approval_method="item/fileChange/requestApproval")))
                self.assertEqual(result["error"], "unsupported_file_permission")
                self.assertIsNone(result["text"])

    def test_turn_wide_permissions_are_not_silently_auto_approved(self):
        result = self.drain(self.start("approval", options={
            "approval_method": "item/permissions/requestApproval"}))
        self.assertEqual(result["error"], "unsupported_interaction")
        self.assertIsNone(result["text"])

    def test_command_approval_cannot_hide_extended_permission_grants(self):
        variants = (
            {"additionalPermissions": {"network": {"enabled": True}}},
            {"networkApprovalContext": {"host": "synthetic.invalid", "protocol": "https"}},
            {"kind": "network"},
            {"availableDecisions": ["acceptForSession", "decline"]},
            {"availableDecisions": ["accept"]},
            {"availableDecisions": "accept"},
        )
        for fields in variants:
            with self.subTest(fields=fields):
                result = self.drain(self.start("approval", options={"approval_params": fields}))
                self.assertEqual(result["error"], "unsupported_permission_scope")
                self.assertIsNone(result["text"])
        self.assertFalse(any(event["kind"] == "approval_required" for event in self.events))
        self.assertFalse(any(frame.get("result", {}).get("decision") == "accept"
                             for frame in self.requests()))

    def test_wrong_identity_digest_and_unsupported_decision_are_rejected(self):
        run = self.start("approval")
        self.drain(run, lambda: bool(run.approvals))
        for field in ("scope", "execution_id", "attempt_id", "action_digest"):
            with self.subTest(field=field):
                self.assertFalse(self.answer(run, **{field: "wrong"}))
        self.assertFalse(self.answer(run, "allow_forever"))
        self.assertTrue(self.answer(run, "deny"))
        self.drain(run)
        replies = [r for r in self.requests() if r.get("id") == "approval-a" and "result" in r]
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0]["result"]["decision"], "decline")

    def test_duplicate_approval_is_idempotent_and_can_only_be_answered_once(self):
        run = self.start("duplicate_approval")
        self.drain(run, lambda: bool(run.approvals))
        self.events.extend(run.poll(0.02))
        approval = dict(run.approvals["approval-a"])
        self.assertEqual(sum(e["kind"] == "approval_required" for e in self.events), 1)
        self.assertTrue(self.answer(run))
        self.assertFalse(run.answer("approval-a", "allow_once", **{k: approval[k]
            for k in ("scope", "execution_id", "attempt_id", "action_digest")}))
        self.drain(run)

    def test_changed_or_replayed_approval_fails_closed(self):
        run = self.start("changed_approval")
        result = self.drain(run)
        self.assertEqual(result["error"], "changed_approval")
        run = self.start("replayed_approval")
        self.drain(run, lambda: bool(run.approvals))
        self.assertTrue(self.answer(run))
        self.assertEqual(self.drain(run)["error"], "replayed_approval")

    def test_approval_expiry_cannot_be_approved(self):
        run = self.start("approval", approval_timeout=0.04)
        self.drain(run, lambda: bool(run.approvals))
        time.sleep(0.06)
        self.assertFalse(self.answer(run))
        result = self.drain(run)
        self.assertEqual(result["status"], "timed_out")
        self.assertIsNone(result["text"])
        replies = [r for r in self.requests() if r.get("id") == "approval-a" and "result" in r]
        self.assertNotIn("accept", [r["result"]["decision"] for r in replies])

    def test_resolved_request_invalidates_approval_without_granting(self):
        run = self.start("resolved_approval")
        self.drain(run, lambda: any(e["kind"] == "approval_expired" for e in self.events))
        self.assertEqual(run.approvals, {})
        self.assertFalse(run.answer("approval-a", "allow_once", scope=run.request.scope,
            execution_id="exec-a", attempt_id="attempt-a", action_digest="unknown"))
        run.cancel()
        self.drain(run)

    def test_unsafe_approval_action_is_rejected_without_leaking_secrets(self):
        run = self.start("approval", options={"approval_params": {"command":
            "echo token=SYNTHETIC-SECRET https://example.invalid/private Bearer SYNTHETIC-BEARER"}})
        result = self.drain(run)
        self.assertEqual(result["error"], "unsafe_action_summary")
        self.assertFalse(any(e["kind"] == "approval_required" for e in self.events))
        summary = json.dumps(self.events)
        for secret in ("SYNTHETIC-SECRET", "SYNTHETIC-BEARER", "example.invalid"):
            self.assertNotIn(secret, summary)

    def test_approval_from_wrong_turn_is_rejected(self):
        result = self.drain(self.start("approval", options={"approval_params": {"turnId": "foreign-turn"}}))
        self.assertEqual(result["error"], "approval_scope_mismatch")
        self.assertIsNone(result["text"])

    def test_unknown_server_request_fails_closed(self):
        result = self.drain(self.start("unknown_request"))
        self.assertEqual(result["error"], "unsupported_interaction")
        self.assertIsNone(result["text"])

    def test_cancel_waits_for_interrupted_terminal(self):
        run = self.start("wait")
        run.cancel()
        result = self.drain(run)
        self.assertEqual(result["status"], "cancelled")
        self.assertTrue(result["interrupt_confirmed"])
        self.assertTrue(result["stop_confirmed"])
        self.assertIsNone(result["text"])

    def test_interrupt_ack_without_terminal_is_unconfirmed(self):
        run = self.start("interrupt_ack_only")
        run.cancel()
        result = self.drain(run)
        self.assertEqual((result["status"], result["error"]), ("cancelled", "interrupt_unconfirmed"))
        self.assertFalse(result["interrupt_confirmed"])
        self.assertTrue(result["stop_confirmed"])

    def test_timeout_interrupts_owned_turn(self):
        result = self.drain(self.start("wait", timeout=0.8))
        self.assertEqual(result["status"], "timed_out")
        self.assertTrue(result["interrupt_confirmed"])
        self.assertIsNone(result["text"])

    def test_final_after_cancellation_is_never_published(self):
        run = self.start("late_final")
        run.cancel()
        result = self.drain(run)
        self.assertEqual(result["status"], "cancelled")
        self.assertFalse(result["interrupt_confirmed"])
        self.assertIsNone(result["text"])

    def test_cwd_symlink_cannot_escape_workspace_root(self):
        outside = self.root / "outside"
        outside.mkdir()
        link = self.workspace / "escape"
        link.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "workspace_outside_root"):
            self.make_run(cwd=link)
        self.assertFalse((self.root / "child-environment.json").exists())

    def test_child_does_not_inherit_parent_credentials_or_session(self):
        names = ("WBRIDGE_FAKE_SECRET", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GITHUB_TOKEN", "CODEX_THREAD_ID")
        with patch.dict(os.environ, {name: "SYNTHETIC-PARENT-SECRET" for name in names}):
            self.drain(self.start())
        environment = json.loads((self.root / "child-environment.json").read_text())
        self.assertTrue(all(environment[name] is None for name in names))
        self.assertEqual(environment["HOME"], os.environ.get("HOME"))
        self.assertEqual(environment["PATH"], os.environ.get("PATH"))


class CodexPublicContractTests(FakeCodexCase):
    def test_start_bounds_blocked_large_input_by_shared_startup_budget(self):
        backend = self.backend("blocked_stdin")
        request = self.request(deadline_at=time.time() + 20,
            messages=[{"text": "x" * 950000, "attachments": []}])
        before = time.monotonic()
        backend.start(request)
        self.assertLess(time.monotonic() - before, 5.0)
        final, _ = self.finish(backend)
        self.assertEqual(final["type"], "unknown")
        self.assertEqual(final["error_code"], "codex_write_timeout")

    def config(self, **changes):
        return dict({"command": [str(self.fake)], "cwd": str(self.workspace.resolve()),
            "instance_id": "local-codex-a", "execution_policy": {
                "approval": "explicit", "sandbox": "workspace-write"}}, **changes)

    def request(self, **changes):
        return dict({"schema_version": 1, "execution_id": "job-a", "attempt_id": "attempt-a",
            "account_scope": "account-a", "sender_key": "sender-a", "backend_id": "codex",
            "backend_instance_ref": "local-codex-a", "conversation_key": "conversation-a",
            "session_id": None, "messages": [{"message_id": "message-a", "text": "Synthetic task",
                                               "attachments": []}],
            "cwd": str(self.workspace.resolve()), "deadline_at": time.time() + 5,
            "execution_policy": {"approval": "explicit", "sandbox": "workspace-write"}}, **changes)

    def backend(self, mode="final", *, options=None, config=None):
        self.write_fake(mode, options)
        backend = create_backend(self.config(**(config or {})))
        self.addCleanup(backend.close)
        return backend

    def public_events(self, backend, wanted):
        events = []
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            before = time.monotonic()
            batch = backend.poll()
            self.assertLess(time.monotonic() - before, 1.0)
            self.assertLessEqual(len(batch), 100)
            events.extend(batch)
            if any(event["type"] in wanted for event in events):
                return events
            time.sleep(0.005)
        self.fail("public contract event did not arrive: %r" % events)

    def finish(self, backend):
        events = self.public_events(backend, {"final", "failed", "cancelled", "unknown"})
        self.assertEqual(sum(e["type"] in {"final", "failed", "cancelled", "unknown"} for e in events), 1)
        self.assertEqual(backend.poll(), [])
        return events[-1], events

    def pending_pair(self, mode="parallel_approval", *, approval_timeout=120.0, options=None):
        backend = self.backend(mode, options=options)
        backend.protocol_config = replace(backend.protocol_config, approval_timeout=approval_timeout)
        backend.start(self.request())
        events = self.public_events(backend, {"approval_requested"})
        deadline = time.monotonic() + 2
        while len(backend.run.approvals) < 2 and time.monotonic() < deadline:
            events.extend(backend.poll())
            time.sleep(0.005)
        self.assertEqual(len(backend.run.approvals), 2)
        approvals = [event for event in events if event["type"] == "approval_requested"]
        self.assertEqual(len(approvals), 1, "only one approval may reach the public dispatcher")
        return backend, approvals[0], {key: dict(value) for key, value in backend.run.approvals.items()}

    def wait_parallel_followup(self):
        deadline = time.monotonic() + 2
        while not (self.root / "parallel-followup-ready").exists() and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertTrue((self.root / "parallel-followup-ready").exists())

    def assert_approvals_cleared(self, backend):
        self.assertEqual(backend.run.approvals, {})
        self.assertFalse(backend._approval_ids)
        self.assertFalse(backend._approval_queue)
        self.assertIsNotNone(backend.run.transport.process.poll())

    @staticmethod
    def session_payload(token):
        return json.loads(base64.urlsafe_b64decode(token[5:] + "=" * (-len(token[5:]) % 4)))

    @staticmethod
    def encode_session(prefix, payload, total_chars=None):
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        if total_chars is not None:
            # Valid trailing JSON whitespace lets the exact public boundary be
            # tested independently of a particular encoder's compact layout.
            raw += b" " * (((total_chars - len(prefix)) * 3 // 4) - len(raw))
        return prefix + base64.urlsafe_b64encode(raw).decode().rstrip("=")

    def test_factory_requires_executable_argv_and_explicit_policy(self):
        self.write_fake()
        for change in ({"command": str(self.fake)}, {"command": [str(self.fake), "--bypass"]},
                       {"execution_policy": {"approval": "never", "sandbox": "workspace-write"}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                create_backend(self.config(**change))

    def test_final_event_identity_ids_session_and_frozen_input(self):
        backend = self.backend()
        request = self.request()
        self.assertIsNone(backend.start(request))
        request["messages"][0]["text"] = "mutated after dispatch"
        final, events = self.finish(backend)
        self.assertEqual(final["type"], "final")
        self.assertEqual(final["text"], "Synthetic final answer")
        self.assertTrue(final["session_id"].startswith("wbc2."))
        self.assertEqual(len({event["event_id"] for event in events}), len(events))
        for event in events:
            self.assertEqual(event["execution_id"], "job-a")
            self.assertEqual(event["attempt_id"], "attempt-a")
        start = next(r for r in self.requests() if r.get("method") == "turn/start")
        self.assertEqual(start["params"]["input"][0]["text"], "Synthetic task")
        with self.assertRaisesRegex(ValueError, "already_used"):
            backend.start(self.request())

    def test_two_explicit_resumes_preserve_same_conversation(self):
        backend = self.backend()
        backend.start(self.request())
        previous, _ = self.finish(backend)
        for number in (2, 3):
            backend = self.backend()
            backend.start(self.request(session_id=previous["session_id"], execution_id="job-%d" % number,
                                       attempt_id="attempt-%d" % number))
            previous, _ = self.finish(backend)
            self.assertEqual(previous["type"], "final")
            self.assertEqual(previous["execution_id"], "job-%d" % number)
        starts = [r for r in self.requests() if r.get("method") == "thread/start"]
        resumes = [r for r in self.requests() if r.get("method") == "thread/resume"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(len(resumes), 2)
        self.assertEqual([r["params"]["threadId"] for r in resumes], ["synthetic-thread-a"] * 2)

    def test_session_token_rejects_other_account_sender_conversation_or_instance(self):
        backend = self.backend()
        backend.start(self.request())
        final, _ = self.finish(backend)
        for name in ("account_scope", "sender_key", "conversation_key", "backend_instance_ref"):
            with self.subTest(name=name):
                backend = self.backend(config={"instance_id": "other"} if name == "backend_instance_ref" else None)
                with self.assertRaisesRegex(ValueError, "session_scope_mismatch"):
                    backend.start(self.request(session_id=final["session_id"], **{name: "other"}))
        self.assertEqual(sum(r.get("method") == "thread/start" for r in self.requests()), 1)
        self.assertFalse(any(r.get("method") == "thread/resume" for r in self.requests()))

    def test_request_version_instance_cwd_policy_and_deadline_fail_before_launch(self):
        for change in ({"schema_version": 2}, {"backend_id": "claude_code"},
                       {"backend_instance_ref": "other"}, {"cwd": str(self.root)},
                       {"execution_policy": {"approval": "never"}}, {"deadline_at": 0},
                       {"deadline_at": float("nan")}):
            with self.subTest(change=change):
                backend = self.backend()
                with self.assertRaises(ValueError):
                    backend.start(self.request(**change))
        self.assertEqual(self.requests(), [])

    def test_public_allow_deny_digest_binding_and_duplicate_rejection(self):
        for decision, wire in (("allow", "accept"), ("deny", "decline")):
            with self.subTest(decision=decision):
                backend = self.backend("approval")
                backend.start(self.request())
                events = self.public_events(backend, {"approval_requested"})
                approval = next(e for e in events if e["type"] == "approval_requested")
                self.assertEqual(len(approval["action_digest"]), 64)
                self.assertLessEqual(len(approval["action_summary"]), 2000)
                self.assertGreater(approval["expires_at"], time.time())
                with self.assertRaises(ValueError):
                    backend.respond_approval(approval["approval_id"], decision, "0" * 64)
                backend.respond_approval(approval["approval_id"], decision, approval["action_digest"])
                with self.assertRaises(ValueError):
                    backend.respond_approval(approval["approval_id"], decision, approval["action_digest"])
                final, _ = self.finish(backend)
                self.assertEqual(final["text"], "Decision: " + wire)

    def test_parallel_approvals_are_serial_with_original_ids_digests_and_expiry(self):
        for decisions, expected in ((("allow", "allow"), "accept,accept"),
                                    (("deny", "allow"), "decline,accept")):
            with self.subTest(decisions=decisions):
                backend, first, original = self.pending_pair()
                self.assertEqual(first["approval_id"], "i:17")
                self.assertEqual(first["action_digest"], original[17]["action_digest"])
                with self.assertRaises(ValueError):
                    backend.respond_approval("s:17", "allow", original["17"]["action_digest"])
                time.sleep(0.04)
                self.assertFalse(any(event["type"] == "approval_requested" for event in backend.poll()))
                backend.respond_approval(first["approval_id"], decisions[0], first["action_digest"])
                events = self.public_events(backend, {"approval_requested"})
                second = next(event for event in events if event["type"] == "approval_requested")
                self.assertEqual(second["approval_id"], "s:17")
                self.assertEqual(second["action_digest"], original["17"]["action_digest"])
                self.assertEqual(second["action_summary"], original["17"]["summary"])
                self.assertAlmostEqual(second["expires_at"] - time.time(),
                                       original["17"]["deadline"] - time.monotonic(), delta=0.03)
                with self.assertRaises(ValueError):
                    backend.respond_approval(second["approval_id"], decisions[1], first["action_digest"])
                backend.respond_approval(second["approval_id"], decisions[1], second["action_digest"])
                final, _ = self.finish(backend)
                self.assertEqual(final["text"], "Decisions: " + expected)
                replies = [frame for frame in self.requests() if frame.get("id") in (17, "17") and "result" in frame]
                self.assertEqual([(type(frame["id"]), frame["id"]) for frame in replies[-2:]],
                                 [(int, 17), (str, "17")])
                self.assert_approvals_cleared(backend)

    def test_long_provider_approval_id_is_bounded_and_maps_to_original(self):
        long_id = "synthetic-" + "a" * 256
        backend, first, original = self.pending_pair(options={"approval_ids": [long_id, 17]})
        self.assertLessEqual(len(first["approval_id"]), 256)
        backend.respond_approval(first["approval_id"], "deny", first["action_digest"])
        second = next(event for event in self.public_events(backend, {"approval_requested"})
                      if event["type"] == "approval_requested")
        backend.respond_approval(second["approval_id"], "allow", second["action_digest"])
        self.assertEqual(self.finish(backend)[0]["type"], "final")
        replies = [frame for frame in self.requests() if frame.get("id") == long_id and "result" in frame]
        self.assertEqual([frame["result"]["decision"] for frame in replies], ["decline"])
        self.assertEqual(first["action_digest"], original[long_id]["action_digest"])

    def test_cancel_and_close_reject_active_and_queued_approvals(self):
        for action in ("cancel", "close"):
            with self.subTest(action=action):
                backend, first, _ = self.pending_pair()
                with patch.object(backend.run.transport, "send", wraps=backend.run.transport.send) as send:
                    getattr(backend, action)()
                    final, events = self.finish(backend)
                    rejected = [call.args[0]["id"] for call in send.call_args_list
                                if call.args[0].get("result", {}).get("decision") == "cancel"]
                self.assertEqual(rejected, [17, "17"])
                self.assertEqual(final["type"], "cancelled" if action == "cancel" else "unknown")
                self.assertFalse(any(event["type"] == "approval_requested" for event in events))
                with self.assertRaises(ValueError):
                    backend.respond_approval(first["approval_id"], "allow", first["action_digest"])
                self.assert_approvals_cleared(backend)

    def test_error_terminal_and_resolved_request_clear_queued_approvals(self):
        for mode, expected in (("parallel_error_approval", "failed"),
                               ("parallel_terminal_approval", "unknown"),
                               ("parallel_resolved_approval", "cancelled")):
            with self.subTest(mode=mode):
                (self.root / "parallel-followup-ready").unlink(missing_ok=True)
                backend, first, _ = self.pending_pair(mode)
                backend.respond_approval(first["approval_id"], "allow", first["action_digest"])
                self.wait_parallel_followup()
                with patch.object(backend.run.transport, "send", wraps=backend.run.transport.send) as send:
                    final, events = self.finish(backend)
                self.assertEqual(final["type"], expected)
                if mode == "parallel_terminal_approval":
                    self.assertEqual(final["error_code"], "codex_unresolved_approval")
                self.assertFalse(any(event["type"] == "approval_requested" for event in events))
                if mode != "parallel_resolved_approval":
                    rejected = [call.args[0]["id"] for call in send.call_args_list
                                if call.args[0].get("result", {}).get("decision") == "cancel"]
                    self.assertEqual(rejected, ["17"])
                self.assert_approvals_cleared(backend)

    def test_queued_approval_keeps_original_expiry_and_cannot_be_promoted_late(self):
        backend, first, original = self.pending_pair(approval_timeout=0.15)
        backend.respond_approval(first["approval_id"], "allow", first["action_digest"])
        time.sleep(max(0, original["17"]["deadline"] - time.monotonic()) + 0.03)
        with patch.object(backend.run.transport, "send", wraps=backend.run.transport.send) as send:
            final, events = self.finish(backend)
        self.assertEqual(final["type"], "failed")
        self.assertFalse(any(event["type"] == "approval_requested" for event in events))
        self.assertTrue(any(call.args[0].get("id") == "17" and
                            call.args[0].get("result", {}).get("decision") == "cancel"
                            for call in send.call_args_list))
        self.assert_approvals_cleared(backend)

    def test_public_final_exact_limit_passes_and_oversized_is_explicit_failure(self):
        for size in (64000, 64001):
            with self.subTest(size=size):
                backend = self.backend(options={"final_length": size})
                backend.start(self.request())
                terminal, events = self.finish(backend)
                if size == 64000:
                    self.assertEqual(terminal["type"], "final")
                    self.assertEqual(terminal["text"], "x" * size)
                else:
                    self.assertEqual((terminal["type"], terminal["error_code"], terminal["side_effect_state"]),
                                     ("failed", "codex_final_too_large", "possible"))
                    self.assertNotIn("text", terminal)
                    self.assertFalse(any(event["type"] == "final" for event in events))

    def test_long_cwd_and_maximum_ascii_ids_fit_session_and_resume_verbatim(self):
        cwd = self.workspace
        for _ in range(5):
            cwd = cwd / ("workspace-" + "a" * 80)
        cwd.mkdir(parents=True)
        options = {"thread_id": "t" * 256, "response_session": "s" * 256}
        for previous in (None, "resume"):
            backend = self.backend(options=options, config={"cwd": str(cwd.resolve())})
            backend.start(self.request(cwd=str(cwd.resolve()),
                                       session_id=None if previous is None else token))
            final, _ = self.finish(backend)
            self.assertEqual(final["type"], "final")
            token = final["session_id"]
            self.assertLessEqual(len(token), 1024)
            self.assertEqual(self.session_payload(token)[2:], ["t" * 256, "s" * 256])
        resume = next(frame for frame in self.requests() if frame.get("method") == "thread/resume")
        self.assertEqual(resume["params"]["threadId"], "t" * 256)
        other = self.backend()
        with self.assertRaisesRegex(ValueError, "session_scope_mismatch"):
            other.start(self.request(session_id=token))

    def test_session_input_exact_limit_legacy_v1_and_invalid_id_boundaries(self):
        backend = self.backend()
        backend.start(self.request())
        token = self.finish(backend)[0]["session_id"]
        payload = self.session_payload(token)
        for size in (1024, 1025):
            with self.subTest(size=size):
                bounded = self.encode_session("wbc2.", payload, size)
                self.assertEqual(len(bounded), size)
                backend = self.backend()
                if size == 1024:
                    backend.start(self.request(session_id=bounded))
                    self.assertEqual(self.finish(backend)[0]["type"], "final")
                else:
                    before = len(self.requests())
                    with self.assertRaisesRegex(ValueError, "session_scope_mismatch"):
                        backend.start(self.request(session_id=bounded))
                    self.assertEqual(len(self.requests()), before)
        legacy = {"v": 1, "scope": payload[0], "cwd": str(self.workspace.resolve()),
                  "thread": payload[2], "session": payload[3]}
        backend = self.backend()
        backend.start(self.request(session_id=self.encode_session("wbc1.", legacy)))
        self.assertEqual(self.finish(backend)[0]["type"], "final")
        for field, invalid in (("thread", "t" * 257), ("session", "s" * 257), ("session", 17)):
            with self.subTest(field=field, invalid=invalid):
                backend = self.backend()
                with self.assertRaisesRegex(ValueError, "session_scope_mismatch"):
                    backend.start(self.request(session_id=self.encode_session("wbc1.", dict(legacy, **{field: invalid}))))

    def test_unrepresentable_session_never_emits_oversized_public_final(self):
        backend = self.backend(options={"thread_id": "中" * 256, "response_session": "文" * 256})
        backend.start(self.request())
        terminal, events = self.finish(backend)
        self.assertEqual((terminal["type"], terminal["error_code"], terminal["side_effect_state"]),
                         ("failed", "codex_session_ref_too_large", "possible"))
        self.assertNotIn("session_id", terminal)
        self.assertFalse(any(event["type"] == "final" for event in events))

    def test_local_image_uses_verified_local_path(self):
        image = self.workspace / "synthetic.png"
        image.write_bytes(b"\x89PNG\r\n\x1a\nsynthetic bytes")
        backend = self.backend()
        backend.start(self.request(messages=[{"message_id": "image-a", "text": "", "attachments": [
            {"path": str(image), "status": "available"}]}]))
        final, _ = self.finish(backend)
        self.assertEqual(final["type"], "final")
        start = next(r for r in self.requests() if r.get("method") == "turn/start")
        images = [item for item in start["params"]["input"] if item["type"] == "localImage"]
        self.assertEqual(images, [{"type": "localImage", "path": str(image.resolve())}])

    def test_attachment_outside_root_and_symlink_escape_are_rejected(self):
        outside = self.root / "outside.png"
        outside.write_bytes(b"\x89PNG\r\n\x1a\nsynthetic")
        link = self.workspace / "escape.png"
        link.symlink_to(outside)
        for path in (str(outside), str(link), "relative.png", "https://example.invalid/image.png"):
            with self.subTest(path=path):
                backend = self.backend()
                with self.assertRaises(ValueError):
                    backend.start(self.request(messages=[{"text": "synthetic", "attachments": [
                        {"path": path, "status": "available"}]}]))
        self.assertEqual(self.requests(), [])

    def test_failed_attachment_is_explicitly_unavailable(self):
        backend = self.backend()
        backend.start(self.request(messages=[{"text": "Synthetic text", "attachments": [
            {"status": "failed", "count": 1}]}]))
        self.finish(backend)
        inputs = next(r["params"]["input"] for r in self.requests() if r.get("method") == "turn/start")
        self.assertEqual(len(inputs), 1)
        self.assertIn("Attachment unavailable", inputs[0]["text"])

    def test_cancel_emits_one_terminal_without_final(self):
        backend = self.backend("late_final")
        backend.start(self.request())
        backend.cancel()
        final, events = self.finish(backend)
        self.assertEqual(final["type"], "cancelled")
        self.assertFalse(any(event["type"] == "final" for event in events))

    def test_failed_and_disconnected_provider_have_distinct_terminal_events(self):
        for mode, expected in (("failed", "failed"), ("disconnect", "unknown")):
            with self.subTest(mode=mode):
                backend = self.backend(mode)
                backend.start(self.request())
                terminal, _ = self.finish(backend)
                self.assertEqual(terminal["type"], expected)
                self.assertNotIn("text", terminal)
                self.assertIn(terminal["side_effect_state"], ("none", "possible", "confirmed", "unknown"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
