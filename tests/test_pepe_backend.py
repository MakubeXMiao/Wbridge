"""PEPE adapter boundaries using original Dot2API JSON and offline fakes.

No real Dot, API credential, network, or WeChat state is used. HTTP and
journal calls are fakes except one owned, fixed local sleeper process that
checks actual HTTP-child timeout cleanup; that child performs no network I/O.
The wire fixtures follow Dot2API 66a7615 Service.public and models.Submit.
"""
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechat_bridge.backends.contract import BackendError
from wechat_bridge.backend_state import digest
from wechat_bridge.backends import pepe
from wechat_bridge.backends.pepe import extract_text, parse_task, validate_snapshot


NOW = 1900000000.0
QUEUE = "pepe-test"
TASK_ID = "task_" + "a" * 32


def request(**overrides):
    value = {
        "schema_version": 1,
        "execution_id": "job-synthetic-pepe-a",
        "attempt_id": "attempt-synthetic-pepe-a",
        "account_scope": "synthetic-wechat-account",
        "sender_key": "synthetic-owner",
        "backend_id": "pepe",
        "backend_instance_ref": "synthetic-current-pepe",
        "conversation_key": "synthetic-conversation-a",
        "session_id": None,
        "messages": [
            {"message_id": "synthetic-input-a", "text": "你好，PEPE。", "attachments": []},
        ],
        "transport": "http",
        "deadline_at": NOW + 1800,
        "execution_policy": {"mode": "native_delegate", "approval": "platform_native"},
    }
    value.update(overrides)
    return value


def config(**overrides):
    value = {
        "provider": "pepe", "transport": "http", "instance_id": "synthetic-current-pepe",
        "base_url": "https://example.invalid", "credential_env": "WBRIDGE_TEST_DOT2API_KEY",
        "tenant": "synthetic-tenant", "principal": "synthetic-api-principal", "queue": QUEUE,
        "queue_ownership": "single_store", "instructions": "Answer from the supplied text.",
        "execution_policy": {"mode": "native_delegate", "approval": "platform_native"},
    }
    value.update(overrides)
    return value


def frozen_snapshot(source=None, selected=None):
    """A test-only frozen snapshot; this does not exercise 01 persistence."""
    source, selected = source or request(), selected or config()
    submit = {
        "queue": selected["queue"], "instructions": selected["instructions"],
        "payload": {"messages": [{"role": "user", "content": item["text"]}
                                  for item in source["messages"]]},
        "idempotency_key": "wbr-" + "b" * 64, "max_attempts": 1, "ttl_seconds": 1795,
    }
    return {
        "schema_version": 1, "execution_id": source["execution_id"],
        "attempt_id": source["attempt_id"], "backend_instance_ref": source["backend_instance_ref"],
        "binding": {"service": selected["base_url"], "tenant": selected["tenant"],
                    "principal": selected["principal"], "queue": selected["queue"]},
        "submit": submit, "submit_digest": digest(submit), "input_digest": digest(source["messages"]),
        "state": "prepared", "task_id": None,
    }


class FakeJournal:
    """Only the granted 03 journal interface; not a public-core test double PASS."""
    def __init__(self, snapshot):
        self.value = copy.deepcopy(snapshot)
        self.begins = 0
        self.bindings = []

    def begin_submit(self):
        if self.value["state"] != "prepared":
            raise BackendError("synthetic_begin_already_consumed")
        self.begins += 1
        self.value["state"] = "submitting"
        return self.snapshot()

    def bind_task(self, task_id):
        if self.value["state"] not in ("submitting", "bound"):
            raise BackendError("synthetic_bind_before_submit")
        if self.value["task_id"] not in (None, task_id):
            raise BackendError("synthetic_task_conflict")
        self.bindings.append(task_id)
        self.value.update(state="bound", task_id=task_id)
        return self.snapshot()

    def snapshot(self):
        return copy.deepcopy(self.value)


def task_for(snapshot, status="queued", result=None, **overrides):
    value = task_response(status, result,
                          instructions=snapshot["submit"]["instructions"],
                          payload=copy.deepcopy(snapshot["submit"]["payload"]),
                          max_attempts=snapshot["submit"]["max_attempts"])
    value["task"].update(overrides)
    return value


class FakeTransport:
    """Scripted calls only; never starts HTTP workers or touches the network."""
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.closed = False
        self._lock = threading.Lock()

    def request(self, method, path, body=None, timeout=5):
        with self._lock:
            self.calls.append((method, path, copy.deepcopy(body), timeout))
            if not self.answers:
                raise pepe.TransportError("synthetic_no_scripted_response")
            answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer(method, path, body, timeout)
        return copy.deepcopy(answer)

    def close(self):
        self.closed = True
        self.release.set()


def execution(snapshot, journal=None):
    value = request()
    value["remote_task"] = copy.deepcopy(snapshot)
    value["remote_task_journal"] = journal or FakeJournal(snapshot)
    return value


def collect_terminal(driver, timeout=3):
    events = []
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        events.extend(driver.poll())
        if any(item["type"] in ("final", "failed", "cancelled", "unknown") for item in events):
            return events
        time.sleep(0.005)
    raise AssertionError("synthetic driver did not produce a terminal event")


def task_response(status="queued", result=None, **overrides):
    """Exact original public-task field names, including real nullable fields."""
    task = {
        "id": TASK_ID,
        "sequence": 1,
        "queue": QUEUE,
        "instructions": "Return a plain-text reply using payload.messages.",
        "payload": {"messages": [{"role": "user", "content": "Synthetic request"}]},
        "status": status,
        "result": result,
        "error": None,
        "attempts": 0 if status == "queued" else 1,
        "max_attempts": 1,
        "revision": 1 if status == "queued" else 2,
        "created_at": "2030-03-17T17:46:40Z",
        "updated_at": "2030-03-17T17:46:40Z",
        "expires_at": "2030-03-17T18:16:40Z",
        "available_at": "2030-03-17T17:46:40Z",
        "lease_expires_at": "2030-03-17T17:56:40Z" if status == "running" else None,
    }
    task.update(overrides)
    return json.loads(json.dumps({"task": task}, ensure_ascii=False))


class OfflineOnly(unittest.TestCase):
    def setUp(self):
        self.no_socket = patch("socket.create_connection", side_effect=AssertionError("real network forbidden"))
        self.no_socket.start()
        self.addCleanup(self.no_socket.stop)


class SnapshotTests(OfflineOnly):
    def test_exact_frozen_submit_is_validated_without_rebuilding(self):
        source = request()
        snapshot = frozen_snapshot(source)
        before = copy.deepcopy(snapshot)
        validated = validate_snapshot(source, snapshot, config())
        self.assertEqual(validated, before)
        self.assertIsNot(validated, snapshot)
        self.assertEqual(snapshot, before)
        wire = validated["submit"]
        self.assertEqual(set(wire), {
            "queue", "instructions", "payload", "idempotency_key", "max_attempts", "ttl_seconds",
        })
        self.assertEqual(wire["queue"], QUEUE)
        self.assertEqual(wire["max_attempts"], 1)
        self.assertTrue(wire["instructions"].strip())
        self.assertEqual(wire["payload"]["messages"], [
            {"role": "user", "content": "你好，PEPE。"},
        ])
        self.assertNotIn("model", wire)
        self.assertIs(type(wire["ttl_seconds"]), int)
        self.assertEqual(wire["ttl_seconds"], 1795)
        self.assertEqual(wire["idempotency_key"], before["submit"]["idempotency_key"])
        json.dumps(wire, allow_nan=False)

    def test_task_echo_digest_and_identity_cannot_be_rebound(self):
        variants = []
        value = frozen_snapshot(); value["submit_digest"] = "0" * 64; variants.append(value)
        value = frozen_snapshot(); value["execution_id"] = "other-job"; variants.append(value)
        value = frozen_snapshot(); value["binding"]["principal"] = "other-principal"; variants.append(value)
        value = frozen_snapshot(); value["binding"]["service"] = "https://other.invalid"; variants.append(value)
        value = frozen_snapshot(); value["submit"]["payload"]["messages"][0]["content"] = "changed"
        value["submit_digest"] = digest(value["submit"]); variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(BackendError):
                validate_snapshot(request(), value, config())

    def test_any_attachment_is_refused_even_with_valid_frozen_submit(self):
        attachments = [
            [{"status": "available", "path": "/synthetic/photo.png"}],
            [{"status": "failed", "count": 1}],
        ]
        for value in attachments:
            with self.subTest(attachments=value), self.assertRaises(BackendError):
                validate_snapshot(request(messages=[{
                    "message_id": "synthetic-media", "text": "图片", "attachments": value,
                }]), frozen_snapshot(), config())

    def test_snapshot_rejects_nonoriginal_submit_fields_and_bad_limits(self):
        for changes in ({"model": "pepe"}, {"max_attempts": 2}, {"ttl_seconds": 59},
                        {"ttl_seconds": True}, {"queue": "bad/queue"}):
            value = frozen_snapshot()
            value["submit"].update(changes)
            value["submit_digest"] = digest(value["submit"])
            with self.subTest(changes=changes), self.assertRaises(BackendError):
                validate_snapshot(request(), value, config())

    def test_original_ascii_expanded_operation_limit_is_respected(self):
        # Raw UTF-8 is under 128 KiB, but upstream Input.bounded defaults to
        # ensure_ascii=True and rejects this operation after JSON expansion.
        text = "测" * 23000
        self.assertLess(len(text.encode("utf-8")), 131072)
        with self.assertRaises(BackendError):
            source = request(messages=[{
                "message_id": "synthetic-large", "text": text, "attachments": [],
            }])
            validate_snapshot(source, frozen_snapshot(source), config())


class TaskParsingTests(OfflineOnly):
    def test_accepts_original_response_for_each_original_state(self):
        for status in ("queued", "running", "completed", "failed", "cancelled", "expired"):
            with self.subTest(status=status):
                response = task_response(status, {"content": "Synthetic answer"} if status == "completed" else None)
                task = parse_task(response, expected_task_id=TASK_ID, expected_queue=QUEUE)
                self.assertEqual(task["id"], TASK_ID)
                self.assertEqual(task["queue"], QUEUE)
                self.assertEqual(task["status"], status)

    def test_wrong_task_or_queue_is_refused(self):
        with self.assertRaises(BackendError):
            parse_task(task_response(), expected_task_id="task_" + "b" * 32, expected_queue=QUEUE)
        with self.assertRaises(BackendError):
            parse_task(task_response(), expected_task_id=TASK_ID, expected_queue="other-queue")

    def test_completed_task_requires_valid_plain_text_result(self):
        for result in (None, "", " \n", {"content": []}, {"tool_calls": []}):
            with self.subTest(result=result), self.assertRaises(BackendError):
                parse_task(task_response("completed", result),
                           expected_task_id=TASK_ID, expected_queue=QUEUE)

    def test_malformed_wrapper_identity_or_state_is_refused(self):
        for value in (None, [], {}, {"task": None}, {"task": []}):
            with self.subTest(value=value), self.assertRaises(BackendError):
                parse_task(value)
        for changes in ({"id": "bad/id"}, {"id": True}, {"queue": "bad/queue"},
                        {"status": "succeeded"}, {"status": True}):
            with self.subTest(changes=changes), self.assertRaises(BackendError):
                parse_task(task_response(**changes))


class ResultTextTests(OfflineOnly):
    def test_result_is_nonempty_bounded_text(self):
        self.assertEqual(extract_text("原样的文字答复"), "原样的文字答复")
        self.assertEqual(extract_text({"content": "PEPE文字答复"}), "PEPE文字答复")
        self.assertEqual(extract_text("x" * 64000), "x" * 64000)

    def test_nontext_empty_and_oversized_results_are_refused(self):
        for result in (None, False, 1, [], {}, "", " \n\t", {"content": ""},
                       {"content": []}, {"content": {"text": "nested"}}, "x" * 64001):
            with self.subTest(kind=type(result).__name__), self.assertRaises(BackendError):
                extract_text(result)


def http_child(status=200, data=None):
    """A fake held child; wait/kill observations here are mocks, not OS proof."""
    child = Mock()
    child.returncode = 0
    child.stdin, child.stdout = io.BytesIO(), io.BytesIO()
    child.poll.return_value = 0
    child.communicate.return_value = (
        json.dumps({"status": status, "data": data}).encode("ascii"), b"",
    )
    child.wait.return_value = 0
    return child


class HTTPTransportTests(OfflineOnly):
    def test_credential_and_body_use_stdin_only_with_fixed_environment(self):
        credential = "SYNTHETIC-NOT-A-REAL-CREDENTIAL"
        text = "SYNTHETIC-PRIVATE-BODY"
        child = http_child(data=task_response())
        transport = pepe.HTTPTransport("https://example.invalid", credential)
        with patch.dict(os.environ, {"WBRIDGE_FAKE_SECRET": "DO-NOT-INHERIT"}), \
                patch.object(pepe.subprocess, "Popen", return_value=child) as spawn:
            result = transport.request("POST", "/v1/tasks", {"payload": text}, timeout=3)
        self.assertEqual(result, task_response())
        self.assertEqual(spawn.call_count, 1)
        argv, = spawn.call_args.args
        options = spawn.call_args.kwargs
        exposed = json.dumps({"argv": argv, "env": options["env"]})
        self.assertNotIn(credential, exposed)
        self.assertNotIn(text, exposed)
        self.assertNotIn("DO-NOT-INHERIT", exposed)
        self.assertEqual(set(options["env"]), {"PATH", "LANG", "LC_ALL"})
        self.assertNotIn("shell", options)
        self.assertEqual(argv[:4], [sys.executable, "-I", "-S", "-c"])
        wire = json.loads(child.communicate.call_args.args[0])
        self.assertEqual(wire["credential"], credential)
        self.assertEqual(wire["body"], {"payload": text})
        self.assertEqual(wire["url"], "https://example.invalid/v1/tasks")
        self.assertGreater(child.communicate.call_args.kwargs["timeout"], 0)
        self.assertLessEqual(child.communicate.call_args.kwargs["timeout"], 3)
        child.wait.assert_called_once()
        child.kill.assert_not_called()
        self.assertTrue(child.stdin.closed and child.stdout.closed)

    def test_timeout_kills_and_waits_only_owned_http_child(self):
        child = http_child()
        child.returncode = None
        child.poll.return_value = None
        child.communicate.side_effect = subprocess.TimeoutExpired("synthetic-http", 1)
        def killed():
            child.returncode = -9
        child.kill.side_effect = killed
        child.wait.return_value = -9
        transport = pepe.HTTPTransport("http://127.0.0.1:18792", "SYNTHETIC-CREDENTIAL")
        with patch.object(pepe.subprocess, "Popen", return_value=child) as spawn:
            with self.assertRaises(pepe.TransportError) as caught:
                transport.request("POST", "/v1/tasks", {"payload": "synthetic"}, timeout=1)
        self.assertEqual(caught.exception.code, "pepe_http_timeout")
        self.assertEqual(caught.exception.status, 0)
        self.assertEqual(spawn.call_count, 1)
        child.kill.assert_called_once_with()
        child.wait.assert_called_once()
        self.assertIsNone(transport._child)
        # No provider cancel operation was sent. A local child mock being
        # reaped is deliberately not described as stopping PEPE.
        self.assertEqual(json.loads(child.communicate.call_args.args[0])["url"],
                         "http://127.0.0.1:18792/v1/tasks")
        self.assertNotIn("cancel", str(caught.exception))

    def test_http_error_status_is_fixed_and_response_body_is_not_exposed(self):
        secret = "SYNTHETIC-SECRET-SERVER-ERROR"
        for status in (302, 401, 403, 404, 409, 500):
            with self.subTest(status=status):
                child = http_child(status, {"error": {"message": secret}})
                transport = pepe.HTTPTransport("https://example.invalid", "SYNTHETIC-CREDENTIAL")
                with patch.object(pepe.subprocess, "Popen", return_value=child) as spawn:
                    with self.assertRaises(pepe.TransportError) as caught:
                        transport.request("GET", "/v1/tasks/" + TASK_ID, timeout=1)
                self.assertEqual(caught.exception.code, "pepe_http_status")
                self.assertEqual(caught.exception.status, status)
                self.assertNotIn(secret, str(caught.exception))
                self.assertEqual(spawn.call_count, 1)

    def test_invalid_http_operation_never_starts_child(self):
        cases = [
            ("DELETE", "/v1/tasks", 1),
            ("GET", "https://other.invalid/v1/tasks", 1),
            ("POST", "/mcp", 1),
            ("GET", "/v1/tasks/x?credential=synthetic", 1),
            ("GET", "/v1/tasks/x/../other", 1),
            ("GET", "/v1/tasks", 0),
            ("GET", "/v1/tasks", True),
            ("GET", "/v1/tasks", 31),
        ]
        transport = pepe.HTTPTransport("https://example.invalid", "SYNTHETIC-CREDENTIAL")
        with patch.object(pepe.subprocess, "Popen") as spawn:
            for method, path, timeout in cases:
                with self.subTest(method=method, path=path, timeout=timeout), \
                        self.assertRaises(pepe.TransportError):
                    transport.request(method, path, timeout=timeout)
        spawn.assert_not_called()

    def test_endpoint_rejects_secret_urls_or_cleartext_remote_hosts(self):
        for endpoint in (
            "https://user:password@example.invalid", "https://example.invalid?token=synthetic",
            "https://example.invalid#token", "https://example.invalid/mcp/secret",
            "http://example.invalid", "http://127.0.0.1.example.invalid", "file:///tmp/secret",
            "https://example.invalid:0", "https://example.invalid/\n",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(BackendError):
                pepe.HTTPTransport(endpoint, "SYNTHETIC-CREDENTIAL")
        for endpoint in ("http://127.0.0.1:18792", "http://[::1]:18792", "https://example.invalid/"):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(pepe.HTTPTransport(endpoint, "SYNTHETIC").base_url,
                                 endpoint.rstrip("/"))

    def test_unverified_policy_rejects_before_environment_or_transport_access(self):
        environment = Mock()
        with patch.object(pepe.os, "environ", environment), \
                patch.object(pepe, "HTTPTransport") as transport:
            with self.assertRaisesRegex(BackendError, "pepe_execution_policy_unverified"):
                pepe.create_backend(config(execution_policy={"mode": "text_only", "enforcement": "unverified"}))
        environment.get.assert_not_called()
        transport.assert_not_called()

    def test_actual_worker_program_disables_redirect_and_environment_proxy(self):
        args = {"url": "https://example.invalid/v1/tasks", "method": "POST",
                "body": {"payload": "synthetic"}, "credential": "SYNTHETIC-CREDENTIAL", "timeout": 1}
        output = io.BytesIO()
        opener = Mock()
        opener.open.side_effect = urllib.error.HTTPError(
            args["url"], 302, "SYNTHETIC-SECRET-ERROR", {}, None)
        with patch.object(sys, "stdin", types.SimpleNamespace(buffer=io.BytesIO(json.dumps(args).encode()))), \
                patch.object(sys, "stdout", types.SimpleNamespace(buffer=output)), \
                patch.object(urllib.request, "build_opener", return_value=opener) as build:
            namespace = {"__name__": "synthetic_http_worker"}
            exec(compile(pepe._HTTP_PROGRAM, "<fixed-http-worker>", "exec"), namespace)
        self.assertEqual(json.loads(output.getvalue()), {"status": 302, "data": None})
        handlers = build.call_args.args
        self.assertEqual(len(handlers), 2)
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsNone(handlers[1].redirect_request(None, None, 302, "redirect", {},
                                                     "https://other.invalid"))
        self.assertEqual(opener.open.call_count, 1)

    def test_real_fixed_local_sleeper_is_killed_waited_and_closed_on_timeout(self):
        actual_popen = subprocess.Popen
        children = []
        def cleanup_if_assertion_fails(child):
            if child.poll() is None:
                child.kill()
            child.wait(timeout=2)
            for stream in (child.stdin, child.stdout):
                if stream is not None:
                    stream.close()
        def spawn(argv, **options):
            child = actual_popen(argv, **options)
            children.append(child)
            # Runs after the assertions below, so fallback cleanup cannot
            # satisfy the observations asserted for production cleanup.
            self.addCleanup(cleanup_if_assertion_fails, child)
            return child
        transport = pepe.HTTPTransport("https://example.invalid", "SYNTHETIC-CREDENTIAL")
        with patch.object(pepe, "_HTTP_PROGRAM", "import time; time.sleep(10)"), \
                patch.object(pepe.subprocess, "Popen", side_effect=spawn):
            with self.assertRaises(pepe.TransportError) as caught:
                transport.request("GET", "/v1/tasks/" + TASK_ID, timeout=0.2)
        self.assertEqual(caught.exception.code, "pepe_http_timeout")
        self.assertEqual(len(children), 1)
        child = children[0]
        self.assertIsNotNone(child.returncode)
        self.assertIsNotNone(child.poll())
        self.assertTrue(child.stdin.closed and child.stdout.closed)
        self.assertIsNone(transport._child)
        # ECHILD after the production cleanup is separate evidence that the
        # owned PID was actually reaped; it says nothing about remote PEPE.
        with self.assertRaises(ChildProcessError):
            os.waitpid(child.pid, os.WNOHANG)


class DriverTests(OfflineOnly):
    def driver(self, transport, clock=lambda: NOW):
        interval = patch.object(pepe, "POLL_SECONDS", 0.001)
        interval.start()
        self.addCleanup(interval.stop)
        environment = patch.dict(os.environ, {"WBRIDGE_TEST_DOT2API_KEY": "SYNTHETIC-ONLY-NOT-A-TOKEN"})
        environment.start()
        self.addCleanup(environment.stop)
        driver = pepe.PepeBackend(config(), transport=transport, clock=clock)
        self.addCleanup(driver.close)
        return driver

    def test_nonblocking_start_poll_and_exact_frozen_submission_then_one_final(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        transport = FakeTransport()
        def submit(method, path, body, timeout):
            self.assertEqual(journal.snapshot()["state"], "submitting")
            transport.entered.set()
            if not transport.release.wait(2):
                raise AssertionError("synthetic test did not release I/O")
            return task_for(snapshot)
        transport.answers = [submit, task_for(snapshot, "completed", {"content": "PEPE合成答复"})]
        driver = self.driver(transport)
        before = time.monotonic()
        driver.start(execution(snapshot, journal))
        self.assertLess(time.monotonic() - before, 1)
        self.assertTrue(transport.entered.wait(1))
        before = time.monotonic()
        initial = driver.poll()
        self.assertLess(time.monotonic() - before, 1)
        self.assertFalse(any(item["type"] == "final" for item in initial))
        transport.release.set()
        events = initial + collect_terminal(driver)
        events += driver.poll()
        finals = [item for item in events if item["type"] == "final"]
        self.assertEqual(len(finals), 1)
        self.assertEqual(finals[0]["text"], "PEPE合成答复")
        self.assertEqual(finals[0]["remote_task_id"], TASK_ID)
        self.assertIsNone(finals[0].get("session_id"))
        self.assertEqual(finals[0]["execution_id"], snapshot["execution_id"])
        self.assertEqual(finals[0]["attempt_id"], snapshot["attempt_id"])
        self.assertEqual(journal.begins, 1)
        self.assertEqual(journal.snapshot()["task_id"], TASK_ID)
        self.assertEqual(transport.calls[0][:3], ("POST", "/v1/tasks", snapshot["submit"]))
        self.assertEqual(transport.calls[1][:3], ("GET", "/v1/tasks/" + TASK_ID, None))
        self.assertEqual(sum(call[0] == "POST" for call in transport.calls), 1)

    def test_lost_post_response_is_unknown_without_second_submission(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        transport = FakeTransport(pepe.TransportError("pepe_http_timeout"))
        driver = self.driver(transport)
        driver.start(execution(snapshot, journal))
        events = collect_terminal(driver)
        self.assertEqual(events[-1]["type"], "unknown")
        self.assertEqual(journal.snapshot()["state"], "submitting")
        self.assertIsNone(journal.snapshot()["task_id"])
        driver.poll(); driver.poll()
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(transport.calls[0][2], snapshot["submit"])

    def test_journal_begin_failure_prevents_any_http_submission(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        journal.begin_submit = Mock(side_effect=BackendError("synthetic_persist_failed"))
        transport = FakeTransport()
        driver = self.driver(transport)
        driver.start(execution(snapshot, journal))
        events = collect_terminal(driver)
        # A journal exception may itself be an ambiguous durable commit;
        # conservatively retain the unresolved job rather than free it.
        self.assertEqual(events[-1]["type"], "unknown")
        self.assertEqual(transport.calls, [])

    def test_known_task_inaccessible_is_unknown_and_never_resubmitted(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        transport = FakeTransport(task_for(snapshot), pepe.TransportError("pepe_http_status", 404))
        driver = self.driver(transport)
        driver.start(execution(snapshot, journal))
        events = collect_terminal(driver)
        self.assertEqual(events[-1]["type"], "unknown")
        self.assertEqual(journal.snapshot()["task_id"], TASK_ID)
        self.assertEqual(events[-1]["remote_task_id"], TASK_ID)
        self.assertEqual([call[0] for call in transport.calls], ["POST", "GET"])

    def test_remote_failure_expiry_cancel_do_not_prove_pepe_stopped(self):
        for status in ("failed", "expired", "cancelled"):
            with self.subTest(status=status):
                snapshot = frozen_snapshot()
                transport = FakeTransport(task_for(snapshot), task_for(snapshot, status))
                driver = self.driver(transport)
                driver.start(execution(snapshot))
                events = collect_terminal(driver)
                self.assertEqual(events[-1]["type"], "unknown")
                self.assertFalse(any(item["type"] in ("final", "cancelled") for item in events))

    def test_wrong_task_queue_or_echo_cannot_publish_a_final(self):
        cases = ({"id": "task_" + "c" * 32}, {"queue": "other-queue"},
                 {"payload": {"messages": [{"role": "user", "content": "different input"}]}})
        for changes in cases:
            with self.subTest(changes=changes):
                snapshot = frozen_snapshot()
                wrong = task_for(snapshot, "completed", {"content": "do not publish"}, **changes)
                transport = FakeTransport(task_for(snapshot), wrong)
                driver = self.driver(transport)
                driver.start(execution(snapshot))
                events = collect_terminal(driver)
                self.assertEqual(events[-1]["type"], "unknown")
                self.assertFalse(any(item["type"] == "final" for item in events))

    def test_deadline_after_submit_preserves_original_task_and_ttl(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        clock = [NOW]
        def submitted(method, path, body, timeout):
            clock[0] = NOW + 1801
            return task_for(snapshot)
        transport = FakeTransport(submitted)
        driver = self.driver(transport, clock=lambda: clock[0])
        driver.start(execution(snapshot, journal))
        events = collect_terminal(driver)
        self.assertEqual(events[-1]["type"], "unknown")
        self.assertEqual(journal.snapshot()["task_id"], TASK_ID)
        self.assertEqual(transport.calls[0][2]["ttl_seconds"], 1795)
        self.assertEqual(len(transport.calls), 1)

    def test_first_post_auth_rejection_after_intent_retains_unknown_fence_without_retry(self):
        for status in (401, 403):
            with self.subTest(status=status):
                snapshot = frozen_snapshot()
                journal = FakeJournal(snapshot)
                transport = FakeTransport(pepe.TransportError("pepe_http_status", status))
                driver = self.driver(transport)
                driver.start(execution(snapshot, journal))
                events = collect_terminal(driver)
                self.assertEqual(events[-1]["type"], "unknown")
                self.assertEqual(events[-1]["error_code"], "pepe_auth_rejected_after_intent")
                self.assertEqual(journal.snapshot()["state"], "submitting")
                self.assertIsNone(journal.snapshot()["task_id"])
                self.assertFalse(any(item["type"] == "final" or
                                     item["type"] == "failed" and item.get("side_effect_state") == "none"
                                     for item in events))
                self.assertEqual(len(transport.calls), 1)
                self.assertEqual(transport.calls[0][0], "POST")

    def test_cancel_or_close_keeps_late_task_binding_without_final_or_stop_claim(self):
        for action in ("cancel", "close"):
            with self.subTest(action=action):
                snapshot = frozen_snapshot()
                journal = FakeJournal(snapshot)
                transport = FakeTransport()
                def late_submit(method, path, body, timeout):
                    transport.entered.set()
                    if not transport.release.wait(2):
                        raise AssertionError("synthetic late POST not released")
                    return task_for(snapshot, "completed", {"content": "late answer not to publish"})
                transport.answers = [late_submit]
                driver = self.driver(transport)
                driver.start(execution(snapshot, journal))
                self.assertTrue(transport.entered.wait(1))
                getattr(driver, action)()
                transport.release.set()
                events = collect_terminal(driver)
                end = time.monotonic() + 1
                while journal.snapshot()["task_id"] is None and time.monotonic() < end:
                    time.sleep(0.005)
                events += driver.poll()
                self.assertEqual(journal.snapshot()["task_id"], TASK_ID)
                self.assertEqual(events[-1]["type"], "unknown")
                self.assertFalse(any(item["type"] in ("final", "cancelled") for item in events))

    def test_scope_input_or_snapshot_tampering_is_refused_before_http(self):
        variants = []
        value = execution(frozen_snapshot()); value["session_id"] = "other-conversation"; variants.append(value)
        value = execution(frozen_snapshot()); value["backend_instance_ref"] = "other-pepe"; variants.append(value)
        value = execution(frozen_snapshot()); value["remote_task"]["submit_digest"] = "0" * 64; variants.append(value)
        value = execution(frozen_snapshot()); value["messages"][0]["text"] = "changed input"; variants.append(value)
        for value in variants:
            with self.subTest(kind=value.get("session_id")):
                transport = FakeTransport()
                driver = self.driver(transport)
                driver.start(value)
                events = collect_terminal(driver)
                self.assertEqual(events[-1]["type"], "failed")
                self.assertEqual(events[-1]["side_effect_state"], "none")
                self.assertEqual(transport.calls, [])

    def test_first_completed_response_with_bad_result_preserves_known_task_id(self):
        snapshot = frozen_snapshot()
        journal = FakeJournal(snapshot)
        transport = FakeTransport(task_for(snapshot, "completed", {"content": []}))
        driver = self.driver(transport)
        driver.start(execution(snapshot, journal))
        events = collect_terminal(driver)
        self.assertEqual(journal.snapshot()["state"], "bound")
        self.assertEqual(journal.snapshot()["task_id"], TASK_ID)
        self.assertEqual(events[-1]["type"], "unknown")
        self.assertEqual(events[-1]["remote_task_id"], TASK_ID)
        self.assertFalse(any(item["type"] == "final" for item in events))
        self.assertEqual(len(transport.calls), 1)

    def test_multiple_input_records_are_refused_before_http(self):
        source = request()
        source["messages"].append({
            "message_id": "synthetic-input-b", "text": "second text", "attachments": [],
        })
        # Keep the snapshot, digest and journal consistent with both records;
        # rejection must enforce the first-release single-record boundary,
        # rather than merely detect a mismatched request versus snapshot.
        snapshot = frozen_snapshot(source)
        value = execution(snapshot)
        value["messages"] = copy.deepcopy(source["messages"])
        transport = FakeTransport()
        driver = self.driver(transport)
        driver.start(value)
        events = collect_terminal(driver)
        self.assertEqual(events[-1]["type"], "failed")
        self.assertEqual(events[-1]["side_effect_state"], "none")
        self.assertEqual(transport.calls, [])


if __name__ == "__main__":
    unittest.main()
