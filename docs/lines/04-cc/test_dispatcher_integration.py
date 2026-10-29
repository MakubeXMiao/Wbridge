"""CC-only integration regression against a fixed, archived public core.

Run from a checkout containing the integration commit:
    python3 docs/lines/04-cc/test_dispatcher_integration.py

The runner archives the fixed Git commit into a temporary directory, overlays
only this line's adapter and fake fixture, then launches a separate interpreter.
The real config validator, registry, Dispatcher, state and sender execute there.
Only the HTTP boundary, network prohibition and sender pacing are synthetic.
No installed Claude, credentials, network service or existing state is used.
"""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time


INTEGRATION_SHA = "cad9aeec2db30d594d8fdca184360c0e8499e7e4"
ADAPTER = "wechat_bridge/backends/claude_code.py"
FIXTURE = "tests/test_claude_code_backend.py"


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check(condition, detail):
    if not condition:
        raise AssertionError(detail)


def child_run(archive_root):
    """Execute in the archived core; no scheduler/adapter mocks or factory injection."""
    sys.path.insert(0, str(archive_root))
    import socket
    from unittest.mock import patch
    from wechat_bridge import ilink, serve
    from wechat_bridge.backends.registry import validate_config
    from wechat_bridge.dispatcher import Dispatcher
    from wechat_bridge.store import Store

    fixture_spec = importlib.util.spec_from_file_location("cc_synthetic_fixture", archive_root / FIXTURE)
    fixture = importlib.util.module_from_spec(fixture_spec)
    fixture_spec.loader.exec_module(fixture)
    check(Path(sys.modules[Dispatcher.__module__].__file__).resolve().is_relative_to(archive_root)
          if hasattr(Path(), "is_relative_to") else str(Path(sys.modules[Dispatcher.__module__].__file__).resolve()).startswith(str(archive_root) + os.sep),
          "Dispatcher must load from the archived public core")
    summaries = []

    def scenario(alphabet, count):
        character = "界" if alphabet == "unicode" else "X"
        expected_answer = character * count
        with tempfile.TemporaryDirectory(prefix="wbridge-cc-wiring-") as directory:
            root = Path(directory).resolve()
            workspace = root / "workspace"
            workspace.mkdir()
            trace = root / "synthetic-trace.jsonl"
            executable = root / "fake claude"
            executable.write_text("#!" + sys.executable + "\n" + fixture.FAKE_CLI, encoding="utf-8")
            executable.chmod(0o700)
            selected = {
                "instance_id": "synthetic-cc-instance", "command": [str(executable)],
                "cwd": str(workspace), "execution_policy": {"approval": "explicit"},
                "env": {"WBR_FAKE_MODE": "size_%s_%d" % (alphabet, count),
                        "WBR_FAKE_TRACE": str(trace), "WBR_FAKE_INPUT": json.dumps({"command": "echo synthetic"})},
                "startup_timeout_seconds": 15, "cancel_grace_seconds": 5,
                "max_output_bytes": 4 * 1024 * 1024,
            }
            config = validate_config({"schema_version": 1, "account_scope": "synthetic-cc-account",
                "default_backend": "claude_code", "job_timeout_seconds": 30, "approval_timeout_seconds": 10,
                "backends": {"claude_code": selected}})
            store = Store(str(root / "synthetic-state"))
            dispatcher = Dispatcher(store, config, "synthetic-owner", "synthetic-bot")
            check(dispatcher.factory is None, "Registry factory must remain the actual core factory")
            dispatcher.acquire()
            store.managed_inbox_dir = dispatcher.inbox
            network_receipts = []

            def receive(message_id, text, context=None):
                serve._write_inbox_record(store, {"message_id": message_id,
                    "context_token": context or "synthetic-context-" + message_id}, "synthetic-owner", text)

            def rows(table):
                with dispatcher.state.transaction() as db:
                    return [dict(row) for row in db.execute("SELECT * FROM " + table + " ORDER BY rowid")]

            def jobs():
                return [json.loads(row["payload"]) for row in rows("jobs")]

            def wait_for_job(index):
                deadline = time.monotonic() + 35
                while time.monotonic() < deadline:
                    dispatcher.tick()
                    current = jobs()
                    if len(current) > index and current[index]["status"] in ("succeeded", "failed", "cancelled", "unknown"):
                        return current[index]
                    time.sleep(0.01)
                raise AssertionError("Actual dispatcher did not terminate synthetic job: %r" % jobs())

            def synthetic_http(method, url, headers, body=None, **kwargs):
                check(method == "POST" and url == "https://fake.invalid/ilink/bot/sendmessage", "Unexpected HTTP operation")
                message = body["msg"]
                check(message["to_user_id"] == "synthetic-owner", "Synthetic recipient changed")
                check(kwargs.get("retries") == 1, "Sender must retain its once-per-attempt HTTP behavior")
                network_receipts.append({"client_id": message["client_id"],
                                         "text": message["item_list"][0]["text_item"]["text"]})
                return {"message_id": "synthetic-http-accepted-%d" % len(network_receipts)}

            try:
                receive("synthetic-first", "SYNTHETIC_BOUNDARY")
                first = wait_for_job(0)
                first_id, first_attempt = first["execution_id"], first["attempt_id"]
                expected_status = "succeeded" if count == 64000 else "failed"
                check(first["status"] == expected_status, {"case": [alphabet, count], "job": first})
                if count > 64000:
                    check(first.get("error_code") == "final_text_too_large", first)
                    check(first.get("side_effect_state") == "possible", first)
                initial_outputs = [json.loads(row["payload"]) for row in rows("outputs")]
                first_outputs = [output for output in initial_outputs if output["backend_v1"].get("execution_id") == first_id]
                check(len(first_outputs) == 1, "Exactly one first-job output is required")
                if count == 64000:
                    check(first_outputs[0]["backend_v1"]["output_kind"] == "final", first_outputs[0]["backend_v1"])
                    check(first_outputs[0]["text"] == expected_answer, "Boundary answer was truncated or altered")
                else:
                    check(first_outputs[0]["backend_v1"]["output_kind"] == "status", first_outputs[0]["backend_v1"])
                    check(first_outputs[0]["text"] != expected_answer[:64000], "Oversized answer was silently truncated")
                    check(not any(output["backend_v1"]["output_kind"] == "final" for output in first_outputs), "Invalid final admitted")

                # Durable duplicate receipt must not create or retry the first task.
                receive("synthetic-first", "SYNTHETIC_BOUNDARY")
                receive("synthetic-next", "SYNTHETIC_FOLLOWUP")
                second = wait_for_job(1)
                check(second["status"] == "succeeded", {"followup": second})
                check(len(jobs()) == 2 and jobs()[0]["attempt_id"] == first_attempt, "Unexpected replay/retry of first task")
                check(jobs()[0]["execution_id"] == first_id and jobs()[0]["status"] == expected_status, "First terminal changed")

                # Preserve real token/chunk budgeting. Synthetic /status input
                # renews context only when more sender budget is needed; the core
                # processes those controls without starting another provider job.
                with patch.object(ilink, "http_json", synthetic_http), patch.object(serve, "http_json", synthetic_http), \
                     patch.object(serve, "PACE_SECONDS", 0), \
                     patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden")), \
                     patch.object(socket.socket, "connect_ex", side_effect=AssertionError("Network forbidden")):
                    for cycle in range(100):
                        check(serve.flush_outbox(store, "https://fake.invalid", "synthetic-token"), "Synthetic sender became invalid")
                        if not list(Path(store.pending_dir).glob("*.json")) and not list(Path(store.sending_dir).glob("*.json")):
                            break
                        receive("synthetic-context-refresh-%d" % cycle, "/status", "synthetic-refreshed-context-%d" % cycle)
                        dispatcher.tick()
                    else:
                        raise AssertionError("Synthetic sender did not finish within bounded context renewals")

                output_rows = rows("outputs")
                output_envelopes = [json.loads(row["payload"]) for row in output_rows]
                check(all(row["phase"] == "admitted" for row in output_rows), "Do not equate admission with delivery")
                check(not list(Path(store.failed_dir).glob("*.json")), "Synthetic sender produced a failed outbox item")
                receipt_map = {receipt["client_id"]: receipt["text"] for receipt in network_receipts}
                check(len(receipt_map) == len(network_receipts), "Synthetic sender repeated an accepted chunk")
                for row, envelope in zip(output_rows, output_envelopes):
                    sent_path = Path(store.sent_dir) / (row["id"] + ".json")
                    check(sent_path.is_file(), "No real sender sent/ receipt for admitted output")
                    sent = json.loads(sent_path.read_text(encoding="utf-8"))
                    check(sent["text"] == envelope["text"], "Sender altered or truncated original text")
                    total = len(ilink.split_text(envelope["text"]))
                    check(sent["chunks_done"] == total, "Sender receipt is incomplete")
                    delivered = "".join(receipt_map[ilink._stable_client_id(envelope["client_key"], index)] for index in range(total))
                    check(delivered == envelope["text"], "Accepted synthetic chunks do not reconstruct complete answer")
                followups = [output for output in output_envelopes if output["backend_v1"].get("execution_id") == second["execution_id"]]
                check(len(followups) == 1 and followups[0]["text"] == "synthetic final", "Followup answer missing")
                records = [json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines()]
                starts = [record for record in records if "argv" in record]
                check(len(starts) == 2 and len(jobs()) == 2, "Boundary failure caused a provider retry or control task")
                summaries.append({"alphabet": alphabet, "result_characters": count, "first_status": first["status"],
                    "error_code": first.get("error_code"), "side_effect_state": first.get("side_effect_state"),
                    "followup_status": second["status"], "provider_executions": len(starts), "jobs": len(jobs()),
                    "output_database_phase": "admitted", "sender_filesystem_state": "sent",
                    "synthetic_http_accepted_chunks": len(network_receipts), "answer_truncated": False,
                    "real_provider_or_phone_acceptance": "not_tested"})
            finally:
                dispatcher.close()

    for alphabet in ("ascii", "unicode"):
        for count in (64000, 64001, 200000):
            scenario(alphabet, count)
    return summaries


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        archive_root = Path(sys.argv[2]).resolve()
        result = child_run(archive_root)
        print(json.dumps({"integration_core_sha": INTEGRATION_SHA,
                          "overlay_adapter_sha256": sha256(archive_root / ADAPTER),
                          "overlay_fixture_sha256": sha256(archive_root / FIXTURE),
                          "cases": result}, ensure_ascii=False), flush=True)
        return 0
    check(len(sys.argv) == 1, "This fixed regression runner takes no external configuration")
    checkout = Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory(prefix="wbridge-cc-archived-core-") as directory:
        archive_root = Path(directory).resolve()
        tar_bytes = subprocess.run(["git", "archive", "--format=tar", INTEGRATION_SHA], cwd=str(checkout),
                                   check=True, stdout=subprocess.PIPE).stdout
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
            for member in archive.getmembers():
                check(not Path(member.name).is_absolute() and ".." not in Path(member.name).parts,
                      "Archive contains a path outside its temporary checkout")
                check(member.isdir() or member.isfile(), "Archive contains an unsupported link or special file")
            archive.extractall(str(archive_root))
        for relative in (ADAPTER, FIXTURE):
            shutil.copyfile(str(checkout / relative), str(archive_root / relative))
        env = {key: value for key, value in os.environ.items() if key not in ("PYTHONPATH", "PYTHONHOME")}
        env.update(PYTHONPATH=str(archive_root), PYTHONDONTWRITEBYTECODE="1")
        child = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--child", str(archive_root)],
                               cwd=str(archive_root), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, timeout=240)
        if child.returncode:
            sys.stdout.write(child.stdout)
            sys.stderr.write(child.stderr)
            return child.returncode
        result = json.loads(child.stdout.splitlines()[-1])
        check(len(result["cases"]) == 6, "Missing integration boundary cases")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
