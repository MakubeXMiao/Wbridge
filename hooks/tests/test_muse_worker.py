"""Synthetic, offline tests for the Muse adapter; never use live bot state."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge import worker_state as ws
from wechat_bridge.store import atomic_write_json, load_json

SPEC = importlib.util.spec_from_file_location("muse_worker", ROOT / "hooks" / "muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class MuseWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-muse-adapter-")
        self.addCleanup(self.tmp.cleanup)
        self.base = str(Path(self.tmp.name) / "bridge")
        self.state_dir = str(Path(self.tmp.name) / "hook-state")
        self.now = 2000000000
        self.state = ws.WorkerState(self.base, self.state_dir, lambda: self.now)
        Path(self.state.store.session_path).write_text("{}")

    def incoming(self, mid, text="synthetic message", sender="synthetic-owner"):
        atomic_write_json(str(Path(self.base) / "inbox" / (mid + ".json")),
                          {"message_id": mid, "from_user_id": sender,
                           "context_token": "SYNTHETIC-CONTEXT-SECRET", "text": text,
                           "create_time_ms": None, "received_at": self.now})

    def call(self, request, factory=None):
        return adapter.handle_request(request, self.base, self.state_dir,
                                      factory or (lambda base, state_dir: self.state))

    def begin(self, mid="a", text="synthetic message"):
        self.incoming(mid, text)
        event = self.state.plan()["payload"]
        result = self.call({"action": "begin", "lease": event["lease"]})
        self.assertTrue(result["ok"], result)
        return event, result

    def session(self, handle):
        return load_json(str(Path(self.state_dir) / "muse-adapter" / (handle + ".json")))

    def test_begin_hides_private_state_and_stores_only_batch_digest(self):
        event, result = self.begin()
        private = self.session(result["handle"])
        self.assertNotEqual(private["lease"], event["lease"])
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["owned_count"], 1)
        self.assertEqual(result["messages"][0]["text"], "synthetic message")
        self.assertEqual(result["expires_at"], self.now + 90)
        self.assertEqual(result["hard_deadline"], self.now + 570)
        self.assertNotIn("messages", private)
        encoded = json.dumps(result)
        for secret in (private["lease"], "SYNTHETIC-CONTEXT-SECRET", "synthetic-owner", '"to"', '"context"'):
            self.assertNotIn(secret, encoded)
        directory = Path(self.state_dir) / "muse-adapter"
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        for path in directory.iterdir():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertFalse(self.call({"action": "begin", "lease": event["lease"]})["ok"])

    def test_round_trip_uses_code_owned_ids_and_exact_text(self):
        _, batch = self.begin(text="只回复指定文字")
        text = '合成测试 "引号"\n$(touch /tmp/never-run) `id` \\ 结束'
        result = self.call({"action": "reply", "handle": batch["handle"],
                            "revision": batch["revision"], "text": text})
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["delivery_confirmed"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["text"], text)
        self.assertEqual(job["covers_message_ids"], ["a"])
        self.assertEqual(job["context_token"], "SYNTHETIC-CONTEXT-SECRET")
        self.assertEqual(self.session(batch["handle"])["phase"], "queued")
        for action in ("reply", "refresh", "release"):
            request = {"action": action, "handle": batch["handle"]}
            if action == "reply":
                request.update(revision=1, text="must not enqueue")
            self.assertEqual(self.call(request)["error"], "session_closed")
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)

    def test_late_arrival_requires_new_messages_refresh_and_revision(self):
        _, batch = self.begin()
        handle = batch["handle"]
        self.incoming("b", text="new synthetic question")
        old_reply = {"action": "reply", "handle": handle, "revision": 1, "text": "old answer"}
        result = self.call(old_reply)
        self.assertEqual(result["error"], "needs_refresh")
        self.assertEqual(result["revision"], 2)
        self.assertEqual([message["message_id"] for message in result["messages"]], ["a", "b"])
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))
        self.assertTrue((Path(self.base) / "inbox" / "a.json").exists())
        self.assertEqual(self.call(old_reply)["error"], "revision_mismatch")
        self.assertEqual(self.call(dict(old_reply, revision=2))["error"], "needs_refresh")
        refreshed = self.call({"action": "refresh", "handle": handle})
        self.assertTrue(refreshed["ok"])
        self.assertEqual(refreshed["revision"], 3)
        result = self.call(dict(old_reply, revision=3, text="regenerated complete answer"))
        self.assertTrue(result["ok"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["covers_message_ids"], ["a", "b"])
        self.assertEqual(job["text"], "regenerated complete answer")

    def test_refresh_revision_and_hard_deadline_unchanged(self):
        _, batch = self.begin()
        handle = batch["handle"]
        for elapsed in range(60, 541, 60):
            self.now = 2000000000 + elapsed
            result = self.call({"action": "refresh", "handle": handle})
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["hard_deadline"], 2000000570)
            self.assertLessEqual(result["expires_at"], 2000000570)
        self.now = 2000000570
        result = self.call({"action": "refresh", "handle": handle})
        self.assertEqual(result["error"], "lease_lost")
        self.assertTrue(result["must_stop"])

    def test_soft_expiry_and_event_expiry_cannot_be_revived(self):
        self.incoming("a")
        event = self.state.plan()["payload"]
        self.now += 180
        self.assertEqual(self.call({"action": "begin", "lease": event["lease"]})["error"], "lease_lost")
        event = self.state.plan()["payload"]
        batch = self.call({"action": "begin", "lease": event["lease"]})
        self.now += 90
        result = self.call({"action": "reply", "handle": batch["handle"], "revision": 1, "text": "late"})
        self.assertEqual(result["error"], "lease_lost")
        self.assertEqual(self.call({"action": "refresh", "handle": batch["handle"]})["error"], "lease_lost")
        self.assertFalse(list((Path(self.base) / "outbox" / "pending").glob("*.json")))

    def test_reply_failure_after_publication_is_unknown_and_never_retried(self):
        _, batch = self.begin()
        original = self.state.reply
        called = []

        def publish_then_fail(*args):
            called.append(True)
            original(*args)
            raise OSError("SYNTHETIC-CONTEXT-SECRET private-lease-must-not-leak")

        self.state.reply = publish_then_fail
        request = {"action": "reply", "handle": batch["handle"], "revision": 1, "text": "one reply"}
        result = self.call(request)
        self.assertEqual(result, {"ok": False, "error": "outcome_unknown", "must_stop": True})
        for action in ("reply", "refresh", "release"):
            retry = {"action": action, "handle": batch["handle"]}
            if action == "reply":
                retry.update(revision=1, text="would duplicate")
            self.assertEqual(self.call(retry)["error"], "outcome_unknown")
        self.assertEqual(len(called), 1)
        self.assertEqual(self.session(batch["handle"])["phase"], "uncertain")
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)

    def test_crash_marker_and_concurrent_reply_stop_replay(self):
        _, batch = self.begin()
        request = {"action": "reply", "handle": batch["handle"], "revision": 1, "text": "once"}
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.call(request), range(2)))
        self.assertEqual(sum(result["ok"] for result in results), 1)
        self.assertEqual(len(list((Path(self.base) / "outbox" / "pending").glob("*.json"))), 1)
        session = self.session(batch["handle"])
        session["phase"] = "replying"
        adapter._save(Path(self.state_dir) / "muse-adapter", batch["handle"], session)
        self.assertEqual(self.call({"action": "release", "handle": batch["handle"]})["error"], "outcome_unknown")

    def test_different_senders_not_joined_and_release_is_not_cancellation(self):
        self.incoming("a", sender="synthetic-alice")
        self.incoming("b", sender="synthetic-bob")
        event = self.state.plan()["payload"]
        batch = self.call({"action": "begin", "lease": event["lease"]})
        self.assertEqual(batch["owned_count"], 1)
        self.assertEqual([message["message_id"] for message in batch["messages"]], ["a"])
        result = self.call({"action": "release", "handle": batch["handle"]})
        self.assertTrue(result["released"])
        self.assertFalse(result["external_tools_cancelled"])
        self.assertEqual(self.call({"action": "refresh", "handle": batch["handle"]})["error"], "session_closed")

    def test_validation_precedes_state_construction_and_rejects_injection(self):
        def forbidden_factory(*args):
            self.fail("invalid request constructed state")
        cases = [None, [], {}, {"action": "claim", "lease": "x"},
                 {"action": "begin", "lease": "x y"},
                 {"action": "begin", "lease": "x", "base": "/tmp/untrusted"},
                 {"action": "refresh", "handle": "../escape"},
                 {"action": "refresh", "handle": "a" * 32, "lease": "not-allowed"},
                 {"action": "reply", "handle": "a" * 32, "revision": True, "text": "x"},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": 1},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": ""},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "text": "x", "ids": ["a"]},
                 {"action": "reply", "handle": "a" * 32, "revision": 1, "media_path": "relative"}]
        for request in cases:
            with self.subTest(request=request):
                self.assertEqual(self.call(request, forbidden_factory)["error"], "invalid_request")
        self.assertFalse((Path(self.state_dir) / "muse-adapter").exists())

    def test_strict_json_unknown_handle_and_wrong_environment(self):
        for raw in ('{"action":"begin","action":"reply"}', '{"lease":NaN}', '[] []', ''):
            with self.subTest(raw=raw), self.assertRaises(adapter.InvalidRequest):
                adapter.parse_request(io.StringIO(raw))
        self.assertEqual(self.call({"action": "refresh", "handle": "a" * 32})["error"], "unknown_handle")
        _, batch = self.begin()
        result = adapter.handle_request({"action": "refresh", "handle": batch["handle"]},
                                        str(Path(self.tmp.name) / "different-bridge"), self.state_dir)
        self.assertEqual(result["error"], "invalid_session")

    def test_pure_media_reply_uses_same_core_queue(self):
        _, batch = self.begin()
        media = Path(self.tmp.name) / "synthetic.png"
        media.write_bytes(b"synthetic fixture, no real media")
        result = self.call({"action": "reply", "handle": batch["handle"], "revision": 1,
                            "media_path": str(media)})
        self.assertTrue(result["ok"])
        job = load_json(str(Path(self.base) / "outbox" / "pending" / result["queued"]))
        self.assertEqual(job["media_path"], str(media.resolve()))
        self.assertEqual(job["text"], "")

    def test_cli_emits_one_json_and_no_raw_error_or_context(self):
        self.incoming("a")
        event = self.state.plan()["payload"]
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1")
        command = [sys.executable, str(ROOT / "hooks" / "muse-worker.py"),
                   "--base", self.base, "--state-dir", self.state_dir]
        result = subprocess.run(command, input=json.dumps({"action": "begin", "lease": event["lease"]}),
                                text=True, capture_output=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        parsed = json.loads(result.stdout)
        self.assertTrue(parsed["ok"])
        self.assertNotIn("SYNTHETIC-CONTEXT-SECRET", result.stdout + result.stderr)
        self.assertNotIn(self.session(parsed["handle"])["lease"], result.stdout + result.stderr)
        self.assertIn('"stage": "adopted"', result.stderr)
        bad = subprocess.run(command, input='{"action":"begin","lease":"SECRET with spaces"}',
                             text=True, capture_output=True, env=env)
        self.assertEqual(bad.returncode, 2)
        self.assertEqual(json.loads(bad.stdout)["error"], "invalid_request")
        self.assertNotIn("SECRET", bad.stdout + bad.stderr)
        missing = subprocess.run(command[:2] + ["--base", "SECRET"], input="{}",
                                 text=True, capture_output=True, env=env)
        self.assertEqual(json.loads(missing.stdout)["error"], "invalid_request")
        self.assertNotIn("SECRET", missing.stdout + missing.stderr)


if __name__ == "__main__":
    unittest.main()
