"""Offline worker/Hook protocol tests; no platform runtime or network needed."""
from concurrent.futures import ThreadPoolExecutor
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
from wechat_bridge import worker_state as ws
from wechat_bridge.store import atomic_write_json, load_json


class WorkerStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="worker-state-test-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.base = self.home / "bridge"
        self.state_dir = self.home / "hook-state"
        self.now = int(time.time())
        self.state = ws.WorkerState(str(self.base), str(self.state_dir), lambda: self.now)
        Path(self.state.store.session_path).write_text("{}")

    def incoming(self, mid, owner="test-owner"):
        atomic_write_json(str(self.base / "inbox" / (str(mid) + ".json")),
                          {"message_id": str(mid), "from_user_id": owner,
                           "context_token": "synthetic", "text": "test"})

    def test_claim_is_unique_under_concurrent_workers(self):
        self.incoming("201")
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.state.claim(["201"]), range(2)))
        self.assertEqual(sum(len(r["claimed"]) for r in results), 1)
        self.assertTrue((self.state.started / "201.started").exists())

    def test_hook_and_worker_use_atomic_same_claim(self):
        self.incoming("202")
        with ThreadPoolExecutor(max_workers=2) as pool:
            hook = pool.submit(self.state.plan)
            worker = pool.submit(self.state.claim, ["202"])
            planned, claimed = hook.result(), worker.result()
        selected = planned["payload"].get("message_ids", [])
        self.assertEqual(len(selected) + len(claimed["claimed"]), 1)

    def test_two_hook_plans_only_emit_one_new_lease(self):
        self.incoming("203")
        first, second = self.state.plan(), self.state.plan()
        self.assertEqual(first["payload"]["message_ids"], ["203"])
        self.assertEqual(second["decision"], "silent")

    def test_supplement_gets_started_marker_and_same_owner(self):
        self.incoming("1")
        self.incoming("2")
        first = self.state.claim(["1"])
        extra = self.state.claim(["2"], first["lease"])
        self.assertEqual(extra["claimed"], ["2"])
        self.now += 89
        self.assertEqual(self.state.plan()["decision"], "silent")

    def test_rewake_only_adopt_and_old_lease_rejected(self):
        self.incoming("3")
        first = self.state.plan()
        old = first["payload"]["lease"]
        self.now += 181
        replay = self.state.plan()["payload"]
        self.assertEqual(replay["files"], [])
        self.assertEqual(replay["message_ids"], ["3"])
        self.assertEqual(self.state.adopt(["3"], old)["skipped"], {"3": "lease_lost"})
        self.assertEqual(self.state.adopt(["3"], replay["lease"])["owned"], ["3"])

    def test_duplicate_wake_payload_can_only_be_adopted_once(self):
        self.incoming("3")
        event = self.state.plan()["payload"]
        first = self.state.adopt(["3"], event["lease"])
        duplicate = self.state.adopt(["3"], event["lease"])
        self.assertEqual(first["owned"], ["3"])
        self.assertNotEqual(first["lease"], event["lease"])
        self.assertEqual(duplicate["owned"], [])
        self.state.enqueue_reply(["3"], "test-owner", "x", "one reply", first["lease"])

    def test_started_worker_expires_after_90_seconds(self):
        self.incoming("4")
        payload = self.state.plan()["payload"]
        self.state.adopt(["4"], payload["lease"])
        marker = self.state.started / "4.started"
        os.utime(marker, (self.now, self.now))
        self.now += 89
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        self.assertEqual(self.state.plan()["payload"]["message_ids"], ["4"])

    def test_three_rewakes_then_stuck_notice(self):
        self.incoming("5")
        self.state.plan()
        self.now += 180
        for i in range(3):
            self.assertEqual(self.state.plan()["reason"], "new_wechat_message")
            self.now += 600
        result = self.state.plan()
        self.assertEqual(result["reason"], "wechat_stuck_message")
        self.assertEqual(result["payload"]["stuck_files"], ["5.json"])

    def test_legacy_rewake_budget_migrates_without_reset(self):
        self.incoming("legacy")
        self.state.claim_path.write_text("legacy %d %d\n" % (self.now - 2000, self.now - 2000))
        (self.state.state / "wechat_inbox_rewake.txt").write_text(
            "legacy 2 %d 0\n" % (self.now - 600))
        self.assertEqual(self.state.plan()["reason"], "new_wechat_message")
        self.assertEqual(load_json(str(self.state.rewake_path))["legacy"]["count"], 3)
        self.now += 600
        self.assertEqual(self.state.plan()["reason"], "wechat_stuck_message")

    def test_enqueue_covers_entire_batch_and_completes_receipts(self):
        for mid in ("a", "b"):
            self.incoming(mid)
        lease = self.state.claim(["a", "b"])["lease"]
        result = self.state.enqueue_reply(["a", "b"], "test-owner", "synthetic", "reply", lease)
        job = load_json(str(self.base / "outbox/pending/b.json"))
        self.assertEqual(job["covers_message_ids"], ["a", "b"])
        self.assertEqual(job["client_key"], "worker:b")
        for mid in ("a", "b"):
            self.assertEqual(self.state._receipt(mid)["status"], "complete")
            self.assertFalse((self.base / "inbox" / (mid + ".json")).exists())
        self.assertEqual(result["completed"], ["a", "b"])

    def test_media_with_caption_uses_same_private_lease_task(self):
        self.incoming("media")
        media = self.home / "synthetic.png"
        media.write_bytes(b"synthetic media, no upload")
        lease = self.state.claim(["media"])["lease"]
        result = self.state.enqueue_reply(["media"], "test-owner", "ctx", "caption", lease, str(media))
        queued = self.base / "outbox/pending/media.json"
        job = load_json(str(queued))
        self.assertEqual(job["media_path"], str(media.resolve()))
        self.assertEqual(job["text"], "caption")
        self.assertEqual(job["covers_message_ids"], ["media"])
        self.assertEqual(job["client_key"], "worker:media")
        self.assertEqual(queued.stat().st_mode & 0o777, 0o600)
        registry_path = self.state.batch_dir / "media.json"
        self.assertEqual(registry_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(load_json(str(registry_path))["job"], job)
        self.assertEqual(result["completed"], ["media"])

    def test_pure_media_cli_does_not_require_text_file(self):
        self.incoming("media")
        media = self.home / "synthetic.pdf"
        media.write_bytes(b"synthetic file, no upload")
        lease = self.state.claim(["media"])["lease"]
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        result = subprocess.run([sys.executable, "-m", "wechat_bridge.worker_state",
                                 "--base", str(self.base), "--state-dir", str(self.state_dir),
                                 "enqueue-reply", "--ids", "media", "--to", "test-owner",
                                 "--context=synthetic", "--lease", lease, "--media-path", str(media)],
                                env=env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        job = load_json(str(self.base / "outbox/pending/media.json"))
        self.assertEqual(job["text"], "")
        self.assertEqual(job["media_path"], str(media.resolve()))
        self.assertEqual(self.state._receipt("media")["status"], "complete")

    def test_prepared_media_recovery_preserves_caption_path_and_identity(self):
        self.incoming("media")
        media = self.home / "synthetic.png"
        media.write_bytes(b"synthetic media, no upload")
        lease = self.state.claim(["media"])["lease"]
        calls = [0]
        def failing(path, obj):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError("crash before publication intent")
            atomic_write_json(path, obj)
        with patch.object(ws, "atomic_write_json", side_effect=failing):
            with self.assertRaises(OSError):
                self.state.enqueue_reply(["media"], "test-owner", "ctx", "caption", lease, str(media))
        original = load_json(str(self.state.batch_dir / "media.json"))["job"]
        self.state.recover_prepared("media.json")
        recovered = load_json(str(self.base / "outbox/pending/media.json"))
        self.assertEqual(recovered, original)
        self.assertEqual(recovered["client_key"], "worker:media")
        self.assertEqual(recovered["text"], "caption")
        self.assertEqual(recovered["media_path"], str(media.resolve()))

    def test_missing_or_nonregular_media_refused_before_state_publication(self):
        self.incoming("media")
        lease = self.state.claim(["media"])["lease"]
        candidates = [self.home / "missing.png", self.home]
        if hasattr(os, "mkfifo"):
            fifo = self.home / "fifo"
            os.mkfifo(fifo)
            candidates.append(fifo)
        for path in candidates:
            with self.subTest(path=path.name):
                with self.assertRaises((ValueError, OSError)):
                    self.state.enqueue_reply(["media"], "test-owner", "ctx", "", lease, str(path))
                self.assertTrue((self.base / "inbox/media.json").exists())
                self.assertFalse((self.base / "outbox/pending/media.json").exists())
                self.assertFalse((self.state.batch_dir / "media.json").exists())
        with self.assertRaises(ValueError):
            self.state.enqueue_reply(["media"], "test-owner", "ctx", "", lease)

    def test_enqueue_rejects_cross_user_and_stale_lease(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        with self.assertRaises(ValueError):
            self.state.enqueue_reply(["a"], "other-user", "x", "reply", lease)
        with self.assertRaises(ValueError):
            self.state.enqueue_reply(["a"], "test-owner", "x", "reply", "old")
        self.assertEqual(list((self.base / "outbox/pending").glob("*")), [])

    def test_second_reply_cannot_overwrite_same_task(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        self.state.enqueue_reply(["a"], "test-owner", "x", "first", lease)
        with self.assertRaises(ValueError):
            self.state.enqueue_reply(["a"], "test-owner", "x", "replacement", lease)
        self.assertEqual(load_json(str(self.base / "outbox/pending/a.json"))["text"], "first")

    def test_complete_requires_admitted_reply(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        with self.assertRaises(ValueError):
            self.state.complete(["a"], lease)
        self.assertTrue((self.base / "inbox/a.json").exists())

    def test_covered_sending_member_no_duplicate_reply(self):
        self.incoming("a")
        self.incoming("b")
        payload = self.state.plan()["payload"]
        atomic_write_json(str(self.base / "outbox/sending/b.json"),
                          {"covers_message_ids": ["a", "b"], "text": "already queued"})
        result = self.state.adopt(["a", "b"], payload["lease"])
        self.assertEqual(result["owned"], [])
        self.assertEqual(result["covered"]["a"]["status"], "sending")
        self.assertEqual(self.state._receipt("a")["status"], "complete")

    def test_failed_cover_never_marks_unsent_message_complete(self):
        self.incoming("a")
        payload = self.state.plan()["payload"]
        atomic_write_json(str(self.base / "outbox/failed/a.json"),
                          {"covers_message_ids": ["a"], "failed_reason": "synthetic"})
        result = self.state.adopt(["a"], payload["lease"])
        self.assertEqual(result["covered"]["a"]["status"], "failed")
        self.assertTrue((self.base / "inbox/a.json").exists())
        self.assertNotEqual(self.state._receipt("a").get("status"), "complete")

    def test_admitted_registry_covers_transient_queue_move(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        with patch.object(self.state, "_complete", side_effect=RuntimeError("crash before completion")):
            with self.assertRaises(RuntimeError):
                self.state.enqueue_reply(["a"], "test-owner", "x", "fixed", lease)
        (self.base / "outbox/pending/a.json").unlink()  # transient queue rename gap
        self.assertEqual(self.state.covered("a")["status"], "queued")
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.assertEqual(self.state._receipt("a")["status"], "complete")

    def test_prepared_never_published_recovery_preserves_identity(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        calls = [0]
        def failing(path, obj):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError("crash before publication intent")
            atomic_write_json(path, obj)
        with patch.object(ws, "atomic_write_json", side_effect=failing):
            with self.assertRaises(OSError):
                self.state.enqueue_reply(["a"], "test-owner", "x", "fixed", lease)
        self.assertEqual(self.state.covered("a")["status"], "uncertain")
        recovered = self.state.recover_prepared("a.json")
        self.assertEqual(recovered["client_key"], "worker:a")
        self.assertEqual(load_json(str(self.base / "outbox/pending/a.json"))["text"], "fixed")

    def test_unknown_publication_never_auto_recreated(self):
        self.incoming("a")
        lease = self.state.claim(["a"])["lease"]
        calls = [0]
        def failing(path, obj):
            calls[0] += 1
            if calls[0] == 3:
                raise OSError("ambiguous publish interruption")
            atomic_write_json(path, obj)
        with patch.object(ws, "atomic_write_json", side_effect=failing):
            with self.assertRaises(OSError):
                self.state.enqueue_reply(["a"], "test-owner", "x", "fixed", lease)
        with self.assertRaises(ValueError):
            self.state.recover_prepared("a.json")
        self.assertEqual(self.state.plan()["reason"], "wechat_stuck_message")

    def test_completed_receipt_blocks_replayed_inbox_after_seen_trim(self):
        self.incoming("201")
        lease = self.state.claim(["201"])["lease"]
        self.state.enqueue_reply(["201"], "test-owner", "x", "fixed", lease)
        self.incoming("201")  # legacy receiver residue/externally restored file
        self.state.seen_path.write_text("201\n" + "\n".join(str(i) for i in range(10000, 15001)) + "\n")
        self.state.prune_seen()
        self.assertNotIn("201", self.state._seen())
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.assertFalse((self.base / "inbox/201.json").exists())

    def test_seen_prune_and_complete_share_lock(self):
        self.incoming("new")
        lease = self.state.claim(["new"])["lease"]
        atomic_write_json(str(self.base / "outbox/pending/new.json"), {"covers_message_ids": ["new"]})
        self.state.seen_path.write_text("\n".join(str(i) for i in range(5001)) + "\n")
        with ThreadPoolExecutor(max_workers=2) as pool:
            pruning = pool.submit(self.state.prune_seen)
            finishing = pool.submit(self.state.complete, ["new"], lease)
            pruning.result()
            finishing.result()
        self.assertIn("new", self.state._seen())
        self.assertEqual(self.state._receipt("new")["status"], "complete")

    def test_failed_notice_retries_without_false_confirmation(self):
        atomic_write_json(str(self.base / "outbox/failed/dead.json"), {"failed_reason": "synthetic"})
        with patch.dict(os.environ, {"WECHAT_NOTICE_RETRY_SECONDS": "1"}):
            first = self.state.plan()
            self.state.notice_result(first["payload"]["notice_keys"], False)
            self.assertEqual(self.state.plan()["decision"], "silent")
            self.now += 1
            self.assertEqual(self.state.plan()["reason"], "wechat_dead_letter")
        row = load_json(str(self.state.notice_path))["failed:dead.json"]
        self.assertFalse(row["confirmed"])
        self.assertFalse(row["last_runtime_accepted"])

    def test_deadletter_does_not_consume_new_message_lease(self):
        self.incoming("a")
        atomic_write_json(str(self.base / "outbox/failed/dead.json"), {"failed_reason": "synthetic"})
        self.assertEqual(self.state.plan()["reason"], "wechat_dead_letter")
        self.assertEqual(self.state._claims(), {})
        self.assertEqual(self.state.plan()["payload"]["message_ids"], ["a"])

    def test_old_heartbeat_does_not_kill_new_process(self):
        Path(self.state.store.pid_path).write_text("99999")
        Path(self.state.store.flusher_hb_path).write_text(str(self.now - 1000))
        identity = lambda pid: {"pid": pid, "start": self.now - 10, "tokens": ["wechat-bridge", "serve"]}
        with patch.object(ws.os, "kill") as kill:
            result = ws.supervise(str(self.base), "unused", lambda: self.now, identity, lambda _: None)
        self.assertTrue(result["alive"])
        kill.assert_not_called()

    def test_old_heartbeat_after_startup_grace_restarts_stale_process(self):
        Path(self.state.store.pid_path).write_text("99999")
        Path(self.state.store.flusher_hb_path).write_text(str(self.now - 1000))
        alive = [True]
        def identity(pid):
            return {"pid": pid, "start": self.now - 181, "tokens": ["wechat-bridge", "serve"]} if alive[0] else None
        def kill(pid, sig):
            self.assertEqual(pid, 99999)
            alive[0] = False
        with patch.object(ws.os, "kill", side_effect=kill) as mocked:
            result = ws.supervise(str(self.base), "/missing-test-executable", lambda: self.now, identity, lambda _: None)
        self.assertFalse(result["alive"])
        self.assertEqual(mocked.call_count, 1)

    def test_unconfirmed_notice_resumes_after_burst_budget(self):
        Path(self.state.store.needs_relogin_path).write_text("synthetic")
        for delay in (0, 600, 600, 3600):
            self.now += delay
            result = self.state.plan()
            self.assertEqual(result["decision"], "wake")
            self.state.notice_result(result["payload"]["notice_keys"], False)
        row = load_json(str(self.state.notice_path))["relogin"]
        self.assertEqual(row["attempts"], 4)
        self.assertFalse(row["confirmed"])

    def test_busy_inbox_carries_stuck_notice_in_message_event(self):
        self.incoming("stuck")
        self.state.plan()
        self.now += 180
        for _ in range(3):
            self.state.plan()
            self.now += 600
        self.incoming("new")
        result = self.state.plan()
        self.assertEqual(result["reason"], "new_wechat_message")
        self.assertEqual(result["payload"]["message_ids"], ["new"])
        self.assertEqual(result["payload"]["stuck_files"], ["stuck.json"])

    def test_process_identity_checks_command_position_not_substrings(self):
        self.assertTrue(ws.is_serve_command(["/bin/wechat-bridge", "serve"]))
        self.assertTrue(ws.is_serve_command(["/bin/python3", "/v/bin/wechat-bridge", "serve"]))
        self.assertTrue(ws.is_serve_command(["python3.12", "-m", "wechat_bridge", "serve"]))
        for tokens in (["sleep", "serve", "wechat-bridge"],
                       ["python", "maintenance.py", "wechat-bridge", "serve"],
                       ["/bin/wechat-bridge", "status"], ["python", "-m", "other", "serve"]):
            self.assertFalse(ws.is_serve_command(tokens))

    def test_six_hour_fallback_releases_only_old_lease(self):
        self.incoming("a")
        old = self.state.plan()["payload"]["lease"]
        self.now += 21600
        result = self.state.plan()["payload"]
        self.assertEqual(result["message_ids"], ["a"])
        self.assertNotEqual(result["lease"], old)
        self.assertEqual(self.state.adopt(["a"], old)["owned"], [])

    def test_sent_prune_preserves_failed_and_completion_receipts(self):
        old = self.base / "outbox/sent/old.json"
        recent = self.base / "outbox/sent/recent.json"
        failed = self.base / "outbox/failed/old.json"
        for p in (old, recent, failed):
            atomic_write_json(str(p), {"text": "synthetic"})
        os.utime(old, (self.now - 31 * 86400, self.now - 31 * 86400))
        os.utime(failed, (self.now - 31 * 86400, self.now - 31 * 86400))
        self.state.plan()  # first round alerts on failed
        self.state.plan()  # next round performs ordinary maintenance
        self.assertFalse(old.exists())
        self.assertTrue(recent.exists())
        self.assertTrue(failed.exists())

    def test_legacy_seen_residue_gets_sender_bound_receipt_before_prune(self):
        self.incoming("201")
        self.state.seen_path.write_text("201\n")
        self.assertEqual(self.state.plan()["decision"], "silent")
        receipt = self.state._receipt("201")
        self.assertEqual(receipt["from_user_id"], "test-owner")
        self.assertEqual(receipt["status"], "complete")
        self.assertFalse((self.base / "inbox/201.json").exists())

    def test_public_prune_migrates_legacy_seen_residue_before_eviction(self):
        self.incoming("201")
        self.state.seen_path.write_text("201\n" + "\n".join(str(i) for i in range(10000, 15001)) + "\n")
        self.state.prune_seen()
        self.assertNotIn("201", self.state._seen())
        self.assertEqual(self.state._receipt("201")["status"], "complete")
        self.assertEqual(self.state._receipt("201")["from_user_id"], "test-owner")
        self.assertFalse((self.base / "inbox/201.json").exists())
        self.assertEqual(self.state.plan()["decision"], "silent")

    def test_public_prune_cli_migrates_before_eviction(self):
        self.incoming("201")
        self.state.seen_path.write_text("201\n" + "\n".join(str(i) for i in range(10000, 15001)) + "\n")
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        result = subprocess.run([sys.executable, "-m", "wechat_bridge.worker_state",
                                 "--base", str(self.base), "--state-dir", str(self.state_dir), "prune-seen"],
                                env=env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["after"], 2000)
        self.assertEqual(self.state._receipt("201")["status"], "complete")
        self.assertEqual(self.state.plan()["decision"], "silent")

    def test_plan_payload_uses_actual_interpreter_for_prompt(self):
        self.incoming("a")
        payload = self.state.plan()["payload"]
        self.assertEqual(payload["python"], sys.executable)
        prompt = load_json(str(Path(__file__).resolve().parents[1] / "hooks/wechat-inbox-watch.json"))["prompt"]
        self.assertIn("P=payload.python", prompt)


class HookScriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="hook-adapter-test-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.root = Path(__file__).resolve().parents[1]
        self.script = Path(os.environ.get("HOOK_SCRIPT", self.root / "hooks/wechat-inbox-watch.sh"))
        self.runtime = Path(os.environ.get("HOOK_RUNTIME", self.root / "tests/fixtures/hook_runtime.sh"))
        self.env = dict(os.environ, HOME=str(self.home), PYTHONPATH=str(self.root),
                        HATCH_HOOK_RUNTIME=str(self.runtime), WECHAT_BRIDGE_PYTHON=sys.executable,
                        WECHAT_BRIDGE_DIR=str(self.home / "bridge"),
                        WECHAT_HOOK_STATE_DIR=str(self.home / "hook-state"))

    def run_hook(self, **extra):
        return subprocess.run(["bash", str(self.script)], env=dict(self.env, **extra),
                              text=True, capture_output=True, timeout=15)

    def test_dry_run_creates_no_paths_or_changes_mtime(self):
        before = list(self.home.rglob("*"))
        result = self.run_hook(HATCH_HOOK_DRY_RUN="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(list(self.home.rglob("*")), before)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent")

    def test_runtime_failure_is_recorded_but_unconfirmed(self):
        state = ws.WorkerState(self.env["WECHAT_BRIDGE_DIR"], self.env["WECHAT_HOOK_STATE_DIR"])
        Path(state.store.needs_relogin_path).write_text("synthetic")
        result = self.run_hook(FAKE_WAKE_FAIL="1")
        self.assertEqual(result.returncode, 1, result.stderr)
        row = load_json(str(state.notice_path))["relogin"]
        self.assertFalse(row["confirmed"])
        self.assertFalse(row["last_runtime_accepted"])
        self.assertEqual(json.loads(result.stdout)["reason"], "wechat_relogin_needed")

    def test_default_script_is_checkout_not_home_install(self):
        self.assertEqual(self.script, self.root / "hooks/wechat-inbox-watch.sh")
        state = ws.WorkerState(self.env["WECHAT_BRIDGE_DIR"], self.env["WECHAT_HOOK_STATE_DIR"])
        Path(state.store.needs_relogin_path).write_text("synthetic")
        installed = self.home / "hooks/scripts/wechat-inbox-watch.sh"
        installed.parent.mkdir(parents=True)
        installed.write_text("echo WRONG_INSTALLED_SCRIPT; exit 42\n")
        result = self.run_hook()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("WRONG_INSTALLED_SCRIPT", result.stdout)

    def test_shell_event_to_cli_adopt_and_enqueue_offline(self):
        state = ws.WorkerState(self.env["WECHAT_BRIDGE_DIR"], self.env["WECHAT_HOOK_STATE_DIR"])
        Path(state.store.session_path).write_text("{}")
        fake = self.home / "bin/wechat-bridge"
        fake.parent.mkdir()
        fake.write_text("import time\ntime.sleep(30)\n")
        child = subprocess.Popen([sys.executable, str(fake), "serve"],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        def stop():
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)
        self.addCleanup(stop)
        Path(state.store.pid_path).write_text(str(child.pid))
        Path(state.store.flusher_hb_path).write_text(str(time.time()))
        atomic_write_json(str(Path(state.store.inbox_dir) / "201.json"),
                          {"message_id": "201", "from_user_id": "test-owner", "text": "synthetic"})
        result = self.run_hook()
        self.assertEqual(result.returncode, 0, result.stderr)
        event = json.loads(result.stdout)
        self.assertEqual(event["reason"], "new_wechat_message")
        self.assertEqual(os.path.abspath(event["payload"]["python"]), os.path.abspath(sys.executable))
        common = [sys.executable, "-m", "wechat_bridge.worker_state", "--base", str(state.store.base),
                  "--state-dir", str(state.state)]
        adopted = subprocess.run(common + ["adopt", "201", "--lease", event["payload"]["lease"]],
                                 env=self.env, text=True, capture_output=True, timeout=5)
        self.assertEqual(adopted.returncode, 0, adopted.stderr)
        private = json.loads(adopted.stdout)
        self.assertEqual(private["owned"], ["201"])
        text = self.home / "reply.txt"
        text.write_text("offline reply")
        queued = subprocess.run(common + ["enqueue-reply", "--ids", "201", "--to", "test-owner",
                                          "--context", "synthetic", "--text-file", str(text),
                                          "--lease", private["lease"]],
                                env=self.env, text=True, capture_output=True, timeout=5)
        self.assertEqual(queued.returncode, 0, queued.stderr)
        self.assertEqual(json.loads(queued.stdout)["queued"], "201.json")
        self.assertEqual(state._receipt("201")["status"], "complete")


if __name__ == "__main__":
    unittest.main()
