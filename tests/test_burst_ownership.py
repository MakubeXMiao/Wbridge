"""Offline burst/owner lifecycle regressions with real file locks.

All records and clocks are synthetic. These tests prove local ownership and
queue admission, not Muse scheduling latency or receipt on a real phone.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge.worker_state import WorkerState
from wechat_bridge.store import atomic_write_json, load_json
from wechat_bridge import serve


class BurstOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="burst-owner-test-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.base = self.home / "bridge"
        self.state_dir = self.home / "hook-state"
        self.start = self.now = 2_000_000_000
        self.state = self.new_state()
        Path(self.state.store.session_path).write_text("{}")
        self.sequence = 0

    def new_state(self):
        return WorkerState(str(self.base), str(self.state_dir), lambda: self.now)

    def incoming(self, mid, sender="owner-a", text=None):
        self.sequence += 1
        record = {"message_id": str(mid), "from_user_id": sender,
                  "context_token": "synthetic-context-" + str(mid),
                  "text": text or "synthetic message " + str(mid),
                  "received_at": self.now + self.sequence / 1000}
        atomic_write_json(str(self.base / "inbox" / (str(mid) + ".json")), record)
        return record

    def planned(self):
        result = self.state.plan()
        self.assertEqual(result["decision"], "wake", result)
        self.assertEqual(result["reason"], "new_wechat_message", result)
        return result["payload"]

    def start_owner(self, mid="1", sender="owner-a"):
        self.incoming(mid, sender)
        event = self.planned()
        batch = self.state.begin(event["lease"])
        self.assertEqual(batch["owned"], [mid], batch)
        self.assertNotEqual(batch["lease"], event["lease"])
        return event, batch

    def assert_no_batch(self):
        self.assertEqual(list(self.state.batch_dir.glob("*.json")), [])
        self.assertEqual(list((self.base / "outbox/pending").glob("*.json")), [])

    def assert_reply_rejected(self, ids, lease):
        try:
            result = self.state.reply(ids, "late synthetic reply", lease)
        except ValueError as error:
            self.assertIn("lease", str(error).lower())
        else:
            self.assertEqual(result.get("error"), "lease_lost", result)

    def concurrent(self, operation, count=6):
        # Separate objects/open file descriptions exercise flock, rather than
        # relying on a Python instance lock or serialized one-object calls.
        instances = [self.new_state() for _ in range(count)]
        barrier = threading.Barrier(count)
        def run(state):
            barrier.wait(timeout=5)
            return operation(state)
        with ThreadPoolExecutor(max_workers=count) as pool:
            return list(pool.map(run, instances))

    def test_concurrent_wakes_emit_one_event_and_duplicate_begin_has_one_owner(self):
        for mid in ("1", "2", "3", "4", "5"):
            self.incoming(mid)
        plans = self.concurrent(lambda state: state.plan())
        events = [plan["payload"] for plan in plans
                  if plan.get("reason") == "new_wechat_message"]
        self.assertEqual(len(events), 1, plans)
        self.assertEqual(events[0]["message_ids"], ["1", "2", "3", "4", "5"])
        batches = self.concurrent(lambda state: state.begin(events[0]["lease"]))
        owners = [batch for batch in batches if batch["owned"]]
        self.assertEqual(len(owners), 1, batches)
        self.assertEqual(owners[0]["owned"], ["1", "2", "3", "4", "5"])
        private = owners[0]["lease"]
        self.assertNotEqual(private, events[0]["lease"])
        self.assertEqual({row["lease"] for row in self.state._claims().values()}, {private})

    def test_concurrent_plan_and_begin_attach_arrivals_without_a_sibling_event(self):
        self.incoming("1")
        event = self.planned()
        for mid in ("2", "3", "4", "5"):
            self.incoming(mid)
        instances = [self.new_state() for _ in range(12)]
        barrier = threading.Barrier(len(instances))
        def run(pair):
            index, state = pair
            barrier.wait(timeout=5)
            if index % 2:
                return "begin", state.begin(event["lease"])
            return "plan", state.plan()
        with ThreadPoolExecutor(max_workers=len(instances)) as pool:
            results = list(pool.map(run, enumerate(instances)))
        plans = [result for operation, result in results if operation == "plan"]
        self.assertTrue(all(result["decision"] == "silent" for result in plans), plans)
        owners = [result for operation, result in results
                  if operation == "begin" and result["owned"]]
        self.assertEqual(len(owners), 1, results)
        self.assertEqual(owners[0]["owned"], ["1", "2", "3", "4", "5"])

    def test_integer_message_id_is_normalized_before_begin_and_reply(self):
        # Exercise the receiver's real disk publication: iLink may supply a
        # numeric ID even though the filename/receipt key is a string.
        with patch.object(serve.time, "time", return_value=self.now), \
                patch.object(serve, "log"):
            accepted = serve._write_inbox_record(
                self.state.store,
                {"message_id": 123, "context_token": "synthetic-context-123"},
                "owner-a", "synthetic numeric identifier")
        self.assertTrue(accepted)
        self.assertEqual(load_json(str(self.base / "inbox/123.json"))["message_id"], 123)
        event = self.planned()
        self.assertEqual(event["message_ids"], ["123"])
        owner = self.state.begin(event["lease"])
        self.assertEqual(owner["owned"], ["123"])
        self.assertEqual(owner["messages"][0]["message_id"], "123")
        result = self.state.reply(owner["owned"], "synthetic numeric reply", owner["lease"])
        self.assertEqual(result["completed"], ["123"])
        job = load_json(str(self.base / "outbox/pending/123.json"))
        self.assertEqual(job["covers_message_ids"], ["123"])
        self.assertEqual(self.state._receipt("123")["status"], "complete")

    def test_real_cli_begin_and_reply_each_emit_exactly_one_json_object(self):
        # Actual CLI subprocesses use wall time, so the fixture's plan must
        # use the same clock. No shell interpolation or intermediary text file.
        self.now = int(time.time())
        self.incoming("cli")
        event = self.planned()
        cli = [sys.executable, "-m", "wechat_bridge.worker_state",
               "--base", str(self.base), "--state-dir", str(self.state_dir)]
        calls = []
        def invoke(arguments):
            proc = subprocess.run(cli + arguments, cwd=ROOT, capture_output=True,
                                  text=True, timeout=10)
            calls.append(proc)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(len(proc.stdout.splitlines()), 1, proc.stdout)
            # json.loads rejects trailing telemetry or another JSON object.
            result = json.loads(proc.stdout)
            self.assertIsInstance(result, dict)
            return result
        owner = invoke(["begin", "--lease", event["lease"]])
        self.assertEqual(owner["owned"], ["cli"])
        self.assertNotEqual(owner["lease"], event["lease"])
        text = "synthetic CLI reply: 'quoted' $(not_a_command)\n中文"
        admitted = invoke(["reply", "--lease", owner["lease"],
                           "--ids", "cli", "--text", text])
        self.assertEqual(len(calls), 2)
        self.assertEqual(admitted["completed"], ["cli"])
        job = load_json(str(self.base / "outbox/pending/cli.json"))
        self.assertEqual(job["text"], text)
        self.assertEqual(job["covers_message_ids"], ["cli"])
        self.assertEqual(job["to_user_id"], "owner-a")
        self.assertEqual(self.state._receipt("cli")["status"], "complete")
        self.assertIn('"stage": "adopted"', calls[0].stderr)
        self.assertIn('"stage": "enqueued"', calls[1].stderr)

    def test_retired_private_lease_cannot_claim_new_arrival_after_takeover_completes(self):
        _, old = self.start_owner()
        self.now += 90
        replacement = self.planned()
        new_owner = self.state.begin(replacement["lease"])
        self.state.reply(["1"], "replacement completed", new_owner["lease"])
        self.assertEqual(self.state._claims(), {})
        self.incoming("2")
        # No Hook plan has claimed this new message; a retired capability
        # still must not regain authority merely because its rows disappeared.
        rejected = self.state.claim(["2"], old["lease"])
        self.assertEqual(rejected["claimed"], [])
        self.assertEqual(rejected["owned"], [])
        self.assertEqual(rejected["skipped"], {"2": "lease_lost"})
        self.assertEqual(self.state._claims(), {})
        fresh_manual = self.state.claim(["2"])
        self.assertEqual(fresh_manual["claimed"], ["2"])
        self.assertNotEqual(fresh_manual["lease"], old["lease"])

    def test_five_messages_across_polls_join_one_worker_and_one_reply(self):
        self.incoming("1")
        event = self.planned()
        self.now += 5
        self.incoming("2")
        self.incoming("3")
        self.assertEqual(self.state.plan()["decision"], "silent")
        owner = self.state.begin(event["lease"])
        self.assertEqual(owner["owned"], ["1", "2", "3"])
        self.now += 5
        self.incoming("4")
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 5
        self.incoming("5")
        self.assertEqual(self.state.plan()["decision"], "silent")
        latest = self.state.refresh(owner["lease"])
        self.assertEqual(latest["owned"], ["1", "2", "3", "4", "5"])
        self.assertEqual([message["message_id"] for message in latest["messages"]], latest["owned"])
        self.assertEqual(latest["to"], "owner-a")
        self.assertEqual(latest["context"], "synthetic-context-5")
        result = self.state.reply(latest["owned"], "one merged synthetic reply", owner["lease"])
        self.assertEqual(result["completed"], latest["owned"])
        jobs = list((self.base / "outbox/pending").glob("*.json"))
        self.assertEqual(len(jobs), 1)
        job = load_json(str(jobs[0]))
        self.assertEqual(job["covers_message_ids"], latest["owned"])
        self.assertEqual(job["context_token"], "synthetic-context-5")
        self.assertEqual(len(list(self.state.batch_dir.glob("*.json"))), 1)
        self.assertEqual(self.state.plan()["decision"], "silent")

    def test_late_arrivals_do_not_keep_an_unstarted_event_alive(self):
        self.incoming("1")
        event = self.planned()
        self.now = self.start + 179
        self.incoming("2")
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        replacement = self.planned()
        self.assertNotEqual(replacement["lease"], event["lease"])
        self.assertEqual(replacement["message_ids"], ["1", "2"])
        self.assertEqual(self.state.begin(event["lease"])["owned"], [])

    def test_late_arrivals_do_not_renew_a_silent_worker(self):
        _, owner = self.start_owner()
        self.now += 89
        self.incoming("2")
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        replacement = self.planned()
        self.assertEqual(replacement["message_ids"], ["1", "2"])
        self.assertEqual(self.state.refresh(owner["lease"])["error"], "lease_lost")

    def test_dead_owner_process_without_reply_is_recovered_at_90_seconds(self):
        self.incoming("1")
        event = self.planned()
        owner_path = self.home / "owner-before-death.json"
        # The short-lived CLI's PID is not used as a worker heartbeat. The
        # subprocess models a platform worker that dies after claim, with no
        # reply publication and no renewal or orderly release.
        script = (
            "import json,os,sys; from pathlib import Path; "
            "sys.path.insert(0,sys.argv[1]); "
            "from wechat_bridge.worker_state import WorkerState; "
            "state=WorkerState(sys.argv[2],sys.argv[3],lambda:int(sys.argv[4])); "
            "batch=state.begin(sys.argv[5]); "
            "Path(sys.argv[6]).write_text(json.dumps(batch)); os._exit(0)"
        )
        proc = subprocess.run([sys.executable, "-I", "-c", script, str(ROOT),
                               str(self.base), str(self.state_dir), str(self.now),
                               event["lease"], str(owner_path)],
                              capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        dead = json.loads(owner_path.read_text())
        self.assertEqual(dead["owned"], ["1"])
        self.assert_no_batch()
        self.now += 89
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        replacement = self.planned()
        recovered = self.state.begin(replacement["lease"])
        self.assertEqual(recovered["owned"], ["1"])
        self.assertNotEqual(recovered["lease"], dead["lease"])
        self.assert_reply_rejected(["1"], dead["lease"])
        self.assert_no_batch()
        self.state.reply(["1"], "recovered synthetic reply", recovered["lease"])
        self.assertEqual(len(list(self.state.batch_dir.glob("*.json"))), 1)

    def test_expired_worker_cannot_reply_or_renew_before_a_new_hook_poll(self):
        _, owner = self.start_owner()
        self.now += 90
        self.assertEqual(self.state.refresh(owner["lease"])["error"], "lease_lost")
        self.assert_reply_rejected(["1"], owner["lease"])
        self.assert_no_batch()
        self.assertEqual(self.state._receipt("1").get("status"), None)

    def test_expired_event_cannot_begin_before_replacement_is_planned(self):
        self.incoming("1")
        event = self.planned()
        self.now += 180
        self.assertEqual(self.state.begin(event["lease"])["owned"], [])
        self.assert_no_batch()

    def test_refresh_extends_soft_lease_from_actual_progress(self):
        _, owner = self.start_owner()
        self.now += 30
        fresh = self.state.refresh(owner["lease"])
        self.assertEqual(fresh["expires_at"], self.start + 120)
        self.assertEqual(fresh["hard_deadline"], self.start + 570)
        self.now = self.start + 119
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        self.assertEqual(self.planned()["message_ids"], ["1"])

    def test_continuous_refresh_cannot_extend_the_570_second_hard_limit(self):
        _, owner = self.start_owner()
        for offset in range(30, 570, 30):
            self.now = self.start + offset
            fresh = self.state.refresh(owner["lease"])
            self.assertEqual(fresh["owned"], ["1"])
            self.assertEqual(fresh["hard_deadline"], self.start + 570)
            self.assertEqual(fresh["expires_at"], min(self.now + 90, self.start + 570))
        self.now = self.start + 569
        self.assertEqual(self.state.refresh(owner["lease"])["expires_at"], self.start + 570)
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 1
        self.assertEqual(self.state.refresh(owner["lease"])["error"], "lease_lost")
        self.assert_reply_rejected(["1"], owner["lease"])
        self.assertEqual(self.planned()["message_ids"], ["1"])

    def test_reply_detects_new_arrival_before_plan_or_refresh(self):
        _, owner = self.start_owner()
        self.now += 1
        self.incoming("2")
        result = self.state.reply(["1"], "obsolete partial reply", owner["lease"])
        self.assertEqual(result.get("error"), "needs_refresh", result)
        self.assertEqual(result["owned"], ["1", "2"])
        self.assertEqual([message["message_id"] for message in result["messages"]], ["1", "2"])
        self.assert_no_batch()
        self.assertTrue((self.base / "inbox/1.json").exists())
        self.assertTrue((self.base / "inbox/2.json").exists())
        self.state.reply(result["owned"], "recomposed reply for both", owner["lease"])
        self.assertEqual(self.state._receipt("1")["status"], "complete")
        self.assertEqual(self.state._receipt("2")["status"], "complete")

    def test_reply_cannot_omit_an_arrival_attached_by_hook(self):
        _, owner = self.start_owner()
        self.now += 5
        self.incoming("2")
        self.assertEqual(self.state.plan()["decision"], "silent")
        result = self.state.reply(["1"], "obsolete partial reply", owner["lease"])
        self.assertEqual(result.get("error"), "needs_refresh", result)
        self.assertEqual(result["owned"], ["1", "2"])
        self.assert_no_batch()

    def test_message_arriving_after_reply_admission_gets_a_new_owner(self):
        _, owner = self.start_owner()
        self.state.reply(["1"], "already admitted", owner["lease"])
        self.now += 1
        self.incoming("2")
        event = self.planned()
        next_owner = self.state.begin(event["lease"])
        self.assertEqual(next_owner["owned"], ["2"])
        self.assertNotEqual(next_owner["lease"], owner["lease"])
        self.state.reply(["2"], "next reply", next_owner["lease"])
        jobs = [load_json(str(path)) for path in (self.base / "outbox/pending").glob("*.json")]
        self.assertEqual(sorted(job["covers_message_ids"] for job in jobs), [["1"], ["2"]])

    def test_explicit_release_rewakes_without_waiting_for_soft_expiry(self):
        _, owner = self.start_owner()
        self.now += 1
        self.assertEqual(self.state.release(owner["lease"])["released"], ["1"])
        replacement = self.planned()
        fresh = self.state.begin(replacement["lease"])
        self.assertEqual(fresh["owned"], ["1"])
        self.assertNotEqual(fresh["lease"], owner["lease"])
        self.assertEqual(self.state.release(owner["lease"])["released"], [])
        self.assertEqual(self.state.refresh(fresh["lease"])["owned"], ["1"])

    def test_active_owner_does_not_block_other_sender_or_mix_batches(self):
        _, first = self.start_owner("1", "owner-a")
        self.incoming("2", "owner-b")
        second_event = self.planned()
        self.assertEqual(second_event["message_ids"], ["2"])
        second = self.state.begin(second_event["lease"])
        self.incoming("3", "owner-a")
        self.incoming("4", "owner-b")
        self.assertEqual(self.state.plan()["decision"], "silent")
        a = self.state.refresh(first["lease"])
        b = self.state.refresh(second["lease"])
        self.assertEqual(a["owned"], ["1", "3"])
        self.assertEqual(b["owned"], ["2", "4"])
        self.assertEqual(a["to"], "owner-a")
        self.assertEqual(b["to"], "owner-b")
        with self.assertRaises(ValueError):
            self.state.reply(["1", "2", "3", "4"], "wrong combined recipient", first["lease"])
        self.assert_no_batch()
        self.state.reply(a["owned"], "reply a", first["lease"])
        self.state.reply(b["owned"], "reply b", second["lease"])
        jobs = [load_json(str(path)) for path in (self.base / "outbox/pending").glob("*.json")]
        self.assertEqual({job["to_user_id"] for job in jobs}, {"owner-a", "owner-b"})

    def test_three_recoveries_stop_even_with_new_messages_and_new_worker_ids(self):
        _, owner = self.start_owner()
        for recovery in range(1, 4):
            self.now += 90
            self.incoming(str(recovery + 1))
            event = self.planned()
            self.assertIn("1", event["message_ids"])
            owner = self.state.begin(event["lease"])
            self.assertIn("1", owner["owned"])
            self.assertEqual(load_json(str(self.state.rewake_path))["1"]["count"], recovery)
        self.now += 90
        self.incoming("5")
        event = self.planned()
        self.assertNotIn("1", event["message_ids"])
        self.assertIn("1.json", event.get("stuck_files", []))
        fresh = self.state.begin(event["lease"])
        self.assertNotIn("1", fresh["owned"], "begin must not silently restore an exhausted message")
        self.assertNotIn("1", self.state.refresh(fresh["lease"])["owned"])
        self.assertEqual(load_json(str(self.state.rewake_path))["1"]["count"], 3)
        self.assertTrue((self.base / "inbox/1.json").exists())

    def test_concurrent_takeover_plans_and_begin_do_not_split_dead_owner_batch(self):
        _, owner = self.start_owner()
        for mid in ("2", "3", "4", "5"):
            self.incoming(mid)
        self.assertEqual(self.state.plan()["decision"], "silent")
        self.now += 90
        plans = self.concurrent(lambda state: state.plan())
        events = [plan["payload"] for plan in plans
                  if plan.get("reason") == "new_wechat_message"]
        self.assertEqual(len(events), 1, plans)
        batches = self.concurrent(lambda state: state.begin(events[0]["lease"]))
        owners = [batch for batch in batches if batch["owned"]]
        self.assertEqual(len(owners), 1, batches)
        self.assertEqual(owners[0]["owned"], ["1", "2", "3", "4", "5"])
        self.assertEqual(self.state.refresh(owner["lease"])["error"], "lease_lost")
        self.assert_reply_rejected(["1", "2", "3", "4", "5"], owner["lease"])
        self.assert_no_batch()


if __name__ == "__main__":
    unittest.main()
