"""Real public context and Muse adapter on temporary synthetic bridge state."""
import importlib.util
import importlib
import json
import os
from pathlib import Path
import sys
import subprocess
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
try:
    context_runtime = importlib.import_module("wechat_bridge.context_runtime")
except ModuleNotFoundError as error:
    if error.name != "wechat_bridge.context_runtime":
        raise
    context_runtime = cloud_context = None

if context_runtime is not None:
    from wechat_bridge import cloud_context
    import test_attachment_runtime as public_fixture

SPEC = importlib.util.spec_from_file_location("muse_context_runtime_adapter", ROOT / "hooks/muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


@unittest.skipUnless(context_runtime is not None, "requires the paired public context implementation")
class MuseContextRuntimeTests(unittest.TestCase):
    def setUp(self):
        flags = patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "1", "WECHAT_CLOUD_CONTEXT": "0"})
        flags.start()
        self.addCleanup(flags.stop)
        self.f = public_fixture.RuntimeTests(methodName="runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.state_dir = self.f.home / "muse-adapter-state"
        self.counter = 0

    def call(self, request, binding=None):
        return adapter.handle_request(request, str(self.f.base), str(self.state_dir),
            state_factory=lambda base, state: self.f.worker, cloud_binding=binding)

    def begin(self, text="SYNTHETIC original question", binding=None):
        self.counter += 1
        self.f.receive(self.f.message("muse-context-%03d" % self.counter, text=text))
        self.f.now = time.time()
        lease = self.f.worker.plan()["payload"]["lease"]
        result = self.call({"action": "begin", "lease": lease}, binding)
        self.assertTrue(result["ok"], result)
        return result

    def delta(self, batch):
        return {"schema_version": 1, "handle": batch["handle"], "revision": batch["revision"],
            "source_message_ids": [m["message_id"] for m in batch["messages"]],
            "topic": "SYNTHETIC topic", "intent": "continue", "bot_action": "queued response",
            "open_loop": "awaiting evidence", "historical_refs": []}

    def reply(self, batch, delta=None):
        return self.call({"action": "reply", "handle": batch["handle"], "revision": batch["revision"],
            "text": "SYNTHETIC original reply", "memory_delta_v1": self.delta(batch) if delta is None else delta})

    def test_actual_receive_queue_commit_next_begin_and_exact_quote(self):
        first = self.begin()
        self.assertTrue(self.reply(first)["ok"])
        second = self.begin("SYNTHETIC continue the same case")
        recent = second["context"]["recent_context_v1"]
        self.assertEqual(len(recent["entries"]), 1)
        entry = recent["entries"][0]
        self.assertEqual(entry["topic"], "SYNTHETIC topic")
        self.assertGreater(entry["quote_count"], 0)
        self.assertNotIn("quotes", entry)
        self.assertNotIn("SYNTHETIC original question", json.dumps(second))
        quoted = self.call({"action": "context_quote", "handle": second["handle"],
            "revision": second["revision"], "entry_id": entry["id"], "quote_index": 0})
        self.assertEqual(quoted["result"]["status"], "ok", quoted)
        self.assertEqual(quoted["result"]["quote"]["text"], "SYNTHETIC original question")
        self.assertTrue(self.reply(second)["ok"])

    def test_real_bad_summary_and_commit_failure_preserve_business(self):
        for index, fail in enumerate((False, True)):
            batch = self.begin("SYNTHETIC failure isolation %d" % index)
            if fail:
                with patch.object(self.f.worker, "context_commit", side_effect=OSError("private write error")):
                    result = self.reply(batch)
            else:
                result = self.reply(batch, delta={"schema_version": 999})
            self.assertTrue(result["ok"], result)
            phase = json.loads((self.state_dir / "muse-adapter" / (batch["handle"] + ".json")).read_text())
            self.assertEqual(phase["phase"], "queued")
            self.assertEqual(self.reply(batch)["error"], "session_closed")
        self.assertFalse((self.f.base / "state/continuity/recent.json").exists())

    def test_real_attachment_terminal_creates_recent_entry(self):
        attachment_id = self.f.file(mid="attachment-source")
        batch = self.begin("SYNTHETIC return this original file")
        result = self.call({"action": "attachment", "handle": batch["handle"],
            "revision": batch["revision"], "request": {"schema_version": 1, "action": "reply",
                "arguments": {"attachment_ids": [attachment_id], "caption": "SYNTHETIC file"}},
            "memory_delta_v1": self.delta(batch)})
        self.assertTrue(result["ok"], result)
        self.assertTrue(result["result"]["queued"])
        next_batch = self.begin("SYNTHETIC continue the file request")
        entry = next_batch["context"]["recent_context_v1"]["entries"][0]
        self.assertEqual(entry["admission"]["kind"], "attachment")
        self.assertEqual(entry["admission"]["group_id"], result["result"]["group_id"])

    def source_projection(self, revision):
        return {"source_revision": revision, "source_updated_at": self.f.now,
            "source_ref": "synthetic://wbridge/case/%d" % revision, "fact_status": "confirmed",
            "title": "SYNTHETIC case", "facts": "local test only", "change": "revision %d" % revision,
            "next_step": "next begin reads this revision", "open_loop": ""}

    def test_real_attachment_continue_records_its_own_terminal_memory(self):
        attachment_id = self.f.file(mid="continue-attachment-source")
        batch = self.begin("SYNTHETIC send the original group")
        last_id = batch["messages"][-1]["message_id"]
        record = json.loads((self.f.base / "inbox" / (last_id + ".json")).read_text())
        ledger = public_fixture.runtime.ledger(self.f.worker.store)
        ledger.reserve_units(self.f.scope, record["context_token"],
            ["synthetic-prior:%d" % index for index in range(8)])
        first = self.call({"action": "attachment", "handle": batch["handle"],
            "revision": batch["revision"], "request": {"schema_version": 1, "action": "reply",
                "arguments": {"attachment_ids": [attachment_id], "caption": "SYNTHETIC caption"}},
            "memory_delta_v1": self.delta(batch)})
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["result"]["status"]["state"], "awaiting_continue")
        followup = self.begin("继续")
        continued = self.call({"action": "attachment", "handle": followup["handle"],
            "revision": followup["revision"], "request": {"schema_version": 1, "action": "continue",
                "arguments": {"group_id": first["result"]["group_id"]}},
            "memory_delta_v1": self.delta(followup)})
        self.assertTrue(continued["ok"], continued)
        self.assertNotEqual(continued["result"]["job_id"], first["result"]["job_id"])
        next_batch = self.begin("SYNTHETIC after continuation")
        entries = next_batch["context"]["recent_context_v1"]["entries"]
        self.assertEqual([row["admission"]["kind"] for row in entries], ["attachment", "attachment"])

    def test_independent_synthetic_publisher_begin_and_maintenance_share_binding(self):
        with patch.dict(os.environ, {"WECHAT_CLOUD_CONTEXT": "1"}):
            fixed = adapter.SYNTHETIC_CLOUD_BINDING
            binding = cloud_context.CloudBinding(**fixed)
            # This source-agent publication is an independent workflow step.
            published = cloud_context.publish(self.f.store, binding, binding.publisher_id,
                self.source_projection(1), None, self.f.now, enabled=True)
            self.assertEqual(published["status"], "published", published)
            batch = self.begin(binding=fixed)
            cloud = batch["context"]["cloud_context_v1"]
            self.assertEqual(cloud["projection"]["source_revision"], 1, cloud)
            self.assertEqual(cloud["projection"]["source_id"], fixed["source_id"])
            self.assertTrue(self.reply(batch)["ok"])
            published = cloud_context.publish(self.f.store, binding, binding.publisher_id,
                self.source_projection(2), 1, self.f.now, enabled=True)
            self.assertEqual(published["status"], "published", published)
            maintained = context_runtime.recent_context_maintain(str(self.f.base),
                clock=lambda: self.f.now, cloud_binding=binding)
            self.assertIn("cloud", maintained)
            batch = self.begin(binding=fixed)
            cloud = batch["context"]["cloud_context_v1"]
            self.assertEqual(cloud["projection"]["source_revision"], 2, cloud)
            self.assertLessEqual(len(json.dumps(batch["context"], ensure_ascii=False,
                separators=(",", ":")).encode("utf-8")), 16 << 10)

    def test_independent_disable_returns_to_original_current_batch_path(self):
        batch = self.begin()
        self.assertTrue(self.reply(batch)["ok"])
        before = (self.f.base / "state/continuity/recent.json").read_bytes()
        with patch.dict(os.environ, {"WECHAT_RECENT_CONTEXT": "0", "WECHAT_CLOUD_CONTEXT": "0"}):
            batch = self.begin()
            self.assertNotIn("context", batch)
            self.assertTrue(self.reply(batch)["ok"])
        self.assertEqual(before, (self.f.base / "state/continuity/recent.json").read_bytes())

    def test_actual_hook_program_applies_source_without_waking_a_model(self):
        binding = cloud_context.CloudBinding(**adapter.SYNTHETIC_CLOUD_BINDING)
        published = cloud_context.publish(self.f.store, binding, binding.publisher_id,
            self.source_projection(1), None, self.f.now, enabled=True)
        self.assertEqual(published["status"], "published")
        # Hold a healthy serve-shaped sleeper so unrelated serve-down notices
        # cannot be mistaken for a model wake caused by source maintenance.
        sleeper = self.f.home / "bin/wechat-bridge"
        sleeper.parent.mkdir()
        sleeper.write_text("import time\ntime.sleep(30)\n")
        child = subprocess.Popen([sys.executable, str(sleeper), "serve"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def stop():
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)
        self.addCleanup(stop)
        Path(self.f.store.pid_path).write_text(str(child.pid))
        Path(self.f.store.flusher_hb_path).write_text(str(time.time()))
        env = dict(os.environ, HOME=str(self.f.home), PYTHONPATH=str(ROOT),
            PYTHONDONTWRITEBYTECODE="1", WECHAT_BRIDGE_PYTHON=sys.executable,
            WECHAT_BRIDGE_DIR=str(self.f.base), WECHAT_HOOK_STATE_DIR=str(self.state_dir),
            WECHAT_CLOUD_CONTEXT="1", HATCH_HOOK_RUNTIME=str(ROOT / "tests/fixtures/hook_runtime.sh"),
            WECHAT_BRIDGE_EXECUTABLE=str(self.f.home / "absent-synthetic-serve"))
        result = subprocess.run(["bash", str(ROOT / "hooks/wechat-inbox-watch.sh")],
            env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["decision"], "silent", result.stdout)
        self.assertNotIn('"stage": "wake_called"', result.stderr)
        cloud = cloud_context.read(self.f.store, self.f.scope, time.time(), enabled=True, binding=binding)
        self.assertEqual(cloud["projection"]["source_revision"], 1, cloud)


if __name__ == "__main__":
    unittest.main()
