"""Real public-package integration for deterministic legacy-media rejection.

Run with the fixed public package overlaid with this Muse candidate. Older
packages without the shared guard cannot exercise these integration cases.
No receiver, network, media upload or real account is used.
"""
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge import serve, worker_state as ws
from wechat_bridge.store import Store, atomic_write_json, load_json

try:
    from wechat_bridge import attachment_runtime as runtime
except ImportError:
    # This suite is explicitly unavailable on the old package, not a pass for
    # the new coupling. The fixed-package execution receipt must show no skips.
    runtime = None

SPEC = importlib.util.spec_from_file_location("muse_prequeue_adapter", ROOT / "hooks/muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


@unittest.skipUnless(runtime is not None and callable(getattr(runtime, "is_catalog_original_path", None)),
                     "requires public catalog-original guard")
class MusePrequeueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-muse-prequeue-")
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.base, self.state_dir = root / "bridge", root / "hook-state"
        self.base.mkdir(mode=0o700)
        self.state_dir.mkdir(mode=0o700)
        self.store = Store(str(self.base))
        self.addCleanup(self.store.close)
        atomic_write_json(str(self.base / "state/attachment-policy.json"),
                          {"max_bytes": 65536, "quota_bytes": 32 << 20})
        atomic_write_json(self.store.session_path, {
            "bot_token": "SYNTHETIC_NOT_A_CREDENTIAL", "ilink_user_id": "synthetic-owner",
            "ilink_bot_id": "synthetic-bot"})
        self.scope = runtime.initialize(self.store)
        self.now = time.time()
        self.worker = ws.WorkerState(str(self.base), str(self.state_dir), lambda: self.now)
        event = self.incoming("original", "请发回此附件", attachment=True)
        ref = event["attachment_refs"][0]
        stage = Path(ref["stage_path"])
        stage.write_bytes(b"SYNTHETIC ORIGINAL")
        stage.chmod(0o600)
        self.attachment_id = ref["attachment_id"]
        runtime.catalog(self.store).publish(self.scope, self.attachment_id, str(stage),
            decryption_state="protocol_decoded", detected_type=None, preview_state="unavailable")
        plan = self.worker.plan()
        self.batch = self.api({"action": "begin", "lease": plan["payload"]["lease"]})
        self.assertTrue(self.batch["ok"], self.batch)
        got = self.attachment("get", attachment_id=self.attachment_id)
        self.assertTrue(got["ok"], got)
        self.original = Path(got["result"]["material"]["path"])
        self.assertTrue(self.attachment("release", hold_id=got["result"]["material"]["hold_id"])["ok"])
        self.pdf = root / "generated.pdf"
        self.pdf.write_bytes(b"%PDF-1.4\nSYNTHETIC GENERATED FILE\n%%EOF\n")

    def incoming(self, mid, text, attachment=False):
        message = {"message_id": mid, "context_token": "SYNTHETIC_CONTEXT_" + mid}
        items = [{"item_index": 0, "kind": "file", "original_name": "original.bin",
                  "declared_type": None}] if attachment else []
        scope, event = runtime.receive_event(self.store, message, "synthetic-owner", items, text, self.now)
        serve._write_inbox_record(self.store, message, "synthetic-owner", text,
            attachment_event=event, login_epoch=scope["login_epoch"], received_at=self.now)
        return event

    def api(self, request):
        return adapter.handle_request(request, str(self.base), str(self.state_dir),
                                      lambda base, state: self.worker)

    def reply(self, path):
        return {"action": "reply", "handle": self.batch["handle"],
                "revision": self.batch["revision"], "media_path": str(path)}

    def attachment(self, action, **arguments):
        return self.api({"action": "attachment", "handle": self.batch["handle"],
            "revision": self.batch["revision"], "request": {
                "schema_version": 1, "action": action, "arguments": arguments}})

    def session_path(self):
        return self.state_dir / "muse-adapter" / (self.batch["handle"] + ".json")

    def session(self):
        return load_json(str(self.session_path()))

    def counted(self, request, cli=False):
        # Observe the real method's code object without replacing public logic.
        calls, code = [], ws.WorkerState.reply.__code__
        previous = sys.getprofile()
        def observe(frame, event, unused):
            if event == "call" and frame.f_code is code:
                calls.append(True)
        sys.setprofile(observe)
        try:
            if not cli:
                return self.api(request), len(calls)
            output = io.StringIO()
            with patch.object(sys, "stdin", io.StringIO(json.dumps(request))), patch.object(sys, "stdout", output):
                exit_code = adapter.main(["--base", str(self.base), "--state-dir", str(self.state_dir)])
            return (exit_code, json.loads(output.getvalue())), len(calls)
        finally:
            sys.setprofile(previous)

    def assert_unpublished(self):
        self.assertEqual(load_json(str(Path(self.store.receipts_dir) / "original.json"))["status"], "received")
        self.assertFalse(list(Path(self.store.pending_dir).glob("*.json")))
        self.assertFalse(list(self.worker.batch_dir.glob("*.json")))

    def test_actual_cli_rejection_keeps_same_handle_for_correct_id_group(self):
        before = self.session_path().read_bytes()
        (exit_code, refused), count = self.counted(self.reply(self.original), cli=True)
        self.assertEqual(exit_code, 2)
        self.assertEqual(refused["error"], "invalid_request")
        self.assertFalse(refused["must_stop"])
        self.assertEqual(refused["handle"], self.batch["handle"])
        self.assertEqual(refused["revision"], self.batch["revision"])
        self.assertEqual(count, 0)
        self.assertEqual(self.session_path().read_bytes(), before)
        self.assertNotIn(str(self.original), json.dumps(refused))
        self.assert_unpublished()
        corrected = self.attachment("reply", attachment_ids=[self.attachment_id])
        self.assertTrue(corrected["ok"], corrected)
        self.assertTrue(corrected["result"]["queued"])
        self.assertFalse(corrected["result"]["delivery_confirmed"])
        self.assertEqual(self.session()["phase"], "queued")

    def test_shared_guard_handles_symlink_directory_alias_and_case_spelling(self):
        alias = Path(self.tmp.name) / "original-link.bin"
        alias.symlink_to(self.original)
        directory = Path(self.tmp.name) / "directory-link"
        directory.symlink_to(self.original.parent, target_is_directory=True)
        paths = [alias, directory / self.original.name]
        case_path = self.original.parent.with_name("OBJECTS") / self.original.name
        if case_path.exists():
            paths.append(case_path)
        before = self.session_path().read_bytes()
        for path in paths:
            with self.subTest(alias_kind=path.name):
                self.assertTrue(runtime.is_catalog_original_path(self.store, path))
                refused, count = self.counted(self.reply(path))
                self.assertEqual(refused["error"], "invalid_request")
                self.assertFalse(refused["must_stop"])
                self.assertEqual(count, 0)
                self.assertEqual(self.session_path().read_bytes(), before)
                self.assert_unpublished()

    def test_generated_pdf_still_uses_real_ordinary_reply(self):
        result, count = self.counted(self.reply(self.pdf))
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["delivery_confirmed"])
        self.assertEqual(count, 1)
        job = load_json(str(Path(self.store.pending_dir) / result["queued"]))
        self.assertEqual(job["media_path"], str(self.pdf))
        self.assertEqual(self.session()["phase"], "queued")

    def test_existing_replying_or_uncertain_is_never_reopened(self):
        for phase in ("replying", "uncertain"):
            private = self.session()
            private["phase"] = phase
            atomic_write_json(str(self.session_path()), private)
            before = self.session_path().read_bytes()
            refused, count = self.counted(self.reply(self.original))
            self.assertEqual(refused["error"], "outcome_unknown")
            self.assertTrue(refused["must_stop"])
            self.assertEqual(count, 0)
            self.assertEqual(self.session_path().read_bytes(), before)

    def test_real_publication_then_timeout_or_unknown_error_stays_uncertain(self):
        real_reply = self.worker.reply
        def publish_then_lose_result(*args):
            real_reply(*args)
            raise TimeoutError("SYNTHETIC private result lost")
        with patch.object(self.worker, "reply", publish_then_lose_result):
            result = self.api(self.reply(self.pdf))
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertTrue(result["must_stop"])
        self.assertEqual(self.session()["phase"], "uncertain")
        self.assertEqual(len(list(Path(self.store.pending_dir).glob("*.json"))), 1)
        self.assertEqual(self.attachment("reply", attachment_ids=[self.attachment_id])["error"], "outcome_unknown")
        self.assertNotIn("private result", json.dumps(result))

    def test_unknown_public_valueerror_still_closes_handle(self):
        real_reply = self.worker.reply
        def publish_then_unknown(*args):
            real_reply(*args)
            raise ValueError("SYNTHETIC unrecognized publication error")
        with patch.object(self.worker, "reply", publish_then_unknown):
            result = self.api(self.reply(self.pdf))
        self.assertEqual(result["error"], "outcome_unknown")
        self.assertTrue(result["must_stop"])
        self.assertEqual(self.session()["phase"], "uncertain")
        self.assertNotIn("unrecognized", json.dumps(result))

    def test_revision_needs_refresh_and_lost_lease_still_fence_reply(self):
        stale = dict(self.reply(self.original), revision=self.batch["revision"] + 1)
        result, count = self.counted(stale)
        self.assertEqual(result["error"], "revision_mismatch")
        self.assertEqual(count, 0)
        self.incoming("later", "later owned text")
        result = self.api(self.reply(self.pdf))
        self.assertEqual(result["error"], "needs_refresh")
        self.batch["revision"] = result["revision"]
        result, count = self.counted(self.reply(self.original))
        self.assertEqual(result["error"], "needs_refresh")
        self.assertEqual(count, 0)
        refreshed = self.api({"action": "refresh", "handle": self.batch["handle"]})
        self.assertTrue(refreshed["ok"])
        self.batch = refreshed
        self.now += 91
        result = self.api(self.reply(self.pdf))
        self.assertEqual(result["error"], "lease_lost")
        self.assertTrue(result["must_stop"])
        self.assert_unpublished()

    def test_guard_errors_do_not_fall_through_or_expose_details(self):
        before = self.session_path().read_bytes()
        for error in (ValueError("SYNTHETIC private path"), OSError("SYNTHETIC private IO")):
            with patch.object(runtime, "is_catalog_original_path", side_effect=error):
                result, count = self.counted(self.reply(self.pdf))
            self.assertEqual(result["error"], "adapter_state_error")
            self.assertTrue(result["must_stop"])
            self.assertEqual(count, 0)
            self.assertNotIn("private", json.dumps(result))
            self.assertEqual(self.session_path().read_bytes(), before)
            self.assert_unpublished()


if __name__ == "__main__":
    unittest.main()
