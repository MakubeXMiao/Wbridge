"""Public inbox -> Muse projection coupling, with synthetic download results.

Run against the image-first public contract and this Muse candidate together.
The download boundary is replaced; these cases do not prove image decoding,
catalog preservation, a platform tool read, or phone delivery.
"""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from wechat_bridge import media_diagnostics, serve, worker_state
from wechat_bridge.store import Store, atomic_write_json, load_json

SPEC = importlib.util.spec_from_file_location("muse_image_adapter", ROOT / "hooks/muse-worker.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


class MuseImageDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="synthetic-muse-image-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name) / "bridge"
        self.state_dir = Path(temporary.name) / "hook-state"
        self.store = Store(str(self.base))
        self.addCleanup(self.store.close)
        atomic_write_json(self.store.session_path, {})
        self.worker = worker_state.WorkerState(str(self.base), str(self.state_dir), lambda: 2000000000)
        self.addCleanup(self.worker.store.close)

    @staticmethod
    def diagnostic(source="param", reason="unknown", size=32):
        return {"source": source, "outcome": "success" if reason == "ok" else "failed",
                "reason": reason, "downloaded_bytes": size}

    def api(self, request):
        return adapter.handle_request(request, str(self.base), str(self.state_dir),
                                      lambda base, state_dir: self.worker)

    def receive(self, message):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(serve.handle_incoming(self.store, message, "synthetic-owner"))

    def test_public_and_muse_accept_the_same_bounded_contract(self):
        self.assertIn("http_body_incomplete", media_diagnostics.REASONS,
                      "requires the frozen image-first public contract in the test assembly")
        self.assertEqual(adapter.MEDIA_DIAGNOSTIC_REASONS, media_diagnostics.REASONS)
        for reason in media_diagnostics.REASONS:
            with self.subTest(reason=reason):
                item = dict(self.diagnostic(reason=reason), item_index=0)
                value = {"version": 1, "items": [item], "truncated": False}
                self.assertEqual(adapter._safe_media_diagnostics(value), value)
                self.assertEqual(media_diagnostics.sanitize_media_diagnostics(value), value)

    def test_receiver_inbox_to_three_entries_and_one_reply(self):
        def download(media, key, dest, diagnostics=None, **options):
            diagnostics.append(dict(self.diagnostic(reason="http_body_incomplete"),
                                    raw_url="SYNTHETIC_PRIVATE_URL", aeskey="SYNTHETIC_PRIVATE_KEY"))
            if media.get("synthetic_failure"):
                raise ValueError("SYNTHETIC_PRIVATE_EXCEPTION")
            diagnostics.append(self.diagnostic(source="full_url", reason="ok", size=64))
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(b"SYNTHETIC_DOWNLOAD_BOUNDARY_ONLY")

        message = {"message_id": "image", "from_user_id": "synthetic-owner", "message_type": 1,
                   "context_token": "SYNTHETIC_PRIVATE_CONTEXT",
                   "media_diagnostics": {"SYNTHETIC_PRIVATE_UPSTREAM": "untrusted"}, "item_list": [
                       {"type": 1, "text_item": {"text": "useful text with mixed media results"}},
                       {"type": 2, "image_item": {"media": {}}},
                       {"type": 2, "image_item": {"media": {"synthetic_failure": True}}}]}
        expected = {"version": 1, "items": [
            dict(self.diagnostic(reason="http_body_incomplete"), item_index=1),
            dict(self.diagnostic(source="full_url", reason="ok", size=64), item_index=1),
            dict(self.diagnostic(reason="http_body_incomplete"), item_index=2)], "truncated": False}
        with patch.object(serve, "download_media", download):
            self.receive(message)
        record = load_json(str(self.base / "inbox/image.json"))
        self.assertEqual(record["media_diagnostics"], expected)
        self.assertEqual(record["media_failed_count"], 1)
        self.assertEqual(len(record["media_paths"]), 1)
        self.assertTrue(Path(record["media_paths"][0]).is_file())
        event = self.worker.plan()["payload"]
        begun = self.api({"action": "begin", "lease": event["lease"]})
        self.assertTrue(begun["ok"], begun)
        refreshed = self.api({"action": "refresh", "handle": begun["handle"]})
        self.assertTrue(refreshed["ok"], refreshed)
        self.receive({"message_id": "later", "from_user_id": "synthetic-owner", "message_type": 1,
                      "context_token": "SYNTHETIC_PRIVATE_CONTEXT", "item_list": [
                          {"type": 1, "text_item": {"text": "later useful question"}}]})
        pending = self.api({"action": "reply", "handle": begun["handle"],
                            "revision": refreshed["revision"], "text": "stale answer"})
        self.assertEqual(pending["error"], "needs_refresh")
        for batch in (begun, refreshed, pending):
            received = batch["messages"][0]
            self.assertEqual(received["media_diagnostics"], expected)
            self.assertEqual(received["text"], record["text"])
            self.assertEqual(received["media_paths"], record["media_paths"])
            self.assertEqual(received["media_failed_count"], 1)
            self.assertNotIn("SYNTHETIC_PRIVATE", json.dumps(batch))
        jobs = self.base / "outbox/pending"
        self.assertEqual(list(jobs.glob("*.json")), [])
        latest = self.api({"action": "refresh", "handle": begun["handle"]})
        request = {"action": "reply", "handle": begun["handle"],
                   "revision": latest["revision"], "text": "one complete answer"}
        queued = self.api(request)
        self.assertTrue(queued["ok"], queued)
        self.assertFalse(queued["delivery_confirmed"])
        self.assertEqual(self.api(request)["error"], "session_closed")
        paths = list(jobs.glob("*.json"))
        self.assertEqual(len(paths), 1)
        job = load_json(str(paths[0]))
        self.assertEqual(job["covers_message_ids"], ["image", "later"])
        self.assertNotIn("media_diagnostics", job)
        private = load_json(str(self.state_dir / "muse-adapter" / (begun["handle"] + ".json")))
        self.assertNotIn("media_diagnostics", private)
        self.assertNotIn("messages", private)
        self.assertNotIn("SYNTHETIC_PRIVATE", json.dumps(private))


if __name__ == "__main__":
    unittest.main()
