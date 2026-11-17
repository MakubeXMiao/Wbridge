"""Image-first receive -> original/index -> accounted return, synthetic only.

Reuse the real 003 store/worker harness without inheriting its test methods.
Only HTTP edges are replaced; its socket guard also forbids accidental I/O.
"""

import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_attachment_runtime as harness
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge import media, serve, worker_state
from wechat_bridge.attachments import AttachmentStore
from wechat_bridge.store import Store, atomic_write_json, load_json


class Response(harness.Response):
    def __init__(self, body, declared=None):
        super().__init__(body)
        if declared is not None:
            self.headers["Content-Length"] = str(declared)


class ImageFirstChainTests(unittest.TestCase):
    setUp = harness.RuntimeTests.setUp
    message = harness.RuntimeTests.message
    transport = harness.RuntimeTests.transport
    receive = harness.RuntimeTests.receive
    batch = harness.RuntimeTests.batch
    call = harness.RuntimeTests.call
    success = harness.RuntimeTests.success
    error = harness.RuntimeTests.error
    send_spy = harness.RuntimeTests.send_spy
    flush = harness.RuntimeTests.flush

    def image(self, mid, color="blue"):
        original = harness.png(color)
        message = self.message(mid, attachments=[("image", original, None)])
        cipher = self.bodies[mid + "-0"]
        return original, message, cipher

    def receive_sequence(self, message, responses):
        calls = []

        def transport(request, **kwargs):
            self.assertEqual(request.get_method(), "GET")
            calls.append((request.full_url, kwargs.get("timeout")))
            self.assertLessEqual(len(calls), len(responses), "unbounded GET retry")
            body, declared = responses[len(calls) - 1]
            return Response(body, declared)

        with patch.object(media.urllib.request, "urlopen", side_effect=transport):
            self.assertTrue(serve.handle_incoming(self.store, message, harness.OWNER))
        row = load_json(str(Path(self.store.inbox_dir) / (message["message_id"] + ".json")))
        return row, calls

    def ready_original(self, row, original, received_at=None):
        self.assertEqual(len(row["attachment_refs"]), 1)
        self.assertEqual(row["attachment_refs"][0]["status"], "ready")
        self.assertEqual(len(row["media_paths"]), 1)
        path = Path(row["media_paths"][0])
        self.assertEqual(path.read_bytes(), original)
        metadata = runtime.catalog(self.store).query(self.scope)["items"]
        self.assertEqual(len(metadata), 1)
        metadata = metadata[0]
        self.assertEqual(metadata["status"], "ready")
        self.assertEqual(metadata["kind"], "image")
        self.assertEqual(metadata["message_id"], row["message_id"])
        self.assertEqual(metadata["received_at"], row["received_at"] if received_at is None else received_at)
        self.assertEqual(metadata["item_index"], 0)
        self.assertEqual(metadata["size"], len(original))
        self.assertEqual(metadata["sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(metadata["attachment_id"], row["attachment_refs"][0]["attachment_id"])
        return metadata["attachment_id"], path

    def test_short_first_get_recovers_same_source_then_queries_and_returns_once(self):
        original, message, cipher = self.image("synthetic-image-retry-return")
        self.assertGreater(len(cipher), 16)
        row, requests = self.receive_sequence(message, [
            (cipher[:-16], len(cipher)), (cipher, len(cipher))])
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][0], requests[1][0])
        self.assertTrue(all(timeout is not None and timeout > 0 for _, timeout in requests))
        aid, path = self.ready_original(row, original)
        self.assertEqual([(v["source"], v["outcome"], v["reason"]) for v in
                          row["media_diagnostics"]["items"]], [("param", "success", "ok")])

        batch = self.batch()
        queried = self.success(self.call(batch, "query", {"filters": {"kind": "image", "status": "ready"}}))
        self.assertEqual([v["attachment_id"] for v in queried["items"]], [aid])
        self.assertNotIn("path", queried["items"][0])
        material = self.success(self.call(batch, "get", {"attachment_id": aid}))["material"]
        self.assertEqual(Path(material["path"]), path)
        self.assertEqual(Path(material["path"]).read_bytes(), original)
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        self.assertTrue(group["queued"])
        self.assertFalse(group["delivery_confirmed"])
        messages, sent_originals = self.send_spy()
        self.flush()
        self.assertEqual([m["item_list"][0]["type"] for m in messages], [media.ITEM_IMAGE])
        self.assertEqual(sent_originals, [original])
        state = runtime.ledger(self.store)
        status = state.group_status(self.scope, group["group_id"])
        self.assertEqual(status["state"], "complete")
        self.assertEqual(status["accepted_attachment_ids"], [aid])
        self.assertEqual(status["units"]["accepted"], 1)

        # Replay the exact durable intent through the existing core publication
        # path, then replay the old caller capability. Neither may send again.
        paths = list((self.base / "state/attachment_admissions").glob("*.json"))
        self.assertEqual(len(paths), 1)
        record = load_json(str(paths[0]))
        replay = runtime.publish_admission(self.store, record, str(paths[0]))
        self.assertEqual((replay["group_id"], replay["job_id"]), (group["group_id"], group["job_id"]))
        self.assertFalse(self.call(batch, "reply", {"attachment_ids": [aid]})["ok"])
        self.flush()
        self.assertEqual(len(messages), 1)
        self.assertEqual(len(sent_originals), 1)
        repeated_status = state.group_status(self.scope, group["group_id"])
        self.assertEqual(repeated_status["state"], "complete")
        self.assertEqual(repeated_status["accepted_attachment_ids"], [aid])
        self.assertEqual(repeated_status["units"]["accepted"], 1)
        self.assertEqual(state.job_status(self.scope, group["job_id"])["request_id"], record["request_id"])
        self.assertEqual(state.inspect_readonly(self.base)["counts"]["groups"], {"complete": 1})
        self.assertEqual(state.register_context(self.scope, batch["context"])["used"], 1)

    def test_all_short_sources_keep_failed_position_without_decrypt_or_ready_body(self):
        _, message, cipher = self.image("synthetic-image-all-short")
        message["item_list"][0]["image_item"]["media"]["full_url"] = "https://synthetic.invalid/fallback"
        responses = [(cipher[:-16], len(cipher))] * 4
        with patch.object(media, "_decrypt_original", wraps=media._decrypt_original) as decrypt:
            row, requests = self.receive_sequence(message, responses)
        self.assertEqual(len(requests), 4)
        self.assertEqual(requests[0][0], requests[1][0])
        self.assertEqual(requests[2][0], requests[3][0])
        self.assertNotEqual(requests[0][0], requests[2][0])
        decrypt.assert_not_called()
        self.assertTrue(row["media_failed"])
        self.assertEqual(row["media_failed_count"], 1)
        self.assertNotIn("media_paths", row)
        self.assertEqual(row["attachment_refs"][0]["status"], "failed")
        self.assertEqual([(v["source"], v["reason"]) for v in row["media_diagnostics"]["items"]],
                         [("param", "http_body_incomplete"), ("full_url", "http_body_incomplete")])
        self.assertFalse(row["media_diagnostics"]["truncated"])
        indexed = runtime.catalog(self.store).query(self.scope)["items"]
        self.assertEqual(len(indexed), 1)
        self.assertEqual(indexed[0]["status"], "failed")
        self.assertIsNone(indexed[0]["size"])
        self.assertEqual(list((self.base / "attachments/objects").iterdir()), [])
        self.assertEqual(list((self.base / "attachments/staging").iterdir()), [])

    def test_exhausted_short_param_falls_back_without_losing_success_diagnostic(self):
        original, message, cipher = self.image("synthetic-image-source-fallback")
        message["item_list"][0]["image_item"]["media"]["full_url"] = "https://synthetic.invalid/complete"
        row, requests = self.receive_sequence(message, [
            (cipher[:-16], len(cipher)), (cipher[:-16], len(cipher)), (cipher, len(cipher))])
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[0][0], requests[1][0])
        self.assertNotEqual(requests[1][0], requests[2][0])
        self.ready_original(row, original)
        self.assertEqual([(v["source"], v["outcome"], v["reason"]) for v in
                          row["media_diagnostics"]["items"]], [
                              ("param", "failed", "http_body_incomplete"),
                              ("full_url", "success", "ok")])
        self.assertFalse(row["media_diagnostics"]["truncated"])

    def test_ready_image_replay_never_downloads_or_changes_original_identity(self):
        original, message, _ = self.image("synthetic-image-ready-replay")
        first = self.receive(message)
        aid, path = self.ready_original(first, original)
        before = (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        with patch.object(media.urllib.request, "urlopen", side_effect=AssertionError("ready replay downloaded")) as http:
            self.assertTrue(serve.handle_incoming(self.store, message, harness.OWNER))
        http.assert_not_called()
        repeated = load_json(str(Path(self.store.inbox_dir) / (message["message_id"] + ".json")))
        self.assertEqual(first, repeated)
        self.assertEqual(self.ready_original(repeated, original)[0], aid)
        self.assertEqual((path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes()), before)

    def test_current_authority_blocks_foreign_owner_and_expired_worker_image_access(self):
        original, message, _ = self.image("synthetic-image-authority")
        row = self.receive(message)
        aid, path = self.ready_original(row, original)
        batch = self.batch()
        claims = copy.deepcopy(self.worker._claims())
        session = load_json(self.store.session_path)
        foreign = dict(session, ilink_user_id="synthetic-other-image-owner")
        atomic_write_json(self.store.session_path, foreign)
        for action, args in (("query", {}), ("get", {"attachment_id": aid})):
            with self.subTest(authority="foreign_owner", action=action):
                result = self.call(batch, action, args)
                self.error(result, "scope_mismatch")
                self.assertNotIn(str(self.base), json.dumps(result))
        atomic_write_json(self.store.session_path, session)
        self.now += worker_state.WORKER_LEASE_SECONDS
        for action, args in (("query", {}), ("get", {"attachment_id": aid})):
            with self.subTest(authority="expired_worker", action=action):
                result = self.call(batch, action, args)
                self.error(result, "lease_lost")
                self.assertNotIn(str(self.base), json.dumps(result))
        self.assertEqual(self.worker._claims(), claims)
        self.assertEqual(path.read_bytes(), original)

    def test_image_api_uncertainty_survives_restart_without_resend_or_budget_refund(self):
        original, message, _ = self.image("synthetic-image-send-unknown")
        row = self.receive(message)
        aid, path = self.ready_original(row, original)
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        messages, sent_originals = self.send_spy(final_result=TimeoutError("synthetic send outcome unknown"))
        self.flush()
        self.assertEqual(len(messages), 1)
        self.assertEqual(sent_originals, [original])
        status = runtime.ledger(self.store).group_status(self.scope, group["group_id"])
        self.assertEqual(status["state"], "unknown")
        self.assertEqual(status["units"]["unknown"], 1)
        self.assertFalse(status["can_continue"])
        restarted = Store(str(self.base))
        self.addCleanup(restarted.close)
        runtime.initialize(restarted)
        runtime.ledger(restarted).recover()
        self.flush(restarted)
        self.assertEqual(len(messages), 1)
        self.assertEqual(runtime.ledger(restarted).group_status(self.scope, group["group_id"])["state"], "unknown")
        self.assertEqual(runtime.ledger(restarted).register_context(self.scope, batch["context"])["used"], 1)
        self.assertEqual(path.read_bytes(), original)

    def test_prepared_image_publish_interruption_recovers_without_redownload(self):
        original, message, _ = self.image("synthetic-image-publish-recovery")
        Path(self.store.buf_path).write_text("SYNTHETIC-CURSOR-BEFORE")
        original_fault = AttachmentStore._fault
        interruptions = []

        def stop_after_prepared(catalog, point):
            if point == "publish_prepared" and not interruptions:
                interruptions.append(point)
                raise OSError("synthetic image publish interruption")
            return original_fault(catalog, point)

        with patch.object(media.urllib.request, "urlopen", side_effect=self.transport) as first_http, \
                patch.object(AttachmentStore, "_fault", stop_after_prepared):
            self.assertFalse(serve.handle_incoming(self.store, message, harness.OWNER))
        self.assertEqual(first_http.call_count, 1)
        self.assertEqual(interruptions, ["publish_prepared"])
        self.assertEqual(Path(self.store.buf_path).read_text(), "SYNTHETIC-CURSOR-BEFORE")
        self.assertFalse((Path(self.store.inbox_dir) / (message["message_id"] + ".json")).exists())
        waiting = runtime.catalog(self.store).query(self.scope)["items"]
        self.assertEqual(len(waiting), 1)
        self.assertEqual(waiting[0]["status"], "pending")
        stage = list((self.base / "attachments/staging").glob("*.part"))
        self.assertEqual(len(stage), 1)
        self.assertEqual(stage[0].read_bytes(), original)
        self.assertEqual(list((self.base / "attachments/objects").iterdir()), [])

        with patch.object(media.urllib.request, "urlopen", side_effect=AssertionError("prepared recovery downloaded")) as replay_http:
            self.assertTrue(serve.handle_incoming(self.store, message, harness.OWNER))
        replay_http.assert_not_called()
        recovered = load_json(str(Path(self.store.inbox_dir) / (message["message_id"] + ".json")))
        # Catalog time is the first event registration; an inbox repaired by a
        # later upstream replay has its own receive time without changing it.
        self.assertLessEqual(waiting[0]["received_at"], recovered["received_at"])
        self.assertEqual(self.ready_original(recovered, original,
                         received_at=waiting[0]["received_at"])[0], waiting[0]["attachment_id"])
        self.assertEqual(Path(self.store.buf_path).read_text(), "SYNTHETIC-CURSOR-BEFORE")
        self.assertEqual(list((self.base / "attachments/staging").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
