"""Offline receiver regressions: no real account, socket, or production paths."""
import contextlib
import io
import os
import pathlib
import socket
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from wechat_bridge import serve
from wechat_bridge.store import (
    Store, atomic_write_json, atomic_write_text, load_json,
    message_receipt_lock, safe_message_id,
)


def no_network(*args, **kwargs):
    raise AssertionError("receiver regression tests forbid real network")


def message(mid=1, text="hello", items=None, context="ctx"):
    return {"message_type": 1, "message_id": mid, "from_user_id": "owner",
            "context_token": context,
            "item_list": items if items is not None else [
                {"type": 1, "text_item": {"text": text}}]}


def image(marker):
    return {"type": 2, "image_item": {"media": {"marker": marker}}}


class ReceiverHardening(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wechat-receiver-test-")
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(self.tmp.name)
        self.output = io.StringIO()
        self.capture = contextlib.redirect_stdout(self.output)
        self.capture.__enter__()
        self.addCleanup(self.capture.__exit__, None, None, None)
        for target in ((socket.socket, "connect"), (socket.socket, "connect_ex"),
                       (socket, "create_connection"), (socket, "getaddrinfo")):
            blocker = patch.object(target[0], target[1], no_network)
            blocker.start()
            self.addCleanup(blocker.stop)

    def inbox(self, mid=1):
        return load_json(os.path.join(self.store.inbox_dir, "%s.json" % mid))

    def receipt(self, mid=1):
        return load_json(os.path.join(self.store.receipts_dir, "%s.json" % mid))

    def run_polls(self, responses):
        polls, delays = [], []
        iterator = iter(responses)
        def fake_http(method, url, headers, body=None, timeout=None):
            polls.append(body["get_updates_buf"])
            try:
                response = next(iterator)
            except StopIteration:
                raise KeyboardInterrupt()
            return response() if callable(response) else response
        with patch.object(serve, "http_json", fake_http), \
                patch.object(serve.time, "sleep", lambda seconds: delays.append(seconds)), \
                patch.object(serve, "FLUSH_INTERVAL_SECONDS", 60):
            result = serve.run(self.store, "https://qa.invalid", "FAKE", "owner")
        return result, polls, delays

    @staticmethod
    def fake_download(media, key, dest):
        pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(dest).write_bytes(media["marker"].encode())
        return dest

    def test_mixed_text_and_image_both_survive(self):
        msg = message(items=[{"type": 1, "text_item": {"text": "look here"}}, image("A")])
        with patch.object(serve, "download_media", self.fake_download):
            self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        record = self.inbox()
        self.assertEqual(record["text"], "look here")
        self.assertEqual(pathlib.Path(record["media_paths"][0]).read_bytes(), b"A")

    def test_multiple_images_have_distinct_stable_paths(self):
        msg = message(items=[image("FIRST"), image("SECOND")])
        with patch.object(serve, "download_media", self.fake_download):
            self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
            paths = self.inbox()["media_paths"]
            self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        self.assertEqual(len(set(paths)), 2)
        self.assertEqual([pathlib.Path(p).read_bytes() for p in paths], [b"FIRST", b"SECOND"])
        self.assertEqual(self.inbox()["media_paths"], paths)

    def test_same_filename_and_sanitized_names_do_not_collide(self):
        items = [{"type": 4, "file_item": {"file_name": name, "media": {"marker": value}}}
                 for name, value in [("../same.txt", "A"), ("..\\same.txt", "B")]]
        with patch.object(serve, "download_media", self.fake_download):
            self.assertTrue(serve.handle_incoming(self.store, message(items=items), "owner"))
        paths = self.inbox()["media_paths"]
        self.assertEqual(len(set(paths)), 2)
        self.assertEqual([pathlib.Path(p).read_bytes() for p in paths], [b"A", b"B"])
        for path in paths:
            self.assertEqual(os.path.commonpath([self.store.media_dir, path]), self.store.media_dir)

    def test_long_safe_id_and_utf8_filename_fit_filesystem(self):
        items = [{"type": 4, "file_item": {"file_name": "中" * 500, "media": {"marker": "A"}}}]
        with patch.object(serve, "download_media", self.fake_download):
            self.assertTrue(serve.handle_incoming(self.store, message("a" * 180, items=items), "owner"))
        self.assertTrue(self.inbox("a" * 180).get("media_paths"))

    def test_voice_without_transcription_keeps_audio(self):
        voice = {"type": 3, "voice_item": {"media": {"marker": "SILK_AUDIO"}}}
        with patch.object(serve, "download_media", self.fake_download):
            self.assertTrue(serve.handle_incoming(self.store, message(items=[voice]), "owner"))
        self.assertEqual(self.inbox()["text"], "[语音]")
        self.assertEqual(pathlib.Path(self.inbox()["media_paths"][0]).read_bytes(), b"SILK_AUDIO")

    def test_media_failure_preserves_text_and_marks_failure(self):
        msg = message(items=[{"type": 1, "text_item": {"text": "keep me"}}, image("A")])
        with patch.object(serve, "download_media", side_effect=ValueError("private URL secret")):
            self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        self.assertEqual(self.inbox()["text"], "keep me")
        self.assertEqual(self.inbox()["media_failed_count"], 1)
        self.assertNotIn("private URL secret", self.output.getvalue())

    def test_owner_missing_fails_closed_and_serve_does_not_poll(self):
        self.assertFalse(serve.handle_incoming(self.store, message(), None))
        with patch.object(serve, "http_json", side_effect=AssertionError("must not poll")):
            self.assertEqual(serve.run(self.store, "https://qa.invalid", "FAKE", None), 1)
        self.assertEqual(os.listdir(self.store.inbox_dir), [])

    def test_foreign_message_not_retained_even_if_malformed(self):
        msg = message()
        msg.update(from_user_id="stranger", item_list="bad", message_id=None)
        self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        self.assertEqual(os.listdir(self.store.inbox_dir), [])
        self.assertEqual(os.listdir(self.store.quarantine_dir), [])

    def test_invalid_ids_are_rejected_or_quarantined_without_path_escape(self):
        for bad in (None, True, False, "", -1, "-option", "../escaped", "/absolute", "a/b", "a\\b", "a\nbody", "a" * 181):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    safe_message_id(bad)
                self.assertTrue(serve.handle_incoming(self.store, message(bad), "owner"))
        self.assertEqual(os.listdir(self.store.inbox_dir), [])
        self.assertFalse(os.path.exists(os.path.join(self.store.base, "escaped.json")))
        self.assertTrue(os.path.exists(self.store.quarantine_status_path))
        self.assertEqual(safe_message_id(0), "0")
        self.assertEqual(safe_message_id("id_42-x.y"), "id_42-x.y")

    def test_malformed_item_preserves_raw_in_private_quarantine(self):
        msg = message(items=[None])
        self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        path = pathlib.Path(self.store.quarantine_dir)/os.listdir(self.store.quarantine_dir)[0]
        rejected = load_json(str(path))
        self.assertEqual(rejected["payload"], msg)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(os.listdir(self.store.inbox_dir), [])

    def test_malformed_messages_batch_is_quarantined_before_cursor_advances(self):
        atomic_write_text(self.store.buf_path, "OLD")
        result, polls, delays = self.run_polls([{"msgs": 42, "get_updates_buf": "NEW"}])
        self.assertEqual((result, polls, delays), (0, ["OLD", "NEW"], []))
        self.assertEqual(pathlib.Path(self.store.buf_path).read_text(), "NEW")
        self.assertEqual(len(os.listdir(self.store.quarantine_dir)), 1)

    def test_poison_item_no_longer_blocks_valid_following_messages(self):
        atomic_write_text(self.store.buf_path, "OLD")
        result, polls, delays = self.run_polls([{"msgs": [None, message()], "get_updates_buf": "NEW"}])
        self.assertEqual((result, polls, delays), (0, ["OLD", "NEW"], []))
        self.assertEqual(self.inbox()["text"], "hello")
        self.assertEqual(len(os.listdir(self.store.quarantine_dir)), 1)

    def test_failed_quarantine_holds_cursor_and_backs_off(self):
        atomic_write_text(self.store.buf_path, "OLD")
        bad = {"msgs": 42, "get_updates_buf": "NEW"}
        with patch.object(serve, "quarantine_record", side_effect=OSError("disk full")):
            result, polls, delays = self.run_polls([bad, bad, bad])
        self.assertEqual((result, polls, delays), (0, ["OLD"] * 4, [2, 2, 30]))
        self.assertEqual(pathlib.Path(self.store.buf_path).read_text(), "OLD")

    def test_repeated_poison_data_reuses_private_quarantine_record(self):
        msg = message(items=[None])
        self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        self.assertTrue(serve.handle_incoming(self.store, msg, "owner"))
        self.assertEqual(len(os.listdir(self.store.quarantine_dir)), 1)

    def test_cursor_write_failure_does_not_advance_in_memory(self):
        atomic_write_text(self.store.buf_path, "OLD")
        original = serve.atomic_write_text
        attempts = []
        def write(path, text):
            if path == self.store.buf_path and not attempts:
                attempts.append(path)
                raise OSError("synthetic write error")
            return original(path, text)
        batch = {"msgs": [message()], "get_updates_buf": "NEW"}
        with patch.object(serve, "atomic_write_text", write):
            result, polls, delays = self.run_polls([batch, batch])
        self.assertEqual((result, polls, delays), (0, ["OLD", "OLD", "NEW"], [2]))
        self.assertEqual(len(os.listdir(self.store.receipts_dir)), 1)

    def test_received_receipt_prevents_replay_after_inbox_deleted(self):
        self.assertTrue(serve.handle_incoming(self.store, message(), "owner"))
        os.unlink(os.path.join(self.store.inbox_dir, "1.json"))
        self.assertTrue(serve.handle_incoming(self.store, message(), "owner"))
        self.assertIsNone(self.inbox())
        self.assertEqual(self.receipt()["status"], "received")

    def test_partial_batch_retry_and_worker_completion_do_not_resurrect_message(self):
        atomic_write_text(self.store.buf_path, "OLD")
        batch = {"msgs": [message(201), message(202)], "get_updates_buf": "NEW"}
        original = serve.atomic_write_json
        failures = []
        def write(path, obj):
            if path == os.path.join(self.store.inbox_dir, "202.json") and not failures:
                failures.append(path)
                raise OSError("temporary filesystem failure")
            return original(path, obj)
        def complete_first_message_and_redeliver():
            with message_receipt_lock(self.store):
                receipt = self.receipt(201)
                receipt["status"] = "complete"
                original(os.path.join(self.store.receipts_dir, "201.json"), receipt)
                os.unlink(os.path.join(self.store.inbox_dir, "201.json"))
            return batch
        with patch.object(serve, "atomic_write_json", write):
            result, polls, delays = self.run_polls([batch, complete_first_message_and_redeliver])
        self.assertEqual((result, polls, delays), (0, ["OLD", "OLD", "NEW"], [2]))
        self.assertIsNone(self.inbox(201))
        self.assertEqual(self.receipt(201)["status"], "complete")
        self.assertEqual(self.inbox(202)["text"], "hello")

    def test_complete_receipt_survives_seen_pruning_and_preserves_new_context(self):
        old = message(1, context="OLD_CTX")
        self.assertTrue(serve.handle_incoming(self.store, old, "owner"))
        self.assertTrue(serve.handle_incoming(self.store, message(2, context="NEW_CTX"), "owner"))
        with message_receipt_lock(self.store):
            receipt = self.receipt()
            receipt["status"] = "complete"
            atomic_write_json(os.path.join(self.store.receipts_dir, "1.json"), receipt)
            os.unlink(os.path.join(self.store.inbox_dir, "1.json"))
        # No Hook seen cache exists; the durable receipt is sufficient.
        self.assertTrue(serve.handle_incoming(self.store, old, "owner"))
        self.assertIsNone(self.inbox())
        self.assertEqual(load_json(self.store.last_ctx_path)["context_token"], "NEW_CTX")

    def test_preledger_inbox_is_adopted_without_overwrite(self):
        record = dict(message(), text="original worker record")
        atomic_write_json(os.path.join(self.store.inbox_dir, "1.json"), record)
        self.assertTrue(serve.handle_incoming(self.store, message(text="replay"), "owner"))
        self.assertEqual(self.inbox()["text"], "original worker record")
        self.assertEqual(self.receipt()["status"], "received")

    def test_receipt_write_failure_can_retry_existing_inbox(self):
        original = serve.atomic_write_json
        def write(path, obj):
            if path.startswith(self.store.receipts_dir + os.sep):
                raise OSError("receipt disk failure")
            return original(path, obj)
        with patch.object(serve, "atomic_write_json", write):
            self.assertFalse(serve.handle_incoming(self.store, message(), "owner"))
        self.assertEqual(self.inbox()["text"], "hello")
        self.assertTrue(serve.handle_incoming(self.store, message(), "owner"))
        self.assertEqual(self.receipt()["status"], "received")

    def test_receipt_sender_mismatch_is_not_treated_as_duplicate(self):
        atomic_write_json(os.path.join(self.store.receipts_dir, "1.json"), {
            "message_id": "1", "from_user_id": "different-owner", "status": "complete"})
        self.assertFalse(serve.handle_incoming(self.store, message(), "owner"))
        self.assertIsNone(self.inbox())

    def test_ack_cannot_delete_between_inbox_and_receipt_publication(self):
        published = threading.Event()
        release = threading.Event()
        ack_done = threading.Event()
        original = serve.atomic_write_json
        results = []
        def write(path, obj):
            original(path, obj)
            if path == os.path.join(self.store.inbox_dir, "1.json"):
                published.set()
                if not release.wait(3):
                    raise AssertionError("test synchronization timed out")
        def acknowledge():
            with message_receipt_lock(self.store):
                receipt = self.receipt()
                receipt["status"] = "complete"
                atomic_write_json(os.path.join(self.store.receipts_dir, "1.json"), receipt)
                os.unlink(os.path.join(self.store.inbox_dir, "1.json"))
                ack_done.set()
        with patch.object(serve, "atomic_write_json", write):
            receiver = threading.Thread(target=lambda: results.append(serve.handle_incoming(self.store, message(), "owner")))
            receiver.start()
            self.assertTrue(published.wait(3))
            worker = threading.Thread(target=acknowledge)
            worker.start()
            try:
                self.assertFalse(ack_done.wait(0.05))
            finally:
                release.set()
                receiver.join(3)
                worker.join(3)
        self.assertEqual(results, [True])
        self.assertTrue(ack_done.is_set())
        self.assertEqual(self.receipt()["status"], "complete")
        self.assertTrue(serve.handle_incoming(self.store, message(), "owner"))
        self.assertIsNone(self.inbox())

    def test_text_body_and_context_are_absent_from_logs(self):
        self.assertTrue(serve.handle_incoming(self.store, message(text="PRIVATE_BODY_7391", context="PRIVATE_CTX_9275"), "owner"))
        self.assertNotIn("PRIVATE_BODY_7391", self.output.getvalue())
        self.assertNotIn("PRIVATE_CTX_9275", self.output.getvalue())
        self.assertEqual(self.inbox()["text"], "PRIVATE_BODY_7391")

    def test_media_key_parser_accepts_raw16_and_rejects_short_hex(self):
        self.assertEqual(serve._media_key({"aeskey": "0123456789abcdef"}), b"0123456789abcdef")
        self.assertEqual(serve._media_key({"aeskey": "30313233343536373839616263646566"}), b"0123456789abcdef")
        self.assertIsNone(serve._media_key({"aeskey": "deadbeef"}))


if __name__ == "__main__":
    unittest.main()
