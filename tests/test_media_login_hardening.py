"""真实 AES、假 HTTP、临时状态：媒体边界/登录隔离回归。"""
import argparse
import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechat_bridge import cli, crypto, ilink, media
from wechat_bridge.store import Store, atomic_write_json, load_json
from wechat_bridge.worker_state import WorkerState


KEY = b"0123456789abcdef"


class Response:
    def __init__(self, body):
        self.body = body
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def read(self, size=-1):
        return self.body[:size] if size >= 0 else self.body


class HardeningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wechat-hardening-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(str(self.root / "state-root"))
        for name in ("socket.connect", "socket.connect_ex", "create_connection", "getaddrinfo"):
            guard = patch("socket." + name, side_effect=AssertionError("Real network forbidden"))
            guard.start()
            self.addCleanup(guard.stop)
        for guard in (patch.object(cli, "get_store", lambda: self.store),
                      patch.object(cli, "RELOGIN_NOTIFIED_PATH", str(self.root / "fake-notified"))):
            guard.start()
            self.addCleanup(guard.stop)

    def session(self, owner="owner-a", token="FAKE-A", bot="bot-a"):
        return {"bot_token": token, "ilink_user_id": owner, "ilink_bot_id": bot,
                "baseurl": "https://api.invalid"}

    def login(self, response):
        atomic_write_json(self.store.qr_path, {"qrcode": "FAKE-QR"})
        with patch.object(cli, "http_json", return_value=response), \
             contextlib.redirect_stdout(io.StringIO()):
            return cli.cmd_wait_login(argparse.Namespace())

    def send_args(self, **kwargs):
        return argparse.Namespace(**dict({"text": "synthetic", "image": None, "file": None,
                                         "video": None, "to": None, "context": None}, **kwargs))

    def test_fresh_login_and_same_account_refresh_preserve_data(self):
        self.assertEqual(self.login(dict(self.session(), status="confirmed")), 0)
        atomic_write_json(self.store.last_ctx_path, {"to_user_id": "owner-a", "context_token": "FAKE-CONTEXT"})
        Path(self.store.buf_path).write_text("CURSOR-A")
        job = Path(self.store.pending_dir) / "a.json"
        atomic_write_json(job, {"text": "keep"})
        Path(self.store.needs_relogin_path).write_text("expired")
        response = dict(self.session(token="FAKE-REFRESH"), status="confirmed")
        self.assertEqual(self.login(response), 0)
        self.assertEqual(load_json(self.store.session_path)["bot_token"], "FAKE-REFRESH")
        self.assertEqual(Path(self.store.buf_path).read_text(), "CURSOR-A")
        self.assertEqual(load_json(job), {"text": "keep"})
        self.assertFalse(Path(self.store.needs_relogin_path).exists())
        self.assertFalse(Path(self.store.qr_path).exists())

    def test_changed_account_cannot_overwrite_or_send_old_state(self):
        old = self.session()
        atomic_write_json(self.store.session_path, old)
        atomic_write_json(self.store.last_ctx_path, {"to_user_id": "owner-a", "context_token": "OLD"})
        Path(self.store.buf_path).write_text("CURSOR-A")
        job = Path(self.store.pending_dir) / "old.json"
        atomic_write_json(job, {"to_user_id": "owner-a", "text": "old"})
        snapshots = {p: p.read_bytes() for p in (Path(self.store.session_path), Path(self.store.last_ctx_path),
                                               Path(self.store.buf_path), job)}
        self.assertEqual(self.login(dict(self.session("owner-b", "FAKE-B", "bot-b"), status="confirmed")), 1)
        for path, content in snapshots.items():
            self.assertEqual(path.read_bytes(), content)

    def test_same_owner_different_bot_is_rejected(self):
        old = self.session()
        atomic_write_json(self.store.session_path, old)
        self.assertEqual(self.login(dict(self.session(bot="different-bot"), status="confirmed")), 1)
        self.assertEqual(load_json(self.store.session_path), old)

    def test_missing_owner_cannot_be_saved_or_served(self):
        self.assertEqual(self.login({"status": "confirmed", "bot_token": "FAKE"}), 1)
        self.assertFalse(Path(self.store.session_path).exists())
        atomic_write_json(self.store.session_path, {"bot_token": "FAKE"})
        with patch.object(cli, "serve_run") as run, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.cmd_serve(argparse.Namespace()), 1)
            run.assert_not_called()

    def test_unowned_nonempty_state_directory_is_rejected(self):
        Path(self.store.buf_path).write_text("UNOWNED")
        self.assertEqual(self.login(dict(self.session(), status="confirmed")), 1)
        self.assertFalse(Path(self.store.session_path).exists())
        self.assertEqual(Path(self.store.buf_path).read_text(), "UNOWNED")

    def test_hook_plan_before_first_login_does_not_claim_account_ownership(self):
        worker = WorkerState(self.store.base, str(self.root / "hook-state"))
        self.assertEqual(worker.plan()["reason"], "not logged in yet")
        lock = Path(self.store.receipt_lock_path)
        self.assertTrue(lock.exists())
        self.assertEqual(lock.stat().st_size, 0)
        self.assertEqual(self.login(dict(self.session(), status="confirmed")), 0)

    def test_unknown_or_nonempty_lock_files_still_block_unowned_rebind(self):
        for index, (name, content) in enumerate((
                ("message_receipts.lock", "unknown-data"),
                ("serve.lock", "unexpected-data"), ("unknown.lock", ""))):
            with self.subTest(name=name):
                self.store = Store(str(self.root / ("unknown-" + str(index))))
                path = Path(self.store.state_dir) / name
                path.write_text(content)
                self.assertEqual(self.login(dict(self.session(), status="confirmed")), 1)
                self.assertFalse(Path(self.store.session_path).exists())
                self.assertEqual(path.read_text(), content)

    def test_status_reports_quarantine_without_exposing_raw_payload(self):
        secret = "SYNTHETIC-PRIVATE-BODY-AND-TOKEN"
        atomic_write_json(Path(self.store.quarantine_dir) / "one.json", {"payload": secret})
        Path(self.store.quarantine_status_path).write_text(secret)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.cmd_status(argparse.Namespace()), 0)
        text = output.getvalue()
        self.assertIn("接收隔离: 1 条", text)
        self.assertIn("隔离提示标记: 存在", text)
        self.assertIn("需人工检查", text)
        self.assertNotIn(secret, text)

    def test_binded_without_new_token_requires_verified_existing_session(self):
        response = {"status": "binded_redirect", "ilink_user_id": "owner-a", "ilink_bot_id": "bot-a"}
        self.assertEqual(self.login(response), 1)
        atomic_write_json(self.store.session_path, self.session())
        self.assertEqual(self.login(response), 0)
        self.assertEqual(self.login({"status": "binded_redirect"}), 1)
        Path(self.store.needs_relogin_path).write_text("expired")
        self.assertEqual(self.login(response), 1)
        self.assertTrue(Path(self.store.needs_relogin_path).exists())

    def test_active_serve_blocks_login_without_polling_and_releases_lock(self):
        atomic_write_json(self.store.qr_path, {"qrcode": "FAKE"})
        with open(self.store.lock_path, "a") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(cli, "http_json") as network, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cli.cmd_wait_login(argparse.Namespace()), 1)
                network.assert_not_called()
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        self.assertEqual(self.login(dict(self.session(), status="confirmed")), 0)
        with open(self.store.lock_path, "a") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def test_new_cli_operations_have_unique_ids_and_distinct_subkeys(self):
        atomic_write_json(self.store.session_path, self.session())
        with patch.object(cli, "send_message", return_value={"message_id": 1}) as send, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.cmd_send(self.send_args(text="first")), 0)
            self.assertEqual(cli.cmd_send(self.send_args(text="second")), 0)
        self.assertNotEqual(send.call_args_list[0].kwargs["client_key"],
                            send.call_args_list[1].kwargs["client_key"])
        attachment = self.root / "image.png"
        attachment.write_bytes(b"fake")
        with patch.object(cli, "send_message", return_value={"message_id": 1}) as caption, \
             patch.object(cli, "send_media_message", return_value={"message_id": 2}) as picture, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.cmd_send(self.send_args(image=str(attachment))), 0)
        caption_key, media_key = caption.call_args.kwargs["client_key"], picture.call_args.kwargs["client_key"]
        self.assertNotEqual(caption_key, media_key)
        self.assertEqual(caption_key.rsplit(":", 1)[0], media_key.rsplit(":", 1)[0])

    def download(self, body, **kwargs):
        path = self.root / "download.bin"
        with patch.object(media.urllib.request, "urlopen", return_value=Response(body)):
            media.download_media({"full_url": "https://cdn.invalid/a", "aes_key": KEY.decode()},
                                 dest_path=str(path), **kwargs)
        return path.read_bytes()

    def test_invalid_or_wrong_override_falls_back_to_valid_nested_key(self):
        plain = b"\xff\xd8real-aes-test"
        cipher = crypto._aes_ecb_encrypt(plain, KEY)
        for override in (b"\xff", b"wrong-key-16byte!"):
            with self.subTest(override=override):
                self.assertEqual(self.download(cipher, aes_key_override=override), plain)

    def test_bad_ciphertext_source_falls_back_to_valid_source(self):
        plain = b"\xff\xd8valid-image"
        good = crypto._aes_ecb_encrypt(plain, KEY)
        calls = []
        def fake(req, **kwargs):
            calls.append(req.full_url)
            return Response(b"invalid" if "/download?" in req.full_url else good)
        path = self.root / "fallback.jpg"
        with patch.object(media.urllib.request, "urlopen", fake):
            media.download_media({"encrypt_query_param": "FAKE", "full_url": "https://cdn.invalid/a",
                                  "aes_key": KEY.decode()}, dest_path=str(path))
        self.assertEqual(path.read_bytes(), plain)
        self.assertEqual(len(calls), 2)

    def test_plaintext_and_ciphertext_size_boundaries(self):
        with patch.object(media, "MAX_MEDIA_BYTES", 32):
            self.assertEqual(self.download(crypto._aes_ecb_encrypt(b"x" * 32, KEY)), b"x" * 32)
            for plain in (b"x" * 33, b"x" * 49):
                with self.subTest(size=len(plain)), self.assertRaises(ValueError):
                    self.download(crypto._aes_ecb_encrypt(plain, KEY))

    def test_oversized_upload_rejected_before_upload(self):
        path = self.root / "big.bin"
        path.write_bytes(b"x" * 33)
        with patch.object(media, "MAX_MEDIA_BYTES", 32), patch.object(media, "upload_media") as upload:
            with self.assertRaises(ValueError):
                media.send_media_message("https://api.invalid", "FAKE", "owner", "FAKE", str(path), 4)
            upload.assert_not_called()

    def test_growing_file_read_is_bounded(self):
        path = self.root / "grows.bin"
        path.write_bytes(b"small")
        real_fdopen = os.fdopen
        requested = []
        class GrowingFile:
            def __init__(self, fd):
                self.file = real_fdopen(fd, "rb")
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.file.close()
            def fileno(self):
                return self.file.fileno()
            def read(self, size=-1):
                requested.append(size)
                return b"x" * min(33, size) if size >= 0 else b"x" * 33
        with patch.object(media, "MAX_MEDIA_BYTES", 32), \
             patch.object(media.os, "fdopen", lambda fd, mode: GrowingFile(fd)), \
             patch.object(media, "upload_media") as upload:
            with self.assertRaises(ValueError):
                media.send_media_message("https://api.invalid", "FAKE", "owner", "FAKE", str(path), 4)
        self.assertEqual(requested, [33])
        upload.assert_not_called()

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO not supported")
    def test_fifo_is_rejected_without_waiting_for_writer(self):
        path = self.root / "fifo"
        os.mkfifo(path)
        with self.assertRaises(ValueError):
            media.send_media_message("https://api.invalid", "FAKE", "owner", "FAKE", str(path), 4)

    def test_empty_ciphertext_and_bad_key_fail_cleanly(self):
        self.assertEqual(crypto.try_decrypt_variants(b"", KEY.decode()), (None, None))
        with self.assertRaises(ValueError):
            crypto._aes_ecb_decrypt(b"", KEY)
        for value in (None, 123, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                crypto.parse_aes_key(value)

    def test_declared_qr_dependency_generates_real_png_offline(self):
        manifest = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
        self.assertIn('"qrcode[pil]"', manifest)
        qrpath = self.root / "qr.png"
        saved = []
        with patch.object(ilink, "http_json", return_value={"qrcode": "FAKE-QR", "qrcode_img_content": "https://example.invalid/qr"}):
            code, _ = ilink.fetch_qrcode(saved.append, str(qrpath))
        self.assertEqual(code, "FAKE-QR")
        self.assertTrue(qrpath.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertEqual(saved[0]["qrcode"], "FAKE-QR")


if __name__ == "__main__":
    unittest.main(verbosity=2)
