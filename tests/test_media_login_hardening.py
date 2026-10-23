"""真实 AES、假 HTTP、临时状态：媒体边界/登录隔离回归。"""
import argparse
import base64
import contextlib
import fcntl
import hashlib
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

from wechat_bridge import cli, crypto, ilink, media, serve
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


class ImageReceiveDiagnostics(unittest.TestCase):
    """Synthetic bytes only; no credentials, CDN or running service."""
    PNG = b"\x89PNG\r\n\x1a\n" + b"synthetic-image-body" * 2

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wechat-image-diagnostic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "received.jpg"
        self.logs = []
        for guard in (patch.object(media, "log", self.logs.append),
                      patch.object(serve, "log", self.logs.append)):
            guard.start()
            self.addCleanup(guard.stop)
        for name in ("socket.connect", "socket.connect_ex", "create_connection", "getaddrinfo"):
            guard = patch("socket." + name, side_effect=AssertionError("Real network forbidden"))
            guard.start()
            self.addCleanup(guard.stop)

    def download(self, body, raw_key=KEY, nested_key=None, **kwargs):
        reference = {"full_url": "https://cdn.invalid/PRIVATE-URL?secret=PRIVATE-QUERY"}
        if nested_key is not None:
            reference["aes_key"] = nested_key
        with patch.object(media.urllib.request, "urlopen", return_value=Response(body)):
            media.download_media(reference, raw_key, str(self.path), expected_type="image", **kwargs)
        return self.path.read_bytes()

    def diagnostics(self):
        return [json.loads(line.split(" ", 1)[1]) for line in self.logs
                if line.startswith("media_receive ")]

    def cbc(self, plain, key, iv):
        from Crypto.Cipher import AES
        pad = 16 - len(plain) % 16
        return AES.new(key, AES.MODE_CBC, iv=iv).encrypt(plain + bytes([pad]) * pad)

    def test_image_magic_categories_and_near_misses(self):
        cases = [(b"\xff\xd8\xffx", "JPEG"), (self.PNG, "PNG"),
                 (b"GIF87a...", "GIF"), (b"GIF89a...", "GIF"),
                 (b"RIFF\x10\x00\x00\x00WEBP...", "WEBP"),
                 (b"RIFF....WAVE", "other"), (b"\xff\xd8x", "other"),
                 (b"\x89PNG", "other"), (b"\x00" * 32, "other")]
        for value, expected in cases:
            self.assertEqual(crypto.image_magic(value), expected)

    def test_plaintext_images_need_no_key_and_may_be_unaligned(self):
        for body in (b"\xff\xd8\xffplain", self.PNG, b"GIF89aplain",
                     b"RIFF....WEBPplain"):
            with self.subTest(category=crypto.image_magic(body)):
                self.assertEqual(self.download(body, raw_key=None), body)
                event = self.diagnostics()[-1]
                self.assertEqual(event["strategy"], "plaintext-image")
                self.assertEqual(event["bytes"], len(body))
                self.assertEqual(event["block_aligned"], len(body) % 16 == 0)

    def test_image_requires_magic_as_well_as_padding(self):
        for plain in (b"not-an-image", b"\x00" * 24,
                      b"\x00\x00\x00\x20ftypisom", b"\xff\xd8short"):
            with self.subTest(plain_class="synthetic-negative"):
                with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                    self.download(crypto._aes_ecb_encrypt(plain, KEY))
        self.assertFalse(self.path.exists())

    def test_encrypted_image_magic_formats(self):
        for plain in (b"\xff\xd8\xffsynthetic", self.PNG, b"GIF89asynthetic",
                      b"RIFF....WEBPsynthetic"):
            self.assertEqual(self.download(crypto._aes_ecb_encrypt(plain, KEY)), plain)

    def test_magic_without_valid_padding_is_rejected(self):
        from Crypto.Cipher import AES
        invalid = b"\x89PNG\r\n\x1a\n" + b"x" * 7 + b"\x00"
        with self.assertRaises(ValueError):
            self.download(AES.new(KEY, AES.MODE_ECB).encrypt(invalid))
        self.assertFalse(self.path.exists())

    def test_cbc_prefixed_iv(self):
        iv = bytes(range(16))
        self.assertEqual(self.download(iv + self.cbc(self.PNG, KEY, iv)), self.PNG)
        self.assertIn("CBC-prefix-IV", self.diagnostics()[-1]["strategy"])

    def test_cbc_key_iv(self):
        self.assertEqual(self.download(self.cbc(self.PNG, KEY, KEY)), self.PNG)
        self.assertIn("CBC-key", self.diagnostics()[-1]["strategy"])

    def test_cbc_zero_iv_and_aes256_prefix_iv(self):
        self.assertEqual(self.download(self.cbc(self.PNG, KEY, b"\x00" * 16)), self.PNG)
        key32 = bytes(range(32))
        iv = b"p" * 16
        self.assertEqual(self.download(iv + self.cbc(self.PNG, key32, iv),
                                       raw_key=base64.b64encode(key32).decode()), self.PNG)
        self.assertIn("CBC-prefix-IV", self.diagnostics()[-1]["strategy"])
        self.assertEqual(self.download(self.cbc(self.PNG, key32, key32[:16]),
                                       raw_key=base64.b64encode(key32).decode()), self.PNG)
        self.assertIn("CBC-key-first16", self.diagnostics()[-1]["strategy"])

    def test_md5_original_item_string_and_raw_truncation(self):
        from Crypto.Cipher import AES
        raw = "  synthetic:原始-key-not-normalized  "
        self.assertEqual(self.download(crypto._aes_ecb_encrypt(
            self.PNG, hashlib.md5(raw.encode()).digest()), raw_key=raw), self.PNG)
        self.assertIn("item:raw-md5-ECB", self.diagnostics()[-1]["strategy"])
        for count in (16, 32):
            raw = "synthetic-key-material-with-a-long-prefix:private-suffix"
            key = raw.encode()[:count]
            self.assertEqual(self.download(crypto._aes_ecb_encrypt(self.PNG, key), raw_key=raw), self.PNG)
            self.assertIn("raw-first%d" % count, self.diagnostics()[-1]["strategy"])

    def test_wrong_or_malformed_item_key_falls_back_to_nested_raw(self):
        raw = "synthetic:nested-unparsed-key"
        cipher = crypto._aes_ecb_encrypt(self.PNG, hashlib.md5(raw.encode()).digest())
        self.assertEqual(self.download(cipher, raw_key=b"wrong-key-16byte!", nested_key=raw), self.PNG)
        self.assertIn("media:raw-md5", self.diagnostics()[-1]["strategy"])

    def test_parsed_padding_only_result_does_not_block_next_key(self):
        with patch.object(crypto, "_aes_ecb_decrypt", side_effect=[b"not an image", self.PNG]):
            self.assertEqual(self.download(b"x" * 16, raw_key=KEY, nested_key=b"different-key-16"), self.PNG)
        self.assertEqual(self.diagnostics()[-1]["strategy"], "media:parsed-ECB")

    def test_dual_source_decrypt_failure_then_success(self):
        good = crypto._aes_ecb_encrypt(self.PNG, KEY)
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(b"bad"), Response(good)]):
            media.download_media({"encrypt_query_param": "PRIVATE-PARAM", "full_url": "https://cdn.invalid/PRIVATE-URL"},
                                 KEY, self.path, expected_type="image")
        self.assertEqual(self.path.read_bytes(), self.PNG)
        self.assertEqual([(x["source"], x["stage"], x["outcome"]) for x in self.diagnostics()],
                         [("param", "decrypt", "failed"), ("full_url", "decrypt", "success")])

    def test_http_failure_has_no_previous_source_bytes_and_no_secrets(self):
        import urllib.error
        secret_key = "PRIVATE-KEY-CHARACTERS-SENTINEL"
        error = urllib.error.HTTPError("https://cdn.invalid/PRIVATE-URL?PRIVATE-QUERY", 403,
                                       "PRIVATE-BODY", None, None)
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(b"bad"), error]):
            with self.assertRaises(ValueError) as caught:
                media.download_media({"encrypt_query_param": "PRIVATE-PARAM", "full_url": "https://cdn.invalid/PRIVATE-URL",
                                      "aes_key": secret_key}, KEY, self.path, expected_type="image")
        events = self.diagnostics()
        self.assertEqual(events[0]["bytes"], 3)
        self.assertEqual(events[1]["stage"], "http")
        self.assertIsNone(events[1]["bytes"])
        self.assertIsNone(events[1]["block_aligned"])
        self.assertEqual(events[1]["attempted"], [])
        self.assertEqual(events[1]["http_status"], 403)
        logged = "\n".join(self.logs) + str(caught.exception)
        for secret in (secret_key, KEY.decode(), "PRIVATE-URL", "PRIVATE-QUERY", "PRIVATE-PARAM", "PRIVATE-BODY"):
            self.assertNotIn(secret, logged)
        self.assertEqual(events[1]["keys"]["media.aes_key"]["length"], len(secret_key))

    def test_plain_and_encrypted_size_boundaries_preserve_old_file(self):
        self.path.write_bytes(b"previous-file")
        exact = b"\x89PNG\r\n\x1a\n" + b"x" * 24
        with patch.object(media, "MAX_MEDIA_BYTES", 32):
            iv = bytes(range(16))
            for body in (exact, crypto._aes_ecb_encrypt(exact, KEY),
                         iv + self.cbc(exact, KEY, iv)):
                self.assertEqual(self.download(body), exact)
            for body in (exact + b"x", crypto._aes_ecb_encrypt(exact + b"x", KEY),
                         iv + self.cbc(exact + b"x", KEY, iv), b"x" * 80):
                with self.assertRaises(ValueError):
                    self.download(body)
                self.assertEqual(self.path.read_bytes(), exact)
        self.assertEqual(list(self.root.glob(".media-*")), [])

    def test_atomic_replace_failure_preserves_old_file_and_cleans_temp(self):
        self.path.write_bytes(b"previous-file")
        with patch.object(media.os, "replace", side_effect=OSError("synthetic replace failure")):
            with self.assertRaises(OSError):
                self.download(self.PNG)
        self.assertEqual(self.path.read_bytes(), b"previous-file")
        self.assertEqual(list(self.root.glob(".media-*")), [])

    def test_field_tree_is_bounded_and_never_includes_values_or_unsafe_names(self):
        item = {"aeskey": "PRIVATE-KEY", "media": {"aes_key": "PRIVATE-NESTED", "full_url": "PRIVATE-URL"},
                "PRIVATE-BODY": "PRIVATE-BODY", "https://PRIVATE-URL\n": "PRIVATE-BODY"}
        for _ in range(12):
            item = {"media": item}
        media.log_image_item_fields(item)
        tree = json.loads(self.logs[-1].split(" ", 1)[1])
        self.assertTrue(tree["truncated"])
        self.assertLessEqual(len(tree["fields"]), 64)
        self.logs.clear()
        media.log_image_item_fields({"aeskey": "PRIVATE-KEY", "media": {"aes_key": "PRIVATE-NESTED"},
                                     "full_url": "PRIVATE-URL", "https://PRIVATE-BODY\n": "PRIVATE-BODY"})
        self.assertEqual(len(self.logs), 1)
        for secret in ("PRIVATE-KEY", "PRIVATE-NESTED", "PRIVATE-URL", "PRIVATE-BODY"):
            self.assertNotIn(secret, self.logs[0])
        self.assertNotIn("\n", self.logs[0])
        media.log_image_item_fields({str(n): {"aeskey": "PRIVATE"} for n in range(100)})
        self.assertLessEqual(len(json.loads(self.logs[-1].split(" ", 1)[1])["fields"]), 64)

    def test_new_identifier_field_names_are_visible_but_values_are_not(self):
        media.log_image_item_fields({"aeskey": "SensitiveKeyName", "SensitiveKeyName": "private",
                                     "cdn_image_media": {"encryption_key_blob": "PRIVATE-BLOB"}})
        logged = self.logs[-1]
        self.assertIn('"cdn_image_media"', logged)
        self.assertIn('"encryption_key_blob"', logged)
        self.assertNotIn("PRIVATE-BLOB", logged)
        self.assertNotIn("SensitiveKeyName", logged)

    def test_receiver_keeps_raw_item_key_and_logs_one_field_tree(self):
        raw = "synthetic:original-item-key"
        cipher = crypto._aes_ecb_encrypt(self.PNG, hashlib.md5(raw.encode()).digest())
        store = Store(str(self.root / "store"))
        msg = {"message_id": "synthetic-image", "message_type": 1, "from_user_id": "owner",
               "context_token": "PRIVATE-CONTEXT", "item_list": [{"type": ilink.ITEM_IMAGE,
               "image_item": {"aeskey": raw, "media": {"full_url": "https://cdn.invalid/PRIVATE-URL"}}}]}
        with patch.object(media.urllib.request, "urlopen", return_value=Response(cipher)):
            self.assertTrue(serve.handle_incoming(store, msg, "owner"))
        inbox = load_json(Path(store.inbox_dir) / "synthetic-image.json")
        self.assertEqual(Path(inbox["media_paths"][0]).read_bytes(), self.PNG)
        self.assertEqual(len([x for x in self.logs if x.startswith("image_item_fields ")]), 1)
        self.assertNotIn(raw, "\n".join(self.logs))
        self.assertNotIn("PRIVATE-CONTEXT", "\n".join(self.logs))


if __name__ == "__main__":
    unittest.main(verbosity=2)
