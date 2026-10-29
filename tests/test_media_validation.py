"""012 image acceptance, all fixtures synthetic and HTTP fully mocked."""
import io
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from PIL import Image
from Crypto.Cipher import AES
from wechat_bridge import crypto, image_validation, media


def picture(fmt="PNG", color="red"):
    stream = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(stream, format=fmt)
    return stream.getvalue()


class Response:
    def __init__(self, body): self.body = body
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self, size): return self.body[:size]


class MediaValidation(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="wbr012-image-")
        self.addCleanup(self.temporary.cleanup)
        self.target = Path(self.temporary.name, "result.png")
        self.logs = []
        logger = patch.object(media, "log", self.logs.append)
        logger.start(); self.addCleanup(logger.stop)
        for target, name in ((socket.socket, "connect"), (socket.socket, "connect_ex"),
                             (socket, "create_connection"), (socket, "getaddrinfo")):
            guard = patch.object(target, name, side_effect=AssertionError("real network forbidden"))
            guard.start(); self.addCleanup(guard.stop)

    def download(self, bodies, key=None, diagnostics=None, capture=None):
        params = {"full_url": "https://synthetic.invalid/PRIVATE-URL"}
        if len(bodies) == 2: params["encrypt_query_param"] = "PRIVATE-PARAM"
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(x) for x in bodies]):
            return media.download_media(params, key, self.target, expected_type="image",
                                        diagnostics=diagnostics, failure_capture=capture)

    def test_actual_supported_plaintext_images(self):
        for fmt in ("JPEG", "PNG", "GIF", "WEBP"):
            with self.subTest(fmt=fmt):
                body = picture(fmt)
                self.download([body])
                self.assertEqual(self.target.read_bytes(), body)
                with Image.open(self.target) as image: image.load()

    def test_bmp_validator_preserves_format_without_expanding_wire_magic(self):
        self.assertTrue(image_validation.validate_image(picture("BMP")))
        self.assertEqual(crypto.image_magic(picture("BMP")), "other")

    def test_fake_magic_and_wrong_key_prefixed_iv_cannot_replace_old_target(self):
        key, wrong = b"0123456789abcdef", b"wrong-key-16byte!"
        plain = picture()
        pad = 16 - len(plain) % 16
        for prefix in (b"\x89PNG\r\n\x1a\n" + b"iv-bytes", b"\xff\xd8\xff" + b"Q" * 13):
            body = prefix + AES.new(key, AES.MODE_CBC, iv=prefix).encrypt(plain + bytes([pad]) * pad)
            self.target.write_bytes(b"OLD")
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download([body], wrong)
            self.assertEqual(self.target.read_bytes(), b"OLD")
        for body in (b"\xff\xd8\xffgarbage", b"\x89PNG\r\n\x1a\ngarbage", b"GIF89agarbage", b"RIFF....WEBPgarbage"):
            with self.assertRaises(ValueError): self.download([body])
            self.assertEqual(self.target.read_bytes(), b"OLD")

    def test_verify_passes_but_load_fails_jpeg_is_rejected(self):
        body = picture("JPEG")[:-2]
        with Image.open(io.BytesIO(body)) as image: image.verify()
        with self.assertRaises(OSError):
            with Image.open(io.BytesIO(body)) as image: image.load()
        with self.assertRaises(ValueError): self.download([body])
        self.assertFalse(self.target.exists())

    def test_decrypt_precedes_plaintext_fallback_for_magic_iv(self):
        key, plain = b"0123456789abcdef", picture()
        iv = b"\x89PNG\r\n\x1a\n" + b"iv-bytes"
        pad = 16 - len(plain) % 16
        body = iv + AES.new(key, AES.MODE_CBC, iv).encrypt(plain + bytes([pad]) * pad)
        self.download([body], key)
        self.assertEqual(self.target.read_bytes(), plain)
        self.assertIn("CBC-prefix-IV", self.logs[-2])

    def test_valid_padding_and_magic_but_invalid_image_is_not_a_candidate(self):
        key = b"0123456789abcdef"
        body = crypto._aes_ecb_encrypt(b"\x89PNG\r\n\x1a\ngarbage", key)
        with self.assertRaises(ValueError): self.download([body], key)
        self.assertFalse(self.target.exists())

    def test_two_sources_and_fixed_collector_preserve_order(self):
        diagnostics = []
        self.download([b"\xff\xd8\xffgarbage", picture()], diagnostics=diagnostics)
        self.assertEqual([(x["source"], x["outcome"], x["reason"]) for x in diagnostics],
                         [("param", "failed", "image_invalid"), ("full_url", "success", "ok")])
        self.assertTrue(all(set(x) == {"source", "outcome", "reason", "downloaded_bytes"} for x in diagnostics))

    def test_write_failure_changes_success_collector_and_keeps_target(self):
        diagnostics = []
        self.target.write_bytes(b"OLD")
        with patch.object(media.os, "replace", side_effect=OSError("PRIVATE-PATH")):
            with self.assertRaises(OSError): self.download([picture()], diagnostics=diagnostics)
        self.assertEqual(self.target.read_bytes(), b"OLD")
        self.assertEqual(diagnostics[-1]["reason"], "write_failed")
        self.assertEqual(diagnostics[-1]["outcome"], "failed")
        self.assertNotIn("PRIVATE-PATH", "".join(self.logs))

    def test_ambiguity_never_uses_raw_fallback(self):
        with patch.object(crypto, "try_decrypt_variants", return_value=(None, "ambiguous-image")):
            with self.assertRaises(ValueError): self.download([picture()])
        self.assertFalse(self.target.exists())

    def test_oversized_geometry_and_excess_frames_are_bounded(self):
        def chunk(kind, data):
            return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff)
        png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 5000, 4000, 8, 2, 0, 0, 0))
        png += chunk(b"IDAT", zlib.compress(b"")) + chunk(b"IEND", b"")
        budget = image_validation.ImageValidationBudget()
        self.assertFalse(budget(png)); self.assertEqual(budget.reason, "image_resource_limit")
        stream = io.BytesIO()
        frames = [Image.new("RGB", (2, 2), (i, 0, 0)) for i in range(33)]
        frames[0].save(stream, format="GIF", save_all=True, append_images=frames[1:], optimize=False)
        self.assertFalse(image_validation.validate_image(stream.getvalue()))

    def test_wall_timeout_kills_and_reaps_worker(self):
        budget = image_validation.ImageValidationBudget(); budget.remaining = 0.15
        started = time.monotonic()
        with patch.object(image_validation, "_WORKER", "import time; time.sleep(30)"):
            self.assertFalse(budget(picture()))
        self.assertEqual(budget.reason, "image_validation_timeout")
        self.assertLess(time.monotonic() - started, 1.5)

    def test_capture_failure_does_not_change_source_fallback(self):
        class BrokenCapture:
            def capture(self, *args): raise OSError("PRIVATE-EVIDENCE")
        self.download([b"bad", picture()], capture=BrokenCapture())
        self.assertNotIn("PRIVATE-EVIDENCE", "".join(self.logs))

    @unittest.skipUnless(sys.platform == "darwin", "Darwin watchdog; Linux uses RLIMIT_AS")
    def test_failed_or_invalid_rss_probe_stops_live_child(self):
        for code, output in ((1, b""), (0, b""), (0, b"not-a-number")):
            with self.subTest(code=code, output_class="synthetic"), \
                    patch.object(image_validation, "_WORKER", "import time; time.sleep(30)"), \
                    patch.object(image_validation.subprocess, "run", return_value=subprocess.CompletedProcess([], code, output)):
                budget = image_validation.ImageValidationBudget()
                started = time.monotonic()
                self.assertFalse(budget(picture()))
                self.assertEqual(budget.reason, "image_resource_limit")
                self.assertLess(time.monotonic() - started, 1.5)


if __name__ == "__main__": unittest.main(verbosity=2)
