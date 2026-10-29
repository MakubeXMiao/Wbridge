"""003 original-byte receive tests; all material and transports are synthetic."""
import io
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from Crypto.Cipher import AES
from PIL import Image
from wechat_bridge import crypto, media
from wechat_bridge.progress import OperationAborted


def deny(*args, **kwargs):
    raise AssertionError("synthetic original tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny


def png(color="red"):
    stream = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(stream, format="PNG")
    return stream.getvalue()


class Response(io.BytesIO):
    def __init__(self, body):
        super().__init__(body)
        self.status = 200
        self.headers = {"Content-Length": str(len(body)), "Content-Type": "application/octet-stream"}
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def geturl(self):
        return "https://synthetic.invalid/download"


class OriginalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-originals-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.target = self.base / "synthetic-original.part"
        self.key = b"0123456789abcdef"
        self.logs = []
        logger = patch.object(media, "log", self.logs.append)
        logger.start()
        self.addCleanup(logger.stop)

    def download(self, plain, expected_type=None, progress=None, **options):
        encrypted = crypto._aes_ecb_encrypt(plain, self.key)
        response = Response(encrypted)
        metadata = {}
        with patch.object(media.urllib.request, "urlopen", return_value=response) as transport:
            result = media.download_media({"encrypt_query_param": "synthetic-param", "aes_key": self.key.hex()},
                dest_path=str(self.target), expected_type=expected_type, preserve_original=True,
                metadata=metadata, progress=progress, **options)
        self.assertEqual(result, str(self.target))
        self.assertEqual(transport.call_count, 1)
        self.assertEqual(self.target.read_bytes(), plain)
        self.assertEqual(list(self.base.iterdir()), [self.target])
        return metadata, response

    def test_normal_png_is_byte_exact_and_readable(self):
        original = png()
        metadata, _ = self.download(original, expected_type="image")
        self.assertEqual(metadata, {"decryption_state": "content_verified",
            "preview_state": "readable", "detected_type": "PNG"})
        with Image.open(self.target) as actual:
            actual.load()
            self.assertEqual(actual.size, (4, 4))

    def test_protocol_unknown_bytes_are_saved_without_claiming_verification(self):
        metadata, _ = self.download(b"SYNTHETIC arbitrary executable-like or document bytes")
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")
        self.assertEqual(metadata["preview_state"], "unavailable")
        self.assertIsNone(metadata["detected_type"])

    def test_unknown_image_and_heic_are_originals_without_codec_promise(self):
        original = b"\x00\x00\x00\x18ftypheic" + b"SYNTHETIC-container-without-real-decoder"
        metadata, _ = self.download(original, expected_type="image")
        self.assertEqual(metadata["detected_type"], "HEIC")
        self.assertEqual(metadata["preview_state"], "unavailable")
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")

    def test_padding_only_file_is_not_content_verified(self):
        metadata, _ = self.download(b"X")
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")
        self.assertNotEqual(metadata["preview_state"], "readable")

    def test_empty_protocol_file_is_saved_as_zero_original_bytes(self):
        metadata, _ = self.download(b"")
        self.assertEqual(self.target.stat().st_size, 0)
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")

    def test_damaged_known_image_is_preserved_but_not_readable(self):
        metadata, _ = self.download(b"\x89PNG\r\n\x1a\nSYNTHETIC-broken", expected_type="image")
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")
        self.assertEqual(metadata["preview_state"], "invalid")

    def test_decoder_resource_rejection_is_not_mislabeled_invalid_content(self):
        # Inject the bounded decoder's documented result, not an actual OOM.
        with patch("wechat_bridge.image_validation._run", return_value=(False, "image_resource_limit")):
            metadata, _ = self.download(png(), expected_type="image")
        self.assertEqual(metadata["decryption_state"], "protocol_decoded")
        self.assertEqual(metadata["preview_state"], "resource_limited")

    def test_two_distinct_protocol_candidates_are_rejected_without_saving(self):
        encrypted = crypto._aes_ecb_encrypt(b"SYNTHETIC-body", self.key)
        alternate = None
        # Construct a real second PKCS7-valid AES candidate, rather than mock
        # the decryptor to claim a cryptographic ambiguity.
        for value in range(1, 10000):
            key = value.to_bytes(16, "big")
            raw = AES.new(key, AES.MODE_ECB).decrypt(encrypted)
            padding = raw[-1]
            if 1 <= padding <= 16 and raw[-padding:] == bytes([padding]) * padding:
                if raw[:-padding] != b"SYNTHETIC-body":
                    alternate = key
                    break
        self.assertIsNotNone(alternate)
        diagnostics = []
        with patch.object(media.urllib.request, "urlopen", return_value=Response(encrypted)):
            with self.assertRaises(ValueError):
                media.download_media({"encrypt_query_param": "synthetic", "aes_key": alternate.hex()},
                    self.key.hex(), str(self.target), preserve_original=True, diagnostics=diagnostics)
        self.assertFalse(self.target.exists())
        self.assertEqual(diagnostics[0]["reason"], "decrypt_ambiguous")

    def test_identical_protocol_keys_deduplicate_normal_original(self):
        original = png("blue")
        encrypted = crypto._aes_ecb_encrypt(original, self.key)
        metadata = {}
        with patch.object(media.urllib.request, "urlopen", return_value=Response(encrypted)):
            media.download_media({"encrypt_query_param": "synthetic", "aes_key": self.key.hex()},
                self.key, str(self.target), expected_type="image", preserve_original=True, metadata=metadata)
        self.assertEqual(self.target.read_bytes(), original)
        self.assertEqual(metadata["preview_state"], "readable")

    def test_progress_cancellation_is_not_swallowed_or_retried(self):
        def cancelled(event):
            raise OperationAborted("lease_lost")
        with patch.object(media.urllib.request, "urlopen") as transport:
            with self.assertRaises(OperationAborted) as error:
                media.download_media({"encrypt_query_param": "synthetic", "full_url": "https://synthetic.invalid/fallback",
                    "aes_key": self.key.hex()}, dest_path=str(self.target), preserve_original=True,
                    progress=cancelled)
        self.assertEqual(error.exception.code, "lease_lost")
        transport.assert_not_called()
        self.assertFalse(self.target.exists())

    def test_real_download_progress_is_monotone_and_bounds_each_read(self):
        events = []
        metadata, response = self.download(b"SYNTHETIC" * 20000, progress=events.append)
        downloaded = [e["completed"] for e in events if e["stage"] == "download"]
        self.assertGreater(len(downloaded), 2)
        self.assertEqual(downloaded, sorted(downloaded))
        self.assertTrue(all(0 < size <= 65536 for size in response.read_sizes))
        self.assertEqual(events[-1]["stage"], "persist")
        self.assertEqual(metadata["preview_state"], "unavailable")

    def test_fallback_source_progress_does_not_reset_completed_work(self):
        events = []
        responses = [Response(b"SYNTHETIC invalid cipher"),
            Response(crypto._aes_ecb_encrypt(b"SYNTHETIC-success", self.key))]
        with patch.object(media.urllib.request, "urlopen", side_effect=responses):
            media.download_media({"encrypt_query_param": "synthetic", "full_url": "https://synthetic.invalid/fallback",
                "aes_key": self.key.hex()}, dest_path=str(self.target), preserve_original=True, progress=events.append)
        downloaded = [e["completed"] for e in events if e["stage"] == "download"]
        self.assertEqual(downloaded, sorted(downloaded))
        self.assertEqual(self.target.read_bytes(), b"SYNTHETIC-success")

    def test_local_byte_boundary_is_enforced_before_persist(self):
        # Scaled policy seam: not a claim that this test allocated 100 MiB.
        with patch.object(media, "MAX_MEDIA_BYTES", 8):
            self.download(b"12345678")
            self.target.unlink()
            encrypted = crypto._aes_ecb_encrypt(b"123456789", self.key)
            with patch.object(media.urllib.request, "urlopen", return_value=Response(encrypted)):
                with self.assertRaises(ValueError):
                    media.download_media({"encrypt_query_param": "synthetic", "aes_key": self.key.hex()},
                        dest_path=str(self.target), preserve_original=True)
            self.assertFalse(self.target.exists())

    def test_original_stage_rejects_symlink_without_altering_target(self):
        other = self.base / "synthetic-other.bin"
        other.write_bytes(b"SYNTHETIC-protected")
        self.target.symlink_to(other)
        encrypted = crypto._aes_ecb_encrypt(b"SYNTHETIC-new", self.key)
        with patch.object(media.urllib.request, "urlopen", return_value=Response(encrypted)):
            with self.assertRaises(OSError):
                media.download_media({"encrypt_query_param": "synthetic", "aes_key": self.key.hex()},
                    dest_path=str(self.target), preserve_original=True)
        self.assertEqual(other.read_bytes(), b"SYNTHETIC-protected")

    def test_upload_cancellation_and_final_send_guard_stop_network(self):
        def cancelled(event):
            raise OperationAborted("cancelled")
        with patch.object(media, "_get_upload_target") as target:
            with self.assertRaises(OperationAborted):
                media.upload_media("https://synthetic.invalid", "synthetic-token", "synthetic-owner",
                    b"SYNTHETIC", media.ITEM_FILE, progress=cancelled)
            target.assert_not_called()
        self.target.write_bytes(b"SYNTHETIC")
        def final_guard(index, total):
            raise OperationAborted("epoch_mismatch")
        with patch.object(media, "upload_media", return_value=("synthetic-param", "synthetic-key", 16)), \
                patch.object(media, "http_json") as send:
            with self.assertRaises(OperationAborted):
                media.send_media_message("https://synthetic.invalid", "synthetic-token", "synthetic-owner",
                    "synthetic-context", str(self.target), media.ITEM_FILE, before_send=final_guard)
            send.assert_not_called()

    def test_file_reply_uses_explicit_original_display_name(self):
        self.target.write_bytes(b"SYNTHETIC")
        with patch.object(media, "upload_media", return_value=("synthetic-param", "synthetic-key", 16)), \
                patch.object(media, "http_json", return_value={"ret": 0}) as send:
            response = media.send_media_message("https://synthetic.invalid", "synthetic-token", "synthetic-owner",
                "synthetic-context", str(self.target), media.ITEM_FILE,
                file_name="SYNTHETIC-original-name.pdf", client_key="synthetic-stable-unit")
        payload = send.call_args.args[3]
        self.assertEqual(payload["msg"]["item_list"][0]["file_item"]["file_name"], "SYNTHETIC-original-name.pdf")
        self.assertEqual(response["ret"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
