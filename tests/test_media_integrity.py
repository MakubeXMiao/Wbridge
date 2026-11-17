"""Comparable-body integrity on generic media and real receive/catalog paths.

All bodies and accounts are synthetic. No sockets, live samples or model calls.
"""
from contextlib import ExitStack
from email.message import Message
import http.client
import io
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
import test_attachment_runtime as fixture
from wechat_bridge import attachment_runtime as runtime, crypto, media, serve
from wechat_bridge.store import load_json

KEY = b"0123456789abcdef"
PLAIN = b"A" * 15 + b"\x01" + b"B" * 16
CIPHER = crypto._aes_ecb_encrypt(PLAIN, KEY)
URL = "https://synthetic.invalid/media?private=SYNTHETIC_ONLY"
OWNER = fixture.OWNER


def deny(*args, **kwargs):
    raise AssertionError("integrity tests forbid real sockets")

socket.create_connection = socket.getaddrinfo = deny
socket.socket.connect = socket.socket.connect_ex = deny
socket.socket.bind = socket.socket.listen = deny


class Response(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(body)
        self.status = 200
        self.headers = headers if headers is not None else {
            "Content-Length": str(len(CIPHER)), "Content-Encoding": "identity"}

    def geturl(self):
        return URL


class MemorySocket:
    def __init__(self, data):
        self.data = data

    def makefile(self, mode):
        return io.BytesIO(self.data)


def parsed(body):
    raw = b"HTTP/1.1 200 OK\r\nContent-Length: 48\r\nContent-Encoding: identity\r\nConnection: close\r\n\r\n" + body
    return parsed_wire(raw)


def parsed_wire(raw):
    response = http.client.HTTPResponse(MemorySocket(raw))
    response.begin()
    return response


def parsed_chunked(complete):
    body = (b"30\r\n" + CIPHER + b"\r\n0\r\n\r\n" if complete else
            b"30\r\n" + CIPHER[:16])
    return parsed_wire(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + body)


def parsed_eof():
    return parsed_wire(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n" + CIPHER)


class CaptureSpy:
    def __init__(self):
        self.calls = []

    def capture(self, *args):
        self.calls.append(args)


class GenericBodyIntegrity(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="synthetic-integrity-")
        self.addCleanup(temp.cleanup)
        self.destination = Path(temp.name) / "original.bin"
        self.logs = []
        logger = patch.object(media, "log", self.logs.append)
        logger.start()
        self.addCleanup(logger.stop)

    def tearDown(self):
        text = "\n".join(self.logs)
        for private in (URL, "synthetic.invalid", "SYNTHETIC_ONLY", KEY.decode(), KEY.hex()):
            self.assertNotIn(private, text)

    def records(self):
        return [json.loads(row.split(" ", 1)[1]) for row in self.logs
                if row.startswith("media_receive ")]

    def download(self, responses, **kwargs):
        diagnostics = kwargs.pop("diagnostics", [])
        sources = kwargs.pop("sources", {"full_url": URL, "aes_key": KEY})
        with patch.object(media.urllib.request, "urlopen", side_effect=responses) as opened:
            result = media.download_media(sources, KEY, self.destination,
                expected_type=None, diagnostics=diagnostics, **kwargs)
        return result, opened, diagnostics

    def test_known_short_valid_padding_prefix_never_reaches_decrypt_or_file(self):
        for progress in (False, True):
            for preserve in (False, True):
                with self.subTest(progress=progress, preserve_original=preserve):
                    capture, diagnostics, metadata, events = CaptureSpy(), [], {}, []
                    with ExitStack() as stack:
                        decrypt = stack.enter_context(patch.object(media, "_decrypt_media", wraps=media._decrypt_media))
                        original = stack.enter_context(patch.object(media, "_decrypt_original", wraps=media._decrypt_original))
                        opened = stack.enter_context(patch.object(media.urllib.request, "urlopen", side_effect=[parsed(CIPHER[:16])]))
                        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                            media.download_media({"full_url": URL, "aes_key": KEY}, KEY,
                                self.destination, expected_type=None, diagnostics=diagnostics,
                                preserve_original=preserve, metadata=metadata,
                                failure_capture=capture, progress=events.append if progress else None)
                    self.assertEqual(opened.call_count, 1)
                    decrypt.assert_not_called()
                    original.assert_not_called()
                    self.assertFalse(self.destination.exists())
                    self.assertEqual(metadata, {})
                    self.assertEqual(capture.calls, [])
                    self.assertEqual(diagnostics, [{"source": "full_url", "outcome": "failed",
                        "downloaded_bytes": 16, "reason": "http_body_incomplete"}])
                    self.assertFalse(self.records()[-1]["retry_planned"])

    def test_complete_generic_original_is_byte_exact_in_both_read_paths(self):
        for progress in (False, True):
            for preserve in (False, True):
                with self.subTest(progress=progress, preserve_original=preserve):
                    self.destination.unlink(missing_ok=True)
                    capture, metadata = CaptureSpy(), {}
                    _, opened, diagnostics = self.download([parsed(CIPHER)],
                        preserve_original=preserve, metadata=metadata, failure_capture=capture,
                        progress=(lambda event: None) if progress else None)
                    self.assertEqual(self.destination.read_bytes(), PLAIN)
                    self.assertEqual(opened.call_count, 1)
                    self.assertEqual(opened.call_args.kwargs["timeout"], 60 if preserve else 120)
                    self.assertEqual(diagnostics[0]["reason"], "ok")
                    self.assertEqual(diagnostics[0]["downloaded_bytes"], 48)
                    self.assertEqual(capture.calls, [])
                    if preserve:
                        self.assertEqual(metadata["decryption_state"], "protocol_decoded")

    def test_known_overlong_generic_body_is_rejected_without_new_attempt_or_capture(self):
        # Defensive read-API boundary only. This Response can return bytes past
        # its declared CL, whereas real HTTPResponse clamps to the CL frame.
        # The case is not evidence that the real client returns extra wire data.
        capture, diagnostics = CaptureSpy(), []
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(CIPHER,
                {"Content-Length": "16", "Content-Encoding": "identity"})]) as opened, \
                patch.object(media, "_decrypt_media", wraps=media._decrypt_media) as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL, "aes_key": KEY}, KEY, self.destination,
                    diagnostics=diagnostics, failure_capture=capture)
        self.assertEqual(opened.call_count, 1)
        decrypt.assert_not_called()
        self.assertEqual(diagnostics[0]["reason"], "http_body_read_failed")
        self.assertFalse(self.destination.exists())
        self.assertEqual(capture.calls, [])

    def test_real_http_client_never_returns_extra_bytes_beyond_content_length(self):
        raw = b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\n" + CIPHER
        response = parsed_wire(raw)
        self.assertEqual(response.read(65536), CIPHER[:16])
        self.assertEqual(response.read(65536), b"")
        # This is a complete 16-byte HTTP frame. A known 48-byte source body
        # cannot be inferred from bytes the application did not receive.

    def test_real_chunked_truncation_is_rejected_by_parser_and_never_decrypted(self):
        # The advertised chunk is 48 bytes, but only 16 bytes and no chunk
        # terminator arrive. Derive only what this real parser exposes; its
        # partial may omit bytes consumed inside the chunk parser.
        with self.assertRaises(http.client.IncompleteRead) as observed:
            parsed_chunked(False).read(65536)
        exposed = observed.exception.partial
        self.assertIs(type(exposed), bytes)
        self.assertLessEqual(len(exposed), 16)
        for progress in (False, True):
            for preserve in (False, True):
                with self.subTest(progress=progress, preserve_original=preserve):
                    diagnostics, capture, metadata = [], CaptureSpy(), {}
                    with ExitStack() as stack:
                        decrypt = stack.enter_context(patch.object(media, "_decrypt_media", wraps=media._decrypt_media))
                        original = stack.enter_context(patch.object(media, "_decrypt_original", wraps=media._decrypt_original))
                        opened = stack.enter_context(patch.object(media.urllib.request, "urlopen",
                            side_effect=[parsed_chunked(False)]))
                        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                            media.download_media({"full_url": URL, "aes_key": KEY}, KEY,
                                self.destination, diagnostics=diagnostics, metadata=metadata,
                                preserve_original=preserve, failure_capture=capture,
                                progress=(lambda event: None) if progress else None)
                    self.assertEqual(opened.call_count, 1)
                    decrypt.assert_not_called()
                    original.assert_not_called()
                    self.assertFalse(self.destination.exists())
                    self.assertEqual(metadata, {})
                    self.assertEqual(capture.calls, [])
                    self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
                    self.assertEqual(diagnostics[0]["downloaded_bytes"], len(exposed))
                    self.assertEqual(self.records()[-1]["attempted"], [])
                    self.assertFalse(self.records()[-1]["retry_planned"])

    def test_real_complete_chunked_and_eof_frames_keep_original_generic_bytes(self):
        for frame in ("chunked", "eof"):
            for progress in (False, True):
                with self.subTest(frame=frame, progress=progress):
                    self.destination.unlink(missing_ok=True)
                    response = parsed_chunked(True) if frame == "chunked" else parsed_eof()
                    self.assertIsNone(media._comparable_body_length(response))
                    _, opened, diagnostics = self.download([response],
                        progress=(lambda event: None) if progress else None)
                    self.assertEqual(opened.call_count, 1)
                    self.assertEqual(self.destination.read_bytes(), PLAIN)
                    self.assertEqual(diagnostics[0]["reason"], "ok")
                    self.assertEqual(diagnostics[0]["downloaded_bytes"], 48)

    def test_unknown_or_noncomparable_metadata_keeps_complete_generic_compatibility(self):
        duplicate = Message()
        duplicate["Content-Length"] = "64"
        duplicate["Content-Length"] = "80"
        cases = [{}, {"Content-Length": "64", "Transfer-Encoding": "chunked"},
            {"Content-Length": "64", "Content-Encoding": "gzip"},
            {"Content-Length": "64", "Content-Encoding": "identity; invalid"},
            duplicate, {"Content-Length": "1, 2"}, {"Content-Length": "bad"}]
        for progress in (False, True):
            for headers in cases:
                with self.subTest(progress=progress, header_names=list(headers)):
                    self.destination.unlink(missing_ok=True)
                    _, opened, diagnostics = self.download([Response(CIPHER, headers)],
                        progress=(lambda event: None) if progress else None)
                    self.assertEqual(opened.call_count, 1)
                    self.assertEqual(self.destination.read_bytes(), PLAIN)
                    self.assertEqual(diagnostics[0]["reason"], "ok")

    def test_identity_optional_whitespace_remains_comparable(self):
        diagnostics = []
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(CIPHER[:16],
                {"Content-Length": " \t48 \t", "Content-Encoding": " identity\t"})]) as opened:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL, "aes_key": KEY}, KEY, self.destination,
                    diagnostics=diagnostics)
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertFalse(self.destination.exists())

    def test_two_generic_sources_each_get_one_attempt_and_never_publish_short_body(self):
        diagnostics, capture = [], CaptureSpy()
        with patch.object(media.urllib.request, "urlopen", side_effect=[Response(CIPHER[:16]),
                Response(CIPHER[:16])]) as opened, \
                patch.object(media, "_decrypt_media", wraps=media._decrypt_media) as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"encrypt_query_param": "synthetic-param", "full_url": URL,
                    "aes_key": KEY}, KEY, self.destination, diagnostics=diagnostics,
                    failure_capture=capture, progress=lambda event: None)
        self.assertEqual(opened.call_count, 2)
        decrypt.assert_not_called()
        self.assertEqual([d["source"] for d in diagnostics], ["param", "full_url"])
        self.assertEqual([d["downloaded_bytes"] for d in diagnostics], [16, 16])
        self.assertEqual([d["reason"] for d in diagnostics], ["http_body_incomplete"] * 2)
        records = self.records()
        self.assertEqual([r["source_attempt"] for r in records], [1, 2])
        self.assertEqual([r["request_attempt"] for r in records], [1, 1])
        self.assertTrue(all(not r["retry_planned"] for r in records))
        self.assertEqual(capture.calls, [])
        self.assertFalse(self.destination.exists())

    def test_short_first_source_can_use_existing_second_source_without_extra_get(self):
        diagnostics, capture, events = [], CaptureSpy(), []
        _, opened, _ = self.download([Response(CIPHER[:16]), Response(CIPHER)],
            sources={"encrypt_query_param": "synthetic-param", "full_url": URL, "aes_key": KEY},
            diagnostics=diagnostics, failure_capture=capture, progress=events.append)
        self.assertEqual(opened.call_count, 2)
        self.assertEqual(self.destination.read_bytes(), PLAIN)
        self.assertEqual([(d["source"], d["reason"]) for d in diagnostics],
                         [("param", "http_body_incomplete"), ("full_url", "ok")])
        self.assertEqual(capture.calls, [])
        completed = [e["completed"] for e in events if e["stage"] == "download"]
        self.assertEqual(completed, sorted(completed))
        self.assertEqual(completed[-1], 64)


class RealReceiveCatalogIntegrity(unittest.TestCase):
    def test_real_incomplete_chunked_file_and_video_never_publish_ready(self):
        for kind in ("file", "video"):
            for complete in (False, True):
                with self.subTest(kind=kind, complete=complete):
                    f = fixture.RuntimeTests(methodName="runTest")
                    f.setUp()
                    try:
                        mid = "chunked-%s-%s" % (kind, "complete" if complete else "broken")
                        item_type, subkey = {"file": (media.ITEM_FILE, "file_item"),
                            "video": (media.ITEM_VIDEO, "video_item")}[kind]
                        msg = {"message_id": mid, "from_user_id": OWNER, "message_type": 1,
                            "context_token": "synthetic-context", "item_list": [{"type": item_type,
                            subkey: {"media": {"full_url": URL, "aes_key": KEY.hex(), "encrypt_type": 1},
                                     "file_name": "synthetic.bin", "len": "32"}}]}
                        with ExitStack() as stack:
                            original = stack.enter_context(patch.object(media, "_decrypt_original", wraps=media._decrypt_original))
                            opened = stack.enter_context(patch.object(media.urllib.request, "urlopen",
                                side_effect=lambda *args, **kw: parsed_chunked(complete)))
                            self.assertTrue(serve.handle_incoming(f.store, msg, OWNER))
                        row = load_json(str(f.base / "inbox" / (mid + ".json")))
                        item = runtime.catalog(f.store).query(f.scope, {}, 20)["items"][0]
                        self.assertEqual(opened.call_count, 1)
                        if complete:
                            self.assertEqual(original.call_count, 1)
                            self.assertEqual(item["status"], "ready")
                            self.assertEqual(item["size"], 32)
                            self.assertEqual(Path(row["media_paths"][0]).read_bytes(), PLAIN)
                        else:
                            original.assert_not_called()
                            self.assertEqual(item["status"], "failed")
                            self.assertEqual(row.get("media_paths", []), [])
                            self.assertTrue(row["media_failed"])
                            self.assertEqual(row["attachment_refs"][0]["status"], "failed")
                            self.assertEqual(list((f.base / "attachments/objects").iterdir()), [])
                            self.assertEqual(list((f.base / "attachments/staging").iterdir()), [])
                            self.assertIsNone(item["size"])
                            self.assertIsNone(item["sha256"])
                    finally:
                        f.doCleanups()

    def test_file_video_voice_prefix_is_failed_without_ready_object_or_media_path(self):
        for kind in ("file", "video", "voice"):
            for short in (False, True):
                with self.subTest(kind=kind, short=short):
                    f = fixture.RuntimeTests(methodName="runTest")
                    f.setUp()
                    try:
                        mid = "integrity-%s-%s" % (kind, "short" if short else "complete")
                        item_type, subkey = {"file": (media.ITEM_FILE, "file_item"),
                            "video": (media.ITEM_VIDEO, "video_item"),
                            "voice": (serve.ITEM_VOICE, "voice_item")}[kind]
                        msg = {"message_id": mid, "from_user_id": OWNER, "message_type": 1,
                            "context_token": "synthetic-context", "item_list": [{"type": item_type,
                            subkey: {"media": {"full_url": URL, "aes_key": KEY.hex(), "encrypt_type": 1},
                                     "file_name": "synthetic.bin", "len": "32"}}]}
                        self.assertIsNone(getattr(f.store, "failure_capture", None))
                        with patch.object(media.urllib.request, "urlopen",
                                side_effect=lambda *args, **kw: parsed(CIPHER[:16] if short else CIPHER)) as opened:
                            self.assertTrue(serve.handle_incoming(f.store, msg, OWNER))
                        row = load_json(str(f.base / "inbox" / (mid + ".json")))
                        item = runtime.catalog(f.store).query(f.scope, {}, 20)["items"][0]
                        self.assertEqual(opened.call_count, 1)
                        if short:
                            self.assertEqual(item["status"], "failed")
                            self.assertEqual(row.get("media_paths", []), [])
                            self.assertTrue(row["media_failed"])
                            self.assertEqual(row["attachment_refs"][0]["status"], "failed")
                            self.assertEqual(list((f.base / "attachments/objects").iterdir()), [])
                            self.assertEqual(list((f.base / "attachments/staging").iterdir()), [])
                            self.assertIsNone(item["size"])
                            self.assertIsNone(item["sha256"])
                        else:
                            self.assertEqual(item["status"], "ready")
                            self.assertEqual(item["size"], 32)
                            self.assertEqual(item["decryption_state"], "protocol_decoded")
                            self.assertEqual(len(row["media_paths"]), 1)
                            self.assertEqual(Path(row["media_paths"][0]).read_bytes(), PLAIN)
                    finally:
                        f.doCleanups()


if __name__ == "__main__":
    unittest.main()
