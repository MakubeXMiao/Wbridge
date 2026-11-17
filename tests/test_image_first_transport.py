"""Image-first transport recovery on synthetic bodies; no sockets or samples.

These are implementation self-tests. They do not establish platform delivery or
phone acceptance, and the optional capture spy never writes sample files.
"""
import http.client
import io
import json
from email.message import Message
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from PIL import Image
from wechat_bridge import crypto, media
from wechat_bridge.progress import OperationAborted


KEY = b"0123456789abcdef"
PRIVATE = "SYNTHETIC_IMAGE_TRANSPORT_SECRET"
URL = "https://image.invalid/signed?private=" + PRIVATE
_image = io.BytesIO()
Image.new("RGB", (3, 2), "green").save(_image, format="PNG")
PLAIN = _image.getvalue()
CIPHER = crypto._aes_ecb_encrypt(PLAIN, KEY)
SHORT = CIPHER[:-16]


class Response(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(body)
        self.status = 200
        self.headers = ({"Content-Length": str(len(body)),
                         "Content-Type": "application/octet-stream"}
                        if headers is None else headers)

    def getcode(self):
        return self.status

    def geturl(self):
        return URL


def short_response(body=SHORT):
    return Response(body, {"Content-Length": str(len(CIPHER))})


class InterruptedResponse(Response):
    """A consumed chunk followed by a partial chunk framing failure."""
    def __init__(self):
        super().__init__(b"", {"Content-Length": "96"})
        self.read_count = 0

    def read(self, size=-1):
        self.read_count += 1
        if self.read_count == 1:
            return b"a" * 16
        raise http.client.IncompleteRead(b"b" * 7, 73)


class LegacyResponse:
    """The old no-metadata contract does not consume its read result."""
    def __init__(self, body):
        self.body = body
        self.read_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size=-1):
        self.read_sizes.append(size)
        return self.body[:size]


class CaptureSpy:
    def __init__(self):
        self.calls = []

    def capture(self, *args):
        self.calls.append(args)
        return {"status": "synthetic_spy_no_sample_files"}


class ImageFirstTransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="image-first-transport-")
        self.addCleanup(self.temp.cleanup)
        self.destination = Path(self.temp.name) / "image.png"
        self.logs, self.network_attempts = [], []
        self.patch(media, "log", self.logs.append)
        for target, name in ((socket, "create_connection"), (socket, "getaddrinfo"),
                             (socket.socket, "connect"), (socket.socket, "connect_ex"),
                             (socket.socket, "bind"), (socket.socket, "listen")):
            self.patch(target, name, self.deny_network)
        self.patch(media.urllib.request, "urlopen", self.deny_network)

    def patch(self, obj, name, value):
        item = patch.object(obj, name, value)
        result = item.start()
        self.addCleanup(item.stop)
        return result

    def deny_network(self, *args, **kwargs):
        self.network_attempts.append("blocked")
        raise AssertionError("real network forbidden in image transport self-test")

    def tearDown(self):
        self.assertEqual(self.network_attempts, [])
        text = "\n".join(self.logs)
        for secret in (PRIVATE, URL, "image.invalid", KEY.decode(), KEY.hex()):
            self.assertNotIn(secret, text)

    def records(self):
        return [json.loads(line.split(" ", 1)[1]) for line in self.logs
                if line.startswith("media_receive ")]

    def download(self, responses, diagnostics, **options):
        sources = options.pop("sources", {"full_url": URL, "aes_key": KEY})
        with patch.object(media.urllib.request, "urlopen", side_effect=responses) as opened:
            media.download_media(sources, KEY, self.destination,
                                 expected_type=options.pop("expected_type", "image"),
                                 diagnostics=diagnostics, **options)
        return opened

    def test_real_http_parser_ows_short_valid_cipher_recovers_before_aes(self):
        class MemorySocket:
            def __init__(self, raw):
                self.raw = raw
            def makefile(self, mode):
                return io.BytesIO(self.raw)
        def parsed(body, declared, tail):
            raw = ("HTTP/1.1 200 OK\r\nContent-Length: %d%s\r\n"
                   "Connection: close\r\n\r\n" % (declared, tail)).encode("ascii") + body
            response = http.client.HTTPResponse(MemorySocket(raw))
            response.begin()
            return response
        for tail in (" ", "\t", " \t"):
            for use_progress in (False, True):
                with self.subTest(tail=repr(tail), progress=use_progress):
                    # The short transport body is itself valid encrypted PNG.
                    # A missing integrity gate would save it on attempt one.
                    short = parsed(CIPHER, len(CIPHER) + 16, tail)
                    self.assertTrue(short.headers.get("Content-Length").endswith(tail))
                    full = parsed(CIPHER, len(CIPHER), tail)
                    diagnostics, progress = [], []
                    with patch.object(media, "_decrypt_media", wraps=media._decrypt_media) as decrypt:
                        opened = self.download([short, full], diagnostics,
                            progress=progress.append if use_progress else None)
                    self.assertEqual(opened.call_count, 2)
                    self.assertEqual(decrypt.call_count, 1)
                    self.assertEqual(self.destination.read_bytes(), PLAIN)
                    self.assertEqual(len(diagnostics), 1)
                    self.assertEqual(diagnostics[0]["outcome"], "success")
                    recent = self.records()[-2:]
                    self.assertEqual(recent[0]["failure_reason"], "http_body_incomplete")
                    self.assertEqual(recent[0]["content_length"], len(CIPHER) + 16)
                    self.assertIs(recent[0]["body_length_matches_content_length"], False)
                    self.assertEqual(recent[1]["outcome"], "success")

    def test_optional_header_whitespace_is_still_comparable(self):
        diagnostics = []
        headers = {"Content-Length": " \t" + str(len(CIPHER)) + " \t", "Content-Encoding": " identity\t"}
        responses = [Response(SHORT, headers), Response(SHORT, headers)]
        with patch.object(media, "_decrypt_media") as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download(responses, diagnostics)
        decrypt.assert_not_called()
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertEqual(len(self.records()), 2)
        self.assertTrue(all(row["body_length_matches_content_length"] is False for row in self.records()))

    def test_fresh_same_source_get_recovers_short_body_before_decrypt(self):
        diagnostics, capture = [], CaptureSpy()
        with patch.object(media, "_decrypt_media", wraps=media._decrypt_media) as decrypt, \
                patch.object(media, "ImageValidationBudget", wraps=media.ImageValidationBudget) as validator:
            opened = self.download([short_response(), Response(CIPHER)], diagnostics,
                                   failure_capture=capture)
        self.assertEqual(self.destination.read_bytes(), PLAIN)
        self.assertEqual(opened.call_count, 2)
        self.assertEqual([x.args[0].get_method() for x in opened.call_args_list], ["GET", "GET"])
        self.assertEqual(opened.call_args_list[0].args[0].full_url,
                         opened.call_args_list[1].args[0].full_url)
        self.assertEqual(decrypt.call_count, 1)
        self.assertEqual(validator.call_count, 1)
        self.assertEqual(capture.calls, [])
        self.assertEqual(diagnostics, [{"source": "full_url", "outcome": "success",
                                       "downloaded_bytes": len(CIPHER), "reason": "ok"}])
        first, second = self.records()
        self.assertEqual(first["failure_reason"], "http_body_incomplete")
        self.assertEqual(first["attempted"], [])
        self.assertTrue(first["retry_planned"])
        self.assertEqual([first["request_attempt"], second["request_attempt"]], [1, 2])
        self.assertEqual([first["source_attempt"], second["source_attempt"]], [1, 1])
        self.assertEqual(first["transfer_id"], second["transfer_id"])
        self.assertGreater(opened.call_args_list[1].kwargs["timeout"], 0)
        self.assertLessEqual(opened.call_args_list[1].kwargs["timeout"], 120)

    def test_exhausted_same_source_collects_and_captures_only_final_failure(self):
        diagnostics, capture = [], CaptureSpy()
        with patch.object(media.urllib.request, "urlopen",
                          side_effect=[short_response(), short_response(CIPHER[:-1])]) as opened, \
                patch.object(media, "_decrypt_media") as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type="image", diagnostics=diagnostics,
                                     failure_capture=capture)
        self.assertEqual(opened.call_count, 2)
        decrypt.assert_not_called()
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertEqual(diagnostics[0]["downloaded_bytes"], len(CIPHER) - 1)
        self.assertEqual(len(capture.calls), 1)
        self.assertEqual(capture.calls[0][1:3], ("full_url", CIPHER[:-1]))
        self.assertEqual(capture.calls[0][-1], "http_body_incomplete")
        records = self.records()
        self.assertEqual(len({r["transfer_id"] for r in records}), 1)
        self.assertFalse(records[-1]["retry_planned"])

    def test_exhausted_param_falls_back_without_duplicate_source_summary(self):
        diagnostics = []
        opened = self.download([short_response(), short_response(), Response(CIPHER)], diagnostics,
                               sources={"encrypt_query_param": PRIVATE, "full_url": URL})
        self.assertEqual(opened.call_count, 3)
        urls = [c.args[0].full_url for c in opened.call_args_list]
        self.assertEqual(urls[0], urls[1])
        self.assertNotEqual(urls[1], urls[2])
        self.assertEqual(self.destination.read_bytes(), PLAIN)
        self.assertEqual([(d["source"], d["outcome"]) for d in diagnostics],
                         [("param", "failed"), ("full_url", "success")])
        self.assertEqual([r["source_attempt"] for r in self.records()], [1, 1, 2])
        self.assertEqual(len({r["transfer_id"] for r in self.records()}), 1)

    def test_raw_headers_skip_noncomparable_coding_framing_and_duplicate_lengths(self):
        duplicate = Message()
        duplicate["Content-Length"] = str(len(CIPHER) + 16)
        duplicate["Content-Length"] = str(len(CIPHER) + 32)
        ordinary = Message()
        ordinary["Content-Length"] = str(len(CIPHER))
        ordinary["Content-Encoding"] = "identity"
        cases = [ordinary, duplicate, {},
                 {"Content-Length": str(len(CIPHER) + 16), "Transfer-Encoding": "chunked"},
                 {"Content-Length": str(len(CIPHER) + 16), "Content-Encoding": "gzip"},
                 {"Content-Length": str(len(CIPHER) + 16), "Content-Encoding": "identity; bogus"},
                 {"Content-Length": "1, 2"},
                 {"Content-Length": "1\r\nPRIVATE: " + PRIVATE}]
        for headers in cases:
            with self.subTest(headers_type=type(headers).__name__, fields=len(headers)):
                diagnostics = []
                opened = self.download([Response(CIPHER, headers)], diagnostics)
                self.assertEqual(opened.call_count, 1)
                self.assertEqual(self.destination.read_bytes(), PLAIN)
                self.assertEqual(diagnostics[0]["reason"], "ok")
                self.assertEqual(diagnostics[0]["downloaded_bytes"], len(CIPHER))

    def test_incomplete_read_counts_prior_chunks_and_partial_once_per_attempt(self):
        diagnostics, capture, progress = [], CaptureSpy(), []
        with patch.object(media.urllib.request, "urlopen",
                          side_effect=[InterruptedResponse(), InterruptedResponse()]) as opened, \
                patch.object(media, "_decrypt_media") as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type="image", diagnostics=diagnostics,
                                     progress=progress.append, failure_capture=capture)
        self.assertEqual(opened.call_count, 2)
        decrypt.assert_not_called()
        self.assertEqual([r["bytes"] for r in self.records()], [23, 23])
        self.assertEqual(diagnostics[0]["downloaded_bytes"], 23)
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertEqual(len(capture.calls), 1)
        self.assertEqual(capture.calls[0][2], b"a" * 16 + b"b" * 7)
        completed = [row["completed"] for row in progress if row["stage"] == "download"]
        self.assertEqual(completed, sorted(completed))
        self.assertEqual(completed[-1], 46)
        self.assertFalse(self.destination.exists())

    def test_exhausted_source_soft_budget_prevents_another_get(self):
        clock, requests, diagnostics = [0.0], [], []
        def open_short(request, **kwargs):
            requests.append((request, kwargs))
            clock[0] = 121.0
            return short_response()
        with patch.object(media.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(media.urllib.request, "urlopen", side_effect=open_short):
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type="image", diagnostics=diagnostics)
        self.assertEqual(len(requests), 1)
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertFalse(self.records()[-1]["retry_planned"])
        self.assertFalse(self.destination.exists())

    def test_epoch_cancellation_between_attempts_prevents_retry_or_capture(self):
        diagnostics, capture, progressed = [], CaptureSpy(), [0]
        def abort_at_retry(event):
            if event["stage"] == "download" and event["completed"] > 0:
                progressed[0] += 1
                if progressed[0] == 2:
                    raise OperationAborted("epoch_mismatch")
        with patch.object(media.urllib.request, "urlopen",
                          side_effect=[short_response(), Response(CIPHER)]) as opened:
            with self.assertRaises(OperationAborted) as caught:
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type="image", diagnostics=diagnostics,
                                     progress=abort_at_retry, failure_capture=capture)
        self.assertEqual(caught.exception.code, "epoch_mismatch")
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(diagnostics, [])
        self.assertEqual(capture.calls, [])
        self.assertFalse(self.destination.exists())

    def test_nonretryable_image_failures_and_nonimage_keep_single_attempt(self):
        cases = [(Response(b"not-image"), "image", "ciphertext_not_block_aligned"),
                 (urllib.error.HTTPError(URL, 403, PRIVATE, {}, None), "image", "request_or_http_error"),
                 (OSError(PRIVATE + URL), "image", "request_or_http_error")]
        for response, kind, reason in cases:
            with self.subTest(reason=reason):
                diagnostics = []
                with patch.object(media.urllib.request, "urlopen", side_effect=[response]) as opened:
                    with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                        media.download_media({"full_url": URL}, KEY, self.destination,
                                             expected_type=kind, diagnostics=diagnostics)
                self.assertEqual(opened.call_count, 1)
                self.assertEqual(diagnostics[0]["reason"], reason)
                self.assertFalse(self.records()[-1]["retry_planned"])
        diagnostics = []
        with patch.object(media.urllib.request, "urlopen",
                          side_effect=[Response(CIPHER, {"Content-Length": str(len(CIPHER) + 16)})]) as opened, \
                patch.object(media, "_decrypt_media") as decrypt:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type=None, diagnostics=diagnostics)
        self.assertEqual(opened.call_count, 1)
        decrypt.assert_not_called()
        self.assertEqual(diagnostics[0]["reason"], "http_body_incomplete")
        self.assertFalse(self.destination.exists())

    def test_complete_generic_keeps_single_attempt_and_original_bytes(self):
        diagnostics = []
        opened = self.download([Response(CIPHER)], diagnostics, expected_type=None)
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(diagnostics[0]["reason"], "ok")
        self.assertEqual(self.destination.read_bytes(), PLAIN)

    def test_persist_failure_mutates_the_shared_final_summary_and_keeps_old_file(self):
        self.destination.write_bytes(b"existing synthetic original")
        diagnostics, capture = [], CaptureSpy()
        with patch.object(media.os, "replace", side_effect=OSError(PRIVATE)), \
                patch.object(media.urllib.request, "urlopen", side_effect=[Response(CIPHER)]) as opened:
            with self.assertRaises(OSError):
                media.download_media({"full_url": URL}, KEY, self.destination,
                                     expected_type="image", diagnostics=diagnostics,
                                     failure_capture=capture)
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(self.destination.read_bytes(), b"existing synthetic original")
        self.assertEqual(diagnostics, [{"source": "full_url", "outcome": "failed",
                                       "downloaded_bytes": len(CIPHER), "reason": "write_failed"}])
        self.assertEqual(len(capture.calls), 1)
        self.assertEqual(capture.calls[0][-1], "write_failed")
        persisted = [json.loads(row.split(" ", 1)[1]) for row in self.logs
                     if row.startswith("media_persist ")]
        self.assertEqual(persisted[-1]["outcome"], "failed")
        self.assertEqual(sorted(p.name for p in self.destination.parent.iterdir()), ["image.png"])

    def test_legacy_nonconsuming_response_remains_one_bounded_read(self):
        response, diagnostics = LegacyResponse(CIPHER), []
        opened = self.download([response], diagnostics)
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(len(response.read_sizes), 1)
        self.assertGreater(response.read_sizes[0], len(CIPHER))
        self.assertEqual(self.destination.read_bytes(), PLAIN)
        self.assertEqual(diagnostics[0]["reason"], "ok")


if __name__ == "__main__":
    unittest.main()
