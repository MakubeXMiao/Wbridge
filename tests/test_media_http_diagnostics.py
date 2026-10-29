"""Synthetic, socket-blocked HTTP/decrypt diagnostic-v2 regression tests."""

import base64
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechat_bridge import crypto, media


SECRET = "SYNTHETIC_DIAG_SECRET_DO_NOT_LOG"
SOURCE_URL = "https://source.invalid/signed?token=" + SECRET
from PIL import Image
_image_stream = io.BytesIO()
Image.new("RGB", (2, 2), "red").save(_image_stream, format="PNG")
PNG = _image_stream.getvalue()
KEY = b"0123456789abcdef"


class OldResponse:
    """The earlier offline Response contract deliberately has no HTTP metadata."""

    def __init__(self, body):
        self.body = body
        self.read_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size):
        self.read_sizes.append(size)
        return self.body[:size]


class Response(OldResponse):
    def __init__(self, body, status=200, headers=None, final_url=SOURCE_URL):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers
        self.final_url = final_url

    def getcode(self):
        return self.status

    def geturl(self):
        return self.final_url


class ReadErrorResponse(Response):
    def read(self, size):
        raise OSError("body read failed " + SECRET + SOURCE_URL)


class HostileMetadataResponse(OldResponse):
    @property
    def status(self):
        raise ValueError(SECRET)

    @property
    def headers(self):
        raise ValueError(SECRET)

    def getcode(self):
        raise ValueError(SECRET)

    def geturl(self):
        raise ValueError(SECRET)


class HttpDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="media-http-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.logs = []
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(media, "log", side_effect=self.logs.append).start()
        mock.patch.object(socket, "create_connection", side_effect=AssertionError("real sockets forbidden")).start()
        mock.patch.object(socket.socket, "connect", side_effect=AssertionError("real sockets forbidden")).start()
        mock.patch.object(media.urllib.request, "urlopen", side_effect=AssertionError("urlopen must be mocked")).start()
        self.destination = os.path.join(self.temp.name, "synthetic.png")

    def records(self):
        return [json.loads(line.split(" ", 1)[1]) for line in self.logs
                if line.startswith("media_receive ")]

    def download(self, responses, params=None, expected_type="image", override=None):
        values = {"full_url": SOURCE_URL, "aes_key": KEY}
        if params:
            values.update(params)
        with mock.patch.object(media.urllib.request, "urlopen", side_effect=responses) as urlopen:
            result = media.download_media(values, override, self.destination,
                                          expected_type=expected_type)
        return result, urlopen

    def assert_private(self):
        text = "\n".join(self.logs)
        for private in (SECRET, SECRET.lower(), SOURCE_URL, "source.invalid", "redirect.invalid",
                        KEY.decode("ascii"), KEY.hex(), base64.b64encode(KEY).decode("ascii")):
            self.assertNotIn(private, text)

    def assert_schema(self, record):
        self.assertEqual(record["diagnostic_version"], 2)
        self.assertRegex(record["transfer_id"], r"^[0-9a-f]{32}$")
        self.assertIn(record["source"], ("param", "full_url"))
        self.assertIn(record["source_count"], (1, 2))
        self.assertIn(record["source_attempt"], range(1, record["source_count"] + 1))
        self.assertEqual(record["magic_scope"], "http_response_body")
        self.assertEqual(record["outcome_scope"], "download_decrypt")
        self.assertIn(record["stage"], ("http", "decrypt"))
        self.assertIn(record["magic"], ("other", "PNG", "JPEG", "GIF", "WEBP"))
        self.assertIsInstance(record["attempted"], list)
        status = record["http_status"]
        if status is not None:
            self.assertIs(type(status), int)
            self.assertGreaterEqual(status, 100)
            self.assertLessEqual(status, 599)
        self.assertIn(record["content_type"], (
            "missing", "other", "invalid", "image/jpeg", "image/png", "image/gif", "image/webp",
            "application/octet-stream", "text/html", "application/json", "text/plain"))
        self.assertIn(record["content_encoding"], ("missing", "identity", "gzip", "deflate", "br", "other", "invalid"))
        self.assertIn(record["content_length_state"], ("missing", "valid", "invalid"))
        content_length = record["content_length"]
        if content_length is not None:
            self.assertIs(type(content_length), int)
            self.assertGreaterEqual(content_length, 0)
            self.assertLessEqual(content_length, 2 ** 63 - 1)
        self.assertIn(record["final_host_relation"], ("same", "different", "unknown"))
        matches = record["body_length_matches_content_length"]
        self.assertTrue(matches is None or type(matches) is bool)
        if record["bytes"] is None:
            self.assertIsNone(record["block_remainder"])
        else:
            self.assertEqual(record["block_remainder"], record["bytes"] % 16)
        self.assertNotIsInstance(record["elapsed_ms"], bool)
        self.assertGreaterEqual(record["elapsed_ms"], 0)
        self.assertLessEqual(record["elapsed_ms"], 3600000)
        if record["outcome"] == "success":
            self.assertNotIn("failure_reason", record)
        else:
            self.assertEqual(record["outcome"], "failed")
            self.assertIn(record["failure_reason"], (
                "request_or_http_error", "http_body_read_failed", "ciphertext_too_large", "empty_body",
                "ciphertext_not_block_aligned", "decrypt_no_candidate", "decrypt_ambiguous", "plaintext_too_large"))
        self.assert_private()

    def test_legacy_response_without_metadata_still_downloads(self):
        self.download([OldResponse(PNG)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["outcome"], "success")
        for field in ("http_status", "content_length", "body_length_matches_content_length"):
            self.assertIsNone(record[field])
        for field in ("content_type", "content_encoding", "content_length_state"):
            self.assertEqual(record[field], "missing")
        self.assertEqual(record["final_host_relation"], "unknown")
        with open(self.destination, "rb") as handle:
            self.assertEqual(handle.read(), PNG)

    def test_png_ecb_success_distinguishes_input_magic_from_plaintext(self):
        encrypted = crypto._aes_ecb_encrypt(PNG, KEY)
        response = Response(encrypted, headers={"Content-Type": "application/octet-stream",
                                               "Content-Encoding": "identity",
                                               "Content-Length": str(len(encrypted))})
        self.download([response])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["magic"], "other")
        self.assertEqual(record["plaintext_magic"], "PNG")
        self.assertEqual(record["stage"], "decrypt")
        self.assertEqual(record["http_status"], 200)
        self.assertEqual(record["content_type"], "application/octet-stream")
        self.assertEqual(record["content_encoding"], "identity")
        self.assertEqual(record["content_length"], len(encrypted))
        self.assertTrue(record["body_length_matches_content_length"])
        self.assertEqual(record["final_host_relation"], "same")
        self.assertRegex(record["strategy"], r"ECB$")
        with open(self.destination, "rb") as handle:
            self.assertEqual(handle.read(), PNG)

    def test_two_sources_share_transfer_but_next_download_is_distinct(self):
        self.download([Response(b"bad"), Response(PNG)], {"encrypt_query_param": SECRET})
        first, second = self.records()
        self.assertEqual(first["transfer_id"], second["transfer_id"])
        self.assertEqual([(row["source"], row["source_attempt"], row["source_count"])
                          for row in (first, second)], [("param", 1, 2), ("full_url", 2, 2)])
        self.download([Response(PNG)])
        third = self.records()[-1]
        self.assertNotEqual(first["transfer_id"], third["transfer_id"])
        self.assertEqual((third["source_attempt"], third["source_count"]), (1, 1))
        for record in (first, second, third):
            self.assert_schema(record)

    def test_50380_non_aligned_body_does_not_try_decryption(self):
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([Response(b"x" * 50380)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["bytes"], 50380)
        self.assertEqual(record["block_remainder"], 12)
        self.assertEqual(record["attempted"], [])
        self.assertEqual(record["failure_reason"], "ciphertext_not_block_aligned")
        self.assertEqual(record["stage"], "decrypt")
        self.assertFalse(os.path.exists(self.destination))

    def test_61072_aligned_body_has_no_image_candidate(self):
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([Response(b"\x00" * 61072)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["bytes"], 61072)
        self.assertEqual(record["block_remainder"], 0)
        self.assertTrue(record["block_aligned"])
        self.assertTrue(record["attempted"])
        self.assertEqual(record["failure_reason"], "decrypt_no_candidate")

    def test_empty_body_has_no_decryption_attempts(self):
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([Response(b"", headers={"Content-Length": "0"})])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "empty_body")
        self.assertEqual(record["stage"], "decrypt")
        self.assertEqual(record["attempted"], [])
        self.assertEqual(record["bytes"], 0)
        self.assertTrue(record["body_length_matches_content_length"])

    def test_body_read_error_retains_safe_http_metadata(self):
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([ReadErrorResponse(b"", headers={"Content-Type": "image/png", "Content-Length": "33"})])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "http_body_read_failed")
        self.assertEqual(record["http_status"], 200)
        self.assertEqual(record["content_type"], "image/png")
        self.assertEqual(record["content_length"], 33)
        self.assertIsNone(record["bytes"])
        self.assertIsNone(record["body_length_matches_content_length"])

    def test_http_error_retains_only_allowed_metadata(self):
        error = urllib.error.HTTPError(SOURCE_URL, 403, SECRET,
                                       {"Content-Type": "text/html", "Content-Encoding": "gzip",
                                        "Content-Length": "123", "X-Secret": SECRET}, io.BytesIO(SECRET.encode()))
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([error])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "request_or_http_error")
        self.assertEqual(record["http_status"], 403)
        self.assertEqual(record["content_type"], "text/html")
        self.assertEqual(record["content_encoding"], "gzip")
        self.assertEqual(record["content_length"], 123)
        self.assertEqual(record["attempted"], [])

    def test_request_exception_does_not_leak_url_or_error_text(self):
        with mock.patch.object(media.urllib.request, "Request", side_effect=ValueError(SECRET + SOURCE_URL)):
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download([])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "request_or_http_error")
        self.assertIsNone(record["http_status"])

    def test_url_error_does_not_leak_reason(self):
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([urllib.error.URLError(SECRET + SOURCE_URL)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "request_or_http_error")

    def test_malicious_header_values_are_redacted_not_download_failures(self):
        cases = [({"Content-Type": "application/x-" + SECRET,
                   "Content-Encoding": "x-" + SECRET, "Content-Length": SECRET},
                  ("other", "other", "invalid")),
                 ({"Content-Type": "image/png\r\nX-Secret: " + SECRET,
                   "Content-Encoding": "gzip\r\n" + SECRET, "Content-Length": "10\r\n" + SECRET},
                  ("invalid", "invalid", "invalid")),
                 ({"Content-Type": {"private": SECRET}, "Content-Encoding": [SECRET],
                   "Content-Length": {"private": SECRET}}, ("invalid", "invalid", "invalid"))]
        for headers, expected in cases:
            with self.subTest(expected=expected):
                self.download([Response(PNG, headers=headers)])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["outcome"], "success")
                self.assertEqual(tuple(record[field] for field in (
                    "content_type", "content_encoding", "content_length_state")), expected)
                self.assertIsNone(record["content_length"])

    def test_known_content_types_and_encodings_are_fixed_categories(self):
        for content_type in ("image/jpeg", "image/png", "image/gif", "image/webp", "application/octet-stream",
                             "text/html", "application/json", "text/plain"):
            with self.subTest(content_type=content_type):
                self.download([Response(PNG, headers={"Content-Type": content_type.upper() + "; charset=utf-8"})])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["content_type"], content_type)
        for encoding in ("identity", "gzip", "deflate", "br"):
            with self.subTest(encoding=encoding):
                self.download([Response(PNG, headers={"Content-Encoding": encoding.upper()})])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["content_encoding"], encoding)

    def test_content_length_valid_and_invalid_boundaries(self):
        for value, expected in (("0", 0), (str(len(PNG)), len(PNG)), (str(2 ** 63 - 1), 2 ** 63 - 1)):
            with self.subTest(value=value):
                self.download([Response(PNG, headers={"Content-Length": value})])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["content_length_state"], "valid")
                self.assertEqual(record["content_length"], expected)
                self.assertEqual(record["body_length_matches_content_length"], expected == len(PNG))
        for value in ("-1", "+1", "1.5", "1, 1", str(2 ** 63), "", True, 3, SECRET):
            with self.subTest(value=value):
                self.download([Response(PNG, headers={"Content-Length": value})])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["content_length_state"], "invalid")
                self.assertIsNone(record["content_length"])
                self.assertIsNone(record["body_length_matches_content_length"])

    def test_success_status_accepts_only_http_integer_range(self):
        for status in (100, 200, 599, 99, 600, True, "200", None):
            with self.subTest(status=status):
                self.download([Response(PNG, status=status)])
                record = self.records()[-1]
                self.assert_schema(record)
                expected = status if type(status) is int and 100 <= status <= 599 else None
                self.assertEqual(record["http_status"], expected)

    def test_http_error_invalid_status_not_logged_as_http_status(self):
        for status in (99, 600, True, "SECRET_200"):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(SOURCE_URL, status, SECRET, {}, None)
                with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                    self.download([error])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertIsNone(record["http_status"])

    def test_final_url_reports_host_relation_only(self):
        cases = (("https://source.invalid/other?private=" + SECRET, "same"),
                 ("https://redirect.invalid/new?private=" + SECRET, "different"),
                 ("not a URL " + SECRET, "unknown"), (None, "unknown"))
        for final_url, expected in cases:
            with self.subTest(expected=expected):
                self.download([Response(PNG, final_url=final_url)])
                record = self.records()[-1]
                self.assert_schema(record)
                self.assertEqual(record["final_host_relation"], expected)

    def test_metadata_access_exceptions_do_not_break_body_download(self):
        self.download([HostileMetadataResponse(PNG)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["outcome"], "success")
        self.assertIsNone(record["http_status"])
        self.assertEqual(record["final_host_relation"], "unknown")

    def test_header_getter_exception_also_does_not_break_download(self):
        class HostileHeaders:
            def get(self, *args):
                raise OSError(SECRET)

        self.download([Response(PNG, headers=HostileHeaders())])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["outcome"], "success")

    def test_atomic_write_failure_is_not_persist_success_and_preserves_target(self):
        with open(self.destination, "wb") as handle:
            handle.write(b"previous-synthetic-file")
        with mock.patch.object(media.os, "replace", side_effect=OSError(SECRET + self.destination)):
            with self.assertRaises(OSError):
                self.download([Response(PNG)])
        received = self.records()[-1]
        self.assert_schema(received)
        self.assertEqual(received["outcome"], "success")
        persist = [json.loads(line.split(" ", 1)[1]) for line in self.logs
                   if line.startswith("media_persist ")]
        self.assertEqual(len(persist), 1)
        self.assertEqual(persist[0]["transfer_id"], received["transfer_id"])
        self.assertEqual(persist[0]["outcome"], "failed")
        self.assertEqual(persist[0]["error"], "write_failed")
        self.assertNotIn(self.destination, "\n".join(self.logs))
        self.assertEqual(os.listdir(self.temp.name), ["synthetic.png"])
        with open(self.destination, "rb") as handle:
            self.assertEqual(handle.read(), b"previous-synthetic-file")
        self.assert_private()

    def test_persist_success_has_separate_correlated_record(self):
        self.download([Response(PNG)])
        received = self.records()[-1]
        persist = [json.loads(line.split(" ", 1)[1]) for line in self.logs
                   if line.startswith("media_persist ")]
        self.assertEqual(len(persist), 1)
        self.assertEqual(persist[0]["transfer_id"], received["transfer_id"])
        self.assertEqual(persist[0]["outcome"], "success")
        self.assertNotIn("error", persist[0])
        self.assertNotIn(self.destination, "\n".join(self.logs))
        self.assert_private()

    def test_ciphertext_size_limit_is_distinct_and_preserves_old_file(self):
        with open(self.destination, "wb") as handle:
            handle.write(b"previous-synthetic-file")
        limit = 32
        cipher_limit = (limit // 16 + 1) * 16 + 16
        response = Response(b"x" * (cipher_limit + 50))
        with mock.patch.object(media, "MAX_MEDIA_BYTES", limit):
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download([response])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "ciphertext_too_large")
        self.assertEqual(record["stage"], "http")
        self.assertEqual(record["bytes"], cipher_limit + 1)
        self.assertEqual(response.read_sizes, [cipher_limit + 1])
        self.assertEqual(record["attempted"], [])
        with open(self.destination, "rb") as handle:
            self.assertEqual(handle.read(), b"previous-synthetic-file")

    def test_plaintext_limit_is_distinct_and_exact_boundary_succeeds(self):
        encrypted = crypto._aes_ecb_encrypt(PNG, KEY)
        with mock.patch.object(media, "MAX_MEDIA_BYTES", len(PNG)):
            self.download([Response(encrypted)])
        self.assertEqual(self.records()[-1]["outcome"], "success")
        with mock.patch.object(media, "MAX_MEDIA_BYTES", len(PNG) - 1):
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download([Response(encrypted)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "plaintext_too_large")
        self.assertEqual(record["stage"], "decrypt")
        with open(self.destination, "rb") as handle:
            self.assertEqual(handle.read(), PNG)

    def test_ambiguity_is_not_reclassified_as_no_candidate_or_plaintext_success(self):
        with mock.patch.object(media, "_decrypt_media", side_effect=ValueError("media_decrypt_ambiguous")):
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                self.download([Response(PNG)])
        record = self.records()[-1]
        self.assert_schema(record)
        self.assertEqual(record["failure_reason"], "decrypt_ambiguous")
        self.assertEqual(record["error"], "decrypt_ambiguous")
        self.assertFalse(os.path.exists(self.destination))

    def test_failing_second_source_does_not_inherit_first_body_metadata(self):
        failure = urllib.error.HTTPError(SOURCE_URL, 502, SECRET, {"Content-Type": "text/html"}, None)
        with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
            self.download([Response(b"bad"), failure], {"encrypt_query_param": SECRET})
        first, second = self.records()
        self.assertEqual(first["bytes"], 3)
        self.assertIsNone(second["bytes"])
        self.assertIsNone(second["block_remainder"])
        self.assertEqual(second["magic"], "other")
        self.assertEqual(second["attempted"], [])
        self.assertEqual(second["http_status"], 502)
        self.assertEqual(first["transfer_id"], second["transfer_id"])
        self.assert_schema(first)
        self.assert_schema(second)


if __name__ == "__main__":
    unittest.main(verbosity=2)
