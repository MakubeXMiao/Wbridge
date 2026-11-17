"""Observed read-prefix bytes on synthetic failures, without new sample bodies.

Only HTTP is mocked. No socket, decrypt, file publication or real sample is
permitted in these failure-path checks. This is implementation self-validation.
"""
import base64
import http.client
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import media
from wechat_bridge.progress import OperationAborted

SECRET = "SYNTHETIC_READ_PROGRESS_PRIVATE"
URL = "https://read-progress.invalid/body?private=" + SECRET
KEY = b"0123456789abcdef"


class SequenceResponse:
    """Each read either returns explicit application-visible bytes or raises."""
    status = 200

    def __init__(self, steps, *, declared=96, after_read=None, exit_error=None):
        self.steps = list(steps)
        self.headers = {"Content-Length": str(declared),
                        "Content-Type": "application/octet-stream"}
        self.after_read = after_read
        self.exit_error = exit_error
        self.read_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        if typ is None and self.exit_error is not None:
            raise self.exit_error
        return False

    def getcode(self):
        return self.status

    def geturl(self):
        return URL

    def read(self, size=-1):
        self.read_sizes.append(size)
        if not self.steps:
            raise AssertionError("synthetic response exhausted")
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if self.after_read is not None:
            self.after_read(step)
        return step


class CaptureSpy:
    def __init__(self):
        self.calls = []

    def capture(self, *args):
        self.calls.append(args)


class MediaReadProgressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-read-prefix-")
        self.addCleanup(self.temp.cleanup)
        self.destination = Path(self.temp.name) / "synthetic.bin"
        self.logs = []
        self.progress = []
        self.network = []
        self.capture = CaptureSpy()
        self.patch(media, "log", self.logs.append)
        for target, name in ((socket, "create_connection"), (socket, "getaddrinfo"),
                             (socket.socket, "connect"), (socket.socket, "connect_ex"),
                             (socket.socket, "bind"), (socket.socket, "listen")):
            self.patch(target, name, self.deny_network)
        self.patch(media.urllib.request, "urlopen", self.deny_network)

    def patch(self, target, name, value):
        item = patch.object(target, name, value)
        value = item.start()
        self.addCleanup(item.stop)
        return value

    def deny_network(self, *args, **kwargs):
        self.network.append("blocked")
        raise AssertionError("synthetic read tests forbid network")

    def records(self):
        return [json.loads(line.split(" ", 1)[1]) for line in self.logs
                if line.startswith("media_receive ")]

    def tearDown(self):
        self.assertEqual(self.network, [])
        self.assertFalse(self.destination.exists())
        text = "\n".join(self.logs)
        for private in (SECRET, URL, "read-progress.invalid", KEY.decode(),
                        KEY.hex(), base64.b64encode(KEY).decode()):
            self.assertNotIn(private, text)

    def failed(self, responses, *, kind=None, sources=None, use_progress=True):
        self.logs.clear()
        self.progress.clear()
        self.capture.calls.clear()
        diagnostics = []
        source = {"full_url": URL, "aes_key": KEY} if sources is None else sources
        with patch.object(media.urllib.request, "urlopen", side_effect=responses) as opened, \
                patch.object(media, "_decrypt_media") as decrypt, \
                patch.object(media, "_decrypt_original") as original:
            with self.assertRaisesRegex(ValueError, "media_receive_all_sources_failed"):
                media.download_media(source, KEY, self.destination,
                    expected_type=kind, diagnostics=diagnostics,
                    progress=self.progress.append if use_progress else None,
                    preserve_original=True, failure_capture=self.capture)
        decrypt.assert_not_called()
        original.assert_not_called()
        return opened, diagnostics

    def assert_prefix(self, record, summary, size, reason="http_body_read_failed"):
        self.assertEqual(record["stage"], "http")
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(record["failure_reason"], reason)
        self.assertEqual(record["bytes"], size)
        self.assertEqual(record["block_remainder"], size % 16)
        self.assertEqual(record["block_aligned"], bool(size) and size % 16 == 0)
        self.assertEqual(summary["downloaded_bytes"], size)
        self.assertEqual(summary["reason"], reason)
        self.assertEqual(record["attempted"], [])
        self.assertFalse(record["retry_planned"])
        self.assertEqual(record["request_attempt"], 1)

    def test_returned_prefix_survives_timeout_urlerror_and_read_error(self):
        errors = [TimeoutError(SECRET + URL),
                  urllib.error.URLError(OSError(SECRET + URL)),
                  OSError(SECRET + URL)]
        for kind in (None, "image"):
            for error in errors:
                with self.subTest(kind=kind, error_type=type(error).__name__):
                    response = SequenceResponse([b"a" * 16, b"b" * 5, error])
                    opened, diagnostics = self.failed([response], kind=kind)
                    self.assertEqual(opened.call_count, 1)
                    self.assertEqual(len(response.read_sizes), 3)
                    self.assertTrue(all(0 < size <= 65536 for size in response.read_sizes))
                    self.assert_prefix(self.records()[0], diagnostics[0], 21)
                    self.assertEqual(self.capture.calls, [])
                    completed = [e["completed"] for e in self.progress if e["stage"] == "download"]
                    self.assertEqual(completed[-1], 21)
                    self.assertEqual(completed, sorted(completed))

    def test_first_read_error_keeps_unobserved_body_size_null(self):
        for kind in (None, "image"):
            for use_progress in (False, True):
                with self.subTest(kind=kind, progress=use_progress):
                    response = SequenceResponse([TimeoutError(SECRET + URL)])
                    opened, diagnostics = self.failed([response], kind=kind,
                                                       use_progress=use_progress)
                    self.assertEqual(opened.call_count, 1)
                    record = self.records()[0]
                    self.assertIsNone(record["bytes"])
                    self.assertIsNone(record["block_remainder"])
                    self.assertIsNone(record["block_aligned"])
                    self.assertIsNone(diagnostics[0]["downloaded_bytes"])
                    self.assertEqual(record["failure_reason"], "http_body_read_failed")
                    self.assertFalse(record["retry_planned"])
                    self.assertEqual(self.capture.calls, [])

    def test_source_deadline_after_chunk_preserves_count_without_retry_or_capture(self):
        clock = [0.0]
        response = SequenceResponse([b"a" * 17],
                                    after_read=lambda chunk: clock.__setitem__(0, 61.0))
        with patch.object(media.time, "monotonic", side_effect=lambda: clock[0]):
            opened, diagnostics = self.failed([response], kind="image")
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(len(response.read_sizes), 1)
        self.assert_prefix(self.records()[0], diagnostics[0], 17)
        self.assertEqual(self.capture.calls, [])

    def test_each_source_count_is_local_while_existing_progress_is_cumulative(self):
        responses = [SequenceResponse([b"a" * 16, b"b" * 4, TimeoutError(SECRET)]),
                     SequenceResponse([b"c" * 32, b"d" * 3, OSError(URL)])]
        opened, diagnostics = self.failed(responses,
            sources={"encrypt_query_param": SECRET, "full_url": URL, "aes_key": KEY})
        records = self.records()
        self.assertEqual(opened.call_count, 2)
        self.assertEqual(len(records), 2)
        self.assert_prefix(records[0], diagnostics[0], 20)
        self.assert_prefix(records[1], diagnostics[1], 35)
        self.assertEqual([r["source_attempt"] for r in records], [1, 2])
        self.assertEqual([r["source"] for r in records], ["param", "full_url"])
        self.assertEqual(len({r["transfer_id"] for r in records}), 1)
        completed = [e["completed"] for e in self.progress if e["stage"] == "download"]
        self.assertEqual(completed[-1], 55)
        self.assertEqual(completed, sorted(completed))
        self.assertEqual(self.capture.calls, [])

    def test_generic_incomplete_read_counts_partial_without_body_or_new_checkpoint(self):
        response = SequenceResponse([b"a" * 16,
                                     http.client.IncompleteRead(b"b" * 7, 73)])
        opened, diagnostics = self.failed([response])
        self.assertEqual(opened.call_count, 1)
        self.assert_prefix(self.records()[0], diagnostics[0], 23, "http_body_incomplete")
        self.assertEqual(self.capture.calls, [])
        completed = [e["completed"] for e in self.progress if e["stage"] == "download"]
        self.assertEqual(completed[-1], 16)

    def test_generic_first_read_incomplete_partial_is_observed_without_progress(self):
        response = SequenceResponse([http.client.IncompleteRead(b"b" * 7, 89)])
        opened, diagnostics = self.failed([response], use_progress=False)
        self.assertEqual(opened.call_count, 1)
        self.assert_prefix(self.records()[0], diagnostics[0], 7, "http_body_incomplete")
        self.assertEqual(self.capture.calls, [])
        self.assertEqual(self.progress, [])

    def test_generic_incomplete_partial_observation_stays_within_cipher_guard(self):
        response = SequenceResponse([b"a" * 32,
                                     http.client.IncompleteRead(b"b" * 20, 64)])
        with patch.object(media, "MAX_MEDIA_BYTES", 32):
            opened, diagnostics = self.failed([response])
        self.assertEqual(opened.call_count, 1)
        self.assert_prefix(self.records()[0], diagnostics[0], 49, "ciphertext_too_large")
        self.assertEqual(self.capture.calls, [])

    def test_image_incomplete_read_retains_existing_partial_recovery_once(self):
        responses = [SequenceResponse([b"a" * 16, http.client.IncompleteRead(b"b" * 7, 73)])
                     for _ in range(2)]
        opened, diagnostics = self.failed(responses, kind="image")
        self.assertEqual(opened.call_count, 2)
        self.assertEqual([r["bytes"] for r in self.records()], [23, 23])
        self.assertEqual([r["request_attempt"] for r in self.records()], [1, 2])
        self.assertEqual([r["retry_planned"] for r in self.records()], [True, False])
        self.assertEqual(diagnostics[0]["downloaded_bytes"], 23)
        self.assertEqual(self.progress[-1]["completed"], 46)
        self.assertEqual(len(self.capture.calls), 1)
        self.assertEqual(self.capture.calls[0][2], b"a" * 16 + b"b" * 7)

    def test_successful_read_followed_by_context_exit_error_counts_body_once(self):
        for kind in (None, "image"):
            for use_progress in (False, True):
                with self.subTest(kind=kind, progress=use_progress):
                    body = b"a" * 16 + b"b" * 5
                    response = SequenceResponse([b"a" * 16, b"b" * 5, b""]
                        if use_progress else [body], declared=len(body),
                        exit_error=OSError(SECRET + URL))
                    opened, diagnostics = self.failed([response], kind=kind,
                                                       use_progress=use_progress)
                    self.assertEqual(opened.call_count, 1)
                    self.assert_prefix(self.records()[0], diagnostics[0], 21)
                    # This pre-existing path already has a complete read result;
                    # preserve its one capture call instead of creating extras.
                    self.assertEqual(len(self.capture.calls), 1)
                    self.assertEqual(self.capture.calls[0][2], body)

    def test_authority_abort_after_chunk_is_immediate_without_terminal_side_effects(self):
        for code in ("cancelled", "lease_lost", "epoch_mismatch", "expired"):
            with self.subTest(code=code):
                self.logs.clear(); self.progress.clear(); self.capture.calls.clear()
                diagnostics = []
                def progress(event):
                    self.progress.append(event)
                    if event["stage"] == "download" and event["completed"] > 0:
                        raise OperationAborted(code)
                response = SequenceResponse([b"a" * 16, b"b" * 16])
                with patch.object(media.urllib.request, "urlopen", side_effect=[response]) as opened:
                    with self.assertRaises(OperationAborted) as caught:
                        media.download_media({"full_url": URL, "aes_key": KEY}, KEY,
                            self.destination, expected_type="image", progress=progress,
                            diagnostics=diagnostics, failure_capture=self.capture,
                            preserve_original=True)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(opened.call_count, 1)
                self.assertEqual(len(response.read_sizes), 1)
                self.assertEqual(self.records(), [])
                self.assertEqual(diagnostics, [])
                self.assertEqual(self.capture.calls, [])
                self.assertEqual([e["completed"] for e in self.progress], [0, 0, 16])

    def test_before_response_error_does_not_invent_observed_prefix(self):
        opened, diagnostics = self.failed([urllib.error.URLError(SECRET + URL)])
        self.assertEqual(opened.call_count, 1)
        record = self.records()[0]
        self.assertIsNone(record["bytes"])
        self.assertIsNone(diagnostics[0]["downloaded_bytes"])
        self.assertEqual(record["failure_reason"], "request_or_http_error")
        self.assertEqual(self.capture.calls, [])


if __name__ == "__main__":
    unittest.main()
