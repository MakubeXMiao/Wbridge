"""Finite sender phases, 404 fallback, no POST replay or secret error text."""
import io
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from wechat_bridge import media
from test_attachment_runtime import png, Response

SECRET = "SYNTHETIC_PRIVATE_URL_KEY_RESPONSE"


class ImageSendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="image-first-send-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "image.png"
        self.path.write_bytes(png())
        self.logs = []
        self.addCleanup(patch.stopall)
        patch.object(media, "log", side_effect=self.logs.append).start()
        patch.object(socket, "create_connection", side_effect=AssertionError("no real sockets")).start()

    def send(self, **kwargs):
        return media.send_media_message("https://synthetic.invalid", "FAKE-TOKEN", "owner", "FAKE-CONTEXT",
            str(self.path), media.ITEM_IMAGE, client_key="synthetic-stable-unit", **kwargs)

    def successful_upload(self):
        patch.object(media, "upload_media", return_value=("synthetic-param", "synthetic-key", 96)).start()

    def assert_private(self, error=None, response=None):
        self.assertNotIn(SECRET, "\n".join(self.logs) + str(error) + str(response))

    def test_getuploadurl_error_has_fixed_phase_and_no_automatic_retry(self):
        calls = []
        def http(method, url, headers, body, **kwargs):
            calls.append((url, kwargs))
            return {"_raw": SECRET, "upload_full_url": "https://synthetic.invalid/" + SECRET}
        with patch.object(media, "http_json", side_effect=http), patch.object(media, "_post_cdn") as cdn:
            with self.assertRaises(media.MediaSendError) as caught:
                self.send()
        self.assertEqual(caught.exception.stage, "getuploadurl")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["retries"], 1)
        cdn.assert_not_called()
        self.assert_private(caught.exception)

    def test_cdn_http404_preserves_machine_status_without_response_or_url(self):
        error = urllib.error.HTTPError("https://synthetic.invalid/" + SECRET, 404, SECRET, {}, io.BytesIO(SECRET.encode()))
        with patch.object(media.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(media.MediaSendError) as caught:
                media._post_cdn("https://synthetic.invalid/" + SECRET, "full_url", b"synthetic-cipher")
        self.assertEqual(caught.exception.http_status, 404)
        self.assertEqual(caught.exception.stage, "cdn_upload")
        self.assert_private(caught.exception)

    def test_cdn404_uses_existing_param_fallback_and_final_send_once(self):
        calls, uploads = [], []
        def http(method, url, headers, body, **kwargs):
            calls.append((url, kwargs))
            if url.endswith("/getuploadurl"):
                return {"ret": 0, "upload_full_url": "https://synthetic.invalid/full", "upload_param": "synthetic-param"}
            self.assertTrue(url.endswith("/sendmessage"))
            return {"ret": 0}
        def cdn(request, **kwargs):
            uploads.append(request.full_url)
            if request.full_url.endswith("/full"):
                raise urllib.error.HTTPError(request.full_url, 404, SECRET, {}, io.BytesIO())
            response = Response(b"")
            response.headers["x-encrypted-param"] = "synthetic-uploaded"
            return response
        with patch.object(media, "http_json", side_effect=http), patch.object(media.urllib.request, "urlopen", side_effect=cdn):
            self.assertEqual(self.send(), {"ret": 0})
        self.assertEqual(len(uploads), 2)
        self.assertIn("/upload?", uploads[1])
        self.assertEqual(sum(url.endswith("/sendmessage") for url, _ in calls), 1)
        self.assertTrue(all(options["retries"] == 1 for _, options in calls))
        events = [json.loads(row.split(" ", 1)[1]) for row in self.logs
                  if row.startswith("media_send ")]
        for stage in ("getuploadurl", "cdn_upload", "sendmessage"):
            timings = [event["elapsed_ms"] for event in events if event["stage"] == stage]
            self.assertTrue(timings, stage)
            self.assertTrue(all(type(value) is int and value >= 0 for value in timings), stage)
        self.assert_private()

    def test_send_transport_unknown_is_not_retried_or_reported_as_settled(self):
        self.successful_upload()
        with patch.object(media, "http_json", side_effect=OSError(SECRET)) as http, \
             patch.object(media, "_send_event", wraps=media._send_event) as events:
            settled = []
            with self.assertRaises(media.MediaSendError) as caught:
                self.send(on_result=lambda *args: settled.append(args))
        self.assertEqual(caught.exception.stage, "sendmessage")
        self.assertEqual(http.call_count, 1)
        self.assertEqual(http.call_args.kwargs["retries"], 1)
        self.assertEqual(settled, [])
        self.assertNotIn(("settle", "success"), [call.args[:2] for call in events.call_args_list])
        self.assert_private(caught.exception)

    def test_settle_failure_after_api_acceptance_does_not_send_again(self):
        self.successful_upload()
        with patch.object(media, "http_json", return_value={"ret": 0}) as http, \
             patch.object(media, "_send_event", wraps=media._send_event) as events:
            def fail(*args):
                raise OSError(SECRET)
            with self.assertRaises(media.MediaSendError) as caught:
                self.send(on_result=fail)
        self.assertEqual(caught.exception.stage, "settle")
        self.assertEqual(http.call_count, 1)
        self.assertIn(("sendmessage", "success"), [call.args[:2] for call in events.call_args_list])
        self.assertIn(("settle", "failed"), [call.args[:2] for call in events.call_args_list])
        self.assert_private(caught.exception)

    def test_locally_accepted_unit_is_not_logged_as_a_new_http_send(self):
        self.successful_upload()
        settled = []
        with patch.object(media, "http_json") as http:
            result = self.send(before_send=lambda *args: False,
                               on_result=lambda *args: settled.append(args[2]))
        http.assert_not_called()
        self.assertTrue(result["_locally_accepted"])
        self.assertEqual(settled, [result])
        events = [json.loads(row.split(" ", 1)[1]) for row in self.logs if row.startswith("media_send ")]
        send_events = [row for row in events if row["stage"] == "sendmessage"]
        self.assertEqual(len(send_events), 1)
        self.assertIs(send_events[0]["performed"], False)

    def test_failed_send_response_is_safe_but_token_invalid_is_preserved(self):
        self.successful_upload()
        for response in ({"_raw": SECRET}, {"ret": -14, "errmsg": SECRET}):
            with self.subTest(response_fields=tuple(response)):
                settled = []
                with patch.object(media, "http_json", return_value=response) as http:
                    result = self.send(on_result=lambda *args: settled.append(args[2]))
                self.assertEqual(http.call_count, 1)
                self.assertEqual(result["ret"], response.get("ret", -1))
                self.assertEqual(settled, [result])
                self.assert_private(response=result)


if __name__ == "__main__":
    unittest.main()
