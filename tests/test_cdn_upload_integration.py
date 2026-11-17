"""Exercise the original send boundary with the supervised CDN entry enabled."""

from contextlib import contextmanager
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_attachment_runtime as fixture
from wechat_bridge import attachment_runtime as runtime, cdn_process, media, serve
from wechat_bridge.progress import OperationAborted
from wechat_bridge.store import load_json


@contextmanager
def active_sender():
    case = fixture.RuntimeTests("runTest")
    case.setUp()
    start = len(serve._lock_fhs)
    try:
        assert serve._acquire_lock(case.store)
        with patch.object(serve, "_bot_lock_root", return_value=str(case.home / "bot-locks")):
            assert serve._acquire_bot_lock(case.session, store=case.store)
        yield case
    finally:
        for handle in serve._lock_fhs[start:]:
            handle.close()
        del serve._lock_fhs[start:]
        case.doCleanups()


class CDNIntegrationTests(unittest.TestCase):
    def admit_group(self, case):
        aid = case.file()
        batch = case.batch()
        return case.success(case.call(batch, "reply", {
            "attachment_ids": [aid], "caption": "SYNTHETIC caption",
        }))

    def test_group_500_retries_under_same_original_deadline_without_video_post(self):
        with active_sender() as case:
            group = self.admit_group(case)
            messages, _ = case.send_spy()
            state = runtime.ledger(case.store)
            job = state.job_status(case.scope, group["job_id"])
            reports = []

            def failed(url, encrypted, **options):
                options["check"]()
                reports.append((options["deadline"], options["wall_deadline"], options["guard_fds"]))
                options["report"]({"phase": "response_headers", "outcome": "success",
                                   "elapsed_ms": 15, "bytes_written": len(encrypted),
                                   "bytes_total": len(encrypted), "error": None})
                raise cdn_process.CDNUploadError(500)

            with patch.object(cdn_process, "upload", side_effect=failed), \
                    patch.object(media.time, "sleep") as backoff:
                case.flush()
            backoff.assert_called_once_with(2)
            self.assertEqual(len(reports), 2)
            self.assertEqual(reports[0], reports[1])
            self.assertEqual(reports[0][1:], (job["expires_at"], case.store._cdn_guard_fds))
            self.assertEqual(len(reports[0][2]), 2)
            self.assertEqual(len(messages), 1)
            self.assertEqual(state.job_status(case.scope, group["job_id"])["state"], "partial")
            status = state.group_status(case.scope, group["group_id"])
            self.assertTrue(status["can_continue"])
            for line in (log for log in case.logs if log.startswith("cdn_transport ")):
                self.assertNotIn("synthetic.invalid", line)

    def test_group_cancel_during_upload_never_enters_final_post(self):
        with active_sender() as case:
            group = self.admit_group(case)
            messages, _ = case.send_spy()
            state = runtime.ledger(case.store)

            def cancel(url, encrypted, **options):
                options["check"]()
                state.cancel_group(case.scope, group["group_id"])
                with self.assertRaises(OperationAborted) as raised:
                    options["check"]()
                self.assertEqual(raised.exception.code, "cancelled")
                raise raised.exception

            with patch.object(cdn_process, "upload", side_effect=cancel) as upload:
                case.flush()
            upload.assert_called_once()
            self.assertEqual(len(messages), 1)
            self.assertEqual(state.group_status(case.scope, group["group_id"])["state"], "cancelled")

    def test_group_full_url_404_keeps_one_fresh_target_and_one_final_post(self):
        with active_sender() as case:
            group = self.admit_group(case)
            messages, _ = case.send_spy()
            original_http = media.http_json
            target_keys = []
            attempts = []

            def http(method, url, headers, body, **options):
                if url.endswith("/getuploadurl"):
                    self.assertEqual((options["timeout"], options["retries"]), (30, 1))
                    target_keys.append(body["filekey"])
                    if len(target_keys) == 1:
                        return {"ret": 0, "upload_full_url": "https://synthetic.invalid/first"}
                    return {"ret": 0, "upload_param": "synthetic-fallback"}
                self.assertEqual(options["retries"], 1)
                return original_http(method, url, headers, body, **options)

            def upload(url, encrypted, **options):
                options["check"]()
                attempts.append((url, options["deadline"]))
                if len(attempts) == 1:
                    raise cdn_process.CDNUploadError(404)
                return 200, "synthetic-param"

            with patch.object(media, "http_json", side_effect=http), \
                    patch.object(cdn_process, "upload", side_effect=upload):
                case.flush()
            self.assertEqual(len(set(target_keys)), 2)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(attempts[0][1], attempts[1][1])
            self.assertIn("encrypted_query_param=synthetic-fallback", attempts[1][0])
            self.assertEqual(len(messages), 2)
            state = runtime.ledger(case.store)
            self.assertEqual(state.group_status(case.scope, group["group_id"])["state"], "complete")

    def test_group_final_loss_uses_original_unknown_accounting(self):
        with active_sender() as case:
            group = self.admit_group(case)
            messages, _ = case.send_spy()
            original_http = media.http_json

            def lost(method, url, headers, body, **options):
                response = original_http(method, url, headers, body, **options)
                if url.endswith("/sendmessage"):
                    self.assertEqual(options["retries"], 1)
                    raise TimeoutError("SYNTHETIC final response lost")
                return response

            with patch.object(cdn_process, "upload", return_value=(200, "synthetic-param")) as upload, \
                    patch.object(media, "http_json", side_effect=lost):
                case.flush()
                case.flush()
            upload.assert_called_once()
            self.assertEqual(len(messages), 2)  # One caption and one final media request.
            state = runtime.ledger(case.store)
            self.assertEqual(state.group_status(case.scope, group["group_id"])["state"], "unknown")

    def test_ordinary_generated_media_respects_original_expiry_and_stop(self):
        with active_sender() as case:
            case.file()
            batch = case.batch()
            generated = case.home / "generated.pdf"
            generated.write_bytes(b"%PDF-1.4\nSYNTHETIC\n%%EOF\n")
            result = case.worker.reply(batch["owned"], "", batch["lease"], str(generated))
            path = case.base / "outbox/pending" / result["queued"]
            job = load_json(str(path))
            expiry = job["enqueued_at"] + serve.MAX_JOB_AGE
            messages, _ = case.send_spy()
            stopped = threading.Event()

            def cancel(url, encrypted, **options):
                self.assertEqual(options["wall_deadline"], expiry)
                options["check"]()
                stopped.set()
                options["check"]()

            with patch.object(cdn_process, "upload", side_effect=cancel) as upload:
                self.assertTrue(serve.flush_outbox(case.store, "https://synthetic.invalid",
                                                    case.session["bot_token"], stopped))
            upload.assert_called_once()
            self.assertEqual(messages, [])
            self.assertTrue(path.is_file())
            self.assertEqual(load_json(str(path))["enqueued_at"], job["enqueued_at"])
            self.assertLess(time.time(), expiry)

    def test_serve_does_not_recover_or_restart_flusher_after_unconfirmed_exit(self):
        case = fixture.RuntimeTests("runTest")
        case.setUp()
        handles = len(serve._lock_fhs)
        entered = threading.Event()
        stop_signals = []

        def fatal(store, baseurl, token, stop_event):
            stop_signals.append(stop_event)
            entered.set()
            raise cdn_process.CDNReapError()

        def poll(*args, **kwargs):
            if not entered.wait(2) or not stop_signals[0].wait(2):
                raise KeyboardInterrupt
            return {"msgs": []}

        try:
            with patch.object(serve, "_bot_lock_root", return_value=str(case.home / "bot-locks")), \
                    patch.object(serve, "FLUSH_INTERVAL_SECONDS", 0.001), \
                    patch.object(serve, "flush_outbox", side_effect=fatal) as flush, \
                    patch.object(serve, "_recover_sending", wraps=serve._recover_sending) as recover, \
                    patch.object(serve, "http_json", side_effect=poll):
                self.assertEqual(serve.run(case.store, "https://synthetic.invalid",
                                           case.session["bot_token"], fixture.OWNER), 1)
            flush.assert_called_once()
            recover.assert_called_once()  # Startup only.
        finally:
            for handle in serve._lock_fhs[handles:]:
                handle.close()
            del serve._lock_fhs[handles:]
            case.doCleanups()


if __name__ == "__main__":
    unittest.main()
