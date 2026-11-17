"""Exercise trial selection and original send boundaries on temporary state."""

from pathlib import Path
import tempfile
from types import SimpleNamespace
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_cdn_upload_integration as integration
from wechat_bridge import attachment_runtime as runtime, cdn_process, cdn_trial, media
from wechat_bridge.progress import OperationAborted


MIB = 1 << 20


class TrialPolicyTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)
        self.now = 1000
        clock = SimpleNamespace(monotonic=lambda: self.now, time=lambda: 10_000 + self.now)
        mock = patch.object(cdn_trial, "time", clock)
        mock.start()
        self.addCleanup(mock.stop)

    def attempt(self, key, size, send, *, enabled=True, job_remaining=600, wall_remaining=600,
                check=lambda: None):
        return cdn_trial.upload_attempt(self.state, key, size, enabled=enabled, send=send,
                                        check=check, deadline=self.now + job_remaining,
                                        wall_deadline=10_000 + self.now + wall_remaining)

    def test_default_unenrolled_uses_phase_a_and_creates_no_file(self):
        def send(**options):
            self.assertEqual(options, {})
            return 200, "synthetic-ack"

        self.assertEqual(self.attempt("synthetic-key", 29 * MIB, send, enabled=False),
                         (200, "synthetic-ack"))
        self.assertFalse(list(self.state.iterdir()))

    def test_plaintext_exact_tier_edges_and_original_deadline(self):
        cases = ((5 * MIB, 60, None), (5 * MIB + 1, 180, 60), (20 * MIB, 180, 60),
                 (20 * MIB + 1, 300, 60), (29_198_334, 300, 60), (100 * MIB, 300, 60))
        for index, (size, seconds, idle) in enumerate(cases):
            with self.subTest(size=size):
                def send(**options):
                    self.assertEqual(options, {"attempt_deadline": self.now + seconds,
                                               "attempt_wall_deadline": 10_000 + self.now + seconds,
                                               "write_idle_timeout": idle})
                    return 200, "synthetic-ack"

                self.attempt("synthetic-%d" % index, size, send)
        for job, wall in ((73, 120), (120, 73)):
            with self.subTest(job=job, wall=wall):
                def capped(**options):
                    self.assertEqual(options["attempt_deadline"], self.now + 73)
                    return 200, "synthetic-ack"

                self.attempt("deadline-%d" % job, 29 * MIB, capped, job_remaining=job, wall_remaining=wall)

    def test_confirmed_retry_and_flag_off_recovery_share_original_unit_cap(self):
        key = "same-logical-unit"

        def first(**options):
            self.assertEqual(options["attempt_deadline"] - self.now, 300)
            self.now += 87
            raise cdn_process.CDNUploadError(500, confirmed=True)

        with self.assertRaises(cdn_process.CDNUploadError):
            self.attempt(key, 29 * MIB, first)

        def second(**options):
            self.assertEqual(options["attempt_deadline"] - self.now, 213)
            self.now += 120
            return 200, "synthetic-ack"

        self.attempt(key, 29 * MIB, second)

        def resumed(**options):
            self.assertEqual(options, {"attempt_deadline": self.now + 93,
                                       "attempt_wall_deadline": 10_000 + self.now + 93,
                                       "write_idle_timeout": 60})
            self.now += 93
            return 200, "synthetic-ack"

        self.attempt(key, 29 * MIB, resumed, enabled=False)
        with self.assertRaises(cdn_trial.TrialAborted) as stopped:
            self.attempt(key, 29 * MIB, lambda **kw: self.fail("exhausted unit reached transport"))
        self.assertEqual(stopped.exception.trial_reason, "cdn_trial_budget_exhausted")

    def test_flag_off_keeps_original_mid_tier_and_rejects_size_drift(self):
        key, size = "enrolled-mid", 10 * MIB

        def original(**options):
            self.assertEqual(options["attempt_deadline"] - self.now, 180)
            self.assertEqual(options["write_idle_timeout"], 60)
            self.now += 1
            return 200, "synthetic-ack"

        self.attempt(key, size, original)
        for size_drift in (size - 1, size + 1, 21 * MIB):
            with self.assertRaises(cdn_trial.TrialAborted) as stopped:
                self.attempt(key, size_drift, lambda **kw: self.fail("different size reached transport"),
                             enabled=False)
            self.assertEqual(stopped.exception.trial_reason, "cdn_trial_budget_unavailable")
        self.attempt(key, size, original, enabled=False)

    def test_confirmed_null_status_failure_preserves_old_retry(self):
        key = "confirmed-no-status"

        def failed(**options):
            self.now += 1
            raise cdn_process.CDNUploadError(confirmed=True)

        with self.assertRaises(cdn_process.CDNUploadError):
            self.attempt(key, 29 * MIB, failed)

        def retried(**options):
            self.assertEqual(options["attempt_deadline"] - self.now, 299)
            return 200, "synthetic-ack"

        self.attempt(key, 29 * MIB, retried)

    def test_unconfirmed_execution_retains_debit_across_restart(self):
        for error in (cdn_process.CDNReapError(), cdn_process.CDNUploadError()):
            key = type(error).__name__

            def lost(**options):
                self.now += 1
                raise error

            with self.assertRaises(type(error)):
                self.attempt(key, 29 * MIB, lost)
            with self.assertRaises(cdn_trial.TrialAborted):
                self.attempt(key, 29 * MIB, lambda **kw: self.fail("old debit was reset"), enabled=False)


class TrialSendTests(unittest.TestCase):
    def test_managed_small_plaintext_boundary_ignores_encryption_padding(self):
        with integration.active_sender() as case, patch.dict("os.environ", {cdn_trial.TRIAL_ENV: "1"}):
            aid = case.file(body=b"S" * (5 * MIB))
            batch = case.batch()
            group = case.success(case.call(batch, "reply", {"attachment_ids": [aid]}))
            messages, _ = case.send_spy()
            received = []

            def uploaded(url, encrypted, **options):
                received.append((len(encrypted), options["write_idle_timeout"],
                                 options["attempt_deadline"] - time.monotonic()))
                return 200, "synthetic-ack"

            with patch.object(cdn_process, "upload", side_effect=uploaded):
                case.flush()
            self.assertEqual(len(messages), 1)
            self.assertEqual(received[0][:2], (5 * MIB + 16, None))
            self.assertGreater(received[0][2], 59)
            self.assertLessEqual(received[0][2], 60)
            self.assertEqual(runtime.ledger(case.store).group_status(case.scope, group["group_id"])["state"],
                             "complete")

    def test_large_confirmed_null_status_preserves_one_existing_retry(self):
        with integration.active_sender() as case, patch.dict("os.environ", {cdn_trial.TRIAL_ENV: "1"}):
            aid = case.file(body=b"S" * (21 * MIB))
            batch = case.batch()
            group = case.success(case.call(batch, "reply", {"attachment_ids": [aid]}))
            messages, _ = case.send_spy()
            limits = []

            def uploaded(url, encrypted, **options):
                limits.append(options["attempt_deadline"] - time.monotonic())
                if len(limits) == 1:
                    raise cdn_process.CDNUploadError(confirmed=True)
                return 200, "synthetic-ack"

            with patch.object(cdn_process, "upload", side_effect=uploaded), \
                    patch.object(media.time, "sleep") as backoff:
                case.flush()
            backoff.assert_called_once_with(2)
            self.assertEqual(len(limits), 2)
            self.assertTrue(all(299 < value < 300 for value in limits))
            self.assertEqual(len(messages), 1)
            self.assertEqual(runtime.ledger(case.store).group_status(case.scope, group["group_id"])["state"],
                             "complete")

    def test_final_ack_loss_stays_unknown_and_never_restarts_cdn(self):
        with integration.active_sender() as case, patch.dict("os.environ", {cdn_trial.TRIAL_ENV: "1"}):
            aid = case.file()
            batch = case.batch()
            group = case.success(case.call(batch, "reply", {"attachment_ids": [aid]}))
            messages, _ = case.send_spy(final_result=TimeoutError("synthetic final ACK lost"))
            with patch.object(cdn_process, "upload", return_value=(200, "synthetic-ack")) as sent:
                case.flush()
                case.flush()
            sent.assert_called_once()
            self.assertEqual(len(messages), 1)
            self.assertEqual(runtime.ledger(case.store).group_status(case.scope, group["group_id"])["state"],
                             "unknown")


if __name__ == "__main__":
    unittest.main()
