"""Local, durable trial accounting across independent send attempts."""

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import cdn_trial_budget as budget


class TrialBudgetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name) / "state"
        self.state.mkdir(mode=0o700)
        self.ledger = budget.TrialBudget(self.state)

    def reserve(self, client_key, limit_ms, *, plaintext_bytes=4 << 20):
        return self.ledger.reserve(client_key, limit_ms, plaintext_bytes=plaintext_bytes)

    def test_fresh_read_is_side_effect_free_and_enrollment_is_per_unit(self):
        self.assertFalse(self.ledger.has_unit("synthetic-unit"))
        self.assertEqual(list(self.state.iterdir()), [])
        first = self.reserve("synthetic-unit", 60_000)
        self.assertEqual(first.granted_ms, 60_000)
        self.assertTrue(budget.TrialBudget(self.state).has_unit("synthetic-unit"))
        self.assertFalse(self.ledger.has_unit("other-synthetic-unit"))
        self.assertNotIn(b"synthetic-unit", (self.state / budget._LEDGER).read_bytes())
        self.assertEqual((self.state / budget._LEDGER).stat().st_mode & 0o777, 0o600)
        other_root = self.state.with_name("other")
        other_root.mkdir(mode=0o700)
        self.assertFalse(budget.TrialBudget(other_root).has_unit("synthetic-unit"))

    def test_absent_trial_preserves_legacy_state_permissions_and_lock(self):
        self.state.chmod(0o755)
        fd = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertFalse(self.ledger.has_unit("synthetic-unit"))
        finally:
            os.close(fd)
        self.assertEqual(list(self.state.iterdir()), [])
        with patch.object(budget.os, "lstat", side_effect=PermissionError("sensitive path")):
            with self.assertRaises(budget.BudgetError) as caught:
                self.ledger.has_unit("synthetic-unit")
        self.assertNotIn("sensitive path", str(caught.exception))

    def test_two_attempts_share_remaining_time_and_settle_once(self):
        first = self.reserve("synthetic-unit", 180_000)
        self.ledger.settle(first, 130_000)
        self.ledger.settle(first, 130_000)
        with self.assertRaises(budget.BudgetError):
            self.ledger.settle(first, 100_000)
        second = budget.TrialBudget(self.state).reserve("synthetic-unit", 300_000, plaintext_bytes=4 << 20)
        self.assertEqual(second.granted_ms, 170_000)
        self.assertNotEqual(second.id, first.id)
        self.ledger.settle(second, 170_000)
        with self.assertRaises(budget.BudgetError) as caught:
            self.reserve("synthetic-unit", 60_000)
        self.assertEqual(caught.exception.code, "cdn_trial_budget_exhausted")
        self.assertEqual(self.reserve("other-unit", 300_000).granted_ms, 300_000)

    def test_real_abrupt_process_exit_preserves_full_predebit(self):
        child = """
import os, sys
sys.path.insert(0, sys.argv[1])
from wechat_bridge.cdn_trial_budget import TrialBudget
assert TrialBudget(sys.argv[2]).reserve("synthetic-unit", 180_000, plaintext_bytes=4 << 20).granted_ms == 180_000
os._exit(23)
"""
        result = subprocess.run(
            [sys.executable, "-I", "-B", "-c", child, str(ROOT), str(self.state)],
            check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 23)
        self.assertTrue(self.ledger.has_unit("synthetic-unit"))
        resumed = self.reserve("synthetic-unit", 180_000)
        self.assertEqual(resumed.granted_ms, 120_000)
        with self.assertRaises(budget.BudgetError) as caught:
            self.reserve("synthetic-unit", 1)
        self.assertEqual(caught.exception.code, "cdn_trial_budget_exhausted")

    def test_late_completion_never_refunds_an_unconfirmed_old_attempt(self):
        first = self.reserve("synthetic-unit", 180_000)
        second = self.reserve("synthetic-unit", 180_000)
        self.assertEqual(second.granted_ms, 120_000)
        with self.assertRaises(budget.BudgetError):
            self.ledger.settle(first, 1)
        self.ledger.settle(second, 30_000)
        self.assertEqual(self.reserve("synthetic-unit", 180_000).granted_ms, 90_000)

    def test_enrollment_requires_exact_original_plaintext_size(self):
        size = 20 << 20
        first = self.reserve("synthetic-unit", 180_000, plaintext_bytes=size)
        self.ledger.settle(first, 130_000)
        path = self.state / budget._LEDGER
        before = path.read_bytes()
        for changed in (size - 1, size + 1, -1, 101 << 20, True):
            with self.subTest(changed=changed):
                with self.assertRaises(budget.BudgetError):
                    self.reserve("synthetic-unit", 180_000, plaintext_bytes=changed)
                self.assertEqual(path.read_bytes(), before)
        second = self.reserve("synthetic-unit", 180_000, plaintext_bytes=size)
        self.assertEqual(second.granted_ms, 170_000)

    def test_missing_or_changed_ledger_after_activation_is_unavailable(self):
        self.reserve("synthetic-unit", 60_000)
        path = self.state / budget._LEDGER
        original = path.read_bytes()
        path.write_bytes(original.replace(b"60000", b"10000", 1))
        with self.assertRaises(budget.BudgetError):
            self.ledger.has_unit("synthetic-unit")
        document = json.loads(original)
        row = next(iter(document["units"].values()))
        row[0], row[2], row[3] = 200_000, 300_000, 1000
        document["sha256"] = budget._checksum(document["units"])
        path.write_bytes(budget._encoded(document))
        with self.assertRaises(budget.BudgetError):
            self.ledger.has_unit("synthetic-unit")
        path.unlink()
        with self.assertRaises(budget.BudgetError) as caught:
            self.reserve("synthetic-unit", 60_000)
        self.assertEqual(caught.exception.code, "cdn_trial_budget_unavailable")
        self.assertFalse(path.exists())
        self.assertTrue((self.state / budget._MARKER).is_file())

    def test_orphan_private_temporary_files_cannot_reset_absent_ledger(self):
        for name in (budget._MARKER, budget._LEDGER):
            with self.subTest(name=name):
                pending = self.state / budget._temp_name(name)
                pending.write_bytes(b"")
                pending.chmod(0o600)
                with self.assertRaises(budget.BudgetError):
                    self.ledger.has_unit("synthetic-unit")
                with self.assertRaises(budget.BudgetError):
                    self.reserve("synthetic-unit", 60_000)
                self.assertEqual(list(self.state.iterdir()), [pending])
                pending.unlink()

    def test_torn_private_temp_retains_canonical_and_symlink_temp_fails_closed(self):
        self.reserve("synthetic-unit", 180_000)
        ledger = self.state / budget._LEDGER
        before = ledger.read_bytes()
        for name in (budget._MARKER, budget._LEDGER):
            with self.subTest(name=name):
                pending = self.state / budget._temp_name(name)
                pending.write_bytes(b"torn")
                pending.chmod(0o600)
                self.assertTrue(self.ledger.has_unit("synthetic-unit"))
                self.assertEqual(ledger.read_bytes(), before)
                self.assertEqual(pending.read_bytes(), b"torn")
                pending.unlink()
                pending.symlink_to(ledger)
                with self.assertRaises(budget.BudgetError):
                    self.ledger.has_unit("synthetic-unit")
                self.assertEqual(ledger.read_bytes(), before)
                pending.unlink()
        self.assertEqual(self.reserve("synthetic-unit", 300_000).granted_ms, 120_000)

    def test_unsafe_sidecar_lock_contention_and_storage_full_fail_closed(self):
        first = self.reserve("synthetic-unit", 60_000)
        path = self.state / budget._LEDGER
        initial = path.read_bytes()
        with patch.object(budget, "_MAX_LEDGER_BYTES", len(initial) + 1):
            with self.assertRaises(budget.BudgetError):
                self.reserve("other-unit", 60_000)
        self.assertEqual(path.read_bytes(), initial)
        fd = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(budget.BudgetError):
                self.ledger.has_unit("synthetic-unit")
            with self.assertRaises(budget.BudgetError):
                self.ledger.settle(first, 1000)
        finally:
            os.close(fd)
        extra = self.state / "hard-link"
        os.link(path, extra)
        with self.assertRaises(budget.BudgetError):
            self.ledger.has_unit("synthetic-unit")
        self.assertEqual(extra.read_bytes(), initial)


if __name__ == "__main__":
    unittest.main()
