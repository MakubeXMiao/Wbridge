"""Opt-in attempt caps over the existing supervised CDN path."""

import math
import os
import time

from .cdn_process import CDNUploadError
from .cdn_trial_budget import BudgetError, TOTAL_MS, TrialBudget
from .ilink import MAX_MEDIA_BYTES
from .progress import OperationAborted


TRIAL_ENV = "WECHAT_CDN_TIER_TRIAL"
_MIB = 1 << 20


class TrialAborted(OperationAborted):
    """An enrolled unit must not bypass its persistent limit."""

    def __init__(self, reason):
        self.trial_reason = reason
        super().__init__("expired" if reason == "cdn_trial_budget_exhausted" else "cancelled")


def _attempt_policy(plaintext_bytes):
    if type(plaintext_bytes) is not int or not 0 <= plaintext_bytes <= MAX_MEDIA_BYTES:
        raise TrialAborted("cdn_trial_invalid_size")
    if plaintext_bytes <= 5 * _MIB:
        return 60_000, None
    if plaintext_bytes <= 20 * _MIB:
        return 180_000, 60
    return TOTAL_MS, 60


def upload_attempt(state_dir, client_key, plaintext_bytes, *, enabled, send, check,
                   deadline, wall_deadline):
    """Reserve before a trial attempt; retain debit if its outcome is unconfirmed."""
    budget = TrialBudget(state_dir)
    try:
        if not enabled and not budget.has_unit(client_key):
            return send()
        limit_ms, idle_timeout = _attempt_policy(plaintext_bytes)
        check()
        remaining = min(deadline - time.monotonic(), wall_deadline - time.time())
        allowed_ms = math.floor(min(limit_ms / 1000, remaining) * 1000)
        if allowed_ms <= 0:
            raise OperationAborted("expired")
        reservation = budget.reserve(client_key, allowed_ms, plaintext_bytes=plaintext_bytes)
        started = time.monotonic()
        wall_started = time.time()
        grant = reservation.granted_ms / 1000

        def settle():
            elapsed = math.ceil(max(0, time.monotonic() - started, time.time() - wall_started) * 1000)
            budget.settle(reservation, min(reservation.granted_ms, elapsed))

        try:
            response = send(attempt_deadline=started + grant, attempt_wall_deadline=wall_started + grant,
                            write_idle_timeout=idle_timeout)
        except CDNUploadError as error:
            if error.confirmed:
                settle()
            raise
        settle()
        return response
    except BudgetError as error:
        raise TrialAborted(error.code) from None


def trial_enabled():
    return os.environ.get(TRIAL_ENV) == "1"
