"""Bounded, real operation checkpoints. They never renew a worker lease."""


class OperationAborted(RuntimeError):
    """A cancellation/authority loss which retry loops must never swallow."""

    def __init__(self, code="cancelled"):
        if code not in ("cancelled", "lease_lost", "epoch_mismatch", "expired"):
            code = "cancelled"
        self.code = code
        super().__init__(code)


def checkpoint(callback, stage, completed=0, total=None):
    if callback is not None:
        callback({"schema_version": 1, "stage": stage,
                  "completed": completed, "total": total})
