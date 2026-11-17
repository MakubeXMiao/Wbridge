"""Conservative elapsed-time accounting for opt-in CDN upload attempts."""

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
import re
import uuid

from .delivery_failures import _directory, _private_file, _unique_pairs
from .ilink import MAX_MEDIA_BYTES


TOTAL_MS = 300_000
_LEDGER = "cdn-trial-budget.json"
_MARKER = "cdn-trial-budget.initialized"
_MARKER_BYTES = b"cdn-trial-budget-v1\n"
_MAX_LEDGER_BYTES = 4 << 20
_DIGEST = re.compile(r"[0-9a-f]{64}")
_RESERVATION_ID = re.compile(r"[0-9a-f]{32}")
_UNAVAILABLE = "cdn_trial_budget_unavailable"
_EXHAUSTED = "cdn_trial_budget_exhausted"


class BudgetError(RuntimeError):
    def __init__(self, code=_UNAVAILABLE):
        self.code = _EXHAUSTED if code == _EXHAUSTED else _UNAVAILABLE
        super().__init__(self.code)


@dataclass(frozen=True)
class Reservation:
    id: str
    granted_ms: int
    _unit: str


def _integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def _hex(pattern, value):
    return type(value) is str and pattern.fullmatch(value) is not None


def _unit_key(client_key):
    if type(client_key) is not str or not client_key or len(client_key) > 4096:
        raise BudgetError()
    try:
        raw = client_key.encode("utf-8")
    except UnicodeError:
        raise BudgetError() from None
    return hashlib.sha256(b"cdn-trial-unit-v1\x00" + raw).hexdigest()


def _read(state, name, limit):
    try:
        with _private_file(state, name) as (stream, info):
            if info.st_size > limit:
                raise BudgetError()
            data = stream.read(limit + 1)
            if len(data) != info.st_size:
                raise BudgetError()
            return data
    except FileNotFoundError:
        return None


def _temp_name(name):
    return "." + name + ".tmp"


def _publish(state, name, data):
    # A fixed temporary name bounds crash leftovers. Reuse it only under
    # the exclusive state lock and after validating any orphan as private.
    temp = _temp_name(name)
    if _read(state, temp, _MAX_LEDGER_BYTES) is not None:
        os.unlink(temp, dir_fd=state)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(temp, flags, 0o600, dir_fd=state)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, name, src_dir_fd=state, dst_dir_fd=state)
        os.fsync(state)
    finally:
        try:
            os.unlink(temp, dir_fd=state)
        except FileNotFoundError:
            pass


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


def _checksum(units):
    return hashlib.sha256(_encoded(units)).hexdigest()


def _validate(units):
    if type(units) is not dict:
        raise BudgetError()
    for key, row in units.items():
        if not _hex(_DIGEST, key) or type(row) is not list or len(row) != 5:
            raise BudgetError()
        used, ident, granted, elapsed, plaintext_bytes = row
        if (not _integer(used, 1, TOTAL_MS) or not _hex(_RESERVATION_ID, ident)
                or not _integer(granted, 1, TOTAL_MS)
                or not _integer(plaintext_bytes, 0, MAX_MEDIA_BYTES)
                or (elapsed is not None and not _integer(elapsed, 1, granted))
                or used < (granted if elapsed is None else elapsed)
                or (elapsed is not None and used - elapsed + granted > TOTAL_MS)):
            raise BudgetError()


def _load(state):
    marker = _read(state, _MARKER, len(_MARKER_BYTES))
    data = _read(state, _LEDGER, _MAX_LEDGER_BYTES)
    marker_temp = _read(state, _temp_name(_MARKER), len(_MARKER_BYTES))
    ledger_temp = _read(state, _temp_name(_LEDGER), _MAX_LEDGER_BYTES)
    if marker is None and data is None:
        if marker_temp is not None or ledger_temp is not None:
            raise BudgetError()
        return None
    if marker != _MARKER_BYTES or data is None:
        raise BudgetError()
    document = json.loads(data, object_pairs_hook=_unique_pairs)
    if (type(document) is not dict or set(document) != {"schema_version", "units", "sha256"}
            or not _integer(document["schema_version"], 1, 1)):
        raise BudgetError()
    units = document["units"]
    _validate(units)
    if document["sha256"] != _checksum(units):
        raise BudgetError()
    return units


def _save(state, units):
    data = _encoded({"schema_version": 1, "units": units, "sha256": _checksum(units)})
    if len(data) > _MAX_LEDGER_BYTES:
        raise BudgetError()
    _publish(state, _LEDGER, data)


class TrialBudget:
    """A private ledger for one state root. It never owns a network attempt."""

    def __init__(self, state_dir):
        self._state_dir = state_dir

    @contextmanager
    def _locked(self, *, write=False):
        try:
            with _directory(self._state_dir, private=True) as state:
                kind = fcntl.LOCK_EX if write else fcntl.LOCK_SH
                fcntl.flock(state, kind | fcntl.LOCK_NB)
                yield state, _load(state)
        except (OSError, ValueError, RecursionError):
            raise BudgetError() from None

    def has_unit(self, client_key):
        """Read without creating any files, including for a fresh state root."""
        # The sole sender owns enrollment. Preserve the legacy path when
        # neither file exists, even if its state directory predates the trial.
        try:
            for name in (_MARKER, _LEDGER, _temp_name(_MARKER), _temp_name(_LEDGER)):
                try:
                    os.lstat(os.path.join(self._state_dir, name))
                except FileNotFoundError:
                    continue
                break
            else:
                return False
        except OSError:
            raise BudgetError() from None
        unit = _unit_key(client_key)
        with self._locked() as (_, units):
            return units is not None and unit in units

    def reserve(self, client_key, limit_ms, *, plaintext_bytes):
        """Durably charge the entire grant before returning a launch permit."""
        unit = _unit_key(client_key)
        if (not _integer(limit_ms, 1, TOTAL_MS)
                or not _integer(plaintext_bytes, 0, MAX_MEDIA_BYTES)):
            raise BudgetError()
        with self._locked(write=True) as (state, units):
            if units is None:
                _publish(state, _MARKER, _MARKER_BYTES)
                units = {}
            previous = units.get(unit)
            if previous is not None and previous[4] != plaintext_bytes:
                raise BudgetError()
            used = previous[0] if previous is not None else 0
            grant = min(limit_ms, TOTAL_MS - used)
            if grant <= 0:
                raise BudgetError(_EXHAUSTED)
            ident = uuid.uuid4().hex
            # Replacing an unfinished reservation leaves its full charge in
            # used. A late return cannot refund that superseded reservation.
            units[unit] = [used + grant, ident, grant, None, plaintext_bytes]
            _save(state, units)
            return Reservation(ident, grant, unit)

    def settle(self, reservation, elapsed_ms):
        """Refund only the caller's confirmed unused grant after real reap."""
        if (type(reservation) is not Reservation
                or not _hex(_RESERVATION_ID, reservation.id)
                or not _hex(_DIGEST, reservation._unit)
                or not _integer(reservation.granted_ms, 1, TOTAL_MS)
                or not _integer(elapsed_ms, 0, reservation.granted_ms)):
            raise BudgetError()
        charged = max(1, elapsed_ms)
        with self._locked(write=True) as (state, units):
            row = (units or {}).get(reservation._unit)
            if row is None or row[1:3] != [reservation.id, reservation.granted_ms]:
                raise BudgetError()
            if row[3] is not None:
                if row[3] != charged:
                    raise BudgetError()
                return
            row[0] -= reservation.granted_ms - charged
            row[3] = charged
            _save(state, units)
