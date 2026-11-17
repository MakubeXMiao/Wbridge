"""Optional bounded recent-context snapshots, independent of business ledgers."""
import copy
import hashlib
import math
import re

from . import context_files as files
from .context_files import ContextError
from .store import safe_message_id

TTL_SECONDS = 48 * 3600
MAX_ENTRIES = 10
MAX_ENTRY_BYTES = 8 << 10
MAX_SNAPSHOT_BYTES = 96 << 10
MAX_PROJECTION_BYTES = 12 << 10
MAX_QUOTE_BYTES = 2 << 10
NAME = 'recent.json'
TEMP = '.recent.json.tmp'
_HEX = re.compile(r'[0-9a-f]{64}')
_FIELDS = {'topic': 48, 'intent': 80, 'bot_action': 128, 'open_loop': 128}
_SCOPE = {'bot_id', 'owner_id', 'conversation_id', 'login_epoch'}
_ENTRY_KEYS = {'schema_version', 'id', 'revision', 'topic', 'intent',
    'bot_action', 'open_loop', 'sources', 'source_at', 'expires_at',
    'cutoff_seq', 'historical_refs', 'admission', 'quotes'}


def _require(condition, code='invalid_entry'):
    if not condition:
        raise ContextError(code)


def _time(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _string(value, limit=512, empty=False):
    return (type(value) is str and (empty or bool(value)) and len(value) <= limit
            and not any(0xD800 <= ord(ch) <= 0xDFFF for ch in value))


def _mid(value):
    try:
        return type(value) is str and safe_message_id(value) == value
    except ValueError:
        return False


def _hex(value):
    return type(value) is str and _HEX.fullmatch(value) is not None


def _scope(scope):
    _require(type(scope) is dict and set(scope) == _SCOPE and
             all(_string(v) for v in scope.values()), 'scope_unavailable')
    return dict(scope)


def validate_entry(value):
    """Return a detached strict entry. This does not grant publication authority."""
    _require(type(value) is dict and set(value) == _ENTRY_KEYS)
    _require(type(value['schema_version']) is int and value['schema_version'] == 1)
    _require(_hex(value['id']) and type(value['revision']) is int and value['revision'] >= 1)
    _require(all(_string(value[k], maximum, empty=True) for k, maximum in _FIELDS.items()))
    _require(sum(len(value[k]) for k in _FIELDS) <= 256)
    sources = value['sources']
    _require(type(sources) is list and 1 <= len(sources) <= 1000)
    mids, seqs, times = set(), set(), []
    for source in sources:
        _require(type(source) is dict and set(source) ==
                 {'message_id', 'seq', 'received_at', 'fingerprint'})
        mid, seq, at = source['message_id'], source['seq'], source['received_at']
        _require(_mid(mid))
        _require(mid not in mids and type(seq) is int and seq > 0 and seq not in seqs)
        _require(_time(at) and _hex(source['fingerprint']))
        mids.add(mid); seqs.add(seq); times.append(at)
    _require(_time(value['source_at']) and value['source_at'] == min(times))
    _require(_time(value['expires_at']) and
             value['expires_at'] == value['source_at'] + TTL_SECONDS)
    _require(type(value['cutoff_seq']) is int and value['cutoff_seq'] == max(seqs))
    refs = value['historical_refs']
    _require(type(refs) is list and len(refs) <= MAX_ENTRIES)
    ref_ids = set()
    for ref in refs:
        _require(type(ref) is dict and set(ref) == {'entry_id', 'expires_at'})
        _require(_hex(ref['entry_id']) and _time(ref['expires_at']) and
                 ref['entry_id'] not in ref_ids and ref['entry_id'] != value['id'])
        ref_ids.add(ref['entry_id'])
    admission = value['admission']
    _require(type(admission) is dict and set(admission) == {'kind', 'job_id', 'group_id'})
    _require(admission['kind'] in ('reply', 'attachment') and _string(admission['job_id']))
    _require(admission['group_id'] is None or _string(admission['group_id']))
    quotes = value['quotes']
    _require(type(quotes) is list and len(quotes) <= 3)
    users = bots = size = 0
    for quote in quotes:
        _require(type(quote) is dict and set(quote) ==
                 {'speaker', 'message_id', 'text', 'truncated'})
        _require(quote['speaker'] in ('user', 'bot') and
                 _string(quote['text'], MAX_QUOTE_BYTES, empty=True) and
                 type(quote['truncated']) is bool)
        _require(_mid(quote['message_id']))
        if quote['speaker'] == 'user':
            users += 1
            _require(quote['message_id'] in mids)
        else:
            bots += 1
            _require(quote['message_id'] in mids)
        size += len(quote['text'].encode('utf-8'))
    _require(users <= 2 and bots <= 1 and size <= MAX_QUOTE_BYTES)
    _require(len(files.encode(value)) <= MAX_ENTRY_BYTES, 'entry_too_large')
    return copy.deepcopy(value)


def _snapshot(raw, scope, now):
    _require(type(raw) is dict and set(raw) ==
             {'schema_version', 'scope', 'checked_at', 'high_water_seq', 'entries'}, 'storage_invalid')
    _require(type(raw['schema_version']) is int and raw['schema_version'] == 1, 'storage_invalid')
    _scope(raw['scope'])
    _require(raw['scope'] == scope, 'scope_mismatch')
    _require(_time(raw['checked_at']) and _time(now), 'storage_invalid')
    _require(now >= raw['checked_at'], 'clock_anomaly')
    _require(type(raw['high_water_seq']) is int and raw['high_water_seq'] >= 0, 'storage_invalid')
    _require(type(raw['entries']) is list and len(raw['entries']) <= MAX_ENTRIES, 'storage_invalid')
    entries = [validate_entry(v) for v in raw['entries']]
    _require(len({v['id'] for v in entries}) == len(entries), 'storage_invalid')
    cuts = [v['cutoff_seq'] for v in entries]
    _require(cuts == sorted(set(cuts)) and (not cuts or max(cuts) <= raw['high_water_seq']), 'storage_invalid')
    _require(all(all(s['received_at'] <= raw['checked_at'] for s in v['sources']) for v in entries), 'storage_invalid')
    _require(len(files.encode(raw)) <= MAX_SNAPSHOT_BYTES, 'storage_too_large')
    return dict(raw, entries=entries)


def _empty(scope, now):
    return {'schema_version': 1, 'scope': scope, 'checked_at': now,
            'high_water_seq': 0, 'entries': []}


def _active(entry, now):
    return entry['source_at'] <= now < entry['expires_at']


def _projection(snapshot, now):
    rows = []
    for value in snapshot['entries']:
        if not _active(value, now):
            continue
        row = {k: copy.deepcopy(v) for k, v in value.items() if k != 'quotes'}
        row['quote_count'] = len(value['quotes'])
        # Expired references do not become live again because a newer summary
        # mentioned them. Their source expiry remains available while live.
        row['historical_refs'] = [r for r in row['historical_refs'] if now < r['expires_at']]
        rows.append(row)
    result = {'schema_version': 1, 'snapshot_id': '', 'entries': rows,
              'high_water_seq': snapshot['high_water_seq']}
    while True:
        result['snapshot_id'] = hashlib.sha256(files.encode({k: v for k, v in result.items()
                                                           if k != 'snapshot_id'})).hexdigest()
        if len(files.encode(result)) <= MAX_PROJECTION_BYTES:
            return result
        if not rows:
            raise ContextError('projection_too_large')
        rows.pop(0)


def read(store, scope, now):
    scope = _scope(scope)
    _require(_time(now), 'clock_anomaly')
    with files.locked(store) as directory:
        files.check(directory, TEMP)
        raw = files.read(directory, NAME, MAX_SNAPSHOT_BYTES)
        snapshot = _empty(scope, now) if raw is None else _snapshot(raw, scope, now)
        return _projection(snapshot, now)


def get_entry(store, scope, entry_id, now):
    """Private material only. The Worker wrapper separately checks a live basis."""
    scope = _scope(scope)
    _require(_hex(entry_id), 'entry_unavailable')
    with files.locked(store) as directory:
        files.check(directory, TEMP)
        raw = files.read(directory, NAME, MAX_SNAPSHOT_BYTES)
        if raw is None:
            return None
        snapshot = _snapshot(raw, scope, now)
        for entry in snapshot['entries']:
            if entry['id'] == entry_id and _active(entry, now):
                return entry
    return None


def append(store, scope, entry, now):
    """Append one proven batch after business queued state is safely saved."""
    scope = _scope(scope)
    _require(_time(now), 'clock_anomaly')
    entry = validate_entry(entry)
    if not _active(entry, now) or any(s['received_at'] > now for s in entry['sources']):
        return {'status': 'skipped', 'reason': 'source_expired'}
    with files.locked(store, create=True) as directory:
        # Resolve authority again after taking the lock. A capsule from an old
        # queued worker may outlive its lease, but never its login scope.
        from .attachment_runtime import current_scope
        try:
            actual_scope = current_scope(store)
        except Exception:
            return {'status': 'skipped', 'reason': 'scope_unavailable'}
        if actual_scope != scope:
            return {'status': 'skipped', 'reason': 'scope_mismatch'}
        files.check(directory, TEMP)
        raw = files.read(directory, NAME, MAX_SNAPSHOT_BYTES)
        snapshot = _empty(scope, now) if raw is None else _snapshot(raw, scope, now)
        if any(v['id'] == entry['id'] for v in snapshot['entries']):
            return {'status': 'skipped', 'reason': 'duplicate_batch'}
        if entry['cutoff_seq'] <= snapshot['high_water_seq']:
            return {'status': 'skipped', 'reason': 'late_batch'}
        previous = {v['id']: v for v in snapshot['entries'] if _active(v, now)}
        if any(r['entry_id'] not in previous or r['expires_at'] != previous[r['entry_id']]['expires_at']
               for r in entry['historical_refs']):
            return {'status': 'skipped', 'reason': 'historical_reference_unavailable'}
        rows = [v for v in snapshot['entries'] if _active(v, now)] + [entry]
        rows = rows[-MAX_ENTRIES:]
        updated = {'schema_version': 1, 'scope': scope, 'checked_at': now,
                   'high_water_seq': entry['cutoff_seq'], 'entries': rows}
        if len(files.encode(updated)) > MAX_SNAPSHOT_BYTES:
            return {'status': 'skipped', 'reason': 'snapshot_too_large'}
        files.write(directory, NAME, updated, MAX_SNAPSHOT_BYTES)
        return {'status': 'applied', 'entry_id': entry['id'],
                'snapshot_id': _projection(updated, now)['snapshot_id']}


def maintain(store, scope, now):
    """Inspect only recent.json and its temp. No runtime lifecycle changes."""
    _require(_time(now), 'clock_anomaly')
    if scope is not None:
        scope = _scope(scope)
    with files.locked(store) as directory:
        if directory is None:
            return {'status': 'absent', 'removed': 0}
        # A crash leftover is never an authoritative snapshot. Safe oversized
        # temporary files can be unlinked without loading their content.
        removed = int(files.remove(directory, TEMP, MAX_SNAPSHOT_BYTES))
        try:
            raw = files.read(directory, NAME, MAX_SNAPSHOT_BYTES)
            if raw is None:
                return {'status': 'absent', 'removed': removed}
            if scope is None:
                reason = 'scope_unavailable'
                raise ContextError(reason)
            snapshot = _snapshot(raw, scope, now)
        except ContextError as error:
            removed += int(files.remove(directory, NAME, MAX_SNAPSHOT_BYTES))
            return {'status': 'cleared', 'reason': error.code, 'removed': removed}
        rows = [v for v in snapshot['entries'] if _active(v, now)]
        if not rows:
            removed += int(files.remove(directory, NAME, MAX_SNAPSHOT_BYTES))
            return {'status': 'cleared', 'reason': 'expired', 'removed': removed}
        if len(rows) != len(snapshot['entries']):
            snapshot.update(entries=rows, checked_at=now)
            files.write(directory, NAME, snapshot, MAX_SNAPSHOT_BYTES)
            return {'status': 'pruned', 'removed': removed,
                    'entries_removed': len(raw['entries']) - len(rows)}
        if now != snapshot['checked_at']:
            snapshot['checked_at'] = now
            files.write(directory, NAME, snapshot, MAX_SNAPSHOT_BYTES)
        return {'status': 'unchanged', 'removed': removed}
