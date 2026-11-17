"""Synthetic continuity storage and private-file failure tests. No network."""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge import context_files as files
from wechat_bridge import recent_context as recent
from wechat_bridge.context_files import ContextError


def deny(*a, **kw):
    raise AssertionError('recent context tests forbid sockets')

socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny

SCOPE = {'bot_id': 'synthetic-bot', 'owner_id': 'synthetic-owner',
         'conversation_id': 'synthetic-owner', 'login_epoch': 'a' * 32}


def entry(seq=1, at=1000, **overrides):
    mid = 'synthetic-%d' % seq
    result = {'schema_version': 1, 'id': hashlib.sha256(mid.encode()).hexdigest(),
        'revision': 1, 'topic': '网络', 'intent': '继续排查',
        'bot_action': '已将排查建议入队', 'open_loop': '等待用户检查',
        'sources': [{'message_id': mid, 'seq': seq, 'received_at': at,
                     'fingerprint': 'b' * 64}],
        'source_at': at, 'expires_at': at + recent.TTL_SECONDS,
        'cutoff_seq': seq, 'historical_refs': [],
        'admission': {'kind': 'reply', 'job_id': mid + '.json', 'group_id': None},
        'quotes': [{'speaker': 'user', 'message_id': mid, 'text': '收到原文', 'truncated': False},
                   {'speaker': 'bot', 'message_id': mid, 'text': '已接纳排查建议', 'truncated': False}]}
    result.update(overrides)
    return result


class RecentContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic-context-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve() / 'bridge'
        self.base.mkdir(mode=0o700)
        (self.base / 'state').mkdir(mode=0o700)
        self.store = SimpleNamespace(base=str(self.base))
        self.scope = dict(SCOPE)
        self.current = patch('wechat_bridge.attachment_runtime.current_scope',
                             lambda store: dict(self.scope))
        self.current.start()
        self.addCleanup(self.current.stop)

    @property
    def directory(self):
        return self.base / 'state/continuity'

    def raw(self):
        return json.loads((self.directory / recent.NAME).read_text())

    def save(self, value):
        with files.locked(self.store, create=True) as directory:
            files.write(directory, recent.NAME, value, recent.MAX_SNAPSHOT_BYTES)

    def test_absent_read_and_maintenance_do_not_create_optional_directory(self):
        before = sorted(str(p.relative_to(self.base)) for p in self.base.rglob('*'))
        view = recent.read(self.store, SCOPE, 1001)
        self.assertEqual(view['entries'], [])
        self.assertEqual(recent.maintain(self.store, SCOPE, 1001)['status'], 'absent')
        self.assertEqual(before, sorted(str(p.relative_to(self.base)) for p in self.base.rglob('*')))

    def test_append_read_private_get_and_quote_not_in_auto_projection(self):
        first = entry()
        self.assertEqual(recent.append(self.store, SCOPE, first, 1001)['status'], 'applied')
        view = recent.read(self.store, SCOPE, 1001)
        self.assertEqual(view['entries'][0]['quote_count'], 2)
        self.assertNotIn('quotes', view['entries'][0])
        self.assertNotIn('收到原文', files.encode(view).decode())
        self.assertEqual(recent.get_entry(self.store, SCOPE, first['id'], 1001), first)
        self.assertIsNone(recent.get_entry(self.store, SCOPE, '0' * 64, 1001))
        self.assertEqual((self.directory / recent.NAME).stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(sorted(p.name for p in self.directory.iterdir()), [recent.NAME])

    def test_field_and_combined_unicode_limits_are_both_enforced(self):
        fields = {'topic': '😀' * 48, 'intent': '😀' * 80,
                  'bot_action': '😀' * 64, 'open_loop': '😀' * 64}
        self.assertEqual(recent.append(self.store, SCOPE, entry(**fields), 1001)['status'], 'applied')
        for changed in ({'topic': 'x' * 49}, {'intent': 'x' * 81},
                        {'bot_action': 'x' * 129}, {'open_loop': 'x' * 129},
                        {'topic': 'x' * 48, 'intent': 'x' * 80,
                         'bot_action': 'x' * 128, 'open_loop': 'x'}):
            with self.subTest(changed=changed):
                with self.assertRaises(ContextError):
                    recent.validate_entry(entry(**changed))

    def test_quote_limits_utf8_boundary_speaker_counts_and_exact_schema(self):
        q = [{'speaker': 'user', 'message_id': 'synthetic-1', 'text': '😀' * 512, 'truncated': True}]
        recent.validate_entry(entry(quotes=q))
        negatives = [q + [{'speaker': 'bot', 'message_id': 'synthetic-1', 'text': 'x', 'truncated': False}],
                     [{'speaker': 'user', 'message_id': 'synthetic-1', 'text': 'x', 'truncated': False}] * 3,
                     [{'speaker': 'bot', 'message_id': 'synthetic-1', 'text': 'x', 'truncated': False}] * 2]
        for quotes in negatives:
            with self.subTest(quotes=len(quotes)), self.assertRaises(ContextError):
                recent.validate_entry(entry(quotes=quotes))
        with self.assertRaises(ContextError): recent.validate_entry(entry(revision=True))
        with self.assertRaises(ContextError): recent.validate_entry(entry(revision='1'))
        value = entry(); value['unrecognized'] = 1
        with self.assertRaises(ContextError): recent.validate_entry(value)
        value = entry(); value['quotes'][0]['truncated'] = 1
        with self.assertRaises(ContextError): recent.validate_entry(value)
        value = entry(); value['sources'][0]['seq'] = True
        with self.assertRaises(ContextError): recent.validate_entry(value)
        value = entry(); value['sources'][0]['message_id'] = '../escape'
        with self.assertRaises(ContextError): recent.validate_entry(value)

    def test_earliest_source_expiry_and_future_source_refused(self):
        value = entry()
        value['sources'].append({'message_id': 'synthetic-2', 'seq': 2,
                                'received_at': 1010, 'fingerprint': 'c' * 64})
        value['cutoff_seq'] = 2
        self.assertEqual(recent.append(self.store, SCOPE, value, 1005)['reason'], 'source_expired')
        self.assertEqual(recent.append(self.store, SCOPE, value, 1011)['status'], 'applied')
        boundary = 1000 + recent.TTL_SECONDS
        self.assertEqual(len(recent.read(self.store, SCOPE, boundary - 0.001)['entries']), 1)
        self.assertEqual(recent.read(self.store, SCOPE, boundary)['entries'], [])
        self.assertIsNone(recent.get_entry(self.store, SCOPE, value['id'], boundary))
        self.assertTrue((self.directory / recent.NAME).exists())
        self.assertEqual(recent.maintain(self.store, SCOPE, boundary)['status'], 'cleared')
        self.assertFalse((self.directory / recent.NAME).exists())

    def test_ten_entries_evict_oldest_and_snapshot_size_bounded(self):
        for seq in range(1, 12):
            recent.append(self.store, SCOPE, entry(seq, 1000 + seq), 1100 + seq)
        raw = self.raw()
        self.assertEqual(len(raw['entries']), 10)
        self.assertEqual([r['cutoff_seq'] for r in raw['entries']], list(range(2, 12)))
        self.assertEqual(raw['high_water_seq'], 11)
        self.assertLessEqual((self.directory / recent.NAME).stat().st_size, recent.MAX_SNAPSHOT_BYTES)

    def test_projection_drops_whole_older_entries_at_twelve_kib(self):
        for seq in range(1, 11):
            value = entry(seq, 1000 + seq)
            # Long but legal source metadata produces close to the entry budget.
            value['sources'] = [{'message_id': 's%d-%s' % (i, 'z' * 174),
                                'seq': seq * 100 + i, 'received_at': 1000 + seq,
                                'fingerprint': 'b' * 64} for i in range(1, 18)]
            value['quotes'] = []
            value['cutoff_seq'] = seq * 100 + 17
            recent.append(self.store, SCOPE, value, 1100 + seq)
        view = recent.read(self.store, SCOPE, 1200)
        self.assertEqual(len(self.raw()['entries']), 10)
        self.assertLess(len(view['entries']), 10)
        self.assertGreater(len(view['entries']), 0)
        self.assertEqual(view['entries'][-1]['cutoff_seq'], 1017)
        self.assertLessEqual(len(files.encode(view)), 12 << 10)
        self.assertTrue(all(len(r['sources']) == 17 for r in view['entries']))

    def test_entry_oversize_rejected_before_optional_state_creation(self):
        value = entry()
        value['sources'] = [{'message_id': 's%d-%s' % (i, 'z' * 174), 'seq': i,
                            'received_at': 1000, 'fingerprint': 'b' * 64} for i in range(1, 50)]
        value['cutoff_seq'] = 49; value['quotes'] = []
        with self.assertRaises(ContextError) as error:
            recent.append(self.store, SCOPE, value, 1100)
        self.assertEqual(error.exception.code, 'entry_too_large')
        self.assertFalse(self.directory.exists())

    def test_duplicate_and_late_batch_do_not_replace_new_topic(self):
        new = entry(2, topic='新话题')
        recent.append(self.store, SCOPE, new, 1001)
        self.assertEqual(recent.append(self.store, SCOPE, new, 1002)['reason'], 'duplicate_batch')
        self.assertEqual(recent.append(self.store, SCOPE, entry(1), 1002)['reason'], 'late_batch')
        self.assertEqual(recent.read(self.store, SCOPE, 1003)['entries'][0]['topic'], '新话题')

    def test_historical_references_preserve_original_expiry(self):
        old = entry(1)
        recent.append(self.store, SCOPE, old, 1001)
        new = entry(2, 1100, historical_refs=[{'entry_id': old['id'], 'expires_at': old['expires_at']}])
        self.assertEqual(recent.append(self.store, SCOPE, new, 1101)['status'], 'applied')
        drift = entry(3, 1100, historical_refs=[{'entry_id': old['id'], 'expires_at': old['expires_at'] + 1}])
        self.assertEqual(recent.append(self.store, SCOPE, drift, 1102)['reason'], 'historical_reference_unavailable')
        at = old['expires_at']
        view = recent.read(self.store, SCOPE, at)
        self.assertEqual(len(view['entries']), 1)
        self.assertEqual(view['entries'][0]['historical_refs'], [])
        self.assertEqual(view['entries'][0]['expires_at'], 1100 + recent.TTL_SECONDS)

    def test_scope_change_refuses_old_then_maintenance_permits_new(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        self.scope['login_epoch'] = 'd' * 32
        with self.assertRaises(ContextError) as error:
            recent.read(self.store, self.scope, 1002)
        self.assertEqual(error.exception.code, 'scope_mismatch')
        with self.assertRaises(ContextError): recent.append(self.store, self.scope, entry(2), 1002)
        self.assertEqual(recent.maintain(self.store, self.scope, 1002)['status'], 'cleared')
        self.assertEqual(recent.append(self.store, self.scope, entry(2), 1002)['status'], 'applied')
        self.assertEqual(recent.append(self.store, SCOPE, entry(3), 1003)['reason'], 'scope_mismatch')

    def test_logout_cleanup_without_runtime_identity_does_not_touch_source(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        with files.locked(self.store, source=True, create=True) as directory:
            files.write(directory, 'current.json', {'authority': 'synthetic'}, 4096)
        unknown = self.directory / 'leave-me'; unknown.write_text('synthetic')
        self.assertEqual(recent.maintain(self.store, None, 1002)['status'], 'cleared')
        self.assertTrue(unknown.exists())
        self.assertTrue((self.base / 'state/continuity-source/current.json').exists())

    def test_clock_rollback_rejected_and_maintenance_clears(self):
        recent.append(self.store, SCOPE, entry(), 1010)
        with self.assertRaises(ContextError) as error:
            recent.read(self.store, SCOPE, 1009)
        self.assertEqual(error.exception.code, 'clock_anomaly')
        self.assertEqual(recent.maintain(self.store, SCOPE, 1009)['reason'], 'clock_anomaly')
        self.assertFalse((self.directory / recent.NAME).exists())

    def test_later_maintenance_persists_clock_observation_across_reader_restarts(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        self.assertEqual(recent.maintain(self.store, SCOPE, 2000)['status'], 'unchanged')
        raw = self.raw()
        self.assertEqual(raw['checked_at'], 2000)
        self.assertEqual(raw['entries'][0]['source_at'], 1000)
        self.assertEqual(raw['entries'][0]['expires_at'], 1000 + recent.TTL_SECONDS)
        reopened = SimpleNamespace(base=str(self.base))
        with self.assertRaises(ContextError) as error:
            recent.read(reopened, SCOPE, 1999)
        self.assertEqual(error.exception.code, 'clock_anomaly')
        self.assertEqual(recent.maintain(reopened, SCOPE, 1999)['reason'], 'clock_anomaly')
        self.assertEqual(recent.read(reopened, SCOPE, 3000)['entries'], [])

    def test_lock_busy_is_nonblocking_for_all_entry_points(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        fd = os.open(str(self.directory), os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for action in (lambda: recent.read(self.store, SCOPE, 1002),
                       lambda: recent.append(self.store, SCOPE, entry(2), 1002),
                       lambda: recent.maintain(self.store, SCOPE, 1002)):
            with self.assertRaises(ContextError) as error: action()
            self.assertEqual(error.exception.code, 'storage_busy')

    def test_corrupt_oversize_and_partial_temporary_maintenance(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        path = self.directory / recent.NAME
        path.write_bytes(b'{"broken":')
        with self.assertRaises(ContextError): recent.read(self.store, SCOPE, 1002)
        self.assertEqual(recent.maintain(self.store, SCOPE, 1002)['status'], 'cleared')
        path.write_bytes(b'x' * (recent.MAX_SNAPSHOT_BYTES + 1)); path.chmod(0o600)
        with patch('wechat_bridge.context_files.os.read', side_effect=AssertionError('must not read oversized')):
            with self.assertRaises(ContextError) as error: recent.read(self.store, SCOPE, 1003)
            self.assertEqual(error.exception.code, 'storage_too_large')
            self.assertEqual(recent.maintain(self.store, SCOPE, 1003)['status'], 'cleared')
        temporary = self.directory / recent.TEMP
        temporary.write_bytes(b'partial synthetic private text'); temporary.chmod(0o600)
        self.assertEqual(recent.maintain(self.store, SCOPE, 1004)['removed'], 1)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_symlink_hardlink_permissions_and_nonregular_rejected(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        destination = self.directory / recent.NAME
        external = self.base / 'external'; external.write_text('synthetic'); external.chmod(0o600)
        for kind in ('symlink', 'hardlink', 'permissions', 'fifo'):
            destination.unlink(missing_ok=True)
            if kind == 'symlink': destination.symlink_to(external)
            elif kind == 'hardlink': os.link(str(external), str(destination))
            elif kind == 'permissions': destination.write_text('{}'); destination.chmod(0o644)
            else: os.mkfifo(str(destination), 0o600)
            with self.subTest(kind=kind):
                with self.assertRaises(ContextError): recent.read(self.store, SCOPE, 1002)
                with self.assertRaises(ContextError): recent.maintain(self.store, SCOPE, 1002)
            self.assertEqual(external.read_text(), 'synthetic')
        destination.unlink()

    def test_unknown_names_and_symlink_parent_are_rejected(self):
        with files.locked(self.store, create=True) as directory:
            for name in ('../escape', 'other.json', 'recent.json/child'):
                with self.assertRaises(ContextError): files.read(directory, name, 1024)
                with self.assertRaises(ContextError): files.write(directory, name, {}, 1024)
                with self.assertRaises(ContextError): files.remove(directory, name)
        linked = self.base.parent / 'linked'
        linked.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(ContextError):
            with files.locked(SimpleNamespace(base=str(linked)), create=True): pass

    def test_failed_atomic_replace_keeps_old_authority_then_cleans_temp(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        before = (self.directory / recent.NAME).read_bytes()
        with patch('wechat_bridge.context_files.os.replace', side_effect=OSError('synthetic failure')):
            with self.assertRaises(ContextError): recent.append(self.store, SCOPE, entry(2), 1002)
        self.assertEqual((self.directory / recent.NAME).read_bytes(), before)
        self.assertTrue((self.directory / recent.TEMP).exists())
        self.assertEqual(recent.read(self.store, SCOPE, 1003)['high_water_seq'], 1)
        self.assertEqual(recent.maintain(self.store, SCOPE, 1003)['removed'], 1)

    def test_strict_stored_snapshot_and_duplicate_json_keys(self):
        recent.append(self.store, SCOPE, entry(), 1001)
        path = self.directory / recent.NAME
        raw = self.raw(); raw['high_water_seq'] = True; self.save(raw)
        with self.assertRaises(ContextError): recent.read(self.store, SCOPE, 1002)
        path.write_bytes(b'{"schema_version":1,"schema_version":2}')
        with self.assertRaises(ContextError): recent.read(self.store, SCOPE, 1002)
        path.write_bytes(b'{"value":NaN}')
        with self.assertRaises(ContextError): recent.read(self.store, SCOPE, 1002)


if __name__ == '__main__': unittest.main()
