"""Fault injection against outbox progress, identity and recipient FIFO."""
import collections
import io
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import attachment_runtime, cli, ilink, media, serve
from wechat_bridge.store import Store, atomic_write_json, load_json

REAL_HTTP_JSON = ilink.http_json

class Crash(BaseException):
    pass

class Net:
    def __init__(self):
        self.calls=[]
        self.visible={}
        self.fail_at=None
        self.crash=False
        self.reject=False
    def http(self, method,url,headers,body=None,**kw):
        if url.endswith('/getupdates'):
            return {'ret':0}
        assert url.endswith('/sendmessage'),url
        msg=body['msg']; self.calls.append(msg)
        if self.reject:
            return {'ret':-2}
        self.visible.setdefault(msg['client_id'],msg)
        if self.crash:
            raise Crash()
        if self.fail_at==len(self.calls):
            raise TimeoutError('synthetic response lost after acceptance')
        return {'message_id':len(self.calls)}

class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store=Store(self.tmp.name)
        self.net=Net()
        for name in ('connect','connect_ex'):
            p=patch.object(socket.socket,name,side_effect=AssertionError('real network forbidden'))
            p.start();self.addCleanup(p.stop)
        p=patch.object(ilink,'http_json',self.net.http);p.start();self.addCleanup(p.stop)
        p=patch.object(serve,'http_json',self.net.http);p.start();self.addCleanup(p.stop)
        p=patch.object(serve,'PACE_SECONDS',0);p.start();self.addCleanup(p.stop)
    def put(self,name,text='text',**extra):
        job=dict(message_id=name[:-5],to_user_id='owner',text=text,
                 context_token='ctx',enqueued_at=time.time()-60)
        job.update(extra)
        atomic_write_json(os.path.join(self.store.pending_dir,name),job)
    def flush(self):
        return serve.flush_outbox(self.store,'https://fake.invalid','fake')
    def retry(self,name):
        path=os.path.join(self.store.pending_dir,name)
        job=load_json(path);job['next_attempt_at']=0;atomic_write_json(path,job)
    def burst(self,text1='A',text2='B'):
        self.put('a.json',text1,enqueued_at=time.time()-61)
        self.put('b.json',text2)
    def test_typed_poison_is_quarantined_and_neighbor_sent(self):
        self.put('bad.json',['not text'])
        self.put('good.json','good')
        self.flush()
        self.assertTrue(Path(self.store.sent_dir,'good.json').exists())
        self.assertTrue(any(n.startswith('bad.corrupt-') for n in os.listdir(self.store.failed_dir)))
        self.assertEqual(len(self.net.calls),1)
    def test_bad_metadata_is_not_infinite_pending(self):
        for field,value in [('attempts','bad'),('chunks_done',{}),('next_attempt_at',float('inf'))]:
            with self.subTest(field=field):
                self.put(field+'.json',**{field:value})
        self.flush()
        self.assertFalse(os.listdir(self.store.pending_dir))
        self.assertEqual(len(os.listdir(self.store.failed_dir)),3)
    def test_transient_deadletter_write_recovers_without_resend(self):
        self.put('a.json');self.net.fail_at=1
        real=serve.atomic_write_json
        def fail_deadletter(path,data):
            if Path(path).parent==Path(self.store.failed_dir):
                raise OSError('transient journal fault')
            return real(path,data)
        with patch.object(serve,'atomic_write_json',fail_deadletter):
            self.flush()
        self.assertTrue(Path(self.store.sending_dir,'a.json').exists())
        self.net.fail_at=None
        self.flush()
        self.assertEqual(load_json(Path(self.store.failed_dir,'a.json'))['delivery_outcome'], 'unknown')
        self.assertFalse(os.listdir(self.store.sending_dir))
        self.assertEqual(len(self.net.calls),1)
    def test_single_chunk_merge_timeout_keeps_envelope_and_id(self):
        self.burst();self.net.fail_at=1;self.flush()
        failed=load_json(Path(self.store.failed_dir,'b.json'))
        self.assertEqual(failed['text'],'A\n\nB')
        self.assertEqual(failed['client_key'],'merged:b.json')
        self.assertEqual(failed['delivery_outcome'],'unknown')
        self.assertEqual(len(self.net.calls),1)
        self.net.fail_at=None;self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['A\n\nB'])
        self.assertEqual(len(self.net.visible),1)
    def test_multichunk_timeout_preserves_progress_and_chunk_identity(self):
        self.burst('A'*1500,'B'*1500);self.net.fail_at=2;self.flush()
        failed=load_json(Path(self.store.failed_dir,'b.json'))
        self.assertEqual(failed['chunks_done'],1)
        self.assertEqual(failed['delivery_outcome'],'unknown')
        self.net.fail_at=None;self.flush()
        self.assertEqual(len(self.net.calls),2)
        self.assertNotEqual(self.net.calls[0]['client_id'],self.net.calls[1]['client_id'])
        visible=''.join(m['item_list'][0]['text_item']['text'] for m in self.net.visible.values())
        self.assertEqual(visible.count('A'),1500)
        self.assertEqual(visible.count('B'),1500)
    def test_crash_after_merged_acceptance_does_not_split_or_regroup(self):
        self.burst();self.net.crash=True
        with self.assertRaises(Crash):self.flush()
        self.net.crash=False;self.put('c.json','C');self.flush()
        texts=[m['item_list'][0]['text_item']['text'] for m in self.net.calls]
        self.assertEqual(texts,['A\n\nB','C'])
        self.assertEqual(load_json(Path(self.store.failed_dir,'b.json'))['delivery_outcome'], 'unknown')
    def test_crash_after_envelope_journal_before_aliases_is_recoverable(self):
        self.burst()
        with patch.object(serve,'_archive_merge_member',side_effect=Crash()):
            with self.assertRaises(Crash):self.flush()
        self.assertFalse(self.net.calls)
        self.flush()
        self.assertEqual(len(self.net.calls),1)
        self.assertEqual(self.net.calls[0]['item_list'][0]['text_item']['text'],'A\n\nB')
        self.assertEqual(sorted(os.listdir(self.store.sent_dir)),['a.json','b.json'])
    def test_recovered_single_cannot_join_a_new_batch(self):
        self.put('a.json','A');self.net.crash=True
        with self.assertRaises(Crash):self.flush()
        self.net.crash=False;self.put('b.json','B');self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['A','B'])
        self.assertEqual(load_json(Path(self.store.failed_dir,'a.json'))['delivery_outcome'], 'unknown')
    def test_chunk_budget_defers_then_resumes_with_new_context(self):
        self.put('long.json','X'*(2000*9));self.flush()
        self.assertEqual(len(self.net.calls),8)
        job=load_json(Path(self.store.pending_dir,'long.json'))
        self.assertEqual(job['chunks_done'],8)
        atomic_write_json(self.store.last_ctx_path,dict(to_user_id='owner',context_token='new-context'))
        self.flush()
        self.assertEqual(len(self.net.calls),9)
        self.assertEqual(self.net.calls[-1]['context_token'],'new-context')
        self.assertTrue(Path(self.store.sent_dir,'long.json').exists())
    def test_explicit_failure_also_keeps_fixed_batch(self):
        self.burst();self.net.reject=True;self.flush()
        self.assertEqual(len(self.net.calls),1)
        failed=load_json(Path(self.store.failed_dir,'b.json'))
        self.assertEqual(failed['text'],'A\n\nB')
        self.assertEqual(failed['delivery_outcome'],'unknown')
        self.net.reject=False;self.flush()
        self.assertEqual(len(self.net.calls),1)
    def test_retry_blocks_only_same_recipient(self):
        self.put('a.json','first',attempts=1,delivery_state=serve._SEND_READY,
                 embedded_send_authority=serve._embedded_send_authority('owner', 'fake'),
                 next_attempt_at=time.time()+30,enqueued_at=time.time()-400)
        self.put('b.json','second',enqueued_at=time.time()-60)
        self.put('other.json','other',to_user_id='other',enqueued_at=time.time()-30)
        self.flush()
        self.assertEqual([m['to_user_id'] for m in self.net.calls],['other'])
        self.retry('a.json');self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['other','first','second'])
    def test_failed_transition_cleanup_never_resurrects_deadletter(self):
        self.put('a.json');self.net.reject=True
        real_unlink=os.unlink
        def fail_source(path,*a,**kw):
            if Path(path)==Path(self.store.sending_dir,'a.json'):raise OSError('cleanup fault')
            return real_unlink(path,*a,**kw)
        with patch.object(serve.os,'unlink',fail_source):self.flush()
        count=len(self.net.calls);self.net.reject=False;self.flush()
        self.assertEqual(len(self.net.calls),count)
        self.assertTrue(Path(self.store.failed_dir,'a.json').exists())
    def test_pending_retry_state_survives_repeated_source_cleanup_failure(self):
        path=Path(self.store.base,'attachment.bin')
        path.write_bytes(b'synthetic')
        self.put('a.json',text='',media_path=str(path),caption_split=True)
        real_unlink=os.unlink
        def fail_source(path,*a,**kw):
            if Path(path)==Path(self.store.sending_dir,'a.json'):
                raise PermissionError('source cleanup unavailable')
            return real_unlink(path,*a,**kw)
        with patch.object(serve.os,'unlink',fail_source), \
                patch.object(serve,'send_media_message',side_effect=media.MediaSendError('getuploadurl')) as send:
            for _ in range(3):self.flush()
        self.assertEqual(send.call_count,1)
        self.assertEqual(len(self.net.calls),0)
        job=load_json(Path(self.store.pending_dir,'a.json'))
        self.assertEqual(job['attempts'],1)
        self.assertGreater(job['next_attempt_at'],time.time())
    def test_impossible_progress_is_quarantined_not_fake_sent(self):
        self.put('a.json',chunks_done=999)
        self.flush()
        self.assertFalse(self.net.calls)
        self.assertFalse(Path(self.store.sent_dir,'a.json').exists())
        self.assertEqual(len(os.listdir(self.store.failed_dir)),1)
        with self.assertRaises(ValueError):
            ilink.send_message('https://fake.invalid','t','owner','x','ctx',start_chunk=2)

    def test_http_error_with_success_body_cannot_confirm_final(self):
        self.put('a.json')
        error = urllib.error.HTTPError('https://fake.invalid', 503, 'synthetic', {},
                                       io.BytesIO(b'{"ret":0,"message_id":99}'))
        with patch.object(ilink, 'http_json', REAL_HTTP_JSON), \
                patch.object(ilink.urllib.request, 'urlopen', side_effect=error) as request:
            self.flush()
            self.flush()
        self.assertEqual(request.call_count, 1)
        self.assertEqual(load_json(Path(self.store.failed_dir, 'a.json'))['delivery_outcome'], 'unknown')
        self.assertFalse(os.listdir(self.store.sent_dir))

    def test_api_ack_archival_failure_and_stale_pending_copy_never_resend(self):
        self.put('a.json')
        stale = load_json(Path(self.store.pending_dir, 'a.json'))
        with patch.object(serve, '_mark_sent', side_effect=OSError('synthetic rename fault')):
            self.flush()
        saved = load_json(Path(self.store.sending_dir, 'a.json'))
        self.assertEqual((saved['delivery_state'], saved['chunks_done']), (serve._API_ACCEPTED, 1))
        atomic_write_json(Path(self.store.pending_dir, 'a.json'), stale)
        self.flush()
        self.assertEqual(len(self.net.calls), 1)
        self.assertEqual(load_json(Path(self.store.sent_dir, 'a.json')), saved)
        self.assertFalse(os.listdir(self.store.pending_dir))

    def test_published_prefix_survives_phase_write_error_and_resumes_only_unsent_chunk(self):
        self.put('a.json', text='A' * 2000 + 'B' * 500)
        real_write = serve.atomic_write_json

        def fail_after_prefix(path, job):
            real_write(path, job)
            if (Path(path) == Path(self.store.sending_dir, 'a.json') and
                    job.get('delivery_state') == serve._SEND_READY and job.get('chunks_done') == 1):
                raise OSError('synthetic post-rename fsync error')

        with patch.object(serve, 'atomic_write_json', side_effect=fail_after_prefix):
            self.flush()
        self.assertEqual(len(self.net.calls), 1)
        self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],
                         ['A' * 2000, 'B' * 500])
        self.assertEqual(load_json(Path(self.store.sent_dir, 'a.json'))['chunks_done'], 2)

    def test_old_attempted_and_shadowed_inflight_records_are_unknown(self):
        self.put('a.json', attempts=1)
        self.put('b.json')
        shadow = dict(load_json(Path(self.store.pending_dir, 'b.json')),
                      attempts=1, delivery_state='inflight')
        atomic_write_json(Path(self.store.sending_dir, 'b.json'), shadow)
        self.flush()
        self.flush()
        self.assertFalse(self.net.calls)
        self.assertFalse(os.listdir(self.store.pending_dir))
        for name in ('a.json', 'b.json'):
            self.assertEqual(load_json(Path(self.store.failed_dir, name))['delivery_outcome'], 'unknown')

    def test_preparation_failure_cannot_requeue_past_original_deadline(self):
        path = Path(self.store.base, 'attachment.bin')
        path.write_bytes(b'synthetic')
        enqueued_at = time.time() - serve.MAX_JOB_AGE + serve.BACKOFFS[0] / 2
        self.put('a.json', text='', media_path=str(path), caption_split=True, enqueued_at=enqueued_at)
        with patch.object(serve, 'send_media_message', side_effect=media.MediaSendError('getuploadurl')):
            self.flush()
        failed = load_json(Path(self.store.failed_dir, 'a.json'))
        self.assertEqual(failed['enqueued_at'], enqueued_at)
        self.assertEqual(failed['delivery_outcome'], 'proven_unsent')
        self.assertFalse(self.net.calls)
        self.assertFalse(os.listdir(self.store.pending_dir))

    def test_lost_bound_session_blocks_next_chunk_and_preserves_acknowledged_prefix(self):
        self.store._attachments_disabled = True
        atomic_write_json(self.store.session_path, {
            'bot_token': 'fake', 'ilink_user_id': 'owner', 'ilink_bot_id': 'synthetic-bot',
            'login_epoch': 'a' * 32,
        })
        self.put('a.json', text='A' * 2500)

        def remove_session(stage, *args, **kwargs):
            if stage == 'api_accepted':
                os.unlink(self.store.session_path)

        with patch.object(serve, '_trace_delivery', side_effect=remove_session):
            self.flush()
        self.flush()
        failed = load_json(Path(self.store.failed_dir, 'a.json'))
        self.assertEqual((failed['chunks_done'], failed['delivery_outcome']), (1, 'proven_unsent'))
        self.assertEqual(len(self.net.calls), 1)

    def test_managed_serve_binds_legacy_session_without_enabling_attachments(self):
        atomic_write_json(self.store.session_path, {
            'bot_token': 'fake', 'ilink_user_id': 'owner', 'ilink_bot_id': 'synthetic-bot',
        })
        state = Path(self.store.state_dir)
        atomic_write_json(state / 'backends.json', {
            'schema_version': 1, 'account_scope': 'synthetic', 'default_backend': 'muse',
            'backends': {'muse': {}}, 'legacy_state_dir': self.store.base,
        })
        self.put('managed.json', text='managed reply')

        def poll(*args, **kwargs):
            self.flush()
            raise KeyboardInterrupt

        lock_count = len(serve._lock_fhs)
        try:
            with patch.object(serve, '_bot_lock_root', return_value=str(state / 'bot-locks')), \
                    patch.object(serve, 'http_json', side_effect=poll):
                self.assertEqual(serve.run(self.store, 'https://fake.invalid', 'fake', 'owner'), 0)
        finally:
            for handle in serve._lock_fhs[lock_count:]:
                handle.close()
            del serve._lock_fhs[lock_count:]

        scope = attachment_runtime.current_scope(self.store, 'owner')
        sent = load_json(Path(self.store.sent_dir, 'managed.json'))
        self.assertEqual(sent['login_epoch'], scope['login_epoch'])
        self.assertEqual(len(self.net.calls), 1)
        self.assertFalse((state / 'attachments_enabled.json').exists())
        self.assertFalse((state / 'attachment-delivery.sqlite3').exists())

    def activate_epoch(self, token):
        self.assertTrue(cli._save_login_session(self.store, {
            'bot_token': token, 'ilink_user_id': 'owner', 'ilink_bot_id': 'synthetic-bot',
        }))
        scope = attachment_runtime.initialize(self.store)
        attachment_runtime.ledger(self.store).register_context(scope, 'ctx', fresh=True)
        return scope['login_epoch']

    def stop_after_logged_prefix(self):
        epoch = self.activate_epoch('fake')
        self.put('partial.json', text='A' * 2000 + 'B' * 50)
        pending_path = Path(self.store.pending_dir, 'partial.json')
        self.assertNotIn('login_epoch', load_json(pending_path))
        stop = threading.Event()

        def first_post(*args, **kwargs):
            inflight = load_json(Path(self.store.sending_dir, 'partial.json'))
            self.assertEqual(inflight['delivery_state'], serve._FINAL_INFLIGHT)
            self.assertEqual(inflight['login_epoch'], epoch)
            stop.set()
            return self.net.http(*args, **kwargs)

        with patch.object(ilink, 'http_json', side_effect=first_post):
            serve.flush_outbox(self.store, 'https://fake.invalid', 'fake', stop)
        self.assertEqual(len(self.net.calls), 1)
        pending = load_json(pending_path)
        self.assertEqual((pending['chunks_done'], pending['login_epoch']), (1, epoch))
        return epoch

    def test_restart_same_epoch_sends_only_remaining_suffix(self):
        epoch = self.stop_after_logged_prefix()
        self.store = Store(self.tmp.name)
        self.assertEqual(attachment_runtime.initialize(self.store)['login_epoch'], epoch)
        self.flush()
        sent = load_json(Path(self.store.sent_dir, 'partial.json'))
        self.assertEqual((sent['chunks_done'], sent['login_epoch']), (2, epoch))
        self.assertEqual([msg['item_list'][0]['text_item']['text'] for msg in self.net.calls],
                         ['A' * 2000, 'B' * 50])
        self.assertNotEqual(self.net.calls[0]['client_id'], self.net.calls[1]['client_id'])

    def test_actual_relogin_epoch_blocks_previously_bound_suffix(self):
        old_epoch = self.stop_after_logged_prefix()
        self.store = Store(self.tmp.name)
        self.assertNotEqual(self.activate_epoch('new-fake'), old_epoch)
        serve.flush_outbox(self.store, 'https://fake.invalid', 'new-fake')
        failed = load_json(Path(self.store.failed_dir, 'partial.json'))
        self.assertEqual(len(self.net.calls), 1)
        self.assertEqual((failed['chunks_done'], failed['login_epoch']), (1, old_epoch))
        self.assertEqual(failed['delivery_outcome'], 'proven_unsent')

    def test_progressed_legacy_envelopes_never_acquire_current_epoch(self):
        self.activate_epoch('fake')
        for directory, name in (('pending_dir', 'pending.json'), ('sending_dir', 'sending.json')):
            self.put(name, text='A' * 2500, attempts=1, chunks_done=1, delivery_state=serve._SEND_READY)
            if directory == 'sending_dir':
                os.replace(Path(self.store.pending_dir, name), Path(self.store.sending_dir, name))
        self.store = Store(self.tmp.name)
        self.flush()
        self.assertFalse(self.net.calls)
        for name in ('pending.json', 'sending.json'):
            failed = load_json(Path(self.store.failed_dir, name))
            self.assertEqual((failed['chunks_done'], failed['delivery_outcome']), (1, 'unknown'))
            self.assertNotIn('login_epoch', failed)

    def test_different_login_epochs_cannot_merge_or_combine_duplicate_progress(self):
        old_epoch = self.activate_epoch('fake')
        new_epoch = self.activate_epoch('fake')
        self.assertNotEqual(old_epoch, new_epoch)
        self.put('older.json', text='older', login_epoch=old_epoch, enqueued_at=time.time() - 70)
        self.put('newer.json', text='newer', login_epoch=new_epoch)
        self.flush()
        self.assertEqual([msg['item_list'][0]['text_item']['text'] for msg in self.net.calls], ['newer'])
        self.assertEqual(load_json(Path(self.store.failed_dir, 'older.json'))['login_epoch'], old_epoch)
        self.put('duplicate.json', text='A' * 2500, attempts=1, delivery_state=serve._SEND_READY,
                 client_key='same-client', login_epoch=new_epoch, chunks_done=0)
        pending_path = Path(self.store.pending_dir, 'duplicate.json')
        sending_path = Path(self.store.sending_dir, 'duplicate.json')
        pending = load_json(pending_path)
        sending = dict(pending, login_epoch=old_epoch, chunks_done=1)
        atomic_write_json(sending_path, sending)
        self.flush()
        self.assertEqual(len(self.net.calls), 1)
        self.assertEqual(load_json(pending_path), pending)
        self.assertEqual(load_json(sending_path), sending)
        terminal_path = Path(self.store.sent_dir, 'duplicate.json')
        terminal = dict(sending, delivery_state=serve._API_ACCEPTED, chunks_done=2)
        atomic_write_json(terminal_path, terminal)
        os.unlink(sending_path)
        self.flush()
        self.assertEqual(len(self.net.calls), 1)
        self.assertEqual(load_json(pending_path), pending)
        self.assertEqual(load_json(terminal_path), terminal)

    def test_conflicting_interrupted_merge_keeps_alias_and_anchor_for_review(self):
        old_epoch = self.activate_epoch('fake')
        new_epoch = self.activate_epoch('fake')
        self.put('anchor.json', text='member\n\nanchor', login_epoch=new_epoch,
                 client_key='merged:anchor.json', merged_from=['member.json', 'anchor.json'],
                 delivery_state=serve._SEND_READY)
        self.put('member.json', text='member', login_epoch=old_epoch)
        anchor_path = Path(self.store.pending_dir, 'anchor.json')
        alias_path = Path(self.store.sending_dir, 'member.json')
        os.replace(Path(self.store.pending_dir, 'member.json'), alias_path)
        anchor, alias = load_json(anchor_path), load_json(alias_path)
        self.flush()
        self.assertFalse(self.net.calls)
        self.assertEqual(load_json(anchor_path), anchor)
        self.assertEqual(load_json(alias_path), alias)

        terminal_path = Path(self.store.sent_dir, 'member.json')
        terminal = dict(alias, delivery_state='covered')
        alias = dict(alias, login_epoch=new_epoch)
        atomic_write_json(terminal_path, terminal)
        atomic_write_json(alias_path, alias)
        self.flush()
        self.assertFalse(self.net.calls)
        self.assertEqual(load_json(anchor_path), anchor)
        self.assertEqual(load_json(alias_path), alias)
        self.assertEqual(load_json(terminal_path), terminal)

    def test_active_budget_keeps_final_unknown_charged_across_recovery(self):
        atomic_write_json(self.store.session_path,
                          {'bot_token': 'fake', 'ilink_user_id': 'owner', 'ilink_bot_id': 'synthetic-bot'})
        scope = attachment_runtime.initialize(self.store)
        ledger = attachment_runtime.ledger(self.store)
        ledger.register_context(scope, 'ctx', fresh=True)
        self.put('a.json', text='A' * 2500, login_epoch=scope['login_epoch'])
        self.net.fail_at = 2
        self.flush()
        ledger.recover()
        self.flush()
        failed = load_json(Path(self.store.failed_dir, 'a.json'))
        self.assertEqual((failed['chunks_done'], failed['delivery_outcome']), (1, 'unknown'))
        self.assertEqual(ledger.register_context(scope, 'ctx')['used'], 2)
        self.assertEqual(len(self.net.calls), 2)

    def test_strict_final_success_and_legacy_upload_shapes(self):
        for response in ({'ret': False}, {'ret': 0.0}, {'ret': 0, 'errcode': -14},
                         {'ret': 0, 'message_id': []}, {'message_id': ''}, {'message_id': None}):
            with self.subTest(response=response):
                self.assertFalse(ilink.resp_ok(response))
        self.assertTrue(ilink.resp_ok({'message_id': 'synthetic-message'}))
        self.assertTrue(ilink.resp_ok({'upload_param': 'synthetic'}, 'upload'))

if __name__=='__main__':unittest.main()
