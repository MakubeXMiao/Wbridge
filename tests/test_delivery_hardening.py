"""Fault injection against outbox progress, identity and recipient FIFO."""
import collections
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import ilink, serve
from wechat_bridge.store import Store, atomic_write_json, load_json

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
    def test_transient_retry_journal_failure_recovers_without_restart(self):
        self.put('a.json');self.net.fail_at=1
        real=serve.atomic_write_json
        def fail_pending(path,data):
            if Path(path).parent==Path(self.store.pending_dir):
                raise OSError('transient journal fault')
            return real(path,data)
        with patch.object(serve,'atomic_write_json',fail_pending):
            self.flush()
        self.assertTrue(Path(self.store.sending_dir,'a.json').exists())
        self.net.fail_at=None
        self.flush()
        self.assertTrue(Path(self.store.sent_dir,'a.json').exists())
        self.assertFalse(os.listdir(self.store.sending_dir))
        self.assertEqual(len({m['client_id'] for m in self.net.calls}),1)
    def test_single_chunk_merge_timeout_keeps_envelope_and_id(self):
        self.burst();self.net.fail_at=1;self.flush()
        pending=load_json(Path(self.store.pending_dir,'b.json'))
        self.assertEqual(pending['text'],'A\n\nB')
        self.assertEqual(pending['client_key'],'merged:b.json')
        self.assertEqual(len(self.net.calls),1)
        self.retry('b.json');self.net.fail_at=None;self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['A\n\nB']*2)
        self.assertEqual(len(self.net.visible),1)
    def test_multichunk_timeout_preserves_progress_and_chunk_identity(self):
        self.burst('A'*1500,'B'*1500);self.net.fail_at=2;self.flush()
        pending=load_json(Path(self.store.pending_dir,'b.json'))
        self.assertEqual(pending['chunks_done'],1)
        self.retry('b.json');self.net.fail_at=None;self.flush()
        self.assertEqual(len(self.net.calls),3)
        self.assertEqual(self.net.calls[1]['client_id'],self.net.calls[2]['client_id'])
        visible=''.join(m['item_list'][0]['text_item']['text'] for m in self.net.visible.values())
        self.assertEqual(visible.count('A'),1500)
        self.assertEqual(visible.count('B'),1500)
    def test_crash_after_merged_acceptance_does_not_split_or_regroup(self):
        self.burst();self.net.crash=True
        with self.assertRaises(Crash):self.flush()
        self.net.crash=False;self.put('c.json','C');self.flush()
        texts=[m['item_list'][0]['text_item']['text'] for m in self.net.calls]
        self.assertEqual(texts,['A\n\nB','A\n\nB','C'])
        self.assertEqual(self.net.calls[0]['client_id'],self.net.calls[1]['client_id'])
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
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['A','A','B'])
        self.assertEqual(self.net.calls[0]['client_id'],self.net.calls[1]['client_id'])
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
        self.assertEqual(load_json(Path(self.store.pending_dir,'b.json'))['text'],'A\n\nB')
        self.retry('b.json');self.net.reject=False;self.flush()
        self.assertEqual(self.net.calls[0]['client_id'],self.net.calls[1]['client_id'])
    def test_retry_blocks_only_same_recipient(self):
        self.put('a.json','first',attempts=1,next_attempt_at=time.time()+30,enqueued_at=time.time()-400)
        self.put('b.json','second',enqueued_at=time.time()-60)
        self.put('other.json','other',to_user_id='other',enqueued_at=time.time()-30)
        self.flush()
        self.assertEqual([m['to_user_id'] for m in self.net.calls],['other'])
        self.retry('a.json');self.flush()
        self.assertEqual([m['item_list'][0]['text_item']['text'] for m in self.net.calls],['other','first','second'])
    def test_failed_transition_cleanup_never_resurrects_deadletter(self):
        self.put('a.json',attempts=4);self.net.reject=True
        real_unlink=os.unlink
        def fail_source(path,*a,**kw):
            if Path(path)==Path(self.store.sending_dir,'a.json'):raise OSError('cleanup fault')
            return real_unlink(path,*a,**kw)
        with patch.object(serve.os,'unlink',fail_source):self.flush()
        count=len(self.net.calls);self.net.reject=False;self.flush()
        self.assertEqual(len(self.net.calls),count)
        self.assertTrue(Path(self.store.failed_dir,'a.json').exists())
    def test_pending_retry_state_survives_repeated_source_cleanup_failure(self):
        self.put('a.json');self.net.reject=True
        real_unlink=os.unlink
        def fail_source(path,*a,**kw):
            if Path(path)==Path(self.store.sending_dir,'a.json'):
                raise PermissionError('source cleanup unavailable')
            return real_unlink(path,*a,**kw)
        with patch.object(serve.os,'unlink',fail_source):
            for _ in range(3):self.flush()
        self.assertEqual(len(self.net.calls),1)
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

if __name__=='__main__':unittest.main()
