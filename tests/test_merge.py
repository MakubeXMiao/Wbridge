"""机械合并兜底套件（2026-10-21 重建，持久化版本）。

全离线，假网络用真实响应形状（成功 {"message_id": int} 无 ret）。
运行：python3 tests/test_merge.py
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import wechat_bridge.ilink as ilink
import wechat_bridge.media as media
import wechat_bridge.serve as serve
from wechat_bridge.store import Store, atomic_write_json, load_json

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS %s" % name)
    else:
        FAIL += 1
        print("FAIL %s %s" % (name, extra))


class FakeCDNResp:
    def __init__(self, status=200):
        self.status = status
        self.headers = {"x-encrypted-param": "enc-param-123"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return b""


class FakeNet:
    def __init__(self):
        self.calls = []
        self.send_script = []
        self.probe_resp = {"ret": 0}
        self.upload_resp = {"upload_full_url": "https://cdn.invalid/full",
                            "upload_param": "param123"}
        self._mid = 9000

    def http_json(self, method, url, headers, body=None, timeout=None, retries=3):
        if url.endswith("/ilink/bot/sendmessage"):
            self.calls.append(("sendmessage", body))
            if self.send_script:
                r = self.send_script.pop(0)
                if isinstance(r, BaseException):
                    raise r
                return r
            self._mid += 1
            return {"message_id": self._mid}
        if url.endswith("/ilink/bot/getupdates"):
            self.calls.append(("getupdates", body))
            return self.probe_resp
        if url.endswith("/ilink/bot/getuploadurl"):
            self.calls.append(("getuploadurl", body))
            return self.upload_resp
        raise AssertionError("unexpected URL: " + url)

    def send_count(self):
        return sum(1 for k, _ in self.calls if k == "sendmessage")

    def send_bodies(self):
        return [b for k, b in self.calls if k == "sendmessage"]

    def send_texts(self):
        out = []
        for b in self.send_bodies():
            txt = ""
            for it in (b or {}).get("msg", {}).get("item_list") or []:
                if it.get("type") == 1:
                    txt += (it.get("text_item") or {}).get("text") or ""
            out.append(txt)
        return out


def fresh_store(net):
    store = Store(tempfile.mkdtemp(prefix="wbmerge-"))
    serve.http_json = net.http_json
    ilink.http_json = net.http_json
    media.http_json = net.http_json
    media.urllib.request.urlopen = lambda req, timeout=None: FakeCDNResp()
    serve.PACE_SECONDS = 0
    return store


def write_job(store, name, **kw):
    job = {"to_user_id": "user1", "text": "hello", "context_token": "tok1",
           "message_id": 1, "enqueued_at": time.time() - 30}
    job.update(kw)
    atomic_write_json(os.path.join(store.pending_dir, name), job)
    return job


def flush(store):
    return serve.flush_outbox(store, "https://api.invalid", "BOTTOKEN")


def ls(d):
    return sorted(os.listdir(d))


def make_media_file(store):
    p = os.path.join(store.base, "pic.jpg")
    with open(p, "wb") as f:
        f.write(b"\xff\xd8" + b"x" * 200)
    return p


def t_burst_merges():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="t1", context_token="tokA",
              enqueued_at=now - 30)
    write_job(store, "2.json", text="t2", context_token="tokB",
              enqueued_at=now - 29)
    write_job(store, "3.json", text="t3", context_token="tokC",
              enqueued_at=now - 28)
    flush(store)
    check("合并：3 条连发只发 1 次", net.send_count() == 1, net.send_count())
    body = net.send_bodies()[0]
    check("合并：用最新成员的 token", body["msg"]["context_token"] == "tokC")
    check("合并：正文按序拼接", net.send_texts() == ["t1\n\nt2\n\nt3"],
          net.send_texts())
    check("合并：全组进 sent", ls(store.sent_dir) == ["1.json", "2.json", "3.json"])


def t_lone_not_merged():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "only.json", text="solo")
    flush(store)
    check("合并：单条照常单发", net.send_count() == 1
          and ls(store.sent_dir) == ["only.json"])


def t_apart_not_merged():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "a.json", text="A", enqueued_at=time.time() - 400)
    write_job(store, "b.json", text="B", enqueued_at=time.time() - 30)
    flush(store)
    check("合并：间隔 >120s 不合并", net.send_count() == 2
          and net.send_texts() == ["A", "B"], net.send_texts())


def t_media_not_merged():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    now = time.time()
    write_job(store, "a.json", text="A", enqueued_at=now - 30)
    write_job(store, "m.json", text="mc", media_path=mp, enqueued_at=now - 29)
    write_job(store, "b.json", text="B", enqueued_at=now - 28)
    flush(store)
    texts = net.send_texts()
    check("合并：媒体不并入文本组", "A" in texts and "B" in texts
          and all("\n\n" not in t for t in texts), texts)
    check("合并：媒体本体送达", "m.json" in ls(store.sent_dir))


def t_merge_fail_degrades():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="x1", enqueued_at=now - 30)
    write_job(store, "2.json", text="x2", enqueued_at=now - 29)
    net.send_script = [{"ret": -2, "errmsg": "prepare failed"}]
    flush(store)
    check("合并：失败保留单一批次，不拆开重发", net.send_count() == 1, net.send_count())
    path = os.path.join(store.pending_dir, "2.json")
    pending = load_json(path) or {}
    check("合并：固定正文和发送身份留队", pending.get("text") == "x1\n\nx2"
          and pending.get("client_key") == "merged:2.json")
    pending["next_attempt_at"] = 0
    atomic_write_json(path, pending)
    flush(store)
    bodies = net.send_bodies()
    check("合并：重试复用标识后全组送达", len(bodies) == 2
          and bodies[0]["msg"]["client_id"] == bodies[1]["msg"]["client_id"]
          and ls(store.sent_dir) == ["1.json", "2.json"])


def t_fresh_group_defers():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "1.json", text="f1", enqueued_at=time.time())
    write_job(store, "2.json", text="f2", enqueued_at=time.time())
    flush(store)
    check("合并：组太新整组推迟", net.send_count() == 0
          and len(ls(store.pending_dir)) == 2, net.send_count())
    for n in ("1.json", "2.json"):
        p = os.path.join(store.pending_dir, n)
        j = load_json(p)
        j["enqueued_at"] = time.time() - 30
        j["next_attempt_at"] = 0
        atomic_write_json(p, j)
    flush(store)
    check("合并：老化后正常合并发", net.send_count() == 1
          and len(ls(store.sent_dir)) == 2, net.send_count())


def t_caption_excluded():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "m_caption.json", text="cap", kind="caption",
              enqueued_at=now - 30)
    write_job(store, "a.json", text="A", enqueued_at=now - 29)
    flush(store)
    check("合并：配文任务不参与合并", net.send_count() == 2
          and net.send_texts() == ["cap", "A"], net.send_texts())


def t_partial_chunk_collapse():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="x" * 1500, enqueued_at=now - 30)
    write_job(store, "2.json", text="y" * 1500, enqueued_at=now - 29)
    net.send_script = [{"message_id": 1}, {"ret": -2, "errmsg": "x"}]
    flush(store)
    p = os.path.join(store.pending_dir, "2.json")
    check("合并：部分送达收拢续发任务", os.path.exists(p))
    if os.path.exists(p):
        j = load_json(p)
        check("合并：续发任务带 chunks_done=1", j.get("chunks_done") == 1, j)
        check("合并：续发任务是合并全文",
              j.get("text") == "x" * 1500 + "\n\n" + "y" * 1500)
    sent1 = load_json(os.path.join(store.sent_dir, "1.json")) or {}
    check("合并：其余成员标 merged_into 进 sent",
          sent1.get("merged_into") == "2.json", sent1)


def t_merge_probe_ok_degrades():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="p1", enqueued_at=now - 30)
    write_job(store, "2.json", text="p2", enqueued_at=now - 29)
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": 0}
    r = flush(store)
    check("合并：-14 探活正常保留原批次重试", r is True and net.send_count() == 1
          and not os.path.exists(store.needs_relogin_path), net.send_count())
    path = os.path.join(store.pending_dir, "2.json")
    pending = load_json(path) or {}
    pending["next_attempt_at"] = 0
    atomic_write_json(path, pending)
    flush(store)
    check("合并：-14 重试保持标识且全组送达", len(ls(store.sent_dir)) == 2
          and net.send_bodies()[0]["msg"]["client_id"] == net.send_bodies()[1]["msg"]["client_id"])


def t_merge_probe_bad():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="p1", enqueued_at=now - 30)
    write_job(store, "2.json", text="p2", enqueued_at=now - 29)
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": -14}
    r = flush(store)
    check("合并：探活 -14 判失效整组留队", r is False
          and os.path.exists(store.needs_relogin_path)
          and ls(store.pending_dir) == ["2.json"]
          and (load_json(os.path.join(store.pending_dir, "2.json")) or {}).get("merged_from")
              == ["1.json", "2.json"])


def main():
    t_burst_merges()
    t_lone_not_merged()
    t_apart_not_merged()
    t_media_not_merged()
    t_merge_fail_degrades()
    t_fresh_group_defers()
    t_caption_excluded()
    t_partial_chunk_collapse()
    t_merge_probe_ok_degrades()
    t_merge_probe_bad()
    print("== test_merge: %d passed, %d failed ==" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
