"""P1–P7 回归套件（2026-10-21 重建，持久化版本）。

全离线：假网络层用真实 iLink 响应形状——sendmessage 成功是
{"message_id": int}（没有 ret）、失败 {"ret": -2}、token 失效
{"ret": -14}、getuploadurl 成功带 upload_full_url/upload_param。
运行：python3 tests/test_p1p7.py
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import wechat_bridge.ilink as ilink
import wechat_bridge.media as media
import wechat_bridge.serve as serve
from wechat_bridge.ilink import resp_ok
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
    """假 iLink 服务端：按 URL 分发，sendmessage 走脚本队列。"""

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
    store = Store(tempfile.mkdtemp(prefix="wbtest-"))
    serve.http_json = net.http_json
    ilink.http_json = net.http_json
    media.http_json = net.http_json
    media.urllib.request.urlopen = lambda req, timeout=None: FakeCDNResp()
    serve.PACE_SECONDS = 0
    return store


def write_job(store, name, **kw):
    job = {"to_user_id": "user1", "text": "hello", "context_token": "tok1",
           "message_id": 1, "enqueued_at": time.time()}
    job.update(kw)
    atomic_write_json(os.path.join(store.pending_dir, name), job)
    return job


def flush(store):
    return serve.flush_outbox(store, "https://api.invalid", "BOTTOKEN")


def force_retry(store, name):
    p = os.path.join(store.pending_dir, name)
    job = load_json(p)
    job["next_attempt_at"] = 0
    atomic_write_json(p, job)


def ls(d):
    return sorted(os.listdir(d))


def make_media_file(store):
    p = os.path.join(store.base, "pic.jpg")
    with open(p, "wb") as f:
        f.write(b"\xff\xd8" + b"x" * 200)
    return p


# ---------- resp_ok 判据（P4 实测形状） ----------
def t_resp_ok():
    cases = [
        ({"message_id": 123}, "send", True),
        ({"ret": 0}, "send", True),
        ({"ret": -2, "errmsg": "prepare failed"}, "send", False),
        ({"ret": -14}, "send", False),
        ({"_raw": "<html>502</html>"}, "send", False),
        ({}, "send", False),
        ({"upload_full_url": "https://x"}, "upload", True),
        ({"upload_param": "abc"}, "upload", True),
        ({"ret": -2}, "upload", False),
        ({}, "upload", False),
    ]
    ok = all(resp_ok(r, k) == want for r, k, want in cases)
    check("resp_ok 10 种形状判据", ok)


# ---------- P1：毒任务死信不挡好任务 + 退避 + 年龄死信 ----------
def t_poison_deadletter():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "bad.json", text="bad", attempts=4,
              enqueued_at=time.time() - 10)
    write_job(store, "good.json", text="good", attempts=1,
              enqueued_at=time.time() - 9)
    net.send_script = [{"ret": -2, "errmsg": "prepare failed"}]
    flush(store)
    check("P1 毒任务第 5 次失败进死信", ls(store.failed_dir) == ["bad.json"])
    check("P1 好任务同轮照常送达", ls(store.sent_dir) == ["good.json"])
    check("P1 发送调用次数", net.send_count() == 2, net.send_count())


def t_backoff_and_age():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "wait.json", next_attempt_at=time.time() + 999)
    flush(store)
    check("P1 退避未到点不发送", net.send_count() == 0
          and ls(store.pending_dir) == ["wait.json"])
    net2 = FakeNet()
    store2 = fresh_store(net2)
    write_job(store2, "old.json", enqueued_at=time.time() - 2000)
    flush(store2)
    check("P1 超龄任务直接死信不发送", net2.send_count() == 0
          and ls(store2.failed_dir) == ["old.json"])


# ---------- P3：单 token 上限 8 ----------
def t_token_cap():
    net = FakeNet()
    store = fresh_store(net)
    for i in range(9):
        write_job(store, "j%d.json" % i, text="m%d" % i, attempts=1,
                  enqueued_at=time.time() - 100 + i)
    flush(store)
    check("P3 单 token 最多发 8 条", net.send_count() == 8, net.send_count())
    check("P3 第 9 条留队", len(ls(store.pending_dir)) == 1)


# ---------- P3：未尝试任务升级新 token ----------
def t_token_upgrade():
    net = FakeNet()
    store = fresh_store(net)
    atomic_write_json(store.last_ctx_path,
                      {"to_user_id": "user1", "context_token": "tokNEW"})
    write_job(store, "u.json", context_token="tokOLD")
    flush(store)
    body = net.send_bodies()[0]
    check("P3 未尝试任务升级到最新 token",
          body["msg"]["context_token"] == "tokNEW", body["msg"])


# ---------- P3：配文只发一次 ----------
def t_caption_once():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    write_job(store, "m.json", text="captext", media_path=mp)
    net.send_script = [{"message_id": 1},          # 配文
                       {"ret": -2},                # 媒体第一次失败
                       {"message_id": 2}]          # 媒体重试成功
    flush(store)
    force_retry(store, "m.json")
    flush(store)
    check("P3 配文恰好发一次", net.send_texts().count("captext") == 1,
          net.send_texts())
    check("P3 媒体最终送达", "m.json" in ls(store.sent_dir))


# ---------- P4：-14 先探活 ----------
def t_probe_ok():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "p.json", attempts=1)
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": 0}
    r = flush(store)
    check("P4 探活正常不当失效", r is True
          and not os.path.exists(store.needs_relogin_path)
          and ls(store.pending_dir) == ["p.json"])


def t_probe_bad():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "p.json", attempts=1)
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": -14}
    r = flush(store)
    check("P4 探活也 -14 才判失效", r is False
          and os.path.exists(store.needs_relogin_path))


# ---------- P3：长文本分片续发 ----------
def t_chunk_resume():
    net = FakeNet()
    fresh_store(net)
    text = "a" * 4500
    net.send_script = [{"message_id": 1}, {"ret": -2, "errmsg": "x"}]
    resp = ilink.send_message("https://api.invalid", "T", "user1", text,
                              "tok", pace=0)
    check("P3 分片失败报告进度 1/3", resp.get("_chunks_done") == 1
          and resp.get("_chunks_total") == 3, resp)
    resp2 = ilink.send_message("https://api.invalid", "T", "user1", text,
                               "tok", start_chunk=resp["_chunks_done"], pace=0)
    check("P3 续发只发剩余片", resp2.get("_chunks_done") == 3
          and net.send_count() == 4, net.send_count())


# ---------- P5：上传 full_url 404 回退 param ----------
def t_upload_fallback():
    net = FakeNet()
    store = fresh_store(net)

    import urllib.error

    def picky_urlopen(req, timeout=None):
        if "cdn.invalid/full" in req.full_url:
            raise urllib.error.HTTPError(req.full_url, 404, "nf", None, None)
        return FakeCDNResp()

    media.urllib.request.urlopen = picky_urlopen
    param, _k, _s = media.upload_media("https://api.invalid", "T", "user1",
                                       b"imagedata", ilink.ITEM_IMAGE)
    check("P5 full_url 404 回退 param 成功", param == "enc-param-123")
    n_up = sum(1 for k, _ in net.calls if k == "getuploadurl")
    check("P5 回退重新取址一次", n_up == 2, n_up)


# ---------- P7：单实例锁 ----------
def t_flock():
    net = FakeNet()
    store = fresh_store(net)
    store2 = Store(store.base)
    a = serve._acquire_lock(store)
    b = serve._acquire_lock(store2)
    check("P7 第二个实例拿不到锁", a is True and b is False)


# ---------- P2：认领前文件消失不崩 ----------
def t_vanish():
    net = FakeNet()
    store = fresh_store(net)
    job = write_job(store, "v.json")
    os.unlink(os.path.join(store.pending_dir, "v.json"))
    cand = {"name": "v.json", "job": job,
            "pending_path": os.path.join(store.pending_dir, "v.json"),
            "sending_path": os.path.join(store.sending_dir, "v.json"),
            "enq": time.time()}
    r = serve._send_single(store, "https://api.invalid", "T", {}, cand)
    check("P2 认领前消失跳过不崩", r is True and net.send_count() == 0)


def main():
    t_resp_ok()
    t_poison_deadletter()
    t_backoff_and_age()
    t_token_cap()
    t_token_upgrade()
    t_caption_once()
    t_probe_ok()
    t_probe_bad()
    t_chunk_resume()
    t_upload_fallback()
    t_flock()
    t_vanish()
    print("== test_p1p7: %d passed, %d failed ==" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
