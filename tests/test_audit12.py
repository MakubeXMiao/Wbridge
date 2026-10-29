"""全盘审查 12 项修复套件（2026-10-21）。

覆盖 Python 侧第 1/3/4/5/6/7/8/12 项；第 2/9/10 项（hook 层）由
tests/test_hook_audit12.sh 覆盖，本文件最后会调用它并汇总。
全离线，假网络用真实 iLink 响应形状。运行：python3 tests/test_audit12.py
"""
import argparse
import io
import os
import sys
import tempfile
import time
from contextlib import redirect_stdout

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import wechat_bridge.cli as cli
import wechat_bridge.ilink as ilink
import wechat_bridge.media as media
import wechat_bridge.serve as serve
from wechat_bridge.store import Store, atomic_write_json, atomic_write_text, load_json

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

    def send_kinds(self):
        """每次 sendmessage 的内容种类序列：text:<正文> 或 media。"""
        out = []
        for b in self.send_bodies():
            items = (b or {}).get("msg", {}).get("item_list") or []
            if items and items[0].get("type") == 1:
                out.append("text:" + ((items[0].get("text_item") or {}).get("text") or ""))
            else:
                out.append("media")
        return out


def fresh_store(net):
    store = Store(tempfile.mkdtemp(prefix="wbaud12-"))
    serve.http_json = net.http_json
    ilink.http_json = net.http_json
    media.http_json = net.http_json
    media.urllib.request.urlopen = lambda req, timeout=None: FakeCDNResp()
    serve.PACE_SECONDS = 0
    return store


def write_job(store, name, where="pending", **kw):
    job = {"to_user_id": "user1", "text": "hello", "context_token": "tok1",
           "message_id": 1, "enqueued_at": time.time() - 30}
    job.update(kw)
    d = {"pending": store.pending_dir, "sending": store.sending_dir,
         "failed": store.failed_dir}[where]
    atomic_write_json(os.path.join(d, name), job)
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


# ---------- 第 1 项：sending/ 启动回收 + status 显示 ----------
def t_fix1_recovery():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "stuck.json", where="sending", attempts=2,
              next_attempt_at=time.time() + 9999,
              delivery_state="inflight")
    # Every serial flush now recovers abandoned sending files, but a committed
    # retry schedule must not be erased by source-cleanup recovery.
    flush(store)
    check("F1 flush自动回收sending且保留退避",
          net.send_count() == 0 and ls(store.sending_dir) == []
          and ls(store.pending_dir) == ["stuck.json"])
    serve._recover_sending(store)
    check("F1 回收后任务回到 pending", ls(store.pending_dir) == ["stuck.json"]
          and ls(store.sending_dir) == [])
    j = load_json(os.path.join(store.pending_dir, "stuck.json"))
    check("F1 回收保留attempts和已持久退避",
          j.get("attempts") == 2 and j.get("next_attempt_at") > time.time(), j)
    j["next_attempt_at"] = 0
    atomic_write_json(os.path.join(store.pending_dir, "stuck.json"), j)
    flush(store)
    check("F1 回收后任务正常发出", ls(store.sent_dir) == ["stuck.json"])


def t_fix1_status_counts_sending():
    net = FakeNet()
    store = fresh_store(net)
    atomic_write_json(store.session_path,
                      {"bot_token": "tok", "ilink_bot_id": "b",
                       "ilink_user_id": "u", "baseurl": "https://api.invalid"})
    write_job(store, "s1.json", where="sending")
    cli.get_store = lambda: store
    buf = io.StringIO()
    with redirect_stdout(buf):
        cli.cmd_status(argparse.Namespace())
    check("F1 status 显示 sending 计数",
          "outbox/sending: 1 条" in buf.getvalue(), buf.getvalue())


# ---------- 第 3 项：分片进度异常不丢 + client_id 稳定 ----------
def t_fix3_exception_progress():
    net = FakeNet()
    fresh_store(net)
    net.send_script = [{"message_id": 1}, ConnectionError("boom")]
    try:
        ilink.send_message("https://api.invalid", "T", "user1", "a" * 4500,
                           "tok", pace=0)
        raised = None
    except ConnectionError as e:
        raised = e
    check("F3 异常对象挂已送达片数",
          raised is not None and getattr(raised, "_chunks_done", None) == 1
          and getattr(raised, "_chunks_total", None) == 3, raised)


def t_fix3_serve_persists_progress():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "long.json", text="a" * 4500)
    net.send_script = [{"message_id": 1}, ConnectionError("boom")]
    flush(store)
    j = load_json(os.path.join(store.pending_dir, "long.json")) or {}
    check("F3 serve 异常分支落盘 chunks_done=1",
          j.get("chunks_done") == 1 and j.get("attempts") == 1, j)


def t_fix3_stable_client_id():
    net = FakeNet()
    fresh_store(net)
    ilink.send_message("https://api.invalid", "T", "user1", "a" * 2500,
                       "tok", client_key="jobX", pace=0)
    first = [b["msg"]["client_id"] for b in net.send_bodies()]
    net.calls.clear()
    ilink.send_message("https://api.invalid", "T", "user1", "a" * 2500,
                       "tok", client_key="jobX", pace=0)
    second = [b["msg"]["client_id"] for b in net.send_bodies()]
    check("F3 同任务重试 client_id 不变", first == second and len(first) == 2,
          (first, second))
    check("F3 不同片 client_id 不同", first[0] != first[1])


# ---------- 第 4 项：配文先行（含重试路径与死信联动） ----------
def t_fix4_caption_first():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    write_job(store, "m.json", text="captext", media_path=mp)
    flush(store)
    kinds = net.send_kinds()
    check("F4 配文先于媒体发出", kinds == ["text:captext", "media"], kinds)
    check("F4 媒体与配文同轮送达",
          "m.json" in ls(store.sent_dir)
          and "m_caption.json" in ls(store.sent_dir))


def t_fix4_caption_retry_defers_media():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    write_job(store, "m.json", text="captext", media_path=mp)
    net.send_script = [{"ret": -2, "errmsg": "prepare failed"}]  # 配文失败
    flush(store)
    kinds = net.send_kinds()
    check("F4 配文失败时媒体不抢跑", kinds == ["text:captext"], kinds)
    check("F4 配文与媒体都回 pending 等重试",
          "m_caption.json" in ls(store.pending_dir)
          and "m.json" in ls(store.pending_dir), ls(store.pending_dir))


def t_fix4_caption_dead_media_follows():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    write_job(store, "x_caption.json", where="failed", kind="caption",
              text="captext", failed_reason="attempts 超限")
    write_job(store, "x.json", media_path=mp, text="", caption_split=True)
    flush(store)
    check("F4 配文已死信则媒体随之死信",
          "x.json" in ls(store.failed_dir) and net.send_count() == 0,
          (ls(store.failed_dir), net.send_count()))


# ---------- 第 5 项：合并分组截断 + 按片计数 + 片间节奏 ----------
def t_fix5_group_pierced():
    net = FakeNet()
    store = fresh_store(net)
    mp = make_media_file(store)
    now = time.time()
    write_job(store, "a.json", text="A", enqueued_at=now - 30)
    write_job(store, "m.json", text="", media_path=mp, enqueued_at=now - 29)
    write_job(store, "b.json", text="B", enqueued_at=now - 28)
    flush(store)
    kinds = net.send_kinds()
    check("F5 媒体截断合并组、顺序不乱",
          kinds == ["text:A", "media", "text:B"], kinds)


def t_fix5_chunk_counting():
    net = FakeNet()
    store = fresh_store(net)
    now = time.time()
    write_job(store, "1.json", text="x" * 2500, context_token="tokM",
              enqueued_at=now - 30)
    write_job(store, "2.json", text="y" * 1500, context_token="tokM",
              enqueued_at=now - 29)
    flush(store)
    counts = serve._token_counts(store)
    check("F5 合并额度按实际分片数计（3 片）",
          counts.get("tokM") == 3, counts)


def t_fix5_chunk_pacing():
    net = FakeNet()
    fresh_store(net)
    t0 = time.time()
    ilink.send_message("https://api.invalid", "T", "user1", "a" * 2500,
                       "tok", pace=0.3)
    dt = time.time() - t0
    check("F5 片间有 pacing", dt >= 0.3, dt)


# ---------- 第 6 项：getupdates 响应健壮性 ----------
def t_fix6_bad_shapes():
    bad = [[], {"_raw": "x"}, {"ret": -2}, {"errcode": 500}, "str", None]
    good = [{}, {"ret": 0}, {"msgs": []}, {"ret": -14}]
    check("F6 坏响应判定", all(serve._poll_response_bad(r) for r in bad))
    check("F6 正常/-14 响应不误判",
          not any(serve._poll_response_bad(r) for r in good))


def t_fix6_run_survives_array():
    net = FakeNet()
    store = fresh_store(net)
    write_job(store, "w.json")
    atomic_write_text(store.buf_path, "BUF1")
    seq = {"n": 0}

    def http_json_seq(method, url, headers, body=None, timeout=None, retries=3):
        seq["n"] += 1
        if seq["n"] == 1:
            return ["not-a-dict"]          # 旧实现会直接崩 serve 的形状
        if seq["n"] == 2:
            return {"ret": -2}             # 错误 dict：应退避而非空转
        raise KeyboardInterrupt()

    serve.http_json = http_json_seq
    delays = []
    real_sleep = time.sleep
    time.sleep = lambda s: delays.append(s)
    try:
        rc = serve.run(store, "https://api.invalid", "T", "user1")
    finally:
        time.sleep = real_sleep
    check("F6 非 dict/错误响应不崩 serve", rc == 0, rc)
    check("F6 错误响应走退避", delays == [2, 2], delays)
    with open(store.buf_path, encoding="utf-8") as f:
        check("F6 错误响应不保存游标", f.read().strip() == "BUF1")
    j = load_json(os.path.join(store.pending_dir, "w.json")) or {}
    check("F6 错误响应不 flush（任务没被动）", "attempts" not in j, j)


# ---------- 第 7 项：消息永不静默丢失 ----------
def t_fix7_bad_aeskey():
    net = FakeNet()
    store = fresh_store(net)
    msg = {"message_id": 777, "message_type": 1,
           "from_user_id": "user1", "context_token": "tok",
           "item_list": [{"type": 2,
                          "image_item": {"aeskey": "zz!!nothex",
                                         "media": {}}}]}
    r = serve.handle_incoming(store, msg, "user1")
    p = os.path.join(store.inbox_dir, "777.json")
    rec = load_json(p) or {}
    check("F7 aeskey 非 hex 不丢消息", r is True and os.path.exists(p)
          and rec.get("media_failed") is True, rec)


def t_fix7_write_failure_returns_false():
    net = FakeNet()
    store = fresh_store(net)
    real = serve.atomic_write_json

    def boom(path, obj):
        raise OSError("disk on fire")

    serve.atomic_write_json = boom
    try:
        msg = {"message_id": 778, "message_type": 1,
               "from_user_id": "user1", "context_token": "tok",
               "item_list": [{"type": 1, "text_item": {"text": "hi"}}]}
        r = serve.handle_incoming(store, msg, "user1")
    finally:
        serve.atomic_write_json = real
    check("F7 记录落盘失败返回 False（游标不推进）", r is False, r)


def t_fix7_run_holds_cursor():
    net = FakeNet()
    store = fresh_store(net)
    atomic_write_text(store.buf_path, "BUF1")
    msg = {"message_id": 779, "message_type": 1,
           "from_user_id": "user1", "context_token": "tok",
           "item_list": [{"type": 1, "text_item": {"text": "hi"}}]}
    seq = {"n": 0}

    def http_json_seq(method, url, headers, body=None, timeout=None, retries=3):
        seq["n"] += 1
        if seq["n"] == 1:
            return {"msgs": [msg], "get_updates_buf": "BUF2"}
        raise KeyboardInterrupt()

    serve.http_json = http_json_seq
    real = serve.atomic_write_json

    def selective_boom(path, obj):
        if os.path.basename(os.path.dirname(path)) == "inbox":
            raise OSError("inbox unwritable")
        return real(path, obj)

    serve.atomic_write_json = selective_boom
    real_sleep = time.sleep
    time.sleep = lambda s: None
    try:
        rc = serve.run(store, "https://api.invalid", "T", "user1")
    finally:
        serve.atomic_write_json = real
        time.sleep = real_sleep
    with open(store.buf_path, encoding="utf-8") as f:
        buf_now = f.read().strip()
    check("F7 落盘失败时 run 不推进游标", rc == 0 and buf_now == "BUF1",
          (rc, buf_now))


# ---------- 第 8 项：CLI send 判据 + binded_redirect 落盘 ----------
def _cli_store(net):
    store = fresh_store(net)
    atomic_write_json(store.session_path,
                      {"bot_token": "tok", "ilink_bot_id": "b",
                       "ilink_user_id": "u", "baseurl": "https://api.invalid"})
    atomic_write_json(store.last_ctx_path,
                      {"to_user_id": "u", "context_token": "ct"})
    # Response-judgement cases need a proven current owner's context. Unknown
    # pre-cutover and foreign contexts are separately tested as refusals.
    from wechat_bridge.attachment_runtime import initialize
    initialize(store)
    assert serve.handle_incoming(store, {
        "message_id": "synthetic-audit12-current", "from_user_id": "u",
        "message_type": 1, "context_token": "synthetic-audit12-current-context",
        "item_list": [{"type": 1, "text_item": {"text": "synthetic current request"}}]}, "u")
    cli.get_store = lambda: store
    return store


def t_fix8_send_judged():
    net = FakeNet()
    _cli_store(net)
    args = argparse.Namespace(text="hi", image=None, file=None, video=None,
                              to=None, context=None)
    cli.send_message = lambda *a, **k: {"ret": -2, "errmsg": "prepare failed"}
    check("F8 send 回 -2 不再假成功", cli.cmd_send(args) == 1)
    cli.send_message = lambda *a, **k: {"message_id": 42}
    check("F8 send 成功返回 0", cli.cmd_send(args) == 0)


def t_fix8_caption_judged():
    net = FakeNet()
    store = _cli_store(net)
    mp = make_media_file(store)
    called = {"media": False}
    cli.send_message = lambda *a, **k: {"ret": -2}
    cli.send_media_message = lambda *a, **k: called.__setitem__("media", True) or {"message_id": 1}
    args = argparse.Namespace(text="cap", image=mp, file=None, video=None,
                              to=None, context=None)
    rc = cli.cmd_send(args)
    check("F8 配文失败返回 1 且不发媒体", rc == 1 and not called["media"],
          (rc, called))


def t_fix8_binded_redirect_persists():
    net = FakeNet()
    store = _cli_store(net)
    atomic_write_json(store.qr_path,
                      {"qrcode": "abc", "api": "https://api.invalid"})
    cli.http_json = lambda *a, **k: {
        "status": "binded_redirect", "bot_token": "tokXYZ",
        "ilink_bot_id": "b", "ilink_user_id": "u"}
    rc = cli.cmd_wait_login(argparse.Namespace())
    sess = load_json(store.session_path) or {}
    check("F8 binded_redirect 同账号续期落盘", rc == 0
          and sess.get("bot_token") == "tokXYZ", (rc, sess))
    atomic_write_json(store.qr_path,
                      {"qrcode": "second", "api": "https://api.invalid"})
    cli.http_json = lambda *a, **k: {
        "status": "binded_redirect", "bot_token": "different-token",
        "ilink_bot_id": "b2", "ilink_user_id": "u2"}
    rc = cli.cmd_wait_login(argparse.Namespace())
    check("F8 binded_redirect 换账号拒绝并保留原会话", rc == 1
          and load_json(store.session_path) == sess,
          (rc, load_json(store.session_path)))


# ---------- 第 12 项：坏 pending 文件隔离 ----------
def t_fix12_corrupt_isolated():
    net = FakeNet()
    store = fresh_store(net)
    d = os.path.join(store.pending_dir, "bad.json")
    os.makedirs(d)
    old = time.time() - 120
    os.utime(d, (old, old))
    junk = os.path.join(store.pending_dir, "junk.json")
    with open(junk, "w", encoding="utf-8") as f:
        f.write("{not json")
    os.utime(junk, (old, old))
    write_job(store, "good.json", text="good", attempts=1)
    flush(store)
    failed = ls(store.failed_dir)
    check("F12 坏文件被隔离到 failed",
          any(f.startswith("bad.corrupt-") for f in failed)
          and any(f.startswith("junk.corrupt-") for f in failed), failed)
    check("F12 好任务不受堵照常送达", ls(store.sent_dir) == ["good.json"])
    check("F12 pending 已清空", ls(store.pending_dir) == [])


def t_hook_suite():
    import subprocess
    here = os.path.dirname(os.path.abspath(__file__))
    r = subprocess.run(["bash", os.path.join(here, "test_hook_audit12.sh")],
                       capture_output=True, text=True, timeout=120)
    tail = [l for l in r.stdout.splitlines() if l.startswith("==")]
    check("F2/F9/F10 hook 套件（test_hook_audit12.sh）",
          r.returncode == 0 and tail and "0 failed" in tail[-1],
          (r.returncode, tail, r.stdout[-500:]))


def main():
    t_fix1_recovery()
    t_fix1_status_counts_sending()
    t_fix3_exception_progress()
    t_fix3_serve_persists_progress()
    t_fix3_stable_client_id()
    t_fix4_caption_first()
    t_fix4_caption_retry_defers_media()
    t_fix4_caption_dead_media_follows()
    t_fix5_group_pierced()
    t_fix5_chunk_counting()
    t_fix5_chunk_pacing()
    t_fix6_bad_shapes()
    t_fix6_run_survives_array()
    t_fix7_bad_aeskey()
    t_fix7_write_failure_returns_false()
    t_fix7_run_holds_cursor()
    t_fix8_send_judged()
    t_fix8_caption_judged()
    t_fix8_binded_redirect_persists()
    t_fix12_corrupt_isolated()
    t_hook_suite()
    print("== test_audit12: %d passed, %d failed ==" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
