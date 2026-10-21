"""二次全盘审查修复套件（2026-10-21 深夜，P1–P3 全修中的代码项）。

覆盖：
- P1-1 flusher 监督：线程被 BaseException 杀死 → 主循环进程内重启 +
  回收 sending/ 遗留任务，队列里的任务最终恰好成功送达一次；
  心跳文件持续刷新、flush 卡住时心跳停滞（hook 判死所依赖的信号）。
- P3-1 移入 sent/ 的任务文件权限收紧为 0600（源文件 0660 时）。
- P3-5 media client_id 按任务名稳定派生（同任务重试同 id、异任务异 id）；
  cli wait-login 遇非 dict 响应不崩、按轮询失败继续。
- P3-6 http_json retries 语义 + send_message 单次尝试（连接错误立刻
  抛给任务层，不再传输层连打 3 次）。

全离线：假网络用真实 iLink 响应形状（sendmessage 成功
{"message_id": int}、无 ret）。运行：python3 tests/test_audit2fix.py
"""
import hashlib
import http.client
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import wechat_bridge.cli as cli
import wechat_bridge.ilink as ilink
import wechat_bridge.media as media
import wechat_bridge.serve as serve
from wechat_bridge.store import Store, atomic_write_json, load_json

PASS = FAIL = 0

# fresh_store 会把 ilink.http_json 换成假网络；P3-6 要测的是真 http_json，
# 先把真身存下来，测试时恢复。
_REAL_HTTP_JSON = ilink.http_json


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


class ThreadKiller(BaseException):
    """模拟杀死 flusher 线程的锁外异常（BaseException，Exception 兜不住）。"""


class RunNet:
    """serve.run() 用假网络：轮询短切片（~1s 一轮，让主循环监督能转起来）。

    sendmessage 默认成功；send_script 里的条目依次消费，条目是
    BaseException 实例时抛出（ThreadKiller 用来杀 flusher 线程）。
    """

    def __init__(self, poll_cycles=10):
        self.calls = []
        self.send_script = []
        self.probe_resp = {"ret": 0}
        self.release = threading.Event()
        self.poll_cycles = poll_cycles
        self._mid = 5000
        self._lock = threading.Lock()
        self.send_ok_texts = []

    def http_json(self, method, url, headers, body=None, timeout=None,
                  retries=3):
        if url.endswith("/ilink/bot/getupdates"):
            if timeout == 15:
                return self.probe_resp
            # 短轮询：等 release 或 ~poll_cycles*0.1s 后返回空批
            for _ in range(self.poll_cycles):
                if self.release.wait(0.1):
                    break
            return {"msgs": []}
        if url.endswith("/ilink/bot/sendmessage"):
            with self._lock:
                self.calls.append(("sendmessage", body))
                if self.send_script:
                    r = self.send_script.pop(0)
                    if isinstance(r, BaseException):
                        raise r
                    return r
                self._mid += 1
                for it in (body or {}).get("msg", {}).get("item_list") or []:
                    if it.get("type") == 1:
                        self.send_ok_texts.append(
                            (it.get("text_item") or {}).get("text") or "")
                return {"message_id": self._mid}
        if url.endswith("/ilink/bot/getuploadurl"):
            with self._lock:
                self.calls.append(("getuploadurl", body))
            return {"upload_full_url": "https://cdn.invalid/full",
                    "upload_param": "param123"}
        raise AssertionError("unexpected URL: " + url)


def fresh_store(net):
    store = Store(tempfile.mkdtemp(prefix="wba2-"))
    serve.http_json = net.http_json
    ilink.http_json = net.http_json
    media.http_json = net.http_json
    media.urllib.request.urlopen = lambda req, timeout=None: FakeCDNResp()
    serve.PACE_SECONDS = 0
    serve.FLUSH_INTERVAL_SECONDS = 0.3
    return store


def write_job(store, name, **kw):
    job = {"to_user_id": "user1", "text": "hello", "context_token": "tok1",
           "message_id": 1, "enqueued_at": time.time() - 30}
    job.update(kw)
    atomic_write_json(os.path.join(store.pending_dir, name), job)
    return job


def ls(d):
    return sorted(os.listdir(d))


def wait_until(pred, timeout=12.0, step=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def start_run(store):
    result = {}

    def _target():
        result["rc"] = serve.run(store, "https://api.invalid", "BOTTOKEN",
                                 "user1")

    t = threading.Thread(target=_target, name="serve-run-a2", daemon=True)
    t.start()
    return t, result


def stop_run(store, net, t):
    with open(store.needs_relogin_path, "w") as f:
        f.write("test stop\n")
    net.release.set()
    t.join(timeout=15)


def t_flusher_death_restart_recovers():
    """P1-1：flusher 被 BaseException 杀死 → 主循环重启线程 + 回收遗留。

    job1 的发送让假网络抛 ThreadKiller（线程当场死、job1 卡 sending/）；
    主循环下一轮应发现并重启 flusher、先回收 job1；job1 与随后写入的
    job2 最终都恰好成功送达一次。
    """
    net = RunNet(poll_cycles=30)  # 轮询 ~3s 一轮，让 sending/ 滞留可观测
    net.send_script = [ThreadKiller("boom")]
    store = fresh_store(net)
    write_job(store, "j1.json", text="first")
    t, result = start_run(store)
    try:
        ok1 = wait_until(
            lambda: "j1.json" in ls(store.sending_dir), timeout=6)
        check("P1-1 前置：杀线程的任务卡在 sending/", ok1,
              "sending=%s" % ls(store.sending_dir))
        write_job(store, "j2.json", text="second")
        ok2 = wait_until(
            lambda: ls(store.sent_dir) == ["j1.json", "j2.json"]
            and not ls(store.pending_dir) and not ls(store.sending_dir),
            timeout=15)
        check("P1-1：flusher 重启后两条任务都送达且队列清空", ok2,
              "sent=%s pending=%s sending=%s" % (
                  ls(store.sent_dir), ls(store.pending_dir),
                  ls(store.sending_dir)))
        # 两条都是 attempts=0 的文本任务，会被机械合并成一条发出——
        # 按合并后的正文核对每条恰好出现一次（与 test_speed 同口径）。
        joined = "\n\n".join(net.send_ok_texts)
        check("P1-1：每条正文恰好成功送达一次",
              joined.count("first") == 1 and joined.count("second") == 1,
              net.send_ok_texts)
        check("P1-1：心跳文件存在且新鲜",
              os.path.exists(store.flusher_hb_path)
              and abs(time.time() - int(
                  open(store.flusher_hb_path).read().strip())) < 10)
    finally:
        stop_run(store, net, t)
    check("P1-1：run 正常收尾返回 1", result.get("rc") == 1, result)


def t_heartbeat_stalls_when_flush_wedged():
    """P1-1：flush 卡住时心跳停滞（hook 据此判死并杀进程重拉的信号）。"""
    net = RunNet()
    store = fresh_store(net)
    orig_flush = serve.flush_outbox
    gate = threading.Event()

    def wedged_flush(store_, baseurl, token):
        gate.wait(30)
        return True

    t, _result = start_run(store)
    try:
        ok = wait_until(lambda: os.path.exists(store.flusher_hb_path),
                        timeout=6)
        check("P1-1：心跳文件被创建", ok)
        serve.flush_outbox = wedged_flush
        # 等至少一轮 wedged flush 开始占用线程后，记录心跳并观察停滞
        time.sleep(2.0)
        hb1 = open(store.flusher_hb_path).read().strip()
        time.sleep(2.0)
        hb2 = open(store.flusher_hb_path).read().strip()
        check("P1-1：flush 卡住期间心跳不再前进", hb1 == hb2,
              "%s vs %s" % (hb1, hb2))
        serve.flush_outbox = orig_flush
        gate.set()
        ok2 = wait_until(
            lambda: open(store.flusher_hb_path).read().strip() != hb2,
            timeout=8)
        check("P1-1：flush 恢复后心跳继续前进", ok2)
    finally:
        serve.flush_outbox = orig_flush
        gate.set()
        stop_run(store, net, t)


def t_sent_file_mode_0600():
    """P3-1：0660 的 pending 任务送达后，sent/ 里必须是 0600。"""
    net = RunNet()
    store = fresh_store(net)
    write_job(store, "m.json", text="mode test")
    p = os.path.join(store.pending_dir, "m.json")
    os.chmod(p, 0o660)
    ok = serve.flush_outbox(store, "https://api.invalid", "BOTTOKEN")
    sp = os.path.join(store.sent_dir, "m.json")
    check("P3-1：flush 成功且任务进 sent/", ok and os.path.exists(sp))
    mode = os.stat(sp).st_mode & 0o777 if os.path.exists(sp) else None
    check("P3-1：sent 文件权限 0600", mode == 0o600, oct(mode or 0))


def t_media_client_id_stable():
    """P3-5：同 client_key 两次发送 client_id 相同且为稳定派生；异 key 异 id。"""
    net = RunNet()
    store = fresh_store(net)
    img = os.path.join(store.base, "pic.jpg")
    with open(img, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 64)

    def send_and_capture(key):
        net.calls.clear()
        media.send_media_message("https://api.invalid", "T", "user1", "tok",
                                 img, media.ITEM_IMAGE, client_key=key)
        for kind, body in net.calls:
            if kind == "sendmessage":
                return body["msg"]["client_id"]
        return None

    a1 = send_and_capture("jobA.json")
    a2 = send_and_capture("jobA.json")
    b1 = send_and_capture("jobB.json")
    expect = hashlib.md5("jobA.json#0".encode("utf-8")).hexdigest()
    check("P3-5：同任务重试 client_id 不变", a1 is not None and a1 == a2,
          "%s vs %s" % (a1, a2))
    check("P3-5：client_id 为 md5(任务名#0) 稳定派生", a1 == expect, a1)
    check("P3-5：不同任务 client_id 不同", b1 is not None and b1 != a1)


def t_http_json_retry_counts():
    """P3-6：http_json 按 retries 参数尝试；send_message 只试一次。"""
    attempts = {"n": 0}
    ilink.http_json = _REAL_HTTP_JSON  #  undo fresh_store 的假网络替换
    real_urlopen = ilink.urllib.request.urlopen
    real_sleep = ilink.time.sleep

    def boom(req, timeout=None):
        attempts["n"] += 1
        raise http.client.RemoteDisconnected("simulated drop")

    ilink.urllib.request.urlopen = boom
    ilink.time.sleep = lambda s: None
    try:
        attempts["n"] = 0
        try:
            ilink.http_json("POST", "https://api.invalid/x", {}, {},
                            timeout=5, retries=1)
            raised = False
        except Exception:
            raised = True
        check("P3-6：retries=1 时连接错误只试一次并抛出",
              raised and attempts["n"] == 1, attempts)
        attempts["n"] = 0
        try:
            ilink.http_json("POST", "https://api.invalid/x", {}, {},
                            timeout=5)
            raised = False
        except Exception:
            raised = True
        check("P3-6：默认 retries=3（轮询路径不变）",
              raised and attempts["n"] == 3, attempts)
        attempts["n"] = 0
        try:
            ilink.send_message("https://api.invalid", "T", "user1", "hi",
                               "tok", pace=0)
            raised = False
            chunks = None
        except Exception as e:
            raised = True
            chunks = getattr(e, "_chunks_done", None)
        check("P3-6：send_message 连接错误只试一次（任务层接管重试）",
              raised and attempts["n"] == 1 and chunks == 0,
              "attempts=%s chunks=%s" % (attempts["n"], chunks))
    finally:
        ilink.urllib.request.urlopen = real_urlopen
        ilink.time.sleep = real_sleep


def t_wait_login_list_body():
    """P3-5：wait-login 收到 JSON 数组体不崩，跳过该轮后正常完成登录。"""
    store = Store(tempfile.mkdtemp(prefix="wba2cli-"))
    atomic_write_json(store.qr_path, {"qrcode": "qr123",
                                      "api": "https://api.invalid"})
    seq = [["not-a-dict"],
           {"status": "confirmed", "bot_token": "TOK",
            "baseurl": "https://api.invalid", "ilink_bot_id": "b",
            "ilink_user_id": "u"}]
    real_http = cli.http_json
    real_sleep = cli.time.sleep
    cli.http_json = lambda *a, **k: seq.pop(0)
    cli.time.sleep = lambda s: None
    os.environ["WECHAT_BRIDGE_DIR"] = store.base
    try:
        rc = cli.cmd_wait_login(None)
    finally:
        cli.http_json = real_http
        cli.time.sleep = real_sleep
    sess = load_json(store.session_path) or {}
    check("P3-5：wait-login 非 dict 响应不崩且完成登录",
          rc == 0 and sess.get("bot_token") == "TOK",
          "rc=%s sess=%s" % (rc, list(sess.keys())))


def main():
    t_flusher_death_restart_recovers()
    t_heartbeat_stalls_when_flush_wedged()
    t_sent_file_mode_0600()
    t_media_client_id_stable()
    t_http_json_retry_counts()
    t_wait_login_list_body()
    print("== test_audit2fix: %d passed, %d failed ==" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
