"""提速改造套件（2026-10-21 晚，ABC 全做中的 A 项：独立 flush 线程）。

背景：旧实现 flush 搭在 getupdates 长轮询主循环里跑，闲置时一轮轮询
最长挂 ~120s，回复写好也要等轮询返回才发得出（闲置后第一条回复
慢 1–2 分钟的主因）。改造后 flush 由 serve 内的独立 flusher 线程
每 FLUSH_INTERVAL_SECONDS 秒跑一轮，主循环只收消息。

全离线：假网络用真实 iLink 响应形状（sendmessage 成功
{"message_id": int}、无 ret；失败 {"ret": -2}；失效 {"ret": -14}）。
运行：python3 tests/test_speed.py
"""
import os
import sys
import tempfile
import threading
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


class RunNet:
    """供 serve.run() 用的假网络。

    - getupdates（timeout=45，轮询）：阻塞在 release 事件上（0.1s 切片），
      模拟闲置时长轮询迟迟不返回；release 置位后返回空批次。
    - getupdates（timeout=15）：serve 的 -14 探活调用，按 probe_resp 回。
    - sendmessage：按 send_script 回，否则成功 {"message_id": n}。
    """

    def __init__(self):
        self.calls = []
        self.send_script = []
        self.probe_resp = {"ret": 0}
        self.release = threading.Event()
        self._mid = 7000
        self._lock = threading.Lock()

    def http_json(self, method, url, headers, body=None, timeout=None, retries=3):
        if url.endswith("/ilink/bot/getupdates"):
            if timeout == 15:  # 探活
                with self._lock:
                    self.calls.append(("probe", body))
                return self.probe_resp
            with self._lock:
                self.calls.append(("poll", body))
            # 阻塞式长轮询：切片等 release，最长 30s 兜底防测试挂死
            for _ in range(300):
                if self.release.wait(0.1):
                    break
            return {"msgs": []}
        if url.endswith("/ilink/bot/sendmessage"):
            with self._lock:
                self.calls.append(("sendmessage", body, time.time()))
                if self.send_script:
                    r = self.send_script.pop(0)
                    if isinstance(r, BaseException):
                        raise r
                    return r
                self._mid += 1
                return {"message_id": self._mid}
        if url.endswith("/ilink/bot/getuploadurl"):
            return {"upload_full_url": "https://cdn.invalid/full",
                    "upload_param": "param123"}
        raise AssertionError("unexpected URL: " + url)

    def send_count(self):
        with self._lock:
            return sum(1 for c in self.calls if c[0] == "sendmessage")

    def send_times(self):
        with self._lock:
            return [c[2] for c in self.calls if c[0] == "sendmessage"]


def fresh_store(net):
    store = Store(tempfile.mkdtemp(prefix="wbspeed-"))
    serve.http_json = net.http_json
    ilink.http_json = net.http_json
    media.http_json = net.http_json
    serve.PACE_SECONDS = 0
    serve.FLUSH_INTERVAL_SECONDS = 0.5  # 测试加速，生产默认 2.5
    return store


def write_job(store, name, **kw):
    job = {"to_user_id": "user1", "text": "hello", "context_token": "tok1",
           "message_id": 1, "enqueued_at": time.time() - 30}
    job.update(kw)
    atomic_write_json(os.path.join(store.pending_dir, name), job)
    return job


def ls(d):
    return sorted(os.listdir(d))


def start_run(store):
    result = {}

    def _target():
        result["rc"] = serve.run(store, "https://api.invalid", "BOTTOKEN",
                                 "user1")

    t = threading.Thread(target=_target, name="serve-run-test", daemon=True)
    t.start()
    return t, result


def stop_run(store, net, t):
    """置 NEEDS_RELOGIN + 放行长轮询，让主循环收尾。"""
    with open(store.needs_relogin_path, "w") as f:
        f.write("test stop\n")
    net.release.set()
    t.join(timeout=15)


def wait_until(pred, timeout=10.0, step=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def t_flusher_sends_while_poll_blocked():
    """核心场景：轮询被长轮询堵住时，落盘的回复被 flusher 独立发出。"""
    net = RunNet()
    store = fresh_store(net)
    t, result = start_run(store)
    try:
        time.sleep(0.5)  # 等 flusher 与轮询都就位（轮询此时正堵着）
        t0 = time.time()
        write_job(store, "a.json", text="speed test")
        ok = wait_until(lambda: "a.json" in ls(store.sent_dir), timeout=6)
        dt = time.time() - t0
        check("提速：轮询阻塞时 flusher 仍发出回复", ok)
        check("提速：落盘到发出 <5s（实测 %.2fs）" % dt, ok and dt < 5)
        check("提速：只发一次", net.send_count() == 1, net.send_count())
    finally:
        stop_run(store, net, t)
    check("提速：置 NEEDS_RELOGIN 后 run 收尾返回 1",
          result.get("rc") == 1 and not t.is_alive(), result)


def t_concurrent_writes_no_dup_no_loss():
    """flusher 运行时并发写任务：不丢、不重，最终全部恰好送达一次。"""
    net = RunNet()
    store = fresh_store(net)
    t, _result = start_run(store)
    names = ["w%02d.json" % i for i in range(10)]
    try:
        time.sleep(0.5)

        def _writer():
            for i, n in enumerate(names):
                write_job(store, n, text="msg-%d" % i,
                          enqueued_at=time.time() - 30)
                time.sleep(0.3)

        w = threading.Thread(target=_writer, name="job-writer", daemon=True)
        w.start()
        w.join(timeout=10)
        ok = wait_until(
            lambda: ls(store.sent_dir) == names and not ls(store.pending_dir)
            and not ls(store.sending_dir), timeout=15)
        check("提速：并发写 10 条全部送达且队列清空", ok,
              "sent=%s pending=%s sending=%s" % (
                  ls(store.sent_dir), ls(store.pending_dir),
                  ls(store.sending_dir)))
        # 恰好一次：sent 里每个文件名只出现一次（目录语义保证），且
        # 发送正文集合与写入集合一致（合并发送时正文拼接，逐条核对子串）
        bodies = []
        for c in list(net.calls):
            if c[0] == "sendmessage":
                for it in (c[1] or {}).get("msg", {}).get("item_list") or []:
                    if it.get("type") == 1:
                        bodies.append((it.get("text_item") or {}).get("text") or "")
        joined = "\n\n".join(bodies)
        check("提速：每条正文恰好出现一次",
              all(joined.count("msg-%d" % i) == 1 for i in range(10)),
              joined[:200])
    finally:
        stop_run(store, net, t)


def t_flusher_probe_relogin_stops():
    """发送侧 -14 且探活也 -14：flusher 写 NEEDS_RELOGIN 并让 run 收尾。"""
    net = RunNet()
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": -14}
    store = fresh_store(net)
    write_job(store, "p.json", text="probe me")
    t, result = start_run(store)
    try:
        ok = wait_until(lambda: os.path.exists(store.needs_relogin_path),
                        timeout=8)
        check("提速：flusher -14 探活确认后写 NEEDS_RELOGIN", ok)
        # 主循环的检查点在每轮轮询之间：放行被堵住的长轮询（模拟真实
        # 环境里新消息到达/轮询超时返回），它应立刻据标记收尾。
        net.release.set()
        ok2 = wait_until(lambda: not t.is_alive(), timeout=15)
        check("提速：确认失效后 run 收尾返回 1",
              ok2 and result.get("rc") == 1, result)
        check("提速：任务留队（回 pending、未送达未死信）",
              ls(store.pending_dir) == ["p.json"]
              and not ls(store.sent_dir) and not ls(store.failed_dir),
              "pending=%s sent=%s failed=%s" % (
                  ls(store.pending_dir), ls(store.sent_dir),
                  ls(store.failed_dir)))
    finally:
        if t.is_alive():
            stop_run(store, net, t)


def t_flusher_probe_ok_no_relogin():
    """发送侧 -14 但探活正常：不判失效，任务按可重试失败退避留队。"""
    net = RunNet()
    net.send_script = [{"ret": -14}]
    net.probe_resp = {"ret": 0}
    store = fresh_store(net)
    write_job(store, "q.json", text="probe ok")
    ok_flush = serve.flush_outbox(store, "https://api.invalid", "BOTTOKEN")
    check("提速：探活正常时 flush 返回 True 且不写 NEEDS_RELOGIN",
          ok_flush is True and not os.path.exists(store.needs_relogin_path))
    j = load_json(os.path.join(store.pending_dir, "q.json")) or {}
    check("提速：探活正常时任务记 attempts 退避留队",
          j.get("attempts") == 1 and float(j.get("next_attempt_at") or 0) > 0,
          j)


def main():
    t_flusher_sends_while_poll_blocked()
    t_concurrent_writes_no_dup_no_loss()
    t_flusher_probe_relogin_stops()
    t_flusher_probe_ok_no_relogin()
    print("== test_speed: %d passed, %d failed ==" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
