"""状态管理：路径、原子读写、会话。

凭据只存 state/session.json（0600），绝不进 git。
"""

import json
import os
import time


# 与 hook 统一的「已通知重新登录」标记（P7）：hook 用 ~/hooks/state/wechat_relogin_notified，
# cli.py 清理时也用这一个，不再有 state/notified_relogin 这个对不上的名字。
RELOGIN_NOTIFIED_PATH = os.path.join(
    os.path.expanduser("~"), "hooks", "state", "wechat_relogin_notified")


def log(msg, job=None, attempt=None):
    """统一日志（P6）：时间戳 + 可选任务名/尝试次数。"""
    parts = ["[%s]" % time.strftime("%Y-%m-%d %H:%M:%S")]
    if job:
        parts.append("[job=%s]" % job)
    if attempt is not None:
        parts.append("[attempt=%s]" % attempt)
    parts.append(str(msg))
    print(" ".join(parts), flush=True)


class Store:
    """桥接的数据目录布局。"""

    def __init__(self, base_dir):
        self.base = os.path.abspath(base_dir)
        self.state_dir = os.path.join(self.base, "state")
        self.inbox_dir = os.path.join(self.base, "inbox")
        self.pending_dir = os.path.join(self.base, "outbox", "pending")
        self.sending_dir = os.path.join(self.base, "outbox", "sending")
        self.sent_dir = os.path.join(self.base, "outbox", "sent")
        self.failed_dir = os.path.join(self.base, "outbox", "failed")
        self.media_dir = os.path.join(self.base, "media")
        for d in (self.state_dir, self.inbox_dir, self.pending_dir,
                  self.sending_dir, self.sent_dir, self.failed_dir,
                  self.media_dir):
            os.makedirs(d, exist_ok=True)

        self.session_path = os.path.join(self.state_dir, "session.json")
        self.qr_path = os.path.join(self.state_dir, "qr.json")
        self.buf_path = os.path.join(self.state_dir, "get_updates_buf.txt")
        self.pid_path = os.path.join(self.state_dir, "serve.pid")
        self.lock_path = os.path.join(self.state_dir, "serve.lock")
        self.last_ctx_path = os.path.join(self.state_dir, "last_context.json")
        self.needs_relogin_path = os.path.join(self.state_dir, "NEEDS_RELOGIN")
        # 二次审查 P1-1：flusher 线程心跳（epoch 文本），hook 据此判断
        # 「进程活着但发送线程死/卡」——进程存活检查发现不了线程级故障。
        self.flusher_hb_path = os.path.join(self.state_dir, "flusher_heartbeat")
        self.qr_img_path = os.path.join(self.base, "qrcode.png")


def atomic_write_json(path, obj):
    _atomic_write(path, lambda f: json.dump(obj, f, ensure_ascii=False, indent=2))


def atomic_write_text(path, text):
    _atomic_write(path, lambda f: f.write(text))


def _atomic_write(path, writer):
    """原子写入：先写唯一临时文件再替换，避免 tmp 不存在或并发冲突导致崩溃。"""
    import tempfile
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            writer(f)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def mark_needs_relogin(store, reason):
    atomic_write_text(
        store.needs_relogin_path,
        "%s %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), reason),
    )


def masked(s):
    """脱敏显示 token。"""
    s = s or ""
    return s[:4] + "****" + s[-4:] if len(s) > 12 else "****"
