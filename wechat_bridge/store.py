"""状态管理：路径、原子读写、会话。

凭据只存 state/session.json（0600），绝不进 git。
"""

import fcntl
import hashlib
import json
import os
import re
import time
from contextlib import contextmanager


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
        # This ledger is intentionally independent of the Hook's bounded seen log.
        # Pruning it can re-enable old message IDs and requires an explicit policy.
        self.receipts_dir = os.path.join(self.state_dir, "received")
        self.receipt_lock_path = os.path.join(self.state_dir, "message_receipts.lock")
        self.quarantine_dir = os.path.join(self.state_dir, "quarantine")
        self.quarantine_status_path = os.path.join(self.state_dir, "QUARANTINE_NOTICE")
        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        from .media_retention import pin_store
        pin_store(self)
        for d in (self.inbox_dir, self.pending_dir,
                  self.sending_dir, self.sent_dir, self.failed_dir,
                  self.media_dir, self.receipts_dir, self.quarantine_dir):
            os.makedirs(d, mode=0o700, exist_ok=True)

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

    def close(self):
        """Release the conservative media pin once this user has stopped."""
        self._media_pin()


def safe_message_id(value):
    """Return an ID safe in file names and Hook arguments; never normalize paths."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("message_id must be a nonempty string or integer")
    value = str(value)
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,179}", value):
        raise ValueError("message_id contains unsupported characters or length")
    return value


@contextmanager
def message_receipt_lock(store):
    """Serialize receive/inbox publication with worker acknowledgement/deletion."""
    fd = os.open(store.receipt_lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def quarantine_record(store, payload, reason):
    """Durably preserve rejected input before acknowledging its poll batch.

    The raw input may contain messages/tokens and is only written to a private
    JSON file. Logs/status expose a fixed reason and digest, never that input.
    Replays use the same digest, keeping a poisoned response from growing disk.
    Failure propagates: the receiver must retain its cursor and back off.
    """
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"))
    digest = hashlib.sha256((reason + "\n" + raw).encode("utf-8")).hexdigest()
    path = os.path.join(store.quarantine_dir, digest + ".json")
    if not os.path.exists(path):
        atomic_write_json(path, {"reason": reason, "payload": payload,
                                 "quarantined_at": time.time()})
    atomic_write_text(store.quarantine_status_path, "%s %s %s\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), reason, digest))
    log("接收数据已隔离，需人工检查 state/quarantine（reason=%s id=%s）"
        % (reason, digest[:12]))
    return True


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
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        # Publish the rename durably as well as the file contents (POSIX runtime).
        dir_fd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
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
