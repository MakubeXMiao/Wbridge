"""命令行入口：login / wait-login / serve / send / status。"""

import argparse
from contextlib import contextmanager
import fcntl
import os
import sys
import time
import urllib.parse
import uuid

from .ilink import (
    DEFAULT_API, QR_MAX_REFRESH, auth_headers, fetch_qrcode, http_json,
    is_token_invalid, resp_ok, send_message, app_headers,
)
from .media import media_type_from_path, send_media_message
from .serve import probe_token_invalid
from .serve import run as serve_run
from .store import (
    RELOGIN_NOTIFIED_PATH, Store, atomic_write_json, load_json,
    mark_needs_relogin, masked,
)


def get_store():
    base = os.environ.get("WECHAT_BRIDGE_DIR",
                          os.path.join(os.path.expanduser("~"), "workspace", "wechat-bridge"))
    return Store(base)


def cmd_login(args):
    store = get_store()
    qrcode, _ = fetch_qrcode(
        lambda obj: atomic_write_json(store.qr_path, obj),
        store.qr_img_path,
    )
    if not qrcode:
        return 1
    print("二维码图片已保存: %s" % store.qr_img_path)
    print("请用手机微信扫码，扫完后运行: python3 -m wechat_bridge wait-login")
    return 0


@contextmanager
def _login_lock(store):
    """与 serve 共用锁，整个确认流程不允许运行中的服务重绑凭据。"""
    with open(store.lock_path, "a") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("serve 正在运行或另一个登录正在确认；请先停止服务再登录。")
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _valid_session(session):
    return (isinstance(session, dict)
            and isinstance(session.get("bot_token"), str)
            and bool(session["bot_token"].strip())
            and isinstance(session.get("ilink_user_id"), str)
            and bool(session["ilink_user_id"].strip()))


def _has_account_state(store):
    """只检查是否存在账号数据；不清空、不迁移任何现有状态。"""
    for name in os.listdir(store.state_dir):
        if name == "qr.json":
            continue
        path = os.path.join(store.state_dir, name)
        # Hook 在首登前也会取收件锁；仅明确、空的常规锁文件可忽略。
        if (name in ("serve.lock", "message_receipts.lock")
                and os.path.isfile(path) and not os.path.islink(path)
                and os.path.getsize(path) == 0):
            continue
        # Store 会预建空的 received/quarantine 等目录，它们不是旧账号数据。
        if os.path.isdir(path) and not os.path.islink(path) and not os.listdir(path):
            continue
        return True
    return any(os.listdir(path) for path in (
        store.inbox_dir, store.pending_dir, store.sending_dir,
        store.sent_dir, store.failed_dir, store.media_dir))


def _save_login_session(store, session):
    if not _valid_session(session):
        print("登录响应缺少有效 bot_token 或机主 ID，未保存会话。")
        return False
    previous = load_json(store.session_path)
    if previous:
        same_owner = (_valid_session(previous)
                      and previous["ilink_user_id"] == session["ilink_user_id"])
        old_bot = previous.get("ilink_bot_id") if isinstance(previous, dict) else None
        new_bot = session.get("ilink_bot_id")
        if not same_owner or (old_bot and new_bot and old_bot != new_bot):
            print("当前目录属于其他或无法确认的账号；请停止服务，并为新账号设置独立 WECHAT_BRIDGE_DIR。现有状态已保留。")
            return False
    elif _has_account_state(store):
        print("当前目录已有无法确认归属的运行数据；请停止服务，并使用独立 WECHAT_BRIDGE_DIR。现有状态已保留。")
        return False
    atomic_write_json(store.session_path, session)
    for path in (store.qr_path, store.needs_relogin_path, RELOGIN_NOTIFIED_PATH):
        try:
            os.remove(path)
        except OSError:
            pass
    return True


def cmd_wait_login(args):
    store = get_store()
    try:
        with _login_lock(store):
            return _wait_login_locked(args, store)
    except (OSError, RuntimeError) as error:
        print("登录未完成: %s" % error)
        return 1


def _wait_login_locked(args, store):
    qr = load_json(store.qr_path)
    if not qr or not qr.get("qrcode"):
        print("没有待扫码的二维码，请先运行: python3 -m wechat_bridge login")
        return 1
    qrcode = qr["qrcode"]
    api = qr.get("api") or DEFAULT_API
    refresh_left = QR_MAX_REFRESH
    deadline = time.time() + 600
    print("等待扫码确认（最长 10 分钟，二维码过期自动刷新 %d 次）…" % QR_MAX_REFRESH)
    while time.time() < deadline:
        url = api + "/ilink/bot/get_qrcode_status?qrcode=" + urllib.parse.quote(qrcode, safe="")
        try:
            resp = http_json("GET", url, app_headers(), None, timeout=45)
        except Exception as e:
            print("轮询出错，2 秒后重试: %s" % e)
            time.sleep(2)
            continue
        if not isinstance(resp, dict):
            # 二次审查 P3-5：服务端偶发返回 JSON 数组体，旧实现下一行
            # resp.get(...) 直接 AttributeError 把 wait-login 打崩。
            # 按轮询失败处理：记一笔、等 2 秒继续，dict 行为完全不变。
            print("轮询返回非 dict 响应，2 秒后重试: %s" % str(resp)[:200])
            time.sleep(2)
            continue
        status = resp.get("status")
        if status == "expired" and refresh_left > 0:
            refresh_left -= 1
            print("二维码已过期，自动刷新（剩余 %d 次）…" % refresh_left)
            qrcode, _ = fetch_qrcode(
                lambda obj: atomic_write_json(store.qr_path, obj),
                store.qr_img_path,
            )
            if not qrcode:
                print("刷新二维码失败。")
                return 1
            print("新二维码已生成: %s，请重新扫码" % store.qr_img_path)
            continue
        if status == "wait":
            pass
        elif status == "scaned":
            print("已扫码，等待在手机上确认…")
        elif status == "confirmed":
            session = {
                "bot_token": resp.get("bot_token"),
                "baseurl": resp.get("baseurl") or DEFAULT_API,
                "ilink_bot_id": resp.get("ilink_bot_id"),
                "ilink_user_id": resp.get("ilink_user_id"),
                "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if not _save_login_session(store, session):
                return 1
            print("登录成功，会话已保存到本地受限权限文件。")
            return 0
        elif status == "expired":
            print("二维码已过期且刷新次数用完，请重新运行 login。")
            return 1
        elif status == "need_verifycode":
            print("手机上显示了验证码，请重新运行 login 并按手机提示操作。")
            return 1
        elif status == "verify_code_blocked":
            print("验证失败次数过多，请重新运行 login。")
            return 1
        elif status == "scaned_but_redirect":
            host = resp.get("redirect_host")
            if host:
                api = host if host.startswith("http") else "https://" + host
                print("切换到重定向地址继续轮询: %s" % api)
            continue
        elif status == "binded_redirect":
            # 审查第 8 项：带 bot_token 时与 confirmed 一样落盘保存会话。
            # 旧实现只打印「已绑定」就返回，会出现命令行说已登录、
            # serve 却拿不到会话的自相矛盾状态。
            if resp.get("bot_token"):
                session = {
                    "bot_token": resp.get("bot_token"),
                    "baseurl": resp.get("baseurl") or api,
                    "ilink_bot_id": resp.get("ilink_bot_id"),
                    "ilink_user_id": resp.get("ilink_user_id"),
                    "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                if not _save_login_session(store, session):
                    return 1
                print("该 Bot 已绑定到当前实例，会话已保存。")
                return 0
            previous = load_json(store.session_path)
            same_owner = (_valid_session(previous)
                          and resp.get("ilink_user_id") == previous["ilink_user_id"])
            old_bot = previous.get("ilink_bot_id") if isinstance(previous, dict) else None
            new_bot = resp.get("ilink_bot_id")
            if (not same_owner or (old_bot and new_bot and old_bot != new_bot)
                    or os.path.exists(store.needs_relogin_path)):
                print("已绑定响应没有新 token，且无法确认可沿用的同账号有效会话；请重新登录。")
                return 1
            print("该 Bot 已绑定到当前实例，无需重复登录（未返回新 token，沿用已有会话）。")
            return 0
        else:
            print("未知状态 %r，继续等待…" % (status,))
        time.sleep(1)
    print("等待超时（10 分钟），请重新运行 login。")
    return 1


def cmd_serve(args):
    store = get_store()
    session = load_json(store.session_path)
    if not _valid_session(session):
        print("缺少有效登录凭据或机主 ID，请先运行 login / wait-login。")
        return 1
    if os.path.exists(store.needs_relogin_path):
        print("检测到 NEEDS_RELOGIN 标记，token 已失效，请重新扫码登录。")
        return 1
    baseurl = (session.get("baseurl") or DEFAULT_API).rstrip("/")
    return serve_run(store, baseurl, session["bot_token"], session.get("ilink_user_id"))


def _send_target(store, session, args):
    ctx = load_json(store.last_ctx_path) or {}
    to_user_id = args.to or ctx.get("to_user_id") or session.get("ilink_user_id")
    context_token = args.context or ctx.get("context_token") or ""
    return to_user_id, context_token


def _handle_send_token_invalid(store, baseurl, session):
    """send 侧 -14（P4）：先探活，探活也 -14 才判失效，与 serve 行为一致。"""
    if probe_token_invalid(store, baseurl, session["bot_token"]):
        mark_needs_relogin(store, "send 返回 -14 且探活 -14，token 确认失效")
        print("token 失效，已写 NEEDS_RELOGIN 标记，请重新扫码。")
    else:
        print("发送返回 -14，但探活正常（可能限流），未标记失效，稍后再试。")
    return 1


def cmd_send(args):
    store = get_store()
    session = load_json(store.session_path)
    if not _valid_session(session):
        print("缺少有效登录凭据或机主 ID，请先运行 login / wait-login。")
        return 1
    if os.path.exists(store.needs_relogin_path):
        print("token 已失效，请重新扫码登录。")
        return 1
    if not args.text and not args.image and not args.file and not args.video:
        print("请用 --text / --image / --file / --video 指定内容。")
        return 1
    to_user_id, context_token = _send_target(store, session, args)
    baseurl = (session.get("baseurl") or DEFAULT_API).rstrip("/")
    media_path = args.image or args.file or args.video
    # 每次新的 CLI 操作有独立身份；本次操作的配文和媒体也各自独立。
    operation_key = "cli:%s" % uuid.uuid4().hex
    try:
        if media_path:
            if not os.path.exists(media_path):
                print("文件不存在: %s" % media_path)
                return 1
            # P3：配文先作为独立文本发（媒体函数不再发配文，避免重试重复发）
            # 审查第 8 项：配文响应也要判成败，旧实现完全不查。
            if args.text:
                resp = send_message(baseurl, session["bot_token"], to_user_id,
                                    args.text, context_token,
                                    client_key=operation_key + ":caption", pace=0)
                if is_token_invalid(resp):
                    return _handle_send_token_invalid(store, baseurl, session)
                if not resp_ok(resp):
                    print("配文发送失败: %s" % str(resp)[:300])
                    return 1
            resp = send_media_message(baseurl, session["bot_token"], to_user_id,
                                      context_token, media_path,
                                      media_type_from_path(media_path),
                                      client_key=operation_key + ":media")
        else:
            resp = send_message(baseurl, session["bot_token"], to_user_id,
                                args.text, context_token,
                                client_key=operation_key + ":text", pace=0)
    except Exception as e:
        print("发送出错: %s" % e)
        return 1
    if is_token_invalid(resp):
        return _handle_send_token_invalid(store, baseurl, session)
    # 审查第 8 项：旧实现只查 -14，其余一律打印结果返回 0 ——服务端回
    # {"ret": -2} 也报「发送成功」。改用 resp_ok 与 serve 同一判据。
    if not resp_ok(resp):
        print("发送失败: %s" % str(resp)[:300])
        return 1
    print("发送结果: %s" % str(resp)[:300])
    return 0


def cmd_status(args):
    store = get_store()
    session = load_json(store.session_path)
    print("== 登录状态 ==")
    if session and session.get("bot_token"):
        print("已登录  bot_id=%s  user_id=%s"
              % (session.get("ilink_bot_id"), session.get("ilink_user_id")))
        print("baseurl=%s  登录时间=%s" % (session.get("baseurl"), session.get("saved_at")))
        print("bot_token=%s（已脱敏）" % masked(session.get("bot_token")))
    else:
        print("未登录（请先运行 login / wait-login）")
    if os.path.exists(store.needs_relogin_path):
        with open(store.needs_relogin_path, encoding="utf-8") as f:
            print("!! 需要重新扫码登录: %s" % f.read().strip())
    print("== 运行状态 ==")
    pid = None
    if os.path.exists(store.pid_path):
        with open(store.pid_path, encoding="utf-8") as f:
            pid = f.read().strip()
    alive = bool(pid and pid.isdigit())
    if alive:
        try:
            os.kill(int(pid), 0)
        except OSError:
            alive = False
    if alive:
        # P7：防 pid 被无关进程复用误判——校验 cmdline 确是 wechat-bridge
        try:
            with open("/proc/%s/cmdline" % pid, "rb") as f:
                alive = b"wechat-bridge" in f.read()
        except OSError:
            pass  # /proc 不可读时退回 kill -0 的结果
    print("serve: %s" % ("运行中 (pid %s)" % pid if alive else "未运行"))
    try:
        inbox_n = len([f for f in os.listdir(store.inbox_dir) if f.endswith(".json")])
    except OSError:
        inbox_n = 0
    try:
        pending_n = len([f for f in os.listdir(store.pending_dir) if f.endswith(".json")])
    except OSError:
        pending_n = 0
    # 审查第 1 项：sending/ 也计数显示——滞留任务不能再隐形
    try:
        sending_n = len([f for f in os.listdir(store.sending_dir) if f.endswith(".json")])
    except OSError:
        sending_n = 0
    try:
        failed_n = len([f for f in os.listdir(store.failed_dir) if f.endswith(".json")])
    except OSError:
        failed_n = 0
    print("inbox: %d 条，outbox/pending: %d 条待发送，outbox/sending: %d 条发送中，outbox/failed: %d 条死信"
          % (inbox_n, pending_n, sending_n, failed_n))
    try:
        quarantine_n = len([name for name in os.listdir(store.quarantine_dir)
                            if name.endswith(".json")])
    except OSError:
        quarantine_n = None
    quarantine_notice = os.path.exists(store.quarantine_status_path)
    print("接收隔离: %s；隔离提示标记: %s" % (
        "%d 条" % quarantine_n if quarantine_n is not None else "无法读取",
        "存在" if quarantine_notice else "无"))
    if quarantine_n or quarantine_n is None or quarantine_notice:
        # 不读取或显示原始隔离内容，其中可能含消息正文与认证信息。
        print("!! 接收数据需人工检查: %s" % store.quarantine_dir)
    ctx = load_json(store.last_ctx_path)
    print("最近会话: %s" % ("有" if ctx else "无（需先在微信里发一条消息）"))
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="微信 iLink / Clawbot Bot 桥接")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("login", help="获取登录二维码")
    sub.add_parser("wait-login", help="轮询扫码状态完成登录")
    sub.add_parser("serve", help="常驻收发消息")
    sp = sub.add_parser("send", help="发送消息")
    sp.add_argument("--text", default="", help="消息文本（发媒体时作为配文）")
    sp.add_argument("--image", default=None, help="图片路径")
    sp.add_argument("--file", default=None, help="文件路径")
    sp.add_argument("--video", default=None, help="视频路径")
    sp.add_argument("--to", default=None, help="目标用户 ID（默认用缓存的）")
    sp.add_argument("--context", default=None, help="context_token（默认用缓存的）")
    sub.add_parser("status", help="查看登录和运行状态")
    args = p.parse_args(argv)
    fn = {
        "login": cmd_login,
        "wait-login": cmd_wait_login,
        "serve": cmd_serve,
        "send": cmd_send,
        "status": cmd_status,
    }
    return fn[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
