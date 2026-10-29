"""iLink 协议层：HTTP 基础、认证头、登录、消息收发。

只处理微信官方接口的请求/响应，不含业务逻辑。
"""

import base64
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request

from .store import log

DEFAULT_API = "https://ilinkai.weixin.qq.com"
CDN_BASE = "https://novac2c.cdn.weixin.qq.com/c2c"
APP_ID = "bot"
CLIENT_VERSION = "132102"      # 0x020406 -> 2.4.6
CHANNEL_VERSION = "2.4.8"
BOT_AGENT = "Muse"

# 消息 item 类型
ITEM_TEXT, ITEM_IMAGE, ITEM_VOICE, ITEM_FILE, ITEM_VIDEO = 1, 2, 3, 4, 5
# getuploadurl 的 media_type（编号与 item 类型不同）
UPLOAD_IMAGE, UPLOAD_VIDEO, UPLOAD_FILE, UPLOAD_VOICE = 1, 2, 3, 4

MAX_MEDIA_BYTES = 100 << 20  # 100 MB
MAX_TEXT_RUNES = 2000  # 微信单条文本上限（按字符计）
QR_MAX_REFRESH = 3  # 二维码过期自动刷新次数


def rand_uin_b64():
    """随机 uint32 转十进制字符串再 base64（X-WECHAT-UIN 头）。"""
    n = random.randint(0, 0xFFFFFFFF)
    return base64.b64encode(str(n).encode("utf-8")).decode("ascii")


def base_info():
    return {"channel_version": CHANNEL_VERSION, "bot_agent": BOT_AGENT}


def app_headers():
    return {"iLink-App-Id": APP_ID, "iLink-App-ClientVersion": CLIENT_VERSION}


def auth_headers(token):
    h = app_headers()
    h.update({
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "Authorization": "Bearer " + token,
        "X-WECHAT-UIN": rand_uin_b64(),
    })
    return h


def http_json(method, url, headers, body=None, timeout=40, retries=3):
    """发 HTTP JSON 请求。微信服务端偶发直接断开连接，连接层错误自动重试。"""
    import http.client
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    h = {"Accept": "*/*", "User-Agent": "Mozilla/5.0 (compatible; wechat-bridge/1.0)"}
    h.update(headers or {})
    last_err = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            break
        except urllib.error.HTTPError as e:
            raw = e.read()  # HTTP 错误响应也带业务体，不重试
            break
        except (http.client.RemoteDisconnected, http.client.HTTPException,
                ConnectionError, TimeoutError, OSError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
            raise
    else:
        raise last_err
    text = raw.decode("utf-8", "replace")
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_raw": text[:500]}


def is_token_invalid(resp):
    """token 失效的唯一信号：ret / errcode == -14。"""
    if not isinstance(resp, dict):
        return False
    return resp.get("ret") == -14 or resp.get("errcode") == -14


def resp_ok(resp, kind="send"):
    """成功判据（P4，2026-10-21 实测修正）。

    实测 iLink 的 sendmessage 成功响应是 {"message_id": ...}，根本没有 ret；
    getuploadurl 成功响应带 upload_full_url/upload_param，也可能没有 ret。
    初版按「必须 ret==0」判，把已送达的消息当失败重试、造成重复发送，已改：
    有 ret 时以 ret==0 为准；没有 ret 时看接口的成功标志字段；
    无 ret 又无成功标志、或是非 JSON 的 _raw 体，才算可重试失败。
    """
    if not isinstance(resp, dict) or "_raw" in resp:
        return False
    if resp.get("ret") is not None:
        return resp.get("ret") == 0
    if kind == "upload":
        return bool(resp.get("upload_full_url") or resp.get("upload_param"))
    return resp.get("message_id") is not None


def split_text(text, max_runes=MAX_TEXT_RUNES):
    """超长文本按 max_runes 切分（学 wx-bot 的 SplitText）。"""
    if max_runes <= 0:
        max_runes = MAX_TEXT_RUNES
    if len(text) <= max_runes:
        return [text]
    return [text[i:i + max_runes] for i in range(0, len(text), max_runes)]


def _stable_client_id(client_key, chunk_index):
    """由任务标识 + 片号稳定派生 client_id（审查第 3 项）。

    同一任务的同一片在每次重试中用同一个 client_id，服务端若按
    client_id 去重可避免重复投递；旧实现每次调用都用新随机 uuid，
    跨重试毫无幂等键。没有 client_key 时回退为按内容派生。
    """
    seed = ("%s#%d" % (client_key or "", chunk_index)).encode("utf-8")
    return hashlib.md5(seed).hexdigest()


def send_message(baseurl, token, to_user_id, text, context_token, start_chunk=0,
                 on_chunk=None, client_key=None, pace=1.5, max_chunks=None,
                 before_send=None, on_result=None):
    """发文本。超 2000 字自动分片多条发出，返回最后一条的响应。

    分片续发（P3）：返回的 resp 带 `_chunks_done`（下次应从第几片续发）和
    `_chunks_total`。某片收到失败响应时已成功的片不会被重发，调用方把
    _chunks_done 存回任务、下次用 start_chunk 续发即可。

    异常路径的进度（审查第 3 项）：某片抛异常（超时/断连）时，异常对象上
    会挂 `_chunks_done`/`_chunks_total` 后再向上抛——旧实现异常直接穿透、
    进度丢失、任务以 chunks_done=0 回队，重试必然重发第一片。
    若传了 on_chunk 回调，则在每片送达后立即以 (done, total) 回调，
    调用方可实时把进度落盘（进程被杀也不丢）。

    片间节奏（审查第 5 项）：相邻两片之间 sleep pace 秒（默认 1.5，与
    serve 的任务间 pacing 一致），长文本不再连珠炮发出。pace=None 时
    从 PACE_SECONDS 环境读取不到则用 1.5；传 0 关闭（测试用）。
    """
    if pace is None:
        pace = 1.5
    chunks = split_text(text)
    total = len(chunks)
    if isinstance(start_chunk, bool) or not isinstance(start_chunk, int) or start_chunk < 0 or start_chunk > total:
        raise ValueError("invalid confirmed chunk progress")
    if start_chunk == total:
        return {"ret": 0, "_chunks_done": total, "_chunks_total": total}
    if not client_key:
        client_key = "text:%s:%s" % (to_user_id, hashlib.md5(
            (text or "").encode("utf-8")).hexdigest()[:12])
    last_resp = {}
    done_so_far = max(0, start_chunk)
    stop_chunk = total if max_chunks is None else min(
        total, done_so_far + max(0, int(max_chunks)))
    for i in range(max(0, start_chunk), total):
        if i >= stop_chunk:
            return {"_deferred": True, "_chunks_done": done_so_far,
                    "_chunks_total": total}
        payload = {
            "msg": {
                "from_user_id": "",          # 必须空字符串，否则发不出去还不报错
                "to_user_id": to_user_id,
                "client_id": _stable_client_id(client_key, i),
                "message_type": 2,
                "message_state": 2,
                "context_token": context_token or "",
                "item_list": [{"type": ITEM_TEXT, "text_item": {"text": chunks[i]}}],
            },
            "base_info": base_info(),
        }
        try:
            send_allowed = before_send(i, total) if before_send is not None else True
            # 二次审查 P3-6：sendmessage 关掉传输层自动重试（retries=1)。
            # 旧默认 retries=3：服务端已收下、连接才断时，传输层会拿同一
            # 请求再打两次，是否重复投递全靠服务端按 client_id 去重——
            # 未验证、不该赌。改单次尝试：连接错误立刻抛给调用方，由任务
            # 层退避重试（带稳定的 client_id 派生），恢复路径明确可控。
            # getupdates/探活保持默认重试（轮询幂等、无投递副作用）。
            last_resp = (http_json("POST", baseurl + "/ilink/bot/sendmessage",
                                  auth_headers(token), payload, timeout=30, retries=1)
                         if send_allowed is not False else {"ret": 0, "_locally_accepted": True})
        except Exception as e:
            # 审查第 3 项：把已送达片数挂在异常上带出，调用方据此落盘
            e._chunks_done = done_so_far
            e._chunks_total = total
            raise
        if not isinstance(last_resp, dict):
            last_resp = {"_raw": str(last_resp)[:500]}
        last_resp = dict(last_resp)
        if on_result is not None:
            on_result(i, total, last_resp)
        if is_token_invalid(last_resp) or not resp_ok(last_resp):
            last_resp["_chunks_done"] = i  # 第 i 片没发出去，续发从 i 开始
            last_resp["_chunks_total"] = total
            return last_resp
        done_so_far = i + 1
        last_resp["_chunks_done"] = i + 1
        last_resp["_chunks_total"] = total
        if on_chunk is not None:
            on_chunk(i + 1, total)
        # 审查第 5 项：片与片之间也 pacing（最后一片不等）
        if pace and i + 1 < total:
            time.sleep(pace)
    return last_resp


def fetch_qrcode(save_qr_json, save_qr_img):
    """请求新的登录二维码。

    save_qr_json: 保存 {"qrcode","qrcode_img_content","created_at","api"} 的回调
    save_qr_img: 保存二维码图片文件的回调
    返回 (qrcode, img_content)，失败返回 (None, None)。
    """
    url = DEFAULT_API + "/ilink/bot/get_bot_qrcode?bot_type=3"
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": rand_uin_b64(),
        "iLink-App-Id": APP_ID,
        "iLink-App-ClientVersion": CLIENT_VERSION,
    }
    resp = http_json("POST", url, headers, {"local_token_list": []}, timeout=30)
    if not isinstance(resp, dict):
        # 二次审查 P3-5 同类防护：服务端返回 JSON 数组体时 .get 会崩，
        # 登录流按「获取失败」处理而不是 AttributeError 打断。
        log("获取二维码失败: 非 dict 响应 %s" % str(resp)[:200])
        return None, None
    qrcode = resp.get("qrcode")
    img_content = resp.get("qrcode_img_content")
    if not qrcode or not img_content:
        log("获取二维码失败: %s" % json.dumps(resp, ensure_ascii=False)[:500])
        return None, None
    save_qr_json({
        "qrcode": qrcode,
        "qrcode_img_content": img_content,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "api": DEFAULT_API,
    })
    import qrcode as qr_lib
    qr_lib.make(img_content).save(save_qr_img)
    return qrcode, img_content
