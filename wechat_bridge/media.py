"""媒体收发：微信 CDN 的上传下载。

上传：随机 AES 密钥加密 → getuploadurl → POST 密文到 CDN → sendmessage 带媒体引用。
下载：CDN 下载 → AES 解密 → 落盘。
思路参考 lawrencewzen/wx-bot 的 Go 实现。
"""

import base64
import hashlib
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from . import crypto
from .ilink import (
    CDN_BASE, MAX_MEDIA_BYTES,
    ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE,
    UPLOAD_IMAGE, UPLOAD_VIDEO, UPLOAD_FILE, UPLOAD_VOICE,
    _stable_client_id, auth_headers, base_info, http_json, is_token_invalid,
    resp_ok,
)
from .store import log


def media_type_from_path(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
        return ITEM_IMAGE
    if ext in (".mp4", ".mov", ".avi", ".mkv"):
        return ITEM_VIDEO
    return ITEM_FILE


def download_media(media, aes_key_override=None, dest_path=None):
    """从微信 CDN 下载并解密媒体文件。

    media: dict，含 encrypt_query_param / full_url / aes_key。
    注意：视频必须用 encrypt_query_param 拼的地址，full_url 的数据解密会失败。
    """
    if aes_key_override:
        key = aes_key_override
        key_src = "override"
    else:
        raw_key = media.get("aes_key") or ""
        key = crypto.parse_aes_key(raw_key)
        key_src = "media.aes_key(len=%d)" % len(raw_key)

    # 优先用 encrypt_query_param（视频只有这个能解开）
    urls = []
    param = media.get("encrypt_query_param") or ""
    if param:
        urls.append(("param", CDN_BASE + "/download?encrypted_query_param=" +
                     urllib.parse.quote(param, safe="")))
    if media.get("full_url"):
        urls.append(("full_url", media["full_url"]))
    if not urls:
        raise ValueError("media 既无 full_url 也无 encrypt_query_param")

    encrypted = None
    for name, url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read(MAX_MEDIA_BYTES + 1)
            log("下载源 %s: %d 字节" % (name, len(data)))
            if encrypted is None:
                encrypted = data
            break  # 用第一个成功的源
        except Exception as e:
            log("下载源 %s 失败: %s" % (name, e))
    if encrypted is None:
        raise ValueError("所有下载源均失败")
    if len(encrypted) > MAX_MEDIA_BYTES:
        raise ValueError("媒体文件超过 100MB 上限")

    try:
        plain = crypto._aes_ecb_decrypt(encrypted, key)
    except Exception as e:
        log("主解密失败(%s)，尝试其他组合…" % e)
        raw_key = "" if aes_key_override else (media.get("aes_key") or "")
        plain, strategy = crypto.try_decrypt_variants(encrypted, raw_key, aes_key_override)
        if plain is None:
            log("所有解密组合均失败，放弃 (密钥来源=%s)" % key_src)
            raise
        log("解密成功，策略: %s" % strategy)

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp = dest_path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(plain)
    os.chmod(tmp, 0o600)
    os.replace(tmp, dest_path)
    return dest_path


def _get_upload_target(baseurl, token, to_user_id, data, encrypted,
                       upload_media_type, aes_key, heartbeat=None):
    """调 getuploadurl（新 filekey），返回 (full_url, param_url)。两者可缺一。"""
    if heartbeat is not None:
        heartbeat()
    filekey = os.urandom(16).hex()
    body = {
        "filekey": filekey,
        "media_type": upload_media_type,
        "to_user_id": to_user_id,
        "rawsize": len(data),
        "rawfilemd5": hashlib.md5(data).hexdigest(),
        "filesize": len(encrypted),
        "no_need_thumb": True,
        "aeskey": aes_key.hex(),
        "base_info": base_info(),
    }
    # 二次审查 P3-6：getuploadurl 与 sendmessage 同属「发送」类调用，
    # 关掉传输层自动重试（理由见 ilink.send_message 注释）。
    resp = http_json("POST", baseurl + "/ilink/bot/getuploadurl",
                     auth_headers(token), body, timeout=30, retries=1)
    if not resp_ok(resp, "upload"):
        raise RuntimeError("getuploadurl 失败: %s" % str(resp)[:300])
    full_url = (resp.get("upload_full_url") or "").strip()
    param = resp.get("upload_param") or ""
    param_url = ""
    if param:
        param_url = (CDN_BASE + "/upload?encrypted_query_param=" +
                     urllib.parse.quote(param, safe="") +
                     "&filekey=" + urllib.parse.quote(filekey, safe=""))
    if not full_url and not param_url:
        raise RuntimeError("getuploadurl 未返回上传地址")
    return full_url, param_url


def _post_cdn(url, form, encrypted):
    """POST 密文到 CDN，返回 x-encrypted-param；失败抛带状态码的 RuntimeError。"""
    req = urllib.request.Request(
        url, data=encrypted,
        headers={"Content-Type": "application/octet-stream",
                 "User-Agent": "Mozilla/5.0"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status = r.status
            download_param = r.headers.get("x-encrypted-param") or ""
    except urllib.error.HTTPError as e:
        log("CDN 上传失败 form=%s status=%s" % (form, e.code))
        raise RuntimeError("CDN 上传 HTTP %s (form=%s)" % (e.code, form))
    log("CDN 上传 form=%s status=%s" % (form, status))
    if status != 200:
        raise RuntimeError("CDN 上传 HTTP %s (form=%s)" % (status, form))
    if not download_param:
        raise RuntimeError("CDN 响应缺少 x-encrypted-param (form=%s)" % form)
    return download_param


def upload_media(baseurl, token, to_user_id, data, item_type, heartbeat=None):
    """上传媒体到微信 CDN，返回 (encrypt_query_param, aes_key_b64, cipher_size)。

    P5：单次超时 60s；同一地址最多试 2 次；upload_full_url 吃 404 时，
    用新 filekey 重新取址并回退 upload_param 拼接地址再试一次；
    最终错误保留 HTTP 状态码与所用地址形式，便于事后定位。

    heartbeat（二次审查 P1-1）：可选回调，在每次取址/每次上传尝试前
    调用一次。CDN 全挂时本函数最坏能磨 ~5 分钟，serve 靠它在长尝试
    期间持续刷新 flusher 心跳，免得 hook 把健康线程误判为卡死。
    """
    aes_key = os.urandom(16)
    encrypted = crypto._aes_ecb_encrypt(data, aes_key)
    upload_media_type = {ITEM_IMAGE: UPLOAD_IMAGE, ITEM_VIDEO: UPLOAD_VIDEO}.get(
        item_type, UPLOAD_FILE)

    full_url, param_url = _get_upload_target(
        baseurl, token, to_user_id, data, encrypted, upload_media_type,
        aes_key, heartbeat=heartbeat)

    # 候选地址顺序：先 full_url 后 param_url（历史行为优先 full_url 语义的等价物
    # 是原代码在 full_url 非空时只用它；这里 param 作为回退，不改变首选）。
    candidates = []
    if full_url:
        candidates.append(("full_url", full_url))
    if param_url:
        candidates.append(("param", param_url))

    last_err = None
    fallback_used = False
    idx = 0
    while idx < len(candidates):
        form, url = candidates[idx]
        idx += 1
        for attempt in range(2):
            try:
                if heartbeat is not None:
                    heartbeat()
                download_param = _post_cdn(url, form, encrypted)
                # 与官方插件一致：aes_key 取 32 位 hex 字符串再 base64
                aes_key_b64 = base64.b64encode(
                    aes_key.hex().encode("ascii")).decode("ascii")
                return download_param, aes_key_b64, len(encrypted)
            except Exception as e:
                last_err = e
                if "HTTP 404" in str(e) and form == "full_url" and not fallback_used:
                    # 回退一次：新 filekey 重新取址，优先用 param 拼接地址
                    fallback_used = True
                    log("CDN full_url 返回 404，回退重新取址（新 filekey）")
                    try:
                        full2, param2 = _get_upload_target(
                            baseurl, token, to_user_id, data, encrypted,
                            upload_media_type, aes_key, heartbeat=heartbeat)
                        if param2:
                            candidates.append(("param", param2))
                        elif full2 and full2 != url:
                            candidates.append(("full_url", full2))
                    except Exception as e2:
                        log("回退取址失败: %s" % e2)
                    break  # 转到下一个候选（已有 param 或刚追加的回退地址）
                if attempt + 1 < 2:
                    time.sleep(2)
    raise RuntimeError("CDN 上传失败: %s" % last_err)


def send_media_message(baseurl, token, to_user_id, context_token, file_path,
                       item_type, caption="", client_key=None, heartbeat=None):
    """发图片/视频/文件到微信（只发媒体本体）。

    P3：配文不再由本函数发送。旧实现先发配文再发媒体，媒体重试时会把
    配文重复发一遍；现在配文由调用方拆成独立的文本任务（serve 在媒体任务
    前排队一个 *_caption 任务，靠 sent/ 语义保证只发一次），caption 参数
    仅为兼容旧调用而保留、被忽略。
    上传仍在发送之前（失败无用户可见副作用，可安全重试）。

    二次审查 P3-5：client_id 改为按 client_key（调用方传任务名）稳定
    派生，与文本同规则（md5(client_key#0)）。旧实现每次尝试都生成新
    uuid：服务端已收下但响应丢失时，任务层重试会换一个全新 client_id
    再发一遍，图必然重复。传了 client_key 后重试复用同一 client_id，
    服务端按 client_id 去重时可免重；没传才回退随机 uuid（旧行为）。
    """
    with open(file_path, "rb") as f:
        data = f.read()
    if len(data) > MAX_MEDIA_BYTES:
        raise ValueError("文件超过 100MB 上限")
    download_param, aes_key_b64, cipher_size = upload_media(
        baseurl, token, to_user_id, data, item_type, heartbeat=heartbeat)
    media_ref = {"encrypt_query_param": download_param,
                 "aes_key": aes_key_b64, "encrypt_type": 1}
    if item_type == ITEM_IMAGE:
        item = {"type": ITEM_IMAGE,
                "image_item": {"media": media_ref, "mid_size": cipher_size}}
    elif item_type == ITEM_VIDEO:
        item = {"type": ITEM_VIDEO,
                "video_item": {"media": media_ref, "video_size": cipher_size}}
    else:
        item = {"type": ITEM_FILE,
                "file_item": {"media": media_ref,
                              "file_name": os.path.basename(file_path),
                              "len": str(len(data))}}
    payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": (_stable_client_id(client_key, 0) if client_key
                          else uuid.uuid4().hex),
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token or "",
            "item_list": [item],
        },
        "base_info": base_info(),
    }
    # 二次审查 P3-6：同 sendmessage 文本路径，单次尝试（见 ilink 注释）。
    return http_json("POST", baseurl + "/ilink/bot/sendmessage",
                     auth_headers(token), payload, timeout=60, retries=1)
