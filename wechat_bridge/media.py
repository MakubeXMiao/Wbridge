"""媒体收发：微信 CDN 的上传下载。

上传：随机 AES 密钥加密 → getuploadurl → POST 密文到 CDN → sendmessage 带媒体引用。
下载：CDN 下载 → AES 解密 → 落盘。
思路参考 lawrencewzen/wx-bot 的 Go 实现。
"""

import base64
import hashlib
import json
import os
import re
import stat
import tempfile
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


def _key_metadata(value, present=None):
    """Only shape/charset metadata; never a value, digest, prefix or repr."""
    if isinstance(value, str):
        data = value.encode("utf-8", "replace")
        kind, length = "str", len(value)
    elif isinstance(value, (bytes, bytearray)):
        data = bytes(value)
        kind, length = "bytes", len(value)
    else:
        data = b""
        kind, length = ("null" if value is None else "invalid_type"), None
    sample = data[:4097]
    if not sample:
        charset = "empty"
    elif len(data) > 4096:
        charset = "oversized"
    elif all(c in b"0123456789abcdefABCDEF" for c in sample):
        charset = "hex"
    elif all(c in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-" for c in sample):
        charset = "base64_charset"
    elif all(32 <= c < 127 for c in sample):
        charset = "ascii"
    else:
        charset = "non_ascii_or_binary"
    return {"present": value is not None if present is None else present,
            "type": kind, "length": length, "byte_length": len(data),
            "charset": charset}


def log_image_item_fields(item):
    """Bounded identifier-name tree, with all values excluded from output."""
    fields = []
    truncated = False
    private_values = set()

    def remember(value):
        if isinstance(value, bytes):
            try:
                value = value.decode("ascii")
            except UnicodeError:
                return
        if not isinstance(value, str) or not 0 < len(value) <= 4096:
            return
        private_values.add(value)
        try:
            key = crypto.parse_aes_key(value)
        except (ValueError, TypeError):
            return
        private_values.update((key.hex(), key.hex().upper()))
        for encode in (base64.b64encode, base64.urlsafe_b64encode):
            encoded = encode(key).decode("ascii")
            private_values.update((encoded, encoded.rstrip("=")))

    # These actual key fields must not lose protection when unrelated metadata
    # exhausts the bounded generic traversal before it reaches them.
    if isinstance(item, dict):
        remember(item.get("aeskey"))
        nested = item.get("media")
        if isinstance(nested, dict):
            remember(nested.get("aes_key"))

    def opaque_key_name(name):
        if re.fullmatch(r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{64})", name):
            return True
        # Unpadded AES base64 can also be a syntactically valid identifier.
        # Canonical round-trip limits false positives; some unknown names may
        # still be conservatively hidden. No decoded bytes leave this function.
        if len(name) not in (22, 24, 43, 44):
            return False
        raw = name.encode("ascii")
        for alt, encode in ((None, base64.b64encode), (b"-_", base64.urlsafe_b64encode)):
            try:
                decoded = base64.b64decode(raw + b"=" * ((-len(raw)) % 4),
                                           altchars=alt, validate=True)
            except (ValueError, TypeError):
                continue
            if len(decoded) in (16, 32) and encode(decoded).rstrip(b"=") == raw.rstrip(b"="):
                return True
        return False

    def collect(value, depth, budget):
        if depth > 5 or budget[0] <= 0:
            return
        if isinstance(value, dict):
            for child in value.values():
                if budget[0] <= 0:
                    break
                budget[0] -= 1
                collect(child, depth + 1, budget)
        elif isinstance(value, list):
            if value:
                collect(value[0], depth + 1, budget)
        elif isinstance(value, str) and 0 < len(value) <= 64:
            private_values.add(value)
    collect(item, 0, [64])

    def walk(value, path, depth):
        nonlocal truncated
        if len(fields) >= 64 or depth > 5:
            truncated = True
            return
        if isinstance(value, dict):
            for key, child in value.items():
                if len(fields) >= 64:
                    truncated = True
                    break
                safe = (isinstance(key, str) and len(key) <= 64
                        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
                        and key not in private_values and not opaque_key_name(key))
                name = key if safe else "<redacted-field>"
                entry = {"path": path + [name]}
                if isinstance(key, str) and ("key" in key.lower() or key == "iv"):
                    entry["key"] = _key_metadata(child, True)
                fields.append(entry)
                if isinstance(child, (dict, list)):
                    walk(child, entry["path"], depth + 1)
        elif isinstance(value, list):
            if len(value) > 1:
                truncated = True
            if value:
                walk(value[0], path + ["[]"], depth + 1)
    walk(item, ["image_item"], 0)
    log("image_item_fields " + json.dumps(
        {"fields": fields, "truncated": truncated}, separators=(",", ":")))


def download_media(media, aes_key_override=None, dest_path=None, expected_type=None):
    """从微信 CDN 下载并解密媒体文件。

    media: dict，含 encrypt_query_param / full_url / aes_key。
    注意：视频必须用 encrypt_query_param 拼的地址，full_url 的数据解密会失败。
    """
    if not isinstance(media, dict):
        raise ValueError("media 必须是对象")
    if not dest_path:
        raise ValueError("必须指定媒体保存路径")

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

    plain = None
    # MAX_MEDIA_BYTES 是明文上限；PKCS7 即使整块也增加一个完整块。
    # 这里只界定本地读取边界，不推定微信平台允许更大的上传文件。
    cipher_limit = (MAX_MEDIA_BYTES // 16 + 1) * 16
    if expected_type == "image":
        cipher_limit += 16  # A prefixed CBC IV is not part of the plaintext.
    for name, url in urls:
        diagnostic = {"event": "media_receive", "source": name, "stage": "http",
                      "bytes": None, "block_aligned": None, "magic": "other",
                      "keys": {"item.aeskey": _key_metadata(aes_key_override),
                               "media.aes_key": _key_metadata(media.get("aes_key"), "aes_key" in media)},
                      "attempted": [], "expected_type": "image" if expected_type == "image" else "generic"}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read(cipher_limit + 1)
            diagnostic.update(bytes=len(data), block_aligned=bool(data) and len(data) % 16 == 0,
                              magic=crypto.image_magic(data))
            if len(data) > cipher_limit:
                raise ValueError("媒体密文超过本地大小上限")
            diagnostic["stage"] = "decrypt"
            try:
                candidate, strategy = _decrypt_media(
                    data, media, aes_key_override, expected_type,
                    diagnostic["attempted"])
            except ValueError as error:
                # A legal CBC IV can itself start with an image signature.
                # Try keyed decoding before the plaintext fallback; never turn
                # an ambiguous decryption into a successful raw-cipher save.
                if (error.args == ("media_decrypt_ambiguous",)
                        or expected_type != "image" or crypto.image_magic(data) == "other"):
                    raise
                candidate, strategy = data, "plaintext-image"
                diagnostic["attempted"].append(strategy)
            if len(candidate) > MAX_MEDIA_BYTES:
                raise ValueError("媒体明文超过 100MB 上限")
            diagnostic.update(outcome="success", strategy=strategy,
                              plaintext_magic=crypto.image_magic(candidate))
            log("media_receive " + json.dumps(diagnostic, separators=(",", ":")))
            plain = candidate
            break  # 完整下载、解密及大小校验成功后才认定此源成功
        except Exception as e:
            # HTTPError/URLError/Request exception text can contain the entire
            # signed URL. Only fixed classifications may leave this function.
            error = "http_failed" if diagnostic["stage"] == "http" else "decrypt_or_size_failed"
            if isinstance(e, ValueError) and e.args == ("media_decrypt_ambiguous",):
                error = "decrypt_ambiguous"
            diagnostic.update(outcome="failed", error=error)
            if isinstance(e, urllib.error.HTTPError) and isinstance(e.code, int):
                diagnostic["http_status"] = e.code
            log("media_receive " + json.dumps(diagnostic, separators=(",", ":")))
    if plain is None:
        raise ValueError("media_receive_all_sources_failed") from None

    dest_path = os.fspath(dest_path)
    directory = os.path.dirname(os.path.abspath(dest_path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".media-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(plain)
        os.replace(tmp, dest_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return dest_path


def _decrypt_media(encrypted, media, aes_key_override, expected_type=None, attempted=None):
    """旧字段密钥无效/解密失败时，仍尝试合法的 media.aes_key。"""
    if attempted is None:
        attempted = []
    sources = (("item", aes_key_override), ("media", media.get("aes_key")))
    if expected_type == "image":
        selected = None
        selected_strategy = None
        for label, source in sources:
            names = []
            plain, strategy = crypto.try_decrypt_variants(
                encrypted, source, expected_type="image", attempted=names)
            attempted.extend(label + ":" + name for name in names)
            if strategy == "ambiguous-image":
                raise ValueError("media_decrypt_ambiguous")
            if plain is not None:
                if selected is not None and selected != plain:
                    selected_valid = crypto._verify_image_structure(selected)
                    current_valid = crypto._verify_image_structure(plain)
                    if selected_valid == current_valid:
                        raise ValueError("media_decrypt_ambiguous")
                    if selected_valid:
                        continue
                selected, selected_strategy = plain, label + ":" + strategy
        if selected is not None:
            return selected, selected_strategy
        raise ValueError("media_decrypt_failed")
    candidates = []
    for label, source in sources:
        if expected_type == "image" and (not isinstance(source, (str, bytes, bytearray))
                                         or len(source) > 4096):
            continue
        try:
            key = crypto.parse_aes_key(source)
        except (ValueError, TypeError):
            continue
        if key not in [value for _, value in candidates]:
            candidates.append((label, key))
    for label, key in candidates:
        attempted.append(label + ":parsed-ECB")
        try:
            plain = crypto._aes_ecb_decrypt(encrypted, key)
            if expected_type != "image" or crypto.image_magic(plain) != "other":
                return plain, label + ":parsed-ECB"
        except (ValueError, IndexError):
            pass
    for label, raw_key in sources:
        names = []
        plain, strategy = crypto.try_decrypt_variants(
            encrypted, raw_key, expected_type=expected_type, attempted=names)
        attempted.extend(label + ":" + name for name in names)
        if plain is not None:
            return plain, label + ":" + strategy
    raise ValueError("media_decrypt_failed")


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
    if len(data) > MAX_MEDIA_BYTES:
        raise ValueError("文件超过 100MB 上限")
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
    # O_NONBLOCK 避免在 fstat 能拒绝 FIFO 之前就卡在 open 上。
    fd = os.open(file_path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb") as f:
        info = os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("只允许发送普通文件")
        if info.st_size > MAX_MEDIA_BYTES:
            raise ValueError("文件超过 100MB 上限")
        # 即使文件在 fstat 后增长，也不能无界读取。
        data = f.read(MAX_MEDIA_BYTES + 1)
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
