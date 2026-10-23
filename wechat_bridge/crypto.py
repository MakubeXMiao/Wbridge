"""加解密：微信 CDN 媒体的 AES-128-ECB + PKCS7。

aes_key 有 4 种格式：16 字节原文 / 32 hex 字符 / base64(16字节) / base64(32hex)。
"""

import base64
import hashlib


def _aes_ecb_encrypt(data, key):
    from Crypto.Cipher import AES
    pad = 16 - len(data) % 16
    data = data + bytes([pad]) * pad
    return AES.new(key, AES.MODE_ECB).encrypt(data)


def _aes_ecb_decrypt(data, key):
    from Crypto.Cipher import AES
    if not data or len(data) % AES.block_size:
        raise ValueError("AES 密文必须是非空的完整块")
    out = AES.new(key, AES.MODE_ECB).decrypt(data)
    pad = out[-1]
    if pad < 1 or pad > 16:
        raise ValueError("AES 解密 padding 非法")
    if out[-pad:] != bytes([pad]) * pad:
        raise ValueError("AES 解密 padding 校验失败")
    return out[:-pad]


def _is_hex(b):
    return all((48 <= c <= 57) or (97 <= c <= 102) or (65 <= c <= 70) for c in b)


def parse_aes_key(s):
    """解析微信 CDN 的 aes_key。base64 严格解析（标准 + URL 安全），避免静默丢字符。"""
    if isinstance(s, str):
        s = s.encode("utf-8")
    if not isinstance(s, (bytes, bytearray)):
        raise ValueError("aes_key 必须是字符串或字节")
    if len(s) == 16:
        return bytes(s)
    if len(s) == 32 and _is_hex(s):
        return bytes.fromhex(s.decode("ascii"))

    def _b64_try(data, altchars=None):
        data = data.strip()
        pad = (-len(data)) % 4
        try:
            return base64.b64decode(data + b"=" * pad, altchars=altchars, validate=True)
        except Exception:
            return None

    for alt in (None, b"-_"):
        decoded = _b64_try(s, altchars=alt)
        if decoded is None:
            continue
        if len(decoded) == 32 and _is_hex(decoded):
            return bytes.fromhex(decoded.decode("ascii"))
        if len(decoded) == 16:
            return decoded
    raise ValueError("aes_key 格式无法解析 (len=%d)" % len(s))


def image_magic(data):
    """Return a fixed category, never any bytes from the supplied data."""
    if data.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "GIF"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "WEBP"
    return "other"


def _key_derivations(value):
    # Bound speculative parsing; retain the original bytes for raw MD5/truncation.
    if isinstance(value, str):
        try:
            value = value.encode("utf-8")
        except UnicodeError:
            return []
    if not isinstance(value, (bytes, bytearray)) or not 0 < len(value) <= 4096:
        return []
    raw = bytes(value)
    result = []
    seen = set()

    def add(key, name):
        if len(key) in (16, 32) and key not in seen:
            seen.add(key)
            result.append((key, name))

    if len(raw) == 16:
        add(raw, "raw16")
    if len(raw) == 32 and _is_hex(raw):
        add(bytes.fromhex(raw.decode("ascii")), "hex32")
    for alt in (None, b"-_"):
        data = raw.strip()
        try:
            decoded = base64.b64decode(data + b"=" * ((-len(data)) % 4),
                                       altchars=alt, validate=True)
        except Exception:
            continue
        tag = "b64" if alt is None else "b64url"
        if len(decoded) == 32 and _is_hex(decoded):
            add(bytes.fromhex(decoded.decode("ascii")), tag + "-hex32")
        if len(decoded) == 16:
            add(decoded, tag + "-16")
        if len(decoded) == 32:
            add(decoded, tag + "-32(AES256)")
            add(hashlib.md5(decoded).digest(), tag + "-md5")
        if len(decoded) >= 16:
            add(decoded[:16], tag + "-first16")
        if len(decoded) >= 32:
            add(decoded[:32], tag + "-first32")
    if len(raw) == 32:
        add(raw, "raw32(AES256)")
    if len(raw) >= 16:
        add(raw[:16], "raw-first16")
    if len(raw) >= 32:
        add(raw[:32], "raw-first32")
    add(hashlib.md5(raw).digest(), "raw-md5")
    return result


def try_decrypt_variants(encrypted, raw_key_str, key_override=None,
                         expected_type=None, attempted=None):
    """暴力尝试多种密钥派生 × 加密模式组合，返回 (明文, 策略名)。

    用于微信视频等搞不清密钥格式的场景：哪个能解出有效文件头
    （MP4 的 ftyp / JPEG 头）就用哪个。
    """
    from Crypto.Cipher import AES
    if not encrypted or len(encrypted) % AES.block_size:
        return None, None
    derivations = _key_derivations(raw_key_str)
    if isinstance(key_override, bytes) and len(key_override) in (16, 32):
        derivations.insert(0, (key_override, "override"))
    candidates = []
    seen = set()
    for kb, dname in derivations:
        variants = [("ECB", None, 0, "ECB"),
                    ("CBC", b"\x00" * 16, 0, "CBC0")]
        if expected_type == "image":
            variants.append(("CBC", kb[:16], 0,
                             "CBC-key" if len(kb) == 16 else "CBC-key-first16"))
            if len(encrypted) >= 32:
                variants.append(("CBC", encrypted[:16], 16, "CBC-prefix-IV"))
        for mode, iv, offset, suffix in variants:
            identity = (kb, mode, iv, offset)
            if identity not in seen:
                seen.add(identity)
                candidates.append((kb, mode, iv, offset, dname + "-" + suffix))
    for kb, mode, iv, offset, name in candidates:
        if attempted is not None:
            attempted.append(name)
        try:
            def cipher():
                return AES.new(kb, AES.MODE_ECB) if mode == "ECB" else \
                    AES.new(kb, AES.MODE_CBC, iv=iv)
            # Reject impossible image heads before decrypting a potentially
            # 100MB body for every speculative key/mode. CBC is reset below.
            if expected_type == "image" and image_magic(
                    cipher().decrypt(encrypted[offset:offset + 16])) == "other":
                continue
            out = cipher().decrypt(encrypted[offset:])
        except Exception:
            continue
        pad = out[-1]
        if pad < 1 or pad > 16:
            continue
        if out[-pad:] != bytes([pad]) * pad:
            continue
        plain = out[:-pad]
        if expected_type == "image":
            if image_magic(plain) != "other":
                return plain, name
        elif image_magic(plain) != "other" or plain[:2] == b"\xff\xd8" or b"ftyp" in plain[:32] \
           or plain[:4] == b"\x00\x00\x00\x20" or plain[:8].startswith(b"\x00\x00\x00"):
            return plain, name
    return None, None
