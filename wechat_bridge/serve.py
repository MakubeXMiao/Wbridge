"""Single-owner iLink receiver and durable outbox service.

Inbox publication and cursor advancement follow durable receipt writes. Replies
are claimed atomically and journaled before network delivery. Merged envelopes
keep their complete text, member coverage and client key across retries/crashes;
they are never split into new identities after a delivery attempt. The sole
flusher recovers abandoned sending files between cycles, preserves committed
backoff/progress, and enforces per-recipient FIFO and a per-context chunk budget.

A stable client ID does not prove server-side exactly-once delivery. The actual
iLink deduplication and Muse platform scheduling still require owner acceptance.
"""

import fcntl
import json
import math
import os
import threading
import time

from .ilink import (
    ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE,
    auth_headers, http_json, is_token_invalid, resp_ok, send_message, split_text,
)
from .media import download_media, media_type_from_path, send_media_message
from .store import (
    Store, atomic_write_json, atomic_write_text, load_json, log,
    mark_needs_relogin, message_receipt_lock, quarantine_record, safe_message_id,
)

MAX_ATTEMPTS = 5
MAX_JOB_AGE = 1800          # 秒：任务龄超过 30 分钟进死信
BACKOFFS = [30, 120, 600]   # 第 1/2/3+ 次失败后的重试间隔
TOKEN_SEND_CAP = 8          # 单个 context_token 在本进程内最多发送条数
PACE_SECONDS = 1.5          # 成功发送之间的间隔
MERGE_WINDOW_SECONDS = 120  # 机械合并：连发文本任务组内 enqueued_at 跨度上限
MERGE_FRESH_SECONDS = 20    # 机械合并：组内最新任务入队未满此时长，整组推迟一轮
FLUSH_INTERVAL_SECONDS = 2.5  # 提速 A：独立 flusher 线程的扫描间隔

_lock_fhs = []  # 单实例锁文件句柄（进程生命周期内持有；用 list 防句柄被 GC 回收放锁）


def extract_text(msg):
    """Extract text independently of attachments (input validated by receiver)."""
    parts = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 1:
            text = (item.get("text_item") or {}).get("text")
            if text:
                parts.append(text)
        elif item.get("type") == ITEM_VOICE:
            text = (item.get("voice_item") or {}).get("text")
            if text:
                parts.append("[语音转写]" + text)
    return "".join(parts)


def _publish_context(store, record, received_at):
    """Publish monotonically, including repair of an interrupted inbox adopt."""
    current = load_json(store.last_ctx_path) or {}
    if not isinstance(current, dict):
        current = {}
    current_at = current.get("received_at")
    if not isinstance(current_at, (int, float)):
        try:
            current_at = time.mktime(time.strptime(current.get("updated_at", ""), "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError, OverflowError):
            current_at = 0
    if received_at < current_at:
        return
    atomic_write_json(store.last_ctx_path, {
        "to_user_id": record["from_user_id"],
        "context_token": record.get("context_token") or "",
        "message_id": record["message_id"], "received_at": received_at,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })


def _receipt_known_locked(store, message_id, from_id):
    """Called under message_receipt_lock; adopt pre-ledger inbox records too."""
    receipt_path = os.path.join(store.receipts_dir, message_id + ".json")
    inbox_path = os.path.join(store.inbox_dir, message_id + ".json")
    if os.path.exists(receipt_path):
        receipt = load_json(receipt_path)
        if (not isinstance(receipt, dict)
                or safe_message_id(receipt.get("message_id")) != message_id
                or receipt.get("from_user_id") != from_id
                or receipt.get("status") not in ("received", "complete")):
            raise ValueError("existing message receipt is inconsistent")
        return True
    if os.path.exists(inbox_path):
        existing = load_json(inbox_path)
        if (not isinstance(existing, dict)
                or safe_message_id(existing.get("message_id")) != message_id
                or existing.get("from_user_id") != from_id):
            raise ValueError("existing inbox record is inconsistent")
        received_at = existing.get("received_at")
        if not isinstance(received_at, (int, float)):
            received_at = os.path.getmtime(inbox_path)
        _publish_context(store, existing, received_at)
        atomic_write_json(receipt_path, {
            "message_id": message_id, "from_user_id": from_id,
            "status": "received", "received_at": time.time(),
        })
        return True
    return False


def _write_inbox_record(store, msg, from_id, text, media_paths=None,
                        media_failed=0):
    """Publish inbox then durable receipt while excluding worker completion.

    A receipt remains after the worker deletes the inbox, so upstream replay
    cannot resurrect a completed task when the Hook trims its seen cache.
    """
    message_id = safe_message_id(msg.get("message_id"))
    record = {
        "message_id": msg["message_id"], "text": text,
        "from_user_id": from_id,
        "context_token": msg.get("context_token") or "",
        "create_time_ms": msg.get("create_time_ms"),
        "received_at": time.time(),
    }
    if media_paths:
        record["media_paths"] = media_paths
    if media_failed:
        record["media_failed"] = True
        record["media_failed_count"] = media_failed
    with message_receipt_lock(store):
        if _receipt_known_locked(store, message_id, from_id):
            return True
        path = os.path.join(store.inbox_dir, message_id + ".json")
        atomic_write_json(path, record)
        # Context is only updated for a newly received message. A replay must
        # not downgrade the latest context token to an old message's token.
        _publish_context(store, record, record["received_at"])
        atomic_write_json(os.path.join(store.receipts_dir, message_id + ".json"), {
            "message_id": message_id, "from_user_id": from_id,
            "status": "received", "received_at": time.time(),
        })
    log("收到消息 [%s]（text_chars=%d media=%d media_failed=%d）" % (
        message_id, len(text), len(media_paths or []), media_failed))
    return True


def _quarantine_incoming(store, payload, reason):
    """Only a durable quarantine counts as accepted; failed writes retain cursor."""
    try:
        return quarantine_record(store, payload, reason)
    except Exception as error:
        log("接收隔离写入失败，本批游标保留（%s）" % type(error).__name__)
        return False


def _incoming_shape_error(msg):
    if not isinstance(msg, dict):
        return "message_not_object"
    if type(msg.get("message_type")) is not int:
        return "message_type_invalid"
    if msg.get("message_type") != 1 or msg.get("group_id"):
        return None  # Non-user/group messages are explicitly filtered.
    if not isinstance(msg.get("from_user_id"), str) or not msg["from_user_id"]:
        return "sender_invalid"
    try:
        safe_message_id(msg.get("message_id"))
    except ValueError:
        return "message_id_invalid"
    if msg.get("context_token") is not None and not isinstance(msg["context_token"], str):
        return "context_token_invalid"
    items = msg.get("item_list")
    if items is None:
        return None
    if not isinstance(items, list):
        return "item_list_not_array"
    subkeys = {1: "text_item", ITEM_IMAGE: "image_item", ITEM_VOICE: "voice_item",
               ITEM_FILE: "file_item", ITEM_VIDEO: "video_item"}
    for item in items:
        if not isinstance(item, dict) or type(item.get("type")) is not int:
            return "item_invalid"
        key = subkeys.get(item["type"])
        if key:
            sub = item.get(key)
            if not isinstance(sub, dict):
                return "item_payload_invalid"
            if item["type"] in (1, ITEM_VOICE) and sub.get("text") is not None \
                    and not isinstance(sub["text"], str):
                return "item_text_invalid"
            if item["type"] in (ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE) \
                    and sub.get("media") is not None and not isinstance(sub["media"], dict):
                return "item_media_invalid"
            if item["type"] == ITEM_FILE and sub.get("file_name") is not None \
                    and not isinstance(sub["file_name"], str):
                return "item_filename_invalid"
    return None


def handle_incoming(store, msg, owner_id):
    """Accept a filtered, persisted, or visibly quarantined input; never drop raw invalid data."""
    if not isinstance(owner_id, str) or not owner_id.strip():
        log("机主 ID 缺失，拒绝接收；请重新确认登录会话")
        return False
    # Don't retain foreign message bodies, even in quarantine.
    if isinstance(msg, dict) and isinstance(msg.get("from_user_id"), str) \
            and msg.get("from_user_id") and msg["from_user_id"] != owner_id:
        return True
    reason = _incoming_shape_error(msg)
    if reason:
        return _quarantine_incoming(store, msg, reason)
    if msg.get("group_id") or msg.get("message_type") != 1:
        return True
    from_id = msg["from_user_id"]
    message_id = safe_message_id(msg.get("message_id"))
    try:
        with message_receipt_lock(store):
            if _receipt_known_locked(store, message_id, from_id):
                return True
    except Exception as error:
        log("接收账本检查失败，本批游标保留（%s）" % type(error).__name__)
        return False

    text = extract_text(msg)
    media_paths = []
    media_failed = 0
    kinds = {ITEM_IMAGE: ("image", "jpg", "image_item"),
             ITEM_VIDEO: ("video", "mp4", "video_item"),
             ITEM_FILE: ("file", "bin", "file_item"),
             ITEM_VOICE: ("voice", "silk", "voice_item")}
    for index, item in enumerate(msg.get("item_list") or []):
        kind_info = kinds.get(item.get("type"))
        if not kind_info:
            continue
        kind, ext, subkey = kind_info
        sub = item[subkey]
        if item["type"] == ITEM_VOICE and not sub.get("media"):
            continue  # The transcription remains useful without downloadable audio.
        filename = sub.get("file_name") if item["type"] == ITEM_FILE else ""
        if filename:
            # Keep readable names, but no path separators/control bytes. The
            # item index also distinguishes duplicate/sanitized file names.
            filename = "".join(c for c in filename if c not in "/\\" and ord(c) >= 32)
            filename = filename.encode("utf-8")[:50].decode("utf-8", "ignore")
        suffix = filename or (kind + "." + ext)
        dest_name = "%s_%03d_%s" % (message_id, index, suffix)
        dest = os.path.join(store.media_dir, time.strftime("%Y%m%d"), dest_name)
        try:
            download_media(sub.get("media") or {}, _media_key(sub), dest)
            media_paths.append(dest)
            log("已接收附件 [%s] item=%d kind=%s" % (message_id, index, kind))
        except Exception as error:
            media_failed += 1
            # Exception strings may carry full_url/query secrets or file names.
            log("附件接收失败 [%s] item=%d kind=%s（%s）" % (
                message_id, index, kind, type(error).__name__))
    if not text.strip():
        if media_paths:
            labels = {".jpg": "[图片]", ".mp4": "[视频]", ".silk": "[语音]"}
            text = labels.get(os.path.splitext(media_paths[0])[1], "[文件]")
        elif media_failed:
            text = "[媒体接收失败：下载/解密失败]"
        else:
            text = "[不支持的消息类型]"
    try:
        return _write_inbox_record(store, msg, from_id, text, media_paths, media_failed)
    except Exception as error:
        # Never replace an actual text with a fallback placeholder after a
        # transient write failure. Replay retains and retries the original data.
        log("写入接收记录失败，本批游标保留（%s）" % type(error).__name__)
        return False


def _media_key(sub):
    """Use the same strict raw16/hex32/base64 parser as nested media.aes_key."""
    value = sub.get("aeskey")
    if not value:
        return None
    try:
        from .crypto import parse_aes_key
        return parse_aes_key(value)
    except (ValueError, TypeError):
        return None


def probe_token_invalid(store, baseurl, token):
    """-14 探活（P4）：用已保存的游标调一次 getupdates，只看是否 -14。

    不保存探活返回的任何东西（游标不动），消息会在正式轮询时正常投递。
    探活本身出错时按「未确认失效」处理（返回 False）。
    """
    buf = ""
    try:
        if os.path.exists(store.buf_path):
            with open(store.buf_path, encoding="utf-8") as f:
                buf = f.read().strip()
    except OSError:
        pass
    body = {"get_updates_buf": buf,
            "base_info": {"channel_version": "2.4.8", "bot_agent": "Muse"}}
    try:
        resp = http_json("POST", baseurl + "/ilink/bot/getupdates",
                         auth_headers(token), body, timeout=15)
    except Exception as e:
        log("token 探活出错（按未失效处理）: %s" % e)
        return False
    return is_token_invalid(resp)


def _token_counts(store):
    counts = getattr(store, "_token_send_counts", None)
    if counts is None:
        counts = {}
        store._token_send_counts = counts
    return counts


def _touch_flusher_heartbeat(store):
    """刷新 flusher 心跳（二次审查 P1-1）。

    内容是 epoch 秒，原子写。flusher 每轮开头、每个发送单元开头、
    以及长操作内部（分片回调、媒体上传每次尝试前）都会调用——任何
    单次阻塞调用都有超时（最长 CDN 上传 60s），所以健康线程的心跳
    间隔远小于 hook 的 120s 判死阈值；心跳真过期 = 线程死或卡死。
    写失败不许影响发送主流程，吞掉。
    """
    try:
        atomic_write_text(store.flusher_hb_path, "%d\n" % int(time.time()))
    except Exception:
        pass


def _mark_sent(store, sending_path, name):
    """把已认领任务移入 sent/ 并收紧权限（二次审查 P3-1）。

    worker 建的 pending 文件是 umask 0660（组可读），任务文件含回复
    全文与 context_token；serve 自己用 atomic_write 写的本就是 0600。
    凡是 os.replace 进 sent/ 的，一律 chmod 0600 对齐。
    """
    dst = os.path.join(store.sent_dir, name)
    os.replace(sending_path, dst)
    try:
        os.chmod(dst, 0o600)
    except OSError:
        pass


def _job_enqueued_at(path, job):
    ts = job.get("enqueued_at")
    if isinstance(ts, (int, float)) and ts > 0:
        return float(ts)
    try:
        return os.path.getmtime(path)
    except OSError:
        return time.time()


def _fail_job(store, sending_path, name, job, reason):
    """把已认领的任务移入 failed/ 并记录原因。"""
    job["failed_reason"] = reason
    job["failed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    atomic_write_json(os.path.join(store.failed_dir, name), job)
    try:
        os.unlink(sending_path)
    except OSError:
        pass
    log("任务进死信 failed/: %s" % reason, job=name)


def _return_to_pending(store, sending_path, pending_path, job):
    """把已认领的任务连同退避状态写回 pending/。"""
    atomic_write_json(pending_path, job)
    try:
        os.unlink(sending_path)
    except OSError:
        pass


def _job_schema_error(job):
    if not isinstance(job, dict):
        return "task must be an object"
    for field in ("text", "media_path", "to_user_id", "context_token", "client_key"):
        if field in job and not isinstance(job[field], str):
            return "%s must be text" % field
    for field in ("attempts", "chunks_done"):
        value = job.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return "%s must be a nonnegative integer" % field
    for field in ("enqueued_at", "next_attempt_at"):
        if field not in job:
            continue
        value = job[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            return "%s must be a finite nonnegative timestamp" % field
    for field in ("merged_from", "covers_message_ids"):
        if field in job and (not isinstance(job[field], list) or
            any(not isinstance(v, (str, int)) or isinstance(v, bool) for v in job[field])):
            return "%s must be an ID list" % field
    if not job.get("media_path") and job.get("chunks_done", 0) > len(split_text(job.get("text", ""))):
        return "confirmed chunk progress exceeds text length"
    return None


def _list_pending(store):
    """读 pending 任务并按 enqueued_at 排序（P1）。

    审查第 12 项：逐文件兜底——单个坏文件（不可读、JSON 损坏、顶层不是
    dict、名为 *.json 的目录）直接改名移入 failed/ 并记原因，绝不让一个
    坏文件堵住整个发送通道，也不在原地每轮重试。load_json 本身吞了
    FileNotFoundError/JSONDecodeError 并返回 default，所以这里先自己把
    原文读出来验一遍，坏的才走隔离。
    """
    try:
        names = [f for f in os.listdir(store.pending_dir) if f.endswith(".json")]
    except OSError:
        return []
    jobs = []
    for name in names:
        path = os.path.join(store.pending_dir, name)
        try:
            job = None
            if os.path.isfile(path):
                try:
                    import json as _json
                    with open(path, encoding="utf-8") as f:
                        job = _json.load(f)
                except Exception:
                    job = None
            schema_error = _job_schema_error(job) if isinstance(job, dict) else "not an object"
            if schema_error:
                log("invalid outbox task: %s" % schema_error, job=name)
            if not isinstance(job, dict) or schema_error:
                # 宽限期：文件太新可能是 worker 正在写（非原子写一半），
                # 本轮先跳过，下轮再判，避免误杀在写任务。
                try:
                    if not isinstance(job, dict) and time.time() - os.path.getmtime(path) < 10:
                        log("任务文件暂不可解析且较新，本轮跳过（可能在写）",
                            job=name)
                        continue
                except OSError:
                    continue
                quarantine = os.path.join(
                    store.failed_dir,
                    name[:-5] + ".corrupt-" +
                    time.strftime("%Y%m%d%H%M%S") + ".json")
                try:
                    os.replace(path, quarantine)
                    if os.path.isfile(quarantine):
                        os.chmod(quarantine, 0o600)
                    log("任务文件损坏，已隔离到 failed/: %s" % quarantine,
                        job=name)
                except OSError as e:
                    log("任务文件损坏且隔离失败，本轮跳过: %s" % e, job=name)
                continue
        except OSError as e:
            log("读取任务文件时异常，本轮跳过: %s" % e, job=name)
            continue
        jobs.append((_job_enqueued_at(path, job), name, job))
    jobs.sort(key=lambda x: (x[0], x[1]))
    return jobs


def _split_caption(store, name, job):
    """P3 + 审查第 4 项：把媒体任务的配文拆成独立文本任务。

    历史问题：旧实现只把配文写进 pending 就立刻发送媒体，配文实际要等
    下一轮 flush 才发（图先到、说明后到），与注释/文档写的「排在媒体
    之前」正好相反。现在返回 (处理后的 job, 配文任务名, 配文 job|None)：
    配文 job 非 None 表示本轮刚拆出、由 _deliver_claimed 在同一次流程里
    先认领发出；配文名为 None 表示本任务没有配文。job 已拆过
    （caption_split）或配文任务已存在于任一目录时配文 job 为 None，
    但仍返回配文名——调用方据此查配文的实际状态（sent/pending/failed）
    决定媒体是发、是等、还是跟着进死信，任何情况下都不抢跑。

    配文任务文件名 <原任务名去 .json>_caption.json；只创建一次。
    """
    base = name[:-5] if name.endswith(".json") else name
    cap_name = base + "_caption.json"
    if job.get("caption_split"):
        return job, cap_name, None
    caption = job.get("text") or ""
    if not caption:
        return job, None, None
    job = dict(job)
    job["text"] = ""
    job["caption_split"] = True
    for d in (store.pending_dir, store.sending_dir, store.sent_dir,
              store.failed_dir):
        if os.path.exists(os.path.join(d, cap_name)):
            return job, cap_name, None
    cap_job = {
        "to_user_id": job.get("to_user_id") or "",
        "text": caption,
        "context_token": job.get("context_token") or "",
        "message_id": job.get("message_id"),
        "kind": "caption",
        "enqueued_at": _job_enqueued_at(
            os.path.join(store.sending_dir, name), job),
    }
    # Preserve observation times through caption splitting. Queue identity,
    # coverage and delivery decisions remain unchanged.
    for field in ("received_at", "worker_started_at"):
        if field in job:
            cap_job[field] = job[field]
    return job, cap_name, cap_job


def _is_mergeable_text(name, job):
    """机械合并候选：纯文本、未尝试过、无分片进度、非配文任务。"""
    if name.endswith("_caption.json") or job.get("kind") == "caption":
        return False
    if job.get("media_path") or job.get("client_key") or job.get("merged_from"):
        return False
    if job.get("delivery_state") in ("inflight", "recovered", "prepared"):
        return False
    if int(job.get("attempts") or 0) != 0:
        return False
    if int(job.get("chunks_done") or 0) != 0:
        return False
    return bool((job.get("text") or "").strip())


def _trace_delivery(stage, name, job, started_at=None, chunks_done=None,
                    chunks_total=None):
    """Best-effort timing only; never serialize reply bodies or credentials.

    ``api_accepted`` records an observed API acknowledgement, not phone receipt.
    Text acknowledgements are per chunk; elapsed time spans this send attempt
    (including any earlier chunks/pacing), not an individual HTTP round trip.
    """
    try:
        at = time.time()
        row = {"stage": stage, "at": at}
        job_id = name[:-5] if isinstance(name, str) and name.endswith(".json") else name
        try:
            row["job_id"] = safe_message_id(job_id)
        except (ValueError, TypeError):
            pass
        ids = job.get("covers_message_ids")
        if not isinstance(ids, (list, tuple)) or not ids:
            ids = [job.get("message_id")]
        message_ids = []
        for value in ids:
            try:
                mid = safe_message_id(value)
            except (ValueError, TypeError):
                continue
            if mid not in message_ids:
                message_ids.append(mid)
        row["message_ids"] = message_ids
        row["count"] = len(message_ids)
        for field in ("received_at", "worker_started_at", "enqueued_at"):
            value = job.get(field)
            if (not isinstance(value, bool) and isinstance(value, (int, float))
                    and math.isfinite(value) and value >= 0):
                row[field] = value
        for field in ("received_at", "worker_started_at", "enqueued_at"):
            if field in row:
                row["since_" + field[:-3] + "_ms"] = round(max(0, at - row[field]) * 1000, 3)
        if (not isinstance(started_at, bool) and isinstance(started_at, (int, float))
                and math.isfinite(started_at) and started_at >= 0):
            row["send_started_at"] = started_at
            row["attempt_elapsed_ms"] = round(max(0, at - started_at) * 1000, 3)
        for field, value in (("chunks_done", chunks_done), ("chunks_total", chunks_total)):
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                row[field] = value
        log("latency " + json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
    except Exception:
        # Observability failures must not change delivery or retry decisions.
        pass


def _deliver_claimed(store, baseurl, token, counts, name, job,
                     sending_path, pending_path, enqueued_at, attempts):
    """任务已认领（文件在 sending/）后的发送与结果处置。

    单发与合并组降级逐条发送共用。返回 False 仅当 token 确认失效
    （sendmessage -14 且探活也 -14，已写 NEEDS_RELOGIN、任务留队）。
    """
    to_user_id = job.get("to_user_id") or ""
    tok_key = job.get("context_token") or ""
    attempt_no = attempts + 1
    # 状态机（任务书设计）：claimed → inflight → 分发处置。
    # 一进入发送流程就打上 inflight 并落盘，配合 run() 启动时对
    # sending/ 的全量回收，serve 中途被杀也能被识别、退回重发。
    job["delivery_state"] = "inflight"
    job.setdefault("client_key", name)
    job["attempts"] = attempt_no
    try:
        atomic_write_json(sending_path, job)
        media_path = job.get("media_path") or ""
        if media_path:
            # 审查第 4 项：配文必须先于媒体送达。本轮新拆出的配文先在
            # 同一次流程里认领发出；配文若只是排了重试（还没送达），
            # 媒体顺延到配文的下一次重试之后，绝不抢跑。
            job, cap_name, cap_job = _split_caption(store, name, job)
            if cap_job is not None:
                cap_sending = os.path.join(store.sending_dir, cap_name)
                cap_pending = os.path.join(store.pending_dir, cap_name)
                cap_job["delivery_state"] = "inflight"
                atomic_write_json(cap_sending, cap_job)
                log("配文已拆为独立任务 %s（先于媒体发送）" % cap_name,
                    job=name)
                if not _deliver_claimed(store, baseurl, token, counts,
                                        cap_name, cap_job, cap_sending,
                                        cap_pending, enqueued_at, 0):
                    # token 确认失效：媒体也别发了，原样回 pending
                    job["next_attempt_at"] = 0
                    _return_to_pending(store, sending_path, pending_path, job)
                    return False
            if cap_name:
                # 统一按配文的实际落点决定媒体的命运（本轮刚发的、以前
                # 拆好还在排队的都一样）：
                # - 配文已死信 → 媒体失去说明，跟着进死信等人工处置，
                #   不发无头无尾的图，配文也不会被无限单独重发；
                # - 配文还在 pending/sending（未送达）→ 媒体顺延到它
                #   送达之后，绝不抢跑（审查第 4 项的重试路径）；
                # - 配文已在 sent（或从未拆出）→ 正常发媒体。
                cap_pending = os.path.join(store.pending_dir, cap_name)
                if os.path.exists(os.path.join(store.failed_dir, cap_name)):
                    _fail_job(store, sending_path, name, job,
                              "配文 %s 已进死信，媒体随之进死信待人工处置"
                              % cap_name)
                    return True
                if os.path.exists(cap_pending) or os.path.exists(
                        os.path.join(store.sending_dir, cap_name)):
                    cap_state = load_json(cap_pending) or {}
                    job["next_attempt_at"] = float(
                        cap_state.get("next_attempt_at") or
                        (time.time() + BACKOFFS[0]))
                    _return_to_pending(store, sending_path, pending_path, job)
                    log("配文未送达，媒体顺延", job=name)
                    return True
            if not os.path.exists(media_path):
                _fail_job(store, sending_path, name, job,
                          "媒体文件不存在: %s" % media_path)
                return True
            if counts.get(tok_key, 0) >= TOKEN_SEND_CAP:
                job["budget_deferred"] = True
                job["attempts"] = attempts
                job["next_attempt_at"] = 0
                _return_to_pending(store, sending_path, pending_path, job)
                return True
            send_started_at = time.time()
            _trace_delivery("send_started", name, job, started_at=send_started_at)
            resp = send_media_message(
                baseurl, token, to_user_id,
                job.get("context_token") or "", media_path,
                media_type_from_path(media_path),
                client_key=job["client_key"],
                heartbeat=lambda: _touch_flusher_heartbeat(store))
            sent_units = 1
            if resp_ok(resp):
                _trace_delivery("api_accepted", name, job, started_at=send_started_at)
                counts[tok_key] = counts.get(tok_key, 0) + 1
        else:
            start_chunk = int(job.get("chunks_done") or 0)

            def _persist_chunk_progress(done, total,
                                        _job=job, _path=sending_path):
                _trace_delivery("api_accepted", name, _job,
                                started_at=send_started_at,
                                chunks_done=done, chunks_total=total)
                # 每片送达立即落盘：进程被杀也不丢已达片数（审查第 3 项）
                previous = int(_job.get("chunks_done") or 0)
                _job["chunks_done"] = done
                counts[tok_key] = counts.get(tok_key, 0) + max(0, done - previous)
                atomic_write_json(_path, _job)
                # 顺手刷新心跳：长文本逐片发时 flusher 显然活着（P1-1）
                _touch_flusher_heartbeat(store)

            send_started_at = time.time()
            if start_chunk < len(split_text(job.get("text") or "")):
                _trace_delivery("send_started", name, job, started_at=send_started_at)
            resp = send_message(
                baseurl, token, to_user_id, job.get("text") or "",
                job.get("context_token") or "",
                start_chunk=start_chunk,
                on_chunk=_persist_chunk_progress,
                client_key=job["client_key"], pace=PACE_SECONDS,
                max_chunks=max(0, TOKEN_SEND_CAP - counts.get(tok_key, 0)))
            done = int(resp.get("_chunks_done") or 0) if isinstance(resp, dict) else 0
            total = int(resp.get("_chunks_total") or 0) if isinstance(resp, dict) else 0
            sent_units = max(0, done - start_chunk)
            if isinstance(resp, dict) and total:
                job["chunks_done"] = done

        if isinstance(resp, dict) and resp.get("_deferred"):
            job["budget_deferred"] = True
            job["attempts"] = attempts
            job["next_attempt_at"] = 0
            _return_to_pending(store, sending_path, pending_path, job)
            return True

        if is_token_invalid(resp):
            # P4：先探活，探活也 -14 才判失效
            if probe_token_invalid(store, baseurl, token):
                mark_needs_relogin(store, "sendmessage -14 且探活 -14，token 确认失效")
                log("token 确认失效，已写 NEEDS_RELOGIN，任务留队", job=name)
                job["next_attempt_at"] = 0
                _return_to_pending(store, sending_path, pending_path, job)
                return False
            log("sendmessage 返回 -14 但探活正常，按可重试失败处理",
                job=name, attempt=attempt_no)
            resp = {"ret": -14, "errmsg": "send -14, probe ok"}

        if resp_ok(resp):
            _mark_sent(store, sending_path, name)
            log("已发送回复", job=name, attempt=attempt_no)
            if PACE_SECONDS > 0:
                time.sleep(PACE_SECONDS)
            return True

        # 可重试失败：记 attempts/退避，或进死信
        reason = "发送返回异常: %s" % str(resp)[:200]
        job["attempts"] = attempt_no
        job["last_error"] = reason
        if attempt_no >= MAX_ATTEMPTS or (time.time() - enqueued_at) > MAX_JOB_AGE:
            _fail_job(store, sending_path, name, job,
                      "attempts=%d 后仍失败: %s" % (attempt_no, reason))
        else:
            backoff = BACKOFFS[min(attempt_no - 1, len(BACKOFFS) - 1)]
            job["next_attempt_at"] = time.time() + backoff
            _return_to_pending(store, sending_path, pending_path, job)
            log("%s；%ds 后重试" % (reason, backoff),
                job=name, attempt=attempt_no)
    except Exception as e:
        # P2：单任务异常隔离——写回 pending 退避，绝不向上抛
        try:
            job["attempts"] = attempt_no
            job["last_error"] = "异常: %s" % e
            # 审查第 3 项：异常路径上也要保住分片进度（异常对象上挂着
            # send_message 记下的已送达片数），否则重试从第一片重发。
            exc_done = getattr(e, "_chunks_done", None)
            if exc_done is not None:
                job["chunks_done"] = int(exc_done)
            if attempt_no >= MAX_ATTEMPTS:
                _fail_job(store, sending_path, name, job,
                          "attempts=%d 后异常: %s" % (attempt_no, e))
            elif os.path.exists(sending_path):
                backoff = BACKOFFS[min(attempt_no - 1, len(BACKOFFS) - 1)]
                job["next_attempt_at"] = time.time() + backoff
                _return_to_pending(store, sending_path, pending_path, job)
            log("发送异常，已退避: %s" % e, job=name, attempt=attempt_no)
        except Exception as e2:
            log("发送异常且回写失败: %s / %s" % (e, e2), job=name)
    return True


def _send_single(store, baseurl, token, counts, c):
    """发送单个任务：额度复检 → 原子认领 → _deliver_claimed。

    返回 False 仅当 token 确认失效。
    """
    name, job = c["name"], c["job"]
    # P3(b) 复检：预处理之后额度可能已被前面的发送单元吃满
    tok_key = job.get("context_token") or ""
    if counts.get(tok_key, 0) >= TOKEN_SEND_CAP:
        job["budget_deferred"] = True
        atomic_write_json(c["pending_path"], job)
        log("context_token 发送额度已满 (%d)，任务留队等新 token"
            % TOKEN_SEND_CAP, job=name)
        return True
    # P2：原子认领。文件已被移走（操作员/worker 介入）则跳过，不崩。
    try:
        os.replace(c["pending_path"], c["sending_path"])
    except FileNotFoundError:
        log("任务在认领前消失，跳过", job=name)
        return True
    except OSError as e:
        log("认领任务失败: %s" % e, job=name)
        return True
    return _deliver_claimed(store, baseurl, token, counts, name, job,
                            c["sending_path"], c["pending_path"],
                            c["enq"], int(job.get("attempts") or 0))


def _archive_merge_member(store, name, job, anchor):
    alias = dict(job)
    alias["merged_into"] = anchor
    alias["delivery_state"] = "covered"
    atomic_write_json(os.path.join(store.sent_dir, name), alias)
    for directory in (store.sending_dir, store.pending_dir):
        try:
            os.unlink(os.path.join(directory, name))
        except FileNotFoundError:
            pass


def _send_merged_group(store, baseurl, token, counts, members):
    """Persist the complete envelope BEFORE sending; never split after a send.

    Even a read timeout may follow server acceptance. Retrying a stable batch
    preserves both its text and client ID; member aliases are never deliveries.
    """
    newest = members[-1]
    name = newest["name"]
    claimed = []
    try:
        for member in members:
            os.replace(member["pending_path"], member["sending_path"])
            claimed.append(member)
    except OSError:
        for member in claimed:
            try:
                os.replace(member["sending_path"], member["pending_path"])
            except OSError:
                pass
        return True
    envelope = dict(newest["job"])
    envelope["text"] = "\n\n".join(m["job"].get("text", "") for m in members)
    envelope["client_key"] = "merged:" + name
    envelope["merged_from"] = [m["name"] for m in members]
    covers = []
    for member in members:
        ids = member["job"].get("covers_message_ids") or [member["job"].get("message_id")]
        for mid in ids:
            if mid is not None and str(mid) not in covers:
                covers.append(str(mid))
    envelope["covers_message_ids"] = covers
    envelope["enqueued_at"] = min(m["enq"] for m in members)
    envelope["delivery_state"] = "prepared"
    # A crash at any following point can discover this manifest and suppress
    # the remaining original members, even before their aliases were written.
    atomic_write_json(newest["sending_path"], envelope)
    for member in members[:-1]:
        _archive_merge_member(store, member["name"], member["job"], name)
    log("prepared durable merged reply (%d members)" % len(members), job=name)
    return _deliver_claimed(store, baseurl, token, counts, name, envelope,
                            newest["sending_path"], newest["pending_path"],
                            envelope["enqueued_at"], int(envelope.get("attempts", 0)))


def flush_outbox(store, baseurl, token):
    """把 outbox/pending/ 里的回复发出去（P1–P4 + 合并兜底语义）。

    先逐任务定性（死信/退避跳过/候选），再把候选里的连发纯文本任务
    按 enqueued_at 贪心分组（同收件人、组内跨度 ≤MERGE_WINDOW_SECONDS、
    最新任务入队满 MERGE_FRESH_SECONDS 才发），组 ≥2 时合并成一条发出；
    其余任务逐条发。返回 False 仅当确认 token 失效，run() 据此退出 serve；
    其余异常都逐单元吞掉记日志，绝不向上抛。
    """
    _recover_sending(store)
    counts = _token_counts(store)
    last_ctx = load_json(store.last_ctx_path) or {}
    if not isinstance(last_ctx, dict):
        last_ctx = {}
    session = load_json(store.session_path) or {}
    owner = session.get("ilink_user_id") if isinstance(session, dict) else None
    now = time.time()

    # —— 预处理：逐任务定性，不产生发送副作用 ——
    candidates = []
    blocked_users = set()
    for enq, name, job in _list_pending(store):
        pending_path = os.path.join(store.pending_dir, name)
        sending_path = os.path.join(store.sending_dir, name)
        try:
            job["enqueued_at"] = enq
            attempts = int(job.get("attempts") or 0)
            if job.get("to_user_id") in blocked_users:
                continue
            # P1：次数/年龄到顶 → 死信
            if attempts >= MAX_ATTEMPTS or (now - enq) > MAX_JOB_AGE:
                try:
                    os.replace(pending_path, sending_path)
                except OSError:
                    continue
                _fail_job(store, sending_path, name, job,
                          "attempts=%d age=%.0fs 超限" % (attempts, now - enq))
                continue
            # P1：退避未到点，跳过不试
            if float(job.get("next_attempt_at") or 0) > now:
                blocked_users.add(job.get("to_user_id"))
                continue
            # 内容校验：无效任务直接死信，不陪跑
            to_user_id = job.get("to_user_id") or ""
            if not to_user_id or (not (job.get("text") or "") and not (job.get("media_path") or "")):
                try:
                    os.replace(pending_path, sending_path)
                except OSError:
                    continue
                _fail_job(store, sending_path, name, job,
                          "任务无效：缺 to_user_id 或内容为空")
                continue
            # P3(c)：未尝试过的任务，若有更新的同用户 token 则升级
            if attempts == 0 or job.get("budget_deferred"):
                new_tok = last_ctx.get("context_token") or ""
                if (new_tok and last_ctx.get("to_user_id") == to_user_id
                        and new_tok != (job.get("context_token") or "")):
                    job["context_token"] = new_tok
                    job.pop("budget_deferred", None)
                    log("任务升级到最新 context_token", job=name)
            if owner and to_user_id != owner:
                os.replace(pending_path, sending_path)
                _fail_job(store, sending_path, name, job, "recipient differs from logged-in owner")
                continue
            candidates.append({"enq": enq, "name": name, "job": job,
                               "pending_path": pending_path,
                               "sending_path": sending_path})
        except Exception as e:
            log("处理任务时未预期异常（已隔离）: %s" % e, job=name)

    # —— 分组：可合并文本任务贪心成组；媒体/配文/已尝试任务各自成单元 ——
    # 审查第 5 项：任何非合并任务都先关闭正在攒的文本组——旧实现把非合并
    # 任务直接 append 成独立 unit 却不关组，后面的文本会跨过它并进前面的
    # 组，导致发送顺序被改（文本 B 越过中间的媒体和文本 A 合并先发）。
    units = []  # [(anchor_enq, [member, ...]), ...]
    group = []
    group_anchor = 0.0
    group_user = None

    def _close_group():
        nonlocal group, group_anchor, group_user
        if group:
            units.append((group_anchor, list(group)))
        group, group_anchor, group_user = [], 0.0, None

    for c in candidates:
        if _is_mergeable_text(c["name"], c["job"]):
            if (group and c["job"].get("to_user_id") == group_user
                    and c["enq"] - group_anchor <= MERGE_WINDOW_SECONDS):
                group.append(c)
            else:
                _close_group()
                group, group_anchor = [c], c["enq"]
                group_user = c["job"].get("to_user_id")
        else:
            _close_group()
            units.append((c["enq"], [c]))
    _close_group()
    units.sort(key=lambda u: u[0])

    # —— 逐单元发送 ——
    blocked_users = set()
    for _anchor, members in units:
        newest = members[-1]
        recipient = newest["job"].get("to_user_id")
        if recipient in blocked_users:
            continue
        # P1-1：每个发送单元开头刷新心跳——一轮 flush 里排了很多任务时，
        # 轮与轮之间可能隔很久，单元级刷新保证健康线程的心跳不假过期。
        _touch_flusher_heartbeat(store)
        try:
            if len(members) >= 2:
                # 组内最新任务还太新：整组推迟一轮，等 worker 把这波写完
                if time.time() - newest["enq"] < MERGE_FRESH_SECONDS:
                    for m in members:
                        m["job"]["next_attempt_at"] = (
                            m["enq"] + MERGE_FRESH_SECONDS)
                        atomic_write_json(m["pending_path"], m["job"])
                    log("合并组最新任务未满 %ds，整组推迟到下一轮: %s"
                        % (MERGE_FRESH_SECONDS,
                           ", ".join(m["name"] for m in members)),
                        job=newest["name"])
                    blocked_users.add(recipient)
                    continue
                if not _send_merged_group(store, baseurl, token, counts, members):
                    return False
            else:
                if not _send_single(store, baseurl, token, counts, members[0]):
                    return False
            if any(os.path.exists(m["pending_path"]) or os.path.exists(m["sending_path"])
                   for m in members):
                blocked_users.add(recipient)
        except Exception as e:
            blocked_users.add(recipient)
            log("发送单元未预期异常（已隔离）: %s" % e, job=newest["name"])
    return True


def _acquire_lock(store):
    """P7：serve 单实例锁。拿到返回 True；已被占用返回 False。"""
    try:
        fh = open(store.lock_path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        return False
    _lock_fhs.append(fh)
    return True


def _recover_sending(store):
    """Recover only between flush cycles, preserving canonical merge identity."""
    try:
        names = [n for n in os.listdir(store.sending_dir) if n.endswith(".json")]
    except OSError:
        return
    if not names:
        return
    records = {}
    anchors = {}
    for directory in (store.sent_dir, store.failed_dir, store.pending_dir, store.sending_dir):
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if not name.endswith(".json"):
                continue
            try:
                job = load_json(os.path.join(directory, name))
            except (OSError, ValueError):
                continue
            if not isinstance(job, dict):
                continue
            records[name] = job
            members = job.get("merged_from")
            if isinstance(members, list) and job.get("client_key"):
                for member in members:
                    if isinstance(member, str) and member != name:
                        anchors[member] = name
    # Resolve member aliases first, while canonical envelopes still exist.
    for name in sorted(names, key=lambda n: n not in anchors):
        src = os.path.join(store.sending_dir, name)
        dst = os.path.join(store.pending_dir, name)
        try:
            job = load_json(src)
            if not isinstance(job, dict):
                os.replace(src, dst)
                continue
            anchor = anchors.get(name) or job.get("merged_into")
            if anchor:
                if anchor not in records:
                    _fail_job(store, src, name, job, "missing canonical merge envelope; manual review required")
                else:
                    _archive_merge_member(store, name, job, anchor)
                continue
            if (os.path.exists(os.path.join(store.sent_dir, name)) or
                    os.path.exists(os.path.join(store.failed_dir, name))):
                # Sent transition already committed, only old source cleanup
                # failed. Never resurrect a committed completed identity.
                os.unlink(src)
                continue
            existing = load_json(dst)
            if isinstance(existing, dict) and existing.get("client_key") == job.get("client_key"):
                if any(existing.get(k) != job.get(k) for k in ("text", "media_path", "to_user_id")):
                    log("conflicting delivery copies require manual review", job=name)
                    continue
                pending = dict(existing)
                pending["chunks_done"] = max(int(existing.get("chunks_done") or 0), int(job.get("chunks_done") or 0))
                pending["attempts"] = max(int(existing.get("attempts") or 0), int(job.get("attempts") or 0))
                pending["next_attempt_at"] = max(float(existing.get("next_attempt_at") or 0), float(job.get("next_attempt_at") or 0))
                job = pending
            else:
                job.setdefault("next_attempt_at", 0)
            job["delivery_state"] = "recovered"
            atomic_write_json(dst, job)
            os.unlink(src)
            log("recovered interrupted delivery", job=name)
        except (OSError, ValueError, TypeError) as error:
            log("delivery recovery pending: %s" % error, job=name)


def _poll_response_bad(resp):
    """getupdates 响应是否属于「错误响应」（审查第 6 项）。

    非 dict、带 _raw（非 JSON 体）、或带非零 ret/errcode 的响应一律按
    轮询失败处理：走连续失败退避、不存游标。-14 不在此列，
    它有单独的确认流程。
    """
    if not isinstance(resp, dict):
        return True
    if "_raw" in resp:
        return True
    for k in ("ret", "errcode"):
        v = resp.get(k)
        if v is not None and v != 0 and v != -14:
            return True
    return False


def run(store, baseurl, token, owner_id):
    """常驻收发主循环。返回 0 正常退出，1 异常退出。

    提速 A（2026-10-21 晚）：flush 已拆到独立 flusher 线程（见下方
    _flusher），主循环只管收消息。线程安全约定：
    - flush_outbox 的所有调用被 _flush_lock 严格串行化，本进程内同时
      只有一个 flusher 在跑；任务认领靠 pending→sending 原子 rename，
      天然只有一个认领者，与主循环写 inbox、worker 写 pending 并发安全。
    - 跨线程共享状态只有三处：store._token_send_counts（token 额度，
      只有 flusher 读写）；last_context.json（主循环原子写、flusher 读，
      读到的永远是完整文件）；NEEDS_RELOGIN 标记文件（任一侧确认
      token 失效时写，两个线程都会据此在数秒内收尾）。
    - flusher 在发送侧确认失效（-14 探活也 -14、flush_outbox 返回
      False）时置位 stop_event；主循环顶部每轮都查 stop_event 与
      NEEDS_RELOGIN，flusher 每轮查 stop_event，于是协同退出。
      _recover_sending 仍在主线程、flusher 启动之前执行。
    - flusher 监督（二次审查 P1-1）：审计发现旧结构里 flusher 线程
      若死于 _flush_once 兜不住的异常（BaseException、解释器级故障），
      没有任何人发现——主循环照常收消息、hook 只验进程存活、status
      照报运行中，回复会静默永停。双保险：(1) 主循环每轮查
      flusher.is_alive()，死了（且不在收尾）就进程内重启线程并记
      日志；(2) flusher 持续刷新 state/flusher_heartbeat，hook 在
      serve 进程活着但心跳 >120s 未更新时按宕机杀进程重拉，覆盖
      「线程卡在无超时路径上、is_alive 仍为真」的情况。
    """
    if not isinstance(owner_id, str) or not owner_id.strip():
        log("机主 ID 缺失，serve 拒绝启动；请重新确认登录会话")
        return 1
    if not _acquire_lock(store):
        log("serve 已有实例在运行（锁被占用），本实例退出")
        return 1
    current = load_json(store.session_path)
    if isinstance(current, dict) and (current.get("bot_token") != token or current.get("ilink_user_id") != owner_id):
        log("session changed before service lock; restart with current credentials")
        return 1
    # 审查第 1 项：独占锁已拿到，先回收上次进程滞留在 sending/ 的任务
    _recover_sending(store)
    atomic_write_text(store.pid_path, str(os.getpid()))
    buf = ""
    if os.path.exists(store.buf_path):
        with open(store.buf_path, encoding="utf-8") as f:
            buf = f.read().strip()
    log("serve 启动，监听用户 %s 的消息…" % owner_id)
    stop_event = threading.Event()
    flush_lock = threading.Lock()

    def _flush_once():
        """锁内跑一轮 flush。出 False 仅当 token 确认失效。"""
        with flush_lock:
            try:
                return flush_outbox(store, baseurl, token)
            except Exception as e:
                # P2：flush 异常永不杀任何线程
                log("flush_outbox 未预期异常（已吞）: %s" % e)
                return True

    def _flusher():
        while not stop_event.wait(FLUSH_INTERVAL_SECONDS):
            if os.path.exists(store.needs_relogin_path):
                return
            # P1-1：每轮开头先打心跳。放在 flush 之前，这样一轮 flush
            # 跑得再久，hook 看到的也是「本轮开始时线程还活着」；轮内
            # 的长耗时由 flush_outbox 的单元级/操作级刷新续命。
            _touch_flusher_heartbeat(store)
            if not _flush_once():
                log("flusher 确认 token 失效，通知主循环收尾")
                stop_event.set()
                return

    def _start_flusher():
        t = threading.Thread(target=_flusher, name="wechat-flush",
                             daemon=True)
        t.start()
        return t

    flusher = _start_flusher()
    consec_failures = 0
    poll_timeout = 45
    try:
        while True:
            if stop_event.is_set() or os.path.exists(store.needs_relogin_path):
                log("NEEDS_RELOGIN 存在，退出 serve。")
                return 1
            # P1-1 监督 (1)：flusher 死了且不在收尾 → 进程内重启。
            # 主循环每轮（每轮最长一轮长轮询）都会走到这里，线程死亡
            # 最迟在下轮轮询返回时被发现；卡死型故障由心跳路径兜底。
            # 重启前先回收 sending/：此刻能往 sending/ 放任务的只有刚
            # 死掉的 flusher（新线程还没起、主循环不认领），里面的任务
            # 必然是它没发完的遗留，移回 pending 由新线程接着发，
            # 语义与启动回收一致。
            if not flusher.is_alive():
                log("flusher 线程已死，回收在途任务并进程内重启"
                    "（回复发送曾中断）")
                _recover_sending(store)
                flusher = _start_flusher()
            url = baseurl + "/ilink/bot/getupdates"
            body = {"get_updates_buf": buf, "base_info": {"channel_version": "2.4.8", "bot_agent": "Muse"}}
            try:
                resp = http_json("POST", url, auth_headers(token), body, timeout=poll_timeout)
            except Exception as e:
                consec_failures += 1
                delay = 30 if consec_failures >= 3 else 2
                log("getupdates 出错，%d 秒后重试 (连续失败 %d 次，%s)"
                    % (delay, consec_failures, type(e).__name__))
                time.sleep(delay)
                continue
            # 审查第 6 项：先判形状再取字段（旧实现先 .get 后判 dict，
            # 服务端返回 JSON 数组能直接打崩 serve）；错误响应与连接
            # 异常同等对待——计数、退避、不存游标、不 flush。
            if _poll_response_bad(resp) and not is_token_invalid(resp):
                consec_failures += 1
                delay = 30 if consec_failures >= 3 else 2
                log("getupdates 返回异常响应，%d 秒后重试 (连续失败 %d 次)"
                    % (delay, consec_failures))
                time.sleep(delay)
                continue
            try:
                suggested = int(resp.get("longpolling_timeout_ms") or 0)
                if suggested > 0:
                    poll_timeout = min(suggested // 1000 + 10, 120)
            except (TypeError, ValueError):
                pass
            if is_token_invalid(resp):
                # -14 也可能是限流：先暂停 5 分钟重试一次，仍失败才判 token 死亡
                log("收到 -14，暂停 5 分钟后重试一次…")
                time.sleep(300)
                try:
                    resp = http_json("POST", url, auth_headers(token), body, timeout=poll_timeout)
                except Exception:
                    resp = {}
                if is_token_invalid(resp):
                    mark_needs_relogin(store, "getupdates 返回 -14，token 失效")
                    log("token 确认失效，已写 NEEDS_RELOGIN 标记，退出。")
                    return 1
                if _poll_response_bad(resp):
                    continue  # 重试后仍是坏响应：下轮按失败计数处理
            # P2 + 审查第 7 项：整批消息全部落 inbox 后，才保存新游标；
            # 任一消息记录落盘失败（handle_incoming 返回 False 或抛出
            # 未被它兜住的异常），本批不保存游标，消息下轮重新投递。
            batch_ok = True
            messages = resp.get("msgs")
            if messages is None:
                messages = []
            elif not isinstance(messages, list):
                # A schema-invalid batch is acknowledged only after its raw
                # contents are durably quarantined and a visible status exists.
                batch_ok = _quarantine_incoming(store, resp, "msgs_not_array")
                messages = []
            for msg in messages:
                try:
                    if handle_incoming(store, msg, owner_id) is False:
                        batch_ok = False
                        log("消息记录落盘失败，本批游标不推进")
                except Exception as e:
                    batch_ok = False
                    log("处理消息出错，本批游标不推进（%s）" % type(e).__name__)
            new_buf = resp.get("get_updates_buf")
            if new_buf is not None and not isinstance(new_buf, str):
                _quarantine_incoming(store, resp, "cursor_not_string")
                batch_ok = False  # There is no safe usable cursor to advance to.
            if batch_ok and new_buf:
                try:
                    atomic_write_text(store.buf_path, new_buf)
                    buf = new_buf  # Never advance in memory before persistence.
                except Exception as e:
                    batch_ok = False
                    log("写入轮询游标失败，保留旧游标（%s）" % type(e).__name__)
            if not batch_ok:
                consec_failures += 1
                delay = 30 if consec_failures >= 3 else 2
                log("本批尚未安全接收，%d 秒后重试 (连续失败 %d 次)"
                    % (delay, consec_failures))
                time.sleep(delay)
                continue
            consec_failures = 0
            # 提速 A：发送已拆给 flusher 线程，主循环不再调用 flush。
    except KeyboardInterrupt:
        log("收到中断，退出 serve。")
        return 0
    finally:
        # 通知 flusher 收尾并等它退出（daemon 线程，超时也不挂死主流程）
        stop_event.set()
        flusher.join(timeout=10)
        try:
            os.remove(store.pid_path)
        except OSError:
            pass
