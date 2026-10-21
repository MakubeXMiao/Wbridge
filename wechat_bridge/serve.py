"""常驻服务：长轮询收消息 → inbox/；outbox/pending/ → sendmessage 发出。

2026-10-21 P1–P7 改造要点：
- P1 任务带 attempts/next_attempt_at/enqueued_at；退避 30s→2m→10m；
  attempts>=5 或任务龄 >30 分钟移入 outbox/failed/（死信），不再无限重试；
  按 enqueued_at 排序，不按文件名。
- P2 每条任务处理全程异常隔离，发送侧异常永不杀 serve；任务先原子认领
  进 outbox/sending/ 再发，成功移 sent/，可重试失败写回 pending/；
  get_updates_buf 只在整批消息落 inbox 后才保存；媒体下载失败的消息也写
  inbox 占位记录（media_failed），不静默丢弃。
- P3 配文拆成独立文本任务排在媒体任务之前（只发一次）；发送成功间隔
  1.5s；单 context_token 在 serve 生命周期内最多发 TOKEN_SEND_CAP 条，
  余量留给用户下一条消息；未尝试过的任务若有更新的 token 自动升级。
- P4 resp_ok 按实测判据（sendmessage 成功响应是 {"message_id": ...}、
  无 ret，详见 ilink.resp_ok）；sendmessage 返回 -14 时先用 getupdates
  探活，探活也 -14 才写 NEEDS_RELOGIN（与轮询路径一致）。
- 合并兜底（2026-10-21 加）：同一批连发的纯文本任务在 flush 时机械合并
  为一条发出（同收件人、组内跨度 ≤120s、最新任务入队满 20s 才发），
  防 worker 逐条回复刷屏；合并整条未送达时降级逐条发送（一条毒任务
  不拖全组），分片部分送达时收拢续发、已达分片不重发。
- P7 启动时 fcntl.flock 单实例锁，占不到直接退出。
- 全盘审查 12 项（2026-10-21 晚，用户批准全修）：
  1 sending/ 启动回收（run 拿到锁后全量移回 pending）+ status 显示计数；
  3 分片进度异常路径不丢（异常挂 _chunks_done、on_chunk 实时落盘）+
     client_id 由任务名+片号稳定派生；4 配文在同一次流程里先于媒体
     送达，配文死信则媒体随之死信、配文排重试则媒体顺延；5 合并分组
     被非文本任务截断、token 额度按实际分片数计、片间 1.5s pacing；
  6 getupdates 先判 dict、错误响应走失败退避且不存游标不 flush；
  7 handle_incoming 任何失败都写占位记录，落盘失败则本批不推进游标；
  12 pending 坏文件逐个隔离到 failed/，不再堵通道。
- 提速 A（2026-10-21 晚，用户批准「ABC 全做」）：flush 从 getupdates
  轮询主循环拆出到独立 flusher 线程（每 FLUSH_INTERVAL_SECONDS 秒一
  轮、_flush_lock 串行化）。闲置时轮询一轮最长挂 ~120s，旧实现里回
  复写好也要等轮询返回才发得出，是「闲置后第一条回复慢 1–2 分钟」
  的主因；拆开后回复落盘 ~3 秒内就被 flusher 发走。
- 二次全盘审查修复（2026-10-21 深夜，用户批准 P1–P3 全修）：
  P1-1 flusher 监督双保险——主循环每轮查 flusher.is_alive()，线程死
  于锁外异常（BaseException 等）时进程内重启并记日志；flusher 每轮
  及每个发送单元/长操作内部都刷新 state/flusher_heartbeat，hook 发
  现 serve 进程活着但心跳 >120s 未更新时按宕机处理（杀进程重拉），
  覆盖线程卡死而进程未死的情况。P3-1 移入 sent/ 的任务文件统一
  chmod 0600（worker 建的 pending 文件是 umask 0660，含回复全文与
  context_token，不该组可读）；sent/ 的 30 天保留期清理由 hook 侧做。
"""

import fcntl
import os
import threading
import time

from .ilink import (
    ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE,
    auth_headers, http_json, is_token_invalid, resp_ok, send_message,
)
from .media import download_media, media_type_from_path, send_media_message
from .store import (
    Store, atomic_write_json, atomic_write_text, load_json, log,
    mark_needs_relogin,
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
    parts = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 1:  # 文本
            t = (item.get("text_item") or {}).get("text")
            if t:
                parts.append(t)
        elif item.get("type") == 3:  # 语音：可能带转写文本
            t = (item.get("voice_item") or {}).get("text")
            if t:
                parts.append("[语音转写]" + t)
    return "".join(parts)


def _write_inbox_record(store, msg, from_id, text, media_paths=None,
                        media_failed=0):
    """写 inbox 记录。返回 True 表示落盘成功。

    审查第 7 项：本函数的异常由调用方兜底处理（失败意味着本批游标
    不能推进），不在这里吞。
    """
    message_id = msg.get("message_id")
    record = {
        "message_id": message_id,
        "text": text,
        "from_user_id": from_id,
        "context_token": msg.get("context_token") or "",
        "create_time_ms": msg.get("create_time_ms"),
    }
    if media_paths:
        record["media_paths"] = media_paths
    if media_failed:
        record["media_failed"] = True
        record["media_failed_count"] = media_failed
    path = os.path.join(store.inbox_dir, "%s.json" % message_id)
    if not os.path.exists(path):
        atomic_write_json(path, record)
        log("收到消息 [%s]: %s" % (message_id, (text or "")[:60]))
    # 缓存最新会话上下文，供 send 测试用
    atomic_write_json(store.last_ctx_path, {
        "to_user_id": from_id,
        "context_token": record["context_token"],
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    return True


def handle_incoming(store, msg, owner_id):
    """处理一条收到的消息，写入 inbox/。

    返回 True 表示消息已被「接住」：或是被策略过滤（群/非用户消息/
    非机主，直接忽略没任何问题），或是 inbox 记录已落盘。
    返回 False 表示记录落盘失败——调用方绝不能保存新游标，
    否则这条消息永久丢失（审查第 7 项）。
    """
    if msg.get("group_id"):
        return True  # 群消息忽略
    if msg.get("message_type") != 1:
        return True  # 只处理用户发来的消息（1=用户，2=Bot）
    from_id = msg.get("from_user_id") or ""
    if owner_id and from_id != owner_id:
        return True  # 只处理机主微信 ID 发来的消息

    text = ""
    media_paths = []
    media_failed = 0
    try:
        text = extract_text(msg)
        if not text.strip():
            # 非文本消息：图片/视频/文件下载到本地
            item_types = sorted({str(it.get("type")) for it in (msg.get("item_list") or [])})
            log(f"非文本消息 [{msg.get('message_id')}] item_types={item_types}")
            for it in (msg.get("item_list") or []):
                itype = it.get("type")
                sub = it.get("image_item") or it.get("video_item") or it.get("file_item")
                if itype in (ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE) and sub:
                    kind = {ITEM_IMAGE: "image", ITEM_VIDEO: "video"}.get(itype, "file")
                    ext = {ITEM_IMAGE: "jpg", ITEM_VIDEO: "mp4"}.get(itype, "bin")
                    fname = sub.get("file_name") if itype == ITEM_FILE else ""
                    if fname:
                        fname = "".join(c for c in fname if c not in "/\\")
                        dest_name = "%s_%s" % (msg.get("message_id"), fname)
                    else:
                        dest_name = "%s_%s.%s" % (msg.get("message_id"), kind, ext)
                    day = time.strftime("%Y%m%d")
                    dest = os.path.join(store.media_dir, day, dest_name)
                    # 审查第 7 项：item 级解析（aeskey 用多格式解析）也
                    # 纳入 try——旧实现 bytes.fromhex 在 try 外，aeskey
                    # 不是纯 hex 时整条消息在写 inbox 前就被抛掉。
                    try:
                        key = _media_key(sub)
                        download_media(sub.get("media") or {}, key, dest)
                        media_paths.append(dest)
                        log("已下载%s -> %s" % (kind, dest))
                    except Exception as e:
                        media_failed += 1
                        m = sub.get("media") or {}
                        log("%s下载失败: %s | encrypt_type=%s full_url=%s" % (
                            kind, e, m.get("encrypt_type"), bool(m.get("full_url"))))
                elif itype == ITEM_VOICE:
                    pass  # 语音转写文本已在 extract_text 中提取
            if media_paths:
                kinds = {".jpg": "[图片]", ".mp4": "[视频]"}
                text = kinds.get(os.path.splitext(media_paths[0])[1], "[文件]")
            elif media_failed:
                # P2：媒体全下失败也写占位记录，worker/用户能知道这条消息到过
                text = "[媒体接收失败：下载/解密失败]"
            else:
                text = "[不支持的消息类型]"
    except Exception as e:
        # 单 item 之外的意外（item_list 结构畸形等）：降级为占位记录，
        # 不让整条消息蒸发（审查第 7 项）
        log("处理消息内容时异常，降级占位记录: %s" % e)
        media_failed += 1
        if not text.strip():
            text = "[媒体接收失败：处理异常]"

    try:
        return _write_inbox_record(store, msg, from_id, text,
                                   media_paths, media_failed)
    except Exception as e:
        # 连占位记录都写不进：最后再试一条最小记录；仍失败返回 False，
        # run() 据此不保存游标，消息会在下轮重新投递。
        log("写入 inbox 记录失败: %s" % e)
        try:
            return _write_inbox_record(
                store, msg, from_id, "[接收失败：记录写入异常]",
                media_failed=media_failed + 1)
        except Exception as e2:
            log("最小占位记录也写入失败，本批游标不能推进: %s" % e2)
            return False


def _media_key(sub):
    """从 image/video/file item 解析 AES 密钥（字节或 None）。

    sub['aeskey'] 历史上按 hex 解；iLink 还可能给 base64 等形态，
    统一走 crypto.parse_aes_key 的多格式解析；解析不了返回 None，
    让 download_media 改用 media.aes_key 自行解析。
    """
    key_hex = sub.get("aeskey") or ""
    if not key_hex:
        return None
    try:
        return bytes.fromhex(key_hex)
    except (ValueError, TypeError):
        pass
    try:
        from . import crypto
        return crypto.parse_aes_key(key_hex)
    except Exception:
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
            if not isinstance(job, dict):
                # 宽限期：文件太新可能是 worker 正在写（非原子写一半），
                # 本轮先跳过，下轮再判，避免误杀在写任务。
                try:
                    if time.time() - os.path.getmtime(path) < 10:
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
    return job, cap_name, cap_job


def _is_mergeable_text(name, job):
    """机械合并候选：纯文本、未尝试过、无分片进度、非配文任务。"""
    if name.endswith("_caption.json") or job.get("kind") == "caption":
        return False
    if job.get("media_path"):
        return False
    if int(job.get("attempts") or 0) != 0:
        return False
    if int(job.get("chunks_done") or 0) != 0:
        return False
    return bool((job.get("text") or "").strip())


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
    try:
        atomic_write_json(sending_path, job)
    except OSError:
        pass
    try:
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
            resp = send_media_message(
                baseurl, token, to_user_id,
                job.get("context_token") or "", media_path,
                media_type_from_path(media_path),
                client_key=name,
                heartbeat=lambda: _touch_flusher_heartbeat(store))
            sent_units = 1
        else:
            start_chunk = int(job.get("chunks_done") or 0)

            def _persist_chunk_progress(done, total,
                                        _job=job, _path=sending_path):
                # 每片送达立即落盘：进程被杀也不丢已达片数（审查第 3 项）
                _job["chunks_done"] = done
                try:
                    atomic_write_json(_path, _job)
                except OSError:
                    pass
                # 顺手刷新心跳：长文本逐片发时 flusher 显然活着（P1-1）
                _touch_flusher_heartbeat(store)

            resp = send_message(
                baseurl, token, to_user_id, job.get("text") or "",
                job.get("context_token") or "",
                start_chunk=start_chunk,
                on_chunk=_persist_chunk_progress,
                client_key=name, pace=PACE_SECONDS)
            done = int(resp.get("_chunks_done") or 0) if isinstance(resp, dict) else 0
            total = int(resp.get("_chunks_total") or 0) if isinstance(resp, dict) else 0
            sent_units = max(0, done - start_chunk)
            if isinstance(resp, dict) and total:
                job["chunks_done"] = done

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
            counts[tok_key] = counts.get(tok_key, 0) + max(1, sent_units)
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


def _send_merged_group(store, baseurl, token, counts, members):
    """把一组连发文本任务合并成一条 sendmessage 发出（机械合并兜底）。

    members 已按 enqueued_at 升序；用最新成员的 context_token 发送，
    合并发送只等一次任务间 pacing，token 额度按实际送达的分片数计
    （审查第 5 项，不再按任务数低估）。

    失败语义：
    - 合并整条未送达（失败响应且已送达分片为 0，或发送抛异常）：降级为
      逐条发送（成员已认领，逐个走 _deliver_claimed，各自记 attempts/
      退避或进 sent/）——一条毒任务不拖全组，下一轮起它们也不再合并。
      合并尝试本身不单独给成员记 attempts。
    - 长文本分片部分送达（0<done<total）：已达分片绝不重发——未达部分
      收拢为最新成员文件上的续发任务（text=合并全文、chunks_done=done），
      其余成员标记 merged_into 后进 sent/。
    - token 失效（-14）：走既有探活流程，不降级。
    返回 False 仅当 token 确认失效（探活也 -14）。
    """
    newest = members[-1]
    name = newest["name"]
    names = ", ".join(m["name"] for m in members)
    token_used = newest["job"].get("context_token") or ""
    to_user_id = newest["job"].get("to_user_id") or ""
    # P3(b)：额度复检（合并发送计 1 条）
    if counts.get(token_used, 0) >= TOKEN_SEND_CAP:
        log("context_token 发送额度已满 (%d)，合并组留队等新 token"
            % TOKEN_SEND_CAP, job=name)
        return True
    # 认领整组；任一成员认领失败则已认领的放回，整组本轮跳过
    claimed = []
    for m in members:
        try:
            os.replace(m["pending_path"], m["sending_path"])
            claimed.append(m)
        except (FileNotFoundError, OSError) as e:
            log("合并组成员认领失败 (%s)，整组本轮跳过: %s" % (e, names),
                job=m["name"])
            for cm in claimed:
                try:
                    os.replace(cm["sending_path"], cm["pending_path"])
                except OSError:
                    pass
            return True

    merged_text = "\n\n".join((m["job"].get("text") or "") for m in members)
    log("合并发送 %d 条任务为一条: %s" % (len(members), names), job=name)
    for m in members:
        m["job"]["delivery_state"] = "inflight"
        try:
            atomic_write_json(m["sending_path"], m["job"])
        except OSError:
            pass
    resp = None
    send_err = None
    chunk_progress = {"done": 0, "total": 0}

    def _note_chunk(done, total):
        chunk_progress["done"] = done
        chunk_progress["total"] = total
        _touch_flusher_heartbeat(store)

    try:
        resp = send_message(baseurl, token, to_user_id, merged_text, token_used,
                            on_chunk=_note_chunk,
                            client_key="merged:" + name, pace=PACE_SECONDS)
    except Exception as e:
        send_err = e
        # 审查第 3 项：异常路径也要拿回已送达片数
        chunk_progress["done"] = int(getattr(e, "_chunks_done", 0) or 0)
        chunk_progress["total"] = int(getattr(e, "_chunks_total", 0) or 0)

    if resp is not None and is_token_invalid(resp):
        # P4：先探活，探活也 -14 才判失效（与单发一致，不降级）
        if probe_token_invalid(store, baseurl, token):
            mark_needs_relogin(store, "sendmessage -14 且探活 -14，token 确认失效")
            log("token 确认失效，已写 NEEDS_RELOGIN，合并组留队", job=name)
            for m in members:
                m["job"]["next_attempt_at"] = 0
                _return_to_pending(store, m["sending_path"],
                                   m["pending_path"], m["job"])
            return False
        log("合并发送返回 -14 但探活正常，按可重试失败处理",
            job=name, attempt=1)
        resp = {"ret": -14, "errmsg": "send -14, probe ok"}

    if resp is not None and resp_ok(resp):
        for m in members:
            _mark_sent(store, m["sending_path"], m["name"])
        # 审查第 5 项：额度按实际送达的分片数计，不再按任务数低估
        _total = int(resp.get("_chunks_total") or 0) if isinstance(resp, dict) else 0
        counts[token_used] = counts.get(token_used, 0) + max(1, _total)
        log("已发送合并回复（%d 条合一）: %s" % (len(members), names),
            job=name, attempt=1)
        if PACE_SECONDS > 0:
            time.sleep(PACE_SECONDS)
        return True

    done = int(resp.get("_chunks_done") or 0) if isinstance(resp, dict) else 0
    total = int(resp.get("_chunks_total") or 0) if isinstance(resp, dict) else 0
    if send_err is not None:
        done, total = chunk_progress["done"], chunk_progress["total"]
    if resp is not None and total > 1 and 0 < done < total:
        # 分片部分送达：收拢续发，其余成员标记后进 sent/，已达分片不重发
        reason = "发送返回异常: %s" % str(resp)[:200]
        cont = newest["job"]
        cont["text"] = merged_text
        cont["chunks_done"] = done
        cont["attempts"] = 1
        cont["last_error"] = reason + "（合并分片部分送达，续发）"
        cont["next_attempt_at"] = time.time() + BACKOFFS[0]
        cont["merged_from"] = [m["name"] for m in members]
        _return_to_pending(store, newest["sending_path"],
                           newest["pending_path"], cont)
        for m in members[:-1]:
            mj = dict(m["job"])
            mj["merged_into"] = name
            atomic_write_json(os.path.join(store.sent_dir, m["name"]), mj)
            try:
                os.unlink(m["sending_path"])
            except OSError:
                pass
        log("合并发送部分送达 %d/%d 片，余下收拢为 %s 续发"
            % (done, total, name), job=name, attempt=1)
        return True

    # 合并整条未送达：降级逐条发送（成员都已认领，attempts 从 0 起算）
    if send_err is not None:
        log("合并发送异常 (%s)，降级逐条发送: %s" % (send_err, names), job=name)
    else:
        log("合并发送未送达 (%s)，降级逐条发送: %s"
            % (str(resp)[:120], names), job=name)
    for idx, m in enumerate(members):
        ok = _deliver_claimed(store, baseurl, token, counts, m["name"],
                              m["job"], m["sending_path"], m["pending_path"],
                              m["enq"], 0)
        if not ok:
            # 中途确认 token 失效：未轮到的成员原样放回 pending
            for rest in members[idx + 1:]:
                rest["job"]["next_attempt_at"] = 0
                _return_to_pending(store, rest["sending_path"],
                                   rest["pending_path"], rest["job"])
            return False
    return True


def flush_outbox(store, baseurl, token):
    """把 outbox/pending/ 里的回复发出去（P1–P4 + 合并兜底语义）。

    先逐任务定性（死信/退避跳过/候选），再把候选里的连发纯文本任务
    按 enqueued_at 贪心分组（同收件人、组内跨度 ≤MERGE_WINDOW_SECONDS、
    最新任务入队满 MERGE_FRESH_SECONDS 才发），组 ≥2 时合并成一条发出；
    其余任务逐条发。返回 False 仅当确认 token 失效，run() 据此退出 serve；
    其余异常都逐单元吞掉记日志，绝不向上抛。
    """
    counts = _token_counts(store)
    last_ctx = load_json(store.last_ctx_path) or {}
    now = time.time()

    # —— 预处理：逐任务定性，不产生发送副作用 ——
    candidates = []
    for enq, name, job in _list_pending(store):
        pending_path = os.path.join(store.pending_dir, name)
        sending_path = os.path.join(store.sending_dir, name)
        try:
            job["enqueued_at"] = enq
            attempts = int(job.get("attempts") or 0)
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
            if attempts == 0:
                new_tok = last_ctx.get("context_token") or ""
                if (new_tok and last_ctx.get("to_user_id") == to_user_id
                        and new_tok != (job.get("context_token") or "")):
                    job["context_token"] = new_tok
                    log("任务升级到最新 context_token", job=name)
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
    for _anchor, members in units:
        newest = members[-1]
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
                    continue
                if not _send_merged_group(store, baseurl, token, counts, members):
                    return False
            else:
                if not _send_single(store, baseurl, token, counts, members[0]):
                    return False
        except Exception as e:
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
    """启动回收（审查第 1 项）：把 sending/ 里滞留的任务全量移回 pending。

    任务被认领进 sending/ 后若 serve 中途被杀（崩溃/VM 替换/人工 kill），
    旧实现里它就永久卡在 sending/——不重发、不死信、status 还不显示。
    run() 在拿到 flock（独占性有保证）之后、开始轮询之前调用本函数：
    文件级移动，保留 attempts/enqueued_at/chunks_done；next_attempt_at
    一律清零立即重试（崩溃前可能正卡在长退避里，回收后没必要再等）。
    """
    try:
        names = [f for f in os.listdir(store.sending_dir) if f.endswith(".json")]
    except OSError:
        return
    for name in names:
        src = os.path.join(store.sending_dir, name)
        dst = os.path.join(store.pending_dir, name)
        try:
            job = load_json(src)
            if isinstance(job, dict):
                job["next_attempt_at"] = 0
                job["delivery_state"] = "recovered"
                atomic_write_json(dst, job)
                os.unlink(src)
            else:
                os.replace(src, dst)
            log("启动回收：sending/ 滞留任务已移回 pending", job=name)
        except OSError as e:
            log("启动回收 sending/ 任务失败: %s" % e, job=name)


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
    if not _acquire_lock(store):
        log("serve 已有实例在运行（锁被占用），本实例退出")
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
                log("getupdates 出错，%d 秒后重试 (连续失败 %d 次): %s"
                    % (delay, consec_failures, e))
                time.sleep(delay)
                continue
            # 审查第 6 项：先判形状再取字段（旧实现先 .get 后判 dict，
            # 服务端返回 JSON 数组能直接打崩 serve）；错误响应与连接
            # 异常同等对待——计数、退避、不存游标、不 flush。
            if _poll_response_bad(resp) and not is_token_invalid(resp):
                consec_failures += 1
                delay = 30 if consec_failures >= 3 else 2
                log("getupdates 返回异常响应，%d 秒后重试 (连续失败 %d 次): %s"
                    % (delay, consec_failures, str(resp)[:200]))
                time.sleep(delay)
                continue
            consec_failures = 0
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
            for msg in resp.get("msgs") or []:
                try:
                    if handle_incoming(store, msg, owner_id) is False:
                        batch_ok = False
                        log("消息记录落盘失败，本批游标不推进: [%s]"
                            % msg.get("message_id"))
                except Exception as e:
                    batch_ok = False
                    log("处理消息出错，本批游标不推进: %s" % e)
            new_buf = resp.get("get_updates_buf")
            if new_buf and batch_ok:
                buf = new_buf
                atomic_write_text(store.buf_path, buf)
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
