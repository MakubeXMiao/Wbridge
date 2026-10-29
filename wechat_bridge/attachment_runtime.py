"""Trusted attachment integration. Model requests never supply a principal."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
import uuid
import stat

from .store import atomic_write_json, load_json, safe_message_id
from .progress import OperationAborted

CAPABILITIES = {"schema_version": 1, "protocol": "attachment_inbox_v1", "version": 1,
    "features": ["attachments_v1", "references_v1", "attachment_groups_v1", "attachment_labels_v1", "attachment_cancel_v1"],
    "limits": {"request_bytes": 65536, "response_bytes": 65536, "query_limit": 20,
               "attachment_ids": 16, "reference_ordinals": 16}}
PUBLIC_METADATA = ("attachment_id", "message_id", "item_index", "received_at", "kind",
    "original_name", "name_source", "declared_type", "detected_type", "size", "sha256",
    "status", "decryption_state", "preview_state", "tags", "expires_at", "pinned")
STATUS_FIELDS = ("schema_version", "group_id", "created_at", "state", "attachment_count",
    "accepted_attachment_ids", "pending_attachment_ids", "unknown_attachment_ids", "units",
    "can_continue", "requires_explicit_continue")
ERRORS = set(("unsupported_version unsupported_action invalid_request response_too_large "
    "lease_lost needs_refresh epoch_mismatch not_found scope_mismatch attachment_unavailable "
    "snapshot_expired processing quota_exceeded storage_busy storage_invalid hold_forbidden "
    "group_unknown group_busy continue_required budget_exhausted cancelled expired").split())


class AttachmentCallError(ValueError):
    def __init__(self, code):
        self.code = code if code in ERRORS else "storage_invalid"
        super().__init__(self.code)


def is_catalog_original_path(store, path):
    """Recognize published and staged originals without opening a catalog.

    Both the supplied path and its resolved target matter: an ordinary symlink
    to an original is still an original. This is not inode-alias/copy detection.
    """
    if not path:
        return False
    candidate = Path(os.path.expanduser(os.fspath(path)))
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    roots = tuple(Path(store.base) / "attachments" / name for name in ("objects", "staging"))
    try:
        # Resolve the original components, including symlinks BEFORE '..'.
        # abspath/normpath first would change the path the OS actually opens.
        targets = (candidate, candidate.resolve())
        namespaces = roots + tuple(root.resolve() for root in roots)
    except RuntimeError:
        raise ValueError("media path cannot be resolved") from None
    for value in targets:
        for namespace in namespaces:
            try:
                value.relative_to(namespace)
                return True
            except ValueError:
                pass
    # Case-insensitive filesystems may preserve the caller's directory spelling
    # through resolve(). Compare directory identity, not the file inode (which
    # would extend this guard to hardlink/copy aliases outside this namespace).
    identities = set()
    for namespace in roots:
        try:
            info = namespace.stat()
        except FileNotFoundError:
            continue
        identities.add((info.st_dev, info.st_ino))
    if identities:
        directories = dict.fromkeys(directory for value in targets for directory in value.parents)
        for directory in directories:
            try:
                info = directory.stat()
            except FileNotFoundError:
                continue
            if (info.st_dev, info.st_ino) in identities:
                return True
    return False


def effective_policy(base):
    from .attachments import AttachmentStore
    path = Path(base) / "state" / "attachment-policy.json"
    policy = None
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    except FileNotFoundError:
        pass
    else:
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
                raise AttachmentCallError("storage_invalid")
            raw = os.read(fd, 65537)
            policy = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise AttachmentCallError("storage_invalid") from None
        finally:
            os.close(fd)
    return AttachmentStore(base, policy=policy).policy


def capabilities(base):
    value = json.loads(json.dumps(CAPABILITIES))
    value["policy"] = {"ambiguity_seconds": effective_policy(base)["ambiguity_seconds"]}
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _session(store):
    value = load_json(store.session_path)
    return value if isinstance(value, dict) else {}


def ensure_identity(store):
    """Only a controlled startup/login may call this mutating bootstrap."""
    import fcntl
    path = store.session_path + ".attachment.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        session = _session(store)
        if not session.get("bot_token") or not session.get("ilink_user_id"):
            raise AttachmentCallError("epoch_mismatch")
        changed = False
        for field in ("login_epoch", "attachment_runtime_id"):
            if field not in session:
                session[field], changed = uuid.uuid4().hex, True
            elif not isinstance(session[field], str) or not re.fullmatch(r"[0-9a-f]{32}", session[field]):
                raise AttachmentCallError("storage_invalid")
        if changed:
            atomic_write_json(store.session_path, session)
        return session
    finally:
        os.close(fd)


def current_scope(store, sender=None):
    session = _session(store)
    owner, epoch = session.get("ilink_user_id"), session.get("login_epoch")
    if (not isinstance(owner, str) or not owner or not isinstance(epoch, str)
            or not re.fullmatch(r"[0-9a-f]{32}", epoch) or not session.get("bot_token")
            or os.path.exists(store.needs_relogin_path)):
        raise AttachmentCallError("epoch_mismatch")
    if sender is not None and sender != owner:
        raise AttachmentCallError("scope_mismatch")
    bot = session.get("ilink_bot_id") or session.get("attachment_runtime_id")
    if not isinstance(bot, str) or not bot:
        raise AttachmentCallError("epoch_mismatch")
    return {"bot_id": bot, "owner_id": owner, "conversation_id": owner, "login_epoch": epoch}


def active(store):
    if getattr(store, "_attachments_disabled", False):
        return False
    if getattr(store, "_attachment_runtime_blocked", False):
        return True  # Never fall back to unaccounted legacy sends after cutover failed.
    path = Path(store.state_dir) / "attachments_enabled.json"
    if path.is_symlink():
        raise AttachmentCallError("storage_invalid")
    if not path.exists():
        return False
    value = load_json(str(path))
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise AttachmentCallError("storage_invalid")
    return True


def ledger(store):
    from .attachment_delivery import DeliveryLedger
    marker = Path(store.state_dir) / "attachments_enabled.json"
    database = Path(store.state_dir) / "attachment-delivery.sqlite3"
    if os.path.lexists(str(marker)) and not database.is_file():
        raise AttachmentCallError("storage_invalid")
    if not hasattr(store, "_attachment_ledger"):
        store._attachment_ledger = DeliveryLedger(store.base)
    return store._attachment_ledger


def _existing_admission_ledger(store):
    from .attachment_delivery import DeliveryLedger
    if not hasattr(store, "_attachment_ledger"):
        store._attachment_ledger = DeliveryLedger.open_existing(store.base)
    state = store._attachment_ledger
    try:
        info = state.path.lstat()
    except FileNotFoundError:
        raise AttachmentCallError("storage_invalid") from None
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise AttachmentCallError("storage_invalid")
    return state


def catalog(store, check=None):
    from .attachments import AttachmentStore
    def guard(point):
        if point == "transaction_before_commit" and check is not None:
            check()
    return AttachmentStore(store.base, policy=effective_policy(store.base), fault=guard if check else None)


def initialize(store):
    """One controlled cutover. Unknown old in-memory budgets remain full."""
    ensure_identity(store)
    scope = current_scope(store)
    was_active = active(store)
    state = ledger(store)
    if not was_active:
        # Stream bounded old metadata; never retain all historic reply bodies
        # in memory merely to inspect their context tokens.
        scanned_bytes, scanned_files = 0, 0
        def legacy_row(path):
            nonlocal scanned_bytes, scanned_files
            try:
                fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
            except FileNotFoundError:
                return {}
            try:
                info = os.fstat(fd)
                scanned_files += 1
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > (1 << 20)
                        or scanned_files > 10000):
                    raise AttachmentCallError("storage_invalid")
                raw = os.read(fd, (1 << 20) + 1)
                scanned_bytes += len(raw)
                if len(raw) > (1 << 20) or scanned_bytes > (64 << 20):
                    raise AttachmentCallError("storage_invalid")
                row = json.loads(raw.decode("utf-8"))
                if not isinstance(row, dict):
                    raise AttachmentCallError("storage_invalid")
                return row
            finally:
                os.close(fd)
        def migrate_context(row):
            if isinstance(row.get("context_token"), str):
                state.register_context(scope, row["context_token"], fresh=False)
        migrate_context(legacy_row(store.last_ctx_path))
        for directory in (store.inbox_dir, store.pending_dir, store.sending_dir, store.failed_dir,
                          store.sent_dir, str(Path(store.state_dir) / "reply_batches")):
            for path in Path(directory).glob("*.json"):
                row = legacy_row(path)
                migrate_context(row)
                if isinstance(row.get("job"), dict):
                    migrate_context(row["job"])
        # Upgrade only the proven same-owner inbox at the initial cutover.
        # Subsequent login rotations must never relabel old worker authority.
        from .store import message_receipt_lock
        with message_receipt_lock(store):
            for path in Path(store.inbox_dir).glob("*.json"):
                row = load_json(str(path), {})
                if (isinstance(row, dict) and row.get("from_user_id") == scope["owner_id"]
                        and "login_epoch" not in row):
                    row["login_epoch"] = scope["login_epoch"]
                    atomic_write_json(str(path), row)
        atomic_write_json(str(Path(store.state_dir) / "attachments_enabled.json"),
            {"schema_version": 1, "enabled_at": time.time()})
    # Activate only a newly created login epoch using the trusted receive path.
    # An empty token cannot grant a send budget.
    state.register_context(scope, "", fresh=True)
    return scope


def receive_event(store, msg, sender, items, text, received_at):
    if not active(store):
        return None
    if getattr(store, "_attachment_runtime_blocked", False):
        raise AttachmentCallError("storage_invalid")
    scope = current_scope(store, sender)
    ledger(store).register_context(scope, msg.get("context_token") or "", fresh=True)
    event = catalog(store).register_event(scope, str(msg["message_id"]), items,
                                          text=text, received_at=received_at)
    return scope, event


def public_metadata(value):
    return {key: value.get(key) for key in PUBLIC_METADATA}


def public_status(value):
    return {key: value[key] for key in STATUS_FIELDS}


def _identifier(value):
    if not isinstance(value, str) or not value or len(value) > 512 or "\x00" in value:
        raise AttachmentCallError("invalid_request")
    return value


def _keys(args, required=(), optional=()):
    if type(args) is not dict or not set(required) <= set(args) or set(args) - set(required) - set(optional):
        raise AttachmentCallError("invalid_request")


def _limit(args):
    limit = args.get("limit", 20)
    if type(limit) is not int or not 1 <= limit <= 20:
        raise AttachmentCallError("invalid_request")
    return limit


def _holder(scope, lease):
    return "worker:" + _digest([scope, lease])


def _batch_scope(worker, ids, lease, claims, records):
    if not isinstance(lease, str) or not lease or not ids:
        raise AttachmentCallError("lease_lost")
    normalized = [safe_message_id(mid) for mid in ids]
    if len(normalized) != len(set(normalized)):
        raise AttachmentCallError("invalid_request")
    for mid in normalized:
        row = claims.get(mid, {})
        if row.get("lease") != lease or row.get("kind") != "worker" or not worker._alive(mid, row):
            raise AttachmentCallError("lease_lost")
    senders = {records.get(mid, {}).get("from_user_id") for mid in normalized}
    if len(senders) != 1 or None in senders:
        raise AttachmentCallError("scope_mismatch")
    scope = current_scope(worker.store, next(iter(senders)))
    if any(records[mid].get("login_epoch") != scope["login_epoch"] for mid in normalized):
        raise AttachmentCallError("epoch_mismatch")
    template = dict(claims[normalized[0]])
    worker._collect(claims, lease, scope["owner_id"], template, records)
    worker._write_claims(claims)
    batch = worker._batch(claims, lease, records)
    if set(batch["owned"]) != set(normalized):
        raise AttachmentCallError("needs_refresh")
    return scope, batch


def _reference_identity(value):
    result = {key: value.get(key) for key in ("schema_version", "snapshot_id", "text_message_id",
        "cutoff_seq", "group_id", "login_epoch", "created_at", "expires_at")}
    result["members"] = [{k: slot.get(k) for k in ("ordinal", "message_id", "item_index", "attachment_id")}
                         for slot in value.get("slots", [])]
    return result


def _explicit_continue(batch):
    # Deliberately narrow: ambiguous/narrated/negative text must be clarified.
    text = "\n".join(str(row.get("text") or "") for row in batch["messages"]).strip()
    if re.search(r"不要|别再|别发|停止|取消|不继续|don.t|stop|cancel", text, re.I):
        return False
    return bool(re.fullmatch(r"(?:请|麻烦)?\s*(?:继续(?:发送|发|传)?|接着(?:发送|发|传)|"
        r"(?:把)?剩下的?(?:附件|图片|文件)?(?:发|传)(?:给我)?|continue(?:\s+sending)?)"
        r"(?:给?我)?(?:\s*(?:这组|这一组|上组|上一组|附件|图片|文件|第?[一二三四五六七八九十\d]+组|ag_[0-9a-f]{8,32}|[0-9a-f]{32}))*[。！!\s]*", text, re.I))


def _explicit_cancel(text):
    if type(text) is not str:
        return False
    return bool(re.fullmatch(r"(?:请|麻烦)?\s*(?:取消(?:发送)?|停止(?:发送)?|"
        r"别(?:再)?(?:发|传)(?:送)?|不要(?:再)?(?:发|传)(?:送)?)"
        r"(?:\s*(?:这组|这一组|上组|上一组|附件|图片|文件|第?[一二三四五六七八九十\d]+组|ag_[0-9a-f]{8,32}|[0-9a-f]{32}))*[。！!\s]*", text.strip()))


def _require_group_choice(state, scope, group_id, text):
    selectors = re.findall(r"(?<![A-Za-z0-9_])ag_[0-9a-f]{8,32}(?![A-Za-z0-9_])", text)
    if len(selectors) > 1:
        raise AttachmentCallError("continue_required")
    selected = state.resolve_group_selector(scope, selectors[0] if selectors else None)
    if selected != group_id:
        raise AttachmentCallError("continue_required")


def _explicit_label(text, label):
    if type(text) is not str or type(label) is not str or not label.strip() or len(label) > 128:
        return False
    # A removal/cancellation anywhere in the request cannot authorize append,
    # including a command in a later clause.  A quoted value immediately after
    # an explicit naming command is literal data (e.g. 标记为「取消订单」).
    quoted_label = "(?:" + "|".join(re.escape(left + label + right)
        for left, right in (("\"", "\""), ("'", "'"), ("“", "”"), ("「", "」"), ("『", "』"))) + ")"
    naming = re.compile(r"((?:(?:标记|标注|命名)为|存为|归为|叫作|叫做)\s*)" + quoted_label)
    parts, positions, last, length = [], [], 0, 0
    for match in naming.finditer(text):
        leading = text[last:match.start()] + match.group(1)
        parts.extend((leading, "\x00"))
        length += len(leading)
        positions.append(length)
        length += 1
        last = match.end()
    parts.append(text[last:])
    action_text = "".join(parts)
    if re.search(r"删除|删掉|删去|移除|移去|去掉|去除|清除|清空|取消|撤销|撤掉|摘掉|抹去|解除|"
            r"\b(?:delete|remove|clear|unset|unlabel|cancel)\b", action_text, re.I):
        return False
    # Track actual naming spans, not a user-spellable placeholder. Delimiters
    # inside a quoted value disappear only from this intent view, so a later
    # negation cannot be lost by zipping differently split raw/masked clauses.
    start, selected = 0, 0
    ends = [match.start() for match in re.finditer(r"[，,。；;!！\n]", action_text)] + [len(action_text)]
    for end in ends:
        clause = action_text[start:end].strip()
        named_here = selected < len(positions) and positions[selected] < end
        while selected < len(positions) and positions[selected] < end:
            selected += 1
        start = end + 1
        if (label in clause or named_here) and re.search(r"[?？]|还是|是否|是不是|可能|也许|好像|"
                r"不是|并非|不要|别|不准|不得|don't|not\b", clause, re.I):
            return False
        # A later "don't label" overrides an earlier positive identification,
        # without treating a description of some other label as a veto.
        if (re.search(r"不要|别|不准|不得|don't|do not", clause, re.I)
                and (re.search(r"标记|标注|标签|命名|存为|归为|算作|添加|加上|打上|贴上", clause)
                     or re.fullmatch(r"不要|别|不准|不得", clause))):
            return False
    if positions:
        return True  # A real explicit naming match, after all outside vetoes.
    for clause in (value.strip() for value in re.split(r"[，,。；;!！\n]", text)):
        if label not in clause:
            continue
        literal = re.escape(label)
        if (clause == label or re.search(r"是\s*[\"'“「『]?" + literal, clause)
                or re.search(r"(?:标记|标注|叫|存为|命名|归为|算作).*" + literal, clause)
                or re.search(r"标签\s*[:：]?\s*[\"'“「『]?" + literal, clause)
                or re.search(r"(?:添加|加上|打上|贴上)\s*(?:一个)?\s*[\"'“「『]?" + literal
                    + r"[\"'”」』]?\s*(?:这个|的)?标签", clause)):
            return True
    return False


def _validate_admission_intent(record):
    phase, admitted, authorized = record.get("phase"), record.get("admitted"), record.get("authorized_at")
    if (phase not in ("prepared", "publishing", "admitted", "blocked") or type(admitted) is not bool
            or (phase == "admitted" and admitted is not True)
            or (phase in ("prepared", "publishing") and admitted is not False)
            or type(authorized) not in (int, float) or not math.isfinite(authorized) or authorized < 0):
        raise AttachmentCallError("storage_invalid")


def _fresh_admission_preflight(store, scope, payload):
    # These are known-input checks, not a promise that the later filesystem or
    # SQL publication cannot change/fail. No hash, hold, context or DB creation.
    state, files = _existing_admission_ledger(store), catalog(store)
    metadata = state.preflight_admission(scope, payload["request_id"], payload["context_token"],
        group_id=payload["requested_group_id"], check_materials=lambda: files.preflight_admission(
            scope, payload["request_id"], payload["attachment_ids"]),
        journal_directory_exists=_admission_directory(store).is_dir(),
        check_remaining_materials=lambda ids: files.preflight_admission(scope, payload["request_id"], ids))
    if payload["attachment_ids"] is not None:
        from .attachment_caution import append_download_caution
        append_download_caution(payload["caption"], metadata)


def _resume_admitted_job(store, scope, record, path, check_authority):
    state = _existing_admission_ledger(store)
    ids, caption = record.get("attachment_ids"), None
    if ids is not None:
        from .attachment_caution import append_download_caution
        caption = append_download_caution(record["caption"], catalog(store).admission_metadata(scope, ids))
    job = state.admission_job(scope, record["request_id"], expected_ids=ids, expected_caption=caption)
    if job is None:
        if record.get("admitted") or record.get("job_id") is not None:
            raise AttachmentCallError("storage_invalid")
        return None
    if (record.get("group_id") != job["group_id"]
            or record.get("job_id", job["job_id"]) != job["job_id"]
            or (ids is None and record.get("requested_group_id") != job["group_id"])
            or state.context_reference(scope, record["context_token"]) != job["context_ref"]):
        raise AttachmentCallError("storage_invalid")
    # Keep the original normal expiry maintenance; never refresh authorized_at
    # or require a completed original to be acquired/hashed again just to replay.
    job = state.job_status(scope, job["job_id"])
    check_authority()
    if record.get("phase") != "admitted" or record.get("admitted") is not True:
        record.update(job_id=job["job_id"], admitted=True, phase="admitted")
        atomic_write_json(path, record)
    return {"queued": True, "delivery_confirmed": False, "group_id": job["group_id"],
            "job_id": job["job_id"], "status": public_status(
                state.group_status(scope, job["group_id"], maintenance=False))}


def _admit(worker, scope, batch, claims, request_id, group_id=None, ids=None, caption=""):
    from .attachments import AttachmentError
    def check_authority():
        if any(not worker._alive(mid, claims[mid]) for mid in batch["owned"]):
            raise AttachmentError("lease_lost")
        try:
            latest_scope = current_scope(worker.store)
        except AttachmentCallError:
            raise AttachmentError("epoch_mismatch") from None
        if latest_scope != scope:
            raise AttachmentError("epoch_mismatch")
    check_authority()
    store = worker.store
    directory = _admission_directory(store)
    path = directory / (request_id + ".json")
    record = _read_admission_for_release(path)
    payload = {"schema_version": 1, "type": "attachment_group_v1", "request_id": request_id,
        "scope": scope, "covers_message_ids": batch["owned"], "context_token": batch["context"],
        "attachment_ids": ids, "caption": caption, "requested_group_id": group_id}
    fresh = record is None
    if fresh:
        _fresh_admission_preflight(store, scope, payload)
        check_authority()
        record = dict(payload, phase="prepared", admitted=False, authorized_at=worker.clock())
    else:
        _validate_admission_intent(record)
        if any(record.get(key) != value for key, value in payload.items()):
            raise AttachmentCallError("storage_invalid")
        if record["phase"] == "blocked":
            code = record.get("blocked_reason")
            raise AttachmentCallError(code if code in ("lease_lost", "epoch_mismatch") else "storage_invalid")
    try:
        if fresh:
            # From the first durable-write attempt onward the outcome may be
            # persistent even when an exception is returned. Never erase it.
            directory.mkdir(mode=0o700, exist_ok=True)
            _admission_directory(store, required=True)
            atomic_write_json(str(path), record)
        result = None if fresh else _resume_admitted_job(store, scope, record, str(path), check_authority)
        if result is None:
            result = publish_admission(store, record, str(path), check_authority)
        worker._complete(batch["owned"], claims)
    except Exception as error:
        if getattr(error, "code", None) in ("lease_lost", "epoch_mismatch"):
            if record.get("admitted") is not True:
                record.update(phase="blocked", blocked_reason=error.code)
                atomic_write_json(str(path), record)
            raise
        # The same not_found/busy/etc code is no longer evidence of zero
        # effects after intent. Existing Muse publishing maps this fixed code
        # to outcome_unknown/must_stop; no new wire field or enum is needed.
        raise AttachmentCallError("storage_invalid") from None
    return result


def publish_admission(store, record, path, check_authority=None):
    if check_authority is not None:
        check_authority()
    scope = current_scope(store, record["scope"]["owner_id"])
    if scope != record["scope"]:
        raise AttachmentCallError("epoch_mismatch")
    state, files = ledger(store), catalog(store, check_authority)
    holds = []
    metadata = []
    if record.get("attachment_ids") is not None:
        for aid in record["attachment_ids"]:
            if check_authority is not None:
                check_authority()
            material = files.acquire(scope, aid, "group:request:" + record["request_id"])
            holds.append(material["hold_id"])
            metadata.append(material["metadata"])
        from .attachment_caution import append_download_caution
        caption_with_caution = append_download_caution(record["caption"], metadata)
        caution = caption_with_caution[len(record["caption"]):]
        group = state.create_group(scope, record["request_id"], record["attachment_ids"],
                                   record["caption"], trusted_caution=caution,
                                   check_authority=check_authority)
        group_id = group["group_id"]
    else:
        group_id = record["requested_group_id"]
    if check_authority is not None:
        check_authority()
    record.update(group_id=group_id, hold_ids=holds, phase="publishing")
    atomic_write_json(path, record)
    job = state.prepare_page(scope, group_id, record["request_id"], record["context_token"],
        explicit_continue=record.get("attachment_ids") is None, authorized_at=record["authorized_at"],
        check_authority=check_authority)
    record.update(job_id=job["job_id"], admitted=True, phase="admitted")
    atomic_write_json(path, record)
    return {"queued": True, "delivery_confirmed": False, "group_id": group_id,
            "job_id": job["job_id"], "status": public_status(state.group_status(scope, group_id, maintenance=False))}


def recover_admissions(store):
    directory = _admission_directory(store)
    recovered = 0
    for index, path in enumerate(sorted(directory.glob("*.json"))):
        if index >= 10000 or path.is_symlink():
            raise AttachmentCallError("storage_invalid")
        record = _read_admission_for_release(path)
        if record is None:
            continue
        _validate_admission_intent(record)
        if record["admitted"] is not True and record["phase"] != "blocked":
            try:
                scope = current_scope(store, record["scope"]["owner_id"])
                if scope != record["scope"]:
                    raise AttachmentCallError("epoch_mismatch")
                def current_epoch():
                    if current_scope(store) != scope:
                        raise AttachmentCallError("epoch_mismatch")
                result = _resume_admitted_job(store, scope, record, str(path), current_epoch)
                if result is None:
                    publish_admission(store, record, str(path))
                recovered += 1
            except Exception as error:
                if getattr(error, "code", None) not in ("expired", "epoch_mismatch", "budget_exhausted"):
                    raise
                # Preserve the fixed record for explicit reconciliation.
    return recovered


def covered_attachment(store, mid):
    directory = Path(store.state_dir) / "attachment_admissions"
    for index, path in enumerate(directory.glob("*.json")):
        if index >= 10000 or path.is_symlink():
            raise AttachmentCallError("storage_invalid")
        record = load_json(str(path), {})
        if not isinstance(record, dict):
            raise AttachmentCallError("storage_invalid")
        if mid in record.get("covers_message_ids", []):
            return {"status": "queued" if record.get("admitted") else "uncertain", "job": path.name}
    return None


def worker_call(worker, ids, lease, request):
    action = request.get("action") if type(request) is dict else None
    try:
        if type(request) is not dict or type(request.get("schema_version")) is not int or request["schema_version"] != 1:
            raise AttachmentCallError("unsupported_version")
        if set(request) != {"schema_version", "action", "arguments"}:
            raise AttachmentCallError("invalid_request")
        if len(json.dumps(request, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 65536:
            raise AttachmentCallError("invalid_request")
        if action not in ("query", "get", "reference", "release", "pin", "label", "reply", "continue", "cancel", "status"):
            raise AttachmentCallError("unsupported_action")
        args = request["arguments"]
        with worker.locked():
            claims, records = worker._claims(), worker._records()
            scope, batch = _batch_scope(worker, ids, lease, claims, records)
            def mutation_authority():
                from .attachments import AttachmentError
                if any(not worker._alive(mid, claims[mid]) for mid in batch["owned"]):
                    raise AttachmentError("lease_lost")
                try:
                    latest_scope = current_scope(worker.store)
                except AttachmentCallError:
                    raise AttachmentError("epoch_mismatch") from None
                if latest_scope != scope:
                    raise AttachmentError("epoch_mismatch")
            files, holder = catalog(worker.store, mutation_authority), _holder(scope, lease)
            if action == "query":
                _keys(args, optional=("filters", "cursor", "limit"))
                filters = args.get("filters", {})
                _keys(filters, optional=("received_after", "received_before", "name_contains", "tag", "kind", "status"))
                if ("status" in filters and filters["status"] not in ("pending", "ready", "failed", "quota", "missing")
                        or "kind" in filters and filters["kind"] not in ("image", "video", "voice", "file")):
                    raise AttachmentCallError("invalid_request")
                mapped = {("name" if k == "name_contains" else k): v for k, v in filters.items()}
                limit = _limit(args)
                while True:
                    raw = files.query(scope, mapped, limit, args.get("cursor"))
                    result = {"items": [public_metadata(v) for v in raw["items"]],
                              "next_cursor": raw["next_cursor"], "complete": raw["complete"]}
                    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) < 64000 or limit == 1:
                        break
                    limit = max(1, limit // 2)
            elif action == "get":
                _keys(args, ("attachment_id",))
                aid = _identifier(args["attachment_id"])
                material = files.acquire(scope, aid, holder)
                result = {"attachment": public_metadata(material["metadata"]), "material":
                    {"attachment_id": aid, **{k: material[k] for k in ("path", "hold_id", "expires_at")}}}
            elif action == "reference":
                _keys(args, ("text_message_id", "snapshot_id"), ("ordinals",))
                mid, sid = _identifier(args["text_message_id"]), _identifier(args["snapshot_id"])
                bound = records.get(mid, {}).get("reference_context")
                if mid not in batch["owned"] or type(bound) is not dict or bound.get("snapshot_id") != sid:
                    raise AttachmentCallError("scope_mismatch")
                observed = files.resolve_reference(scope, sid, args.get("ordinals"))
                if _reference_identity(bound) != _reference_identity(observed["reference_context"]):
                    raise AttachmentCallError("scope_mismatch")
                raw = files.resolve_reference(scope, sid, args.get("ordinals"), holder=holder)
                result = {k: raw[k] for k in ("reference_context", "materials", "unavailable", "complete")}
            elif action == "release":
                _keys(args, ("hold_id",))
                files.release(scope, _identifier(args["hold_id"]), holder=holder)
                result = {"released": True}
            elif action == "pin":
                _keys(args, ("attachment_id", "pinned"))
                result = {"attachment": public_metadata(files.pin(scope, args["attachment_id"], args["pinned"]))}
            elif action == "label":
                _keys(args, ("text_message_id", "label"), ("snapshot_id", "ordinals", "attachment_ids"))
                mid = _identifier(args["text_message_id"])
                if mid not in batch["owned"] or not _explicit_label(records[mid].get("text"), args["label"]):
                    raise AttachmentCallError("invalid_request")
                if "attachment_ids" in args:
                    if "snapshot_id" in args or "ordinals" in args:
                        raise AttachmentCallError("invalid_request")
                    aids = args["attachment_ids"]
                    if (type(aids) is not list or not 1 <= len(aids) <= 16 or
                            any(type(v) is not str for v in aids) or len(set(aids)) != len(aids)):
                        raise AttachmentCallError("invalid_request")
                else:
                    if "snapshot_id" not in args or "ordinals" not in args:
                        raise AttachmentCallError("invalid_request")
                    bound = records[mid].get("reference_context")
                    if not isinstance(bound, dict) or bound.get("snapshot_id") != args["snapshot_id"]:
                        raise AttachmentCallError("scope_mismatch")
                    observed = files.resolve_reference(scope, args["snapshot_id"], args["ordinals"])
                    if _reference_identity(bound) != _reference_identity(observed["reference_context"]):
                        raise AttachmentCallError("scope_mismatch")
                    aids = [slot["attachment_id"] for ordinal in args["ordinals"]
                        for slot in observed["reference_context"]["slots"] if slot["ordinal"] == ordinal]
                if any(not worker._alive(value, claims[value]) for value in batch["owned"]):
                    raise AttachmentCallError("lease_lost")
                result = {"attachments": [public_metadata(value) for value in
                    files.label(scope, aids, args["label"], mid, response_bytes=64000)]}
            elif action == "reply":
                _keys(args, ("attachment_ids",), ("caption",))
                aids, caption = args["attachment_ids"], args.get("caption", "")
                if (type(aids) is not list or not 1 <= len(aids) <= 16 or
                        any(not isinstance(v, str) for v in aids) or len(set(aids)) != len(aids)
                        or type(caption) is not str or len(caption) > 16000):
                    raise AttachmentCallError("invalid_request")
                if _explicit_continue(batch):
                    raise AttachmentCallError("continue_required")
                if any(not worker._alive(mid, claims[mid]) for mid in batch["owned"]):
                    raise AttachmentCallError("lease_lost")
                request_id = "reply-" + _digest(batch["owned"])
                result = _admit(worker, scope, batch, claims, request_id, ids=aids, caption=caption)
            elif action == "continue":
                _keys(args, ("group_id",))
                if not _explicit_continue(batch):
                    raise AttachmentCallError("continue_required")
                group_id = _identifier(args["group_id"])
                _require_group_choice(ledger(worker.store), scope, group_id,
                    "\n".join(row.get("text") or "" for row in batch["messages"]))
                request_id = "continue-" + _digest(batch["owned"])
                result = _admit(worker, scope, batch, claims, request_id, group_id=group_id)
            elif action == "cancel":
                _keys(args, ("text_message_id", "group_id"))
                mid = _identifier(args["text_message_id"])
                if mid not in batch["owned"] or not _explicit_cancel(records[mid].get("text")):
                    raise AttachmentCallError("invalid_request")
                state, group_id = ledger(worker.store), _identifier(args["group_id"])
                previous = state.group_status(scope, group_id, maintenance=False)
                if previous["state"] == "cancelled":
                    result = {"group": public_status(previous)}
                else:
                    _require_group_choice(state, scope, group_id, records[mid]["text"])
                    result = {"group": public_status(state.cancel_group(scope, group_id,
                                                                         check_authority=mutation_authority))}
            else:
                _keys(args, optional=("group_id", "cursor", "limit"))
                state = ledger(worker.store)
                if args.get("group_id"):
                    if "cursor" in args or "limit" in args:
                        raise AttachmentCallError("invalid_request")
                    result = {"group": public_status(state.group_status(scope, args["group_id"]))}
                else:
                    raw = state.list_groups(scope, _limit(args), args.get("cursor"))
                    result = {"groups": [public_status(v) for v in raw["groups"]], "next_cursor": raw["next_cursor"]}
            if action not in ("reply", "continue") and any(not worker._alive(mid, claims[mid]) for mid in batch["owned"]):
                raise AttachmentCallError("lease_lost")
        envelope = {"schema_version": 1, "action": action, "ok": True, "result": result, "error": None}
        if len(json.dumps(envelope, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 65536:
            raise AttachmentCallError("response_too_large")
        return envelope
    except Exception as error:
        code = getattr(error, "code", "storage_invalid")
        code = code if code in ERRORS else "storage_invalid"
        return {"schema_version": 1, "action": action if isinstance(action, str) and len(action) <= 32 else None,
                "ok": False, "result": None, "error": {"code": code,
                "retryable": code in ("storage_busy", "processing"), "refresh_required": code == "needs_refresh"}}


class BudgetUnavailable(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class SendBudget:
    """One legacy/CLI operation, accounted by actual physical sends."""
    def __init__(self, store, recipient, token, context_token, client_key):
        self.store, self.scope = store, current_scope(store, recipient)
        self.token, self.context, self.key = token, context_token, client_key
        self.state, self.current, self.inflight = ledger(store), None, False

    def check(self):
        try:
            current = current_scope(self.store)
        except AttachmentCallError:
            raise OperationAborted("epoch_mismatch") from None
        if (current != self.scope or
                _session(self.store).get("bot_token") != self.token):
            raise OperationAborted("epoch_mismatch")

    def before(self, index, total):
        self.check()
        unit_id = "legacy-" + _digest([self.key, index])
        try:
            self.state.reserve_units(self.scope, self.context, [unit_id])
            item = self.state.begin_reserved(self.scope, self.context, unit_id)
        except Exception as error:
            if getattr(error, "code", None) in ("budget_exhausted", "group_unknown"):
                raise BudgetUnavailable(error.code) from None
            raise
        self.current, self.inflight = unit_id, item["send_allowed"]
        return item["send_allowed"]

    def prepare(self, index=0):
        self.check()
        self.current = "legacy-" + _digest([self.key, index])
        try:
            reservation = self.state.reserve_units(self.scope, self.context, [self.current])
        except Exception as error:
            if getattr(error, "code", None) in ("budget_exhausted", "group_unknown"):
                self.current = None
                raise BudgetUnavailable(error.code) from None
            raise
        return reservation["units"][0]["state"]

    def result(self, index, total, response):
        from .ilink import resp_ok
        if self.current is not None:
            self.state.settle_reserved(self.scope, self.context, self.current,
                                       "accepted" if resp_ok(response) else "unknown")
        self.inflight = False

    def abort(self):
        if self.current is not None:
            try:
                self.state.settle_reserved(self.scope, self.context, self.current,
                                          "unknown" if self.inflight else "proven_unsent")
            finally:
                self.inflight = False


def send_budget(store, recipient, token, context_token, client_key):
    if getattr(store, "_attachment_runtime_blocked", False):
        raise AttachmentCallError("storage_invalid")
    if not active(store) and not getattr(store, "_attachments_disabled", False):
        session = _session(store)
        if session.get("bot_token") and session.get("ilink_user_id"):
            initialize(store)
    return SendBudget(store, recipient, token, context_token, client_key) if active(store) else None


def stage_progress(store, check=None):
    """A callback for a single real operation; no timer or worker refresh."""
    last = [None, -1]
    def progress(event):
        if check is not None:
            check()
        stage, completed = event["stage"], event["completed"]
        if stage != last[0] or completed > last[1]:
            last[:] = [stage, completed]
            atomic_write_json(str(Path(store.state_dir) / "flusher_progress.json"),
                dict(event, at=time.time()))
            atomic_write_json(str(Path(store.state_dir) / "attachment_progress.json"),
                dict(event, at=time.time()))
            # Preserve the existing numeric heartbeat format and thresholds.
            from .store import atomic_write_text
            atomic_write_text(store.flusher_hb_path, str(time.time()))
    return progress


def _safe_name(metadata):
    name = metadata.get("original_name") or (
        "attachment." + {"HEIC": "heic", "HEIF": "heif", "MP4": "mp4"}.get(
            metadata.get("detected_type"), "bin"))
    name = "".join(c for c in name if c not in "/\\" and ord(c) >= 32)
    return name.encode("utf-8")[:240].decode("utf-8", "ignore") or "attachment.bin"


def _read_admission_for_release(path):
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    except FileNotFoundError:
        return None
    except OSError:
        raise AttachmentCallError("storage_invalid") from None
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or info.st_mode & 0o077
                or info.st_size > (1 << 20)):
            raise AttachmentCallError("storage_invalid")
        raw = os.read(fd, (1 << 20) + 1)
        if len(raw) > (1 << 20):
            raise AttachmentCallError("storage_invalid")
        record = json.loads(raw.decode("utf-8"))
        if (type(record) is not dict or type(record.get("schema_version")) is not int
                or record["schema_version"] != 1 or record.get("type") != "attachment_group_v1"):
            raise AttachmentCallError("storage_invalid")
        return record
    except (ValueError, UnicodeError, OSError):
        raise AttachmentCallError("storage_invalid") from None
    finally:
        os.close(fd)


def _release_group_holds(store, scope, group_id):
    """Only at a sole sender safe point, after all its material reads ended."""
    material = ledger(store).terminal_materials(scope, group_id)
    if material is None:
        return 0
    request_id = material["request_id"]
    if (type(request_id) is not str or request_id in (".", "..")
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", request_id)):
        raise AttachmentCallError("storage_invalid")
    path = Path(store.state_dir) / "attachment_admissions" / (request_id + ".json")
    record = _read_admission_for_release(path)
    if record is None or record.get("admitted") is not True or record.get("phase") != "admitted":
        return 0  # Missing/uncertain admission remains conservatively protected.
    if (record.get("scope") != scope or record.get("group_id") != group_id
            or record.get("request_id") != request_id
            or record.get("attachment_ids") != material["attachment_ids"]):
        raise AttachmentCallError("storage_invalid")
    from .attachments import AttachmentError
    def current_epoch():
        try:
            valid = current_scope(store) == scope
        except AttachmentCallError:
            valid = False
        if not valid:
            raise AttachmentError("epoch_mismatch")
    current_epoch()
    return catalog(store, current_epoch).release_delivery_holds(
        scope, group_id, request_id, material["attachment_ids"])["released_count"]


def _admission_directory(store, required=False):
    directory = Path(store.state_dir) / "attachment_admissions"
    try:
        info = directory.lstat()
    except FileNotFoundError:
        if required:
            raise AttachmentCallError("storage_invalid") from None
        return directory
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise AttachmentCallError("storage_invalid")
    return directory


def _job_admission_context(store, scope, state, job):
    """Return context only for an admitted journal linked to a verified job.

    None isolates only a local missing/incomplete/mismatched record. Reader,
    storage, schema and I/O errors deliberately propagate to the caller.
    """
    request_id = job["request_id"]
    if (type(request_id) is not str or request_id in (".", "..")
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", request_id)):
        raise AttachmentCallError("storage_invalid")
    directory = _admission_directory(store, required=True)
    record = _read_admission_for_release(directory / (request_id + ".json"))
    if record is None:
        _admission_directory(store, required=True)
        return None
    if (record.get("admitted") is not True or record.get("phase") != "admitted"
            or record.get("request_id") != request_id
            or record.get("job_id") != job["job_id"]
            or record.get("group_id") != job["group_id"] or record.get("scope") != scope):
        return None
    context = record.get("context_token")
    if type(context) is not str or len(context) > 65536 or "\x00" in context:
        return None
    if state.context_reference(scope, context) != job["context_ref"]:
        return None
    return context


def _reconcile_terminal_holds(store, scope, active_requests=()):
    # Bounded normal sender maintenance. No worker/cancel/GC preview calls this.
    directory = Path(store.state_dir) / "attachment_admissions"
    names = []
    try:
        # glob can suppress directory PermissionError. A failed global scan
        # must never masquerade as an empty collection of healthy journals.
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.endswith(".json"):
                    names.append(entry.name)
                    if len(names) > 10000:
                        raise AttachmentCallError("storage_invalid")
    except FileNotFoundError:
        if active_requests:
            raise AttachmentCallError("storage_invalid") from None
        return
    names.sort()
    cursor = getattr(store, "_attachment_terminal_cursor", "")
    selected = [name for name in names if name > cursor][:100]
    if not selected:
        store._attachment_terminal_cursor = ""
        return
    for name in selected:
        record = _read_admission_for_release(directory / name)
        if name[:-5] in active_requests:
            # A current verified pending job cannot provide terminal-release
            # proof. Its local journal linkage is checked by the job gate.
            store._attachment_terminal_cursor = name
            continue
        if (record is not None and record.get("scope") == scope
                and record.get("admitted") is True and record.get("attachment_ids") is not None):
            group_id = record.get("group_id")
            if ledger(store).terminal_materials(scope, group_id) is not None:
                _release_group_holds(store, scope, group_id)
        store._attachment_terminal_cursor = name


def flush_groups(store, baseurl, token):
    """Only the public sender runs this; no latest-context upgrade path."""
    from .ilink import ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, send_message, resp_ok
    from .media import send_media_message
    from .store import log
    if not active(store):
        return
    if getattr(store, "_attachment_runtime_blocked", False):
        return
    scope, state, files = current_scope(store), ledger(store), catalog(store)
    if _session(store).get("bot_token") != token:
        raise AttachmentCallError("epoch_mismatch")
    # Validate global ledger/catalog/root structure outside any per-job gate.
    # This reads metadata only, never body/usage or a new write transaction.
    jobs = [state.job_status(scope, jid) for jid in state.pending_jobs(scope)["job_ids"]]
    with files._db(False):
        pass
    _admission_directory(store, required=bool(jobs))
    # Validate every pending journal's global format/file safety before any
    # job may enter next_unit. None is cached only for known local v1 blocks.
    admissions = [(job, _job_admission_context(store, scope, state, job)) for job in jobs]
    active_requests = {job["request_id"] for job in jobs}
    _reconcile_terminal_holds(store, scope, active_requests)
    for job, context in admissions:
        if current_scope(store) != scope or _session(store).get("bot_token") != token:
            raise AttachmentCallError("epoch_mismatch")
        job_id = job["job_id"]
        if context is None:
            log("附件任务admission未完成或连接不符，保留并跳过本轮", job=job_id)
            continue
        while True:
            unit = state.next_unit(scope, job_id)
            if unit is None:
                break
            started = [False]
            settled = [False]
            def check():
                try:
                    current_scope_value = current_scope(store)
                except AttachmentCallError:
                    raise OperationAborted("epoch_mismatch") from None
                if current_scope_value != scope or _session(store).get("bot_token") != token:
                    raise OperationAborted("epoch_mismatch")
                current = state.job_status(scope, job_id)
                if current["state"] not in ("queued", "sending"):
                    raise OperationAborted("expired" if current["state"] == "expired" else "cancelled")
            def before(index, total):
                check()
                claimed = state.begin_unit(scope, job_id, unit["unit_id"])
                started[0] = claimed["send_allowed"]
                return claimed["send_allowed"]
            def result(index, total, response):
                state.settle_unit(scope, job_id, unit["unit_id"], "accepted" if resp_ok(response) else "unknown")
                settled[0] = True
            progress = stage_progress(store, check)
            try:
                if unit["kind"] == "attachment":
                    material = files.acquire(scope, unit["attachment_id"], "group:" + unit["group_id"])
                    meta = material["metadata"]
                    item_type = (ITEM_IMAGE if meta["kind"] == "image" and meta["preview_state"] == "readable"
                        else ITEM_VIDEO if meta["kind"] == "video" and meta.get("detected_type") == "MP4" else ITEM_FILE)
                    # After the accepted result the job can legitimately be
                    # complete; the final observational checkpoint is not new authority.
                    def media_progress(event):
                        if event["stage"] == "send_after":
                            stage_progress(store)(event)
                        else:
                            progress(event)
                    response = send_media_message(baseurl, token, scope["owner_id"], context,
                        material["path"], item_type, client_key=unit["client_key"],
                        progress=media_progress, before_send=before, on_result=result,
                        file_name=_safe_name(meta))
                else:
                    response = send_message(baseurl, token, scope["owner_id"], unit["text"], context,
                        client_key=unit["client_key"], pace=0, before_send=before, on_result=result)
                if not settled[0]:
                    raise AttachmentCallError("storage_invalid")
                if not resp_ok(response):
                    break
                stage_progress(store)({"schema_version": 1, "stage": "send_after", "completed": 1, "total": 1})
            except Exception as error:
                if not settled[0]:
                    try:
                        state.settle_unit(scope, job_id, unit["unit_id"], "unknown" if started[0] else "proven_unsent")
                    except Exception:
                        pass  # Durable inflight is reconciled to unknown at restart.
                log("附件发送单元暂停（reason=%s）" % getattr(error, "code", "operation_failed"))
                break
        status = state.group_status(scope, job["group_id"])
        if status["state"] in ("complete", "cancelled"):
            _release_group_holds(store, scope, job["group_id"])
