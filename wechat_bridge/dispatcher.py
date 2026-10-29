"""Opt-in backend supervisor: durable jobs, frozen routing, explicit approvals.

No provider I/O occurs inside a database transaction. Legacy WorkerState is not
used as a long-running job lease. Only this module admits managed outbox items.
"""
import fcntl
import json
import os
import re
import time
import uuid
from pathlib import Path

from .backend_state import BackendState, digest, encode
from .store import atomic_write_json, load_json, log, message_receipt_lock, safe_message_id

TERMINAL = {"succeeded", "failed", "cancelled", "abandoned"}
ACTIVE = {"running", "waiting_approval", "cancelling"}
RESERVED = {"/backend", "/approve", "/cancel", "/status"}


def managed_inbox(store):
    path = os.path.join(store.base, "backend_inbox")
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


class Dispatcher:
    def __init__(self, store, config, owner_id, bot_id, factory=None, clock=time.time):
        self.store, self.config, self.owner_id = store, config, owner_id
        self.clock = clock
        self.account = config["account_scope"] + ":" + digest([bot_id, owner_id])[:24]
        self.inbox = managed_inbox(store)
        self.state = BackendState(store, config, owner_id, bot_id)
        self.factory = factory
        self.drivers = {}
        self.lock = None

    def acquire(self):
        if self.lock is not None:
            return
        path = os.path.join(self.store.state_dir, "backend-worker.lock")
        lock = open(path, "a")
        os.chmod(path, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            lock.close()
            raise RuntimeError("another backend scheduler owns this state")
        self.lock = lock
        self.state.recover(self.clock())

    def close(self):
        try:
            if self.lock is not None:
                # Try durable fencing first. A persistence failure propagates,
                # but must never skip provider cleanup in the finally block.
                self.state.recover(self.clock())
        finally:
            try:
                for driver in list(self.drivers.values()):
                    try:
                        driver.close()
                    except Exception:
                        pass
                self.drivers.clear()
            finally:
                if self.lock is not None:
                    self.lock.close()
                    self.lock = None

    def _route(self, db, sender):
        row = db.execute("SELECT backend,revision FROM routes WHERE sender=?", (sender,)).fetchone()
        if row:
            return row[0], row[1]
        backend = self.config["default_backend"]
        db.execute("INSERT INTO routes VALUES (?,?,1)", (sender, backend))
        return backend, 1

    def _legacy_busy(self):
        if any(Path(self.store.inbox_dir).glob("*.json")):
            return True
        legacy = self.config.get("legacy_state_dir")
        if legacy:
            from .worker_state import WorkerState
            worker = WorkerState(self.store.base, legacy, clock=self.clock)
            # Claims are atomically replaced. Do not reacquire receipt lock here:
            # ingestion already holds it, and flock on a second fd would deadlock.
            if worker._claims():
                return True
        return False

    def _busy(self, db, sender):
        if db.execute("SELECT 1 FROM jobs WHERE sender=? AND status NOT IN "
                      "('succeeded','failed','cancelled','abandoned') LIMIT 1", (sender,)).fetchone():
            return True
        if db.execute("SELECT 1 FROM outputs WHERE phase IN ('prepared','publishing','unknown') LIMIT 1").fetchone():
            return True
        return self._legacy_busy() or any(Path(self.store.pending_dir).glob("*.json")) or any(
            Path(self.store.sending_dir).glob("*.json"))

    def _output(self, db, identity, record, text, kind, job=None):
        output_id = "v1-" + digest([self.account, identity])[:48]
        envelope = {"message_id": output_id, "to_user_id": record["from_user_id"],
                    "context_token": record.get("context_token", ""), "text": text,
                    "enqueued_at": self.clock(), "client_key": "backend-v1:" + output_id,
                    "backend_v1": {"account_scope": self.account, "output_kind": kind}}
        if job:
            envelope["backend_v1"].update({key: job[key] for key in (
                "execution_id", "attempt_id", "backend_id", "backend_instance_ref", "conversation_key")})
        db.execute("INSERT OR IGNORE INTO outputs VALUES (?,'prepared',?)", (output_id, encode(envelope)))

    def _control(self, db, record, words):
        sender, mid = record["from_user_id"], str(record["message_id"])
        command = words[0]
        action = None
        if command == "/backend":
            backend, revision = self._route(db, sender)
            if len(words) == 1:
                text = "当前后端：%s；可选：%s。用 /backend 名称 切换（仅空闲）。" % (
                    backend, ", ".join(sorted(self.config["backends"])))
            elif len(words) != 2 or words[1] not in self.config["backends"]:
                text = "未知后端。用 /backend 查看可选名称。"
            elif self._busy(db, sender):
                text = "仍有任务、待发回复或待核状态，暂不能切换。用 /status 查看。"
            else:
                db.execute("UPDATE routes SET backend=?,revision=? WHERE sender=?", (words[1], revision + 1, sender))
                text = "已切换至 %s。" % words[1]
        elif command == "/status":
            rows = db.execute("SELECT id,status FROM jobs WHERE sender=? ORDER BY rowid DESC LIMIT 3", (sender,)).fetchall()
            text = "当前后端：%s\n%s" % (self._route(db, sender)[0], "\n".join(
                "%s %s" % (row[0], row[1]) for row in rows) or "暂无任务")
            if db.execute("SELECT 1 FROM outputs WHERE phase='unknown'").fetchone():
                text += "\n有发送结果不明记录，请本机核对，系统不会自动补发。"
        elif command == "/approve":
            text = "用 /approve 审批ID allow 或 deny；仅对该动作生效一次。"
            if len(words) == 3 and words[2] in ("allow", "deny"):
                row = db.execute("SELECT status,payload FROM approvals WHERE id=?", (words[1],)).fetchone()
                if row:
                    approval = json.loads(row[1])
                    job = self.state.get_job(db, approval["execution_id"])
                    if (row[0] == "pending" and job and job["status"] == "waiting_approval"
                            and approval["sender_key"] == sender and approval["account_scope"] == self.account
                            and approval["attempt_id"] == job["attempt_id"]
                            and approval["backend_instance_ref"] == job["backend_instance_ref"]
                            and approval["expires_at"] > self.clock()
                            and job["deadline_at"] > self.clock()
                            and job["execution_id"] in self.drivers):
                        approval["decision"] = words[2]
                        db.execute("UPDATE approvals SET status='deciding',payload=? WHERE id=?", (encode(approval), words[1]))
                        action = ("approve", approval)
                        text = "已记录一次性%s；执行结果另行回报。" % ("允许" if words[2] == "allow" else "拒绝")
                    else:
                        text = "审批已失效、已处理或不属于当前执行；未放行。"
                else:
                    text = "审批ID不存在；未放行。"
        else:  # /cancel
            text = "用 /cancel 任务ID 请求取消；确认停止后才算取消完成。"
            if len(words) == 2:
                job = self.state.get_job(db, words[1])
                if job and job["sender_key"] == sender:
                    if job["status"] == "queued":
                        job.update(status="cancelled", updated_at=self.clock())
                        self.state.put_job(db, job)
                        text = "已取消尚未启动的任务。"
                    elif job["status"] in ACTIVE:
                        self._mark_cancelling(db, job, "user_cancel")
                        action = ("cancel", job)
                        text = "取消请求已记录，等待后端确认。"
                    else:
                        text = "任务状态：%s。结果不明时需本机核对，不能凭取消命令视为已停止。" % job["status"]
        self._output(db, "control:" + mid, record, text, "control")
        return action

    def ingest(self):
        actions = []
        records = []
        for path in Path(self.inbox).glob("*.json"):
            record = load_json(str(path))
            if not isinstance(record, dict) or record.get("from_user_id") != self.owner_id:
                raise ValueError("managed inbox identity invalid")
            if safe_message_id(record.get("message_id")) != path.stem:
                raise ValueError("managed inbox filename mismatch")
            records.append((record.get("received_at", 0), path.name, path, record))
        for _, _, path, record in sorted(records):
            with message_receipt_lock(self.store):
                mid, sender = str(record["message_id"]), record["from_user_id"]
                with self.state.transaction() as db:
                    if not db.execute("SELECT 1 FROM inputs WHERE id=?", (mid,)).fetchone():
                        words = record.get("text", "").strip().split()
                        if words and words[0] in RESERVED:
                            action = self._control(db, record, words)
                            if action:
                                actions.append(action)
                            db.execute("INSERT INTO inputs VALUES (?,?,'control',NULL,?)", (mid, sender, encode(record)))
                        else:
                            backend, revision = self._route(db, sender)
                            if backend == "muse":
                                # Keep the original identity/context for the legacy worker.
                                receipt = load_json(os.path.join(self.store.receipts_dir, mid + ".json"), {})
                                if receipt.get("status") != "complete":
                                    atomic_write_json(os.path.join(self.store.inbox_dir, mid + ".json"), record)
                                db.execute("INSERT INTO inputs VALUES (?,?,'legacy',NULL,?)", (mid, sender, encode(record)))
                            else:
                                selected = self.config["backends"][backend]
                                execution = "job-" + uuid.uuid4().hex
                                conversation = digest([self.account, sender, backend, selected["instance_id"]])
                                job = {"execution_id": execution, "attempt_id": "attempt-" + uuid.uuid4().hex,
                                       "account_scope": self.account, "sender_key": sender,
                                       "backend_id": backend, "backend_instance_ref": selected["instance_id"],
                                       "conversation_key": conversation, "route_revision": revision,
                                       "records": [record], "status": "queued", "created_at": self.clock(),
                                       "events": [], "session_id": None}
                                self.state.put_job(db, job)
                                db.execute("INSERT INTO inputs VALUES (?,?,'job',?,?)", (mid, sender, execution, encode(record)))
                # Receipt stays durable even after managed inbox disappears.
                if path.exists():
                    path.unlink()
        for action, payload in actions:
            if action == "approve":
                self._deliver_decision(payload)
            else:
                self._request_cancel(payload)

    def _deliver_decision(self, approval):
        job_id = approval["execution_id"]
        # A later control in the same durable ingest batch may revoke this
        # decision. Re-read immediately before external delivery, never trust
        # the earlier in-memory action collected by ingest().
        with self.state.transaction() as db:
            row = db.execute("SELECT status,payload FROM approvals WHERE id=?", (approval["id"],)).fetchone()
            job = self.state.get_job(db, job_id)
            if (not row or row[0] != "deciding" or json.loads(row[1]) != approval
                    or not job or job["status"] != "waiting_approval"
                    or job["attempt_id"] != approval["attempt_id"]
                    or job["account_scope"] != approval["account_scope"]
                    or job["sender_key"] != approval["sender_key"]
                    or job["backend_instance_ref"] != approval["backend_instance_ref"]
                    or self.clock() >= min(job["deadline_at"], approval["expires_at"])):
                return
        try:
            self.drivers[job_id].respond_approval(approval["provider_id"], approval["decision"], approval["action_digest"])
        except Exception:
            self._unknown(job_id, "approval_delivery_unknown")
            return
        with self.state.transaction() as db:
            db.execute("UPDATE approvals SET status='consumed' WHERE id=? AND status='deciding'", (approval["id"],))
            job = self.state.get_job(db, job_id)
            if job["status"] == "waiting_approval":
                job.update(status="running", updated_at=self.clock())
                self.state.put_job(db, job)

    def _start_next(self):
        with self.state.transaction() as db:
            busy = db.execute("SELECT 1 FROM jobs WHERE status IN ('running','waiting_approval','cancelling','unknown')").fetchone()
            uncertain_output = db.execute("SELECT 1 FROM outputs WHERE phase='unknown' LIMIT 1").fetchone()
            if busy or uncertain_output or self._legacy_busy():
                return
            row = db.execute("SELECT payload FROM jobs WHERE status='queued' ORDER BY rowid LIMIT 1").fetchone()
            if not row:
                return
            job = json.loads(row[0])
            session = db.execute("SELECT session_id FROM sessions WHERE key=?", (job["conversation_key"],)).fetchone()
            job.update(status="running", started_at=self.clock(), deadline_at=self.clock() + self.config.get("job_timeout_seconds", 1800),
                       session_id=session[0] if session else None)
            self.state.put_job(db, job)
        selected = self.config["backends"][job["backend_id"]]
        request = {key: job[key] for key in ("execution_id", "attempt_id", "account_scope", "sender_key",
                   "backend_id", "backend_instance_ref", "conversation_key", "session_id", "deadline_at")}
        request.update(schema_version=1, cwd=selected["cwd"], execution_policy=selected.get("execution_policy", {"approval": "explicit"}))
        request["messages"] = []
        for record in job["records"]:
            attachments = []
            for path in record.get("media_paths", []):
                if os.path.isfile(path):
                    attachments.append({"path": os.path.realpath(path), "status": "available"})
                else:
                    attachments.append({"status": "failed", "count": 1})
            if record.get("media_failed_count"):
                attachments.append({"status": "failed", "count": record["media_failed_count"]})
            request["messages"].append({"message_id": str(record["message_id"]), "text": record.get("text", ""), "attachments": attachments})
        try:
            if self.factory is None:
                from .backends.registry import create_backend
                driver = create_backend(job["backend_id"], selected)
            else:
                driver = self.factory(job["backend_id"], selected)
            self.drivers[job["execution_id"]] = driver
            driver.start(request)
        except Exception:
            self._unknown(job["execution_id"], "backend_start_unknown")

    def _mark_cancelling(self, db, job, reason):
        if job["status"] != "cancelling":
            job.update(status="cancelling", cancel_reason=reason, cancel_deadline=self.clock() + 10)
            self.state.put_job(db, job)
        for row in db.execute("SELECT id,payload FROM approvals WHERE status IN ('pending','deciding')").fetchall():
            if json.loads(row[1])["execution_id"] == job["execution_id"]:
                db.execute("UPDATE approvals SET status='invalidated' WHERE id=?", (row[0],))

    def _request_cancel(self, job):
        driver = self.drivers.get(job["execution_id"])
        if driver:
            try:
                driver.cancel()
            except Exception:
                self._unknown(job["execution_id"], "cancel_delivery_unknown")
        else:
            self._unknown(job["execution_id"], "cancel_runtime_missing")

    def _unknown(self, job_id, reason):
        with self.state.transaction() as db:
            job = self.state.get_job(db, job_id)
            if not job or job["status"] in TERMINAL or job["status"] == "unknown":
                return
            job.update(status="unknown", error_code=reason, updated_at=self.clock())
            self.state.put_job(db, job)
            for row in db.execute("SELECT id,payload FROM approvals WHERE status IN ('pending','deciding')").fetchall():
                if json.loads(row[1])["execution_id"] == job_id:
                    db.execute("UPDATE approvals SET status='invalidated' WHERE id=?", (row[0],))
            self._output(db, "unknown:" + job_id, job["records"][-1],
                         "任务 %s 结果不明（%s），已停止自动推进；请在本机核对。" % (job_id, reason), "error", job)
        self._close_driver(job_id)

    def _close_driver(self, job_id):
        driver = self.drivers.pop(job_id, None)
        if driver:
            try:
                driver.close()
            except Exception:
                pass

    def event(self, job_id, event):
        terminal = False
        with self.state.transaction() as db:
            job = self.state.get_job(db, job_id)
            if not job or job["status"] not in ACTIVE:
                return
            from .backends.contract import validate_event
            event = validate_event(event, job_id, job["attempt_id"])
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id or len(event_id) > 256:
                raise ValueError("invalid event id")
            if event_id in job["events"]:
                return
            if len(job["events"]) >= 10000:
                raise ValueError("backend event limit exceeded")
            kind = event.get("type")
            if kind not in {"started", "progress", "approval_requested", "final", "failed", "cancelled", "unknown"}:
                raise ValueError("unsupported backend event")
            job["events"].append(event_id)
            now = self.clock()
            if kind == "approval_requested":
                if job["status"] != "running" or now >= job["deadline_at"]:
                    raise ValueError("approval outside running execution")
                summary, action_hash = event.get("action_summary"), event.get("action_digest")
                provider_id = event.get("approval_id")
                if (not isinstance(summary, str) or not summary.strip() or len(summary) > 2000
                        or not isinstance(action_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", action_hash)
                        or not isinstance(provider_id, str) or not provider_id or len(provider_id) > 256):
                    raise ValueError("approval requires bounded action and exact digest")
                for row in db.execute("SELECT payload FROM approvals").fetchall():
                    previous = json.loads(row[0])
                    if previous["execution_id"] == job_id and previous["provider_id"] == provider_id:
                        raise ValueError("provider approval id reused")
                expiry = min(now + self.config.get("approval_timeout_seconds", 300), job["deadline_at"])
                if "expires_at" in event:
                    if isinstance(event["expires_at"], bool) or not isinstance(event["expires_at"], (int, float)):
                        raise ValueError("invalid approval expiry")
                    expiry = min(expiry, event["expires_at"])
                if not now < expiry:
                    raise ValueError("approval already expired")
                approval_id = "ap-" + uuid.uuid4().hex
                approval = {key: job[key] for key in ("execution_id", "attempt_id", "account_scope", "sender_key", "backend_instance_ref")}
                approval.update(id=approval_id, provider_id=provider_id, action_summary=summary,
                                action_digest=action_hash, expires_at=expiry)
                db.execute("INSERT INTO approvals VALUES (?,'pending',?)", (approval_id, encode(approval)))
                job["status"] = "waiting_approval"
                self._output(db, "approval:" + approval_id, job["records"][-1],
                    "任务 %s 请求一次性权限：\n%s\n动作摘要校验：%s\n有效至 %s\n/approve %s allow\n/approve %s deny" % (
                        job_id, summary, action_hash[:16], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expiry)), approval_id, approval_id), "approval", job)
            elif kind == "final":
                if job["status"] != "running" or now >= job["deadline_at"]:
                    raise ValueError("final outside running deadline")
                text = event.get("text")
                if not isinstance(text, str) or not text.strip() or len(text) > 64000:
                    raise ValueError("final must be nonempty bounded text")
                session = event.get("session_id")
                if session is not None and (not isinstance(session, str) or not session or len(session) > 1024):
                    raise ValueError("invalid provider session")
                if session:
                    db.execute("INSERT OR REPLACE INTO sessions VALUES (?,?)", (job["conversation_key"], session))
                job.update(status="succeeded", session_id=session)
                self._output(db, "final:" + job_id, job["records"][-1], text, "final", job)
                terminal = True
            elif kind in ("failed", "cancelled", "unknown"):
                effect = event.get("side_effect_state", "unknown")
                if effect not in {"none", "possible", "confirmed", "unknown"}:
                    raise ValueError("invalid side effect state")
                job.update(status=kind, error_code=str(event.get("error_code", kind))[:100], side_effect_state=effect)
                self._output(db, "terminal:" + job_id, job["records"][-1],
                             "任务 %s：%s。" % (job_id, kind), "status", job)
                terminal = True
            job["updated_at"] = now
            self.state.put_job(db, job)
            if terminal:
                for row in db.execute("SELECT id,payload FROM approvals WHERE status IN ('pending','deciding')").fetchall():
                    if json.loads(row[1])["execution_id"] == job_id:
                        db.execute("UPDATE approvals SET status='invalidated' WHERE id=?", (row[0],))
        if terminal:
            self._close_driver(job_id)

    def _poll(self):
        for job_id, driver in list(self.drivers.items()):
            try:
                events = driver.poll()
                if not isinstance(events, list) or len(events) > 100:
                    raise ValueError("poll must return <=100 events")
                for event in events:
                    self.event(job_id, event)
            except Exception:
                self._unknown(job_id, "backend_protocol_or_connection_error")

    def _expire(self):
        cancel, unknown = [], []
        with self.state.transaction() as db:
            for row in db.execute("SELECT payload FROM jobs WHERE status IN ('running','waiting_approval','cancelling')").fetchall():
                job = json.loads(row[0])
                if job["status"] == "cancelling":
                    if self.clock() >= job["cancel_deadline"]:
                        unknown.append(job["execution_id"])
                    continue
                expired = self.clock() >= job["deadline_at"]
                if job["status"] == "waiting_approval":
                    for approval in db.execute("SELECT payload FROM approvals WHERE status='pending'").fetchall():
                        data = json.loads(approval[0])
                        if data["execution_id"] == job["execution_id"] and self.clock() >= data["expires_at"]:
                            expired = True
                if expired:
                    self._mark_cancelling(db, job, "deadline_or_approval_expired")
                    cancel.append(job)
        for job in cancel:
            self._request_cancel(job)
        for job_id in unknown:
            self._unknown(job_id, "cancel_not_confirmed")

    def publish(self):
        with self.state.transaction() as db:
            rows = db.execute("SELECT id,payload FROM outputs WHERE phase='prepared' ORDER BY rowid").fetchall()
        for row in rows:
            output_id, envelope = row[0], json.loads(row[1])
            with self.state.transaction() as db:
                if db.execute("UPDATE outputs SET phase='publishing' WHERE id=? AND phase='prepared'", (output_id,)).rowcount != 1:
                    continue
            name = output_id + ".json"
            # Identity is stable across retries, and is never handed to merge.
            existing = any(os.path.exists(os.path.join(path, name)) for path in (
                self.store.pending_dir, self.store.sending_dir, self.store.sent_dir, self.store.failed_dir))
            if not existing:
                atomic_write_json(os.path.join(self.store.pending_dir, name), envelope)
            with self.state.transaction() as db:
                db.execute("UPDATE outputs SET phase='admitted' WHERE id=?", (output_id,))

    def tick(self):
        if self.lock is None:
            raise RuntimeError("scheduler lock required")
        self.ingest()
        self._expire()
        self._poll()
        self._start_next()
        self.publish()

    def run(self, stop):
        self.acquire()
        try:
            while not stop.is_set():
                self.tick()
                stop.wait(0.2)
        finally:
            self.close()
