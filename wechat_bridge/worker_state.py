"""Machine-enforced inbox ownership and worker reply handoff.

The Hook and workers share the receipt -> claim lock order.  A lease is a
batch capability, not proof that a platform worker ran.  Queue admission is
not delivery: failed/uncertain batches remain visible for manual handling.
"""
import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from .store import (Store, atomic_write_json, atomic_write_text, load_json,
                    message_receipt_lock, safe_message_id)

FIRST_STALE = 180
REWAKE_INTERVAL = 600
STARTED_STALE = 700
CLAIM_STALE = 21600
REWAKE_MAX = 3


@contextmanager
def file_lock(path, nonblocking=False):
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


class WorkerState:
    def __init__(self, base, state_dir, clock=time.time):
        self.store = Store(base)
        self.state = Path(state_dir)
        self.state.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.claim_path = self.state / "wechat_inbox_claimed.txt"
        self.claim_lock = self.state / "wechat_inbox_claimed.lock"
        self.seen_path = self.state / "wechat_inbox_seen.txt"
        self.started = self.state / "wechat_worker_started"
        self.started.mkdir(exist_ok=True)
        self.rewake_path = self.state / "wechat_inbox_rewake.json"
        self.notice_path = self.state / "wechat_notices.json"
        self.batch_dir = Path(self.store.state_dir) / "reply_batches"
        self.batch_dir.mkdir(exist_ok=True)

    @contextmanager
    def locked(self):
        with message_receipt_lock(self.store):
            with file_lock(self.claim_lock):
                yield

    def _seen(self):
        try:
            return self.seen_path.read_text().splitlines()
        except FileNotFoundError:
            return []

    def _claims(self):
        claims = {}
        try:
            lines = self.claim_path.read_text().splitlines()
        except FileNotFoundError:
            lines = []
        for line in lines:
            fields = line.split()
            if len(fields) < 2:
                continue
            try:
                mid = safe_message_id(fields[0])
                first = int(fields[1])
                last = int(fields[2]) if len(fields) > 2 else first
            except (ValueError, TypeError):
                continue
            claims[mid] = {"first": first, "last": last,
                           "lease": fields[3] if len(fields) > 3 else "legacy"}
        return claims

    def _write_claims(self, claims):
        atomic_write_text(str(self.claim_path), "".join(
            "%s %d %d %s\n" % (mid, row["first"], row["last"], row["lease"])
            for mid, row in sorted(claims.items())))

    def _rewakes(self):
        rows = load_json(str(self.rewake_path))
        if rows is None:
            rows = {}
            # One-time migration preserves the existing three-rewake budget.
            try:
                legacy = (self.state / "wechat_inbox_rewake.txt").read_text().splitlines()
            except FileNotFoundError:
                legacy = []
            for line in legacy:
                fields = line.split()
                if len(fields) >= 3:
                    rows[fields[0]] = {"count": fields[1], "last": fields[2]}
        normalized = {}
        if not isinstance(rows, dict):
            return normalized
        for key, row in rows.items():
            try:
                normalized[safe_message_id(key)] = {
                    "count": min(REWAKE_MAX, max(0, int(row["count"]))),
                    "last": max(0, int(row["last"]))}
            except (ValueError, TypeError, KeyError):
                continue
        return normalized

    def _receipt(self, mid):
        return load_json(os.path.join(self.store.receipts_dir, mid + ".json"), {}) or {}

    def _complete(self, ids, claims):
        """Caller holds both locks. Commit receipt before deleting any inbox."""
        seen = self._seen()
        known = set(seen)
        for mid in ids:
            receipt = self._receipt(mid)
            if not receipt.get("from_user_id"):
                record = load_json(os.path.join(self.store.inbox_dir, mid + ".json"), {}) or {}
                if (not isinstance(record, dict) or
                        safe_message_id(record.get("message_id")) != mid or
                        not record.get("from_user_id")):
                    raise ValueError("cannot complete without matching inbox identity: " + mid)
                receipt["from_user_id"] = record["from_user_id"]
            receipt.update(message_id=mid, status="complete", completed_at=self.clock())
            atomic_write_json(os.path.join(self.store.receipts_dir, mid + ".json"), receipt)
            if mid not in known:
                seen.append(mid)
                known.add(mid)
            for path in (Path(self.store.inbox_dir) / (mid + ".json"),
                         self.started / (mid + ".started")):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
            claims.pop(mid, None)
        # Every writer and the pruning reader use claim_lock, so neither inode
        # replacement nor truncation can lose an in-flight helper append.
        atomic_write_text(str(self.seen_path), "\n".join(seen) + ("\n" if seen else ""))
        self._write_claims(claims)

    def covered(self, mid):
        """Return explicit queue/registry status, never count failed as sent."""
        for status, directory in (("failed", self.store.failed_dir),
                                  ("pending", self.store.pending_dir),
                                  ("sending", self.store.sending_dir),
                                  ("sent", self.store.sent_dir)):
            for path in Path(directory).glob("*.json"):
                job = load_json(str(path), {}) or {}
                if not isinstance(job, dict):
                    continue
                ids = job.get("covers_message_ids") or [job.get("message_id") or path.stem]
                if mid in [str(x) for x in ids]:
                    return {"status": status, "job": path.name}
        # A durable batch registry covers the rename windows between queue
        # directories. 'prepared' is deliberately not treated as enqueued.
        for path in self.batch_dir.glob("*.json"):
            batch = load_json(str(path), {}) or {}
            if isinstance(batch, dict) and mid in batch.get("covers_message_ids", []):
                return {"status": "queued" if batch.get("admitted") else "uncertain",
                        "job": batch.get("job_name", path.name)}
        return None

    def claim(self, ids, lease=None):
        lease = lease or uuid.uuid4().hex
        if not lease or any(c.isspace() for c in lease):
            raise ValueError("invalid lease")
        result = {"lease": lease, "claimed": [], "owned": [], "skipped": {}}
        with self.locked():
            claims, seen = self._claims(), set(self._seen())
            for value in ids:
                mid = safe_message_id(value)
                if mid in seen or self._receipt(mid).get("status") == "complete":
                    result["skipped"][mid] = "complete"
                    continue
                if not (Path(self.store.inbox_dir) / (mid + ".json")).is_file():
                    result["skipped"][mid] = "missing"
                    continue
                coverage = self.covered(mid)
                if coverage:
                    result["skipped"][mid] = coverage["status"]
                    continue
                previous = claims.get(mid)
                if previous:
                    if previous["lease"] == lease:
                        result["owned"].append(mid)
                    else:
                        result["skipped"][mid] = "claimed"
                    continue
                now = int(self.clock())
                claims[mid] = {"first": now, "last": now, "lease": lease}
                atomic_write_text(str(self.started / (mid + ".started")), str(now))
                result["claimed"].append(mid)
            self._write_claims(claims)
        return result

    def adopt(self, ids, lease):
        """Start only the exact Hook lease; a late old worker cannot steal it."""
        # Consume the event capability once. Duplicate platform deliveries of
        # the same payload cannot both start workers under the public lease.
        result = {"lease": uuid.uuid4().hex, "owned": [], "skipped": {}, "covered": {}}
        with self.locked():
            claims = self._claims()
            complete = []
            for value in ids:
                mid = safe_message_id(value)
                if self._receipt(mid).get("status") == "complete":
                    result["skipped"][mid] = "complete"
                    continue
                if claims.get(mid, {}).get("lease") != lease:
                    result["skipped"][mid] = "lease_lost"
                    continue
                coverage = self.covered(mid)
                if coverage:
                    result["covered"][mid] = coverage
                    if coverage["status"] in ("pending", "sending", "sent", "queued"):
                        complete.append(mid)
                    continue
                if not (Path(self.store.inbox_dir) / (mid + ".json")).is_file():
                    result["skipped"][mid] = "missing"
                    continue
                claims[mid]["lease"] = result["lease"]
                atomic_write_text(str(self.started / (mid + ".started")), str(int(self.clock())))
                result["owned"].append(mid)
            self._complete(complete, claims)
        return result

    def complete(self, ids, lease):
        with self.locked():
            claims = self._claims()
            normalized = [safe_message_id(mid) for mid in ids]
            for mid in normalized:
                if self._receipt(mid).get("status") != "complete" and claims.get(mid, {}).get("lease") != lease:
                    raise ValueError("lease lost for " + mid)
                coverage = self.covered(mid)
                if self._receipt(mid).get("status") != "complete" and (
                        not coverage or coverage["status"] not in ("pending", "sending", "sent", "queued")):
                    raise ValueError("no admitted reply for " + mid)
            self._complete(normalized, claims)
        return {"completed": normalized}

    def enqueue_reply(self, ids, to, context, text, lease, media_path=None):
        normalized = list(dict.fromkeys(safe_message_id(mid) for mid in ids))
        if not normalized or not to or not isinstance(text, str):
            raise ValueError("ids, recipient and a text string required")
        media = None
        if media_path is not None:
            media = Path(os.path.expanduser(str(media_path))).resolve(strict=True)
            if not media.is_file():
                raise ValueError("media_path must be an existing regular file")
        if not text.strip() and media is None:
            raise ValueError("reply text or a regular media file required")
        with self.locked():
            claims = self._claims()
            for mid in normalized:
                if claims.get(mid, {}).get("lease") != lease:
                    raise ValueError("lease lost for " + mid)
                coverage = self.covered(mid)
                if coverage:
                    raise ValueError("reply already registered for %s (%s)" % (mid, coverage["status"]))
                record = load_json(os.path.join(self.store.inbox_dir, mid + ".json"), {})
                if not isinstance(record, dict) or record.get("from_user_id") != to:
                    raise ValueError("recipient does not match owned inbox")
            latest = normalized[-1]
            name = latest + ".json"
            job = {"message_id": latest, "to_user_id": to, "context_token": context,
                   "text": text, "covers_message_ids": normalized,
                   "enqueued_at": self.clock(), "client_key": "worker:" + latest}
            if media is not None:
                # The sender owns upload/type/size checks. Keep the exact path
                # in the same private immutable task as its caption/identity.
                job["media_path"] = str(media)
            registry = {"covers_message_ids": normalized, "job_name": name,
                        "job": job, "admitted": False, "phase": "prepared"}
            registry_path = self.batch_dir / name
            # Register before publication. A crash here is explicitly uncertain,
            # so recovery cannot silently manufacture a second reply.
            atomic_write_json(str(registry_path), registry)
            registry["phase"] = "publishing"
            atomic_write_json(str(registry_path), registry)
            atomic_write_json(os.path.join(self.store.pending_dir, name), job)
            registry["admitted"] = True
            registry["phase"] = "admitted"
            atomic_write_json(str(registry_path), registry)
            self._complete(normalized, claims)
        return {"queued": name, "covers_message_ids": normalized, "completed": normalized}

    def recover_prepared(self, job_name):
        """Recreate only a batch proven never published; unknown sends stop."""
        if not job_name.endswith(".json") or safe_message_id(job_name[:-5]) + ".json" != job_name:
            raise ValueError("invalid job name")
        with self.locked():
            path = self.batch_dir / job_name
            batch = load_json(str(path), {}) or {}
            if batch.get("phase") != "prepared" or batch.get("admitted"):
                raise ValueError("publication may have started; manual reconciliation required")
            for directory in (self.store.pending_dir, self.store.sending_dir,
                              self.store.sent_dir, self.store.failed_dir):
                if (Path(directory) / job_name).exists():
                    raise ValueError("job already present; do not republish")
            job = batch.get("job")
            if not isinstance(job, dict) or not job.get("client_key"):
                raise ValueError("missing fixed prepared task identity")
            batch["phase"] = "publishing"
            atomic_write_json(str(path), batch)
            atomic_write_json(os.path.join(self.store.pending_dir, job_name), job)
            batch.update(phase="admitted", admitted=True)
            atomic_write_json(str(path), batch)
            self._complete(batch["covers_message_ids"], self._claims())
        return {"recovered": job_name, "client_key": job["client_key"]}

    def prune_seen(self):
        with self.locked():
            seen = self._seen()
            known = set(seen)
            # Public maintenance must preserve the same legacy migration as
            # plan(): a replayed, already-consumed inbox must gain a permanent
            # sender-bound receipt before its old seen row can be evicted.
            completed = []
            for path in Path(self.store.inbox_dir).glob("*.json"):
                try:
                    mid = safe_message_id(path.stem)
                except (TypeError, ValueError):
                    continue
                if mid in known or self._receipt(mid).get("status") == "complete":
                    completed.append(mid)
            if completed:
                self._complete(completed, self._claims())
                seen = self._seen()
            if len(seen) > 5000:
                atomic_write_text(str(self.seen_path), "\n".join(seen[-2000:]) + "\n")
            return {"before": len(seen), "after": min(len(seen), 2000) if len(seen) > 5000 else len(seen)}

    def _notice(self, notices, key, now):
        row = notices.get(key, {})
        if now < row.get("next_attempt_at", 0):
            return False
        attempts = int(row.get("attempts", 0)) + 1
        cooldown = max(1, int(os.environ.get("WECHAT_NOTICE_RETRY_SECONDS", "600")))
        budget = max(1, int(os.environ.get("WECHAT_NOTICE_RETRY_BUDGET", "3")))
        # No delivery ACK contract is known. Keep unconfirmed and periodically
        # retry with bounded bursts, rather than latching 'notified' forever.
        delay = cooldown if attempts % budget else max(cooldown, 3600)
        row.update(attempts=attempts, last_attempt_at=now,
                   next_attempt_at=now + delay, confirmed=False)
        notices[key] = row
        return True

    def notice_result(self, keys, accepted):
        with self.locked():
            rows = load_json(str(self.notice_path), {}) or {}
            for key in keys:
                if key in rows:
                    rows[key]["last_runtime_accepted"] = bool(accepted)
            atomic_write_json(str(self.notice_path), rows)

    def plan(self, serve_alive=True):
        now = int(self.clock())
        with self.locked():
            claims, seen = self._claims(), self._seen()
            rw = self._rewakes()
            notices = load_json(str(self.notice_path), {}) or {}
            active_notices = set()
            result = {"decision": "silent", "reason": "no new wechat messages", "payload": {}}

            def notice(reason, keys, payload):
                active_notices.update(keys)
                eligible = [key for key in keys if self._notice(notices, key, now)]
                if eligible:
                    return {"decision": "wake", "reason": reason,
                            "payload": dict(payload, notice_keys=eligible)}
                return {"decision": "silent", "reason": reason + " cooling down", "payload": {}}

            if Path(self.store.needs_relogin_path).exists():
                result = notice("wechat_relogin_needed", ["relogin"], {"reason": "NEEDS_RELOGIN flag present"})
            elif not Path(self.store.session_path).exists():
                result["reason"] = "not logged in yet"
            elif not serve_alive:
                result = notice("wechat_serve_down", ["serve_down"], {"reason": "serve restart failed"})
            else:
                failed = sorted(p.name for p in Path(self.store.failed_dir).glob("*.json"))
                failed_keys = ["failed:" + name for name in failed]
                active_notices.update(failed_keys)
                eligible_failed = [name for name in failed if self._notice(notices, "failed:" + name, now)]
                if eligible_failed:
                    # An alert must not consume/revoke a message lease when
                    # its message wake will not be emitted in this round.
                    notices = {key: row for key, row in notices.items() if key in active_notices}
                    atomic_write_json(str(self.notice_path), notices)
                    return {"decision": "wake", "reason": "wechat_dead_letter",
                            "payload": {"failed_files": eligible_failed,
                                        "notice_keys": ["failed:" + name for name in eligible_failed]}}
                # Clean completed/redelivered inbox under the receiver's lock.
                candidates = []
                for path in sorted(Path(self.store.inbox_dir).glob("*.json")):
                    try:
                        mid = safe_message_id(path.stem)
                    except (ValueError, TypeError):
                        continue
                    if self._receipt(mid).get("status") == "complete" or mid in seen:
                        self._complete([mid], claims)
                        continue
                    candidates.append((mid, str(path)))
                present = {mid for mid, _ in candidates}
                claims = {mid: row for mid, row in claims.items() if mid in present}
                rw = {mid: row for mid, row in rw.items() if mid in present}
                for path in self.started.glob("*.started"):
                    if path.stem not in present:
                        path.unlink()
                new, rewoken, stuck = [], [], []
                lease = uuid.uuid4().hex
                selected = []
                for mid, path in candidates:
                    coverage = self.covered(mid)
                    if coverage and coverage["status"] in ("pending", "sending", "sent", "queued"):
                        self._complete([mid], claims)
                        continue
                    if coverage and coverage["status"] in ("failed", "uncertain"):
                        # failed jobs are handled above. Uncertain publication
                        # must be reported, never counted as delivered.
                        if coverage["status"] == "uncertain":
                            stuck.append(mid + ".json")
                        continue
                    claim = claims.get(mid)
                    if claim:
                        age = now - claim["first"]
                        marker = self.started / (mid + ".started")
                        row = rw.get(mid, {"count": 0, "last": 0})
                        if age < CLAIM_STALE:
                            eligible = age >= FIRST_STALE
                            if marker.exists():
                                eligible = eligible and now - marker.stat().st_mtime >= STARTED_STALE
                            elif row["count"]:
                                eligible = eligible and now - row["last"] >= REWAKE_INTERVAL
                            if not eligible or (row["count"] and now - row["last"] < REWAKE_INTERVAL):
                                continue
                            if row["count"] >= REWAKE_MAX:
                                stuck.append(mid + ".json")
                                continue
                            rewoken.append(path)
                            rw[mid] = {"count": row["count"] + 1, "last": now}
                        else:
                            new.append(path)
                            rw.pop(mid, None)
                    else:
                        new.append(path)
                    selected.append(mid)
                    claims[mid] = {"first": claim["first"] if claim and now - claim["first"] < CLAIM_STALE else now,
                                   "last": now, "lease": lease}
                    # Old marker cannot protect a newly assigned lease.
                    try:
                        (self.started / (mid + ".started")).unlink()
                    except FileNotFoundError:
                        pass
                active_notices.update("stuck:" + name for name in stuck)
                if selected:
                    result = {"decision": "wake", "reason": "new_wechat_message",
                              "payload": {"files": new, "rewoken_files": rewoken,
                                          "message_ids": selected, "lease": lease,
                                          "base": self.store.base, "state_dir": str(self.state),
                                          "python": sys.executable}}
                    # A busy inbox must not starve an already-stuck alert.
                    inline = notice("wechat_stuck_message", ["stuck:" + name for name in stuck],
                                    {"stuck_files": stuck}) if stuck else None
                    if inline and inline["decision"] == "wake":
                        result["payload"].update(inline["payload"])
                elif stuck:
                    result = notice("wechat_stuck_message", ["stuck:" + name for name in stuck],
                                    {"stuck_files": stuck})
                self._write_claims(claims)
                atomic_write_json(str(self.rewake_path), rw)
                # Prune only while holding the same lock as every helper append.
                seen = self._seen()
                if len(seen) > 5000:
                    atomic_write_text(str(self.seen_path), "\n".join(seen[-2000:]) + "\n")
                stamp = self.state / "wechat_sent_prune.stamp"
                try:
                    last = float(stamp.read_text())
                except (OSError, ValueError):
                    last = 0
                if now - last >= 86400:
                    for path in Path(self.store.sent_dir).glob("*.json"):
                        try:
                            if now - path.stat().st_mtime > 30 * 86400:
                                path.unlink()
                        except FileNotFoundError:
                            pass
                    atomic_write_text(str(stamp), str(now))
            notices = {key: row for key, row in notices.items() if key in active_notices}
            atomic_write_json(str(self.notice_path), notices)
            return result


def process_identity(pid):
    """Return start epoch and command tokens; no PID-only fallback to kill."""
    if not isinstance(pid, int) or pid <= 1:
        return None
    try:
        os.kill(pid, 0)
        raw = Path("/proc/%d/cmdline" % pid)
        if raw.exists():
            tokens = [s.decode(errors="replace") for s in raw.read_bytes().split(b"\0") if s]
            stat = Path("/proc/%d/stat" % pid).read_text().rsplit(")", 1)[1].split()
            boot = next(int(line.split()[1]) for line in Path("/proc/stat").read_text().splitlines() if line.startswith("btime "))
            start = boot + int(stat[19]) / os.sysconf("SC_CLK_TCK")
        else:
            # Used by portable offline tests / macOS; production Muse is Linux.
            import datetime
            import shlex
            env = dict(os.environ, LC_ALL="C")
            command = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True, env=env).strip()
            tokens = shlex.split(command)
            started = subprocess.check_output(["ps", "-p", str(pid), "-o", "lstart="], text=True, env=env).strip()
            start = datetime.datetime.strptime(started, "%a %b %d %H:%M:%S %Y").timestamp()
        if not is_serve_command(tokens):
            return None
        return {"pid": pid, "start": start, "tokens": tokens}
    except (OSError, ValueError, IndexError, StopIteration, subprocess.SubprocessError):
        return None


def is_serve_command(tokens):
    """Check command positions, not an arbitrary argument containing a name."""
    if not tokens:
        return False
    if Path(tokens[0]).name == "wechat-bridge":
        return len(tokens) > 1 and tokens[1] == "serve"
    python = Path(tokens[0]).name.lower().startswith(("python", "pypy"))
    if not python:
        return False
    if len(tokens) > 2 and Path(tokens[1]).name == "wechat-bridge":
        return tokens[2] == "serve"
    return (len(tokens) > 3 and tokens[1] == "-m" and
            tokens[2] in ("wechat_bridge", "wechat_bridge.__main__") and tokens[3] == "serve")


def supervise(base, executable, clock=time.time, identity=process_identity, sleep=time.sleep):
    store = Store(base)
    try:
        pid = int(Path(store.pid_path).read_text().strip())
    except (OSError, ValueError):
        pid = 0
    before = identity(pid)
    stale = False
    now = clock()
    if before:
        try:
            heartbeat = float(Path(store.flusher_hb_path).read_text().strip())
        except (OSError, ValueError):
            heartbeat = 0
        # A previous process's heartbeat cannot condemn its replacement during
        # startup. Heartbeat age only applies after it belongs to this process.
        stale = (now - heartbeat > 120 if heartbeat >= before["start"]
                 else now - before["start"] > 180)
    if stale and identity(pid) == before:
        os.kill(pid, signal.SIGTERM)
        sleep(1)
        if identity(pid) == before:
            os.kill(pid, signal.SIGKILL)
        before = None
    if before:
        return {"alive": True, "restarted": False}
    log_path = Path(store.state_dir) / "serve.log"
    try:
        if log_path.exists() and log_path.stat().st_size > 5242880:
            os.replace(str(log_path), str(log_path) + ".1")
        env = dict(os.environ, WECHAT_BRIDGE_DIR=base, TZ="Asia/Shanghai")
        with open(log_path, "ab") as log:
            subprocess.Popen([executable, "serve"], stdin=subprocess.DEVNULL,
                             stdout=log, stderr=log, env=env,
                             start_new_session=True, close_fds=True)
        sleep(2)
        new_pid = int(Path(store.pid_path).read_text().strip())
        return {"alive": bool(identity(new_pid)), "restarted": True}
    except (OSError, ValueError):
        return {"alive": False, "restarted": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default=os.environ.get("WECHAT_BRIDGE_DIR", os.path.expanduser("~/workspace/wechat-bridge")))
    parser.add_argument("--state-dir", default=os.environ.get("WECHAT_HOOK_STATE_DIR", os.path.expanduser("~/hooks/state")))
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("claim", "adopt", "complete"):
        p = sub.add_parser(name)
        p.add_argument("ids", nargs="+")
        p.add_argument("--lease", required=name != "claim")
    p = sub.add_parser("enqueue-reply")
    p.add_argument("--ids", nargs="+", required=True)
    for name in ("to", "context", "lease"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--text-file")
    p.add_argument("--media-path")
    p = sub.add_parser("plan")
    p.add_argument("--serve-down", action="store_true")
    p = sub.add_parser("notice-result")
    p.add_argument("--keys", nargs="*", default=[])
    p.add_argument("--accepted", action="store_true")
    sub.add_parser("prune-seen")
    p = sub.add_parser("recover-prepared")
    p.add_argument("job_name")
    p = sub.add_parser("supervise")
    p.add_argument("--executable", required=True)
    p = sub.add_parser("lock-run")
    p.add_argument("--lock", required=True)
    p.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command == "lock-run":
        with file_lock(args.lock, nonblocking=True) as acquired:
            if not acquired:
                print(json.dumps({"decision": "silent", "reason": "another hook instance is running", "payload": {}}))
                return 0
            command = args.args[1:] if args.args[:1] == ["--"] else args.args
            return subprocess.call(command, env=dict(os.environ, WECHAT_HOOK_LOCKED="1"))
    if args.command == "supervise":
        print(json.dumps(supervise(args.base, args.executable)))
        return 0
    state = WorkerState(args.base, args.state_dir)
    if args.command == "claim":
        result = state.claim(args.ids, args.lease)
    elif args.command == "adopt":
        result = state.adopt(args.ids, args.lease)
    elif args.command == "complete":
        result = state.complete(args.ids, args.lease)
    elif args.command == "enqueue-reply":
        result = state.enqueue_reply(args.ids, args.to, args.context,
                                     Path(args.text_file).read_text() if args.text_file else "",
                                     args.lease, args.media_path)
    elif args.command == "plan":
        result = state.plan(not args.serve_down)
    elif args.command == "notice-result":
        state.notice_result(args.keys, args.accepted)
        result = {"runtime_attempt_recorded": True, "delivery_confirmed": False}
    elif args.command == "recover-prepared":
        result = state.recover_prepared(args.job_name)
    else:
        result = state.prune_seen()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
