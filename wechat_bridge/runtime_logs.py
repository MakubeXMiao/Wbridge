"""Bounded process-log sink and detached stdout/stderr relay (POSIX).

New services write only to a pipe; the relay owns regular-file descriptors and
reopens them under a lock for every bounded append. Existing direct-file writers
can only be trimmed at supervision checks: continuous bounds require the next
controlled service start. No restart is triggered merely to change logging.
"""

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import select
import selectors
import stat
import subprocess
import sys
import threading
import time


LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUPS = 2
EVENT_MAX_BYTES = 256 * 1024
EVENT_BACKUPS = 1
READ_CHUNK = 64 * 1024


class BoundedLogSink:
    """Best-effort bytes sink; all cooperative writers share one lock.

    Current + backups never exceed (backups + 1) * max_bytes after a successful
    operation. No temporary copies are created. Noncooperating legacy writers
    may grow the current inode again after maintenance returns.
    """

    def __init__(self, path, max_bytes=LOG_MAX_BYTES, backups=LOG_BACKUPS):
        if (type(max_bytes) is not int or not 1 <= max_bytes <= LOG_MAX_BYTES
                or type(backups) is not int or not 0 <= backups <= LOG_BACKUPS):
            raise ValueError("invalid bounded log limits")
        self.path = os.path.abspath(os.fspath(path))
        self.max_bytes, self.backups = max_bytes, backups

    def _slot(self, index):
        return self.path if index == 0 else self.path + ".%d" % index

    @staticmethod
    def _regular(path):
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise OSError("unsafe log object")
        return True

    @contextmanager
    def _locked(self):
        # State directory already belongs to Store; never follow log links.
        parent = os.path.dirname(self.path)
        if not os.path.isdir(parent) or os.path.islink(parent):
            raise OSError("log directory unavailable")
        lock_path = self.path + ".lock"
        if os.path.lexists(lock_path):
            self._regular(lock_path)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
                raise OSError("unsafe log lock")
            os.fchmod(fd, 0o600)
            # Log contention drops this bounded append, never blocks the child
            # behind another writer's filesystem operation.
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def _trim(self, path):
        if not self._regular(path):
            return
        fd = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise OSError("unsafe log object")
            os.fchmod(fd, 0o600)
            size = info.st_size
            if size > self.max_bytes:
                # Preserve the inode for an already-open legacy O_APPEND FD.
                # A concurrent uncooperative append can be lost during trimming;
                # this is bounded log retention, never a reliable event ledger.
                tail = os.pread(fd, self.max_bytes, size - self.max_bytes)
                offset = 0
                while offset < len(tail):
                    written = os.pwrite(fd, tail[offset:], offset)
                    if written <= 0:
                        raise OSError("log trim made no progress")
                    offset += written
                os.ftruncate(fd, len(tail))
        finally:
            os.close(fd)

    def _maintain_locked(self):
        # Known historical serve.log.1/.2 are bounded too. With zero/one backup,
        # remove only these reserved slots, not arbitrary sibling files.
        for index in range(LOG_BACKUPS + 1):
            path = self._slot(index)
            if index > self.backups:
                if self._regular(path):
                    os.unlink(path)
            else:
                self._trim(path)

    def maintain(self):
        try:
            with self._locked():
                self._maintain_locked()
            return True
        except (OSError, ValueError):
            return False

    def _rotate_locked(self):
        # All files were validated before rotation. Rotation never retains an
        # open destination FD, nor a copied temporary file.
        if self.backups == 0:
            fd = os.open(self.path, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.ftruncate(fd, 0)
            finally:
                os.close(fd)
            return
        oldest = self._slot(self.backups)
        if self._regular(oldest):
            os.unlink(oldest)
        for index in range(self.backups - 1, 0, -1):
            source = self._slot(index)
            if self._regular(source):
                os.replace(source, self._slot(index + 1))
        if self._regular(self.path):
            os.replace(self.path, self._slot(1))

    def write(self, data, atomic=False):
        if not isinstance(data, bytes) or (atomic and len(data) > self.max_bytes):
            return False
        try:
            with self._locked():
                self._maintain_locked()
                if atomic:
                    size = os.stat(self.path).st_size if self._regular(self.path) else 0
                    if size and size + len(data) > self.max_bytes:
                        self._rotate_locked()
                offset = 0
                while offset < len(data):
                    size = os.stat(self.path).st_size if self._regular(self.path) else 0
                    if size >= self.max_bytes:
                        self._rotate_locked()
                        size = 0
                    chunk = data[offset:offset + min(READ_CHUNK, self.max_bytes - size)]
                    fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT |
                                 getattr(os, "O_NOFOLLOW", 0), 0o600)
                    try:
                        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
                            raise OSError("unsafe log file")
                        os.fchmod(fd, 0o600)
                        cursor = 0
                        while cursor < len(chunk):
                            written = os.write(fd, chunk[cursor:])
                            if written <= 0:
                                raise OSError("log append made no progress")
                            cursor += written
                    finally:
                        os.close(fd)
                    offset += len(chunk)
            return True
        except (OSError, ValueError):
            # Disk/rotation failure must not make the relay stop reading a pipe.
            return False


def maintain_runtime_logs(state_dir):
    """Periodic legacy trim; relay-backed services rotate continuously."""
    try:
        return BoundedLogSink(os.path.join(state_dir, "serve.log")).maintain()
    except Exception:
        return False


def append_supervision_event(state_dir, event):
    try:
        raw = (json.dumps(event, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")
        if len(raw) > 4096:
            return False
        return BoundedLogSink(os.path.join(state_dir, "serve-supervision.jsonl"),
                              max_bytes=EVENT_MAX_BYTES, backups=EVENT_BACKUPS).write(raw, atomic=True)
    except Exception:
        return False


def _write_ready(fd, record):
    if fd is None:
        return
    try:
        os.write(fd, json.dumps(record, separators=(",", ":")).encode("ascii") + b"\n")
    except OSError:
        pass
    finally:
        os.close(fd)


def run_relay(command, log_path, ready_fd=None, max_bytes=LOG_MAX_BYTES, backups=LOG_BACKUPS):
    """Spawn exactly one child; drain both output streams until its exit.

    The detached relay is the child's parent and reaps it. It never kills an
    unrelated PID or infers a provider exit signal. A short post-exit drain ends
    even when an orphan descendant inherited the pipe. A failed log write drops
    only that read chunk; no error messages are recursively written to the sink.
    """
    sink = BoundedLogSink(log_path, max_bytes=max_bytes, backups=backups)
    try:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, close_fds=True, start_new_session=True)
    except (OSError, ValueError):
        _write_ready(ready_fd, {"started": False, "child_pid": None})
        return 1
    _write_ready(ready_fd, {"started": True, "child_pid": child.pid})
    pipe = child.stdout
    selector = selectors.DefaultSelector()
    selector.register(pipe, selectors.EVENT_READ)
    exited_at = None
    try:
        while True:
            status = child.poll()
            if status is not None and exited_at is None:
                exited_at = time.monotonic()
            if exited_at is not None and time.monotonic() - exited_at >= 1:
                break
            ready = selector.select(timeout=0.2)
            if ready:
                try:
                    chunk = os.read(pipe.fileno(), READ_CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
                sink.write(chunk)
            elif status is not None:
                break
        # EOF can arrive before poll observes the exit. A child that deliberately
        # closes stdout is still owned by its relay; waiting uses no log buffer.
        exit_status = child.wait()
        # This is direct wait() evidence for this exact child. Supervision's
        # later PID observation cannot reconstruct it and remains "unknown".
        append_supervision_event(os.path.dirname(os.path.abspath(log_path)), {
            "event": "serve_supervision", "schema_version": 1, "at": time.time(),
            "reason": "child_exit", "action": "observe", "result": "stopped",
            "old_pid": child.pid, "old_start": None, "new_pid": None, "new_start": None,
            "heartbeat_at": None, "heartbeat_age_seconds": None,
            "exit_code": exit_status if exit_status >= 0 else "unknown",
            "exit_signal": -exit_status if exit_status < 0 else "unknown"})
        return 0
    finally:
        selector.close()
        pipe.close()


def launch_logged_service(executable, base, env):
    """Launch a detached relay, returning a bounded spawn acknowledgement.

    The service still writes its own serve.pid and heartbeat. Its identity is
    independently checked by supervise; a relay ACK is not service readiness.
    """
    read_fd, write_fd = os.pipe()
    try:
        command = [sys.executable, str(Path(__file__).resolve()), "--relay",
                   "--log-path", os.path.join(base, "state", "serve.log"),
                   "--ready-fd", str(write_fd), "--", executable, "serve"]
        relay = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, env=env, pass_fds=(write_fd,),
                                 close_fds=True, start_new_session=True)
        # Library callers may remain alive after supervise returns. Reap the
        # detached relay without blocking them or abandoning a Popen handle.
        threading.Thread(target=relay.wait, name="wechat-log-relay-reaper", daemon=True).start()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    os.close(write_fd)
    result = {"started": None, "relay_pid": relay.pid, "child_pid": None}
    try:
        ready, _, _ = select.select([read_fd], [], [], 2)
        if ready:
            raw = os.read(read_fd, 1024)
            record = json.loads(raw.decode("ascii"))
            if (record.get("started") is True and type(record.get("child_pid")) is int
                    and record["child_pid"] > 1):
                result.update(started=True, child_pid=record["child_pid"])
            elif record.get("started") is False:
                result["started"] = False
    except (OSError, ValueError, UnicodeError):
        pass
    finally:
        os.close(read_fd)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relay", action="store_true", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--ready-fd", type=int)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        return 1
    return run_relay(command, args.log_path, ready_fd=args.ready_fd)


if __name__ == "__main__":
    sys.exit(main())
