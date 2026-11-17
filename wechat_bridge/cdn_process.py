"""Run one CDN attempt in a child that holds the sender's guard until exit."""

import json
import math
import os
import selectors
import signal
import subprocess
import sys
import threading
import time

from .delivery_failures import _unique_pairs
from .ilink import MAX_MEDIA_BYTES
from .progress import OperationAborted


_BODY_LIMIT = MAX_MEDIA_BYTES + 16
_REQUEST_LIMIT = 512 * 1024
_FRAME_LIMIT = 512 * 1024
_IO_CHUNK = 64 * 1024
_CHECK_INTERVAL = 0.2
_EXIT_GRACE = 1
_TERM_GRACE = 1
_KILL_GRACE = 2
_PARENT_GONE = 70
_DEADLINE_REACHED = 71
_CHILD_FAILED = 72
_ATTEMPT_REACHED = 73
_WRITE_IDLE_REACHED = 74
_PROGRESS_FIELDS = {"phase", "outcome", "elapsed_ms", "bytes_written", "bytes_total", "error"}
_PHASES = {"dns", "tcp", "proxy", "tls", "request_headers", "write", "response_headers", "transport"}
_OUTCOMES = {"started", "progress", "success", "failed"}
_ERRORS = {"timeout", "dns", "connection", "tls", "http_protocol", "io", "other"}
_BOOTSTRAP = (
    "import runpy,sys; sys.path.insert(0,sys.argv.pop(1)); "
    "runpy.run_module('wechat_bridge.cdn_process',run_name='__main__')"
)


class CDNUploadError(RuntimeError):
    """An attempted transfer failed, with an allowlisted reason and HTTP status."""

    def __init__(self, status=None, *, reason="cdn_upload_failed", confirmed=False):
        self.http_status = status if type(status) is int and 100 <= status <= 599 else None
        self.reason = (reason if reason in ("attempt_expired", "write_idle_timeout")
                       else "cdn_upload_failed")
        self.confirmed = confirmed is True
        super().__init__(self.reason)


class CDNReapError(BaseException):
    """No caller may resume sending while the old child may hold its guards."""

    def __init__(self):
        super().__init__("cdn_worker_exit_unconfirmed")


class _WorkerProtocolError(BaseException):
    """A worker IPC fault must bypass transport error wrapping."""


def _progress(row):
    if type(row) is not dict or set(row) != _PROGRESS_FIELDS:
        raise CDNUploadError()
    phase, outcome, error = (row[key] for key in ("phase", "outcome", "error"))
    if (type(phase) is not str or phase not in _PHASES
            or type(outcome) is not str or outcome not in _OUTCOMES
            or (outcome == "failed" and (type(error) is not str or error not in _ERRORS))
            or (outcome != "failed" and error is not None)):
        raise CDNUploadError()
    elapsed, written, total = (row[key] for key in ("elapsed_ms", "bytes_written", "bytes_total"))
    if (type(elapsed) is not int or not 0 <= elapsed <= (1 << 53)
            or type(written) is not int or type(total) is not int or not 0 <= written <= total <= _BODY_LIMIT):
        raise CDNUploadError()
    return row


def _start(live_fd, guard_fds, deadline, wall_deadline, attempt_deadline, attempt_wall, write_idle_timeout):
    package_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    clock_offset = 0
    if deadline is not None or attempt_deadline is not None:
        # Python 3.9 on macOS offsets time.monotonic() per process.
        clock_offset = time.clock_gettime(time.CLOCK_MONOTONIC) - time.monotonic()
    command = [sys.executable, "-I", "-B", "-c", _BOOTSTRAP, package_root,
               str(os.getpid()), str(live_fd), "" if deadline is None else repr(deadline + clock_offset),
               "" if wall_deadline is None else repr(wall_deadline),
               "" if attempt_deadline is None else repr(attempt_deadline + clock_offset),
               "" if attempt_wall is None else repr(attempt_wall),
               "" if write_idle_timeout is None else repr(write_idle_timeout)]
    return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, pass_fds=(*guard_fds, live_fd))


def _stop(child):
    """Retain uncertainty unless wait has observed the direct child's exit."""
    try:
        if child.poll() is not None:
            return
    except (OSError, KeyboardInterrupt):
        pass
    for send_signal, grace in ((child.terminate, _TERM_GRACE), (child.kill, _KILL_GRACE)):
        try:
            send_signal()
        except (OSError, KeyboardInterrupt):
            pass
        stop_at = time.monotonic() + grace
        while True:
            try:
                child.wait(timeout=max(0, stop_at - time.monotonic()))
                return
            except (OSError, subprocess.TimeoutExpired):
                break
            except KeyboardInterrupt:
                if time.monotonic() >= stop_at:
                    break
    try:
        if child.poll() is not None:
            return
    except (OSError, KeyboardInterrupt):
        pass
    raise CDNReapError()


def _check_deadline(deadline, wall_deadline, attempt_deadline=None, attempt_wall=None):
    now = time.monotonic()
    if ((deadline is not None and now >= deadline)
            or (wall_deadline is not None and time.time() >= wall_deadline)):
        raise OperationAborted("expired")
    if ((attempt_deadline is not None and now >= attempt_deadline)
            or (attempt_wall is not None and time.time() >= attempt_wall)):
        raise CDNUploadError(reason="attempt_expired")


def _decode_frame(line, report):
    if len(line) > _FRAME_LIMIT:
        raise CDNUploadError()
    frame = json.loads(line, object_pairs_hook=_unique_pairs)
    if type(frame) is not dict:
        raise CDNUploadError()
    kind = frame.get("kind")
    if kind == "progress" and set(frame) == {"kind", "row"}:
        report(_progress(frame["row"]))
        return None
    if kind == "error" and set(frame) == {"kind", "status"}:
        status = frame["status"]
        if status is None or type(status) is int and 100 <= status <= 599:
            return status, None, None
    if (kind == "error" and set(frame) == {"kind", "status", "reason"}
            and frame["status"] is None and frame["reason"] == "write_idle_timeout"):
        return None, None, "write_idle_timeout"
    if kind == "result" and set(frame) == {"kind", "status", "param"}:
        status, param = frame["status"], frame["param"]
        if type(status) is int and 100 <= status <= 599 and type(param) is str:
            return status, param, None
    raise CDNUploadError()


def _exchange(child, header, encrypted, check, deadline, wall_deadline, attempt_deadline, attempt_wall, report):
    input_fd, output_fd = child.stdin.fileno(), child.stdout.fileno()
    os.set_blocking(input_fd, False)
    os.set_blocking(output_fd, False)
    payload = iter((memoryview(header), memoryview(encrypted)))
    pending = next(payload)
    buffer = bytearray()
    terminal = None
    next_check = 0
    with selectors.DefaultSelector() as selector:
        selector.register(input_fd, selectors.EVENT_WRITE)
        selector.register(output_fd, selectors.EVENT_READ)
        while True:
            _check_deadline(deadline, wall_deadline, attempt_deadline, attempt_wall)
            now = time.monotonic()
            if now >= next_check:
                check()
                _check_deadline(deadline, wall_deadline, attempt_deadline, attempt_wall)
                next_check = time.monotonic() + _CHECK_INTERVAL
            wake_at = min(next_check, deadline) if deadline is not None else next_check
            if attempt_deadline is not None:
                wake_at = min(wake_at, attempt_deadline)
            for key, _mask in selector.select(max(0, wake_at - time.monotonic())):
                if key.fd == input_fd:
                    try:
                        written = os.write(input_fd, pending[:_IO_CHUNK])
                    except BlockingIOError:
                        continue
                    except BrokenPipeError:
                        selector.unregister(input_fd)
                        continue
                    pending = pending[written:]
                    if not pending:
                        pending = next(payload, None)
                        if not pending:
                            selector.unregister(input_fd)
                            child.stdin.close()
                    continue
                try:
                    chunk = os.read(output_fd, _IO_CHUNK)
                except BlockingIOError:
                    continue
                if not chunk:
                    rc = child.wait(timeout=_EXIT_GRACE)
                    if rc == _DEADLINE_REACHED:
                        raise OperationAborted("expired")
                    if rc in (_ATTEMPT_REACHED, _WRITE_IDLE_REACHED):
                        _check_deadline(deadline, wall_deadline)
                        reason = "attempt_expired" if rc == _ATTEMPT_REACHED else "write_idle_timeout"
                        raise CDNUploadError(reason=reason)
                    if rc or buffer or terminal is None:
                        raise CDNUploadError()
                    check()
                    _check_deadline(deadline, wall_deadline, attempt_deadline, attempt_wall)
                    status, param, reason = terminal
                    if param is None:
                        raise CDNUploadError(status, reason=reason, confirmed=True)
                    return status, param
                buffer.extend(chunk)
                while True:
                    end = buffer.find(b"\n")
                    if end < 0:
                        if len(buffer) > _FRAME_LIMIT:
                            raise CDNUploadError()
                        break
                    if terminal is not None:
                        raise CDNUploadError()
                    terminal = _decode_frame(bytes(buffer[:end]), report)
                    del buffer[:end + 1]


def upload(url, encrypted, *, check, deadline, guard_fds, report, wall_deadline=None,
           attempt_deadline=None, attempt_wall_deadline=None, write_idle_timeout=None):
    """Execute one attempt; the caller retains retry and final-POST authority."""
    if (type(url) is not str or not url or type(encrypted) is not bytes
            or len(encrypted) > _BODY_LIMIT
            or any(value is not None and (type(value) not in (int, float) or not math.isfinite(value))
                   for value in (deadline, wall_deadline, attempt_deadline, attempt_wall_deadline))
            or (write_idle_timeout is not None and
                (type(write_idle_timeout) not in (int, float) or write_idle_timeout != 60))):
        raise CDNUploadError()
    # Pair the trial's monotonic cap with a wall bound. A backward wall jump
    # cannot extend the cap, and a suspended host cannot refresh the attempt.
    attempt_wall = attempt_wall_deadline
    if attempt_wall is None and attempt_deadline is not None:
        attempt_wall = time.time() + (attempt_deadline - time.monotonic())
    _check_deadline(deadline, wall_deadline, attempt_deadline, attempt_wall)
    check()
    _check_deadline(deadline, wall_deadline, attempt_deadline, attempt_wall)
    header = json.dumps({"url": url, "size": len(encrypted)}, separators=(",", ":")).encode("ascii") + b"\n"
    if len(header) > _REQUEST_LIMIT:
        raise CDNUploadError()
    live_fd, parent_fd = os.pipe()
    try:
        try:
            child = _start(live_fd, guard_fds, deadline, wall_deadline,
                           attempt_deadline, attempt_wall, write_idle_timeout)
        finally:
            os.close(live_fd)
        try:
            return _exchange(child, header, encrypted, check, deadline, wall_deadline,
                             attempt_deadline, attempt_wall, report)
        finally:
            try:
                _stop(child)
            finally:
                for stream in (child.stdin, child.stdout):
                    try:
                        stream.close()
                    except OSError:
                        pass
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise CDNUploadError() from None
    finally:
        os.close(parent_fd)


def _worker_remaining(shared_deadline, wall_deadline, attempt_deadline, attempt_wall, write_state):
    """Exit on the first effective limit, preferring the original authority."""
    bounds = []
    if shared_deadline is not None:
        bounds.append((_DEADLINE_REACHED, shared_deadline - time.clock_gettime(time.CLOCK_MONOTONIC)))
    if wall_deadline is not None:
        bounds.append((_DEADLINE_REACHED, wall_deadline - time.time()))
    if attempt_deadline is not None:
        bounds.append((_ATTEMPT_REACHED, attempt_deadline - time.clock_gettime(time.CLOCK_MONOTONIC)))
    if attempt_wall is not None:
        bounds.append((_ATTEMPT_REACHED, attempt_wall - time.time()))
    for code, remaining in bounds:
        if remaining <= 0:
            os._exit(code)
    if write_state is not None:
        write_bounds = write_state[0]
        if write_bounds is not None:
            write_deadline, write_wall = write_bounds
            remaining = min(write_deadline - time.monotonic(), write_wall - time.time())
            # A concurrent confirmed write may have renewed the deadline
            # while the watchdog was reading the old value.
            if remaining <= 0 and write_state[0] == write_bounds:
                os._exit(_WRITE_IDLE_REACHED)
            bounds.append((_WRITE_IDLE_REACHED, max(0, remaining)))
    return min((remaining for _, remaining in bounds), default=None)


def _watch_parent(fd, shared_deadline, wall_deadline, attempt_deadline, attempt_wall, write_state, ready):
    """Independently bound networking, even when the parent is inside a callback."""
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(fd, selectors.EVENT_READ)
            ready.set()
            while True:
                wait = _worker_remaining(shared_deadline, wall_deadline, attempt_deadline, attempt_wall, write_state)
                if wall_deadline is not None or attempt_wall is not None or write_state is not None:
                    wait = _CHECK_INTERVAL if wait is None else min(wait, _CHECK_INTERVAL)
                if selector.select(wait):
                    os._exit(_PARENT_GONE)
    except BaseException:
        os._exit(_CHILD_FAILED)


def _arm_parent_watch(parent_pid, live_fd, shared_deadline, wall_deadline, attempt_deadline, attempt_wall, write_state):
    if sys.platform == "linux":
        # The kernel signal also covers a native call holding the interpreter lock.
        try:
            import ctypes
            ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0)  # PR_SET_PDEATHSIG
        except (AttributeError, OSError):
            pass  # The pipe monitor remains active on every supported host.
    if os.getppid() != parent_pid:
        os._exit(_PARENT_GONE)
    with selectors.DefaultSelector() as selector:
        selector.register(live_fd, selectors.EVENT_READ)
        if selector.select(0):
            os._exit(_PARENT_GONE)
    _worker_remaining(shared_deadline, wall_deadline, attempt_deadline, attempt_wall, None)
    ready = threading.Event()
    threading.Thread(target=_watch_parent,
                     args=(live_fd, shared_deadline, wall_deadline, attempt_deadline, attempt_wall,
                           write_state, ready),
                     daemon=True).start()
    ready.wait()


def _emit(frame):
    line = json.dumps(frame, separators=(",", ":"), allow_nan=False).encode("ascii")
    if len(line) > _FRAME_LIMIT:
        raise CDNUploadError()
    sys.stdout.buffer.write(line + b"\n")
    sys.stdout.buffer.flush()


def _worker_main():
    try:
        parent_pid, live_fd, *bounds = sys.argv[1:]
        deadline, wall_deadline, attempt_deadline, attempt_wall, write_idle_timeout = (
            float(value) if value else None for value in bounds)
        write_state = [None] if write_idle_timeout is not None else None
        _arm_parent_watch(int(parent_pid), int(live_fd), deadline, wall_deadline,
                          attempt_deadline, attempt_wall, write_state)
        line = sys.stdin.buffer.readline(_REQUEST_LIMIT + 1)
        if len(line) > _REQUEST_LIMIT or not line.endswith(b"\n"):
            raise CDNUploadError()
        header = json.loads(line, object_pairs_hook=_unique_pairs)
        if type(header) is not dict or set(header) != {"url", "size"}:
            raise CDNUploadError()
        url, size = header["url"], header["size"]
        if type(url) is not str or not url or type(size) is not int or not 0 <= size <= _BODY_LIMIT:
            raise CDNUploadError()
        encrypted = sys.stdin.buffer.read(size)
        if len(encrypted) != size:
            raise CDNUploadError()
        from .cdn_transport import UploadError, upload as transfer

        def report(row):
            try:
                _emit({"kind": "progress", "row": _progress(row)})
            except Exception as error:
                raise _WorkerProtocolError() from error

        try:
            if write_state is None:
                status, param = transfer(url, encrypted, report)
            else:
                from .cdn_transport import WriteIdleTimeout

                def on_write_state(write_deadline, write_wall):
                    write_state[0] = None if write_deadline is None else (write_deadline, write_wall)

                try:
                    status, param = transfer(url, encrypted, report, write_idle_timeout=write_idle_timeout,
                                             on_write_state=on_write_state)
                except WriteIdleTimeout:
                    _emit({"kind": "error", "status": None, "reason": "write_idle_timeout"})
                    os._exit(0)
        except UploadError as error:
            _emit({"kind": "error", "status": CDNUploadError(error.http_status).http_status})
        else:
            _emit({"kind": "result", "status": status, "param": param})
    except OperationAborted as error:
        os._exit(_DEADLINE_REACHED if error.code == "expired" else _CHILD_FAILED)
    except (Exception, _WorkerProtocolError):
        os._exit(_CHILD_FAILED)
    os._exit(0)


if __name__ == "__main__":
    _worker_main()
