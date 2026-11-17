"""Observe the worker's existing urllib CDN request without recording secrets."""

from contextlib import contextmanager
from functools import partial
import http.client
import socket
import ssl
import time
import urllib.error
import urllib.request


SOCKET_TIMEOUT = 60
_WRITE_SLICE = 256 * 1024
_PROGRESS_BYTES = 1024 * 1024


class UploadError(RuntimeError):
    def __init__(self, status=None):
        self.http_status = status if type(status) is int and 100 <= status <= 599 else None
        super().__init__("cdn_upload_failed")


class WriteIdleTimeout(TimeoutError):
    def __init__(self):
        super().__init__("cdn_write_idle_timeout")


def _error_kind(error):
    if isinstance(error, urllib.error.URLError) and isinstance(error.reason, Exception):
        error = error.reason
    if isinstance(error, (TimeoutError, socket.timeout)):
        return "timeout"
    if isinstance(error, socket.gaierror):
        return "dns"
    if isinstance(error, ssl.SSLError):
        return "tls"
    if isinstance(error, ConnectionError):
        return "connection"
    if isinstance(error, http.client.HTTPException):
        return "http_protocol"
    if isinstance(error, OSError):
        return "io"
    return "other"


class _Trace:
    def __init__(self, total, report):
        self.total = total
        self.written = self.last_written = 0
        self.last_progress = time.monotonic()
        self.report = report
        self.failed = False

    def emit(self, phase, outcome, started, error=None):
        self.report({"phase": phase, "outcome": outcome,
                     "elapsed_ms": int(max(0, time.monotonic() - started) * 1000),
                     "bytes_written": self.written, "bytes_total": self.total,
                     "error": error})

    @contextmanager
    def phase(self, name):
        started = time.monotonic()
        self.emit(name, "started", started)
        try:
            yield started
        except Exception as error:
            self.failed = True
            self.emit(name, "failed", started, _error_kind(error))
            raise
        else:
            self.emit(name, "success", started)

    def write(self, count, started):
        self.written += count
        now = time.monotonic()
        if (self.written - self.last_written >= _PROGRESS_BYTES
                or now - self.last_progress >= 1):
            self.last_written, self.last_progress = self.written, now
            self.emit("write", "progress", started)


class _Connection:
    def __init__(self, *args, trace, write_idle_timeout, on_write_state, **kwargs):
        super().__init__(*args, **kwargs)
        self._trace = trace
        self._write_idle_timeout = write_idle_timeout
        self._on_write_state = on_write_state
        self._create_connection = self._dial
        self._request_body = None
        self._in_tunnel = False

    def _dial(self, address, timeout, source_address):
        # Match socket.create_connection's address order and per-socket timeout.
        with self._trace.phase("dns"):
            addresses = socket.getaddrinfo(*address, 0, socket.SOCK_STREAM)
        with self._trace.phase("tcp"):
            last_error = None
            for family, kind, protocol, _, endpoint in addresses:
                candidate = None
                try:
                    candidate = socket.socket(family, kind, protocol)
                    candidate.settimeout(timeout)
                    if source_address:
                        candidate.bind(source_address)
                    candidate.connect(endpoint)
                    return candidate
                except OSError as error:
                    last_error = error
                    if candidate is not None:
                        candidate.close()
            if last_error is not None:
                raise last_error
            raise OSError("no CDN addresses")

    def _tunnel(self):
        self._in_tunnel = True
        try:
            with self._trace.phase("proxy"):
                return super()._tunnel()
        finally:
            self._in_tunnel = False

    def _send_output(self, message_body=None, encode_chunked=False):
        self._request_body = message_body
        try:
            return super()._send_output(message_body, encode_chunked)
        finally:
            self._request_body = None

    def send(self, data):
        if self._in_tunnel:
            return super().send(data)
        if self.sock is None:
            self.connect()
        if data is self._request_body:
            idle_timeout = self._write_idle_timeout
            idle_bounds = None
            if idle_timeout is not None:
                idle_bounds = time.monotonic() + idle_timeout, time.time() + idle_timeout
            watch = self._on_write_state if idle_bounds is not None else None
            try:
                if watch is not None:
                    watch(*idle_bounds)
                with self._trace.phase("write") as started:
                    self._write_body(data, started, idle_bounds, watch)
            finally:
                if watch is not None:
                    watch(None, None)
            return
        with self._trace.phase("request_headers"):
            return super().send(data)

    def _write_body(self, data, started, idle_bounds, watch):
        timeout = self.sock.gettimeout()
        deadline, wall_deadline = idle_bounds or (None, None)
        if idle_bounds is None and timeout:
            # Default and small trial uploads keep the original body budget.
            deadline = time.monotonic() + timeout
        with memoryview(data) as body:
            offset = 0
            try:
                while offset < len(body):
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if wall_deadline is not None:
                            remaining = min(remaining, wall_deadline - time.time())
                        if remaining <= 0:
                            raise TimeoutError("CDN body write timed out")
                        self.sock.settimeout(remaining)
                    count = self.sock.send(body[offset:offset + _WRITE_SLICE])
                    if count <= 0:
                        raise ConnectionError("CDN connection closed during write")
                    offset += count
                    if idle_bounds is not None:
                        confirmed_at, wall_confirmed_at = time.monotonic(), time.time()
                        if confirmed_at >= deadline or wall_confirmed_at >= wall_deadline:
                            self._trace.write(count, started)
                            raise WriteIdleTimeout()
                        deadline = confirmed_at + self._write_idle_timeout
                        wall_deadline = wall_confirmed_at + self._write_idle_timeout
                        if watch is not None:
                            watch(deadline, wall_deadline)
                    self._trace.write(count, started)
            except (TimeoutError, socket.timeout):
                if idle_bounds is not None:
                    raise WriteIdleTimeout() from None
                raise
            finally:
                if deadline is not None:
                    self.sock.settimeout(timeout)

    def getresponse(self):
        with self._trace.phase("response_headers"):
            return super().getresponse()


class _HTTPConnection(_Connection, http.client.HTTPConnection):
    pass


class _HTTPSConnection(_Connection, http.client.HTTPSConnection):
    def connect(self):
        http.client.HTTPConnection.connect(self)
        with self._trace.phase("tls"):
            self.sock = self._context.wrap_socket(
                self.sock, server_hostname=self._tunnel_host or self.host)


class _MeasuredHandler:
    def __init__(self, trace, write_idle_timeout, on_write_state):
        super().__init__()
        self._factory = partial(self._connection, trace=trace,
                                write_idle_timeout=write_idle_timeout, on_write_state=on_write_state)

    def do_open(self, http_class, req, **kwargs):
        return super().do_open(self._factory, req, **kwargs)


class _HTTPHandler(_MeasuredHandler, urllib.request.HTTPHandler):
    _connection = _HTTPConnection


class _HTTPSHandler(_MeasuredHandler, urllib.request.HTTPSHandler):
    _connection = _HTTPSConnection


def upload(url, encrypted, report, *, write_idle_timeout=None, on_write_state=None):
    """Post once with stock urllib behavior and return status and upload parameter."""
    trace = _Trace(len(encrypted), report)
    started = time.monotonic()
    status = None
    try:
        request = urllib.request.Request(url, data=encrypted, method="POST", headers={
            "Content-Type": "application/octet-stream", "User-Agent": "Mozilla/5.0",
        })
        opener = urllib.request.build_opener(
            _HTTPHandler(trace, write_idle_timeout, on_write_state),
            _HTTPSHandler(trace, write_idle_timeout, on_write_state))
        with opener.open(request, timeout=SOCKET_TIMEOUT) as response:
            status = response.status
            return status, response.headers.get("x-encrypted-param") or ""
    except urllib.error.HTTPError as error:
        try:
            error.close()
        except Exception:
            pass
        raise UploadError(error.code) from None
    except Exception as error:
        if not trace.failed:
            trace.emit("transport", "failed", started, _error_kind(error))
        reason = error.reason if isinstance(error, urllib.error.URLError) else error
        if isinstance(reason, WriteIdleTimeout):
            raise reason from None
        if trace.failed and isinstance(reason, (OSError, http.client.HTTPException)):
            raise UploadError(status) from None
        raise
