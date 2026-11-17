"""Exercise urllib's CDN transport with deterministic in-memory sockets."""

from contextlib import ExitStack, contextmanager
import io
from pathlib import Path
import socket
import ssl
import sys
import unittest
from unittest.mock import patch
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import cdn_transport


OK = b"HTTP/1.1 200 OK\r\nX-Encrypted-Param: synthetic-result\r\nContent-Length: 0\r\n\r\n"


class _Socket:
    def __init__(self, *responses, send_limit=None, fail_after=None):
        self.responses = iter(responses or (OK,))
        self.send_limit, self.fail_after = send_limit, fail_after
        self.headers = []
        self.body = bytearray()
        self.body_offers = []
        self.body_timeouts = []
        self.timeout = None

    def settimeout(self, value):
        self.timeout = value

    def gettimeout(self):
        return self.timeout

    def connect(self, address):
        self.connected_to = address

    def setsockopt(self, *args):
        pass

    def sendall(self, data):
        if "send" not in self.__dict__:
            self.headers.append(bytes(data))
            return
        # Match the native TLS sendall's use of the unmodified timeout for
        # each successful send of the remaining view.
        remaining = memoryview(data)
        while remaining:
            remaining = remaining[self.send(remaining):]

    def send(self, data):
        self.body_offers.append(len(data))
        self.body_timeouts.append(self.timeout)
        if self.fail_after is not None and len(self.body) >= self.fail_after:
            raise TimeoutError("synthetic-write-secret")
        count = min(len(data), self.send_limit or len(data))
        self.body.extend(data[:count])
        return count

    def makefile(self, mode):
        self.response_timeout = self.timeout
        return io.BytesIO(next(self.responses))

    def close(self):
        pass


class CdnTransportTests(unittest.TestCase):
    @contextmanager
    def network(self, sockets, *, proxies=None):
        endpoint = ("192.0.2.1", 443)
        with ExitStack() as stack:
            stack.enter_context(patch.object(socket, "socket", side_effect=sockets))
            dns = stack.enter_context(patch.object(socket, "getaddrinfo", return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", endpoint),
            ]))
            stack.enter_context(patch.object(urllib.request, "getproxies", return_value=proxies or {}))
            stack.enter_context(patch.object(urllib.request, "proxy_bypass", return_value=False))
            yield dns

    def test_plain_post_keeps_headers_bytes_and_socket_timeout(self):
        sock = _Socket()
        data = b"z" * (2 * 1024 * 1024)
        rows = []
        with self.network([sock]) as dns:
            result = cdn_transport.upload("http://synthetic.invalid/upload?secret=123", data, rows.append)
        dns.assert_called_once_with("synthetic.invalid", 80, 0, socket.SOCK_STREAM)
        self.assertEqual(result, (200, "synthetic-result"))
        self.assertIn(b"Content-Type: application/octet-stream", sock.headers[0])
        self.assertIn(b"User-Agent: Mozilla/5.0", sock.headers[0])
        self.assertIn(b"Content-Length: 2097152", sock.headers[0])
        self.assertEqual(sock.body, data)
        self.assertTrue(all(0 < timeout <= 60 for timeout in sock.body_timeouts))
        self.assertEqual(sock.response_timeout, 60)
        self.assertEqual([row["phase"] for row in rows if row["outcome"] == "success"],
                         ["dns", "tcp", "request_headers", "write", "response_headers"])
        self.assertEqual([row["bytes_written"] for row in rows if row["outcome"] == "progress"],
                         [1024 * 1024, 2 * 1024 * 1024])
        self.assertNotIn("secret=123", repr(rows))

    def test_tls_proxy_bounds_writes_even_when_native_send_accepts_whole_view(self):
        tunnel = b"HTTP/1.1 200 Connection established\r\n\r\n"
        sock = _Socket(tunnel, OK)
        rows = []
        data = b"z" * (4 * 1024 * 1024)
        with self.network([sock], proxies={"https": "http://proxy.invalid:8080"}) as dns, \
                patch.object(ssl.SSLContext, "wrap_socket", return_value=sock) as handshake:
            self.assertEqual(cdn_transport.upload("https://synthetic.invalid/upload", data, rows.append),
                             (200, "synthetic-result"))
        dns.assert_called_once_with("proxy.invalid", 8080, 0, socket.SOCK_STREAM)
        handshake.assert_called_once_with(sock, server_hostname="synthetic.invalid")
        self.assertTrue(sock.headers[0].startswith(b"CONNECT synthetic.invalid:443 "))
        self.assertTrue(sock.headers[1].startswith(b"POST /upload "))
        self.assertEqual(sock.body, data)
        self.assertGreater(len(sock.body_offers), 1)
        self.assertLessEqual(max(sock.body_offers), cdn_transport._WRITE_SLICE)
        self.assertTrue(all(0 < timeout <= 60 for timeout in sock.body_timeouts))
        self.assertEqual(sock.response_timeout, 60)
        self.assertEqual([row["phase"] for row in rows if row["outcome"] == "success"],
                         ["dns", "tcp", "proxy", "tls", "request_headers", "write", "response_headers"])
        progress = [row["bytes_written"] for row in rows if row["outcome"] == "progress"]
        self.assertTrue(any(0 < count < len(data) for count in progress))
        self.assertEqual(progress, sorted(set(progress)))
        self.assertEqual(rows[-1]["bytes_written"], len(data))
        self.assertNotIn("send", sock.__dict__)

    def test_tls_shares_write_deadline_and_restores_socket_timeout_on_error(self):
        now = 0

        class SlowSocket(_Socket):
            def send(self, data):
                nonlocal now
                if self.timeout < 31:
                    self.body_offers.append(len(data))
                    self.body_timeouts.append(self.timeout)
                    now += self.timeout
                    raise TimeoutError("synthetic TLS write secret")
                now += 31
                return super().send(data)

        sock = SlowSocket()
        data = b"z" * (3 * cdn_transport._WRITE_SLICE)
        rows = []
        with self.network([sock]), patch.object(ssl.SSLContext, "wrap_socket", return_value=sock), \
                patch.object(cdn_transport.time, "monotonic", side_effect=lambda: now), \
                self.assertRaises(cdn_transport.UploadError):
            cdn_transport.upload("https://synthetic.invalid/upload", data, rows.append)
        self.assertEqual(sock.body_timeouts, [60, 29])
        self.assertEqual(sock.body, data[:cdn_transport._WRITE_SLICE])
        self.assertEqual(sock.gettimeout(), 60)
        self.assertEqual((rows[-1]["phase"], rows[-1]["outcome"], rows[-1]["error"]),
                         ("write", "failed", "timeout"))
        self.assertEqual(rows[-1]["bytes_written"], cdn_transport._WRITE_SLICE)
        self.assertNotIn("synthetic TLS write secret", repr(rows))

    def test_trial_tls_successful_writes_renew_idle_deadline_before_reporting(self):
        now = 0

        class SlowSocket(_Socket):
            def send(self, data):
                nonlocal now
                now += 31
                return super().send(data)

        sock = SlowSocket()
        data = b"z" * (3 * cdn_transport._WRITE_SLICE)
        events = []

        def report(row):
            if row["phase"] == "write" and row["outcome"] == "progress":
                events.append(("bytes", row["bytes_written"]))

        with self.network([sock]), patch.object(ssl.SSLContext, "wrap_socket", return_value=sock), \
                patch.object(cdn_transport.time, "monotonic", side_effect=lambda: now), \
                patch.object(cdn_transport.time, "time", side_effect=lambda: 1000 + now):
            self.assertEqual(cdn_transport.upload(
                "https://synthetic.invalid/upload", data, report, write_idle_timeout=60,
                on_write_state=lambda mono, wall: events.append(("deadline", mono, wall))),
                (200, "synthetic-result"))

        chunk = cdn_transport._WRITE_SLICE
        self.assertEqual(events, [
            ("deadline", 60, 1060),
            ("deadline", 91, 1091), ("bytes", chunk),
            ("deadline", 122, 1122), ("bytes", 2 * chunk),
            ("deadline", 153, 1153), ("bytes", 3 * chunk),
            ("deadline", None, None),
        ])
        self.assertEqual(now, 93)
        self.assertEqual(sock.body, data)
        self.assertEqual(sock.body_timeouts, [60, 60, 60])
        self.assertEqual(sock.response_timeout, 60)

    def test_trial_tls_no_confirmed_progress_times_out_without_extending_idle(self):
        now = 0

        class StalledSocket(_Socket):
            def send(self, data):
                nonlocal now
                if not self.body:
                    now += 10
                    return super().send(data)
                self.body_timeouts.append(self.timeout)
                now += self.timeout
                raise socket.timeout("synthetic TLS idle secret")

        sock = StalledSocket()
        chunk = cdn_transport._WRITE_SLICE
        data = b"z" * (2 * chunk)
        rows = []
        deadlines = []
        with self.network([sock]), patch.object(ssl.SSLContext, "wrap_socket", return_value=sock), \
                patch.object(cdn_transport.time, "monotonic", side_effect=lambda: now), \
                patch.object(cdn_transport.time, "time", side_effect=lambda: 1000 + now), \
                self.assertRaises(cdn_transport.WriteIdleTimeout) as raised:
            cdn_transport.upload("https://synthetic.invalid/upload", data, rows.append,
                                 write_idle_timeout=60,
                                 on_write_state=lambda mono, wall: deadlines.append((mono, wall)))

        self.assertEqual(deadlines, [(60, 1060), (70, 1070), (None, None)])
        self.assertEqual(sock.body_timeouts, [60, 60])
        self.assertEqual(now, 70)
        self.assertEqual(sock.body, data[:chunk])
        self.assertEqual(sock.gettimeout(), 60)
        self.assertFalse(hasattr(sock, "response_timeout"))
        self.assertEqual((rows[-1]["phase"], rows[-1]["outcome"], rows[-1]["error"]),
                         ("write", "failed", "timeout"))
        self.assertEqual(rows[-1]["bytes_written"], chunk)
        self.assertNotIn("synthetic TLS idle secret", repr(rows) + str(raised.exception))

    def test_trial_tls_late_return_after_host_resume_does_not_renew_idle(self):
        now, wall = 0, 1000

        class ResumingSocket(_Socket):
            def send(self, data):
                nonlocal now, wall
                now += 1
                wall += 61
                return super().send(data)

        sock = ResumingSocket()
        chunk = cdn_transport._WRITE_SLICE
        rows, deadlines = [], []
        with self.network([sock]), patch.object(ssl.SSLContext, "wrap_socket", return_value=sock), \
                patch.object(cdn_transport.time, "monotonic", side_effect=lambda: now), \
                patch.object(cdn_transport.time, "time", side_effect=lambda: wall), \
                self.assertRaises(cdn_transport.WriteIdleTimeout):
            cdn_transport.upload("https://synthetic.invalid/upload", b"z" * (2 * chunk), rows.append,
                                 write_idle_timeout=60,
                                 on_write_state=lambda mono, real: deadlines.append((mono, real)))
        self.assertEqual(deadlines, [(60, 1060), (None, None)])
        self.assertEqual(sock.body_offers, [chunk])
        self.assertEqual(sock.gettimeout(), 60)
        self.assertEqual((rows[-1]["outcome"], rows[-1]["error"], rows[-1]["bytes_written"]),
                         ("failed", "timeout", chunk))

    def test_partial_body_failure_retains_confirmed_bytes_and_redacts_error(self):
        sock = _Socket(send_limit=4, fail_after=4)
        rows = []
        with self.network([sock]), self.assertRaises(cdn_transport.UploadError) as raised:
            cdn_transport.upload("http://synthetic.invalid/upload", b"123456789", rows.append)
        self.assertIsNone(raised.exception.http_status)
        self.assertEqual(rows[-1]["phase"], "write")
        self.assertEqual(rows[-1]["outcome"], "failed")
        self.assertEqual(rows[-1]["error"], "timeout")
        self.assertEqual((rows[-1]["bytes_written"], rows[-1]["bytes_total"]), (4, 9))
        self.assertNotIn("synthetic-write-secret", repr(rows) + str(raised.exception))

    def test_response_timeout_is_after_complete_local_write(self):
        class _SilentSocket(_Socket):
            def makefile(self, mode):
                raise TimeoutError("synthetic-response-secret")

        rows = []
        with self.network([_SilentSocket()]), self.assertRaises(cdn_transport.UploadError):
            cdn_transport.upload("http://synthetic.invalid/upload", b"123456789", rows.append)
        self.assertEqual((rows[-1]["phase"], rows[-1]["error"]), ("response_headers", "timeout"))
        self.assertEqual(rows[-1]["bytes_written"], 9)
        self.assertNotIn("synthetic-response-secret", repr(rows))

    def test_dns_tcp_and_tls_faults_have_distinct_phases(self):
        class _NoConnection(_Socket):
            def connect(self, address):
                raise ConnectionRefusedError("synthetic-address")

        faults = [
            ("dns", "dns", patch.object(socket, "getaddrinfo", side_effect=socket.gaierror("synthetic-host"))),
            ("tcp", "connection", patch.object(socket, "socket", return_value=_NoConnection())),
            ("tls", "tls", patch.object(ssl.SSLContext, "wrap_socket", side_effect=ssl.SSLError("synthetic-cert"))),
        ]
        for phase, category, fault in faults:
            with self.subTest(phase=phase):
                rows = []
                with self.network([_Socket()]), fault, self.assertRaises(cdn_transport.UploadError):
                    cdn_transport.upload("https://synthetic.invalid/upload", b"body", rows.append)
                self.assertEqual((rows[-1]["phase"], rows[-1]["outcome"], rows[-1]["error"]),
                                 (phase, "failed", category))
                self.assertEqual(rows[-1]["bytes_written"], 0)
                self.assertNotIn("synthetic", repr(rows))

    def test_unexpected_tls_runtime_error_is_not_a_normal_network_terminal(self):
        rows = []
        with self.network([_Socket()]), patch.object(ssl.SSLContext, "wrap_socket", side_effect=RuntimeError), \
                self.assertRaises(RuntimeError) as raised:
            cdn_transport.upload("https://synthetic.invalid/upload", b"body", rows.append)
        self.assertNotIsInstance(raised.exception, cdn_transport.UploadError)
        self.assertEqual((rows[-1]["phase"], rows[-1]["outcome"], rows[-1]["error"]),
                         ("tls", "failed", "other"))

    def test_standard_redirect_and_http_error_status(self):
        redirect = b"HTTP/1.1 302 Found\r\nLocation: /other\r\nContent-Length: 0\r\n\r\n"
        fail = b"HTTP/1.1 500 Private upstream detail\r\nContent-Length: 0\r\n\r\n"
        first, second = _Socket(redirect), _Socket(fail)
        rows = []
        with self.network([first, second]), self.assertRaises(cdn_transport.UploadError) as raised:
            cdn_transport.upload("http://synthetic.invalid/upload", b"body", rows.append)
        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue(first.headers[0].startswith(b"POST /upload "))
        self.assertTrue(second.headers[0].startswith(b"GET /other "))
        self.assertEqual(first.body, b"body")
        self.assertEqual(second.body, b"")
        self.assertNotIn("Private upstream detail", repr(rows) + str(raised.exception))


if __name__ == "__main__":
    unittest.main()
