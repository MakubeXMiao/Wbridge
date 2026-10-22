"""Offline timing observations must not affect delivery or expose content."""
import contextlib
import io
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import ilink, serve
from wechat_bridge.store import Store, atomic_write_json, load_json


class LatencyTraceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wechat-trace-")
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name)
        self.secret_body = "SYNTHETIC-PRIVATE-REPLY"
        self.secret_context = "SYNTHETIC-PRIVATE-CONTEXT"
        self.secret_lease = "SYNTHETIC-PRIVATE-LEASE"
        self.calls = []
        for name in ("connect", "connect_ex"):
            p = patch.object(socket.socket, name, side_effect=AssertionError("real network forbidden"))
            p.start()
            self.addCleanup(p.stop)
        for p in (patch.object(socket, "getaddrinfo", side_effect=AssertionError("real network forbidden")),
                  patch.object(socket, "create_connection", side_effect=AssertionError("real network forbidden")),
                  patch.object(ilink, "http_json", self.fake_http), patch.object(serve, "PACE_SECONDS", 0)):
            p.start()
            self.addCleanup(p.stop)

    def fake_http(self, method, url, headers, body=None, **kwargs):
        self.calls.append(body["msg"])
        return {"message_id": len(self.calls)}

    def put(self, **extras):
        now = time.time()
        job = dict(message_id="m2", covers_message_ids=["m1", "m2"], to_user_id="owner",
                   text=self.secret_body, context_token=self.secret_context, lease=self.secret_lease,
                   client_key="worker:m2", received_at=now - 12, worker_started_at=now - 8,
                   enqueued_at=now - 1, **extras)
        atomic_write_json(Path(self.store.pending_dir) / "m2.json", job)

    def flush(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            serve.flush_outbox(self.store, "https://api.invalid", "FAKE-PRIVATE-TOKEN")
        logs = output.getvalue()
        traces = [json.loads(line.split("latency ", 1)[1])
                  for line in logs.splitlines() if "latency " in line]
        return logs, traces

    def test_text_trace_correlates_existing_stages_without_private_data(self):
        self.put()
        logs, traces = self.flush()
        self.assertEqual([item["stage"] for item in traces], ["send_started", "api_accepted"])
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(Path(self.store.sent_dir, "m2.json").exists())
        for trace in traces:
            self.assertEqual(trace["job_id"], "m2")
            self.assertEqual(trace["message_ids"], ["m1", "m2"])
            self.assertEqual(trace["count"], 2)
            self.assertGreaterEqual(trace["since_received_ms"], 12000)
            self.assertGreaterEqual(trace["since_enqueued_ms"], 1000)
            self.assertIn("worker_started_at", trace)
            self.assertGreaterEqual(trace["since_worker_started_ms"], 8000)
        self.assertEqual(traces[1]["chunks_done"], 1)
        self.assertEqual(traces[1]["chunks_total"], 1)
        for secret in (self.secret_body, self.secret_context, self.secret_lease, "FAKE-PRIVATE-TOKEN", "worker:m2"):
            self.assertNotIn(secret, logs)

    def test_multichunk_acknowledgements_are_per_chunk_not_phone_receipt(self):
        self.put()
        path = Path(self.store.pending_dir) / "m2.json"
        job = load_json(path)
        job["text"] = "X" * 4001
        atomic_write_json(path, job)
        _, traces = self.flush()
        self.assertEqual([t["stage"] for t in traces], ["send_started"] + ["api_accepted"] * 3)
        self.assertEqual([t["chunks_done"] for t in traces[1:]], [1, 2, 3])
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(any(t["stage"] == "phone_received" for t in traces))

    def test_rejected_api_has_no_success_trace(self):
        self.put()
        with patch.object(ilink, "http_json", return_value={"ret": -2}):
            _, traces = self.flush()
        self.assertEqual([t["stage"] for t in traces], ["send_started"])
        self.assertTrue(Path(self.store.pending_dir, "m2.json").exists())

    def test_media_trace_preserves_existing_delivery_calls(self):
        path = Path(self.store.base) / "image.png"
        path.write_bytes(b"synthetic")
        self.put(media_path=str(path), caption_split=True)
        job_path = Path(self.store.pending_dir) / "m2.json"
        job = load_json(job_path)
        job["text"] = ""
        atomic_write_json(job_path, job)
        with patch.object(serve, "send_media_message", return_value={"message_id": 1}) as send:
            _, traces = self.flush()
        self.assertEqual(send.call_count, 1)
        self.assertEqual([t["stage"] for t in traces], ["send_started", "api_accepted"])

    def test_exhausted_budget_and_already_confirmed_text_emit_no_fake_api_stage(self):
        self.put()
        self.store._token_send_counts = {self.secret_context: serve.TOKEN_SEND_CAP}
        _, traces = self.flush()
        self.assertEqual(traces, [])
        self.assertEqual(self.calls, [])
        self.store._token_send_counts = {}
        path = Path(self.store.pending_dir) / "m2.json"
        job = load_json(path)
        job["chunks_done"] = 1
        atomic_write_json(path, job)
        _, traces = self.flush()
        self.assertEqual(traces, [])
        self.assertEqual(self.calls, [])

    def test_observation_failure_does_not_change_delivery(self):
        self.put()
        original_log = serve.log
        def broken_trace(message, *args, **kwargs):
            if message.startswith("latency "):
                raise OSError("synthetic observation sink failure")
            return original_log(message, *args, **kwargs)
        with patch.object(serve, "log", broken_trace):
            self.flush()
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(Path(self.store.sent_dir, "m2.json").exists())

    def test_malformed_observation_metadata_is_omitted(self):
        with patch.object(serve, "log") as log:
            serve._trace_delivery("send_started", "m2.json", {
                "message_id": "m2", "covers_message_ids": "not-a-list",
                "received_at": float("inf"), "worker_started_at": True,
                "enqueued_at": self.secret_body, "context_token": self.secret_context,
                "lease": self.secret_lease, "client_key": "worker:m2",
            }, started_at=float("nan"))
        self.assertEqual(log.call_count, 1)
        trace = json.loads(log.call_args.args[0].split("latency ", 1)[1])
        self.assertEqual(set(trace), {"stage", "at", "job_id", "message_ids", "count"})
        self.assertEqual(trace["message_ids"], ["m2"])

    def test_caption_split_retains_timing_without_changing_send_order(self):
        path = Path(self.store.base) / "image.png"
        path.write_bytes(b"synthetic")
        self.put(media_path=str(path))
        with patch.object(serve, "send_media_message", return_value={"message_id": 2}) as send:
            _, traces = self.flush()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(send.call_count, 1)
        self.assertEqual([(t["job_id"], t["stage"]) for t in traces], [
            ("m2_caption", "send_started"), ("m2_caption", "api_accepted"),
            ("m2", "send_started"), ("m2", "api_accepted"),
        ])
        for trace in traces:
            self.assertIn("received_at", trace)
            self.assertIn("worker_started_at", trace)

    def test_transport_failure_keeps_retry_state_and_emits_no_ack(self):
        self.put()
        with patch.object(ilink, "http_json", side_effect=TimeoutError("synthetic timeout")):
            _, traces = self.flush()
        self.assertEqual([t["stage"] for t in traces], ["send_started"])
        queued = load_json(Path(self.store.pending_dir) / "m2.json")
        self.assertEqual(queued["attempts"], 1)
        self.assertEqual(queued["chunks_done"], 0)
        self.assertGreater(queued["next_attempt_at"], time.time())
        self.assertEqual(queued["client_key"], "worker:m2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
