import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import serve
from wechat_bridge.media_diagnostics import sanitize_media_diagnostics as clean
from wechat_bridge.store import Store, load_json


def entry(index=0, source="param", outcome="failed", size=None, reason="unknown"):
    return {"item_index": index, "source": source, "outcome": outcome,
            "downloaded_bytes": size, "reason": reason}


def box(items, **kw):
    result = {"version": 1, "items": items, "truncated": False}
    result.update(kw)
    return result


class DiagnosticsTests(unittest.TestCase):
    def test_old_null_invalid_and_unknown_containers_are_omitted(self):
        for value in (None, [], {}, box([], version=True), box([], version=2), box([], truncated=0)):
            self.assertIsNone(clean(value))

    def test_bad_types_limits_and_sensitive_extras_are_dropped(self):
        good = entry(size=50380)
        good["url"] = "PRIVATE_URL"
        good["context"] = {"body": "PRIVATE_BODY"}
        good["key"] = object()
        cases = [entry(size=True), entry(size=1.0), entry(size=-1), entry(size=104857634),
                 entry(index=True), entry(index=1000), entry(source="PRIVATE_URL"),
                 entry(reason="x" * 100000), entry(outcome="success"), None]
        result = clean(box([good] + cases, raw_key="PRIVATE_KEY"))
        self.assertEqual(result["items"], [entry(size=50380)])
        self.assertTrue(result["truncated"])
        self.assertNotIn("PRIVATE", json.dumps(result))

    def test_two_source_and_16_32_byte_budgets_are_deterministic(self):
        items = [entry(0), entry(0), entry(0, "full_url"), entry(0, "unknown")]
        items += [entry(i) for i in range(1, 100)]
        result = clean(box(items))
        self.assertEqual(len(result["items"]), 16)
        self.assertEqual([x["source"] for x in result["items"][:2]], ["param", "full_url"])
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(json.dumps(result, separators=(",", ":")).encode()), 4096)

    def test_receiver_mixed_failures_and_secondary_success_keep_business_fields(self):
        with tempfile.TemporaryDirectory(prefix="diagnostics-synthetic-") as root:
            store = Store(root)
            def download(media, key, dest, diagnostics=None, **kw):
                diagnostics.append({k: v for k, v in entry(size=99).items() if k != "item_index"})
                if media.get("fail"):
                    raise ValueError("PRIVATE_URL")
                diagnostics.append({k: v for k, v in entry(source="full_url", outcome="success", size=128,
                                                         reason="ok").items() if k != "item_index"})
                Path(dest).parent.mkdir(parents=True, exist_ok=True)
                Path(dest).write_bytes(b"synthetic")
            msg = {"message_id": "synthetic1", "from_user_id": "owner", "message_type": 1,
                   "media_diagnostics": {"PRIVATE": "UPSTREAM"}, "item_list": [
                {"type": 1, "text_item": {"text": "useful synthetic text"}},
                {"type": 2, "image_item": {"media": {}}},
                {"type": 2, "image_item": {"media": {"fail": True}}}]}
            with patch.object(serve, "download_media", download), contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(serve.handle_incoming(store, msg, "owner"))
            record = load_json(str(Path(root) / "inbox" / "synthetic1.json"))
            self.assertEqual(record["text"], "useful synthetic text")
            self.assertEqual(len(record["media_paths"]), 1)
            self.assertEqual(record["media_failed_count"], 1)
            self.assertEqual(len(record["media_diagnostics"]["items"]), 3)
            self.assertNotIn("PRIVATE", json.dumps(record["media_diagnostics"]))
            store.close()

    def test_optional_bad_diagnostic_does_not_reject_valid_text(self):
        with tempfile.TemporaryDirectory(prefix="diagnostics-synthetic-") as root:
            store = Store(root)
            msg = {"message_id": "synthetic2", "from_user_id": "owner"}
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(serve._write_inbox_record(store, msg, "owner", "valid", media_diagnostics={"bad": True}))
            record = load_json(str(Path(root) / "inbox" / "synthetic2.json"))
            self.assertEqual(record["text"], "valid")
            self.assertNotIn("media_diagnostics", record)
            store.close()


if __name__ == "__main__":
    unittest.main()
