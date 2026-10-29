"""003 public receive/worker integration on synthetic local stores, offline only."""
import copy
import hashlib
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.parse
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from PIL import Image
from wechat_bridge import attachment_runtime as runtime
from wechat_bridge.attachments import AttachmentStore
from wechat_bridge import cli, crypto, ilink, media, serve, worker_state
from wechat_bridge.store import Store, atomic_write_json, load_json


def deny(*args, **kwargs):
    raise AssertionError("synthetic attachment runtime tests forbid real sockets")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny

OWNER = "synthetic-owner"
KEY = b"0123456789abcdef"


def png(color="red"):
    result = io.BytesIO()
    Image.new("RGB", (3, 3), color).save(result, format="PNG")
    return result.getvalue()


class Response(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.status = 200
        self.headers = {"Content-Length": str(len(data)), "Content-Type": "application/octet-stream"}

    def geturl(self):
        return "https://synthetic.invalid/media"


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-runtime-")
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        self.base = self.home / "bridge"
        self.store = Store(str(self.base))
        self.addCleanup(self.store.close)
        self.now = time.time()
        self.worker = worker_state.WorkerState(str(self.base), str(self.home / "hook"), lambda: self.now)
        self.addCleanup(self.worker.store.close)
        notice = patch.object(cli, "RELOGIN_NOTIFIED_PATH", str(self.home / "synthetic-relogin-notice"))
        notice.start()
        self.addCleanup(notice.stop)
        self.session = {"bot_token": "synthetic-private-bot-token", "ilink_user_id": OWNER,
                        "ilink_bot_id": "synthetic-bot", "baseurl": "https://synthetic.invalid"}
        atomic_write_json(self.store.session_path, self.session)
        self.scope = runtime.initialize(self.store)
        self.counter = 0
        self.bodies = {}
        self.logs = []
        for module in (serve, media):
            logger = patch.object(module, "log", lambda message, **fields: self.logs.append(message))
            logger.start()
            self.addCleanup(logger.stop)

    def message(self, mid=None, text=None, attachments=()):
        self.counter += 1
        mid = mid or "synthetic-%04d" % self.counter
        items = []
        for index, spec in enumerate(attachments):
            kind, data, name = spec
            item_type, subkey = {"image": (media.ITEM_IMAGE, "image_item"),
                "file": (media.ITEM_FILE, "file_item"), "video": (media.ITEM_VIDEO, "video_item")}[kind]
            source = "%s-%d" % (mid, index)
            self.bodies[source] = data if isinstance(data, Exception) else crypto._aes_ecb_encrypt(data, KEY)
            sub = {"media": {"encrypt_query_param": source, "aes_key": KEY.hex(), "encrypt_type": 1}}
            if name is not None:
                sub["file_name"] = name
            items.append({"type": item_type, subkey: sub})
        if text is not None:
            items.append({"type": 1, "text_item": {"text": text}})
        return {"message_id": mid, "from_user_id": OWNER, "message_type": 1,
                "context_token": "synthetic-context-" + mid, "item_list": items}

    def transport(self, request, **kwargs):
        source = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["encrypted_query_param"][0]
        body = self.bodies[source]
        if isinstance(body, Exception):
            raise body
        return Response(body)

    def receive(self, message):
        with patch.object(media.urllib.request, "urlopen", side_effect=self.transport):
            self.assertTrue(serve.handle_incoming(self.store, message, OWNER))
        return load_json(str(self.base / "inbox" / (message["message_id"] + ".json")))

    def file(self, name="SYNTHETIC-file.pdf", body=b"SYNTHETIC original", mid=None):
        row = self.receive(self.message(mid, attachments=[("file", body, name)]))
        return row["attachment_refs"][0]["attachment_id"]

    def batch(self):
        wake = self.worker.plan()
        self.assertEqual(wake["decision"], "wake", wake)
        result = self.worker.begin(wake["payload"]["lease"])
        self.assertTrue(result["owned"], result)
        return result

    def call(self, batch, action, arguments=None, **request_fields):
        request = {"schema_version": 1, "action": action, "arguments": arguments or {}}
        request.update(request_fields)
        return self.worker.attachment_call(batch["owned"], batch["lease"], request)

    def success(self, result):
        self.assertTrue(result["ok"], result)
        self.assertIsNone(result["error"])
        self.assertEqual(result["schema_version"], 1)
        return result["result"]

    def error(self, result, code):
        self.assertFalse(result["ok"], result)
        self.assertIsNone(result["result"])
        self.assertEqual(result["error"]["code"], code)

    def complete_plain(self, batch):
        result = self.worker.reply(batch["owned"], "SYNTHETIC acknowledged", batch["lease"])
        self.assertTrue(result.get("queued"), result)

    def send_spy(self, final_result=None, after_upload=None):
        """Fake only HTTP edges; run real upload/encryption and sender code."""
        messages, originals, targets = [], [], []
        def http(method, url, headers, body, **kwargs):
            if url.endswith("/getuploadurl"):
                targets.append(body)
                return {"ret": 0, "upload_full_url": "https://synthetic.invalid/upload"}
            if not url.endswith("/sendmessage"):
                raise AssertionError("unexpected synthetic HTTP path")
            messages.append(copy.deepcopy(body["msg"]))
            if isinstance(final_result, Exception):
                raise final_result
            return final_result if final_result is not None else {"ret": 0}
        def cdn(request, **kwargs):
            if request.get_method() != "POST" or request.full_url != "https://synthetic.invalid/upload":
                raise AssertionError("unexpected synthetic CDN request")
            originals.append(crypto._aes_ecb_decrypt(request.data, bytes.fromhex(targets[-1]["aeskey"])))
            if after_upload is not None:
                after_upload()
            response = Response(b"")
            response.headers["x-encrypted-param"] = "synthetic-uploaded-param"
            return response
        patches = [patch.object(media, "http_json", side_effect=http),
                   patch.object(ilink, "http_json", side_effect=http),
                   patch.object(media.urllib.request, "urlopen", side_effect=cdn),
                   patch.object(serve, "PACE_SECONDS", 0)]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        return messages, originals

    def flush(self, store=None):
        chosen = store or self.store
        session = load_json(chosen.session_path)
        self.assertTrue(serve.flush_outbox(chosen, "https://synthetic.invalid", session["bot_token"]))

    def test_identity_bootstrap_is_stable_and_successful_login_rotates_epoch(self):
        first = load_json(self.store.session_path)
        self.assertRegex(first["login_epoch"], r"^[0-9a-f]{32}$")
        self.assertNotEqual(first["login_epoch"], hashlib.sha256(first["bot_token"].encode()).hexdigest())
        self.assertEqual(runtime.ensure_identity(self.store), first)
        relogin = dict(self.session, bot_token="synthetic-new-token")
        with patch("builtins.print"):
            self.assertTrue(cli._save_login_session(self.store, relogin))
        second = load_json(self.store.session_path)
        self.assertNotEqual(first["login_epoch"], second["login_epoch"])
        self.assertEqual(first["attachment_runtime_id"], second["attachment_runtime_id"])
        self.assertEqual(runtime.ensure_identity(self.store), second)

    def test_capability_probe_and_effective_policy_are_readonly(self):
        fresh = self.home / "not-created"
        caps = runtime.capabilities(str(fresh))
        self.assertFalse(fresh.exists())
        self.assertEqual(caps["policy"]["ambiguity_seconds"], 900)
        atomic_write_json(str(self.base / "state/attachment-policy.json"), {"ambiguity_seconds": 37})
        before = {str(p): (p.stat().st_mtime_ns, p.stat().st_size) for p in self.base.rglob("*") if p.is_file()}
        self.assertEqual(runtime.capabilities(str(self.base))["policy"]["ambiguity_seconds"], 37)
        after = {str(p): (p.stat().st_mtime_ns, p.stat().st_size) for p in self.base.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        command = [sys.executable, "-I", "-B", "-c",
            "import sys;sys.path.insert(0,sys.argv[1]);from wechat_bridge.worker_state import main;"
            "raise SystemExit(main(['--base',sys.argv[2],'--state-dir',sys.argv[3],'attachment-capabilities']))",
            str(ROOT), str(fresh), str(self.home / "not-created-hook")]
        process = subprocess.run(command, capture_output=True, text=True, timeout=10)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout)["policy"]["ambiguity_seconds"], 900)
        self.assertFalse(fresh.exists())
        self.assertFalse((self.home / "not-created-hook").exists())

    def test_receive_saves_original_name_bytes_and_replay_identity(self):
        message = self.message("synthetic-name", attachments=[("file", b"SYNTHETIC contents", "../SYNTHETIC landlord.pdf")])
        first = self.receive(message)
        repeated = self.receive(message)
        self.assertEqual(first["attachment_refs"], repeated["attachment_refs"])
        rows = runtime.catalog(self.store).query(self.scope)["items"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["original_name"], "../SYNTHETIC landlord.pdf")
        self.assertEqual(rows[0]["name_source"], "original_name")
        self.assertEqual(Path(first["media_paths"][0]).read_bytes(), b"SYNTHETIC contents")
        self.assertNotIn("landlord", Path(first["media_paths"][0]).name)

    def test_inbox_write_failure_recovers_catalog_without_redownloading_original(self):
        message = self.message("synthetic-inbox-window", attachments=[("file", b"SYNTHETIC durable", "SYNTHETIC.bin")])
        original_write = serve.atomic_write_json
        def fail_inbox(path, value):
            if Path(path).parent == Path(self.store.inbox_dir):
                raise OSError("synthetic inbox publication interruption")
            return original_write(path, value)
        with patch.object(media.urllib.request, "urlopen", side_effect=self.transport) as first_http, \
                patch.object(serve, "atomic_write_json", side_effect=fail_inbox):
            self.assertFalse(serve.handle_incoming(self.store, message, OWNER))
        self.assertEqual(first_http.call_count, 1)
        self.assertFalse((self.base / "inbox/synthetic-inbox-window.json").exists())
        saved = runtime.catalog(self.store).query(self.scope)["items"]
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["status"], "ready")
        with patch.object(media.urllib.request, "urlopen") as replay_http:
            self.assertTrue(serve.handle_incoming(self.store, message, OWNER))
        replay_http.assert_not_called()
        row = load_json(str(self.base / "inbox/synthetic-inbox-window.json"))
        self.assertEqual(row["attachment_refs"][0]["attachment_id"], saved[0]["attachment_id"])
        self.assertEqual(Path(row["media_paths"][0]).read_bytes(), b"SYNTHETIC durable")

    def test_unknown_name_is_null_and_unknown_image_still_has_original(self):
        row = self.receive(self.message(attachments=[("image", b"SYNTHETIC unknown image bytes", None)]))
        attachment = runtime.catalog(self.store).query(self.scope)["items"][0]
        self.assertIsNone(attachment["original_name"])
        self.assertEqual(attachment["preview_state"], "unavailable")
        self.assertEqual(attachment["decryption_state"], "protocol_decoded")
        self.assertEqual(Path(row["media_paths"][0]).read_bytes(), b"SYNTHETIC unknown image bytes")

    def test_download_starts_after_all_pending_original_positions_exist(self):
        images = [("image", png(), None)] * 3
        message = self.message("synthetic-pending", attachments=images)
        observations = []
        def inspect(request, **kwargs):
            rows = runtime.catalog(self.store).query(self.scope)["items"]
            observations.append([(r["item_index"], r["status"]) for r in rows])
            return self.transport(request, **kwargs)
        with patch.object(media.urllib.request, "urlopen", side_effect=inspect):
            self.assertTrue(serve.handle_incoming(self.store, message, OWNER))
        self.assertEqual(observations[0], [(0, "pending"), (1, "pending"), (2, "pending")])

    def test_first_image_failure_does_not_renumber_second_of_nine(self):
        pictures = [("image", urllib.error.URLError("synthetic unavailable"), None)] + [
            ("image", png("blue"), None) for _ in range(8)]
        self.receive(self.message("synthetic-nine", attachments=pictures))
        question = self.receive(self.message("synthetic-question", text="上一批第二张是什么"))
        context = question["reference_context"]
        self.assertEqual([r["ordinal"] for r in context["slots"]], list(range(1, 10)))
        self.assertEqual(context["slots"][0]["status"], "failed")
        second = context["slots"][1]
        batch = self.batch()
        result = self.success(self.call(batch, "reference", {"text_message_id": "synthetic-question",
            "snapshot_id": context["snapshot_id"], "ordinals": [1, 2]}))
        self.assertEqual(result["unavailable"][0]["ordinal"], 1)
        self.assertEqual(result["materials"][0]["attachment_id"], second["attachment_id"])
        self.assertEqual(result["materials"][0]["ordinal"], 2)
        self.assertEqual(Path(result["materials"][0]["path"]).read_bytes(), png("blue"))
        self.assertFalse(result["complete"])

    def test_text_snapshot_does_not_switch_to_a_later_image_group(self):
        self.receive(self.message("synthetic-first-image", attachments=[("image", png(), None)]))
        text = self.receive(self.message("synthetic-first-question", text="这张图是什么"))
        old = text["reference_context"]
        self.receive(self.message("synthetic-new-image", attachments=[("image", png("blue"), None)]))
        batch = self.batch()
        result = self.success(self.call(batch, "reference", {"text_message_id": "synthetic-first-question",
            "snapshot_id": old["snapshot_id"], "ordinals": [1]}))
        self.assertEqual(result["reference_context"]["cutoff_seq"], old["cutoff_seq"])
        self.assertEqual(result["materials"][0]["attachment_id"], old["slots"][0]["attachment_id"])
        self.assertEqual(Path(result["materials"][0]["path"]).read_bytes(), png())

    def test_picture_with_text_binds_its_own_image_and_original_item_index(self):
        row = self.receive(self.message("synthetic-caption", text="SYNTHETIC this picture",
            attachments=[("image", png(), None), ("file", b"SYNTHETIC table", "SYNTHETIC.csv")]))
        context = row["reference_context"]
        self.assertEqual(context["text_message_id"], "synthetic-caption")
        self.assertEqual(context["slots"][0]["item_index"], 0)
        self.assertEqual(context["slots"][0]["attachment_id"], row["attachment_refs"][0]["attachment_id"])
        self.assertEqual(context["total_positions"], 1)

    def test_pending_picture_is_observable_without_falling_back_to_old_image(self):
        self.receive(self.message("synthetic-old", attachments=[("image", png("blue"), None)]))
        self.receive(self.message("synthetic-old-close", text="SYNTHETIC old"))
        previous = self.batch()
        self.complete_plain(previous)
        pending = self.message("synthetic-blocked", attachments=[("image", png(), None)])
        entered, release = threading.Event(), threading.Event()
        answer = []
        def blocked(request, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError("synthetic barrier timeout")
            return self.transport(request, **kwargs)
        def receive_pending():
            try:
                answer.append(serve.handle_incoming(self.store, pending, OWNER))
            except BaseException as error:
                answer.append(error)
        with patch.object(media.urllib.request, "urlopen", side_effect=blocked):
            thread = threading.Thread(target=receive_pending)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                question = self.receive(self.message("synthetic-during", text="刚才那张图呢"))
                context = question["reference_context"]
                self.assertEqual(context["slots"][0]["message_id"], "synthetic-blocked")
                self.assertEqual(context["slots"][0]["status"], "pending")
                batch = self.batch()
                self.assertNotIn("synthetic-blocked", batch["owned"])
                result = self.success(self.call(batch, "reference", {"text_message_id": "synthetic-during",
                    "snapshot_id": context["snapshot_id"], "ordinals": [1]}))
                self.assertEqual(result["materials"], [])
                self.assertEqual(result["unavailable"][0]["status"], "pending")
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(answer, [True])

    def test_query_get_release_and_pin_do_not_renew_or_close_the_batch(self):
        aid = self.file()
        self.receive(self.message("synthetic-request", text="SYNTHETIC get file"))
        batch = self.batch()
        before = copy.deepcopy(self.worker._claims())
        self.now += 5
        queried = self.success(self.call(batch, "query", {"filters": {"name_contains": "FILE"}}))
        self.assertEqual(queried["items"][0]["attachment_id"], aid)
        self.assertNotIn("path", queried["items"][0])
        self.assertNotIn("object_key", queried["items"][0])
        get = self.success(self.call(batch, "get", {"attachment_id": aid}))
        self.assertEqual(Path(get["material"]["path"]).read_bytes(), b"SYNTHETIC original")
        self.assertEqual(self.success(self.call(batch, "pin", {"attachment_id": aid, "pinned": True}))["attachment"]["pinned"], True)
        hold = get["material"]["hold_id"]
        self.assertEqual(self.success(self.call(batch, "release", {"hold_id": hold})), {"released": True})
        self.assertEqual(self.success(self.call(batch, "release", {"hold_id": hold})), {"released": True})
        self.assertEqual(before, self.worker._claims())
        self.assertTrue((self.base / "inbox/synthetic-request.json").exists())

    def test_release_cannot_release_another_worker_holder(self):
        aid = self.file()
        first = self.batch()
        hold = self.success(self.call(first, "get", {"attachment_id": aid}))["material"]["hold_id"]
        self.complete_plain(first)
        self.receive(self.message("synthetic-new-request", text="SYNTHETIC new request"))
        second = self.batch()
        self.error(self.call(second, "release", {"hold_id": hold}), "hold_forbidden")
        self.assertTrue(self.worker._claims())

    def test_arrival_requires_refresh_and_old_lease_cannot_read(self):
        aid = self.file()
        batch = self.batch()
        self.receive(self.message("synthetic-arrival", text="SYNTHETIC more"))
        result = self.call(batch, "get", {"attachment_id": aid})
        self.error(result, "needs_refresh")
        self.assertTrue(result["error"]["refresh_required"])
        fresh = self.worker.refresh(batch["lease"])
        self.assertIn("synthetic-arrival", fresh["owned"])
        self.now += worker_state.WORKER_LEASE_SECONDS
        self.error(self.call(fresh, "get", {"attachment_id": aid}), "lease_lost")

    def test_reference_must_be_bound_to_owned_text_and_exact_snapshot(self):
        self.receive(self.message("synthetic-img", attachments=[("image", png(), None)]))
        row = self.receive(self.message("synthetic-text", text="SYNTHETIC image"))
        batch = self.batch()
        sid = row["reference_context"]["snapshot_id"]
        self.error(self.call(batch, "reference", {"text_message_id": "synthetic-not-owned", "snapshot_id": sid}), "scope_mismatch")
        self.error(self.call(batch, "reference", {"text_message_id": "synthetic-text", "snapshot_id": "0" * 32}), "scope_mismatch")
        bad = self.call(batch, "reference", {"text_message_id": "synthetic-text", "snapshot_id": sid, "ordinals": [True]})
        self.error(bad, "invalid_request")

    def test_relogin_revokes_old_worker_but_history_is_still_owned(self):
        aid = self.file()
        batch = self.batch()
        self.complete_plain(batch)
        session = load_json(self.store.session_path)
        session["login_epoch"] = "b" * 32
        atomic_write_json(self.store.session_path, session)
        self.receive(self.message("synthetic-after-login", text="SYNTHETIC find my old file"))
        current = self.batch()
        material = self.success(self.call(current, "get", {"attachment_id": aid}))["material"]
        self.assertEqual(Path(material["path"]).read_bytes(), b"SYNTHETIC original")
        self.error(self.call(batch, "get", {"attachment_id": aid}), "lease_lost")

    def test_foreign_bot_or_owner_never_gets_an_original_path(self):
        aid = self.file()
        batch = self.batch()
        session = load_json(self.store.session_path)
        session["ilink_bot_id"] = "synthetic-foreign-bot"
        atomic_write_json(self.store.session_path, session)
        result = self.call(batch, "get", {"attachment_id": aid})
        self.error(result, "scope_mismatch")
        self.assertNotIn(str(self.base), json.dumps(result))

    def test_missing_entity_returns_no_path_and_query_reports_missing(self):
        aid = self.file()
        batch = self.batch()
        material = self.success(self.call(batch, "get", {"attachment_id": aid}))["material"]
        Path(material["path"]).unlink()
        self.error(self.call(batch, "get", {"attachment_id": aid}), "attachment_unavailable")
        result = self.success(self.call(batch, "query"))
        self.assertEqual(result["items"][0]["status"], "missing")

    def test_wire_rejects_unknown_version_fields_types_and_privilege_fields(self):
        self.file()
        batch = self.batch()
        self.error(self.call(batch, "query", schema_version=True), "unsupported_version")
        self.error(self.call(batch, "query", schema_version=2), "unsupported_version")
        self.error(self.call(batch, "invented"), "unsupported_action")
        self.error(self.call(batch, "query", {"limit": True}), "invalid_request")
        self.error(self.call(batch, "get", {"attachment_id": "0" * 32, "owner_id": OWNER}), "invalid_request")
        self.error(self.call(batch, "query", {"filters": {"sql": "SYNTHETIC"}}), "invalid_request")
        self.error(self.call(batch, "reply", {"attachment_ids": ["x", "x"]}), "invalid_request")

    def test_single_ordered_reply_registers_group_and_completes_only_current_owned(self):
        first, second = self.file(mid="synthetic-first"), self.file(mid="synthetic-second")
        batch = self.batch()
        result = self.success(self.call(batch, "reply", {"attachment_ids": [second, first], "caption": "SYNTHETIC caption"}))
        self.assertTrue(result["queued"])
        self.assertFalse(result["delivery_confirmed"])
        self.assertEqual(result["status"]["attachment_count"], 2)
        state = runtime.ledger(self.worker.store)
        job = state.job_status(self.scope, result["job_id"])
        self.assertEqual([u["attachment_id"] for u in job["units"] if u["kind"] == "attachment"], [second, first])
        for mid in batch["owned"]:
            self.assertEqual(load_json(str(self.base / "state/received" / (mid + ".json")))["status"], "complete")
            self.assertFalse((self.base / "inbox" / (mid + ".json")).exists())
            self.assertEqual(self.worker.covered(mid)["status"], "queued")
        self.assertEqual(list((self.base / "outbox/pending").glob("*.json")), [])
        self.assertEqual(self.worker._claims(), {})
        self.assertEqual(self.worker.plan()["decision"], "silent")

    def test_admission_record_interruption_recovers_same_job_without_reexecution(self):
        aid = self.file()
        batch = self.batch()
        original_write = runtime.atomic_write_json
        interrupted = []
        def fail_admitted(path, value):
            if Path(path).parent.name == "attachment_admissions" and value.get("phase") == "admitted" and not interrupted:
                interrupted.append(True)
                raise OSError("synthetic admission record interruption")
            return original_write(path, value)
        with patch.object(runtime, "atomic_write_json", side_effect=fail_admitted):
            self.error(self.call(batch, "reply", {"attachment_ids": [aid]}), "storage_invalid")
        state = runtime.ledger(self.worker.store)
        original_jobs = state.pending_jobs(self.scope)["job_ids"]
        self.assertEqual(len(original_jobs), 1)
        self.assertTrue((self.base / "inbox" / (batch["owned"][0] + ".json")).exists())
        self.assertEqual(runtime.recover_admissions(self.worker.store), 1)
        self.assertEqual(state.pending_jobs(self.scope)["job_ids"], original_jobs)
        self.assertEqual(self.worker.plan()["decision"], "silent")
        self.assertFalse((self.base / "inbox" / (batch["owned"][0] + ".json")).exists())
        self.assertEqual(len(state.list_groups(self.scope)["groups"]), 1)

    def test_worker_expiry_during_acquire_blocks_final_group_publication(self):
        aid = self.file()
        batch = self.batch()
        original_acquire = AttachmentStore.acquire
        def slow_acquire(catalog, scope, attachment_id, holder, *args, **kwargs):
            result = original_acquire(catalog, scope, attachment_id, holder, *args, **kwargs)
            if holder.startswith("group:request:"):
                self.now += worker_state.WORKER_LEASE_SECONDS
            return result
        with patch.object(AttachmentStore, "acquire", slow_acquire):
            result = self.call(batch, "reply", {"attachment_ids": [aid]})
        self.error(result, "lease_lost")
        state = runtime.ledger(self.worker.store)
        self.assertEqual(state.pending_jobs(self.scope)["job_ids"], [])
        runtime.recover_admissions(self.worker.store)
        self.assertEqual(state.pending_jobs(self.scope)["job_ids"], [])
        self.assertEqual(load_json(str(self.base / "state/received" / (batch["owned"][0] + ".json")))["status"], "received")

    def test_exhausted_group_waits_for_explicit_new_continue_and_new_context(self):
        aid = self.file()
        batch = self.batch()
        state = runtime.ledger(self.worker.store)
        state.reserve_units(self.scope, batch["context"], ["synthetic-ordinary:%d" % i for i in range(8)])
        first = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        self.assertEqual(first["status"]["state"], "awaiting_continue")
        self.assertTrue(first["status"]["can_continue"])
        self.assertEqual(state.pending_jobs(self.scope)["job_ids"], [])
        self.receive(self.message("synthetic-unrelated", text="SYNTHETIC change the topic"))
        changed = self.batch()
        self.error(self.call(changed, "continue", {"group_id": first["group_id"]}), "continue_required")
        listed = self.success(self.call(changed, "status"))["groups"]
        self.assertEqual(listed[0]["group_id"], first["group_id"])
        self.complete_plain(changed)
        self.receive(self.message("synthetic-continue", text="继续"))
        followup = self.batch()
        continued = self.success(self.call(followup, "continue", {"group_id": first["group_id"]}))
        self.assertNotEqual(continued["job_id"], first["job_id"])
        old = state.job_status(self.scope, first["job_id"])
        new = state.job_status(self.scope, continued["job_id"])
        self.assertNotEqual(old["context_ref"], new["context_ref"])
        self.assertEqual(len(new["units"]), 1)

    def test_unknown_group_is_discoverable_and_not_continued(self):
        aid = self.file()
        batch = self.batch()
        first = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        state = runtime.ledger(self.worker.store)
        unit = state.next_unit(self.scope, first["job_id"])
        state.begin_unit(self.scope, first["job_id"], unit["unit_id"])
        state.settle_unit(self.scope, first["job_id"], unit["unit_id"], "unknown")
        self.receive(self.message("synthetic-unknown-query", text="继续"))
        current = self.batch()
        status = self.success(self.call(current, "status"))["groups"][0]
        self.assertEqual(status["state"], "unknown")
        self.assertFalse(status["can_continue"])
        self.assertEqual(status["units"]["unknown"], 1)
        self.error(self.call(current, "continue", {"group_id": first["group_id"]}), "group_unknown")

    def test_real_flush_sends_caption_ordered_originals_and_heic_as_file(self):
        heic = b"\x00\x00\x00\x18ftypheicSYNTHETIC-original"
        h = self.file("SYNTHETIC.heic", heic, "synthetic-heic")
        picture_row = self.receive(self.message("synthetic-png", attachments=[("image", png(), None)]))
        p = picture_row["attachment_refs"][0]["attachment_id"]
        f = self.file("SYNTHETIC-report.pdf", b"SYNTHETIC-PDF", "synthetic-pdf")
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [h, p, f], "caption": "SYNTHETIC caption"}))
        messages, originals = self.send_spy()
        self.flush()
        self.assertEqual([m["item_list"][0]["type"] for m in messages], [1, media.ITEM_FILE, media.ITEM_IMAGE, media.ITEM_FILE])
        self.assertEqual(originals, [heic, png(), b"SYNTHETIC-PDF"])
        self.assertEqual(messages[1]["item_list"][0]["file_item"]["file_name"], "SYNTHETIC.heic")
        self.assertEqual(messages[3]["item_list"][0]["file_item"]["file_name"], "SYNTHETIC-report.pdf")
        status = runtime.ledger(self.store).group_status(self.scope, group["group_id"])
        self.assertEqual(status["state"], "complete")
        self.assertEqual(status["accepted_attachment_ids"], [h, p, f])
        self.assertEqual(status["units"]["accepted"], 4)
        self.assertEqual(len({m["client_id"] for m in messages}), 4)
        self.flush()
        self.assertEqual(len(messages), 4)

    def test_actual_ordinary_chunks_consume_same_budget_before_group(self):
        first = self.file(mid="synthetic-first-budget")
        second = self.file(mid="synthetic-second-budget")
        batch = self.batch()
        atomic_write_json(str(self.base / "outbox/pending/synthetic-ordinary.json"),
            {"message_id": "synthetic-ordinary", "to_user_id": OWNER, "context_token": batch["context"],
             "text": "S" * 12000, "enqueued_at": time.time(), "client_key": "synthetic-ordinary"})
        messages, originals = self.send_spy()
        self.flush()
        self.assertEqual(len(messages), 6)
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, batch["context"])["remaining"], 2)
        group = self.success(self.call(batch, "reply", {"attachment_ids": [first, second]}))
        self.flush()
        self.assertEqual(len(messages), 8)
        self.assertEqual(len(originals), 2)
        self.assertEqual(runtime.ledger(self.store).group_status(self.scope, group["group_id"])["state"], "complete")
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, batch["context"])["remaining"], 0)

    def test_timeout_unknown_survives_restart_and_new_topic_without_resend(self):
        aid = self.file()
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        messages, _ = self.send_spy(final_result=TimeoutError("synthetic no final receipt"))
        self.flush()
        self.assertEqual(len(messages), 1)
        self.assertEqual(runtime.ledger(self.store).group_status(self.scope, group["group_id"])["state"], "unknown")
        restarted = Store(str(self.base))
        self.addCleanup(restarted.close)
        runtime.initialize(restarted)
        runtime.ledger(restarted).recover()
        self.flush(restarted)
        self.assertEqual(len(messages), 1)
        self.receive(self.message("synthetic-unrelated-later", text="SYNTHETIC another topic"))
        self.flush(restarted)
        self.assertEqual(len(messages), 1)
        self.assertEqual(runtime.ledger(restarted).register_context(self.scope, batch["context"])["used"], 1)

    def test_partial_group_does_not_take_new_token_until_explicit_continue(self):
        aids = [self.file(mid="synthetic-page-%d" % i) for i in range(3)]
        batch = self.batch()
        state = runtime.ledger(self.store)
        # Six real ordinary chunks leave two physical units: one payload and
        # one honest page-status unit, not three attachments disguised as one.
        atomic_write_json(str(self.base / "outbox/pending/synthetic-prior.json"),
            {"message_id": "synthetic-prior", "to_user_id": OWNER, "context_token": batch["context"],
             "text": "P" * 12000, "enqueued_at": time.time(), "client_key": "synthetic-prior"})
        messages, originals = self.send_spy()
        self.flush()
        group = self.success(self.call(batch, "reply", {"attachment_ids": aids}))
        self.flush()
        self.assertEqual(len(messages), 8)
        self.assertEqual(len(originals), 1)
        status = state.group_status(self.scope, group["group_id"])
        self.assertEqual(status["accepted_attachment_ids"], aids[:1])
        self.assertEqual(status["pending_attachment_ids"], aids[1:])
        original_context = messages[-1]["context_token"]
        self.receive(self.message("synthetic-new-topic", text="SYNTHETIC change topic"))
        self.flush()
        self.assertEqual(len(messages), 8)
        unrelated = self.batch()
        self.complete_plain(unrelated)
        self.receive(self.message("synthetic-send-rest", text="继续"))
        followup = self.batch()
        continued = self.success(self.call(followup, "continue", {"group_id": group["group_id"]}))
        self.assertNotEqual(continued["job_id"], group["job_id"])
        self.flush()
        status = state.group_status(self.scope, group["group_id"])
        self.assertEqual(status["state"], "complete")
        self.assertEqual(status["accepted_attachment_ids"], aids)
        self.assertEqual(len(originals), 3)
        media_messages = [m for m in messages if m["item_list"][0]["type"] != 1]
        self.assertEqual(len(media_messages), 3)
        self.assertEqual(media_messages[0]["context_token"], original_context)
        self.assertNotEqual(media_messages[1]["context_token"], original_context)

    def test_cli_long_text_cannot_bypass_durable_cap_or_restart(self):
        row = self.receive(self.message("synthetic-cli-context", text="SYNTHETIC request"))
        context = row["context_token"]
        arguments = SimpleNamespace(text="C" * 18001, image=None, file=None, video=None,
                                    to=OWNER, context=context)
        messages, _ = self.send_spy()
        with patch.object(cli, "get_store", return_value=self.store), patch("builtins.print"):
            self.assertEqual(cli.cmd_send(arguments), 1)
        self.assertEqual(len(messages), 8)
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, context)["used"], 8)
        restarted = Store(str(self.base))
        self.addCleanup(restarted.close)
        with patch.object(cli, "get_store", return_value=restarted), patch("builtins.print"):
            self.assertEqual(cli.cmd_send(arguments), 1)
        self.assertEqual(len(messages), 8)

    def test_cli_caption_and_media_are_two_units_in_same_cap(self):
        row = self.receive(self.message("synthetic-cli-caption-context", text="SYNTHETIC request"))
        context = row["context_token"]
        arguments = SimpleNamespace(text="C" * 14000, image=None, file=None, video=None,
                                    to=OWNER, context=context)
        messages, originals = self.send_spy()
        with patch.object(cli, "get_store", return_value=self.store), patch("builtins.print"):
            self.assertEqual(cli.cmd_send(arguments), 0)
            source = self.home / "SYNTHETIC-file.bin"
            source.write_bytes(b"SYNTHETIC original")
            arguments.text, arguments.file = "SYNTHETIC caption", str(source)
            self.assertEqual(cli.cmd_send(arguments), 1)
        self.assertEqual(len(messages), 8)
        self.assertEqual(originals, [])
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, context)["remaining"], 0)

    def test_cancel_during_real_upload_prevents_final_wechat_send(self):
        aid = self.file()
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        state = runtime.ledger(self.store)
        messages, originals = self.send_spy(after_upload=lambda: state.cancel_group(self.scope, group["group_id"]))
        self.flush()
        self.assertEqual(len(originals), 1)
        self.assertEqual(messages, [])
        self.assertEqual(state.group_status(self.scope, group["group_id"])["state"], "cancelled")

    def test_epoch_change_after_upload_prevents_final_wechat_send(self):
        aid = self.file()
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        def new_epoch():
            session = load_json(self.store.session_path)
            session["login_epoch"] = "c" * 32
            atomic_write_json(self.store.session_path, session)
        messages, originals = self.send_spy(after_upload=new_epoch)
        self.flush()
        self.assertEqual(len(originals), 1)
        self.assertEqual(messages, [])
        status = runtime.ledger(self.store).group_status(self.scope, group["group_id"])
        self.assertEqual(status["accepted_attachment_ids"], [])

    def test_token_change_after_upload_prevents_final_wechat_send(self):
        aid = self.file()
        batch = self.batch()
        self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        def new_token():
            session = load_json(self.store.session_path)
            session["bot_token"] = "synthetic-replaced-token"
            atomic_write_json(self.store.session_path, session)
        messages, originals = self.send_spy(after_upload=new_token)
        self.flush()
        self.assertEqual(len(originals), 1)
        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
