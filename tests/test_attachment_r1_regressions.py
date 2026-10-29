"""003 R1 implementation regressions; real public paths, synthetic state only.

These tests retain the first CLI, group selection, cancellation transaction,
and sessionless embedded-serve boundaries found after candidate e6e49cc.
They are implementation self-tests, not independent QA or platform acceptance.
"""
import contextlib
import copy
import io
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_delivery as delivery
from wechat_bridge import attachments as catalog_module
from wechat_bridge import attachment_runtime as runtime, cli, ilink, media, serve
from wechat_bridge import worker_state
from wechat_bridge.store import Store, atomic_write_json, load_json


class R1Regressions(unittest.TestCase):
    setUp = fixtures.RuntimeTests.setUp
    message = fixtures.RuntimeTests.message
    transport = fixtures.RuntimeTests.transport
    receive = fixtures.RuntimeTests.receive
    file = fixtures.RuntimeTests.file
    batch = fixtures.RuntimeTests.batch
    call = fixtures.RuntimeTests.call
    success = fixtures.RuntimeTests.success
    error = fixtures.RuntimeTests.error
    complete_plain = fixtures.RuntimeTests.complete_plain
    send_spy = fixtures.RuntimeTests.send_spy
    flush = fixtures.RuntimeTests.flush

    def new_store(self, suffix):
        value = Store(str(self.home / suffix))
        self.addCleanup(value.close)
        return value

    def cli_send(self, store, context=None, text="R" * 18001):
        arguments = SimpleNamespace(text=text, image=None, file=None, video=None,
                                    to=fixtures.OWNER, context=context)
        with patch.object(cli, "get_store", return_value=store), \
                contextlib.redirect_stdout(io.StringIO()):
            return cli.cmd_send(arguments)

    def ledger_rows(self, store=None):
        path = Path((store or self.store).state_dir) / "attachment-delivery.sqlite3"
        with sqlite3.connect(str(path)) as db:
            return {table: db.execute("SELECT * FROM %s ORDER BY rowid" % table).fetchall()
                    for table in ("contexts", "groups", "jobs", "reservations", "payload_units", "job_units")}

    def hold_rows(self):
        with sqlite3.connect(str(self.base / "state/attachments.sqlite3")) as db:
            return db.execute("SELECT * FROM holds ORDER BY rowid").fetchall()

    def parked_groups(self, count=2, prefix_ids=None):
        aid = self.file(mid="synthetic-r1-source")
        batch = self.batch()
        state = runtime.ledger(self.worker.store)
        result = []
        for index in range(count):
            request = "synthetic-r1-parked-%03d" % index
            if index:
                _, batch = self.request("SYNTHETIC park this attachment", request)
            # Occupied ordinary units are a real shared-budget condition. Use
            # the public reply path so each parked group retains its original
            # admission, hold and completed source batch.
            state.reserve_units(self.scope, batch["context"],
                [request + ":ordinary:%d" % unit for unit in range(delivery.CAP)])
            forced = (prefix_ids or {}).get(index)
            if forced:
                create_group = state.create_group
                def fixed_group(*args, **kwargs):
                    # Restrict the collision fixture to the real group UUID;
                    # acquisition holds, worker leases and jobs stay unique.
                    with patch.object(delivery.uuid, "uuid4", return_value=SimpleNamespace(hex=forced)):
                        return create_group(*args, **kwargs)
                with patch.object(state, "create_group", side_effect=fixed_group):
                    group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
            else:
                group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
            admission = load_json(str(self.base / "state/attachment_admissions" /
                ("reply-" + runtime._digest(batch["owned"]) + ".json")))
            self.assertEqual((admission["phase"], admission["admitted"],
                              admission["group_id"], admission["job_id"]),
                             ("admitted", True, group["group_id"], group["job_id"]))
            job = state.job_status(self.scope, group["job_id"])
            self.assertEqual((job["state"], job["units"]), ("budget_exhausted", []))
            for mid in batch["owned"]:
                self.assertEqual(self.worker._receipt(mid)["status"], "complete")
            status = state.group_status(self.scope, group["group_id"])
            self.assertEqual(status["state"], "awaiting_continue")
            self.assertTrue(status["can_continue"])
            result.append(group["group_id"])
        return result

    def request(self, words, mid="synthetic-r1-current"):
        text = self.receive(self.message(mid, text=words))
        return text, self.batch()

    def unchanged_call(self, batch, action, arguments, code="continue_required"):
        before = self.ledger_rows()
        claims = copy.deepcopy(self.worker._claims())
        self.error(self.call(batch, action, arguments), code)
        self.assertEqual(self.ledger_rows(), before)
        self.assertEqual(self.worker._claims(), claims)
        for mid in batch["owned"]:
            self.assertEqual(self.worker._receipt(mid)["status"], "received")

    def test_first_cli_migrates_unknown_legacy_budget_then_receiver_context_has_persistent_cap8(self):
        old = self.new_store("synthetic-precutover")
        atomic_write_json(old.session_path, self.session)
        atomic_write_json(old.last_ctx_path, {"to_user_id": fixtures.OWNER,
                                             "context_token": "synthetic-old-context"})
        marker = Path(old.state_dir) / "attachments_enabled.json"
        path = Path(old.state_dir) / "attachment-delivery.sqlite3"
        self.assertFalse(marker.exists())
        self.assertFalse(path.exists())
        messages, _ = self.send_spy()
        self.assertEqual(self.cli_send(old), 1)
        self.assertEqual(messages, [])
        self.assertTrue(marker.is_file())
        self.assertTrue(path.is_file())
        scope = runtime.current_scope(old)
        state = runtime.ledger(old)
        budget = state.register_context(scope, "synthetic-old-context")
        self.assertTrue(budget["legacy_unknown"])
        self.assertEqual((budget["used"], budget["remaining"]), (8, 0))
        fresh = self.message("synthetic-r1-trusted-fresh", text="SYNTHETIC new request")
        self.assertTrue(serve.handle_incoming(old, fresh, fixtures.OWNER))
        self.assertEqual(self.cli_send(old), 1)
        self.assertEqual(len(messages), 8)
        self.assertEqual({msg["context_token"] for msg in messages}, {fresh["context_token"]})
        self.assertEqual(len({msg["client_id"] for msg in messages}), 8)
        restarted = Store(old.base)
        self.addCleanup(restarted.close)
        self.assertEqual(self.cli_send(restarted), 1)
        self.assertEqual(len(messages), 8)
        self.assertEqual(runtime.ledger(restarted).register_context(scope, fresh["context_token"])["remaining"], 0)

    def test_arbitrary_cli_context_is_never_assumed_fresh_or_sent(self):
        messages, _ = self.send_spy()
        for token in ("synthetic-r1-unregistered-a", "synthetic-r1-unregistered-b"):
            with self.subTest(token=token):
                self.assertEqual(self.cli_send(self.store, context=token), 1)
                self.assertEqual(messages, [])
                budget = runtime.ledger(self.store).register_context(self.scope, token)
                self.assertTrue(budget["legacy_unknown"])
                self.assertEqual((budget["used"], budget["remaining"]), (8, 0))

    def test_multiple_groups_bare_continue_and_generic_cancel_cannot_select_any_model_id(self):
        groups = self.parked_groups()
        for action, words in (("continue", "继续"), ("cancel", "取消这组附件")):
            text, batch = self.request(words, "synthetic-r1-ambiguous-" + action)
            for group in groups:
                arguments = {"group_id": group}
                if action == "cancel":
                    arguments["text_message_id"] = text["message_id"]
                self.unchanged_call(batch, action, arguments)
            self.complete_plain(batch)

    def test_explicit_unique_prefix_in_current_text_selects_continue_target_only(self):
        first, second = self.parked_groups()
        prefix = first[:11]  # ag_ plus eight hexadecimal digits.
        _, batch = self.request("继续 " + prefix)
        self.unchanged_call(batch, "continue", {"group_id": second})
        before = runtime.ledger(self.store).group_status(self.scope, second)
        result = self.success(self.call(batch, "continue", {"group_id": first}))
        self.assertEqual(result["group_id"], first)
        self.assertTrue(result["queued"])
        self.assertEqual(runtime.ledger(self.store).group_status(self.scope, second), before)

    def test_explicit_full_id_in_current_text_selects_cancel_target_only_and_repeats(self):
        first, second = self.parked_groups()
        text, batch = self.request("取消 " + first)
        self.unchanged_call(batch, "cancel", {"text_message_id": text["message_id"], "group_id": second})
        arguments = {"text_message_id": text["message_id"], "group_id": first}
        result = self.success(self.call(batch, "cancel", arguments))
        self.assertEqual(result["group"]["state"], "cancelled")
        rows = self.ledger_rows()
        self.assertEqual(self.success(self.call(batch, "cancel", arguments)), result)
        self.assertEqual(self.ledger_rows(), rows)
        self.assertEqual(runtime.ledger(self.store).group_status(self.scope, second)["state"], "awaiting_continue")

    def test_sole_candidate_allows_bare_continue_and_generic_cancel(self):
        group, = self.parked_groups(count=1)
        _, batch = self.request("继续", "synthetic-r1-sole-continue")
        result = self.success(self.call(batch, "continue", {"group_id": group}))
        self.assertEqual(result["group_id"], group)
        text, cancel = self.request("取消这组附件", "synthetic-r1-sole-cancel")
        result = self.success(self.call(cancel, "cancel", {"text_message_id": text["message_id"], "group_id": group}))
        self.assertEqual(result["group"]["state"], "cancelled")

    def test_prefix_collision_beyond_first_status_page_refuses_both_actions_without_mutation(self):
        forced = {0: "deadbeef" + "0" * 23 + "1", 101: "deadbeef" + "0" * 23 + "2"}
        groups = self.parked_groups(count=102, prefix_ids=forced)
        page = runtime.ledger(self.store).list_groups(self.scope, limit=20)
        self.assertIsNotNone(page["next_cursor"])
        first_page = {row["group_id"] for row in page["groups"]}
        self.assertNotIn(groups[0], first_page)
        self.assertIn(groups[-1], first_page)
        for action in ("continue", "cancel"):
            words = ("继续 " if action == "continue" else "取消 ") + "ag_deadbeef"
            text, batch = self.request(words, "synthetic-r1-page-two-" + action)
            for group in (groups[0], groups[-1]):
                arguments = {"group_id": group}
                if action == "cancel":
                    arguments["text_message_id"] = text["message_id"]
                self.unchanged_call(batch, action, arguments)
            self.complete_plain(batch)

    def test_cancel_losing_worker_after_real_sql_update_rolls_back_group_jobs_and_refund(self):
        aid = self.file()
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        text, cancel = self.request("取消 " + group["group_id"])
        state = runtime.ledger(self.worker.store)
        before = self.ledger_rows()
        original = state._transaction
        injected = []
        owner = self
        class AfterCancellationSQL:
            def __init__(self, db):
                self.db = db
            def __getattr__(self, key):
                return getattr(self.db, key)
            def execute(self, sql, arguments=()):
                result = self.db.execute(sql, arguments)
                if sql.lstrip().upper().startswith("UPDATE GROUPS SET CANCELLED=1"):
                    owner.assertEqual(self.db.execute("SELECT cancelled FROM groups WHERE group_id=?",
                                                     (group["group_id"],)).fetchone()[0], 1)
                    injected.append("real SQL mutation before transaction commit")
                    owner.now += worker_state.WORKER_LEASE_SECONDS
                return result
        @contextlib.contextmanager
        def expiry_inside_transaction(*args, **kwargs):
            with original(*args, **kwargs) as db:
                yield AfterCancellationSQL(db)
        with patch.object(state, "_transaction", expiry_inside_transaction):
            self.error(self.call(cancel, "cancel", {"text_message_id": text["message_id"],
                       "group_id": group["group_id"]}), "lease_lost")
        self.assertEqual(injected, ["real SQL mutation before transaction commit"])
        self.assertEqual(self.ledger_rows(), before)
        self.assertEqual(runtime.ledger(self.store).group_status(self.scope, group["group_id"])["state"], "queued")
        self.assertEqual(runtime.ledger(self.store).register_context(self.scope, batch["context"])["remaining"], 7)

    def test_unknown_delivery_cancel_repeats_without_refund_resend_or_releasing_original_hold(self):
        aid = self.file()
        batch = self.batch()
        group = self.success(self.call(batch, "reply", {"attachment_ids": [aid]}))
        messages, originals = self.send_spy(final_result=TimeoutError("synthetic unknown final HTTP outcome"))
        self.flush()
        self.assertEqual((len(messages), len(originals)), (1, 1))
        state = runtime.ledger(self.store)
        self.assertEqual(state.group_status(self.scope, group["group_id"])["state"], "unknown")
        before_holds = self.hold_rows()
        self.assertTrue(before_holds)
        text, cancel = self.request("取消 " + group["group_id"])
        arguments = {"text_message_id": text["message_id"], "group_id": group["group_id"]}
        for _ in range(2):
            result = self.success(self.call(cancel, "cancel", arguments))
            self.assertEqual(result["group"]["state"], "unknown")
            self.assertEqual(result["group"]["unknown_attachment_ids"], [aid])
            self.assertEqual(state.register_context(self.scope, batch["context"])["remaining"], 7)
            self.assertEqual(self.hold_rows(), before_holds)
        self.flush()
        self.assertEqual((len(messages), len(originals)), (1, 1))

    def blocked_admission_cannot_recover(self, batch, code):
        journals = list((self.base / "state/attachment_admissions").glob("*.json"))
        self.assertEqual(len(journals), 1)
        record = load_json(str(journals[0]))
        self.assertEqual(record["phase"], "blocked")
        self.assertEqual(record["blocked_reason"], code)
        self.assertFalse(record["admitted"])
        before = self.ledger_rows()
        holds = self.hold_rows()
        raw = journals[0].read_bytes()
        self.assertEqual(runtime.recover_admissions(self.worker.store), 0)
        self.assertEqual(self.ledger_rows(), before)
        self.assertEqual(self.hold_rows(), holds)
        self.assertEqual(journals[0].read_bytes(), raw)
        for mid in batch["owned"]:
            self.assertEqual(self.worker._receipt(mid)["status"], "received")
        return record

    def reply_hash_authority_loss(self, code):
        body = b"SYNTHETIC actual streamed acquire hash " * 2048
        aid = self.file(body=body)
        batch = self.batch()
        before, holds = self.ledger_rows(), self.hold_rows()
        objects = list((self.base / "attachments/objects").iterdir())
        self.assertEqual(len(objects), 1)
        info = objects[0].stat()
        actual_read = os.read
        observed = []
        def hash_read(fd, size):
            data = actual_read(fd, size)
            current = os.fstat(fd)
            if (data and not observed and
                    (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino)):
                observed.append(len(data))
                if code == "lease_lost":
                    self.now += worker_state.WORKER_LEASE_SECONDS
                else:
                    session = load_json(self.store.session_path)
                    session["login_epoch"] = "0" * 32
                    atomic_write_json(self.store.session_path, session)
            return data
        with patch.object(catalog_module.os, "read", hash_read):
            self.error(self.call(batch, "reply", {"attachment_ids": [aid]}), code)
        self.assertEqual(observed, [len(body)])
        self.assertEqual(self.ledger_rows(), before)
        self.assertEqual(self.hold_rows(), holds)
        self.blocked_admission_cannot_recover(batch, code)

    def test_reply_real_acquire_hash_crossing_worker_expiry_leaves_no_hold_group_or_job(self):
        self.reply_hash_authority_loss("lease_lost")

    def test_reply_real_acquire_hash_changing_epoch_leaves_no_hold_group_or_job(self):
        self.reply_hash_authority_loss("epoch_mismatch")

    def test_reply_group_before_commit_expiry_rolls_back_group_and_blocks_journal(self):
        aid = self.file()
        batch = self.batch()
        before = self.ledger_rows()
        before_holds = self.hold_rows()
        state = runtime.ledger(self.worker.store)
        reached = []
        def cross_expiry(point):
            if point == "group_before_commit":
                reached.append(point)
                self.now += worker_state.WORKER_LEASE_SECONDS
        with patch.object(state, "fault", cross_expiry):
            self.error(self.call(batch, "reply", {"attachment_ids": [aid]}), "lease_lost")
        self.assertEqual(reached, ["group_before_commit"])
        self.assertEqual(self.ledger_rows(), before)
        # The hold was established in a separate, still-authorized catalog
        # transaction. Its retention is not a cross-database atomicity claim.
        retained = self.hold_rows()
        self.assertEqual([row for row in retained if row[0] in {old[0] for old in before_holds}], before_holds)
        added = [row for row in retained if row[0] not in {old[0] for old in before_holds}]
        self.assertEqual(len(added), 1)
        self.assertEqual(added[0][1], aid)
        self.assertTrue(added[0][3].startswith("group:request:"))
        self.blocked_admission_cannot_recover(batch, "lease_lost")

    def test_reply_page_before_commit_expiry_rolls_back_job_and_reservations_only(self):
        aid = self.file()
        batch = self.batch()
        before = self.ledger_rows()
        before_holds = self.hold_rows()
        state = runtime.ledger(self.worker.store)
        reached = []
        def cross_expiry(point):
            if point == "page_before_commit":
                reached.append(point)
                self.now += worker_state.WORKER_LEASE_SECONDS
        with patch.object(state, "fault", cross_expiry):
            self.error(self.call(batch, "reply", {"attachment_ids": [aid]}), "lease_lost")
        self.assertEqual(reached, ["page_before_commit"])
        after = self.ledger_rows()
        for table in ("contexts", "jobs", "reservations", "job_units"):
            self.assertEqual(after[table], before[table])
        # A valid preceding group/hold commit may remain. The guarded page
        # transaction must publish neither a job nor physical reservations.
        self.assertEqual(len(after["groups"]), 1)
        self.assertGreaterEqual(len(after["payload_units"]), 1)
        holds = self.hold_rows()
        self.assertEqual([row for row in holds if row[0] in {old[0] for old in before_holds}], before_holds)
        added = [row for row in holds if row[0] not in {old[0] for old in before_holds}]
        self.assertEqual(len(added), 1)
        self.assertEqual(added[0][1], aid)
        self.assertTrue(added[0][3].startswith("group:request:"))
        record = self.blocked_admission_cannot_recover(batch, "lease_lost")
        self.assertEqual(record["group_id"], after["groups"][0][0])
        self.assertEqual(record["hold_ids"], [added[0][0]])

    def embedded_run(self, store, should_send):
        """Exercise real serve.run and flusher threads; fake only HTTP/time pace."""
        messages = []
        attempted = threading.Event()
        def edge(method, url, headers, body, **kwargs):
            if url.endswith("/getupdates"):
                attempted.wait(0.6 if should_send else 0.08)
                raise KeyboardInterrupt()
            if url.endswith("/sendmessage"):
                messages.append(copy.deepcopy(body["msg"]))
                attempted.set()
                return {"ret": 0}
            raise AssertionError("unexpected synthetic embedded HTTP path")
        existing_handles = len(serve._lock_fhs)
        try:
            with patch.object(serve, "http_json", side_effect=edge), \
                    patch.object(ilink, "http_json", side_effect=edge), \
                    patch.object(media, "http_json", side_effect=edge), \
                    patch.object(serve, "_bot_lock_root", return_value=str(self.home / "synthetic-bot-locks")), \
                    patch.object(serve, "FLUSH_INTERVAL_SECONDS", 0.005), \
                    patch.object(serve, "PACE_SECONDS", 0):
                result = serve.run(store, "https://synthetic.invalid", "synthetic-embedded-token", fixtures.OWNER)
        finally:
            for handle in serve._lock_fhs[existing_handles:]:
                handle.close()
            del serve._lock_fhs[existing_handles:]
        return result, messages

    def legacy_job(self, store):
        atomic_write_json(str(Path(store.pending_dir) / "synthetic-r1-embedded.json"), {
            "message_id": "synthetic-r1-embedded", "to_user_id": fixtures.OWNER,
            "context_token": "synthetic-embedded-old-context", "text": "SYNTHETIC embedded reply",
            "enqueued_at": time.time() - 30, "client_key": "synthetic-embedded-client"})

    def test_embedded_serve_without_session_or_attachment_state_preserves_legacy_flusher(self):
        old = self.new_store("synthetic-embedded-legacy")
        self.legacy_job(old)
        self.assertFalse(Path(old.session_path).exists())
        self.assertFalse((Path(old.state_dir) / "attachments_enabled.json").exists())
        self.assertFalse((Path(old.state_dir) / "attachment-delivery.sqlite3").exists())
        result, messages = self.embedded_run(old, should_send=True)
        self.assertEqual(result, 0)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["context_token"], "synthetic-embedded-old-context")
        self.assertEqual(messages[0]["item_list"][0]["text_item"]["text"], "SYNTHETIC embedded reply")
        self.assertFalse((Path(old.state_dir) / "attachments_enabled.json").exists())
        self.assertFalse((Path(old.state_dir) / "attachment-delivery.sqlite3").exists())

    def test_embedded_serve_existing_new_state_or_bad_session_never_downgrades(self):
        for mode in ("ledger-only-no-session", "marker-only-no-session", "malformed-session", "invalid-session"):
            with self.subTest(mode=mode):
                store = self.new_store("synthetic-embedded-" + mode)
                if mode == "ledger-only-no-session":
                    atomic_write_json(store.session_path, self.session)
                    runtime.initialize(store)
                    (Path(store.state_dir) / "attachments_enabled.json").unlink()
                    Path(store.session_path).unlink()
                elif mode == "marker-only-no-session":
                    atomic_write_json(str(Path(store.state_dir) / "attachments_enabled.json"),
                                      {"schema_version": 1, "enabled_at": time.time()})
                elif mode == "malformed-session":
                    Path(store.session_path).write_text("{synthetic invalid json")
                else:
                    atomic_write_json(store.session_path, {"ilink_user_id": fixtures.OWNER})
                self.legacy_job(store)
                _, messages = self.embedded_run(store, should_send=False)
                self.assertEqual(messages, [])
                self.assertEqual(list(Path(store.sent_dir).glob("*.json")), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
