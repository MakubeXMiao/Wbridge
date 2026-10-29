"""Consolidated managed-media entry/queue tests; synthetic transport only."""
import argparse
from contextlib import contextmanager, ExitStack, redirect_stdout
import copy
import errno
import importlib.util
import io
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime, cli, media, serve
from wechat_bridge.attachments import AttachmentStore
from wechat_bridge.store import atomic_write_json, load_json

SPEC = importlib.util.spec_from_file_location("second_review_muse_fixture", ROOT / "hooks/tests/test_muse_prequeue.py")
MUSE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MUSE)


@contextmanager
def runtime_fixture():
    case = fixtures.RuntimeTests("runTest")
    case.setUp()
    try:
        yield case
    finally:
        case.doCleanups()


@contextmanager
def muse_fixture():
    case = MUSE.MusePrequeueTests()
    case.setUp()
    try:
        yield case
    finally:
        case.doCleanups()
        case.worker.store.close()


def staged(store, now, suffix="path"):
    _, event = runtime.receive_event(store,
        {"message_id": "synthetic-stage-" + suffix, "context_token": "SYNTHETIC_STAGE_" + suffix},
        "synthetic-owner", [{"item_index": 0, "kind": "file", "original_name": "unfinished.bin",
                              "declared_type": None}], "", now)
    path = Path(event["attachment_refs"][0]["stage_path"])
    path.write_bytes(b"SYNTHETIC_UNFINISHED_PREFIX")
    path.chmod(0o600)
    return path


def managed_pair(case):
    aid = case.file(mid="synthetic-ready-managed")
    original = next((case.base / "attachments/objects").glob(aid + ".*"))
    return {"objects": original, "staging": staged(case.store, case.now)}


def ordinary_pdf(case):
    path = case.home / "generated.pdf"
    path.write_bytes(b"%PDF-1.4\nSYNTHETIC GENERATED FILE\n%%EOF\n")
    return path


def database_rows(path):
    if not path.exists():
        return None
    with sqlite3.connect(str(path)) as db:
        names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {name: db.execute('SELECT * FROM "' + name + '" ORDER BY rowid').fetchall() for name in names}


def queue_files(store):
    roots = [Path(store.pending_dir), Path(store.sending_dir), Path(store.sent_dir), Path(store.failed_dir),
             Path(store.state_dir) / "reply_batches", Path(store.receipts_dir), Path(store.inbox_dir)]
    return {str(path): (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
            for directory in roots for path in directory.glob("*.json")}


def publication_snapshot(case):
    return {"queue": queue_files(case.store), "claims": copy.deepcopy(case.worker._claims()),
            "ledger": database_rows(case.base / "state/attachment-delivery.sqlite3"),
            "catalog": database_rows(case.base / "state/attachments.sqlite3")}


def historical_pending(case, target, caption="SYNTHETIC caption"):
    """Construct valid ordinary identity, then inject a pre-fix managed path."""
    batch = case.batch()
    result = case.worker.reply(batch["owned"], caption, batch["lease"], str(ordinary_pdf(case)))
    path = Path(case.store.pending_dir) / result["queued"]
    job = load_json(str(path))
    job["media_path"] = str(target)
    atomic_write_json(str(path), job)
    registry_path = case.worker.batch_dir / path.name
    registry = load_json(str(registry_path))
    registry["job"] = job
    atomic_write_json(str(registry_path), registry)
    return path, job, registry_path


class ManagedPathBatchTests(unittest.TestCase):
    def test_guard_covers_both_namespaces_aliases_and_raw_symlink_parent_order_without_body_or_catalog(self):
        with tempfile.TemporaryDirectory(prefix="synthetic-managed-pure-") as temp:
            home = Path(temp).resolve()
            base = home / "base"
            paths = {}
            for kind, name in (("objects", "known.bin"), ("staging", "reserved.part")):
                directory = base / "attachments" / kind
                directory.mkdir(parents=True, mode=0o700)
                paths[kind] = directory / name
                paths[kind].write_bytes(b"SYNTHETIC")
            pdf = home / "ordinary.pdf"
            pdf.write_bytes(b"SYNTHETIC ordinary")
            positives = list(paths.values())
            for kind, target in paths.items():
                file_alias = home / (kind + "-file-link")
                file_alias.symlink_to(target)
                directory_alias = home / (kind + "-directory-link")
                directory_alias.symlink_to(target.parent, target_is_directory=True)
                positives.extend((file_alias, directory_alias / target.name))
                case_alias = target.parent.with_name(kind.upper()) / target.name
                if case_alias.exists() and case_alias.samefile(target):
                    positives.append(case_alias)
                    case_target_link = home / (kind + "-case-target-file-link")
                    case_target_link.symlink_to(case_alias)
                    positives.append(case_target_link)
                    nested = target.parent / "nested"
                    nested.mkdir()
                    (nested / "SYNTHETIC.bin").write_bytes(b"SYNTHETIC nested")
                    positives.append(target.parent.with_name(kind.upper()) / "nested/SYNTHETIC.bin")
            # These retain '..' after a directory symlink. abspath-before-
            # resolve would inspect a different external spelling.
            positives.extend((home / "objects-directory-link/../staging" / paths["staging"].name,
                              home / "staging-directory-link/../objects" / paths["objects"].name))
            pdf_alias = home / "ordinary-link.pdf"
            pdf_alias.symlink_to(pdf)
            similar = base / "attachments/objects-generated"
            similar.mkdir()
            (similar / "ordinary.pdf").write_bytes(b"SYNTHETIC ordinary")
            before = sorted(str(p.relative_to(home)) for p in home.rglob("*"))
            real_open = os.open
            def metadata_only_open(path, flags, *args, **kwargs):
                if flags & getattr(os, "O_DIRECTORY", 0):
                    return real_open(path, flags, *args, **kwargs)
                raise AssertionError("guard must not open a file body")
            with patch.object(runtime, "catalog", side_effect=AssertionError("guard must not construct catalog")), \
                    patch.object(runtime, "initialize", side_effect=AssertionError("guard must not initialize")), \
                    patch.object(AttachmentStore, "__init__", side_effect=AssertionError("guard must not construct store")), \
                    patch("builtins.open", side_effect=AssertionError("guard must not read body")), \
                    patch.object(os, "open", side_effect=metadata_only_open):
                store = SimpleNamespace(base=str(base))
                for path in positives:
                    with self.subTest(path=path.name):
                        self.assertTrue(runtime.is_catalog_original_path(store, path))
                for path in (pdf, pdf_alias, similar / "ordinary.pdf"):
                    self.assertFalse(runtime.is_catalog_original_path(store, path))
                absent = home / "absent-runtime"
                self.assertFalse(runtime.is_catalog_original_path(SimpleNamespace(base=str(absent)), pdf))
                self.assertFalse(absent.exists())
                with patch.object(Path, "resolve", side_effect=OSError(errno.EIO, "SYNTHETIC path metadata IO")):
                    with self.assertRaises((OSError, ValueError)):
                        runtime.is_catalog_original_path(store, pdf)
            self.assertEqual(sorted(str(p.relative_to(home)) for p in home.rglob("*")), before)

    def test_muse_staging_prequeue_rejects_without_attempt_and_same_handle_can_send_generated_pdf(self):
        with muse_fixture() as case:
            stage = staged(case.store, case.now, "muse")
            alias = Path(case.tmp.name) / "stage-file-link"
            alias.symlink_to(stage)
            objects_alias = Path(case.tmp.name) / "objects-parent-link"
            objects_alias.symlink_to(case.original.parent, target_is_directory=True)
            raw = objects_alias / "../staging" / stage.name
            paths = [stage, alias, raw]
            for kind, target in (("objects", case.original), ("staging", stage)):
                case_target = target.parent.with_name(kind.upper()) / target.name
                if case_target.exists() and case_target.samefile(target):
                    case_link = Path(case.tmp.name) / (kind + "-muse-case-target-file-link")
                    case_link.symlink_to(case_target)
                    paths.append(case_link)
            before_session = case.session_path().read_bytes()
            before = publication_snapshot(case)
            for path in paths:
                with self.subTest(path=path.name):
                    result, count = case.counted(case.reply(path))
                    self.assertEqual(result["error"], "invalid_request")
                    self.assertFalse(result["must_stop"])
                    self.assertEqual(count, 0)
                    self.assertEqual(case.session_path().read_bytes(), before_session)
                    self.assertEqual(publication_snapshot(case), before)
                    case.assert_unpublished()
            result, count = case.counted(case.reply(case.pdf))
            self.assertTrue(result["ok"], result)
            self.assertEqual(count, 1)
            self.assertEqual(case.session()["phase"], "queued")

    def test_worker_reply_and_enqueue_reject_both_roots_before_publication(self):
        with runtime_fixture() as case:
            paths = managed_pair(case)
            batch = case.batch()
            before = publication_snapshot(case)
            for kind, path in paths.items():
                for entry in ("reply", "enqueue"):
                    with self.subTest(root=kind, entry=entry):
                        with self.assertRaises(ValueError):
                            if entry == "reply":
                                case.worker.reply(batch["owned"], "SYNTHETIC caption", batch["lease"], str(path))
                            else:
                                case.worker.enqueue_reply(batch["owned"], batch["to"], batch["context"],
                                    "SYNTHETIC caption", batch["lease"], str(path))
                        self.assertEqual(publication_snapshot(case), before)

    def test_cli_all_three_media_parameters_reject_both_roots_before_initialize_caption_budget_or_media(self):
        with runtime_fixture() as case:
            paths = managed_pair(case)
            context = load_json(case.store.last_ctx_path)["context_token"]
            before = publication_snapshot(case)
            cases = [(kind, path, option, caption) for kind, path in paths.items()
                     for option in ("image", "file", "video") for caption in ("", "SYNTHETIC caption")]
            for kind, target in paths.items():
                alias = case.home / (kind + "-cli-file-link")
                alias.symlink_to(target)
                directory = case.home / (kind + "-cli-directory-link")
                directory.symlink_to(target.parent, target_is_directory=True)
                cases.extend(((kind + "-file-alias", alias, "file", "SYNTHETIC caption"),
                              (kind + "-directory-alias", directory / target.name, "file", "SYNTHETIC caption")))
                case_alias = target.parent.with_name(kind.upper()) / target.name
                if case_alias.exists() and case_alias.samefile(target):
                    cases.append((kind + "-case-alias", case_alias, "file", "SYNTHETIC caption"))
                    case_target_link = case.home / (kind + "-cli-case-target-file-link")
                    case_target_link.symlink_to(case_alias)
                    cases.append((kind + "-case-target-link", case_target_link, "file", "SYNTHETIC caption"))
            raw = case.home / "objects-cli-directory-link/../staging" / paths["staging"].name
            cases.append(("raw-symlink-parent", raw, "file", "SYNTHETIC caption"))
            for kind, path, option, caption in cases:
                with self.subTest(root=kind, option=option, caption=bool(caption)), ExitStack() as stack:
                    args = argparse.Namespace(text=caption, image=None, file=None, video=None,
                        to=fixtures.OWNER, context=context)
                    setattr(args, option, str(path))
                    stack.enter_context(patch.object(cli, "get_store", return_value=case.store))
                    initialize = stack.enter_context(patch.object(runtime, "initialize",
                        side_effect=AssertionError("must reject before initialize")))
                    budget = stack.enter_context(patch.object(runtime, "send_budget",
                        side_effect=AssertionError("must reject before budget")))
                    text_send = stack.enter_context(patch.object(cli, "send_message"))
                    media_send = stack.enter_context(patch.object(cli, "send_media_message"))
                    upload = stack.enter_context(patch.object(media, "_get_upload_target"))
                    with redirect_stdout(io.StringIO()):
                        self.assertNotEqual(cli.cmd_send(args), 0)
                    for call in (initialize, budget, text_send, media_send, upload):
                        call.assert_not_called()
                    self.assertEqual(publication_snapshot(case), before)

    def test_cli_generated_pdf_still_reads_encrypts_uploads_and_sends_once(self):
        with runtime_fixture() as case:
            managed_pair(case)
            pdf = ordinary_pdf(case)
            context = load_json(case.store.last_ctx_path)["context_token"]
            args = argparse.Namespace(text="", image=None, file=str(pdf), video=None,
                to=fixtures.OWNER, context=context)
            messages, bodies = case.send_spy()
            with patch.object(cli, "get_store", return_value=case.store), redirect_stdout(io.StringIO()):
                self.assertEqual(cli.cmd_send(args), 0)
            self.assertEqual((len(messages), bodies), (1, [pdf.read_bytes()]))

    def test_historical_pending_and_direct_claimed_both_roots_do_not_split_caption_or_touch_accounting(self):
        for kind in ("objects", "staging"):
            with self.subTest(root=kind), runtime_fixture() as case:
                target = managed_pair(case)[kind]
                path, job, registry = historical_pending(case, target)
                before = publication_snapshot(case)
                messages, bodies = case.send_spy()
                case.flush()
                case.flush()
                self.assertEqual((messages, bodies), ([], []))
                self.assertEqual(publication_snapshot(case), before)
                self.assertFalse(list(Path(case.store.pending_dir).glob("*_caption.json")))
                with self.assertRaises(ValueError):
                    serve._split_caption(case.store, path.name, job)
                sending = Path(case.store.sending_dir) / path.name
                os.replace(str(path), str(sending))
                before = publication_snapshot(case)
                counts = {}
                self.assertTrue(serve._deliver_claimed(case.store, "https://synthetic.invalid", case.session["bot_token"],
                    counts, sending.name, job, str(sending), str(path), job["enqueued_at"], 0))
                self.assertEqual((counts, messages, bodies), ({}, [], []))
                self.assertEqual(publication_snapshot(case), before)

    def test_historical_sending_caption_and_real_generic_unknown_remain_unchanged(self):
        for kind in ("objects", "staging"):
            with self.subTest(root=kind), runtime_fixture() as case:
                target = managed_pair(case)[kind]
                batch = case.batch()
                queued = case.worker.reply(batch["owned"], "", batch["lease"], str(ordinary_pdf(case)))
                first_messages, first_bodies = case.send_spy(final_result=TimeoutError("SYNTHETIC uncertain send"))
                case.flush()
                self.assertEqual((len(first_messages), len(first_bodies)), (1, 1))
                path = Path(case.store.pending_dir) / queued["queued"]
                job = load_json(str(path))
                job.update(media_path=str(target), delivery_state="inflight", caption_split=True)
                sending = Path(case.store.sending_dir) / path.name
                atomic_write_json(str(sending), job)
                path.unlink()
                registry_path = case.worker.batch_dir / path.name
                record = load_json(str(registry_path))
                record["job"] = job
                atomic_write_json(str(registry_path), record)
                caption = Path(case.store.pending_dir) / (path.stem + "_caption.json")
                atomic_write_json(str(caption), dict(to_user_id=job["to_user_id"], text="SYNTHETIC old caption",
                    context_token=job["context_token"], kind="caption", login_epoch=job["login_epoch"],
                    enqueued_at=job["enqueued_at"], delivery_state="inflight", attempts=1))
                before = publication_snapshot(case)
                self.assertIn("unknown", str(before["ledger"]))
                messages, bodies = case.send_spy()
                serve._recover_sending(case.store)
                case.flush()
                self.assertEqual(publication_snapshot(case), before)
                self.assertEqual((messages, bodies), ([], []))
                self.assertFalse(path.exists())

    def test_registry_only_caption_and_prepared_recovery_both_roots_are_retained(self):
        for kind in ("objects", "staging"):
            with self.subTest(root=kind), runtime_fixture() as case:
                target = managed_pair(case)[kind]
                pending, job, registry_path = historical_pending(case, target)
                record = load_json(str(registry_path))
                record.update(phase="prepared", admitted=False)
                atomic_write_json(str(registry_path), record)
                pending.unlink()
                before = publication_snapshot(case)
                with self.assertRaises(ValueError):
                    case.worker.recover_prepared(pending.name)
                self.assertEqual(publication_snapshot(case), before)
                caption = Path(case.store.pending_dir) / (pending.stem + "_caption.json")
                atomic_write_json(str(caption), dict(to_user_id=job["to_user_id"], text="SYNTHETIC orphan caption",
                    context_token=job["context_token"], kind="caption", enqueued_at=job["enqueued_at"]))
                before = publication_snapshot(case)
                messages, bodies = case.send_spy()
                case.flush()
                self.assertEqual(publication_snapshot(case), before)
                self.assertEqual((messages, bodies), ([], []))


if __name__ == "__main__":
    unittest.main()
