"""Synthetic metadata and real local-budget integration for download cautions."""
from contextlib import ExitStack
import importlib
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from wechat_bridge.attachment_caution import (
    DOWNLOAD_CAUTION, append_download_caution, needs_download_caution,
)


def metadata(name=None, mime=None):
    return {"original_name": name, "declared_type": mime}


class CautionPolicyTests(unittest.TestCase):
    def test_known_executable_installer_and_macro_names(self):
        for name in ("SYNTHETIC.exe", "SYNTHETIC.MSI", "SYNTHETIC.pkg", "SYNTHETIC.dmg",
                     "SYNTHETIC.apk", "SYNTHETIC.command", "SYNTHETIC.ps1",
                     "SYNTHETIC.docm", "SYNTHETIC.XLSM", "SYNTHETIC.xlam", "SYNTHETIC.pptm"):
            with self.subTest(name=name):
                self.assertTrue(needs_download_caution(name))

    def test_known_mime_signal_does_not_require_or_trust_filename(self):
        for mime in ("application/x-msdownload", "application/vnd.android.package-archive",
                     "application/vnd.ms-word.document.macroEnabled.12",
                     "application/vnd.ms-excel.sheet.macroEnabled.12"):
            self.assertTrue(needs_download_caution(None, mime))
            self.assertTrue(needs_download_caution("SYNTHETIC.pdf", mime))

    def test_mime_parameters_case_and_whitespace_are_metadata_only(self):
        self.assertTrue(needs_download_caution(None, "  APPLICATION/X-MSDOWNLOAD ; charset=UTF-8"))
        self.assertFalse(needs_download_caution(None, "application/pdf; charset=UTF-8"))

    def test_normal_image_pdf_and_nonmacro_documents_have_no_extra_caption(self):
        for name, mime in (("SYNTHETIC.png", "image/png"), ("SYNTHETIC.jpg", "image/jpeg"),
                           ("SYNTHETIC.pdf", "application/pdf"), ("SYNTHETIC.docx", None),
                           ("SYNTHETIC.xlsx", None), ("SYNTHETIC.txt", "text/plain")):
            self.assertFalse(needs_download_caution(name, mime))
            self.assertEqual(append_download_caution("SYNTHETIC caption", [metadata(name, mime)]), "SYNTHETIC caption")

    def test_unknown_is_no_known_signal_not_a_safety_verdict(self):
        for name in (None, "", "SYNTHETIC.unknown", "SYNTHETIC.bin", "SYNTHETIC.zip"):
            self.assertFalse(needs_download_caution(name, "application/octet-stream"))
        self.assertFalse(needs_download_caution("SYNTHETIC.exe.txt"))
        self.assertFalse(needs_download_caution(None, "application/not-real-msdownload"))

    def test_unicode_and_filename_disguises_do_not_become_paths(self):
        for name in ("房东安装包.EXE", "SYNTHETIC．ｅｘｅ", "SYNTHETIC.exe. ",
                     "SYNTHETIC.pdf\u202e.exe", "../../SYNTHETIC.xlsm", "C:\\SYNTHETIC\\run.cmd"):
            self.assertTrue(needs_download_caution(name))
        self.assertFalse(needs_download_caution("房东照片😀.png"))

    def test_caption_is_unchanged_except_for_the_one_fixed_tail(self):
        caption = "SYNTHETIC 原有配文😀  "
        result = append_download_caution(caption, [metadata("SYNTHETIC.exe"), metadata("SYNTHETIC.xlsm")])
        self.assertEqual(result, caption + "\n" + DOWNLOAD_CAUTION)
        self.assertEqual(result.count(DOWNLOAD_CAUTION), 1)
        self.assertEqual(append_download_caution(result, [metadata("SYNTHETIC.exe")]), result)

    def test_existing_exact_notice_anywhere_is_not_duplicated(self):
        caption = DOWNLOAD_CAUTION + "\nSYNTHETIC caption"
        self.assertEqual(append_download_caution(caption, [metadata("SYNTHETIC.pkg")]), caption)
        self.assertEqual(append_download_caution("SYNTHETIC\n", [metadata("SYNTHETIC.pkg")]), "SYNTHETIC\n" + DOWNLOAD_CAUTION)

    def test_empty_caption_creates_only_the_budgeted_notice(self):
        self.assertEqual(append_download_caution("", [metadata("SYNTHETIC.exe")]), DOWNLOAD_CAUTION)
        self.assertEqual(append_download_caution("", [metadata("SYNTHETIC.pdf")]), "")
        self.assertEqual(append_download_caution("SYNTHETIC", []), "SYNTHETIC")

    def test_machine_suffix_is_bounded_without_changing_legacy_cli_input_cap(self):
        self.assertLessEqual(len(DOWNLOAD_CAUTION) + 1, 128)
        for size in (16000, 18001):
            caption = "C" * size
            result = append_download_caution(caption, [metadata("SYNTHETIC.exe")])
            self.assertTrue(result.startswith(caption))
            self.assertLessEqual(len(result) - len(caption), 128)
            if size == 16000:
                self.assertLessEqual(len(result), 16128)

    def test_invalid_metadata_types_and_unicode_are_rejected_without_echo(self):
        for name, mime in ((True, None), (b"SYNTHETIC.exe", None), (None, False),
                           ("SYNTHETIC\x00.exe", None), ("\ud800.exe", None)):
            with self.assertRaisesRegex(ValueError, "^invalid_attachment_metadata$"):
                needs_download_caution(name, mime)
        for caption, items in ((False, []), ("SYNTHETIC", {}), ("SYNTHETIC", [True])):
            with self.assertRaisesRegex(ValueError, "^invalid_attachment_metadata$"):
                append_download_caution(caption, items)

    def test_metadata_bounds_are_local_and_exact(self):
        self.assertTrue(needs_download_caution("x" * 4092 + ".exe"))
        for name, mime in (("x" * 4093 + ".exe", None), (None, "x" * 257)):
            with self.assertRaises(ValueError):
                needs_download_caution(name, mime)

    def test_helper_does_not_read_execute_unpack_or_connect(self):
        def forbidden(*args, **kwargs):
            raise AssertionError("metadata helper attempted file/process/network I/O")
        with ExitStack() as stack:
            for name in ("builtins.open", "os.open", "os.system", "subprocess.run", "subprocess.Popen",
                         "pathlib.Path.read_bytes", "urllib.request.urlopen", "socket.create_connection"):
                stack.enter_context(patch(name, side_effect=forbidden))
            self.assertTrue(needs_download_caution("../../$(not-executed).docm"))
            self.assertEqual(append_download_caution("", [metadata("SYNTHETIC.exe")]), DOWNLOAD_CAUTION)


class CautionRuntimeTests(unittest.TestCase):
    """Reuse only existing synthetic setup/helpers; do not inherit its cases."""
    def setUp(self):
        sys.path.insert(0, str(ROOT / "tests"))
        self.fixtures = importlib.import_module("test_attachment_runtime")
        self.fixture = self.fixtures.RuntimeTests(methodName="setUp")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()

    def test_public_multiple_risky_attachments_have_one_budgeted_caption(self):
        f = self.fixture
        ids = [f.file(name="SYNTHETIC.exe"), f.file(name="SYNTHETIC.xlsm"), f.file(name="SYNTHETIC.pdf")]
        batch = f.batch()
        result = f.success(f.call(batch, "reply", {"attachment_ids": ids, "caption": "SYNTHETIC caption"}))
        ledger = self.fixtures.runtime.ledger(f.worker.store)
        job = ledger.job_status(f.scope, result["job_id"])
        texts = [unit["text"] for unit in job["units"] if unit["kind"] == "text"]
        self.assertEqual(len(texts), 1)
        self.assertEqual(texts[0], "SYNTHETIC caption\n" + DOWNLOAD_CAUTION)
        self.assertEqual(texts[0].count(DOWNLOAD_CAUTION), 1)
        messages, originals = f.send_spy()
        f.flush()
        self.assertEqual(len(messages), 4)
        self.assertEqual(len(originals), 3)
        self.assertEqual(ledger.register_context(f.scope, batch["context"])["used"], 4)

    def test_public_png_pdf_empty_caption_has_no_extra_physical_unit(self):
        f = self.fixture
        image = f.receive(f.message(attachments=[("image", self.fixtures.png(), None)]))
        pdf = f.file(name="SYNTHETIC.pdf")
        ids = [image["attachment_refs"][0]["attachment_id"], pdf]
        batch = f.batch()
        result = f.success(f.call(batch, "reply", {"attachment_ids": ids}))
        ledger = self.fixtures.runtime.ledger(f.worker.store)
        job = ledger.job_status(f.scope, result["job_id"])
        self.assertEqual([unit["kind"] for unit in job["units"]], ["attachment", "attachment"])
        messages, originals = f.send_spy()
        f.flush()
        self.assertEqual(len(messages), 2)
        self.assertEqual(len(originals), 2)
        self.assertEqual(ledger.register_context(f.scope, batch["context"])["used"], 2)

    def test_actual_cli_empty_caption_risky_file_consumes_two_physical_units(self):
        f = self.fixture
        row = f.receive(f.message(text="SYNTHETIC CLI request"))
        source = f.home / "SYNTHETIC.exe"
        source.write_bytes(b"SYNTHETIC original")
        args = SimpleNamespace(text="", image=None, file=str(source), video=None,
                               to=self.fixtures.OWNER, context=row["context_token"])
        messages, originals = f.send_spy()
        with patch.object(self.fixtures.cli, "get_store", return_value=f.store), patch("builtins.print"):
            self.assertEqual(self.fixtures.cli.cmd_send(args), 0)
        self.assertEqual(messages[0]["item_list"][0]["text_item"]["text"], DOWNLOAD_CAUTION)
        self.assertEqual(len(messages), 2)
        self.assertEqual(originals, [b"SYNTHETIC original"])
        self.assertEqual(self.fixtures.runtime.ledger(f.store).register_context(f.scope, row["context_token"])["used"], 2)

    def test_ordinary_worker_media_job_repeated_flush_does_not_repeat_caution(self):
        f = self.fixture
        f.receive(f.message(text="SYNTHETIC ordinary reply"))
        batch = f.batch()
        source = f.home / "SYNTHETIC.xlsm"
        source.write_bytes(b"SYNTHETIC original")
        reply = f.worker.reply(batch["owned"], "", batch["lease"], media_path=str(source))
        self.assertTrue(reply.get("queued"), reply)
        messages, originals = f.send_spy()
        f.flush()
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["item_list"][0]["text_item"]["text"], DOWNLOAD_CAUTION)
        self.assertEqual(originals, [b"SYNTHETIC original"])
        f.flush()
        self.assertEqual(len(messages), 2)
        self.assertEqual(self.fixtures.runtime.ledger(f.store).register_context(f.scope, batch["context"])["used"], 2)

    def test_actual_cli_caution_cannot_bypass_last_physical_budget_slot(self):
        f = self.fixture
        row = f.receive(f.message(text="SYNTHETIC CLI request"))
        args = SimpleNamespace(text="C" * 14000, image=None, file=None, video=None,
                               to=self.fixtures.OWNER, context=row["context_token"])
        source = f.home / "SYNTHETIC.pkg"
        source.write_bytes(b"SYNTHETIC original")
        messages, originals = f.send_spy()
        with patch.object(self.fixtures.cli, "get_store", return_value=f.store), patch("builtins.print"):
            self.assertEqual(self.fixtures.cli.cmd_send(args), 0)
            args.text, args.file = "", str(source)
            self.assertEqual(self.fixtures.cli.cmd_send(args), 1)
        self.assertEqual(len(messages), 8)
        self.assertEqual(messages[-1]["item_list"][0]["text_item"]["text"], DOWNLOAD_CAUTION)
        self.assertEqual(originals, [])
        self.assertEqual(self.fixtures.runtime.ledger(f.store).register_context(f.scope, row["context_token"])["remaining"], 0)


if __name__ == "__main__":
    unittest.main()
