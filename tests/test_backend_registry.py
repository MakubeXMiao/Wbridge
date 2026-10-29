"""Offline registry/CLI boundary tests; no real auth, model, or WeChat calls."""

import contextlib
import copy
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from wechat_bridge import cli
from wechat_bridge.backends.contract import BackendError, validate_event
from wechat_bridge.backends import registry
from wechat_bridge.store import Store, atomic_write_json


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(os.path.join(self.temp.name, "runtime"))
        self.config = {
            "schema_version": 1, "account_scope": "synthetic-bot-a",
            "default_backend": "codex",
            "backends": {"codex": {"instance_id": "synthetic-instance-a",
                                     "command": [sys.executable], "cwd": self.temp.name}},
        }
        self.session = {"bot_token": "SYNTHETIC_SECRET_TOKEN", "ilink_user_id": "synthetic-owner",
                        "ilink_bot_id": "synthetic-bot"}

    def write_config(self, value=None):
        atomic_write_json(os.path.join(self.store.state_dir, "backends.json"),
                          self.config if value is None else value)

    def test_missing_config_preserves_legacy_and_no_directory_creation(self):
        absent = os.path.join(self.temp.name, "does-not-exist")
        store = SimpleNamespace(state_dir=os.path.join(absent, "state"))
        self.assertIsNone(registry.load_config(store))
        self.assertEqual(registry.preflight(store, None, None)["mode"], "legacy")
        self.assertFalse(os.path.exists(absent))

    def test_normalized_copy_defaults_and_provider_extras(self):
        self.config["backends"]["codex"]["provider_private"] = {"auth": "SYNTHETIC_SECRET"}
        original = copy.deepcopy(self.config)
        validated = registry.validate_config(self.config)
        self.assertEqual(self.config, original)
        self.assertEqual(validated["job_timeout_seconds"], 1800)
        self.assertEqual(validated["approval_timeout_seconds"], 300)
        self.assertEqual(validated["backends"]["codex"]["execution_policy"],
                         {"approval": "explicit", "sandbox": "workspace-write"})
        validated["backends"]["codex"]["provider_private"]["auth"] = "changed"
        self.assertEqual(self.config, original)

    def test_root_and_timeout_validation(self):
        changes = [("schema_version", True), ("schema_version", 2), ("account_scope", ""),
                   ("default_backend", "other"), ("job_timeout_seconds", 0),
                   ("job_timeout_seconds", 86401), ("job_timeout_seconds", True),
                   ("approval_timeout_seconds", 301), ("approval_timeout_seconds", 2.5),
                   ("unexpected", "private")]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                config = copy.deepcopy(self.config)
                config[key] = value
                with self.assertRaises(BackendError):
                    registry.validate_config(config)

    def test_invalid_backend_shape(self):
        for value in (None, [], {}, {"unknown": {}}, {"codex": []}):
            with self.subTest(value=value):
                config = copy.deepcopy(self.config)
                config["backends"] = value
                with self.assertRaises(BackendError):
                    registry.validate_config(config)

    def test_runtime_path_validation_and_explicit_test_bypass(self):
        self.config["backends"]["codex"]["command"] = ["/nonexistent/wbridge-test-binary"]
        with self.assertRaisesRegex(BackendError, "executable"):
            registry.validate_config(self.config)
        registry.validate_config(self.config, check_runtime=False)
        self.config["backends"]["codex"]["command"] = [sys.executable]
        self.config["backends"]["codex"]["cwd"] = "/nonexistent/wbridge-test-cwd"
        with self.assertRaisesRegex(BackendError, "directory"):
            registry.validate_config(self.config)

    def test_relative_and_non_executable_command_rejected(self):
        for command in ("shell text", ["codex"], [], [True], [sys.executable, "\x00"]):
            with self.subTest(command=command):
                self.config["backends"]["codex"]["command"] = command
                with self.assertRaises(BackendError):
                    registry.validate_config(self.config)
        binary = os.path.join(self.temp.name, "nonexecutable")
        with open(binary, "w") as handle:
            handle.write("#!/bin/sh\nexit 0\n")
        self.config["backends"]["codex"]["command"] = [binary]
        with self.assertRaises(BackendError):
            registry.validate_config(self.config)

    def test_policy_and_command_cannot_bypass_permissions(self):
        for policy in ({"approval": "never"}, {"approval": "bypass"},
                       {"sandbox": "unrestricted"}, {"sandbox": "danger-full-access"},
                       {"permissionMode": "bypassPermissions"}):
            with self.subTest(policy=policy):
                self.config["backends"]["codex"]["execution_policy"] = policy
                with self.assertRaises(BackendError):
                    registry.validate_config(self.config)
        self.config["backends"]["codex"].pop("execution_policy")
        for flag in ("--dangerously-bypass-approvals-and-sandbox", "--dangerously-skip-permissions",
                     "--permission-mode=bypassPermissions", "--approval-policy=never", "--sandbox=read-only"):
            with self.subTest(flag=flag):
                self.config["backends"]["codex"]["command"] = [sys.executable, flag]
                with self.assertRaises(BackendError):
                    registry.validate_config(self.config)

    def test_muse_requires_explicit_existing_hook_state(self):
        config = {"schema_version": 1, "account_scope": "synthetic", "default_backend": "muse",
                  "backends": {"muse": {}}}
        with self.assertRaises(BackendError):
            registry.validate_config(config)
        config["legacy_state_dir"] = self.temp.name
        self.assertEqual(registry.validate_config(config)["backends"]["muse"], {})
        # It is an external Hook-state directory, not Store.base or state_dir.
        self.assertNotEqual(config["legacy_state_dir"], self.store.base)
        config["backends"]["muse"] = {"command": [sys.executable]}
        with self.assertRaises(BackendError):
            registry.validate_config(config)

    def test_malformed_config_never_falls_back_to_legacy(self):
        path = os.path.join(self.store.state_dir, "backends.json")
        for text in ("{", "null", '{"schema_version":1,"schema_version":2}',
                     '{"schema_version":NaN}', '[1,2]'):
            with self.subTest(text=text):
                with open(path, "w") as handle:
                    handle.write(text)
                with self.assertRaises(BackendError):
                    registry.load_config(self.store)

    def test_load_valid_config(self):
        self.write_config()
        self.assertEqual(registry.load_config(self.store), registry.validate_config(self.config))

    def test_broken_config_link_does_not_fall_back(self):
        path = os.path.join(self.store.state_dir, "backends.json")
        os.symlink(os.path.join(self.temp.name, "missing.json"), path)
        with self.assertRaisesRegex(BackendError, "unavailable config"):
            registry.load_config(self.store)

    def test_missing_config_with_managed_input_or_db_fails_closed(self):
        inbox = os.path.join(self.store.base, "backend_inbox")
        os.mkdir(inbox)
        record = os.path.join(inbox, "synthetic.json")
        with open(record, "w") as handle:
            handle.write("{}")
        with self.assertRaisesRegex(BackendError, "managed input"):
            registry.load_config(self.store)
        os.remove(record)
        with self.assertRaisesRegex(BackendError, "managed input"):
            registry.load_config(self.store)
        database = os.path.join(self.store.state_dir, "backend_jobs.sqlite3")
        with open(database, "w") as handle:
            handle.write("")
        with self.assertRaisesRegex(BackendError, "managed database"):
            registry.load_config(self.store)

    def test_existing_db_binding_checks_profile_without_writes(self):
        from wechat_bridge.backend_state import BackendState
        normalized = registry.validate_config(self.config)
        state = BackendState(self.store, normalized, self.session["ilink_user_id"], self.session["ilink_bot_id"])
        with open(state.path, "rb") as handle:
            before = handle.read()
        with mock.patch.object(registry.importlib, "import_module", return_value=SimpleNamespace(create_backend=mock.Mock())):
            registry.preflight(self.store, normalized, self.session)
            changed = copy.deepcopy(normalized)
            changed["backends"]["codex"]["instance_id"] = "other"
            with self.assertRaisesRegex(BackendError, "identity/config changed"):
                registry.preflight(self.store, changed, self.session)
            changed_owner = dict(self.session, ilink_user_id="other-owner")
            with self.assertRaisesRegex(BackendError, "identity/config changed"):
                registry.preflight(self.store, normalized, changed_owner)
        with open(state.path, "rb") as handle:
            self.assertEqual(before, handle.read())

    def test_preflight_checks_factory_without_instantiating_or_exposing_secrets(self):
        self.config["backends"]["codex"]["env"] = {"API_KEY": "SYNTHETIC_SECRET_ENV"}
        factory = mock.Mock(side_effect=AssertionError("preflight must not construct drivers"))
        with mock.patch.object(registry.importlib, "import_module",
                               return_value=SimpleNamespace(create_backend=factory)):
            result = registry.preflight(self.store, self.config, self.session)
        factory.assert_not_called()
        serialized = json.dumps(result)
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider_auth"], "not_checked")
        self.assertNotIn("SYNTHETIC_SECRET", serialized)
        self.assertNotIn(self.temp.name, serialized)
        self.assertNotIn("synthetic-owner", serialized)

    def test_preflight_rejects_missing_identity_and_invalid_login(self):
        for field in self.session:
            with self.subTest(field=field):
                session = dict(self.session)
                session.pop(field)
                with self.assertRaisesRegex(BackendError, "saved WeChat"):
                    registry.preflight(self.store, self.config, session)
        with open(self.store.needs_relogin_path, "w") as handle:
            handle.write("synthetic")
        with self.assertRaisesRegex(BackendError, "invalid"):
            registry.preflight(self.store, self.config, self.session)

    def test_missing_adapter_is_clear_and_sanitized(self):
        with mock.patch.object(registry.importlib, "import_module",
                               side_effect=ImportError("SYNTHETIC_SECRET_IMPORT")):
            with self.assertRaisesRegex(BackendError, "codex adapter unavailable") as caught:
                registry.preflight(self.store, self.config, self.session)
        self.assertNotIn("SECRET", str(caught.exception))

    def test_create_backend_copies_config_and_requires_methods(self):
        driver = SimpleNamespace(**{method: mock.Mock() for method in registry._DRIVER_METHODS})
        factory = mock.Mock(return_value=driver)
        selected = self.config["backends"]["codex"]
        with mock.patch.object(registry.importlib, "import_module",
                               return_value=SimpleNamespace(create_backend=factory)):
            self.assertIs(registry.create_backend("codex", selected), driver)
        factory.call_args[0][0]["cwd"] = "changed"
        self.assertEqual(selected["cwd"], self.temp.name)
        driver.start.assert_not_called()
        with mock.patch.object(registry.importlib, "import_module",
                               return_value=SimpleNamespace(create_backend=lambda _: object())):
            with self.assertRaisesRegex(BackendError, "v1 driver"):
                registry.create_backend("codex", selected)

    def test_cli_readonly_missing_directory(self):
        base = os.path.join(self.temp.name, "absent-runtime")
        with mock.patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": base}), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(["backend-check"]), 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["mode"], "legacy")
        self.assertFalse(os.path.exists(base))

    def test_cli_status_reads_only_job_counts_and_redacts_unknown_status(self):
        database = os.path.join(self.store.state_dir, "backend_jobs.sqlite3")
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE jobs(id TEXT, status TEXT, payload TEXT)")
            connection.executemany("INSERT INTO jobs VALUES (?, ?, ?)", [
                ("a", "unknown", "SYNTHETIC_SECRET_PAYLOAD"),
                ("b", "unknown", "SYNTHETIC_SECRET_PAYLOAD"),
                ("c", "SYNTHETIC_SECRET_STATUS", "SYNTHETIC_SECRET_PAYLOAD")])
        with open(database, "rb") as handle:
            before = handle.read()
        with mock.patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": self.store.base}), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(["backends", "status"]), 1)
        result = json.loads(output.getvalue())
        self.assertEqual(result["state"]["jobs"], {"unknown": 2, "unrecognized": 1})
        self.assertNotIn("SECRET", output.getvalue())
        with open(database, "rb") as handle:
            self.assertEqual(before, handle.read())

    def test_cli_invalid_database_reports_failure(self):
        database = os.path.join(self.store.state_dir, "backend_jobs.sqlite3")
        with open(database, "w") as handle:
            handle.write("not sqlite")
        with mock.patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": self.store.base}), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(["backend-check"]), 1)
        self.assertEqual(json.loads(output.getvalue())["state"]["database"], "unavailable_or_incompatible")

    def test_serve_preflight_failure_does_not_start_transport(self):
        self.write_config()
        atomic_write_json(self.store.session_path, self.session)
        with mock.patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": self.store.base}), \
                mock.patch.object(registry.importlib, "import_module", side_effect=ImportError()), \
                mock.patch.object(cli, "serve_run") as serve, contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(["serve"]), 1)
        serve.assert_not_called()
        self.assertIn("后端预检失败", output.getvalue())
        self.assertNotIn("SYNTHETIC_SECRET", output.getvalue())

    def test_serve_legacy_no_preflight_keeps_existing_arguments(self):
        atomic_write_json(self.store.session_path, self.session)
        with mock.patch.dict(os.environ, {"WECHAT_BRIDGE_DIR": self.store.base}), \
                mock.patch.object(registry, "preflight") as preflight, \
                mock.patch.object(cli, "serve_run", return_value=7) as serve:
            self.assertEqual(cli.main(["serve"]), 7)
        preflight.assert_not_called()
        self.assertEqual(serve.call_args[0][2:],
                         (self.session["bot_token"], self.session["ilink_user_id"]))


class EventTests(unittest.TestCase):
    def event(self, kind="final", **fields):
        result = {"event_id": "e1", "execution_id": "j1", "attempt_id": "a1",
                  "type": kind, "text": "synthetic result"}
        result.update(fields)
        return result

    def test_valid_final_is_copy(self):
        event = self.event()
        self.assertEqual(validate_event(event, "j1", "a1"), event)
        self.assertIsNot(validate_event(event, "j1", "a1"), event)

    def test_wrong_identity_empty_final_and_unknown_types_rejected(self):
        for event in (self.event(execution_id="other"), self.event(attempt_id="other"),
                      self.event(text=" "), self.event(event_id=""), self.event(kind="invalid")):
            with self.subTest(event=event), self.assertRaises(BackendError):
                validate_event(event, "j1", "a1")

    def test_approval_digest_summary_expiry_validation(self):
        event = self.event("approval_requested", approval_id="provider1",
                           action_summary="write synthetic file", action_digest="a" * 64,
                           expires_at=1900000000)
        validate_event(event, "j1", "a1")
        for key, value in (("action_digest", "A" * 64), ("action_summary", ""),
                           ("expires_at", True), ("expires_at", float("nan"))):
            invalid = dict(event)
            invalid[key] = value
            with self.subTest(key=key), self.assertRaises(BackendError):
                validate_event(invalid, "j1", "a1")

    def test_failure_needs_explicit_side_effect_state(self):
        event = self.event("failed", error_code="provider_error", side_effect_state="unknown")
        validate_event(event, "j1", "a1")
        event["side_effect_state"] = []
        with self.assertRaises(BackendError):
            validate_event(event, "j1", "a1")

    def test_contract_bounds_match_dispatcher(self):
        validate_event(self.event(event_id="e" * 256, text="x" * 64000,
                                  session_id="s" * 1024), "j1", "a1")
        for field, value in (("event_id", "e" * 257), ("text", "x" * 64001),
                             ("session_id", "s" * 1025)):
            with self.subTest(field=field), self.assertRaises(BackendError):
                validate_event(self.event(**{field: value}), "j1", "a1")
        approval = self.event("approval_requested", approval_id="p" * 257,
                              action_summary="synthetic action", action_digest="a" * 64)
        with self.assertRaises(BackendError):
            validate_event(approval, "j1", "a1")


if __name__ == "__main__":
    unittest.main()
