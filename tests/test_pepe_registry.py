"""Offline HTTP configuration and honest readiness checks; no credentials/I/O."""
import copy
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from wechat_bridge.backends import registry
from wechat_bridge.backends.contract import BackendError
from wechat_bridge.pepe_core import require_pepe_dispatch_policy
from wechat_bridge.store import Store


class PepeRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.selected = {
            "provider": "pepe", "transport": "http", "instance_id": "pepe-test",
            "base_url": "https://dot2api.example.invalid/", "credential_env": "SYNTHETIC_DOT_TOKEN",
            "tenant": "test-tenant", "principal": "test-principal", "queue": "pepe",
            "queue_ownership": "single_store", "instructions": "Answer this text.\nNo session reuse.",
            "execution_policy": {"mode": "text_only", "enforcement": "unverified"},
        }
        self.config = {"schema_version": 1, "account_scope": "test-account",
                       "default_backend": "pepe", "backends": {"pepe": self.selected}}

    def test_explicit_http_config_needs_neither_executable_nor_directory(self):
        before = copy.deepcopy(self.config)
        result = registry.validate_config(self.config)
        self.assertEqual(self.config, before)
        self.assertEqual(result["backends"]["pepe"]["base_url"], "https://dot2api.example.invalid")
        self.assertNotIn("command", result["backends"]["pepe"])
        self.assertNotIn("cwd", result["backends"]["pepe"])
        result["backends"]["pepe"]["execution_policy"]["mode"] = "changed"
        self.assertEqual(self.selected["execution_policy"]["mode"], "text_only")

    def test_cli_and_muse_can_coexist_without_policy_change(self):
        self.config["backends"].update({"codex": {"instance_id": "cli", "command": [sys.executable],
                                                 "cwd": self.temp.name}, "muse": {}})
        self.config["legacy_state_dir"] = self.temp.name
        result = registry.validate_config(self.config)
        self.assertEqual(result["backends"]["muse"], {})
        self.assertEqual(result["backends"]["codex"]["execution_policy"],
                         {"approval": "explicit", "sandbox": "workspace-write"})

    def test_preflight_cannot_accept_a_pepe_timeout_that_never_fits_minimum_ttl(self):
        for timeout in (1, 60, 64, 65):
            with self.subTest(timeout=timeout):
                changed = dict(self.config, job_timeout_seconds=timeout)
                with self.assertRaisesRegex(BackendError, "at least 66"):
                    registry.validate_config(changed)
        registry.validate_config(dict(self.config, job_timeout_seconds=66))

    def test_http_schema_rejects_fake_cli_and_inline_credentials(self):
        for field in ("command", "cwd", "token", "api_key", "headers", "env", "model"):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.config)
                changed["backends"]["pepe"][field] = "SYNTHETIC_SECRET"
                with self.assertRaises(BackendError) as caught:
                    registry.validate_config(changed)
                self.assertNotIn("SYNTHETIC_SECRET", str(caught.exception))

    def test_missing_or_malformed_identity_never_falls_back(self):
        for field in self.selected:
            with self.subTest(field=field):
                changed = copy.deepcopy(self.config)
                del changed["backends"]["pepe"][field]
                with self.assertRaises(BackendError):
                    registry.validate_config(changed)
        for field, value in (("provider", "codex"), ("transport", "stdio"), ("tenant", ""),
                             ("principal", True), ("principal", " principal"), ("instance_id", "instance "),
                             ("queue", "x\n"), ("queue", "队列"), ("queue", "q" * 129),
                             ("queue", "_prefix"), ("tenant", "tenant "), ("queue_ownership", "shared"),
                             ("credential_env", "token=SECRET"), ("credential_env", 1)):
            with self.subTest(field=field, value=value):
                changed = copy.deepcopy(self.config)
                changed["backends"]["pepe"][field] = value
                with self.assertRaises(BackendError):
                    registry.validate_config(changed)

    def test_unverified_policy_is_not_a_configurable_bypass(self):
        for policy in ({"approval": "none"}, {"approval": "explicit"},
                       {"mode": "text_only", "enforcement": "verified"},
                       {"mode": "text_only", "enforcement": True},
                       {"mode": "text_only", "enforcement": "unverified", "bypass": True}):
            with self.subTest(policy=policy):
                changed = copy.deepcopy(self.config)
                changed["backends"]["pepe"]["execution_policy"] = policy
                with self.assertRaises(BackendError):
                    registry.validate_config(changed)
        with self.assertRaisesRegex(BackendError, "pepe_execution_policy_unverified"):
            require_pepe_dispatch_policy(self.selected)

    def test_native_delegation_is_explicit_and_does_not_certify_platform_controls(self):
        changed = copy.deepcopy(self.config)
        selected = changed["backends"]["pepe"]
        selected["execution_policy"] = {"mode": "native_delegate", "approval": "platform_native"}
        normalized = registry.validate_config(changed)
        require_pepe_dispatch_policy(normalized["backends"]["pepe"])
        session = {"bot_token": "SYNTHETIC_WECHAT_SECRET", "ilink_user_id": "owner", "ilink_bot_id": "bot"}
        with patch.object(registry.importlib, "import_module", return_value=SimpleNamespace(create_backend=Mock())):
            result = registry.preflight(Store(self.temp.name), normalized, session)
        provider = result["backends"][0]
        self.assertEqual(provider["dispatch"], "native_delegation_configured")
        self.assertEqual(provider["policy_enforcement"], "not_checked")
        self.assertEqual(provider["approval"], "platform_native_not_checked")
        self.assertEqual(provider["wechat_tool_approval"], "unsupported")
        self.assertEqual(provider["provider_auth"], "not_checked")

    def test_transport_url_cannot_embed_credentials_or_redirect_parameters(self):
        for url in ("http://remote.example.invalid", "file:///tmp/service", "https://user:SECRET@host",
                    "https://host?q=SECRET", "https://host/#SECRET", "https://host:bad", "https://host:0",
                    "https://host\\evil", "https://host/ bad", "https://host/api", "https://"):
            with self.subTest(url=url):
                changed = copy.deepcopy(self.config)
                changed["backends"]["pepe"]["base_url"] = url
                with self.assertRaises(BackendError) as caught:
                    registry.validate_config(changed)
                self.assertNotIn("SECRET", str(caught.exception))
        for url in ("http://localhost:18792", "http://127.0.0.1:18792", "http://[::1]:18792"):
            changed = copy.deepcopy(self.config)
            changed["backends"]["pepe"]["base_url"] = url
            registry.validate_config(changed)

    def test_preflight_imports_adapter_only_and_reports_real_dispatch_block(self):
        store = Store(self.temp.name)
        session = {"bot_token": "SYNTHETIC_WECHAT_SECRET", "ilink_user_id": "owner", "ilink_bot_id": "bot"}
        factory = Mock(side_effect=AssertionError("preflight must not instantiate"))
        with patch.object(registry.importlib, "import_module", return_value=SimpleNamespace(create_backend=factory)), \
                patch.dict(os.environ, {"SYNTHETIC_DOT_TOKEN": "SYNTHETIC_REMOTE_SECRET"}):
            result = registry.preflight(store, self.config, session)
        factory.assert_not_called()
        self.assertTrue(result["ok"])
        self.assertEqual(result["backends"][0]["dispatch"], "blocked_execution_policy_unverified")
        serialized = json.dumps(result)
        self.assertNotIn("SECRET", serialized)
        self.assertNotIn("test-principal", serialized)
        self.assertNotIn("https://", serialized)
        self.assertFalse(os.path.exists(os.path.join(store.state_dir, "backend_jobs.sqlite3")))

    def test_missing_adapter_is_honest_until03_is_integrated(self):
        with patch.object(registry.importlib, "import_module", side_effect=ImportError("SYNTHETIC_SECRET")):
            with self.assertRaisesRegex(BackendError, "pepe adapter unavailable") as caught:
                registry.create_backend("pepe", self.selected)
        self.assertNotIn("SECRET", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
