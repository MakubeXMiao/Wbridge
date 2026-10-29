"""Strict opt-in configuration and read-only backend preflight."""

import copy
from contextlib import closing
import importlib
import json
import os
import re
import sqlite3
import urllib.parse

from .contract import BackendError


_ROOT_KEYS = frozenset(("schema_version", "account_scope", "default_backend",
                        "legacy_state_dir", "job_timeout_seconds",
                        "approval_timeout_seconds", "backends"))
_PROVIDERS = frozenset(("codex", "claude_code"))
_BACKENDS = _PROVIDERS | frozenset(("muse",))
_DRIVER_METHODS = ("start", "poll", "respond_approval", "cancel", "close")


def _nonempty(value, limit=200):
    return (isinstance(value, str) and bool(value.strip()) and len(value) <= limit
            and not any(ord(ch) < 32 for ch in value))


def _integer(config, key, default, maximum):
    value = config.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise BackendError("%s must be an integer in 1..%s" % (key, maximum))
    return value


def _directory(value, field, check_runtime):
    if not isinstance(value, str) or not os.path.isabs(value) or "\x00" in value:
        raise BackendError("%s must be an absolute directory" % field)
    if check_runtime and not os.path.isdir(value):
        raise BackendError("%s directory does not exist" % field)


def _selected(backend_id, selected, check_runtime):
    if not isinstance(selected, dict):
        raise BackendError("backend configuration must be an object")
    result = copy.deepcopy(selected)
    if backend_id == "muse":
        if selected:
            raise BackendError("muse configuration must be {}; use legacy_state_dir")
        return result
    if not _nonempty(result.get("instance_id")):
        raise BackendError("backend instance_id is required and must be bounded")
    command = result.get("command")
    if (not isinstance(command, list) or not command or len(command) > 100
            or any(not isinstance(arg, str) or not arg or "\x00" in arg
                   or len(arg) > 16384 for arg in command)
            or not os.path.isabs(command[0])):
        raise BackendError("backend command must be an argv list with an absolute executable")
    if check_runtime and (not os.path.isfile(command[0]) or not os.access(command[0], os.X_OK)):
        raise BackendError("backend executable is missing or not executable")
    # Fail closed on explicit permission bypass in command-line overrides too.
    for arg in command[1:]:
        normalized = re.sub(r"[-_\s]", "", arg).lower()
        if any(word in normalized for word in (
                "bypass", "unrestricted", "dangerfullaccess", "skippermissions")):
            raise BackendError("backend command may not bypass permissions or sandboxing")
        if "approvalpolicy" in normalized or "sandboxmode" in normalized:
            raise BackendError("set permission and sandbox policy only in execution_policy")
        flag = arg.split("=", 1)[0]
        if flag in ("--approval-policy", "--ask-for-approval", "-a", "--sandbox", "-s",
                    "--permission-mode", "--allowedTools", "--allowed-tools", "--full-auto"):
            raise BackendError("set permission and sandbox policy only in execution_policy")
    _directory(result.get("cwd"), "backend cwd", check_runtime)
    policy = result.get("execution_policy", {})
    if not isinstance(policy, dict) or set(policy) - {"approval", "sandbox"}:
        raise BackendError("execution_policy contains unsupported fields")
    policy = copy.deepcopy(policy)
    if policy.setdefault("approval", "explicit") != "explicit":
        raise BackendError("execution_policy.approval must be explicit")
    if backend_id == "codex":
        policy.setdefault("sandbox", "workspace-write")
    if "sandbox" in policy and policy["sandbox"] not in ("workspace-write", "read-only"):
        raise BackendError("execution_policy.sandbox must be workspace-write or read-only")
    result["execution_policy"] = policy
    return result


def validate_config(config, check_runtime=True):
    """Normalize a deep copy; never mutate caller state or execute a provider."""
    if not isinstance(config, dict) or set(config) - _ROOT_KEYS:
        raise BackendError("backend config must be an object with supported v1 fields")
    if type(config.get("schema_version")) is not int or config["schema_version"] != 1:
        raise BackendError("backend config schema_version must be 1")
    if not _nonempty(config.get("account_scope")):
        raise BackendError("backend config account_scope is required and must be bounded")
    backends = config.get("backends")
    if (not isinstance(backends, dict) or not backends
            or any(not isinstance(key, str) or key not in _BACKENDS for key in backends)):
        raise BackendError("backends must enable codex, claude_code and/or muse")
    default = config.get("default_backend")
    if not isinstance(default, str) or default not in backends:
        raise BackendError("default_backend must name an enabled backend")
    result = copy.deepcopy(config)
    result["job_timeout_seconds"] = _integer(config, "job_timeout_seconds", 1800, 86400)
    result["approval_timeout_seconds"] = _integer(config, "approval_timeout_seconds", 300, 300)
    legacy = config.get("legacy_state_dir")
    if "muse" in backends and legacy is None:
        raise BackendError("muse requires an explicit legacy_state_dir matching its Hook")
    if legacy is not None:
        _directory(legacy, "legacy_state_dir", check_runtime)
    result["legacy_state_dir"] = legacy
    result["backends"] = {key: _selected(key, value, check_runtime)
                          for key, value in backends.items()}
    return result


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BackendError("backend config contains duplicate JSON keys")
        result[key] = value
    return result


def _invalid_json_constant(value):
    raise BackendError("backend config must contain standard JSON values")


def _require_no_managed_state(store):
    if os.path.lexists(os.path.join(store.state_dir, "backend_jobs.sqlite3")):
        raise BackendError("managed database exists without backends.json; restore its original config, do not replay through legacy")
    inbox = os.path.join(getattr(store, "base", os.path.dirname(store.state_dir)), "backend_inbox")
    if os.path.lexists(inbox):
        raise BackendError("managed input directory exists without backends.json; restore its original config")


def load_config(store):
    """Missing file means legacy; malformed/unreadable files never mean legacy."""
    path = os.path.join(store.state_dir, "backends.json")
    try:
        with open(path, encoding="utf-8") as handle:
            if os.fstat(handle.fileno()).st_size > 1024 * 1024:
                raise BackendError("backend config exceeds 1 MiB")
            config = json.load(handle, object_pairs_hook=_object_pairs,
                               parse_constant=_invalid_json_constant)
    except FileNotFoundError:
        if os.path.lexists(path):
            raise BackendError("state/backends.json points to an unavailable config") from None
        _require_no_managed_state(store)
        return None
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError):
        raise BackendError("cannot read valid state/backends.json; check its JSON and permissions") from None
    return validate_config(config)


def _factory(backend_id):
    if not isinstance(backend_id, str) or backend_id not in _PROVIDERS:
        raise BackendError("only codex and claude_code have a v1 backend factory")
    try:
        module = importlib.import_module(".%s" % backend_id, package=__package__)
    except Exception:
        raise BackendError("%s adapter unavailable; install the reviewed adapter and dependencies" % backend_id) from None
    factory = getattr(module, "create_backend", None)
    if not callable(factory):
        raise BackendError("%s adapter has no create_backend factory" % backend_id)
    return factory


def create_backend(backend_id, selected_config):
    """Lazily create one driver, without starting an execution."""
    factory = _factory(backend_id)
    selected = _selected(backend_id, selected_config, True)
    try:
        driver = factory(selected)
    except Exception:
        raise BackendError("%s adapter initialization failed; inspect its local diagnostic" % backend_id) from None
    if any(not callable(getattr(driver, method, None)) for method in _DRIVER_METHODS):
        close = getattr(driver, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
        raise BackendError("%s adapter does not implement the v1 driver methods" % backend_id)
    return driver


def _check_existing_binding(store, config, session):
    path = os.path.join(store.state_dir, "backend_jobs.sqlite3")
    if not os.path.lexists(path):
        return
    if not os.path.isfile(path):
        raise BackendError("managed database is unavailable; preserve state for reconciliation")
    from ..backend_state import digest
    expected = {"version": 1, "owner": session["ilink_user_id"],
                "bot": session["ilink_bot_id"], "config_digest": digest(config),
                "base": os.path.realpath(store.base)}
    uri = "file:%s?mode=ro" % urllib.parse.quote(path, safe="/")
    try:
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as connection:
            connection.execute("PRAGMA query_only=ON")
            row = connection.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()
        binding = json.loads(row[0]) if row else None
    except (sqlite3.Error, ValueError, TypeError):
        raise BackendError("managed database binding is unreadable or incompatible; preserve state") from None
    if binding != expected:
        raise BackendError("managed state identity/config changed; restore its original profile or use a fresh bot/state")


def preflight(store, config, session):
    """Inspect configuration/identity only; no network, subprocess or state writes.

    Authenticated provider generation and actual WeChat token validity remain
    unknown. The caller may display this allowlisted result without secrets.
    """
    if config is None:
        _require_no_managed_state(store)
        return {"ok": True, "mode": "legacy", "backends": [],
                "provider_auth": "not_checked"}
    config = validate_config(config)
    if not isinstance(session, dict) or any(not _nonempty(session.get(key), 16384)
                                           for key in ("bot_token", "ilink_user_id", "ilink_bot_id")):
        raise BackendError("managed backends require a saved WeChat bot token, owner ID and bot ID; run login / wait-login")
    if os.path.exists(os.path.join(store.state_dir, "NEEDS_RELOGIN")):
        raise BackendError("WeChat login is marked invalid; run login / wait-login")
    _check_existing_binding(store, config, session)
    providers = []
    for backend_id in sorted(config["backends"]):
        if backend_id != "muse":
            _factory(backend_id)
        providers.append({"backend_id": backend_id,
                          "adapter": "legacy_hook" if backend_id == "muse" else "available",
                          "provider_auth": "not_checked"})
    return {"ok": True, "mode": "backend_v1", "schema_version": 1,
            "default_backend": config["default_backend"], "backends": providers,
            "wechat_identity": "present", "wechat_token_validity": "not_checked",
            "job_timeout_seconds": config["job_timeout_seconds"],
            "approval_timeout_seconds": config["approval_timeout_seconds"],
            "provider_auth": "not_checked"}
