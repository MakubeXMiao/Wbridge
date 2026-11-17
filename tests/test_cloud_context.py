"""Synthetic same-host cloud publication/cache checks, without network/model I/O."""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge import cloud_context as cloud
from wechat_bridge import context_files as files


def deny(*args, **kwargs):
    raise AssertionError("cloud context tests forbid network I/O")


socket.socket.connect = deny
socket.socket.connect_ex = deny
socket.socket.bind = deny
socket.socket.listen = deny
socket.create_connection = deny
socket.getaddrinfo = deny


class CloudContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-cloud-context-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve() / "bridge"
        self.base.mkdir(mode=0o700)
        self.state = self.base / "state"
        self.state.mkdir(mode=0o700)
        self.store = SimpleNamespace(base=str(self.base), state_dir=str(self.state),
            session_path=str(self.state / "session.json"), needs_relogin_path=str(self.state / "NEEDS_RELOGIN"))
        self.now = 1_800_000_000
        self.scope = dict(bot_id="synthetic-bot", owner_id="synthetic-owner",
                          conversation_id="synthetic-owner", login_epoch="a" * 32)
        self.binding = cloud.CloudBinding("synthetic-wbridge", "one-synthetic-case", "fixed-synthetic-publisher")
        self.session = dict(bot_token="synthetic-only", ilink_user_id=self.scope["owner_id"],
                            ilink_bot_id=self.scope["bot_id"], login_epoch=self.scope["login_epoch"])
        self.write_raw(self.state / "session.json", self.session)

    @staticmethod
    def write_raw(path, value):
        if type(value) is dict:
            value = files.encode(value)
        path.write_bytes(value)
        path.chmod(0o600)

    def projection(self, revision=1, **changes):
        result = dict(source_revision=revision, source_updated_at=self.now - 10,
            source_ref="synthetic://case/evidence/1", fact_status="confirmed", title="合成事项",
            facts="仅验证公共状态接口", change="首个已核版本", next_step="等待下一批读取", open_loop="")
        result.update(changes)
        return result

    def publish(self, revision=1, previous=None, **changes):
        return cloud.publish(self.store, self.binding, self.binding.publisher_id,
            self.projection(revision, **changes), previous, self.now, enabled=True)

    def refresh(self):
        return cloud.refresh(self.store, self.binding, self.scope, self.now, enabled=True)

    def read(self):
        return cloud.read(self.store, self.scope, self.now, enabled=True, binding=self.binding)

    def seed(self):
        self.assertEqual(self.publish()["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")

    @property
    def source(self):
        return self.state / "continuity-source" / "current.json"

    @property
    def cache(self):
        return self.state / "continuity" / "cloud.json"

    def assert_tombstone_no_text(self):
        raw = json.loads(self.cache.read_text())
        self.assertTrue(raw["tombstone"])
        self.assertNotIn("projection", raw)
        self.assertEqual(set(raw), {"schema_version", "tombstone", "scope_digest", "binding_digest",
            "clock_high_water", "retired_revision", "retired_digest", "retired_source_updated_at", "retired_expires_at", "reason"})
        data = self.cache.read_bytes()
        for old_text in ("合成事项", "仅验证公共状态接口", self.scope["owner_id"], self.binding.source_id, self.binding.case_id):
            self.assertNotIn(old_text.encode(), data)
        self.assertLessEqual(len(data), cloud.MAX_BYTES)
        return raw

    def test_default_off_creates_no_context_directories(self):
        self.assertEqual(cloud.publish(self.store, self.binding, self.binding.publisher_id,
            self.projection(), None, self.now)["status"], "disabled")
        self.assertEqual(cloud.refresh(self.store, self.binding, self.scope, self.now)["status"], "disabled")
        self.assertEqual(cloud.read(self.store, self.scope, self.now)["status"], "disabled")
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "unchanged")
        self.assertFalse((self.state / "continuity").exists())
        self.assertFalse((self.state / "continuity-source").exists())

    def test_synthetic_publication_then_read_has_versions_and_separate_times(self):
        self.seed()
        initial = self.read()
        self.assertEqual(initial["status"], "available")
        projection = initial["projection"]
        self.assertEqual(projection["source_revision"], 1)
        self.assertEqual(projection["source_updated_at"], self.now - 10)
        self.assertEqual(projection["bridge_checked_at"], self.now)
        self.assertEqual(projection["bridge_applied_at"], self.now)
        self.assertLessEqual(len(files.encode(initial)), 4096)
        encoded = files.encode(initial)
        for private in (b"bot_token", b"login_epoch", b"synthetic-only", b"context_token"):
            self.assertNotIn(private, encoded)
        source_bytes = self.source.read_bytes()
        self.now += 30
        self.assertEqual(self.refresh()["status"], "unchanged")
        checked = self.read()["projection"]
        self.assertEqual(checked["bridge_checked_at"], self.now)
        self.assertEqual(checked["bridge_applied_at"], projection["bridge_applied_at"])
        self.assertEqual(checked["source_updated_at"], projection["source_updated_at"])
        self.assertEqual(self.source.read_bytes(), source_bytes)

    def test_cas_rejects_late_wrong_publisher_binding_and_revision_redefinition(self):
        self.seed()
        before = self.source.read_bytes()
        for revision, previous, changes in ((2, 99, {}), (1, 1, {"facts": "changed"})):
            result = self.publish(revision, previous, **changes)
            self.assertEqual(result["status"], "conflict")
            self.assertEqual(self.source.read_bytes(), before)
        bad = cloud.publish(self.store, self.binding, "wrong-publisher", self.projection(2), 1, self.now, enabled=True)
        self.assertEqual(bad["reason"], "publisher_mismatch")
        other = cloud.CloudBinding(self.binding.source_id, "another-case", self.binding.publisher_id)
        self.assertEqual(cloud.publish(self.store, other, other.publisher_id, self.projection(2),
                                     1, self.now, enabled=True)["reason"], "binding_mismatch")
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(self.publish()["status"], "unchanged")
        self.now += 1
        self.assertEqual(self.publish(2, 1, facts="新结论", fact_status="corrected")["status"], "published")
        self.assertEqual(self.publish(4, 1)["status"], "conflict")
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertEqual(self.read()["projection"]["facts"], "新结论")

    def test_new_revision_and_different_payload_cannot_reuse_cached_revision(self):
        self.seed()
        raw = json.loads(self.source.read_text())
        raw["projection"]["facts"] = "unversioned replacement"
        raw["digest"] = cloud._digest(raw["projection"])
        self.write_raw(self.source, raw)
        self.assertEqual(self.refresh()["status"], "update_pending")
        observed = self.read()
        self.assertEqual(observed["status"], "update_pending")
        self.assertEqual(observed["projection"]["facts"], "仅验证公共状态接口")

    def test_source_unavailable_preserves_last_checked_and_applied(self):
        self.seed()
        before = self.read()["projection"]
        self.source.unlink()
        self.now += 100
        self.assertEqual(self.refresh()["status"], "unavailable")
        observed = self.read()
        self.assertEqual(observed["status"], "unavailable")
        self.assertEqual(observed["projection"]["bridge_checked_at"], before["bridge_checked_at"])
        self.assertEqual(observed["projection"]["bridge_applied_at"], before["bridge_applied_at"])
        self.assertEqual(observed["projection"]["source_updated_at"], before["source_updated_at"])

    def test_bad_or_oversize_source_keeps_old_version_but_marks_pending(self):
        self.seed()
        source_bytes = self.source.read_bytes()
        before = self.read()["projection"]
        for data in (b'{"schema_version":1,"projection":{"source_revision":2}}', b"x" * 4097):
            self.write_raw(self.source, data)
            self.now += 1
            self.assertEqual(self.refresh()["status"], "update_pending")
            observed = self.read()
            self.assertEqual(observed["status"], "update_pending")
            self.assertEqual(observed["projection"]["source_revision"], 1)
            self.assertEqual(observed["projection"]["bridge_checked_at"], before["bridge_checked_at"])
        self.write_raw(self.source, source_bytes)
        self.assertEqual(self.refresh()["status"], "unchanged")
        self.assertEqual(self.read()["status"], "available")

    def test_unicode_budget_total_metadata_bytes_and_bad_types_are_independent(self):
        full = self.projection(title="😀" * 512, facts="", change="", next_step="", open_loop="", source_ref="s")
        good = cloud.publish(self.store, self.binding, self.binding.publisher_id, full, None, self.now, enabled=True)
        self.assertEqual(good["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertLessEqual(self.cache.stat().st_size, 4096)
        before = self.source.read_bytes()
        for changes in ({"title": "x" * 513}, {"source_ref": "😀" * 512, "facts": "😀" * 512},
                        {"source_updated_at": True}, {"fact_status": ["confirmed"]}, {"facts": None}):
            result = cloud.publish(self.store, self.binding, self.binding.publisher_id,
                self.projection(2, **changes), 1, self.now, enabled=True)
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(self.source.read_bytes(), before)

    def test_expiry_is_source_time_not_repeated_check_time(self):
        self.seed()
        original_updated = self.now - 10
        self.now = original_updated + cloud.TTL_SECONDS - 1
        self.assertEqual(self.refresh()["status"], "unchanged")
        self.assertEqual(self.read()["status"], "available")
        self.now += 1
        self.assertEqual(self.read()["status"], "expired")
        source_bytes = self.source.read_bytes()
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()
        self.assertEqual(self.source.read_bytes(), source_bytes)
        self.assertEqual(self.refresh()["status"], "expired")

    def test_clock_rollback_and_future_source_are_not_retention_renewal(self):
        self.seed()
        self.now -= 1
        self.assertEqual(self.read()["reason"], "clock_invalid")
        self.assertEqual(self.refresh()["reason"], "clock_invalid")
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()
        before = self.source.read_bytes()
        self.assertEqual(self.publish(2, 1, source_updated_at=self.now + 1)["reason"], "clock_invalid")
        self.assertEqual(self.source.read_bytes(), before)

    def test_login_epoch_or_logout_refuses_reads_then_maintenance_cleans_only_bridge(self):
        self.seed()
        source_bytes = self.source.read_bytes()
        self.session["login_epoch"] = "b" * 32
        self.write_raw(self.state / "session.json", self.session)
        next_scope = dict(self.scope, login_epoch="b" * 32)
        self.assertEqual(cloud.read(self.store, next_scope, self.now, enabled=True)["reason"], "scope_mismatch")
        self.assertEqual(cloud.refresh(self.store, self.binding, next_scope, self.now, enabled=True)["reason"], "scope_mismatch")
        self.assertTrue(self.cache.exists())
        self.assertEqual(cloud.maintain(self.store, next_scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()
        self.assertEqual(self.source.read_bytes(), source_bytes)
        self.assertEqual(cloud.publish(self.store, self.binding, self.binding.publisher_id,
            self.projection(2), 1, self.now, enabled=True)["reason"], "scope_mismatch")
        self.session["login_epoch"] = "a" * 32
        self.write_raw(self.state / "session.json", self.session)
        self.now += 1
        self.assertEqual(self.publish(2, 1, source_updated_at=self.now)["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        (self.state / "NEEDS_RELOGIN").touch(mode=0o600)
        self.assertEqual(self.read()["reason"], "scope_mismatch")
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()
        self.assertTrue(self.source.exists())

    def test_pause_and_revocation_hide_projection_without_changing_source_time(self):
        self.seed()
        updated = self.projection()["source_updated_at"]
        self.now += 5
        self.assertEqual(cloud.pause(self.store, self.binding, self.binding.publisher_id,
                                    1, self.now, enabled=True)["status"], "paused")
        self.assertEqual(self.refresh()["status"], "paused")
        self.assertIsNone(self.read()["projection"])
        self.assertEqual(json.loads(self.source.read_text())["projection"]["source_updated_at"], updated)
        self.assertEqual(cloud.publish(self.store, self.binding, self.binding.publisher_id,
            json.loads(self.source.read_text())["projection"], None, self.now, enabled=True)["status"], "unchanged")
        self.assertFalse(json.loads(self.source.read_text())["active"])
        self.assertEqual(self.publish(2, 1, fact_status="revoked")["status"], "published")
        self.assertEqual(self.refresh()["status"], "revoked")
        self.assert_tombstone_no_text()
        self.assertIsNone(self.read()["projection"])

    def test_nonblocking_source_and_cache_locks_skip_without_overwrite(self):
        self.seed()
        before = self.cache.read_bytes()
        for path, operation in ((self.source.parent, self.refresh), (self.cache.parent, self.refresh)):
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual(operation()["reason"], "storage_busy")
            finally:
                os.close(fd)
            self.assertEqual(self.cache.read_bytes(), before)

    def test_parallel_publication_has_one_winner_without_blind_merge(self):
        self.seed()
        def run(revision):
            return self.publish(revision, 1, facts=str(revision))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, (2, 3)))
        self.assertEqual(sum(result["status"] == "published" for result in results), 1)
        self.assertTrue(all(result["status"] in ("published", "conflict", "skipped") for result in results))
        self.assertIn(json.loads(self.source.read_text())["projection"]["source_revision"], (2, 3))

    def test_maintenance_cleans_oversize_and_tmp_but_does_not_touch_unknown_or_source(self):
        self.seed()
        unknown = self.cache.parent / "unknown-private.json"
        self.write_raw(unknown, b"untouched")
        temporary = self.cache.parent / ".cloud.json.tmp"
        self.write_raw(temporary, b"x" * 8192)
        self.write_raw(self.cache, b"x" * 8192)
        source_bytes = self.source.read_bytes()
        result = cloud.maintain(self.store, self.scope, self.now)
        self.assertEqual(result["status"], "cleaned")
        self.assertEqual(set(result["removed"]), {".cloud.json.tmp"})
        self.assert_tombstone_no_text()
        self.assertEqual(unknown.read_bytes(), b"untouched")
        self.assertEqual(self.source.read_bytes(), source_bytes)

    def test_illegal_symlinks_are_not_followed_or_deleted_and_errors_do_not_escape(self):
        self.seed()
        self.cache.unlink()
        outside = self.base / "outside.json"
        self.write_raw(outside, b"do not touch")
        self.cache.symlink_to(outside)
        self.assertEqual(self.read()["reason"], "storage_invalid")
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["reason"], "storage_invalid")
        self.assertEqual(outside.read_bytes(), b"do not touch")
        self.assertTrue(self.cache.is_symlink())
        with patch.object(files, "locked", side_effect=RuntimeError("synthetic hidden detail")):
            result = self.refresh()
            self.assertEqual(result["reason"], "storage_invalid")
            self.assertNotIn("synthetic hidden detail", json.dumps(result))

    def test_disable_stops_injection_while_cleanup_remains_available(self):
        self.seed()
        self.assertIsNone(cloud.read(self.store, self.scope, self.now)["projection"])
        self.now += cloud.TTL_SECONDS
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()

    def test_reader_can_reject_changed_source_binding(self):
        self.seed()
        other = cloud.CloudBinding("different-source", self.binding.case_id, self.binding.publisher_id)
        observed = cloud.read(self.store, self.scope, self.now, enabled=True, binding=other)
        self.assertEqual(observed["reason"], "binding_mismatch")
        self.assertIsNone(observed["projection"])

    def test_revision_types_monotonicity_and_source_time_are_all_checked(self):
        for revision in (True, False, 0, -1, 1.0, "1", 1 << 63):
            self.assertEqual(self.publish(revision)["status"], "skipped")
        self.assertFalse(self.source.exists())
        self.seed()
        original = self.source.read_bytes()
        for previous in (True, 0, 1.0, "1"):
            self.assertEqual(self.publish(2, previous)["status"], "skipped")
            self.assertEqual(self.source.read_bytes(), original)
        self.assertEqual(self.publish(2, 1, source_updated_at=self.now - 11)["reason"], "older_source")
        self.assertEqual(self.source.read_bytes(), original)
        self.assertEqual(self.publish(3, 1)["status"], "published")
        latest = self.source.read_bytes()
        self.assertEqual(self.publish(2, 3)["status"], "conflict")
        self.assertEqual(self.source.read_bytes(), latest)
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertEqual(self.read()["projection"]["source_revision"], 3)

    def test_older_authority_file_cannot_replace_newer_bridge_cache(self):
        self.seed()
        version_one = self.source.read_bytes()
        self.now += 30
        self.assertEqual(self.publish(2, 1)["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        self.write_raw(self.source, version_one)
        self.now += 1
        self.assertEqual(self.refresh()["status"], "update_pending")
        observed = self.read()
        self.assertEqual(observed["status"], "update_pending")
        self.assertEqual(observed["projection"]["source_revision"], 2)
        self.assertEqual(observed["projection"]["observed_source_revision"], 1)

    def test_cache_application_write_failure_is_observable_without_marker_write(self):
        self.seed()
        before = self.read()["projection"]
        before_bytes = self.cache.read_bytes()
        self.now += 1
        self.assertEqual(self.publish(2, 1)["status"], "published")
        with patch.object(files, "write", side_effect=files.ContextError("storage_invalid")):
            refreshed = self.refresh()
        self.assertEqual(refreshed["status"], "update_pending")
        observed = self.read()
        self.assertEqual(observed["status"], "update_pending")
        self.assertEqual(observed["projection"]["source_revision"], 1)
        self.assertEqual(observed["projection"]["observed_source_revision"], 2)
        self.assertEqual(observed["projection"]["bridge_checked_at"], before["bridge_checked_at"])
        self.assertEqual(self.cache.read_bytes(), before_bytes)
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertEqual(self.read()["projection"]["source_revision"], 2)

    def test_unavailable_first_source_still_returns_a_visible_status_card(self):
        self.assertEqual(self.refresh()["status"], "unavailable")
        result = self.read()
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["reason"], "source_missing")
        self.assertIsNone(result["projection"])
        self.assertFalse(self.source.exists())

    def test_pause_marker_write_failure_still_hides_old_projection(self):
        self.seed()
        self.assertEqual(cloud.pause(self.store, self.binding, self.binding.publisher_id,
                                    1, self.now, enabled=True)["status"], "paused")
        with patch.object(files, "write", side_effect=files.ContextError("storage_invalid")):
            result = self.refresh()
        self.assertEqual(result["status"], "paused")
        # The source pause is known even if the bridge marker cannot be saved.
        # The refresh observation must not expose the now-paused old text.
        self.assertIsNone(self.read()["projection"])

    def test_source_multiline_text_is_data_within_collective_budget(self):
        self.assertEqual(self.publish(facts="第一行\n第二行\t仍是数据")["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertIn("\n", self.read()["projection"]["facts"])

    def test_full_source_that_exceeds_cache_metadata_budget_marks_pending(self):
        self.seed()
        original = self.read()["projection"]
        candidate = None
        for width in range(1, 513):
            projection = self.projection(2, title="😀" * 512, facts="", change="", next_step="", open_loop="",
                                         source_ref="😀" * width)
            record = dict(schema_version=1, scope=self.scope, binding=cloud._binding(self.binding),
                          projection=projection, digest=cloud._digest(projection), published_at=self.now, active=True)
            cache = dict(schema_version=1, scope=self.scope, binding=cloud._binding(self.binding), projection=projection,
                         source_digest=cloud._digest(projection), bridge_checked_at=self.now,
                         bridge_applied_at=self.now, sync_status="applied", observed_source_revision=2, clock_high_water=self.now)
            if len(files.encode(record)) <= 4096 < len(files.encode(cache)):
                candidate = projection
                break
        self.assertIsNotNone(candidate)
        result = cloud.publish(self.store, self.binding, self.binding.publisher_id, candidate, 1, self.now, enabled=True)
        self.assertEqual(result["status"], "published")
        self.assertLessEqual(self.source.stat().st_size, 4096)
        self.assertEqual(self.refresh()["status"], "update_pending")
        result = self.read()
        self.assertEqual(result["status"], "update_pending")
        self.assertEqual(result["projection"]["source_revision"], 1)
        self.assertEqual(result["projection"]["bridge_checked_at"], original["bridge_checked_at"])

    def test_revision_rollback_is_rejected_even_with_later_source_time(self):
        self.seed()
        self.assertEqual(self.publish(3, 1)["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        raw = json.loads(self.source.read_text())
        raw["projection"]["source_revision"] = 2
        raw["projection"]["source_updated_at"] = self.now
        raw["digest"] = cloud._digest(raw["projection"])
        self.write_raw(self.source, raw)
        self.assertEqual(self.refresh()["status"], "update_pending")
        self.assertEqual(self.read()["projection"]["source_revision"], 3)

    def test_source_atomic_write_failure_does_not_claim_published(self):
        self.seed()
        source_bytes = self.source.read_bytes()
        with patch.object(files, "write", side_effect=files.ContextError("storage_invalid")):
            result = self.publish(2, 1)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(self.source.read_bytes(), source_bytes)
        self.assertEqual(self.refresh()["status"], "unchanged")

    def refresh_with_cache_lock(self):
        fd = os.open(self.cache.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return self.refresh()
        finally:
            os.close(fd)

    def test_known_new_source_survives_cache_lock_contention_as_pending(self):
        self.seed()
        before = self.cache.read_bytes()
        self.assertEqual(self.publish(2, 1, change="已发布新版本")["status"], "published")
        result = self.refresh_with_cache_lock()
        self.assertEqual(result["status"], "update_pending")
        self.assertEqual(result["reason"], "storage_busy")
        self.assertEqual(result["source_revision"], 2)
        read = self.read()
        self.assertEqual(read["status"], "update_pending")
        self.assertEqual(read["projection"]["source_revision"], 1)
        self.assertEqual(read["projection"]["observed_source_revision"], 2)
        self.assertEqual(self.cache.read_bytes(), before)

    def test_known_revocation_survives_cache_lock_contention_without_old_text(self):
        self.seed()
        before = self.cache.read_bytes()
        self.assertEqual(self.publish(2, 1, fact_status="revoked")["status"], "published")
        result = self.refresh_with_cache_lock()
        self.assertEqual(result["status"], "revoked")
        self.assertEqual(result["source_revision"], 2)
        self.assertEqual(self.cache.read_bytes(), before)
        read = self.read()
        self.assertEqual(read["reason"], "source_revoked")
        self.assertIsNone(read["projection"])

    def test_known_pause_survives_cache_lock_contention_without_old_text(self):
        self.seed()
        before = self.cache.read_bytes()
        self.assertEqual(cloud.pause(self.store, self.binding, self.binding.publisher_id,
                                    1, self.now, enabled=True)["status"], "paused")
        result = self.refresh_with_cache_lock()
        self.assertEqual(result["status"], "paused")
        self.assertEqual(self.cache.read_bytes(), before)
        read = self.read()
        self.assertEqual(read["status"], "paused")
        self.assertIsNone(read["projection"])

    def replace_store(self):
        self.store = SimpleNamespace(base=str(self.base), state_dir=str(self.state),
            session_path=str(self.state / "session.json"), needs_relogin_path=str(self.state / "NEEDS_RELOGIN"))

    def test_expired_text_cannot_be_rebuilt_by_new_process_after_wall_rollback(self):
        self.seed()
        original_now, source_bytes = self.now, self.source.read_bytes()
        self.now += cloud.TTL_SECONDS
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        marker = self.assert_tombstone_no_text()
        self.assertEqual(marker["retired_revision"], 1)
        self.assertEqual(marker["clock_high_water"], self.now)
        self.assertEqual(self.source.read_bytes(), source_bytes)
        self.replace_store()
        self.now = original_now + 1
        self.assertEqual(self.refresh()["reason"], "clock_invalid")
        self.assertIsNone(self.read()["projection"])
        self.assert_tombstone_no_text()
        self.assertEqual(self.source.read_bytes(), source_bytes)

    def test_retired_revision_cannot_revive_with_forged_later_time_but_new_conclusion_can(self):
        self.seed()
        self.now += cloud.TTL_SECONDS
        cloud.maintain(self.store, self.scope, self.now)
        self.assert_tombstone_no_text()
        # Simulate a malformed authority integration redefining the same
        # revision with a later timestamp. The real publish API also rejects
        # such a redefinition, but the cache defends its own retired lineage.
        raw = json.loads(self.source.read_text())
        raw["projection"]["source_updated_at"] = self.now
        raw["published_at"] = self.now
        raw["digest"] = cloud._digest(raw["projection"])
        self.write_raw(self.source, raw)
        self.replace_store()
        self.assertEqual(self.refresh()["status"], "expired")
        self.assertIsNone(self.read()["projection"])
        self.assert_tombstone_no_text()
        self.now += 1
        self.assertEqual(self.publish(2, 1, source_updated_at=self.now, facts="新的可核结论")["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertEqual(self.read()["projection"]["facts"], "新的可核结论")

    def test_clock_retirement_requires_new_revision_and_new_source_time(self):
        self.seed()
        original_now, source_at = self.now, self.projection()["source_updated_at"]
        self.now -= 1
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        self.assert_tombstone_no_text()
        self.replace_store()
        self.now = original_now + 1
        self.assertEqual(self.publish(2, 1, source_updated_at=source_at)["status"], "published")
        self.assertEqual(self.refresh()["status"], "expired")
        self.assertIsNone(self.read()["projection"])
        self.assertEqual(self.publish(3, 2, source_updated_at=self.now)["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")

    def test_scope_change_clears_text_then_accepts_new_scope_after_source_owned_reset(self):
        self.seed()
        old_source = self.source.read_bytes()
        self.session["login_epoch"] = "b" * 32
        self.write_raw(self.state / "session.json", self.session)
        self.scope = dict(self.scope, login_epoch="b" * 32)
        self.now += 10
        self.assertEqual(cloud.maintain(self.store, self.scope, self.now)["status"], "cleaned")
        marker = self.assert_tombstone_no_text()
        self.assertEqual(marker["scope_digest"], cloud._digest(dict(self.scope, login_epoch="a" * 32)))
        self.assertEqual(self.source.read_bytes(), old_source)
        self.assertIsNone(self.read()["projection"])
        # Synthetic source owner resets its own authority for a newly bound
        # login. The bridge maintenance above never touched that source file.
        self.source.unlink()
        self.replace_store()
        self.assertEqual(self.publish(1, None, source_updated_at=self.now)["status"], "published")
        self.assertEqual(self.refresh()["status"], "applied")
        self.assertEqual(self.read()["projection"]["source_revision"], 1)

    def test_maintenance_advances_control_watermark_without_fake_source_check(self):
        self.seed()
        initial = self.read()["projection"]
        self.now += 100
        cloud.maintain(self.store, self.scope, self.now)
        stored = json.loads(self.cache.read_text())
        self.assertEqual(stored["clock_high_water"], self.now)
        self.assertEqual(self.read()["projection"]["bridge_checked_at"], initial["bridge_checked_at"])
        self.now -= 1
        self.replace_store()
        self.assertEqual(self.read()["reason"], "clock_invalid")
        self.assertIsNone(self.read()["projection"])


if __name__ == "__main__":
    unittest.main()
