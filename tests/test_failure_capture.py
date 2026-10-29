"""Private synthetic capture only: no account, HTTP, original image or key."""
import concurrent.futures
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wechat_bridge.failure_capture import FailureCapture, MAX_SOURCE_BYTES, MAX_TOTAL_BYTES, RETENTION_SECONDS


class FailureCaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wbr012-capture-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name, "private-evidence")
        self.now = 1900000000.0
        self.instances = []
        self.addCleanup(self.close_all)

    def close_all(self):
        for instance in self.instances: instance.close()

    def make(self):
        value = FailureCapture(self.root, enabled=True, clock=lambda: self.now)
        self.instances.append(value)
        return value

    def capture(self, instance, transfer=1, source="param", body=b"SYNTHETIC-CIPHER"):
        return instance.capture("%032x" % transfer, source, body,
            b"SYNTHETIC-KEY-16", "SYNTHETIC-NESTED-KEY", "image", "image_invalid")

    def test_default_off_is_zero_touch_even_invalid_paths(self):
        capture = FailureCapture(self.root)
        self.assertFalse(capture.cleanup_enabled)
        self.assertEqual(self.capture(capture), {"status": "disabled"})
        self.assertEqual(capture.cleanup_expired()["status"], "disabled")
        self.assertFalse(self.root.exists())

    def test_private_separate_body_key_and_manifest(self):
        result = self.capture(self.make())
        self.assertEqual(result["status"], "captured")
        sample = result["sample_id"]
        body = self.root / "bodies" / (sample + ".bin")
        key = self.root / "keys" / (sample + ".json")
        manifest = self.root / "manifests" / (sample + ".json")
        self.assertEqual(body.read_bytes(), b"SYNTHETIC-CIPHER")
        self.assertNotIn("SYNTHETIC", manifest.read_text())
        self.assertIn("base64", key.read_text())
        for path in self.root.rglob("*"):
            self.assertEqual(path.stat().st_mode & 0o077, 0)
        self.assertEqual(set(result), {"status", "sample_id", "truncated", "replayable"})

    def test_three_by_two_budget_survives_new_object_and_duplicate(self):
        capture = self.make()
        first = self.capture(capture)
        self.assertEqual(self.capture(capture)["sample_id"], first["sample_id"])
        for transfer in range(1, 4):
            for source in ("param", "full_url"):
                self.assertIn(self.capture(capture, transfer, source)["status"], ("captured", "already_captured"))
        reopened = self.make()
        self.assertEqual(self.capture(reopened, 4)["status"], "budget_exhausted")
        self.assertEqual(self.capture(reopened, 1, "unknown")["status"], "budget_exhausted")
        self.assertEqual(len(list((self.root / "bodies").iterdir())), 6)

    def test_concurrent_objects_cannot_bypass_budget(self):
        capture = self.make()
        reopened = self.make()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda i: self.capture(capture if i % 2 else reopened, i + 1), range(8)))
        accepted = sum(x["status"] == "captured" for x in values)
        self.assertGreater(accepted, 0)
        self.assertLessEqual(accepted, 3)
        self.assertEqual(len(list((self.root / "bodies").iterdir())), accepted)

    def test_one_mebibyte_per_source_and_no_false_replay_claim(self):
        result = self.capture(self.make(), body=b"x" * (MAX_SOURCE_BYTES + 1))
        self.assertTrue(result["truncated"]); self.assertFalse(result["replayable"])
        body = self.root / "bodies" / (result["sample_id"] + ".bin")
        self.assertEqual(body.stat().st_size, MAX_SOURCE_BYTES)

    def test_six_sources_including_keys_and_manifests_fit_total_cap(self):
        capture = self.make()
        values = []
        for transfer in range(1, 4):
            for source in ("param", "full_url"):
                values.append(capture.capture("%032x" % transfer, source,
                    b"x" * MAX_SOURCE_BYTES, b"K" * 4096, "M" * 4096,
                    "image", "image_invalid"))
        self.assertTrue(values[-1]["truncated"])
        self.assertFalse(values[-1]["replayable"])
        actual = sum(path.stat().st_size for path in self.root.rglob("*") if path.is_file())
        self.assertLessEqual(actual, MAX_TOTAL_BYTES)

    def test_budget_cannot_reset_in_a_new_process(self):
        capture = self.make()
        for transfer in range(1, 4): self.capture(capture, transfer)
        code = """import sys
sys.path.insert(0,sys.argv[1])
from wechat_bridge.failure_capture import FailureCapture
value=FailureCapture(sys.argv[2],enabled=True,clock=lambda:1900000000)
print(value.capture('%032x'%4,'param',b'SYNTHETIC')['status'])
value.close()
"""
        result = subprocess.run([sys.executable, "-I", "-c", code,
            str(Path(__file__).resolve().parents[1]), str(self.root)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), b"budget_exhausted")

    def test_key_shape_whitelist_drops_url_context_and_body(self):
        capture = self.make()
        result = capture.capture("%032x" % 1, "param", b"SYNTHETIC-CIPHER",
            "https://private.invalid/signed?token=PRIVATE", "正文或上下文 PRIVATE-CONTEXT " * 20,
            "image", "image_invalid")
        self.assertFalse(result["replayable"])
        raw = (self.root / "keys" / (result["sample_id"] + ".json")).read_text()
        self.assertNotIn("PRIVATE", raw)
        self.assertEqual(json.loads(raw), {"item_key": {"type": "invalid"}, "media_key": {"type": "invalid"}})
        for value in (b"0123456789abcdef", bytes(range(32)), "0123456789abcdef0123456789abcdef",
                      base64.b64encode(bytes(range(32))).decode(), base64.b64encode(b"0123456789abcdef").decode()):
            record = FailureCapture._key(value)
            self.assertIn("base64", record)
            self.assertEqual(base64.b64decode(record["base64"]), value.encode() if isinstance(value, str) else value)

    def test_exact_24_hour_expiry_and_scoped_cleanup(self):
        capture = self.make(); self.capture(capture)
        self.now += RETENTION_SECONDS - 1
        self.assertEqual(capture.cleanup_expired()["status"], "not_expired")
        self.now += 1
        self.assertEqual(self.capture(capture, 2)["status"], "expired")
        result = capture.cleanup_expired()
        self.assertEqual((result["status"], result["deleted"]), ("cleaned", 0))
        self.assertEqual(self.capture(self.make(), 3)["status"], "expired")

    def test_unknown_or_linked_registered_files_fail_closed_without_deletion(self):
        capture = self.make(); result = self.capture(capture)
        extra = self.root / "bodies" / "unowned.bin"
        extra.write_bytes(b"KEEP")
        self.assertEqual(self.capture(capture, 2)["status"], "capture_failed")
        with self.assertRaises(ValueError): self.make()
        self.assertEqual(extra.read_bytes(), b"KEEP")
        extra.unlink()
        replacement = self.make()
        key = self.root / "keys" / (result["sample_id"] + ".json")
        key.unlink(); key.symlink_to(extra)
        self.assertEqual(self.capture(replacement, 2)["status"], "capture_failed")
        self.assertTrue(key.is_symlink())
        with self.assertRaises(ValueError): self.make()

    def test_registered_body_cannot_exceed_reserved_count_or_total_file_cap(self):
        capture = self.make(); result = self.capture(capture)
        body = self.root / "bodies" / (result["sample_id"] + ".bin")
        body.write_bytes(b"x" * 100)
        self.assertEqual(self.capture(capture, 2)["status"], "capture_failed")
        with self.assertRaises(ValueError): self.make()
        body.write_bytes(b"SYNTHETIC-CIPHER")
        reopened = self.make()
        key = self.root / "keys" / (result["sample_id"] + ".json")
        with key.open("r+b") as stream: stream.truncate(MAX_TOTAL_BYTES + 1)
        self.assertEqual(self.capture(reopened, 2)["status"], "capture_failed")
        self.assertEqual(key.stat().st_size, MAX_TOTAL_BYTES + 1)
        with self.assertRaises(ValueError): self.make()

    def test_missing_reserved_crash_files_do_not_reopen_spent_budget(self):
        capture = self.make(); result = self.capture(capture)
        for name, extension in (("bodies", ".bin"), ("keys", ".json"), ("manifests", ".json")):
            (self.root / name / (result["sample_id"] + extension)).unlink()
        reopened = self.make()
        self.assertEqual(self.capture(reopened)["status"], "already_captured")
        self.assertEqual(self.capture(reopened, 1, "full_url")["status"], "captured")

    def test_independent_process_lock_never_blocks_optional_capture(self):
        capture, cleanup = self.make(), self.make()
        code = "import fcntl,sys,time; f=open(sys.argv[1],'r+'); fcntl.flock(f,fcntl.LOCK_EX); print('READY',flush=True); time.sleep(30)"
        holder = subprocess.Popen([sys.executable, "-I", "-c", code,
            str(self.root / ".failure-capture.lock")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertEqual(holder.stdout.readline().strip(), b"READY")
            started = time.monotonic()
            self.assertEqual(self.capture(capture)["status"], "capture_busy")
            self.assertEqual(cleanup.cleanup_expired()["status"], "cleanup_busy")
            self.assertTrue(capture.enabled); self.assertTrue(cleanup.cleanup_enabled)
            with self.assertRaises(ValueError): self.make()
            self.assertLess(time.monotonic() - started, 0.3)
        finally:
            holder.kill(); holder.communicate(timeout=2)
        self.assertEqual(self.capture(capture)["status"], "captured")
        self.now += RETENTION_SECONDS
        self.assertEqual(cleanup.cleanup_expired(), {"status": "cleaned", "deleted": 3})

    def test_reject_repo_shared_permissions_and_symlinks(self):
        with self.assertRaises(ValueError): FailureCapture(Path(__file__).parents[1] / "BAD-CAPTURE", enabled=True)
        os.mkdir(self.root, 0o755)
        with self.assertRaises(ValueError): self.make()
        os.chmod(self.root, 0o700)
        link = Path(self.temp.name, "link"); link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError): FailureCapture(link, enabled=True)

    def test_body_directory_symlink_cannot_redirect_capture_or_cleanup(self):
        capture = self.make()
        outside = Path(self.temp.name, "outside"); outside.mkdir(mode=0o700)
        bodies = self.root / "bodies"; bodies.rmdir(); bodies.symlink_to(outside, target_is_directory=True)
        self.assertEqual(self.capture(capture)["status"], "capture_failed")
        self.assertEqual(list(outside.iterdir()), [])
        self.now += RETENTION_SECONDS
        self.assertEqual(capture.cleanup_expired()["status"], "cleanup_failed")
        self.assertTrue(capture.cleanup_enabled)
        self.assertEqual(list(outside.iterdir()), [])

    def counts(self):
        return {name: len(list((self.root / name).iterdir()))
                for name in ("bodies", "keys", "manifests")}

    def marker(self):
        return json.loads((self.root / ".failure-capture-v1.json").read_bytes())

    def test_normal_capture_and_cleanup_overlap_keep_both_lifecycles(self):
        # Exercise both orderings using actual public operations holding flock.
        from wechat_bridge.serve import _cleanup_failure_capture
        for held in ("capture", "cleanup"):
            with self.subTest(held=held):
                self.root = Path(self.temp.name, "normal-" + held)
                self.now = 1900000000.0
                value = self.make(); self.capture(value)
                entered, release = threading.Event(), threading.Event()
                answers = []
                if held == "capture":
                    original = value._write_bytes
                    def paused_write(name, data, directory):
                        original(name, data, directory)
                        if name.endswith(".bin"):
                            entered.set()
                            if not release.wait(3): raise AssertionError("barrier timeout")
                    manager = patch.object(value, "_write_bytes", paused_write)
                    operation = lambda: self.capture(value, 2)
                else:
                    original = value._read_json
                    def paused_read(name, directory):
                        result = original(name, directory)
                        if threading.current_thread().name == "held-cleanup":
                            entered.set()
                            if not release.wait(3): raise AssertionError("barrier timeout")
                        return result
                    manager = patch.object(value, "_read_json", paused_read)
                    operation = value.cleanup_expired
                with manager:
                    worker = threading.Thread(name="held-" + held, target=lambda: answers.append(operation()))
                    worker.start()
                    try:
                        self.assertTrue(entered.wait(3))
                        started = time.monotonic()
                        busy = value.cleanup_expired() if held == "capture" else self.capture(value, 2)
                        self.assertEqual(busy["status"], "cleanup_busy" if held == "capture" else "capture_busy")
                        self.assertLess(time.monotonic() - started, 0.3)
                        self.assertTrue(value.enabled); self.assertTrue(value.cleanup_enabled)
                    finally:
                        release.set(); worker.join(3)
                    self.assertFalse(worker.is_alive())
                self.assertEqual(answers[0]["status"], "captured" if held == "capture" else "not_expired")
                self.now += RETENTION_SECONDS
                _cleanup_failure_capture(SimpleNamespace(failure_capture=value))
                self.assertFalse(any(self.counts().values()))
                journal = (self.root / ".failure-capture-journal-v1.jsonl").read_bytes()
                self.assertEqual(value.cleanup_expired(), {"status": "cleaned", "deleted": 0})
                self.assertEqual((self.root / ".failure-capture-journal-v1.jsonl").read_bytes(), journal)

    def crash_child(self, phase):
        code = r'''
import os,sys
from pathlib import Path
def no_network(event,args):
    if event in ("socket.connect","socket.connect_ex","socket.getaddrinfo","socket.bind"):
        raise AssertionError("network forbidden")
sys.addaudithook(no_network)
sys.path.insert(0,sys.argv[1])
from wechat_bridge.failure_capture import FailureCapture,RETENTION_SECONDS
root,phase=Path(sys.argv[2]),sys.argv[3]
real_replace,real_unlink,real_fsync=os.replace,os.unlink,os.fsync
def location(directory):
    info=os.fstat(directory)
    for name in ("keys","manifests","bodies"):
        path=root/name
        if path.exists():
            item=os.stat(path)
            if (info.st_dev,info.st_ino)==(item.st_dev,item.st_ino): return name
    return "root"
def die():
    os.write(1,b"OWNED_WRITER_CRASH\n")
    os._exit(73)
def crashing_replace(src,dst,*args,**kwargs):
    place=location(kwargs["dst_dir_fd"])
    target="marker" if place=="root" and dst==".failure-capture-v1.json" else place
    selected=phase.endswith(target+"_pre") or phase.endswith(target+"_post")
    if selected and phase.endswith("_pre"): die()
    answer=real_replace(src,dst,*args,**kwargs)
    if selected: die()
    return answer
def crashing_unlink(name,*args,**kwargs):
    answer=real_unlink(name,*args,**kwargs)
    if phase=="cleanup_first_unlink" and name.endswith(".bin"): die()
    if phase=="recovery_temp_unlink" and name.startswith(".tmp-capture-"): die()
    if phase=="bootstrap_unlink" and name==".failure-capture-journal-v1.jsonl": die()
    return answer
os.replace,os.unlink=crashing_replace,crashing_unlink
if phase=="init_journal_open":
    real_fdopen=os.fdopen
    def crash_fdopen(fd,mode,*args,**kwargs):
        path=root/".failure-capture-journal-v1.jsonl"
        if mode=="wb" and path.exists():
            info,item=os.fstat(fd),os.stat(path)
            if (info.st_dev,info.st_ino)==(item.st_dev,item.st_ino): die()
        return real_fdopen(fd,mode,*args,**kwargs)
    os.fdopen=crash_fdopen
if phase in ("reservation_journal_fsync","bootstrap_journal_fsync"):
    def crashing_fsync(fd):
        answer=real_fsync(fd)
        path=root/".failure-capture-journal-v1.jsonl"
        if path.exists():
            info,item=os.fstat(fd),os.stat(path)
            if (info.st_dev,info.st_ino)==(item.st_dev,item.st_ino): die()
        return answer
    os.fsync=crashing_fsync
if phase in ("torn_reservation","init_journal_half"):
    real_fdopen=os.fdopen
    class TornWrite:
        def __init__(self,stream): self.stream=stream
        def __enter__(self): return self
        def __exit__(self,*args): return self.stream.__exit__(*args)
        def __getattr__(self,name): return getattr(self.stream,name)
        def write(self,data):
            self.stream.write(data[:len(data)//2])
            self.stream.flush()
            real_fsync(self.stream.fileno())
            die()
    def torn_fdopen(fd,mode,*args,**kwargs):
        stream=real_fdopen(fd,mode,*args,**kwargs)
        return TornWrite(stream) if mode==("wb" if phase=="init_journal_half" else "ab") else stream
    os.fdopen=torn_fdopen
if phase.startswith("init_"):
    FailureCapture(root,enabled=True,clock=lambda:1900000000.0)
elif phase in ("recovery_temp_unlink","bootstrap_unlink","bootstrap_journal_fsync"):
    FailureCapture(root,enabled=True,clock=lambda:1900000000.0)
else:
    value=FailureCapture(root,enabled=True,clock=lambda:1900000000.0)
    if phase.startswith("cleanup_"):
        value.cleanup_expired(now=1900000000.0+RETENTION_SECONDS)
    else:
        value.capture("%032x"%2,"param",b"SYNTHETIC-CRASH-BODY",
            b"0123456789abcdef","0123456789abcdef0123456789abcdef","image","image_invalid")
raise AssertionError("crash point not reached")
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", code,
            str(Path(__file__).resolve().parents[1]), str(self.root), phase],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        self.assertEqual(result.returncode, 73, result.stderr.decode())
        self.assertEqual(result.stdout, b"OWNED_WRITER_CRASH\n")

    def test_real_hard_exit_matrix_keeps_budget_and_original_expiry(self):
        phases = ("init_marker_pre", "init_marker_post", "reservation_marker_pre", "reservation_marker_post",
                  "reservation_journal_fsync", "capture_keys_pre", "capture_keys_post",
                  "capture_manifests_pre", "capture_manifests_post", "cleanup_marker_pre",
                  "cleanup_marker_post", "cleanup_first_unlink")
        for phase in phases:
            with self.subTest(phase=phase):
                self.root = Path(self.temp.name, phase)
                self.now = 1900000000.0
                if not phase.startswith("init_"):
                    initial = self.make(); self.capture(initial); initial.close()
                self.crash_child(phase)
                # At least one pending private temp is from the public writer,
                # not a fake file; pre marker crashes may leave the old cache.
                before = (self.root / ".failure-capture-journal-v1.jsonl").read_bytes()
                reopened = self.make()
                window = self.marker()
                self.assertEqual(window["expires_at"], 1900000000.0 + RETENTION_SECONDS)
                self.assertFalse(any(p.name.startswith(".tmp-") for p in self.root.rglob("*")))
                self.assertEqual((self.root / ".failure-capture-journal-v1.jsonl").read_bytes(), before)
                if phase.startswith("cleanup_"):
                    self.assertTrue(window["cleaned"])
                    # A cleaned journal must finish even after a clock rollback.
                    self.now -= 100
                    answer = reopened.cleanup_expired()
                    self.assertEqual(answer["status"], "cleaned")
                    self.assertFalse(any(self.counts().values()))
                    self.assertEqual(self.capture(reopened, 3)["status"], "expired")
                else:
                    count = sum(map(len, window["transfers"].values()))
                    self.assertEqual(count, 0 if phase.startswith("init_") else 2)
                    if count:
                        duplicate = self.capture(reopened, 2)
                        self.assertEqual(duplicate["status"], "already_captured")
                        self.assertNotIn("replayable", duplicate)
                        self.assertEqual(self.marker()["body_bytes"], window["body_bytes"])
                    self.now += RETENTION_SECONDS
                    self.assertEqual(reopened.cleanup_expired()["status"], "cleaned")
                    self.assertFalse(any(self.counts().values()))
                self.assertLessEqual(sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file()), MAX_TOTAL_BYTES)

    def test_real_exit_after_initial_journal_open_only_recovers_empty_bootstrap(self):
        for contamination in (None, "unknown", "nonzero", "symlink"):
            with self.subTest(contamination=contamination):
                self.root = Path(self.temp.name, "empty-journal-" + str(contamination))
                self.crash_child("init_journal_open")
                journal = self.root / ".failure-capture-journal-v1.jsonl"
                self.assertEqual(journal.read_bytes(), b"")
                self.assertFalse((self.root / ".failure-capture-v1.json").exists())
                if contamination == "unknown":
                    extra = self.root / "unknown"
                    extra.write_bytes(b"KEEP"); extra.chmod(0o600)
                elif contamination == "nonzero":
                    journal.write_bytes(b"unrecognized partial initialization")
                elif contamination == "symlink":
                    outside = Path(self.temp.name, "empty-outside")
                    outside.touch(mode=0o600)
                    journal.unlink(); journal.symlink_to(outside)
                if contamination is not None:
                    with self.assertRaises(ValueError): self.make()
                    self.assertTrue(journal.exists())
                    if contamination == "unknown": self.assertEqual(extra.read_bytes(), b"KEEP")
                    if contamination == "nonzero": self.assertEqual(journal.read_bytes(), b"unrecognized partial initialization")
                    if contamination == "symlink": self.assertTrue(journal.is_symlink())
                else:
                    reopened = self.make()
                    self.assertTrue(reopened.cleanup_enabled)
                    self.assertEqual(self.marker()["transfers"], {})
                    self.assertEqual(self.marker()["body_bytes"], 0)
                    self.assertEqual(self.capture(reopened)["status"], "captured")

    def test_recovery_itself_can_exit_after_owned_temp_unlink(self):
        value = self.make(); self.capture(value); value.close()
        self.crash_child("capture_keys_pre")
        before = (self.root / ".failure-capture-journal-v1.jsonl").read_bytes()
        self.crash_child("recovery_temp_unlink")
        reopened = self.make()
        self.assertEqual((self.root / ".failure-capture-journal-v1.jsonl").read_bytes(), before)
        self.assertEqual(sum(map(len, self.marker()["transfers"].values())), 2)
        self.now += RETENTION_SECONDS
        self.assertEqual(reopened.cleanup_expired()["status"], "cleaned")
        self.assertFalse(any(self.counts().values()))

    def test_real_torn_journal_keeps_expiry_cleanup_and_never_refills(self):
        value = self.make(); self.capture(value); value.close()
        self.crash_child("torn_reservation")
        journal = self.root / ".failure-capture-journal-v1.jsonl"
        torn = journal.read_bytes()
        self.assertFalse(torn.endswith(b"\n"))
        reopened = self.make()
        self.assertFalse(reopened.enabled); self.assertTrue(reopened.cleanup_enabled)
        self.assertEqual(self.capture(reopened, 2)["status"], "disabled")
        self.assertEqual(reopened.cleanup_expired()["status"], "not_expired")
        self.assertEqual(journal.read_bytes(), torn)
        self.now += RETENTION_SECONDS
        self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 3})
        self.assertFalse(any(self.counts().values()))
        self.assertEqual(journal.read_bytes(), torn)
        self.now -= RETENTION_SECONDS + 1
        again = self.make()
        self.assertFalse(again.enabled); self.assertTrue(again.cleanup_enabled)
        self.assertEqual(self.capture(again, 2)["status"], "disabled")
        self.assertFalse(any(self.counts().values()))

    def test_cleanup_partial_reports_actual_unlinks_then_retries_via_serve(self):
        from wechat_bridge.serve import _cleanup_failure_capture
        value = self.make(); self.capture(value); self.capture(value, 2)
        self.now += RETENTION_SECONDS
        real_unlink = os.unlink
        calls = [0]
        def fail_second_body(name, *args, **kwargs):
            if name.endswith(".bin"):
                calls[0] += 1
                if calls[0] == 2: raise OSError("synthetic deletion failure")
            return real_unlink(name, *args, **kwargs)
        with patch("wechat_bridge.failure_capture.os.unlink", fail_second_body):
            result = value.cleanup_expired()
        self.assertEqual(result, {"status": "cleanup_partial", "deleted": 1})
        self.assertFalse(value.enabled); self.assertTrue(value.cleanup_enabled)
        self.assertEqual(sum(self.counts().values()), 5)
        self.now -= RETENTION_SECONDS + 10
        _cleanup_failure_capture(SimpleNamespace(failure_capture=value))
        self.assertFalse(any(self.counts().values()))
        self.assertTrue(self.marker()["cleaned"])
        journal = (self.root / ".failure-capture-journal-v1.jsonl").read_bytes()
        self.assertEqual(value.cleanup_expired(), {"status": "cleaned", "deleted": 0})
        self.assertEqual((self.root / ".failure-capture-journal-v1.jsonl").read_bytes(), journal)

    def test_recovered_temp_unlink_is_counted_before_directory_fsync_failure(self):
        value = self.make(); self.capture(value)
        self.crash_child("capture_keys_pre")
        pending = next((self.root / "keys").glob(".tmp-capture-*"))
        keys_inode = os.stat(self.root / "keys")
        real_fsync = os.fsync
        def fail_after_temp_unlink(fd):
            info = os.fstat(fd)
            if ((info.st_dev, info.st_ino) == (keys_inode.st_dev, keys_inode.st_ino)
                    and not pending.exists()):
                raise OSError("synthetic directory sync failure")
            return real_fsync(fd)
        self.now += RETENTION_SECONDS
        with patch("wechat_bridge.failure_capture.os.fsync", fail_after_temp_unlink):
            self.assertEqual(value.cleanup_expired(), {"status": "cleanup_partial", "deleted": 1})
        self.assertFalse(pending.exists())
        self.assertFalse(value.enabled); self.assertTrue(value.cleanup_enabled)
        self.assertEqual(value.cleanup_expired(), {"status": "cleaned", "deleted": 4})
        self.assertFalse(any(self.counts().values()))

    def test_valid_legacy_marker_bootstrap_keeps_budget_and_ttl(self):
        value = self.make(); self.capture(value); value.close()
        before = self.marker()
        (self.root / ".failure-capture-journal-v1.jsonl").unlink()
        self.now += 500
        reopened = self.make()
        self.assertEqual(self.marker(), before)
        self.assertEqual(self.capture(reopened)["status"], "already_captured")
        self.now = before["expires_at"]
        self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 3})

    def seed_original_legacy_module(self, cleaned=False):
        # Exact versioned legacy bytes; public snapshots need no old Git object.
        fixtures = Path(__file__).resolve().parent / "fixtures" / "legacy_capture_v1"
        expected = {
            "failure_capture.py": "9f05aacf7b16d4231cb8a53b0910250bd728dbb6448057a63a8bed39305c751b",
            "media_diagnostics.py": "138a24e589af9891341e087c692f3d3d036d148d81ad6ca138b1325e148968e6",
        }
        originals = {}
        for name, digest in expected.items():
            try:
                raw = (fixtures / name).read_bytes()
            except OSError:
                self.fail("bundled legacy fixture missing or unreadable: " + name)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), digest,
                             "bundled legacy fixture hash mismatch: " + name)
            originals[name] = raw
        source = Path(self.temp.name, "legacy-source")
        package = source / "wechat_bridge"
        if not source.exists():
            try:
                with tempfile.TemporaryDirectory(prefix="legacy-source-stage-", dir=self.temp.name) as staging:
                    staged = Path(staging, "source", "wechat_bridge")
                    staged.mkdir(parents=True)
                    (staged / "__init__.py").write_bytes(b"")
                    for name, raw in originals.items():
                        (staged / name).write_bytes(raw)
                    os.replace(staged.parent, source)
            except OSError:
                self.fail("could not stage bundled legacy fixture")
        try:
            self.assertEqual((package / "__init__.py").read_bytes(), b"")
            for name, raw in originals.items():
                self.assertEqual((package / name).read_bytes(), raw,
                                 "cached legacy fixture mismatch: " + name)
        except OSError:
            self.fail("cached legacy fixture is incomplete")
        self.assertEqual(hashlib.sha256((package / "failure_capture.py").read_bytes()).hexdigest(),
                         "9f05aacf7b16d4231cb8a53b0910250bd728dbb6448057a63a8bed39305c751b")
        code = r'''
import sys
def no_network(event,args):
    if event in ("socket.connect","socket.connect_ex","socket.getaddrinfo","socket.bind"):
        raise AssertionError("network forbidden")
sys.addaudithook(no_network)
sys.path.insert(0,sys.argv[1])
from wechat_bridge.failure_capture import FailureCapture,RETENTION_SECONDS
value=FailureCapture(sys.argv[2],enabled=True,clock=lambda:1900000000.0)
assert value.capture("%032x"%1,"param",b"SYNTHETIC-CIPHER",b"0123456789abcdef",None,"image","image_invalid")["status"]=="captured"
if sys.argv[3]=="cleaned": assert value.cleanup_expired(now=1900000000.0+RETENTION_SECONDS)["deleted"]==3
value.close()
'''
        child = subprocess.run([sys.executable, "-I", "-B", "-c", code,
            str(source), str(self.root), "cleaned" if cleaned else "active"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        self.assertEqual(child.returncode, 0, child.stderr.decode())
        self.assertFalse((self.root / ".failure-capture-journal-v1.jsonl").exists())

    def test_real_legacy_bootstrap_zero_and_half_write_can_crash_again(self):
        recovery_points = (None, "bootstrap_unlink", "init_journal_open", "init_journal_half", "bootstrap_journal_fsync")
        for first in ("init_journal_open", "init_journal_half"):
            for again in recovery_points:
                with self.subTest(first=first, recovery=again):
                    self.root = Path(self.temp.name, "legacy-" + first + "-" + str(again))
                    self.now = 1900000060.0
                    self.seed_original_legacy_module()
                    original = self.marker()
                    marker_bytes = (self.root / ".failure-capture-v1.json").read_bytes()
                    self.crash_child(first)
                    if again is not None: self.crash_child(again)
                    reopened = self.make()
                    self.assertEqual(self.marker(), original)
                    self.assertEqual((self.root / ".failure-capture-v1.json").read_bytes(), marker_bytes)
                    duplicate = self.capture(reopened)
                    self.assertEqual(duplicate["status"], "already_captured")
                    self.assertNotIn("replayable", duplicate)
                    self.assertEqual(self.marker()["body_bytes"], original["body_bytes"])
                    self.now = original["expires_at"]
                    self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 3})
                    self.assertFalse(any(self.counts().values()))
                    self.assertEqual(self.marker(), dict(original, cleaned=True))
        # A legacy cleaned window is never resurrected by migration or rollback.
        for first in ("init_journal_open", "init_journal_half"):
            with self.subTest(cleaned=True, first=first):
                self.root = Path(self.temp.name, "legacy-cleaned-" + first)
                self.seed_original_legacy_module(cleaned=True)
                original = self.marker()
                self.crash_child(first)
                self.now = original["created_at"] - 1
                reopened = self.make()
                self.assertEqual(self.marker(), original)
                self.assertEqual(self.capture(reopened, 2)["status"], "expired")
                self.assertFalse(any(self.counts().values()))

    def test_legacy_bootstrap_unknown_prefix_or_tree_stays_untouched(self):
        for contamination in ("mismatch", "unknown_file", "marker_changed"):
            with self.subTest(contamination=contamination):
                self.root = Path(self.temp.name, "legacy-protect-" + contamination)
                value = self.make(); self.capture(value); value.close()
                window = self.marker()
                _, expected = FailureCapture._journal_line(window, [])
                journal = self.root / ".failure-capture-journal-v1.jsonl"
                raw = expected[:len(expected) // 2]
                if contamination == "mismatch": raw = b"unrecognized partial journal"
                journal.write_bytes(raw)
                if contamination == "unknown_file":
                    extra = self.root / "keys" / "unknown"
                    extra.write_bytes(b"KEEP"); extra.chmod(0o600)
                if contamination == "marker_changed":
                    marker = self.root / ".failure-capture-v1.json"
                    marker.write_text(json.dumps(dict(window, window_id="f" * 32)))
                with self.assertRaises(ValueError): self.make()
                self.assertEqual(journal.read_bytes(), raw)
                if contamination == "unknown_file": self.assertEqual(extra.read_bytes(), b"KEEP")

    def test_torn_cleanup_latch_survives_crash_and_rollback(self):
        for phase in ("cleanup_marker_pre", "cleanup_marker_post", "cleanup_first_unlink"):
            for again in (None, "recovery_marker_pre", "recovery_marker_post"):
                if again is not None and phase != "cleanup_marker_pre": continue
                with self.subTest(phase=phase, recovery=again):
                    self.root = Path(self.temp.name, "torn-latch-" + phase + "-" + str(again))
                    self.now = 1900000000.0
                    value = self.make(); self.capture(value); value.close()
                    original = self.marker()
                    self.crash_child("torn_reservation")
                    journal = self.root / ".failure-capture-journal-v1.jsonl"
                    before = journal.read_bytes()
                    self.crash_child(phase)
                    if again is not None: self.crash_child(again)
                    self.now = original["created_at"] - 10
                    reopened = self.make()
                    self.assertFalse(reopened.enabled); self.assertTrue(reopened.cleanup_enabled)
                    self.assertEqual(self.marker(), dict(original, cleaned=True))
                    self.assertEqual(reopened.cleanup_expired()["status"], "cleaned")
                    self.assertFalse(any(self.counts().values()))
                    self.assertEqual(self.capture(reopened, 2)["status"], "disabled")
                    self.assertEqual(journal.read_bytes(), before)
                    self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 0})

    def test_torn_cleaned_latch_must_match_latest_window_exactly(self):
        for target in ("marker", "pending"):
            with self.subTest(target=target):
                self.root = Path(self.temp.name, "invalid-latch-" + target)
                value = self.make(); self.capture(value); value.close()
                self.crash_child("torn_reservation")
                if target == "pending":
                    self.crash_child("cleanup_marker_pre")
                    path = next(self.root.glob(".tmp-capture-*-marker.json"))
                else:
                    path = self.root / ".failure-capture-v1.json"
                invalid = dict(self.marker(), cleaned=True, window_id="e" * 32)
                path.write_text(json.dumps(invalid))
                original_bytes = path.read_bytes()
                with self.assertRaises(ValueError): self.make()
                self.assertEqual(path.read_bytes(), original_bytes)
                self.assertEqual(self.counts(), {"bodies": 1, "keys": 1, "manifests": 1})

    def test_pending_latch_strict_types_before_equality_or_promotion(self):
        for mutation in (None, "cleaned_int", "version_bool", "body_bytes_float"):
            with self.subTest(mutation=mutation):
                self.root = Path(self.temp.name, "pending-types-" + str(mutation))
                self.now = 1900000000.0
                value = self.make(); self.capture(value); value.close()
                self.crash_child("torn_reservation")
                self.crash_child("cleanup_marker_pre")
                pending = next(self.root.glob(".tmp-capture-*-marker.json"))
                temporary = json.loads(pending.read_bytes())
                if mutation == "cleaned_int": temporary["cleaned"] = 1
                elif mutation == "version_bool": temporary["version"] = True
                elif mutation == "body_bytes_float": temporary["body_bytes"] = float(temporary["body_bytes"])
                if mutation is not None:
                    pending.write_text(json.dumps(temporary, separators=(",", ":")))
                before = {str(path.relative_to(self.root)): path.read_bytes()
                          for path in self.root.rglob("*") if path.is_file()}
                if mutation is not None:
                    # Repeat construction to prove neither the pending bytes
                    # nor the official marker/sample state was changed first.
                    for _ in range(2):
                        with self.assertRaisesRegex(ValueError, "failure_capture_configuration_invalid"):
                            self.make()
                        after = {str(path.relative_to(self.root)): path.read_bytes()
                                 for path in self.root.rglob("*") if path.is_file()}
                        self.assertEqual(after, before)
                        self.assertTrue(pending.exists())
                        self.assertEqual(self.counts(), {"bodies": 1, "keys": 1, "manifests": 1})
                else:
                    self.now -= 10
                    reopened = self.make()
                    self.assertFalse(pending.exists())
                    self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 3})
                    self.assertEqual(reopened.cleanup_expired(), {"status": "cleaned", "deleted": 0})
                    self.assertFalse(any(self.counts().values()))
                    marker = self.marker()
                    self.assertIs(type(marker["version"]), int)
                    self.assertIs(type(marker["cleaned"]), bool)
                    self.assertIs(type(marker["body_bytes"]), int)

    def test_private_empty_bootstrap_recovers_lock_and_empty_directories(self):
        self.root.mkdir(mode=0o700)
        for directory in ("bodies", "keys", "manifests"):
            (self.root / directory).mkdir(mode=0o700)
        lock = self.root / ".failure-capture.lock"
        lock.touch(mode=0o600)
        self.assertTrue(self.make().cleanup_enabled)

    def test_old_random_temp_and_unregistered_temp_are_never_deleted(self):
        value = self.make(); self.capture(value); value.close()
        for name in (".tmp-" + "f" * 32,
                     ".tmp-capture-%s-1-keys-%s.json" % (self.marker()["window_id"], "f" * 32)):
            with self.subTest(name=name):
                path = self.root / "keys" / name
                path.write_bytes(b"UNKNOWN-KEEP"); path.chmod(0o600)
                with self.assertRaises(ValueError): self.make()
                self.assertEqual(path.read_bytes(), b"UNKNOWN-KEEP")
                path.unlink()

    def test_real_owned_temp_replaced_by_link_is_protected(self):
        value = self.make(); self.capture(value); value.close()
        self.crash_child("capture_keys_pre")
        pending = next((self.root / "keys").glob(".tmp-capture-*"))
        outside = Path(self.temp.name, "not-owned")
        outside.write_bytes(b"KEEP"); outside.chmod(0o600)
        pending.unlink(); pending.symlink_to(outside)
        with self.assertRaises(ValueError): self.make()
        self.assertTrue(pending.is_symlink()); self.assertEqual(outside.read_bytes(), b"KEEP")
        pending.unlink(); os.link(outside, pending)
        with self.assertRaises(ValueError): self.make()
        self.assertTrue(pending.exists()); self.assertEqual(outside.read_bytes(), b"KEEP")

    @staticmethod
    def journal_line(record):
        raw = json.dumps(record, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        return hashlib.sha256(raw).hexdigest().encode("ascii") + b" " + raw + b"\n"

    def test_journal_hash_chain_does_not_replace_monotonic_state_checks(self):
        mutations = ("window_id", "created_at", "expires_at", "replace_sample", "remove_source",
                     "refund_bytes", "two_sources", "bad_revision", "bad_previous", "bad_hash", "revive_cleaned")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.root = Path(self.temp.name, "journal-" + mutation)
                self.now = 1900000000.0
                value = self.make(); self.capture(value)
                if mutation == "revive_cleaned":
                    self.now += RETENTION_SECONDS; value.cleanup_expired()
                value.close()
                path = self.root / ".failure-capture-journal-v1.jsonl"
                original = path.read_bytes()
                lines = original.splitlines()
                previous, raw = lines[-1].split(b" ", 1)
                last = json.loads(raw)
                window = json.loads(json.dumps(last["window"]))
                window["transfers"]["%032x" % 2] = {"param": "a" * 32}
                window["body_bytes"] += 1
                first = window["transfers"]["%032x" % 1]
                if mutation in ("created_at", "expires_at"): window[mutation] += 1
                elif mutation == "window_id": window[mutation] = "b" * 32
                elif mutation == "replace_sample": first["param"] = "c" * 32
                elif mutation == "remove_source": del window["transfers"]["%032x" % 1]
                elif mutation == "refund_bytes": window["body_bytes"] = 0
                elif mutation == "two_sources": window["transfers"]["%032x" % 2]["full_url"] = "d" * 32
                elif mutation == "revive_cleaned": window["cleaned"] = False
                record = {"version": 1, "revision": last["revision"] + 1,
                          "previous": previous.decode(), "window": window}
                if mutation == "bad_revision": record["revision"] = True
                if mutation == "bad_previous": record["previous"] = "0" * 64
                line = self.journal_line(record)
                if mutation == "bad_hash": line = b"0" * 64 + line[64:]
                path.write_bytes(original + line)
                with self.assertRaises(ValueError): self.make()
                self.assertEqual(path.read_bytes(), original + line)


if __name__ == "__main__": unittest.main(verbosity=2)
