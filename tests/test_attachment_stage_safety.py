"""Synthetic receiver stage safety; no real transport, account, or samples.

The swap probes inject a chosen ordering at syscall boundaries. They do not
claim to enumerate real concurrent filesystem races or operating systems.
"""
import contextlib
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import test_attachment_runtime as fixtures
from wechat_bridge import attachment_runtime as runtime, media, serve
from wechat_bridge.store import load_json


class StageSafety(unittest.TestCase):
    setUp = fixtures.RuntimeTests.setUp
    message = fixtures.RuntimeTests.message
    transport = fixtures.RuntimeTests.transport

    def reserve(self, mid="synthetic-stage", body=b"SYNTHETIC original stage bytes\x00\x01"):
        message = self.message(mid, attachments=[("file", body, "SYNTHETIC-stage.bin")])
        _, event = runtime.receive_event(self.store, message, fixtures.OWNER,
            [{"item_index": 0, "kind": "file", "original_name": "SYNTHETIC-stage.bin",
              "declared_type": None}], "", time.time())
        reference = event["attachment_refs"][0]
        self.assertEqual(reference["status"], "pending")
        return message, reference, Path(reference["stage_path"]), body

    def receive(self, message):
        with patch.object(media.urllib.request, "urlopen", side_effect=self.transport):
            return serve.handle_incoming(self.store, message, fixtures.OWNER)

    def metadata(self, message):
        rows = runtime.catalog(self.store).query(self.scope, {"message_id": message["message_id"]})["items"]
        self.assertEqual(len(rows), 1)
        return rows[0]

    def ready_original(self, message, body):
        metadata = self.metadata(message)
        self.assertEqual(metadata["status"], "ready")
        material = runtime.catalog(self.store).acquire(self.scope, metadata["attachment_id"], "synthetic-stage-test")
        path = Path(material["path"])
        self.assertEqual(path.read_bytes(), body)
        self.assertEqual(metadata["sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        return path

    @staticmethod
    def fingerprint(path):
        info = path.stat()
        return {"bytes": path.read_bytes(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": info.st_size, "inode": info.st_ino, "nlink": info.st_nlink,
                "mtime_ns": info.st_mtime_ns}

    @staticmethod
    def fd_fingerprint(fd):
        info = os.fstat(fd)
        data = os.pread(fd, info.st_size, 0)
        return {"bytes": data, "sha256": hashlib.sha256(data).hexdigest(), "size": info.st_size,
                "inode": info.st_ino, "mtime_ns": info.st_mtime_ns}

    def sentinel(self, name="synthetic-external-sentinel"):
        path = self.home / name
        path.write_bytes(b"SYNTHETIC owned external sentinel must remain unchanged")
        path.chmod(0o600)
        return path

    @contextlib.contextmanager
    def watch_stage_io(self, stage, fault=None, before_open=None):
        """Observe real stage FDs and optionally fail a real IO boundary."""
        opened, writers, truncating, writes = [], {}, [], []
        actual_open, actual_fdopen = os.open, os.fdopen
        actual_fsync, actual_replace, actual_fstat = os.fsync, os.replace, os.fstat
        parent = stage.parent.stat()
        faulted = []
        def in_parent(fd):
            if fd is None:
                return False
            info = actual_fstat(fd)
            return (info.st_dev, info.st_ino) == (parent.st_dev, parent.st_ino)
        def relevant(path, kwargs):
            return in_parent(kwargs.get("dir_fd")) or Path(os.fspath(path)).absolute().parent == stage.parent
        def tracked_open(path, flags, *args, **kwargs):
            target = relevant(path, kwargs)
            if target and before_open is not None:
                before_open(path, flags, kwargs)
            if target and flags & os.O_TRUNC:
                truncating.append((os.fspath(path), flags))
            fd = actual_open(path, flags, *args, **kwargs)
            info = actual_fstat(fd)
            if target or (info.st_dev, info.st_ino) == (parent.st_dev, parent.st_ino):
                opened.append(fd)
            if target:
                if flags & (os.O_WRONLY | os.O_RDWR) and stat.S_ISREG(info.st_mode):
                    writers[fd] = (flags, (info.st_dev, info.st_ino))
            return fd
        owner = self
        class Writer:
            def __init__(self, value):
                self.value = value
            def __getattr__(self, key):
                return getattr(self.value, key)
            def __enter__(self):
                self.value.__enter__()
                return self
            def __exit__(self, *args):
                return self.value.__exit__(*args)
            def write(self, data):
                fd = self.value.fileno()
                info = actual_fstat(fd)
                flags, identity = writers[fd]
                owner.assertEqual((info.st_dev, info.st_ino), identity)
                owner.assertTrue(flags & os.O_EXCL)
                owner.assertTrue(flags & os.O_CREAT)
                owner.assertFalse(flags & os.O_TRUNC)
                owner.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
                owner.assertEqual(info.st_uid, os.geteuid())
                owner.assertEqual(info.st_nlink, 1)
                owner.assertEqual(stage.stat().st_ino, info.st_ino)
                owner.assertEqual(sorted(p.name for p in stage.parent.iterdir()), [stage.name])
                writes.append({"fd": fd, "inode": info.st_ino, "flags": flags, "bytes": bytes(data)})
                if fault == "write" and not faulted:
                    faulted.append("write")
                    self.value.write(data[:7])
                    raise OSError(errno.ENOSPC, "synthetic stage write fault")
                return self.value.write(data)
        def tracked_fdopen(fd, mode="r", *args, **kwargs):
            value = actual_fdopen(fd, mode, *args, **kwargs)
            info = actual_fstat(fd)
            identity = (info.st_dev, info.st_ino)
            return Writer(value) if mode == "wb" and fd in writers and writers[fd][1] == identity else value
        def tracked_fsync(fd):
            info = actual_fstat(fd)
            if (fault == "fsync" and not faulted and fd in writers and
                    writers[fd][1] == (info.st_dev, info.st_ino)):
                faulted.append("fsync")
                raise OSError(errno.ENOSPC, "synthetic stage fsync fault")
            return actual_fsync(fd)
        def tracked_replace(src, dst, *args, **kwargs):
            target = (in_parent(kwargs.get("dst_dir_fd")) and os.fspath(dst) == stage.name or
                      Path(os.fspath(dst)).absolute() == stage)
            if fault == "replace" and target and not faulted:
                faulted.append("replace")
                raise OSError(errno.EIO, "synthetic stage replace fault")
            return actual_replace(src, dst, *args, **kwargs)
        with patch.object(media.os, "open", tracked_open), \
                patch.object(media.os, "fdopen", tracked_fdopen), \
                patch.object(media.os, "fsync", tracked_fsync), \
                patch.object(media.os, "replace", tracked_replace):
            yield {"opened": opened, "writers": writers, "writes": writes,
                   "truncating": truncating, "faulted": faulted}
        for fd in set(opened):
            with self.assertRaises(OSError) as closed:
                actual_fstat(fd)
            self.assertEqual(closed.exception.errno, errno.EBADF)

    def test_reserved_missing_stage_writes_exclusive_private_inode_and_exact_original(self):
        message, _, stage, body = self.reserve()
        self.assertFalse(stage.exists())
        self.assertEqual(stat.S_IMODE(stage.parent.stat().st_mode), 0o700)
        with self.watch_stage_io(stage) as trace:
            self.assertTrue(self.receive(message))
        self.assertEqual(trace["truncating"], [])
        self.assertEqual([row["bytes"] for row in trace["writes"]], [body])
        self.ready_original(message, body)
        self.assertEqual(list(stage.parent.iterdir()), [])

    def test_reserved_hardlink_rejects_without_changing_external_sentinel_any_fingerprint(self):
        message, _, stage, _ = self.reserve()
        victim = self.sentinel()
        os.link(str(victim), str(stage))
        before = self.fingerprint(victim)
        self.assertEqual(before["nlink"], 2)
        with self.watch_stage_io(stage) as trace:
            self.assertTrue(self.receive(message))
        self.assertEqual(self.fingerprint(victim), before)
        self.assertNotEqual(self.metadata(message)["status"], "ready")
        self.assertEqual(trace["truncating"], [])
        self.assertEqual(trace["writes"], [])

    def test_legitimate_pending_partial_uses_new_inode_without_writing_the_old_inode(self):
        message, _, stage, body = self.reserve()
        stage.write_bytes(b"SYNTHETIC attributable interrupted partial")
        stage.chmod(0o600)
        old_fd = os.open(str(stage), os.O_RDONLY)
        try:
            before = self.fd_fingerprint(old_fd)
            with self.watch_stage_io(stage) as trace:
                self.assertTrue(self.receive(message))
            self.assertEqual(self.fd_fingerprint(old_fd), before)
            self.assertEqual(trace["truncating"], [])
            self.assertEqual(len(trace["writes"]), 1)
            self.assertNotEqual(trace["writes"][0]["inode"], before["inode"])
        finally:
            os.close(old_fd)
        self.ready_original(message, body)
        self.assertEqual(list(stage.parent.iterdir()), [])

    def test_ready_catalog_replay_repairs_receipt_without_download_or_object_rewrite(self):
        message, _, stage, body = self.reserve()
        self.assertTrue(self.receive(message))
        original = self.ready_original(message, body)
        before = self.fingerprint(original)
        (self.base / "inbox" / (message["message_id"] + ".json")).unlink()
        (self.base / "state/received" / (message["message_id"] + ".json")).unlink()
        with patch.object(media.urllib.request, "urlopen", side_effect=AssertionError("ready replay must not download")) as http:
            self.assertTrue(serve.handle_incoming(self.store, message, fixtures.OWNER))
        http.assert_not_called()
        self.assertEqual(self.fingerprint(original), before)
        self.assertEqual(self.metadata(message)["status"], "ready")
        self.assertEqual(list(stage.parent.iterdir()), [])

    def test_invalid_symlink_fifo_directory_or_public_file_is_rejected_without_external_write(self):
        for kind in ("symlink", "fifo", "directory", "0644"):
            with self.subTest(kind=kind):
                message, _, stage, _ = self.reserve("synthetic-stage-" + kind)
                victim = self.sentinel("synthetic-external-" + kind)
                before = self.fingerprint(victim)
                if kind == "symlink":
                    stage.symlink_to(victim)
                elif kind == "fifo":
                    os.mkfifo(str(stage), 0o600)
                elif kind == "directory":
                    stage.mkdir(mode=0o700)
                else:
                    stage.write_bytes(b"SYNTHETIC public partial must not be touched")
                    stage.chmod(0o644)
                    partial_before = self.fingerprint(stage)
                with self.watch_stage_io(stage) as trace:
                    self.receive(message)
                self.assertEqual(self.fingerprint(victim), before)
                if kind == "0644":
                    self.assertEqual(self.fingerprint(stage), partial_before)
                self.assertNotEqual(self.metadata(message)["status"], "ready")
                self.assertEqual(trace["truncating"], [])
                self.assertEqual(trace["writes"], [])
                # Remove this deliberately unsafe synthetic node before the
                # next case registers an independent pending attachment.
                if kind == "directory" and stage.is_dir():
                    stage.rmdir()
                elif os.path.lexists(str(stage)):
                    stage.unlink()

    def test_nonprivate_staging_directory_is_rejected_without_mutating_partial(self):
        message, _, stage, _ = self.reserve()
        stage.write_bytes(b"SYNTHETIC protected partial")
        stage.chmod(0o600)
        before = self.fingerprint(stage)
        stage.parent.chmod(0o755)
        try:
            self.receive(message)
        finally:
            stage.parent.chmod(0o700)
        self.assertEqual(self.fingerprint(stage), before)
        self.assertNotEqual(self.metadata(message)["status"], "ready")

    def test_controlled_swap_before_file_validation_never_writes_external_inode(self):
        message, _, stage, _ = self.reserve()
        stage.write_bytes(b"SYNTHETIC prior partial")
        stage.chmod(0o600)
        victim = self.sentinel()
        expected = []
        def swap_before_open(path, flags, kwargs):
            if os.fspath(path) == stage.name and not flags & (os.O_CREAT | os.O_TRUNC) and not expected:
                stage.unlink()
                os.link(str(victim), str(stage))
                expected.append(self.fingerprint(victim))
        with self.watch_stage_io(stage, before_open=swap_before_open) as trace:
            self.receive(message)
        self.assertEqual(len(expected), 1)
        self.assertEqual(self.fingerprint(victim), expected[0])
        self.assertNotEqual(self.metadata(message)["status"], "ready")
        self.assertEqual(trace["writes"], [])
        self.assertEqual(trace["truncating"], [])

    def test_controlled_swap_after_validation_does_not_write_replaced_external_inode(self):
        message, _, stage, _ = self.reserve()
        stage.write_bytes(b"SYNTHETIC prior partial")
        stage.chmod(0o600)
        victim = self.sentinel()
        expected = []
        def swap_before_fresh(path, flags, kwargs):
            if flags & os.O_CREAT and flags & os.O_EXCL and os.fspath(path) != stage.name and not expected:
                stage.unlink()
                os.link(str(victim), str(stage))
                expected.append(self.fingerprint(victim))
        with self.watch_stage_io(stage, before_open=swap_before_fresh) as trace:
            self.receive(message)
        self.assertEqual(len(expected), 1)
        self.assertEqual(self.fingerprint(victim), expected[0])
        self.assertNotEqual(self.metadata(message)["status"], "ready")
        self.assertEqual(trace["truncating"], [])
        self.assertEqual(trace["writes"], [])

    def test_real_child_exit_leaves_only_empty_owned_auxiliary_then_receiver_retry_recovers(self):
        message, _, stage, body = self.reserve()
        stage.write_bytes(b"SYNTHETIC attributable earlier interrupted partial")
        stage.chmod(0o600)
        before = self.fingerprint(stage)
        baseline_usage = runtime.catalog(self.store).preview_gc()
        auxiliary = stage.parent / ("." + stage.name + ".empty")
        child = r'''
import hashlib,io,json,os,socket,sys
from pathlib import Path
from unittest.mock import patch
payload=json.loads(sys.stdin.read())
def deny(*a,**kw): raise AssertionError("synthetic stage child forbids sockets")
for key in ("connect","connect_ex","bind","listen"): setattr(socket.socket,key,deny)
socket.create_connection=socket.getaddrinfo=deny
sys.path.insert(0,payload["root"])
from wechat_bridge import media,serve
from wechat_bridge.store import Store
store=Store(payload["base"])
encrypted=bytes.fromhex(payload["encrypted"])
class Response(io.BytesIO):
    status=200
    headers={"Content-Length":str(len(encrypted)),"Content-Type":"application/octet-stream"}
    def geturl(self): return "https://synthetic.invalid/body"
actual_open=os.open
def cut_after_empty_create(path,flags,*args,**kwargs):
    fd=actual_open(path,flags,*args,**kwargs)
    if os.fspath(path)==payload["auxiliary"] and flags & os.O_CREAT and flags & os.O_EXCL:
        print(json.dumps({"point":"actual_O_EXCL_empty_auxiliary_created","size":os.fstat(fd).st_size,
            "module":media.__file__,"media_sha256":hashlib.sha256(Path(media.__file__).read_bytes()).hexdigest()}),flush=True)
        os._exit(77)
    return fd
with patch.object(media.urllib.request,"urlopen",lambda *a,**kw:Response(encrypted)),patch.object(media.os,"open",cut_after_empty_create):
    serve.handle_incoming(store,payload["message"],payload["owner"])
raise SystemExit(12)
'''
        request = {"root": str(ROOT), "base": self.store.base, "message": message,
                   "owner": fixtures.OWNER, "auxiliary": auxiliary.name,
                   "encrypted": self.bodies[message["message_id"] + "-0"].hex()}
        result = subprocess.run([sys.executable, "-I", "-B", "-c", child],
            input=json.dumps(request), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=15)
        self.assertEqual(result.returncode, 77, result.stdout)
        marker = next(json.loads(line) for line in result.stdout.splitlines()
                      if line.startswith('{"point":'))
        self.assertEqual(marker["point"], "actual_O_EXCL_empty_auxiliary_created")
        self.assertEqual(marker["size"], 0)
        self.assertEqual(marker["media_sha256"], hashlib.sha256(Path(media.__file__).read_bytes()).hexdigest())
        self.assertEqual(self.fingerprint(stage), before)
        self.assertEqual(auxiliary.stat().st_size, 0)
        self.assertEqual(stat.S_IMODE(auxiliary.stat().st_mode), 0o600)
        self.assertEqual(sorted(p.name for p in stage.parent.iterdir()), sorted([stage.name, auxiliary.name]))
        usage = runtime.catalog(self.store).preview_gc()
        self.assertEqual(usage["unknown_files"], [])
        for field in ("physical_bytes", "reserved_bytes", "accounted_bytes"):
            self.assertEqual(usage[field], baseline_usage[field])
        self.assertEqual(self.metadata(message)["status"], "pending")
        with self.watch_stage_io(stage) as trace:
            self.assertTrue(self.receive(message))
        self.assertEqual(trace["truncating"], [])
        self.assertEqual([row["bytes"] for row in trace["writes"]], [body])
        self.ready_original(message, body)
        self.assertEqual(list(stage.parent.iterdir()), [])
        print("synthetic_stage_child_exit " + json.dumps({"exit": 77, "empty_aux_bytes": 0,
              "usage_unknown": [], "accounted_bytes_unchanged": True, "retry": "ready"}))

    def assert_io_failure(self, kind):
        message, _, stage, _ = self.reserve()
        stage.write_bytes(b"SYNTHETIC legitimate partial before IO fault")
        stage.chmod(0o600)
        before = self.fingerprint(stage)
        with self.watch_stage_io(stage, fault=kind) as trace:
            self.receive(message)
        self.assertEqual(trace["faulted"], [kind])
        self.assertNotEqual(self.metadata(message)["status"], "ready")
        self.assertEqual(trace["truncating"], [])
        self.assertTrue(set(p.name for p in stage.parent.iterdir()) <= {stage.name})
        if kind == "replace":
            self.assertEqual(self.fingerprint(stage), before)

    def test_write_fault_is_not_ready_and_closes_stage_fds(self):
        self.assert_io_failure("write")

    def test_fsync_fault_is_not_ready_and_closes_stage_fds(self):
        self.assert_io_failure("fsync")

    def test_replace_fault_is_not_ready_preserves_prior_inode_and_closes_stage_fds(self):
        self.assert_io_failure("replace")


if __name__ == "__main__":
    unittest.main(verbosity=2)
