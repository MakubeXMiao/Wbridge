"""Hold an existing bridge state root offline for read-only rollback inspection."""

import argparse
from contextlib import ExitStack, contextmanager
import fcntl
import json
import os
import signal
import stat

from .media_retention import PIN_NAME, _checked_root


def _identity(info):
    return info.st_dev, info.st_ino


@contextmanager
def hold(base):
    """Exclude Store users of this root for as long as the guard is held."""
    root = _checked_root(base)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    lock_flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
    with ExitStack() as stack:
        root_fd = os.open(root, directory_flags)
        stack.callback(os.close, root_fd)
        state_fd = os.open("state", directory_flags, dir_fd=root_fd)
        stack.callback(os.close, state_fd)
        for fd in (root_fd, state_fd):
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                raise ValueError("unsafe rollback directory")

        locks = {}
        # Both releases pin media_usage.lock before Store can enter recovery.
        for name in (PIN_NAME, "serve.lock"):
            try:
                fd = os.open(name, lock_flags, dir_fd=state_fd)
            except FileNotFoundError:
                if name == PIN_NAME:
                    raise
                continue
            stack.callback(os.close, fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size
                    or info.st_uid != os.geteuid() or info.st_mode & 0o022):
                raise ValueError("unsafe rollback lock")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locks[name] = _identity(info)

        # The maintenance CLI can write the review journal without a Store.
        fcntl.flock(state_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        if (_identity(root.lstat()) != _identity(os.fstat(root_fd))
                or _identity(os.stat("state", dir_fd=root_fd, follow_symlinks=False))
                != _identity(os.fstat(state_fd))):
            raise ValueError("rollback directory replaced")
        for name, identity in locks.items():
            if _identity(os.stat(name, dir_fd=state_fd, follow_symlinks=False)) != identity:
                raise ValueError("rollback lock replaced")
        yield


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="absolute path to an existing bridge state root")
    args = parser.parse_args(argv)
    try:
        with hold(args.base):
            print(json.dumps({"status": "held", "mode": "read_only_stop"}), flush=True)
            while True:
                signal.pause()
    except BlockingIOError:
        reason = "state_lock_busy"
    except (OSError, ValueError):
        reason = "state_lock_unavailable"
    except KeyboardInterrupt:
        return 0
    print(json.dumps({"status": "blocked", "reason": reason}), flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
