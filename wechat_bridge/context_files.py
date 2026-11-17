"""Bounded private files for optional continuity data.

Callers hold a nonblocking lock on one pinned directory. The bridge owns only
recent.json/cloud.json and their fixed temporary names. The source authority
uses a separate directory and is never included in bridge maintenance.
"""
from contextlib import contextmanager
import fcntl
import json
import os
import stat

_NAMES = frozenset(('recent.json', '.recent.json.tmp', 'cloud.json',
                    '.cloud.json.tmp', 'current.json', '.current.json.tmp'))


class ContextError(ValueError):
    def __init__(self, code='storage_invalid'):
        self.code = code
        super().__init__(code)


def encode(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ContextError('storage_invalid') from None


def _name(name):
    if type(name) is not str or name not in _NAMES:
        raise ContextError('storage_invalid')


def _stamp(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _private(info):
    return (stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
            and info.st_nlink == 1 and not info.st_mode & 0o077)


def _directory(path, parent=None, private=False):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                 dir_fd=parent)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & (0o077 if private else 0o022):
            raise ContextError('storage_invalid')
    except BaseException:
        os.close(fd)
        raise
    return fd


def _root_path(store):
    path = os.fspath(store.base)
    if not os.path.isabs(path):
        raise ContextError('storage_invalid')
    path = os.path.normpath(path)
    for alias in ('/var', '/tmp'):
        if os.path.islink(alias) and (path == alias or path.startswith(alias + '/')):
            path = os.path.realpath(alias) + path[len(alias):]
    if path == '/' or path == os.path.expanduser('~'):
        raise ContextError('storage_invalid')
    parts = path.split('/')[1:]
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts:
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                          dir_fd=fd)
            os.close(fd)
            fd = new
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ContextError('storage_invalid')
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextmanager
def locked(store, create=False, source=False):
    """Yield a pinned directory fd or None when absent, never wait for a lock."""
    root = state = directory = None
    folder = 'continuity-source' if source else 'continuity'
    try:
        root = _root_path(store)
        state = _directory('state', root, private=True)
        if create:
            try:
                os.mkdir(folder, 0o700, dir_fd=state)
            except FileExistsError:
                pass
        try:
            directory = _directory(folder, state, private=True)
        except FileNotFoundError:
            yield None
            return
        try:
            fcntl.flock(directory, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ContextError('storage_busy') from None
        stamp = os.fstat(directory)
        live = os.stat(folder, dir_fd=state, follow_symlinks=False)
        if (stamp.st_dev, stamp.st_ino) != (live.st_dev, live.st_ino):
            raise ContextError('storage_changed')
        yield directory
        latest_root = _root_path(store)
        try:
            previous_root = os.fstat(root)
            current_root = os.fstat(latest_root)
            live_state = os.stat('state', dir_fd=root, follow_symlinks=False)
            old_state = os.fstat(state)
            if ((previous_root.st_dev, previous_root.st_ino) != (current_root.st_dev, current_root.st_ino)
                    or (live_state.st_dev, live_state.st_ino) != (old_state.st_dev, old_state.st_ino)):
                raise ContextError('storage_changed')
        finally:
            os.close(latest_root)
        live = os.stat(folder, dir_fd=state, follow_symlinks=False)
        if (stamp.st_dev, stamp.st_ino) != (live.st_dev, live.st_ino):
            raise ContextError('storage_changed')
    except ContextError:
        raise
    except OSError:
        raise ContextError('storage_invalid') from None
    finally:
        for fd in (directory, state, root):
            if fd is not None:
                os.close(fd)


@contextmanager
def _file(directory, name, verify_after=True):
    _name(name)
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                 dir_fd=directory)
    try:
        info = os.fstat(fd)
        if not _private(info):
            raise ContextError('storage_invalid')
        yield fd, info
        if verify_after:
            try:
                after = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                raise ContextError('storage_changed') from None
            if _stamp(info) != _stamp(after) or _stamp(info) != _stamp(os.fstat(fd)):
                raise ContextError('storage_changed')
    finally:
        os.close(fd)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContextError('storage_invalid')
        result[key] = value
    return result


def _constant(value):
    raise ContextError('storage_invalid')


def read(directory, name, limit):
    """Check size before reading a regular private file. Missing means None."""
    _name(name)
    if directory is None:
        return None
    try:
        with _file(directory, name) as (fd, info):
            if info.st_size > limit:
                raise ContextError('storage_too_large')
            chunks = []
            remaining = info.st_size
            while remaining:
                chunk = os.read(fd, min(remaining, 8192))
                if not chunk:
                    raise ContextError('storage_changed')
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(fd, 1):
                raise ContextError('storage_changed')
            raw = b''.join(chunks)
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                           parse_constant=_constant)
        if type(value) is not dict:
            raise ContextError('storage_invalid')
        return value
    except FileNotFoundError:
        return None
    except ContextError:
        raise
    except (OSError, ValueError, UnicodeError, RecursionError):
        raise ContextError('storage_invalid') from None


def remove(directory, name, limit=None):
    """Remove only a known private regular inode, without reading its content."""
    _name(name)
    if directory is None:
        return False
    try:
        with _file(directory, name, verify_after=False) as (fd, info):
            # Keep the open inode checked until immediately before unlink.
            live = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if _stamp(info) != _stamp(live):
                raise ContextError('storage_changed')
            os.unlink(name, dir_fd=directory)
            os.fsync(directory)
            return True
    except FileNotFoundError:
        return False
    except ContextError:
        raise
    except OSError:
        raise ContextError('storage_invalid') from None


def check(directory, name):
    """Validate a fixed existing file without parsing it or reading its body."""
    _name(name)
    if directory is None:
        return None
    try:
        with _file(directory, name) as (fd, info):
            return info.st_size
    except FileNotFoundError:
        return None
    except ContextError:
        raise
    except OSError:
        raise ContextError('storage_invalid') from None


def write(directory, name, value, limit):
    _name(name)
    if name.startswith('.') or directory is None:
        raise ContextError('storage_invalid')
    data = encode(value)
    if len(data) > limit:
        raise ContextError('storage_too_large')
    temp = '.' + name + '.tmp'
    _name(temp)
    check(directory, name)
    remove(directory, temp)
    fd = None
    try:
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        with os.fdopen(fd, 'wb', closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(fd)
        live = os.stat(temp, dir_fd=directory, follow_symlinks=False)
        if _stamp(live) != _stamp(os.fstat(fd)) or not _private(live):
            raise ContextError('storage_changed')
        check(directory, name)
        os.replace(temp, name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    except ContextError:
        raise
    except OSError:
        raise ContextError('storage_invalid') from None
    finally:
        if fd is not None:
            os.close(fd)
        # Retain any partial temporary file for the explicit bounded maintenance
        # path. Its existence cannot become a successful snapshot publication.
