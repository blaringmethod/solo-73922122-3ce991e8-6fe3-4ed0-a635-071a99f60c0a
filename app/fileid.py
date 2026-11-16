"""Stable source-file identity.

Why not just ``(st_dev, st_ino)``?
----------------------------------
An inode number is reused as soon as the last link to a deleted file is
gone.  A gateway that deletes a reclaimed file and immediately creates a
new one can therefore hand the *same* inode back to the new file.  Keying
ingestion state by inode alone would then let the new file inherit the old
file's committed byte offset (skipping its first bytes) or merge two
unrelated sources under one ``file_id``.

This module builds an identity that survives rename but not recreation:

* the inode birth time (``statx`` ``STATX_BTIME``), which is stable across
  rename and differs for a recreated inode;
* otherwise a ``user.alarm_source_id`` xattr token created once per inode
  (for filesystems whose statx does not expose a birth time);
* as a last resort, the bare inode number.  On a filesystem with neither
  birth time nor user xattrs a recreated inode cannot be told apart; that
  degradation is documented in the README.

All callers use the same helper, so the gateway, both collectors and the
reclaim worker agree on an identity.  Identity is always read from an
open file descriptor where a write/delete decision depends on it, which
closes the stat/open race (the descriptor cannot be swapped under us).
"""
from __future__ import annotations

import ctypes
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

XATTR_NAME = b"user.alarm_source_id"

# statx(2) constants (Linux UAPI, architecture independent values).
AT_FDCWD = -100
AT_SYMLINK_NOFOLLOW = 0x100
AT_EMPTY_PATH = 0x1000
STATX_BTIME = 0x800

_SYS_STATX_BY_ARCH = {
    "x86_64": 332,
    "aarch64": 291,
    "arm64": 291,
    "i386": 383,
    "i686": 383,
    "riscv64": 291,
    "ppc64le": 383,
}


class _StatxTimestamp(ctypes.Structure):
    _fields_ = [
        ("tv_sec", ctypes.c_int64),
        ("tv_nsec", ctypes.c_uint32),
        ("__reserved", ctypes.c_int32),
    ]


class _Statx(ctypes.Structure):
    _fields_ = [
        ("stx_mask", ctypes.c_uint32),
        ("stx_blksize", ctypes.c_uint32),
        ("stx_attributes", ctypes.c_uint64),
        ("stx_nlink", ctypes.c_uint32),
        ("stx_uid", ctypes.c_uint32),
        ("stx_gid", ctypes.c_uint32),
        ("stx_mode", ctypes.c_uint16),
        ("__spare0", ctypes.c_uint16),
        ("stx_ino", ctypes.c_uint64),
        ("stx_size", ctypes.c_uint64),
        ("stx_blocks", ctypes.c_uint64),
        ("stx_attributes_mask", ctypes.c_uint64),
        ("stx_atime", _StatxTimestamp),
        ("stx_btime", _StatxTimestamp),
        ("stx_ctime", _StatxTimestamp),
        ("stx_mtime", _StatxTimestamp),
        ("stx_rdev_major", ctypes.c_uint32),
        ("stx_rdev_minor", ctypes.c_uint32),
        ("stx_dev_major", ctypes.c_uint32),
        ("stx_dev_minor", ctypes.c_uint32),
        ("stx_mnt_id", ctypes.c_uint64),
        ("stx_dio_mem_align", ctypes.c_uint32),
        ("stx_dio_offset_align", ctypes.c_uint32),
        ("__spare3", ctypes.c_uint64 * 12),
    ]


_libc = None
_statx_fn = None
_statx_ready = False


def _load_statx():
    global _libc, _statx_fn, _statx_ready
    if _statx_ready:
        return _libc, _statx_fn
    _statx_ready = True
    try:
        _libc = ctypes.CDLL(None, use_errno=True)
        fn = getattr(_libc, "statx", None)
        if fn is not None:
            fn.argtypes = [
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_uint,
                ctypes.POINTER(_Statx),
            ]
            fn.restype = ctypes.c_int
            _statx_fn = fn
        else:
            nr = _SYS_STATX_BY_ARCH.get(os.uname().machine)
            if nr is None:
                _statx_fn = None
            else:
                syscall = _libc.syscall
                syscall.restype = ctypes.c_long

                def _via_syscall(dirfd, path, flags, mask, buf):
                    return syscall(
                        ctypes.c_long(nr), ctypes.c_int(dirfd),
                        ctypes.c_char_p(path), ctypes.c_int(flags),
                        ctypes.c_uint(mask), ctypes.byref(buf),
                    )

                _statx_fn = _via_syscall
    except Exception:
        _libc, _statx_fn = None, None
    return _libc, _statx_fn


def _btime_for_fd(fd: int) -> int | None:
    _load_statx()
    if _statx_fn is None:
        return None
    buf = _Statx()
    try:
        rc = _statx_fn(fd, b"", AT_EMPTY_PATH, STATX_BTIME, ctypes.byref(buf))
    except Exception:
        return None
    if rc != 0 or not (buf.stx_mask & STATX_BTIME):
        return None
    return buf.stx_btime.tv_sec * 1_000_000_000 + buf.stx_btime.tv_nsec


def _read_token(fd: int) -> str | None:
    try:
        raw = os.getxattr(fd, XATTR_NAME)
    except (OSError, AttributeError):
        return None
    try:
        value = raw.decode("ascii").strip()
    except UnicodeDecodeError:
        return None
    return value or None


def _make_token(fd: int) -> str | None:
    """Create the xattr fallback token, only when no birth time exists."""
    token = uuid.uuid4().hex
    try:
        os.setxattr(fd, XATTR_NAME, token.encode("ascii"), flags=os.XATTR_CREATE)
        return token
    except FileExistsError:
        # Two collectors raced to label the same inode; whichever won is
        # authoritative, so read it back instead of falling through to the
        # non-reuse-safe bare-inode identity.
        return _read_token(fd)
    except (OSError, AttributeError):
        return None


@dataclass(frozen=True)
class FileIdentity:
    dev: int
    ino: int
    btime_ns: int | None
    token: str | None
    ctime_ns: int
    file_id: str

    @property
    def source_label(self) -> str:
        return f"dev{self.dev}-ino{self.ino}"


def identity_from_fd(fd: int) -> FileIdentity:
    """Compute identity from a descriptor; safe against path replacement.

    Reads the xattr/birth time from the *same* inode the caller will read,
    append to or unlink, never from a path that may have been swapped.
    """
    st = os.fstat(fd)
    token = _read_token(fd)
    btime = _btime_for_fd(fd)
    if btime is None:
        if token is None:
            token = _make_token(fd)
            if token is not None:
                # setxattr changes ctime; refresh so the descriptor state
                # matches what a later process would read.
                st = os.fstat(fd)
        if token is not None:
            file_id = f"dev{st.st_dev}-ino{st.st_ino}-src{token}"
        else:
            # Last resort: the filesystem exposes neither birth time nor
            # user xattrs.  Rename-following still works, but reuse of an
            # inode number cannot be distinguished (documented in README).
            file_id = f"dev{st.st_dev}-ino{st.st_ino}"
    else:
        file_id = f"dev{st.st_dev}-ino{st.st_ino}-bt{btime}"
    return FileIdentity(
        dev=st.st_dev,
        ino=st.st_ino,
        btime_ns=btime,
        token=token,
        ctime_ns=st.st_ctime_ns,
        file_id=file_id,
    )


def identity_from_path(path: Path | str) -> FileIdentity:
    """Compute identity for a path via an open descriptor."""
    fd = os.open(os.fspath(path), os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        return identity_from_fd(fd)
    finally:
        os.close(fd)


def identity_matches(ident: FileIdentity, path: Path | str) -> bool:
    """Whether ``path`` currently names the same inode/instance as ident."""
    try:
        other = identity_from_path(path)
    except OSError:
        return False
    return other.file_id == ident.file_id
