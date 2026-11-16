"""Gateway simulator.

Writes JSONL alarm files into the shared directory and demonstrates
rename-only rotation plus seal-and-reclaim:

* the live file is always ``CURRENT_FILE``;
* rotation renames the live file and immediately creates a new empty
  file with the same name -- nothing is deleted or truncated;
* at most ``MAX_GENERATIONS`` slots are occupied (live file + old files +
  sealed files not yet unlinked).  A sealed file keeps its slot until the
  reclaim worker actually unlinks it, so rotation is refused while all
  slots are held;
* old files can still be appended to after rotation (backfill) unless they
  have been sealed; sealed sources are refused at every write entry point
  by *identity*, not by name;
* a new file that reuses an old path or inode is a new identity, so it can
  never inherit the old byte position or be confused with the old source.

A ``flock`` on a sidecar lock file serialises every mutating operation
(append, rotate, seal, reclaim delete), including across processes.
"""
from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from . import config, db, fileid, reclaim


class GatewayError(RuntimeError):
    def __init__(self, message: str, status: int = 409) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _lock_path() -> Path:
    return config.SHARED_DIR / ".gateway.lock"


@contextmanager
def gateway_lock() -> Iterator[None]:
    config.SHARED_DIR.mkdir(parents=True, exist_ok=True)
    fh = open(_lock_path(), "w")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def _old_path(generation: int) -> Path:
    # alarms.1.jsonl, alarms.2.jsonl, ...
    stem, suffix = os.path.splitext(config.CURRENT_FILE)
    return config.SHARED_DIR / f"{stem}.{generation}{suffix}"


def _safe_name(filename: str) -> str:
    """Only plain top-level JSONL names may be written through the API."""
    name = os.path.basename(filename)
    if name != filename or not name.endswith(".jsonl") or name.startswith("."):
        raise GatewayError(f"非法文件名：{filename!r}", 400)
    return name


def list_files() -> list[dict]:
    """Return every top-level JSONL file in the shared dir with stat info."""
    out: list[dict] = []
    if not config.SHARED_DIR.exists():
        return out
    for p in sorted(config.SHARED_DIR.glob("*.jsonl")):
        try:
            st = p.stat()
            ident = fileid.identity_from_path(p)
        except OSError:
            continue
        out.append(
            {
                "name": p.name,
                "path": str(p),
                "inode": st.st_ino,
                "size": st.st_size,
                "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
                "file_id": ident.file_id,
                "current": p.name == config.CURRENT_FILE,
            }
        )
    return out


def _jsonl_files() -> list[Path]:
    return sorted(config.SHARED_DIR.glob("*.jsonl"))


def _ensure_current() -> Path:
    config.SHARED_DIR.mkdir(parents=True, exist_ok=True)
    cur = config.SHARED_DIR / config.CURRENT_FILE
    if not cur.exists():
        fd = os.open(cur, os.O_CREAT | os.O_WRONLY, 0o644)
        os.close(fd)
    return cur


def make_line(device_id: str | None = None,
              alarm_code: str | None = None,
              event_utc: str | None = None) -> str:
    payload = {
        "device_id": device_id or f"dev-{os.getpid() % 100:03d}",
        "event_utc": event_utc or utc_now(),
        "alarm_code": alarm_code or "ALM-1001",
    }
    return json.dumps(payload, ensure_ascii=False)


def _open_for_append(target: Path, allow_create: bool,
                     expected_file_id: str | None = None
                     ) -> tuple[int, fileid.FileIdentity]:
    """Open for append and verify the *opened inode* is writable.

    Identity is read from the descriptor that will receive the bytes.  If
    that inode is sealed (it should not be reachable by name anymore, but a
    stale path must never bypass the seal) the write is refused before any
    byte is written.

    ``expected_file_id`` is the identity the page displayed.  A late request
    from an old page must not land on a new file that reused the same name
    or inode, so a mismatch is refused before writing.  The check runs on
    the descriptor that will receive the bytes, so there is no stat/open
    window in between.
    """
    if expected_file_id is not None and not target.exists():
        raise GatewayError(
            f"{target.name}: 页面所选的来源已不存在（可能已回收）；"
            "请刷新后重试，不会在旧名下新建", 409)
    flags = os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0)
    flags |= os.O_CREAT if allow_create else 0
    fd = os.open(target, flags, 0o644)
    try:
        ident = fileid.identity_from_fd(fd)
        if reclaim.is_sealed(ident.file_id):
            raise GatewayError(
                f"{target.name}: 该来源已封存，所有追加入口均已拒写", 409)
        if expected_file_id is not None and ident.file_id != expected_file_id:
            raise GatewayError(
                f"{target.name}: 页面所选的来源已被替换（身份已变化），"
                "拒绝写入新件；请刷新后重试", 409)
    except Exception:
        os.close(fd)
        raise
    return fd, ident


def append_alarm(filename: str | None = None,
                 device_id: str | None = None,
                 alarm_code: str | None = None,
                 event_utc: str | None = None,
                 file_id: str | None = None) -> dict:
    """Append one complete newline-terminated JSON record."""
    with gateway_lock():
        if filename:
            target = config.SHARED_DIR / _safe_name(filename)
            allow_create = False
        else:
            target = _ensure_current()
            allow_create = True
        if not allow_create and not target.exists():
            raise GatewayError(
                f"{target.name}: 文件不存在（可能已封存/回收）；不会在旧名下新建", 409)
        line = make_line(device_id, alarm_code, event_utc)
        fd, ident = _open_for_append(target, allow_create, file_id)
        try:
            data = (line + "\n").encode("utf-8")
            os.write(fd, data)
            os.fsync(fd)
            st = os.fstat(fd)
        finally:
            os.close(fd)
        return {
            "file": target.name,
            "inode": st.st_ino,
            "file_id": ident.file_id,
            "size": st.st_size,
            "line": line,
        }


def append_fragment(text: str, filename: str | None = None,
                    file_id: str | None = None) -> dict:
    """Append raw text WITHOUT a trailing newline.

    Used to demonstrate that a record lacking its terminating newline is
    held back by the collectors until the line is completed later.
    """
    return append_raw(text, filename=filename, newline=False, file_id=file_id)


def append_raw(text: str, filename: str | None = None, newline: bool = False,
               file_id: str | None = None) -> dict:
    with gateway_lock():
        if filename:
            target = config.SHARED_DIR / _safe_name(filename)
            allow_create = False
        else:
            target = _ensure_current()
            allow_create = True
        if not allow_create and not target.exists():
            raise GatewayError(
                f"{target.name}: 文件不存在（可能已封存/回收）；不会在旧名下新建", 409)
        fd, ident = _open_for_append(target, allow_create, file_id)
        try:
            data = (text + ("\n" if newline else "")).encode("utf-8")
            os.write(fd, data)
            os.fsync(fd)
            st = os.fstat(fd)
        finally:
            os.close(fd)
        return {"file": target.name, "inode": st.st_ino,
                "file_id": ident.file_id, "size": st.st_size}


def complete_line(tail: str = "\n", filename: str | None = None,
                  file_id: str | None = None) -> dict:
    """Complete a previously fragmented line (default: just the newline)."""
    return append_raw(tail, filename=filename, newline=False, file_id=file_id)


def rotate() -> dict:
    """Rename-only rotation.  Refuses once all slots are occupied.

    Sealed-but-not-yet-unlinked files still hold a slot, so rotation only
    resumes after reclaim actually deleted them.
    """
    with gateway_lock():
        config.SHARED_DIR.mkdir(parents=True, exist_ok=True)
        conn = db.connect()
        try:
            usage = reclaim.slot_usage(conn)
        finally:
            conn.close()
        if not usage["can_rotate"]:
            raise GatewayError(
                f"轮转拒绝：{usage['used']} 个名额已被占用（上限 "
                f"{usage['limit']}，含已封存待删除件）；没有文件被删除或截断",
                409,
            )
        cur = config.SHARED_DIR / config.CURRENT_FILE
        existing = _jsonl_files()
        old_inode = None
        old_size = 0
        if cur.exists():
            st = cur.stat()
            old_inode, old_size = st.st_ino, st.st_size
            # shift existing generations backwards: 2->3, 1->2, ...
            gens = sorted(
                int(p.name.split(".")[-2])
                for p in existing
                if p.name != config.CURRENT_FILE
            )
            for gen in reversed(gens):
                reclaim.rename_noreplace(_old_path(gen), _old_path(gen + 1))
            reclaim.rename_noreplace(cur, _old_path(1))
        # create a brand new empty file with the same name (new inode)
        fd = os.open(cur, os.O_CREAT | os.O_WRONLY, 0o644)
        os.close(fd)
        os.sync()
        return {
            "rotated": old_inode is not None,
            "previous_inode": old_inode,
            "previous_size": old_size,
            "files": list_files(),
        }
