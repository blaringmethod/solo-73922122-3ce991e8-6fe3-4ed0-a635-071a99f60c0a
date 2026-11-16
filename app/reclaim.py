"""Seal-and-reclaim: retire fully ingested old gateway files.

Lifecycle
---------
1. **Seal** (web request, under the same gateway lock every append takes):

   * locate the source by *identity*, not by name, so a rename that
     happened after the page was rendered still selects the original
     file;
   * refuse the current live file and any file already sealed;
   * snapshot ``size`` / ``mtime`` -- this is the exact byte range the
     seal covers, including every backfill append that succeeded before
     the seal;
   * commit an ``archive_jobs`` row **before** moving the file, then
     rename it into ``shared/.reclaim/`` (no-replace rename, verified
     against the inode the seal inspected).  Collectors keep scanning that
     directory, so reads that were in flight (or later scans) still finish
     ingesting the snapshot.  The rename removes the write path: append
     entry points consult ``archive_jobs`` by identity and refuse the
     sealed instance.

2. **Drain** (background worker): delete the source only once
   ``file_states.read_offset`` covers every byte currently in the sealed
   file and the file on disk is still exactly the sealed instance.  Bytes
   appended to the sealed file after the snapshot (for example an operator
   completing a dangling line inside the tombstone) are ingested as well
   before deleting, so nothing is dropped.  A dangling half-line, a bad
   line, a shrunk file or a rewritten snapshot keeps the source on disk
   and is surfaced as a reason.  A failed unlink is retried with backoff
   *and* can be retried from the page.

3. **Slots**: a sealed file keeps occupying one of the
   ``MAX_GENERATIONS`` slots until the unlink actually succeeded; only
   then does rotation have room to run again.

Crash safety
------------
Everything needed to continue is on disk: the job row holds the source
identity, the original path, the tombstone path, whether the rename phase
completed, and the snapshot.  Startup recovery completes a rename
interrupted between registration and rename, resumes a delete interrupted
half-way, and refuses to touch a path whose identity no longer matches the
job (a new file may have reused the old name or inode by then).  Ingested
alarm rows and the ``file_states`` identity are never deleted by reclaim.
"""
from __future__ import annotations

import ctypes
import errno
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, db, fileid, gateway

log = logging.getLogger("reclaim")

TERMINAL_STATES = ("done", "missing")
_REASON_WAIT = "等待入仓"

_libc = None
_renameat2_checked = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def reclaim_dir(shared_dir: Path | None = None) -> Path:
    return (shared_dir or config.SHARED_DIR) / ".reclaim"


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# ----------------------------------------------------------------- errors
class ReclaimError(Exception):
    def __init__(self, message: str, status: int = 409) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


# ----------------------------------------------------------- rename helper
def rename_noreplace(src: Path, dst: Path) -> None:
    """Rename that never silently replaces an existing destination."""
    global _libc, _renameat2_checked
    if os.path.lexists(dst):
        raise FileExistsError(errno.EEXIST, "destination exists", str(dst))
    if not _renameat2_checked:
        _renameat2_checked = True
        try:
            _libc = ctypes.CDLL(None, use_errno=True)
            _libc.renameat2.argtypes = [
                ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                ctypes.c_char_p, ctypes.c_uint,
            ]
            _libc.renameat2.restype = ctypes.c_int
        except (OSError, AttributeError):
            _libc = None
    if _libc is not None:
        rc = _libc.renameat2(
            fileid.AT_FDCWD, os.fsencode(src),
            fileid.AT_FDCWD, os.fsencode(dst), 1,  # RENAME_NOREPLACE
        )
        if rc == 0:
            return
        err = ctypes.get_errno()
        if err not in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
            raise OSError(err, os.strerror(err), str(src), None, str(dst))
    # Fallback (old kernel / unsupported fs): the caller holds the gateway
    # lock and the tombstone name is random, so re-check then rename.
    if os.path.lexists(dst):
        raise FileExistsError(errno.EEXIST, "destination exists", str(dst))
    os.rename(src, dst)


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# --------------------------------------------------------------- DB helpers
def sealed_file_ids(conn: sqlite3.Connection) -> set[str]:
    """Every identity ever sealed.  Kept forever: a sealed source must not
    become writable again through a stale name or inode reuse."""
    return {row["file_id"] for row in conn.execute("SELECT file_id FROM archive_jobs")}


def is_sealed(file_id: str, conn: sqlite3.Connection | None = None) -> bool:
    own = conn is None
    if own:
        conn = db.connect()
    try:
        return conn.execute(
            "SELECT 1 FROM archive_jobs WHERE file_id = ?", (file_id,)
        ).fetchone() is not None
    finally:
        if own:
            conn.close()


def list_jobs(conn: sqlite3.Connection | None = None) -> list[dict]:
    own = conn is None
    if own:
        conn = db.connect()
    try:
        rows = conn.execute("SELECT * FROM archive_jobs ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        if own:
            conn.close()


def _job(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM archive_jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        raise ReclaimError("回收任务不存在", 404)
    return row


# ------------------------------------------------------------- slot quota
def _top_level_files() -> list[Path]:
    """JSONL files that may still be written to (the live directory).

    Sealed files under ``.reclaim/`` are deliberately excluded: they can
    never be selected for a seal or counted twice as a slot.
    """
    try:
        return sorted(config.SHARED_DIR.glob("*.jsonl"))
    except OSError:
        return []


def _identity_or_none(path: Path) -> fileid.FileIdentity | None:
    try:
        return fileid.identity_from_path(path)
    except OSError:
        return None


def slot_usage(conn: sqlite3.Connection | None = None) -> dict:
    """Identities occupying one of the ``MAX_GENERATIONS`` slots.

    A file counts once whether it is still in the live directory or sealed
    but not yet unlinked.  Sealed sources keep their slot until the unlink
    actually succeeded.
    """
    own = conn is None
    if own:
        conn = db.connect()
    try:
        slots: set[str] = set()
        files: list[dict] = []
        for path in _top_level_files():
            ident = _identity_or_none(path)
            if ident is None:
                continue
            slots.add(ident.file_id)
            files.append({"path": str(path), "name": path.name,
                          "file_id": ident.file_id,
                          "current": path.name == config.CURRENT_FILE})
        pending: list[str] = []
        for row in conn.execute(
            "SELECT * FROM archive_jobs WHERE state NOT IN ('done','missing')"
        ):
            tomb = Path(row["tombstone_path"])
            origin = Path(row["origin_path"])
            occupied = tomb.exists()
            if not occupied:
                oid = _identity_or_none(origin)
                occupied = oid is not None and oid.file_id == row["file_id"]
            if occupied:
                slots.add(row["file_id"])
                pending.append(row["file_id"])
        return {
            "used": len(slots),
            "limit": config.MAX_GENERATIONS,
            "slots": sorted(slots),
            "files": files,
            "pending_delete": pending,
            "can_rotate": len(slots) < config.MAX_GENERATIONS,
        }
    finally:
        if own:
            conn.close()


# --------------------------------------------------------------- resolving
def _resolve_identity(conn: sqlite3.Connection,
                      file_id: str) -> tuple[Path, fileid.FileIdentity]:
    """Find the file currently holding ``file_id``.

    Deliberately identity-based: if the gateway rotated (renamed) the file
    after the page was rendered, the original inode is still found under
    its new name.  A new file that reused the old *name* has a different
    identity and is never selected.
    """
    for path in _top_level_files():
        ident = _identity_or_none(path)
        if ident is not None and ident.file_id == file_id:
            return path, ident
    if is_sealed(file_id, conn):
        raise ReclaimError("该来源已封存（或正在回收），不能重复封存", 409)
    raise ReclaimError("所选来源已不存在（可能已被回收，或改名后再也找不到）", 404)


# ------------------------------------------------------------ seal + drain
def seal(file_id: str) -> dict:
    """Seal one non-current source.  Returns the durable job row."""
    if not file_id:
        raise ReclaimError("缺少来源身份 file_id", 400)
    with gateway.gateway_lock():
        conn = db.connect()
        try:
            path, _ident = _resolve_identity(conn, file_id)
            live_path = config.SHARED_DIR / config.CURRENT_FILE
            if path == live_path:
                raise ReclaimError("当前活动文件不能封存；请先轮转，再封存旧件", 409)
            current = _identity_or_none(config.SHARED_DIR / config.CURRENT_FILE)
            if current is not None and current.file_id == file_id:
                raise ReclaimError("当前活动文件不能封存", 409)

            # Keep one descriptor open across the rename; identity and
            # snapshot come from the inode we are moving, never from a path
            # that could be swapped between stat and rename.
            try:
                fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
            except FileNotFoundError as exc:
                raise ReclaimError("来源文件在封存前消失", 404) from exc
            try:
                fd_ident = fileid.identity_from_fd(fd)
                if fd_ident.file_id != file_id:
                    raise ReclaimError("来源身份已变化，请刷新页面后重试", 409)
                st = os.fstat(fd)
                snapshot_size = st.st_size
                snapshot_mtime_ns = st.st_mtime_ns
            finally:
                os.close(fd)

            try:
                lst = os.lstat(path)
            except FileNotFoundError as exc:
                raise ReclaimError("来源文件在封存前消失", 404) from exc
            if (lst.st_dev, lst.st_ino) != (fd_ident.dev, fd_ident.ino):
                raise ReclaimError("来源文件在封存过程中被替换，请刷新后重试", 409)
            if live_path.exists():
                live_ident = _identity_or_none(live_path)
                if live_ident is not None and live_ident.file_id == file_id:
                    raise ReclaimError("当前活动文件不能封存", 409)

            rdir = reclaim_dir()
            rdir.mkdir(parents=True, exist_ok=True)
            tombstone = rdir / f"sealed-{uuid.uuid4().hex}.jsonl"
            now = utc_now()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """INSERT INTO archive_jobs
                           (file_id, dev, inode, btime_ns, source_token,
                            origin_path, tombstone_path, snapshot_size,
                            snapshot_mtime_ns, state, renamed, reason,
                            delete_attempts, sealed_utc, updated_utc)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'archiving', 0, NULL,
                               0, ?, ?)""",
                    (file_id, fd_ident.dev, fd_ident.ino, fd_ident.btime_ns,
                     fd_ident.token, str(path), str(tombstone),
                     snapshot_size, snapshot_mtime_ns, now, now),
                )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise ReclaimError("该来源已封存，不能重复封存", 409) from exc
            except Exception:
                conn.rollback()
                raise

            # Registration is committed; from here on append entry points
            # refuse this identity even if we crash before the rename.
            try:
                rename_noreplace(path, tombstone)
            except OSError as exc:
                # Undo the registration so a failed seal does not strand the
                # file in a permanently unwritable state.
                conn.execute("DELETE FROM archive_jobs WHERE file_id = ?",
                             (file_id,))
                raise ReclaimError(f"封存改名失败：{exc!r}", 500) from exc
            conn.execute("UPDATE archive_jobs SET renamed=1, updated_utc=? WHERE file_id=?",
                         (utc_now(), file_id))
            fsync_dir(rdir)
            fsync_dir(config.SHARED_DIR)
            job_id = conn.execute(
                "SELECT id FROM archive_jobs WHERE file_id=?", (file_id,)
            ).fetchone()["id"]
            return dict(_evaluate(conn, _job(conn, job_id)))
        finally:
            conn.close()


def _hold_reason(tomb: Path, offset: int, size: int,
                 conn: sqlite3.Connection, file_id: str) -> str:
    """Human-readable reason deletion is still waiting."""
    if offset > size:
        return (f"采集位点 {offset} 超过封存快照 {size} 字节，"
                "源件可能被外部截断；保留源件")
    try:
        fd = os.open(tomb, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    except OSError as exc:
        return f"无法读取源件：{exc!r}"
    try:
        buf = bytearray()
        pos = offset
        limit = min(size, offset + max(config.MAX_LINE_BYTES, 1))
        while pos < limit:
            chunk = os.pread(fd, min(65536, limit - pos), pos)
            if not chunk:
                break
            buf.extend(chunk)
            pos += len(chunk)
            if b"\n" in buf:
                break
    except OSError as exc:
        return f"读取源件失败：{exc!r}"
    finally:
        os.close(fd)
    nl = buf.find(b"\n")
    if nl == -1:
        if len(buf) >= max(config.MAX_LINE_BYTES, 1):
            return (f"offset {offset} 起的半行超过 {config.MAX_LINE_BYTES} 字节"
                    "仍无换行；源件保留")
        return (f"尾部半行未入仓（offset {offset}，缺换行）；"
                "源件保留，补齐换行后可重试回收")
    raw = bytes(buf[:nl])
    if not raw.strip():
        # Blank lines are consumed by the collector without a row; the
        # offset just has not caught up yet.
        return (f"{_REASON_WAIT}：offset {offset} 起为空行，"
                f"等待采集器推进（还差 {size - offset} 字节）")
    try:
        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            raise ValueError("JSON value is not an object")
        return (f"{_REASON_WAIT}：offset {offset} 起的完整行有效，"
                f"等待采集器推进（还差 {size - offset} 字节）")
    except Exception:
        pass
    err = conn.execute(
        """SELECT message FROM ingest_errors
            WHERE file_id = ? ORDER BY id DESC LIMIT 1""",
        (file_id,),
    ).fetchone()
    detail = f"；最近错误：{err['message'][:300]}" if err else ""
    return f"坏行未入仓（offset {offset} 起无法解析）；源件保留{detail}"


def _evaluate(conn: sqlite3.Connection, job: sqlite3.Row) -> sqlite3.Row:
    """Advance one job as far as safely possible.

    Idempotent and safe to call after a crash at any point of the
    lifecycle (startup recovery calls it on every unfinished job).
    """
    file_id = job["file_id"]
    if job["state"] in TERMINAL_STATES:
        return job
    tomb = Path(job["tombstone_path"])
    origin = Path(job["origin_path"])

    # ---- phase 1: the file must end up under the tombstone name ---------
    # ``renamed`` is set only *after* the rename (and its directory fsync),
    # so a crash between them leaves renamed=0 and recovery re-checks both
    # paths by identity before doing anything.
    if not job["renamed"]:
        if tomb.exists():
            t_ident = _identity_or_none(tomb)
            if t_ident is not None and t_ident.file_id == file_id:
                conn.execute(
                    "UPDATE archive_jobs SET renamed=1, updated_utc=? WHERE id=?",
                    (utc_now(), job["id"]))
                job = _job(conn, job["id"])
            else:
                # Something else sits at the tombstone name.  Never delete
                # it; leave the job for an operator.
                return _update(conn, job, state="delete_failed", reason=(
                    "回收暂存名已被其他文件占用，拒绝删除（保护新件）"),
                    last_error="tombstone occupied")
        else:
            # The original instance may have been moved by the gateway
            # (rotation renames are identity-preserving) after the crash.
            # Search by identity, never by name; a new file that reused the
            # old name has a different identity and is never selected.
            found = None
            for cand in _top_level_files():
                cid = _identity_or_none(cand)
                if cid is not None and cid.file_id == file_id:
                    found = cand
                    break
            if found is None:
                # The original instance is gone: a new file may already hold
                # that path/name.  Never touch it.
                return _update(conn, job, state="missing", reason=(
                    "封存登记后源件已不存在（未删除、未改写任何新文件）"))
            try:
                fd = os.open(found, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
                try:
                    if fileid.identity_from_fd(fd).file_id != file_id:
                        return _update(conn, job, state="missing", reason=(
                            "封存登记后旧名被新件复用；旧原件已不存在，未触碰新件"))
                finally:
                    os.close(fd)
                reclaim_dir().mkdir(parents=True, exist_ok=True)
                rename_noreplace(found, tomb)
                fsync_dir(tomb.parent)
                conn.execute(
                    "UPDATE archive_jobs SET renamed=1, origin_path=?, updated_utc=? "
                    "WHERE id=?",
                    (str(found), utc_now(), job["id"]))
                job = _job(conn, job["id"])
            except OSError as exc:
                return _update(conn, job, state="delete_failed", reason=(
                    f"封存改名未完成：{exc.strerror or exc!r}；可重试"),
                    last_error=repr(exc), attempts=True)

    # ---- phase 2: verify we are still looking at the sealed instance ----
    try:
        fd = os.open(tomb, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    except FileNotFoundError:
        # Unlink already happened (crash between unlink and DB update).
        return _finish_deleted(conn, job)
    except OSError as exc:
        return _update(conn, job, state="delete_failed",
                       reason=f"无法打开源件验证身份：{exc!r}",
                       last_error=repr(exc), attempts=True)
    try:
        ident = fileid.identity_from_fd(fd)
        st = os.fstat(fd)
        try:
            lst = os.lstat(tomb)
        except FileNotFoundError:
            return _finish_deleted(conn, job)
        if ident.file_id != file_id or (lst.st_dev, lst.st_ino) != (st.st_dev, st.st_ino):
            # A different inode now sits on the tombstone path.  Refuse to
            # delete anything; this protects new files from stale cleanup.
            return _update(conn, job, state="delete_failed",
                           reason="源件身份与封存记录不一致，拒绝删除（保护新件）",
                           last_error="identity mismatch")

        # A seal covers exactly the bytes that existed when it happened,
        # including every backfill append that had succeeded.  Bytes that
        # show up later (an operator completing a dangling line inside the
        # tombstone, or an external writer) are not part of the snapshot;
        # they too must be ingested before the file may be deleted, so the
        # collector is allowed to consume them here.  A shrunk file is
        # refused: data in the snapshot is missing.
        if st.st_size < job["snapshot_size"]:
            return _update(conn, job, state="delete_failed", reason=(
                f"源件变短（{st.st_size} < 封存快照 {job['snapshot_size']}）；"
                "保留源件"), last_error="shrunk")
        current_size = st.st_size
        row = conn.execute(
            "SELECT read_offset FROM file_states WHERE file_id = ?", (file_id,)
        ).fetchone()
        offset = row["read_offset"] if row else 0
        if offset < current_size:
            reason = _hold_reason(tomb, offset, current_size, conn, file_id)
            return _update(conn, job, state="archiving", reason=reason)
        drained_size = current_size
    finally:
        os.close(fd)

    # ---- phase 3: every sealed byte is in the warehouse: delete ---------
    retry_after = _parse_utc(job["retry_after_utc"])
    if retry_after is not None and retry_after > datetime.now(timezone.utc):
        return job  # honour backoff; the page shows the reason
    conn.execute(
        "UPDATE archive_jobs SET state='deleting', delete_started_utc=?, "
        "updated_utc=? WHERE id=?",
        (utc_now(), utc_now(), job["id"]),
    )
    try:
        fd = os.open(tomb, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            ident = fileid.identity_from_fd(fd)
            st = os.fstat(fd)
            lst = os.lstat(tomb)
            if (ident.file_id != file_id
                    or (lst.st_dev, lst.st_ino) != (st.st_dev, st.st_ino)
                    or st.st_size < job["snapshot_size"]):
                return _update(conn, job, state="delete_failed",
                               reason="删除前身份/大小校验失败，拒绝删除（保护新件）",
                               last_error="identity mismatch")
            if st.st_size != drained_size:
                # Bytes appeared after the drain check (an external writer
                # touching the tombstone).  Ingest them before deleting.
                return _update(conn, job, state="archiving", reason=(
                    "封存后源件又被追加，等待其入仓后再删除"))
            os.unlink(tomb)
        finally:
            os.close(fd)
        fsync_dir(tomb.parent)
    except FileNotFoundError:
        pass  # already removed; treat as success
    except OSError as exc:
        backoff = (max(1.0, config.RECLAIM_RETRY_SECONDS)
                   * max(1, job["delete_attempts"] + 1))
        until = datetime.now(timezone.utc) + timedelta(seconds=backoff)
        return _update(
            conn, job, state="delete_failed",
            reason=f"删除源件失败：{exc.strerror or exc!r}；可重试",
            last_error=repr(exc), attempts=True,
            retry_after=until.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )
    return _finish_deleted(conn, job)


def _finish_deleted(conn: sqlite3.Connection, job: sqlite3.Row) -> sqlite3.Row:
    now = utc_now()
    conn.execute(
        "UPDATE archive_jobs SET state='done', reason=NULL, last_error=NULL, "
        "renamed=1, deleted_utc=?, updated_utc=?, retry_after_utc=NULL WHERE id=?",
        (now, now, job["id"]),
    )
    # Ingested rows and the identity record are deliberately kept; only the
    # reclaim marker is added.
    conn.execute(
        "UPDATE file_states SET reclaimed_utc = ?, updated_utc = ? WHERE file_id = ?",
        (now, now, job["file_id"]),
    )
    return _job(conn, job["id"])


def _update(conn: sqlite3.Connection, job: sqlite3.Row, *,
            state: str | None = None, reason: str | None = None,
            last_error: str | None = None, attempts: bool = False,
            retry_after: str | None = None) -> sqlite3.Row:
    sets, params = [], []
    if state is not None:
        sets.append("state=?")
        params.append(state)
    if reason is not None:
        sets.append("reason=?")
        params.append(reason)
    if last_error is not None:
        sets.append("last_error=?")
        params.append(last_error)
    if attempts:
        sets.append("delete_attempts=delete_attempts+1")
    if retry_after is not None:
        sets.append("retry_after_utc=?")
        params.append(retry_after)
    sets.append("updated_utc=?")
    params.append(utc_now())
    params.append(job["id"])
    conn.execute(f"UPDATE archive_jobs SET {', '.join(sets)} WHERE id=?", params)
    return _job(conn, job["id"])


# --------------------------------------------------------------- public ops
def _unfinished(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM archive_jobs WHERE state NOT IN ('done','missing') ORDER BY id"
    ).fetchall()


def recover_pending() -> int:
    """Finish anything interrupted by a restart.  Called at web startup."""
    resumed = 0
    with gateway.gateway_lock():
        conn = db.connect()
        try:
            for job in _unfinished(conn):
                before = (job["state"], job["reason"], job["renamed"])
                after = _evaluate(conn, job)
                if (after["state"], after["reason"], after["renamed"]) != before:
                    resumed += 1
        finally:
            conn.close()
    if resumed:
        log.info("reclaim recovery advanced %d job(s)", resumed)
    return resumed


def process_due() -> int:
    """One background pass over unfinished jobs."""
    changed = 0
    with gateway.gateway_lock():
        conn = db.connect()
        try:
            for job in _unfinished(conn):
                try:
                    after = _evaluate(conn, job)
                except Exception:
                    log.exception("reclaim evaluation failed for job %s", job["id"])
                    continue
                if (after["state"], after["reason"]) != (job["state"], job["reason"]):
                    changed += 1
        finally:
            conn.close()
    return changed


def retry(job_id: int) -> dict:
    """Page-triggered manual retry: clear the backoff and evaluate now."""
    with gateway.gateway_lock():
        conn = db.connect()
        try:
            job = _job(conn, job_id)
            if job["state"] in TERMINAL_STATES:
                raise ReclaimError(f"任务已结束（{job['state']}），无需重试", 409)
            conn.execute(
                "UPDATE archive_jobs SET retry_after_utc=NULL, updated_utc=? WHERE id=?",
                (utc_now(), job_id),
            )
            return dict(_evaluate(conn, _job(conn, job_id)))
        finally:
            conn.close()
