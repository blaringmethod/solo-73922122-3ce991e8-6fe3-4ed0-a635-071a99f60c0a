"""SQLite persistence layer.

Design points
-------------
* WAL mode + a busy timeout let two collector processes and the web
  process open the same database concurrently.
* The durable position for every source file lives in ``file_states``
  (committed byte offset + a stable identity).  A collector only advances
  that offset inside the same transaction that inserts the rows, so a
  crash can never commit progress without the data (no missing rows) and
  can never keep data without the progress marker (re-reading is harmless
  because of the unique constraint -> no duplicates).
* ``alarms`` has a unique key on ``(file_id, byte_offset)`` so the same
  physical line can never be ingested twice, while identical content on
  different lines / different files is still kept as separate rows.
* ``archive_jobs`` is the durable side of seal-and-reclaim.  A row is
  written *before* append is closed, so a crash between sealing and
  deleting is resumed after restart.  Ingested alarm rows are never
  touched by reclaim; the job keeps the source identity even after the
  source file is gone.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config
from . import fileid as fileid_mod

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_states (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id       TEXT    NOT NULL UNIQUE,          -- stable identity (see fileid.py)
    inode         INTEGER NOT NULL,
    last_path     TEXT    NOT NULL,
    size_seen     INTEGER NOT NULL DEFAULT 0,
    read_offset   INTEGER NOT NULL DEFAULT 0,       -- committed safe position
    line_count    INTEGER NOT NULL DEFAULT 0,
    first_seen_utc TEXT   NOT NULL,
    updated_utc   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS alarms (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id      TEXT    NOT NULL,
    line_no      INTEGER NOT NULL,
    byte_offset  INTEGER NOT NULL,                 -- start byte of the JSON line
    raw_line     TEXT    NOT NULL,
    device_id    TEXT,
    event_utc    TEXT,
    alarm_code   TEXT,
    parse_ok     INTEGER NOT NULL DEFAULT 1,
    source_path  TEXT    NOT NULL,
    ingested_by  TEXT    NOT NULL,
    ingested_utc TEXT    NOT NULL,
    UNIQUE (file_id, byte_offset)
);

CREATE TABLE IF NOT EXISTS collector_heartbeats (
    collector_id  TEXT PRIMARY KEY,
    pid           INTEGER NOT NULL,
    last_beat_utc TEXT    NOT NULL,
    status        TEXT    NOT NULL,
    detail        TEXT
);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id      TEXT,
    path         TEXT,
    collector_id TEXT,
    kind         TEXT NOT NULL,                     -- read | parse | db
    message      TEXT NOT NULL,
    occurred_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS archive_jobs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id            TEXT    NOT NULL UNIQUE,     -- source identity at seal time
    dev                INTEGER NOT NULL,
    inode              INTEGER NOT NULL,
    btime_ns           INTEGER,
    source_token       TEXT,
    origin_path        TEXT    NOT NULL,            -- path shown when sealed
    tombstone_path     TEXT    NOT NULL,            -- where the sealed file is moved
    snapshot_size      INTEGER NOT NULL,            -- bytes covered by this seal
    snapshot_mtime_ns  INTEGER,
    renamed            INTEGER NOT NULL DEFAULT 0, -- moved under tombstone_path?
    state              TEXT    NOT NULL,            -- archiving|deleting|delete_failed|done|missing
    reason             TEXT,                        -- why deletion is still waiting
    last_error         TEXT,
    delete_attempts    INTEGER NOT NULL DEFAULT 0,
    retry_after_utc    TEXT,
    sealed_utc         TEXT    NOT NULL,
    updated_utc        TEXT    NOT NULL,
    delete_started_utc TEXT,
    deleted_utc        TEXT
);

CREATE INDEX IF NOT EXISTS idx_alarms_file ON alarms (file_id, id);
CREATE INDEX IF NOT EXISTS idx_alarms_device ON alarms (device_id, event_utc);
CREATE INDEX IF NOT EXISTS idx_archive_state ON archive_jobs (state, id);
"""

# Columns added after the first release; applied to existing databases so a
# previously populated volume keeps its ingested detail and source identity.
_MIGRATIONS: dict[str, dict[str, str]] = {
    "file_states": {
        "dev": "INTEGER",
        "btime_ns": "INTEGER",
        "source_token": "TEXT",
        "reclaimed_utc": "TEXT",
    },
}

_init_lock = threading.Lock()
_initialized: set[Path] = set()


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open a connection with the pragmas required for shared use."""
    path = Path(db_path) if db_path is not None else config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    # busy_timeout first so subsequent statements wait on the writer lock.
    conn.execute("PRAGMA busy_timeout=30000")
    # journal_mode=WAL itself is NOT covered by busy_timeout: if another
    # process holds a write lock exactly when we connect, it raises
    # OperationalError("database is locked"). Retry instead of dying --
    # once WAL is set it persists, so later attempts are effectively no-ops.
    for attempt in range(60):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 59:
                raise
            time.sleep(0.5)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _apply_migrations(conn: sqlite3.Connection) -> None:
    for table, columns in _MIGRATIONS.items():
        existing = {
            row["name"] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        for name, decl in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    _rename_legacy_identities(conn)


def _rename_legacy_identities(conn: sqlite3.Connection) -> None:
    """Upgrade ``dev<n>-ino<m>`` identities from the first release.

    The old format was just the inode, which is not reuse-safe.  The stored
    ``inode`` and the file's own identity fields let us rebuild it exactly
    as :mod:`app.fileid` would now, keeping every ingested row and offset
    attached to the same source.  Runs only for rows that have no birth
    time / token yet (rows written by this release already carry one).
    """
    rows = conn.execute(
        """SELECT * FROM file_states
            WHERE btime_ns IS NULL AND source_token IS NULL
              AND file_id NOT IN (SELECT file_id FROM archive_jobs)"""
    ).fetchall()
    if not rows:
        return
    for row in rows:
        old = row["file_id"]
        path = row["last_path"]
        new = None
        try:
            ident = fileid_mod.identity_from_path(path) if Path(path).exists() else None
        except OSError:
            ident = None
        if ident is not None and ident.ino == row["inode"]:
            new = ident.file_id
            btime_ns, token = ident.btime_ns, ident.token
        else:
            # The source file is gone and its identity fields were never
            # stored.  Keep the legacy id (rows stay searchable); it can no
            # longer collide with a live file because live files always get
            # the new format.
            continue
        if new == old:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            clash = conn.execute(
                "SELECT 1 FROM file_states WHERE file_id = ?", (new,)
            ).fetchone()
            if clash:
                conn.execute(
                    "UPDATE file_states SET btime_ns=?, source_token=? WHERE id=?",
                    (btime_ns, token, row["id"]),
                )
            else:
                conn.execute("UPDATE alarms SET file_id=? WHERE file_id=?", (new, old))
                conn.execute(
                    "UPDATE file_states SET file_id=?, btime_ns=?, source_token=? "
                    "WHERE id=?",
                    (new, btime_ns, token, row["id"]),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.rollback()
            raise


def init_db(db_path: Path | str | None = None) -> None:
    """Create tables.  Safe to call from every process; idempotent DDL.

    Note: sqlite3 ``executescript`` implicitly commits any open transaction,
    so the schema is applied directly -- every statement is
    ``CREATE ... IF NOT EXISTS`` and thus safe under concurrent first start.
    """
    path = Path(db_path) if db_path is not None else config.DB_PATH
    with _init_lock:
        if path in _initialized:
            return
        conn = connect(path)
        try:
            conn.executescript(_SCHEMA)
            _apply_migrations(conn)
        finally:
            conn.close()
        _initialized.add(path)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``BEGIN IMMEDIATE`` transaction: writer lock is taken up front so
    two collectors serialise rather than hit SQLITE_BUSY mid-flight."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def ensure_file_state(conn: sqlite3.Connection, *, file_id: str,
                      inode: int, path: str, dev: int | None = None,
                      btime_ns: int | None = None,
                      token: str | None = None) -> sqlite3.Row:
    """Register an identity, idempotently, without advancing its offset.

    Registration is deliberately a separate, tiny transaction *before* any
    bytes are read.  If the data transaction later fails (disk full, write
    denied), the identity/offset row already exists at offset 0, so
    progress is plainly visible as "nothing committed yet" instead of the
    source being missing from ``file_states``.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            """INSERT OR IGNORE INTO file_states
                   (file_id, inode, last_path, size_seen, read_offset,
                    line_count, first_seen_utc, updated_utc, dev, btime_ns,
                    source_token)
               VALUES (?, ?, ?, 0, 0, 0, ?, ?, ?, ?, ?)""",
            (file_id, inode, path, _utc(), _utc(), dev, btime_ns, token),
        )
        row = conn.execute(
            "SELECT * FROM file_states WHERE file_id = ?", (file_id,)
        ).fetchone()
        if row is not None and row["last_path"] != path:
            conn.execute(
                "UPDATE file_states SET last_path = ?, updated_utc = ? "
                "WHERE file_id = ?",
                (path, _utc(), file_id),
            )
            row = conn.execute(
                "SELECT * FROM file_states WHERE file_id = ?", (file_id,)
            ).fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise


def _utc() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
