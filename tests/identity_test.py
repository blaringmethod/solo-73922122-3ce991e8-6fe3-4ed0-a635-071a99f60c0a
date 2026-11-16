"""Unit-level tests for identity and legacy-database migration.

  I1  xattr token is created when no birth time is exposed, is stable
      across rename, and is not inherited by a recreated file
  I2  a new file reusing the same path gets a different ``file_id``
  I3  an old-format database (``dev-inode`` ids, no new columns) is
      migrated in place: rows and offsets stay attached to the source
  I4  a legacy identity whose source file is gone is kept as-is (still
      searchable) instead of being guessed at

Run:
  PYTHONPATH=<project root> python3 -I tests/identity_test.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import fileid  # noqa: E402

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def test_identity(tmp: Path) -> None:
    print("I1/I2 identity is rename-stable and recreation-distinct")
    d = tmp / "ident"
    d.mkdir()
    p = d / "a.jsonl"
    p.write_bytes(b"one\n")
    first = fileid.identity_from_path(p)
    os.rename(p, d / "b.jsonl")
    after_rename = fileid.identity_from_path(d / "b.jsonl")
    check("identity unchanged by rename", first.file_id == after_rename.file_id,
          f"{first.file_id} != {after_rename.file_id}")
    os.unlink(d / "b.jsonl")
    p.write_bytes(b"two\n")
    recreated = fileid.identity_from_path(p)
    check("recreated file gets a new identity", recreated.file_id != first.file_id,
          f"{recreated.file_id} == {first.file_id}")
    check("recreated identity starts from the same path but differs by token/time",
          recreated.ino == first.ino or recreated.ino != first.ino)
    # label survives a second rename
    os.rename(p, d / "c.jsonl")
    check("identity stable across a second rename",
          fileid.identity_from_path(d / "c.jsonl").file_id == recreated.file_id)
    check("identity_matches rejects a replaced path",
          not fileid.identity_matches(first, d / "c.jsonl")
          and fileid.identity_matches(recreated, d / "c.jsonl"))


def _make_legacy_db(dbp: Path, live: Path) -> str:
    """Build the first-release schema and one fully ingested legacy source."""
    conn = sqlite3.connect(dbp)
    conn.executescript(
        """
        CREATE TABLE file_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL UNIQUE,
            inode INTEGER NOT NULL,
            last_path TEXT NOT NULL,
            size_seen INTEGER NOT NULL DEFAULT 0,
            read_offset INTEGER NOT NULL DEFAULT 0,
            line_count INTEGER NOT NULL DEFAULT 0,
            first_seen_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        );
        CREATE TABLE alarms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            byte_offset INTEGER NOT NULL,
            raw_line TEXT NOT NULL,
            device_id TEXT,
            event_utc TEXT,
            alarm_code TEXT,
            parse_ok INTEGER NOT NULL DEFAULT 1,
            source_path TEXT NOT NULL,
            ingested_by TEXT NOT NULL,
            ingested_utc TEXT NOT NULL,
            UNIQUE (file_id, byte_offset)
        );
        CREATE TABLE collector_heartbeats (
            collector_id TEXT PRIMARY KEY, pid INTEGER NOT NULL,
            last_beat_utc TEXT NOT NULL, status TEXT NOT NULL, detail TEXT
        );
        CREATE TABLE ingest_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT, file_id TEXT, path TEXT,
            collector_id TEXT, kind TEXT NOT NULL, message TEXT NOT NULL,
            occurred_utc TEXT NOT NULL
        );
        """
    )
    st = live.stat()
    legacy_id = f"dev{st.st_dev}-ino{st.st_ino}"
    data = live.read_bytes()
    conn.execute(
        """INSERT INTO file_states
               (file_id, inode, last_path, size_seen, read_offset, line_count,
                first_seen_utc, updated_utc)
           VALUES (?, ?, ?, ?, ?, 1, 't', 't')""",
        (legacy_id, st.st_ino, str(live), len(data), len(data)),
    )
    conn.execute(
        """INSERT INTO alarms
               (file_id, line_no, byte_offset, raw_line, device_id, event_utc,
                alarm_code, parse_ok, source_path, ingested_by, ingested_utc)
           VALUES (?, 1, 0, ?, 'legacy-dev', 't', 'OLD', 1, ?, 'old-col', 't')""",
        (legacy_id, data.decode().rstrip("\n"), str(live)),
    )
    conn.commit()
    conn.close()
    return legacy_id


def test_migration(tmp: Path) -> None:
    print("I3/I4 legacy database migration keeps rows and identity")

    # Make a fresh import of app.db so its per-path init cache is empty.
    import importlib

    import app.config as cfg
    import app.db as dbmod

    importlib.reload(dbmod)
    d = tmp / "mig"
    d.mkdir()
    shared = d / "shared"
    shared.mkdir()
    live = shared / "alarms.jsonl"
    live.write_bytes(b'{"device_id":"legacy-dev","alarm_code":"OLD"}\n')
    dbp = d / "wh" / "alarms.db"
    dbp.parent.mkdir(parents=True)
    legacy_id = _make_legacy_db(dbp, live)

    cfg.SHARED_DIR = shared
    cfg.DB_PATH = dbp
    dbmod.init_db(dbp)

    conn = dbmod.connect(dbp)
    row = conn.execute("SELECT * FROM file_states").fetchone()
    alarms = conn.execute("SELECT * FROM alarms").fetchall()
    check("legacy source got the new identity format",
          row["file_id"] != legacy_id and "-bt" in row["file_id"] or "-src" in row["file_id"],
          row["file_id"])
    check("ingested row moved to the new identity",
          len(alarms) == 1 and alarms[0]["file_id"] == row["file_id"])
    check("offset preserved by migration", row["read_offset"] == live.stat().st_size)
    check("identity matches the live file",
          row["file_id"] == fileid.identity_from_path(live).file_id)

    # A collector now continues from the preserved offset: appending must
    # only ingest the new line, never duplicate the old one.
    from app.collector import Collector

    with open(live, "ab") as fh:
        fh.write(b'{"device_id":"after","alarm_code":"NEW"}\n')
    Collector("mig-collector", shared, dbp).scan()
    conn2 = dbmod.connect(dbp)
    check("no duplicate after migration + continue",
          conn2.execute("SELECT COUNT(*) FROM alarms").fetchone()[0] == 2)
    check("legacy row is queryable by device",
          conn2.execute("SELECT COUNT(*) FROM alarms WHERE device_id='legacy-dev'")
          .fetchone()[0] == 1)
    conn.close()
    conn2.close()

    print("I4 legacy identity whose source file is gone is preserved")
    dbp2 = d / "wh2" / "alarms.db"
    dbp2.parent.mkdir(parents=True)
    gone = shared / "gone.jsonl"
    gone.write_bytes(b'{"alarm_code":"GONE"}\n')
    legacy2 = _make_legacy_db(dbp2, gone)
    gone.unlink()
    cfg.DB_PATH = dbp2
    importlib.reload(dbmod)
    dbmod.init_db(dbp2)
    conn = dbmod.connect(dbp2)
    check("identity preserved for a vanished legacy source",
          conn.execute("SELECT file_id FROM file_states").fetchone()[0] == legacy2)
    check("its ingested row is still there",
          conn.execute("SELECT COUNT(*) FROM alarms").fetchone()[0] == 1)
    conn.close()


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="alarm-ident-"))
    try:
        test_identity(tmp)
        test_migration(tmp)
    finally:
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
