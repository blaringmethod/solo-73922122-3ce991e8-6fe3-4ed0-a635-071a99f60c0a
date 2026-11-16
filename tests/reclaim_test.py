"""End-to-end tests for seal-and-reclaim.

Spins up the same throwaway topology as ``selftest.py`` (one web process,
two collector processes, one SQLite DB) and exercises:

  R1  seal a rotated, fully ingested file -> source deleted, rows + source
      identity kept, slot released, rotation works again
  R2  seal must wait for every byte: a dangling half-line keeps the source
      and shows a reason; completing it lets the background delete finish
  R3  a malformed line keeps the source and reports the reason
  R4  appends to a sealed source are refused by identity (even though the
      name can be recreated by a *different* file)
  R5  backfill appended before the seal is part of the sealed snapshot and
      is ingested, never lost
  R6  selecting a file by the identity seen on the page still seals the
      original after the gateway renamed (rotated) it
  R7  a new file reusing the old name/inode does not inherit the old
      offset and is not confused with the old source
  R8  delete failure (permission) keeps the file, clears the slot only
      after a successful retry
  R9  crash between seal registration and the file rename, and crash
      between unlink and DB update -> restart recovery finishes the job
  R10 a new file at the old path is never deleted by a stale job
  R11 collectors keep reading while sealing; other files continue to be
      appended, collected and queried; late reads of the sealed file land

Run:
  PYTHONPATH=<project root> python3 -I tests/reclaim_test.py
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def http_status(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    try:
        return 200, http(method, url, body)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except Exception:
            return exc.code, {"raw": raw.decode(errors="replace")}


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            http("GET", url)
            return
        except (urllib.error.URLError, ConnectionError):
            time.sleep(0.2)
    raise RuntimeError(f"service not ready: {url}")


def wait_until(fn, timeout: float = 20.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:  # transient DB lock etc.
            last = exc
        time.sleep(interval)
    return last


def free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_collector(cid: str, shared: Path, dbp: Path, env: dict) -> subprocess.Popen:
    e = dict(env)
    e.update({"COLLECTOR_ID": cid, "SHARED_DIR": str(shared), "DB_PATH": str(dbp)})
    return subprocess.Popen(
        [sys.executable, "-m", "app.collector"],
        cwd=str(ROOT), env=e,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def start_web(shared: Path, dbp: Path, env: dict, port: int) -> subprocess.Popen:
    e = dict(env)
    e.update({"SHARED_DIR": str(shared), "DB_PATH": str(dbp)})
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(ROOT), env=e,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def db_rows(dbp: Path, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    conn = sqlite3.connect(dbp, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def one(dbp: Path, sql: str, params: tuple = ()):
    rows = db_rows(dbp, sql, params)
    return rows[0][0] if rows else None


def files_on_disk(shared: Path) -> dict[str, int]:
    return {p.name: p.stat().st_size for p in shared.glob("*.jsonl")}


def jobs(base: str) -> list[dict]:
    return http("GET", base + "/api/reclaim/jobs")["items"]


def job_for(base: str, file_id: str) -> dict | None:
    for j in jobs(base):
        if j["file_id"] == file_id:
            return j
    return None


def gateway_files(base: str) -> list[dict]:
    return http("GET", base + "/api/gateway/files")["files"]


def file_by_name(base: str, name: str) -> dict | None:
    for f in gateway_files(base):
        if f["name"] == name:
            return f
    return None


def reclaim_all_others(c: "Cluster", base: str, timeout: float = 25.0) -> None:
    """Seal + wait for deletion of every non-current file, freeing slots."""
    for f in gateway_files(base):
        if not f["current"]:
            try:
                http("POST", base + "/api/reclaim/seal", {"file_id": f["file_id"]})
            except urllib.error.HTTPError:
                pass

    def _clean():
        return not [f for f in gateway_files(base) if not f["current"]]

    wait_until(_clean, timeout)


class Cluster:
    """A throwaway web + two collectors sharing temp dirs."""

    def __init__(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="alarm-reclaim-"))
        self.shared = self.tmp / "shared"
        self.dbp = self.tmp / "wh" / "alarms.db"
        self.shared.mkdir(parents=True)
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = str(ROOT) + os.pathsep + self.env.get("PYTHONPATH", "")
        self.env.update({
            "SCAN_INTERVAL": "0.1", "MAX_GENERATIONS": "3",
            "ALLOW_TEST_SHUTDOWN": "1", "RECLAIM_POLL_SECONDS": "0.2",
            "RECLAIM_RETRY_SECONDS": "1",
        })
        self.procs: dict[str, subprocess.Popen] = {}

    def start(self) -> None:
        self.web = start_web(self.shared, self.dbp, self.env, self.port)
        self.procs["web"] = self.web
        self.ca = start_collector("collector-a", self.shared, self.dbp, self.env)
        self.cb = start_collector("collector-b", self.shared, self.dbp, self.env)
        self.procs["collector-a"] = self.ca
        self.procs["collector-b"] = self.cb
        wait_http(self.base + "/healthz")
        time.sleep(0.6)

    def stop(self, keep: bool = False) -> None:
        try:
            req = urllib.request.Request(
                self.base + "/__test/shutdown", data=b"{}", method="POST",
                headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
        for cid in ("collector-a", "collector-b"):
            (self.shared / f".stop-{cid}").touch()
        for p in self.procs.values():
            try:
                p.wait(timeout=8)
                continue
            except subprocess.TimeoutExpired:
                pass
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=5)
            except (PermissionError, subprocess.TimeoutExpired):
                pass
        if not keep:
            shutil.rmtree(self.tmp, ignore_errors=True)

    def append(self, **body) -> dict:
        return http("POST", self.base + "/api/gateway/append", body)

    def rotate(self) -> dict:
        return http("POST", self.base + "/api/gateway/rotate")

    def wait_rows(self, expected: int, timeout: float = 20.0) -> int:
        def _ready():
            n = one(self.dbp, "SELECT COUNT(*) FROM alarms")
            return n if n is not None and n >= expected else None

        res = wait_until(_ready, timeout)
        return res if isinstance(res, int) else one(self.dbp, "SELECT COUNT(*) FROM alarms")


def main() -> int:
    c = Cluster()
    c.start()
    base = c.base
    dbp = c.dbp

    try:
        # ---------------------------------------------------------------- R1
        print("R1 seal a fully ingested rotated file -> delete, keep detail, free slot")
        for i in range(4):
            c.append(device_id=f"r1-{i}", alarm_code="R1")
        c.wait_rows(4)
        r = c.rotate()
        old = file_by_name(base, "alarms.1.jsonl")
        check("rotation produced alarms.1.jsonl", old is not None and r["rotated"])
        c.append(device_id="live", alarm_code="LIVE")
        c.wait_rows(5)
        # seal exactly the identity displayed on the page
        try:
            sealed = http("POST", base + "/api/reclaim/seal", {"file_id": old["file_id"]})
            ok = True
        except urllib.error.HTTPError as exc:
            sealed, ok = {"detail": exc.read().decode()}, False
        check("seal accepted for old file", ok, str(sealed))
        if ok:
            check("seal snapshot equals old file size", sealed["snapshot_size"] == old["size"],
                  f"{sealed.get('snapshot_size')} != {old['size']}")
        done = wait_until(lambda: (job_for(base, old["file_id"]) or {}).get("state") == "done"
                          if job_for(base, old["file_id"]) else None, 20)
        j = job_for(base, old["file_id"])
        check("job reaches done after all bytes ingested", bool(done), str(j))
        check("source file actually removed", not (c.shared / "alarms.1.jsonl").exists())
        check("source identity retained in warehouse",
              one(dbp, "SELECT COUNT(*) FROM file_states WHERE file_id=?",
                  (old["file_id"],)) == 1)
        check("ingested detail retained after reclaim",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE file_id=?",
                  (old["file_id"],)) >= 4)
        slots = http("GET", base + "/api/reclaim/jobs")["slots"]
        check("slot released only after delete", old["file_id"] not in slots["slots"],
              str(slots))
        c.rotate()
        check("rotation works again after a slot was freed",
              file_by_name(base, "alarms.1.jsonl") is not None)

        # ---------------------------------------------------------------- R2
        print("R2 seal waits for every byte (dangling half-line)")
        c.append(device_id="r2", alarm_code="R2")
        c.wait_rows(6)
        http("POST", base + "/api/gateway/rotate")
        # R1 rotated the live file to .1; .2 is a leftover from a refused
        # rotate in the older selftest, but here the only non-current file
        # at the cap is .1 after the rotate above.
        target = file_by_name(base, "alarms.1.jsonl")
        c.append(device_id="later", alarm_code="LATER")  # keeps other files flowing
        # write an unterminated line into the file we are about to seal
        http("POST", base + "/api/gateway/fragment",
             {"text": '{"device_id":"half","alarm_code":"HALF"', "filename": target["name"]})
        job = http("POST", base + "/api/reclaim/seal", {"file_id": target["file_id"]})
        time.sleep(1.5)
        j = job_for(base, target["file_id"])
        check("source kept while a half-line is un-ingested",
              Path(j["tombstone_path"]).exists(), str(j))
        check("state still archiving and a reason is shown",
              j["state"] in ("archiving", "delete_failed") and bool(j["reason"]), str(j))
        check("reason mentions the missing newline",
              "半行" in (j["reason"] or "") or "换行" in (j["reason"] or ""), str(j["reason"]))
        check("other file's append still works while sealed file waits",
              c.append(device_id="flow", alarm_code="FLOW")["size"] > 0)
        c.wait_rows(8)
        # complete the sealed file's half-line (through the tombstone, as an
        # operator/repair would) -> background delete must finish
        tombs = list((c.shared / ".reclaim").glob("*.jsonl"))
        tomb = [p for p in tombs if job and p.name == Path(job["tombstone_path"]).name][0]
        with open(tomb, "ab") as fh:
            fh.write(b"}\n")
        done = wait_until(lambda: (job_for(base, target["file_id"]) or {}).get("state") == "done"
                          if job_for(base, target["file_id"]) else None, 20)
        check("after completion the source is deleted", bool(done), str(job_for(base, target["file_id"])))

        # ---------------------------------------------------------------- R3
        print("R3 malformed line keeps the source and reports the reason")
        c.append(device_id="r3", alarm_code="R3")
        c.wait_rows(9)
        c.rotate()
        # after rotate: alarms.1 = previous live, alarms.2 = previous .1(subject R2)
        bad_target = file_by_name(base, "alarms.1.jsonl")
        with open(c.shared / bad_target["name"], "ab") as fh:
            fh.write(b"{bad json " + b"x" * 120 + b"}\n")
            fh.flush()
            os.fsync(fh.fileno())
        time.sleep(0.8)
        http("POST", base + "/api/reclaim/seal", {"file_id": bad_target["file_id"]})
        time.sleep(1.5)
        j = job_for(base, bad_target["file_id"])
        check("source with a bad line is kept", Path(j["tombstone_path"]).exists(), str(j))
        check("bad-line reason shown",
              j["state"] in ("archiving", "delete_failed") and "坏行" in (j["reason"] or ""),
              str(j))
        # repair in place with a valid, same-length line -> should drain
        # (the sealed source now lives under its tombstone path)
        p = Path(j["tombstone_path"])
        raw = p.read_bytes()
        bad = b"{bad json " + b"x" * 120 + b"}\n"
        good = (b'{"alarm_code":"FIXED"}' + b" " * 80)
        replacement = good + b" " * (len(bad) - 1 - len(good)) + b"\n"
        assert len(replacement) == len(bad), (len(replacement), len(bad))
        p.write_bytes(raw.replace(bad, replacement, 1))
        done = wait_until(lambda: (job_for(base, bad_target["file_id"]) or {}).get("state") == "done"
                          if job_for(base, bad_target["file_id"]) else None, 20)
        check("repaired line lets reclaim finish", bool(done), str(job_for(base, bad_target["file_id"])))
        check("repaired line reached the warehouse",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE alarm_code='FIXED'") == 1)

        # ---------------------------------------------------------------- R4
        print("R4 all append entry points refuse a sealed identity")
        sealed_files = {j["file_id"] for j in jobs(base) if j["state"] == "done"}
        target = file_by_name(base, "alarms.jsonl")  # current live is never sealed
        check("live file is not sealed", target["file_id"] not in sealed_files)
        for fid in list(sealed_files)[:1]:
            job = next(j for j in jobs(base) if j["file_id"] == fid)
            name = Path(job["origin_path"]).name
            for path, body in (
                ("/api/gateway/append", {"filename": name, "device_id": "x"}),
                ("/api/gateway/fragment", {"filename": name, "text": "{"}),
                ("/api/gateway/complete", {"filename": name}),
            ):
                code, resp = http_status("POST", base + path, body)
                check(f"{path} refuses sealed name {name}", code in (404, 409), f"{code} {resp}")
            # Recreating a *new* file under the sealed name must not become
            # writable as the sealed source, nor be confused with it.
            with open(c.shared / name, "wb") as fh:
                fh.write(b"")
            newf = file_by_name(base, name)
            check("recreated path has a different identity", newf["file_id"] != fid,
                  f"{newf.get('file_id')} == {fid}")
            r = c.append(filename=name, device_id="new-under-old-name", alarm_code="REUSE")
            check("append to the recreated file works (it is a new identity)",
                  r["file_id"] != fid)
            c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 0)
            os.unlink(c.shared / name)  # clean up so slot count stays sane

        # ---------------------------------------------------------------- R5
        print("R5 backfill written before the seal is inside the snapshot")
        c.append(device_id="r5", alarm_code="R5")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        base_name = "alarms.1.jsonl"
        # the file at alarms.1 may already be a recreated identity from R4;
        # write two backfills then seal immediately
        for i in range(2):
            c.append(filename=base_name, device_id="bf", alarm_code=f"BF-{i}")
        bf = file_by_name(base, base_name)
        sealed = http("POST", base + "/api/reclaim/seal", {"file_id": bf["file_id"]})
        check("seal snapshot includes the backfill bytes", sealed["snapshot_size"] >= bf["size"],
              f"{sealed.get('snapshot_size')} < {bf.get('size')}")
        done = wait_until(lambda: (job_for(base, bf["file_id"]) or {}).get("state") == "done"
                          if job_for(base, bf["file_id"]) else None, 20)
        check("backfilled sealed file reclaimed", bool(done))
        got = one(dbp, "SELECT COUNT(*) FROM alarms WHERE alarm_code LIKE 'BF-%' AND file_id=?",
                  (bf["file_id"],))
        check("all backfilled bytes are in the warehouse", got == 2, f"got {got}")

        reclaim_all_others(c, base)
        # ---------------------------------------------------------------- R6
        print("R6 identity from a stale page still seals the original after rename")
        c.append(device_id="r6", alarm_code="R6")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        stale = file_by_name(base, "alarms.1.jsonl")   # identity shown on the page
        c.rotate()                                    # renamed: .1 -> .2
        renamed = file_by_name(base, "alarms.2.jsonl")
        check("rename changed name but not identity",
              renamed is not None and renamed["file_id"] == stale["file_id"])
        stale_job = http("POST", base + "/api/reclaim/seal", {"file_id": stale["file_id"]})
        check("seal by displayed identity targets the renamed original",
              stale_job["origin_path"].endswith("alarms.2.jsonl"), str(stale_job["origin_path"]))
        check("new .1 (different identity) untouched",
              file_by_name(base, "alarms.1.jsonl")["file_id"] != stale["file_id"])
        wait_until(lambda: (job_for(base, stale["file_id"]) or {}).get("state") == "done"
                   if job_for(base, stale["file_id"]) else None, 20)

        reclaim_all_others(c, base)
        # ---------------------------------------------------------------- R7
        print("R7 new file reusing name/inode does not inherit the old offset")
        c.append(device_id="r7", alarm_code="R7")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        posts = list((c.shared).glob("alarms.*.jsonl"))
        for p in posts:
            if p.name != "alarms.1.jsonl":
                p.unlink()
        # Drop every trace of the old identity except its file_states row by
        # sealing (which keeps the row) and letting the worker delete.
        http("POST", base + "/api/reclaim/seal", {"file_id": victim["file_id"]})
        wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "done"
                   if job_for(base, victim["file_id"]) else None, 20)
        old_offset = one(dbp, "SELECT read_offset FROM file_states WHERE file_id=?",
                         (victim["file_id"],))
        # create the replacement at the very same path
        fresh_line = json.dumps({"device_id": "fresh-reuse", "alarm_code": "REUSE2"}) + "\n"
        with open(c.shared / "alarms.1.jsonl", "w") as fh:
            fh.write(fresh_line)
        fresh = file_by_name(base, "alarms.1.jsonl")
        check("recreated file has a new identity", fresh["file_id"] != victim["file_id"])
        fresh_mod = wait_until(
            lambda: one(dbp, "SELECT read_offset FROM file_states WHERE file_id=?",
                        (fresh["file_id"],)), 15)
        check("new identity starts at offset 0 and ingests its bytes",
              fresh_mod == len(fresh_line.encode()), f"offset={fresh_mod}")
        check("old identity keeps its ingested detail",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE file_id=?",
                  (victim["file_id"],)) >= 1 and old_offset > 0)
        check("no rows mixed between old and new identity",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE file_id=? AND alarm_code='REUSE2'",
                  (victim["file_id"],)) == 0)
        check("new file row present exactly once",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE file_id=?",
                  (fresh["file_id"],)) == 1)

        reclaim_all_others(c, base)
        # ---------------------------------------------------------------- R11
        print("R11 other files keep flowing while a seal is in progress")
        c.append(device_id="flow-a", alarm_code="FLOWA")
        n_before = one(dbp, "SELECT COUNT(*) FROM alarms")
        http("POST", base + "/api/gateway/rotate")
        pending = file_by_name(base, "alarms.1.jsonl")
        http("POST", base + "/api/gateway/fragment",
             {"text": '{"device_id":"slow"', "filename": pending["name"]})
        http("POST", base + "/api/reclaim/seal", {"file_id": pending["file_id"]})
        for i in range(3):
            c.append(device_id="flow-b", alarm_code=f"FLOWB-{i}")
        got = c.wait_rows(n_before + 3)
        check("appends to other files ingested while a seal waits", got >= n_before + 3,
              f"{got} < {n_before + 3}")
        listed = http("GET", base + "/api/alarms?limit=5")
        check("queries still work during a pending seal", "items" in listed)
        # now complete the slow sealed line and confirm a late scan ingests it
        job = job_for(base, pending["file_id"])
        tomb = Path(job["tombstone_path"])
        with open(tomb, "ab") as fh:
            fh.write(b'}\n')
        wait_until(lambda: (job_for(base, pending["file_id"]) or {}).get("state") == "done"
                   if job_for(base, pending["file_id"]) else None, 20)
        check("late read of the sealed file was ingested, then deleted",
              one(dbp, "SELECT COUNT(*) FROM alarms WHERE file_id=? AND device_id='slow'",
                  (pending["file_id"],)) == 1
              and not tomb.exists())

        reclaim_all_others(c, base)
        # ---------------------------------------------------------------- R8
        print("R8 delete failure keeps the slot; retry releases it")
        c.append(device_id="r8", alarm_code="R8")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        http("POST", base + "/api/gateway/fragment",
             {"text": '{"device_id":"r8-slow","alarm_code":"SLOW"',
              "filename": victim["name"]})
        job = http("POST", base + "/api/reclaim/seal", {"file_id": victim["file_id"]})
        tomb = Path(job["tombstone_path"])
        rdir = tomb.parent
        wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "archiving"
                   and "半行" in (job_for(base, victim["file_id"]) or {}).get("reason", ""), 15)
        # Block unlink without blocking a write through an already-open
        # descriptor: remove write permission from the directory.
        os.chmod(rdir, 0o500)
        try:
            if os.access(rdir, os.W_OK):
                print("  [SKIP] deletion failure injection not permitted (root)")
            else:
                with open(tomb, "ab") as fh:
                    fh.write(b"}\n")
                    fh.flush()
                    os.fsync(fh.fileno())
                failed = wait_until(
                    lambda: (job_for(base, victim["file_id"]) or {}).get("state")
                    == "delete_failed", 20)
                j2 = job_for(base, victim["file_id"])
                slots = http("GET", base + "/api/reclaim/jobs")["slots"]["slots"]
                check("failed delete keeps the source and the slot",
                      bool(failed) and tomb.exists()
                      and victim["file_id"] in slots, str(j2))
                check("failed delete shows a retryable reason",
                      "删除源件失败" in (j2["reason"] or ""), str(j2.get("reason")))
                os.chmod(rdir, 0o700)
                j3 = http("POST", base + f"/api/reclaim/retry/{j2['id']}")
                check("manual retry deletes and releases the slot",
                      j3["state"] == "done"
                      and not Path(j3["tombstone_path"]).exists()
                      and victim["file_id"] not in
                      http("GET", base + "/api/reclaim/jobs")["slots"]["slots"], str(j3))
        finally:
            os.chmod(rdir, 0o700)

        # ---------------------------------------------------------------- R10
        reclaim_all_others(c, base)
        print("R10 stale job never deletes a new file at the old path")
        c.append(device_id="r10", alarm_code="R10")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        # Simulate a crash right after registration: create the job row
        # without moving the file, then replace the path with a new file.
        conn = sqlite3.connect(dbp, timeout=30)
        tomb = c.shared / ".reclaim" / "sealed-stale.jsonl"
        conn.execute(
            """INSERT INTO archive_jobs
                   (file_id, dev, inode, origin_path, tombstone_path,
                    snapshot_size, state, renamed, delete_attempts,
                    sealed_utc, updated_utc)
               VALUES (?,0,0,?,?,0,'archiving',0,0,'x','x')""",
            (victim["file_id"], str(c.shared / victim["name"]), str(tomb)),
        )
        conn.commit()
        conn.close()
        os.unlink(c.shared / victim["name"])          # original gone
        new_line = '{"device_id":"new-after-crash","alarm_code":"NEW"}\n'
        with open(c.shared / victim["name"], "w") as fh:
            fh.write(new_line)
        # restart the web process -> startup recovery evaluates the stale job
        req = urllib.request.Request(
            base + "/__test/shutdown", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        c.web.wait(timeout=10)
        c.web = start_web(c.shared, c.dbp, c.env, c.port)
        c.procs["web"] = c.web
        wait_http(base + "/healthz")
        j = wait_until(lambda: job_for(base, victim["file_id"]), 15)
        check("stale job resolves without deleting the new file",
              j["state"] in ("missing", "delete_failed") and (c.shared / victim["name"]).exists(),
              str(j))
        check("new file at the reused path keeps its own identity",
              (file_by_name(base, victim["name"]) or {}).get("file_id") != victim["file_id"])
        check("new file at reused path is still ingested",
              wait_until(lambda: one(dbp, "SELECT COUNT(*) FROM alarms WHERE device_id="
                                     "'new-after-crash'"), 15) == 1)

        # ---------------------------------------------------------------- R9
        reclaim_all_others(c, base)
        print("R9 restart recovery for interrupted seal + interrupted unlink")
        # (a) interrupted rename: a job row with renamed=0 and the original
        #     file still present at its path -> recovery must move + finish.
        c.append(device_id="r9a", alarm_code="R9A")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        conn = sqlite3.connect(dbp, timeout=30)
        tomb = c.shared / ".reclaim" / "sealed-recover.jsonl"
        conn.execute(
            """INSERT INTO archive_jobs
                   (file_id, dev, inode, origin_path, tombstone_path,
                    snapshot_size, state, renamed, delete_attempts,
                    sealed_utc, updated_utc)
               VALUES (?,0,0,?,?,0,'archiving',0,0,'x','x')""",
            (victim["file_id"], str(c.shared / victim["name"]), str(tomb)),
        )
        conn.commit()
        conn.close()
        # appends must already be refused for this identity even though the
        # rename has not happened yet
        code, _ = http_status("POST", base + "/api/gateway/append",
                              {"filename": victim["name"], "device_id": "nope"})
        check("append refused after seal registration, before rename", code in (404, 409))
        req = urllib.request.Request(
            base + "/__test/shutdown", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        c.web.wait(timeout=10)
        c.web = start_web(c.shared, c.dbp, c.env, c.port)
        c.procs["web"] = c.web
        wait_http(base + "/healthz")
        j = wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "done"
                       if job_for(base, victim["file_id"]) else None, 20)
        check("recovery completed the interrupted seal and delete",
              bool(j) and not (c.shared / victim["name"]).exists(), str(job_for(base, victim["file_id"])))

        # (b) interrupted unlink: unlink the tombstone but leave the job row
        #     active; recovery must notice and mark it done.
        c.append(device_id="r9b", alarm_code="R9B")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        http("POST", base + "/api/reclaim/seal", {"file_id": victim["file_id"]})
        j = wait_until(lambda: job_for(base, victim["file_id"]), 10)
        # freeze the job as if the process died between unlink and update
        tomb = Path(j["tombstone_path"])
        wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "done", 20)
        j = job_for(base, victim["file_id"])
        with open(tomb, "wb") as fh:
            fh.write(b"")  # tombstone removed by "crash"; recreate empty file
        conn = sqlite3.connect(dbp, timeout=30)
        conn.execute(
            "UPDATE archive_jobs SET state='deleting', renamed=1, deleted_utc=NULL "
            "WHERE id=?", (j["id"],))
        conn.commit()
        conn.close()
        os.unlink(tomb)
        req = urllib.request.Request(
            base + "/__test/shutdown", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        c.web.wait(timeout=10)
        c.web = start_web(c.shared, c.dbp, c.env, c.port)
        c.procs["web"] = c.web
        wait_http(base + "/healthz")
        j2 = wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "done"
                        and job_for(base, victim["file_id"])["deleted_utc"] is not None
                        if job_for(base, victim["file_id"]) else None, 20)
        check("recovery finishes an already-unlinked job", bool(j2), str(job_for(base, victim["file_id"])))
        check("reclaim did not touch ingested rows",
              one(dbp, "SELECT COUNT(*) FROM alarms") >= 1)

        # ---------------------------------------------------------------- R9c
        print("R9c crash during delete (renamed=1, tombstone still present)")
        c.append(device_id="r9c", alarm_code="R9C")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        victim = file_by_name(base, "alarms.1.jsonl")
        wait_until(lambda: one(dbp, "SELECT read_offset FROM file_states "
                                "WHERE file_id=?", (victim["file_id"],)) == victim["size"], 15)
        # Reproduce the on-disk state a crash between the rename and the
        # unlink leaves: real sealed file already under .reclaim/, job row
        # marked as delete-in-progress.
        rdir = c.shared / ".reclaim"
        tomb = rdir / "sealed-interrupted.jsonl"
        os.rename(c.shared / victim["name"], tomb)
        conn = sqlite3.connect(dbp, timeout=30)
        conn.execute(
            """INSERT INTO archive_jobs
                   (file_id, dev, inode, origin_path, tombstone_path,
                    snapshot_size, state, renamed, delete_attempts,
                    sealed_utc, updated_utc)
               VALUES (?,0,0,?,?,?,'deleting',1,0,'x','x')""",
            (victim["file_id"], str(c.shared / victim["name"]), str(tomb),
             victim["size"]),
        )
        conn.commit()
        conn.close()
        req = urllib.request.Request(
            base + "/__test/shutdown", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        c.web.wait(timeout=10)
        c.web = start_web(c.shared, c.dbp, c.env, c.port)
        c.procs["web"] = c.web
        wait_http(base + "/healthz")
        done = wait_until(lambda: (job_for(base, victim["file_id"]) or {}).get("state") == "done"
                          if job_for(base, victim["file_id"]) else None, 20)
        j = job_for(base, victim["file_id"])
        check("recovery resumes an interrupted delete",
              bool(done) and not tomb.exists() and j["deleted_utc"], str(j))

        reclaim_all_others(c, base)
        # ---------------------------------------------------------------- R12
        reclaim_all_others(c, base)
        print("R12 stale page write and stale seal never touch the replacement")
        c.append(device_id="r12", alarm_code="R12")
        c.wait_rows(one(dbp, "SELECT COUNT(*) FROM alarms") or 1)
        c.rotate()
        shown = file_by_name(base, "alarms.1.jsonl")
        # the page still shows `shown`; meanwhile the file is replaced by a
        # new instance at the same path, and the old one is gone
        os.unlink(c.shared / shown["name"])
        with open(c.shared / shown["name"], "wb") as fh:
            fh.write(b'{"device_id":"replacement","alarm_code":"REPL"}\n')
        replacement = file_by_name(base, shown["name"])
        check("replacement has a different identity",
              replacement["file_id"] != shown["file_id"])
        for path, body in (
            ("/api/gateway/append",
             {"filename": shown["name"], "file_id": shown["file_id"],
              "device_id": "stale-page", "alarm_code": "STALE"}),
            ("/api/gateway/fragment",
             {"filename": shown["name"], "file_id": shown["file_id"],
              "text": '{"device_id":"stale-page"'}),
        ):
            code, resp = http_status("POST", base + path, body)
            check(f"{path} refuses a stale-page identity", code == 409, f"{code} {resp}")
        data = (c.shared / shown["name"]).read_bytes()
        check("staleness check ran before writing any byte",
              data == b'{"device_id":"replacement","alarm_code":"REPL"}\n', repr(data))
        code, resp = http_status("POST", base + "/api/reclaim/seal",
                                 {"file_id": shown["file_id"]})
        check("stale seal request is rejected", code == 404, f"{code} {resp}")
        check("replacement file still there", (c.shared / shown["name"]).exists())
        check("replacement still ingested",
              wait_until(lambda: one(dbp, "SELECT COUNT(*) FROM alarms WHERE "
                                     "file_id=?", (replacement["file_id"],)), 15) == 1)
        # a fresh request pinned to the replacement identity works
        r = c.append(filename=shown["name"], file_id=replacement["file_id"],
                     device_id="fresh-page", alarm_code="FRESH")
        check("fresh identity pinned write works", r["file_id"] == replacement["file_id"])
        reclaim_all_others(c, base)

        # ---------------------------------------------------------------- page
        print("page + API sanity")
        with urllib.request.urlopen(base + "/", timeout=10) as resp:
            html = resp.read().decode()
        check("page exposes seal / retry controls",
              "reclaim/seal" in html and "retryJob" in html)
        check("files API reports slot usage",
              "slots" in http("GET", base + "/api/gateway/files"))
    finally:
        c.stop()

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
