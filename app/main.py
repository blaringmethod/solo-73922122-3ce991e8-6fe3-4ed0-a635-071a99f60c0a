"""FastAPI web service: gateway control, ingestion inspection and
seal-and-reclaim control."""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import config, db, gateway, reclaim


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (gateway.GatewayError, reclaim.ReclaimError)):
        return HTTPException(status_code=exc.status, detail=exc.message)
    return HTTPException(status_code=500, detail=repr(exc))


async def _reclaim_worker() -> None:
    """Background drain: only deletes once every sealed byte is stored.

    Runs in a thread because the DB/gateway work is synchronous.  A crash
    or restart is covered by ``recover_pending`` in the lifespan below.
    """
    while True:
        try:
            await asyncio.to_thread(reclaim.process_due)
        except asyncio.CancelledError:
            raise
        except Exception:  # keep the worker alive; jobs stay durable
            import logging

            logging.getLogger("reclaim").exception("reclaim worker pass failed")
        await asyncio.sleep(max(0.2, config.RECLAIM_POLL_SECONDS))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    config.SHARED_DIR.mkdir(parents=True, exist_ok=True)
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db.init_db()
    # Make sure the live file exists so the page is usable immediately.
    gateway._ensure_current()
    # Resume a seal/delete that was interrupted by the last shutdown.
    try:
        await asyncio.to_thread(reclaim.recover_pending)
    except Exception:
        import logging

        logging.getLogger("reclaim").exception("startup recovery failed")
    task = asyncio.create_task(_reclaim_worker())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(title="Alarm Gateway & Collector Demo", version="2.0.0",
              lifespan=lifespan)


# --------------------------------------------------------------------- models
class AppendIn(BaseModel):
    filename: str | None = Field(None, description="target file; live file if omitted")
    file_id: str | None = Field(None, description="identity the page displayed")
    device_id: str | None = None
    alarm_code: str | None = None
    event_utc: str | None = None


class FragmentIn(BaseModel):
    text: str = Field(..., description="raw bytes written WITHOUT trailing newline")
    filename: str | None = None
    file_id: str | None = Field(None, description="identity the page displayed")


class CompleteIn(BaseModel):
    tail: str = Field("\n", description="text appended to finish the line")
    filename: str | None = None
    file_id: str | None = Field(None, description="identity the page displayed")


class SealIn(BaseModel):
    file_id: str = Field(..., description="identity shown for the file on the page")


# --------------------------------------------------------------------- gateway
@app.post("/api/gateway/append", tags=["gateway"])
def append_record(body: AppendIn) -> dict:
    """Append one complete newline-terminated JSON alarm record."""
    try:
        return gateway.append_alarm(
            filename=body.filename,
            device_id=body.device_id,
            alarm_code=body.alarm_code,
            event_utc=body.event_utc,
            file_id=body.file_id,
        )
    except (gateway.GatewayError, OSError) as exc:
        raise _http_error(exc) from exc


@app.post("/api/gateway/fragment", tags=["gateway"])
def append_fragment(body: FragmentIn) -> dict:
    """Write a record without its trailing newline (held back by collectors)."""
    try:
        return gateway.append_fragment(body.text, filename=body.filename,
                                   file_id=body.file_id)
    except (gateway.GatewayError, OSError) as exc:
        raise _http_error(exc) from exc


@app.post("/api/gateway/complete", tags=["gateway"])
def complete_line(body: CompleteIn) -> dict:
    """Finish a previously fragmented line."""
    try:
        return gateway.complete_line(tail=body.tail, filename=body.filename,
                                 file_id=body.file_id)
    except (gateway.GatewayError, OSError) as exc:
        raise _http_error(exc) from exc


@app.post("/api/gateway/rotate", tags=["gateway"])
def rotate() -> dict:
    """Rename-only rotation; refused while all slots are occupied."""
    try:
        return gateway.rotate()
    except (gateway.GatewayError, OSError) as exc:
        raise _http_error(exc) from exc


@app.get("/api/gateway/files", tags=["gateway"])
def files() -> dict:
    conn = db.connect()
    try:
        usage = reclaim.slot_usage(conn)
    finally:
        conn.close()
    return {
        "shared_dir": str(config.SHARED_DIR),
        "current_file": config.CURRENT_FILE,
        "files": gateway.list_files(),
        "slots": usage,
    }


# -------------------------------------------------------------------- reclaim
@app.post("/api/reclaim/seal", tags=["reclaim"])
def seal(body: SealIn) -> dict:
    """Seal one non-current source *by identity*.

    The page sends the identity it displayed.  If the gateway renamed the
    file after that render, the same physical file is still sealed (rename
    does not change identity); a new file that reuses the old name or inode
    is never selected.
    """
    try:
        return reclaim.seal(body.file_id)
    except (reclaim.ReclaimError, gateway.GatewayError, OSError) as exc:
        raise _http_error(exc) from exc


@app.post("/api/reclaim/retry/{job_id}", tags=["reclaim"])
def retry(job_id: int) -> dict:
    """Manual retry for a failed/sealed-but-waiting reclaim job."""
    try:
        return reclaim.retry(job_id)
    except (reclaim.ReclaimError, OSError) as exc:
        raise _http_error(exc) from exc


@app.get("/api/reclaim/jobs", tags=["reclaim"])
def jobs() -> dict:
    conn = db.connect()
    try:
        return {"items": reclaim.list_jobs(conn), "slots": reclaim.slot_usage(conn)}
    finally:
        conn.close()


# ----------------------------------------------------------------- inspection
@app.get("/api/alarms", tags=["warehouse"])
def list_alarms(limit: int = Query(200, ge=1, le=5000),
                offset: int = Query(0, ge=0),
                file_id: str | None = None,
                device_id: str | None = None) -> dict:
    conn = db.connect()
    try:
        where, params = "", []
        if file_id:
            where, params = "WHERE file_id = ?", [file_id]
        elif device_id:
            where, params = "WHERE device_id = ?", [device_id]
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM alarms {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT id, file_id, line_no, byte_offset, raw_line, device_id,
                       event_utc, alarm_code, parse_ok, source_path,
                       ingested_by, ingested_utc
                  FROM alarms {where}
                 ORDER BY id DESC LIMIT ? OFFSET ?""",
            [*params, limit, offset],
        ).fetchall()
        return {"total": total, "items": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/file-states", tags=["warehouse"])
def file_states() -> dict:
    conn = db.connect()
    try:
        rows = conn.execute(
            """SELECT fs.*,
                      aj.id AS archive_job_id, aj.state AS archive_state,
                      aj.reason AS archive_reason,
                      aj.snapshot_size AS archive_snapshot_size
                 FROM file_states fs
                 LEFT JOIN archive_jobs aj ON aj.file_id = fs.file_id
                ORDER BY fs.id"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/collectors", tags=["warehouse"])
def collectors() -> dict:
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT * FROM collector_heartbeats ORDER BY collector_id"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/errors", tags=["warehouse"])
def errors(limit: int = Query(100, ge=1, le=1000)) -> dict:
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT * FROM ingest_errors ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/stats", tags=["warehouse"])
def stats() -> dict:
    conn = db.connect()
    try:
        alarm_total = conn.execute("SELECT COUNT(*) c FROM alarms").fetchone()["c"]
        file_total = conn.execute("SELECT COUNT(*) c FROM file_states").fetchone()["c"]
        offset_sum = conn.execute(
            "SELECT COALESCE(SUM(read_offset),0) s FROM file_states"
        ).fetchone()["s"]
        reclaimed = conn.execute(
            "SELECT COUNT(*) c FROM archive_jobs WHERE state='done'"
        ).fetchone()["c"]
        pending = conn.execute(
            "SELECT COUNT(*) c FROM archive_jobs WHERE state NOT IN ('done','missing')"
        ).fetchone()["c"]
        disk_size = 0
        if config.SHARED_DIR.exists():
            disk_size = sum(
                p.stat().st_size for p in config.SHARED_DIR.glob("*.jsonl")
            )
        reclaimed_offset = conn.execute(
            """SELECT COALESCE(SUM(fs.read_offset),0) s
                 FROM file_states fs
                 JOIN archive_jobs aj ON aj.file_id = fs.file_id
                WHERE aj.state = 'done'"""
        ).fetchone()["s"]
        return {
            "alarm_rows": alarm_total,
            "tracked_files": file_total,
            "committed_bytes": offset_sum,
            "disk_bytes": disk_size,
            # Bytes of already-reclaimed sources no longer count against the
            # on-disk total; compare only still-live files.
            "live_committed_bytes": offset_sum - reclaimed_offset,
            "reclaimed_files": reclaimed,
            "pending_reclaim": pending,
        }
    finally:
        conn.close()


# ------------------------------------------------------------------------ page
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(str(Path(__file__).parent / "static" / "index.html"))


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "pid": os.getpid()}


@app.post("/__test/shutdown", include_in_schema=False)
def shutdown() -> dict:
    """Test-only graceful shutdown, enabled by ALLOW_TEST_SHUTDOWN=1."""
    if os.getenv("ALLOW_TEST_SHUTDOWN") != "1":
        raise HTTPException(status_code=404, detail="not found")
    import threading

    def _stop() -> None:
        import time as _time

        _time.sleep(0.1)
        os._exit(0)

    threading.Thread(target=_stop, daemon=True).start()
    return {"status": "shutting down"}
