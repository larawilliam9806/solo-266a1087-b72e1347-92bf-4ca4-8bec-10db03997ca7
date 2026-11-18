"""Core data-lake logic: uploads, consistent reads, background compaction.

Consistency model (single process, SQLite as the source of truth):

* Upload: parquet is written to a tmp file, fsynced, atomically renamed,
  then registered ``active`` in SQLite.  A read only ever sees fully
  registered files.
* Read: the active file list is snapshotted and refcounted under one lock,
  so a read always corresponds to the complete pre- or post-switch state --
  never a mix.  Retired files stay on disk until their refcount hits zero.
* Compaction: merges the files registered at job start into one parquet,
  then switches the registry in a single transaction.  Rows appended during
  the merge are not part of the input snapshot, so they survive the switch
  untouched and remain readable throughout.
* Recovery: the switch and the job-status update commit atomically, so a
  job still ``running`` at startup is guaranteed not to have switched; it
  is simply re-executed.  Orphaned bytes (failed uploads, superseded
  compaction outputs, tmp fragments) are swept before serving traffic.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .csvio import CsvError, empty_table, parse_csv
from .db import connect, init_schema
from .storage import write_parquet_atomic

log = logging.getLogger("buoy-lake")


class ConflictError(RuntimeError):
    """A compaction is already running for this day."""


class NotFoundError(KeyError):
    """Requested object does not exist."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Lake:
    def __init__(self, data_dir: str | os.PathLike, compaction_delay_sec: float = 0.0):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "lake_meta.db"
        # Test/demo hook: pause after the input snapshot so callers can
        # observe uploads and reads happening *while* a compaction runs.
        self.compaction_delay_sec = compaction_delay_sec
        # Guards the inflight refcounts and serialises
        # (snapshot + acquire) against (retired-file reclamation).
        self._state_lock = threading.Lock()
        self._inflight: dict[str, int] = {}
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_schema(str(self.db_path))

    # ------------------------------------------------------------------
    # paths
    # ------------------------------------------------------------------
    def _day_dir(self, day: str) -> Path:
        return self.data_dir / f"dt={day}"

    # ------------------------------------------------------------------
    # uploads
    # ------------------------------------------------------------------
    def upload(self, day: str, content: bytes) -> dict:
        """Parse CSV, write one parquet file, register it. Raises CsvError."""
        table = parse_csv(content)
        n = table.num_rows
        day_dir = self._day_dir(day)
        day_dir.mkdir(parents=True, exist_ok=True)
        final = day_dir / f"part-{uuid4().hex}.parquet"
        write_parquet_atomic(table, final)
        try:
            with connect(str(self.db_path)) as conn:
                conn.execute(
                    "INSERT INTO files(day, path, kind, state, rows, created_at)"
                    " VALUES (?, ?, 'upload', 'active', ?, ?)",
                    (day, str(final), n, _now_iso()),
                )
        except Exception:
            final.unlink(missing_ok=True)  # never registered -> remove bytes
            raise
        log.info("uploaded %s rows to %s", n, final.name)
        return {
            "day": day,
            "file": final.name,
            "rows": n,
            "file_count": self.file_count(day),
        }

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def _snapshot_active(self, day: str) -> list[str]:
        """Active files for ``day`` + inflight ref, atomic w.r.t. the reclaimer."""
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    "SELECT path FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                    (day,),
                ).fetchall()
            paths = [r["path"] for r in rows]
            for p in paths:
                self._inflight[p] = self._inflight.get(p, 0) + 1
            return paths

    def _release(self, paths: list[str]) -> None:
        with self._state_lock:
            for p in paths:
                remaining = self._inflight.get(p, 0) - 1
                if remaining <= 0:
                    self._inflight.pop(p, None)
                else:
                    self._inflight[p] = remaining
        self.collect_garbage()

    def read_day(self, day: str) -> dict:
        """All records for the day from one consistent (pre/post-switch) snapshot."""
        paths = self._snapshot_active(day)
        try:
            tables = [pq.read_table(p) for p in paths]
            merged = pa.concat_tables(tables, promote_options="none") if tables else empty_table()
            cols = merged.to_pydict()
            records = [
                {"buoy_id": b, "ts_utc": _iso_z(t), "salinity": s}
                for b, t, s in zip(cols["buoy_id"], cols["ts_utc"], cols["salinity"])
            ]
            return {
                "day": day,
                "file_count": len(paths),
                "row_count": merged.num_rows,
                "records": records,
            }
        finally:
            self._release(paths)

    def file_count(self, day: str) -> int:
        with connect(str(self.db_path)) as conn:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM files WHERE day = ? AND state = 'active'", (day,)
            ).fetchone()
        return n

    def list_files(self, day: str) -> dict:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, kind, rows, created_at FROM files"
                " WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
        return {
            "day": day,
            "file_count": len(rows),
            "files": [
                {
                    "file": Path(r["path"]).name,
                    "kind": r["kind"],
                    "rows": r["rows"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
        }

    # ------------------------------------------------------------------
    # compaction
    # ------------------------------------------------------------------
    def start_compaction(self, day: str) -> dict:
        """Register and launch a background compaction. Raises ConflictError."""
        input_rows = 0
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, rows FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
            input_paths = [r["path"] for r in rows]
            input_rows = sum(r["rows"] for r in rows)
            try:
                cur = conn.execute(
                    "INSERT INTO compactions(day, status, input_paths, input_rows, created_at)"
                    " VALUES (?, 'running', ?, ?, ?)",
                    (day, json.dumps(input_paths), input_rows, _now_iso()),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(f"a compaction is already running for day {day}") from exc
            job_id = cur.lastrowid
        self._launch_worker(job_id, day, input_paths)
        log.info("compaction job %s started for %s over %d files", job_id, day, len(input_paths))
        return self.get_compaction(job_id)

    def _launch_worker(self, job_id: int, day: str, input_paths: list[str]) -> None:
        t = threading.Thread(
            target=self._run_compaction,
            args=(job_id, day, input_paths),
            name=f"compact-{day}-{job_id}",
            daemon=True,
        )
        t.start()

    def _run_compaction(self, job_id: int, day: str, input_paths: list[str]) -> None:
        output_path: Path | None = None
        try:
            if self.compaction_delay_sec > 0:
                time.sleep(self.compaction_delay_sec)

            if input_paths:
                tables = [pq.read_table(p) for p in input_paths]
                merged = pa.concat_tables(tables, promote_options="none")
            else:
                merged = empty_table()

            if input_paths:
                day_dir = self._day_dir(day)
                day_dir.mkdir(parents=True, exist_ok=True)
                output_path = day_dir / f"compacted-{job_id}-{uuid4().hex[:8]}.parquet"
                # Fully written + fsynced *before* the registry switch.
                write_parquet_atomic(merged, output_path)

            # Atomic switch: retire inputs, register output, mark job done.
            conn = connect(str(self.db_path))
            try:
                conn.execute("BEGIN IMMEDIATE")
                if input_paths:
                    placeholders = ",".join("?" for _ in input_paths)
                    conn.execute(
                        f"UPDATE files SET state = 'retired'"
                        f" WHERE day = ? AND state = 'active' AND path IN ({placeholders})",
                        [day, *input_paths],
                    )
                    conn.execute(
                        "INSERT INTO files(day, path, kind, state, rows, created_at)"
                        " VALUES (?, ?, 'compacted', 'active', ?, ?)",
                        (day, str(output_path), merged.num_rows, _now_iso()),
                    )
                conn.execute(
                    "UPDATE compactions SET status = 'done', output_path = ?,"
                    " output_rows = ?, finished_at = ? WHERE id = ?",
                    (str(output_path) if output_path else None, merged.num_rows, _now_iso(), job_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            log.info("compaction job %s done: %d files -> %d rows", job_id, len(input_paths), merged.num_rows)
        except Exception as exc:  # originals stay registered; nothing is lost
            log.exception("compaction job %s failed", job_id)
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            try:
                with connect(str(self.db_path)) as conn:
                    conn.execute(
                        "UPDATE compactions SET status = 'failed', error = ?, finished_at = ?"
                        " WHERE id = ? AND status = 'running'",
                        (f"{type(exc).__name__}: {exc}", _now_iso(), job_id),
                    )
            except Exception:
                log.exception("could not mark compaction job %s failed", job_id)
        finally:
            try:
                self.collect_garbage()
            except Exception:
                log.exception("garbage collection after job %s failed", job_id)

    # ------------------------------------------------------------------
    # compaction status
    # ------------------------------------------------------------------
    @staticmethod
    def _job_dict(row: sqlite3.Row) -> dict:
        return {
            "job_id": row["id"],
            "day": row["day"],
            "status": row["status"],
            "input_files": [Path(p).name for p in json.loads(row["input_paths"])],
            "input_rows": row["input_rows"],
            "output_file": Path(row["output_path"]).name if row["output_path"] else None,
            "output_rows": row["output_rows"],
            "error": row["error"],
            "created_at": row["created_at"],
            "finished_at": row["finished_at"],
        }

    def get_compaction(self, job_id: int) -> dict:
        with connect(str(self.db_path)) as conn:
            row = conn.execute("SELECT * FROM compactions WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"compaction job {job_id} not found")
        return self._job_dict(row)

    def list_compactions(self, day: str) -> list[dict]:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM compactions WHERE day = ? ORDER BY id DESC", (day,)
            ).fetchall()
        return [self._job_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # garbage collection of retired files
    # ------------------------------------------------------------------
    def collect_garbage(self) -> int:
        """Unlink retired files whose in-flight readers have all finished.

        Never touches ``active`` files, so freshly appended parquet files are
        always safe.
        """
        collected = 0
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                retired = conn.execute(
                    "SELECT id, path FROM files WHERE state = 'retired'"
                ).fetchall()
                for row in retired:
                    if self._inflight.get(row["path"], 0) > 0:
                        continue  # a reader is still using these bytes
                    try:
                        os.unlink(row["path"])
                    except FileNotFoundError:
                        pass
                    except OSError:
                        log.exception("could not unlink retired file %s", row["path"])
                        continue  # keep the row, retry on the next sweep
                    conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                    collected += 1
        if collected:
            log.info("collected %d retired file(s)", collected)
        return collected

    # ------------------------------------------------------------------
    # crash recovery (run once at startup, before serving traffic)
    # ------------------------------------------------------------------
    def recover(self) -> dict:
        """Reconcile disk and registry, then resume interrupted compactions."""
        stats = {"retired_removed": 0, "orphans_removed": 0, "jobs_resumed": 0}

        with connect(str(self.db_path)) as conn:
            # 1. No reader can be in flight right after boot -> retired files
            #    are safe to remove immediately.
            retired = conn.execute("SELECT id, path FROM files WHERE state = 'retired'").fetchall()
            for row in retired:
                Path(row["path"]).unlink(missing_ok=True)
                conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                stats["retired_removed"] += 1
            known = {r["path"] for r in conn.execute("SELECT path FROM files").fetchall()}
            running = conn.execute(
                "SELECT id, day, input_paths FROM compactions WHERE status = 'running'"
            ).fetchall()

        # 2. Sweep tmp fragments and parquet files the registry does not know
        #    (interrupted uploads, superseded compaction outputs).
        for day_dir in sorted(self.data_dir.glob("dt=*")):
            if not day_dir.is_dir():
                continue
            for f in day_dir.iterdir():
                if not f.is_file():
                    continue
                if ".tmp" in f.name or (f.suffix == ".parquet" and str(f) not in known):
                    f.unlink(missing_ok=True)
                    stats["orphans_removed"] += 1

        # 3. A job still 'running' cannot have switched the registry (switch
        #    and status update are one transaction), so its inputs are all
        #    still active: simply run it again.
        for job in running:
            input_paths = json.loads(job["input_paths"])
            log.warning("resuming interrupted compaction job %s for %s", job["id"], job["day"])
            self._launch_worker(job["id"], job["day"], input_paths)
            stats["jobs_resumed"] += 1

        log.info("recovery finished: %s", stats)
        return stats
