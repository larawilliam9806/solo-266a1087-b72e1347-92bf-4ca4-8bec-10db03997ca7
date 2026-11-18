"""End-to-end tests for the buoy data-lake compaction service."""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import connect  # noqa: E402
from app.lake import Lake  # noqa: E402
from app.main import create_app  # noqa: E402

DAY = "2026-11-18"

CSV_A = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:01:00Z,35.1\n"
    "B002,2026-11-18T00:02:00Z,35.2\n"
)
CSV_B = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T01:01:00Z,35.3\n"
    "B003,2026-11-18T01:02:00Z,34.9\n"
)
CSV_C = "buoy_id,ts_utc,salinity\nB004,2026-11-18T02:00:00Z,36.0\n"
# duplicate rows on purpose: compaction must keep every occurrence
CSV_DUP = (
    "buoy_id,ts_utc,salinity\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
)


def upload(client: TestClient, csv_text: str, day: str = DAY, raw: bool = False):
    if raw:
        r = client.post(f"/days/{day}/upload", content=csv_text.encode(),
                        headers={"Content-Type": "text/csv"})
    else:
        r = client.post(f"/days/{day}/upload",
                        files={"file": ("obs.csv", csv_text.encode(), "text/csv")})
    assert r.status_code == 200, r.text
    return r.json()


def wait_job(client: TestClient, day: str, job_id: int, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/days/{day}/compactions/{job_id}")
        assert r.status_code == 200, r.text
        job = r.json()
        if job["status"] in ("done", "failed"):
            return job
        time.sleep(0.05)
    pytest.fail(f"compaction job {job_id} did not finish in {timeout}s")


def make_client(tmp_path, **kwargs) -> TestClient:
    return TestClient(create_app(data_dir=str(tmp_path / "data"), **kwargs))


# ---------------------------------------------------------------------------
# basic upload / read
# ---------------------------------------------------------------------------

def test_upload_and_read_roundtrip(tmp_path):
    with make_client(tmp_path) as c:
        up1 = upload(c, CSV_A)                      # multipart
        assert up1["rows"] == 2 and up1["file_count"] == 1
        up2 = upload(c, CSV_B, raw=True)            # raw text/csv body
        assert up2["rows"] == 2 and up2["file_count"] == 2

        r = c.get(f"/days/{DAY}/records")
        assert r.status_code == 200
        body = r.json()
        assert body["file_count"] == 2
        assert body["row_count"] == 4
        got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in body["records"]}
        assert got == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
            ("B001", "2026-11-18T01:01:00Z", 35.3),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }

        r = c.get(f"/days/{DAY}/files")
        assert r.status_code == 200
        assert r.json()["file_count"] == 2


def test_headerless_csv_and_naive_timestamps(tmp_path):
    csv_text = "B007,2026-11-18 05:06:07,35.55\n"
    with make_client(tmp_path) as c:
        upload(c, csv_text, raw=True)
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 1
        assert body["records"][0]["ts_utc"] == "2026-11-18T05:06:07Z"
        assert body["records"][0]["salinity"] == 35.55


def test_invalid_csv_rejected(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/days/{DAY}/upload", content=b"buoy_id,ts_utc,salinity\nB1,not-a-time,35\n",
                   headers={"Content-Type": "text/csv"})
        assert r.status_code == 422
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 0


def test_invalid_day_rejected(tmp_path):
    with make_client(tmp_path) as c:
        assert c.post("/days/18-11-2026/upload", content=b"x").status_code == 400
        assert c.get("/days/2026-13-01/records").status_code == 400


# ---------------------------------------------------------------------------
# compaction
# ---------------------------------------------------------------------------

def test_compaction_merges_and_preserves_duplicates(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        upload(c, CSV_DUP)          # two identical rows
        upload(c, CSV_DUP)          # ... uploaded twice

        before = c.get(f"/days/{DAY}/records").json()
        assert before["file_count"] == 4 and before["row_count"] == 8

        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202, r.text
        job = wait_job(c, DAY, r.json()["job_id"])
        assert job["status"] == "done", job
        assert job["input_rows"] == 8 and job["output_rows"] == 8

        after = c.get(f"/days/{DAY}/records").json()
        assert after["file_count"] == 1
        assert after["row_count"] == 8  # duplicates preserved, nothing lost
        key = lambda rec: (rec["buoy_id"], rec["ts_utc"], rec["salinity"])
        assert sorted(map(key, after["records"])) == sorted(map(key, before["records"]))


def test_concurrent_compaction_rejected(tmp_path):
    with make_client(tmp_path, compaction_delay_sec=0.6) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        r1 = c.post(f"/days/{DAY}/compactions")
        assert r1.status_code == 202
        r2 = c.post(f"/days/{DAY}/compactions")
        assert r2.status_code == 409, r2.text
        job = wait_job(c, DAY, r1.json()["job_id"])
        assert job["status"] == "done"
        # after it finishes a new compaction may be started again
        r3 = c.post(f"/days/{DAY}/compactions")
        assert r3.status_code == 202
        assert wait_job(c, DAY, r3.json()["job_id"])["status"] == "done"


def test_upload_and_read_during_compaction(tmp_path):
    """Appends during compaction stay readable and survive the switch."""
    with make_client(tmp_path, compaction_delay_sec=0.8) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202
        job_id = r.json()["job_id"]

        # appended while the compaction is merging its input snapshot
        up = upload(c, CSV_C)
        assert up["rows"] == 1

        # reads never wait for compaction and always see a complete state
        expected = {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
            ("B001", "2026-11-18T01:01:00Z", 35.3),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
            ("B004", "2026-11-18T02:00:00Z", 36.0),
        }
        for _ in range(10):
            body = c.get(f"/days/{DAY}/records").json()
            got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in body["records"]}
            assert got == expected
            assert body["row_count"] == 5
            assert body["file_count"] in (2, 3)  # pre- or post-switch, never torn
            time.sleep(0.05)

        job = wait_job(c, DAY, job_id)
        assert job["status"] == "done"
        assert job["input_rows"] == 4 and job["output_rows"] == 4

        final = c.get(f"/days/{DAY}/records").json()
        # compacted output + the file appended during compaction
        assert final["file_count"] == 2
        assert final["row_count"] == 5
        got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in final["records"]}
        assert got == expected


def test_read_is_single_consistent_snapshot(tmp_path):
    """A read must reflect a complete pre- or post-switch registry state."""
    with make_client(tmp_path, compaction_delay_sec=0.5) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]

        lake = c.app.state.lake
        seen = []
        stop = time.time() + 2.0
        while time.time() < stop:
            with lake._state_lock:  # observe exactly what a reader would pin
                inflight = dict(lake._inflight)
            body = c.get(f"/days/{DAY}/records").json()
            seen.append((body["file_count"], body["row_count"]))
            time.sleep(0.02)
        assert wait_job(c, DAY, job_id)["status"] == "done"
        # every observation was a complete state: 2 files/4 rows before the
        # switch, 1 file/4 rows after -- never anything else
        assert set(seen) <= {(2, 4), (1, 4)}
        assert (2, 4) in seen and (1, 4) in seen


def test_compaction_on_empty_day_is_noop(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202
        job = wait_job(c, DAY, r.json()["job_id"])
        assert job["status"] == "done"
        assert job["output_rows"] == 0
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 0


# ---------------------------------------------------------------------------
# failure handling
# ---------------------------------------------------------------------------

def test_failed_compaction_keeps_original_data(tmp_path, monkeypatch):
    import app.lake as lake_mod

    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)

        original = lake_mod.write_parquet_atomic
        def boom(*args, **kwargs):
            raise IOError("disk full (simulated)")
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", boom)

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        job = wait_job(c, DAY, job_id)
        assert job["status"] == "failed"
        assert "disk full" in job["error"]

        # originals untouched and readable
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 2 and body["row_count"] == 4

        # retry succeeds once the "disk" works again
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", original)
        job_id2 = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id2)["status"] == "done"
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 4


# ---------------------------------------------------------------------------
# garbage collection of retired files
# ---------------------------------------------------------------------------

def test_retired_files_collected_appends_kept(tmp_path):
    with make_client(tmp_path, compaction_delay_sec=0.5, gc_interval_sec=0.1) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        data_dir = tmp_path / "data" / f"dt={DAY}"
        old_files = {p.name for p in data_dir.glob("*.parquet")}
        assert len(old_files) == 2

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        upload(c, CSV_C)  # appended during compaction -> must never be collected
        assert wait_job(c, DAY, job_id)["status"] == "done"

        deadline = time.time() + 10
        while time.time() < deadline:
            remaining = {p.name for p in data_dir.glob("*.parquet")}
            if old_files.isdisjoint(remaining):
                break
            time.sleep(0.1)
        else:
            pytest.fail("retired files were not reclaimed")

        remaining = {p.name for p in data_dir.glob("*.parquet")}
        assert len(remaining) == 2  # compacted output + the appended file
        assert any(n.startswith("compacted-") for n in remaining)
        assert any(n.startswith("part-") for n in remaining)
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 5 and body["file_count"] == 2


# ---------------------------------------------------------------------------
# crash recovery
# ---------------------------------------------------------------------------

def test_restart_resumes_interrupted_compaction(tmp_path):
    """A job that was 'running' when the process died is resumed on boot."""
    data_dir = tmp_path / "data"

    lake1 = Lake(str(data_dir))
    lake1.upload(DAY, CSV_A.encode())
    lake1.upload(DAY, CSV_B.encode())
    # simulate a crash mid-compaction: job row persisted as 'running',
    # worker thread never ran (process died)
    with connect(str(lake1.db_path)) as conn:
        rows = conn.execute(
            "SELECT path FROM files WHERE day = ? AND state = 'active'", (DAY,)
        ).fetchall()
        conn.execute(
            "INSERT INTO compactions(day, status, input_paths, input_rows, created_at)"
            " VALUES (?, 'running', ?, ?, ?)",
            (DAY, json.dumps([r["path"] for r in rows]), 4, "2026-11-18T00:00:00Z"),
        )
    del lake1  # "process dies"

    with make_client(tmp_path) as c:
        # recovery resumed the job; wait for it to finish
        deadline = time.time() + 15
        job = None
        while time.time() < deadline:
            jobs = c.get(f"/days/{DAY}/compactions").json()["jobs"]
            assert len(jobs) == 1
            job = jobs[0]
            if job["status"] in ("done", "failed"):
                break
            time.sleep(0.05)
        assert job["status"] == "done", job
        assert job["output_rows"] == 4

        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 4


def test_restart_preserves_uploads_and_cleans_orphans(tmp_path):
    data_dir = tmp_path / "data"

    lake1 = Lake(str(data_dir))
    lake1.upload(DAY, CSV_A.encode())
    # orphan: bytes renamed into place but never registered (upload crash),
    # plus a tmp fragment from an interrupted write
    day_dir = data_dir / f"dt={DAY}"
    (day_dir / "part-orphan.parquet").write_bytes(b"not registered")
    (day_dir / "part-x.parquet.tmp-deadbeef").write_bytes(b"partial")
    del lake1

    with make_client(tmp_path) as c:
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2  # committed upload survived
        remaining = {p.name for p in day_dir.glob("*")}
        assert "part-orphan.parquet" not in remaining
        assert not any(".tmp" in n for n in remaining)


def test_concurrent_reads_during_compaction_are_consistent(tmp_path):
    """Hammer reads from many threads while a compaction switches state."""
    with make_client(tmp_path, compaction_delay_sec=0.4) as c:
        for csv in (CSV_A, CSV_B, CSV_C):
            upload(c, csv)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]

        errors = []
        def reader():
            try:
                while not done.is_set():
                    body = c.get(f"/days/{DAY}/records").json()
                    assert body["row_count"] == 5
                    assert body["file_count"] in (1, 3)
                    assert len(body["records"]) == 5
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        done = threading.Event()
        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        assert wait_job(c, DAY, job_id)["status"] == "done"
        time.sleep(0.2)
        done.set()
        for t in threads:
            t.join()
        assert not errors, errors
