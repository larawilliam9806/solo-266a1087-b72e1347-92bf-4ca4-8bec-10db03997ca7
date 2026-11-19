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
from app.csvio import parse_csv  # noqa: E402
from app.lake import Lake  # noqa: E402
from app.main import create_app  # noqa: E402
from app.storage import write_parquet_atomic  # noqa: E402

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


def increment(client: TestClient, day: str = DAY, cursor=None, limit=None) -> dict:
    params = {}
    if cursor is not None:
        params["cursor"] = cursor
    if limit is not None:
        params["limit"] = limit
    r = client.get(f"/days/{day}/increment", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def drain_increment(client: TestClient, day: str = DAY, limit: int = 500):
    """Follow cursors from the start of the feed to its current end."""
    records: list[dict] = []
    cursor = None
    for _ in range(1000):  # safety net against a looping cursor
        body = increment(client, day=day, cursor=cursor, limit=limit)
        assert body["row_count"] == len(body["records"]) <= limit
        records.extend(body["records"])
        if not body["has_more"]:
            return records, body
        assert body["next_cursor"] is not None
        cursor = body["next_cursor"]
    raise AssertionError("increment feed did not terminate")


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


# ---------------------------------------------------------------------------
# incremental feed (/days/{day}/increment)
# ---------------------------------------------------------------------------

def test_increment_returns_upload_order_and_duplicates(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_DUP)
        upload(c, CSV_DUP)  # same observation again -> delivered again
        body = increment(c)
        assert body["day"] == DAY
        assert body["row_count"] == 6
        assert body["has_more"] is False
        keys = [(r["buoy_id"], r["ts_utc"], r["salinity"]) for r in body["records"]]
        assert keys == [
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
            ("B009", "2026-11-18T03:00:00Z", 33.3),
            ("B009", "2026-11-18T03:00:00Z", 33.3),
            ("B009", "2026-11-18T03:00:00Z", 33.3),
            ("B009", "2026-11-18T03:00:00Z", 33.3),
        ]
        # exactly the raw three columns
        assert all(set(r) == {"buoy_id", "ts_utc", "salinity"} for r in body["records"])


def test_increment_pagination_never_loses_or_repeats_rows(tmp_path):
    """Any page size -- including ones cutting through a batch -- is exact."""
    expected = [
        ("B001", "2026-11-18T00:01:00Z", 35.1),
        ("B002", "2026-11-18T00:02:00Z", 35.2),
        ("B001", "2026-11-18T01:01:00Z", 35.3),
        ("B003", "2026-11-18T01:02:00Z", 34.9),
        ("B004", "2026-11-18T02:00:00Z", 36.0),
        ("B009", "2026-11-18T03:00:00Z", 33.3),
        ("B009", "2026-11-18T03:00:00Z", 33.3),
    ]
    with make_client(tmp_path) as c:
        upload(c, CSV_A)     # 2 rows
        upload(c, CSV_B)     # 2 rows
        upload(c, CSV_C)     # 1 row
        upload(c, CSV_DUP)   # 2 identical rows
        for limit in (1, 2, 3, 4, 5, 500):
            records, _ = drain_increment(c, limit=limit)
            keys = [(r["buoy_id"], r["ts_utc"], r["salinity"]) for r in records]
            assert keys == expected, f"limit={limit}"


def test_increment_stops_mid_batch_and_resumes(tmp_path):
    """A page boundary inside one upload batch must not lose rows."""
    with make_client(tmp_path) as c:
        upload(c, CSV_A)  # rows 1-2
        upload(c, CSV_B)  # rows 3-4
        page1 = increment(c, limit=3)  # cuts through CSV_B's batch
        assert [r["buoy_id"] for r in page1["records"]] == ["B001", "B002", "B001"]
        assert page1["has_more"] is True
        page2 = increment(c, cursor=page1["next_cursor"], limit=3)
        assert [r["buoy_id"] for r in page2["records"]] == ["B003"]
        assert page2["has_more"] is False


def test_increment_page_size_capped_at_500(tmp_path):
    big = "buoy_id,ts_utc,salinity\n" + "".join(
        f"B{i % 9:02d},2026-11-18T06:{i // 60:02d}:{i % 60:02d}Z,3{i % 10}.{i % 9}\n"
        for i in range(600)
    )
    with make_client(tmp_path) as c:
        upload(c, big)
        page1 = increment(c)  # default page size
        assert page1["row_count"] == 500 and page1["has_more"] is True
        page2 = increment(c, cursor=page1["next_cursor"], limit=9999)  # clamped
        assert page2["row_count"] == 100 and page2["has_more"] is False
        # pages tile the full feed exactly, no overlap
        all_records, _ = drain_increment(c)
        assert page1["records"] + page2["records"] == all_records


def test_increment_follows_new_uploads(tmp_path):
    """Rows uploaded after a page was served appear in later requests."""
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        page = increment(c)
        assert page["row_count"] == 2 and page["has_more"] is False
        cursor = page["next_cursor"]

        upload(c, CSV_B)
        page = increment(c, cursor=cursor)
        assert [r["buoy_id"] for r in page["records"]] == ["B001", "B003"]
        cursor = page["next_cursor"]

        upload(c, CSV_C)
        page = increment(c, cursor=cursor)
        assert [r["buoy_id"] for r in page["records"]] == ["B004"]
        assert page["has_more"] is False


def test_increment_empty_day_and_bad_params(tmp_path):
    with make_client(tmp_path) as c:
        body = increment(c)
        assert body["records"] == [] and body["row_count"] == 0
        assert body["next_cursor"] is None and body["has_more"] is False

        upload(c, CSV_A)
        for bad in ("abc", "-1", "1.5", "1e3", " "):
            r = c.get(f"/days/{DAY}/increment", params={"cursor": bad})
            assert r.status_code == 400, bad
        assert c.get(f"/days/{DAY}/increment", params={"limit": 0}).status_code == 400
        assert c.get(f"/days/{DAY}/increment", params={"limit": -3}).status_code == 400
        assert c.get("/days/18-11-2026/increment").status_code == 400


def test_increment_excludes_failed_uploads(tmp_path, monkeypatch):
    import app.lake as lake_mod

    with make_client(tmp_path) as c:
        # CSV parse failure -> 422, nothing registered
        r = c.post(f"/days/{DAY}/upload", content=b"buoy_id,ts_utc,salinity\nB1,nope,35\n",
                   headers={"Content-Type": "text/csv"})
        assert r.status_code == 422

        # parquet write failure -> request fails, nothing registered
        original = lake_mod.write_parquet_atomic
        def boom(*args, **kwargs):
            raise IOError("disk full (simulated)")
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", boom)
        with pytest.raises(IOError):
            c.post(f"/days/{DAY}/upload", content=CSV_A.encode(),
                   headers={"Content-Type": "text/csv"})
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", original)

        assert increment(c)["records"] == []
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 0

        upload(c, CSV_B)
        assert [r["buoy_id"] for r in increment(c)["records"]] == ["B001", "B003"]


def test_increment_unaffected_by_compaction_and_gc(tmp_path):
    """Compaction + retired-file reclamation must not move or replay rows."""
    with make_client(tmp_path, gc_interval_sec=0.1) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)

        page1 = increment(c, limit=3)
        assert page1["row_count"] == 3 and page1["has_more"] is True
        cursor = page1["next_cursor"]

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"
        c.app.state.lake.collect_garbage()  # force retired-file reclamation
        data_dir = tmp_path / "data" / f"dt={DAY}"
        assert len(list(data_dir.glob("*.parquet"))) == 1  # only compacted output

        # the issued cursor still means exactly the same position
        page2 = increment(c, cursor=cursor)
        assert [r["buoy_id"] for r in page2["records"]] == ["B003"]
        assert page2["has_more"] is False

        # full re-drain: every uploaded row exactly once, no re-delivery
        records, _ = drain_increment(c)
        assert [r["buoy_id"] for r in records] == ["B001", "B002", "B001", "B003"]


def test_increment_cursor_survives_restart(tmp_path):
    with make_client(tmp_path) as c1:
        upload(c1, CSV_A)
        page = increment(c1, limit=1)
        assert page["has_more"] is True
        cursor = page["next_cursor"]

    with make_client(tmp_path) as c2:  # same data dir, fresh process state
        upload(c2, CSV_B)
        page = increment(c2, cursor=cursor)
        assert [r["buoy_id"] for r in page["records"]] == ["B002", "B001", "B003"]
        assert page["has_more"] is False
        records, _ = drain_increment(c2)
        assert [r["buoy_id"] for r in records] == ["B001", "B002", "B001", "B003"]


def test_increment_concurrent_uploads_stay_contiguous(tmp_path):
    """Concurrent uploads appear as whole batches in registration order."""
    def csv_for(i: int) -> str:
        return (
            "buoy_id,ts_utc,salinity\n"
            f"T{i:02d},2026-11-18T04:{i:02d}:00Z,30.{i}\n"
            f"T{i:02d},2026-11-18T04:{i:02d}:30Z,31.{i}\n"
        )

    with make_client(tmp_path) as c:
        errors = []
        def worker(i: int):
            try:
                upload(c, csv_for(i))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors

        records, _ = drain_increment(c)
        assert len(records) == 16
        # each upload batch occupies one contiguous run of feed rows
        runs: list[list] = []
        for rec in records:
            if not runs or runs[-1][0] != rec["buoy_id"]:
                runs.append([rec["buoy_id"], 1])
            else:
                runs[-1][1] += 1
        assert sorted(b for b, _ in runs) == [f"T{i:02d}" for i in range(8)]
        assert all(n == 2 for _, n in runs)


# schema as it existed before the incremental feed was introduced
OLD_SCHEMA = """
CREATE TABLE files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    day        TEXT NOT NULL,
    path       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted')),
    state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    input_paths TEXT NOT NULL,
    input_rows  INTEGER NOT NULL,
    output_path TEXT,
    output_rows INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
"""


def test_increment_excludes_pre_feature_data(tmp_path):
    """Rows from before the feed existed stay in /records only (no backfill)."""
    data_dir = tmp_path / "data"
    day_dir = data_dir / f"dt={DAY}"
    day_dir.mkdir(parents=True)
    # simulate a deployment from before the incremental feed existed:
    # old-schema DB plus one registered parquet file
    conn = connect(str(data_dir / "lake_meta.db"))
    conn.executescript(OLD_SCHEMA)
    old_file = day_dir / "part-old.parquet"
    write_parquet_atomic(parse_csv(CSV_C.encode()), old_file)
    conn.execute(
        "INSERT INTO files(day, path, kind, state, rows, created_at)"
        " VALUES (?, ?, 'upload', 'active', 1, '2026-11-18T00:00:00Z')",
        (DAY, str(old_file)),
    )
    conn.commit()
    conn.close()

    with make_client(tmp_path) as c:
        # original by-day read still serves the historical row
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 1
        assert body["records"][0]["buoy_id"] == "B004"
        # ...but the feed starts empty: only post-enablement uploads appear
        feed = increment(c)
        assert feed["records"] == [] and feed["has_more"] is False
        assert feed["next_cursor"] is None

        upload(c, CSV_A)
        feed = increment(c)
        assert [r["buoy_id"] for r in feed["records"]] == ["B001", "B002"]
        assert feed["has_more"] is False  # historical row not backfilled
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 3
