"""SQLite metadata layer.

Three tables:

* ``files``       -- registry of every parquet file that belongs to a daily
                     partition.  ``state`` is ``active`` (visible to readers)
                     or ``retired`` (replaced by compaction, waiting for
                     in-flight readers to drain before the bytes are
                     unlinked).
* ``compactions`` -- one row per compaction job.  A partial unique index
                     guarantees at most one ``running`` job per day, which is
                     what lets us reject concurrent compactions for the same
                     day atomically (even across a process restart).
* ``feed_rows``   -- append-only incremental feed for downstream consumers
                     (transactional outbox): one row per uploaded
                     observation, inserted in the SAME transaction that
                     registers the upload's parquet file, so a batch is
                     visible in the feed iff its file is registered.

The compaction *switch* (retire inputs + register output + mark job done)
happens in a single SQLite transaction, so a crash can never leave the
registry half-switched.  ``feed_rows`` is never touched by compaction or
reclamation, so feed cursors (``seq`` high-water marks) keep their meaning
across compactions, GC sweeps and restarts.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    day        TEXT NOT NULL,
    path       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted')),
    state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_files_day_state ON files(day, state);

CREATE TABLE IF NOT EXISTS compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    input_paths TEXT NOT NULL,          -- JSON array, snapshot taken at job start
    input_rows  INTEGER NOT NULL,
    output_path TEXT,
    output_rows INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
-- At most one running compaction per day (enforced by the DB itself).
CREATE UNIQUE INDEX IF NOT EXISTS ux_compactions_one_running_per_day
    ON compactions(day) WHERE status = 'running';

-- Append-only incremental feed (transactional outbox).  ``seq`` is the
-- global ingestion (successful-registration) order and is what feed
-- cursors refer to; AUTOINCREMENT guarantees it is never reused, so an
-- issued cursor keeps its meaning across compactions, GC and restarts.
-- Rows are never updated or deleted.  ``file_id`` is informational only:
-- GC may delete the referenced ``files`` row, hence deliberately no FK.
CREATE TABLE IF NOT EXISTS feed_rows (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    day      TEXT NOT NULL,
    file_id  INTEGER NOT NULL,
    buoy_id  TEXT NOT NULL,
    ts_utc   TEXT NOT NULL,
    salinity REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_feed_rows_day_seq ON feed_rows(day, seq);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a short-lived connection with sane durability/concurrency pragmas."""
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # FULL so a committed upload / compaction switch survives a power cut.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
