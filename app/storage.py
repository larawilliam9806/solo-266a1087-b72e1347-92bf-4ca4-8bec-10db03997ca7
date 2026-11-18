"""Atomic parquet file writes.

A file becomes visible (via ``os.replace``) only after it has been fully
written and fsynced, so readers never observe a partial parquet file and a
crash cannot leave a truncated file under its final name -- at worst it
leaves a ``*.tmp-*`` fragment that startup recovery sweeps away.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_parquet_atomic(table, final_path: Path) -> Path:
    """Write ``table`` to ``final_path`` atomically (tmp + fsync + rename)."""
    final_path = Path(final_path)
    tmp = final_path.with_name(f"{final_path.name}.tmp-{uuid4().hex}")
    try:
        with open(tmp, "wb") as fh:
            pq.write_table(table, fh, compression="zstd")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, final_path)
        _fsync_dir(final_path.parent)
        return final_path
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
