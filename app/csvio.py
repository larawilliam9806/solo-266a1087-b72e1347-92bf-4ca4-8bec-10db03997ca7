"""CSV parsing for buoy observations.

Expected columns: buoy id, UTC timestamp, salinity.  A header row is
optional; when present the columns are matched by (English) name, otherwise
positional order ``buoy_id, ts_utc, salinity`` is assumed.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

import pyarrow as pa

# Canonical schema used for every parquet file in the lake (uploads and
# compaction outputs alike) so concatenation never needs type promotion.
CANONICAL_SCHEMA = pa.schema(
    [
        pa.field("buoy_id", pa.string()),
        pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
        pa.field("salinity", pa.float64()),
    ]
)


class CsvError(ValueError):
    """Raised when an uploaded CSV cannot be parsed."""


_BUOY_NAMES = {"buoy_id", "buoyid", "buoy", "buoy_no", "buoyno", "id", "station", "station_id"}
_TS_NAMES = {"ts_utc", "ts", "timestamp", "time", "datetime", "utc_time", "obs_time", "observed_at"}
_SAL_NAMES = {"salinity", "salt", "sal", "sss"}

_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
)


def parse_timestamp(raw: str) -> datetime:
    """Parse an ISO-8601-ish timestamp; naive values are assumed to be UTC."""
    s = raw.strip()
    if not s:
        raise ValueError("empty timestamp")
    dt: datetime | None = None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        for fmt in _TS_FORMATS:
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        raise ValueError(f"unrecognized timestamp {raw!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _column_index(header: list[str], names: set[str], label: str) -> int:
    for i, name in enumerate(header):
        if name in names:
            return i
    raise CsvError(f"missing {label} column (accepted names: {sorted(names)})")


def parse_csv(content: bytes) -> pa.Table:
    """Parse CSV bytes into a canonical-schema table. Raises CsvError."""
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvError(f"CSV must be UTF-8 encoded: {exc}") from exc

    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if r and any(c.strip() for c in r)]
    if not rows:
        raise CsvError("empty CSV")

    header = [c.strip().lower() for c in rows[0]]
    has_header = bool(set(header) & (_BUOY_NAMES | _TS_NAMES | _SAL_NAMES))
    if has_header:
        bi = _column_index(header, _BUOY_NAMES, "buoy id")
        ti = _column_index(header, _TS_NAMES, "timestamp")
        si = _column_index(header, _SAL_NAMES, "salinity")
        data_rows = rows[1:]
        first_lineno = 2
    else:
        if len(rows[0]) < 3:
            raise CsvError("expected 3 columns: buoy_id, ts_utc, salinity")
        bi, ti, si = 0, 1, 2
        data_rows = rows
        first_lineno = 1

    if not data_rows:
        raise CsvError("CSV contains no data rows")

    buoys: list[str] = []
    tss: list[datetime] = []
    sals: list[float] = []
    errors: list[str] = []
    need = max(bi, ti, si) + 1
    for offset, r in enumerate(data_rows):
        lineno = first_lineno + offset
        if len(r) < need:
            errors.append(f"line {lineno}: expected at least {need} columns, got {len(r)}")
            continue
        buoy = r[bi].strip()
        if not buoy:
            errors.append(f"line {lineno}: empty buoy_id")
            continue
        try:
            ts = parse_timestamp(r[ti])
        except ValueError as exc:
            errors.append(f"line {lineno}: {exc}")
            continue
        try:
            sal = float(r[si])
        except ValueError:
            errors.append(f"line {lineno}: bad salinity {r[si]!r}")
            continue
        buoys.append(buoy)
        tss.append(ts)
        sals.append(sal)

    if errors:
        msg = "; ".join(errors[:5])
        if len(errors) > 5:
            msg += f" ... and {len(errors) - 5} more"
        raise CsvError(msg)

    return pa.Table.from_pydict(
        {"buoy_id": buoys, "ts_utc": tss, "salinity": sals},
        schema=CANONICAL_SCHEMA,
    )


def empty_table() -> pa.Table:
    return CANONICAL_SCHEMA.empty_table()
