"""Read-only access to Frigate's ``recordings`` index.

We never scan the filesystem: the segment timeline comes entirely from the
``recordings`` table (path, start_time, end_time, duration). All times are UTC
epoch seconds, as Frigate stores them.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .config import settings

# A gap larger than this (seconds) between consecutive segments breaks a
# continuous playback run — ffmpeg concat would otherwise hide the discontinuity.
MAX_GAP = 1.5


@dataclass(frozen=True)
class Segment:
    path: str  # filesystem path in *this* process (already remapped)
    start: float
    end: float
    duration: float


def _open() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{settings.frigate_db}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _seg(row: sqlite3.Row) -> Segment:
    return Segment(
        path=settings.remap_path(row["path"]),
        start=float(row["start_time"]),
        end=float(row["end_time"]),
        duration=float(row["duration"]),
    )


def list_cameras() -> list[str]:
    """Cameras configured for live, unioned with whatever has recordings."""
    cams = set(settings.cameras)
    with _open() as conn:
        for row in conn.execute("SELECT DISTINCT camera FROM recordings"):
            cams.add(row["camera"])
    return sorted(cams)


def bounds(camera: str) -> tuple[float, float] | None:
    with _open() as conn:
        row = conn.execute(
            "SELECT MIN(start_time) AS s, MAX(end_time) AS e "
            "FROM recordings WHERE camera = ?",
            (camera,),
        ).fetchone()
    if row is None or row["s"] is None:
        return None
    return float(row["s"]), float(row["e"])


def segment_at(camera: str, t: float) -> Segment | None:
    """Segment covering instant ``t``; if none covers it, the next one after ``t``."""
    with _open() as conn:
        row = conn.execute(
            "SELECT path, start_time, end_time, duration FROM recordings "
            "WHERE camera = ? AND start_time <= ? AND end_time > ? "
            "ORDER BY start_time DESC LIMIT 1",
            (camera, t, t),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT path, start_time, end_time, duration FROM recordings "
                "WHERE camera = ? AND start_time >= ? "
                "ORDER BY start_time ASC LIMIT 1",
                (camera, t),
            ).fetchone()
    return _seg(row) if row else None


def availability(
    camera: str, start: float, end: float, merge_gap: float = 5.0
) -> list[list[float]]:
    """Merged [start, end] intervals where recording exists within [start, end].

    Consecutive segments (and tiny inter-segment gaps <= ``merge_gap``) collapse
    into one interval; the gaps that remain are genuine "no recording" spans, used
    to shade the timeline. Returned intervals are clipped to the query window.
    """
    with _open() as conn:
        rows = conn.execute(
            "SELECT start_time, end_time FROM recordings "
            "WHERE camera = ? AND end_time >= ? AND start_time <= ? "
            "ORDER BY start_time ASC",
            (camera, start, end),
        ).fetchall()
    intervals: list[list[float]] = []
    for row in rows:
        s = max(start, float(row["start_time"]))
        e = min(end, float(row["end_time"]))
        if e <= s:
            continue
        if intervals and s - intervals[-1][1] <= merge_gap:
            intervals[-1][1] = max(intervals[-1][1], e)
        else:
            intervals.append([s, e])
    return intervals


def contiguous_from(
    camera: str, t: float, max_duration: float = 6 * 3600
) -> list[Segment]:
    """Segments starting at the one covering ``t``, walking forward while they
    stay back-to-back (gap <= MAX_GAP), capped at ``max_duration`` of footage."""
    first = segment_at(camera, t)
    if first is None:
        return []
    with _open() as conn:
        rows = conn.execute(
            "SELECT path, start_time, end_time, duration FROM recordings "
            "WHERE camera = ? AND start_time >= ? "
            "ORDER BY start_time ASC LIMIT 5000",
            (camera, first.start),
        ).fetchall()
    out: list[Segment] = []
    total = 0.0
    prev_end: float | None = None
    for row in rows:
        seg = _seg(row)
        if prev_end is not None and seg.start - prev_end > MAX_GAP:
            break
        out.append(seg)
        prev_end = seg.end
        total += seg.duration
        if total >= max_duration:
            break
    return out
