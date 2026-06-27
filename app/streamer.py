"""ffmpeg-backed media producers: archive (concat of segments), live (RTSP),
and scrub previews (single keyframe JPEG). Everything is ``-c copy`` (remux
only, no transcoding) except the JPEG preview, which decodes one frame.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

from .config import settings
from .db import Segment

# fMP4 flags that make ffmpeg emit a streamable, MSE-appendable file:
# empty_moov (init segment up front) + frag at each keyframe + default_base_moof.
_FMP4 = "+frag_keyframe+empty_moov+default_base_moof"

# Map H.264 profile names to the profile_idc byte of an avc1 codec string.
_PROFILE_IDC = {
    "Baseline": 0x42,
    "Constrained Baseline": 0x42,
    "Main": 0x4D,
    "Extended": 0x58,
    "High": 0x64,
    "High 10": 0x6E,
    "High 4:2:2": 0x7A,
    "High 4:4:4": 0xF4,
}


async def _run(*args: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )


async def probe_codecs(path: str) -> list[str]:
    """Return candidate MSE codec strings for the H.264 video in ``path``.

    Browsers are lenient about constraint flags but strict about presence, so we
    return the probed profile/level first, then permissive fallbacks the client
    can fall back to via ``MediaSource.isTypeSupported``.
    """
    candidates: list[str] = []
    proc = await asyncio.create_subprocess_exec(
        settings.ffprobe, "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=profile,level",
        "-of", "default=noprint_wrappers=1:nokey=1", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await proc.communicate()
    lines = out.decode(errors="ignore").splitlines()
    if len(lines) >= 2:
        profile, level_s = lines[0].strip(), lines[1].strip()
        idc = _PROFILE_IDC.get(profile)
        try:
            level = int(level_s)
        except ValueError:
            level = None
        if idc is not None and level is not None:
            candidates.append(f"avc1.{idc:02X}00{level:02X}")
    # Permissive fallbacks (High@5.0, High@4.1, Main@4.0, Baseline@3.1).
    for fb in ("avc1.640032", "avc1.640029", "avc1.4D0028", "avc1.42E01F"):
        if fb not in candidates:
            candidates.append(fb)
    return candidates


class ArchiveProcess:
    """One ffmpeg concat run producing a continuous fMP4 byte stream from ``t``."""

    def __init__(self, segments: list[Segment], offset: float):
        self._segments = segments
        self._offset = max(0.0, offset)
        self._proc: asyncio.subprocess.Process | None = None
        self._listfile: str | None = None

    @property
    def first_segment(self) -> Segment:
        return self._segments[0]

    async def start(self) -> None:
        fd, self._listfile = tempfile.mkstemp(suffix=".txt", prefix="sff_concat_")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for seg in self._segments:
                safe = seg.path.replace("'", "'\\''")
                fh.write(f"file '{safe}'\n")
        self._proc = await _run(
            settings.ffmpeg, "-hide_banner", "-loglevel", "error",
            "-ss", f"{self._offset:.3f}",
            "-f", "concat", "-safe", "0", "-i", self._listfile,
            "-an", "-c:v", "copy",
            "-movflags", _FMP4, "-frag_duration", "500000",
            "-f", "mp4", "pipe:1",
        )

    async def read(self, n: int = 32768) -> bytes:
        assert self._proc and self._proc.stdout
        return await self._proc.stdout.read(n)

    async def stop(self) -> None:
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass
            await self._proc.wait()
        if self._listfile and os.path.exists(self._listfile):
            os.unlink(self._listfile)


class LiveProcess:
    """ffmpeg remuxing an RTSP H.264 stream to fMP4 in real time."""

    def __init__(self, rtsp_url: str):
        self._url = rtsp_url
        self._proc: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        self._proc = await _run(
            settings.ffmpeg, "-hide_banner", "-loglevel", "error",
            "-rtsp_transport", "tcp", "-fflags", "nobuffer", "-flags", "low_delay",
            "-i", self._url,
            "-an", "-c:v", "copy",
            "-movflags", _FMP4, "-frag_duration", "200000",
            "-f", "mp4", "pipe:1",
        )

    async def read(self, n: int = 32768) -> bytes:
        assert self._proc and self._proc.stdout
        return await self._proc.stdout.read(n)

    async def stop(self) -> None:
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass
            await self._proc.wait()


async def preview_jpeg(segment: Segment, offset: float, width: int = 480) -> bytes:
    """Decode a single keyframe near ``offset`` into a small JPEG (scrub thumb)."""
    proc = await asyncio.create_subprocess_exec(
        settings.ffmpeg, "-hide_banner", "-loglevel", "error",
        "-ss", f"{max(0.0, offset):.3f}", "-i", segment.path,
        "-frames:v", "1", "-q:v", "6", "-vf", f"scale={width}:-1",
        "-f", "mjpeg", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await proc.communicate()
    return out
