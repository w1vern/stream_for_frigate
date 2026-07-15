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


# A camera's encoder doesn't change at runtime, but ffprobe was ~70% of the
# time-to-first-frame. Cache the result per camera for the process lifetime;
# a streamer restart re-probes (e.g. if you ever switch a camera's codec).
_codec_cache: dict[str, list[str]] = {}


async def codecs_for(camera: str, sample_path: str) -> list[str]:
    """Cached :func:`probe_codecs` keyed by camera."""
    cached = _codec_cache.get(camera)
    if cached is not None:
        return cached
    codecs = await probe_codecs(sample_path)
    _codec_cache[camera] = codecs
    return codecs


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
            # No -readrate: pacing is done by the credit window in ws.py, which
            # bounds in-flight bytes without idling the TCP connection (an idle
            # connection collapses cwnd and throttles throughput on resume).
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
            # Fragment ONLY at keyframes (no -frag_duration): every fragment then
            # starts with an IDR and is independently decodable, so the client can
            # skip to the live edge cleanly. Mid-GOP fragments caused the "half
            # frame / one person becomes two" decode artifacts after a skip/drop.
            "-movflags", _FMP4,
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


class TimelapseProcess:
    """One ffmpeg run producing a sped-up fMP4 overview of a time range.

    Two methods, both re-encoding to H.264 (this is the one place we transcode):

    * **keyframe** (default, cheap): ``-skip_frame nokey`` tells the decoder to
      emit only keyframes, so ffmpeg decodes ~1 frame per GOP (~4 s of footage)
      instead of every frame. Those keyframes are re-timed to ``out_fps`` and
      encoded, giving a slideshow-like overview at ~``speed``x for a fraction of
      the CPU of a full decode. To reach speeds beyond what the keyframe density
      allows at a sane frame rate, keyframes are additionally *decimated* — only
      every ``stride``-th kept — so any speed is reachable (see :meth:`_plan`).
    * **smooth** (opt-in, expensive): full decode + ``setpts=PTS/speed`` for fluid
      motion. Fine for short ranges; heavy for many hours on the cottage CPU.
    """

    # Playback rate the keyframe overview aims for. Keyframe decimation keeps the
    # output near this rate instead of running the frame rate up to its ceiling as
    # the requested speed climbs.
    _TARGET_FPS = 24.0

    def __init__(
        self, segments: list[Segment], offset: float, duration: float,
        speed: float, smooth: bool,
    ):
        self._segments = segments
        self._offset = max(0.0, offset)
        self._duration = max(0.0, duration)
        self._speed = max(1.0, speed)
        self._smooth = smooth
        self._proc: asyncio.subprocess.Process | None = None
        self._listfile: str | None = None

    def _plan(self) -> tuple[int, float]:
        """Return ``(keyframe stride, output fps)`` for the requested speed.

        In keyframe mode each *kept* keyframe spans ``interval * stride`` seconds of
        footage and is shown for ``1 / fps`` of playback, so the effective speed is
        ``interval * stride * fps``. We first pick a stride that lands the frame
        rate near ``_TARGET_FPS`` (so high speeds decimate keyframes rather than
        pushing fps past a sane ceiling), then set fps to hit the requested speed.
        Smooth mode keeps every frame (stride 1) at a steady 25 fps and does the
        time compression with ``setpts`` instead."""
        if self._smooth:
            return 1, 25.0
        interval = max(0.5, settings.timelapse_keyframe_interval)
        stride = max(1, round(self._speed / (interval * self._TARGET_FPS)))
        fps = self._speed / (interval * stride)
        return stride, min(30.0, max(1.0, fps))

    async def start(self) -> None:
        fd, self._listfile = tempfile.mkstemp(suffix=".txt", prefix="sff_tl_")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for seg in self._segments:
                safe = seg.path.replace("'", "'\\''")
                fh.write(f"file '{safe}'\n")
        stride, fps = self._plan()
        fps_s = f"{fps:.4f}"
        pre_input: list[str] = ["-ss", f"{self._offset:.3f}"]
        if self._duration > 0:
            pre_input += ["-t", f"{self._duration:.3f}"]
        if self._smooth:
            # Full decode; compress time via PTS. Output at a steady 25 fps.
            vf = f"setpts=PTS/{self._speed:.4f}"
        else:
            # Decode only keyframes; optionally keep every stride-th one; then
            # re-stamp the survivors onto a constant fps grid (N = running index).
            # The comma inside mod() is protected by the single quotes around the
            # select expression, so no filtergraph escaping is needed.
            pre_input = ["-skip_frame", "nokey"] + pre_input
            sel = "" if stride <= 1 else f"select='not(mod(n,{stride}))',"
            vf = f"{sel}setpts=N/{fps_s}/TB"
        self._proc = await _run(
            settings.ffmpeg, "-hide_banner", "-loglevel", "error",
            *pre_input,
            "-f", "concat", "-safe", "0", "-i", self._listfile,
            "-an", "-vf", vf, "-r", fps_s, "-fps_mode", "cfr",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
            "-pix_fmt", "yuv420p",
            "-movflags", _FMP4, "-frag_duration", "500000",
            "-f", "mp4", "pipe:1",
        )

    async def read(self, n: int = 65536) -> bytes:
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
