"""WebSocket protocol: one socket per client carries both commands (JSON text)
and media (binary). Mirrors the Hik-Connect idea — a single persistent session
instead of many short HTTP round-trips.

Binary frames from server -> client are tagged by a 1-byte type prefix:
    0x01  media   : remaining bytes are fMP4 to append to the SourceBuffer
    0x02  preview : 8-byte big-endian float64 (epoch time) + JPEG bytes

Text frames (JSON) carry control in both directions; see ``_handle`` and the
``init`` / ``bounds`` / ``error`` messages below.
"""

from __future__ import annotations

import asyncio
import json
import struct

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from . import db

from . import streamer
from .auth import verify_token
from .config import settings

MEDIA = b"\x01"
PREVIEW = b"\x02"


class Session:
    """Drives a single live or archive playback for one connection."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self._task: asyncio.Task | None = None
        self._archive: streamer.ArchiveProcess | None = None
        self._live: streamer.LiveProcess | None = None
        # Flow control (archive only): seconds the client is buffered ahead.
        self._ahead = 0.0
        self._resume = asyncio.Event()
        self._resume.set()
        # Bumped on every new stream; acks from an earlier stream carry a stale
        # gen and are ignored, so a leftover "buffer full" ack can't pause the
        # freshly started one (the cause of the every-other-seek black screen).
        self._gen = 0
        # Serializes ALL writes to the socket. The pump task (media) and the
        # receive loop (scrub previews / json) would otherwise call send
        # concurrently; `websockets` forbids overlapping drains and asserts,
        # killing the connection — which is exactly what broke scrubbing once
        # latency (the relay) made drains actually block.
        self._send_lock = asyncio.Lock()

    # -- lifecycle ----------------------------------------------------------
    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._archive:
            await self._archive.stop()
            self._archive = None
        if self._live:
            await self._live.stop()
            self._live = None

    def set_ahead(self, seconds: float, gen: int) -> None:
        if gen != self._gen:
            return  # stale ack from a previous stream
        self._ahead = seconds
        if seconds <= settings.buffer_low:
            self._resume.set()
        elif seconds >= settings.buffer_high:
            self._resume.clear()

    # -- archive ------------------------------------------------------------
    async def play(self, camera: str, t: float) -> None:
        await self.stop()
        segments = db.contiguous_from(camera, t)
        if not segments:
            await self._send_json({"type": "error", "message": "no recording at that time"})
            return
        first = segments[0]
        offset = max(0.0, t - first.start)
        codecs = await streamer.codecs_for(camera, first.path)
        self._gen += 1
        self._ahead = 0.0
        self._resume.set()
        # stream_start: epoch of the first frame the client will see. ffmpeg
        # seeks to the keyframe at/just before ``offset`` and rebases PTS to 0,
        # so currentTime 0 maps to roughly ``t`` (within one GOP).
        await self._send_json({
            "type": "init", "mode": "archive", "camera": camera, "gen": self._gen,
            "codecs": codecs, "streamStart": first.start + offset,
        })
        self._archive = streamer.ArchiveProcess(segments, offset)
        await self._archive.start()
        self._task = asyncio.create_task(self._pump_archive())

    async def _pump_archive(self) -> None:
        assert self._archive
        try:
            while True:
                await self._resume.wait()
                chunk = await self._archive.read()
                if not chunk:
                    await self._send_json({"type": "ended"})
                    break
                await self._send_bytes(MEDIA + chunk)
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            pass

    # -- live ---------------------------------------------------------------
    async def live(self, camera: str) -> None:
        await self.stop()
        url = settings.cameras.get(camera)
        if not url:
            await self._send_json({"type": "error", "message": f"no live url for {camera}"})
            return
        # Probe a recent recording for the codec string (same encoder as live).
        seg = db.segment_at(camera, db.bounds(camera)[1] - 1) if db.bounds(camera) else None
        codecs = await streamer.codecs_for(camera, seg.path) if seg else ["avc1.640029", "avc1.4D0028"]
        self._gen += 1
        self._ahead = 0.0
        self._resume.set()
        await self._send_json({
            "type": "init", "mode": "live", "camera": camera, "gen": self._gen,
            "codecs": codecs,
        })
        self._live = streamer.LiveProcess(url)
        await self._live.start()
        self._task = asyncio.create_task(self._pump_live())

    async def _pump_live(self) -> None:
        assert self._live
        try:
            while True:
                chunk = await self._live.read()
                if not chunk:
                    await self._send_json({"type": "ended"})
                    break
                await self._send_bytes(MEDIA + chunk)
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            pass

    # -- scrub --------------------------------------------------------------
    async def scrub(self, camera: str, t: float) -> None:
        seg = db.segment_at(camera, t)
        if seg is None:
            return
        offset = max(0.0, t - seg.start)
        jpeg = await streamer.preview_jpeg(seg, offset)
        if jpeg and self.ws.application_state == WebSocketState.CONNECTED:
            await self._send_bytes(PREVIEW + struct.pack(">d", t) + jpeg)

    async def _send_bytes(self, data: bytes) -> None:
        if self.ws.application_state == WebSocketState.CONNECTED:
            async with self._send_lock:
                await self.ws.send_bytes(data)

    async def _send_json(self, obj: dict) -> None:
        if self.ws.application_state == WebSocketState.CONNECTED:
            async with self._send_lock:
                await self.ws.send_text(json.dumps(obj))


async def handle(ws: WebSocket) -> None:
    await ws.accept()
    session = Session(ws)

    # First message must authenticate.
    try:
        first = await ws.receive_text()
        msg = json.loads(first)
    except (WebSocketDisconnect, ValueError, json.JSONDecodeError):
        await ws.close(code=1008)
        return
    if msg.get("type") != "auth" or not verify_token(msg.get("token", "")):
        await ws.send_text(json.dumps({"type": "error", "message": "unauthorized"}))
        await ws.close(code=1008)
        return

    await ws.send_text(json.dumps({"type": "ready", "cameras": db.list_cameras()}))

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await _dispatch(session, msg)
    except WebSocketDisconnect:
        pass
    finally:
        await session.stop()


async def _dispatch(session: Session, msg: dict) -> None:
    kind = msg.get("type")
    if kind == "play":
        await session.play(msg["camera"], float(msg["time"]))
    elif kind == "live":
        await session.live(msg["camera"])
    elif kind == "scrub":
        await session.scrub(msg["camera"], float(msg["time"]))
    elif kind == "ack":
        session.set_ahead(float(msg.get("aheadSec", 0)), int(msg.get("gen", -1)))
    elif kind == "bounds":
        b = db.bounds(msg["camera"])
        await session._send_json({
            "type": "bounds", "camera": msg["camera"],
            "start": b[0] if b else None, "end": b[1] if b else None,
        })
    elif kind == "stop":
        await session.stop()
