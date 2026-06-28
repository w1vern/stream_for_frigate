"""WebSocket protocol: one socket per client carries both commands (JSON text)
and media (binary). Mirrors the Hik-Connect idea — a single persistent session
instead of many short HTTP round-trips.

Binary frames from server -> client are tagged by a 1-byte type prefix:
    0x01  media   : [0x01][gen uint32][fMP4] to append to the SourceBuffer
    0x02  preview : [0x02][epoch float64][JPEG] scrub thumbnail

Text frames (JSON) carry control in both directions; see ``handle`` and the
``init`` / ``bounds`` / ``error`` messages below.

All outbound writes go through a single ``_writer`` task fed by two queues. The
receive loop and the media pump never touch the socket directly: they enqueue
and return. This is essential — if the receive loop blocked on a socket write
(which stalls under relay backpressure), it would stop processing commands and
seeks would be silently ignored. Control messages take priority over media so an
``init`` is never stuck behind a backed-up media queue.
"""

from __future__ import annotations

import asyncio
import json
import struct

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from . import db, streamer
from .auth import verify_token
from .config import settings

MEDIA = b"\x01"
PREVIEW = b"\x02"

# Bound the media queue so a slow client (relay backpressure) makes the pump
# block on put() -> ffmpeg blocks -> natural backpressure, without unbounded RAM.
MEDIA_QUEUE_MAX = 256


class Session:
    """Drives a single live or archive playback for one connection."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self._task: asyncio.Task | None = None
        self._writer_task: asyncio.Task | None = None
        self._archive: streamer.ArchiveProcess | None = None
        self._live: streamer.LiveProcess | None = None
        # Outbound queues drained by the single _writer task.
        self._ctrlq: asyncio.Queue[bytes | str] = asyncio.Queue()
        self._mediaq: asyncio.Queue[bytes] = asyncio.Queue(maxsize=MEDIA_QUEUE_MAX)
        self._has_output = asyncio.Event()
        # Soft prefetch cap (archive only): seconds the client is buffered ahead.
        self._resume = asyncio.Event()
        self._resume.set()
        # Credit window: bytes sent vs. bytes the client has acked receiving, for
        # the current stream. The pump stops once `_sent - _acked` reaches the
        # window, so at most `flow_window_bytes` are ever in flight across the
        # whole path. `_can_send` is set while there's credit. Reset on every new
        # stream. This is what bounds seek/switch latency over a buffered relay.
        self._window = settings.flow_window_bytes
        self._sent = 0
        self._acked = 0
        self._can_send = asyncio.Event()
        self._can_send.set()
        # Bumped on every new stream; tags media frames and is echoed in acks so
        # stale frames/acks from a previous stream are dropped after a seek.
        self._gen = 0

    # -- writer -------------------------------------------------------------
    def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(self._writer())

    async def _writer(self) -> None:
        try:
            while True:
                sent = False
                while not self._ctrlq.empty():           # control has priority
                    await self._raw_send(self._ctrlq.get_nowait())
                    sent = True
                if not self._mediaq.empty():
                    await self._raw_send(self._mediaq.get_nowait())
                    sent = True
                if not sent:
                    await self._has_output.wait()
                    self._has_output.clear()
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            pass

    async def _raw_send(self, data: bytes | str) -> None:
        if self.ws.application_state != WebSocketState.CONNECTED:
            raise ConnectionError("websocket closed")
        if isinstance(data, (bytes, bytearray)):
            await self.ws.send_bytes(data)
        else:
            await self.ws.send_text(data)

    def _enqueue_ctrl(self, data: bytes | str) -> None:
        self._ctrlq.put_nowait(data)
        self._has_output.set()

    async def _send_json(self, obj: dict) -> None:
        self._enqueue_ctrl(json.dumps(obj))

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
        # Drop any media still queued from the stream we just stopped.
        while not self._mediaq.empty():
            self._mediaq.get_nowait()

    async def close(self) -> None:
        await self.stop()
        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None

    def set_progress(self, gen: int, recv: int | None, ahead: float) -> None:
        if gen != self._gen:
            return  # stale ack from a previous stream
        # Credit window: client reports cumulative bytes received for this stream.
        if recv is None:
            self._can_send.set()  # client without byte-acks -> don't gate on credit
        else:
            self._acked = recv
            if self._sent - self._acked < self._window:
                self._can_send.set()
            else:
                self._can_send.clear()
        # Soft prefetch cap (archive): don't read more than buffer_high s ahead.
        if ahead <= settings.buffer_low:
            self._resume.set()
        elif ahead >= settings.buffer_high:
            self._resume.clear()

    def _reset_flow(self) -> None:
        """Reset credit + prefetch gates for a freshly started stream."""
        self._sent = 0
        self._acked = 0
        self._can_send.set()
        self._resume.set()

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
        self._reset_flow()
        # streamStart: epoch the client maps currentTime 0 to (within one GOP).
        await self._send_json({
            "type": "init", "mode": "archive", "camera": camera, "gen": self._gen,
            "codecs": codecs, "streamStart": first.start + offset,
        })
        self._archive = streamer.ArchiveProcess(segments, offset)
        await self._archive.start()
        self._task = asyncio.create_task(self._pump_archive())

    async def _pump_archive(self) -> None:
        assert self._archive
        header = MEDIA + struct.pack(">I", self._gen)
        try:
            while True:
                await self._resume.wait()    # soft prefetch cap
                await self._can_send.wait()  # credit window
                chunk = await self._archive.read()
                if not chunk:
                    await self._send_json({"type": "ended"})
                    break
                await self._mediaq.put(header + chunk)
                self._has_output.set()
                self._sent += len(chunk)
                if self._sent - self._acked >= self._window:
                    self._can_send.clear()
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
        self._reset_flow()
        await self._send_json({
            "type": "init", "mode": "live", "camera": camera, "gen": self._gen,
            "codecs": codecs,
        })
        self._live = streamer.LiveProcess(url)
        await self._live.start()
        self._task = asyncio.create_task(self._pump_live())

    async def _pump_live(self) -> None:
        assert self._live
        header = MEDIA + struct.pack(">I", self._gen)
        try:
            while True:
                await self._can_send.wait()  # credit window (bounds live backlog)
                chunk = await self._live.read()
                if not chunk:
                    await self._send_json({"type": "ended"})
                    break
                await self._mediaq.put(header + chunk)
                self._has_output.set()
                self._sent += len(chunk)
                if self._sent - self._acked >= self._window:
                    self._can_send.clear()
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            pass

    # -- scrub --------------------------------------------------------------
    async def scrub(self, camera: str, t: float) -> None:
        seg = db.segment_at(camera, t)
        if seg is None:
            return
        offset = max(0.0, t - seg.start)
        jpeg = await streamer.preview_jpeg(seg, offset)
        if jpeg:
            self._enqueue_ctrl(PREVIEW + struct.pack(">d", t) + jpeg)


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
    session.start_writer()

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
        await session.close()


async def _dispatch(session: Session, msg: dict) -> None:
    kind = msg.get("type")
    if kind == "play":
        await session.play(msg["camera"], float(msg["time"]))
    elif kind == "live":
        await session.live(msg["camera"])
    elif kind == "scrub":
        await session.scrub(msg["camera"], float(msg["time"]))
    elif kind == "ack":
        recv = msg.get("recv")
        session.set_progress(
            int(msg.get("gen", -1)),
            int(recv) if recv is not None else None,
            float(msg.get("aheadSec", 0)),
        )
    elif kind == "bounds":
        b = db.bounds(msg["camera"])
        await session._send_json({
            "type": "bounds", "camera": msg["camera"],
            "start": b[0] if b else None, "end": b[1] if b else None,
        })
    elif kind == "availability":
        frm, to = float(msg["from"]), float(msg["to"])
        await session._send_json({
            "type": "availability", "camera": msg["camera"],
            "from": frm, "to": to,
            "intervals": db.availability(msg["camera"], frm, to),
        })
    elif kind == "stop":
        await session.stop()
