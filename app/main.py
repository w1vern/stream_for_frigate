"""FastAPI entrypoint: login, static frontend, and the single media WebSocket."""

from __future__ import annotations

import hashlib
from pathlib import Path

from fastapi import FastAPI, Query, Request, Response, WebSocket
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import db

from . import streamer
from . import ws as ws_module
from .auth import authenticate, issue_token, verify_token
from .config import settings, tz_offset_minutes
from .db import list_cameras

WEB_DIR = Path(__file__).resolve().parent.parent / "web"

# Content-hash of the frontend assets, used both as a cache-busting query (so a
# redeploy forces browsers to fetch the new files without a manual cache clear)
# and as the human-visible version label. Computed once at startup — the files
# don't change inside the running container.
def _asset_version() -> str:
    h = hashlib.sha1()
    for name in ("app.js", "style.css", "index.html"):
        try:
            h.update((WEB_DIR / name).read_bytes())
        except FileNotFoundError:
            pass
    return h.hexdigest()[:8]


ASSET_VER = _asset_version()
# index.html carries `__VER__` placeholders (asset query strings + a global the
# client reads for its version label); substitute them once.
_INDEX_HTML = (WEB_DIR / "index.html").read_text(encoding="utf-8").replace("__VER__", ASSET_VER)
_LONG_CACHE = {"Cache-Control": "public, max-age=31536000, immutable"}

app = FastAPI(title="Stream for Frigate")


class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/api/login")
def login(body: LoginBody) -> JSONResponse:
    role = authenticate(body.username, body.password)
    if role is None:
        return JSONResponse({"error": "invalid credentials"}, status_code=401)
    token = issue_token(body.username, role)
    return JSONResponse({
        "token": token, "role": role, "cameras": list_cameras(),
        "tzOffsetMinutes": tz_offset_minutes(),
    })


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await ws_module.handle(websocket)


@app.get("/api/timelapse")
async def timelapse(
    token: str = Query(...),
    camera: str = Query(...),
    start: float = Query(...),
    end: float = Query(...),
    speed: float = Query(60.0),
    mode: str = Query("keyframe"),
    dl: int = Query(0),
) -> Response:
    """Sped-up overview of ``[start, end]`` as a small fragmented MP4.

    Deliberately a plain one-shot bulk GET (not the media WebSocket): the output
    is a single small, already-compressed artifact with no interactive seeking,
    so the WS flow-control machinery buys nothing here. High relay latency only
    adds one RTT of startup — it can't stall the transfer the way interactive
    HTTP streaming did (which is why the live/archive path uses the WS). Auth via
    the same session token, passed in the query so a bare ``<a download>`` /
    ``fetch`` works without custom headers."""
    if verify_token(token) is None:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if end <= start:
        return JSONResponse({"error": "empty range"}, status_code=400)
    max_span = settings.timelapse_max_hours * 3600
    if end - start > max_span:
        start = end - max_span  # clamp to the most recent max_span of the range
    segments = db.segments_between(camera, start, end)
    if not segments:
        return JSONResponse({"error": "no recording in range"}, status_code=404)

    offset = max(0.0, start - segments[0].start)
    proc = streamer.TimelapseProcess(
        segments, offset, end - start, speed, smooth=(mode == "smooth"),
    )
    await proc.start()

    async def body():
        try:
            while True:
                chunk = await proc.read()
                if not chunk:
                    break
                yield chunk
        finally:
            await proc.stop()

    headers = {"Cache-Control": "no-store"}
    if dl:
        fname = f"timelapse_{camera}_{int(start)}-{int(end)}_{int(speed)}x.mp4"
        headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    return StreamingResponse(body(), media_type="video/mp4", headers=headers)


_INDEX_ETAG = f'"{ASSET_VER}"'
# Cache the HTML for instant repeat loads, but keep it self-invalidating:
#  - max-age=0 + ETag: the browser may reuse the cached copy but revalidates;
#    when unchanged the server returns a tiny 304 (no body) -> ~1 RTT, no re-parse.
#  - stale-while-revalidate: serve the cached copy INSTANTLY and refresh in the
#    background, so a redeploy is picked up on the next load without ever blocking
#    first paint. Assets are content-hashed + immutable, so a new HTML pulls new
#    JS/CSS automatically. Net: opening the site is as fast as the cache allows
#    yet a deploy still propagates on its own.
_INDEX_CACHE = "max-age=0, stale-while-revalidate=604800"


@app.get("/")
def index(request: Request) -> Response:
    if request.headers.get("if-none-match") == _INDEX_ETAG:
        return Response(status_code=304, headers={"ETag": _INDEX_ETAG, "Cache-Control": _INDEX_CACHE})
    return HTMLResponse(_INDEX_HTML, headers={"ETag": _INDEX_ETAG, "Cache-Control": _INDEX_CACHE})


@app.get("/app.js")
def app_js() -> FileResponse:
    return FileResponse(WEB_DIR / "app.js", media_type="text/javascript", headers=_LONG_CACHE)


@app.get("/style.css")
def style_css() -> FileResponse:
    return FileResponse(WEB_DIR / "style.css", media_type="text/css", headers=_LONG_CACHE)


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


if __name__ == "__main__":
    import uvicorn

    # ws_ping_interval=None disables the websockets library's built-in keepalive
    # ping. That ping calls drain() on the socket on its own ~20s timer, racing
    # our media writes -> websockets' _drain_helper asserts and kills the
    # connection (every stream went dead ~20s in). We don't need protocol pings:
    # media (live) and acks (archive) keep the socket live, and TCP/relay
    # timeouts handle truly dead peers.
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=5000,
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )
