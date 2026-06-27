# Stream for Frigate

Low-latency web player for **live** and **archive** footage from a Frigate NVR,
built on the Hik-Connect principle: **one persistent WebSocket per client**
carries both commands and the media stream, instead of the many short HTTP
round-trips Frigate's own UI makes (HLS segments, preview clips). Every
round-trip is expensive over the cottage's frp/traefik relay, so collapsing them
into a single streamed session is the whole point.

The frontend is **bare MSE** (no player library): the backend remuxes H.264 into
fragmented MP4 with `ffmpeg -c copy` (no transcoding) and the browser appends the
fragments to a `SourceBuffer`.

## How it works

```
browser  ──WebSocket──►  FastAPI (app/)  ──►  ffmpeg -c copy  ──►  fMP4 ──► back over WS
   MSE  ◄── fMP4 bytes ──┘     │
                              ├─ recordings index ── frigate.db (read-only)
                              ├─ archive: concat of consecutive segments from time T
                              ├─ live:    RTSP (main stream) straight from the camera
                              └─ scrub:   single keyframe → JPEG preview
```

- **Auth** (`app/auth.py`): same credentials as Frigate. We verify the
  `pbkdf2_sha256$...` hashes from Frigate's `user` table (read-only) and issue our
  own short HMAC session token. Frigate's signing secret is never needed.
- **Archive** (`app/db.py`, `app/streamer.py`): the segment timeline comes
  entirely from the `recordings` table (`path`, `start_time`, `end_time`) — the
  filesystem is never scanned. "Play from T" finds the segment covering T, then a
  single `ffmpeg` concat run streams consecutive segments as one continuous fMP4.
  Flow control: the client reports how far it is buffered ahead and the server
  pauses reading ffmpeg above the high watermark (natural pipe backpressure).
- **Scrubbing**: while dragging the timeline, the client requests keyframe
  previews; the server decodes one frame near each position into a small JPEG.
  On release it switches back to the continuous stream from that point.
- **Live**: `ffmpeg` pulls the camera's main H.264 RTSP stream directly (go2rtc
  and Frigate are not in the live path) and remuxes to fMP4 into the same socket.

All times in `frigate.db` and in paths are **UTC**; the UI renders them in the
viewer's local timezone.

## WebSocket protocol

Client authenticates first: `{"type":"auth","token":"..."}`. Then commands:
`play` (`camera`,`time`), `live` (`camera`), `scrub` (`camera`,`time`),
`ack` (`aheadSec`), `bounds` (`camera`), `stop`.

Server → client text is JSON (`ready`, `init`, `bounds`, `ended`, `error`).
Server → client **binary** frames are tagged by a 1-byte prefix:
`0x01` = fMP4 media bytes; `0x02` = 8-byte float64 epoch + JPEG (scrub preview).

## Deploy (on the cottage PC, alongside Frigate)

The combined `compose.yml` (a drop-in replacement for Frigate's own compose: the
original Frigate services **unchanged** plus the `streamer` service) lives in the
Frigate folder; this repo is cloned next to it as a subfolder. Expected layout:

```
frigate/                 # the existing Frigate folder
  compose.yml            # combined Frigate + streamer compose
  config/                # Frigate config + frigate.db (existing)
  stream_for_frigate/     # ← this repo (git pull updates it)
  .env                   # secrets (created in step 2)
```

1. In the Frigate folder, clone/pull this repo as `stream_for_frigate/`, and make
   sure `compose.yml` there is the combined one (with the `streamer` service).
2. `cp stream_for_frigate/.env.example .env` (in the Frigate folder, next to
   `compose.yml`) and fill in `SECRET_KEY` and the camera RTSP URLs.
3. `docker compose up -d --build`
4. Open `http://<host>:5000`, log in with your Frigate username/password.

> The `streamer` build context is the `stream_for_frigate/` subfolder. If you
> rename the repo/folder, update `build:` in `compose.yml` to match.

To expose it externally, point your existing frp/traefik relay at port `5000`
(WebSocket upgrade must be allowed). Example `frpc` entry:

```toml
[[proxies]]
name = "streamer"
type = "tcp"        # or http with WS upgrade through traefik
localIP = "127.0.0.1"
localPort = 5000
remotePort = 5050
```

> The `streamer` container mounts Frigate's config dir writable **only** so
> SQLite can attach the WAL shared-memory index; the app opens the DB strictly
> read-only (`mode=ro` + `PRAGMA query_only`) and never writes. Recordings are
> mounted read-only.

## Local development

```
uv run uvicorn app.main:app --reload --port 5000
```
Set `FRIGATE_DB`, `MEDIA_ROOT`/`DB_MEDIA_PREFIX` and `CAMERAS` to point at a
local copy of the DB and recordings. `ffmpeg`/`ffprobe` must be on `PATH`.

## Notes / scope

- Both cameras currently record **H.264** → pure remux, plays in any MSE browser.
  If a camera is switched back to **H.265/HEVC**, MSE playback works only in
  Chrome/Edge with hardware HEVC (and Safari), not Firefox — the codec is probed
  per stream and offered to the client, which falls back via `isTypeSupported`.
- No transcoding, no second copy of the archive, and Frigate's detection /
  notifications are untouched — this is a read-only viewer beside Frigate.
- Audio is dropped (`-an`); the recordings are video-only anyway.
