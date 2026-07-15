"""Runtime configuration, read from environment variables.

All paths default to the in-container layout used by ``compose.yml`` so that the
``recordings.path`` values stored in ``frigate.db`` (``/media/frigate/...``) are
usable as-is without rewriting.
"""

from __future__ import annotations

import json
import os
import secrets
import time
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    # Path to a (read-only) copy/mount of Frigate's sqlite database.
    frigate_db: str
    # Filesystem prefix under which recordings live in *this* process. The DB
    # stores paths as ``/media/frigate/...``; if your mount differs, set
    # MEDIA_ROOT and DB_MEDIA_PREFIX so paths get rewritten accordingly.
    media_root: str
    db_media_prefix: str
    # name -> RTSP URL for the live (main) stream of each camera.
    cameras: dict[str, str]
    # HMAC secret for signing our own session tokens (independent of Frigate).
    secret_key: str
    # Session token lifetime, seconds.
    token_ttl: int
    ffmpeg: str
    ffprobe: str
    # Credit-based flow control: the server never has more than this many media
    # bytes outstanding (sent but not yet acked as received by the client). This
    # bounds the data in flight across the *whole* path — frp buffers, the WAN
    # socket, the kernel — none of which we otherwise control. On a seek/switch
    # only this much stale data has to drain before the new stream is visible, so
    # it caps seek-to-first-frame latency (~WINDOW / link_rate). It must stay >=
    # the bandwidth-delay product to keep throughput up: 256 KB sustains ~13 Mbps
    # at 150 ms RTT, well above any single camera, while draining in ~1.7s @ 1.2
    # Mbps. Applies to live and archive alike.
    flow_window_bytes: int
    # Soft prefetch cap (archive only): stop reading ahead once the client is this
    # far (seconds) ahead of its playhead, so we don't buffer the whole archive
    # into client RAM; resume below the low watermark. Over a thin link the client
    # can't get this far ahead, so this never engages and feeding stays continuous.
    buffer_high: float
    buffer_low: float
    # Wall-clock timezone the UI renders archive times in. The server runs on the
    # cottage PC, so its own local UTC offset *is* cottage time — that's the
    # default. Set DISPLAY_TZ_OFFSET (minutes east of UTC, e.g. 180 for MSK) to
    # override when the container's clock isn't on cottage-local time. ``None``
    # means "use the process's local offset". See ``tz_offset_minutes``.
    display_tz_offset: int | None
    # Ceiling on a single timelapse request's source span (hours), so one export
    # can't pin ffmpeg on the whole archive.
    timelapse_max_hours: float
    # Assumed keyframe spacing (seconds) of the recordings. Used to turn a
    # requested speed-up into an output frame rate for the cheap keyframe-only
    # timelapse (out_fps = speed / this). Frigate GOP is ~4 s. See streamer.
    timelapse_keyframe_interval: float

    def remap_path(self, db_path: str) -> str:
        if self.db_media_prefix and db_path.startswith(self.db_media_prefix):
            return self.media_root + db_path[len(self.db_media_prefix):]
        return db_path


def _load_cameras() -> dict[str, str]:
    raw = os.environ.get("CAMERAS", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - config error
        raise SystemExit(f"CAMERAS is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit("CAMERAS must be a JSON object {name: rtsp_url}")
    return {str(k): str(v) for k, v in data.items()}


def load_settings() -> Settings:
    secret = os.environ.get("SECRET_KEY", "").strip()
    if not secret:
        # Ephemeral secret: tokens are invalidated on restart. Set SECRET_KEY
        # in production so sessions survive a redeploy.
        secret = secrets.token_hex(32)
    return Settings(
        frigate_db=_env("FRIGATE_DB", "/frigate.db"),
        media_root=_env("MEDIA_ROOT", "/media/frigate"),
        db_media_prefix=_env("DB_MEDIA_PREFIX", "/media/frigate"),
        cameras=_load_cameras(),
        secret_key=secret,
        token_ttl=int(_env("TOKEN_TTL", "86400")),
        ffmpeg=_env("FFMPEG", "ffmpeg"),
        ffprobe=_env("FFPROBE", "ffprobe"),
        flow_window_bytes=int(_env("FLOW_WINDOW_BYTES", str(256 * 1024))),
        buffer_high=float(_env("BUFFER_HIGH", "20")),
        buffer_low=float(_env("BUFFER_LOW", "10")),
        display_tz_offset=(
            int(os.environ["DISPLAY_TZ_OFFSET"])
            if os.environ.get("DISPLAY_TZ_OFFSET", "").strip()
            else None
        ),
        timelapse_max_hours=float(_env("TIMELAPSE_MAX_HOURS", "48")),
        timelapse_keyframe_interval=float(_env("TIMELAPSE_KEYFRAME_INTERVAL", "4")),
    )


settings = load_settings()


def tz_offset_minutes() -> int:
    """Minutes east of UTC that the UI should render archive wall-clock times in.

    Configured override wins; otherwise the process's current local offset (which,
    on the cottage PC, is cottage time). Resolved per-call so a DST change is
    picked up without a restart (the cottage TZ has no DST, but this stays correct
    if the deployment ever moves)."""
    if settings.display_tz_offset is not None:
        return settings.display_tz_offset
    off = time.localtime().tm_gmtoff
    return off // 60 if off is not None else 0
