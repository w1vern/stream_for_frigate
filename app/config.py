"""Runtime configuration, read from environment variables.

All paths default to the in-container layout used by ``compose.yml`` so that the
``recordings.path`` values stored in ``frigate.db`` (``/media/frigate/...``) are
usable as-is without rewriting.
"""

from __future__ import annotations

import json
import os
import secrets
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
    # Pace archive output to this multiple of real-time (ffmpeg -readrate). Sending
    # as fast as possible saturates a thin relay link and starves the upstream
    # command channel, so seeks get ignored. ~1.5x builds a small buffer while
    # leaving headroom; seek-to-first-frame is unaffected (the -ss seek is instant).
    archive_readrate: float
    # Flow-control: pause archive feeding when the client is buffered this far
    # (seconds) ahead of its playhead; resume below the low watermark.
    buffer_high: float
    buffer_low: float

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
        archive_readrate=float(_env("ARCHIVE_READRATE", "1.5")),
        buffer_high=float(_env("BUFFER_HIGH", "20")),
        buffer_low=float(_env("BUFFER_LOW", "10")),
    )


settings = load_settings()
