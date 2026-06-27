"""Authentication against Frigate's own user table + lightweight session tokens.

Frigate (0.17) stores credentials in the ``user`` table of ``frigate.db`` as
``pbkdf2_sha256$<iterations>$<salt>$<base64(pbkdf2_hmac_sha256)>`` — exactly the
format produced by ``frigate/api/auth.py:hash_password``. We re-implement the
verification with the stdlib so the same login works here, read-only.

Session tokens are our own HMAC-signed blobs (not Frigate JWTs) so we never need
Frigate's signing secret.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import time

from .config import settings

PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"


def _open_db() -> sqlite3.Connection:
    # Read-only; immutable=0 so we still see rows Frigate appends in WAL mode.
    uri = f"file:{settings.frigate_db}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt, b64_hash = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != PASSWORD_HASH_ALGORITHM:
        return False
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), int(iterations)
    )
    computed = base64.b64encode(dk).decode("ascii")
    return hmac.compare_digest(computed, b64_hash)


def authenticate(username: str, password: str) -> str | None:
    """Return the user's role on success, else ``None``."""
    if not username or not password:
        return None
    with _open_db() as conn:
        row = conn.execute(
            "SELECT password_hash, role FROM user WHERE username = ?", (username,)
        ).fetchone()
    if row is None:
        return None
    if not _verify_password(password, row["password_hash"]):
        return None
    return row["role"] or "viewer"


# --- session tokens ---------------------------------------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + pad)


def issue_token(username: str, role: str) -> str:
    payload = {
        "sub": username,
        "role": role,
        "exp": int(time.time()) + settings.token_ttl,
    }
    body = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = hmac.new(settings.secret_key.encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64url(sig)}"


def verify_token(token: str) -> dict | None:
    try:
        body, sig = token.split(".", 1)
    except ValueError:
        return None
    expected = hmac.new(
        settings.secret_key.encode(), body.encode(), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(_b64url(expected), sig):
        return None
    try:
        payload = json.loads(_b64url_decode(body))
    except (ValueError, json.JSONDecodeError):
        return None
    if payload.get("exp", 0) < time.time():
        return None
    return payload
