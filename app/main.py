"""FastAPI entrypoint: login, static frontend, and the single media WebSocket."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Response, WebSocket
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import ws as ws_module
from .auth import authenticate, issue_token
from .db import list_cameras

WEB_DIR = Path(__file__).resolve().parent.parent / "web"

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
    return JSONResponse({"token": token, "role": role, "cameras": list_cameras()})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await ws_module.handle(websocket)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


app.mount("/", StaticFiles(directory=WEB_DIR), name="static")


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
