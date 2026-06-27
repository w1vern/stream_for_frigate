"use strict";

// ---- DOM ------------------------------------------------------------------
const $ = (id) => document.getElementById(id);
const loginEl = $("login");
const appEl = $("app");
const video = $("video");
const previewImg = $("preview");
const badge = $("badge");
const statusEl = $("status");
const clockEl = $("clock");
const camerasEl = $("cameras");
const timelineEl = $("timeline");
const playheadEl = $("playhead");

// ---- State ----------------------------------------------------------------
let ws = null;
let token = null;
let cameras = [];
let camera = null;
let mode = "live"; // "live" | "archive"

let mediaSource = null;
let sourceBuffer = null;
const appendQueue = [];
let streamStart = 0; // epoch of currentTime 0 (archive)
let streamGen = 0;   // server stream generation; echoed in acks to drop stale ones
let archiveBounds = null; // {start, end}
let scrubbing = false;
// Scrub previews are paced one-in-flight (request/response) instead of on a
// fixed timer, so the rate adapts to RTT and never builds a backlog that would
// delay the seek on release (which is what made scrubbing unusable over the
// relay). `scrubPending` holds the latest position not yet requested.
let scrubInFlight = false;
let scrubPending = null;
let scrubTimer = 0;

// ---- Login ----------------------------------------------------------------
$("login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const username = $("username").value;
  const password = $("password").value;
  $("login-error").textContent = "";
  try {
    const res = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    if (!res.ok) {
      $("login-error").textContent = "Неверный логин или пароль";
      return;
    }
    const data = await res.json();
    token = data.token;
    cameras = data.cameras || [];
    startApp();
  } catch {
    $("login-error").textContent = "Ошибка соединения";
  }
});

function startApp() {
  loginEl.classList.add("hidden");
  appEl.classList.remove("hidden");
  buildCameraButtons();
  camera = cameras[0] || null;
  connect();
}

function buildCameraButtons() {
  camerasEl.innerHTML = "";
  cameras.forEach((c) => {
    const btn = document.createElement("button");
    btn.textContent = c;
    btn.dataset.cam = c;
    btn.addEventListener("click", () => selectCamera(c));
    camerasEl.appendChild(btn);
  });
}

function markActiveCamera() {
  [...camerasEl.children].forEach((b) =>
    b.classList.toggle("active", b.dataset.cam === camera)
  );
}

// ---- WebSocket ------------------------------------------------------------
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.binaryType = "arraybuffer";
  ws.onopen = () => ws.send(JSON.stringify({ type: "auth", token }));
  ws.onmessage = onMessage;
  ws.onclose = () => setStatus("отключено");
  ws.onerror = () => setStatus("ошибка WS");
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function onMessage(ev) {
  if (typeof ev.data === "string") return onControl(JSON.parse(ev.data));
  onBinary(new Uint8Array(ev.data));
}

function onControl(msg) {
  switch (msg.type) {
    case "ready":
      cameras = msg.cameras || cameras;
      buildCameraButtons();
      if (!camera) camera = cameras[0];
      markActiveCamera();
      requestBounds();
      startLive(); // default view
      break;
    case "init":
      setupMediaSource(msg);
      break;
    case "bounds":
      if (msg.camera === camera && msg.start != null) {
        archiveBounds = { start: msg.start, end: msg.end };
        renderRange();
      }
      break;
    case "ended":
      setStatus("поток завершён");
      break;
    case "error":
      setStatus("⚠ " + msg.message);
      break;
  }
}

function onBinary(bytes) {
  const type = bytes[0];
  const payload = bytes.subarray(1);
  if (type === 0x01) {
    enqueueMedia(payload);
  } else if (type === 0x02) {
    // 8-byte float64 (epoch) + JPEG
    const t = new DataView(bytes.buffer, bytes.byteOffset + 1, 8).getFloat64(0, false);
    showPreview(t, payload.subarray(8));
  }
}

// ---- MSE ------------------------------------------------------------------
function pickCodec(codecs) {
  for (const c of codecs) {
    const mime = `video/mp4; codecs="${c}"`;
    if (window.MediaSource && MediaSource.isTypeSupported(mime)) return mime;
  }
  return null;
}

function setupMediaSource(msg) {
  // Tear down any previous pipeline.
  appendQueue.length = 0;
  sourceBuffer = null;
  if (mediaSource && mediaSource.readyState === "open") {
    try { mediaSource.endOfStream(); } catch {}
  }
  const mime = pickCodec(msg.codecs || []);
  if (!mime) {
    setStatus("⚠ браузер не поддерживает кодек (HEVC/H.264)");
    return;
  }
  mode = msg.mode;
  streamStart = msg.streamStart || 0;
  streamGen = msg.gen || 0;
  badge.classList.toggle("hidden", mode !== "live");

  mediaSource = new MediaSource();
  video.src = URL.createObjectURL(mediaSource);
  mediaSource.addEventListener("sourceopen", () => {
    URL.revokeObjectURL(video.src);
    try {
      sourceBuffer = mediaSource.addSourceBuffer(mime);
      sourceBuffer.mode = "segments";
      sourceBuffer.addEventListener("updateend", onUpdateEnd);
      drainQueue();
    } catch (e) {
      setStatus("⚠ ошибка SourceBuffer: " + e.message);
    }
  }, { once: true });

  setStatus(mode === "live" ? `LIVE · ${camera}` : `Архив · ${camera}`);
}

function enqueueMedia(payload) {
  // Copy out of the WS frame buffer (subarray shares memory).
  appendQueue.push(payload.slice());
  drainQueue();
}

// True only while `sourceBuffer` is still attached to an open MediaSource.
// After a stream switch (or a media error) the old SourceBuffer is detached and
// touching .updating/.buffered throws — guard every access through this.
function sbReady() {
  return (
    sourceBuffer &&
    mediaSource &&
    mediaSource.readyState === "open" &&
    Array.prototype.indexOf.call(mediaSource.sourceBuffers, sourceBuffer) !== -1
  );
}

function onUpdateEnd() {
  drainQueue();
  ensurePlaying();
}

function drainQueue() {
  if (!sbReady() || sourceBuffer.updating || appendQueue.length === 0) return;
  const chunk = appendQueue[0];
  try {
    sourceBuffer.appendBuffer(chunk);
    appendQueue.shift();
  } catch (e) {
    if (e.name === "QuotaExceededError") {
      // Drop the oldest buffered video; eviction fires updateend -> retry.
      evictBuffer();
    } else {
      appendQueue.shift();
      setStatus("⚠ append: " + e.message);
    }
  }
}

// Start/keep playback going. play() must happen AFTER media is appended (calling
// it earlier, before the SourceBuffer has data, just rejects and never retries —
// the classic "one frame then frozen" symptom). Also nudge currentTime back into
// the buffered range if the playhead ever falls outside it.
function ensurePlaying() {
  let b;
  try { b = video.buffered; } catch { return; }
  if (!b || !b.length) return;
  // Only nudge forward if the playhead is *behind* the buffer (startup, or after
  // eviction trimmed the front). An underrun at the buffer's end is left alone —
  // playback resumes by itself once more media is appended.
  if (video.currentTime < b.start(0)) {
    try { video.currentTime = b.start(0); } catch {}
  }
  if (video.paused) video.play().catch(() => {});
}

function evictBuffer() {
  if (!sbReady() || sourceBuffer.updating) return;
  const buffered = sourceBuffer.buffered;
  if (buffered.length && video.currentTime - buffered.start(0) > 30) {
    try { sourceBuffer.remove(buffered.start(0), video.currentTime - 20); } catch {}
  }
}

// ---- Playback controls ----------------------------------------------------
function selectCamera(c) {
  camera = c;
  markActiveCamera();
  requestBounds();
  if (mode === "live") startLive();
  else startArchive(currentEpoch() || (archiveBounds && archiveBounds.end - 5));
}

function startLive() {
  mode = "live";
  setModeButtons();
  // Playback is kicked off in ensurePlaying() once the first media is appended.
  send({ type: "live", camera });
}

function startArchive(t) {
  mode = "archive";
  setModeButtons();
  if (!archiveBounds) return;
  const target = clamp(t || archiveBounds.end - 30, archiveBounds.start, archiveBounds.end - 1);
  send({ type: "play", camera, time: target });
}

function setModeButtons() {
  $("mode-live").classList.toggle("active", mode === "live");
  $("mode-archive").classList.toggle("active", mode === "archive");
  badge.classList.toggle("hidden", mode !== "live");
}

$("mode-live").addEventListener("click", startLive);
$("mode-archive").addEventListener("click", () =>
  startArchive(archiveBounds ? archiveBounds.end - 30 : 0)
);

function requestBounds() {
  send({ type: "bounds", camera });
}

// ---- Timeline + scrubbing -------------------------------------------------
function currentEpoch() {
  if (mode !== "archive" || !streamStart) return null;
  return streamStart + video.currentTime;
}

function timeToX(t) {
  if (!archiveBounds) return 0;
  const { start, end } = archiveBounds;
  return ((t - start) / (end - start)) * timelineEl.clientWidth;
}

function xToTime(x) {
  if (!archiveBounds) return 0;
  const { start, end } = archiveBounds;
  const frac = clamp(x / timelineEl.clientWidth, 0, 1);
  return start + frac * (end - start);
}

function pointerTime(ev) {
  const rect = timelineEl.getBoundingClientRect();
  return xToTime(ev.clientX - rect.left);
}

timelineEl.addEventListener("pointerdown", (ev) => {
  if (!archiveBounds) return;
  scrubbing = true;
  timelineEl.setPointerCapture(ev.pointerId);
  previewImg.classList.remove("hidden");
  onScrubMove(ev);
});

timelineEl.addEventListener("pointermove", (ev) => {
  if (scrubbing) onScrubMove(ev);
});

timelineEl.addEventListener("pointerup", (ev) => {
  if (!scrubbing) return;
  scrubbing = false;
  scrubPending = null;
  clearTimeout(scrubTimer);
  previewImg.classList.add("hidden");
  const t = pointerTime(ev);
  startArchive(t); // resume continuous stream from release point
});

function onScrubMove(ev) {
  const t = pointerTime(ev);
  playheadEl.style.left = `${timeToX(t)}px`;
  clockEl.textContent = fmtClock(t);
  scrubPending = t;
  pumpScrub();
}

// Send the latest pending scrub position iff none is outstanding. A safety
// timer clears the in-flight flag if a preview is ever dropped, so scrubbing
// can't get wedged.
function pumpScrub() {
  if (!scrubbing || scrubInFlight || scrubPending == null) return;
  const t = scrubPending;
  scrubPending = null;
  scrubInFlight = true;
  send({ type: "scrub", camera, time: t });
  clearTimeout(scrubTimer);
  scrubTimer = setTimeout(() => { scrubInFlight = false; pumpScrub(); }, 700);
}

function showPreview(t, jpegBytes) {
  scrubInFlight = false;
  clearTimeout(scrubTimer);
  if (scrubbing) {
    const blob = new Blob([jpegBytes], { type: "image/jpeg" });
    const url = URL.createObjectURL(blob);
    previewImg.onload = () => URL.revokeObjectURL(url);
    previewImg.src = url;
  }
  pumpScrub(); // fire the next pending position, if any
}

// ---- Periodic UI + flow control ------------------------------------------
setInterval(() => {
  drainQueue();    // recover if a prior append stalled (e.g. after eviction)
  ensurePlaying(); // recover if playback stalled / was never (re)started
  if (mode === "archive" && !scrubbing) {
    const t = currentEpoch();
    if (t != null) {
      playheadEl.style.left = `${timeToX(t)}px`;
      clockEl.textContent = fmtClock(t);
    }
    // Flow control: tell the server how far we are buffered ahead. Send EVERY
    // tick (even with an empty buffer -> aheadSec 0) so a freshly seeked stream
    // always gets a resume signal; tag it with the stream gen so the server can
    // drop acks left over from the previous stream. Read the element's buffered
    // (always safe) rather than the SourceBuffer's (throws once detached).
    const ahead = video.buffered.length
      ? Math.max(0, video.buffered.end(video.buffered.length - 1) - video.currentTime)
      : 0;
    send({ type: "ack", gen: streamGen, aheadSec: ahead });
  } else if (mode === "live") {
    clockEl.textContent = fmtClock(Date.now() / 1000);
  }
}, 500);

function renderRange() {
  if (!archiveBounds) return;
  $("range-start").textContent = fmtClock(archiveBounds.start);
  $("range-end").textContent = fmtClock(archiveBounds.end);
}

// ---- Helpers --------------------------------------------------------------
function setStatus(s) { statusEl.textContent = s; }
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function fmtClock(epoch) {
  // epoch is UTC seconds; Date renders in the viewer's local timezone.
  const d = new Date(epoch * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
