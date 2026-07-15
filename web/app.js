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
const debugEl = $("debug");

// Bump this by hand on meaningful frontend changes. Shown in the corner so a
// stale browser cache is obvious at a glance. (Asset cache-busting is automatic
// via the server's content-hash; this is just the human-readable marker.)
const APP_VERSION = "0.3.1";
$("version").textContent = "v" + APP_VERSION;

// Wall-clock timezone the archive UI renders in. Recordings are stored as UTC
// epochs; the server (running on the cottage PC) reports the offset that is
// cottage-local time, so day boundaries and the clock reflect what actually
// happened at the cottage regardless of the viewer's own timezone. Set from the
// login response / ws `ready`; 0 until then. See tzWall()/dayStartOf().
let tzOffsetSec = 0;

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

// Day-based archive timeline: the bar always spans exactly one local day, so a
// fixed pixel step is always a fixed time jump. `dayStart`/`dayEnd` are the epoch
// bounds of the selected day; `availIntervals` are the recorded spans within it.
const DAY = 86400;
let dayStart = 0;
let dayEnd = 0;
let availIntervals = [];
let availKey = "";   // camera|dayStart of the in-flight availability request
// Credit-window flow control: count media bytes received for the current stream
// and ack them back so the server keeps only a bounded amount in flight. Reset on
// every new stream (init). `lastAckBytes` throttles how often we ack.
let recvBytes = 0;
let lastAckBytes = 0;
const ACK_EVERY = 32 * 1024;

// Playback management. LIVE trades a little latency for staying near the live
// edge: if we drift too far behind (a stall accumulated lag) we skip forward;
// small drift is absorbed by a gentle speed-up. ARCHIVE never skips real frames —
// it just waits on a stall. Both jump across true holes (no data exists there).
const LIVE_TARGET_LATENCY = 2.5; // seconds behind the live edge we aim to sit at
const LIVE_MAX_LATENCY = 6.0;    // beyond this, hard-skip to the edge
let recovering = false;          // re-initialising after a decode error
let lastErrorAt = 0;
let userPaused = false;          // archive: user hit pause; don't auto-resume
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
    if (data.tzOffsetMinutes != null) tzOffsetSec = data.tzOffsetMinutes * 60;
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
  // A decode failure fires 'error' on the element; rebuild instead of freezing.
  video.addEventListener("error", () => recoverFromError());
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
      if (msg.tzOffsetMinutes != null) tzOffsetSec = msg.tzOffsetMinutes * 60;
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
        updateDayNav();
      }
      break;
    case "availability":
      if (msg.camera === camera && `${msg.camera}|${dayStart}` === availKey &&
          msg.from === dayStart) {
        availIntervals = msg.intervals || [];
        renderAvailability();
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
    // [0x01][gen uint32][fMP4]. Drop media tagged with an old generation: after
    // a seek, chunks of the previous stream are still in flight (especially over
    // the relay) and would otherwise be appended to the new buffer, snapping
    // playback to wherever the old stream was.
    const gen = new DataView(bytes.buffer, bytes.byteOffset + 1, 4).getUint32(0, false);
    if (gen === streamGen) {
      const media = payload.subarray(4);
      recvBytes += media.length;
      enqueueMedia(media);
      // Ack received bytes promptly so the server's credit window stays open and
      // throughput isn't throttled by the ack interval.
      if (recvBytes - lastAckBytes >= ACK_EVERY) sendAck();
    }
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
  recvBytes = 0;
  lastAckBytes = 0;
  userPaused = false;
  updatePlayPauseBtn();
  try { video.playbackRate = 1; } catch {}
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
  managePlayback();
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
    } else if (video.error) {
      // The media element decoder has failed; every further append throws. Don't
      // limp on dropping chunks (that was the permanent-freeze symptom) — rebuild.
      recoverFromError();
    } else {
      appendQueue.shift();
      setStatus("⚠ append: " + e.message);
    }
  }
}

function inBuffered(b, t) {
  for (let i = 0; i < b.length; i++) {
    if (t >= b.start(i) - 0.05 && t < b.end(i)) return true;
  }
  return false;
}

function nextRangeStart(b, t) {
  for (let i = 0; i < b.length; i++) {
    if (b.start(i) > t) return b.start(i);
  }
  return null;
}

// The playback manager: keep video playing, mode-appropriately. Runs both on
// `updateend` (right after new media lands) and on the periodic tick.
//   - currentTime behind the buffer (startup / front eviction): jump in.
//   - playhead in a true hole (no data exists there): skip to next data. This is
//     not dropping frames — there are none here (a recording gap, or a live drop).
//   - LIVE only: hold ~LIVE_TARGET_LATENCY behind the edge — hard-skip if too far
//     back, gently speed up on mild drift. ARCHIVE plays strictly 1x and waits.
function managePlayback() {
  if (recovering || scrubbing || !sbReady()) return;
  if (video.error) { recoverFromError(); return; }
  let b;
  try { b = video.buffered; } catch { return; }
  if (!b.length) return;
  const end = b.end(b.length - 1);

  if (video.currentTime < b.start(0)) {
    try { video.currentTime = b.start(0); } catch {}
  } else if (!inBuffered(b, video.currentTime)) {
    const nxt = nextRangeStart(b, video.currentTime);
    if (nxt != null) { try { video.currentTime = nxt; } catch {} }
  }

  if (mode === "live") {
    const latency = end - video.currentTime;
    if (latency > LIVE_MAX_LATENCY) {
      try { video.currentTime = end - LIVE_TARGET_LATENCY; } catch {}
      video.playbackRate = 1;
    } else if (latency > LIVE_TARGET_LATENCY + 1.0) {
      video.playbackRate = 1.05; // catch up smoothly, no visible jump
    } else if (video.playbackRate !== 1) {
      video.playbackRate = 1;
    }
  }

  if (!userPaused && video.paused) video.play().catch(() => {});
}

// Decode error -> rebuild the pipeline and resume: live jumps back to the edge,
// archive resumes from where it died. A short backoff avoids a tight crash loop.
function recoverFromError() {
  if (recovering) return;
  recovering = true;
  const now = Date.now();
  const wait = now - lastErrorAt < 3000 ? 1500 : 300;
  lastErrorAt = now;
  setStatus("⟳ восстановление…");
  const resumeEpoch = mode === "archive" ? currentEpoch() : null;
  setTimeout(() => {
    recovering = false;
    if (mode === "live") startLive();
    else if (resumeEpoch != null) startArchive(resumeEpoch);
  }, wait);
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
  else {
    startArchive(currentEpoch() || (archiveBounds && archiveBounds.end - 5));
    requestAvailability(); // same day, new camera -> refresh the shading
  }
}

function startLive() {
  mode = "live";
  setModeButtons();
  // Playback is kicked off in managePlayback() once the first media is appended.
  send({ type: "live", camera });
}

function startArchive(t) {
  mode = "archive";
  setModeButtons();
  if (!archiveBounds) return;
  const target = clamp(t || archiveBounds.end - 30, archiveBounds.start, archiveBounds.end - 1);
  setDay(target); // bring the timeline to the day we're about to play
  send({ type: "play", camera, time: target });
}

function setModeButtons() {
  $("mode-live").classList.toggle("active", mode === "live");
  $("mode-archive").classList.toggle("active", mode === "archive");
  badge.classList.toggle("hidden", mode !== "live");
  appEl.classList.toggle("live", mode === "live"); // hides archive chrome via CSS
}

$("mode-live").addEventListener("click", startLive);
$("mode-archive").addEventListener("click", () =>
  startArchive(archiveBounds ? archiveBounds.end - 30 : 0)
);

// ---- Transport controls (archive) + fullscreen ---------------------------
function updatePlayPauseBtn() {
  const btn = $("play-pause");
  if (btn) btn.textContent = userPaused ? "▶" : "⏸";
}

function togglePlay() {
  userPaused = !userPaused;
  if (userPaused) video.pause();
  else video.play().catch(() => {});
  updatePlayPauseBtn();
}

// Jump by `delta` seconds. Stays local (instant) if the target is already
// buffered; otherwise re-requests the stream from the server at that time.
function seekRelative(delta) {
  if (mode !== "archive" || !archiveBounds) return;
  const cur = currentEpoch();
  if (cur == null) return;
  const target = clamp(cur + delta, archiveBounds.start, archiveBounds.end - 1);
  const localT = target - streamStart;
  let buffered = false;
  try {
    const b = video.buffered;
    for (let i = 0; i < b.length; i++) {
      if (localT >= b.start(i) && localT < b.end(i)) { buffered = true; break; }
    }
  } catch {}
  if (buffered) {
    userPaused = false;
    updatePlayPauseBtn();
    video.currentTime = localT;
    video.play().catch(() => {});
  } else {
    startArchive(target);
  }
}

$("play-pause").addEventListener("click", togglePlay);
// One handler for every interval button; the jump size lives in data-seek.
document.querySelectorAll(".seek").forEach((b) =>
  b.addEventListener("click", () => seekRelative(Number(b.dataset.seek)))
);

// Keyboard transport (archive): ←/→ jump 10 s, Shift+←/→ jump 1 min, Space
// toggles play. Ignored while typing in a field (the timelapse form).
addEventListener("keydown", (e) => {
  const el = document.activeElement;
  if (el && (el.tagName === "INPUT" || el.tagName === "SELECT" || el.tagName === "TEXTAREA")) return;
  if (!$("tl-modal").classList.contains("hidden")) return; // modal open
  if (mode !== "archive") return;
  if (e.key === "ArrowLeft") { seekRelative(e.shiftKey ? -60 : -10); e.preventDefault(); }
  else if (e.key === "ArrowRight") { seekRelative(e.shiftKey ? 60 : 10); e.preventDefault(); }
  else if (e.key === " ") { togglePlay(); e.preventDefault(); }
});

function toggleFullscreen() {
  const el = appEl;
  if (!document.fullscreenElement) {
    (el.requestFullscreen || el.webkitRequestFullscreen || (() => {})).call(el);
  } else {
    document.exitFullscreen();
  }
}
$("fs-btn").addEventListener("click", toggleFullscreen);

function requestBounds() {
  send({ type: "bounds", camera });
}

// ---- Timeline (one day) + scrubbing --------------------------------------
function currentEpoch() {
  if (mode !== "archive" || !streamStart) return null;
  return streamStart + video.currentTime;
}

// A Date shifted into cottage-local time; read it via its getUTC* accessors to
// get cottage wall-clock fields regardless of the viewer's own timezone.
function tzWall(epoch) {
  return new Date((epoch + tzOffsetSec) * 1000);
}

// Epoch of cottage-local midnight for the day containing `epoch`. The offset is
// fixed (cottage TZ has no DST), so day math is exact arithmetic.
function dayStartOf(epoch) {
  const shifted = epoch + tzOffsetSec;
  return Math.floor(shifted / DAY) * DAY - tzOffsetSec;
}

function addDays(dayStartEpoch, n) {
  return dayStartEpoch + n * DAY;
}

function maxDay() {
  // Latest navigable day: today, or the last day with data if that's later.
  const last = archiveBounds ? Math.max(archiveBounds.end, Date.now() / 1000) : Date.now() / 1000;
  return dayStartOf(last);
}

const daySpan = () => Math.max(1, dayEnd - dayStart); // ~86400 (handles odd days)

// Switch the visible day (any epoch within it). Clamps to [first data day, today].
function setDay(epoch) {
  let ds = dayStartOf(epoch);
  if (archiveBounds) ds = clamp(ds, dayStartOf(archiveBounds.start), maxDay());
  if (ds === dayStart) return;
  dayStart = ds;
  dayEnd = addDays(ds, 1);
  availIntervals = [];
  renderAvailability();
  updateDayNav();
  requestAvailability();
}

function requestAvailability() {
  if (!camera || !dayStart) return;
  availKey = `${camera}|${dayStart}`;
  send({ type: "availability", camera, from: dayStart, to: dayEnd });
}

function updateDayNav() {
  $("day-label").textContent = dayStart ? fmtDate(dayStart) : "—";
  if (archiveBounds) {
    $("day-prev").disabled = dayStart <= dayStartOf(archiveBounds.start);
    $("day-next").disabled = dayStart >= maxDay();
  }
}

function renderAvailability() {
  const host = $("avail");
  host.innerHTML = "";
  if (!dayStart) return;
  const w = timelineEl.clientWidth || 1;
  for (const [s, e] of availIntervals) {
    const x0 = clamp((s - dayStart) / daySpan(), 0, 1) * w;
    const x1 = clamp((e - dayStart) / daySpan(), 0, 1) * w;
    const seg = document.createElement("div");
    seg.className = "seg";
    seg.style.left = `${x0}px`;
    seg.style.width = `${Math.max(1, x1 - x0)}px`;
    host.appendChild(seg);
  }
  renderFuture();
}

function renderFuture() {
  const fut = $("future");
  const now = Date.now() / 1000;
  if (now <= dayStart) { // whole day still in the future
    fut.style.left = "0";
    fut.classList.remove("hidden");
  } else if (now < dayEnd) {
    fut.style.left = `${((now - dayStart) / daySpan()) * (timelineEl.clientWidth || 1)}px`;
    fut.classList.remove("hidden");
  } else {
    fut.classList.add("hidden");
  }
}

// Nearest recorded time to `t` within the day, so a click on an empty span still
// plays something sensible. null if the day has no recording at all.
function snapToAvailable(t) {
  if (!availIntervals.length) return null;
  for (const [s, e] of availIntervals) if (t >= s && t < e) return t;
  let best = null, bestDist = Infinity;
  for (const [s, e] of availIntervals) {
    for (const edge of [s, e - 1]) {
      const d = Math.abs(edge - t);
      if (d < bestDist) { bestDist = d; best = edge; }
    }
  }
  return best;
}

function timeToX(t) {
  return ((t - dayStart) / daySpan()) * timelineEl.clientWidth;
}

function xToTime(x) {
  const frac = clamp(x / timelineEl.clientWidth, 0, 1);
  return dayStart + frac * daySpan();
}

function pointerTime(ev) {
  const rect = timelineEl.getBoundingClientRect();
  return xToTime(ev.clientX - rect.left);
}

timelineEl.addEventListener("pointerdown", (ev) => {
  if (!dayStart) return;
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
  const t = snapToAvailable(pointerTime(ev));
  if (t != null) startArchive(t); // resume continuous stream from release point
  else setStatus("нет записи в этот момент");
});

// Day arrows jump PLAYBACK to the start of the adjacent day (the server snaps to
// that day's first recording). They used to only move the timeline's viewed day,
// which the 250 ms follow-tick immediately snapped back to the still-playing day
// — so day navigation looked dead. Seeking playback makes the follow-tick agree.
function goDay(delta) {
  if (!dayStart || !archiveBounds) return;
  const target = addDays(dayStart, delta);
  if (target < dayStartOf(archiveBounds.start) || target > maxDay()) return;
  startArchive(target); // seeks to day start; server lands on first recording
}
$("day-prev").addEventListener("click", () => goDay(-1));
$("day-next").addEventListener("click", () => goDay(1));
addEventListener("resize", renderAvailability);

function onScrubMove(ev) {
  const t = pointerTime(ev);
  playheadEl.classList.remove("hidden");
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

// Tell the server our progress: cumulative bytes received (credit window) and
// seconds buffered ahead (soft prefetch cap, archive). Tagged with the stream gen
// so the server drops acks left over from a previous stream. Sent both on receipt
// (to keep the window open) and on a heartbeat (so a freshly seeked or stalled
// stream still gets credit even when no media is arriving).
function sendAck() {
  if (!ws || ws.readyState !== WebSocket.OPEN || !streamGen) return;
  let ahead = 0;
  try {
    if (video.buffered.length) {
      ahead = Math.max(0, video.buffered.end(video.buffered.length - 1) - video.currentTime);
    }
  } catch {}
  send({ type: "ack", gen: streamGen, recv: recvBytes, aheadSec: ahead });
  lastAckBytes = recvBytes;
}

// Compact debug readout (mode/gen, buffer ahead, playback rate, throughput).
function updateDebug() {
  if (!sbReady()) { debugEl.textContent = ""; return; }
  let ahead = 0, ranges = 0;
  try {
    const b = video.buffered;
    ranges = b.length;
    if (b.length) ahead = b.end(b.length - 1) - video.currentTime;
  } catch {}
  const label = mode === "live" ? `lag≈${ahead.toFixed(1)}s` : `buf ${ahead.toFixed(1)}s`;
  debugEl.textContent =
    `${mode} · gen ${streamGen} · ${label}\n` +
    `ranges ${ranges} · ${video.playbackRate.toFixed(2)}x · ${(recvBytes / 1048576).toFixed(1)}MB`;
}

// ---- Periodic UI + flow control ------------------------------------------
// 250 ms so live-edge management reacts quickly without being jittery.
setInterval(() => {
  drainQueue();      // recover if a prior append stalled (e.g. after eviction)
  managePlayback();  // keep playing; live-edge / gap / rate management
  if (mode === "archive" && !scrubbing) {
    const t = currentEpoch();
    if (t != null) {
      if (t < dayStart || t >= dayEnd) setDay(t); // follow playback across midnight
      playheadEl.classList.remove("hidden");
      playheadEl.style.left = `${timeToX(t)}px`;
      clockEl.textContent = fmtClock(t);
      renderFuture(); // keep the "now" marker advancing
    }
  } else if (mode === "live") {
    clockEl.textContent = fmtClock(Date.now() / 1000);
  }
  updateDebug();
  if (!scrubbing) sendAck(); // heartbeat (both modes)
}, 250);

// ---- Timelapse ("ускоренный обзор") ---------------------------------------
// A datetime-local field shows/edits COTTAGE wall-clock time (to match the rest
// of the UI), so convert against tzOffsetSec rather than the browser's own zone.
function epochToInput(epoch) {
  const d = tzWall(epoch);
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}` +
         `T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}

function inputToEpoch(str) {
  const m = (str || "").match(/^(\d+)-(\d+)-(\d+)T(\d+):(\d+)(?::(\d+))?$/);
  if (!m) return null;
  const n = m.map(Number);
  const utc = Date.UTC(n[1], n[2] - 1, n[3], n[4], n[5], n[6] || 0) / 1000;
  return utc - tzOffsetSec; // wall time was cottage-local
}

function tlEndDefault() {
  const now = Math.floor(Date.now() / 1000);
  if (mode === "archive") return currentEpoch() || now;
  return archiveBounds ? Math.min(now, archiveBounds.end) : now;
}

function openTimelapse() {
  const end = tlEndDefault();
  $("tl-end").value = epochToInput(end);
  $("tl-start").value = epochToInput(end - 12 * 3600);
  resetTlPlayer();
  $("tl-status").textContent = "";
  $("tl-modal").classList.remove("hidden");
}

function tlSetRange(hours) {
  const end = tlEndDefault();
  $("tl-end").value = epochToInput(end);
  $("tl-start").value = epochToInput(end - hours * 3600);
}

function tlUrl(dl) {
  const start = inputToEpoch($("tl-start").value);
  const end = inputToEpoch($("tl-end").value);
  if (start == null || end == null || end <= start) return null;
  const p = new URLSearchParams({
    token, camera, start: String(start), end: String(end),
    speed: $("tl-speed").value,
    mode: $("tl-smooth").checked ? "smooth" : "keyframe",
  });
  if (dl) p.set("dl", "1");
  return "/api/timelapse?" + p.toString();
}

function resetTlPlayer() {
  const v = $("tl-video");
  try { v.pause(); } catch {}
  if (v.src) {
    try { URL.revokeObjectURL(v.src); } catch {}
    v.removeAttribute("src");
    v.load();
  }
  $("tl-modal").querySelector(".tl-player").classList.add("hidden");
}

function closeTimelapse() {
  resetTlPlayer();
  $("tl-modal").classList.add("hidden");
}

// View = the same one-shot bulk GET as download, buffered fully to a Blob then
// played. No interactive HTTP streaming (which stalled over the relay — the whole
// reason live/archive use the WebSocket); the file is small by design.
async function tlView() {
  const url = tlUrl(false);
  if (!url) { $("tl-status").textContent = "Проверьте диапазон (конец должен быть позже начала)"; return; }
  $("tl-status").textContent = "Готовим обзор…";
  $("tl-view").disabled = $("tl-download").disabled = true;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      let m = res.status;
      try { m = (await res.json()).error || m; } catch {}
      $("tl-status").textContent = "Ошибка: " + m;
      return;
    }
    const blob = await res.blob();
    const v = $("tl-video");
    if (v.src) { try { URL.revokeObjectURL(v.src); } catch {} }
    v.src = URL.createObjectURL(blob);
    $("tl-modal").querySelector(".tl-player").classList.remove("hidden");
    $("tl-status").textContent = `Готово · ${(blob.size / 1048576).toFixed(1)} МБ`;
    v.play().catch(() => {});
  } catch {
    $("tl-status").textContent = "Ошибка загрузки";
  } finally {
    $("tl-view").disabled = $("tl-download").disabled = false;
  }
}

function tlDownload() {
  const url = tlUrl(true);
  if (!url) { $("tl-status").textContent = "Проверьте диапазон (конец должен быть позже начала)"; return; }
  const a = document.createElement("a");
  a.href = url;
  a.download = "";
  document.body.appendChild(a);
  a.click();
  a.remove();
  $("tl-status").textContent = "Скачивание запущено…";
}

$("tl-open").addEventListener("click", openTimelapse);
$("tl-close").addEventListener("click", closeTimelapse);
$("tl-view").addEventListener("click", tlView);
$("tl-download").addEventListener("click", tlDownload);
$("tl-modal").addEventListener("click", (e) => { if (e.target === $("tl-modal")) closeTimelapse(); });
document.querySelectorAll(".tl-q").forEach((b) =>
  b.addEventListener("click", () => tlSetRange(Number(b.dataset.hours)))
);

// ---- Helpers --------------------------------------------------------------
function setStatus(s) { statusEl.textContent = s; }
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function fmtClock(epoch) {
  // Rendered in cottage-local time (tzWall + UTC accessors), not the viewer's.
  const d = tzWall(epoch);
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}

function fmtDate(epoch) {
  const d = tzWall(epoch);
  const p = (n) => String(n).padStart(2, "0");
  const wd = ["вс", "пн", "вт", "ср", "чт", "пт", "сб"][d.getUTCDay()];
  return `${wd} ${p(d.getUTCDate())}.${p(d.getUTCMonth() + 1)}.${d.getUTCFullYear()}`;
}
