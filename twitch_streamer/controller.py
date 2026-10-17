import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import aiohttp

TOKEN = os.environ["SUPERVISOR_TOKEN"]
CORE_BASE = "http://supervisor/core"
REST_BASE = f"{CORE_BASE}/api"
WS_URL = "ws://supervisor/core/api/websocket"
# CameraEntityFeature.STREAM — camera can serve HLS via HA's stream component.
CAMERA_FEATURE_STREAM = 2
HELIX_BASE = "https://api.twitch.tv/helix"
TWITCH_OAUTH = "https://id.twitch.tv/oauth2/token"
TOKEN_STORE = Path("/data/twitch_tokens.json")

with open("/data/options.json") as f:
    CONFIG = json.load(f)


def opt(key: str, default=None):
    v = CONFIG.get(key, default)
    if isinstance(v, str):
        v = v.strip()
        return v if v else default
    return v


TRIGGER_ENTITY = opt("trigger_entity")
CAMERA_ENTITY = opt("camera_entity", "")
START_STATES = {s.strip() for s in CONFIG["start_states"]}
STOP_STATES = {s.strip() for s in CONFIG["stop_states"]}
RTSP_OVERRIDE = opt("rtsp_override", "")
# Core reached directly over the host network (host_network: true). MJPEG
# can't go through the Supervisor proxy: it buffers the full response body
# before replying, and a multipart MJPEG stream never ends.
CORE_DIRECT_URL = opt("core_url", "http://127.0.0.1:8123").rstrip("/")
INGEST_URL = CONFIG["twitch_ingest_url"].rstrip("/")
TWITCH_KEY = CONFIG["twitch_key"]
VBITRATE = CONFIG["video_bitrate"]
PRESET = CONFIG["preset"]
# Every frame is re-encoded (the encoder is fed decoded camera frames), so
# "copy" can no longer pass the camera bitstream through.
VIDEO_CODEC = opt("video_codec", "libx264")
if VIDEO_CODEC == "copy":
    VIDEO_CODEC = "libx264"
POLL = int(CONFIG.get("poll_seconds", 5))
STOP_CONFIRM_POLLS = max(1, int(CONFIG.get("stop_confirm_polls", 3)))
# Reconnect the camera if no new frame arrives this long (0 disables). The
# Twitch encoder keeps running on the last frame meanwhile, so a camera hiccup
# (HLS worker restart, HA restart, Bambuddy reconnect) never drops the stream.
STALL_RESTART_SECONDS = int(CONFIG.get("stall_restart_seconds", 30))
# Grace before a fresh camera connection must deliver its first frame
# (HLS startup can take a while).
STARTUP_GRACE_SECONDS = max(STALL_RESTART_SECONDS, 60)
# Restart the encoder if it stops accepting frames this long (e.g. a wedged
# RTMP connection). This is the only thing that reconnects to Twitch.
ENCODER_STALL_SECONDS = 30
# The encoder is fed at exactly this rate from the newest camera frame, so a
# slow (5 fps MJPEG) or stalled camera never starves Twitch of video.
OUTPUT_FPS = 30
# Fixed output size: a camera resolution change can't break the encoder.
OUTPUT_W, OUTPUT_H = 1920, 1080
HEARTBEAT_SECONDS = 300
# Deadline for the camera/stream websocket round-trip.
HLS_URL_TIMEOUT = 20

TWITCH_CLIENT_ID = opt("twitch_client_id", "")
TWITCH_CLIENT_SECRET = opt("twitch_client_secret", "")
TWITCH_ACCESS_TOKEN = opt("twitch_access_token", "")
TWITCH_REFRESH_TOKEN = opt("twitch_refresh_token", "")
TWITCH_BROADCASTER_LOGIN = opt("twitch_broadcaster_login", "")

FILE_ENTITY = opt("file_entity", "")
PROGRESS_ENTITY = opt("progress_entity", "")
MATERIAL_ENTITY = opt("material_entity", "")
REMAINING_TIME_ENTITY = opt("remaining_time_entity", "")
CURRENT_LAYER_ENTITY = opt("current_layer_entity", "")
TOTAL_LAYERS_ENTITY = opt("total_layers_entity", "")
PROGRESS_STEP = int(CONFIG.get("progress_step", 10))

streaming: bool = False  # a print session is live on Twitch
encoder_proc: subprocess.Popen | None = None  # frames in -> Twitch out
ingest_proc: subprocess.Popen | None = None  # camera in -> JPEG frames out
ingest_started_ts: float = 0.0
latest_frame: bytes | None = None  # newest camera JPEG
latest_frame_ts: float = 0.0  # monotonic time latest_frame arrived
last_write_ts: float = 0.0  # monotonic time the encoder last took a frame
frames_written: int = 0
heartbeat: tuple[float, int] = (0.0, 0)  # (monotonic time, frames_written)
placeholder_frame: bytes | None = None  # black frame until the camera connects
announce_task: asyncio.Task | None = None
broadcaster_id: str | None = None
last_progress_bucket: int | None = None
print_session_meta: dict = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("twitch-streamer")


def twitch_enabled() -> bool:
    return bool(
        TWITCH_CLIENT_ID
        and TWITCH_ACCESS_TOKEN
        and TWITCH_BROADCASTER_LOGIN
    )


def load_tokens() -> None:
    global TWITCH_ACCESS_TOKEN, TWITCH_REFRESH_TOKEN
    if not TOKEN_STORE.exists():
        return
    try:
        data = json.loads(TOKEN_STORE.read_text())
    except Exception as e:
        log.warning("Failed reading %s: %s", TOKEN_STORE, e)
        return

    stored_access = data.get("access_token") or ""
    stored_refresh = data.get("refresh_token") or ""
    config_access = opt("twitch_access_token", "")
    config_refresh = opt("twitch_refresh_token", "")

    if config_access and config_access != stored_access:
        log.info("Config access_token differs from persisted — using config value, discarding persisted")
        try:
            TOKEN_STORE.unlink()
        except Exception:
            pass
        return

    if stored_access:
        TWITCH_ACCESS_TOKEN = stored_access
    if stored_refresh:
        TWITCH_REFRESH_TOKEN = stored_refresh
    log.info("Loaded persisted Twitch tokens from %s", TOKEN_STORE)


def persist_tokens() -> None:
    try:
        TOKEN_STORE.write_text(json.dumps({
            "access_token": TWITCH_ACCESS_TOKEN,
            "refresh_token": TWITCH_REFRESH_TOKEN,
        }))
    except Exception as e:
        log.warning("Failed writing %s: %s", TOKEN_STORE, e)


async def get_state(session: aiohttp.ClientSession, entity_id: str) -> str | None:
    if not entity_id:
        return None
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with session.get(f"{REST_BASE}/states/{entity_id}", headers=headers) as r:
        if r.status != 200:
            log.debug("state fetch %s -> HTTP %s", entity_id, r.status)
            return None
        data = await r.json()
        return data.get("state")


async def get_state_attrs(session: aiohttp.ClientSession, entity_id: str) -> dict | None:
    if not entity_id:
        return None
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with session.get(f"{REST_BASE}/states/{entity_id}", headers=headers) as r:
        if r.status != 200:
            return None
        return await r.json()


async def get_hls_url(session: aiohttp.ClientSession, entity_id: str) -> str | None:
    """Ask HA's stream component for an HLS playlist URL (camera/stream WS command).

    Works for any camera entity that advertises CameraEntityFeature.STREAM (2),
    including integration cameras that expose no stream_source state attribute.
    """
    try:
        # Hard deadline: a wedged stream worker makes camera/stream never
        # answer, and an unbounded await here freezes the whole poll loop —
        # no stall watchdog, no restart, no logs, stream silently dead.
        async with asyncio.timeout(HLS_URL_TIMEOUT):
            async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                msg = await ws.receive_json()
                if msg.get("type") == "auth_required":
                    await ws.send_json({"type": "auth", "access_token": TOKEN})
                    msg = await ws.receive_json()
                if msg.get("type") != "auth_ok":
                    log.error("HA websocket auth failed: %s", msg)
                    return None
                await ws.send_json({"id": 1, "type": "camera/stream", "entity_id": entity_id})
                while True:
                    msg = await ws.receive_json()
                    if msg.get("id") != 1 or msg.get("type") != "result":
                        continue
                    if not msg.get("success"):
                        log.error("camera/stream failed for %s: %s", entity_id, msg.get("error"))
                        return None
                    return f"{CORE_BASE}{msg['result']['url']}"
    except TimeoutError:
        log.error("camera/stream for %s timed out after %ss", entity_id, HLS_URL_TIMEOUT)
        return None
    except Exception as e:
        log.error("camera/stream websocket error for %s: %s", entity_id, e)
        return None


async def resolve_stream_source(
    session: aiohttp.ClientSession, entity_id: str
) -> tuple[str | None, str | None]:
    """Resolve (url, kind) for ffmpeg. kind is one of rtsp, hls, mjpeg."""
    if RTSP_OVERRIDE:
        return RTSP_OVERRIDE, "rtsp"
    if not entity_id:
        log.error("No rtsp_override and no camera_entity set")
        return None, None
    data = await get_state_attrs(session, entity_id)
    if data is None:
        log.error("camera entity %s not retrievable", entity_id)
        return None, None
    attrs = data.get("attributes", {}) or {}
    for key in ("stream_source", "rtsp_url", "stream_url"):
        if attrs.get(key):
            return attrs[key], "rtsp"
    if int(attrs.get("supported_features") or 0) & CAMERA_FEATURE_STREAM:
        hls = await get_hls_url(session, entity_id)
        if hls:
            return hls, "hls"
        log.warning("camera %s: HLS failed — falling back to MJPEG", entity_id)
    else:
        # Snapshot/MJPEG-only cameras (e.g. Bambuddy) can't serve HLS.
        log.info("camera %s has no stream support — using MJPEG", entity_id)
    # The entity's rotating access_token is only checked when the request
    # starts, so a long-running stream outlives the rotation.
    access_token = attrs.get("access_token")
    if not access_token:
        log.error("camera %s exposes no access_token for MJPEG", entity_id)
        return None, None
    return (f"{CORE_DIRECT_URL}/api/camera_proxy_stream/{entity_id}"
            f"?token={access_token}"), "mjpeg"


def make_placeholder_frame() -> bytes | None:
    """Black JPEG sent until the camera delivers its first frame."""
    try:
        out = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
             "-i", f"color=c=black:s={OUTPUT_W}x{OUTPUT_H}", "-frames:v", "1",
             "-c:v", "mjpeg", "-f", "image2pipe", "pipe:1"],
            capture_output=True, timeout=30, check=True,
        )
        return out.stdout
    except Exception as e:
        log.error("Could not render placeholder frame: %s", e)
        return None


def _kill(proc: subprocess.Popen, name: str) -> None:
    """SIGKILL: neither process has anything worth flushing, and ffmpeg can
    ignore SIGTERM while blocked on a pipe or the endless anullsrc input."""
    if proc.poll() is not None:
        return
    log.info("Stopping %s", name)
    proc.kill()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        log.warning("%s did not exit after SIGKILL", name)


def start_ingest(src: str, kind: str) -> None:
    """Pull the camera and re-emit it as length-prefixed JPEG frames."""
    global ingest_proc, ingest_started_ts
    log.info("Connecting camera (kind=%s): %s", kind, src.split("?", 1)[0])
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-nostdin"]
    if kind == "rtsp":
        cmd += ["-rtsp_transport", "tcp"]
    elif kind == "mjpeg":
        # Direct-to-Core URL authenticates via ?token=, not the bearer header.
        cmd += ["-f", "mpjpeg"]
    elif kind == "hls":
        # HLS path is signed for HA core, but the supervisor proxy hop still
        # needs the bearer token; ffmpeg propagates it to playlist + segments.
        # NB: do NOT add -reconnect* here — it busy-loops the LL-HLS playlist
        # ("parse_playlist error"). The stall watchdog handles recovery.
        cmd += ["-headers", f"Authorization: Bearer {TOKEN}\r\n"]
    cmd += ["-fflags", "+genpts+igndts", "-i", src, "-map", "0:v:0", "-an"]
    if kind == "mjpeg":
        cmd += ["-c:v", "copy"]
    else:
        cmd += ["-c:v", "mjpeg", "-q:v", "3", "-pix_fmt", "yuvj420p",
                "-fps_mode", "passthrough"]
    # mpjpeg writes a Content-length per frame, so frames split exactly.
    cmd += ["-f", "mpjpeg", "pipe:1"]
    ingest_started_ts = time.monotonic()
    ingest_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    threading.Thread(target=_read_frames, args=(ingest_proc,), daemon=True).start()


def _read_frames(proc: subprocess.Popen) -> None:
    """Parse the ingest's multipart output, keeping only the newest frame.

    Runs until EOF; a replaced ingest's leftover frames are discarded.
    """
    global latest_frame, latest_frame_ts
    out = proc.stdout
    if out is None:
        return
    while True:
        line = out.readline()
        if not line:
            return  # ingest exited; the poll loop reconnects
        if not line.startswith(b"--"):
            continue
        length = 0
        while header := out.readline().strip():
            name, _, value = header.partition(b":")
            if name.strip().lower() == b"content-length":
                try:
                    length = int(value)
                except ValueError:
                    length = 0
        if length <= 0:
            continue
        frame = out.read(length)
        if len(frame) < length:
            return
        if proc is ingest_proc:
            latest_frame = frame
            latest_frame_ts = time.monotonic()


def stop_ingest() -> None:
    global ingest_proc
    proc, ingest_proc = ingest_proc, None
    if proc is not None:
        _kill(proc, "camera ingest")


def camera_stalled_seconds() -> float:
    """Seconds the current camera connection has gone without a frame, once
    past its timeout (0 while healthy or still connecting)."""
    if ingest_proc is None:
        return 0.0
    since = max(ingest_started_ts, latest_frame_ts)
    idle = time.monotonic() - since
    got_frame = latest_frame_ts > ingest_started_ts
    grace = STALL_RESTART_SECONDS if got_frame else STARTUP_GRACE_SECONDS
    return idle if idle > grace else 0.0


def start_encoder() -> None:
    """Open the Twitch connection, fed by _pump_frames at OUTPUT_FPS."""
    global encoder_proc, last_write_ts
    target = f"{INGEST_URL}/{TWITCH_KEY}"
    log.info("Starting Twitch encoder (%s %dx%d@%d) -> %s",
             VIDEO_CODEC, OUTPUT_W, OUTPUT_H, OUTPUT_FPS, INGEST_URL)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-nostdin",
        # Frames arrive at exactly OUTPUT_FPS, so frame count == wall time.
        # Short probe: the default 5s analyzeduration delays going live (and
        # every reconnect) and adds that much latency for good.
        "-f", "image2pipe", "-c:v", "mjpeg", "-framerate", str(OUTPUT_FPS),
        "-analyzeduration", "200000", "-thread_queue_size", "64", "-i", "pipe:0",
        "-re", "-f", "lavfi", "-thread_queue_size", "512",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map", "0:v:0", "-map", "1:a:0",
        "-vf", (f"scale={OUTPUT_W}:{OUTPUT_H}:force_original_aspect_ratio=decrease,"
                f"pad={OUTPUT_W}:{OUTPUT_H}:(ow-iw)/2:(oh-ih)/2,format=yuv420p"),
        "-c:v", VIDEO_CODEC, "-preset", PRESET, "-tune", "zerolatency",
        "-b:v", VBITRATE, "-maxrate", VBITRATE, "-bufsize", VBITRATE,
        "-g", str(OUTPUT_FPS * 2), "-keyint_min", str(OUTPUT_FPS * 2),
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-f", "flv", target,
    ]
    last_write_ts = time.monotonic()
    encoder_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    threading.Thread(target=_pump_frames, args=(encoder_proc,), daemon=True).start()


def _pump_frames(proc: subprocess.Popen) -> None:
    """Feed the encoder the newest camera frame at exactly OUTPUT_FPS.

    Repeating the last frame while the camera is slow or reconnecting keeps
    Twitch receiving real-time video, so the stream never stalls or drops.
    """
    global last_write_ts, frames_written
    interval = 1.0 / OUTPUT_FPS
    next_t = time.monotonic()
    while proc is encoder_proc and proc.poll() is None:
        frame = latest_frame or placeholder_frame
        if frame is not None:
            try:
                proc.stdin.write(frame)
                proc.stdin.flush()
            except (BrokenPipeError, OSError, ValueError):
                return  # encoder died; the poll loop restarts it
            last_write_ts = time.monotonic()
            frames_written += 1
        next_t += interval
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        elif delay < -2:
            # Encoder blocked for a while: resync instead of bursting frames.
            log.warning("Encoder fell %.1fs behind real time — skipping ahead", -delay)
            next_t = time.monotonic()


def stop_encoder() -> None:
    global encoder_proc
    proc, encoder_proc = encoder_proc, None  # pump thread exits on its next frame
    if proc is None:
        return
    _kill(proc, "Twitch encoder")
    try:
        proc.stdin.close()
    except (BrokenPipeError, OSError, ValueError):
        pass


def start_stream() -> None:
    global streaming, placeholder_frame, latest_frame, heartbeat
    if placeholder_frame is None:
        placeholder_frame = make_placeholder_frame()
    latest_frame = None  # never open on the previous print's last frame
    heartbeat = (0.0, 0)
    streaming = True
    start_encoder()


def stop_stream() -> None:
    global streaming
    streaming = False
    stop_encoder()
    stop_ingest()


async def keep_stream_alive(session: aiohttp.ClientSession) -> None:
    """Repair whichever half broke.

    Camera trouble only reconnects the ingest; the encoder keeps Twitch fed
    with the last frame meanwhile. The encoder is only restarted if it dies
    or stops accepting frames.
    """
    global heartbeat
    now = time.monotonic()
    if now - heartbeat[0] >= HEARTBEAT_SECONDS:
        if heartbeat[0]:
            fps = (frames_written - heartbeat[1]) / (now - heartbeat[0])
            age = f"{now - latest_frame_ts:.0f}s ago" if latest_frame_ts else "never"
            log.info("Live: %.1f fps to Twitch, last camera frame %s", fps, age)
        heartbeat = (now, frames_written)

    if encoder_proc is None or encoder_proc.poll() is not None:
        if encoder_proc is not None:
            log.warning("Twitch encoder exited (code %s) — reconnecting",
                        encoder_proc.returncode)
            stop_encoder()  # release its stdin pipe
        start_encoder()
    elif time.monotonic() - last_write_ts > ENCODER_STALL_SECONDS:
        log.warning("Twitch encoder stopped accepting frames — reconnecting")
        stop_encoder()
        start_encoder()

    stall = camera_stalled_seconds() if STALL_RESTART_SECONDS else 0.0
    if ingest_proc is not None and ingest_proc.poll() is None and not stall:
        return
    if stall:
        log.warning("No camera frame for %.0fs — reconnecting camera (stream stays up)", stall)
    elif ingest_proc is not None:
        log.warning("Camera connection ended — reconnecting (stream stays up)")
    stop_ingest()
    src, kind = await resolve_stream_source(session, CAMERA_ENTITY)
    if src:
        start_ingest(src, kind)
    else:
        log.warning("No camera source resolved; will retry next poll")


async def refresh_twitch_token(session: aiohttp.ClientSession) -> bool:
    global TWITCH_ACCESS_TOKEN, TWITCH_REFRESH_TOKEN
    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and TWITCH_REFRESH_TOKEN):
        log.error("Cannot refresh Twitch token — client_secret or refresh_token missing")
        return False
    data = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": TWITCH_REFRESH_TOKEN,
    }
    async with session.post(TWITCH_OAUTH, data=data) as r:
        body = await r.json()
        if r.status != 200:
            log.error("Twitch token refresh failed: %s", body)
            return False
        TWITCH_ACCESS_TOKEN = body["access_token"]
        if body.get("refresh_token"):
            TWITCH_REFRESH_TOKEN = body["refresh_token"]
        persist_tokens()
        log.info("Refreshed Twitch access token")
        return True


async def twitch_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: dict | None = None,
    retried: bool = False,
):
    headers = {
        "Client-Id": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {TWITCH_ACCESS_TOKEN}",
    }
    url = f"{HELIX_BASE}{path}"
    async with session.request(method, url, headers=headers, params=params, json=json_body) as r:
        body_text = await r.text()
        try:
            body = json.loads(body_text) if body_text else {}
        except Exception:
            body = {"raw": body_text}
        if r.status == 401 and not retried:
            log.info("Twitch 401 — attempting token refresh")
            if await refresh_twitch_token(session):
                return await twitch_request(session, method, path,
                                            params=params, json_body=json_body, retried=True)
        if r.status >= 400:
            log.error("Twitch %s %s -> HTTP %s %s", method, path, r.status, body)
            return None
        return body


async def resolve_broadcaster_id(session: aiohttp.ClientSession) -> str | None:
    global broadcaster_id
    if broadcaster_id:
        return broadcaster_id
    if not TWITCH_BROADCASTER_LOGIN:
        return None
    body = await twitch_request(session, "GET", "/users",
                                params={"login": TWITCH_BROADCASTER_LOGIN})
    if not body or not body.get("data"):
        log.error("Could not resolve broadcaster_id for %s", TWITCH_BROADCASTER_LOGIN)
        return None
    broadcaster_id = body["data"][0]["id"]
    log.info("Resolved Twitch broadcaster_id=%s for %s", broadcaster_id, TWITCH_BROADCASTER_LOGIN)
    return broadcaster_id


async def set_stream_title(session: aiohttp.ClientSession, title: str) -> None:
    if not twitch_enabled():
        return
    bid = await resolve_broadcaster_id(session)
    if not bid:
        return
    title = title[:140]
    log.info("Setting stream title: %s", title)
    await twitch_request(session, "PATCH", "/channels",
                         params={"broadcaster_id": bid},
                         json_body={"title": title})


async def send_chat(session: aiohttp.ClientSession, message: str) -> None:
    if not twitch_enabled():
        return
    bid = await resolve_broadcaster_id(session)
    if not bid:
        return
    message = message[:500]
    log.info("Chat: %s", message)
    await twitch_request(session, "POST", "/chat/messages",
                         json_body={
                             "broadcaster_id": bid,
                             "sender_id": bid,
                             "message": message,
                         })


def fmt_minutes(mins) -> str:
    try:
        m = int(float(mins))
    except (TypeError, ValueError):
        return "?"
    if m < 60:
        return f"{m}m"
    return f"{m // 60}h{m % 60:02d}m"


def normalize_minutes(raw, unit: str | None) -> float | None:
    """Convert a remaining-time value to minutes based on its HA unit."""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    u = (unit or "").strip().lower()
    if u in ("h", "hr", "hrs", "hour", "hours"):
        return v * 60
    if u in ("s", "sec", "secs", "second", "seconds"):
        return v / 60
    return v  # minutes or unitless


def clean_filename(raw) -> str:
    """Strip directory path and known print extensions from a gcode filename."""
    if not raw:
        return "print"
    name = raw.replace("\\", "/").rsplit("/", 1)[-1]
    for ext in (".gcode.3mf", ".gcode", ".3mf"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break
    return name or "print"


async def gather_print_meta(session: aiohttp.ClientSession) -> dict:
    keys_entities = {
        "file": FILE_ENTITY,
        "progress": PROGRESS_ENTITY,
        "material": MATERIAL_ENTITY,
        "layer": CURRENT_LAYER_ENTITY,
        "total_layers": TOTAL_LAYERS_ENTITY,
    }
    out: dict = {}
    for k, ent in keys_entities.items():
        if ent:
            out[k] = await get_state(session, ent)
    return out


async def get_remaining_minutes(session: aiohttp.ClientSession) -> float | None:
    """Read remaining-time entity and normalize to minutes via its unit attr."""
    if not REMAINING_TIME_ENTITY:
        return None
    data = await get_state_attrs(session, REMAINING_TIME_ENTITY)
    if not data:
        return None
    unit = (data.get("attributes", {}) or {}).get("unit_of_measurement")
    return normalize_minutes(data.get("state"), unit)


PLACEHOLDER_VALUES = {"", "unknown", "unavailable", "none", "empty"}


async def wait_for_value(session: aiohttp.ClientSession, entity_id: str,
                         attempts: int = 6, delay: float = 2.0) -> str | None:
    """Poll an entity until it reports a real value (not Empty/Unknown/etc).

    At print start the printer briefly reports placeholders (e.g. active_tray
    'Empty' during filament change) before the real values settle.
    """
    if not entity_id:
        return None
    val = None
    for _ in range(attempts):
        val = await get_state(session, entity_id)
        if val and val.strip().lower() not in PLACEHOLDER_VALUES:
            return val
        await asyncio.sleep(delay)
    return val


async def wait_for_remaining(session: aiohttp.ClientSession,
                             attempts: int = 12, delay: float = 5.0) -> float | None:
    if not REMAINING_TIME_ENTITY:
        return None
    mins = None
    for _ in range(attempts):
        mins = await get_remaining_minutes(session)
        if mins is not None and mins > 0:
            return mins
        await asyncio.sleep(delay)
    return mins


async def announce_start(session: aiohttp.ClientSession) -> None:
    global last_progress_bucket, print_session_meta
    last_progress_bucket = None
    file_name = clean_filename(await wait_for_value(session, FILE_ENTITY))
    remaining = await wait_for_remaining(session)
    material = await wait_for_value(session, MATERIAL_ENTITY) or "?"
    eta = fmt_minutes(remaining)
    print_session_meta = {"file": file_name, "material": material}
    await set_stream_title(session, f"🖨️ {file_name}")
    await send_chat(session, f"🟢 Starting: {file_name} • Material: {material} • ETA {eta}")


async def announce_finish(session: aiohttp.ClientSession, final_state: str | None) -> None:
    file_name = print_session_meta.get("file") or "print"
    kind = (final_state or "").lower()  # ha-bambulab: "finish", Bambuddy: "FINISH"
    if kind == "finish":
        await send_chat(session, f"✅ {file_name} complete")
    elif kind == "failed":
        await send_chat(session, f"❌ {file_name} failed")
    elif kind == "pause":
        await send_chat(session, f"⏸️ {file_name} paused")
    else:
        await send_chat(session, f"⏹️ {file_name} stopped ({final_state})")


async def maybe_report_progress(session: aiohttp.ClientSession) -> None:
    global last_progress_bucket
    if not PROGRESS_ENTITY:
        return
    raw = await get_state(session, PROGRESS_ENTITY)
    try:
        pct = int(float(raw))
    except (TypeError, ValueError):
        return
    bucket = pct // PROGRESS_STEP
    if last_progress_bucket is None:
        last_progress_bucket = bucket
        return
    if bucket > last_progress_bucket and pct > 0 and pct < 100:
        last_progress_bucket = bucket
        meta = await gather_print_meta(session)
        layer = meta.get("layer") or "?"
        total = meta.get("total_layers") or "?"
        eta = fmt_minutes(await get_remaining_minutes(session))
        await send_chat(session, f"🟦 {pct}% • Layer {layer}/{total} • {eta} left")


async def announce_start_safe(session: aiohttp.ClientSession) -> None:
    # Runs as a background task: metadata can take a minute to settle, and the
    # poll loop must keep repairing the stream meanwhile.
    try:
        await announce_start(session)
    except Exception as e:
        log.exception("start announcement failed: %s", e)


def validate_config() -> None:
    if not TWITCH_KEY:
        log.error("twitch_key not set in add-on configuration")
        sys.exit(1)
    if not TRIGGER_ENTITY:
        log.error("trigger_entity not set in add-on configuration")
        sys.exit(1)
    if not CAMERA_ENTITY and not RTSP_OVERRIDE:
        log.error("camera_entity or rtsp_override required")
        sys.exit(1)


async def main_loop() -> None:
    log.info("Watching %s — start=%s stop=%s",
             TRIGGER_ENTITY, sorted(START_STATES), sorted(STOP_STATES))
    if twitch_enabled():
        log.info("Twitch API enabled for broadcaster=%s", TWITCH_BROADCASTER_LOGIN)
    else:
        log.info("Twitch API disabled — title/chat updates skipped")
    global announce_task
    async with aiohttp.ClientSession() as session:
        initial = await get_state(session, TRIGGER_ENTITY)
        log.info("Startup state check: %s = %s", TRIGGER_ENTITY, initial)
        if initial in START_STATES:
            log.info("Print already active at startup — opening Twitch session")
        last_state: str | None = initial
        pending_stop = 0

        while True:
            try:
                try:
                    state = await get_state(session, TRIGGER_ENTITY)
                except aiohttp.ClientError as e:
                    # HA restarting: hold the last known state, keep streaming.
                    log.warning("Could not read %s (%s) — holding state", TRIGGER_ENTITY, e)
                    state = last_state
                if state != last_state:
                    log.info("%s: %s -> %s", TRIGGER_ENTITY, last_state, state)

                if state in START_STATES and not streaming:
                    log.info("Print active — going live")
                    start_stream()
                    announce_task = asyncio.create_task(announce_start_safe(session))

                if streaming and state in STOP_STATES:
                    # Debounce: tolerate transient stop states (printer/MQTT
                    # glitches) before tearing down a long stream.
                    pending_stop += 1
                    if pending_stop >= STOP_CONFIRM_POLLS:
                        log.info("Stop state '%s' confirmed (%d polls) — ending stream",
                                 state, pending_stop)
                        stop_stream()
                        await announce_finish(session, state)
                        pending_stop = 0
                    else:
                        log.info("Stop state '%s' (%d/%d) — holding stream",
                                 state, pending_stop, STOP_CONFIRM_POLLS)
                else:
                    # Anything that isn't a confirmed stop (paused, unavailable,
                    # HA restarting) keeps the stream up.
                    pending_stop = 0

                if streaming:
                    await keep_stream_alive(session)
                    if state in START_STATES:
                        await maybe_report_progress(session)

                last_state = state
            except Exception as e:
                log.exception("poll loop error: %s", e)
            await asyncio.sleep(POLL)


def install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    def shutdown() -> None:
        log.info("Shutdown signal received")
        stop_stream()
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)


async def main() -> None:
    validate_config()
    load_tokens()
    install_signal_handlers(asyncio.get_running_loop())
    try:
        await main_loop()
    except asyncio.CancelledError:
        pass
    finally:
        stop_stream()


if __name__ == "__main__":
    asyncio.run(main())
