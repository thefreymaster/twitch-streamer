import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path

import aiohttp

TOKEN = os.environ["SUPERVISOR_TOKEN"]
CORE_BASE = "http://supervisor/core"
REST_BASE = f"{CORE_BASE}/api"
WS_URL = "ws://supervisor/core/api/websocket"
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
INGEST_URL = CONFIG["twitch_ingest_url"].rstrip("/")
TWITCH_KEY = CONFIG["twitch_key"]
VBITRATE = CONFIG["video_bitrate"]
PRESET = CONFIG["preset"]
VIDEO_CODEC = opt("video_codec", "copy")
POLL = int(CONFIG.get("poll_seconds", 5))
STOP_CONFIRM_POLLS = max(1, int(CONFIG.get("stop_confirm_polls", 3)))

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

ffmpeg_proc: subprocess.Popen | None = None
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
    hls = await get_hls_url(session, entity_id)
    if hls:
        return hls, "hls"
    log.warning("camera %s: no stream URL attr and HLS failed — using MJPEG proxy", entity_id)
    return f"{REST_BASE}/camera_proxy_stream/{entity_id}", "mjpeg"


def is_running() -> bool:
    return ffmpeg_proc is not None and ffmpeg_proc.poll() is None


def start_ffmpeg(src: str, kind: str = "rtsp") -> None:
    global ffmpeg_proc
    if is_running():
        return
    target = f"{INGEST_URL}/{TWITCH_KEY}"
    log.info("Starting ffmpeg (video_codec=%s kind=%s): %s -> %s", VIDEO_CODEC, kind, src, INGEST_URL)
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "info", "-stats"]
    if kind == "rtsp":
        cmd += ["-rtsp_transport", "tcp"]
    elif kind == "mjpeg":
        cmd += ["-headers", f"Authorization: Bearer {TOKEN}\r\n", "-f", "mpjpeg"]
    elif kind == "hls":
        # HLS path is signed for HA core, but the supervisor proxy hop still
        # needs the bearer token; ffmpeg propagates it to playlist + segments.
        cmd += ["-headers", f"Authorization: Bearer {TOKEN}\r\n"]
    cmd += ["-thread_queue_size", "1024", "-fflags", "+genpts+igndts"]
    if kind in ("rtsp", "mjpeg"):
        cmd += ["-use_wallclock_as_timestamps", "1"]
    cmd += [
        "-i", src,
        "-f", "lavfi", "-thread_queue_size", "512", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map", "0:v:0", "-map", "1:a:0",
    ]
    if VIDEO_CODEC == "copy":
        cmd += ["-c:v", "copy"]
        if kind == "rtsp":
            cmd += ["-bsf:v", "h264_mp4toannexb"]
    else:
        cmd += [
            "-vf", "scale='min(1920,iw)':'-2'",
            "-c:v", VIDEO_CODEC, "-preset", PRESET, "-tune", "zerolatency",
            "-b:v", VBITRATE, "-maxrate", VBITRATE, "-bufsize", VBITRATE,
            "-pix_fmt", "yuv420p", "-g", "60", "-keyint_min", "60",
        ]
    cmd += [
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-fps_mode", "cfr", "-r", "30",
        "-f", "flv", target,
    ]
    ffmpeg_proc = subprocess.Popen(cmd)


def stop_ffmpeg() -> None:
    global ffmpeg_proc
    if not is_running():
        ffmpeg_proc = None
        return
    log.info("Stopping ffmpeg")
    ffmpeg_proc.terminate()
    try:
        ffmpeg_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log.warning("ffmpeg did not exit on SIGTERM, sending SIGKILL")
        ffmpeg_proc.kill()
        ffmpeg_proc.wait(timeout=5)
    ffmpeg_proc = None


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
    if final_state == "finish":
        await send_chat(session, f"✅ {file_name} complete")
    elif final_state == "failed":
        await send_chat(session, f"❌ {file_name} failed")
    elif final_state == "pause":
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


async def evaluate_state(session: aiohttp.ClientSession, state: str | None,
                         prev_state: str | None) -> None:
    entering_start = state in START_STATES and (prev_state not in START_STATES)
    entering_stop = state in STOP_STATES and (prev_state not in STOP_STATES)

    if state in START_STATES and not is_running():
        src, kind = await resolve_stream_source(session, CAMERA_ENTITY)
        if src:
            start_ffmpeg(src, kind)
            if entering_start or prev_state is None:
                await announce_start(session)
        else:
            log.warning("No stream source resolved; will retry next poll")
    elif state in STOP_STATES and is_running():
        stop_ffmpeg()
        if entering_stop:
            await announce_finish(session, state)


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
    async with aiohttp.ClientSession() as session:
        initial = await get_state(session, TRIGGER_ENTITY)
        log.info("Startup state check: %s = %s", TRIGGER_ENTITY, initial)
        if initial in START_STATES:
            log.info("Print already active at startup — opening Twitch session")
        await evaluate_state(session, initial, None)
        last_state: str | None = initial
        pending_stop = 0

        while True:
            try:
                state = await get_state(session, TRIGGER_ENTITY)
                changed = state != last_state
                if changed:
                    log.info("%s: %s -> %s", TRIGGER_ENTITY, last_state, state)

                if state in START_STATES:
                    pending_stop = 0
                    if changed or not is_running():
                        if not changed:
                            log.warning("ffmpeg not running during active print — restarting stream")
                        await evaluate_state(session, state, last_state)
                    else:
                        await maybe_report_progress(session)
                elif state in STOP_STATES and is_running():
                    # Debounce: tolerate transient stop states (printer/MQTT
                    # glitches) before tearing down a long stream.
                    pending_stop += 1
                    if pending_stop >= STOP_CONFIRM_POLLS:
                        log.info("Stop state '%s' confirmed (%d polls) — stopping",
                                 state, pending_stop)
                        stop_ffmpeg()
                        await announce_finish(session, state)
                        pending_stop = 0
                    else:
                        log.info("Stop state '%s' (%d/%d) — holding stream",
                                 state, pending_stop, STOP_CONFIRM_POLLS)
                else:
                    # Not a recognized start/stop state (e.g. unavailable) —
                    # keep streaming and wait it out.
                    pending_stop = 0

                last_state = state
            except Exception as e:
                log.exception("poll loop error: %s", e)
            await asyncio.sleep(POLL)


def install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    def shutdown() -> None:
        log.info("Shutdown signal received")
        stop_ffmpeg()
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
        stop_ffmpeg()


if __name__ == "__main__":
    asyncio.run(main())
