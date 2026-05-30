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
REST_BASE = "http://supervisor/core/api"
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


async def resolve_stream_source(session: aiohttp.ClientSession, entity_id: str) -> str | None:
    if RTSP_OVERRIDE:
        return RTSP_OVERRIDE
    if not entity_id:
        log.error("No rtsp_override and no camera_entity set")
        return None
    data = await get_state_attrs(session, entity_id)
    if not data:
        log.error("camera entity %s not retrievable", entity_id)
        return None
    attrs = data.get("attributes", {}) or {}
    for key in ("stream_source", "rtsp_url", "stream_url"):
        if attrs.get(key):
            return attrs[key]
    log.error(
        "Camera entity %s exposes no stream_source attribute. "
        "Set rtsp_override to the direct RTSP URL.",
        entity_id,
    )
    return None


def is_running() -> bool:
    return ffmpeg_proc is not None and ffmpeg_proc.poll() is None


def start_ffmpeg(src: str) -> None:
    global ffmpeg_proc
    if is_running():
        return
    target = f"{INGEST_URL}/{TWITCH_KEY}"
    log.info("Starting ffmpeg (video_codec=%s): %s -> %s", VIDEO_CODEC, src, INGEST_URL)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "info", "-stats",
        "-fflags", "+genpts+igndts",
        "-rtsp_transport", "tcp", "-thread_queue_size", "1024",
        "-use_wallclock_as_timestamps", "1",
        "-i", src,
        "-f", "lavfi", "-thread_queue_size", "512", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map", "0:v:0", "-map", "1:a:0",
    ]
    if VIDEO_CODEC == "copy":
        cmd += [
            "-c:v", "copy",
            "-bsf:v", "h264_mp4toannexb",
        ]
    else:
        cmd += [
            "-vf", "scale='min(1920,iw)':'-2'",
            "-c:v", VIDEO_CODEC, "-preset", PRESET, "-tune", "zerolatency",
            "-b:v", VBITRATE, "-maxrate", VBITRATE, "-bufsize", VBITRATE,
            "-pix_fmt", "yuv420p", "-g", "60", "-keyint_min", "60",
        ]
    cmd += [
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-vsync", "cfr", "-r", "30",
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


async def gather_print_meta(session: aiohttp.ClientSession) -> dict:
    keys_entities = {
        "file": FILE_ENTITY,
        "progress": PROGRESS_ENTITY,
        "material": MATERIAL_ENTITY,
        "remaining": REMAINING_TIME_ENTITY,
        "layer": CURRENT_LAYER_ENTITY,
        "total_layers": TOTAL_LAYERS_ENTITY,
    }
    out: dict = {}
    for k, ent in keys_entities.items():
        if ent:
            out[k] = await get_state(session, ent)
    return out


async def announce_start(session: aiohttp.ClientSession) -> None:
    global last_progress_bucket, print_session_meta
    last_progress_bucket = None
    meta = await gather_print_meta(session)
    file_name = meta.get("file") or "print"
    material = meta.get("material") or "?"
    eta = fmt_minutes(meta.get("remaining"))
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
        eta = fmt_minutes(meta.get("remaining"))
        await send_chat(session, f"🟦 {pct}% • Layer {layer}/{total} • {eta} left")


async def evaluate_state(session: aiohttp.ClientSession, state: str | None,
                         prev_state: str | None) -> None:
    entering_start = state in START_STATES and (prev_state not in START_STATES)
    entering_stop = state in STOP_STATES and (prev_state not in STOP_STATES)

    if state in START_STATES and not is_running():
        src = await resolve_stream_source(session, CAMERA_ENTITY)
        if src:
            start_ffmpeg(src)
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

        while True:
            try:
                state = await get_state(session, TRIGGER_ENTITY)
                if state != last_state:
                    log.info("%s: %s -> %s", TRIGGER_ENTITY, last_state, state)
                    await evaluate_state(session, state, last_state)
                    last_state = state
                elif state in START_STATES:
                    await maybe_report_progress(session)
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
