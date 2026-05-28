import asyncio
import json
import logging
import os
import signal
import subprocess
import sys

import aiohttp

TOKEN = os.environ["SUPERVISOR_TOKEN"]
REST_BASE = "http://supervisor/core/api"
WS_URL = "ws://supervisor/core/websocket"

with open("/data/options.json") as f:
    CONFIG = json.load(f)

TRIGGER_ENTITY = CONFIG["trigger_entity"]
CAMERA_ENTITY = CONFIG["camera_entity"]
START_STATES = {s.strip() for s in CONFIG["start_states"]}
STOP_STATES = {s.strip() for s in CONFIG["stop_states"]}
RTSP_OVERRIDE = (CONFIG.get("rtsp_override") or "").strip()
INGEST_URL = CONFIG["twitch_ingest_url"].rstrip("/")
TWITCH_KEY = CONFIG["twitch_key"]
VBITRATE = CONFIG["video_bitrate"]
PRESET = CONFIG["preset"]
POLL = int(CONFIG.get("poll_seconds", 5))

ffmpeg_proc: subprocess.Popen | None = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("twitch-streamer")


async def get_state(session: aiohttp.ClientSession, entity_id: str) -> str | None:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with session.get(f"{REST_BASE}/states/{entity_id}", headers=headers) as r:
        if r.status != 200:
            log.warning("state fetch %s -> HTTP %s", entity_id, r.status)
            return None
        data = await r.json()
        return data.get("state")


async def resolve_stream_source(session: aiohttp.ClientSession, entity_id: str) -> str | None:
    if RTSP_OVERRIDE:
        return RTSP_OVERRIDE
    try:
        async with session.ws_connect(WS_URL, heartbeat=30) as ws:
            await ws.receive_json()  # auth_required
            await ws.send_json({"type": "auth", "access_token": TOKEN})
            auth_resp = await ws.receive_json()
            if auth_resp.get("type") != "auth_ok":
                log.error("WebSocket auth failed: %s", auth_resp)
                return None
            await ws.send_json({"id": 1, "type": "camera/stream_source", "entity_id": entity_id})
            result = await ws.receive_json()
            if not result.get("success"):
                log.error("stream_source lookup failed: %s", result)
                return None
            return result["result"]["stream_source"]
    except Exception as e:
        log.error("stream_source resolve error: %s", e)
        return None


def is_running() -> bool:
    return ffmpeg_proc is not None and ffmpeg_proc.poll() is None


def start_ffmpeg(src: str) -> None:
    global ffmpeg_proc
    if is_running():
        return
    target = f"{INGEST_URL}/{TWITCH_KEY}"
    log.info("Starting ffmpeg: %s -> %s", src, INGEST_URL)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning",
        "-rtsp_transport", "tcp", "-re", "-i", src,
        "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "libx264", "-preset", PRESET, "-tune", "zerolatency",
        "-b:v", VBITRATE, "-maxrate", VBITRATE, "-bufsize", VBITRATE,
        "-pix_fmt", "yuv420p", "-g", "60", "-keyint_min", "60",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
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


async def evaluate_state(session: aiohttp.ClientSession, state: str | None) -> None:
    if state in START_STATES and not is_running():
        src = await resolve_stream_source(session, CAMERA_ENTITY)
        if src:
            start_ffmpeg(src)
        else:
            log.warning("No stream source resolved; will retry next poll")
    elif state in STOP_STATES and is_running():
        stop_ffmpeg()


async def main_loop() -> None:
    log.info("Watching %s — start=%s stop=%s camera=%s",
             TRIGGER_ENTITY, sorted(START_STATES), sorted(STOP_STATES), CAMERA_ENTITY)
    async with aiohttp.ClientSession() as session:
        initial = await get_state(session, TRIGGER_ENTITY)
        log.info("Startup state check: %s = %s", TRIGGER_ENTITY, initial)
        if initial in START_STATES:
            log.info("Print already active at startup — opening Twitch session")
        await evaluate_state(session, initial)
        last_state: str | None = initial

        while True:
            try:
                state = await get_state(session, TRIGGER_ENTITY)
                if state != last_state:
                    log.info("%s: %s -> %s", TRIGGER_ENTITY, last_state, state)
                    last_state = state
                    await evaluate_state(session, state)
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
    install_signal_handlers(asyncio.get_running_loop())
    try:
        await main_loop()
    except asyncio.CancelledError:
        pass
    finally:
        stop_ffmpeg()


if __name__ == "__main__":
    asyncio.run(main())
