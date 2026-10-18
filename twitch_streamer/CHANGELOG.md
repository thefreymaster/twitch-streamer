# Changelog

## 0.9.0 — 2026-10-18

- Stream Bambuddy's streaming overlay (full-screen camera with live print
  data) instead of a Home Assistant camera. The add-on renders the overlay
  page in Chromium on a virtual display and sends that display to Twitch at
  30 fps.
- New options: `bambuddy_ip`, `bambuddy_port` (default 9023),
  `bambuddy_token` and `overlay_path`. The token is a Bambuddy Camera API
  token with the **Streaming Overlay** scope. A "Camera stream" token can't
  read the print data.
- Twitch stays live while the overlay browser restarts or Bambuddy is down.
  The add-on logs an error if Bambuddy rejects the token.
- Removed 0.8.0's `bambuddy_url`, `bambuddy_api_key`, `bambuddy_printer_id`
  and `camera_fps` options. Leave `bambuddy_ip` blank to stream
  `camera_entity` as before.
- The Docker image now includes Chromium and Xvfb, so it is noticeably larger.

## 0.8.0 — 2026-10-18

- Read the camera straight from Bambuddy at a chosen frame rate (default
  30 fps). The Bambuddy HA camera entity is fixed at 5 fps.
- The 5-minute status line also shows the camera's real frame rate.

## 0.7.0 — 2026-10-17

- Keep the Twitch stream up for the whole print: the camera and the Twitch
  encoder run as separate processes, so a camera reconnect no longer drops
  the broadcast.

## 0.6.9 — 2026-10-17

- Stream MJPEG-only cameras (such as Bambuddy's) directly from Core.

## 0.6.8 — 2026-08-19

- Time-limit the camera/stream websocket request so it can't freeze the
  add-on.

## 0.6.7 — 2026-08-19

- Detect frozen video instead of streaming silence forever.

## 0.6.6 — 2026-06-24

- Remove HLS reconnect flags that broke playlist parsing.

## 0.6.5 — 2026-06-24

- Restart ffmpeg when its output freezes (stall watchdog).

## 0.6.4 — 2026-06-23

- Use the task name for the stream title and wait for real print metadata.

## 0.6.3 — 2026-06-22

- Ignore brief stop states from printer glitches before ending a stream.

## 0.6.2 — 2026-06-20

- Fix the ETA units in chat and clean up file names.

## 0.6.1 — 2026-06-19

- Send the bearer token on HLS input (fixes a Supervisor proxy 401).

## 0.6.0 — 2026-06-19

- Stream integration cameras through HA's HLS; fix `camera_entity` being
  ignored.

## 0.5.1 — 2026-05-29

- Restore libx264 transcoding with timestamp regeneration and a 1080p
  downscale.

## 0.5.0 — 2026-05-29

- Add the `video_codec` option and snapshot the print metadata.

## 0.4.2 — 2026-05-29

- Raise ffmpeg's thread queue size to silence backpressure warnings.

## 0.4.1 — 2026-05-28

- Prefer fresh Twitch tokens from the config over saved ones on startup.

## 0.4.0 — 2026-05-28

- Set the Twitch title from the print file and post progress updates in
  chat.

## 0.3.2 — 2026-05-28

- Read the camera's stream source from its REST attributes.

## 0.3.1 — 2026-05-28

- Run the controller with the Supervisor token in its environment.

## 0.3.0 — 2026-05-28

- Check the printer state on startup and act only on state changes.

## 0.2.1 — 2026-05-28

- Fix the Docker build (missing `BUILD_FROM`).

## 0.2.0 — 2026-05-28

- First release: stream a Home Assistant camera to Twitch while a print runs.
