# Changelog

## 0.11.1 — 2026-10-19

- Fix the overlay freezing about six minutes after it opens, with a blank
  camera and print data stuck where it froze. Bambuddy's overlay page opens
  a new camera connection every minute without closing the old one, and
  Chromium allows only six connections to Bambuddy, so after six minutes
  every request the page makes waits forever. The overlay browser now loads
  a small extension that closes the old camera connections.
- Reopen the overlay when its picture stops changing for 90 seconds, for
  example a frozen camera, a crashed browser tab or a blank page. Until now
  the add-on only watched the encoder and the browser process, so Twitch
  could show a still picture while every log line said "Live: 30.0 fps".
  If reopening doesn't help, it waits longer each time, up to 10 minutes.
  Setting `stall_restart_seconds` to 0 turns this off, and it never runs
  when the overlay hides the camera (`camera=false`).
- The 5-minute status line shows when the overlay picture last changed.
- Mask the Bambuddy token in browser log lines the add-on prints.

## 0.11.0 — 2026-10-18

- Bring back the Bambuddy streaming overlay from 0.9.0: the add-on renders
  the overlay page in Chromium on a virtual display and streams that to
  Twitch. A Home Assistant camera can't show the overlay, because it's a web
  page, and Bambuddy doesn't allow embedding it in other sites.
- To turn it on, set `bambuddy_ip` and `bambuddy_token` (a Camera API token
  with the **Streaming Overlay** scope). With `bambuddy_ip` blank, the add-on
  streams `camera_entity` as in 0.10.0.

## 0.10.0 — 2026-10-18

- Back to streaming a Home Assistant camera (`camera_entity`) to Twitch, as
  in 0.7.0. The overlay mode (Chromium + Xvfb) and its `bambuddy_*` and
  `overlay_path` options are removed, and the image no longer includes a
  browser.
- Kept from 0.8.0: the 5-minute status line shows the camera's real frame
  rate (`camera N fps`), so a slow camera shows up in the log.
- For smooth video, point `camera_entity` at an MJPEG IP Camera entity using
  Bambuddy's `/camera/stream?fps=30` URL. The Bambuddy integration's own
  camera is fixed at 5 fps.

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
