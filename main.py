#!/usr/bin/env python3
"""TikTok Live -> YouTube restreamer that behaves like the TikTok mobile app.

How the mobile app behaves, and how this script copies it:

  * Network drops never end the live. -> FFmpeg (and the YouTube RTMP
    connection) stays alive while TikTok is re-opened through Streamlink.
  * The live ends only when TikTok itself announces it ("Live has ended").
    -> A background thread listens to TikTok's Webcast websocket (via the
    TikTokLive library, the same channel the app uses) and, the moment it
    receives LiveEndEvent, FFmpeg's stdin is closed and YouTube ends.

Safety nets (only used when the websocket signal is missing):
  * Websocket NOT connected  -> falls back to polling TikTok and ends after a
                                 very short confirmation (END_CONFIRM_CHECKS).
  * Websocket connected, but TikTok keeps saying "not live" without ever
    sending LiveEndEvent for ENDED_WITHOUT_EVENT_SECONDS -> ends anyway.
  * TikTok completely unreachable for NETWORK_FAILSAFE_SECONDS -> ends.
"""
import asyncio
import os
import re
import signal
import subprocess
import sys
import threading
import time

import streamlink

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, DisconnectEvent, LiveEndEvent
    _TIKTOKLIVE_IMPORT_ERROR = None
except Exception as _e:  # library missing/broken -> polling-only mode
    TikTokLiveClient = None
    _TIKTOKLIVE_IMPORT_ERROR = _e

# --------------------------------------------------------------------------
# Config (can be overridden with environment variables)
# --------------------------------------------------------------------------
TIKTOK_URL = os.environ.get(
    "TIKTOK_URL", "https://www.tiktok.com/@amin1alg/live"
)
YOUTUBE_RTMP = os.environ.get(
    "YOUTUBE_RTMP", "rtmp://a.rtmp.youtube.com/live2/5czj-a0sq-5cmp-4aps-bq5h"
)
_m = re.search(r"tiktok\.com/@([^/?#]+)", TIKTOK_URL)
TIKTOK_USERNAME = os.environ.get("TIKTOK_USERNAME") or (_m.group(1) if _m else "")

CHUNK_SIZE = 64 * 1024  # 64KB reads from Streamlink

RECONNECT_DELAY = float(os.environ.get("RECONNECT_DELAY", "0.5"))
OFFLINE_POLL_INTERVAL = float(os.environ.get("OFFLINE_POLL_INTERVAL", "5"))
WS_POLL_INTERVAL = float(os.environ.get("WS_POLL_INTERVAL", "5"))
END_CONFIRM_CHECKS = int(os.environ.get("END_CONFIRM_CHECKS", "2"))
END_CONFIRM_INTERVAL = float(os.environ.get("END_CONFIRM_INTERVAL", "0.5"))
ENDED_WITHOUT_EVENT_SECONDS = float(os.environ.get("ENDED_WITHOUT_EVENT_SECONDS", "60"))
NETWORK_FAILSAFE_SECONDS = float(os.environ.get("NETWORK_FAILSAFE_SECONDS", "180"))

LIVE, ENDED, UNKNOWN = "live", "ended", "unknown"


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def build_ffmpeg_cmd():
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-stats",

        "-dts_delta_threshold", "1",
        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",

        "-thread_queue_size", "2048",
        "-i", "-",

        "-map", "0:v:0",
        "-c:v", "copy",

        "-map", "0:a:0?",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",

        "-fps_mode", "passthrough",
        "-flush_packets", "1",

        "-flvflags", "no_duration_filesize",

        "-f", "flv",
        YOUTUBE_RTMP,
    ]


def make_session():
    session = streamlink.Streamlink()

    def set_opt(key, value):
        # Newer Streamlink uses session.options; older uses set_option().
        try:
            session.options.set(key, value)
        except AttributeError:
            session.set_option(key, value)

    set_opt("hls-live-edge", 2)
    set_opt("ringbuffer-size", 64 * 1024 * 1024)  # 64MB
    set_opt("stream-segment-attempts", 10)
    set_opt("stream-segment-timeout", 10)
    set_opt("stream-timeout", 30)
    return session


def classify_error(exc):
    """Decide whether an exception from session.streams() means
    'user not live' (ENDED) or 'we couldn't reach TikTok' (UNKNOWN)."""
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        # requests' ConnectionError/Timeout/etc. are all OSError subclasses.
        if isinstance(cur, OSError):
            response = getattr(cur, "response", None)
            if response is not None and getattr(response, "status_code", None) == 404:
                return ENDED
            return UNKNOWN
        cur = cur.__cause__ or cur.__context__
    if type(exc).__name__ in ("NoStreamsError", "PluginError"):
        return ENDED
    return UNKNOWN  # unexpected error: be conservative, don't end the broadcast


class FFmpegDied(Exception):
    pass


class LiveEndMonitor(threading.Thread):
    """Listens to TikTok's Webcast websocket (same channel the mobile app uses)
    and tells the Restreamer when TikTok announces the live has ended."""

    def __init__(self, owner):
        super().__init__(daemon=True, name="live-end-monitor")
        self.owner = owner

    def run(self):
        try:
            asyncio.run(self._main())
        except Exception as e:
            log(f"[WS] monitor crashed: {type(e).__name__}: {e} (polling fallback active)")
            self.owner.ws_connected.clear()

    async def _main(self):
        owner = self.owner
        last_err = None
        while not owner.stop.is_set():
            client = TikTokLiveClient(unique_id=f"@{TIKTOK_USERNAME}")

            @client.on(ConnectEvent)
            async def _on_connect(event, client=client):
                room_id = getattr(client, "room_id", None)
                log(f"[WS] connected to TikTok webcast (room {room_id})")
                owner.on_ws_connect(room_id)

            @client.on(LiveEndEvent)
            async def _on_end(event, client=client):
                owner.on_live_end(getattr(client, "room_id", None))

            @client.on(DisconnectEvent)
            async def _on_disconnect(event):
                owner.ws_connected.clear()

            try:
                if await client.is_live():
                    last_err = None
                    await client.connect()  # returns when the connection ends
                    log("[WS] webcast connection closed")
                else:
                    owner.ws_connected.clear()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                if msg != last_err:
                    log(f"[WS] error: {msg}")
                    last_err = msg
            owner.ws_connected.clear()
            await asyncio.sleep(WS_POLL_INTERVAL)


class Restreamer:
    def __init__(self):
        self.stop = threading.Event()
        self.session = make_session()
        self.ffmpeg = None
        self.last_data = 0.0
        self._last_status_msg = None

        # TikTok-announced end (set from the websocket thread)
        self.live_over = threading.Event()
        self.ended_room_id = None
        self.ws_connected = threading.Event()
        self.current_fd = None
        self.ended_since = None  # polling says "not live" since...

    # ---------------- websocket callbacks (other thread) ----------------
    def on_ws_connect(self, room_id):
        self.ws_connected.set()
        if (
            self.live_over.is_set()
            and room_id is not None
            and room_id != self.ended_room_id
        ):
            log("New live room detected - ready to restream again.")
            self.live_over.clear()

    def on_live_end(self, room_id):
        log("TikTok announced: LIVE HAS ENDED -> ending YouTube broadcast now.")
        self.ended_room_id = room_id
        self.live_over.set()
        fd = self.current_fd
        if fd is not None:
            try:
                fd.close()  # unblock the reader immediately
            except Exception:
                pass

    # ---------------- FFmpeg lifecycle ----------------
    def start_ffmpeg(self):
        log("Starting FFmpeg -> YouTube RTMP (persistent)...")
        self.ffmpeg = subprocess.Popen(
            build_ffmpeg_cmd(),
            stdin=subprocess.PIPE,
            stdout=None,
            stderr=None,
        )
        self.last_data = time.monotonic()

    def stop_ffmpeg(self, reason):
        proc, self.ffmpeg = self.ffmpeg, None
        if proc is None:
            return
        log(f"Stopping FFmpeg ({reason})...")
        try:
            proc.stdin.close()  # EOF -> FFmpeg finalizes and closes RTMP
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def write(self, data):
        if self.ffmpeg is None:
            self.start_ffmpeg()
        if self.ffmpeg.poll() is not None:
            raise FFmpegDied(f"exit code {self.ffmpeg.returncode}")
        try:
            self.ffmpeg.stdin.write(data)
            self.ffmpeg.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as e:
            raise FFmpegDied(str(e))
        self.last_data = time.monotonic()

    # ---------------- TikTok side ----------------
    def _note(self, msg):
        if msg != self._last_status_msg:
            log(msg)
            self._last_status_msg = msg

    def check_live(self):
        """Ask TikTok (via Streamlink) if the user is live."""
        try:
            streams = self.session.streams(TIKTOK_URL)
        except Exception as e:
            status = classify_error(e)
            if status == ENDED:
                self._note(f"TikTok says NOT live ({type(e).__name__}: {e})")
            else:
                self._note(f"Cannot reach TikTok - network hiccup? ({type(e).__name__}: {e})")
            return status, None
        stream = streams.get("best") if streams else None
        if stream is None:
            self._note("TikTok says NOT live (no streams)")
            return ENDED, None
        self._note("TikTok says LIVE")
        return LIVE, stream

    def stream_session(self, stream):
        """Open the stream and copy it into FFmpeg until it drops/ends."""
        try:
            fd = stream.open()
        except Exception as e:
            log(f"Could not open stream: {e}")
            return
        self.current_fd = fd
        log("Piping TikTok -> FFmpeg...")
        try:
            while not self.stop.is_set() and not self.live_over.is_set():
                try:
                    data = fd.read(CHUNK_SIZE)
                except Exception as e:
                    if not self.live_over.is_set():
                        log(f"Read error from TikTok: {e}")
                    return
                if self.live_over.is_set():
                    return
                if not data:
                    log("TikTok stream delivered EOF")
                    return
                self.write(data)
        except FFmpegDied as e:
            log(f"FFmpeg died ({e}); restarting it with a fresh stream.")
            self.stop_ffmpeg("died")
        finally:
            self.current_fd = None
            try:
                fd.close()
            except Exception:
                pass

    # ---------------- State handlers ----------------
    def handle_ended(self):
        """Polling says 'not live'. Returns the next (status, stream)."""
        if self.ffmpeg is None:
            self.ended_since = None
            self.stop.wait(OFFLINE_POLL_INTERVAL)
            return self.check_live()

        now = time.monotonic()
        if self.ended_since is None:
            self.ended_since = now

        if self.ws_connected.is_set():
            # Mobile-like: don't guess. Wait for TikTok's explicit LiveEndEvent.
            # Safety net in case TikTok never sends it.
            if now - self.ended_since >= ENDED_WITHOUT_EVENT_SECONDS:
                log(
                    f"TikTok reports not-live for {ENDED_WITHOUT_EVENT_SECONDS:.0f}s "
                    "with no LiveEndEvent -> ending."
                )
                self.stop_ffmpeg("not live, no end event")
                self.ended_since = None
                self.stop.wait(OFFLINE_POLL_INTERVAL)
                return self.check_live()
            self.stop.wait(RECONNECT_DELAY)
            return self.check_live()

        # Websocket not available: fall back to a quick polling confirmation.
        for _ in range(END_CONFIRM_CHECKS - 1):
            self.stop.wait(END_CONFIRM_INTERVAL)
            status, stream = self.check_live()
            if status != ENDED:
                self.ended_since = None
                return status, stream
        log("LIVE ENDED confirmed by polling (websocket unavailable) -> ending YouTube.")
        self.stop_ffmpeg("live ended (polling)")
        self.ended_since = None
        self.stop.wait(OFFLINE_POLL_INTERVAL)
        return self.check_live()

    def handle_unknown(self):
        """Couldn't reach TikTok (our network). Keep FFmpeg, retry fast."""
        if (
            self.ffmpeg is not None
            and time.monotonic() - self.last_data > NETWORK_FAILSAFE_SECONDS
        ):
            self.stop_ffmpeg(
                f"no data and TikTok unreachable for {NETWORK_FAILSAFE_SECONDS:.0f}s"
            )
        self.stop.wait(RECONNECT_DELAY if self.ffmpeg is not None else OFFLINE_POLL_INTERVAL)
        return self.check_live()

    # ---------------- Main loop ----------------
    def run(self):
        log("TikTok Live Monitor & Auto-Restreamer (mobile-like, persistent FFmpeg)")
        log(f"Watching: {TIKTOK_URL}")
        if TikTokLiveClient is not None and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()
            log("Live-end detection: TikTok websocket (LiveEndEvent)")
        else:
            reason = _TIKTOKLIVE_IMPORT_ERROR or "no username in TIKTOK_URL"
            log(f"[WS] unavailable ({reason}); using polling-based live-end detection")

        try:
            status, stream = self.check_live()
            while not self.stop.is_set():
                if self.live_over.is_set():
                    # TikTok announced the end: close YouTube, then wait for a
                    # genuinely new live (never restart on a stale stream).
                    if self.ffmpeg is not None:
                        self.stop_ffmpeg("TikTok LiveEndEvent")
                    if status == ENDED:
                        self.live_over.clear()
                    self.ended_since = None
                    self.stop.wait(OFFLINE_POLL_INTERVAL)
                    status, stream = self.check_live()
                    continue

                if status == LIVE:
                    self.ended_since = None
                    self.stream_session(stream)
                    if self.stop.is_set():
                        break
                    if not self.live_over.is_set():
                        log("TikTok data stopped - checking if live is still on...")
                        self.stop.wait(RECONNECT_DELAY)
                    status, stream = self.check_live()
                elif status == ENDED:
                    status, stream = self.handle_ended()
                else:
                    status, stream = self.handle_unknown()
        finally:
            self.stop_ffmpeg("shutdown")


def main():
    restreamer = Restreamer()

    def handler(sig, frame):
        log(f"[SYSTEM] Signal {sig} received, shutting down...")
        restreamer.stop.set()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise SystemExit(0)  # unwinds through run()'s finally -> stops FFmpeg

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    restreamer.run()


if __name__ == "__main__":
    main()
