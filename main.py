#!/usr/bin/env python3
"""TikTok Live -> YouTube restreamer, built to keep running unattended 24/7.

Fix: Re-encodes video with ultrafast preset and fixed 1080x1920 scale/pad
filter so YouTube canvas size NEVER changes when TikTok layout changes.
"""
import asyncio
import logging
import os
import queue
import random
import re
import signal
import subprocess
import sys
import threading
import time
import traceback

import streamlink

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, DisconnectEvent, LiveEndEvent
    _TIKTOKLIVE_IMPORT_ERROR = None
except Exception as _e:  # library missing/broken -> polling-only mode
    TikTokLiveClient = None
    _TIKTOKLIVE_IMPORT_ERROR = _e


# ==========================================================================
# Config (every value can be overridden with an environment variable)
# ==========================================================================
def _env(name, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError:
        print(f"[CONFIG] bad value for {name}={raw!r}; using {default!r}", flush=True)
        return default


TIKTOK_URL = _env("TIKTOK_URL", "https://www.tiktok.com/@zain_kairalah2/live")
YOUTUBE_RTMP = _env("YOUTUBE_RTMP", "rtmp://a.rtmp.youtube.com/live2/5czj-a0sq-5cmp-4aps-bq5h")
_m = re.search(r"tiktok\.com/@([^/?#]+)", TIKTOK_URL)
TIKTOK_USERNAME = _env("TIKTOK_USERNAME", _m.group(1) if _m else "")

# Source / quality
STREAM_QUALITY = _env("STREAM_QUALITY", "best")
HLS_LIVE_EDGE = _env("HLS_LIVE_EDGE", 2, int)
CHUNK_SIZE = 64 * 1024

# Reconnect pacing
RECONNECT_BASE_DELAY = _env("RECONNECT_BASE_DELAY", 0.5, float)
RECONNECT_MAX_DELAY = _env("RECONNECT_MAX_DELAY", 5.0, float)
HEALTHY_SESSION_SECONDS = _env("HEALTHY_SESSION_SECONDS", 10.0, float)
STREAM_STALL_SECONDS = _env("STREAM_STALL_SECONDS", 15.0, float)

# Live / end detection
OFFLINE_POLL_INTERVAL = _env("OFFLINE_POLL_INTERVAL", 5.0, float)
ENDED_POLL_INTERVAL = _env("ENDED_POLL_INTERVAL", 2.0, float)
END_CONFIRM_INTERVAL = _env("END_CONFIRM_INTERVAL", 1.0, float)
OFFLINE_CONFIRM_SECONDS = _env("OFFLINE_CONFIRM_SECONDS", 25.0, float)
ENDED_WITHOUT_EVENT_SECONDS = _env("ENDED_WITHOUT_EVENT_SECONDS", 60.0, float)
NETWORK_FAILSAFE_SECONDS = _env("NETWORK_FAILSAFE_SECONDS", 180.0, float)
STALE_LIVE_SECONDS = _env("STALE_LIVE_SECONDS", 120.0, float)
WS_POLL_INTERVAL = _env("WS_POLL_INTERVAL", 3.0, float)
WS_MAX_BACKOFF = _env("WS_MAX_BACKOFF", 60.0, float)

# FFmpeg supervision
WRITE_STALL_SECONDS = _env("WRITE_STALL_SECONDS", 15.0, float)
FFMPEG_BACKOFF_BASE = _env("FFMPEG_BACKOFF_BASE", 1.0, float)
FFMPEG_BACKOFF_MAX = _env("FFMPEG_BACKOFF_MAX", 30.0, float)
FFMPEG_HEALTHY_SECONDS = _env("FFMPEG_HEALTHY_SECONDS", 30.0, float)
AUDIO_BITRATE = _env("AUDIO_BITRATE", "128k")
FFMPEG_PROBESIZE = _env("FFMPEG_PROBESIZE", "")
FFMPEG_ANALYZEDURATION = _env("FFMPEG_ANALYZEDURATION", "")

# Self-protection / diagnostics
WATCHDOG_SECONDS = _env("WATCHDOG_SECONDS", 300.0, float)
STATUS_INTERVAL = _env("STATUS_INTERVAL", 120.0, float)
LOG_LEVEL = _env("LOG_LEVEL", "WARNING").upper()

LIVE, ENDED, UNKNOWN = "live", "ended", "unknown"


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


class Backoff:
    def __init__(self, base, cap, factor=2.0, jitter=0.2):
        self.base, self.cap, self.factor, self.jitter = base, cap, factor, jitter
        self.attempt = 0

    def reset(self):
        self.attempt = 0

    def next(self):
        delay = min(self.cap, self.base * (self.factor ** self.attempt))
        self.attempt += 1
        return max(0.0, delay * (1.0 + random.uniform(-self.jitter, self.jitter)))


class FFmpegDied(Exception):
    pass


def build_ffmpeg_cmd():
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-stats",
        "-stats_period", "15",

        "-dts_delta_threshold", "1",
        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",
    ]
    if FFMPEG_PROBESIZE:
        cmd += ["-probesize", FFMPEG_PROBESIZE]
    if FFMPEG_ANALYZEDURATION:
        cmd += ["-analyzeduration", FFMPEG_ANALYZEDURATION]
    cmd += [
        "-thread_queue_size", "2048",
        "-i", "-",

        "-map", "0:v:0",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2",
        "-r", "30",
        "-g", "60",

        "-map", "0:a:0?",
        "-c:a", "aac",
        "-b:a", AUDIO_BITRATE,
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",

        "-max_muxing_queue_size", "4096",
        "-flush_packets", "1",
        "-flvflags", "no_duration_filesize",

        "-f", "flv",
        YOUTUBE_RTMP,
    ]
    return cmd


def make_session():
    session = streamlink.Streamlink()

    def set_opt(key, value):
        try:
            try:
                session.options.set(key, value)
            except AttributeError:
                session.set_option(key, value)
        except Exception as e:
            log(f"[CONFIG] could not set Streamlink option {key}: {e}")

    set_opt("hls-live-edge", HLS_LIVE_EDGE)
    set_opt("ringbuffer-size", 64 * 1024 * 1024)
    set_opt("stream-segment-attempts", 10)
    set_opt("stream-segment-timeout", 10)
    set_opt("stream-timeout", 30)
    set_opt("hls-playlist-reload-attempts", 10)
    set_opt("http-timeout", 10)
    return session


def classify_error(exc):
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, OSError):
            response = getattr(cur, "response", None)
            if response is not None and getattr(response, "status_code", None) == 404:
                return ENDED
            return UNKNOWN
        cur = cur.__cause__ or cur.__context__
    if type(exc).__name__ == "NoStreamsError":
        return ENDED
    return UNKNOWN


class FFmpegSink:
    def __init__(self):
        self.proc = None
        self.started_at = 0.0
        self.write_began = None
        self.starts = 0

    @property
    def active(self):
        return self.proc is not None

    def start(self):
        log("Starting FFmpeg -> YouTube RTMP (fixed 1080x1920 canvas)...")
        self.proc = subprocess.Popen(
            build_ffmpeg_cmd(), stdin=subprocess.PIPE, stdout=None, stderr=None
        )
        self.started_at = time.monotonic()
        self.starts += 1

    def write(self, data):
        proc = self.proc
        if proc is None or proc.poll() is not None:
            raise FFmpegDied(f"exit code {proc.returncode if proc else 'n/a'}")
        self.write_began = time.monotonic()
        try:
            proc.stdin.write(data)
            proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as e:
            raise FFmpegDied(str(e)) from e
        finally:
            self.write_began = None

    def kill(self):
        proc = self.proc
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass

    def stop(self, reason, grace=8.0):
        proc, self.proc = self.proc, None
        if proc is None:
            return
        log(f"Stopping FFmpeg ({reason})...")
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


class LiveEndMonitor(threading.Thread):
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
        err_backoff = Backoff(WS_POLL_INTERVAL, WS_MAX_BACKOFF)
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

            delay = WS_POLL_INTERVAL
            try:
                if await client.is_live():
                    owner.wake.set()
                    last_err = None
                    err_backoff.reset()
                    await client.connect()
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
                delay = err_backoff.next()
            owner.ws_connected.clear()
            await asyncio.sleep(delay)


class Watchdog(threading.Thread):
    def __init__(self, owner):
        super().__init__(daemon=True, name="watchdog")
        self.owner = owner

    def run(self):
        owner = self.owner
        last_status = time.monotonic()
        while not owner.stop.wait(0.5):
            now = time.monotonic()

            began = owner.sink.write_began
            if began is not None and now - began > WRITE_STALL_SECONDS:
                log(f"WATCHDOG: FFmpeg not accepting data for {now - began:.0f}s "
                    "(RTMP stuck?) -> killing FFmpeg to restart it.")
                owner.sink.kill()
                owner.sink.write_began = None

            if now - owner.heartbeat > WATCHDOG_SECONDS:
                log(f"WATCHDOG: main loop frozen for {WATCHDOG_SECONDS:.0f}s -> exiting "
                    "so Docker restarts a clean process.")
                owner.sink.kill()
                os._exit(3)

            if owner.sink.active and now - last_status >= STATUS_INTERVAL:
                last_status = now
                s = owner.stats
                log(
                    f"[STATUS] streaming | forwarded {s['bytes'] / 1e6:.0f} MB | "
                    f"source reconnects {s['source_drops']} | "
                    f"FFmpeg starts {owner.sink.starts} (failures {s['ffmpeg_failures']}) | "
                    f"websocket {'on' if owner.ws_connected.is_set() else 'off'}"
                )


class Restreamer:
    def __init__(self):
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.session = make_session()
        self.sink = FFmpegSink()
        self.heartbeat = time.monotonic()
        self.last_data = 0.0
        self.stats = {"bytes": 0, "source_drops": 0, "ffmpeg_failures": 0}

        self._last_status_msg = None
        self._last_quality_msg = None
        self.extra_delay = 0.0
        self.unknown_count = 0
        self.source_backoff = Backoff(RECONNECT_BASE_DELAY, RECONNECT_MAX_DELAY)
        self.unknown_backoff = Backoff(RECONNECT_BASE_DELAY, RECONNECT_MAX_DELAY)
        self.ffmpeg_backoff = Backoff(FFMPEG_BACKOFF_BASE, FFMPEG_BACKOFF_MAX)

        self.live_over = threading.Event()
        self.live_over_since = None
        self.ended_room_id = None
        self.ws_connected = threading.Event()
        self.current_fd = None
        self.ended_since = None

    def beat(self):
        self.heartbeat = time.monotonic()

    def nap(self, seconds, wakeable=False):
        end = time.monotonic() + seconds
        while not self.stop.is_set():
            self.beat()
            if wakeable and self.wake.is_set():
                self.wake.clear()
                return
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self.stop.wait(min(remaining, 0.2))

    def on_ws_connect(self, room_id):
        self.ws_connected.set()
        if self.live_over.is_set() and room_id is not None and room_id != self.ended_room_id:
            log("New live room detected - ready to restream again.")
            self.live_over.clear()

    def on_live_end(self, room_id):
        log("TikTok announced: LIVE HAS ENDED -> ending YouTube broadcast now.")
        self.ended_room_id = room_id
        self.live_over_since = time.monotonic()
        self.live_over.set()
        fd = self.current_fd
        if fd is not None:
            try:
                fd.close()
            except Exception:
                pass

    def write(self, data):
        if not self.sink.active:
            self.sink.start()
            self.last_data = time.monotonic()
        self.sink.write(data)
        self.last_data = time.monotonic()
        self.stats["bytes"] += len(data)

    def on_ffmpeg_failure(self, why):
        uptime = time.monotonic() - self.sink.started_at if self.sink.started_at else 0.0
        self.stats["ffmpeg_failures"] += 1
        self.sink.stop("died", grace=2)
        if uptime >= FFMPEG_HEALTHY_SECONDS:
            self.ffmpeg_backoff.reset()
        wait = self.ffmpeg_backoff.next()
        self.extra_delay = max(self.extra_delay, wait)
        log(f"FFmpeg failed after {uptime:.0f}s ({why}); restarting in {wait:.1f}s with a fresh stream.")

    def _note(self, msg):
        if msg != self._last_status_msg:
            log(msg)
            self._last_status_msg = msg

    def pick_stream(self, streams):
        names = sorted(streams.keys())
        prefs = [q.strip() for q in STREAM_QUALITY.split(",") if q.strip()]
        chosen = next((q for q in prefs if q in streams), None)
        if chosen is None:
            chosen = "best" if "best" in streams else names[0]
        stream = streams[chosen]
        kind = type(stream).__name__
        msg = f"Qualities available: {', '.join(names)} | using '{chosen}' ({kind})"
        if msg != self._last_quality_msg:
            log(msg)
            self._last_quality_msg = msg
        return stream

    def check_live(self):
        self.beat()
        try:
            streams = self.session.streams(TIKTOK_URL)
        except Exception as e:
            status = classify_error(e)
            if status == ENDED:
                self.unknown_backoff.reset()
                self.unknown_count = 0
                self._note(f"TikTok says NOT live ({type(e).__name__}: {e})")
            else:
                self._note(f"Cannot reach TikTok - network hiccup? ({type(e).__name__}: {e})")
            return status, None
        self.unknown_backoff.reset()
        self.unknown_count = 0
        if not streams:
            self._note("TikTok says NOT live (no streams)")
            return ENDED, None
        self._note("TikTok says LIVE")
        return LIVE, self.pick_stream(streams)

    def stream_session(self, stream):
        try:
            fd = stream.open()
        except Exception as e:
            log(f"Could not open stream: {e}")
            return

        self.current_fd = fd
        chunks = queue.Queue(maxsize=32)
        reader_done = threading.Event()

        def reader():
            try:
                while not self.stop.is_set() and not self.live_over.is_set():
                    data = fd.read(CHUNK_SIZE)
                    kind, payload = ("data", data) if data else ("eof", None)
                    while not self.stop.is_set() and not self.live_over.is_set():
                        try:
                            chunks.put((kind, payload), timeout=0.5)
                            break
                        except queue.Full:
                            continue
                    if kind == "eof":
                        return
            except Exception as e:
                try:
                    chunks.put(("error", e), timeout=0.5)
                except queue.Full:
                    pass
            finally:
                reader_done.set()

        threading.Thread(target=reader, daemon=True, name="streamlink-reader").start()
        log("Piping TikTok -> FFmpeg (stall detection enabled)...")
        last_chunk = time.monotonic()
        try:
            while not self.stop.is_set() and not self.live_over.is_set():
                self.beat()
                proc = self.sink.proc
                if proc is not None and proc.poll() is not None:
                    raise FFmpegDied(f"exit code {proc.returncode}")
                try:
                    kind, payload = chunks.get(timeout=0.2)
                except queue.Empty:
                    if time.monotonic() - last_chunk >= STREAM_STALL_SECONDS:
                        log(f"No TikTok data for {STREAM_STALL_SECONDS:.0f}s; reconnecting source...")
                        return
                    if reader_done.is_set():
                        return
                    continue

                if kind == "data":
                    self.write(payload)
                    last_chunk = time.monotonic()
                elif kind == "eof":
                    log("TikTok stream delivered EOF")
                    return
                else:
                    if not self.live_over.is_set():
                        log(f"Read error from TikTok: {payload}")
                    return
        except FFmpegDied as e:
            self.on_ffmpeg_failure(str(e))
        finally:
            try:
                fd.close()
            except Exception:
                pass
            if self.current_fd is fd:
                self.current_fd = None

    def run_live(self, stream):
        self.ended_since = None
        started = time.monotonic()
        self.stream_session(stream)
        if self.stop.is_set():
            return LIVE, None
        if self.live_over.is_set():
            return self.check_live()
        duration = time.monotonic() - started
        self.stats["source_drops"] += 1
        if duration >= HEALTHY_SESSION_SECONDS:
            self.source_backoff.reset()
            delay = RECONNECT_BASE_DELAY
        else:
            delay = self.source_backoff.next()
        delay = max(delay, self.extra_delay)
        self.extra_delay = 0.0
        log(f"TikTok data stopped after {duration:.0f}s - checking if live is still on (in {delay:.1f}s)...")
        self.nap(delay)
        return self.check_live()

    def handle_ended(self):
        if not self.sink.active:
            self.ended_since = None
            self.nap(OFFLINE_POLL_INTERVAL, wakeable=True)
            return self.check_live()

        now = time.monotonic()
        if self.ended_since is None:
            self.ended_since = now

        if self.ws_connected.is_set():
            if now - self.ended_since >= ENDED_WITHOUT_EVENT_SECONDS:
                log(f"TikTok reports not-live for {ENDED_WITHOUT_EVENT_SECONDS:.0f}s "
                    "with no LiveEndEvent -> ending.")
                self.sink.stop("not live, no end event")
                self.ended_since = None
                self.nap(OFFLINE_POLL_INTERVAL)
                return self.check_live()
            self.nap(ENDED_POLL_INTERVAL)
            return self.check_live()

        if now - self.ended_since >= OFFLINE_CONFIRM_SECONDS:
            log(f"TikTok returned NOT LIVE continuously for {OFFLINE_CONFIRM_SECONDS:.0f}s "
                "(websocket unavailable) -> ending YouTube.")
            self.sink.stop("live ended (confirmed by sustained polling)")
            self.ended_since = None
            self.nap(OFFLINE_POLL_INTERVAL)
            return self.check_live()

        self.nap(END_CONFIRM_INTERVAL)
        status, stream = self.check_live()
        if status != ENDED:
            self.ended_since = None
        return status, stream

    def handle_unknown(self):
        self.ended_since = None
        self.unknown_count += 1
        if self.unknown_count % 5 == 0:
            log("Several network errors in a row - rebuilding the HTTP session.")
            self.session = make_session()
        if self.sink.active and time.monotonic() - self.last_data > NETWORK_FAILSAFE_SECONDS:
            self.sink.stop(f"no data and TikTok unreachable for {NETWORK_FAILSAFE_SECONDS:.0f}s")
        delay = self.unknown_backoff.next() if self.sink.active else OFFLINE_POLL_INTERVAL
        self.nap(delay, wakeable=not self.sink.active)
        return self.check_live()

    def step(self, status, stream):
        self.beat()
        if self.live_over.is_set():
            stale = (
                status == LIVE
                and self.live_over_since is not None
                and time.monotonic() - self.live_over_since > STALE_LIVE_SECONDS
            )
            if stale:
                log("TikTok still reports LIVE long after LiveEndEvent - assuming a new live.")
                self.live_over.clear()
            else:
                if self.sink.active:
                    self.sink.stop("TikTok LiveEndEvent")
                if status == ENDED:
                    self.live_over.clear()
                self.ended_since = None
                self.nap(OFFLINE_POLL_INTERVAL, wakeable=True)
                return self.check_live()

        if status == LIVE:
            return self.run_live(stream)
        if status == ENDED:
            return self.handle_ended()
        return self.handle_unknown()

    def run(self):
        log("TikTok Live -> YouTube Auto-Restreamer v4 (fixed 1080x1920 encoding)")
        log(f"Watching: {TIKTOK_URL}")
        Watchdog(self).start()
        if TikTokLiveClient is not None and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()
            log("Live-end detection: TikTok websocket (LiveEndEvent)")
        else:
            reason = _TIKTOKLIVE_IMPORT_ERROR or "no username in TIKTOK_URL"
            log(f"[WS] unavailable ({reason}); using polling-based live-end detection")

        try:
            status, stream = self.check_live()
            while not self.stop.is_set():
                try:
                    status, stream = self.step(status, stream)
                except Exception:
                    log("Unexpected error in main loop (continuing):\n" + traceback.format_exc())
                    self.nap(2.0)
                    status, stream = UNKNOWN, None
        finally:
            self.sink.stop("shutdown", grace=4)


def main():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.WARNING),
        format="[%(asctime)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    restreamer = Restreamer()

    def handler(sig, frame):
        log(f"[SYSTEM] Signal {sig} received, shutting down...")
        restreamer.stop.set()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    restreamer.run()


if __name__ == "__main__":
    main()
