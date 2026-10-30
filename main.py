#!/usr/bin/env python3
"""TikTok Live -> YouTube Restreamer (OBS-Direct-URL Mode).
Behaves exactly like OBS Studio: Extracts direct stream URL and lets FFmpeg handle network buffering natively.
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
except Exception as _e:
    TikTokLiveClient = None
    _TIKTOKLIVE_IMPORT_ERROR = _e


# ==========================================================================
# Config
# ==========================================================================
def _env(name, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError:
        return default


TIKTOK_URL = _env("TIKTOK_URL", "https://www.tiktok.com/@d.shakertawfiqalaroury/live")
YOUTUBE_RTMP = _env("YOUTUBE_RTMP", "rtmp://a.rtmp.youtube.com/live2/vksy-du6k-8cxj-u7kh-7mfm")
_m = re.search(r"tiktok\.com/@([^/?#]+)", TIKTOK_URL)
TIKTOK_USERNAME = _env("TIKTOK_USERNAME", _m.group(1) if _m else "")


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def build_ffmpeg_cmd(stream_url):
    """OBS-style FFmpeg command using Direct Stream URL and native network reconnection."""
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-stats",
        "-stats_period", "15",

        # Network Resilience Flags (Same as OBS Native Source)
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",

        "-i", stream_url,  # Direct TikTok Stream URL input

        "-c:v", "copy",     # Pure Copy (No CPU Overhead)
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",

        "-fps_mode", "passthrough",
        "-flvflags", "no_duration_filesize",

        "-f", "flv",
        YOUTUBE_RTMP,
    ]


def make_session():
    session = streamlink.Streamlink()
    try:
        session.set_option("stream-timeout", 30)
        session.set_option("http-timeout", 15)
    except Exception:
        pass
    return session


# ==========================================================================
# WebSocket Event Monitor (Live End Detector)
# ==========================================================================
class LiveEndMonitor(threading.Thread):
    def __init__(self, owner):
        super().__init__(daemon=True)
        self.owner = owner

    def run(self):
        asyncio.run(self._main())

    async def _main(self):
        while not self.owner.stop_event.is_set():
            try:
                client = TikTokLiveClient(unique_id=f"@{TIKTOK_USERNAME}")

                @client.on(ConnectEvent)
                async def _on_connect(event):
                    log("[WS] متصل بسيرفر تيك توك المباشر مراقبة للإنهاء.")

                @client.on(LiveEndEvent)
                async def _on_end(event):
                    log("تيك توك أعلن: البث انتهى رسمياً -> إغلاق يوتيوب فوراً.")
                    self.owner.on_live_ended()

                if await client.is_live():
                    await client.connect()
            except Exception:
                pass
            await asyncio.sleep(5)


# ==========================================================================
# Main Restreamer Logic
# ==========================================================================
class Restreamer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.live_ended_event = threading.Event()
        self.session = make_session()
        self.proc = None

    def on_live_ended(self):
        self.live_ended_event.set()
        if self.proc:
            try:
                self.proc.terminate()
            except Exception:
                pass

    def get_direct_stream_url(self):
        try:
            streams = self.session.streams(TIKTOK_URL)
            if streams:
                chosen = streams.get("best") or list(streams.values())[0]
                # Extract direct stream URL (HLS / FLV URL)
                return getattr(chosen, "url", None)
        except Exception:
            pass
        return None

    def run(self):
        log("بدء تشغيل النقل بأسلوب OBS المباشر (OBS-Direct-URL Mode)...")
        log(f"الحساب المستهدف: {TIKTOK_URL}")

        if TikTokLiveClient and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()

        while not self.stop_event.is_set():
            stream_url = self.get_direct_stream_url()

            if stream_url:
                log("تم التقاط رابط البث المباشر! تشغيل محرك FFmpeg بنمط OBS...")
                self.live_ended_event.clear()

                cmd = build_ffmpeg_cmd(stream_url)
                self.proc = subprocess.Popen(cmd)

                # الانتظار حتى ينتهي FFmpeg تلقائياً أو يصل أمر إغلاق البث من تيك توك
                while not self.stop_event.is_set() and not self.live_ended_event.is_set():
                    if self.proc.poll() is not None:
                        log("عملية FFmpeg انتهت (البث توقف من المصدر).")
                        break
                    time.sleep(1)

                # تنظيف العملية عند الخروج
                if self.proc and self.proc.poll() is None:
                    try:
                        self.proc.terminate()
                        self.proc.wait(timeout=3)
                    except Exception:
                        self.proc.kill()
                self.proc = None

            else:
                log("البث مقفول حالياً... جاري الفحص مجدداً بعد 5 ثوانٍ.")
                time.sleep(5)


def main():
    restreamer = Restreamer()

    def handler(sig, frame):
        restreamer.stop_event.set()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    restreamer.run()


if __name__ == "__main__":
    main()
