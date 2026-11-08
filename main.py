#!/usr/bin/env python3
"""TikTok Live -> YouTube Restreamer (Stable / Light / High Quality).

Re-encodes video to true CBR 5000k (source resolution untouched),
keyframe every 2 seconds, AAC 128k audio.
"""
import asyncio
import os
import re
import signal
import subprocess
import threading
import time

import streamlink

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, LiveEndEvent
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

VIDEO_BITRATE = _env("VIDEO_BITRATE", "5000k")
AUDIO_BITRATE = _env("AUDIO_BITRATE", "128k")
FPS = _env("FPS", 30, int)                      # المصدر تقريباً 15 فريم، 30 كفاية وأخف من 60
X264_PRESET = _env("X264_PRESET", "veryfast")   # لو speed نزلت عن 1x رجّعها ultrafast
GOP = FPS * 2                                   # keyframe كل ثانيتين

CHECK_INTERVAL = _env("CHECK_INTERVAL", 1, float)  # فحص رابط البث كل ثانية (ارفعها لو تيك توك بدأ يبطّأ)
LIVE_END_COOLDOWN = 15   # ثواني انتظار بعد إعلان تيك توك نهاية البث
RESTART_DELAY = 1        # ثواني بين نهاية ffmpeg وإعادة المحاولة
CRASH_DELAY = 10         # لو ffmpeg وقع بسرعة (أقل من 10 ثواني) استنى أكتر


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def build_ffmpeg_cmd(stream_url):
    return [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-loglevel", "warning",
        "-stats",
        "-stats_period", "15",

        # Network resilience
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_on_network_error", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+discardcorrupt",

        "-i", stream_url,

        # Streams: video + audio (audio optional)
        "-map", "0:v:0",
        "-map", "0:a:0?",

        # Video: true CBR (no scale = same resolution as source)
        "-c:v", "libx264",
        "-preset", X264_PRESET,
        "-profile:v", "high",
        "-pix_fmt", "yuv420p",
        "-b:v", VIDEO_BITRATE,
        "-minrate", VIDEO_BITRATE,
        "-maxrate", VIDEO_BITRATE,
        "-bufsize", VIDEO_BITRATE,
        "-x264-params", "nal-hrd=cbr:force-cfr=1",

        # Constant frame rate + fixed keyframes
        "-r", str(FPS),
        "-fps_mode", "cfr",
        "-g", str(GOP),
        "-keyint_min", str(GOP),
        "-sc_threshold", "0",

        # Audio
        "-c:a", "aac",
        "-b:a", AUDIO_BITRATE,
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",

        "-max_muxing_queue_size", "1024",
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

    def stop_ffmpeg(self):
        proc = self.proc
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def on_live_ended(self):
        self.live_ended_event.set()
        self.stop_ffmpeg()

    def get_direct_stream_url(self):
        try:
            streams = self.session.streams(TIKTOK_URL)
            if streams:
                chosen = streams.get("best") or list(streams.values())[0]
                return getattr(chosen, "url", None)
        except Exception:
            pass
        return None

    def run(self):
        log(f"بدء النقل: CBR {VIDEO_BITRATE} | {FPS}fps | preset={X264_PRESET} | صوت {AUDIO_BITRATE}")
        log(f"الحساب المستهدف: {TIKTOK_URL}")
        log(f"فحص الرابط كل {CHECK_INTERVAL:g} ثانية")

        if TikTokLiveClient and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()
        else:
            log(f"تنبيه: مراقب نهاية البث متوقف ({_TIKTOKLIVE_IMPORT_ERROR})")

        waiting_logged = False

        while not self.stop_event.is_set():
            stream_url = self.get_direct_stream_url()

            if not stream_url:
                if not waiting_logged:
                    log("البث مقفول حالياً... بفحص كل ثانية لحد ما يفتح.")
                    waiting_logged = True
                self.stop_event.wait(CHECK_INTERVAL)
                continue
            waiting_logged = False

            log("تم التقاط رابط البث المباشر! تشغيل FFmpeg...")
            self.live_ended_event.clear()
            started = time.time()

            self.proc = subprocess.Popen(build_ffmpeg_cmd(stream_url))

            while not self.stop_event.is_set() and not self.live_ended_event.is_set():
                if self.proc.poll() is not None:
                    log("عملية FFmpeg انتهت (البث توقف من المصدر).")
                    break
                self.stop_event.wait(1)

            self.stop_ffmpeg()
            self.proc = None

            if self.stop_event.is_set():
                break

            if self.live_ended_event.is_set():
                self.stop_event.wait(LIVE_END_COOLDOWN)
            elif time.time() - started < 10:
                log("FFmpeg وقع بسرعة، استنى شوية قبل المحاولة.")
                self.stop_event.wait(CRASH_DELAY)
            else:
                self.stop_event.wait(RESTART_DELAY)

        log("تم الإيقاف.")


def main():
    restreamer = Restreamer()

    def handler(sig, frame):
        restreamer.stop_event.set()
        restreamer.stop_ffmpeg()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    restreamer.run()


if __name__ == "__main__":
    main()
