import os
import sys
import time
import signal
import subprocess
import threading
from datetime import datetime

# ================== SETTINGS ==================

TIKTOK_URL = os.getenv(
    "TIKTOK_URL",
    "https://www.tiktok.com/@d.shakertawfiqalaroury/live"
).strip()

YOUTUBE_RTMP = os.getenv(
    "YOUTUBE_RTMP",
    "rtmp://a.rtmp.youtube.com/live2/rvz8-2zmy-jxt4-hvdd-a19k"
).strip()

CHECK_INTERVAL_OFFLINE = 15
RETRY_DELAY = 5

stop_event = threading.Event()
streamlink_process = None
ffmpeg_process = None


# ================== LOGGING ==================

def log(message):
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}",
        flush=True
    )


# ================== PROCESS MANAGEMENT ==================

def stop_process(process):
    if process is None or process.poll() is not None:
        return

    try:
        process.terminate()
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
            process.wait(timeout=3)
        except Exception:
            pass
    except Exception as exc:
        log(f"Process cleanup warning: {exc}")


def cleanup():
    global streamlink_process, ffmpeg_process

    stop_process(ffmpeg_process)
    ffmpeg_process = None

    stop_process(streamlink_process)
    streamlink_process = None


def signal_handler(sig, frame):
    log("Shutdown signal received. Cleaning up...")
    stop_event.set()
    cleanup()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ================== STREAMLINK ==================

def make_streamlink_command():
    return [
        "streamlink",
        "--loglevel", "warning",
        "--hls-live-edge", "4",
        "--ringbuffer-size", "128M",
        "--retry-streams", "5",
        "--retry-max", "10",
        "--stream-segment-attempts", "5",
        "--stream-segment-timeout", "10",
        "--stream-timeout", "20",
        "--stdout",
        TIKTOK_URL,
        "best"
    ]


# ================== FFMPEG ==================

def make_ffmpeg_command():
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-stats_period", "10",
        "-nostdin",

        "-thread_queue_size", "1024",
        "-i", "pipe:0",

        "-map", "0:v:0",
        "-c:v", "copy",

        "-map", "0:a:0?",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",

        "-fflags", "+genpts+discardcorrupt+nobuffer+fastseek",
        "-avoid_negative_ts", "make_zero",
        "-use_wallclock_as_timestamps", "1",
        "-tune", "zerolatency",

        "-flvflags", "no_duration_filesize",
        "-f", "flv",
        YOUTUBE_RTMP
    ]


# ================== START STREAM ==================

def start_stream():
    global streamlink_process, ffmpeg_process

    if stop_event.is_set():
        return False

    log("Connecting to TikTok Live...")

    streamlink_process = subprocess.Popen(
        make_streamlink_command(),
        stdout=subprocess.PIPE,
        stderr=None,
        bufsize=0
    )

    for _ in range(4):
        if stop_event.is_set():
            return False

        if streamlink_process.poll() is not None:
            log("Streamlink exited. TikTok may be offline.")
            return False

        time.sleep(0.5)

    if stop_event.is_set():
        return False

    log("Starting FFmpeg -> YouTube RTMP...")

    ffmpeg_process = subprocess.Popen(
        make_ffmpeg_command(),
        stdin=streamlink_process.stdout,
        stdout=subprocess.DEVNULL,
        stderr=None,
        bufsize=0
    )

    streamlink_process.stdout.close()

    log("Relay started. Video COPY; audio AAC 128k.")

    while not stop_event.is_set():
        sl_code = streamlink_process.poll()
        ff_code = ffmpeg_process.poll()

        if sl_code is not None:
            log(f"Streamlink stopped (exit code {sl_code}).")
            return False

        if ff_code is not None:
            log(f"FFmpeg stopped (exit code {ff_code}).")
            return False

        time.sleep(1)

    return False


# ================== MAIN LOOP ==================

def main():
    if not TIKTOK_URL:
        log("ERROR: TIKTOK_URL is empty.")
        sys.exit(1)

    if not YOUTUBE_RTMP:
        log("ERROR: Set YOUTUBE_RTMP environment variable.")
        sys.exit(1)

    log("TikTok -> YouTube Relay Started")
    log("Video encoding: OFF (COPY)")
    log("Audio encoding: AAC 128k")
    log("Automatic reconnection: ON")

    while not stop_event.is_set():
        started_at = time.monotonic()

        try:
            cleanup()
            start_stream()

        except Exception as exc:
            log(f"Unexpected error: {exc}")

        finally:
            cleanup()

        if stop_event.is_set():
            break

        elapsed = time.monotonic() - started_at

        if elapsed > 60:
            delay = RETRY_DELAY
            log(f"Stream disconnected. Retrying in {delay}s...")
        else:
            delay = CHECK_INTERVAL_OFFLINE
            log(f"Stream unavailable. Checking again in {delay}s...")

        stop_event.wait(delay)

    cleanup()
    log("Relay stopped cleanly.")


if __name__ == "__main__":
    main()
