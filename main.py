import os
import subprocess
import time
import signal
import sys

TIKTOK_URL = "https://www.tiktok.com/@d.shakertawfiqalaroury/live"
YOUTUBE_RTMP = "rtmp://a.rtmp.youtube.com/live2/vksy-du6k-8cxj-u7kh-7mfm"

CHECK_INTERVAL_OFFLINE = 15       # وقت الفحص عندما يكون البث متوقفاً تماماً (15 ثانية)
RECONNECT_INTERVAL_ONLINE = 0.1   # إعادة الاتصال فوراً عند التقطيع (0.1 ثانية فقط)

STREAMLINK_CMD = [
    "streamlink",
    "--hls-live-edge", "5",
    "--ringbuffer-size", "512M",
    "--retry-streams", "0",
    "--retry-max", "0",
    "--stream-segment-attempts", "20",
    "--stream-segment-timeout", "10",
    "--stream-timeout", "30",
    "--stdout",
    TIKTOK_URL,
    "best"
]

FFMPEG_CMD = [
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
    YOUTUBE_RTMP
]

streamlink_process = None
ffmpeg_process = None


def stop_process(process):
    if process and process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=3)
        except Exception:
            try:
                process.kill()
                process.wait(timeout=2)
            except Exception:
                pass


def cleanup():
    global streamlink_process, ffmpeg_process
    stop_process(ffmpeg_process)
    stop_process(streamlink_process)
    streamlink_process = None
    ffmpeg_process = None


def signal_handler(sig, frame):
    print("\n[SYSTEM] Stopped by Railway / User.")
    cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


print("========================================")
print("TikTok Live Monitor & Auto-Restreamer")
print("Status: RUNNING & LISTENING...")
print("========================================\n")

while True:
    try:
        cleanup()
        
        streamlink_process = subprocess.Popen(
            STREAMLINK_CMD,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )

        time.sleep(2)
        
        if streamlink_process.poll() is not None:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Stream is OFFLINE. Re-checking in {CHECK_INTERVAL_OFFLINE}s...")
            cleanup()
            time.sleep(CHECK_INTERVAL_OFFLINE)
            continue

        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Stream ONLINE! Starting Restream to YouTube...")
        
        ffmpeg_process = subprocess.Popen(
            FFMPEG_CMD,
            stdin=streamlink_process.stdout,
            stdout=None,
            stderr=None,
            bufsize=0
        )

        streamlink_process.stdout.close()

        ffmpeg_return = ffmpeg_process.wait()
        
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Stream connection dropped (FFmpeg exit code: {ffmpeg_return}).")

    except KeyboardInterrupt:
        print("\nStopping...")
        cleanup()
        break

    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")

    finally:
        cleanup()

    print(f"Instant reconnecting in {RECONNECT_INTERVAL_ONLINE}s...\n")
    time.sleep(RECONNECT_INTERVAL_ONLINE)
