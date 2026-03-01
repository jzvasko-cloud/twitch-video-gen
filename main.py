"""
Twitch Content Pipeline — Video Generator Service
Replaces Pictory.ai. Runs free on Render.com.

Flow: script + topic → TTS audio → Pexels B-roll → FFmpeg assembly → MP4
"""

from flask import Flask, jsonify, send_file, request, abort
from gtts import gTTS
import requests, subprocess, json, os, uuid, threading, shutil, re, time

app = Flask(__name__)

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")  # free at pexels.com/api
OUTPUT_DIR = "/tmp/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

jobs = {}  # job_id -> {"status": str, "path": str}
jobs_lock = threading.Lock()


# ─── Helpers ────────────────────────────────────────────────────────────────

def get_audio_duration(path):
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", path],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    for stream in data.get("streams", []):
        if "duration" in stream:
            return float(stream["duration"])
    raise RuntimeError("Could not determine audio duration")


def generate_srt(script, duration):
    """Split script into timed caption chunks and produce SRT content."""
    # Clean script: remove stage directions like (PAUSE), asterisks, etc.
    clean = re.sub(r'\([^)]*\)', '', script).strip()
    words = clean.split()

    WORDS_PER_CHUNK = 5
    chunks = []
    for i in range(0, len(words), WORDS_PER_CHUNK):
        chunks.append(" ".join(words[i:i + WORDS_PER_CHUNK]))

    if not chunks:
        chunks = [clean[:80]]

    time_per_chunk = duration / len(chunks)
    srt = ""
    for i, chunk in enumerate(chunks):
        start = i * time_per_chunk
        end = min((i + 1) * time_per_chunk, duration)
        srt += f"{i + 1}\n{fmt_time(start)} --> {fmt_time(end)}\n{chunk}\n\n"
    return srt


def fmt_time(sec):
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    ms = int((s - int(s)) * 1000)
    return f"{h:02d}:{m:02d}:{int(s):02d},{ms:03d}"


def get_pexels_video(query, fallback="gaming"):
    """Search Pexels for a vertical (portrait) video. Falls back to 'gaming'."""
    def search(q):
        resp = requests.get(
            "https://api.pexels.com/videos/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": q, "orientation": "portrait", "per_page": 10, "size": "medium"},
            timeout=15
        )
        if resp.status_code != 200:
            return None
        videos = resp.json().get("videos", [])
        for video in videos:
            files = sorted(video.get("video_files", []), key=lambda f: f.get("width", 0))
            for f in files:
                # prefer portrait-oriented HD files
                if f.get("height", 0) >= f.get("width", 0) and f.get("width", 0) >= 480:
                    return f["link"]
            # fallback: any file
            if files:
                return files[0]["link"]
        return None

    url = search(query) or search(fallback)
    return url


def download_file(url, dest):
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


def assemble_video(bg_path, audio_path, srt_path, output_path, duration):
    """
    FFmpeg pipeline:
      1. Loop background video to cover audio duration
      2. Scale + crop to 9:16 (1080x1920)
      3. Burn in SRT captions (bold white text, black outline)
      4. Mix in audio
    """
    # Escape srt path for FFmpeg filter (colons on Linux are fine; wrap in quotes)
    srt_escaped = srt_path.replace("\\", "/").replace(":", "\\:")

    vf = (
        "scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        f"subtitles='{srt_escaped}':force_style='"
        "FontName=Arial,"
        "FontSize=24,"
        "Bold=1,"
        "PrimaryColour=&H00FFFFFF&,"   # white text
        "OutlineColour=&H00000000&,"   # black outline
        "Outline=3,"
        "Shadow=0,"
        "Alignment=2,"                 # centered bottom
        "MarginV=120"
        "'"
    )

    cmd = [
        "ffmpeg", "-y",
        "-stream_loop", "-1",          # loop bg video
        "-i", bg_path,
        "-i", audio_path,
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-t", str(duration + 0.5),    # tiny buffer at end
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        output_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg failed:\n{result.stderr[-2000:]}")


# ─── Background processing ───────────────────────────────────────────────────

def process_video_job(job_id, script, topic):
    work_dir = f"/tmp/job_{job_id}"
    os.makedirs(work_dir, exist_ok=True)
    output_path = f"{OUTPUT_DIR}/{job_id}.mp4"

    try:
        with jobs_lock:
            jobs[job_id]["status"] = "generating_audio"

        # 1. TTS
        audio_path = f"{work_dir}/audio.mp3"
        tts = gTTS(text=script, lang="en", slow=False)
        tts.save(audio_path)

        duration = get_audio_duration(audio_path)

        with jobs_lock:
            jobs[job_id]["status"] = "generating_captions"

        # 2. SRT captions
        srt_content = generate_srt(script, duration)
        srt_path = f"{work_dir}/captions.srt"
        with open(srt_path, "w", encoding="utf-8") as f:
            f.write(srt_content)

        with jobs_lock:
            jobs[job_id]["status"] = "fetching_footage"

        # 3. Pexels B-roll
        keyword = " ".join(topic.split()[:4])  # first 4 words as search query
        video_url = get_pexels_video(keyword)
        if not video_url:
            raise RuntimeError(f"No Pexels video found for query: {keyword}")

        bg_path = f"{work_dir}/bg.mp4"
        download_file(video_url, bg_path)

        with jobs_lock:
            jobs[job_id]["status"] = "assembling_video"

        # 4. FFmpeg
        assemble_video(bg_path, audio_path, srt_path, output_path, duration)

        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["path"] = output_path

    except Exception as e:
        with jobs_lock:
            jobs[job_id]["status"] = f"error: {str(e)}"

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/generate", methods=["POST"])
def generate():
    """
    POST /generate
    Body: { "script": "...", "topic": "..." }
    Returns: { "job_id": "abc123", "status_url": "/status/abc123", "download_url": "/download/abc123" }
    """
    data = request.get_json(force=True) or {}
    script = (data.get("script") or "").strip()
    topic = (data.get("topic") or "gaming").strip()

    if not script:
        return jsonify({"error": "script is required"}), 400

    if not PEXELS_API_KEY:
        return jsonify({"error": "PEXELS_API_KEY env var not set"}), 500

    job_id = uuid.uuid4().hex[:10]
    with jobs_lock:
        jobs[job_id] = {"status": "queued", "path": None}

    t = threading.Thread(target=process_video_job, args=(job_id, script, topic), daemon=True)
    t.start()

    return jsonify({
        "job_id": job_id,
        "status_url": f"/status/{job_id}",
        "download_url": f"/download/{job_id}"
    }), 202


@app.route("/status/<job_id>")
def status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify({"job_id": job_id, "status": job["status"]})


@app.route("/download/<job_id>")
def download(job_id):
    """Blocks (polls internally) until video is ready, then streams it."""
    for _ in range(120):   # wait up to 4 minutes
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            abort(404)
        status = job["status"]
        if status == "done" and job["path"] and os.path.exists(job["path"]):
            return send_file(job["path"], mimetype="video/mp4", as_attachment=True,
                             download_name=f"twitch_video_{job_id}.mp4")
        if status.startswith("error"):
            return jsonify({"error": status}), 500
        time.sleep(2)

    return jsonify({"error": "timeout waiting for video"}), 504


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
