"""
ClipLoreTV Auto-Post Service
Flask backend for automated Twitch clip → TikTok video pipeline.
Endpoints: /generate-script, /fetch-clips, /assemble-video, /post-to-tiktok
"""

import os
import json
import threading
import time
import tempfile
import subprocess
import urllib.request
import uuid
from pathlib import Path

import requests
from flask import Flask, request, jsonify
from gtts import gTTS

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://twitch-video-gen.onrender.com")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TIKTOK_CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
TIKTOK_CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")

TEMP_DIR = Path(tempfile.gettempdir()) / "cliplore"
TEMP_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Keep-alive (Render free tier spin-down prevention)
# ---------------------------------------------------------------------------
_keep_alive_started = False


def keep_alive():
    while True:
        time.sleep(300)
        try:
            urllib.request.urlopen(f"{SELF_URL}/health", timeout=10)
        except Exception:
            pass


@app.before_request
def _start_keep_alive():
    global _keep_alive_started
    if not _keep_alive_started:
        _keep_alive_started = True
        threading.Thread(target=keep_alive, daemon=True).start()


# ---------------------------------------------------------------------------
# Static / utility routes
# ---------------------------------------------------------------------------
@app.route("/")
def home():
    return "ClipLoreTV Auto-Post Service is running."


@app.route("/health")
def health():
    return "ok", 200


@app.route("/tiktokgf5YuUb7tBa2vRZZ306I0dDfa1eCsk2Q.txt")
def tiktok_verify():
    return (
        "tiktok-developers-site-verification=gf5YuUb7tBa2vRZZ306I0dDfa1eCsk2Q",
        200,
        {"Content-Type": "text/plain; charset=utf-8"},
    )


@app.route("/tos")
def tos():
    return """<!DOCTYPE html>
<html><head><title>Terms of Service - ClipLoreTV</title></head>
<body>
<h1>Terms of Service</h1>
<p>Last updated: 2025-03-01</p>
<p>ClipLoreTV Auto-Post is a personal automation tool that posts short gaming
highlight videos to TikTok from Twitch clips.</p>
<h2>Usage</h2>
<p>This service is for personal, non-commercial use by the app developer.
No user data is collected or stored beyond what is required by the TikTok API
for authentication and video posting.</p>
<h2>Content</h2>
<p>All posted content consists of short highlight clips sourced from publicly
available Twitch streams, with AI-generated commentary overlaid.</p>
<h2>Contact</h2>
<p>For questions, contact the developer via the TikTok developer portal.</p>
</body></html>"""


@app.route("/privacy")
def privacy():
    return """<!DOCTYPE html>
<html><head><title>Privacy Policy - ClipLoreTV</title></head>
<body>
<h1>Privacy Policy</h1>
<p>Last updated: 2025-03-01</p>
<p>ClipLoreTV Auto-Post does not collect, store, or share any personal data
from end users.</p>
<h2>Data We Access</h2>
<p>The only data accessed is the TikTok API token required for posting videos,
which is stored securely and never shared with third parties.</p>
<h2>Third-Party Services</h2>
<p>This app uses the TikTok Content Posting API solely to upload video content.
No analytics or tracking services are used.</p>
<h2>Contact</h2>
<p>For privacy questions, contact the developer via the TikTok developer portal.</p>
</body></html>"""


# ===================================================================
#  ENDPOINT 1 — /generate-script
# ===================================================================
CLIPLORE_SYSTEM_PROMPT = """You are a short-form video scriptwriter for ClipLoreTV, a TikTok channel that posts AI-narrated gaming highlight commentary over Twitch clip footage. Your scripts are engineered for maximum watch-through rate, comments, and shares on TikTok.

SCRIPT STRUCTURE (strict — follow every time):

[HOOK] — First 3 seconds. The single most controversial, shocking, or curiosity-gap statement in the entire script. This line determines whether 90% of viewers stay or leave. Must create an immediate urge to keep watching OR to comment in disagreement.

[VISUAL: describe what clip footage should show during this section]

[BUILD] — Next 15–20 seconds. 3–4 rapid-fire beats that escalate the tension or argument. Each beat is 1–2 sentences max. Use pattern interrupts between beats:
  - Pattern interrupt options: rhetorical question, "but here's the thing," dramatic pause cue "[PAUSE 0.5s]", tone shift, or direct audience challenge

[VISUAL: describe clip footage for each beat]

[PAYOFF] — Next 10–15 seconds. Deliver the main point, reveal, or ranking. This is where you cash the check the hook wrote. Be specific — names, events, data.

[VISUAL: describe the key clip moment that matches the payoff]

[CTA] — Final 5 seconds. Comment bait that splits the audience. Never generic. Always force a choice:
  - "S tier or D tier? Comment now."
  - "Who was actually right? Drop a name."
  - "Am I wrong? Prove it below."

VOICE RULES:
- Total length: 120–180 words (40–60 second TikTok)
- Tone: confident, slightly cocky, like a friend who watches too much Twitch
- No "Hey guys," no "Welcome back," no intro fluff — open cold
- Short sentences. Punchy. Never more than 15 words per sentence.
- State opinions as facts. "The truth is..." not "I think..."
- Use present tense for drama: "He walks on stage and the chat goes insane"
- Reference specific moments, usernames, or events — never be vague

OUTPUT FORMAT:
Return ONLY the script text. Include [VISUAL] tags inline. Include [PAUSE] tags where dramatic pauses should go. No headers, no notes, no meta-commentary."""


@app.route("/generate-script", methods=["POST"])
def generate_script():
    """Generate a TikTok commentary script using Claude API.

    JSON body:
      topic (str, required): The topic/prompt for the script.
      pillar (str, optional): One of tier_list | hot_take | nostalgia | accountability | rise_and_fall
    Returns:
      { script: str, word_count: int }
    """
    data = request.get_json(force=True)
    topic = data.get("topic")
    if not topic:
        return jsonify({"error": "topic is required"}), 400

    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 500

    # Build the user message with optional pillar framing
    pillar = data.get("pillar", "")
    pillar_prefix = {
        "tier_list": "Write a tier list script ranking ",
        "hot_take": "Write a hot take script about ",
        "nostalgia": "Write a nostalgia script comparing ",
        "accountability": "Write an accountability script about ",
        "rise_and_fall": "Write a rise-and-fall script about ",
    }
    user_msg = pillar_prefix.get(pillar, "") + topic

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "system": CLIPLORE_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        script_text = result["content"][0]["text"]
        word_count = len(script_text.split())
        return jsonify({"script": script_text, "word_count": word_count})
    except requests.RequestException as e:
        return jsonify({"error": f"Claude API error: {str(e)}"}), 502


# ===================================================================
#  ENDPOINT 2 — /fetch-clips
# ===================================================================
_twitch_token_cache = {"token": None, "expires": 0}


def _get_twitch_token():
    """Get or refresh Twitch app access token."""
    now = time.time()
    if _twitch_token_cache["token"] and now < _twitch_token_cache["expires"]:
        return _twitch_token_cache["token"]

    resp = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    _twitch_token_cache["token"] = data["access_token"]
    _twitch_token_cache["expires"] = now + data.get("expires_in", 3600) - 60
    return data["access_token"]


def _get_broadcaster_id(login):
    """Resolve a Twitch login name to broadcaster ID."""
    token = _get_twitch_token()
    resp = requests.get(
        "https://api.twitch.tv/helix/users",
        headers={
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}",
        },
        params={"login": login},
        timeout=10,
    )
    resp.raise_for_status()
    users = resp.json().get("data", [])
    if not users:
        raise ValueError(f"Twitch user '{login}' not found")
    return users[0]["id"]


@app.route("/fetch-clips", methods=["POST"])
def fetch_clips():
    """Fetch top Twitch clips for given streamers.

    JSON body:
      streamers (list[str], required): Twitch login names e.g. ["xqc", "pokimane"]
      count (int, optional): Clips per streamer (default 5, max 20)
    Returns:
      { clips: [ { streamer, title, url, thumbnail_url, view_count, duration } ] }
    """
    data = request.get_json(force=True)
    streamers = data.get("streamers", [])
    count = min(data.get("count", 5), 20)

    if not streamers:
        return jsonify({"error": "streamers list is required"}), 400
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return jsonify({"error": "Twitch credentials not configured"}), 500

    all_clips = []
    try:
        token = _get_twitch_token()
        for streamer in streamers:
            try:
                bid = _get_broadcaster_id(streamer)
                resp = requests.get(
                    "https://api.twitch.tv/helix/clips",
                    headers={
                        "Client-ID": TWITCH_CLIENT_ID,
                        "Authorization": f"Bearer {token}",
                    },
                    params={"broadcaster_id": bid, "first": count},
                    timeout=10,
                )
                resp.raise_for_status()
                for clip in resp.json().get("data", []):
                    thumb = clip.get("thumbnail_url", "")
                    download_url = thumb.split("-preview-")[0] + ".mp4" if "-preview-" in thumb else ""
                    all_clips.append(
                        {
                            "streamer": streamer,
                            "title": clip.get("title", ""),
                            "url": clip.get("url", ""),
                            "download_url": download_url,
                            "thumbnail_url": thumb,
                            "view_count": clip.get("view_count", 0),
                            "duration": clip.get("duration", 0),
                        }
                    )
            except Exception as e:
                all_clips.append({"streamer": streamer, "error": str(e)})

        return jsonify({"clips": all_clips})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ===================================================================
#  ENDPOINT 3 — /assemble-video
# ===================================================================
@app.route("/assemble-video", methods=["POST"])
def assemble_video():
    """Assemble a TikTok-ready video from script + clips.

    JSON body:
      script (str, required): The voiceover script text.
      clip_urls (list[str], required): Direct MP4 URLs of Twitch clips.
    Returns:
      { video_path: str, duration: float }
    """
    data = request.get_json(force=True)
    script = data.get("script", "")
    clip_urls = data.get("clip_urls", [])

    if not script or not clip_urls:
        return jsonify({"error": "script and clip_urls are required"}), 400

    job_id = uuid.uuid4().hex[:10]
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    try:
        import re

        clean_script = re.sub(r"\[VISUAL:[^\]]*\]", "", script)
        clean_script = re.sub(r"\[PAUSE[^\]]*\]", "", clean_script)
        clean_script = re.sub(r"\[(HOOK|BUILD|PAYOFF|CTA)\]", "", clean_script)
        clean_script = re.sub(r"\n{3,}", "\n\n", clean_script).strip()

        tts_path = job_dir / "voiceover.mp3"
        tts = gTTS(text=clean_script, lang="en", tld="co.uk")
        tts.save(str(tts_path))

        probe = subprocess.run(
            [
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_format", str(tts_path),
            ],
            capture_output=True, text=True, timeout=15,
        )
        vo_duration = float(json.loads(probe.stdout)["format"]["duration"])

        clip_paths = []
        for i, url in enumerate(clip_urls[:5]):
            clip_path = job_dir / f"clip_{i}.mp4"
            try:
                r = requests.get(url, timeout=30, stream=True)
                r.raise_for_status()
                with open(clip_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                clip_paths.append(clip_path)
            except Exception:
                continue

        if not clip_paths:
            return jsonify({"error": "Could not download any clips"}), 400

        scaled_clips = []
        for i, cp in enumerate(clip_paths):
            scaled = job_dir / f"scaled_{i}.mp4"
            subprocess.run(
                [
                    "ffmpeg", "-y", "-i", str(cp),
                    "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1",
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
                    "-an",
                    "-t", str(vo_duration / len(clip_paths) + 1),
                    str(scaled),
                ],
                capture_output=True, timeout=120,
            )
            if scaled.exists():
                scaled_clips.append(scaled)

        if not scaled_clips:
            return jsonify({"error": "FFmpeg scaling failed"}), 500

        concat_list = job_dir / "concat.txt"
        with open(concat_list, "w") as f:
            for sc in scaled_clips:
                f.write(f"file '{sc}'\n")

        concat_video = job_dir / "concat.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", str(concat_list),
                "-c", "copy", "-t", str(vo_duration),
                str(concat_video),
            ],
            capture_output=True, timeout=120,
        )

        final_video = job_dir / "final.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(concat_video),
                "-i", str(tts_path),
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest",
                "-movflags", "+faststart",
                str(final_video),
            ],
            capture_output=True, timeout=120,
        )

        if not final_video.exists():
            return jsonify({"error": "Final video assembly failed"}), 500

        return jsonify({
            "video_path": str(final_video),
            "job_id": job_id,
            "duration": vo_duration,
        })

    except Exception as e:
        return jsonify({"error": f"Assembly failed: {str(e)}"}), 500


# ===================================================================
#  ENDPOINT 4 — /post-to-tiktok
# ===================================================================
@app.route("/post-to-tiktok", methods=["POST"])
def post_to_tiktok():
    """Upload a video to TikTok via Content Posting API.

    JSON body:
      video_path (str, required): Path to the assembled video file on disk.
      access_token (str, required): TikTok user access token (from OAuth flow).
      description (str, optional): Caption/description with hashtags.
    Returns:
      { publish_id: str, status: str }
    """
    data = request.get_json(force=True)
    video_path = data.get("video_path", "")
    access_token = data.get("access_token", "")
    description = data.get("description", "")

    if not video_path or not access_token:
        return jsonify({"error": "video_path and access_token are required"}), 400

    video_file = Path(video_path)
    if not video_file.exists():
        return jsonify({"error": "Video file not found"}), 404

    file_size = video_file.stat().st_size

    try:
        init_resp = requests.post(
            "https://open.tiktokapis.com/v2/post/publish/video/init/",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "post_info": {
                    "title": description[:150] if description else "ClipLoreTV",
                    "privacy_level": "PUBLIC_TO_EVERYONE",
                    "disable_duet": False,
                    "disable_comment": False,
                    "disable_stitch": False,
                },
                "source_info": {
                    "source": "FILE_UPLOAD",
                    "video_size": file_size,
                    "chunk_size": file_size,
                    "total_chunk_count": 1,
                },
            },
            timeout=30,
        )
        init_resp.raise_for_status()
        init_data = init_resp.json()

        if init_data.get("error", {}).get("code") != "ok":
            return jsonify({"error": f"TikTok init failed: {init_data}"}), 502

        upload_url = init_data["data"]["upload_url"]
        publish_id = init_data["data"]["publish_id"]

        with open(video_file, "rb") as vf:
            video_bytes = vf.read()

        upload_resp = requests.put(
            upload_url,
            headers={
                "Content-Type": "video/mp4",
                "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
            },
            data=video_bytes,
            timeout=120,
        )
        upload_resp.raise_for_status()

        return jsonify({
            "publish_id": publish_id,
            "status": "uploaded",
            "message": "Video uploaded to TikTok. Check Creator Center for processing status.",
        })

    except requests.RequestException as e:
        return jsonify({"error": f"TikTok API error: {str(e)}"}), 502


# ===================================================================
#  BONUS — /pipeline (full end-to-end: topic → posted video)
# ===================================================================
@app.route("/pipeline", methods=["POST"])
def pipeline():
    """Run the full pipeline: topic -> script -> clips -> video -> TikTok post.

    JSON body:
      topic (str, required): Topic for the script.
      pillar (str, optional): Content pillar.
      streamers (list[str], required): Twitch streamers to pull clips from.
      tiktok_access_token (str, required): TikTok OAuth token.
      description (str, optional): TikTok caption with hashtags.
    Returns:
      Full pipeline result with script, clips, video path, and TikTok publish ID.
    """
    data = request.get_json(force=True)
    topic = data.get("topic", "")
    pillar = data.get("pillar", "")
    streamers = data.get("streamers", [])
    tiktok_token = data.get("tiktok_access_token", "")
    description = data.get("description", "")

    if not topic or not streamers or not tiktok_token:
        return jsonify({"error": "topic, streamers, and tiktok_access_token are required"}), 400

    results = {"steps": {}}

    # Step 1: Generate script
    try:
        pillar_prefix = {
            "tier_list": "Write a tier list script ranking ",
            "hot_take": "Write a hot take script about ",
            "nostalgia": "Write a nostalgia script comparing ",
            "accountability": "Write an accountability script about ",
            "rise_and_fall": "Write a rise-and-fall script about ",
        }
        user_msg = pillar_prefix.get(pillar, "") + topic

        script_resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "system": CLIPLORE_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=30,
        )
        script_resp.raise_for_status()
        script_text = script_resp.json()["content"][0]["text"]
        results["steps"]["script"] = {"status": "ok", "script": script_text}
    except Exception as e:
        return jsonify({"error": f"Script generation failed: {str(e)}", "steps": results["steps"]}), 500

    # Step 2: Fetch clips
    try:
        token = _get_twitch_token()
        clip_urls = []
        for streamer in streamers:
            bid = _get_broadcaster_id(streamer)
            resp = requests.get(
                "https://api.twitch.tv/helix/clips",
                headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
                params={"broadcaster_id": bid, "first": 3},
                timeout=10,
            )
            resp.raise_for_status()
            for clip in resp.json().get("data", []):
                thumb = clip.get("thumbnail_url", "")
                if "-preview-" in thumb:
                    clip_urls.append(thumb.split("-preview-")[0] + ".mp4")
        results["steps"]["clips"] = {"status": "ok", "count": len(clip_urls)}
    except Exception as e:
        return jsonify({"error": f"Clip fetch failed: {str(e)}", "steps": results["steps"]}), 500

    if not clip_urls:
        return jsonify({"error": "No clips found", "steps": results["steps"]}), 400

    # Step 3: Assemble video
    try:
        import re
        clean = re.sub(r"\[VISUAL:[^\]]*\]", "", script_text)
        clean = re.sub(r"\[PAUSE[^\]]*\]", "", clean)
        clean = re.sub(r"\[(HOOK|BUILD|PAYOFF|CTA)\]", "", clean)
        clean = re.sub(r"\n{3,}", "\n\n", clean).strip()

        job_id = uuid.uuid4().hex[:10]
        job_dir = TEMP_DIR / job_id
        job_dir.mkdir(exist_ok=True)

        tts_path = job_dir / "voiceover.mp3"
        tts = gTTS(text=clean, lang="en", tld="co.uk")
        tts.save(str(tts_path))

        probe = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(tts_path)],
            capture_output=True, text=True, timeout=15,
        )
        vo_duration = float(json.loads(probe.stdout)["format"]["duration"])

        clip_paths = []
        for i, url in enumerate(clip_urls[:5]):
            cp = job_dir / f"clip_{i}.mp4"
            try:
                r = requests.get(url, timeout=30, stream=True)
                r.raise_for_status()
                with open(cp, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                clip_paths.append(cp)
            except Exception:
                continue

        scaled = []
        for i, cp in enumerate(clip_paths):
            sp = job_dir / f"scaled_{i}.mp4"
            subprocess.run(
                [
                    "ffmpeg", "-y", "-i", str(cp),
                    "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1",
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-an",
                    "-t", str(vo_duration / max(len(clip_paths), 1) + 1),
                    str(sp),
                ],
                capture_output=True, timeout=120,
            )
            if sp.exists():
                scaled.append(sp)

        concat_list = job_dir / "concat.txt"
        with open(concat_list, "w") as f:
            for s in scaled:
                f.write(f"file '{s}'\n")

        concat_vid = job_dir / "concat.mp4"
        subprocess.run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_list),
             "-c", "copy", "-t", str(vo_duration), str(concat_vid)],
            capture_output=True, timeout=120,
        )

        final = job_dir / "final.mp4"
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(concat_vid), "-i", str(tts_path),
             "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
             "-c:a", "aac", "-b:a", "128k", "-shortest", "-movflags", "+faststart",
             str(final)],
            capture_output=True, timeout=120,
        )

        if not final.exists():
            return jsonify({"error": "Video assembly failed", "steps": results["steps"]}), 500

        results["steps"]["video"] = {"status": "ok", "path": str(final), "duration": vo_duration}
    except Exception as e:
        return jsonify({"error": f"Assembly failed: {str(e)}", "steps": results["steps"]}), 500

    # Step 4: Post to TikTok
    try:
        file_size = final.stat().st_size
        init_resp = requests.post(
            "https://open.tiktokapis.com/v2/post/publish/video/init/",
            headers={
                "Authorization": f"Bearer {tiktok_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "post_info": {
                    "title": description[:150] if description else "ClipLoreTV",
                    "privacy_level": "PUBLIC_TO_EVERYONE",
                    "disable_duet": False,
                    "disable_comment": False,
                    "disable_stitch": False,
                },
                "source_info": {
                    "source": "FILE_UPLOAD",
                    "video_size": file_size,
                    "chunk_size": file_size,
                    "total_chunk_count": 1,
                },
            },
            timeout=30,
        )
        init_resp.raise_for_status()
        init_data = init_resp.json()

        upload_url = init_data["data"]["upload_url"]
        publish_id = init_data["data"]["publish_id"]

        with open(final, "rb") as vf:
            video_bytes = vf.read()

        requests.put(
            upload_url,
            headers={
                "Content-Type": "video/mp4",
                "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
            },
            data=video_bytes,
            timeout=120,
        ).raise_for_status()

        results["steps"]["tiktok"] = {"status": "ok", "publish_id": publish_id}
    except Exception as e:
        results["steps"]["tiktok"] = {"status": "error", "error": str(e)}

    return jsonify(results)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
