"""
ClipLoreTV Auto-Post Service
Flask backend for automated Twitch clip -> TikTok/YouTube/Instagram video pipeline.
"""

import os
import json
import logging
import threading
import time
import tempfile
import subprocess
import urllib.request
import uuid
import functools
import re
import shutil
import random
import hashlib
import secrets
import base64
from pathlib import Path

import requests
from cryptography.fernet import Fernet, InvalidToken
from flask import Flask, request, jsonify, redirect, abort, send_file
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from gtts import gTTS

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cliplore")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://twitch-video-gen.onrender.com")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TIKTOK_CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
TIKTOK_CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
TIKTOK_REDIRECT_URI = f"{SELF_URL}/tiktok/callback"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = f"{SELF_URL}/youtube/callback"
META_APP_ID = os.environ.get("META_APP_ID", "")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
INSTAGRAM_ACCOUNT_ID = os.environ.get("INSTAGRAM_BUSINESS_ACCOUNT_ID", "")
META_REDIRECT_URI = f"{SELF_URL}/instagram/callback"
API_SECRET = os.environ.get("CLIPLORE_API_SECRET", "")
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")

# Fail fast: require API secret in production to prevent open endpoints
if not API_SECRET and os.environ.get("RENDER"):
    raise RuntimeError("CLIPLORE_API_SECRET must be set in production")

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

TEMP_DIR = Path(tempfile.gettempdir()) / "cliplore"
TEMP_DIR.mkdir(exist_ok=True)
MAX_TEMP_BYTES = 500 * 1024 * 1024

TIKTOK_SCOPES = "user.info.basic,video.publish,video.upload"

PILLAR_PREFIXES = {
    "tier_list": "Write a tier list script ranking ",
    "hot_take": "Write a hot take script about ",
    "nostalgia": "Write a nostalgia script comparing ",
    "accountability": "Write an accountability script about ",
    "rise_and_fall": "Write a rise-and-fall script about ",
}
VALID_PILLARS = set(PILLAR_PREFIXES.keys()) | {""}

DEFAULT_STREAMERS = ["xqc", "pokimane", "asmongold", "hasanabi", "shroud", "nickmercs", "timthetatman"]

# ---------------------------------------------------------------------------
# Encrypted token storage (shared by TikTok, YouTube, Instagram)
# ---------------------------------------------------------------------------
_token_key_material = (API_SECRET or "local-dev-key-not-secure").encode()
_token_fernet_key = base64.urlsafe_b64encode(
    hashlib.sha256(_token_key_material).digest()
)
_token_cipher = Fernet(_token_fernet_key)

_TOKEN_DEFAULTS = {"access_token": None, "refresh_token": None, "expires_at": 0}


def _load_platform_tokens(filename: str, extra_defaults: dict | None = None) -> dict:
    default = {**_TOKEN_DEFAULTS, **(extra_defaults or {})}
    fpath = TEMP_DIR / filename
    try:
        if fpath.exists():
            decrypted = _token_cipher.decrypt(fpath.read_bytes())
            default.update(json.loads(decrypted))
    except (InvalidToken, Exception) as e:
        log.warning("Failed to load %s (may need re-auth): %s", filename, type(e).__name__)
        fpath.unlink(missing_ok=True)
    return default


def _save_platform_tokens(filename: str, store: dict):
    fpath = TEMP_DIR / filename
    try:
        fpath.write_bytes(_token_cipher.encrypt(json.dumps(store).encode()))
        fpath.chmod(0o600)
    except Exception as e:
        log.warning("Failed to save %s: %s", filename, e)


# TikTok tokens
_tiktok_token_store = _load_platform_tokens("tiktok_tokens.enc", {"open_id": None})

def _save_tiktok_tokens():
    _save_platform_tokens("tiktok_tokens.enc", _tiktok_token_store)

# YouTube tokens
_youtube_token_store = _load_platform_tokens("youtube_tokens.enc")

def _save_youtube_tokens():
    _save_platform_tokens("youtube_tokens.enc", _youtube_token_store)

# Instagram tokens
_instagram_token_store = _load_platform_tokens("instagram_tokens.enc")

def _save_instagram_tokens():
    _save_platform_tokens("instagram_tokens.enc", _instagram_token_store)

# Server-side OAuth state store (cookies are unreliable across redirects)
_OAUTH_STATE_FILE = TEMP_DIR / "oauth_states.json"
_OAUTH_STATE_TTL = 600  # 10 minutes


def _save_oauth_state(platform: str, state: str, extras: dict | None = None):
    """Persist an OAuth state token server-side (keyed by platform)."""
    data = {}
    try:
        if _OAUTH_STATE_FILE.exists():
            data = json.loads(_OAUTH_STATE_FILE.read_text())
    except Exception:
        pass
    data[platform] = {"state": state, "ts": time.time(), **(extras or {})}
    _OAUTH_STATE_FILE.write_text(json.dumps(data))


def _pop_oauth_state(platform: str) -> dict | None:
    """Retrieve and delete a stored OAuth state. Returns None if missing/expired."""
    try:
        if not _OAUTH_STATE_FILE.exists():
            return None
        data = json.loads(_OAUTH_STATE_FILE.read_text())
        entry = data.pop(platform, None)
        _OAUTH_STATE_FILE.write_text(json.dumps(data))
        if entry and (time.time() - entry.get("ts", 0)) < _OAUTH_STATE_TTL:
            return entry
    except Exception:
        pass
    return None


# Temporary video serving for Instagram (needs public URL)
_tmp_video_tokens: dict = {}  # {token: (file_path, expires_at)}

# ---------------------------------------------------------------------------
# Security helpers
# ---------------------------------------------------------------------------
_rate_store: dict = {}
RATE_LIMIT = 30
RATE_WINDOW = 60

# Stricter per-endpoint limits for expensive operations
_heavy_rate_store: dict = {}
HEAVY_RATE_LIMIT = 3
HEAVY_RATE_WINDOW = 300  # 3 requests per 5 minutes
HEAVY_ENDPOINTS = {"/pipeline", "/cron", "/assemble-video"}

_SECRET_PATTERNS: list = []


def _build_secret_patterns():
    for val in [ANTHROPIC_API_KEY, TWITCH_CLIENT_SECRET, TIKTOK_CLIENT_SECRET, API_SECRET]:
        if val and len(val) > 4:
            _SECRET_PATTERNS.append(re.compile(re.escape(val)))


_build_secret_patterns()


def _sanitize(msg: str) -> str:
    for pat in _SECRET_PATTERNS:
        msg = pat.sub("[REDACTED]", msg)
    return msg


def _get_client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _check_rate_limit():
    ip = _get_client_ip()
    now = time.time()
    hits = [t for t in _rate_store.get(ip, []) if now - t < RATE_WINDOW]
    if len(hits) >= RATE_LIMIT:
        return True
    hits.append(now)
    _rate_store[ip] = hits
    return False


def _check_heavy_rate_limit():
    if request.path not in HEAVY_ENDPOINTS:
        return False
    ip = _get_client_ip()
    now = time.time()
    hits = [t for t in _heavy_rate_store.get(ip, []) if now - t < HEAVY_RATE_WINDOW]
    if len(hits) >= HEAVY_RATE_LIMIT:
        return True
    hits.append(now)
    _heavy_rate_store[ip] = hits
    return False


def _safe_path(user_path: str) -> Path:
    resolved = Path(user_path).resolve()
    if not str(resolved).startswith(str(TEMP_DIR.resolve())):
        abort(400, description="Invalid file path")
    return resolved


def _cleanup_temp():
    try:
        total = sum(f.stat().st_size for f in TEMP_DIR.rglob("*") if f.is_file())
        if total <= MAX_TEMP_BYTES:
            return
        dirs = sorted(
            [d for d in TEMP_DIR.iterdir() if d.is_dir()],
            key=lambda d: d.stat().st_mtime,
        )
        for d in dirs:
            if total <= MAX_TEMP_BYTES:
                break
            size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
            shutil.rmtree(d, ignore_errors=True)
            total -= size
            log.info("Cleaned up temp dir %s (freed ~%d KB)", d.name, size // 1024)
    except Exception as e:
        log.warning("Temp cleanup error: %s", e)


def require_api_key(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not API_SECRET:
            log.error("API_SECRET not set — rejecting request to %s", request.path)
            return jsonify({"error": "Server misconfigured (no API secret)"}), 500
        key = request.headers.get("X-Api-Key", "")
        if not key and request.is_json:
            key = (request.get_json(silent=True) or {}).get("api_key", "")
        if not secrets.compare_digest(key, API_SECRET):
            log.warning("Unauthorized request to %s from %s", request.path, request.remote_addr)
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Request hooks
# ---------------------------------------------------------------------------
@app.before_request
def _security_checks():
    if _check_rate_limit():
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    if _check_heavy_rate_limit():
        return jsonify({"error": "Too many expensive requests. Try again in a few minutes."}), 429
    log.info("%s %s from %s", request.method, request.path, _get_client_ip())


@app.after_request
def _security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self'; img-src 'none'; script-src 'none'"
    origin = request.headers.get("Origin", "")
    allowed_origins = {SELF_URL, "https://twitch-video-gen.onrender.com"}
    if origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Api-Key"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@app.errorhandler(413)
def _too_large(e):
    return jsonify({"error": "Request too large (max 50 MB)"}), 413


# ---------------------------------------------------------------------------
# Keep-alive (Render free tier spin-down prevention)
# ---------------------------------------------------------------------------
_keep_alive_started = False


def _start_keep_alive():
    global _keep_alive_started
    if _keep_alive_started:
        return
    _keep_alive_started = True

    def ping():
        while True:
            time.sleep(300)
            try:
                urllib.request.urlopen(f"{SELF_URL}/health", timeout=10)
            except Exception:
                pass

    threading.Thread(target=ping, daemon=True).start()


_start_keep_alive()


# ===================================================================
#  System prompt for script generation
# ===================================================================
CLIPLORE_SYSTEM_PROMPT = """You are a short-form video scriptwriter for ClipLoreTV, a TikTok channel that posts AI-narrated gaming highlight commentary over Twitch clip footage. Your scripts are engineered for maximum watch-through rate, comments, and shares on TikTok.

SCRIPT STRUCTURE (strict — follow every time):

[HOOK] — First 3 seconds. The single most controversial, shocking, or curiosity-gap statement in the entire script. This line determines whether 90% of viewers stay or leave. Must create an immediate urge to keep watching OR to comment in disagreement.

[VISUAL: describe what clip footage should show during this section]

[BUILD] — Next 15-20 seconds. 3-4 rapid-fire beats that escalate the tension or argument. Each beat is 1-2 sentences max. Use pattern interrupts between beats:
  - Pattern interrupt options: rhetorical question, "but here's the thing," dramatic pause cue "[PAUSE 0.5s]", tone shift, or direct audience challenge

[VISUAL: describe clip footage for each beat]

[PAYOFF] — Next 10-15 seconds. Deliver the main point, reveal, or ranking. This is where you cash the check the hook wrote. Be specific — names, events, data.

[VISUAL: describe the key clip moment that matches the payoff]

[CTA] — Final 5 seconds. Comment bait that splits the audience. Never generic. Always force a choice:
  - "S tier or D tier? Comment now."
  - "Who was actually right? Drop a name."
  - "Am I wrong? Prove it below."

VOICE RULES:
- Total length: 120-180 words (40-60 second TikTok)
- Tone: confident, slightly cocky, like a friend who watches too much Twitch
- No "Hey guys," no "Welcome back," no intro fluff — open cold
- Short sentences. Punchy. Never more than 15 words per sentence.
- State opinions as facts. "The truth is..." not "I think..."
- Use present tense for drama: "He walks on stage and the chat goes insane"
- Reference specific moments, usernames, or events — never be vague

OUTPUT FORMAT:
Return ONLY the script text. Include [VISUAL] tags inline. Include [PAUSE] tags where dramatic pauses should go. No headers, no notes, no meta-commentary."""


# ===================================================================
#  Core logic — reusable functions (no Flask request dependency)
# ===================================================================

def generate_script_text(topic: str, pillar: str = "") -> str:
    """Call Gemini (free) or Claude to generate a TikTok commentary script."""
    user_msg = PILLAR_PREFIXES.get(pillar, "") + topic

    if GEMINI_API_KEY:
        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}",
                headers={"content-type": "application/json"},
                json={
                    "system_instruction": {"parts": [{"text": CLIPLORE_SYSTEM_PROMPT}]},
                    "contents": [{"parts": [{"text": user_msg}]}],
                    "generationConfig": {"maxOutputTokens": 1024},
                },
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
            log.warning("Gemini API error %s: %s — falling back to Claude", resp.status_code, resp.text[:300])
        except Exception as exc:
            log.warning("Gemini request failed (%s) — falling back to Claude", exc)

    # Fallback to Anthropic Claude
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("No working AI backend: Gemini failed and ANTHROPIC_API_KEY is not set")
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-5-20250929",
            "max_tokens": 1024,
            "system": CLIPLORE_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_msg}],
        },
        timeout=30,
    )
    if resp.status_code != 200:
        log.error("Anthropic API error %s: %s", resp.status_code, resp.text[:500])
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def clean_script(raw: str) -> str:
    """Strip [VISUAL], [PAUSE], [HOOK], etc. tags from a script."""
    text = re.sub(r"\[VISUAL:[^\]]*\]", "", raw)
    text = re.sub(r"\[PAUSE[^\]]*\]", "", text)
    text = re.sub(r"\[(HOOK|BUILD|PAYOFF|CTA)\]", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def fetch_clip_urls(streamers: list, count: int = 3) -> list:
    """Fetch Twitch clip download URLs for the given streamers."""
    token = _get_twitch_token()
    urls = []
    for streamer in streamers:
        try:
            bid = _get_broadcaster_id(streamer)
            resp = requests.get(
                "https://api.twitch.tv/helix/clips",
                headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
                params={"broadcaster_id": bid, "first": count},
                timeout=10,
            )
            resp.raise_for_status()
            for clip in resp.json().get("data", []):
                thumb = clip.get("thumbnail_url", "")
                if "-preview-" in thumb:
                    urls.append(thumb.split("-preview-")[0] + ".mp4")
        except Exception:
            continue
    return urls


def assemble_video_from_parts(script_text: str, clip_urls: list) -> tuple:
    """Download clips, generate TTS, assemble 9:16 video. Returns (video_path, duration)."""
    _cleanup_temp()

    job_id = uuid.uuid4().hex[:10]
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    # TTS voiceover
    tts_path = job_dir / "voiceover.mp3"
    tts = gTTS(text=clean_script(script_text), lang="en", tld="co.uk")
    tts.save(str(tts_path))

    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(tts_path)],
        capture_output=True, text=True, timeout=15,
    )
    vo_duration = float(json.loads(probe.stdout)["format"]["duration"])

    # Download clips
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

    if not clip_paths:
        raise ValueError("Could not download any clips")

    # Scale each clip to 1080x1920 (9:16)
    scaled = []
    per_clip = vo_duration / max(len(clip_paths), 1) + 1
    for i, cp in enumerate(clip_paths):
        sp = job_dir / f"scaled_{i}.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-i", str(cp),
                "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-an",
                "-t", str(per_clip),
                str(sp),
            ],
            capture_output=True, timeout=120,
        )
        if sp.exists():
            scaled.append(sp)

    if not scaled:
        raise ValueError("FFmpeg scaling failed")

    # Concatenate
    concat_list = job_dir / "concat.txt"
    concat_list.write_text("".join(f"file '{s}'\n" for s in scaled))

    concat_vid = job_dir / "concat.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_list),
         "-c", "copy", "-t", str(vo_duration), str(concat_vid)],
        capture_output=True, timeout=120,
    )

    # Merge video + voiceover
    final = job_dir / "final.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(concat_vid), "-i", str(tts_path),
         "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
         "-c:a", "aac", "-b:a", "128k", "-shortest", "-movflags", "+faststart",
         str(final)],
        capture_output=True, timeout=120,
    )

    if not final.exists():
        raise ValueError("Final video assembly failed")

    return final, vo_duration


def upload_to_tiktok(video_path: Path, access_token: str, description: str = "") -> str:
    """Upload video to TikTok. Returns publish_id."""
    file_size = video_path.stat().st_size
    if file_size > 50 * 1024 * 1024:
        raise ValueError("Video too large (max 50 MB)")

    init_resp = requests.post(
        "https://open.tiktokapis.com/v2/post/publish/video/init/",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        },
        json={
            "post_info": {
                "title": (description[:150]) if description else "ClipLoreTV",
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
        raise ValueError(f"TikTok init failed: {init_data}")

    upload_url = init_data["data"]["upload_url"]
    publish_id = init_data["data"]["publish_id"]

    video_bytes = video_path.read_bytes()
    requests.put(
        upload_url,
        headers={
            "Content-Type": "video/mp4",
            "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
        },
        data=video_bytes,
        timeout=120,
    ).raise_for_status()

    return publish_id


def _get_youtube_credentials():
    """Build Google OAuth credentials from stored tokens, refreshing if needed."""
    token = _youtube_token_store.get("access_token")
    refresh = _youtube_token_store.get("refresh_token")
    if not token:
        return None

    creds = Credentials(
        token=token,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
    )
    if _youtube_token_store["expires_at"] - time.time() <= 60 and refresh:
        try:
            resp = requests.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "refresh_token": refresh,
                    "grant_type": "refresh_token",
                },
                timeout=15,
            )
            resp.raise_for_status()
            td = resp.json()
            _youtube_token_store["access_token"] = td["access_token"]
            _youtube_token_store["expires_at"] = time.time() + td.get("expires_in", 3600)
            _save_youtube_tokens()
            creds = Credentials(
                token=td["access_token"],
                refresh_token=refresh,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=GOOGLE_CLIENT_ID,
                client_secret=GOOGLE_CLIENT_SECRET,
            )
        except Exception as e:
            log.error("YouTube token refresh failed: %s", _sanitize(str(e)))
    return creds


def upload_to_youtube(video_path: Path, title: str = "", description: str = "") -> str:
    """Upload video to YouTube Shorts. Returns video ID."""
    creds = _get_youtube_credentials()
    if not creds:
        raise ValueError("YouTube not authenticated. Visit /youtube/auth first.")

    youtube = build("youtube", "v3", credentials=creds)
    body = {
        "snippet": {
            "title": (title[:100]) if title else "ClipLoreTV #Shorts",
            "description": description or "#twitch #gaming #shorts #cliploretv",
            "tags": ["twitch", "gaming", "shorts", "cliploretv"],
            "categoryId": "20",  # Gaming
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True)
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    while response is None:
        status, response = req.next_chunk()
        if status:
            log.info("YouTube upload progress: %d%%", int(status.progress() * 100))

    video_id = response["id"]
    log.info("YouTube upload complete: video_id=%s", video_id)
    return video_id


def upload_to_instagram(video_path: Path, description: str = "") -> str:
    """Upload video as Instagram Reel. Returns media ID."""
    access_token = _instagram_token_store.get("access_token")
    if not access_token:
        raise ValueError("Instagram not authenticated. Visit /instagram/auth first.")
    if not INSTAGRAM_ACCOUNT_ID:
        raise ValueError("INSTAGRAM_BUSINESS_ACCOUNT_ID not set")

    # Create a temporary public URL for the video
    video_url = _create_tmp_video_url(video_path, ttl=600)

    # Step 1: Create media container
    container_resp = requests.post(
        f"https://graph.facebook.com/v21.0/{INSTAGRAM_ACCOUNT_ID}/media",
        data={
            "media_type": "REELS",
            "video_url": video_url,
            "caption": description or "#twitch #gaming #reels #cliploretv",
            "access_token": access_token,
        },
        timeout=30,
    )
    container_resp.raise_for_status()
    container_id = container_resp.json().get("id")
    if not container_id:
        raise ValueError("Instagram container creation failed")

    # Step 2: Poll until container is ready
    for _ in range(30):  # max ~5 minutes of polling
        time.sleep(10)
        status_resp = requests.get(
            f"https://graph.facebook.com/v21.0/{container_id}",
            params={"fields": "status_code", "access_token": access_token},
            timeout=10,
        )
        status_resp.raise_for_status()
        status_code = status_resp.json().get("status_code", "")
        if status_code == "FINISHED":
            break
        if status_code == "ERROR":
            raise ValueError("Instagram container processing failed")
        log.info("Instagram container status: %s", status_code)
    else:
        raise ValueError("Instagram container processing timed out")

    # Step 3: Publish
    publish_resp = requests.post(
        f"https://graph.facebook.com/v21.0/{INSTAGRAM_ACCOUNT_ID}/media_publish",
        data={
            "creation_id": container_id,
            "access_token": access_token,
        },
        timeout=30,
    )
    publish_resp.raise_for_status()
    media_id = publish_resp.json().get("id", "")
    log.info("Instagram Reel published: media_id=%s", media_id)
    return media_id


# ===================================================================
#  Twitch helpers
# ===================================================================
_twitch_token_cache = {"token": None, "expires": 0}


def _get_twitch_token():
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


def _get_broadcaster_id(login: str) -> str:
    token = _get_twitch_token()
    resp = requests.get(
        "https://api.twitch.tv/helix/users",
        headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        params={"login": login},
        timeout=10,
    )
    resp.raise_for_status()
    users = resp.json().get("data", [])
    if not users:
        raise ValueError(f"Twitch user '{login}' not found")
    return users[0]["id"]


# ===================================================================
#  TikTok token helpers
# ===================================================================
def _get_tiktok_token():
    remaining = _tiktok_token_store["expires_at"] - time.time()
    if remaining <= 60 and _tiktok_token_store.get("refresh_token"):
        try:
            resp = requests.post(
                "https://open.tiktokapis.com/v2/oauth/token/",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "client_key": TIKTOK_CLIENT_KEY,
                    "client_secret": TIKTOK_CLIENT_SECRET,
                    "grant_type": "refresh_token",
                    "refresh_token": _tiktok_token_store["refresh_token"],
                },
                timeout=15,
            )
            resp.raise_for_status()
            td = resp.json()
            if "access_token" in td:
                _tiktok_token_store["access_token"] = td["access_token"]
                _tiktok_token_store["refresh_token"] = td.get("refresh_token", _tiktok_token_store["refresh_token"])
                _tiktok_token_store["expires_at"] = time.time() + td.get("expires_in", 86400)
                _save_tiktok_tokens()
            else:
                log.warning("TikTok refresh response missing access_token")
        except Exception as e:
            log.error("TikTok token refresh failed: %s", _sanitize(str(e)))
    token = _tiktok_token_store.get("access_token")
    if token and _tiktok_token_store["expires_at"] - time.time() <= 0:
        log.warning("TikTok token expired and refresh failed — returning None")
        return None
    return token


# ===================================================================
#  Topic bank
# ===================================================================
TOPIC_BANK = [
    ("Rank the top 10 Twitch streamers of all time — most overrated to most underrated", "tier_list"),
    ("xQc is actually bad for Twitch culture and here's the proof", "hot_take"),
    ("The real reason Ninja failed on Mixer (it wasn't Microsoft's fault)", "rise_and_fall"),
    ("Pokimane's Twitch success was built on something most people ignore", "accountability"),
    ("Twitch had one chance to beat YouTube and threw it away in 2021", "hot_take"),
    ("The most overrated streamer of every year from 2015 to present", "tier_list"),
    ("HasanAbi vs Destiny — who was actually right and why the community got it wrong", "hot_take"),
    ("The gambling ban on Twitch was a mistake — here's the data", "hot_take"),
    ("Why the hot tub meta was Twitch's best era for viewer numbers", "hot_take"),
    ("Amouranth's rise proves Twitch rewards one thing above all else", "accountability"),
    ("The top 5 Twitch bans that were 100% deserved — no debate", "tier_list"),
    ("The top 5 Twitch bans that were completely unfair and never explained", "accountability"),
    ("Every major streamer who left Twitch — ranked by how bad it hurt the platform", "tier_list"),
    ("The Dr Disrespect ban theory that actually makes the most sense", "hot_take"),
    ("Twitch 2016 vs Twitch 2024 — a tier list of which era was better by category", "tier_list"),
    ("Ranking Asmongold's most controversial opinions — does he deserve his platform?", "accountability"),
    ("The streamer who deserves more criticism than they get (and why the community protects them)", "accountability"),
    ("Why Just Chatting killed gaming content on Twitch", "hot_take"),
    ("The IRL streaming category tier list — ranked by damage done to the platform", "tier_list"),
    ("Ranking every Twitch policy from 2018 to 2024 from worst to least bad", "tier_list"),
    ("The most toxic Twitch fanbases ranked — prepare to be offended", "tier_list"),
    ("Why YouTube Gaming was actually better and Twitch fans won't admit it", "hot_take"),
    ("Kick.com honest tier list — rating every big streamer who moved there", "tier_list"),
    ("The streamers who should have been banned but never were", "accountability"),
    ("Twitch viewer culture then vs now — what happened to the community", "nostalgia"),
    ("The real winners and losers of the 2021 creator exodus to YouTube", "rise_and_fall"),
    ("Ranking Twitch's worst decisions of all time (the company, not streamers)", "tier_list"),
    ("Which Twitch streamer hurt the platform the most — a data-backed argument", "accountability"),
    ("The most underrated Twitch moments that deserved way more coverage", "nostalgia"),
    ("Why old Twitch fans can't stand modern streaming culture — are they right?", "nostalgia"),
]

USED_TOPICS_FILE = TEMP_DIR / "used_topics.json"


def _pick_topic() -> tuple:
    try:
        used = json.loads(USED_TOPICS_FILE.read_text()) if USED_TOPICS_FILE.exists() else []
    except Exception:
        used = []

    available = [(t, p) for t, p in TOPIC_BANK if t not in used]
    if not available:
        USED_TOPICS_FILE.unlink(missing_ok=True)
        available = TOPIC_BANK

    topic, pillar = random.choice(available)

    used.append(topic)
    try:
        USED_TOPICS_FILE.write_text(json.dumps(used))
    except Exception:
        pass

    return topic, pillar


# ===================================================================
#  Routes — Static / utility
# ===================================================================
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
#  TikTok OAuth routes
# ===================================================================
@app.route("/tiktok/auth")
def tiktok_auth():
    state = secrets.token_urlsafe(16)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()

    auth_url = (
        "https://www.tiktok.com/v2/auth/authorize/"
        f"?client_key={TIKTOK_CLIENT_KEY}"
        f"&scope={TIKTOK_SCOPES}"
        f"&response_type=code"
        f"&redirect_uri={TIKTOK_REDIRECT_URI}"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )

    _save_oauth_state("tiktok", state, {"code_verifier": code_verifier})
    return redirect(auth_url)


@app.route("/tiktok/callback")
def tiktok_callback():
    code = request.args.get("code", "")
    state = request.args.get("state", "")
    error = request.args.get("error", "")

    if error:
        return jsonify({"error": error, "description": request.args.get("error_description", "")}), 400

    stored = _pop_oauth_state("tiktok")
    if not stored or not state or state != stored.get("state"):
        return jsonify({"error": "State mismatch — possible CSRF. Try /tiktok/auth again."}), 403

    code_verifier = stored.get("code_verifier", "")

    try:
        token_resp = requests.post(
            "https://open.tiktokapis.com/v2/oauth/token/",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_key": TIKTOK_CLIENT_KEY,
                "client_secret": TIKTOK_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": TIKTOK_REDIRECT_URI,
                "code_verifier": code_verifier,
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()

        if "access_token" not in token_data:
            log.error("TikTok token exchange missing access_token: %s", _sanitize(str(token_data)))
            return jsonify({"error": "No access_token in response. Check TikTok app configuration."}), 502

        _tiktok_token_store["access_token"] = token_data["access_token"]
        _tiktok_token_store["refresh_token"] = token_data.get("refresh_token", "")
        _tiktok_token_store["expires_at"] = time.time() + token_data.get("expires_in", 86400)
        _tiktok_token_store["open_id"] = token_data.get("open_id", "")
        _save_tiktok_tokens()

        hours = token_data.get("expires_in", 0) // 3600
        return f"""<!DOCTYPE html>
<html><head><title>ClipLoreTV — TikTok Connected</title></head>
<body style="font-family:system-ui;max-width:600px;margin:80px auto;text-align:center">
<h1>TikTok Connected!</h1>
<p>Access token stored. You can now use <code>/pipeline</code> without manually passing a token.</p>
<p>Token expires in {hours} hours.</p>
<p><a href="/tiktok/status">Check token status</a></p>
</body></html>"""

    except requests.RequestException as e:
        return jsonify({"error": f"Token exchange failed: {_sanitize(str(e))}"}), 502


@app.route("/tiktok/refresh", methods=["POST"])
@require_api_key
def tiktok_refresh():
    refresh_token = _tiktok_token_store.get("refresh_token", "")
    if not refresh_token:
        return jsonify({"error": "No refresh token stored. Visit /tiktok/auth first."}), 400

    try:
        resp = requests.post(
            "https://open.tiktokapis.com/v2/oauth/token/",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_key": TIKTOK_CLIENT_KEY,
                "client_secret": TIKTOK_CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=15,
        )
        resp.raise_for_status()
        token_data = resp.json()

        if "access_token" not in token_data:
            log.error("TikTok refresh missing access_token: %s", _sanitize(str(token_data)))
            return jsonify({"error": "Refresh failed. Try /tiktok/auth again."}), 502

        _tiktok_token_store["access_token"] = token_data["access_token"]
        _tiktok_token_store["refresh_token"] = token_data.get("refresh_token", refresh_token)
        _tiktok_token_store["expires_at"] = time.time() + token_data.get("expires_in", 86400)
        _save_tiktok_tokens()

        return jsonify({"status": "refreshed", "expires_in": token_data.get("expires_in", 0)})
    except requests.RequestException as e:
        return jsonify({"error": f"Refresh failed: {_sanitize(str(e))}"}), 502


@app.route("/tiktok/status")
def tiktok_status():
    token = _tiktok_token_store.get("access_token")
    if not token:
        return jsonify({"authenticated": False, "message": "No token. Visit /tiktok/auth to connect."})

    remaining = _tiktok_token_store["expires_at"] - time.time()
    return jsonify({
        "authenticated": True,
        "open_id": _tiktok_token_store.get("open_id", ""),
        "expires_in_seconds": max(0, int(remaining)),
        "expired": remaining <= 0,
    })


# ===================================================================
#  YouTube OAuth routes
# ===================================================================
YOUTUBE_SCOPES = "https://www.googleapis.com/auth/youtube.upload"


@app.route("/youtube/auth")
def youtube_auth():
    if not GOOGLE_CLIENT_ID:
        return jsonify({"error": "Google OAuth not configured"}), 500
    state = secrets.token_urlsafe(16)
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={GOOGLE_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope={YOUTUBE_SCOPES}"
        f"&access_type=offline"
        f"&prompt=consent"
        f"&state={state}"
    )
    _save_oauth_state("youtube", state)
    return redirect(auth_url)


@app.route("/youtube/callback")
def youtube_callback():
    code = request.args.get("code", "")
    state = request.args.get("state", "")
    error = request.args.get("error", "")

    if error:
        return jsonify({"error": error}), 400

    stored = _pop_oauth_state("youtube")
    if not stored or not state or state != stored.get("state"):
        return jsonify({"error": "State mismatch — possible CSRF. Try /youtube/auth again."}), 403

    try:
        token_resp = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": GOOGLE_REDIRECT_URI,
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()

        if "access_token" not in token_data:
            log.error("YouTube token exchange missing access_token: %s", _sanitize(str(token_data)))
            return jsonify({"error": "No access_token in response. Check Google app configuration."}), 502

        _youtube_token_store["access_token"] = token_data["access_token"]
        _youtube_token_store["refresh_token"] = token_data.get("refresh_token", _youtube_token_store.get("refresh_token", ""))
        _youtube_token_store["expires_at"] = time.time() + token_data.get("expires_in", 3600)
        _save_youtube_tokens()

        return f"""<!DOCTYPE html>
<html><head><title>ClipLoreTV — YouTube Connected</title></head>
<body style="font-family:system-ui;max-width:600px;margin:80px auto;text-align:center">
<h1>YouTube Connected!</h1>
<p>Access token stored. Videos will now auto-post to YouTube Shorts.</p>
<p><a href="/youtube/status">Check token status</a></p>
</body></html>"""

    except requests.RequestException as e:
        return jsonify({"error": f"Token exchange failed: {_sanitize(str(e))}"}), 502


@app.route("/youtube/status")
def youtube_status():
    token = _youtube_token_store.get("access_token")
    if not token:
        return jsonify({"authenticated": False, "message": "No token. Visit /youtube/auth to connect."})
    remaining = _youtube_token_store["expires_at"] - time.time()
    return jsonify({
        "authenticated": True,
        "expires_in_seconds": max(0, int(remaining)),
        "expired": remaining <= 0,
    })


# ===================================================================
#  Instagram OAuth routes
# ===================================================================
INSTAGRAM_PERMISSIONS = "instagram_basic,instagram_content_publish,pages_read_engagement"


@app.route("/instagram/auth")
def instagram_auth():
    if not META_APP_ID:
        return jsonify({"error": "Meta/Instagram OAuth not configured"}), 500
    state = secrets.token_urlsafe(16)
    auth_url = (
        "https://www.facebook.com/v21.0/dialog/oauth"
        f"?client_id={META_APP_ID}"
        f"&redirect_uri={META_REDIRECT_URI}"
        f"&scope={INSTAGRAM_PERMISSIONS}"
        f"&response_type=code"
        f"&state={state}"
    )
    _save_oauth_state("instagram", state)
    return redirect(auth_url)


@app.route("/instagram/callback")
def instagram_callback():
    code = request.args.get("code", "")
    state = request.args.get("state", "")
    error = request.args.get("error", "")

    if error:
        return jsonify({"error": error, "description": request.args.get("error_description", "")}), 400

    stored = _pop_oauth_state("instagram")
    if not stored or not state or state != stored.get("state"):
        return jsonify({"error": "State mismatch — possible CSRF. Try /instagram/auth again."}), 403

    try:
        # Exchange code for short-lived token
        token_resp = requests.post(
            "https://graph.facebook.com/v21.0/oauth/access_token",
            data={
                "client_id": META_APP_ID,
                "client_secret": META_APP_SECRET,
                "code": code,
                "redirect_uri": META_REDIRECT_URI,
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        short_token = token_resp.json().get("access_token", "")

        if not short_token:
            log.error("Instagram token exchange failed: %s", _sanitize(str(token_resp.json())))
            return jsonify({"error": "No access_token in response."}), 502

        # Exchange for long-lived token (~60 days)
        ll_resp = requests.get(
            "https://graph.facebook.com/v21.0/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": META_APP_ID,
                "client_secret": META_APP_SECRET,
                "fb_exchange_token": short_token,
            },
            timeout=15,
        )
        ll_resp.raise_for_status()
        ll_data = ll_resp.json()

        _instagram_token_store["access_token"] = ll_data.get("access_token", short_token)
        _instagram_token_store["expires_at"] = time.time() + ll_data.get("expires_in", 5184000)
        _save_instagram_tokens()

        return f"""<!DOCTYPE html>
<html><head><title>ClipLoreTV — Instagram Connected</title></head>
<body style="font-family:system-ui;max-width:600px;margin:80px auto;text-align:center">
<h1>Instagram Connected!</h1>
<p>Long-lived token stored. Videos will now auto-post as Reels.</p>
<p><a href="/instagram/status">Check token status</a></p>
</body></html>"""

    except requests.RequestException as e:
        return jsonify({"error": f"Token exchange failed: {_sanitize(str(e))}"}), 502


@app.route("/instagram/status")
def instagram_status():
    token = _instagram_token_store.get("access_token")
    if not token:
        return jsonify({"authenticated": False, "message": "No token. Visit /instagram/auth to connect."})
    remaining = _instagram_token_store["expires_at"] - time.time()
    return jsonify({
        "authenticated": True,
        "ig_account_id": INSTAGRAM_ACCOUNT_ID or "(not set)",
        "expires_in_seconds": max(0, int(remaining)),
        "expired": remaining <= 0,
    })


# ===================================================================
#  Temporary video serving (for Instagram which requires a public URL)
# ===================================================================
@app.route("/tmp-video/<serve_token>")
def serve_tmp_video(serve_token):
    entry = _tmp_video_tokens.get(serve_token)
    if not entry:
        return jsonify({"error": "Not found or expired"}), 404
    file_path, expires_at = entry
    if time.time() > expires_at:
        _tmp_video_tokens.pop(serve_token, None)
        return jsonify({"error": "Link expired"}), 410
    resolved = Path(file_path).resolve()
    if not str(resolved).startswith(str(TEMP_DIR.resolve())) or not resolved.exists():
        return jsonify({"error": "File not found"}), 404
    return send_file(str(resolved), mimetype="video/mp4")


def _create_tmp_video_url(video_path: Path, ttl: int = 600) -> str:
    token = secrets.token_urlsafe(32)
    _tmp_video_tokens[token] = (str(video_path), time.time() + ttl)
    return f"{SELF_URL}/tmp-video/{token}"


# ===================================================================
#  API endpoints
# ===================================================================
@app.route("/generate-script", methods=["POST"])
@require_api_key
def generate_script_endpoint():
    data = request.get_json(force=True)
    topic = data.get("topic", "")
    if not topic or not isinstance(topic, str):
        return jsonify({"error": "topic is required (string)"}), 400
    if len(topic) > 1000:
        return jsonify({"error": "topic too long (max 1000 chars)"}), 400
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "Server misconfigured"}), 500

    pillar = data.get("pillar", "")
    if pillar not in VALID_PILLARS:
        return jsonify({"error": f"Invalid pillar. Must be one of: {', '.join(VALID_PILLARS - {''})}"}), 400

    try:
        script_text = generate_script_text(topic, pillar)
        return jsonify({"script": script_text, "word_count": len(script_text.split())})
    except requests.RequestException as e:
        return jsonify({"error": f"Script generation failed: {_sanitize(str(e))}"}), 502


@app.route("/fetch-clips", methods=["POST"])
@require_api_key
def fetch_clips_endpoint():
    data = request.get_json(force=True)
    streamers = data.get("streamers", [])
    count = min(data.get("count", 5), 20)

    if not streamers or not isinstance(streamers, list):
        return jsonify({"error": "streamers list is required"}), 400
    if len(streamers) > 10:
        return jsonify({"error": "Max 10 streamers per request"}), 400
    for s in streamers:
        if not isinstance(s, str) or not re.match(r"^[a-zA-Z0-9_]{1,25}$", s):
            return jsonify({"error": f"Invalid streamer name: {s[:30]}"}), 400
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return jsonify({"error": "Server misconfigured"}), 500

    all_clips = []
    try:
        token = _get_twitch_token()
        for streamer in streamers:
            try:
                bid = _get_broadcaster_id(streamer)
                resp = requests.get(
                    "https://api.twitch.tv/helix/clips",
                    headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
                    params={"broadcaster_id": bid, "first": count},
                    timeout=10,
                )
                resp.raise_for_status()
                for clip in resp.json().get("data", []):
                    thumb = clip.get("thumbnail_url", "")
                    download_url = thumb.split("-preview-")[0] + ".mp4" if "-preview-" in thumb else ""
                    all_clips.append({
                        "streamer": streamer,
                        "title": clip.get("title", ""),
                        "url": clip.get("url", ""),
                        "download_url": download_url,
                        "thumbnail_url": thumb,
                        "view_count": clip.get("view_count", 0),
                        "duration": clip.get("duration", 0),
                    })
            except Exception as e:
                all_clips.append({"streamer": streamer, "error": _sanitize(str(e))})

        return jsonify({"clips": all_clips})
    except Exception as e:
        return jsonify({"error": _sanitize(str(e))}), 502


@app.route("/assemble-video", methods=["POST"])
@require_api_key
def assemble_video_endpoint():
    data = request.get_json(force=True)
    script = data.get("script", "")
    clip_urls = data.get("clip_urls", [])

    if not script or not isinstance(script, str):
        return jsonify({"error": "script is required (string)"}), 400
    if len(script) > 10000:
        return jsonify({"error": "script too long (max 10000 chars)"}), 400
    if not clip_urls or not isinstance(clip_urls, list):
        return jsonify({"error": "clip_urls list is required"}), 400
    if len(clip_urls) > 10:
        return jsonify({"error": "Max 10 clip URLs per request"}), 400
    for url in clip_urls:
        if not isinstance(url, str) or not url.startswith("https://"):
            return jsonify({"error": "All clip_urls must be HTTPS URLs"}), 400

    try:
        video_path, duration = assemble_video_from_parts(script, clip_urls)
        return jsonify({
            "video_path": str(video_path),
            "job_id": video_path.parent.name,
            "duration": duration,
        })
    except Exception as e:
        return jsonify({"error": f"Assembly failed: {_sanitize(str(e))}"}), 500


@app.route("/post-to-tiktok", methods=["POST"])
@require_api_key
def post_to_tiktok_endpoint():
    data = request.get_json(force=True)
    video_path = data.get("video_path", "")
    access_token = data.get("access_token", "") or _get_tiktok_token()
    description = data.get("description", "")

    if not video_path:
        return jsonify({"error": "video_path is required"}), 400
    if not access_token:
        return jsonify({"error": "No access_token provided and no stored token. Visit /tiktok/auth first."}), 401

    video_file = _safe_path(video_path)
    if not video_file.exists():
        return jsonify({"error": "Video file not found"}), 404

    try:
        publish_id = upload_to_tiktok(video_file, access_token, description)
        return jsonify({
            "publish_id": publish_id,
            "status": "uploaded",
            "message": "Video uploaded to TikTok. Check Creator Center for processing status.",
        })
    except Exception as e:
        return jsonify({"error": f"TikTok API error: {_sanitize(str(e))}"}), 502


# ===================================================================
#  Full pipeline & cron — both use _run_pipeline
# ===================================================================
@app.route("/pipeline", methods=["POST"])
@require_api_key
def pipeline():
    data = request.get_json(force=True)
    topic = data.get("topic", "")
    pillar = data.get("pillar", "")
    streamers = data.get("streamers", [])
    tiktok_token = data.get("tiktok_access_token", "") or _get_tiktok_token()
    description = data.get("description", "")

    if not topic or not streamers:
        return jsonify({"error": "topic and streamers are required"}), 400

    return _run_pipeline(topic, pillar, streamers, tiktok_token, description)


@app.route("/cron", methods=["POST"])
@require_api_key
def cron():
    tiktok_token = _get_tiktok_token()

    data = request.get_json(silent=True) or {}
    streamers = data.get("streamers", random.sample(DEFAULT_STREAMERS, min(3, len(DEFAULT_STREAMERS))))
    description = data.get("description", "#twitch #gaming #streamer #cliploretv #shorts")

    topic, pillar = _pick_topic()
    log.info("CRON: topic=%s pillar=%s streamers=%s", topic[:50], pillar, streamers)

    return _run_pipeline(topic, pillar, streamers, tiktok_token, description)


def _run_pipeline(topic, pillar, streamers, tiktok_token, description):
    """Shared pipeline logic for /pipeline and /cron."""
    results = {"topic": topic, "pillar": pillar, "streamers": streamers, "steps": {}}

    # Step 1: Generate script
    try:
        script_text = generate_script_text(topic, pillar)
        results["steps"]["script"] = {"status": "ok", "word_count": len(script_text.split())}
    except Exception as e:
        return jsonify({"error": f"Script generation failed: {_sanitize(str(e))}", "steps": results["steps"]}), 500

    # Step 2: Fetch clips
    try:
        clip_urls = fetch_clip_urls(streamers)
        results["steps"]["clips"] = {"status": "ok", "count": len(clip_urls)}
    except Exception as e:
        return jsonify({"error": f"Clip fetch failed: {_sanitize(str(e))}", "steps": results["steps"]}), 500

    if not clip_urls:
        return jsonify({"error": "No clips found", "steps": results["steps"]}), 400

    # Step 3: Assemble video
    try:
        video_path, duration = assemble_video_from_parts(script_text, clip_urls)
        results["steps"]["video"] = {"status": "ok", "duration": duration}
    except Exception as e:
        return jsonify({"error": f"Assembly failed: {_sanitize(str(e))}", "steps": results["steps"]}), 500

    # Step 4: Post to platforms (each independent — one failure doesn't block others)
    all_ok = True
    short_title = topic[:100] if topic else "ClipLoreTV"

    # 4a: TikTok
    if tiktok_token:
        try:
            publish_id = upload_to_tiktok(video_path, tiktok_token, description)
            results["steps"]["tiktok"] = {"status": "ok", "publish_id": publish_id}
        except Exception as e:
            results["steps"]["tiktok"] = {"status": "error", "error": _sanitize(str(e))}
            all_ok = False
    else:
        results["steps"]["tiktok"] = {"status": "skipped", "reason": "No TikTok token"}

    # 4b: YouTube Shorts
    yt_creds = _get_youtube_credentials()
    if yt_creds:
        try:
            video_id = upload_to_youtube(video_path, title=short_title, description=description)
            results["steps"]["youtube"] = {"status": "ok", "video_id": video_id}
        except Exception as e:
            results["steps"]["youtube"] = {"status": "error", "error": _sanitize(str(e))}
            all_ok = False
    else:
        results["steps"]["youtube"] = {"status": "skipped", "reason": "No YouTube token"}

    # 4c: Instagram Reels
    if _instagram_token_store.get("access_token") and INSTAGRAM_ACCOUNT_ID:
        try:
            media_id = upload_to_instagram(video_path, description=description)
            results["steps"]["instagram"] = {"status": "ok", "media_id": media_id}
        except Exception as e:
            results["steps"]["instagram"] = {"status": "error", "error": _sanitize(str(e))}
            all_ok = False
    else:
        results["steps"]["instagram"] = {"status": "skipped", "reason": "No Instagram token or account ID"}

    # Cleanup — keep video if any upload failed so it can be retried
    if all_ok:
        try:
            shutil.rmtree(video_path.parent, ignore_errors=True)
        except Exception:
            pass
    else:
        results["video_path"] = str(video_path)
        log.warning("Some uploads failed — keeping video at %s for retry", video_path)

    platforms = {k: v.get("status") for k, v in results["steps"].items() if k in ("tiktok", "youtube", "instagram")}
    log.info("Pipeline done: topic=%s platforms=%s", topic[:50], platforms)
    return jsonify(results)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
