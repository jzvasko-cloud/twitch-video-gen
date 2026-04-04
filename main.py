"""
ClipLoreTV Service
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

import asyncio

import requests
import edge_tts
from cryptography.fernet import Fernet, InvalidToken
from flask import Flask, request, jsonify, redirect, abort, send_file
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta, timezone
try:
    from PIL import Image, ImageDraw, ImageFont
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False

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
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
RENDER_API_KEY = os.environ.get("RENDER_API_KEY", "")
RENDER_SERVICE_ID = os.environ.get("RENDER_SERVICE_ID", "srv-d6ich8haae7s73ce1i70")
ENCODER_URL = os.environ.get("ENCODER_URL", "https://cliplore-encoder-347562229502.us-central1.run.app")
ENCODER_SECRET = os.environ.get("ENCODER_SECRET", API_SECRET)  # defaults to same key as main service

# Feature flags
KICK_ENABLED = True
REDDIT_TRENDING_ENABLED = True

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

DEFAULT_STREAMERS = [
    # Variety/gaming streamers with clip-heavy content
    "xqc", "asmongold", "shroud", "timthetatman", "nickmercs",
    "summit1g", "lirik", "sodapoppin", "forsen", "tyler1",
    "kai_cenat", "adin", "caseoh_", "ironmouse", "caedrel",
]

# ---------------------------------------------------------------------------
# Encrypted token storage — persisted to Render env vars via API
# ---------------------------------------------------------------------------
# Tokens are stored as a single encrypted JSON blob in the PLATFORM_TOKENS_ENC
# env var. On token change, the app pushes the update to Render's API so it
# survives redeploys and spin-downs. Falls back to local file for dev.
# ---------------------------------------------------------------------------
_token_key_material = (API_SECRET or "local-dev-key-not-secure").encode()
_token_fernet_key = base64.urlsafe_b64encode(
    hashlib.sha256(_token_key_material).digest()
)
_token_cipher = Fernet(_token_fernet_key)

_TOKEN_DEFAULTS = {"access_token": None, "refresh_token": None, "expires_at": 0}

# All platform tokens in one dict: {"tiktok": {...}, "youtube": {...}, "instagram": {...}}
_all_tokens: dict = {}


def _load_all_tokens():
    """Load all platform tokens from env var, falling back to legacy files."""
    global _all_tokens
    loaded = False

    # Try env var first (Render-persisted)
    enc = os.environ.get("PLATFORM_TOKENS_ENC", "")
    if enc:
        try:
            decrypted = _token_cipher.decrypt(enc.encode())
            _all_tokens = json.loads(decrypted)
            loaded = True
            log.info("Loaded tokens from PLATFORM_TOKENS_ENC env var (%d platforms)", len(_all_tokens))
        except Exception as e:
            log.warning("Failed to decrypt PLATFORM_TOKENS_ENC: %s", type(e).__name__)

    # Fall back to legacy files (migration path)
    if not loaded:
        for platform, filename, extras in [
            ("tiktok", "tiktok_tokens.enc", {"open_id": None}),
            ("youtube", "youtube_tokens.enc", None),
            ("instagram", "instagram_tokens.enc", None),
        ]:
            fpath = TEMP_DIR / filename
            try:
                if fpath.exists():
                    decrypted = _token_cipher.decrypt(fpath.read_bytes())
                    _all_tokens[platform] = json.loads(decrypted)
                    log.info("Migrated %s tokens from legacy file", platform)
            except Exception:
                pass

    # Ensure all platforms exist with defaults
    for platform, extras in [("tiktok", {"open_id": None}), ("youtube", None), ("instagram", None)]:
        default = {**_TOKEN_DEFAULTS, **(extras or {})}
        if platform not in _all_tokens:
            _all_tokens[platform] = default
        else:
            for k, v in default.items():
                _all_tokens[platform].setdefault(k, v)


def _persist_tokens():
    """Push all tokens to Render env var via API. Also save to local file as cache."""
    encrypted = _token_cipher.encrypt(json.dumps(_all_tokens).encode()).decode()

    # Save locally for the running process
    try:
        fpath = TEMP_DIR / "platform_tokens.enc"
        fpath.write_bytes(encrypted.encode())
        fpath.chmod(0o600)
    except Exception:
        pass

    # Push to Render API (survives redeploys)
    if RENDER_API_KEY and RENDER_SERVICE_ID:
        try:
            resp = requests.put(
                f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars/PLATFORM_TOKENS_ENC",
                headers={
                    "Authorization": f"Bearer {RENDER_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={"value": encrypted},
                timeout=10,
            )
            if resp.status_code == 200:
                log.info("Tokens persisted to Render env var")
            else:
                log.warning("Render API env var update failed: %s %s", resp.status_code, resp.text[:200])
        except Exception as e:
            log.warning("Failed to persist tokens to Render API: %s", e)
    else:
        log.debug("RENDER_API_KEY not set — tokens saved locally only")


# Initialize on startup
_load_all_tokens()

# Convenience accessors (keep existing interface)
_tiktok_token_store = _all_tokens["tiktok"]
_youtube_token_store = _all_tokens["youtube"]
_instagram_token_store = _all_tokens["instagram"]


def _save_tiktok_tokens():
    _all_tokens["tiktok"] = _tiktok_token_store
    _persist_tokens()

def _save_youtube_tokens():
    _all_tokens["youtube"] = _youtube_token_store
    _persist_tokens()

def _save_instagram_tokens():
    _all_tokens["instagram"] = _instagram_token_store
    _persist_tokens()

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
# Async job tracker — lets /pipeline and /assemble-video return 202 immediately
# ---------------------------------------------------------------------------
_jobs: dict = {}  # {job_id: {"status": str, "result": dict|None, "error": str|None, "created": float}}
_JOBS_MAX = 50  # max jobs to keep in memory
_JOBS_TTL = 3600  # auto-expire after 1 hour


def _create_job() -> str:
    """Create a new job entry and return its ID."""
    job_id = uuid.uuid4().hex[:12]
    # Evict oldest jobs if at capacity
    if len(_jobs) >= _JOBS_MAX:
        oldest = sorted(_jobs, key=lambda k: _jobs[k]["created"])
        for old_id in oldest[:len(_jobs) - _JOBS_MAX + 1]:
            _jobs.pop(old_id, None)
    _jobs[job_id] = {"status": "running", "result": None, "error": None, "created": time.time()}
    return job_id


def _complete_job(job_id: str, result: dict):
    """Mark a job as completed with its result."""
    if job_id in _jobs:
        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["result"] = result


def _fail_job(job_id: str, error: str):
    """Mark a job as failed with an error message."""
    if job_id in _jobs:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = error

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
# Notifications
# ---------------------------------------------------------------------------
_last_pipeline_result = {"timestamp": None, "result": None}


def _send_notification(title: str, message: str, success: bool = True, details: dict | None = None):
    """Send a Discord webhook notification. Silently no-ops if not configured."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        color = 0x00FF00 if success else 0xFF0000
        fields = []
        if details:
            for k, v in details.items():
                fields.append({"name": k, "value": str(v)[:200], "inline": True})
        payload = {
            "embeds": [{
                "title": title,
                "description": message[:2000],
                "color": color,
                "fields": fields[:25],
                "footer": {"text": "ClipLoreTV Pipeline"},
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as exc:
        log.warning("Discord notification failed: %s", exc)


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

[HOOK] — First sentence. THIS IS THE MOST IMPORTANT LINE. It decides if the viewer stays or swipes. Rules:
  1. It MUST be a direct question or bold claim about a SPECIFIC streamer/event
  2. It MUST make the viewer think "wait, what?" or "no way, that's wrong"
  3. It MUST contain at least one proper name (streamer, platform, event)
  4. Keep it under 12 words
  GOOD: "Ninja left Twitch and it was the worst deal in streaming history."
  GOOD: "Why does everyone pretend xQc isn't ruining Twitch?"
  GOOD: "DrDisrespect got banned and nobody actually knows why."
  BAD: "Let me tell you something about streaming." (generic, no names)
  BAD: "This might be controversial." (vague, could be any video)
  BAD: "Here's what nobody talks about." (no specifics)

[VISUAL: describe what clip footage should show during this section]

[BUILD] — Next 15-20 seconds. 3-4 rapid-fire beats that escalate the tension or argument. Each beat is 1-2 sentences max. Use pattern interrupts between beats:
  - Pattern interrupt options: rhetorical question, "but here's the thing," dramatic pause cue "[PAUSE 0.5s]", tone shift, or direct audience challenge

[VISUAL: describe clip footage for each beat]

[PAYOFF] — Next 10-15 seconds. Deliver the main point, reveal, or ranking. This is where you cash the check the hook wrote. Be specific — names, events, data.

[VISUAL: describe the key clip moment that matches the payoff]

[CTA] — Final 5-7 seconds. This is the MOST IMPORTANT part for engagement. You MUST end with a direct question or challenge that makes the viewer feel they NEED to comment. Always force a binary choice or a strong opinion. Examples:
  - "Comment your top 3 right now. I'll pin the best one."
  - "If you disagree, tell me who should have been number one. I'll wait."
  - "Drop a 1 if you agree, 2 if you think I'm delusional."
  - "Comment below — was this an L take or was I right? Be honest."
  - "Follow for part two where I rank the rest. Comment who you want to see next."
  The CTA MUST feel conversational, like you're talking directly to the viewer. Never say "like and subscribe" — that's dead. Ask for their OPINION.

VOICE RULES:
- CRITICAL: Total spoken words MUST be between 80-120 words (30-45 second video). Count them. This is NOT optional. Scripts under 70 spoken words will be rejected.
- Tone: confident, slightly cocky, like a friend who watches too much Twitch
- No "Hey guys," no "Welcome back," no intro fluff — open cold
- Short sentences. Punchy. Never more than 15 words per sentence.
- State opinions as facts. "The truth is..." not "I think..."
- Use present tense for drama: "He walks on stage and the chat goes insane"
- Reference specific moments, usernames, or events — never be vague

OUTPUT FORMAT:
Return ONLY the narration script text that will be read aloud. Do NOT include [VISUAL] tags, [HOOK] tags, [BUILD] tags, [PAYOFF] tags, [CTA] tags, or any markdown formatting. Just write the pure spoken words, 80-120 words total, as one continuous script. No headers, no notes, no meta-commentary, no tags of any kind."""


# ===================================================================
#  Core logic — reusable functions (no Flask request dependency)
# ===================================================================

MIN_SCRIPT_WORDS = 70
MAX_SCRIPT_RETRIES = 3
EDGE_TTS_VOICE = "en-US-AndrewMultilingualNeural"
EDGE_TTS_RATE = "+0%"


def _call_gemini(user_msg: str) -> str | None:
    """Call Gemini API. Returns script text or None on failure."""
    if not GEMINI_API_KEY:
        return None
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"content-type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": CLIPLORE_SYSTEM_PROMPT}]},
                "contents": [{"parts": [{"text": user_msg}]}],
                "generationConfig": {
                    "maxOutputTokens": 2048,
                    "thinkingConfig": {"thinkingBudget": 0},
                },
            },
            timeout=45,
        )
        if resp.status_code == 200:
            # Gemini 2.5 may return multiple parts (thinking + text); get the last text part
            parts = resp.json()["candidates"][0]["content"]["parts"]
            for part in reversed(parts):
                if "text" in part:
                    return part["text"]
        log.warning("Gemini API error %s: %s", resp.status_code, resp.text[:300])
    except Exception as exc:
        log.warning("Gemini request failed: %s", exc)
    return None


def _call_claude(user_msg: str) -> str | None:
    """Call Anthropic Claude API. Returns script text or None on failure."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
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
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        log.warning("Anthropic API error %s: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Anthropic request failed: %s", exc)
    return None


def generate_script_text(topic: str, pillar: str = "", top_topics: list[str] | None = None) -> str:
    """Generate a TikTok script, retrying if too short (< 100 words).

    If top_topics is provided (from YouTube analytics), they are injected into
    the prompt as context to steer content toward proven performers.
    """
    base_msg = PILLAR_PREFIXES.get(pillar, "") + topic

    # UPGRADE 3: Inject analytics context if available
    if top_topics:
        topics_str = "\n".join(f"  - {t}" for t in top_topics)
        base_msg += (
            f"\n\nThese topics performed best on our channel recently:\n{topics_str}\n"
            "Generate content in a similar style or on related topics when relevant."
        )

    for attempt in range(MAX_SCRIPT_RETRIES):
        # Add length enforcement on retries
        if attempt == 0:
            user_msg = base_msg
        else:
            user_msg = (
                base_msg
                + f"\n\nIMPORTANT: Your previous script was only {word_count} words. "
                "That is WAY too short. You MUST write at least 80 words. "
                "Include a full hook, build with 3-4 beats, payoff, and CTA. "
                "Make it 30-45 seconds when read aloud."
            )

        # Try Gemini first, then Claude
        script = _call_gemini(user_msg) or _call_claude(user_msg)

        if not script:
            raise RuntimeError("All AI backends failed — neither Gemini nor Claude returned a script")

        word_count = len(clean_script(script).split())
        log.info("Script attempt %d: %d words (min %d)", attempt + 1, word_count, MIN_SCRIPT_WORDS)

        if word_count >= MIN_SCRIPT_WORDS:
            return script

        log.warning("Script too short (%d words), retrying...", word_count)

    # Return the last script even if short, better than nothing
    log.warning("Script still short after %d retries (%d words), using anyway", MAX_SCRIPT_RETRIES, word_count)
    return script


def generate_short_title(topic: str, script_text: str = "") -> str:
    """Generate a Shorts-optimized title using AI. Falls back to topic truncation."""
    prompt = (
        f"Write a YouTube Shorts / TikTok title for this video topic:\n\n"
        f"Topic: {topic}\n"
        + (f"Script preview: {clean_script(script_text)[:200]}\n\n" if script_text else "\n")
        + "STRICT RULES:\n"
        "1. Front-load a searchable noun (streamer name, game name, platform name) in the FIRST 3 WORDS\n"
        "2. Title MUST be under 60 characters total\n"
        "3. Include an emotional hook word: a number, or words like failed, banned, destroyed, exposed, broke, lost, caught, ruined\n"
        "4. NO editorial phrasing like 'the real reason', 'a tier list of', 'let me explain', 'here is why'\n"
        "5. Format: '[Name/Thing] [shocking verb] [consequence] — [curiosity hook]'\n\n"
        "GOOD examples:\n"
        "- Ninja got $30M and still failed — here's why\n"
        "- These streamers got banned and deserved it\n"
        "- xQc broke Twitch's biggest rule and nothing happened\n"
        "- Pokimane exposed the truth about streaming money\n"
        "- Asmongold destroyed this game's reputation in 1 stream\n\n"
        "BAD examples (DO NOT write like these):\n"
        "- The real reason streaming is dying\n"
        "- A tier list of the best Twitch moments\n"
        "- Here's what nobody talks about in gaming\n\n"
        "Return ONLY the title text. Nothing else. No quotes, no explanation."
    )
    try:
        raw_title = _call_gemini(prompt) or _call_claude(prompt)
        if raw_title:
            # Clean up: strip quotes, whitespace, ensure length
            title = raw_title.strip().strip('"').strip("'").strip()
            # Remove any trailing hashtags the AI might add
            title = re.sub(r'\s*#\w+', '', title).strip()
            if len(title) > 60:
                title = title[:57] + "..."
            if len(title) > 10:
                log.info("Generated Shorts title: %s", title)
                return title
    except Exception as exc:
        log.warning("Title generation failed: %s", exc)

    # Fallback: truncate topic to 60 chars
    fallback = topic[:57] + "..." if len(topic) > 60 else topic
    log.info("Using fallback title: %s", fallback)
    return fallback


def clean_script(raw: str) -> str:
    """Strip [VISUAL], [PAUSE], [HOOK], etc. tags and markdown from a script."""
    text = re.sub(r"\[VISUAL[^\]]*\]", "", raw)   # [VISUAL: ...] and [VISUAL]
    text = re.sub(r"\[PAUSE[^\]]*\]", "", text)    # [PAUSE 0.5s] etc.
    text = re.sub(r"\[(HOOK|BUILD|PAYOFF|CTA)\]", "", text)  # section markers
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)  # **bold** -> bold
    text = re.sub(r"\*([^*]+)\*", r"\1", text)      # *italic* -> italic
    text = re.sub(r"^#+\s+", "", text, flags=re.MULTILINE)  # # headers
    text = re.sub(r"^[-*]\s+", "", text, flags=re.MULTILINE)  # bullet points
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _get_clip_source_url(slug: str) -> str:
    """Use Twitch GQL to get an authenticated clip download URL."""
    resp = requests.post(
        "https://gql.twitch.tv/gql",
        headers={"Client-ID": "kimne78kx3ncx6brgo4mv6wki5h1ko"},
        json=[{
            "operationName": "VideoAccessToken_Clip",
            "variables": {"slug": slug},
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "36b89d2507fce29e5ca551df756d27c1cfe079e2609642b4390aa4c35796eb11",
                }
            },
        }],
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()[0].get("data", {}).get("clip", {})
    if not data:
        raise ValueError(f"No clip data for slug {slug}")
    token = data["playbackAccessToken"]
    qualities = data.get("videoQualities", [])
    if not qualities:
        raise ValueError(f"No video qualities for slug {slug}")
    source_url = qualities[0]["sourceURL"]
    return f"{source_url}?sig={token['signature']}&token={requests.utils.quote(token['value'])}"


def _extract_clip_sources(topic: str) -> dict:
    """Ask AI to extract relevant streamer names and game names from a topic.

    Returns {"streamers": [...], "related": [...], "games": [...]} or empty dict on failure.
    """
    prompt = (
        f"Given this video topic about Twitch/gaming: \"{topic}\"\n\n"
        "Return a JSON object with:\n"
        '- "streamers": list of 2-4 Twitch login usernames (lowercase, no spaces) '
        "that are DIRECTLY mentioned in the topic. These must be the main subjects.\n"
        '- "related": list of 3-5 Twitch login usernames (lowercase, no spaces) '
        "of streamers who are closely related to the topic, were part of the same "
        "events, or are in the same scene. These are used as BACKUP clip sources "
        "when the main streamers have no clips available. Pick streamers who would "
        "make visually relevant background footage for this topic.\n"
        '- "games": list of 1-3 exact Twitch game/category names relevant to the clips '
        "that would look good as background footage. Include 'Just Chatting' if the "
        "topic is about streamer drama or opinions.\n\n"
        "IMPORTANT: For streamers who may no longer be on Twitch (banned, moved platforms), "
        "still include them in 'streamers' but make sure 'related' has active Twitch "
        "streamers who were part of the same story.\n\n"
        "Examples:\n"
        '- "xQc vs Hasan debate" → {"streamers": ["xqc", "hasanabi"], "related": ["destiny", "mizkif", "ludwig"], "games": ["Just Chatting"]}\n'
        '- "Ninja failed on Mixer" → {"streamers": ["ninja"], "related": ["shroud", "timthetatman", "drlupo", "couragejd"], "games": ["Fortnite", "Just Chatting"]}\n'
        '- "Top Valorant plays" → {"streamers": ["tarik", "tenz"], "related": ["shroud", "sinatraa", "kyedae"], "games": ["VALORANT"]}\n'
        '- "DrDisrespect ban" → {"streamers": ["drdisrespect"], "related": ["timthetatman", "nickmercs", "shroud", "summit1g"], "games": ["Just Chatting", "Call of Duty: Warzone"]}\n\n'
        "Return ONLY valid JSON, nothing else."
    )
    try:
        raw = _call_gemini(prompt) or _call_claude(prompt) or ""
        # Extract JSON from response (may have markdown fencing)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        streamers = [s.lower().strip() for s in data.get("streamers", []) if isinstance(s, str)]
        related = [s.lower().strip() for s in data.get("related", []) if isinstance(s, str)]
        games = [g.strip() for g in data.get("games", []) if isinstance(g, str)]
        log.info("AI extracted sources — streamers: %s, related: %s, games: %s", streamers, related, games)
        return {"streamers": streamers[:4], "related": related[:5], "games": games[:3]}
    except Exception as exc:
        log.warning("Failed to extract clip sources from topic: %s", exc)
        # Fallback: try to find streamer names mentioned in the topic
        topic_lower = topic.lower()
        found = [s for s in DEFAULT_STREAMERS if s.lower() in topic_lower]
        if found:
            log.info("Keyword fallback found streamers: %s", found)
            return {"streamers": found, "games": []}
        return {}


def _fetch_clips_by_game(game_name: str, count: int = 5) -> list:
    """Fetch top clips for a Twitch game category."""
    token = _get_twitch_token()
    # Search for game ID
    try:
        resp = requests.get(
            "https://api.twitch.tv/helix/search/categories",
            headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
            params={"query": game_name, "first": 1},
            timeout=10,
        )
        resp.raise_for_status()
        categories = resp.json().get("data", [])
        if not categories:
            log.warning("No Twitch category found for '%s'", game_name)
            return []
        game_id = categories[0]["id"]
        log.info("Found game '%s' → ID %s", categories[0]["name"], game_id)
    except Exception as exc:
        log.warning("Game search failed for '%s': %s", game_name, exc)
        return []

    # Fetch clips by game_id
    urls = []
    try:
        resp = requests.get(
            "https://api.twitch.tv/helix/clips",
            headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
            params={"game_id": game_id, "first": count},
            timeout=10,
        )
        resp.raise_for_status()
        for clip in resp.json().get("data", []):
            slug = clip.get("id", "")
            if slug:
                try:
                    url = _get_clip_source_url(slug)
                    urls.append(url)
                except Exception as exc:
                    log.warning("Failed to resolve game clip %s: %s", slug, exc)
    except Exception as exc:
        log.warning("Game clip fetch failed: %s", exc)
    return urls


def fetch_clip_urls(streamers: list, count: int = 3) -> list:
    """Fetch Twitch clip slugs and resolve to download URLs."""
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
                slug = clip.get("id", "")
                if slug:
                    try:
                        url = _get_clip_source_url(slug)
                        urls.append(url)
                    except Exception as exc:
                        log.warning("Failed to resolve clip %s: %s", slug, exc)
        except Exception:
            continue
    return urls


def _fetch_pexels_clips(query: str = "gaming esports", count: int = 3) -> list:
    """Search Pexels for portrait videos as fallback footage. Returns download URLs."""
    if not PEXELS_API_KEY:
        return []
    try:
        resp = requests.get(
            "https://api.pexels.com/videos/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": query, "per_page": count, "orientation": "portrait"},
            timeout=15,
        )
        resp.raise_for_status()
        urls = []
        for video in resp.json().get("videos", []):
            files = video.get("video_files", [])
            # prefer HD portrait files
            best = sorted(files, key=lambda f: f.get("height", 0), reverse=True)
            if best:
                urls.append(best[0]["link"])
        log.info("Pexels returned %d fallback clips for query '%s'", len(urls), query)
        return urls
    except Exception as exc:
        log.warning("Pexels API failed: %s", exc)
        return []


def _generate_tts_edge(script_text: str, audio_path: Path) -> list:
    """Generate TTS audio using edge-tts and return sentence boundaries.

    Returns list of dicts: [{"offset_s": float, "duration_s": float, "text": str}, ...]
    """
    cleaned = clean_script(script_text)

    async def _run():
        communicate = edge_tts.Communicate(cleaned, EDGE_TTS_VOICE, rate=EDGE_TTS_RATE)
        boundaries = []
        with open(audio_path, "wb") as f:
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    f.write(chunk["data"])
                elif chunk["type"] == "SentenceBoundary":
                    boundaries.append({
                        "offset_s": chunk["offset"] / 10_000_000,
                        "duration_s": chunk["duration"] / 10_000_000,
                        "text": chunk["text"],
                    })
        return boundaries

    return asyncio.run(_run())


def _generate_ass_captions(boundaries: list, duration: float, ass_path: Path):
    """Generate ASS subtitle file with Hormozi-style word-by-word captions."""
    # ASS header with Montserrat-style bold font, centered
    # ASS header: bold Arial 62pt, white text, thick black outline, centered on screen
    # Alignment 5 = center-center, MarginV=100 nudges it slightly below true center
    header = """[Script Info]
Title: ClipLoreTV Captions
ScriptType: v4.00+
PlayResX: 720
PlayResY: 1280

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,52,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,4,0,5,20,20,100,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    events = []

    for boundary in boundaries:
        sentence = boundary["text"]
        sent_start = boundary["offset_s"]
        sent_dur = boundary["duration_s"]
        words = sentence.split()
        if not words:
            continue

        # 2-word chunks for punchy Hormozi-style delivery
        chunk_size = 2
        chunks = []
        for i in range(0, len(words), chunk_size):
            chunks.append(" ".join(words[i:i + chunk_size]))

        time_per_chunk = sent_dur / max(len(chunks), 1)

        for i, chunk in enumerate(chunks):
            cs = sent_start + i * time_per_chunk
            ce = sent_start + (i + 1) * time_per_chunk
            # ASS time format: H:MM:SS.cc
            s_str = f"{int(cs//3600)}:{int((cs%3600)//60):02d}:{int(cs%60):02d}.{int((cs%1)*100):02d}"
            e_str = f"{int(ce//3600)}:{int((ce%3600)//60):02d}:{int(ce%60):02d}.{int((ce%1)*100):02d}"

            # Karaoke-style: show full sentence, highlight active chunk yellow + 120%
            parts = []
            for j, c in enumerate(chunks):
                c_upper = c.upper().replace("\\", "\\\\")
                if j == i:
                    # Active chunk: yellow (&H00FFFF& in ASS BGR), scaled 120%
                    parts.append(
                        f"{{\\1c&H00FFFF&\\fscx120\\fscy120}}{c_upper}"
                        f"{{\\fscx100\\fscy100\\1c&H00FFFFFF&}}"
                    )
                else:
                    # Inactive chunk: white at normal size
                    parts.append(f"{{\\1c&H00FFFFFF&}}{c_upper}")
            text_line = " ".join(parts)
            events.append(f"Dialogue: 0,{s_str},{e_str},Default,,0,0,0,,{text_line}")

    ass_path.write_text(header + "\n".join(events), encoding="utf-8")
    log.info("Generated ASS captions with %d chunks for %.1fs", len(events), duration)


def _assign_captions_to_clips(boundaries: list, num_clips: int, vo_duration: float) -> list:
    """Map TTS sentence boundaries to per-clip caption chunks.

    Uses exact sentence timing from edge-tts (not estimated word chunks)
    to ensure captions are perfectly synced with speech.

    Returns a list of lists: one per clip. Each inner list has dicts:
    {"start": float, "end": float, "text": str} with clip-relative times.
    """
    per_clip_dur = vo_duration / max(num_clips, 1)
    result = [[] for _ in range(num_clips)]

    for boundary in boundaries:
        sent_start = boundary["offset_s"]
        sent_dur = boundary["duration_s"]
        text = boundary["text"].strip()
        if not text:
            continue

        # Truncate long sentences for readability on screen
        display = text.upper()
        if len(display) > 60:
            display = display[:57] + "..."

        # Determine which clip this sentence starts in
        clip_idx = min(int(sent_start / per_clip_dur), num_clips - 1)
        clip_start = clip_idx * per_clip_dur

        # If sentence spans into next clip, clamp end to this clip's boundary
        rel_start = sent_start - clip_start
        rel_end = rel_start + sent_dur
        if rel_end > per_clip_dur + 0.5:
            rel_end = per_clip_dur + 0.5

        result[clip_idx].append({
            "start": max(rel_start, 0),
            "end": rel_end,
            "text": display,
        })

    return result


def _build_drawtext_chain(chunks: list) -> str:
    """Build FFmpeg drawtext filter chain for per-clip captions.

    Each chunk: {"start": float, "end": float, "text": str}
    Returns a comma-separated drawtext filter string.
    """
    parts = []
    for ch in chunks:
        text = ch["text"].replace("'", "").replace(":", " -").replace("\\", "").replace("%", "%%")
        if not text.strip():
            continue
        parts.append(
            f"drawtext=text='{text}':fontsize=38:fontcolor=white:"
            f"borderw=3:bordercolor=black:"
            f"box=1:boxborderw=8:boxcolor=black@0.5:"
            f"x=(w-text_w)/2:y=h*0.82:"
            f"enable='between(t,{ch['start']:.2f},{ch['end']:.2f})'"
        )
    return ",".join(parts)


# Background music: Freesound API for real beats, FFmpeg synth as fallback
_BG_MUSIC_CACHE = TEMP_DIR / "bg_music.mp3"
_BG_MUSIC_SYNTH = TEMP_DIR / "bg_phonk_synth.mp3"
FREESOUND_API_KEY = os.getenv("FREESOUND_API_KEY", "")


def _fetch_freesound_music(duration: float) -> Path | None:
    """Download a royalty-free beat from Freesound.org API (CC0 license)."""
    if not FREESOUND_API_KEY:
        return None
    try:
        # Search for short beats with permissive license
        for query in ["phonk beat", "hip hop beat instrumental", "dark trap beat", "lo-fi beat"]:
            resp = requests.get(
                "https://freesound.org/apiv2/search/text/",
                params={
                    "query": query,
                    "filter": "duration:[10 TO 120] license:\"Creative Commons 0\"",
                    "fields": "id,name,duration,previews",
                    "page_size": 5,
                    "token": FREESOUND_API_KEY,
                },
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                break
        else:
            return None

        track = random.choice(results)
        # Freesound provides HQ mp3 and OGG previews
        preview_url = track.get("previews", {}).get("preview-hq-mp3")
        if not preview_url:
            return None

        log.info("Downloading Freesound track: %s (%.0fs)", track.get("name", "")[:40], track.get("duration", 0))
        dl = requests.get(preview_url, timeout=30)
        dl.raise_for_status()
        _BG_MUSIC_CACHE.write_bytes(dl.content)
        if _BG_MUSIC_CACHE.stat().st_size > 5000:
            return _BG_MUSIC_CACHE
    except Exception as exc:
        log.warning("Freesound music fetch failed: %s", exc)
    return None


def _generate_synth_beat(duration: float) -> Path | None:
    """Generate a phonk-style bass beat using FFmpeg synthesis (fallback)."""
    if _BG_MUSIC_SYNTH.exists() and _BG_MUSIC_SYNTH.stat().st_size > 5000:
        return _BG_MUSIC_SYNTH
    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", f"sine=frequency=55:duration={duration}",
                "-f", "lavfi", "-i", f"sine=frequency=110:duration={duration}",
                "-f", "lavfi", "-i", f"anoisesrc=d={duration}:c=pink:a=0.03",
                "-filter_complex",
                "[0:a]volume=0.06,tremolo=f=5.6:d=0.8[bass];"
                "[1:a]volume=0.03,tremolo=f=2.8:d=0.6[sub];"
                "[2:a]highpass=f=6000,lowpass=f=12000,tremolo=f=11.2:d=0.9[hat];"
                "[bass][sub][hat]amix=inputs=3:duration=longest,"
                "lowpass=f=15000,compand=attacks=0.01:decays=0.1:points=-80/-80|-20/-10|0/-3",
                "-c:a", "libmp3lame", "-b:a", "96k",
                str(_BG_MUSIC_SYNTH),
            ],
            capture_output=True, timeout=30,
        )
        if _BG_MUSIC_SYNTH.exists():
            log.info("Generated synth phonk beat (%.0fs)", duration)
            return _BG_MUSIC_SYNTH
    except Exception as exc:
        log.warning("Synth beat generation failed: %s", exc)
    return None


def _get_bg_music(duration: float = 120) -> Path | None:
    """Get background music: Freesound first, then synth fallback."""
    # Use cached file if fresh (less than 24 hours old)
    if _BG_MUSIC_CACHE.exists() and _BG_MUSIC_CACHE.stat().st_size > 5000:
        age = time.time() - _BG_MUSIC_CACHE.stat().st_mtime
        if age < 86400:
            return _BG_MUSIC_CACHE

    music = _fetch_freesound_music(duration)
    if music:
        return music

    return _generate_synth_beat(duration)


def assemble_video_from_parts(script_text: str, clip_urls: list, topic: str = "", hook_text: str = "") -> tuple:
    """Download clips, generate TTS with edge-tts, assemble 9:16 video with captions + music. Returns (video_path, duration)."""
    _cleanup_temp()

    job_id = uuid.uuid4().hex[:10]
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    # TTS voiceover with edge-tts (Microsoft neural voice)
    tts_path = job_dir / "voiceover.mp3"
    log.info("Generating TTS with edge-tts (%s)", EDGE_TTS_VOICE)
    try:
        boundaries = _generate_tts_edge(script_text, tts_path)
    except Exception as exc:
        log.warning("edge-tts failed (%s), falling back to gTTS", exc)
        from gtts import gTTS
        tts = gTTS(text=clean_script(script_text), lang="en", tld="co.uk")
        tts.save(str(tts_path))
        boundaries = []

    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(tts_path)],
        capture_output=True, text=True, timeout=15,
    )
    vo_duration = float(json.loads(probe.stdout)["format"]["duration"])

    # Download clips
    max_download = min(len(clip_urls), 9)
    log.info("Downloading up to %d clips (vo_duration=%.1fs)", max_download, vo_duration)
    clip_paths = []
    for i, url in enumerate(clip_urls[:max_download]):
        cp = job_dir / f"clip_{i}.mp4"
        try:
            r = requests.get(url, timeout=30, stream=True)
            r.raise_for_status()
            with open(cp, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            clip_paths.append(cp)
        except Exception as exc:
            log.warning("Clip download failed for %s: %s", url[:80], exc)
            continue

    # Pexels fallback if all Twitch clips failed
    if not clip_paths and PEXELS_API_KEY:
        log.warning("All Twitch clips failed — trying Pexels fallback")
        pexels_query = topic if topic else "gaming esports"
        pexels_urls = _fetch_pexels_clips(pexels_query, count=3)
        for i, url in enumerate(pexels_urls):
            cp = job_dir / f"pexels_{i}.mp4"
            try:
                r = requests.get(url, timeout=30, stream=True)
                r.raise_for_status()
                with open(cp, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                clip_paths.append(cp)
            except Exception as exc:
                log.warning("Pexels clip download failed: %s", exc)
                continue

    if not clip_paths:
        raise ValueError("Could not download any clips (Twitch + Pexels both failed)")

    # Scale clips to 720x1280 (9:16) — 720p keeps encoding fast on free tier
    # TikTok/YouTube upscale to 1080p on their end
    max_clips = min(len(clip_paths), max(int(vo_duration / 5), 4))
    use_clips = clip_paths[:max_clips]
    log.info("Scaling %d clips to 720x1280 (~%.0fs each)", len(use_clips), vo_duration / len(use_clips))
    scaled = []
    per_clip = vo_duration / max(len(use_clips), 1) + 0.5

    for i, cp in enumerate(use_clips):
        sp = job_dir / f"scaled_{i}.mp4"
        try:
            vf = "scale=720:1280:force_original_aspect_ratio=increase,crop=720:1280,setsar=1"

            # Add bold hook text overlay on the first clip (first 3 seconds)
            # This is the retention hook — big, centered, impossible to miss
            if i == 0 and hook_text:
                # Word-wrap long hooks: split into 2 lines if over 35 chars
                safe_hook = hook_text[:80].replace("'", "").replace(":", " -").replace("\\", "").replace("%", "%%")
                if len(safe_hook) > 35:
                    # Split at nearest space to midpoint
                    mid = len(safe_hook) // 2
                    space_idx = safe_hook.rfind(" ", 0, mid + 10)
                    if space_idx > 10:
                        safe_hook = safe_hook[:space_idx] + "\n" + safe_hook[space_idx + 1:]

                vf += (
                    f",drawtext=text='{safe_hook}':"
                    "fontsize=48:fontcolor=white:borderw=4:bordercolor=black:"
                    "box=1:boxborderw=14:boxcolor=black@0.7:"
                    "x=(w-text_w)/2:y=(h-text_h)/2:"
                    "enable='between(t,0,3)'"
                )

            result = subprocess.run(
                [
                    "ffmpeg", "-y", "-i", str(cp),
                    "-vf", vf,
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an",
                    "-t", str(per_clip),
                    str(sp),
                ],
                capture_output=True, timeout=180,
            )
            if sp.exists() and sp.stat().st_size > 1000:
                scaled.append(sp)
                log.info("Scaled clip %d/%d (%.0fs)", i + 1, len(use_clips), per_clip)
            else:
                # Fallback without drawtext if it fails (font issues)
                if i == 0:
                    subprocess.run(
                        ["ffmpeg", "-y", "-i", str(cp),
                         "-vf", "scale=720:1280:force_original_aspect_ratio=increase,crop=720:1280,setsar=1",
                         "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an",
                         "-t", str(per_clip), str(sp)],
                        capture_output=True, timeout=180,
                    )
                    if sp.exists():
                        scaled.append(sp)
                else:
                    log.warning("Clip %d scaling produced no output: %s", i,
                                result.stderr[-200:] if result.stderr else "")
        except subprocess.TimeoutExpired:
            log.warning("Clip %d scaling timed out after 180s, skipping", i)
        except Exception as exc:
            log.warning("Clip %d scaling failed: %s", i, exc)

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

    # Generate ASS captions for Cloud Run (or skip if no boundaries)
    ass_path = job_dir / "captions.ass"
    if boundaries:
        try:
            _generate_ass_captions(boundaries, vo_duration, ass_path)
        except Exception as exc:
            log.warning("ASS caption generation failed: %s", exc)

    # Mix voiceover with background music
    bg_music = _get_bg_music(duration=vo_duration + 5)
    mixed_audio = job_dir / "mixed_audio.mp3"
    if bg_music:
        try:
            log.info("Mixing voiceover with background music")
            subprocess.run(
                ["ffmpeg", "-y", "-i", str(tts_path), "-i", str(bg_music),
                 "-filter_complex",
                 "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=2",
                 "-c:a", "aac", "-b:a", "128k",
                 str(mixed_audio)],
                capture_output=True, timeout=60,
            )
            if not mixed_audio.exists() or mixed_audio.stat().st_size < 1000:
                mixed_audio = tts_path
        except Exception as exc:
            log.warning("Audio mixing failed: %s", exc)
            mixed_audio = tts_path
    else:
        mixed_audio = tts_path

    # Local merge: video + audio (no re-encode, fast copy)
    log.info("Local merge (vo=%.1fs, %d clips, music=%s)",
             vo_duration, len(scaled), bg_music is not None)
    merged = job_dir / "merged.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(concat_vid), "-i", str(mixed_audio),
         "-c:v", "copy",
         "-c:a", "aac", "-b:a", "128k", "-shortest", "-movflags", "+faststart",
         str(merged)],
        capture_output=True, timeout=120,
    )

    if not merged.exists():
        raise ValueError("Local video merge failed")

    # Cloud Run encoding: 1080p + captions + hook overlay
    final = job_dir / "final.mp4"
    if ENCODER_URL:
        try:
            final = _cloud_encode(merged, ass_path, hook_text, final)
            log.info("Cloud Run encoding complete: 1080p with captions")
        except Exception as exc:
            log.warning("Cloud Run encoding failed (%s), using local merge", exc)
            shutil.copy2(merged, final)
    else:
        # No Cloud Run — use local merge as-is (720p, no captions)
        shutil.copy2(merged, final)

    if not final.exists():
        raise ValueError("Final video assembly failed")

    return final, vo_duration, boundaries


def _cloud_encode(video_path: Path, ass_path: Path, hook_text: str, output_path: Path) -> Path:
    """Send video to Cloud Run encoder for 1080p re-encode with captions."""
    files = {"video": ("video.mp4", open(video_path, "rb"), "video/mp4")}
    if ass_path.exists() and ass_path.stat().st_size > 50:
        files["captions"] = ("captions.ass", open(ass_path, "rb"), "text/plain")

    data = {}
    if hook_text:
        data["hook_text"] = hook_text

    headers = {}
    if ENCODER_SECRET:
        headers["X-Api-Key"] = ENCODER_SECRET

    log.info("Sending %.1f MB to Cloud Run encoder at %s",
             video_path.stat().st_size / 1024 / 1024, ENCODER_URL)

    resp = requests.post(
        f"{ENCODER_URL}/encode",
        files=files,
        data=data,
        headers=headers,
        timeout=600,  # 10 min max
    )
    resp.raise_for_status()

    output_path.write_bytes(resp.content)
    log.info("Cloud Run returned %.1f MB encoded video", len(resp.content) / 1024 / 1024)
    return output_path


def upload_to_tiktok(video_path: Path, access_token: str, description: str = "") -> str:
    """Upload video to TikTok. Returns publish_id."""
    file_size = video_path.stat().st_size
    if file_size > 50 * 1024 * 1024:
        raise ValueError("Video too large (max 50 MB)")

    # Try direct publish first, fall back to inbox/draft if 403 (unaudited app)
    for endpoint in [
        "https://open.tiktokapis.com/v2/post/publish/video/init/",
        "https://open.tiktokapis.com/v2/post/publish/inbox/video/init/",
    ]:
        init_resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "post_info": {
                    "title": (description[:150]) if description else "ClipLoreTV",
                    "privacy_level": "SELF_ONLY",
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
        log.info("TikTok init (%s) response (%d): %s",
                 endpoint.split("/")[-3], init_resp.status_code, init_resp.text[:500])

        if init_resp.status_code == 403 and "inbox" not in endpoint:
            log.warning("Direct publish returned 403, trying inbox/draft endpoint")
            continue
        break

    if init_resp.status_code != 200:
        try:
            err_data = init_resp.json().get("error", {})
            err_code = err_data.get("code", "unknown")
            err_msg = err_data.get("message", init_resp.text[:300])
            raise ValueError(f"TikTok init failed ({init_resp.status_code}): {err_code} — {err_msg}")
        except (ValueError, KeyError):
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


# ---------------------------------------------------------------------------
# UPGRADE 3: YouTube Analytics Feedback Loop
# ---------------------------------------------------------------------------
def get_top_performing_topics() -> list[str]:
    """Fetch titles of top 5 performing videos from the last 30 days.

    Uses the YouTube Data API (channels + playlistItems + videos endpoints) to
    find the channel's recent uploads sorted by view count. Returns a list of
    title strings. Costs ~7 quota units per call (negligible vs 10,000/day).
    Fails gracefully — returns empty list on any error.
    """
    try:
        creds = _get_youtube_credentials()
        if not creds:
            log.info("YouTube analytics skipped: no credentials")
            return []

        youtube = build("youtube", "v3", credentials=creds)

        # Get the channel's uploads playlist ID
        channels_resp = youtube.channels().list(
            part="contentDetails", mine=True
        ).execute()
        if not channels_resp.get("items"):
            log.warning("YouTube analytics: no channel found for authenticated user")
            return []
        uploads_playlist_id = channels_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Fetch recent uploads (last 50 — we filter by date client-side)
        playlist_resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
        ).execute()

        video_ids = [item["contentDetails"]["videoId"] for item in playlist_resp.get("items", [])]
        if not video_ids:
            log.info("YouTube analytics: no uploads found")
            return []

        # Fetch statistics + snippet for these videos
        videos_resp = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(video_ids),
        ).execute()

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        recent_videos = []
        for v in videos_resp.get("items", []):
            published = v["snippet"].get("publishedAt", "")
            try:
                pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except (ValueError, TypeError):
                continue

            view_count = int(v["statistics"].get("viewCount", 0))
            title = v["snippet"].get("title", "")
            if title:
                recent_videos.append((view_count, title))

        # Sort by views descending, take top 5
        recent_videos.sort(key=lambda x: x[0], reverse=True)
        top_titles = [title for _, title in recent_videos[:5]]

        if top_titles:
            log.info("YouTube analytics: top %d performing titles fetched", len(top_titles))
        else:
            log.info("YouTube analytics: no videos in last 30 days")

        return top_titles

    except Exception as exc:
        log.warning("YouTube analytics feedback loop failed (non-blocking): %s", exc)
        return []


# ---------------------------------------------------------------------------
# UPGRADE 4: Auto-Generate Custom Thumbnails
# ---------------------------------------------------------------------------
def generate_thumbnail(video_path, hook_text: str) -> "Path | None":
    """Generate a 1280x720 YouTube thumbnail with text overlay.

    1. Extracts a high-action frame via FFmpeg scene detection
    2. Overlays dark gradient + bold text using Pillow
    Returns the thumbnail path, or None on failure.
    """
    if not HAS_PILLOW:
        log.warning("Thumbnail generation skipped: Pillow not installed")
        return None

    video_path = Path(video_path)
    thumb_dir = video_path.parent
    frame_path = thumb_dir / "thumb_frame.jpg"
    thumbnail_path = thumb_dir / "thumbnail.jpg"

    try:
        # Step 1: Extract a high-action frame using scene detection
        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-vf", "select=gt(scene\\,0.3)",
            "-frames:v", "1", "-q:v", "2",
            str(frame_path),
        ]
        subprocess.run(ffmpeg_cmd, capture_output=True, timeout=30)

        # Fallback: grab frame at 2 seconds if scene detection found nothing
        if not frame_path.exists() or frame_path.stat().st_size < 1000:
            log.info("Thumbnail: scene detection found nothing, falling back to t=2s")
            ffmpeg_fallback = [
                "ffmpeg", "-y", "-i", str(video_path),
                "-ss", "2", "-frames:v", "1", "-q:v", "2",
                str(frame_path),
            ]
            subprocess.run(ffmpeg_fallback, capture_output=True, timeout=30)

        if not frame_path.exists() or frame_path.stat().st_size < 1000:
            log.warning("Thumbnail: could not extract any frame from video")
            return None

        # Step 2: Build thumbnail with Pillow
        img = Image.open(frame_path)
        img = img.resize((1280, 720), Image.LANCZOS)

        # Dark gradient overlay on bottom 40%
        overlay = Image.new("RGBA", (1280, 720), (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        gradient_start_y = int(720 * 0.6)
        for y in range(gradient_start_y, 720):
            alpha = int(220 * (y - gradient_start_y) / (720 - gradient_start_y))
            draw_overlay.line([(0, y), (1280, y)], fill=(0, 0, 0, alpha))
        img = img.convert("RGBA")
        img = Image.alpha_composite(img, overlay)
        img = img.convert("RGB")

        # Load font — try several common bold fonts
        font = None
        font_size = 60
        font_paths = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
            "C:/Windows/Fonts/impact.ttf",
            "C:/Windows/Fonts/arialbd.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
        ]
        for fp in font_paths:
            try:
                font = ImageFont.truetype(fp, font_size)
                break
            except (OSError, IOError):
                continue
        if font is None:
            font = ImageFont.load_default()
            font_size = 20

        # Prepare hook text — wrap to 2-3 lines, uppercase for impact
        draw = ImageDraw.Draw(img)
        words = hook_text.upper().split()
        lines = []
        current_line = ""
        max_width = 1160  # 1280 - 120px padding

        for word in words:
            test_line = f"{current_line} {word}".strip()
            bbox = draw.textbbox((0, 0), test_line, font=font)
            if bbox[2] - bbox[0] > max_width and current_line:
                lines.append(current_line)
                current_line = word
                if len(lines) >= 3:
                    break
            else:
                current_line = test_line
        if current_line and len(lines) < 3:
            lines.append(current_line)

        # Draw text with black stroke for readability, positioned bottom-center
        text_block = "\n".join(lines)
        bbox = draw.multiline_textbbox((0, 0), text_block, font=font, spacing=8)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        text_x = (1280 - text_w) // 2
        text_y = 720 - text_h - 40  # 40px from bottom

        draw.multiline_text(
            (text_x, text_y), text_block, font=font,
            fill="white", stroke_width=3, stroke_fill="black",
            spacing=8, align="center",
        )

        img.save(str(thumbnail_path), "JPEG", quality=92)
        log.info("Thumbnail generated: %s (%d lines of text)", thumbnail_path, len(lines))

        # Cleanup extracted frame
        frame_path.unlink(missing_ok=True)
        return thumbnail_path

    except Exception as exc:
        log.warning("Thumbnail generation failed (non-blocking): %s", exc)
        for p in [frame_path, thumbnail_path]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        return None


def _set_youtube_thumbnail(video_id: str, thumbnail_path: Path):
    """Upload a custom thumbnail to an existing YouTube video. Best-effort."""
    try:
        creds = _get_youtube_credentials()
        if not creds:
            return
        youtube = build("youtube", "v3", credentials=creds)
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg"),
        ).execute()
        log.info("Custom thumbnail set for YouTube video %s", video_id)
    except Exception as exc:
        log.warning("YouTube thumbnail upload failed (non-blocking): %s", exc)


def upload_to_youtube(video_path: Path, title: str = "", description: str = "") -> str:
    """Upload video to YouTube Shorts. Returns video ID."""
    creds = _get_youtube_credentials()
    if not creds:
        raise ValueError("YouTube not authenticated. Visit /youtube/auth first.")

    youtube = build("youtube", "v3", credentials=creds)

    # Ensure title fits YouTube Shorts requirements (under 100 chars)
    yt_title = (title[:100]) if title else "ClipLoreTV #Shorts"

    # CRITICAL: #Shorts MUST be in the description for YouTube to classify as a Short.
    # Also add gaming hashtags for discoverability.
    shorts_tags = "#Shorts #twitch #gaming #streamer #cliploretv"
    if description:
        # Prepend #Shorts if not already present (case-insensitive check)
        if "#shorts" not in description.lower():
            yt_description = f"{description}\n\n{shorts_tags}"
        else:
            yt_description = description
    else:
        yt_description = shorts_tags

    body = {
        "snippet": {
            "title": yt_title,
            "description": yt_description,
            "tags": ["Shorts", "twitch", "gaming", "shorts", "cliploretv", "streamer", "twitchclips"],
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


def _upload_youtube_captions(video_id: str, boundaries: list):
    """Upload SRT captions to a YouTube video from TTS sentence boundaries."""
    if not boundaries:
        return
    try:
        # Build SRT from boundaries
        srt_lines = []
        for i, b in enumerate(boundaries):
            start = b["offset_s"]
            end = start + b["duration_s"]
            s_h, s_m = int(start // 3600), int((start % 3600) // 60)
            s_s, s_ms = int(start % 60), int((start % 1) * 1000)
            e_h, e_m = int(end // 3600), int((end % 3600) // 60)
            e_s, e_ms = int(end % 60), int((end % 1) * 1000)
            srt_lines.append(str(i + 1))
            srt_lines.append(f"{s_h:02d}:{s_m:02d}:{s_s:02d},{s_ms:03d} --> {e_h:02d}:{e_m:02d}:{e_s:02d},{e_ms:03d}")
            srt_lines.append(b["text"])
            srt_lines.append("")

        srt_path = TEMP_DIR / f"captions_{video_id}.srt"
        srt_path.write_text("\n".join(srt_lines), encoding="utf-8")

        creds = _get_youtube_credentials()
        if not creds:
            return
        youtube = build("youtube", "v3", credentials=creds)
        youtube.captions().insert(
            part="snippet",
            body={
                "snippet": {
                    "videoId": video_id,
                    "language": "en",
                    "name": "English",
                }
            },
            media_body=MediaFileUpload(str(srt_path), mimetype="application/x-subrip"),
        ).execute()
        log.info("Uploaded SRT captions for YouTube video %s", video_id)
        srt_path.unlink(missing_ok=True)
    except Exception as exc:
        log.warning("YouTube caption upload failed: %s", exc)


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


# ---------------------------------------------------------------------------
# Reddit Trending Topics
# ---------------------------------------------------------------------------
def get_trending_topics_reddit(min_score: int = 100) -> list:
    """Fetch hot posts from r/LivestreamFail with 100+ upvotes.

    Returns list of dicts: [{"title": str, "score": int, "url": str}, ...]
    Uses Reddit's public JSON API -- no authentication required.
    Fails gracefully: returns empty list on any error.
    """
    if not REDDIT_TRENDING_ENABLED:
        return []
    try:
        resp = requests.get(
            "https://www.reddit.com/r/LivestreamFail/hot.json?limit=25",
            headers={"User-Agent": "ClipLoreTV/1.0 (automated content pipeline)"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        trending = []
        for post in posts:
            pd = post.get("data", {})
            score = pd.get("score", 0)
            title = pd.get("title", "").strip()
            url = pd.get("url", "")
            if score >= min_score and title and not pd.get("stickied", False):
                trending.append({"title": title, "score": score, "url": url})
        trending.sort(key=lambda x: x["score"], reverse=True)
        log.info("Reddit trending: found %d posts with %d+ upvotes", len(trending), min_score)
        return trending
    except Exception as exc:
        log.warning("Reddit trending fetch failed (non-blocking): %s", exc)
        return []


# ---------------------------------------------------------------------------
# Kick.com Clip Sourcing
# ---------------------------------------------------------------------------
def fetch_kick_clips(streamer_name: str, count: int = 5) -> list:
    """Fetch clips from Kick.com for a given streamer.

    Returns list of clip download URLs (same format as Twitch clip fetcher).
    Uses Kick's public API -- no authentication required.
    Fails gracefully: returns empty list on any error.
    """
    if not KICK_ENABLED:
        return []
    try:
        resp = requests.get(
            f"https://kick.com/api/v2/channels/{streamer_name}/clips",
            headers={"User-Agent": "ClipLoreTV/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        clips_data = data.get("clips", data) if isinstance(data, dict) else data
        if isinstance(clips_data, dict):
            clips_data = clips_data.get("data", [])
        if not isinstance(clips_data, list):
            log.warning("Kick API returned unexpected format for '%s'", streamer_name)
            return []

        # Sort by view count descending, take top N
        clips_data.sort(key=lambda c: c.get("view_count", c.get("views", 0)), reverse=True)
        urls = []
        for clip in clips_data[:count]:
            # Kick clips have various URL fields depending on API version
            clip_url = (
                clip.get("clip_url")
                or clip.get("video_url")
                or clip.get("thumbnail_url", "").replace("-preview.jpg", ".mp4")
            )
            if clip_url and clip_url.endswith(".mp4"):
                urls.append(clip_url)
        log.info("Kick clips for '%s': found %d usable clips", streamer_name, len(urls))
        return urls
    except Exception as exc:
        log.warning("Kick clip fetch failed for '%s' (non-blocking): %s", streamer_name, exc)
        return []


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
@app.route("/tiktokabxYnkFg28kcPiB57ORrVrfxpGFZDtL5.txt")
def tiktok_verify():
    return "tiktok-developers-site-verification=abxYnkFg28kcPiB57ORrVrfxpGFZDtL5", 200, {"Content-Type": "text/plain"}

@app.route("/")
def home():
    return """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClipLoreTV</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0e0e10;color:#efeff1;min-height:100vh;display:flex;flex-direction:column;align-items:center}
header{width:100%;background:#18181b;padding:1rem 2rem;text-align:center;border-bottom:2px solid #9147ff}
header h1{font-size:1.8rem;color:#9147ff}
header p{color:#adadb8;margin-top:.3rem}
main{max-width:700px;padding:2rem 1.5rem;flex:1}
h2{color:#bf94ff;margin:1.5rem 0 .5rem;font-size:1.2rem}
p,li{line-height:1.7;color:#dedee3;margin-bottom:.5rem}
ul{padding-left:1.2rem}
a{color:#9147ff;text-decoration:none}a:hover{text-decoration:underline}
footer{width:100%;text-align:center;padding:1.5rem;color:#636369;font-size:.85rem;border-top:1px solid #26262c}
</style></head><body>
<header><h1>ClipLoreTV</h1><p>Automated gaming highlights for short-form video</p></header>
<main>
<h2>What is ClipLoreTV?</h2>
<p>ClipLoreTV is an automated content pipeline that transforms trending Twitch clips into
short-form videos with AI-generated commentary. Videos are published to TikTok, YouTube Shorts,
and Instagram Reels on a regular schedule.</p>
<h2>How It Works</h2>
<ul>
<li>Curates top clips from popular Twitch streamers every 12 hours</li>
<li>Generates engaging commentary scripts using AI</li>
<li>Assembles vertical (9:16) videos with text-to-speech narration</li>
<li>Publishes automatically to connected social media accounts</li>
</ul>
<h2>Links</h2>
<p><a href="/privacy">Privacy Policy</a> &middot; <a href="/tos">Terms of Service</a></p>
</main>
<footer>&copy; 2025&ndash;2026 ClipLoreTV. All rights reserved.</footer>
</body></html>"""


@app.route("/health")
def health():
    return "ok", 200


@app.route("/preview")
def preview_video():
    """Serve the last generated video for preview (no upload needed).
    Auth via X-Api-Key header or ?key= query param for browser access.
    """
    key = request.headers.get("X-Api-Key", "") or request.args.get("key", "")
    if not key or key != API_SECRET:
        abort(401)
    result = _last_pipeline_result.get("result", {})
    vpath = result.get("video_path", "")
    if vpath and Path(vpath).exists():
        return send_file(vpath, mimetype="video/mp4")
    return "No video available. Run /cron with test_only=true first.", 404


@app.route("/tos")
def tos():
    return """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Terms of Service - ClipLoreTV</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0e0e10;color:#efeff1;max-width:750px;margin:0 auto;padding:2rem 1.5rem;line-height:1.7}
h1{color:#9147ff;margin-bottom:.3rem}
h2{color:#bf94ff;margin:1.5rem 0 .5rem;font-size:1.1rem}
p,li{color:#dedee3;margin-bottom:.5rem}
ul{padding-left:1.2rem}
a{color:#9147ff}
.updated{color:#adadb8;font-size:.9rem;margin-bottom:1.5rem}
</style></head><body>
<h1>Terms of Service</h1>
<p class="updated">Last updated: March 13, 2026</p>

<h2>1. Acceptance of Terms</h2>
<p>By accessing or using ClipLoreTV ("ClipLoreTV", "the Service"), you agree to be bound by these Terms
of Service. If you do not agree, do not use the Service.</p>

<h2>2. Description of Service</h2>
<p>ClipLoreTV is an automated content pipeline that creates short-form gaming highlight
videos from publicly available Twitch clips, enhanced with AI-generated commentary.
Videos are published to third-party platforms including TikTok, YouTube, and Instagram.</p>

<h2>3. Eligibility</h2>
<p>You must be at least 18 years old to use the Service. By using the Service, you
represent that you meet this age requirement.</p>

<h2>4. User Accounts and Authentication</h2>
<p>To enable video publishing, the Service uses OAuth authentication with third-party
platforms. By connecting your social media accounts, you authorize ClipLoreTV to
upload video content on your behalf. You may revoke this access at any time through
the respective platform's settings.</p>

<h2>5. Content and Intellectual Property</h2>
<ul>
<li>Twitch clips used by the Service are publicly available content from the Twitch platform.</li>
<li>AI-generated commentary is original content created by the Service.</li>
<li>You retain ownership of any content on your connected social media accounts.</li>
<li>ClipLoreTV does not claim ownership of third-party content.</li>
</ul>

<h2>6. Prohibited Uses</h2>
<p>You agree not to:</p>
<ul>
<li>Use the Service for any unlawful purpose</li>
<li>Attempt to interfere with or disrupt the Service</li>
<li>Reverse-engineer or extract source code from the Service</li>
<li>Use the Service to distribute spam or misleading content</li>
</ul>

<h2>7. Disclaimer of Warranties</h2>
<p>The Service is provided "as is" without warranties of any kind, whether express
or implied. ClipLoreTV does not guarantee uninterrupted or error-free operation.</p>

<h2>8. Limitation of Liability</h2>
<p>To the fullest extent permitted by law, ClipLoreTV shall not be liable for any
indirect, incidental, or consequential damages arising from your use of the Service.</p>

<h2>9. Changes to Terms</h2>
<p>We reserve the right to modify these Terms at any time. Continued use of the Service
after changes constitutes acceptance of the updated Terms.</p>

<h2>10. Contact</h2>
<p>For questions about these Terms, contact us at
<a href="mailto:cliploretv@gmail.com">cliploretv@gmail.com</a>.</p>

<p style="margin-top:2rem"><a href="/">Back to home</a></p>
</body></html>"""


@app.route("/privacy")
def privacy():
    return """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Privacy Policy - ClipLoreTV</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0e0e10;color:#efeff1;max-width:750px;margin:0 auto;padding:2rem 1.5rem;line-height:1.7}
h1{color:#9147ff;margin-bottom:.3rem}
h2{color:#bf94ff;margin:1.5rem 0 .5rem;font-size:1.1rem}
p,li{color:#dedee3;margin-bottom:.5rem}
ul{padding-left:1.2rem}
a{color:#9147ff}
.updated{color:#adadb8;font-size:.9rem;margin-bottom:1.5rem}
</style></head><body>
<h1>Privacy Policy</h1>
<p class="updated">Last updated: March 13, 2026</p>

<h2>1. Introduction</h2>
<p>ClipLoreTV ("ClipLoreTV", "we", "our", "the Service") respects your privacy. This Privacy Policy
explains what data we collect, how we use it, and your rights regarding that data.</p>

<h2>2. Data We Collect</h2>
<p>ClipLoreTV collects minimal data necessary for operation:</p>
<ul>
<li><strong>OAuth Tokens:</strong> When you connect a social media account (TikTok, YouTube,
Instagram), we store the access token and refresh token required to publish videos on your behalf.</li>
<li><strong>Server Logs:</strong> Standard web server logs may include IP addresses, request timestamps,
and HTTP status codes. These are used for debugging and are not shared.</li>
</ul>
<p>We do <strong>not</strong> collect personal information such as names, email addresses, browsing
history, or device identifiers from end users or viewers of published content.</p>

<h2>3. How We Use Your Data</h2>
<ul>
<li>OAuth tokens are used solely to upload video content to your connected accounts.</li>
<li>Server logs are used for operational monitoring and troubleshooting.</li>
<li>We do not use your data for advertising, profiling, or analytics.</li>
</ul>

<h2>4. Data Storage and Security</h2>
<ul>
<li>OAuth tokens are stored in encrypted form on the server.</li>
<li>The Service is hosted on Render.com with HTTPS encryption in transit.</li>
<li>Access to stored tokens is restricted to the authenticated service only.</li>
</ul>

<h2>5. Data Sharing</h2>
<p>We do not sell, rent, or share your data with any third parties. Data is transmitted
only to the social media platforms you have explicitly authorized (TikTok, YouTube,
Instagram) for the purpose of publishing video content.</p>

<h2>6. Data Retention</h2>
<ul>
<li>OAuth tokens are retained until you revoke access or disconnect your account.</li>
<li>Generated videos are stored temporarily during processing and deleted after upload.</li>
<li>Server logs are retained for up to 30 days.</li>
</ul>

<h2>7. Your Rights</h2>
<p>You have the right to:</p>
<ul>
<li><strong>Revoke access:</strong> Disconnect your social media accounts at any time through
the respective platform's settings or by contacting us.</li>
<li><strong>Request deletion:</strong> Ask us to delete all stored tokens and data associated
with your accounts.</li>
<li><strong>Access your data:</strong> Request a copy of any data we hold about you.</li>
</ul>

<h2>8. Third-Party Services</h2>
<p>ClipLoreTV integrates with the following third-party services, each governed by
their own privacy policies:</p>
<ul>
<li><a href="https://www.tiktok.com/legal/privacy-policy" target="_blank">TikTok</a> &mdash; Content Posting API</li>
<li><a href="https://policies.google.com/privacy" target="_blank">Google/YouTube</a> &mdash; YouTube Data API</li>
<li><a href="https://www.twitch.tv/p/legal/privacy-notice/" target="_blank">Twitch</a> &mdash; Helix API for clip metadata</li>
<li><a href="https://privacycenter.instagram.com/policy" target="_blank">Instagram/Meta</a> &mdash; Graph API</li>
</ul>

<h2>9. Children's Privacy</h2>
<p>The Service is not directed at individuals under the age of 18. We do not knowingly
collect data from minors.</p>

<h2>10. Changes to This Policy</h2>
<p>We may update this Privacy Policy from time to time. Changes will be reflected by
the "Last updated" date above. Continued use of the Service constitutes acceptance
of any updates.</p>

<h2>11. Contact Us</h2>
<p>For privacy-related questions or data requests, contact us at
<a href="mailto:cliploretv@gmail.com">cliploretv@gmail.com</a>.</p>

<p style="margin-top:2rem"><a href="/">Back to home</a></p>
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
YOUTUBE_SCOPES = "https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube.readonly"


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
#  Async job status endpoint
# ===================================================================
@app.route("/job/<job_id>")
def job_status(job_id):
    """Check the status of an async job. Returns status, result (if done), or error (if failed)."""
    # Clean up expired jobs on access
    now = time.time()
    expired = [k for k, v in _jobs.items() if now - v["created"] > _JOBS_TTL]
    for k in expired:
        _jobs.pop(k, None)

    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found or expired"}), 404

    resp = {"job_id": job_id, "status": job["status"]}
    if job["status"] == "completed":
        resp["result"] = job["result"]
    elif job["status"] == "failed":
        resp["error"] = job["error"]
    return jsonify(resp)


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

    job_id = _create_job()

    def _bg():
        try:
            with app.app_context():
                video_path, duration, _ = assemble_video_from_parts(script, clip_urls)
                _complete_job(job_id, {
                    "video_path": str(video_path),
                    "duration": duration,
                })
        except Exception as e:
            log.error("Async assemble-video failed: %s", e, exc_info=True)
            _fail_job(job_id, f"Assembly failed: {_sanitize(str(e))}")

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"job_id": job_id, "status": "accepted", "poll": f"/job/{job_id}"}), 202


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

    job_id = _create_job()

    def _bg():
        try:
            with app.app_context():
                result = _run_pipeline(topic, pillar, streamers, tiktok_token, description, _return_dict=True)
                _complete_job(job_id, result)
        except Exception as exc:
            log.error("Async pipeline failed: %s", exc, exc_info=True)
            _fail_job(job_id, f"Pipeline failed: {_sanitize(str(exc))}")
            _send_notification("Pipeline Crashed", str(exc)[:500], success=False,
                               details={"topic": topic, "pillar": pillar})

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"job_id": job_id, "status": "accepted", "topic": topic, "poll": f"/job/{job_id}"}), 202


@app.route("/cron", methods=["POST"])
@require_api_key
def cron():
    """Kick off pipeline in background thread and return 202 immediately.

    This lets cron-job.org (30s max timeout) get a fast response while the
    pipeline runs for ~2 minutes in the background.
    """
    tiktok_token = _get_tiktok_token()

    data = request.get_json(silent=True) or {}
    streamers = data.get("streamers", random.sample(DEFAULT_STREAMERS, min(4, len(DEFAULT_STREAMERS))))
    description = data.get("description", "#twitch #gaming #streamer #cliploretv #shorts")
    test_only = data.get("test_only", False)

    # Try Reddit trending first, fall back to topic bank
    topic_source = "topic_bank"
    trending = get_trending_topics_reddit()
    if trending:
        picked = trending[0]  # highest-scored post
        topic = f"Create a video about this trending gaming topic: {picked['title']}"
        pillar = "hot_take"
        topic_source = "reddit"
        log.info("CRON: using Reddit trending topic (score=%d): %s", picked["score"], picked["title"][:60])
    else:
        topic, pillar = _pick_topic()
        log.info("CRON: Reddit unavailable or no trending posts, using topic bank")

    log.info("CRON: source=%s topic=%s pillar=%s streamers=%s test_only=%s",
             topic_source, topic[:50], pillar, streamers, test_only)

    def _bg():
        try:
            with app.app_context():
                _run_pipeline(topic, pillar, streamers, tiktok_token, description, skip_upload=test_only)
        except Exception as exc:
            log.error("Background pipeline crashed: %s", exc, exc_info=True)
            _send_notification("Pipeline Crashed", str(exc)[:500], success=False,
                               details={"topic": topic, "pillar": pillar, "topic_source": topic_source})
            _last_pipeline_result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _last_pipeline_result["result"] = {"error": str(exc), "topic": topic}

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"status": "accepted", "topic": topic, "pillar": pillar, "topic_source": topic_source}), 202


def _run_pipeline(topic, pillar, streamers, tiktok_token, description, skip_upload=False, _return_dict=False):
    """Shared pipeline logic for /pipeline and /cron.

    When _return_dict=True, returns a plain dict (for async callers).
    Otherwise returns a Flask jsonify response (legacy behavior for /cron).
    """
    results = {"topic": topic, "pillar": pillar, "streamers": streamers, "steps": {}}

    def _err_response(err_msg, code=500):
        results["error"] = err_msg
        _last_pipeline_result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _last_pipeline_result["result"] = results
        if _return_dict:
            raise RuntimeError(err_msg)
        return jsonify({"error": err_msg, "steps": results["steps"]}), code

    # Step 0: YouTube analytics feedback loop (non-blocking)
    top_topics = []
    try:
        top_topics = get_top_performing_topics()
        if top_topics:
            results["steps"]["analytics"] = {"status": "ok", "top_topics": top_topics}
        else:
            results["steps"]["analytics"] = {"status": "skipped", "reason": "no data"}
    except Exception as e:
        results["steps"]["analytics"] = {"status": "skipped", "reason": str(e)[:100]}
        log.warning("Analytics feedback skipped: %s", e)

    # Step 1: Generate script
    try:
        script_text = generate_script_text(topic, pillar, top_topics=top_topics)
        results["steps"]["script"] = {"status": "ok", "word_count": len(script_text.split())}
    except Exception as e:
        err = f"Script generation failed: {_sanitize(str(e))}"
        _send_notification("❌ Pipeline Failed", err, success=False, details={"topic": topic})
        return _err_response(err)

    # Step 2: Fetch clips — smart sourcing based on topic
    clip_urls = []
    try:
        # First: ask AI for relevant streamers/games/related
        sources = _extract_clip_sources(topic)
        ai_streamers = sources.get("streamers", [])
        ai_related = sources.get("related", [])
        ai_games = sources.get("games", [])

        # Try primary streamers first (the ones directly mentioned in the topic)
        if ai_streamers:
            clip_urls = fetch_clip_urls(ai_streamers, count=3)
            log.info("Got %d clips from primary streamers %s", len(clip_urls), ai_streamers)

        # If not enough, try related streamers (same scene/event, still relevant)
        if len(clip_urls) < 4 and ai_related:
            related_clips = fetch_clip_urls(ai_related, count=3)
            clip_urls.extend(related_clips)
            log.info("Got %d clips from related streamers %s", len(related_clips), ai_related)

        # If still not enough, try game-based clips (visually relevant)
        if len(clip_urls) < 4 and ai_games:
            for game in ai_games:
                game_clips = _fetch_clips_by_game(game, count=5)
                clip_urls.extend(game_clips)
                log.info("Got %d clips from game '%s'", len(game_clips), game)
                if len(clip_urls) >= 8:
                    break

        # Kick.com clip sourcing — merge with Twitch clips
        kick_clip_count = 0
        if KICK_ENABLED:
            kick_streamers = ai_streamers + ai_related if (ai_streamers or ai_related) else streamers
            for ks in kick_streamers[:4]:  # limit to 4 streamers to avoid slowdown
                try:
                    kick_clips = fetch_kick_clips(ks, count=3)
                    if kick_clips:
                        clip_urls.extend(kick_clips)
                        kick_clip_count += len(kick_clips)
                except Exception as exc:
                    log.warning("Kick fetch for '%s' failed (non-blocking): %s", ks, exc)
            if kick_clip_count:
                log.info("Got %d additional clips from Kick.com", kick_clip_count)

        # Last resort: fall back to random DEFAULT_STREAMERS
        if len(clip_urls) < 3:
            fallback = random.sample(DEFAULT_STREAMERS, min(4, len(DEFAULT_STREAMERS)))
            log.info("Falling back to random streamers: %s", fallback)
            clip_urls.extend(fetch_clip_urls(fallback, count=3))

        results["steps"]["clips"] = {"status": "ok", "count": len(clip_urls),
                                     "sources": ai_streamers or streamers,
                                     "kick_clips": kick_clip_count}
    except Exception as e:
        err = f"Clip fetch failed: {_sanitize(str(e))}"
        _send_notification("❌ Pipeline Failed", err, success=False, details={"topic": topic})
        return _err_response(err)

    if not clip_urls:
        _send_notification("❌ Pipeline Failed", "No clips found", success=False, details={"topic": topic})
        return _err_response("No clips found", 400)

    # Extract first sentence from script as hook text for video overlay
    cleaned = clean_script(script_text)
    first_sentence = re.split(r'[.?!]', cleaned)[0].strip() if cleaned else ""
    hook_text = first_sentence if len(first_sentence) > 10 else topic[:45]

    # Step 3: Assemble video
    try:
        video_path, duration, tts_boundaries = assemble_video_from_parts(script_text, clip_urls, topic=topic, hook_text=hook_text)
        results["steps"]["video"] = {"status": "ok", "duration": duration}
    except Exception as e:
        err = f"Assembly failed: {_sanitize(str(e))}"
        _send_notification("❌ Pipeline Failed", err, success=False, details={"topic": topic})
        return _err_response(err)

    # Step 3b: Generate optimized Shorts title
    try:
        short_title = generate_short_title(topic, script_text)
        results["steps"]["title"] = {"status": "ok", "title": short_title}
    except Exception as e:
        short_title = topic[:57] + "..." if len(topic) > 60 else topic
        results["steps"]["title"] = {"status": "fallback", "title": short_title}
        log.warning("Title generation failed, using fallback: %s", e)

    # Step 3c: Generate custom thumbnail (non-blocking)
    thumbnail_path = None
    try:
        thumbnail_path = generate_thumbnail(video_path, hook_text)
        if thumbnail_path:
            results["steps"]["thumbnail"] = {"status": "ok", "path": str(thumbnail_path)}
        else:
            results["steps"]["thumbnail"] = {"status": "skipped", "reason": "generation returned None"}
    except Exception as e:
        results["steps"]["thumbnail"] = {"status": "skipped", "reason": str(e)[:100]}
        log.warning("Thumbnail generation skipped: %s", e)

    # Step 4: Post to platforms (each independent — one failure doesn't block others)
    all_ok = True

    if skip_upload:
        results["steps"]["tiktok"] = {"status": "skipped", "reason": "test_only mode"}
        results["steps"]["youtube"] = {"status": "skipped", "reason": "test_only mode"}
        results["steps"]["instagram"] = {"status": "skipped", "reason": "test_only mode"}
        results["video_path"] = str(video_path)
        log.info("TEST MODE — skipping uploads, video at %s", video_path)
        _last_pipeline_result["result"] = results
        _last_pipeline_result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _send_notification("🧪 Test Pipeline Complete", f"Video ready (no upload)\n{topic[:60]}", success=True, details=results)
        if _return_dict:
            return results
        return jsonify(results), 200

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
            # Upload custom thumbnail (non-blocking, best-effort)
            if thumbnail_path and thumbnail_path.exists():
                _set_youtube_thumbnail(video_id, thumbnail_path)
                results["steps"]["youtube"]["thumbnail"] = "set"
            # Upload SRT captions (non-blocking, best-effort)
            if tts_boundaries:
                _upload_youtube_captions(video_id, tts_boundaries)
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

    # Store last result for /status endpoint
    _last_pipeline_result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _last_pipeline_result["result"] = results

    # Send notification
    if all_ok:
        _send_notification("✅ Pipeline Complete", topic[:200], success=True, details=platforms)
    else:
        _send_notification("⚠️ Pipeline Partial Failure", topic[:200], success=False, details=platforms)

    if _return_dict:
        return results
    return jsonify(results)


@app.route("/status")
def pipeline_status():
    """Return the result of the last pipeline run."""
    if not _last_pipeline_result["timestamp"]:
        return jsonify({"status": "no runs yet"})
    return jsonify(_last_pipeline_result)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
