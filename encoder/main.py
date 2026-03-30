"""
ClipLoreTV Encoder — Cloud Run service for 1080p video encoding with captions.

Accepts a concatenated video + optional ASS subtitle file via multipart POST,
re-encodes at 1080p with captions burned in, and returns the finished video.

Designed to offload heavy FFmpeg work from Render free tier.
"""

import os
import uuid
import subprocess
import tempfile
import shutil
import time
import logging
import secrets
from pathlib import Path

from flask import Flask, request, send_file, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("encoder")

API_SECRET = os.environ.get("ENCODER_API_SECRET", "")
TEMP_DIR = Path(tempfile.gettempdir()) / "encoder"
TEMP_DIR.mkdir(exist_ok=True)
MAX_UPLOAD_MB = 200

app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


def _check_auth():
    if not API_SECRET:
        return None  # No auth configured (dev mode)
    key = request.headers.get("X-Api-Key", "")
    if not key or not secrets.compare_digest(key, API_SECRET):
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.route("/health")
def health():
    return "ok", 200


@app.route("/encode", methods=["POST"])
def encode():
    """Accept video + optional ASS file, return 1080p encoded video with captions.

    Multipart form fields:
    - video: MP4 video file (required)
    - captions: ASS subtitle file (optional)
    - hook_text: Text to overlay on first 3 seconds (optional)
    - crf: Encoding quality 0-51, default 26 (optional)
    """
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    if "video" not in request.files:
        return jsonify({"error": "video file is required"}), 400

    job_id = uuid.uuid4().hex[:10]
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    try:
        # Save uploaded video
        video_file = request.files["video"]
        input_path = job_dir / "input.mp4"
        video_file.save(str(input_path))
        log.info("Job %s: received video (%.1f MB)", job_id, input_path.stat().st_size / 1024 / 1024)

        # Save optional captions
        ass_path = None
        if "captions" in request.files:
            ass_path = job_dir / "captions.ass"
            request.files["captions"].save(str(ass_path))
            log.info("Job %s: received ASS captions", job_id)

        # Optional params
        hook_text = request.form.get("hook_text", "")
        crf = request.form.get("crf", "26")
        try:
            crf = str(max(18, min(35, int(crf))))
        except ValueError:
            crf = "26"

        output_path = job_dir / "output.mp4"
        start_time = time.time()

        # Build FFmpeg command
        vf_parts = ["scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1"]

        # Add hook text overlay (first 3 seconds)
        if hook_text:
            safe_hook = hook_text[:80].replace("'", "").replace(":", " -").replace("\\", "").replace("%", "%%")
            vf_parts.append(
                f"drawtext=text='{safe_hook}':"
                "fontsize=56:fontcolor=white:borderw=4:bordercolor=black:"
                "box=1:boxborderw=12:boxcolor=black@0.6:"
                "x=(w-text_w)/2:y=h*0.15:"
                "enable='between(t,0,3)'"
            )

        # Add ASS caption burn
        if ass_path and ass_path.exists() and ass_path.stat().st_size > 50:
            # Update ASS resolution to 1080x1920
            ass_content = ass_path.read_text(encoding="utf-8")
            ass_content = ass_content.replace("PlayResX: 720", "PlayResX: 1080")
            ass_content = ass_content.replace("PlayResY: 1280", "PlayResY: 1920")
            # Scale font size from 52 (720p) to 80 (1080p)
            ass_content = ass_content.replace(",52,", ",80,")
            ass_content = ass_content.replace(",4,0,5,", ",6,0,5,")  # outline width
            ass_path.write_text(ass_content, encoding="utf-8")

            ass_escaped = str(ass_path).replace("\\", "/").replace(":", "\\:")
            vf_parts.append(f"ass={ass_escaped}")

        vf = ",".join(vf_parts)

        cmd = [
            "ffmpeg", "-y", "-i", str(input_path),
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", crf,
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(output_path),
        ]

        log.info("Job %s: encoding at 1080p (crf=%s, captions=%s, hook=%s)",
                 job_id, crf, ass_path is not None, bool(hook_text))

        result = subprocess.run(cmd, capture_output=True, timeout=600)

        if not output_path.exists() or output_path.stat().st_size < 10000:
            stderr = result.stderr.decode(errors="replace")[-500:] if result.stderr else "no stderr"
            log.error("Job %s: encoding failed: %s", job_id, stderr)
            return jsonify({"error": "Encoding failed", "details": stderr}), 500

        elapsed = time.time() - start_time
        out_size = output_path.stat().st_size / 1024 / 1024
        log.info("Job %s: done in %.1fs (%.1f MB output)", job_id, elapsed, out_size)

        return send_file(
            str(output_path),
            mimetype="video/mp4",
            as_attachment=True,
            download_name=f"encoded_{job_id}.mp4",
        )

    except subprocess.TimeoutExpired:
        log.error("Job %s: encoding timed out after 600s", job_id)
        return jsonify({"error": "Encoding timed out (10 min limit)"}), 504
    except Exception as exc:
        log.error("Job %s: unexpected error: %s", job_id, exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500
    finally:
        # Clean up job directory
        try:
            shutil.rmtree(job_dir, ignore_errors=True)
        except Exception:
            pass


@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": f"Upload too large (max {MAX_UPLOAD_MB} MB)"}), 413


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
