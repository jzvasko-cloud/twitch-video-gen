import os
import threading
import time
import urllib.request
from flask import Flask

app = Flask(__name__)

SELF_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://twitch-video-gen.onrender.com")

_keep_alive_started = False

def keep_alive():
    """Ping ourselves every 5 minutes to prevent Render free-tier spin-down."""
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

@app.route("/")
def home():
    return "ClipLoreTV Auto-Post Service is running."

@app.route("/health")
def health():
    return "ok", 200

@app.route("/tiktokgf5YuUb7tBa2vRZZ306I0dDfa1eCsk2Q.txt")
def tiktok_verify():
    return "tiktok-developers-site-verification=gf5YuUb7tBa2vRZZ306I0dDfa1eCsk2Q", 200, {"Content-Type": "text/plain; charset=utf-8"}

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
