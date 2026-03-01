# Twitch Video Generator — Free Pictory Replacement

Self-hosted Flask service that turns a script into a 9:16 MP4.
**Cost: $0/month** (runs on Render.com free tier).

## What it does
1. Converts your script to speech with Google TTS (free, no key needed)
2. Pulls relevant vertical B-roll from Pexels (free API)
3. Burns in captions with FFmpeg
4. Returns a downloadable MP4

---

## Deploy to Render.com (one-time, ~5 min)

1. **Push this folder to a GitHub repo** (public or private)
   ```
   git init
   git add .
   git commit -m "video generator service"
   gh repo create twitch-video-gen --public --push
   ```

2. **Go to [render.com](https://render.com)** → New → Web Service → Connect your repo

3. **Settings** (Render will auto-detect from render.yaml):
   - Environment: Python
   - Build: `apt-get install -y ffmpeg && pip install -r requirements.txt`
   - Start: `gunicorn main:app --bind 0.0.0.0:$PORT --timeout 300 --workers 1`

4. **Add environment variable** in Render dashboard:
   - Key: `PEXELS_API_KEY`
   - Value: get free key at [pexels.com/api](https://www.pexels.com/api/)

5. **Deploy** — Render gives you a URL like `https://twitch-video-gen.onrender.com`

---

## API Reference

### POST /generate
```json
{
  "script": "The full video script text here...",
  "topic": "xQc banned from Twitch"
}
```
Returns:
```json
{
  "job_id": "abc123",
  "status_url": "/status/abc123",
  "download_url": "/download/abc123"
}
```

### GET /download/{job_id}
Blocks until video is ready, then streams the MP4.
Make.com HTTP module → URL → `/download/{job_id}` → video file.

### GET /status/{job_id}
Returns current status: `queued` → `generating_audio` → `fetching_footage` → `assembling_video` → `done`

---

## Make.com integration

In your Make.com scenario, replace the Pictory HTTP module with:

**Module: HTTP → Make a Request**
- URL: `https://your-service.onrender.com/generate`
- Method: POST
- Body type: Raw / JSON
- Body:
  ```json
  {
    "script": "{{claude_api_response.content[].text}}",
    "topic": "{{topic_from_sheet}}"
  }
  ```

Then add a **Tools → Sleep** module (60 seconds), followed by:

**Module: HTTP → Make a Request**
- URL: `https://your-service.onrender.com/download/{{job_id}}`
- Method: GET
- This returns the MP4 file for uploading to YouTube/TikTok

---

## Free tier limits
- Render free: 750 hours/month, spins down after 15min idle (wakes in ~30s)
- Pexels free: 200 requests/hour, 20,000/month
- Google TTS: free, no limits for reasonable use
