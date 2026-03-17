# ClipLoreTV — Automated Twitch Clip -> TikTok Pipeline

## Project Overview
Flask web service that automatically generates TikTok videos from Twitch clips with AI-generated scripts and voiceover. Deployed on Render free tier with auto-deploy from GitHub.

## Architecture
- **Backend:** Flask (Python 3.11) with Gunicorn
- **Hosting:** Render.com free tier (`https://twitch-video-gen.onrender.com`)
- **Repo:** `github.com/jzvasko-cloud/twitch-video-gen` (branch: `main`, autoDeploy: true)
- **Cron:** External cron service hits POST `/cron` every 12 hours with `X-Api-Key` header

## Key APIs
- **Claude API (Sonnet)** — generates commentary scripts
- **Twitch Helix API** — fetches top clips (client credentials auth, token auto-refreshes)
- **gTTS** — text-to-speech for voiceover
- **FFmpeg** — video processing (scale/crop to 1080x1920 9:16, concat clips, merge audio)
- **TikTok Content Posting API v2** — OAuth Login Kit with PKCE for publishing
- **Pexels API** — stock footage fallback when Twitch clips are unavailable

## Environment Variables (set in Render dashboard)
- `ANTHROPIC_API_KEY` — Claude API
- `TWITCH_CLIENT_ID` / `TWITCH_CLIENT_SECRET` — Twitch Helix
- `TIKTOK_CLIENT_KEY` / `TIKTOK_CLIENT_SECRET` — TikTok OAuth
- `PEXELS_API_KEY` — stock footage
- `CLIPLORE_API_SECRET` — API key for authenticating POST endpoints and cron

## Code Structure
Core logic extracted into reusable functions:
- `generate_script_text(topic, pillar)` — calls Claude API
- `clean_script(raw)` — strips [VISUAL]/[PAUSE] tags
- `fetch_clip_urls(streamers, count)` — gets Twitch clip download URLs
- `assemble_video_from_parts(script, clip_urls)` — TTS + FFmpeg assembly
- `upload_to_tiktok(video_path, token, description)` — TikTok upload
- `_run_pipeline(...)` — shared pipeline used by both /pipeline and /cron

## Render Service Info
- Service ID: `srv-d6ich8haae7s73ce1i70`
- Dashboard: `https://dashboard.render.com/web/srv-d6ich8haae7s73ce1i70`

## Current Status
- [x] Code deployed and running on Render
- [x] All env vars configured
- [x] Code cleaned up — no more duplicated pipeline logic
- [ ] TikTok OAuth — needs redirect URI + scopes verified in TikTok Developer Portal
- [ ] External cron service — needs setup (cron-job.org or similar)
- [ ] End-to-end pipeline test once TikTok auth is resolved
