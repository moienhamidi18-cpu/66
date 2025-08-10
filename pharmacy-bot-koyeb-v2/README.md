
# Koyeb Deploy — Two Ways

This repo supports **both** Dockerfile and Buildpack deployments.

## Option A — Dockerfile (recommended)
- Keep these files at the repo root: `Dockerfile`, `app.py`, `requirements.txt`
- In Koyeb: Create App → Deploy Service → From GitHub → choose this repo
  - Build method: **Dockerfile**
  - Dockerfile path: `Dockerfile`
  - **Expose Port**: 8080
  - Health check: HTTP GET `/health`
  - Env: set `BOT_TOKEN`, `ADMIN_CHAT_ID`, etc.

## Option B — Buildpack
- Ensure `requirements.txt` and **Procfile** exist at the repo root
- Procfile content: `web: python app.py`
- In Koyeb: Build method **Buildpack** (or leave auto-detect), Start command field can be empty because Procfile handles it
- Port/Health same as above

## Monorepo note
If your files are in a subfolder, set the **Monorepo path** (or move files to root).

## Test
After deploy, check logs for:
- `HTTP health server running on 0.0.0.0:PORT`
- `Starting bot... Admin chat id at startup: ...`
Then in Telegram: `/ping`, `/info`, photo upload.
