
# Koyeb-Ready Telegram Bot (long polling + health endpoint)

This app runs a Telegram bot **and** a tiny HTTP server for Koyeb health checks.

## Deploy on Koyeb
1) Create a new GitHub repo with these files (`app.py`, `requirements.txt`, `Dockerfile`).
2) In Koyeb → Create App → Deploy Service → From GitHub → choose this repo.
3) Service settings:
   - **Service type**: Web Service
   - **Expose port**: `8080` (or leave default; Koyeb sets `PORT` env var)
   - **Health check**: HTTP GET `/health` on the exposed port
4) **Environment variables**:
   - `BOT_TOKEN` (required) — your BotFather token
   - `ADMIN_CHAT_ID` (optional) — numeric chat id; or use `/make_me_admin` after deploy
   - Optional: `STORE_NAME`, `STORE_ADDRESS`, `STORE_PHONE`, `STORE_LAT`, `STORE_LON`
5) Deploy and check Logs for: `HTTP health server running on 0.0.0.0:PORT` and `Starting bot...`.

## Commands
- `/start`, `/info`, `/feedback`, `/ping`, `/id`, `/admin`, `/make_me_admin`

## Why an HTTP server?
Koyeb Web Services expect an open port + health checks. The `/health` endpoint keeps the Service marked healthy.
