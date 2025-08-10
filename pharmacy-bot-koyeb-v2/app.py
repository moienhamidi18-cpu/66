
import os
import asyncio
import logging
from typing import Optional
from datetime import datetime

from aiohttp import web
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)
log = logging.getLogger("pharmacy-bot")

# --- Config via env ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set.")

STORE_NAME = os.getenv("STORE_NAME", "Moein Pharmacy")
STORE_ADDRESS = os.getenv("STORE_ADDRESS", "Address not set")
STORE_PHONE = os.getenv("STORE_PHONE", "Phone not set")
STORE_LAT = os.getenv("STORE_LAT")
STORE_LON = os.getenv("STORE_LON")

try:
    ADMIN_CHAT_ID: Optional[int] = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
except Exception:
    ADMIN_CHAT_ID = 0

PORT = int(os.getenv("PORT", "8080"))  # Koyeb sets PORT for web services

FEEDBACK = 1  # conversation state

def kb_main():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Info")],
            [KeyboardButton("Feedback")],
            [KeyboardButton("Send Prescription (photo)")]
        ],
        resize_keyboard=True
    )

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Welcome to {STORE_NAME}! Choose an option:",
        reply_markup=kb_main()
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"Your chat ID is {chat.id}")

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ADMIN_CHAT_ID
    await update.message.reply_text(f"Admin chat id is: {ADMIN_CHAT_ID}")

async def make_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ADMIN_CHAT_ID
    ADMIN_CHAT_ID = update.effective_chat.id
    await update.message.reply_text(f"Done. ADMIN_CHAT_ID set to {ADMIN_CHAT_ID}")
    log.info("ADMIN_CHAT_ID set via /make_me_admin: %s", ADMIN_CHAT_ID)

async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = f"""{STORE_NAME}
Address: {STORE_ADDRESS}
Phone: {STORE_PHONE}
"""
    await update.message.reply_text(txt)
    try:
        if STORE_LAT and STORE_LON:
            lat = float(STORE_LAT); lon = float(STORE_LON)
            await update.message.bot.send_location(chat_id=update.effective_chat.id, latitude=lat, longitude=lon)
    except Exception as e:
        log.warning("Failed to send location: %s", e)

async def feedback_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please type your feedback. I will forward it to the pharmacist.")
    return FEEDBACK

async def feedback_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ADMIN_CHAT_ID
    text = update.message.text or ""
    user = update.effective_user
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    summary = (
        f"📝 New Feedback\n"
        f"From: {user.full_name} (id {user.id})\n"
        f"Time: {ts}\n"
        f"Text: {text}"
    )
    if ADMIN_CHAT_ID:
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=summary)
        await update.message.reply_text("Thanks! Your feedback was sent ✅")
    else:
        await update.message.reply_text("Received your feedback. (Admin isn't configured yet, so it wasn't delivered.)")
        log.info("Feedback received but admin not set.")
    return ConversationHandler.END

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ADMIN_CHAT_ID
    msg = update.message
    if not msg or not msg.photo:
        return

    file_id = msg.photo[-1].file_id
    caption = msg.caption or "(no caption)"
    user = update.effective_user
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    header = (
        f"🧾 Prescription photo\n"
        f"From: {user.full_name} (id {user.id})\n"
        f"Time: {ts}\n"
        f"Caption: {caption}"
    )

    if ADMIN_CHAT_ID:
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=header)
        await context.bot.send_photo(chat_id=ADMIN_CHAT_ID, photo=file_id)
        await msg.reply_text("Thanks! Your prescription was sent ✅")
    else:
        await msg.reply_text("Received the photo (admin isn't configured yet, so it wasn't forwarded).")
        log.info("Photo received but admin not set.")

# --- Minimal HTTP server for Koyeb health checks ---
async def handle_root(request):
    return web.Response(text="OK")

async def handle_health(request):
    return web.json_response({"status": "ok", "admin": ADMIN_CHAT_ID})

async def run_http_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    log.info("HTTP health server running on 0.0.0.0:%s", PORT)

async def main_async():
    log.info("Starting bot... Admin chat id at startup: %s", ADMIN_CHAT_ID)
    tg_app = Application.builder().token(BOT_TOKEN).build()

    # telegram handlers
    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("ping", ping_cmd))
    tg_app.add_handler(CommandHandler("id", id_cmd))
    tg_app.add_handler(CommandHandler("info", info_cmd))
    tg_app.add_handler(CommandHandler("admin", admin_cmd))
    tg_app.add_handler(CommandHandler("make_me_admin", make_admin_cmd))
    tg_app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("feedback", feedback_start), MessageHandler(filters.Regex(r"^(?i)feedback$"), feedback_start)],
        states={
            FEEDBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, feedback_collect)]
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)]
    ))
    tg_app.add_handler(MessageHandler(filters.Regex(r"^(?i)info$"), info_cmd))
    tg_app.add_handler(MessageHandler(filters.PHOTO, photo_handler))

    # Start both the Telegram polling and the HTTP health server
    await tg_app.initialize()
    await tg_app.start()
    await run_http_app()
    # Keep running until interrupted
    try:
        await asyncio.Event().wait()
    finally:
        await tg_app.stop()
        await tg_app.shutdown()

if __name__ == "__main__":
    asyncio.run(main_async())
