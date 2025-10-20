# FINboot.py — one-file Telegram finance helper (Python 3.10+, PTB v21)

import os
import re
import sys
import sqlite3
import html
import logging

try:
    import jdatetime  # optional
except Exception:
    jdatetime = None

from dataclasses import dataclass
from datetime import date, timedelta
from contextlib import contextmanager
from typing import Optional, List, Tuple
from pathlib import Path
from functools import wraps

try:
    # Import the required classes from python‑telegram‑bot.  The library must be version 21 or newer.
    from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.constants import ParseMode, UpdateType
    from telegram.ext import (
        Application,
        ApplicationBuilder,
        CallbackContext,
        CallbackQueryHandler,
        CommandHandler,
        MessageHandler,
        filters,
        Defaults,
        ContextTypes,
    )
    from telegram.error import BadRequest

except ImportError:
    print(
        "ERROR: python‑telegram‑bot v21+ is required. Install with: pip install python-telegram-bot==21.*",
        file=sys.stderr,
    )
    sys.exit(1)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 4096
TRUNCATE_THRESHOLD = 4000
TRUNCATE_BODY_LENGTH = 3900
TRUNCATION_SUFFIX = "\n\n… [truncated]"
HELP_TEXT = (
    "دستورات در دسترس:\n"
    "/start — بازگشت به خانه\n"
    "/help — همین راهنما\n"
    "/about — معرفی ربات\n"
    "/ping — بررسی اتصال\n\n"
    "برای مبالغ می‌توان از k (هزار)، m (میلیون) و b (میلیارد) استفاده کرد."
)
ABOUT_TEXT = (
    "FINbot یک دستیار مالی برای داروخانه‌هاست؛ ذخیره‌سازی داده، گزارش‌گیری و مقایسه ماهانه را ساده می‌کند."
)

def h(value: Optional[str]) -> str:
    """Escape user-sourced strings for safe HTML output."""
    return html.escape(value or "")

def _truncate_text(text: str) -> str:
    """Ensure messages stay within Telegram limits with a friendly suffix."""
    if len(text) <= TRUNCATE_THRESHOLD:
        return text
    trimmed = text[:TRUNCATE_BODY_LENGTH]
    if "\n" in trimmed:
        trimmed = trimmed.rsplit("\n", 1)[0]
    trimmed = trimmed.rstrip()
    suffix = TRUNCATION_SUFFIX
    if len(trimmed) + len(suffix) > MAX_MESSAGE_LENGTH:
        trimmed = trimmed[: MAX_MESSAGE_LENGTH - len(suffix)]
    return f"{trimmed}{suffix}"

def owner_only_access(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else None
        if user_id not in OWNER_USER_IDS:
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

OWNER_USER_IDS = (
    {int(x) for x in os.environ.get("OWNER_USER_IDS", "").split(",") if x.strip().isdigit()}
    if os.environ.get("OWNER_USER_IDS")
    else set()
)


async def safe_edit(
    message,
    text: str,
    reply_markup=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
    log_context: str = "safe_edit",
) -> None:
    """Edit a message with HTML-safe text, handling BadRequest gracefully."""
    if not message:
        return
    primary = _truncate_text(text)
    try:
        await message.edit_text(primary, reply_markup=reply_markup)
        return
    except BadRequest as exc:
        logger.warning("%s failed (primary): %s", log_context, exc)
        fallback = _truncate_text(h(text))
        try:
            if fallback != primary:
                await message.edit_text(fallback, reply_markup=reply_markup)
                return
        except BadRequest as exc2:
            logger.warning("%s fallback failed: %s", log_context, exc2)
        bot = context.bot if context and getattr(context, "bot", None) else getattr(message, "bot", None)
        chat_id = getattr(message, "chat_id", None)
        if bot and chat_id is not None:
            try:
                await bot.send_message(chat_id=chat_id, text=fallback, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            except BadRequest as exc3:
                logger.error("%s fallback send failed: %s", log_context, exc3)
        else:
            logger.error("%s: unable to send fallback message (no bot/chat).", log_context)

async def safe_reply(
    message,
    text: str,
    reply_markup=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
    log_context: str = "safe_reply",
) -> None:
    """Reply to a message with HTML-safe text, guarding against BadRequest issues."""
    if not message:
        return
    primary = _truncate_text(text)
    try:
        await message.reply_text(primary, reply_markup=reply_markup)
        return
    except BadRequest as exc:
        logger.warning("%s failed (primary): %s", log_context, exc)
        fallback = _truncate_text(h(text))
        try:
            if fallback != primary:
                await message.reply_text(fallback, reply_markup=reply_markup)
                return
        except BadRequest as exc2:
            logger.warning("%s fallback failed: %s", log_context, exc2)
        bot = context.bot if context and getattr(context, "bot", None) else getattr(message, "bot", None)
        chat_id = getattr(message, "chat_id", None)
        if bot and chat_id is not None:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=fallback,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=getattr(message, "message_id", None),
                )
            except BadRequest as exc3:
                logger.error("%s fallback send failed: %s", log_context, exc3)
        else:
            logger.error("%s: unable to send fallback message (no bot/chat).", log_context)

def ensure_data_dir(path: str) -> None:
    """Make sure the directory for the database path exists."""
    p = Path(path).parent
    p.mkdir(parents=True, exist_ok=True)

def get_token() -> str:
    """\n    Retrieve the bot token from environment variables.\n\n    In non‑interactive environments (e.g. a deployed bot), prompting the user for a\n    token via ``input()`` is not practical. Instead, we read the token from the\n    ``FINBOT_TOKEN`` or ``BOT_TOKEN`` environment variables. If neither is set\n    or is empty after stripping whitespace, the function exits with an error\n    message.\n    """
    tok = (os.environ.get("FINBOT_TOKEN") or os.environ.get("BOT_TOKEN") or "").strip()
    if not tok:
        print(
            "No token provided. Set FINBOT_TOKEN or BOT_TOKEN environment variables.",
            file=sys.stderr,
        )
        sys.exit(1)
    return tok

BOT_TOKEN = get_token()
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "robot_moein_data.db")

OWNER_USER_IDS = (
    {int(x) for x in os.environ.get("OWNER_USER_IDS", "").split(",") if x.strip().isdigit()}
    if os.environ.get("OWNER_USER_IDS")
    else set()
)
ensure_data_dir(DB_PATH)

TAG_PHARM_SELECT = "fin.pharm.select"
TAG_PHARM_NEW = "fin.pharm.new"
TAG_PERIOD_SELECT = "fin.period.select"
TAG_PERIOD_NEW = "fin.period.new"
TAG_DAILY_START = "main.daily"
TAG_DAILY_PICK_DAY = "fin.daily.pick_day"
TAG_DAILY_FLOW_PICK_DAY = "daily.pick_day"
TAG_SUMMARY_START = "main.summary"
TAG_SUMMARY_PICK_MONTH = "summary.pick_month"
TAG_CHECK_START = "main.check"
TAG_CHECK_PICK_DAY = "check.pick_day"
TAG_WEEKLY_REPORT = "fin.weekly.report"
TAG_PERIOD_NEW_JALALI = "fin.period.new_from_jalali"
TAG_COMPARE_PREV = "fin.compare.prev"
TAG_COMPARE_MONTHS = "fin.compare.months"
TAG_COMPARE_PICK = "fin.compare.pick"
TAG_COMPARE_PICK_FIRST = "fin.compare.pick_first"
TAG_COMPARE_PICK_SECOND = "fin.compare.pick_second"
TAG_COMPARE_START = "fin.compare.start"

# Tag for triggering simple PDF report generation
TAG_PDF_SIMPLE = "fin.pdf.simple"

# Tags for two elver simulation
TAG_SIM_MENU = "fin.sim.menu"
TAG_SIM_DELTA = "fin.sim.delta"
TAG_SIM_RESET = "fin.sim.reset"
TAG_SIM_BACK = "fin.sim.back"

@contextmanager
def db_conn():
    """Context manager for SQLite connections with foreign keys enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def migrate() -> None:
    """Create the necessary tables and indexes if they do not exist."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        CREATE TABLE IF NOT EXISTS pharmacies(\n          id INTEGER PRIMARY KEY,\n          title TEXT NOT NULL,\n          created_at TEXT NOT NULL\n        );\n        """
        )
        c.execute(
            """\n        CREATE TABLE IF NOT EXISTS periods(\n          id INTEGER PRIMARY KEY,\n          pharmacy_id INTEGER NOT NULL,\n          title TEXT NOT NULL,\n          start_date TEXT NOT NULL,\n          end_date TEXT NOT NULL,\n          status TEXT NOT NULL DEFAULT 'open',\n          created_at TEXT NOT NULL,\n          FOREIGN KEY(pharmacy_id) REFERENCES pharmacies(id)\n        );\n        """
        )
        c.execute(
            """\n        CREATE TABLE IF NOT EXISTS period_metrics(\n          id INTEGER PRIMARY KEY,\n          pharmacy_id INTEGER NOT NULL,\n          period_id INTEGER NOT NULL,\n          basis TEXT NOT NULL DEFAULT 'cash',\n          sales_cash REAL NOT NULL DEFAULT 0,\n          sales_ins REAL NOT NULL DEFAULT 0,\n          sales_total REAL NOT NULL DEFAULT 0,\n          var_total REAL NOT NULL DEFAULT 0,\n          fixed_rent REAL NOT NULL DEFAULT 0,\n          fixed_staff REAL NOT NULL DEFAULT 0,\n          fixed_total REAL NOT NULL DEFAULT 0,\n          opex_other_total REAL NOT NULL DEFAULT 0,\n          visits_total INTEGER NOT NULL DEFAULT 0,\n          days_count INTEGER NOT NULL DEFAULT 30,\n          gross_profit REAL NOT NULL DEFAULT 0,\n          net_profit_operational REAL NOT NULL DEFAULT 0,\n          contrib_margin REAL NOT NULL DEFAULT 0,\n          cm_ratio REAL NOT NULL DEFAULT 0,\n          breakeven_sales REAL NOT NULL DEFAULT 0,\n          avg_daily_sales REAL NOT NULL DEFAULT 0,\n          avg_sale_per_visit REAL NOT NULL DEFAULT 0,\n          computed_at TEXT,\n          locked_at TEXT,\n          UNIQUE (pharmacy_id, period_id, basis),\n          FOREIGN KEY(pharmacy_id) REFERENCES pharmacies(id),\n          FOREIGN KEY(period_id) REFERENCES periods(id)\n        );\n        """
        )
        # Add np_ratio column (net profit margin) if it does not exist.
        try:
            c.execute("ALTER TABLE period_metrics ADD COLUMN np_ratio REAL NOT NULL DEFAULT 0;")
        except sqlite3.OperationalError:
            # Column already exists; ignore error
            pass
        c.execute("CREATE INDEX IF NOT EXISTS idx_periods_pharmacy ON periods(pharmacy_id);")
        c.execute(
            """\n        CREATE INDEX IF NOT EXISTS idx_metrics_ppb\n        ON period_metrics(pharmacy_id, period_id, basis);\n        """
        )
        c.execute(
            """\n        CREATE TABLE IF NOT EXISTS daily_logs(\n          id INTEGER PRIMARY KEY,\n          pharmacy_id INTEGER NOT NULL,\n          log_date TEXT NOT NULL,\n          sales_cash REAL NOT NULL DEFAULT 0,\n          sales_ins REAL NOT NULL DEFAULT 0,\n          var_purchases REAL NOT NULL DEFAULT 0,\n          opex_other REAL NOT NULL DEFAULT 0,\n          visits INTEGER NOT NULL DEFAULT 0,\n          note TEXT,\n          created_at TEXT NOT NULL DEFAULT (datetime('now')),\n          FOREIGN KEY(pharmacy_id) REFERENCES pharmacies(id),\n          UNIQUE (pharmacy_id, log_date)\n        );\n        """
        )
        c.execute(
            """\n        CREATE INDEX IF NOT EXISTS idx_daily_logs_pharmacy_date\n        ON daily_logs(pharmacy_id, log_date);\n        """
        )

def upsert_daily_log(
    pharmacy_id: int,
    log_date: str,
    sales_cash: float = 0,
    sales_ins: float = 0,
    var_purchases: float = 0,
    opex_other: float = 0,
    visits: int = 0,
    note: Optional[str] = None,
) -> None:
    """Insert or update a daily log entry keyed by pharmacy and date."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        INSERT INTO daily_logs(\n          pharmacy_id, log_date, sales_cash, sales_ins, var_purchases, opex_other, visits, note\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)\n        ON CONFLICT (pharmacy_id, log_date) DO UPDATE SET\n          sales_cash=excluded.sales_cash,\n          sales_ins=excluded.sales_ins,\n          var_purchases=excluded.var_purchases,\n          opex_other=excluded.opex_other,\n          visits=excluded.visits,\n          note=excluded.note;\n        """,
            (
                pharmacy_id,
                log_date,
                sales_cash,
                sales_ins,
                var_purchases,
                opex_other,
                visits,
                note,
            ),
        )

def get_daily_range(
    pharmacy_id: int, start_iso: str, end_iso: str
) -> List[sqlite3.Row]:
    """Return daily logs within an inclusive date range ordered by date."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        SELECT *\n        FROM daily_logs\n        WHERE pharmacy_id = ? AND log_date BETWEEN ? AND ?\n        ORDER BY log_date ASC;\n        """,
            (pharmacy_id, start_iso, end_iso),
        )
        return c.fetchall()

def get_last_daily_for_month(
    pharmacy_id: int, start_iso: str, end_iso: str
) -> Optional[sqlite3.Row]:
    """Return the most recent daily log within the given inclusive range."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        SELECT *\n        FROM daily_logs\n        WHERE pharmacy_id = ? AND log_date BETWEEN ? AND ?\n        ORDER BY log_date DESC\n        LIMIT 1;\n        """,
            (pharmacy_id, start_iso, end_iso),
        )
        return c.fetchone()

def upsert_metrics_cash(
    pharmacy_id: int,
    period_id: int,
    sales_cash: float,
    sales_ins: float,
    var_total: float,
    fixed_rent: float,
    fixed_staff: float,
    opex_other_total: float,
    visits_total: int,
    days_count: int,
) -> None:
    """Insert or update cash‑basis metrics and recompute derived KPIs."""
    sales_total = (sales_cash or 0.0) + (sales_ins or 0.0)
    fixed_total = (fixed_rent or 0.0) + (fixed_staff or 0.0)
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        INSERT INTO period_metrics(\n          pharmacy_id, period_id, basis,\n          sales_cash, sales_ins, sales_total,\n          var_total, fixed_rent, fixed_staff, fixed_total,\n          opex_other_total, visits_total, days_count, computed_at\n        ) VALUES (?, ?, 'cash', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))\n        ON CONFLICT (pharmacy_id, period_id, basis) DO UPDATE SET\n          sales_cash=excluded.sales_cash,\n          sales_ins=excluded.sales_ins,\n          sales_total=excluded.sales_total,\n          var_total=excluded.var_total,\n          fixed_rent=excluded.fixed_rent,\n          fixed_staff=excluded.fixed_staff,\n          fixed_total=excluded.fixed_total,\n          opex_other_total=excluded.opex_other_total,\n          visits_total=excluded.visits_total,\n          days_count=excluded.days_count,\n          computed_at=excluded.computed_at;\n        """,
            (
                pharmacy_id,
                period_id,
                sales_cash,
                sales_ins,
                sales_total,
                var_total,
                fixed_rent,
                fixed_staff,
                fixed_total,
                opex_other_total,
                visits_total,
                days_count,
            ),
        )
        c.execute(
            """\n        UPDATE period_metrics\n        SET\n          gross_profit = sales_total - var_total,\n          net_profit_operational = (sales_total - var_total) - fixed_total - opex_other_total,\n          contrib_margin = sales_total - var_total,\n          cm_ratio = CASE WHEN sales_total > 0 THEN (sales_total - var_total)/sales_total ELSE 0 END,\n          np_ratio = CASE\n                       WHEN sales_total > 0\n                       THEN ((sales_total - var_total) - fixed_total - opex_other_total) * 1.0 / sales_total\n                       ELSE 0\n                     END,\n          breakeven_sales = CASE\n                             WHEN sales_total > 0 AND (sales_total - var_total)/sales_total > 0\n                             THEN fixed_total / ((sales_total - var_total)/sales_total)\n                             ELSE 0 END,\n          avg_daily_sales = CASE WHEN days_count > 0 THEN sales_total*1.0/days_count ELSE 0 END,\n          avg_sale_per_visit = CASE WHEN visits_total > 0 THEN sales_total*1.0/visits_total ELSE 0 END\n        WHERE pharmacy_id = ? AND period_id = ? AND basis = 'cash';\n        """,
            (pharmacy_id, period_id),
        )

def new_pharmacy(title: str) -> int:
    """Create a new pharmacy entry and return its id (raises on failure)."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO pharmacies(title, created_at) VALUES (?, datetime('now'));", (title,))
        last = c.lastrowid
        if last is None:
            raise RuntimeError("Failed to create pharmacy record")
        return int(last)

def list_pharmacies() -> List[sqlite3.Row]:
    """Return a list of all pharmacies in descending order of ID."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM pharmacies ORDER BY id DESC;")
        return c.fetchall()

PERIOD_OVERLAP_MESSAGE = "❗ این بازه زمانی با یک دورهٔ دیگر هم‌پوشانی دارد."

class PeriodOverlapError(Exception):
    """Raised when attempting to create a period that overlaps an existing one."""

    def __init__(self, message: str = PERIOD_OVERLAP_MESSAGE) -> None:
        super().__init__(message)

def new_period(pharmacy_id: int, title: str, start_date: str, end_date: str) -> int:
    """Insert a new period for a given pharmacy.\n\n    Raises:\n        PeriodOverlapError: If the proposed date range overlaps an existing period.\n    """
    with db_conn() as conn:
        c = conn.cursor()
        # Prevent overlapping periods for the same pharmacy by checking whether any existing
        # period intersects the requested inclusive date range before inserting.
        c.execute(
            """\n        SELECT id\n        FROM periods\n        WHERE pharmacy_id = ?\n          AND NOT (end_date < ? OR start_date > ?)\n        LIMIT 1;\n        """,
            (pharmacy_id, start_date, end_date),
        )
        overlap = c.fetchone()
        if overlap:
            raise PeriodOverlapError()
        c.execute(
            """\n        INSERT INTO periods(pharmacy_id, title, start_date, end_date, status, created_at)\n        VALUES (?, ?, ?, ?, 'open', datetime('now'));\n        """,
            (pharmacy_id, title, start_date, end_date),
        )
        return c.lastrowid

def find_period_by_bounds(
    pharmacy_id: int, start_iso: str, end_iso: str
) -> Optional[dict]:
    """Return the period row (as dict) matching the inclusive ISO bounds, if any."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        SELECT *\n        FROM periods\n        WHERE pharmacy_id = ?\n          AND start_date = ?\n          AND end_date = ?;\n        """,
            (pharmacy_id, start_iso, end_iso),
        )
        row = c.fetchone()
        return dict(row) if row else None

def get_period_by_jalali(pharmacy_id: int, jy: int, jm: int) -> Optional[dict]:
    """Return the period dict for the given Jalali year/month if it exists."""
    start_date, end_date, _ = jalali_month_bounds(jy, jm)
    return find_period_by_bounds(pharmacy_id, start_date.isoformat(), end_date.isoformat())

def list_periods(pharmacy_id: int) -> List[sqlite3.Row]:
    """Fetch all periods for a given pharmacy ordered by start date descending."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        SELECT * FROM periods\n        WHERE pharmacy_id = ?\n        ORDER BY start_date DESC;\n        """,
            (pharmacy_id,),
        )
        return c.fetchall()

def get_period(period_id: int) -> Optional[sqlite3.Row]:
    """Retrieve a single period by its ID."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM periods WHERE id=?;", (period_id,))
        return c.fetchone()

def get_metrics(pharmacy_id: int, period_id: int) -> Optional[sqlite3.Row]:
    """Get the metrics row for a given pharmacy and period using cash basis."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """\n        SELECT * FROM period_metrics\n        WHERE pharmacy_id=? AND period_id=? AND basis='cash';\n        """,
            (pharmacy_id, period_id),
        )
        return c.fetchone()

def set_period_status(period_id: int, status: str) -> None:
    """Update a period's status and lock metrics if closed."""
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("UPDATE periods SET status=? WHERE id=?;", (status, period_id))
        if status == "closed":
            c.execute(
                """\n            UPDATE period_metrics\n            SET locked_at = datetime('now')\n            WHERE period_id = ? AND basis = 'cash';\n            """,
                (period_id,),
            )

def main_menu_kb():
    """Keyboard with a single option to add a pharmacy."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("➕ افزودن داروخانه", callback_data=make_cb(TAG_PHARM_NEW))]])

def make_cb(tag: str, *parts: object) -> str:
    return f"{tag}:" + ":".join(str(p) for p in parts) if parts else tag

def pharm_list_kb():
    """Keyboard listing existing pharmacies with an option to add a new one."""
    rows = []
    for p in list_pharmacies():
        rows.append([
            InlineKeyboardButton(
                f"🏥 {p['title']} (#{p['id']})",
                callback_data=make_cb(TAG_PHARM_SELECT, p['id']),
            )
        ])
    rows.append([InlineKeyboardButton("➕ افزودن داروخانه", callback_data=make_cb(TAG_PHARM_NEW))])
    return InlineKeyboardMarkup(rows)

def period_list_kb(pharmacy_id: int):
    """Keyboard listing periods for a pharmacy with controls to add or return."""
    rows = []
    for pr in list_periods(pharmacy_id):
        badge = (
            "?? ???"
            if pr["status"] == "open"
            else ("?? ?? ?????? ?????" if pr["status"] == "pending_approval" else "?? ????")
        )
        rows.append([
            InlineKeyboardButton(
                f"{badge} {pr['title']} ({pr['start_date']} - {pr['end_date']})",
                callback_data=make_cb(TAG_PERIOD_SELECT, pharmacy_id, pr['id']),
            )
        ])
    rows.append([InlineKeyboardButton("?? ????? ????", callback_data=make_cb(TAG_PERIOD_NEW, pharmacy_id))])
    rows.append([InlineKeyboardButton("?? ??????", callback_data="fin.home")])
    return InlineKeyboardMarkup(rows)

def month_actions_kb(pharmacy_id: int, jy: int, jm: int, period_id: int | None = None, status: str | None = None) -> tuple[InlineKeyboardMarkup, int, str, str]:
    """Create the period actions keyboard and ensure the backing period exists."""
    if period_id is None or status is None:
        period_id, status, _, _, _ = get_or_create_month_period(pharmacy_id, jy, jm)
    month_name = JALALI_MONTH_NAMES[jm - 1] if 1 <= jm <= 12 else str(jm)
    if status == "closed":
        rows = [
            [InlineKeyboardButton("?? ????? ???", callback_data=f"fin.report.view:{pharmacy_id}:{period_id}")],
            [InlineKeyboardButton("?? PDF ????", callback_data=make_cb(TAG_PDF_SIMPLE, pharmacy_id, period_id))],
            [InlineKeyboardButton("?? ????? ?????", callback_data=make_cb(TAG_WEEKLY_REPORT, pharmacy_id, period_id))],
            [InlineKeyboardButton("?? ?????? ?? ??? ???", callback_data=make_cb(TAG_COMPARE_PREV, pharmacy_id, jy, jm))],
            [InlineKeyboardButton("?? ?????? ?? ??? ????", callback_data=make_cb(TAG_COMPARE_MONTHS, pharmacy_id, jy, jm))],
        ]
        rows.append([InlineKeyboardButton("💳 ثبت چک", callback_data=make_cb(TAG_CHECK_START, pharmacy_id))])
        rows.append([InlineKeyboardButton("📊 خلاصه ماه", callback_data=make_cb(TAG_SUMMARY_START, pharmacy_id))])
    else:
        rows = [
            [
                InlineKeyboardButton("?? ???/?????? ?????", callback_data=f"fin.entry.menu:{pharmacy_id}:{period_id}"),
                InlineKeyboardButton("📥 ثبت اطلاعات مالی", callback_data=make_cb(TAG_DAILY_START, pharmacy_id)),
            ],
            [
                InlineKeyboardButton("?? ?????? ??????", callback_data=f"fin.entry.recompute:{pharmacy_id}:{period_id}"),
                InlineKeyboardButton("?? ????? ???", callback_data=f"fin.report.view:{pharmacy_id}:{period_id}"),
            ],
            [
                InlineKeyboardButton("?? ????? ?????", callback_data=make_cb(TAG_WEEKLY_REPORT, pharmacy_id, period_id)),
                InlineKeyboardButton("?? ?????? ?? ??? ???", callback_data=make_cb(TAG_COMPARE_PREV, pharmacy_id, jy, jm)),
            ],
            [
                InlineKeyboardButton("?? ?????? ?? ??? ????", callback_data=make_cb(TAG_COMPARE_MONTHS, pharmacy_id, jy, jm)),
                InlineKeyboardButton("?? PDF ????", callback_data=make_cb(TAG_PDF_SIMPLE, pharmacy_id, period_id)),
            ],
            [InlineKeyboardButton("?? ??????? ????", callback_data=make_cb(TAG_SIM_MENU, pharmacy_id, period_id))],
        ]
        rows.append([InlineKeyboardButton("💳 ثبت چک", callback_data=make_cb(TAG_CHECK_START, pharmacy_id))])
        rows.append([InlineKeyboardButton("📊 خلاصه ماه", callback_data=make_cb(TAG_SUMMARY_START, pharmacy_id))])
    rows.append([InlineKeyboardButton("?? ??????", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))])
    return InlineKeyboardMarkup(rows), period_id, status, month_name

def period_actions_kb(pharmacy_id: int, period_id: int, status: str) -> InlineKeyboardMarkup:
    period_row = get_period(period_id)
    if not period_row:
        return InlineKeyboardMarkup([[InlineKeyboardButton("?? ??????", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))]])
    period = dict(period_row)
    start_iso = period.get("start_date") or date.today().isoformat()
    jy, jm, _ = gregorian_to_jalali(date.fromisoformat(start_iso))
    keyboard, _, _, _ = month_actions_kb(pharmacy_id, jy, jm, period_id=period_id, status=status)
    return keyboard

def entry_menu_kb(pharmacy_id: int, period_id: int):
    """Keyboard for entering or editing numeric values for a period."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "💵 فروش نقدی",
                    callback_data=f"fin.entry.set:sales_cash:{pharmacy_id}:{period_id}",
                ),
                InlineKeyboardButton(
                    "🏦 واریزی بیمه",
                    callback_data=f"fin.entry.set:sales_ins:{pharmacy_id}:{period_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧪 خرید (متغیر)",
                    callback_data=f"fin.entry.set:var_total:{pharmacy_id}:{period_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 اجاره", callback_data=f"fin.entry.set:fixed_rent:{pharmacy_id}:{period_id}"
                ),
                InlineKeyboardButton(
                    "👥 حقوق پرسنل", callback_data=f"fin.entry.set:fixed_staff:{pharmacy_id}:{period_id}"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧾 سایر هزینه‌ها", callback_data=f"fin.entry.set:opex_other_total:{pharmacy_id}:{period_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "👣 تعداد مراجعه", callback_data=f"fin.entry.set:visits_total:{pharmacy_id}:{period_id}"
                ),
                InlineKeyboardButton(
                    "📅 روزهای دوره", callback_data=f"fin.entry.set:days_count:{pharmacy_id}:{period_id}"
                ),
            ],
            [InlineKeyboardButton("🔄 محاسبهٔ دوباره", callback_data=f"fin.entry.recompute:{pharmacy_id}:{period_id}")],
            [InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PERIOD_SELECT, pharmacy_id, period_id))],
        ]
    )

@owner_only_access
async def cb_daily_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start the simplified daily data entry flow by asking for the date."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_DAILY_START}:(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    state = {
        "pharmacy_id": pharmacy_id,
        "step": 0,
        "date": None,
        "data": {},
    }
    if context.user_data is not None:
        context.user_data["daily"] = state
    jy, jm, _ = gregorian_to_jalali(date.today())
    keyboard = daily_day_picker_kb(jy, jm, tag=TAG_DAILY_FLOW_PICK_DAY, extra=str(pharmacy_id))
    await safe_edit(
        message,
        "روز مورد نظر را انتخاب کن:",
        reply_markup=keyboard,
        context=context,
        log_context="cb_daily_start",
    )

@owner_only_access
async def cb_daily_pick_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle day selection for the simplified daily flow."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_DAILY_FLOW_PICK_DAY}:(\d{{4}}-\d{{2}}-\d{{2}}):(\d+)$", data)
    if not match:
        return
    date_str = match.group(1)
    pharmacy_id = int(match.group(2))
    daily_state = context.user_data.get("daily") if context.user_data is not None else None
    if not daily_state or daily_state.get("pharmacy_id") != pharmacy_id:
        return
    daily_state["date"] = date_str
    daily_state["step"] = 1
    await safe_edit(
        message,
        f"📆 تاریخ انتخاب شد: {to_persian_digits(date_str)}",
        reply_markup=None,
        context=context,
        log_context="cb_daily_flow_pick_day_edit",
    )
    await safe_reply(
        message,
        "مقدار فروش نقدی (تومان) را وارد کن:",
        reply_markup=ForceReply(selective=True),
        context=context,
        log_context="cb_daily_flow_pick_day_prompt",
    )

@owner_only_access
async def msg_daily_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle replies for the simplified daily data entry flow."""
    message = update.effective_message
    if not message:
        return
    daily_state = context.user_data.get("daily") if context.user_data is not None else None
    if not daily_state or "step" not in daily_state or "data" not in daily_state:
        return
    if not message.text:
        return
    text = message.text.strip()
    try:
        value, err = parse_smart_number(text, as_int=True)
    except Exception:
        value, err = None, "مقدار نامعتبر است."
    if err:
        await safe_reply(
            message,
            "❗ مقدار نامعتبر است. دوباره تلاش کن:",
            reply_markup=ForceReply(selective=True),
            context=context,
            log_context="msg_daily_flow_invalid",
        )
        return
    step = daily_state.get("step", 0)
    fields_sequence: list[tuple[str, str]] = [
        ("sales_cash", "مقدار فروش بیمه‌ای (تومان) را وارد کن:"),
        ("sales_ins", "تعداد ویزیت را وارد کن:"),
        ("visits", "مقدار هزینه ثابت را وارد کن:"),
        ("fixed_cost", "✅ ثبت شد. اطلاعات ذخیره شد."),
    ]
    if step < 1 or step > len(fields_sequence):
        await safe_reply(
            message,
            "این مرحله ناشناخته است.",
            context=context,
            log_context="msg_daily_flow_unknown_step",
        )
        if context.user_data is not None:
            context.user_data.pop("daily", None)
        return
    key, next_prompt = fields_sequence[step - 1]
    if value is not None:
        daily_state["data"][key] = int(value)
    daily_state["step"] += 1
    if daily_state["step"] <= len(fields_sequence):
        await safe_reply(
            message,
            next_prompt,
            reply_markup=ForceReply(selective=True),
            context=context,
            log_context="msg_daily_flow_next_prompt",
        )
        return
    payload = daily_state["data"]
    pharmacy_id = daily_state["pharmacy_id"]
    log_date = daily_state.get("date") or date.today().isoformat()
    sales_cash = float(payload.get("sales_cash", 0))
    sales_ins = float(payload.get("sales_ins", 0))
    visits = int(payload.get("visits", 0))
    fixed_cost = float(payload.get("fixed_cost", 0))
    upsert_daily_log(
        pharmacy_id,
        log_date,
        sales_cash,
        sales_ins,
        0.0,
        fixed_cost,
        visits,
        None,
    )
    summary = "\n".join(
        [
            "✅ ثبت روزانه انجام شد.",
            f"📆 تاریخ: {to_persian_digits(log_date)}",
            f"💵 فروش نقدی: {fmt_money(sales_cash)} تومان",
            f"💳 فروش بیمه‌ای: {fmt_money(sales_ins)} تومان",
            f"👥 تعداد ویزیت: {visits}",
            f"🏦 هزینه ثابت: {fmt_money(fixed_cost)} تومان",
        ]
    )
    await safe_reply(
        message,
        summary,
        context=context,
        log_context="msg_daily_flow_complete",
    )
    if context.user_data is not None:
        context.user_data.pop("daily", None)

@owner_only_access
async def cb_summary_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to pick a month for the monthly summary."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SUMMARY_START}:(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    jy, _, _ = gregorian_to_jalali(date.today())
    keyboard = month_picker_kb(jy, tag=TAG_SUMMARY_PICK_MONTH, extra=str(pharmacy_id))
    rows = list(keyboard.inline_keyboard) if keyboard.inline_keyboard else []
    rows.append((InlineKeyboardButton("بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id)),))
    keyboard = InlineKeyboardMarkup(rows)
    await safe_edit(
        message,
        "ماه مورد نظر برای خلاصه را انتخاب کن:",
        reply_markup=keyboard,
        context=context,
        log_context="cb_summary_start",
    )

@owner_only_access
async def cb_summary_pick_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Render the monthly summary for the chosen Jalali month."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SUMMARY_PICK_MONTH}:(\d{{4}})-(\d{{2}}):(\d+)$", data)
    if not match:
        return
    jy = int(match.group(1))
    jm = int(match.group(2))
    pharmacy_id = int(match.group(3))
    period = get_period_by_jalali(pharmacy_id, jy, jm)
    if not period:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔁 انتخاب ماه دیگر", callback_data=make_cb(TAG_SUMMARY_START, pharmacy_id)
                    )
                ],
                [InlineKeyboardButton("بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))],
            ]
        )
        await safe_edit(
            message,
            "❗ برای این ماه هنوز دوره‌ای ثبت نشده است.",
            reply_markup=keyboard,
            context=context,
            log_context="cb_summary_pick_month_missing_period",
        )
        return
    metrics_row = get_metrics(pharmacy_id, period["id"])
    if not metrics_row:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔁 انتخاب ماه دیگر", callback_data=make_cb(TAG_SUMMARY_START, pharmacy_id)
                    )
                ],
                [InlineKeyboardButton("بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))],
            ]
        )
        await safe_edit(
            message,
            "❗ برای این ماه هنوز متریک‌ها محاسبه نشده‌اند.",
            reply_markup=keyboard,
            context=context,
            log_context="cb_summary_pick_month_missing_metrics",
        )
        return
    metrics = dict(metrics_row)
    sales_cash = float(metrics.get("sales_cash", 0))
    sales_ins = float(metrics.get("sales_ins", 0))
    sales_total = float(metrics.get("sales_total", 0))
    visits_total = int(metrics.get("visits_total", 0))
    fixed_total = float(metrics.get("fixed_total", 0))
    gross_profit = float(metrics.get("gross_profit", 0))
    checks_total: float | None = None  # Placeholder until check storage is implemented.
    prev_jy, prev_jm = (jy - 1, 12) if jm == 1 else (jy, jm - 1)
    prev_period = get_period_by_jalali(pharmacy_id, prev_jy, prev_jm)
    change_text = ""
    if prev_period:
        prev_metrics_row = get_metrics(pharmacy_id, prev_period["id"])
        if prev_metrics_row:
            prev_metrics_dict = dict(prev_metrics_row)
            prev_gross = float(prev_metrics_dict.get("gross_profit", 0))
            if prev_gross:
                delta = (gross_profit - prev_gross) / prev_gross
                change_text = f"📈 تغییر سود ناخالص نسبت به ماه قبل: {fmt_percent(delta)}"
    month_name = JALALI_MONTH_NAMES[jm - 1] if 1 <= jm <= 12 else f"{jm}"
    summary_lines = [
        f"📊 خلاصه {month_name} {jy} برای داروخانه #{pharmacy_id}",
        "",
        f"💵 فروش نقدی: {fmt_money(sales_cash)} تومان",
        f"🏥 فروش بیمه‌ای: {fmt_money(sales_ins)} تومان",
        f"💰 جمع فروش: {fmt_money(sales_total)} تومان",
        f"👥 تعداد ویزیت‌ها: {visits_total}",
        f"🏦 هزینه ثابت: {fmt_money(fixed_total)} تومان",
        f"💳 جمع چک‌ها: {fmt_money(checks_total) + ' تومان' if checks_total is not None else '—'}",
        f"💹 سود ناخالص: {fmt_money(gross_profit)} تومان",
    ]
    if change_text:
        summary_lines.append("")
        summary_lines.append(change_text)
    summary_text = "\n".join(summary_lines)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔁 انتخاب ماه دیگر", callback_data=make_cb(TAG_SUMMARY_START, pharmacy_id))],
            [InlineKeyboardButton("بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))],
        ]
    )
    await safe_edit(
        message,
        summary_text,
        reply_markup=keyboard,
        context=context,
        log_context="cb_summary_pick_month",
    )

@owner_only_access
async def cb_check_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start the check registration flow by asking for the due date."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_CHECK_START}:(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    state = {
        "step": 0,
        "data": {
            "pharmacy_id": pharmacy_id,
            "date_due": None,
            "amount": None,
            "recipient": None,
        },
    }
    if context.user_data is not None:
        context.user_data["check"] = state
    jy, jm, _ = gregorian_to_jalali(date.today())
    keyboard = daily_day_picker_kb(jy, jm, tag=TAG_CHECK_PICK_DAY, extra=str(pharmacy_id))
    await safe_edit(
        message,
        "تاریخ سررسید چک را انتخاب کن:",
        reply_markup=keyboard,
        context=context,
        log_context="cb_check_start",
    )

@owner_only_access
async def cb_check_pick_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle day selection for the check registration flow."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_CHECK_PICK_DAY}:(\d{{4}}-\d{{2}}-\d{{2}}):(\d+)$", data)
    if not match:
        return
    date_str = match.group(1)
    pharmacy_id = int(match.group(2))
    check_state = context.user_data.get("check") if context.user_data is not None else None
    if not check_state or check_state.get("data", {}).get("pharmacy_id") != pharmacy_id:
        return
    check_state["data"]["date_due"] = date_str
    check_state["step"] = 1
    await safe_edit(
        message,
        f"📆 تاریخ سررسید: {to_persian_digits(date_str)}",
        reply_markup=None,
        context=context,
        log_context="cb_check_pick_day_edit",
    )
    await safe_reply(
        message,
        "مبلغ چک را وارد کن (به تومان):",
        reply_markup=ForceReply(selective=True),
        context=context,
        log_context="cb_check_pick_day_prompt",
    )

@owner_only_access
async def msg_check_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle textual replies for the check registration flow."""
    message = update.effective_message
    if not message:
        return
    text = (message.text or "").strip()
    check_state = context.user_data.get("check") if context.user_data is not None else None
    if not check_state:
        return
    step = check_state.get("step", 0)
    data = check_state.get("data", {})
    if step == 1:
        value, err = parse_smart_number(text, as_int=True)
        if err:
            await safe_reply(
                message,
                "❗ مبلغ نامعتبر است. دوباره تلاش کن:",
                reply_markup=ForceReply(selective=True),
                context=context,
                log_context="msg_check_flow_amount_error",
            )
            return
        if value is None or value <= 0:
            await safe_reply(
                message,
                "❗ مبلغ باید بیشتر از صفر باشد.",
                reply_markup=ForceReply(selective=True),
                context=context,
                log_context="msg_check_flow_amount_nonpositive",
            )
            return
        check_state["data"]["amount"] = int(value)
        check_state["step"] = 2
        await safe_reply(
            message,
            "نام شرکت پخش یا گیرنده چک را وارد کن:",
            reply_markup=ForceReply(selective=True),
            context=context,
            log_context="msg_check_flow_recipient_prompt",
        )
        return
    if step == 2:
        check_state["data"]["recipient"] = text
        check_state["step"] = 3
        payload = check_state["data"]
        logger.info("Check saved: %s", payload)
        summary_lines = [
            "✅ چک با موفقیت ثبت شد.",
            f"📆 تاریخ سررسید: {to_persian_digits(payload['date_due'])}",
            f"💰 مبلغ: {fmt_money(payload['amount'])} تومان",
            f"🏢 گیرنده: {h(payload['recipient'])}",
        ]
        await safe_reply(
            message,
            "\n".join(summary_lines),
            context=context,
            log_context="msg_check_flow_complete",
        )
        if context.user_data is not None:
            context.user_data.pop("check", None)
        return

def fmt_money(x) -> str:
    """Format a number with thousands separators; fallback to string representation."""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)

def fmt_percent(x, *, digits: int = 1) -> str:
    """\n    Format a ratio (0.12 -> '12.0٪') with a configurable number of decimal places.\n\n    If conversion fails, returns a dash to indicate missing value.\n    """
    try:
        return f"{(float(x) * 100):.{digits}f}٪"
    except Exception:
        return "-"

def render_report(metrics_row, period_row) -> str:
    """Create an HTML report for a period's metrics."""
    metrics = dict(metrics_row) if not isinstance(metrics_row, dict) else metrics_row
    period = dict(period_row) if not isinstance(period_row, dict) else period_row
    locked_badge = "🔒 نهایی (قفل‌شده)" if metrics.get("locked_at") else "🟢 زنده"
    lines = [
        f"<b>{h(str(period.get('title', '')))}</b>  {locked_badge}",
        f"{h(period.get('start_date', '-'))} → {h(period.get('end_date', '-'))}  ·  Status: <b>{h(period.get('status', 'open'))}</b>",
        "",
        "<b>ورودی‌ها</b>",
        f"فروش نقدی: {fmt_money(metrics.get('sales_cash', 0))} تومان",
        f"واریزی بیمه: {fmt_money(metrics.get('sales_ins', 0))} تومان",
        f"فروش کل: {fmt_money(metrics.get('sales_total', 0))} تومان",
        f"خرید (متغیر): {fmt_money(metrics.get('var_total', 0))} تومان",
        f"هزینهٔ ثابت (اجاره+حقوق): {fmt_money(metrics.get('fixed_total', 0))} تومان  ·  سایر: {fmt_money(metrics.get('opex_other_total', 0))} تومان",
        f"تعداد مراجعه: {metrics.get('visits_total', 0)} نفر  ·  روزهای Period: {metrics.get('days_count', 0)} روز",
        "",
        "<b>شاخص‌ها</b>",
        f"سود ناخالص: {fmt_money(metrics.get('gross_profit', 0))} تومان",
        f"سود عملیاتی (خالص): {fmt_money(metrics.get('net_profit_operational', 0))} تومان",
        f"حاشیه سود ناخالص: {fmt_percent(metrics.get('cm_ratio', 0))}",
        f"حاشیه سود خالص عملیاتی: {fmt_percent(metrics.get('np_ratio', 0))}",
        f"نقطهٔ سربه‌سر فروش: {fmt_money(metrics.get('breakeven_sales', 0))} تومان",
        f"میانگین فروش روزانه: {fmt_money(metrics.get('avg_daily_sales', 0))} تومان/روز",
        f"میانگین فروش/مراجعه: {fmt_money(metrics.get('avg_sale_per_visit', 0))} تومان/نفر",
        "",
        f"<i>آخرین محاسبه: {h(str(metrics.get('computed_at') or '-'))}</i>",
    ]
    return "\n".join(lines)

def render_compare_table(pharmacy_id: int, period_a_id: int, period_b_id: int) -> str:
    """Render an HTML table comparing key metrics between two periods."""
    a_metrics_row = get_metrics(pharmacy_id, period_a_id) or {}
    b_metrics_row = get_metrics(pharmacy_id, period_b_id) or {}
    a = dict(a_metrics_row) if not isinstance(a_metrics_row, dict) else a_metrics_row
    b = dict(b_metrics_row) if not isinstance(b_metrics_row, dict) else b_metrics_row

    def safe_num(m, k):
        try:
            v = m.get(k)
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def display_num(m, k):
        v = m.get(k)
        return fmt_money(v) if (isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '', 1).isdigit())) else ("-" if v is None else str(v))

    rows = []
    # Simple HTML table with headers: Metric | Period A | Period B | Δ%
    rows.append("<table>")
    rows.append("<tr><th>شاخص</th><th>ماه قبل</th><th>ماه جاری</th><th>Δ%</th></tr>")

    def add_row(label: str, key: str, is_percent: bool = False):
        left = display_num(a, key) if not is_percent else (fmt_percent(a.get(key)) if a.get(key) is not None else "-")
        right = display_num(b, key) if not is_percent else (fmt_percent(b.get(key)) if b.get(key) is not None else "-")
        # compute delta percent where sensible
        delta = "-"
        av = safe_num(a, key)
        bv = safe_num(b, key)
        try:
            if av is not None and av != 0:
                delta = fmt_percent((bv or 0 - av) / abs(av))
            elif av == 0 and (bv is not None and bv != 0):
                delta = "—"
            else:
                delta = "-"
        except Exception:
            delta = "-"
        rows.append(f"<tr><td>{h(label)}</td><td>{h(str(left))}</td><td>{h(str(right))}</td><td>{h(str(delta))}</td></tr>")

    add_row("فروش نقدی", "sales_cash")
    add_row("فروش بیمه", "sales_ins")
    add_row("جمع فروش", "sales_total")
    add_row("خرید (متغیر)", "var_total")
    add_row("هزینه ثابت", "fixed_total")
    add_row("سایر هزینه‌ها", "opex_other_total")
    add_row("سود ناخالص", "gross_profit")
    add_row("سود عملیاتی", "net_profit_operational")
    add_row("حاشیه ناخالص", "cm_ratio", is_percent=True)
    add_row("حاشیه خالص", "np_ratio", is_percent=True)
    add_row("نقطه سربه‌سر", "breakeven_sales")
    add_row("میانگین فروش روزانه", "avg_daily_sales")
    add_row("میانگین فروش/ویزیت", "avg_sale_per_visit")

    rows.append("</table>")
    return "\n".join(rows)

PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
ASCII_TO_PERSIAN_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")

def to_persian_digits(value: int | str) -> str:
    """Convert ASCII digits in ``value`` to Persian numerals."""
    return str(value).translate(ASCII_TO_PERSIAN_DIGITS)

def daily_day_picker_kb(
    jy: int,
    jm: int,
    *,
    tag: str | None = None,
    extra: str | None = None,
) -> InlineKeyboardMarkup:
    """Return a day-selection keyboard for the given Jalali month.

    When ``tag`` is provided, the callback data will use that tag instead of the
    default ``TAG_DAILY_PICK_DAY``. If ``extra`` is provided, it is appended as
    an additional colon-separated part after the ISO date.
    """
    _, _, days_in_month = jalali_month_bounds(jy, jm)
    callback_tag = tag or TAG_DAILY_PICK_DAY
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for day in range(1, days_in_month + 1):
        iso_date = f"{jy:04d}-{jm:02d}-{day:02d}"
        label = to_persian_digits(day)
        callback_data = f"{callback_tag}:{iso_date}"
        if extra is not None:
            callback_data = f"{callback_data}:{extra}"
        current_row.append(InlineKeyboardButton(label, callback_data=callback_data))
        if len(current_row) == 7:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(rows)

def month_picker_kb(
    jy: int,
    *,
    tag: str,
    extra: str | None = None,
) -> InlineKeyboardMarkup:
    """Render a 3x4 grid of Jalali months with optional extra data in callback."""
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for index, title in enumerate(JALALI_MONTH_NAMES, start=1):
        callback = f"{tag}:{jy:04d}-{index:02d}"
        if extra is not None:
            callback = f"{callback}:{extra}"
        row.append(InlineKeyboardButton(title, callback_data=callback))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

JALALI_MONTH_NAMES = [
    "فروردین",
    "اردیبهشت",
    "خرداد",
    "تیر",
    "مرداد",
    "شهریور",
    "مهر",
    "آبان",
    "آذر",
    "دی",
    "بهمن",
    "اسفند",
]

_GREGORIAN_MONTH_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
_JALALI_MONTH_DAYS = [31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29]

def gregorian_to_jalali(d: date) -> tuple[int, int, int]:
    if jdatetime:
        j = jdatetime.date.fromgregorian(date=d)
        return j.year, j.month, j.day
    gy = d.year - 1600
    gm = d.month - 1
    gd = d.day - 1
    g_day_no = 365 * gy + (gy + 3) // 4 - (gy + 99) // 100 + (gy + 399) // 400
    for i in range(gm):
        g_day_no += _GREGORIAN_MONTH_DAYS[i]
    if gm > 1 and ((d.year % 4 == 0 and d.year % 100 != 0) or (d.year % 400 == 0)):
        g_day_no += 1
    g_day_no += gd
    j_day_no = g_day_no - 79
    j_np = j_day_no // 12053
    j_day_no %= 12053
    jy = 979 + 33 * j_np + 4 * (j_day_no // 1461)
    j_day_no %= 1461
    if j_day_no >= 366:
        jy += (j_day_no - 1) // 365
        j_day_no = (j_day_no - 1) % 365
    for i, md in enumerate(_JALALI_MONTH_DAYS):
        if j_day_no < md:
            jm = i + 1
            jd = j_day_no + 1
            break
        j_day_no -= md
    else:
        jm = 12
        jd = j_day_no + 1
    return jy, jm, jd

def jalali_to_gregorian(jy: int, jm: int, jd: int) -> date:
    if jdatetime:
        return jdatetime.date(jy, jm, jd).togregorian()
    jy -= 979
    jm -= 1
    jd -= 1
    j_day_no = 365 * jy + (jy // 33) * 8 + ((jy % 33) + 3) // 4
    for i in range(jm):
        j_day_no += _JALALI_MONTH_DAYS[i]
    j_day_no += jd
    g_day_no = j_day_no + 79
    gy = 1600 + 400 * (g_day_no // 146097)
    g_day_no %= 146097
    leap = True
    if g_day_no >= 36525:
        g_day_no -= 1
        gy += 100 * (g_day_no // 36524)
        g_day_no %= 36524
        if g_day_no >= 365:
            g_day_no += 1
        else:
            leap = False
    gy += 4 * (g_day_no // 1461)
    g_day_no %= 1461
    if g_day_no >= 366:
        leap = False
        g_day_no -= 1
        gy += g_day_no // 365
        g_day_no %= 365
    for i, md in enumerate(_GREGORIAN_MONTH_DAYS):
        dim = md + (1 if i == 1 and ((gy % 4 == 0 and gy % 100 != 0) or (gy % 400 == 0)) else 0)
        if g_day_no < dim:
            gm = i + 1
            gd = g_day_no + 1
            break
        g_day_no -= dim
    else:
        gm = 12
        gd = g_day_no + 1
    return date(gy, gm, gd)

def jalali_month_bounds(jy: int, jm: int) -> tuple[date, date, int]:
    start = jalali_to_gregorian(jy, jm, 1)
    if jm == 12:
        next_start = jalali_to_gregorian(jy + 1, 1, 1)
    else:
        next_start = jalali_to_gregorian(jy, jm + 1, 1)
    end = next_start - timedelta(days=1)
    days = (end - start).days + 1
    return start, end, days

def get_or_create_month_period(pharmacy_id: int, jy: int, jm: int) -> tuple[int, str, date, date, int]:
    """Return period_id/status for the given Jalali month, creating it if needed."""
    start, end, days = jalali_month_bounds(jy, jm)
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT id, status FROM periods WHERE pharmacy_id=? AND start_date=? AND end_date=?",
            (pharmacy_id, start_iso, end_iso),
        )
        row = c.fetchone()
        if row:
            row = dict(row)
            return row["id"], row.get("status", "open"), start, end, days
    title = f"{jy}-{jm:02d} (شمسی)"
    period_id = new_period(pharmacy_id, title, start_iso, end_iso)
    upsert_metrics_cash(
        pharmacy_id,
        period_id,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        days,
    )
    return period_id, "open", start, end, days

# --- Smart numeric parsing (k/m/b), Persian-friendly ---
# ``parse_smart_number`` converts shorthand numeric strings such as ``850k`` or
# ``1.2m`` into their numeric equivalents.  It relies on the global
# ``PERSIAN_DIGITS`` mapping defined above to normalise Persian and Arabic
# numerals.  The ``typing.Tuple`` import and fall‑back definition for
# ``PERSIAN_DIGITS`` were removed to avoid duplicate definitions.  ``Tuple``
# is already imported near the top of this module and ``PERSIAN_DIGITS`` is
# defined once globally.

def parse_smart_number(raw: str, *, as_int: bool = False) -> Tuple[float | int | None, str | None]:
    """\n    Accepts:\n      '1.2', '2500000',\n      '850k', '3.5k',\n      '750m', '1.2m',\n      '1b', '1.2b'\n    Rejects:\n      '1.2 m' (space before suffix), letters beyond k/m/b, multiple dots, empty.\n    Returns: (value, None) or (None, error_message_fa)\n    """
    if raw is None:
        return None, "❗ مقدار نامعتبر بود. مثلاً: 1.2b ، 750m ، 850k"

    s = raw.strip()
    if not s:
        return None, "❗ مقدار نامعتبر بود. مثلاً: 1.2b ، 750m ، 850k"

    # Normalize Persian digits & separators, case-insensitive
    s = s.translate(PERSIAN_DIGITS)
    s = s.replace(",", "").replace("٬", "").lower()

    # Detect suffix (no space allowed)
    factor = 1.0
    if s.endswith("b"):
        factor = 1_000_000_000.0
        core = s[:-1]
    elif s.endswith("m"):
        factor = 1_000_000.0
        core = s[:-1]
    elif s.endswith("k"):
        factor = 1_000.0
        core = s[:-1]
    else:
        core = s

    # quick structural validation
    if " " in core or not core:
        return None, "❗ مقدار نامعتبر بود. مثلاً: 1.2b ، 750m ، 850k"

    # allow an optional leading '+' or '-' and at most one dot
    core_check = core.lstrip("+-")
    if core_check.count(".") > 1 or not core_check or any(c for c in core_check if (not c.isdigit() and c != ".")):
        return None, "❗ مقدار نامعتبر بود. مثلاً: 1.2b ، 750m ، 850k"

    try:
        num = float(core)
    except Exception:
        return None, "❗ مقدار نامعتبر بود. مثلاً: 1.2b ، 750m ، 850k"

    val = num * factor
    if as_int:
        return int(round(val)), None
    return val, None

def compare_months_kb(pharmacy_id: int, base_jy: int, base_jm: int, jy: int) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for index, title in enumerate(JALALI_MONTH_NAMES, start=1):
        row.append(InlineKeyboardButton(title, callback_data=make_cb(TAG_COMPARE_PICK, pharmacy_id, base_jy, base_jm, jy, index)))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))])
    return InlineKeyboardMarkup(buttons)

@owner_only_access
async def cb_compare_prev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.compare\.prev:(\d+):(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    jy = int(match.group(2))
    jm = int(match.group(3))
    prev_jy, prev_jm = (jy - 1, 12) if jm == 1 else (jy, jm - 1)
    prev_start, prev_end, _ = jalali_month_bounds(prev_jy, prev_jm)
    curr_start, curr_end, _ = jalali_month_bounds(jy, jm)
    prev_period = find_period_by_bounds(pharmacy_id, prev_start.isoformat(), prev_end.isoformat())
    if not prev_period:
        back_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))]]
        )
        await safe_edit(
            message,
            "🔍 دورهٔ ماه قبل هنوز ثبت نشده است.",
            reply_markup=back_markup,
            context=context,
            log_context="cb_compare_prev_missing_prev",
        )
        return
    current_period = find_period_by_bounds(pharmacy_id, curr_start.isoformat(), curr_end.isoformat())
    if not current_period:
        back_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))]]
        )
        await safe_edit(
            message,
            "❗ دورهٔ انتخاب‌شده پیدا نشد.",
            reply_markup=back_markup,
            context=context,
            log_context="cb_compare_prev_missing_current",
        )
        return
    prev_period_id = prev_period["id"]
    current_period_id = current_period["id"]
    table = render_compare_table(pharmacy_id, prev_period_id, current_period_id)
    month_name = JALALI_MONTH_NAMES[jm - 1] if 1 <= jm <= 12 else str(jm)
    prev_month_name = JALALI_MONTH_NAMES[prev_jm - 1] if 1 <= prev_jm <= 12 else str(prev_jm)
    back_markup = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PERIOD_SELECT, pharmacy_id, current_period_id))]])
    # Compose the comparison text using a triple-quoted f-string to keep the
    # newline intact. This avoids breaking strings across lines in code.
    comparison_text = (
        f"""<b>مقایسه {month_name} {jy} با {prev_month_name} {prev_jy}</b>\n\n{table}"""
    )
    await safe_edit(
        message,
        comparison_text,
        reply_markup=back_markup,
        context=context,
        log_context="cb_compare_prev",
    )

@owner_only_access
async def cb_compare_months(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.compare\.months:(\d+):(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    base_jy = int(match.group(2))
    base_jm = int(match.group(3))
    jy = base_jy
    kb = compare_months_kb(pharmacy_id, base_jy, base_jm, jy)
    await safe_edit(
        message,
        f"ماه مرجع {base_jy}/{base_jm:02d} — ماه دوم را انتخاب کن",
        reply_markup=kb,
        context=context,
        log_context="cb_compare_months",
    )

@owner_only_access
async def cb_compare_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.compare\.pick:(\d+):(\d+):(\d+):(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    base_jy = int(match.group(2))
    base_jm = int(match.group(3))
    jy = int(match.group(4))
    jm = int(match.group(5))
    # Do not implicitly create periods when comparing months.  Lookup existing periods instead.
    # Compute Jalali month bounds for both the base and target months.
    base_start, base_end, _ = jalali_month_bounds(base_jy, base_jm)
    target_start, target_end, _ = jalali_month_bounds(jy, jm)
    # Attempt to find existing periods by their inclusive date bounds.
    base_period = find_period_by_bounds(
        pharmacy_id, base_start.isoformat(), base_end.isoformat()
    )
    if not base_period:
        # If the base month is not registered, do not create a new period.  Inform the user.
        back_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))]]
        )
        await safe_edit(
            message,
            "🔍 دورهٔ ماه مبدا هنوز ثبت نشده است.",
            reply_markup=back_markup,
            context=context,
            log_context="cb_compare_pick_missing_base",
        )
        return
    target_period = find_period_by_bounds(
        pharmacy_id, target_start.isoformat(), target_end.isoformat()
    )
    if not target_period:
        # If the target month does not exist yet, avoid creating it and inform the user.
        back_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PHARM_SELECT, pharmacy_id))]]
        )
        await safe_edit(
            message,
            "🔍 دورهٔ ماه انتخاب‌شده هنوز ثبت نشده است.",
            reply_markup=back_markup,
            context=context,
            log_context="cb_compare_pick_missing_target",
        )
        return
    # Both periods exist; extract their IDs for comparison.
    base_period_id = base_period["id"]
    target_period_id = target_period["id"]
    table = render_compare_table(pharmacy_id, base_period_id, target_period_id)
    base_name = JALALI_MONTH_NAMES[base_jm - 1] if 1 <= base_jm <= 12 else str(base_jm)
    target_name = JALALI_MONTH_NAMES[jm - 1] if 1 <= jm <= 12 else str(jm)
    back_markup = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ بازگشت", callback_data=make_cb(TAG_PERIOD_SELECT, pharmacy_id, base_period_id))]])
    await safe_edit(
        message,
        f"""<b>مقایسه {base_name} {base_jy} با {target_name} {jy}</b>\n\n{table}""",
        reply_markup=back_markup,
        context=context,
        log_context="cb_compare_pick",
    )

@owner_only_access
async def cb_report_view(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Show the detailed monthly report for a period. This callback expects data\n    in the format ``fin.report.view:<pharmacy_id>:<period_id>``. It fetches\n    the relevant metrics and period details, renders a report via\n    ``render_report``, and then displays it with the standard month actions\n    keyboard so the user can navigate back or perform other actions.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.report\.view:(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    metrics_row = get_metrics(pharmacy_id, period_id)
    period_row = get_period(period_id)
    if not metrics_row or not period_row:
        await safe_edit(
            message,
            "⚠️ دوره پیدا نشد.",
            reply_markup=None,
            context=context,
            log_context="cb_report_view_missing",
        )
        return
    report_html = render_report(metrics_row, period_row)
    # Determine the Jalali month to build the month actions keyboard
    p = dict(period_row)
    start_iso = p.get("start_date") or date.today().isoformat()
    jy, jm, _ = gregorian_to_jalali(date.fromisoformat(start_iso))
    kb, _, _, _ = month_actions_kb(pharmacy_id, jy, jm, period_id=period_id, status=p.get("status", "open"))
    await safe_edit(
        message,
        report_html,
        reply_markup=kb,
        context=context,
        log_context="cb_report_view",
    )

def get_period_baseline(pharmacy_id, period_id):
    raise NotImplementedError

@owner_only_access
async def cb_sim_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Entry point for the two‑lever simulator.\n    Initializes baseline and deltas in context.user_data['sim'] and renders the simulation.\n    Callback data format: ``fin.sim.menu:<pharmacy_id>:<period_id>``.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SIM_MENU}:(\d+):(\d+)$", data)
    if not match:
        await safe_edit(
            message,
            "❗ داده نامعتبر است.",
            context=context,
            log_context="cb_sim_menu_invalid",
        )
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    # Access or initialize simulation state
    sim_state = context.user_data.get("sim") if context.user_data else None
    if not sim_state or sim_state.get("pharmacy_id") != pharmacy_id or sim_state.get("period_id") != period_id:
        baseline = get_period_baseline(pharmacy_id, period_id)
        # Ensure context.user_data exists
        if context.user_data is None:
            context.user_data = {}
        context.user_data["sim"] = {
            "pharmacy_id": pharmacy_id,
            "period_id": period_id,
            "deltas": {"sales": 0.0, "var": 0.0, "fixed": 0.0},
            "baseline": baseline,
        }
    if context.user_data is None:
        context.user_data = {}
    sim_state = context.user_data.get("sim", {})
    baseline = sim_state.get("baseline", {})
    deltas = sim_state.get("deltas", {})
    sim_out = compute_simulated(baseline, deltas)
    period_row = get_period(period_id) or {}
    text = render_sim_text(period_row, sim_out, deltas)
    kb = sim_keyboard(pharmacy_id, period_id)
    await safe_edit(
        message,
        text,
        reply_markup=kb,
        context=context,
        log_context="cb_sim_menu",
    )

@owner_only_access
async def cb_sim_delta(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Adjust one of the simulation deltas and refresh the simulation view.\n    Callback data format: ``fin.sim.delta:<ph_id>:<period_id>:<kind>:<sign>:<pct>``\n    where kind ∈ {sales, var, fixed}, sign ∈ {plus, minus}, pct ∈ {5,10}.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SIM_DELTA}:(\d+):(\d+):(sales|var|fixed):(plus|minus):(5|10)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    kind = match.group(3)
    sign = match.group(4)
    pct_val = int(match.group(5))
    step = pct_val / 100.0
    if sign == "minus":
        step = -step
    # Ensure simulation state exists and matches
    sim_state = context.user_data.get("sim") if context.user_data else None
    if not sim_state or sim_state.get("pharmacy_id") != pharmacy_id or sim_state.get("period_id") != period_id:
        baseline = get_period_baseline(pharmacy_id, period_id)
        if context.user_data is None:
            context.user_data = {}
        context.user_data["sim"] = {
            "pharmacy_id": pharmacy_id,
            "period_id": period_id,
            "deltas": {"sales": 0.0, "var": 0.0, "fixed": 0.0},
            "baseline": baseline,
        }
        sim_state = context.user_data["sim"]
    # Update the specific delta with clamping
    deltas = sim_state["deltas"]
    current_val = float(deltas.get(kind, 0.0))
    new_val = clamp(current_val + step, -0.30, 0.30)
    deltas[kind] = new_val
    baseline = sim_state["baseline"]
    sim_out = compute_simulated(baseline, deltas)
    period_row = get_period(period_id) or {}
    text = render_sim_text(period_row, sim_out, deltas)
    kb = sim_keyboard(pharmacy_id, period_id)
    await safe_edit(
        message,
        text,
        reply_markup=kb,
        context=context,
        log_context="cb_sim_delta",
    )

def render_sim_text(period_row, sim_out, deltas):
    raise NotImplementedError

def sim_keyboard(pharmacy_id, period_id):
    raise NotImplementedError

@owner_only_access
async def cb_sim_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Reset all simulation deltas to zero and refresh the simulation view.\n    Callback data format: ``fin.sim.reset:<ph_id>:<period_id>``.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SIM_RESET}:(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    # Ensure simulation state exists
    sim_state = context.user_data.get("sim") if context.user_data else None
    if not sim_state or sim_state.get("pharmacy_id") != pharmacy_id or sim_state.get("period_id") != period_id:
        baseline = get_period_baseline(pharmacy_id, period_id)
        if context.user_data is None:
            context.user_data = {}
        context.user_data["sim"] = {
            "pharmacy_id": pharmacy_id,
            "period_id": period_id,
            "deltas": {"sales": 0.0, "var": 0.0, "fixed": 0.0},
            "baseline": baseline,
        }
        sim_state = context.user_data["sim"]
    else:
        sim_state["deltas"] = {"sales": 0.0, "var": 0.0, "fixed": 0.0}
    baseline = sim_state["baseline"]
    deltas = sim_state["deltas"]
    sim_out = compute_simulated(baseline, deltas)
    period_row = get_period(period_id) or {}
    text = render_sim_text(period_row, sim_out, deltas)
    kb = sim_keyboard(pharmacy_id, period_id)
    await safe_edit(
        message,
        text,
        reply_markup=kb,
        context=context,
        log_context="cb_sim_reset",
    )

@owner_only_access
async def cb_sim_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Return from the simulator to the month actions menu.\n    Callback data format: ``fin.sim.back:<ph_id>:<period_id>``.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(rf"^{TAG_SIM_BACK}:(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    period_row = get_period(period_id)
    if not period_row:
        await safe_edit(
            message,
            "⚠️ دوره پیدا نشد.",
            reply_markup=None,
            context=context,
            log_context="cb_sim_back_missing_period",
        )
        return
    p = dict(period_row)
    start_iso = p.get("start_date") or date.today().isoformat()
    jy, jm, _ = gregorian_to_jalali(date.fromisoformat(start_iso))
    kb, _, _, month_name = month_actions_kb(pharmacy_id, jy, jm, period_id=period_id, status=p.get("status", "open"))
    # Clear simulation state
    if context.user_data and context.user_data.get("sim"):
        context.user_data.pop("sim", None)
    await safe_edit(
        message,
        f"ماه {jy}/{jm:02d} ({month_name}) — یکی از گزینه‌ها را انتخاب کن",
        reply_markup=kb,
        context=context,
        log_context="cb_sim_back",
    )

def clamp(value: float, min_val: float, max_val: float) -> float:
    """Constrain a value between minimum and maximum bounds."""
    return max(min_val, min(max_val, value))

def compute_simulated(baseline: dict, deltas: dict) -> dict:
    """
    Apply percentage deltas to the baseline period metrics and compute derived KPIs.

    baseline: dict that may contain keys like sales_total, var_total, fixed_total,
              opex_other_total, visits_total, days_count (any missing values default to 0).
    deltas: dict with percentage deltas as fractions, e.g. {"sales": 0.05, "var": -0.10, "fixed": 0.0}
    Returns a dict with recomputed keys similar to period_metrics columns.
    """
    def _float(d, k, default=0.0):
        try:
            return float(d.get(k, default) or default)
        except Exception:
            return float(default)

    sales_total = _float(baseline, "sales_total", 0.0)
    var_total = _float(baseline, "var_total", 0.0)
    fixed_total = _float(baseline, "fixed_total", 0.0)
    opex_other_total = _float(baseline, "opex_other_total", 0.0)
    visits_total = int(baseline.get("visits_total", 0) or 0)
    days_count = int(baseline.get("days_count", 0) or 0)

    sales_delta = float(deltas.get("sales", 0.0) or 0.0)
    var_delta = float(deltas.get("var", 0.0) or 0.0)
    fixed_delta = float(deltas.get("fixed", 0.0) or 0.0)

    new_sales = sales_total * (1.0 + sales_delta)
    new_var = var_total * (1.0 + var_delta)
    new_fixed = fixed_total * (1.0 + fixed_delta)

    # Derived metrics
    gross_profit = new_sales - new_var
    net_profit_operational = gross_profit - new_fixed - opex_other_total

    cm_ratio = (new_sales - new_var) / new_sales if new_sales and new_sales != 0 else 0.0
    np_ratio = net_profit_operational / new_sales if new_sales and new_sales != 0 else 0.0

    breakeven_sales = new_fixed / cm_ratio if cm_ratio and cm_ratio != 0 else 0.0
    avg_daily_sales = new_sales / days_count if days_count and days_count != 0 else 0.0
    avg_sale_per_visit = new_sales / visits_total if visits_total and visits_total != 0 else 0.0

    return {
        "sales_total": new_sales,
        "var_total": new_var,
        "fixed_total": new_fixed,
        "opex_other_total": opex_other_total,
        "visits_total": visits_total,
        "days_count": days_count,
        "gross_profit": gross_profit,
        "net_profit_operational": net_profit_operational,
        "cm_ratio": cm_ratio,
        "np_ratio": np_ratio,
        "breakeven_sales": breakeven_sales,
        "avg_daily_sales": avg_daily_sales,
        "avg_sale_per_visit": avg_sale_per_visit,
    }

@owner_only_access
async def cb_weekly_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Show a report for the last seven days (including today) for the selected\n    pharmacy and period. Callback data format:\n    ``fin.weekly.report:<pharmacy_id>:<period_id>``.\n    Aggregates daily logs and displays per-day entries as well as totals.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.weekly\.report:(\d+):(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    period_id = int(match.group(2))
    today = date.today()
    window_end = today
    window_start = today - timedelta(days=6)
    period_row = get_period(period_id) if period_id else None
    period_info = dict(period_row) if period_row else None
    if period_info:
        period_start_iso = period_info.get("start_date")
        period_end_iso = period_info.get("end_date")
        if period_start_iso and period_end_iso:
            try:
                period_start_date = date.fromisoformat(period_start_iso)
                period_end_date = date.fromisoformat(period_end_iso)
                if period_end_date < period_start_date:
                    period_start_date, period_end_date = period_end_date, period_start_date
                window_end = period_end_date
                period_length = (period_end_date - period_start_date).days + 1
                if period_length >= 7:
                    candidate_start = period_end_date - timedelta(days=6)
                    window_start = candidate_start if candidate_start >= period_start_date else period_start_date
                else:
                    window_start = period_start_date
            except ValueError:
                pass
    if window_end < window_start:
        window_start = window_end
    start_iso = window_start.isoformat()
    end_iso = window_end.isoformat()
    logs = get_daily_range(pharmacy_id, start_iso, end_iso)
    lines: list[str] = [f"<b>گزارش ۷ روزه از {h(start_iso)} تا {h(end_iso)}</b>"]
    total_sales_cash = 0.0
    total_sales_ins = 0.0
    total_var = 0.0
    total_opex = 0.0
    total_visits = 0
    log_map: dict[str, dict] = {}
    for row in logs:
        record = dict(row)
        log_date = record.get("log_date")
        if log_date:
            log_map[log_date] = record
    total_window_days = (window_end - window_start).days
    full_days = [
        window_start + timedelta(days=i) for i in range(total_window_days + 1)
    ]
    if not full_days:
        full_days = [window_start]
    for day in full_days:
        day_iso = day.isoformat()
        record = log_map.get(day_iso)
        if record:
            sales_cash = record.get("sales_cash") or 0.0
            sales_ins = record.get("sales_ins") or 0.0
            var_purchases = record.get("var_purchases") or 0.0
            opex_other = record.get("opex_other") or 0.0
            visits = record.get("visits") or 0
            note_val = record.get("note") or None
            note_display = h(note_val) if note_val else "-"
        else:
            sales_cash = 0.0
            sales_ins = 0.0
            var_purchases = 0.0
            opex_other = 0.0
            visits = 0
            note_display = "– بدون ثبت –"
        lines.append(
            f"{h(day_iso)}: نقدی {fmt_money(sales_cash)}، بیمه {fmt_money(sales_ins)}، متغیر {fmt_money(var_purchases)}، سایر {fmt_money(opex_other)}، مراجعه {visits}، یادداشت: {note_display}"
        )
        total_sales_cash += float(sales_cash)
        total_sales_ins += float(sales_ins)
        total_var += float(var_purchases)
        total_opex += float(opex_other)
        try:
            total_visits += int(visits)
        except Exception:
            pass
    lines.append("")
    lines.append("<b>جمع ۷ روزه</b>")
    lines.append(f"نقدی: {fmt_money(total_sales_cash)}")
    lines.append(f"بیمه: {fmt_money(total_sales_ins)}")
    lines.append(f"خرید متغیر: {fmt_money(total_var)}")
    lines.append(f"سایر هزینه‌ها: {fmt_money(total_opex)}")
    lines.append(f"مراجعه: {total_visits}")
    report_text = "\n".join(lines)
    back_callback = make_cb(TAG_PERIOD_SELECT, pharmacy_id, period_id)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("?? ??????", callback_data=back_callback)]])
    await safe_edit(
        message,
        report_text,
        reply_markup=keyboard,
        context=context,
        log_context="cb_weekly_report",
    )

@owner_only_access
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    if not update.message:
        return
    await update.message.reply_text("به ربات مالی خوش آمدید!", reply_markup=main_menu_kb())

@owner_only_access
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /help command."""
    if not update.message:
        return
    await update.message.reply_text(HELP_TEXT)

@owner_only_access
async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /about command."""
    if not update.message:
        return
    await update.message.reply_text(ABOUT_TEXT)

@owner_only_access
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /ping command."""
    if not update.message:
        return
    await update.message.reply_text("🏓 Pong!")

@owner_only_access
async def cb_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle home button callback."""
    q = update.callback_query
    if not q or not q.message:
        return
    await q.answer()
    await safe_edit(q.message, "به ربات مالی خوش آمدید!", reply_markup=main_menu_kb(), context=context)

@owner_only_access
async def cb_pharm_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle new pharmacy button callback."""
    q = update.callback_query
    if not q or not q.message:
        return
    await q.answer()
    await safe_edit(q.message, "نام داروخانه را وارد کنید:", reply_markup=ForceReply(), context=context)

@owner_only_access
async def cb_pharm_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle pharmacy selection callback."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message
    if not message:
        return
    data = q.data or ""
    match = re.match(r"^fin\.pharm\.select:(\d+)$", data)
    if not match:
        return
    pharmacy_id = int(match.group(1))
    await safe_edit(message, f"داروخانه #{pharmacy_id} انتخاب شد.", reply_markup=period_list_kb(pharmacy_id), context=context)

@owner_only_access
async def cb_compare_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle compare start callback."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await safe_edit(q.message, "ماه اول را انتخاب کنید:", reply_markup=month_picker_kb(date.today().year, tag=TAG_COMPARE_PICK_FIRST), context=context)

@owner_only_access
async def cb_compare_pick_first(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle first month selection for comparison."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    # Implementation here

@owner_only_access
async def cb_compare_pick_second(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle second month selection for comparison."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    # Implementation here

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle regular text messages."""
    if not update.message:
        return
    await update.message.reply_text("لطفاً از دستورات و دکمه‌های موجود استفاده کنید.")

@owner_only_access
async def cb_pdf_simple(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """\n    Generate a simple PDF report for a given pharmacy and period.\n    The callback data format is ``fin.pdf.simple:<pharmacy_id>:<period_id>``.\n\n    This handler attempts to import the reportlab library; if unavailable,\n    it informs the user that PDF generation is not possible. Otherwise,\n    it constructs a one‑page PDF containing the pharmacy name, period\n    information, and a simple three‑column table of key metrics.\n    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    message = q.message or update.effective_message
    if not message:
        return
    data = q.data or ""
    m = re.match(r"^fin\.pdf\.simple:(\d+):(\d+)$", data)
    if not m:
        return
    pharmacy_id = int(m.group(1))
    period_id = int(m.group(2))
    metrics_row = get_metrics(pharmacy_id, period_id)
    period_row = get_period(period_id)
    if not metrics_row or not period_row:
        await safe_edit(
            message,
            "⚠️ دوره یا داده یافت نشد.",
            reply_markup=None,
            context=context,
            log_context="cb_pdf_simple_missing",
        )
        return
    # Attempt to import reportlab components
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        import tempfile
        from datetime import datetime
    except Exception:
        await safe_edit(
            message,
            "⚠️ امکان تولید فایل PDF وجود ندارد. لطفماً کتابخانه reportlab را نصب کنید.",
            reply_markup=None,
            context=context,
            log_context="cb_pdf_simple_no_reportlab",
        )
        return
    # Convert rows to dicts if necessary
    metrics = dict(metrics_row) if not isinstance(metrics_row, dict) else metrics_row
    period = dict(period_row) if not isinstance(period_row, dict) else period_row
    # Retrieve pharmacy name
    pharmacy_name = f"#{pharmacy_id}"
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT title FROM pharmacies WHERE id = ?", (pharmacy_id,))
            row = c.fetchone()
            if row and row["title"]:
                pharmacy_name = row["title"]
    except Exception:
        pass
    # Prepare table data (English)
    table_data: list[list[str]] = []
    table_data.append(["Metric", "Value", "Unit"])
    def _pct(v):
        try:
            return f"{float(v)*100:.1f}%"
        except Exception:
            return "-"
    def add_row(title: str, value, unit: str) -> None:
        table_data.append([title, value, unit])
    add_row("Cash Sales", fmt_money(metrics.get("sales_cash", 0)), "Toman")
    add_row("Insurance Deposits", fmt_money(metrics.get("sales_ins", 0)), "Toman")
    add_row("Total Sales", fmt_money(metrics.get("sales_total", 0)), "Toman")
    add_row("Variable Purchases", fmt_money(metrics.get("var_total", 0)), "Toman")
    add_row("Fixed Costs (Rent+Staff)", fmt_money(metrics.get("fixed_total", 0)), "Toman")
    add_row("Other Opex", fmt_money(metrics.get("opex_other_total", 0)), "Toman")
    add_row("Gross Profit", fmt_money(metrics.get("gross_profit", 0)), "Toman")
    add_row("Net Operating Profit", fmt_money(metrics.get("net_profit_operational", 0)), "Toman")
    add_row("Gross Margin", _pct(metrics.get("cm_ratio", 0)), "%")
    add_row("Net Margin", _pct(metrics.get("np_ratio", 0)), "%")
    add_row("Breakeven Sales", fmt_money(metrics.get("breakeven_sales", 0)), "Toman")
    add_row("Avg Daily Sales", fmt_money(metrics.get("avg_daily_sales", 0)), "Toman/day")
    add_row("Avg Sale/Visit", fmt_money(metrics.get("avg_sale_per_visit", 0)), "Toman/visit")
    add_row("Visits", str(metrics.get("visits_total", 0)), "person")
    add_row("Days in Period", str(metrics.get("days_count", 0)), "day")
    # Create a temporary file for the PDF
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp_path = tmp_file.name
    tmp_file.close()
    # Build the PDF document
    doc = SimpleDocTemplate(tmp_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()
    story = []
    # Attempt to register a Persian font; fall back to default
    try:
        pdfmetrics.registerFont(TTFont("Vazirmatn", "./assets/fonts/Vazirmatn-Regular.ttf"))
        font_name = "Vazirmatn"
    except Exception:
        font_name = "Helvetica"
    title_style = styles["Title"]
    normal_style = styles["Normal"]
    italic_style = styles["Italic"]
    # Title and period info
    story.append(Paragraph(f"Financial Report for Pharmacy {h(str(pharmacy_name))}", title_style))
    story.append(Paragraph(f"Period: {h(str(period.get('title', '')))}", normal_style))
    story.append(Paragraph(f"From {h(str(period.get('start_date', '-')))} to {h(str(period.get('end_date', '-')))}", normal_style))
    story.append(Paragraph(f"Status: {h(str(period.get('status', 'open')))}", normal_style))
    story.append(Paragraph(f"Generated on: {datetime.now().date().isoformat()}", normal_style))
    story.append(Spacer(1, 12))
    # Table with metrics
    table = Table(table_data, colWidths=[230, 120, 80])
    table_style = TableStyle(
        [
            ("GRID", (0, 0), (-1, -1), 0.5, colors.gray),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("ALIGN", (1, 1), (-1, -1), "LEFT"),
        ]
    )
    table.setStyle(table_style)
    story.append(table)
    story.append(Spacer(1, 12))
    story.append(Paragraph("This report was auto-generated by FINbot.", italic_style))
    # Render the PDF
    doc.build(story)
    # Send the generated PDF to the user
    try:
        await context.bot.send_document(
            chat_id=message.chat.id,
            document=open(tmp_path, "rb"),
            filename=f"Report_{pharmacy_id}_{period_id}.pdf",
            caption="📄 PDF report is ready.",
        )
    except Exception:
        await safe_edit(
            message,
            "⚠️ خطا در ارسال فایل PDF.",
            reply_markup=None,
            context=context,
            log_context="cb_pdf_simple_send_error",
        )
    finally:
        import os as _os
        try:
            _os.unlink(tmp_path)
        except Exception:
            pass

def main() -> None:
    """Entry point for running the Telegram bot."""
    migrate()
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .defaults(Defaults(parse_mode=ParseMode.HTML))
        .build()
    )

    # --- Commands ---
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("about", cmd_about))
    application.add_handler(CommandHandler("ping", cmd_ping))

    # --- Main Menu Flow ---
    application.add_handler(CallbackQueryHandler(cb_home, pattern=r"^fin\.home$"))
    application.add_handler(CallbackQueryHandler(cb_pharm_new, pattern=r"^fin\.pharm\.new$"))
    application.add_handler(CallbackQueryHandler(cb_pharm_select, pattern=r"^fin\.pharm\.select:(\d+)$"))

    # --- Check Registration ---
    application.add_handler(CallbackQueryHandler(cb_check_start, pattern=rf"^{TAG_CHECK_START}:(\d+)$"))
    application.add_handler(
        CallbackQueryHandler(cb_check_pick_day, pattern=rf"^{TAG_CHECK_PICK_DAY}:\d{{4}}-\d{{2}}-\d{{2}}:(\d+)$")
    )

    # --- Daily Registration ---
    application.add_handler(CallbackQueryHandler(cb_daily_start, pattern=rf"^{TAG_DAILY_START}:(\d+)$"))
    application.add_handler(
        CallbackQueryHandler(cb_daily_pick_day, pattern=rf"^{TAG_DAILY_FLOW_PICK_DAY}:\d{{4}}-\d{{2}}-\d{{2}}:(\d+)$")
    )

    # --- Monthly Summary ---
    application.add_handler(CallbackQueryHandler(cb_summary_start, pattern=rf"^{TAG_SUMMARY_START}:(\d+)$"))
    application.add_handler(
        CallbackQueryHandler(cb_summary_pick_month, pattern=rf"^{TAG_SUMMARY_PICK_MONTH}:\d{{4}}-\d{{2}}:(\d+)$")
    )

    # --- Compare Months ---
    application.add_handler(CallbackQueryHandler(cb_compare_start, pattern=rf"^{TAG_COMPARE_START}:(\d+)$"))
    application.add_handler(
        CallbackQueryHandler(cb_compare_pick_first, pattern=rf"^{TAG_COMPARE_PICK_FIRST}:\d{{4}}-\d{{2}}:(\d+)$")
    )
    application.add_handler(
        CallbackQueryHandler(cb_compare_pick_second, pattern=rf"^{TAG_COMPARE_PICK_SECOND}:\d{{4}}-\d{{2}}:(\d+)$")
    )

    # --- Unified message flow (check/daily) ---
    application.add_handler(MessageHandler(filters.TEXT & filters.REPLY & ~filters.COMMAND, msg_check_flow))
    application.add_handler(MessageHandler(filters.TEXT & filters.REPLY & ~filters.COMMAND, msg_daily_flow))

    # Fallback handler (optional text logging)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # --- Start polling ---
    application.run_polling(
        allowed_updates=[
            UpdateType.MESSAGE,
            UpdateType.CALLBACK_QUERY,
            UpdateType.EDITED_MESSAGE,
            UpdateType.MY_CHAT_MEMBER,
            UpdateType.CHAT_MEMBER,
        ],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    import sys
    try:
        main()
    except KeyboardInterrupt:
        pass
