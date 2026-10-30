import asyncio
import base64
import html
import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
import httpx
import jwt
from PIL import Image
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.types import (
    BusinessConnection,
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaDocument,
)
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("business_adapter")

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
HERMES_API_URL = os.environ.get("HERMES_API_URL", "http://hermes:8642").rstrip("/")
HERMES_API_KEY = os.environ["HERMES_API_KEY"]
BUSINESS_OWNER_ID = int(os.environ["BUSINESS_OWNER_ID"])
DB_PATH = os.environ.get("ADAPTER_DB_PATH", "/app/data/adapter.sqlite3")
TELEGRAM_MCP_URL = os.environ.get("TELEGRAM_MCP_URL", "http://172.17.0.1:8765/mcp").rstrip("/")
TELEGRAM_MCP_KEY = os.environ.get("TELEGRAM_MCP_KEY", "3R1Pxrd8i8EC5hDdurDY3__f9XNPdLilYl0_4hyOMLQ")

# Admin IDs to notify when payment receipt is received
raw_admins = os.environ.get("ADMIN_CHATS", str(BUSINESS_OWNER_ID))
ADMIN_CHATS = [int(x.strip()) for x in raw_admins.split(",") if x.strip()]
RECEIPT_THREAD_ID = int(os.environ.get("RECEIPT_THREAD_ID", "0")) or None
ESCALATION_THREAD_ID = int(os.environ.get("ESCALATION_THREAD_ID", "0")) or None

UNKNOWN_QUESTION_RE = re.compile(
    r"(aniqlab|aniqlashtirib|surishtirib|bilishim\s*bilan|bilgach|menejerdan\s*so['’`]?rab|аниқлаб|аниқлаштириб|суриштириб)[^\n\.\?!]{0,40}(yoz|javob|ayt|bildir|ёз|жавоб|айт|билдир)",
    re.IGNORECASE
)

# Debounce configuration (in seconds)
DEBOUNCE_WAIT = float(os.environ.get("DEBOUNCE_WAIT", "3.5"))
MAX_BURST_WAIT = float(os.environ.get("MAX_BURST_WAIT", "10.0"))
SAFE_TEST_MODE = os.environ.get("SAFE_TEST_MODE", "true").lower() == "true"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

connections: dict[str, BusinessConnection] = {}

# Buffering structures for debounced bursts
chat_buffers: dict[int, list[str]] = {}
chat_first_message_time: dict[int, float] = {}
chat_tasks: dict[int, asyncio.Task] = {}
chat_connections: dict[int, str | None] = {}
chat_users: dict[int, tuple[str, str]] = {}  # chat_id -> (full_name, handle)
chat_receipts: dict[int, tuple[str, str, str]] = {}  # chat_id -> (file_id, file_type, caption)
chat_last_message_ids: dict[int, int] = {}  # chat_id -> latest message_id
chat_processing_lock: dict[int, asyncio.Lock] = {}

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                chat_id INTEGER PRIMARY KEY,
                last_response_id TEXT,
                updated_at REAL,
                manager_intervened INTEGER DEFAULT 0,
                is_paused INTEGER DEFAULT 0,
                paused_at REAL DEFAULT 0,
                pause_reason TEXT
            )
        """)
        for col_def in [
            "manager_intervened INTEGER DEFAULT 0",
            "is_paused INTEGER DEFAULT 0",
            "paused_at REAL DEFAULT 0",
            "pause_reason TEXT",
        ]:
            try:
                con.execute(f"ALTER TABLE chat_sessions ADD COLUMN {col_def}")
            except Exception:
                pass

        con.execute("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                sender TEXT,
                text TEXT,
                created_at REAL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_id ON chat_messages(chat_id, id DESC)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS receipt_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                conn_id TEXT,
                user_name TEXT,
                user_handle TEXT,
                caption TEXT,
                ai_analysis TEXT,
                status TEXT DEFAULT 'pending',
                handled_by TEXT,
                created_at REAL,
                updated_at REAL
            )
        """)
        try:
            con.execute("ALTER TABLE receipt_records ADD COLUMN ai_analysis TEXT")
        except Exception:
            pass

        con.execute("""
            CREATE TABLE IF NOT EXISTS escalation_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                conn_id TEXT,
                user_name TEXT,
                user_handle TEXT,
                reason TEXT,
                customer_message TEXT,
                status TEXT DEFAULT 'open',
                handled_by TEXT,
                created_at REAL,
                updated_at REAL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS customer_materials (
                chat_id INTEGER PRIMARY KEY,
                sent_at REAL
            )
        """)

def get_last_response_id(chat_id: int) -> str | None:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT last_response_id, updated_at FROM chat_sessions WHERE chat_id = ?", (chat_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            last_resp_id, updated_at = row[0], row[1]
            # If session is older than 4 hours (14400s), expire it to allow fresh conversation
            if updated_at and (time.time() - updated_at > 14400):
                logger.info(f"Session for chat {chat_id} expired (> 4 hours). Starting fresh.")
                return None
            return last_resp_id
    except Exception as e:
        logger.error(f"Error reading last_response_id: {e}")
        return None

def get_session_info(chat_id: int) -> tuple[str | None, float | None, bool]:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT last_response_id, updated_at, manager_intervened FROM chat_sessions WHERE chat_id = ?", (chat_id,))
            row = cur.fetchone()
            if row:
                return row[0], row[1], bool(row[2])
            return None, None, False
    except Exception as e:
        logger.error(f"Error reading session info: {e}")
        return None, None, False

def set_last_response_id(chat_id: int, response_id: str):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "INSERT INTO chat_sessions (chat_id, last_response_id, updated_at, manager_intervened) VALUES (?, ?, ?, 0) "
                "ON CONFLICT(chat_id) DO UPDATE SET last_response_id = excluded.last_response_id, updated_at = excluded.updated_at, manager_intervened = 0",
                (chat_id, response_id, time.time())
            )
    except Exception as e:
        logger.error(f"Error saving last_response_id: {e}")

def set_manager_intervened(chat_id: int, intervened: bool):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "INSERT INTO chat_sessions (chat_id, manager_intervened, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET manager_intervened = excluded.manager_intervened, updated_at = excluded.updated_at",
                (chat_id, 1 if intervened else 0, time.time())
            )
    except Exception as e:
        logger.error(f"Error setting manager_intervened: {e}")

def reset_chat_session(chat_id: int):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute("DELETE FROM chat_sessions WHERE chat_id = ?", (chat_id,))
            con.execute("DELETE FROM customer_materials WHERE chat_id = ?", (chat_id,))
            con.commit()
            logger.info(f"Reset chat session for {chat_id}")
    except Exception as e:
        logger.error(f"Error resetting chat session for {chat_id}: {e}")

def has_sent_course_materials(chat_id: int) -> bool:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM customer_materials WHERE chat_id = ?", (chat_id,))
            return bool(cur.fetchone())
    except Exception as e:
        logger.error(f"Error checking customer_materials for {chat_id}: {e}")
        return False

def set_course_materials_sent(chat_id: int):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute("INSERT OR REPLACE INTO customer_materials (chat_id, sent_at) VALUES (?, ?)", (chat_id, time.time()))
    except Exception as e:
        logger.error(f"Error saving customer_materials for {chat_id}: {e}")

def save_chat_message(chat_id: int, sender: str, text: str):
    clean = text.strip() if text else ""
    if not clean:
        return
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "INSERT INTO chat_messages (chat_id, sender, text, created_at) VALUES (?, ?, ?, ?)",
                (chat_id, sender, clean, time.time())
            )
    except Exception as e:
        logger.error(f"Error saving chat message: {e}")

def save_receipt_record(chat_id: int, conn_id: str | None, user_name: str, user_handle: str, caption: str, ai_analysis: str = "") -> int:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO receipt_records (chat_id, conn_id, user_name, user_handle, caption, ai_analysis, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (chat_id, conn_id, user_name, user_handle, caption, ai_analysis, time.time(), time.time())
            )
            return cur.lastrowid
    except Exception as e:
        logger.error(f"Error saving receipt record: {e}")
        return 0

def get_receipt_record(receipt_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM receipt_records WHERE id = ?", (receipt_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"Error reading receipt record {receipt_id}: {e}")
        return None

def update_receipt_record(receipt_id: int, status: str, handled_by: str):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "UPDATE receipt_records SET status = ?, handled_by = ?, updated_at = ? WHERE id = ?",
                (status, handled_by, time.time(), receipt_id)
            )
    except Exception as e:
        logger.error(f"Error updating receipt record {receipt_id}: {e}")

def is_chat_paused(chat_id: int) -> bool:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT is_paused, paused_at FROM chat_sessions WHERE chat_id = ?", (chat_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                return False
            is_paused, paused_at = row[0], row[1]
            # Auto-unpause after 24 hours if forgotten
            if paused_at and (time.time() - paused_at > 86400):
                set_chat_paused(chat_id, False)
                return False
            return bool(is_paused)
    except Exception as e:
        logger.error(f"Error checking is_chat_paused: {e}")
        return False

def set_chat_paused(chat_id: int, paused: bool, reason: str = ""):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "INSERT INTO chat_sessions (chat_id, is_paused, paused_at, pause_reason, updated_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET is_paused = excluded.is_paused, paused_at = excluded.paused_at, "
                "pause_reason = excluded.pause_reason, updated_at = excluded.updated_at",
                (chat_id, 1 if paused else 0, time.time() if paused else 0, reason if paused else "", time.time())
            )
            logger.info(f"Chat {chat_id} paused={paused} (reason: {reason})")
    except Exception as e:
        logger.error(f"Error setting chat paused state: {e}")

def save_escalation_record(chat_id: int, conn_id: str | None, user_name: str, user_handle: str, reason: str, customer_message: str) -> int:
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO escalation_records (chat_id, conn_id, user_name, user_handle, reason, customer_message, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)",
                (chat_id, conn_id, user_name, user_handle, reason, customer_message, time.time(), time.time())
            )
            return cur.lastrowid
    except Exception as e:
        logger.error(f"Error saving escalation record: {e}")
        return 0

def update_escalation_status(escalation_id: int, status: str, handled_by: str):
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "UPDATE escalation_records SET status = ?, handled_by = ?, updated_at = ? WHERE id = ?",
                (status, handled_by, time.time(), escalation_id)
            )
    except Exception as e:
        logger.error(f"Error updating escalation record {escalation_id}: {e}")

ESCALATION_PATTERNS = [
    (
        re.compile(r"(sud\b|sudga|sudlash|prokura|militsiya|ichki\s*ishlar|advokat|yurist|iste['’`]?molchi|organ\b|ariza\s*yoz|ariza\s*ber)", re.IGNORECASE),
        "Huquqiy / Sud tahdidi yoki shikoyat",
    ),
    (
        re.compile(r"(pulim(ni)?\s*(qaytar|ber)|to['’`]?lov(ni)?\s*qaytar|qaytarib\s*ber|vozvrat|otmena\s*qil|bekor\s*qil(ing)?|pulim\s*kuydimi)", re.IGNORECASE),
        "To'lovni qaytarish (refund) talabi",
    ),
    (
        re.compile(r"(aldov|alda(dingiz|yapsiz|sh)|firibgar|yolg['’`]?on(chi)?|scam\b|moshennik|kidat\b|obman\b)", re.IGNORECASE),
        "Aldov / Firibgarlik ayblovi",
    ),
    (
        re.compile(r"(odam\s*bilan|tirik\s*odam|inson\s*bilan|operator\b|menejer(ingiz)?ni\s*(chaqir|ber|ulang)|menejer\s*kerak|jonli\s*odam)", re.IGNORECASE),
        "Jonli operator / Menejer talabi",
    ),
    (
        re.compile(r"(noroziman|rozi\s*emasman|arz\s*qilaman|ustilaringdan\s*arz|shikoyat\s*qil)", re.IGNORECASE),
        "Jiddiy e'tiroz / Norozilik",
    ),
]

def detect_escalation_keywords(text: str) -> str | None:
    if not text:
        return None
    for pattern, reason in ESCALATION_PATTERNS:
        if pattern.search(text):
            return reason
    return None

def chunk_text(text: str, limit: int = 3500) -> list[str]:
    chunks = []
    current = []
    current_len = 0
    for line in text.split("\n"):
        if current_len + len(line) + 1 > limit:
            if current:
                chunks.append("\n".join(current))
                current = []
                current_len = 0
            while len(line) > limit:
                chunks.append(line[:limit])
                line = line[limit:]
        current.append(line)
        current_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks or [text]

def extract_clean_text(raw_text: str) -> str:
    cleaned = raw_text.strip()
    # Check if wrapped in markdown code blocks like ```json ... ```
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].strip() == "```":
            cleaned = "\n".join(lines[1:-1]).strip()

    # If it's a JSON object like {"action":"reply","text":"...","image":null}
    if cleaned.startswith("{") and cleaned.endswith("}"):
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                if parsed.get("action") == "skip":
                    return ""
                if "text" in parsed and parsed["text"]:
                    return str(parsed["text"]).strip()
        except Exception:
            pass

    return raw_text.strip()

@dp.business_connection()
async def on_business_connection(conn: BusinessConnection):
    logger.info(
        f"Business connection update: id={conn.id}, user={conn.user.id} (@{conn.user.username}), "
        f"is_enabled={conn.is_enabled}, can_reply={conn.can_reply}"
    )
    if conn.is_enabled:
        connections[conn.id] = conn
    else:
        connections.pop(conn.id, None)

def format_telegram_html(text: str) -> str:
    links = []
    def save_link(match):
        label, url = match.group(1), match.group(2)
        idx = len(links)
        links.append((label, url))
        return f"__LINK_PLACEHOLDER_{idx}__"

    text_with_placeholders = re.sub(r"\[([^\]]+)\]\((https?://[^\)]+)\)", save_link, text)
    safe_text = html.escape(text_with_placeholders)
    safe_text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", safe_text)
    for idx, (label, url) in enumerate(links):
        safe_label = html.escape(label)
        safe_url = html.escape(url)
        a_tag = f'<a href="{safe_url}">{safe_label}</a>'
        safe_text = safe_text.replace(f"__LINK_PLACEHOLDER_{idx}__", a_tag)
    return safe_text

def split_human_messages(text: str) -> list[str]:
    cleaned = text.strip()
    if not cleaned:
        return []

    # 1. Explicit delimiter "---" placed by LLM
    if re.search(r"(?:^|\n)\s*---\s*(?:\n|$)", cleaned):
        parts = [p.strip() for p in re.split(r"(?:^|\n)\s*---\s*(?:\n|$)", cleaned) if p.strip()]
        if parts:
            return parts[:3]

    # 2. Natural paragraph splitting if text has distinct paragraphs
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", cleaned) if p.strip()]
    if len(paragraphs) == 2 and len(cleaned) > 80:
        return paragraphs

    return [cleaned]

async def send_customer_message(chat_id: int, conn_id: str | None, text: str):
    sub_messages = split_human_messages(text)
    for m_idx, sub_msg in enumerate(sub_messages):
        if m_idx > 0:
            delay = min(2.2, max(1.2, len(sub_msg) * 0.02))
            try:
                if conn_id:
                    await bot.send_chat_action(chat_id=chat_id, action="typing", business_connection_id=conn_id)
                else:
                    await bot.send_chat_action(chat_id=chat_id, action="typing")
            except Exception:
                pass
            await asyncio.sleep(delay)

        for chunk in chunk_text(sub_msg):
            html_chunk = format_telegram_html(chunk)
            sent = False

            # 1. Try HTML
            try:
                if conn_id:
                    await bot.send_message(
                        chat_id=chat_id,
                        business_connection_id=conn_id,
                        text=html_chunk,
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=html_chunk,
                        parse_mode=ParseMode.HTML,
                    )
                sent = True
            except TelegramBadRequest as e:
                if "BUSINESS_PEER_INVALID" in str(e):
                    logger.warning(f"BUSINESS_PEER_INVALID for chat {chat_id} (conn_id={conn_id}). Attempting direct bot send...")
                    try:
                        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=None)
                        sent = True
                    except Exception as direct_e:
                        logger.error(f"Failed direct fallback for chat {chat_id}: {direct_e}")
                    continue
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
            except Exception as e:
                logger.warning(f"HTML send failed for {chat_id}: {e}")

            if sent:
                continue

            # 2. Try Markdown
            try:
                if conn_id:
                    await bot.send_message(
                        chat_id=chat_id,
                        business_connection_id=conn_id,
                        text=chunk,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                else:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=chunk,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                sent = True
            except TelegramBadRequest as e:
                if "BUSINESS_PEER_INVALID" in str(e):
                    try:
                        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=None)
                        sent = True
                    except Exception as direct_e:
                        logger.error(f"Failed direct fallback for chat {chat_id}: {direct_e}")
                    continue
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
            except Exception as e:
                logger.warning(f"Markdown send failed for {chat_id}: {e}")

            if sent:
                continue

            # 3. Plain text fallback
            try:
                if conn_id:
                    await bot.send_message(
                        chat_id=chat_id,
                        business_connection_id=conn_id,
                        text=chunk,
                        parse_mode=None,
                    )
                else:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=chunk,
                        parse_mode=None,
                    )
                sent = True
            except TelegramBadRequest as e:
                if "BUSINESS_PEER_INVALID" in str(e):
                    try:
                        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=None)
                        sent = True
                    except Exception as direct_e:
                        logger.error(f"Failed direct fallback for chat {chat_id}: {direct_e}")
                else:
                    logger.error(f"Plain text send failed for {chat_id}: {e}")
            except Exception as e:
                logger.error(f"Failed to deliver message to {chat_id}: {e}")

COURSE_ANNOUNCEMENT_TEXT = (
    "🔥«porusski gaplashamiz» 16-potok online kursiga PREDAPIS QABUL OCHILDI! \n\n"
    "💬 1-darsdan boshlab rus tilida gapira boshlaysiz!\n"
    "⸻\n"
    "📅 Darslar boshlanish sanasi: 10-noyabr (10-noyabrdan darajani aniqlash boshlanadi) \n"
    "Davomiyligi: 2 oy\n"
    "🎥 Format: Telegram guruhida va Zoomda jonli video darslar\n"
    "⏰ Vaqt: 06:00 – 00:30 (o‘zingizga qulay guruhni tanlaysiz)\n"
    "⸻\n"
    "Sizni nimalar kutadi?\n"
    "✅ Har kuni jonli darslar\n"
    " • 3 kun — Zaynab Kasimova bilan grammatika\n"
    "• 4 kun — jonli razgovor darslari (1,5–2 soat)\n"
    "• 3 kun — slovar darslari (1 soat)\n"
    "• 2 ta mualliflik kitoblari beriladi («porusski-gaplashamiz» va «0 dan Razgovorgacha») \n"
    "✅ Individual nazorat — uy vazifalaringiz har kuni tekshiriladi\n"
    "— Siz bilan 3 ta kurator va Zaynab Kasimova ishlaydi\n"
    "— 10-15 kishilik guruhlardagi muhit — motivatsiya va qo‘llab-quvvatlash\n"
    "⸻\n"
    "💰 Asl narx: 1 490 000 so‘m / 2 oyga\n"
    "🔥 PREDZAPIS narxi: 980 000 so‘m / 2 oyga\n"
    "💡 Faqat birinchi 100 kishi uchun shu narx!\n\n"
    "Bugun joyingizni 100 ming bilan band qilib, qolgan to'lovni 3-noyabrgacha qilishingiz mumkin! \n\n"
    "💳 Karta raqami:\n"
    "5614 6821 1260 5573\n"
    "Zaynab Qosimova\n\n"
    "🏦 Bank: Ipak yo'li (Uzcard)\n\n"
    "Chekni 👉🏻 @porusski_manager1 ga yuboring! Chatga yubormang!"
)

BOOK_1_FILE_ID = "BQACAgIAAxkDAAEFtcxq4bBzPoKCZte0cJtcdqa250qRlwAC_IsAAqdxsUv2StWhBputRT0E"
BOOK_2_FILE_ID = "BQACAgIAAxkDAAEFtc9q4bBzrTWslYpFnoBSqTZ1cqjsrgACD4wAAqdxsUugHVgccrCOWT0E"
BOOK_2_CAPTION = (
    "online kursimizda o'tiladigan kitoblardan namuna⚡️\n"
    "[Respublika Ta'lim markazidan o'zbeklarga rus tilini o'rgatish metodikasida tuzilganligi tasdiqdan o'tgan☺️]"
)
VIDEO_SAMPLE_FILE_ID = "BAACAgIAAxkDAAEFtdFq4bBzhQ9yiUL_n7YR-UROlpZNOAAC3YMAAixCmEsAAUsTwDrSkAc9BA"
VIDEO_SAMPLE_CAPTION = (
    "⚡️VIDOEODARSLIK\n\n"
    "— «porusski-gaplashamiz» online kursidagi videodarsliklardan namuna ✅\n\n"
    "Mavzu: Rodlar✅"
)

async def send_course_materials_to_customer(chat_id: int, conn_id: str | None = None):
    logger.info(f"Delivering official course materials package to chat {chat_id} (conn_id={conn_id})...")

    # 1. Send the exact course announcement post (Message 9 from topic 6)
    try:
        if conn_id:
            await bot.send_message(
                chat_id=chat_id,
                business_connection_id=conn_id,
                text=COURSE_ANNOUNCEMENT_TEXT,
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=COURSE_ANNOUNCEMENT_TEXT,
            )
        logger.info(f"Official course announcement text delivered to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send course announcement text to {chat_id}: {e}")

    await asyncio.sleep(1.0)

    # 2. Send the two book PDFs as an album (Messages 10 & 11 from topic 6)
    try:
        media = [
            InputMediaDocument(media=BOOK_1_FILE_ID),
            InputMediaDocument(media=BOOK_2_FILE_ID, caption=BOOK_2_CAPTION),
        ]
        if conn_id:
            await bot.send_media_group(
                chat_id=chat_id,
                business_connection_id=conn_id,
                media=media,
            )
        else:
            await bot.send_media_group(
                chat_id=chat_id,
                media=media,
            )
        logger.info(f"Book samples album (2 PDFs) delivered to {chat_id}")
    except Exception as e:
        logger.warning(f"send_media_group failed ({e}), falling back to single documents")
        try:
            if conn_id:
                await bot.send_document(chat_id=chat_id, document=BOOK_1_FILE_ID, business_connection_id=conn_id)
                await asyncio.sleep(0.5)
                await bot.send_document(chat_id=chat_id, document=BOOK_2_FILE_ID, caption=BOOK_2_CAPTION, business_connection_id=conn_id)
            else:
                await bot.send_document(chat_id=chat_id, document=BOOK_1_FILE_ID)
                await asyncio.sleep(0.5)
                await bot.send_document(chat_id=chat_id, document=BOOK_2_FILE_ID, caption=BOOK_2_CAPTION)
        except Exception as doc_err:
            logger.error(f"Failed fallback sending documents to {chat_id}: {doc_err}")

    await asyncio.sleep(1.0)

    # 3. Send Video Lesson (Message 12 from topic 6)
    try:
        if conn_id:
            await bot.send_video(
                chat_id=chat_id,
                business_connection_id=conn_id,
                video=VIDEO_SAMPLE_FILE_ID,
                caption=VIDEO_SAMPLE_CAPTION,
            )
        else:
            await bot.send_video(
                chat_id=chat_id,
                video=VIDEO_SAMPLE_FILE_ID,
                caption=VIDEO_SAMPLE_CAPTION,
            )
        logger.info(f"Video lesson sample delivered to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send video sample to {chat_id}: {e}")

    set_course_materials_sent(chat_id)

async def _keep_typing_action(chat_id: int, conn_id: str | None, stop_ev: asyncio.Event):
    while not stop_ev.is_set():
        try:
            if conn_id:
                await bot.send_chat_action(chat_id=chat_id, action="typing", business_connection_id=conn_id)
            else:
                await bot.send_chat_action(chat_id=chat_id, action="typing")
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_ev.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            pass

async def query_hermes_and_reply(chat_id: int, conn_id: str | None, prompt: str, user_info: tuple[str, str] = ("User", "")):
    is_system_instruction = prompt.startswith("[Tizim:")

    prev_response_id = get_last_response_id(chat_id)
    payload = {
        "model": "default",
        "input": prompt,
        "stream": False,
    }
    if prev_response_id:
        payload["previous_response_id"] = prev_response_id

    # Keep typing status active until Hermes finishes response
    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(_keep_typing_action(chat_id, conn_id, stop_typing))

    # Call Hermes API with retries
    data = None
    try:
        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    session_key = f"tg_{'biz' if conn_id else 'direct'}_{chat_id}"
                    resp = await client.post(
                        f"{HERMES_API_URL}/v1/responses",
                        headers={
                            "Authorization": f"Bearer {HERMES_API_KEY}",
                            "X-Hermes-Session-Key": session_key,
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    break
            except Exception as e:
                logger.warning(f"Attempt {attempt}/3 error querying Hermes API: {e}")
                if attempt < 3:
                    await asyncio.sleep(2)
    finally:
        stop_typing.set()
        typing_task.cancel()

    if not data:
        logger.error(f"Failed to get response from Hermes for chat {chat_id}.")
        return

    # Update conversation chain with the returned response id
    new_response_id = data.get("id")
    if new_response_id:
        set_last_response_id(chat_id, new_response_id)

    # Extract assistant reply text
    reply_text = ""
    for item in data.get("output", []):
        if item.get("type") == "message" and item.get("role") == "assistant":
            for part in item.get("content", []):
                if part.get("type") == "output_text":
                    reply_text += part.get("text", "")

    reply_text = reply_text.strip()
    if not reply_text:
        logger.warning("Hermes returned empty response text.")
        return

    reply_text = extract_clean_text(reply_text)
    if not reply_text:
        logger.info(f"Hermes response for chat {chat_id} resolved to empty/skip after cleanup.")
        return

    # Check if Hermes flagged an escalation via [ESCALATE: ...] or answered with "aniqlab ... yozaman/javob beraman"
    esc_match = re.search(r"\[ESCALATE:\s*([^\]]+)\]", reply_text, re.IGNORECASE)
    unknown_match = UNKNOWN_QUESTION_RE.search(reply_text)

    if esc_match or unknown_match:
        llm_reason = esc_match.group(1).strip() if esc_match else "Menejerdan aniqlash talab etiladi (Mijoz savoli)"
        if esc_match:
            reply_text = re.sub(r"\[ESCALATE:\s*[^\]]+\]", "", reply_text).strip()

        # Safeguard: Do not escalate normal sales inquiries (partial payment, discount, booking, pricing, or "botmisiz")
        non_esc_pattern = re.compile(
            r"(qisman|100\s*000|100\.000|100k|bo['’`]?lib|chegirma|aksiya|narx|skidka|discount|kamroq|arzon|joy\s*band|botmisi|robot)",
            re.IGNORECASE
        )
        is_unknown_question = bool(
            re.search(r"(aniqlash|aniqlab|ma['’`]?lumot|savol|bilish)", llm_reason, re.IGNORECASE)
            or unknown_match
        )

        # Safeguard: Do not escalate on short acknowledgements (e.g. "Xo'p", "Кутаман", "Rahmat", "Ok")
        is_short_ack = bool(re.match(r"^[\s\.\?!,–—]*(xo['’`]?p|xop|hop|ok|mayli|rahmat|raxmat|tushunarli|tushundim|yaxshi|кутаман|ок|хўп|хуп|раҳмат|рахмат|тушундим|тушунарли)[\s\.\?!,–—]*$", prompt.strip(), re.IGNORECASE))
        if is_short_ack and is_unknown_question:
            logger.info(f"Filtered out false escalation on short acknowledgment '{prompt}' for chat {chat_id}")
            if UNKNOWN_QUESTION_RE.search(reply_text):
                reply_text = "Kelishdik, boshqa savollaringiz bo‘lsa bemalol yozishingiz mumkin 🌸"
            is_unknown_question = False

        if non_esc_pattern.search(llm_reason) and not is_unknown_question:
            logger.info(f"Filtered out false sales/bot inquiry escalation '{llm_reason}' for chat {chat_id}")
            if not reply_text:
                if re.search(r"(botmisi|robot)", llm_reason, re.IGNORECASE):
                    reply_text = "Men Zaynab Kasimovaning rasmiy yordamchisiman 😊 Sizga qanday yordam bera olaman?"
                else:
                    reply_text = (
                        "«Po-russki gaplashamiz» kursimizga hozir eng qulay predzapis narxi — 980 000 so‘m o‘rnatilgan. "
                        "Joyingizni saqlab qolish uchun esa hozir bor-yo‘g‘i 100 000 so‘m to‘lab qo‘ysangiz kifoya 😊\n\n---\n100 000 so‘m bilan joyingizni band qilib beraymi?"
                    )
        elif is_unknown_question:
            # Per instruction: notify admins, but do NOT pause the chat!
            if not reply_text:
                reply_text = "Buni aniqlab, sizga tez orada javob beraman 🌸"
            logger.info(f"Unknown question escalation for chat {chat_id} (chat remains active): {llm_reason}")
            user_name, user_handle = user_info
            asyncio.create_task(
                notify_admins_about_escalation(
                    chat_id, user_name, user_handle, llm_reason, prompt, conn_id, is_paused=False, bot_reply=reply_text
                )
            )
        else:
            # Real conflict, complaint, refund demand, legal threat, operator request -> pause bot!
            if not reply_text:
                reply_text = (
                    "Sizni tushundim. Murojaatingizni zudlik bilan bosh menejerimizga yetkazdim. "
                    "Masalangiz shaxsan menejer nazoratida o‘rganiladi va tez orada siz bilan bog‘lanishadi."
                )
            logger.warning(f"Hermes signaled conflict escalation for chat {chat_id}: {llm_reason}")
            set_chat_paused(chat_id, True, reason=llm_reason)
            user_name, user_handle = user_info
            asyncio.create_task(
                notify_admins_about_escalation(
                    chat_id, user_name, user_handle, llm_reason, prompt, conn_id, is_paused=True, bot_reply=reply_text
                )
            )

    send_materials_triggered = (
        "[SEND_COURSE_MATERIALS]" in reply_text
        or bool(re.search(r"(16-potok|darajangiz\s*kursimizga\s*aynan\s*mos|darajangiz\s*mos|ayni\s*muddao)", reply_text, re.IGNORECASE))
    )
    reply_text = reply_text.replace("[SEND_COURSE_MATERIALS]", "").strip()

    # If official materials package will be delivered, ensure reply_text only contains the personal level confirmation
    if send_materials_triggered:
        match = re.search(r"(🔥|⸻|«po-russki gaplashamiz» 16|«porusski gaplashamiz» 16|PREDAPIS|16-potok online)", reply_text)
        if match:
            clean_confirmation = reply_text[:match.start()].strip()
            if len(clean_confirmation) > 10:
                reply_text = clean_confirmation
            else:
                reply_text = "Ayni muddao! «po-russki gaplashamiz 16.0» kursimizda aynan sizga mos alohida guruhimiz bor ✨"

    logger.info(f"Hermes response for chat {chat_id}: {reply_text[:80]}")

    # Save messages to local SQLite store
    if not is_system_instruction:
        save_chat_message(chat_id, "customer", prompt)
    save_chat_message(chat_id, "manager", reply_text)

    await send_customer_message(chat_id, conn_id, reply_text)

    # Deliver course materials (PDF book sample + video sample)
    if send_materials_triggered and not has_sent_course_materials(chat_id):
        logger.info(f"Triggering course materials dispatch for {chat_id}")
        asyncio.create_task(send_course_materials_to_customer(chat_id, conn_id))
    elif not is_system_instruction and re.search(r"(kitob|video|vedio|vd|darslik|namuna|material|pdf)[^\n\.\?!]{0,25}(namuna|ko['’`]?r|ber|yubor|tashla|jonat|ташла|юбор|бер|кўр|видео|китоб)", prompt, re.IGNORECASE):
        logger.info(f"Customer requested samples in prompt for {chat_id}")
        asyncio.create_task(send_course_materials_to_customer(chat_id, conn_id))

async def download_telegram_file(file_id: str) -> bytes | None:
    try:
        file = await bot.get_file(file_id)
        if not file.file_path:
            return None
        bio = io.BytesIO()
        await bot.download_file(file.file_path, destination=bio)
        return bio.getvalue()
    except Exception as e:
        logger.error(f"Failed to download Telegram file {file_id}: {e}")
        return None

def compress_image_for_vision(image_bytes: bytes, max_dim: int = 1024) -> tuple[bytes, str]:
    if not image_bytes:
        return image_bytes, "image/jpeg"
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=85, optimize=True)
        compressed = out.getvalue()
        logger.info(f"Image compressed for vision: {len(image_bytes)} -> {len(compressed)} bytes")
        return compressed, "image/jpeg"
    except Exception as e:
        logger.warning(f"Failed to compress image for vision (using original): {e}")
        return image_bytes, "image/jpeg"

async def analyze_receipt_with_vision(image_bytes: bytes, mime_type: str = "image/jpeg") -> str:
    if not image_bytes:
        return ""
    try:
        compressed_bytes, mime_type = compress_image_for_vision(image_bytes)
        b64_data = base64.b64encode(compressed_bytes).decode("utf-8")
        data_url = f"data:{mime_type};base64,{b64_data}"

        prompt = (
            "Siz to'lov cheklarini tekshiruvchi professional nazoratchisiz. "
            "Ushbu rasmda yuborilgan to'lov chekini sinchiklab tekshiring va rasmiy rekvizitlarimizga mosligini baholang:\n\n"
            "Rasmiy rekvizitlarimiz:\n"
            "1. O'zbekiston (Uzcard): 5614 6821 1260 5573 (Zaynab Qosimova, Ipak yo'li bank)\n"
            "   - Minimal joy band qilish to'lovi: 100 000 UZS\n"
            "   - Qoldiq to'lov (qolgan to'lov: 980 000 - 100 000) yoki 15-potok o'quvchilari to'lovi: 880 000 UZS\n"
            "   - To'liq 2 oylik kurs narxi: 980 000 UZS\n"
            "   - Bo'lib to'lash: har qanday 100 000 UZS dan yuqori summa (200 000, 500 000, 880 000, 980 000) to'liq yoki qisman to'lov sifatida ✅ Yetarli deb baholansin!\n"
            "2. Xorijiy (VISA): 4916 9903 1015 2035 (Qosimova Zaynab, Uzum Bank)\n"
            "   - To'liq kurs: 6 760 RUB / 39 000 KZT / 7 200 KGS / 82 USD (yoki xorijdan bandlov: 10 $ / 1 000 RUB)\n\n"
            "MUHIM SUMMA QOIDALARI:\n"
            "- Agar summa 100 000 UZS yoki undan ko'p bo'lsa (ayniqsa 880 000 UZS qoldiq to'lov bo'lsa), aslo '❌ Kam' demang! '✅ Yetarli (qoldiq / to'lov)' deb belgilang.\n"
            "- FAQAT chekdagi summa 100 000 UZS dan KAM bo'lsagina (masalan 10 000 yoki 50 000 so'm), '❌ Kam (minimal 100 000 so'mga yetmaydi)' deb yozing.\n\n"
            "Chekdan aniq ko'ringan faktlarni oling va qisqa (4-5 qator) o'zbek tilida xulosa bering:\n"
            "• Qabul qiluvchi: [Karta/Ism va mosligi: ✅ Mos / ❌ Noto'g'ri]\n"
            "• Summa: [Chekdagi aniq summa va yetarliligi: ✅ Yetarli / ❌ Kam]\n"
            "• Sana/vaqt: [Chekdagi sana va vaqt]\n"
            "• Holat: [Muvaffaqiyatli / Kutilmoqda / Noma'lum]\n"
            "• Xulosa: [✅ Rekvizitlar to‘g‘ri yoki ⚠️ Mos emas/shubhali]\n\n"
            "MUHIM: Agar rasm to'lov cheki bo'lmasa (masalan: grafik, jadval, selfi, chat skrinshoti, noaniq rasm yoki to'lovga aloqasiz boshqa fayl bo'lsa), Xulosa qismiga qat'iy ravishda: '⚠️ Bu to‘lov cheki emas: [sababi]' deb yozing.\n"
            "Juda lo'nda va aniq yozing."
        )

        payload = {
            "model": "default",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}}
                    ]
                }
            ],
            "stream": False
        }

        async with httpx.AsyncClient(timeout=40.0) as client:
            resp = await client.post(
                f"{HERMES_API_URL}/v1/responses",
                headers={
                    "Authorization": f"Bearer {HERMES_API_KEY}",
                    "X-Hermes-Session-Key": "receipt_vision_audit",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        analysis_text = ""
        for item in data.get("output", []):
            if item.get("type") == "message" and item.get("role") == "assistant":
                for part in item.get("content", []):
                    if part.get("type") == "output_text":
                        analysis_text += part.get("text", "")

        return extract_clean_text(analysis_text.strip())
    except Exception as e:
        logger.error(f"Error analyzing receipt with vision: {e}")
        return ""

def classify_receipt_analysis(analysis: str) -> dict:
    if not analysis:
        return {"is_valid": True, "reject_reason": None, "reply_text": None}

    APOS = r"[\x27\u2018\u2019\u02bb\u02bc\x60]?"

    # 1. Not a receipt at all (graph, table, chat screenshot, photo)
    not_receipt_pat = rf"(bu\s*(to{APOS}lov\s*)?chek(i)?\s*emas|to{APOS}lov\s*chek(i)?\s*emas|chek\s*emas|chek\s*ko{APOS}rinmadi|to{APOS}lov\s*chek(i)?\s*yo{APOS}q|chek\s*yo{APOS}q|to{APOS}lov\s*hujjati\s*emas|to{APOS}lov\s*ma{APOS}lumotlari\s*(ko{APOS}rinmadi|mavjud\s*emas)|grafik|jadval)"
    if re.search(not_receipt_pat, analysis, re.IGNORECASE):
        return {
            "is_valid": False,
            "reject_reason": "not_receipt",
            "reply_text": (
                "Kechirasiz, yuborilgan rasmda to‘lov cheki yoki bank o‘tkazmasi ma’lumotlari ko‘rinmadi 😊\n\n"
                "---\n"
                "Iltimos, bank ilovangizdan to‘lov muvaffaqiyatli amalga oshirilganini tasdiqlovchi chek skrinshotini yubora olasizmi? Rahmat!"
            )
        }

    # 2. Check recipient line
    rec_match = re.search(r"Qabul\s*qiluvchi:[^\n]+", analysis, re.IGNORECASE)
    rec_line = rec_match.group(0) if rec_match else ""
    wrong_recipient = ("❌" in rec_line) or bool(re.search(r"(noto['’`]?g['’`]?ri|mos\s*emas|boshqa|QR-Online|7918)", rec_line, re.IGNORECASE))

    # 3. Check amount line
    amt_match = re.search(r"Summa:[^\n]+", analysis, re.IGNORECASE)
    amt_line = amt_match.group(0) if amt_match else ""

    # Check if amount is valid (100k+, 880k qoldiq, 980k to'liq, or foreign currency)
    has_valid_amount = bool(
        re.search(
            r"(880\s*000|980\s*000|100\s*000|880k|980k|100k|qoldiq|6\s*760|39\s*000|7\s*200|82\s*\$|10\s*\$|1\s*000\s*rub|rubl|tenge|som|dollar)",
            amt_line,
            re.IGNORECASE,
        )
    )
    if not has_valid_amount:
        amt_raw_nums = re.findall(r"\b\d{1,3}(?:[\s\.,]\d{3})+\b|\b\d{5,9}\b", amt_line)
        for nr in amt_raw_nums:
            cleaned_nr = int(re.sub(r"[\s\.,]", "", nr))
            if cleaned_nr >= 100_000:
                has_valid_amount = True
                break

    insufficient_amount = False
    if not has_valid_amount:
        insufficient_amount = ("❌" in amt_line) and bool(
            re.search(r"(kam|yetarli\s*emas|100\s*000.*yetm|minimal.*emas)", amt_line, re.IGNORECASE)
        )

    # 4. Check conclusion / Xulosa
    xul_match = re.search(r"Xulosa:[^\n]+", analysis, re.IGNORECASE)
    xul_line = xul_match.group(0) if xul_match else ""
    suspicious_xulosa = bool(re.search(r"(⚠️|❌|mos\s*emas|shubhali|qabul\s*qilinmaydi|noto['’`]?g['’`]?ri)", xul_line, re.IGNORECASE))

    # If both receiver and amount are missing
    not_found_receiver = bool(re.search(rf"Qabul\s*qiluvchi:.*(ko{APOS}rinmadi|noma{APOS}lum|aniqlanmadi|yo{APOS}q)", analysis, re.IGNORECASE))
    not_found_amount = bool(re.search(rf"Summa:.*(ko{APOS}rinmadi|noma{APOS}lum|aniqlanmadi|yo{APOS}q)", analysis, re.IGNORECASE))
    if not_found_receiver and not_found_amount:
        return {
            "is_valid": False,
            "reject_reason": "not_receipt",
            "reply_text": (
                "Kechirasiz, yuborilgan rasmda to‘lov cheki yoki bank o‘tkazmasi ma’lumotlari ko‘rinmadi 😊\n\n"
                "---\n"
                "Iltimos, bank ilovangizdan to‘lov muvaffaqiyatli amalga oshirilganini tasdiqlovchi chek skrinshotini yubora olasizmi? Rahmat!"
            )
        }

    # Wrong recipient (e.g. QR-Online, different card)
    if wrong_recipient:
        return {
            "is_valid": False,
            "reject_reason": "wrong_recipient",
            "reply_text": (
                "Kechirasiz, yuborilgan chekda to‘lov bizning rasmiy hisobimizga emas, boshqa xizmatga o‘tkazilgani ko‘rinmoqda 😊\n\n"
                "---\n"
                "Iltimos, rasmiy hisobimizga o‘tkazilgan to‘lov chekini qayta yubora olasizmi? Rekvizitlar kerak bo‘lsa, mamnuniyat bilan yuboraman."
            )
        }

    # Insufficient amount (less than 100k UZS)
    if insufficient_amount:
        return {
            "is_valid": False,
            "reject_reason": "insufficient_amount",
            "reply_text": (
                "Kechirasiz, yuborilgan chekdagi summa joy band qilish uchun yetarli emasligi ko‘rinmoqda 😊 Joy band qilish uchun minimal to‘lov 100 000 so‘m.\n\n"
                "---\n"
                "Iltimos, to‘lovni to‘liq amalga oshirib, chekini yuborsangiz, mamnuniyat bilan joyingizni band qilib beramiz."
            )
        }

    # Suspicious / Mismatched
    if suspicious_xulosa and not ("✅" in xul_line):
        return {
            "is_valid": False,
            "reject_reason": "suspicious",
            "reply_text": (
                "Kechirasiz, yuborilgan chek ma’lumotlari rasmiy to‘lov talablarimizga to‘liq mos kelmadi 😊\n\n"
                "---\n"
                "Iltimos, rasmiy hisobimizga to‘g‘ri qilingan to‘lov chekini qayta yubora olasizmi?"
            )
        }

    return {"is_valid": True, "reject_reason": None, "reply_text": None}

def is_obviously_not_receipt(analysis: str) -> bool:
    classification = classify_receipt_analysis(analysis)
    return not classification["is_valid"]

GCP_SA_PATH = os.environ.get("GCP_SA_PATH", "/app/data/gcp-service-account.json")
_gcp_token_cache: dict[str, any] = {"token": None, "expires_at": 0}

async def get_vertex_access_token() -> str | None:
    now = time.time()
    if _gcp_token_cache["token"] and now < _gcp_token_cache["expires_at"] - 60:
        return _gcp_token_cache["token"]

    if not os.path.exists(GCP_SA_PATH):
        logger.warning(f"GCP service account credentials not found at {GCP_SA_PATH}")
        return None

    try:
        with open(GCP_SA_PATH, "r", encoding="utf-8") as f:
            sa = json.load(f)

        now_int = int(now)
        payload = {
            "iss": sa["client_email"],
            "sub": sa["client_email"],
            "aud": sa["token_uri"],
            "iat": now_int,
            "exp": now_int + 3600,
            "scope": "https://www.googleapis.com/auth/cloud-platform",
        }
        signed_jwt = jwt.encode(payload, sa["private_key"], algorithm="RS256")
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                sa["token_uri"],
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": signed_jwt,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            _gcp_token_cache["token"] = token
            _gcp_token_cache["expires_at"] = now + 3500
            return token
    except Exception as e:
        logger.error(f"Error obtaining GCP access token: {e}")
        return None

async def transcribe_voice_with_gemini(audio_bytes: bytes, mime_type: str = "audio/ogg") -> str | None:
    if not audio_bytes:
        return None
    token = await get_vertex_access_token()
    if not token:
        logger.error("Cannot transcribe audio: Vertex access token is missing")
        return None

    try:
        with open(GCP_SA_PATH, "r", encoding="utf-8") as f:
            sa = json.load(f)
        project_id = sa.get("project_id", "proauditmainkey2026")
        location = "us-central1"
        model = "gemini-2.5-flash"
        url = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{location}/publishers/google/models/{model}:generateContent"

        b64_audio = base64.b64encode(audio_bytes).decode("utf-8")
        body = {
            "contents": [{
                "role": "user",
                "parts": [
                    {
                        "inlineData": {
                            "mimeType": mime_type,
                            "data": b64_audio,
                        }
                    },
                    {
                        "text": (
                            "Ushbu audio/ovozli xabarni so‘zma-so‘z to‘liq transkripsiya qiling (Speech-to-Text). "
                            "Faqat gapirilgan matnni yozing. Hech qanday kirish so‘z, qo‘shimcha izoh yoki tarjima qo‘shmang."
                        )
                    }
                ]
            }]
        }

        async with httpx.AsyncClient(timeout=35.0) as client:
            resp = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=body,
            )
            resp.raise_for_status()
            res_data = resp.json()
            candidates = res_data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()
        return None
    except Exception as e:
        logger.error(f"Error transcribing audio with Gemini: {e}")
        return None

async def notify_admins_about_receipt(
    chat_id: int,
    user_name: str,
    user_handle: str,
    receipt_info: tuple[str, str, str],
    conn_id: str | None,
    precomputed_analysis: str = ""
):
    file_id, file_type, caption = receipt_info

    # 1. Run AI Vision analysis (if not already precomputed)
    ai_analysis_text = precomputed_analysis
    if not ai_analysis_text and (file_type == "photo" or (file_type == "document" and file_id)):
        try:
            logger.info(f"Downloading receipt {file_id} for AI Vision check...")
            image_bytes = await download_telegram_file(file_id)
            if image_bytes:
                logger.info(f"Analyzing receipt with Codex Vision ({len(image_bytes)} bytes)...")
                ai_analysis_text = await analyze_receipt_with_vision(image_bytes)
                if ai_analysis_text:
                    logger.info(f"Codex Vision analysis: {ai_analysis_text[:60]}...")
        except Exception as e:
            logger.warning(f"Could not perform AI vision check: {e}")

    receipt_id = save_receipt_record(chat_id, conn_id, user_name, user_handle, caption, ai_analysis_text)

    keyboard = None
    if receipt_id:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Tasdiqlayman", callback_data=f"rcpt:ok:{receipt_id}"),
                InlineKeyboardButton(text="❌ Pul tushmagan", callback_data=f"rcpt:no:{receipt_id}"),
            ]
        ])

    safe_name = html.escape(user_name)
    safe_handle = html.escape(user_handle)
    safe_caption = html.escape(caption) if caption else "Izohsiz chek"

    header = (
        f"🔔 <b>Yangi to'lov cheki keldi!</b>\n\n"
        f"👤 <b>Mijoz:</b> {safe_name} ({safe_handle})\n"
        f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"💬 <b>Mijoz izohi:</b> {safe_caption}"
    )

    analysis_block = ""
    if ai_analysis_text:
        safe_analysis = html.escape(ai_analysis_text.strip())
        analysis_block = f"\n\n🤖 <b>AI Chek tahlili (Codex Vision):</b>\n{safe_analysis}"

    footer = (
        f"\n\n👉 <i>Bank ilovasidan to'lovni tekshirib, quyidagi tugmalardan birini bosing:</i>\n"
        f"🔗 <a href='tg://user?id={chat_id}'>Mijoz profilini ochish</a>"
    )

    notif_text = f"{header}{analysis_block}{footer}"
    if len(notif_text) > 1000 and analysis_block:
        available_space = 1000 - len(header) - len(footer) - 45
        if available_space > 60:
            truncated = html.escape(ai_analysis_text[:available_space].strip()) + "..."
            analysis_block = f"\n\n🤖 <b>AI Chek tahlili (Codex Vision):</b>\n{truncated}"
            notif_text = f"{header}{analysis_block}{footer}"
        else:
            notif_text = f"{header}{footer}"

    for admin_id in ADMIN_CHATS:
        try:
            if RECEIPT_THREAD_ID:
                try:
                    if file_type == "photo":
                        await bot.send_photo(
                            chat_id=admin_id,
                            photo=file_id,
                            caption=notif_text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard,
                            message_thread_id=RECEIPT_THREAD_ID,
                        )
                    elif file_type == "document":
                        await bot.send_document(
                            chat_id=admin_id,
                            document=file_id,
                            caption=notif_text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard,
                            message_thread_id=RECEIPT_THREAD_ID,
                        )
                    else:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=notif_text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard,
                            message_thread_id=RECEIPT_THREAD_ID,
                        )
                    logger.info(f"Payment receipt alert delivered to admin {admin_id} in thread {RECEIPT_THREAD_ID} (receipt_id={receipt_id})")
                    continue
                except TelegramBadRequest as thread_err:
                    logger.warning(f"Failed to send receipt to thread {RECEIPT_THREAD_ID}, falling back to general: {thread_err}")

            if file_type == "photo":
                await bot.send_photo(
                    chat_id=admin_id,
                    photo=file_id,
                    caption=notif_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            elif file_type == "document":
                await bot.send_document(
                    chat_id=admin_id,
                    document=file_id,
                    caption=notif_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            else:
                await bot.send_message(
                    chat_id=admin_id,
                    text=notif_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            logger.info(f"Payment receipt alert delivered to admin {admin_id} (receipt_id={receipt_id})")
        except Exception as e:
            logger.warning(f"Could not deliver receipt notification to admin {admin_id}: {e}")

@dp.callback_query(F.data.startswith("rcpt:"))
async def on_receipt_callback(callback: CallbackQuery):
    if not callback.data:
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        return
    _, action, receipt_id_str = parts
    try:
        receipt_id = int(receipt_id_str)
    except ValueError:
        return

    receipt = get_receipt_record(receipt_id)
    if not receipt:
        await callback.answer("Chek ma'lumotlari topilmadi.", show_alert=True)
        return

    if receipt["status"] != "pending":
        status_label = "tasdiqlangan" if receipt["status"] == "confirmed" else "rad etilgan"
        await callback.answer(f"Bu chek allaqachon ko'rib chiqilgan ({status_label})!", show_alert=True)
        return

    manager_name = callback.from_user.full_name or "Menejer"
    manager_handle = f"@{callback.from_user.username}" if callback.from_user.username else f"id:{callback.from_user.id}"
    manager_info = f"{manager_name} ({manager_handle})"

    new_status = "confirmed" if action == "ok" else "rejected"
    update_receipt_record(receipt_id, new_status, manager_info)

    await callback.answer("Qabul qilindi!")

    # Update caption/text on the group message and remove buttons
    status_header = (
        f"✅ <b>To'lov tasdiqlandi!</b>\n👨‍💼 <b>Menejer:</b> {manager_info}"
        if action == "ok"
        else f"❌ <b>To'lov tushmagan deb belgilandi!</b>\n👨‍💼 <b>Menejer:</b> {manager_info}"
    )

    ai_note = receipt.get("ai_analysis") or ""
    ai_block = f"\n\n🤖 <b>AI Chek tahlili:</b>\n{html.escape(ai_note)}" if ai_note else ""

    caption_text = receipt.get("caption") or ""
    safe_caption = html.escape(caption_text) if caption_text else "Izohsiz chek"
    safe_name = html.escape(receipt['user_name'] or "User")
    safe_handle = html.escape(receipt['user_handle'] or "")

    base_caption = (
        f"👤 <b>Mijoz:</b> {safe_name} ({safe_handle})\n"
        f"🆔 <b>Chat ID:</b> <code>{receipt['chat_id']}</code>\n"
        f"💬 <b>Mijoz izohi:</b> {safe_caption}"
        f"{ai_block}\n\n"
        f"🔗 <a href='tg://user?id={receipt['chat_id']}'>Mijoz profilini ochish</a>"
    )
    final_caption = f"{status_header}\n\n{base_caption}"
    if len(final_caption) > 1000 and ai_block:
        final_caption = f"{status_header}\n\n👤 <b>Mijoz:</b> {safe_name} ({safe_handle})\n🆔 <b>Chat ID:</b> <code>{receipt['chat_id']}</code>\n\n🔗 <a href='tg://user?id={receipt['chat_id']}'>Mijoz profilini ochish</a>"

    try:
        if callback.message:
            if callback.message.photo or callback.message.document:
                await callback.message.edit_caption(
                    caption=final_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=None,
                )
            else:
                await callback.message.edit_text(
                    text=final_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=None,
                )
    except Exception as e:
        logger.warning(f"Could not edit message caption for receipt {receipt_id}: {e}")

    # Now continue conversation with customer via Hermes
    chat_id = receipt["chat_id"]
    conn_id = receipt["conn_id"]

    if action == "ok":
        system_instruction = (
            "[Tizim: Menejer bank hisobiga to'lov tushganini tasdiqladi. "
            "Mijozni real menejer uslubida qisqa va quvnoq tabriklang (masalan: '🎉 Urra! Tabriklayman! Siz ‘po-russki gaplashamiz 16.0’ kursiga chegirma bilan joy band qildingiz! 💫'). "
            "So'ng ism-familiyasi va telefon raqamini yozib yuborishini so'rang. "
            "Joy band qilish uchun to‘langan pul qaytarilmasligini eslatib o'ting.]"
        )
    else:
        system_instruction = (
            "[Tizim: Menejer bank hisobini tekshirdi, ammo to'lov hali hisobga kelib tushmagan. "
            "Mijozga buni muloyim va xushmuomalalik bilan tushuntiring, to'lov chekidagi ma'lumotlar va tranzaksiya holatini "
            "bank ilovasida qayta tekshirib ko'rishni iltimos qiling.]"
        )

    lock = chat_processing_lock.setdefault(chat_id, asyncio.Lock())
    async with lock:
        logger.info(f"Continuing conversation for chat {chat_id} after manager action: {new_status}")
        await query_hermes_and_reply(chat_id, conn_id, system_instruction)

async def notify_admins_about_escalation(
    chat_id: int,
    user_name: str,
    user_handle: str,
    reason: str,
    customer_message: str,
    conn_id: str | None = None,
    is_paused: bool = True,
    bot_reply: str = "",
):
    esc_id = save_escalation_record(chat_id, conn_id, user_name, user_handle, reason, customer_message)

    safe_name = html.escape(user_name)
    safe_handle = html.escape(user_handle)
    safe_reason = html.escape(reason)
    safe_msg = html.escape(customer_message[:500] + ("..." if len(customer_message) > 500 else ""))
    safe_bot_reply = html.escape(bot_reply[:400] + ("..." if len(bot_reply) > 400 else ""))

    if is_paused:
        alert_text = (
            f"🚨 <b>DIQQAT: Mijoz e’tirozi / Muammoli vaziyat! (Eskalatsiya)</b>\n\n"
            f"👤 <b>Mijoz:</b> {safe_name} ({safe_handle})\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n"
            f"⚠️ <b>Aniqlangan sabab:</b> <b>{safe_reason}</b>\n"
            f"💬 <b>Mijoz xabari:</b>\n"
            f"<i>\"{safe_msg}\"</i>\n\n"
            f"🛑 <b>Bot holati:</b> Ushbu chatda bot avtomatik to'xtatildi (pauzada). Menejer bevosita mijoz bilan muloqot qilishi mumkin.\n\n"
            f"👉 <i>Mijoz bilan shaxsan bog'laning:</i>\n"
            f"🔗 <a href='tg://user?id={chat_id}'>Mijoz profilini ochish</a>"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="▶️ Botni qayta yoqish", callback_data=f"escl:resume:{chat_id}:{esc_id}"),
                    InlineKeyboardButton(text="🛑 Bot pauzada", callback_data=f"escl:status:{chat_id}:{esc_id}")
                ]
            ]
        )
    else:
        bot_reply_block = f"\n🤖 <b>Bot javobi:</b>\n<i>\"{safe_bot_reply}\"</i>\n" if safe_bot_reply else ""
        alert_text = (
            f"ℹ️ <b>DIQQAT: Menejerdan ma'lumot talab etiladi! (Yangi savol)</b>\n\n"
            f"👤 <b>Mijoz:</b> {safe_name} ({safe_handle})\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n"
            f"❓ <b>Mavzu / Savol:</b> <b>{safe_reason}</b>\n"
            f"💬 <b>Mijoz xabari:</b>\n"
            f"<i>\"{safe_msg}\"</i>\n"
            f"{bot_reply_block}\n"
            f"🟢 <b>Bot holati:</b> Bot faol (mijoz boshqa savol bersa javob beraveradi). Menejer shaxsan yozganda bot avtomatik to'xtaydi.\n\n"
            f"👉 <i>Mijoz bilan shaxsan bog'laning:</i>\n"
            f"🔗 <a href='tg://user?id={chat_id}'>Mijoz profilini ochish</a>"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🛑 Botni to'xtatish (pauza)", callback_data=f"escl:pause:{chat_id}:{esc_id}"),
                    InlineKeyboardButton(text="🟢 Bot faol", callback_data=f"escl:status:{chat_id}:{esc_id}")
                ]
            ]
        )

    for admin_id in ADMIN_CHATS:
        try:
            if ESCALATION_THREAD_ID:
                try:
                    await bot.send_message(
                        chat_id=admin_id,
                        text=alert_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                        message_thread_id=ESCALATION_THREAD_ID,
                    )
                    logger.info(f"Escalation alert delivered to admin {admin_id} in thread {ESCALATION_THREAD_ID} (esc_id={esc_id}, chat={chat_id})")
                    continue
                except TelegramBadRequest as thread_err:
                    logger.warning(f"Failed to send escalation to thread {ESCALATION_THREAD_ID}, falling back to general: {thread_err}")

            await bot.send_message(
                chat_id=admin_id,
                text=alert_text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            logger.info(f"Escalation alert delivered to admin/group {admin_id} (esc_id={esc_id}, chat={chat_id})")
        except Exception as e:
            logger.error(f"Could not deliver escalation alert to {admin_id}: {e}")

@dp.callback_query(F.data.startswith("escl:"))
async def on_escalation_callback(callback: CallbackQuery):
    if not callback.data:
        return
    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer("Noto'g'ri so'rov", show_alert=True)
        return

    _, action, chat_id_str, esc_id_str = parts
    try:
        chat_id = int(chat_id_str)
        esc_id = int(esc_id_str)
    except ValueError:
        return

    admin_name = callback.from_user.full_name or "Menejer"
    admin_handle = f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)

    if action == "resume":
        set_chat_paused(chat_id, False)
        update_escalation_status(esc_id, "resumed", f"{admin_name} ({admin_handle})")
        await callback.answer("✅ Bot ushbu chatda qayta yoqildi!", show_alert=True)

        try:
            old_text = callback.message.html_text if callback.message else ""
            new_text = old_text + f"\n\n✅ <b>Bot qayta faollashtirildi:</b> {html.escape(admin_name)} ({html.escape(admin_handle)}) tomonidan."
            new_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="🛑 Botni to'xtatish", callback_data=f"escl:pause:{chat_id}:{esc_id}")
                    ]
                ]
            )
            await callback.message.edit_text(new_text, parse_mode=ParseMode.HTML, reply_markup=new_keyboard)
        except Exception as e:
            logger.warning(f"Failed to edit escalation message: {e}")

    elif action == "pause":
        set_chat_paused(chat_id, True, reason="Menejer tomonidan qo'lda to'xtatildi")
        update_escalation_status(esc_id, "paused_manual", f"{admin_name} ({admin_handle})")
        await callback.answer("🛑 Bot ushbu chatda to'xtatildi!", show_alert=True)
        try:
            old_text = callback.message.html_text if callback.message else ""
            new_text = old_text + f"\n\n🛑 <b>Bot qo'lda to'xtatildi:</b> {html.escape(admin_name)} ({html.escape(admin_handle)}) tomonidan."
            new_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="▶️ Botni qayta yoqish", callback_data=f"escl:resume:{chat_id}:{esc_id}")
                    ]
                ]
            )
            await callback.message.edit_text(new_text, parse_mode=ParseMode.HTML, reply_markup=new_keyboard)
        except Exception as e:
            logger.warning(f"Failed to edit escalation message: {e}")

    elif action == "status":
        is_paused = is_chat_paused(chat_id)
        status_msg = "🛑 Bot hozirda to'xtatilgan (pauzada). Menejer mijoz bilan bemalol yozisha oladi." if is_paused else "▶️ Bot hozirda faol."
        await callback.answer(status_msg, show_alert=True)

async def handle_burst_execution(chat_id: int, conn_id: str | None, user_info: tuple[str, str], text: str, receipt_info: tuple[str, str, str] | None, last_msg_id: int | None = None):
    user_name, user_handle = user_info
    lock = chat_processing_lock.setdefault(chat_id, asyncio.Lock())
    async with lock:
        if is_chat_paused(chat_id):
            logger.info(f"Chat {chat_id} is paused. Skipping burst execution.")
            return

        logger.info(f"Processing burst ({'Business' if conn_id else 'Direct'}) from {user_name} in chat {chat_id}:\n{text}")

        # Mark incoming message(s) as read before replying
        if conn_id and last_msg_id:
            try:
                await bot.read_business_message(
                    business_connection_id=conn_id,
                    chat_id=chat_id,
                    message_id=last_msg_id,
                )
                logger.info(f"Marked business messages as read up to {last_msg_id} in chat {chat_id}")
            except Exception as e:
                logger.warning(f"Could not mark business message as read: {e}")

        # Check for immediate escalation triggers (keywords)
        esc_reason = detect_escalation_keywords(text)
        if esc_reason:
            logger.warning(f"Immediate escalation detected in chat {chat_id} ({user_name}): {esc_reason}")
            set_chat_paused(chat_id, True, reason=esc_reason)
            reassurance = (
                "Sizni tushundim. Murojaatingizni zudlik bilan bosh menejerimizga yetkazdim. "
                "Masalangiz shaxsan menejer nazoratida o‘rganiladi va tez orada siz bilan bog‘lanishadi."
            )
            save_chat_message(chat_id, "customer", text)
            save_chat_message(chat_id, "manager", reassurance)
            await send_customer_message(chat_id, conn_id, reassurance)
            await notify_admins_about_escalation(chat_id, user_name, user_handle, esc_reason, text, conn_id)
            return

        # Check if an uploaded image/document is actually a receipt
        precomputed_analysis = ""
        if receipt_info:
            file_id, file_type, caption = receipt_info
            if file_type == "photo" or (file_type == "document" and file_id):
                try:
                    logger.info(f"Pre-validating receipt {file_id} with Vision for chat {chat_id}...")
                    image_bytes = await download_telegram_file(file_id)
                    if image_bytes:
                        precomputed_analysis = await analyze_receipt_with_vision(image_bytes)
                        if precomputed_analysis:
                            logger.info(f"Vision pre-check analysis:\n{precomputed_analysis}")
                            classification = classify_receipt_analysis(precomputed_analysis)
                            if not classification["is_valid"]:
                                reject_reason = classification["reject_reason"]
                                polite_reply = classification["reply_text"]
                                logger.info(f"Receipt rejected ({reject_reason}) for chat {chat_id}. Replying politely without admin alert.")
                                save_chat_message(chat_id, "customer", text)
                                save_chat_message(chat_id, "manager", polite_reply)
                                await send_customer_message(chat_id, conn_id, polite_reply)
                                # Per instruction: strictly do NOT notify admins or group in this situation
                                return
                except Exception as e:
                    logger.warning(f"Vision pre-check failed (falling back to standard flow): {e}")

        hermes_input_text = text
        if precomputed_analysis:
            hermes_input_text = (
                f"{text}\n\n"
                f"[Tizim: Mijoz to'lov cheki yubordi. Chek tahlili natijasi:\n{precomputed_analysis}\n"
                f"Eslatma: Chekdagi summani hisobga oling. Agar to'liq 980 000 so'm bo'lsa to'liq to'lov uchun rahmat ayting. "
                f"Agar qisman to'lov bo'lsa, qoldiq to'lovni 980 000 dan ayirib to'g'ri hisoblang (masalan: 400 000 to'langan bo'lsa, qoldiq 580 000). "
                f"Aslo ko'r-ko'rona '880 000 so'm qoldi' deb taxmin qilmang!]"
            )

        await query_hermes_and_reply(chat_id, conn_id, hermes_input_text, user_info=user_info)

        # If a payment receipt/screenshot was included in this burst, notify admins!
        if receipt_info:
            await notify_admins_about_receipt(chat_id, user_name, user_handle, receipt_info, conn_id, precomputed_analysis=precomputed_analysis)

async def debounce_worker(chat_id: int):
    try:
        while True:
            first_time = chat_first_message_time.get(chat_id, time.time())
            elapsed = time.time() - first_time
            remaining = max(0.5, min(DEBOUNCE_WAIT, MAX_BURST_WAIT - elapsed))
            
            if elapsed >= MAX_BURST_WAIT:
                break
                
            await asyncio.sleep(remaining)
            
            current_elapsed = time.time() - chat_first_message_time.get(chat_id, time.time())
            if current_elapsed >= MAX_BURST_WAIT or (time.time() - chat_first_message_time.get(chat_id, 0)) >= DEBOUNCE_WAIT:
                break

        # Flush buffer
        texts = chat_buffers.pop(chat_id, [])
        chat_first_message_time.pop(chat_id, None)
        conn_id = chat_connections.pop(chat_id, None)
        user_info = chat_users.pop(chat_id, ("User", ""))
        receipt_info = chat_receipts.pop(chat_id, None)
        last_msg_id = chat_last_message_ids.pop(chat_id, None)
        chat_tasks.pop(chat_id, None)

        if texts:
            combined_text = "\n".join(texts).strip()
            if combined_text:
                await handle_burst_execution(chat_id, conn_id, user_info, combined_text, receipt_info, last_msg_id)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"Unexpected error in debounce_worker for {chat_id}: {e}")

def buffer_incoming_message(chat_id: int, user_name: str, user_handle: str, text: str, conn_id: str | None, receipt_tuple: tuple | None, message_id: int | None = None):
    chat_buffers.setdefault(chat_id, []).append(text)
    chat_connections[chat_id] = conn_id
    chat_users[chat_id] = (user_name, user_handle)
    if receipt_tuple:
        chat_receipts[chat_id] = receipt_tuple
    if message_id:
        chat_last_message_ids[chat_id] = message_id

    # Immediate typing status for instant visual feedback to user
    try:
        if conn_id:
            asyncio.create_task(bot.send_chat_action(chat_id=chat_id, action="typing", business_connection_id=conn_id))
        else:
            asyncio.create_task(bot.send_chat_action(chat_id=chat_id, action="typing"))
    except Exception:
        pass

    now = time.time()
    if chat_id not in chat_first_message_time:
        chat_first_message_time[chat_id] = now

    existing_task = chat_tasks.get(chat_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()

    chat_tasks[chat_id] = asyncio.create_task(debounce_worker(chat_id))

@dp.business_message()
async def on_business_message(message: Message):
    conn_id = message.business_connection_id
    if not conn_id:
        return

    # Strict isolation: only process messages for the configured business owner
    conn = connections.get(conn_id)
    if not conn:
        try:
            conn = await bot.get_business_connection(business_connection_id=conn_id)
            if conn and conn.is_enabled:
                connections[conn_id] = conn
        except Exception as e:
            logger.warning(f"Could not fetch business connection {conn_id}: {e}")

    conn_owner_id = conn.user.id if (conn and conn.user) else None
    if conn_owner_id != BUSINESS_OWNER_ID:
        logger.info(
            f"Ignoring business message from connection {conn_id} (owner={conn_owner_id} != configured BUSINESS_OWNER_ID={BUSINESS_OWNER_ID})"
        )
        return

    chat_id = message.chat.id

    # Never reply to messages sent by the business owner (manager) - auto-pause bot when manager intervenes
    if message.from_user and message.from_user.id == BUSINESS_OWNER_ID:
        logger.info(f"Manager ({BUSINESS_OWNER_ID}) directly sent message in chat {chat_id}. Pausing bot for this chat.")
        set_chat_paused(chat_id, True, reason="Menejer bevosita yozdi")
        return

    user_name = message.from_user.full_name if message.from_user else "User"
    user_handle = f"@{message.from_user.username}" if message.from_user and message.from_user.username else f"id:{message.from_user.id}"
    
    caption = (message.caption or "").strip()
    raw_text = (message.text or "").strip()
    
    if raw_text.strip().lower() in ["/reset", "/tozalash"]:
        reset_chat_session(chat_id)
        set_chat_paused(chat_id, False)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text="🔄 Suhbat xotirasi tozalandi va bot qayta yoqildi. Suhbatni yangidan boshlashingiz mumkin.",
                business_connection_id=conn_id,
            )
        except Exception as e:
            logger.error(f"Failed to send reset message: {e}")
        return

    # Safe testing mode: Do not answer real business leads while testing
    if SAFE_TEST_MODE:
        try:
            await bot.read_business_message(
                business_connection_id=conn_id,
                chat_id=chat_id,
                message_id=message.message_id,
            )
        except Exception:
            pass
        logger.info(f"[SAFE_TEST_MODE ACTIVE] Business message from {user_name} ({chat_id}) ignored by bot. Manager handles manually in Telegram.")
        return

    # Check if chat is paused due to escalation or manager intervention
    if is_chat_paused(chat_id):
        try:
            await bot.read_business_message(
                business_connection_id=conn_id,
                chat_id=chat_id,
                message_id=message.message_id,
            )
        except Exception:
            pass
        logger.info(f"Chat {chat_id} is paused (escalation/manager takeover). Customer message ignored by bot.")
        return

    receipt_tuple = None
    if message.photo:
        file_id = message.photo[-1].file_id
        receipt_tuple = (file_id, "photo", caption)
        annotated_text = f"{caption}\n[Mijoz to'lov cheki skrinshotini (rasm) yubordi]".strip()
    elif message.document:
        mime = message.document.mime_type or ""
        if mime.startswith("image/") or mime == "application/pdf":
            file_id = message.document.file_id
            receipt_tuple = (file_id, "document", caption)
            annotated_text = f"{caption}\n[Mijoz to'lov cheki skrinshotini (hujjat) yubordi]".strip()
        else:
            annotated_text = raw_text
    elif message.voice or message.audio:
        audio_obj = message.voice or message.audio
        mime = "audio/ogg" if message.voice else (message.audio.mime_type or "audio/ogg")
        logger.info(f"Downloading audio/voice message from {user_name} (file_id: {audio_obj.file_id})...")
        audio_bytes = await download_telegram_file(audio_obj.file_id)
        if audio_bytes:
            transcription = await transcribe_voice_with_gemini(audio_bytes, mime_type=mime)
            if transcription:
                logger.info(f"Gemini transcribed voice from {user_name}: {transcription}")
                annotated_text = f"{caption}\n{transcription}".strip() if caption else transcription
            else:
                annotated_text = caption or ""
        else:
            annotated_text = caption or ""
    else:
        annotated_text = raw_text

    if not annotated_text:
        return

    logger.info(f"Business message from {user_name} ({user_handle}) in chat {chat_id}: {annotated_text[:80]}")
    buffer_incoming_message(chat_id, user_name, user_handle, annotated_text, conn_id=conn_id, receipt_tuple=receipt_tuple, message_id=message.message_id)

@dp.message(F.chat.type == "private")
async def on_direct_message(message: Message):
    # Ignore bot's own updates or non-user
    if message.from_user and message.from_user.is_bot:
        return

    chat_id = message.chat.id
    user_name = message.from_user.full_name if message.from_user else "User"
    user_handle = f"@{message.from_user.username}" if message.from_user and message.from_user.username else f"id:{message.from_user.id}"
    
    caption = (message.caption or "").strip()
    raw_text = (message.text or "").strip()

    if raw_text.strip().lower() in ["/reset", "/tozalash"]:
        reset_chat_session(chat_id)
        set_chat_paused(chat_id, False)
        await message.reply("🔄 Suhbat xotirasi tozalandi va bot qayta yoqildi. Suhbatni yangidan boshlashingiz mumkin.")
        return

    if raw_text == "/start":
        reset_chat_session(chat_id)
        set_chat_paused(chat_id, False)
        raw_text = "Salom, kurslar haqida ma'lumot bering"

    if is_chat_paused(chat_id):
        logger.info(f"Direct chat {chat_id} is paused (escalation/manager takeover). Customer message ignored by bot.")
        return

    receipt_tuple = None
    if message.photo:
        file_id = message.photo[-1].file_id
        receipt_tuple = (file_id, "photo", caption)
        annotated_text = f"{caption}\n[Mijoz to'lov cheki skrinshotini (rasm) yubordi]".strip()
    elif message.document:
        mime = message.document.mime_type or ""
        if mime.startswith("image/") or mime == "application/pdf":
            file_id = message.document.file_id
            receipt_tuple = (file_id, "document", caption)
            annotated_text = f"{caption}\n[Mijoz to'lov cheki skrinshotini (hujjat) yubordi]".strip()
        else:
            annotated_text = raw_text
    elif message.voice or message.audio:
        audio_obj = message.voice or message.audio
        mime = "audio/ogg" if message.voice else (message.audio.mime_type or "audio/ogg")
        logger.info(f"Downloading audio/voice message from {user_name} (file_id: {audio_obj.file_id})...")
        audio_bytes = await download_telegram_file(audio_obj.file_id)
        if audio_bytes:
            transcription = await transcribe_voice_with_gemini(audio_bytes, mime_type=mime)
            if transcription:
                logger.info(f"Gemini transcribed voice from {user_name}: {transcription}")
                annotated_text = f"{caption}\n{transcription}".strip() if caption else transcription
            else:
                annotated_text = caption or ""
        else:
            annotated_text = caption or ""
    else:
        annotated_text = raw_text

    if not annotated_text:
        return

    logger.info(f"Direct bot message from {user_name} ({user_handle}) in chat {chat_id}: {annotated_text[:80]}")
    buffer_incoming_message(chat_id, user_name, user_handle, annotated_text, conn_id=None, receipt_tuple=receipt_tuple, message_id=message.message_id)

async def main():
    logger.info("Initializing adapter DB...")
    init_db()
    logger.info(f"Starting Telegram Adapter (Direct + Business, admins={ADMIN_CHATS}, debounce={DEBOUNCE_WAIT}s, max_burst={MAX_BURST_WAIT}s)...")
    allowed_updates = ["business_connection", "business_message", "edited_business_message", "message", "callback_query"]
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot, allowed_updates=allowed_updates)

if __name__ == "__main__":
    asyncio.run(main())
