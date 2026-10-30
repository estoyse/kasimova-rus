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

# Dynamic Course Details from Markdown (Zero-restart Hot Reload)
COURSE_DETAILS_PATH = os.environ.get("COURSE_DETAILS_PATH", "/app/data/COURSE_DETAILS.md")

_cached_course_details: dict[str, any] = {}
_cached_course_details_mtime: float = 0.0

def get_course_details() -> dict[str, any]:
    global _cached_course_details, _cached_course_details_mtime

    defaults = {
        "course_name": "po-russki gaplashamiz 16.0",
        "original_price": 1_490_000,
        "full_price": 980_000,
        "min_deposit": 100_000,
        "start_date": "10-noyabr",
        "duration": "2 oy",
        "uzcard_number": "5614 6821 1260 5573",
        "uzcard_holder": "Zaynab Qosimova",
        "uzcard_bank": "Ipak yo'li bank",
        "visa_number": "4916 9903 1015 2035",
        "visa_holder": "Qosimova Zaynab",
        "visa_bank": "Uzum Bank",
        "visa_phone": "+998 97 021 16 60",
        "foreign_prices": "6 760 rubl / 39 000 tenge / 7 200 som / 82 $",
        "raw_markdown": "",
    }

    if not os.path.exists(COURSE_DETAILS_PATH):
        return defaults

    try:
        mtime = os.path.getmtime(COURSE_DETAILS_PATH)
        if _cached_course_details and mtime == _cached_course_details_mtime:
            return _cached_course_details

        with open(COURSE_DETAILS_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        details = dict(defaults)
        details["raw_markdown"] = content

        def extract_num(pattern: str, default_val: int) -> int:
            m = re.search(pattern, content, re.IGNORECASE)
            if m:
                cleaned = re.sub(r"[\s\.,]", "", m.group(1))
                if cleaned.isdigit():
                    return int(cleaned)
            return default_val

        def extract_str(pattern: str, default_val: str) -> str:
            m = re.search(pattern, content, re.IGNORECASE)
            if m:
                return m.group(1).strip()
            return default_val

        details["original_price"] = extract_num(r"Asl\s*narxi[:\*\s]+([\d\s\.,]+)", defaults["original_price"])
        details["full_price"] = extract_num(r"Predzapis\s*narxi[^:\n]*[:\*\s]+([\d\s\.,]+)", defaults["full_price"])
        details["min_deposit"] = extract_num(r"Joy\s*band\s*qilish[^:\n]*[:\*\s]+([\d\s\.,]+)", defaults["min_deposit"])
        details["start_date"] = extract_str(r"boshlanish\s*sanasi[:\*\s]+([^\n\*\)]+)", defaults["start_date"])
        details["duration"] = extract_str(r"davomiyligi[:\*\s]+([^\n\*\)]+)", defaults["duration"])

        uzc = extract_str(r"Uzcard[^\n]*\n[^\n]*Karta\s*raqami[:\*\s`]+([\d\s]+)", "")
        if uzc:
            details["uzcard_number"] = uzc
        vis = extract_str(r"VISA[^\n]*\n[^\n]*Karta\s*raqami[:\*\s`]+([\d\s]+)", "")
        if vis:
            details["visa_number"] = vis

        _cached_course_details = details
        _cached_course_details_mtime = mtime
        logger.info(f"Loaded dynamic course details from {COURSE_DETAILS_PATH} (full_price={details['full_price']:,}, date={details['start_date']})")
        return details
    except Exception as e:
        logger.error(f"Error reading {COURSE_DETAILS_PATH}: {e}")
        return defaults

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

        con.execute("""
            CREATE TABLE IF NOT EXISTS lead_profiles (
                chat_id INTEGER PRIMARY KEY,
                stage TEXT DEFAULT 'yangi_lead',
                student_level TEXT DEFAULT 'aniqlanmagan',
                materials_sent INTEGER DEFAULT 0,
                total_paid INTEGER DEFAULT 0,
                remaining_balance INTEGER DEFAULT 980000,
                payment_history TEXT DEFAULT '[]',
                notes TEXT DEFAULT '',
                summary TEXT DEFAULT '',
                created_at REAL,
                updated_at REAL
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
            con.execute("DELETE FROM lead_profiles WHERE chat_id = ?", (chat_id,))
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

def extract_amount_from_analysis(analysis: str) -> int:
    if not analysis:
        return 0
    amt_match = re.search(r"Summa:[^\n]+", analysis, re.IGNORECASE)
    if not amt_match:
        return 0
    amt_line = amt_match.group(0)
    raw_nums = re.findall(r"\b\d{1,3}(?:[\s\.,]\d{3})+\b|\b\d{5,9}\b", amt_line)
    for nr in raw_nums:
        val = int(re.sub(r"[\s\.,]", "", nr))
        if val >= 10_000:
            return val
    return 0

def get_or_create_lead_profile(chat_id: int) -> dict:
    try:
        with sqlite3.connect(DB_PATH) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM lead_profiles WHERE chat_id = ?", (chat_id,))
            row = cur.fetchone()
            if row:
                return dict(row)
            now = time.time()
            mat_sent = 1 if has_sent_course_materials(chat_id) else 0
            cd = get_course_details()
            cur.execute(
                """INSERT INTO lead_profiles 
                   (chat_id, stage, student_level, materials_sent, total_paid, remaining_balance, payment_history, notes, summary, created_at, updated_at)
                   VALUES (?, 'yangi_lead', 'aniqlanmagan', ?, 0, ?, '[]', '', '', ?, ?)""",
                (chat_id, mat_sent, cd["full_price"], now, now)
            )
            con.commit()
            cur.execute("SELECT * FROM lead_profiles WHERE chat_id = ?", (chat_id,))
            row = cur.fetchone()
            return dict(row) if row else {}
    except Exception as e:
        logger.error(f"Error getting/creating lead profile for {chat_id}: {e}")
        return {
            "chat_id": chat_id, "stage": "yangi_lead", "student_level": "aniqlanmagan",
            "materials_sent": 0, "total_paid": 0, "remaining_balance": get_course_details()["full_price"],
            "payment_history": "[]", "notes": "", "summary": ""
        }

def record_payment_to_lead_profile(chat_id: int, new_amount: int, date_str: str = "") -> dict:
    try:
        now = time.time()
        profile = get_or_create_lead_profile(chat_id)
        try:
            history = json.loads(profile.get("payment_history") or "[]")
        except Exception:
            history = []

        history.append({
            "amount": new_amount,
            "timestamp": now,
            "date": date_str or time.strftime("%d-%b %H:%M")
        })

        total_paid = sum(item["amount"] for item in history if isinstance(item, dict) and "amount" in item)
        cd = get_course_details()
        remaining_balance = max(0, cd["full_price"] - total_paid)

        if remaining_balance == 0:
            stage = "to_liq_to_langan"
        elif total_paid >= cd["min_deposit"]:
            stage = "joy_band_qilingan"
        else:
            stage = "qisman_to_langan"

        summary = f"Jami {total_paid:,} so'm to'langan. Qoldiq: {remaining_balance:,} so'm."

        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                """UPDATE lead_profiles 
                   SET stage = ?, total_paid = ?, remaining_balance = ?, payment_history = ?, summary = ?, updated_at = ?
                   WHERE chat_id = ?""",
                (stage, total_paid, remaining_balance, json.dumps(history), summary, now, chat_id)
            )
            con.commit()

        return get_or_create_lead_profile(chat_id)
    except Exception as e:
        logger.error(f"Error recording payment to lead profile for {chat_id}: {e}")
        return get_or_create_lead_profile(chat_id)

def update_lead_level_and_stage(
    chat_id: int, 
    level: str | None = None, 
    stage: str | None = None, 
    materials_sent: bool | None = None, 
    notes: str | None = None
):
    try:
        now = time.time()
        profile = get_or_create_lead_profile(chat_id)
        updates = []
        params = []

        if level and profile.get("student_level") in ["aniqlanmagan", "", None]:
            updates.append("student_level = ?")
            params.append(level)
        if stage:
            updates.append("stage = ?")
            params.append(stage)
        if materials_sent is not None:
            updates.append("materials_sent = ?")
            params.append(1 if materials_sent else 0)
        if notes:
            existing = profile.get("notes") or ""
            combined = (existing + "; " + notes).strip("; ") if existing else notes
            updates.append("notes = ?")
            params.append(combined[:500])

        if updates:
            updates.append("updated_at = ?")
            params.append(now)
            params.append(chat_id)
            sql = f"UPDATE lead_profiles SET {', '.join(updates)} WHERE chat_id = ?"
            with sqlite3.connect(DB_PATH) as con:
                con.execute(sql, params)
                con.commit()
    except Exception as e:
        logger.error(f"Error updating lead profile for {chat_id}: {e}")

def build_lead_context_header(chat_id: int, incoming_analysis: str = "") -> str:
    cd = get_course_details()
    full_price = cd["full_price"]
    orig_price = cd["original_price"]
    min_deposit = cd["min_deposit"]
    start_date = cd["start_date"]

    profile = get_or_create_lead_profile(chat_id)
    mat_sent = bool(profile.get("materials_sent", 0)) or has_sent_course_materials(chat_id)
    level = profile.get("student_level", "aniqlanmagan")
    total_paid = profile.get("total_paid", 0)
    rem_balance = profile.get("remaining_balance", full_price)
    notes = profile.get("notes", "")

    lines = ["[MIJOZNING OLDINGI KONTEKSTI VA MOLIYAVIY HISOBOTI]"]
    lines.append(f"- Rasmiy kurs parametrlari: To'liq narx {full_price:,} so'm, minimal joy bandlov {min_deposit:,} so'm, darslar boshlanishi {start_date}.")

    if level and level != "aniqlanmagan":
        lines.append(f"- Darajasi: {level} (aniqlangan, qaytadan darajasini so'rash shart emas)")
    else:
        lines.append("- Darajasi: hali aniqlanmagan")

    lines.append(f"- Dars namunalari (kitob va video): {'Yuborilgan ✅' if mat_sent else 'Hali yuborilmagan'}")

    if incoming_analysis:
        if not is_image_payment_receipt(incoming_analysis):
            lines.append("- MIJOZ YUBORGAN RASM: Ushbu rasm to'lov cheki emas (oddiy rasm/skrinshot). Mijoz savoli yoki rasm mazmuniga qarab samimiy va insoniy javob bering.")
        else:
            rec_match = re.search(r"Qabul\s*qiluvchi:[^\n]+", incoming_analysis, re.IGNORECASE)
            rec_line = rec_match.group(0) if rec_match else ""
            wrong_recipient = ("❌" in rec_line) or bool(re.search(r"(noto['’`]?g['’`]?ri|mos\s*emas|boshqa)", rec_line, re.IGNORECASE))

            new_payment_amt = extract_amount_from_analysis(incoming_analysis)

            if wrong_recipient:
                lines.append("- TO'LOV CHEKI TAHLILI (OGOHLANTIRISH):")
                lines.append(f"  * Qabul qiluvchi: Rasmiy hisobimizga ({cd['uzcard_holder']}ga) mos EMAS!")
                lines.append("  * Qat'iy ko'rsatma: To'lov bizning hisobga tushmaganini xushmuomalalik bilan tushuntiring va rasmiy rekvizitlarimizga to'lov qilib chek yuborishini so'rang.")
            elif new_payment_amt > 0:
                updated_profile = record_payment_to_lead_profile(chat_id, new_payment_amt)
                tot_paid = updated_profile.get("total_paid", new_payment_amt)
                new_rem = updated_profile.get("remaining_balance", max(0, full_price - tot_paid))
                prev_paid = tot_paid - new_payment_amt

                lines.append("- TO'LOV CHEKI TAHLILI (Yangi to'lov keldi):")
                lines.append(f"  * Yangi to'lov summasi: {new_payment_amt:,} so'm")
                if prev_paid > 0:
                    lines.append(f"  * Avvalgi to'lovlar summasi: {prev_paid:,} so'm")
                lines.append(f"  * Jami to'langan summa: {tot_paid:,} so'm")
                lines.append(f"  * Kursning to'liq narxi: {full_price:,} so'm")
                lines.append(f"  * Qoldiq to'lov: {new_rem:,} so'm")
                if new_rem <= 0:
                    lines.append(f"  * Qat'iy ko'rsatma: Kurs uchun to'lov TO'LIQ ({full_price:,} so'm) amalga oshirildi! Samimiy minnatdorchilik bildiring va {start_date} atrofida guruh havolasi berilishini ayting.")
                else:
                    if prev_paid > 0:
                        lines.append(f"  * Qat'iy ko'rsatma: Yangi {new_payment_amt:,} so'mlik to'lov qabul qilinganini ayting. Avvalgi to'lovlari ({prev_paid:,} so'm) bilan birga jami {tot_paid:,} so'm to'langanini va qoldiq {new_rem:,} so'm qolganini ({start_date}gacha to'lashi mumkinligini) aniq bildiring.")
                    else:
                        lines.append(f"  * Qat'iy ko'rsatma: {new_payment_amt:,} so'm uchun rahmat aytib, joyi band qilinganini va qoldiq {new_rem:,} so'mni {start_date}gacha to'lashi mumkinligini bildiring.")
            else:
                lines.append("- TO'LOV CHEKI TAHLILI:")
                lines.append(f"  * Chek tafsilotlari:\n{incoming_analysis}")
                lines.append(f"  * Qat'iy ko'rsatma: Chekdagi faktlarga qarab muloyim javob bering. Agar summa {min_deposit:,} so'mdan kam bo'lsa, joy band qilish uchun minimal summa {min_deposit:,} so'm ekanini bildiring.")
    elif total_paid > 0:
        lines.append(f"- Moliyaviy holat: Avval jami {total_paid:,} so'm to'langan, qoldiq {rem_balance:,} so'm (kurs narxi {full_price:,} so'm).")
    else:
        lines.append(f"- Moliyaviy holat: Hali to'lov qilinmagan (asl narx: {orig_price:,} so'm, predzapis narxi: {full_price:,} so'm, joy band qilish: {min_deposit:,} so'm).")

    if notes:
        lines.append(f"- Muhim qaydlar: {notes}")

    return "\n".join(lines)

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

# Legacy escalation keywords removed - escalation is strictly decided by Hermes LLM

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

# Outdated COURSE_ANNOUNCEMENT_TEXT removed - flow is managed dynamically by Hermes LLM

BOOK_1_FILE_ID = "BQACAgIAAxkDAAEFtcxq4bBzPoKCZte0cJtcdqa250qRlwAC_IsAAqdxsUv2StWhBputRT0E"
BOOK_2_FILE_ID = "BQACAgIAAxkDAAEFtc9q4bBzrTWslYpFnoBSqTZ1cqjsrgACD4wAAqdxsUugHVgccrCOWT0E"
BOOK_2_CAPTION = (
    "online kursimizda o'tiladigan kitoblardan namuna⚡️\n"
    "[Respublika Ta'lim markazidan o'zbeklarga rus tilini o'rgatish metodikasida tuzilganligi tasdiqdan o'tgan☺️]"
)
VIDEO_SAMPLE_FILE_ID = "BAACAgIAAxkDAAEFtdFq4bBzhQ9yiUL_n7YR-UROlpZNOAAC3YMAAixCmEsAAUsTwDrSkAc9BA"
VIDEO_SAMPLE_CAPTION = (
    "⚡️VIDEODARSLIK\n\n"
    "— «porusski-gaplashamiz» online kursidagi videodarsliklardan namuna ✅\n\n"
    "Mavzu: Rodlar✅"
)

async def send_course_materials_to_customer(chat_id: int, conn_id: str | None = None):
    logger.info(f"Delivering official course materials package (PDF books + Video sample) to chat {chat_id} (conn_id={conn_id})...")

    # 1. Send the two book PDFs as an album
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

    # 1. Parse LLM student level tag if emitted: [LEVEL: ...]
    level_match = re.search(r"\[(?:SET_)?LEVEL:\s*([^\]]+)\]", reply_text, re.IGNORECASE)
    if level_match:
        lead_level = level_match.group(1).strip()
        reply_text = re.sub(r"\[(?:SET_)?LEVEL:\s*[^\]]+\]", "", reply_text).strip()
        update_lead_level_and_stage(chat_id, level=lead_level, stage="daraja_aniqlangan")
        logger.info(f"Student level updated by Hermes for chat {chat_id}: {lead_level}")

    # 2. Check if Hermes flagged an escalation via [ESCALATE: ...]
    esc_match = re.search(r"\[ESCALATE:\s*([^\]]+)\]", reply_text, re.IGNORECASE)
    if esc_match:
        llm_reason = esc_match.group(1).strip()
        reply_text = re.sub(r"\[ESCALATE:\s*[^\]]+\]", "", reply_text).strip()
        logger.warning(f"Hermes signaled escalation for chat {chat_id}: {llm_reason}")
        set_chat_paused(chat_id, True, reason=llm_reason)
        user_name, user_handle = user_info
        asyncio.create_task(
            notify_admins_about_escalation(
                chat_id, user_name, user_handle, llm_reason, prompt, conn_id, is_paused=True, bot_reply=reply_text
            )
        )

    # 3. Check course materials trigger: [SEND_COURSE_MATERIALS]
    send_materials_triggered = "[SEND_COURSE_MATERIALS]" in reply_text
    reply_text = reply_text.replace("[SEND_COURSE_MATERIALS]", "").strip()

    if send_materials_triggered:
        update_lead_level_and_stage(chat_id, materials_sent=True, stage="materiallar_yuborilgan")

    logger.info(f"Hermes response for chat {chat_id}: {reply_text[:80]}")

    # Save messages to local SQLite store
    if not is_system_instruction:
        save_chat_message(chat_id, "customer", prompt)
    save_chat_message(chat_id, "manager", reply_text)

    await send_customer_message(chat_id, conn_id, reply_text)

    # Deliver course materials strictly when Hermes commanded
    if send_materials_triggered and not has_sent_course_materials(chat_id):
        logger.info(f"Triggering course materials dispatch for {chat_id}")
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

        cd = get_course_details()

        prompt = (
            "Siz to'lov cheklarini tekshiruvchi professional nazoratchisiz. "
            "Ushbu rasmda yuborilgan to'lov chekini sinchiklab tekshiring va rasmiy rekvizitlarimizga mosligini baholang:\n\n"
            "Rasmiy rekvizitlarimiz:\n"
            f"1. O'zbekiston (Uzcard): {cd['uzcard_number']} ({cd['uzcard_holder']}, {cd['uzcard_bank']})\n"
            f"   - Minimal joy band qilish to'lovi: {cd['min_deposit']:,} UZS\n"
            f"   - To'liq 2 oylik kurs narxi: {cd['full_price']:,} UZS\n"
            f"   - Bo'lib to'lash: har qanday {cd['min_deposit']:,} UZS dan yuqori summa to'liq yoki qisman to'lov sifatida ✅ Yetarli deb baholansin!\n"
            f"2. Xorijiy (VISA): {cd['visa_number']} ({cd['visa_holder']}, {cd['visa_bank']})\n"
            f"   - Ulangan telefon: {cd['visa_phone']}\n"
            f"   - Chet el narxlari: {cd['foreign_prices']}\n\n"
            "MUHIM SUMMA QOIDALARI:\n"
            f"- Agar summa {cd['min_deposit']:,} UZS yoki undan ko'p bo'lsa, aslo '❌ Kam' demang! '✅ Yetarli (qoldiq / to'lov)' deb belgilang.\n"
            f"- FAQAT chekdagi summa {cd['min_deposit']:,} UZS dan KAM bo'lsagina, '❌ Kam (minimal {cd['min_deposit']:,} so'mga yetmaydi)' deb yozing.\n\n"
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

def is_image_payment_receipt(analysis: str) -> bool:
    if not analysis:
        return False

    APOS = r"[\x27\u2018\u2019\u02bb\u02bc\x60]?"
    not_receipt_pat = rf"(bu\s*(to{APOS}lov\s*)?chek(i)?\s*emas|to{APOS}lov\s*chek(i)?\s*emas|chek\s*emas|chek\s*ko{APOS}rinmadi|to{APOS}lov\s*chek(i)?\s*yo{APOS}q|chek\s*yo{APOS}q|to{APOS}lov\s*hujjati\s*emas|to{APOS}lov\s*ma{APOS}lumotlari\s*(ko{APOS}rinmadi|mavjud\s*emas)|grafik|jadval)"
    if re.search(not_receipt_pat, analysis, re.IGNORECASE):
        return False

    not_found_receiver = bool(re.search(rf"Qabul\s*qiluvchi:.*(ko{APOS}rinmadi|noma{APOS}lum|aniqlanmadi|yo{APOS}q)", analysis, re.IGNORECASE))
    not_found_amount = bool(re.search(rf"Summa:.*(ko{APOS}rinmadi|noma{APOS}lum|aniqlanmadi|yo{APOS}q)", analysis, re.IGNORECASE))
    if not_found_receiver and not_found_amount:
        return False

    return True

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

        # Check if an uploaded image/document is actually a receipt
        precomputed_analysis = ""
        is_actual_receipt = False
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
                            is_actual_receipt = is_image_payment_receipt(precomputed_analysis)
                except Exception as e:
                    logger.warning(f"Vision pre-check failed (falling back to standard flow): {e}")

        # Build context header with running lead profile & cumulative financial ledger (strictly LLM-driven)
        lead_context_header = build_lead_context_header(chat_id, incoming_analysis=precomputed_analysis)
        hermes_input_text = f"{lead_context_header}\n\n[Mijoz xabari]:\n{text}"

        await query_hermes_and_reply(chat_id, conn_id, hermes_input_text, user_info=user_info)

        # If an actual payment receipt was included in this burst, notify admins!
        if receipt_info and is_actual_receipt:
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
