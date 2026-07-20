import asyncio
import logging
import re
import math
from datetime import datetime, timedelta
import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import Message, ChatPermissions, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

logging.basicConfig(level=logging.INFO)

import os
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAX_WARNS = 5
WARN_BAN_DURATION = 30 * 24 * 60 * 60
WARN_EXPIRE_DAYS = 7
KICK_DURATION = 30
DB_FILE = "bot.db"

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS warns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    chat_id INTEGER,
    admin_id INTEGER,
    reason TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_warns_user_chat ON warns(user_id, chat_id);
CREATE INDEX IF NOT EXISTS idx_warns_chat ON warns(chat_id);
CREATE TABLE IF NOT EXISTS mutes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    chat_id INTEGER,
    until TEXT,
    reason TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_mutes_user_chat ON mutes(user_id, chat_id);
CREATE INDEX IF NOT EXISTS idx_mutes_chat ON mutes(chat_id);
CREATE TABLE IF NOT EXISTS bans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    chat_id INTEGER,
    until TEXT,
    reason TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_bans_chat ON bans(chat_id);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    chat_id INTEGER,
    username TEXT,
    first_name TEXT,
    text_length INTEGER DEFAULT 0,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_user_chat ON messages(user_id, chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at);
"""

async def db_init():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript(DB_SCHEMA)
        await db.commit()
    logging.info("БД готова")

async def db_add_warn(user_id, chat_id, admin_id, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO warns (user_id, chat_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?, ?)", (user_id, chat_id, admin_id, reason, datetime.now().isoformat()))
        await db.commit()

async def db_get_warns(user_id, chat_id):
    expire = (datetime.now() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT COUNT(*) FROM warns WHERE user_id=? AND chat_id=? AND created_at>=?", (user_id, chat_id, expire))
        row = await cur.fetchone()
        return row[0] if row else 0

async def db_get_warns_info(user_id, chat_id):
    expire = (datetime.now() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT reason, admin_id, created_at FROM warns WHERE user_id=? AND chat_id=? AND created_at>=? ORDER BY created_at DESC", (user_id, chat_id, expire))
        return await cur.fetchall()

async def db_remove_warn(user_id, chat_id, warn_id=None):
    expire = (datetime.now() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        if warn_id:
            await db.execute("DELETE FROM warns WHERE id=? AND user_id=? AND chat_id=?", (warn_id, user_id, chat_id))
        else:
            cur = await db.execute("SELECT id FROM warns WHERE user_id=? AND chat_id=? AND created_at>=? ORDER BY created_at DESC LIMIT 1", (user_id, chat_id, expire))
            row = await cur.fetchone()
            if row:
                await db.execute("DELETE FROM warns WHERE id=?", (row[0],))
        await db.commit()

async def db_clear_warns(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM warns WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()

async def db_add_mute(user_id, chat_id, until, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO mutes (user_id, chat_id, until, reason, created_at) VALUES (?, ?, ?, ?, ?)", (user_id, chat_id, until, reason, datetime.now().isoformat()))
        await db.commit()

async def db_get_mute(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT until, reason FROM mutes WHERE user_id=? AND chat_id=? AND until>?", (user_id, chat_id, datetime.now().isoformat()))
        return await cur.fetchone()

async def db_remove_mute(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM mutes WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()

async def db_add_ban(user_id, chat_id, until, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO bans (user_id, chat_id, until, reason, created_at) VALUES (?, ?, ?, ?, ?)", (user_id, chat_id, until, reason, datetime.now().isoformat()))
        await db.commit()

async def db_get_ban(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT until, reason FROM bans WHERE user_id=? AND chat_id=? AND until>?", (user_id, chat_id, datetime.now().isoformat()))
        return await cur.fetchone()

async def db_remove_ban(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM bans WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()

async def db_log_message(user_id, chat_id, username, first_name, text_length):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO messages (user_id, chat_id, username, first_name, text_length, created_at) VALUES (?, ?, ?, ?, ?, ?)", (user_id, chat_id, username, first_name, text_length, datetime.now().isoformat()))
        await db.commit()

async def db_count_active_users(chat_id, days=7):
    since = (datetime.now() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=? AND created_at>=?", (chat_id, since))
        row = await cur.fetchone()
        return row[0] if row else 0

async def db_top_users(chat_id, days=7, limit=5):
    since = (datetime.now() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT first_name, username, COUNT(*) as cnt FROM messages WHERE chat_id=? AND created_at>=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?", (chat_id, since, limit))
        return await cur.fetchall()

async def db_count_active_users_all(chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        return row[0] if row else 0

async def db_top_users_all(chat_id, limit=5):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT first_name, username, COUNT(*) as cnt FROM messages WHERE chat_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?", (chat_id, limit))
        return await cur.fetchall()

async def get_currency_rates():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                rates = data.get("rates", {})
                result = {}
                if "RUB" in rates:
                    result["RUB"] = rates["RUB"]
                if "EUR" in rates:
                    result["EUR"] = 1 / rates["EUR"]
                if "UAH" in rates:
                    result["UAH"] = rates["UAH"]
                if "TRY" in rates:
                    result["TRY"] = rates["TRY"]
                if "KZT" in rates:
                    result["KZT"] = rates["KZT"]
                if "BYN" in rates:
                    result["BYN"] = rates["BYN"]
                if "GBP" in rates:
                    result["GBP"] = 1 / rates["GBP"]
                if "CNY" in rates:
                    result["CNY"] = rates["CNY"]
                return result
    except Exception as e:
        logging.error(f"Ошибка валют: {e}")
        return None

async def get_crypto_rates():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,toncoin,solana,binancecoin,ripple,dogecoin,tron&vs_currencies=usd", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                result = {}
                if "bitcoin" in data:
                    result["BTC"] = data["bitcoin"]["usd"]
                if "ethereum" in data:
                    result["ETH"] = data["ethereum"]["usd"]
                if "toncoin" in data:
                    result["TON"] = data["toncoin"]["usd"]
                if "solana" in data:
                    result["SOL"] = data["solana"]["usd"]
                if "binancecoin" in data:
                    result["BNB"] = data["binancecoin"]["usd"]
                if "ripple" in data:
                    result["XRP"] = data["ripple"]["usd"]
                if "dogecoin" in data:
                    result["DOGE"] = data["dogecoin"]["usd"]
                if "tron" in data:
                    result["TRX"] = data["tron"]["usd"]
                return result
    except Exception as e:
        logging.error(f"Ошибка крипты: {e}")
        return None

class LogMessagesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, Message) and event.from_user and event.chat.type != ChatType.PRIVATE:
                await db_log_message(event.from_user.id, event.chat.id, event.from_user.username or "", event.from_user.first_name or "", len(event.text or ""))
        except Exception as e:
            logging.error(f"middleware error: {e}")
        return await handler(event, data)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
router.message.middleware(LogMessagesMiddleware())
dp.include_router(router)

async def is_admin(message: Message):
    if message.from_user.id == message.chat.id:
        return True
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False

def parse_time(text):
    m = re.search(r"(\d+)\s*(m|min|м|мин|h|ч|d|д|s|сек|sec)", text, re.IGNORECASE)
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2).lower()
    if unit in ("m", "min", "м", "мин"):
        return timedelta(minutes=val)
    if unit in ("h", "ч"):
        return timedelta(hours=val)
    if unit in ("d", "д"):
        return timedelta(days=val)
    if unit in ("s", "сек", "sec"):
        return timedelta(seconds=val)
    return None
@router.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.reply("👋 Привет! Я бот-модератор.\n\nКоманды:\n/mute, /unmute, /ban, /unban, /warn, /unwarn, /kick\n/report, /id, /stats, /currency, /calculator, /help\n\n/currency — фиат + крипто\n/stats [7/30/all] — статистика")

@router.message(F.text == "/help")
async def cmd_help(message: Message):
    await message.reply(
        "📖 <b>Команды бота</b>\n\n"
        "👮 <b>Модерация</b> (админы):\n"
        "/mute 1h причина — замутить (реплай)\n"
        "/unmute — размутить (реплай)\n"
        "/ban 1d причина — забанить (реплай)\n"
        "/unban — разбанить (реплай)\n"
        "/warn причина — варн (реплай)\n"
        "/wwarns — список варнов (реплай)\n"
        "/unwarn — снять варн (реплай)\n"
        "/kick — кикнуть (реплай)\n\n"
        "📊 <b>Инфо</b>:\n"
        "/report — вызвать админов (реплай)\n"
        "/id — узнать ID\n"
        "/stats 7|30|all — статистика чата\n"
        "/currency — фиат + крипто\n"
        "/calculator 2+2*3 — калькулятор\n"
        "/help — эта справка",
        parse_mode="HTML"
    )

@router.message(F.text.startswith("/mute"))
async def cmd_mute(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    duration = parse_time(message.text)
    if not duration:
        await message.reply("Укажи время: /mute 1h, /mute 30m, /mute 1d")
        return
    if duration.total_seconds() > 366 * 24 * 3600:
        await message.reply("Максимум 1 год")
        return
    until_dt = datetime.now() + duration
    reason = message.text.split(maxsplit=2)[2] if len(message.text.split()) > 2 else "Без причины"
    try:
        await bot.restrict_chat_member(message.chat.id, user.id, ChatPermissions(), until_date=until_dt)
        await db_add_mute(user.id, message.chat.id, until_dt.isoformat(), reason)
        await message.reply(f"🔇 @{user.username or user.first_name} замучен на {duration}\nПричина: {reason}")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")

@router.message(F.text.startswith("/unmute"))
async def cmd_unmute(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    try:
        await bot.restrict_chat_member(message.chat.id, user.id, ChatPermissions(can_send_messages=True, can_send_photos=True, can_send_videos=True, can_send_audios=True, can_send_documents=True, can_send_voice_notes=True, can_send_video_notes=True, can_send_other_messages=True, can_add_web_page_previews=True, can_change_info=False, can_invite_users=True, can_pin_messages=False, can_manage_topics=False))
        await db_remove_mute(user.id, message.chat.id)
        await message.reply(f"🔊 @{user.username or user.first_name} размучен")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")

@router.message(F.text.startswith("/ban"))
async def cmd_ban(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    duration = parse_time(message.text)
    until_dt = None
    if duration:
        if duration.total_seconds() > 366 * 24 * 3600:
            await message.reply("Максимум 1 год")
            return
        until_dt = datetime.now() + duration
    reason = message.text.split(maxsplit=2)[2] if len(message.text.split()) > 2 else "Без причины"
    try:
        await bot.ban_chat_member(message.chat.id, user.id, until_date=until_dt)
        await db_add_ban(user.id, message.chat.id, until_dt.isoformat() if until_dt else "forever", reason)
        txt = f"🚫 @{user.username or user.first_name} забанен"
        if until_dt:
            txt += f" на {duration}"
        txt += f"\nПричина: {reason}"
        await message.reply(txt)
    except Exception as e:
        await message.reply(f"Ошибка: {e}")

@router.message(F.text.startswith("/unban"))
async def cmd_unban(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    args = message.text.split()
    user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        user_id = message.reply_to_message.from_user.id
    elif len(args) > 1 and args[1].isdigit():
        user_id = int(args[1])
    if not user_id:
        await message.reply("Укажи пользователя")
        return
    try:
        await bot.unban_chat_member(message.chat.id, user_id, only_if_banned=True)
        await db_remove_ban(user_id, message.chat.id)
        await message.reply(f"✅ Разбанен")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")

@router.message(F.text.startswith("/warn"))
async def cmd_warn(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    reason = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else "Без причины"
    await db_add_warn(user.id, message.chat.id, message.from_user.id, reason)
    count = await db_get_warns(user.id, message.chat.id)
    await message.reply(f"⚠️ @{user.username or user.first_name} предупреждение ({count}/{MAX_WARNS})\nПричина: {reason}")
    if count >= MAX_WARNS:
        try:
            until_dt = datetime.now() + timedelta(seconds=WARN_BAN_DURATION)
            await bot.ban_chat_member(message.chat.id, user.id, until_date=until_dt)
            await db_add_ban(user.id, message.chat.id, until_dt.isoformat(), f"Авто-бан: {MAX_WARNS} варнов")
            await db_clear_warns(user.id, message.chat.id)
            await message.reply(f"🚫 @{user.username or user.first_name} забанен на 30 дней ({MAX_WARNS} варнов)")
        except Exception as e:
            await message.reply(f"Ошибка авто-бана: {e}")

@router.message(F.text.startswith("/wwarns"))
async def cmd_wwarns(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    warns = await db_get_warns_info(user.id, message.chat.id)
    if not warns:
        await message.reply("Нет варнов")
        return
    txt = f"📋 Варны @{user.username or user.first_name}:\n\n"
    for i, (reason, admin_id, created) in enumerate(warns, 1):
        txt += f"{i}. {reason} (от {admin_id}, {created[:16]})\n"
    await message.reply(txt)

@router.message(F.text.startswith("/unwarn"))
async def cmd_unwarn(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    await db_remove_warn(user.id, message.chat.id)
    count = await db_get_warns(user.id, message.chat.id)
    await message.reply(f"✅ Варн снят. Осталось: {count}/{MAX_WARNS}")

@router.message(F.text.startswith("/kick"))
async def cmd_kick(message: Message):
    if not await is_admin(message):
        await message.reply("❌ Только админы")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение")
        return
    user = message.reply_to_message.from_user
    if not user:
        return
    try:
        await bot.ban_chat_member(message.chat.id, user.id)
        await bot.unban_chat_member(message.chat.id, user.id)
        await message.reply(f"👢 @{user.username or user.first_name} кикнут")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")

@router.message(F.text.startswith("/report"))
async def cmd_report(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.reply("Команда работает только в группах")
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение нарушителя")
        return
    reported = message.reply_to_message.from_user
    if not reported:
        return
    reason = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else "Без причины"
    mention = f"@{reported.username}" if reported.username else f"<a href='tg://user?id={reported.id}'>{reported.first_name}</a>"
    admin_mentions = []
    try:
        admins = await bot.get_chat_administrators(message.chat.id)
        for a in admins:
            if not a.user.is_bot and a.user.id != message.from_user.id:
                if a.user.username:
                    admin_mentions.append(f"@{a.user.username}")
                else:
                    admin_mentions.append(f"<a href='tg://user?id={a.user.id}'>{a.user.first_name}</a>")
        text = f"🚨 <b>Репорт</b>\nНарушитель: {mention}\nПричина: {reason}\n"
        if admin_mentions:
            text += "\n" + " ".join(admin_mentions)
        await message.reply(text, parse_mode="HTML")
    except Exception as e:
        await message.reply(f"🚨 Репорт на {mention}\nПричина: {reason}\nОшибка тега админов: {e}")

@router.message(F.text.startswith("/currency"))
async def cmd_currency(message: Message):
    fiat = await get_currency_rates()
    crypto = await get_crypto_rates()
    txt = "💱 <b>Курсы</b>\n\n"
    if fiat:
        txt += "<b>Фиат (1 USD =):</b>\n"
        names = {"RUB": "🇷🇺 Рубль", "EUR": "🇪🇺 Евро", "UAH": "🇺🇦 Гривна", "TRY": "🇹🇷 Лира", "KZT": "🇰🇿 Тенге", "BYN": "🇧🇾 Рубль (Беларусь)", "GBP": "🇬🇧 Фунт", "CNY": "🇨🇳 Юань"}
        for code, value in fiat.items():
            if code in names:
                txt += f"{names[code]}: <b>{value:.2f}</b>\n"
    if crypto:
        txt += "\n<b>Крипто (в USD):</b>\n"
        cnames = {"BTC": "₿ Bitcoin", "ETH": "Ξ Ethereum", "TON": "💎 Toncoin", "SOL": "◎ Solana", "BNB": "🟡 BNB", "XRP": "✕ Ripple", "DOGE": "🐕 Doge", "TRX": "🔺 TRON"}
        for code, value in crypto.items():
            if code in cnames:
                txt += f"{cnames[code]}: <b>${value:,.2f}</b>\n"
    if not fiat and not crypto:
        await message.reply("❌ Не удалось получить курсы. Попробуй позже.")
        return
    txt += f"\n📊 Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}"
    await message.reply(txt, parse_mode="HTML")

@router.message(F.text.startswith("/stats"))
async def cmd_stats(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.reply("Команда работает только в группах")
        return
    args = message.text.split()
    period = 7
    label = "7 дней"
    if len(args) > 1:
        arg = args[1].lower()
        if arg in ("30", "30d", "30д", "month"):
            period = 30
            label = "30 дней"
        elif arg in ("all", "все", "forever", "0"):
            period = 0
            label = "Всё время"
    if period == 0:
        active = await db_count_active_users_all(message.chat.id)
        top = await db_top_users_all(message.chat.id, 5)
    else:
        active = await db_count_active_users(message.chat.id, period)
        top = await db_top_users(message.chat.id, period, 5)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 7 дней", callback_data=f"stats:7:{message.chat.id}"),
         InlineKeyboardButton(text="📅 30 дней", callback_data=f"stats:30:{message.chat.id}"),
         InlineKeyboardButton(text="♾ Всё время", callback_data=f"stats:0:{message.chat.id}")]
    ])
    txt = f"📊 <b>Статистика за {label}</b>\n\nАктивных пользователей: <b>{active}</b>\n\n"
    if top:
        txt += "<b>Топ-5 активных:</b>\n"
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, (name, username, count) in enumerate(top):
            display = f"@{username}" if username else (name or "Unknown")
            txt += f"{medals[i]} {display} — {count} сообщ.\n"
    else:
        txt += "Пока нет данных."
    await message.reply(txt, parse_mode="HTML", reply_markup=kb)

@router.callback_query(F.data.startswith("stats:"))
async def cb_stats(call):
    try:
        _, period, chat_id = call.data.split(":")
        period = int(period)
        chat_id = int(chat_id)
        label = "7 дней" if period == 7 else "30 дней" if period == 30 else "Всё время"
        if period == 0:
            active = await db_count_active_users_all(chat_id)
            top = await db_top_users_all(chat_id, 5)
        else:
            active = await db_count_active_users(chat_id, period)
            top = await db_top_users(chat_id, period, 5)
        txt = f"📊 <b>Статистика за {label}</b>\n\nАктивных пользователей: <b>{active}</b>\n\n"
        if top:
            txt += "<b>Топ-5 активных:</b>\n"
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
            for i, (name, username, count) in enumerate(top):
                display = f"@{username}" if username else (name or "Unknown")
                txt += f"{medals[i]} {display} — {count} сообщ.\n"
        else:
            txt += "Пока нет данных."
        await call.message.edit_text(txt, parse_mode="HTML")
        await call.answer()
    except Exception as e:
        await call.answer(f"Ошибка: {e}", show_alert=True)

@router.message(F.text.startswith("/calculator"))
async def cmd_calculator(message: Message):
    expr = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else ""
    if not expr:
        await message.reply("Пример: /calculator 2+2*3")
        return
    safe = re.sub(r"[^0-9+\-*/().,\s]", "", expr)
    if not safe:
        await message.reply("Только цифры и +-*/()")
        return
    try:
        result = eval(safe)
        await message.reply(f"🔢 {expr} = <b>{result}</b>", parse_mode="HTML")
    except Exception:
        await message.reply("Ошибка в выражении")

@router.message(F.text.startswith("/id"))
async def cmd_id(message: Message):
    if message.reply_to_message and message.reply_to_message.from_user:
        u = message.reply_to_message.from_user
        await message.reply(f"👤 ID: <code>{u.id}</code>\nИмя: {u.first_name}\nUsername: @{u.username or 'нет'}", parse_mode="HTML")
    else:
        u = message.from_user
        chat = message.chat
        await message.reply(f"Твой ID: <code>{u.id}</code>\nЧат ID: <code>{chat.id}</code>\nИмя: {u.first_name}", parse_mode="HTML")

async def main():
    await db_init()
    scheduler = AsyncIOScheduler(timezone=pytz.utc)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
