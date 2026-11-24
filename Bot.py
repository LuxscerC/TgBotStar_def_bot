import asyncio
import ast
import html
import logging
import math
import operator
import os
import re
from datetime import datetime, timedelta, timezone

import aiohttp
import aiosqlite
import pytz
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.types import (
    CallbackQuery,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Укажи токен бота в переменной окружения BOT_TOKEN")

MAX_WARNS = 5
WARN_BAN_DURATION = 30 * 24 * 60 * 60
WARN_EXPIRE_DAYS = 7
MIN_DURATION = 30
MAX_DURATION = 366 * 24 * 3600
DB_FILE = "bot.db"
PREFIXES = ".!/"

BOT_ID = 0
BOT_USERNAME = ""

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
CREATE INDEX IF NOT EXISTS idx_messages_username ON messages(chat_id, username);
"""


def utcnow():
    return datetime.now(timezone.utc)


async def db_init():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript(DB_SCHEMA)
        await db.commit()
    logging.info("БД готова")


async def db_cleanup():
    now = utcnow().isoformat()
    expire = (utcnow() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM mutes WHERE until<=?", (now,))
        await db.execute("DELETE FROM bans WHERE until<=? AND until!='forever'", (now,))
        await db.execute("DELETE FROM warns WHERE created_at<?", (expire,))
        await db.commit()


async def db_add_warn(user_id, chat_id, admin_id, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO warns (user_id, chat_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, chat_id, admin_id, reason, utcnow().isoformat()),
        )
        await db.commit()


async def db_get_warns(user_id, chat_id):
    expire = (utcnow() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM warns WHERE user_id=? AND chat_id=? AND created_at>=?",
            (user_id, chat_id, expire),
        )
        row = await cur.fetchone()
        return row[0] if row else 0


async def db_get_warns_info(user_id, chat_id):
    expire = (utcnow() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT reason, admin_id, created_at FROM warns WHERE user_id=? AND chat_id=? AND created_at>=? ORDER BY created_at DESC",
            (user_id, chat_id, expire),
        )
        return await cur.fetchall()


async def db_remove_warn(user_id, chat_id):
    expire = (utcnow() - timedelta(days=WARN_EXPIRE_DAYS)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT id FROM warns WHERE user_id=? AND chat_id=? AND created_at>=? ORDER BY created_at DESC LIMIT 1",
            (user_id, chat_id, expire),
        )
        row = await cur.fetchone()
        if row:
            await db.execute("DELETE FROM warns WHERE id=?", (row[0],))
        await db.commit()
        return bool(row)


async def db_clear_warns(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM warns WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()


async def db_add_mute(user_id, chat_id, until, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO mutes (user_id, chat_id, until, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, chat_id, until, reason, utcnow().isoformat()),
        )
        await db.commit()


async def db_remove_mute(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM mutes WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()


async def db_add_ban(user_id, chat_id, until, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO bans (user_id, chat_id, until, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, chat_id, until, reason, utcnow().isoformat()),
        )
        await db.commit()


async def db_remove_ban(user_id, chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM bans WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        await db.commit()


async def db_log_message(user_id, chat_id, username, first_name, text_length):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO messages (user_id, chat_id, username, first_name, text_length, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, chat_id, username, first_name, text_length, utcnow().isoformat()),
        )
        await db.commit()


async def db_find_user(chat_id, username):
    uname = username.lower()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT user_id, first_name, username FROM messages WHERE chat_id=? AND LOWER(username)=? ORDER BY id DESC LIMIT 1",
            (chat_id, uname),
        )
        row = await cur.fetchone()
        if not row:
            cur = await db.execute(
                "SELECT user_id, first_name, username FROM messages WHERE LOWER(username)=? ORDER BY id DESC LIMIT 1",
                (uname,),
            )
            row = await cur.fetchone()
        return row


async def db_count_active_users(chat_id, days):
    since = (utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=? AND created_at>=?",
            (chat_id, since),
        )
        row = await cur.fetchone()
        return row[0] if row else 0


async def db_top_users(chat_id, days, limit=5):
    since = (utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT first_name, username, COUNT(*) AS cnt FROM messages WHERE chat_id=? AND created_at>=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?",
            (chat_id, since, limit),
        )
        return await cur.fetchall()


async def db_count_active_users_all(chat_id):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        return row[0] if row else 0


async def db_top_users_all(chat_id, limit=5):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT first_name, username, COUNT(*) AS cnt FROM messages WHERE chat_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT ?",
            (chat_id, limit),
        )
        return await cur.fetchall()


FIAT_NAMES = {
    "RUB": "🇷🇺 Рубль",
    "EUR": "🇪🇺 Евро",
    "UAH": "🇺🇦 Гривна",
    "TRY": "🇹🇷 Лира",
    "KZT": "🇰🇿 Тенге",
    "BYN": "🇧🇾 Рубль (Беларусь)",
    "GBP": "🇬🇧 Фунт",
    "CNY": "🇨🇳 Юань",
}

CRYPTO_NAMES = {
    "bitcoin": "₿ Bitcoin",
    "ethereum": "Ξ Ethereum",
    "the-open-network": "💎 Toncoin",
    "solana": "◎ Solana",
    "binancecoin": "🟡 BNB",
    "ripple": "✕ Ripple",
    "dogecoin": "🐕 Doge",
    "tron": "🔺 TRON",
}


async def fetch_json(url):
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
    except Exception as e:
        logging.error(f"Ошибка запроса {url}: {e}")
        return None


async def get_currency_rates():
    data = await fetch_json("https://api.exchangerate-api.com/v4/latest/USD")
    if not data:
        return None
    rates = data.get("rates", {})
    return {code: rates[code] for code in FIAT_NAMES if code in rates}


async def get_crypto_rates():
    ids = ",".join(CRYPTO_NAMES)
    data = await fetch_json(f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd")
    if not data:
        return None
    return {cid: data[cid]["usd"] for cid in CRYPTO_NAMES if cid in data and "usd" in data[cid]}


class LogMessagesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            if (
                isinstance(event, Message)
                and event.from_user
                and not event.from_user.is_bot
                and event.chat.type != ChatType.PRIVATE
            ):
                await db_log_message(
                    event.from_user.id,
                    event.chat.id,
                    event.from_user.username or "",
                    event.from_user.first_name or "",
                    len(event.text or event.caption or ""),
                )
        except Exception as e:
            logging.error(f"middleware error: {e}")
        return await handler(event, data)


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
router.message.outer_middleware(LogMessagesMiddleware())
dp.include_router(router)


class BotError(Exception):
    pass


class Target:
    def __init__(self, user_id, first_name="", username=""):
        self.id = user_id
        self.first_name = first_name or ""
        self.username = username or ""

    @property
    def mention(self):
        name = self.first_name or (f"@{self.username}" if self.username else str(self.id))
        return f'<a href="tg://user?id={self.id}">{html.escape(name)}</a>'


COMMANDS = {}


def command(*names):
    def deco(func):
        for name in names:
            COMMANDS[name] = func
        return func

    return deco


async def say(message, text, **kwargs):
    return await message.reply(text, parse_mode="HTML", **kwargs)


def parse_command(text):
    text = text.strip()
    if not text:
        return None, ""
    if text[0] in PREFIXES:
        text = text[1:].lstrip()
    parts = text.split(maxsplit=1)
    if not parts:
        return None, ""
    cmd = parts[0].lower()
    args = parts[1] if len(parts) > 1 else ""
    if "@" in cmd:
        base, _, suffix = cmd.partition("@")
        if BOT_USERNAME and suffix == BOT_USERNAME.lower():
            cmd = base
        else:
            return None, ""
    return cmd, args


def unit_seconds(unit):
    u = unit.lower()
    if u in ("с", "s") or u.startswith(("сек", "sec")):
        return 1
    if u in ("м", "m") or u.startswith(("мин", "min")):
        return 60
    if u in ("ч", "h") or u.startswith(("час", "hour")):
        return 3600
    if u in ("д", "d") or u.startswith(("дн", "ден", "day")):
        return 86400
    if u == "w" or u.startswith(("нед", "week")):
        return 604800
    if u.startswith(("мес", "month")):
        return 2592000
    return None


def take_duration(tokens):
    if not tokens:
        return None, tokens
    m = re.fullmatch(r"(\d{1,9})([A-Za-zА-Яа-яЁё]+)", tokens[0])
    if m:
        mult = unit_seconds(m.group(2))
        if mult:
            return int(m.group(1)) * mult, tokens[1:]
    if re.fullmatch(r"\d{1,9}", tokens[0]) and len(tokens) > 1:
        mult = unit_seconds(tokens[1])
        if mult:
            return int(tokens[0]) * mult, tokens[2:]
    return None, tokens


def check_duration(secs):
    if secs < MIN_DURATION:
        raise BotError("Минимальное время — 30 секунд")
    if secs > MAX_DURATION:
        raise BotError("Максимальное время — 1 год")


def format_duration(seconds):
    d, r = divmod(int(seconds), 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = []
    if d:
        parts.append(f"{d} д.")
    if h:
        parts.append(f"{h} ч.")
    if m:
        parts.append(f"{m} мин.")
    if s:
        parts.append(f"{s} сек.")
    return " ".join(parts) or "0 сек."


async def get_member(chat_id, user_id):
    try:
        return await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return None


async def require_group(message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        raise BotError("Команда работает только в группах")


async def require_admin(message, restrict=False):
    await require_group(message)
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return
    if not message.from_user:
        raise BotError("❌ Только админы")
    member = await get_member(message.chat.id, message.from_user.id)
    if member is None:
        raise BotError("Не удалось проверить твои права")
    if member.status == ChatMemberStatus.CREATOR:
        return
    if member.status == ChatMemberStatus.ADMINISTRATOR:
        if restrict and not member.can_restrict_members:
            raise BotError("❌ У тебя нет права «Блокировка участников»")
        return
    raise BotError("❌ Только админы")


async def protect(message, target):
    if target.id == BOT_ID:
        raise BotError("Меня трогать нельзя 🙂")
    if message.from_user and target.id == message.from_user.id:
        raise BotError("Нельзя применить команду к себе")
    member = await get_member(message.chat.id, target.id)
    if member and member.status in (ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR):
        raise BotError("Нельзя применить команду к администратору")


async def resolve_target(message, args):
    rm = message.reply_to_message
    if rm and rm.from_user and not rm.forum_topic_created:
        u = rm.from_user
        return Target(u.id, u.first_name, u.username), args
    for ent in message.entities or []:
        if ent.type == "text_mention" and ent.user:
            fragment = ent.extract_from(message.text)
            u = ent.user
            return Target(u.id, u.first_name, u.username), args.replace(fragment, "", 1)
    tokens = args.split()
    for i, tok in enumerate(tokens):
        rest = " ".join(tokens[:i] + tokens[i + 1 :])
        if re.fullmatch(r"@[A-Za-z0-9_]{3,32}", tok):
            target = await find_by_username(message.chat.id, tok[1:])
            if not target:
                raise BotError(
                    f"Не нашёл {html.escape(tok)}. Пользователь должен хотя бы раз написать в чате — "
                    "или ответь на его сообщение / укажи ID"
                )
            return target, rest
        if re.fullmatch(r"\d{6,15}", tok):
            if i + 1 < len(tokens) and unit_seconds(tokens[i + 1]):
                continue
            target = Target(int(tok))
            member = await get_member(message.chat.id, target.id)
            if member:
                target = Target(member.user.id, member.user.first_name, member.user.username)
            return target, rest
    return None, args


async def find_by_username(chat_id, username):
    row = await db_find_user(chat_id, username)
    if row:
        return Target(row[0], row[1], row[2])
    try:
        chat = await bot.get_chat(f"@{username}")
        if chat.type == ChatType.PRIVATE:
            return Target(chat.id, chat.first_name, chat.username)
    except Exception:
        pass
    return None


async def need_target(message, args, example):
    target, rest = await resolve_target(message, args)
    if not target:
        raise BotError(f"Ответь на сообщение или укажи @юзернейм / ID\nПример: {example}")
    return target, rest


@router.message(F.text)
async def dispatch(message: Message):
    if not message.from_user:
        return
    cmd, args = parse_command(message.text)
    func = COMMANDS.get(cmd)
    if not func:
        return
    try:
        await func(message, args)
    except BotError as e:
        await say(message, str(e))
    except Exception as e:
        logging.exception("Ошибка команды")
        await say(message, f"Ошибка: {html.escape(str(e))}")


@command("старт", "start")
async def cmd_start(message, args):
    await say(
        message,
        "👋 Привет! Я бот-модератор.\n\n"
        "Команды: бан, разбан, мут, размут, варн, варны, снятьварн, кик, репорт, ид, стат, курс, калькулятор, помощь\n\n"
        "Префиксы: . ! / (или без префикса)\n"
        "Пользователя можно указать ответом на сообщение, @юзернеймом или ID.\n"
        "Подробнее: /help",
    )


@command("помощь", "хелп", "команды", "help")
async def cmd_help(message, args):
    await say(
        message,
        "📖 <b>Команды бота</b>\n\n"
        "<b>Как пользоваться</b>\n"
        "Префиксы: . ! / (или без префикса). Все команды на русском.\n"
        "Цель: ответьте на сообщение пользователя или укажите @username / ID в любой части команды. "
        "@username работает для тех, кто писал в чате, пока бот был в нём; иначе ответьте на сообщение или укажите ID.\n"
        "Примеры: <code>бан 1 день @user спам</code>, <code>.мут 30м @user</code>, <code>!ид @user</code>\n\n"
        "<b>Формат времени</b>\n"
        "30с, 10м, 2ч, 1д, 1нед, 1мес — или с пробелом: 10 мин, 2 часа, 3 дня.\n"
        "Минимум 30 секунд, максимум 1 год.\n\n"
        "👮 <b>Модерация</b> (админы чата; для бана, мута и кика нужно право «Блокировка участников»)\n"
        "<code>бан [время] [@user] [причина]</code> — бан (без времени = навсегда)\n"
        "<code>разбан [@user]</code> — разбан\n"
        "<code>мут время [@user] [причина]</code> — мут\n"
        "<code>размут [@user]</code> — размут\n"
        "<code>кик [@user]</code> — кик (можно вернуться)\n"
        "<code>варн [@user] [причина]</code> — варн (5 варнов = бан на 30 дней, варны сгорают через 7 дней)\n"
        "<code>варны [@user]</code> — список варнов\n"
        "<code>снятьварн [@user]</code> — снять последний варн\n\n"
        "📊 <b>Инфо</b>\n"
        "<code>репорт [@user] [причина]</code> — позвать админов\n"
        "<code>ид [@user]</code> — показать ID\n"
        "<code>стат [7|30|все]</code> — статистика активности чата\n"
        "<code>курс</code> — курсы валют и крипты\n"
        "<code>калькулятор 2+2*3</code> — калькулятор (+ - * / // % ** и скобки)\n"
        "<code>помощь</code> — эта справка",
    )


@command("мут", "mute")
async def cmd_mute(message, args):
    await require_admin(message, restrict=True)
    target, rest = await need_target(message, args, "мут 1 час @user причина")
    await protect(message, target)
    secs, tokens = take_duration(rest.split())
    if secs is None:
        raise BotError("Укажи время: мут 30м, мут 1 час, мут 2 дня")
    check_duration(secs)
    reason = " ".join(tokens) or "Без причины"
    until = utcnow() + timedelta(seconds=secs)
    permissions = ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
        can_change_info=False,
        can_invite_users=False,
        can_pin_messages=False,
        can_manage_topics=False,
    )
    await bot.restrict_chat_member(
        message.chat.id,
        target.id,
        permissions=permissions,
        until_date=until,
    )
    await db_add_mute(target.id, message.chat.id, until.isoformat(), reason)
    await say(
        message,
        f"🔇 {target.mention} замучен на {format_duration(secs)}\nПричина: {html.escape(reason)}",
    )


@command("размут", "анмут", "unmute")
async def cmd_unmute(message, args):
    await require_admin(message, restrict=True)
    target, _ = await need_target(message, args, "размут @user")
    chat = await bot.get_chat(message.chat.id)
    permissions = chat.permissions or ChatPermissions(
        can_send_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_invite_users=True,
        can_change_info=False,
        can_pin_messages=False,
        can_manage_topics=False,
    )
    await bot.restrict_chat_member(message.chat.id, target.id, permissions=permissions)
    await db_remove_mute(target.id, message.chat.id)
    await say(message, f"🔊 {target.mention} размучен")


@command("бан", "ban")
async def cmd_ban(message, args):
    await require_admin(message, restrict=True)
    target, rest = await need_target(message, args, "бан 1 день @user причина")
    await protect(message, target)
    secs, tokens = take_duration(rest.split())
    until = None
    if secs is not None:
        check_duration(secs)
        until = utcnow() + timedelta(seconds=secs)
    reason = " ".join(tokens) or "Без причины"
    await bot.ban_chat_member(message.chat.id, target.id, until_date=until)
    await db_add_ban(target.id, message.chat.id, until.isoformat() if until else "forever", reason)
    txt = f"🚫 {target.mention} забанен"
    txt += f" на {format_duration(secs)}" if until else " навсегда"
    txt += f"\nПричина: {html.escape(reason)}"
    await say(message, txt)


@command("разбан", "анбан", "unban")
async def cmd_unban(message, args):
    await require_admin(message, restrict=True)
    target, _ = await need_target(message, args, "разбан @user")
    await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
    await db_remove_ban(target.id, message.chat.id)
    await say(message, f"✅ {target.mention} разбанен")


@command("варн", "пред", "warn")
async def cmd_warn(message, args):
    await require_admin(message)
    target, rest = await need_target(message, args, "варн @user причина")
    await protect(message, target)
    reason = rest.strip() or "Без причины"
    await db_add_warn(target.id, message.chat.id, message.from_user.id, reason)
    count = await db_get_warns(target.id, message.chat.id)
    await say(
        message,
        f"⚠️ {target.mention} предупреждение ({count}/{MAX_WARNS})\nПричина: {html.escape(reason)}",
    )
    if count >= MAX_WARNS:
        until = utcnow() + timedelta(seconds=WARN_BAN_DURATION)
        await bot.ban_chat_member(message.chat.id, target.id, until_date=until)
        await db_add_ban(target.id, message.chat.id, until.isoformat(), f"Авто-бан: {MAX_WARNS} варнов")
        await db_clear_warns(target.id, message.chat.id)
        await say(message, f"🚫 {target.mention} забанен на 30 дней ({MAX_WARNS} варнов)")


@command("варны", "преды", "warns")
async def cmd_warns(message, args):
    await require_admin(message)
    target, _ = await need_target(message, args, "варны @user")
    warns = await db_get_warns_info(target.id, message.chat.id)
    if not warns:
        await say(message, "Нет варнов")
        return
    txt = f"📋 Варны {target.mention}:\n\n"
    for i, (reason, admin_id, created) in enumerate(warns, 1):
        txt += f"{i}. {html.escape(reason)} (от <code>{admin_id}</code>, {created[:16].replace('T', ' ')})\n"
    await say(message, txt)


@command("снятьварн", "снятьпред", "унварн", "анварн", "unwarn")
async def cmd_unwarn(message, args):
    await require_admin(message)
    target, _ = await need_target(message, args, "снятьварн @user")
    removed = await db_remove_warn(target.id, message.chat.id)
    if not removed:
        await say(message, "У пользователя нет активных варнов")
        return
    count = await db_get_warns(target.id, message.chat.id)
    await say(message, f"✅ Варн снят у {target.mention}. Осталось: {count}/{MAX_WARNS}")


@command("кик", "kick")
async def cmd_kick(message, args):
    await require_admin(message, restrict=True)
    target, _ = await need_target(message, args, "кик @user")
    await protect(message, target)
    await bot.ban_chat_member(message.chat.id, target.id)
    await bot.unban_chat_member(message.chat.id, target.id)
    await say(message, f"👢 {target.mention} кикнут")


@command("репорт", "жалоба", "report")
async def cmd_report(message, args):
    await require_group(message)
    target, rest = await need_target(message, args, "репорт @user причина")
    reason = rest.strip() or "Без причины"
    admins = await bot.get_chat_administrators(message.chat.id)
    mentions = []
    for a in admins:
        if a.user.is_bot or a.user.id == message.from_user.id:
            continue
        if a.user.username:
            mentions.append(f"@{a.user.username}")
        else:
            mentions.append(f'<a href="tg://user?id={a.user.id}">{html.escape(a.user.first_name)}</a>')
    txt = f"🚨 <b>Репорт</b>\nНарушитель: {target.mention}\nПричина: {html.escape(reason)}\n"
    if mentions:
        txt += "\n" + " ".join(mentions)
    await say(message, txt)


@command("курс", "курсы", "currency")
async def cmd_currency(message, args):
    fiat = await get_currency_rates()
    crypto = await get_crypto_rates()
    if not fiat and not crypto:
        await say(message, "❌ Не удалось получить курсы. Попробуй позже.")
        return
    txt = "💱 <b>Курсы</b>\n\n"
    if fiat:
        txt += "<b>Фиат (1 USD =):</b>\n"
        for code, value in fiat.items():
            txt += f"{FIAT_NAMES[code]}: <b>{value:.2f}</b>\n"
    if crypto:
        txt += "\n<b>Крипто (в USD):</b>\n"
        for cid, value in crypto.items():
            txt += f"{CRYPTO_NAMES[cid]}: <b>${value:,.2f}</b>\n"
    txt += f"\n📊 Обновлено: {utcnow().strftime('%H:%M %d.%m.%Y')}"
    await say(message, txt)


async def build_stats(chat_id, period):
    if period == 0:
        label = "всё время"
        active = await db_count_active_users_all(chat_id)
        top = await db_top_users_all(chat_id, 5)
    else:
        label = f"{period} дней"
        active = await db_count_active_users(chat_id, period)
        top = await db_top_users(chat_id, period, 5)
    txt = f"📊 <b>Статистика: {label}</b>\n\nАктивных пользователей: <b>{active}</b>\n\n"
    if top:
        txt += "<b>Топ-5 активных:</b>\n"
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, (name, username, count) in enumerate(top):
            display = f"@{username}" if username else (name or "Unknown")
            txt += f"{medals[i]} {html.escape(display)} — {count} сообщ.\n"
    else:
        txt += "Пока нет данных."
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📅 7 дней", callback_data="stats:7"),
                InlineKeyboardButton(text="📅 30 дней", callback_data="stats:30"),
                InlineKeyboardButton(text="♾ Всё время", callback_data="stats:0"),
            ]
        ]
    )
    return txt, kb


@command("стат", "стата", "статистика", "stats")
async def cmd_stats(message, args):
    await require_group(message)
    period = 7
    if args.split():
        arg = args.split()[0].lower()
        if arg in ("30", "30d", "30д", "месяц", "month"):
            period = 30
        elif arg in ("all", "все", "всё", "forever", "0"):
            period = 0
    txt, kb = await build_stats(message.chat.id, period)
    await say(message, txt, reply_markup=kb)


@router.callback_query(F.data.startswith("stats:"))
async def cb_stats(call: CallbackQuery):
    try:
        if not call.message:
            await call.answer()
            return
        period = int(call.data.split(":")[1])
        if period not in (0, 7, 30):
            period = 7
        txt, kb = await build_stats(call.message.chat.id, period)
        await call.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)
        await call.answer()
    except Exception as e:
        if "not modified" in str(e):
            await call.answer()
        else:
            await call.answer(f"Ошибка: {str(e)[:150]}", show_alert=True)


BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def safe_eval(node):
    if isinstance(node, ast.Expression):
        return safe_eval(node.body)
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return node.value
    if isinstance(node, ast.UnaryOp) and type(node.op) in UNARY_OPS:
        return UNARY_OPS[type(node.op)](safe_eval(node.operand))
    if isinstance(node, ast.BinOp) and type(node.op) in BIN_OPS:
        left = safe_eval(node.left)
        right = safe_eval(node.right)
        if isinstance(node.op, ast.Pow) and (abs(right) > 100 or abs(left) > 1e6):
            raise ValueError("слишком большое число")
        return BIN_OPS[type(node.op)](left, right)
    raise ValueError("недопустимое выражение")


@command("калькулятор", "калк", "посчитай", "calc", "calculator")
async def cmd_calculator(message, args):
    expr = args.strip()
    if not expr:
        raise BotError("Пример: калькулятор 2+2*3")
    if len(expr) > 100:
        raise BotError("Слишком длинное выражение")
    clean = expr.replace(",", ".").replace("^", "**").replace("×", "*").replace("÷", "/")
    if not re.fullmatch(r"[0-9+\-*/().%\s]+", clean):
        raise BotError("Только цифры и + - * / // % ** ( )")
    try:
        result = safe_eval(ast.parse(clean.strip(), mode="eval"))
        if isinstance(result, float):
            if not math.isfinite(result):
                raise ValueError
            result = int(result) if result.is_integer() else round(result, 10)
        if abs(result) > 1e100:
            raise ValueError
    except Exception:
        raise BotError("Ошибка в выражении")
    await say(message, f"🔢 {html.escape(expr)} = <b>{result}</b>")


@command("ид", "id")
async def cmd_id(message, args):
    target, _ = await resolve_target(message, args)
    if target:
        txt = f"👤 ID: <code>{target.id}</code>\nИмя: {html.escape(target.first_name or 'неизвестно')}\nUsername: "
        txt += f"@{html.escape(target.username)}" if target.username else "нет"
        await say(message, txt)
        return
    u = message.from_user
    await say(
        message,
        f"Твой ID: <code>{u.id}</code>\nЧат ID: <code>{message.chat.id}</code>\nИмя: {html.escape(u.first_name or '')}",
    )


async def main():
    global BOT_ID, BOT_USERNAME
    await db_init()
    me = await bot.get_me()
    BOT_ID = me.id
    BOT_USERNAME = me.username or ""
    scheduler = AsyncIOScheduler(timezone=pytz.utc)
    scheduler.add_job(db_cleanup, "interval", hours=1)
    scheduler.start()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
