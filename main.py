import asyncio
import logging
import os
import random
import string
import sys
import time
from datetime import datetime, timedelta
from typing import Union, List, Optional, Any, Dict, Final

import aiosqlite
from dotenv import load_dotenv
from supabase import create_client, Client

from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramRetryAfter,
    TelegramBadRequest,
    TelegramNetworkError
)
from aiogram.types import (
    Message,
    CallbackQuery,
    BotCommand,
    ReactionTypeEmoji,
    BufferedInputFile,
    URLInputFile,
    ContentType
)

# --- 1. ИНИЦИАЛИЗАЦИЯ И ЛОГИРОВАНИЕ ---
load_dotenv()

logger = logging.getLogger("SpokElite_v15_supabase")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)

fh = logging.FileHandler("bot_v15_supabase.log", encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Загрузка конфигурации
BOT_TOKEN: Final = os.getenv("BOT_TOKEN")
SUPABASE_URL: Final = os.getenv("SUPABASE_URL")
SUPABASE_KEY: Final = os.getenv("SUPABASE_KEY")
ADMIN_GROUP_ID: Final = int(os.getenv("ADMIN_GROUP_ID", 0))
REVIEWS_TOPIC_ID: Final = int(os.getenv("REVIEWS_TOPIC_ID", 0))
OWNER_ID: Final = int(os.getenv("OWNER_ID", 0))
START_PHOTO_URL: Final = os.getenv("START_PHOTO_URL",
                                   "https://i.yapx.ru/cz2dj.jpg")
DB_NAME: Final = "spok_v15_local.db"
USE_SUPABASE: Final = os.getenv("USE_SUPABASE", "true").lower() == "true"

if not BOT_TOKEN:
    logger.critical("Брат, добавь BOT_TOKEN в .env файл!")
    sys.exit(1)

if USE_SUPABASE and (not SUPABASE_URL or not SUPABASE_KEY):
    logger.critical("Для использования Supabase добавь SUPABASE_URL и SUPABASE_KEY в .env файл!")
    sys.exit(1)

# --- 2. ИНИЦИАЛИЗАЦИЯ SUPABASE ---
supabase: Optional[Client] = None
if USE_SUPABASE:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase клиент инициализирован")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации Supabase: {e}")
        logger.warning("⚠️ Бот будет использовать только локальную базу данных")
        USE_SUPABASE = False


# --- 3. УПРАВЛЕНИЕ БАЗОЙ ДАННЫХ (ЛОКАЛЬНАЯ + SUPABASE) ---
class DatabaseManager:
    def __init__(self, path: str):
        self.path = path
        self.supabase_tables = ['users', 'reviews', 'warns_history', 'referrals', 'broadcast_messages']

    async def initialize(self):
        """Инициализация локальной базы"""
        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA journal_mode=WAL")

            await db.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    anon_id TEXT UNIQUE,
                    topic_id INTEGER UNIQUE,
                    referrer_id INTEGER,
                    warns INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    ban_until DATETIME,
                    ban_reason TEXT,
                    is_active INTEGER DEFAULT 1,
                    msg_count INTEGER DEFAULT 0,
                    created_at DATETIME,
                    last_seen DATETIME
                );
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    admin_alias TEXT,
                    rating INTEGER,
                    comment TEXT,
                    created_at DATETIME
                );
                CREATE TABLE IF NOT EXISTS system_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    admin_id INTEGER,
                    action TEXT,
                    details TEXT,
                    created_at DATETIME
                );
                CREATE TABLE IF NOT EXISTS warns_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    admin_id INTEGER,
                    reason TEXT,
                    created_at DATETIME
                );
                CREATE TABLE IF NOT EXISTS broadcast_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    message_type TEXT,
                    content TEXT,
                    sent_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    created_at DATETIME
                );
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_id INTEGER,
                    created_at DATETIME,
                    UNIQUE(referrer_id, referred_id)
                );
            """)

            await self._migrate_database(db)
            await db.commit()

        logger.info("✅ Локальная база данных инициализирована")

    async def _migrate_database(self, db):
        """Миграция локальной базы данных"""
        try:
            async with db.execute("PRAGMA table_info(users)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}

            if 'last_seen' not in columns:
                await db.execute("ALTER TABLE users ADD COLUMN last_seen DATETIME")
                await db.execute("UPDATE users SET last_seen = created_at WHERE last_seen IS NULL")

            if 'ban_until' not in columns:
                await db.execute("ALTER TABLE users ADD COLUMN ban_until DATETIME")

            if 'ban_reason' not in columns:
                await db.execute("ALTER TABLE users ADD COLUMN ban_reason TEXT")

        except Exception as e:
            logger.error(f"Ошибка миграции: {e}")

    async def register(self, uid: int, rid: int = None):
        """Регистрация пользователя"""
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT 1 FROM users WHERE user_id = ?", (uid,)) as c:
                if not await c.fetchone():
                    aid = "USER-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
                    now = datetime.now().isoformat()

                    await db.execute("""
                        INSERT INTO users (user_id, anon_id, referrer_id, created_at, last_seen) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (uid, aid, rid, now, now))

                    if rid:
                        try:
                            await db.execute("""
                                INSERT OR IGNORE INTO referrals (referrer_id, referred_id, created_at)
                                VALUES (?, ?, ?)
                            """, (rid, uid, now))
                        except Exception as e:
                            logger.error(f"Ошибка записи реферала: {e}")

                    await db.commit()

                    # Синхронизируем с Supabase
                    if USE_SUPABASE:
                        await self.sync_user_to_supabase(uid)

                    logger.info(f"📝 Новый пользователь зарегистрирован: {uid}")
                else:
                    now = datetime.now().isoformat()
                    await db.execute("UPDATE users SET last_seen = ? WHERE user_id = ?", (now, uid))
                    await db.commit()

    async def sync_user_to_supabase(self, user_id: int):
        """Синхронизация пользователя с Supabase"""
        if not USE_SUPABASE:
            return

        try:
            user = await self.get_user(uid=user_id)
            if not user:
                return

            user_data = {
                'user_id': user['user_id'],
                'anon_id': user['anon_id'],
                'topic_id': user.get('topic_id'),
                'referrer_id': user.get('referrer_id'),
                'warns': user['warns'],
                'is_banned': bool(user['is_banned']),
                'ban_until': user.get('ban_until'),
                'ban_reason': user.get('ban_reason'),
                'is_active': bool(user['is_active']),
                'msg_count': user['msg_count'],
                'created_at': user['created_at'],
                'last_seen': user['last_seen'],
                'source_db': 'sqlite'
            }

            # Проверяем, существует ли пользователь в Supabase
            existing = supabase.table('users').select('*').eq('user_id', user_id).execute()

            if existing.data and len(existing.data) > 0:
                # Обновляем существующего
                supabase.table('users').update(user_data).eq('user_id', user_id).execute()
                logger.debug(f"🔄 Пользователь {user_id} обновлен в Supabase")
            else:
                # Создаем нового
                supabase.table('users').insert(user_data).execute()
                logger.debug(f"🔄 Пользователь {user_id} добавлен в Supabase")

        except Exception as e:
            logger.error(f"Ошибка синхронизации пользователя {user_id} с Supabase: {e}")

    async def get_user(self, uid: int = None, tid: int = None):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            if uid:
                async with db.execute("SELECT * FROM users WHERE user_id = ?", (uid,)) as c:
                    r = await c.fetchone()
                    return dict(r) if r else None
            elif tid:
                async with db.execute("SELECT * FROM users WHERE topic_id = ?", (tid,)) as c:
                    r = await c.fetchone()
                    return dict(r) if r else None
            return None

    async def add_warn(self, uid: int, admin_id: int, reason: str = None) -> int:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET warns = warns + 1 WHERE user_id = ?", (uid,))

            async with db.execute("SELECT warns FROM users WHERE user_id = ?", (uid,)) as c:
                w_count = (await c.fetchone())[0]

            now = datetime.now().isoformat()
            await db.execute("""
                INSERT INTO warns_history (user_id, admin_id, reason, created_at) 
                VALUES (?, ?, ?, ?)
            """, (uid, admin_id, reason, now))

            warn_id = None
            async with db.execute("SELECT last_insert_rowid()") as c:
                warn_id = (await c.fetchone())[0]

            await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    # Обновляем пользователя
                    user_data = {
                        'user_id': uid,
                        'warns': w_count
                    }
                    supabase.table('users').update(user_data).eq('user_id', uid).execute()

                    # Добавляем запись в историю
                    warn_data = {
                        'user_id': uid,
                        'admin_id': admin_id,
                        'reason': reason,
                        'created_at': now,
                        'source_db': 'sqlite'
                    }
                    supabase.table('warns_history').insert(warn_data).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации варна: {e}")

            logger.info(f"⚠️ Пользователю {uid} выдан варн ({w_count}/3). Причина: {reason}")
            return w_count

    async def get_active_users_count(self):
        if USE_SUPABASE:
            try:
                response = supabase.table('users').select('count', count='exact').eq('is_active', True).eq('is_banned',
                                                                                                           False).execute()
                return response.count or 0
            except Exception as e:
                logger.error(f"Ошибка получения активных пользователей из Supabase: {e}")

        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND is_banned = 0") as c:
                return (await c.fetchone())[0]

    async def get_today_users(self):
        today = datetime.now().date().isoformat()

        if USE_SUPABASE:
            try:
                response = supabase.table('users').select('count', count='exact').gte('created_at',
                                                                                      f'{today}T00:00:00').execute()
                return response.count or 0
            except Exception as e:
                logger.error(f"Ошибка получения сегодняшних пользователей из Supabase: {e}")

        async with aiosqlite.connect(self.path) as db:
            async with db.execute("""
                SELECT COUNT(*) FROM users 
                WHERE DATE(created_at) = DATE('now')
            """) as c:
                return (await c.fetchone())[0]

    async def get_avg_messages(self):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT AVG(msg_count) FROM users WHERE msg_count > 0") as c:
                return (await c.fetchone())[0] or 0

    async def get_top_referrers(self, limit=5):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT r.referrer_id, COUNT(*) as count, u.anon_id
                FROM referrals r
                LEFT JOIN users u ON r.referrer_id = u.user_id
                GROUP BY r.referrer_id 
                ORDER BY count DESC 
                LIMIT ?
            """, (limit,)) as c:
                return await c.fetchall()

    async def get_daily_stats(self, days=7):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as registrations,
                    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active
                FROM users 
                WHERE created_at >= DATE('now', ?)
                GROUP BY DATE(created_at)
                ORDER BY date
            """, (f'-{days} days',)) as c:
                return await c.fetchall()

    async def close_ticket(self, uid: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET topic_id = NULL WHERE user_id = ?", (uid,))
            await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    update_data = {
                        'user_id': uid,
                        'topic_id': None
                    }
                    supabase.table('users').update(update_data).eq('user_id', uid).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации закрытия тикета: {e}")

            logger.info(f"Тикет пользователя {uid} закрыт")

    async def add_review(self, user_id: int, admin_alias: str, rating: int, comment: str):
        now = datetime.now().isoformat()

        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO reviews (user_id, admin_alias, rating, comment, created_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, admin_alias, rating, comment, now))

            review_id = None
            async with db.execute("SELECT last_insert_rowid()") as c:
                review_id = (await c.fetchone())[0]
                await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    review_data = {
                        'user_id': user_id,
                        'admin_alias': admin_alias,
                        'rating': rating,
                        'comment': comment,
                        'created_at': now,
                        'source_db': 'sqlite'
                    }
                    supabase.table('reviews').insert(review_data).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации отзыва: {e}")

            return review_id

    async def get_reviews_stats(self):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute("SELECT COUNT(*), AVG(rating) FROM reviews") as c:
                total_count, avg_rating = await c.fetchone()

            async with db.execute("""
                SELECT admin_alias, AVG(rating) as avg_r, COUNT(*) as cnt 
                FROM reviews 
                GROUP BY admin_alias 
                HAVING COUNT(*) >= 3 
                ORDER BY avg_r DESC 
                LIMIT 5
            """) as c:
                top_admins = await c.fetchall()

            return {
                'total_count': total_count or 0,
                'avg_rating': avg_rating or 0,
                'top_admins': [dict(admin) for admin in top_admins]
            }

    async def get_latest_reviews(self, limit=10):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT r.*, u.anon_id 
                FROM reviews r 
                LEFT JOIN users u ON r.user_id = u.user_id 
                ORDER BY r.created_at DESC 
                LIMIT ?
            """, (limit,)) as c:
                return await c.fetchall()

    async def increment_message_count(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("UPDATE users SET msg_count = msg_count + 1, last_seen = ? WHERE user_id = ?",
                             (datetime.now().isoformat(), user_id))
            await db.commit()

    async def update_user_ban(self, user_id: int, is_banned: bool, ban_until: str = None, ban_reason: str = None):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE users 
                SET is_banned = ?, ban_until = ?, ban_reason = ?
                WHERE user_id = ?
            """, (1 if is_banned else 0, ban_until, ban_reason, user_id))
            await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    update_data = {
                        'user_id': user_id,
                        'is_banned': is_banned,
                        'ban_until': ban_until,
                        'ban_reason': ban_reason
                    }
                    supabase.table('users').update(update_data).eq('user_id', user_id).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации бана: {e}")

    async def get_all_active_users(self):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT user_id FROM users WHERE is_active = 1 AND is_banned = 0") as c:
                rows = await c.fetchall()
                return [row[0] for row in rows]

    async def save_broadcast_stats(self, admin_id: int, message_type: str, content: str, sent: int, failed: int):
        now = datetime.now().isoformat()

        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO broadcast_messages (admin_id, message_type, content, sent_count, failed_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (admin_id, message_type, content, sent, failed, now))

            await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    broadcast_data = {
                        'admin_id': admin_id,
                        'message_type': message_type,
                        'content': content,
                        'sent_count': sent,
                        'failed_count': failed,
                        'created_at': now,
                        'source_db': 'sqlite'
                    }
                    supabase.table('broadcast_messages').insert(broadcast_data).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации статистики рассылки: {e}")


db_engine = DatabaseManager(DB_NAME)


# --- 4. FSM И ЗАЩИТНАЯ МИДЛВАРЯ ---
class BotStates(StatesGroup):
    choosing_category = State()
    writing_issue = State()
    rev_adm = State()
    rev_rate = State()
    rev_msg = State()
    broadcasting = State()
    broadcast_confirm = State()


class GuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not event.from_user or event.chat.type != "private":
            return await handler(event, data)

        await db_engine.register(event.from_user.id)
        u = await db_engine.get_user(uid=event.from_user.id)

        if u and u['is_banned']:
            if u['ban_until']:
                ban_time = datetime.fromisoformat(u['ban_until']) if u['ban_until'] else None
                if ban_time and ban_time < datetime.now():
                    # Разбан по времени
                    await db_engine.update_user_ban(event.from_user.id, False, None, None)
                elif ban_time:
                    remaining = ban_time - datetime.now()
                    hours = int(remaining.total_seconds() // 3600)
                    minutes = int((remaining.total_seconds() % 3600) // 60)

                    reason_text = f"\nПричина: {u['ban_reason']}" if u['ban_reason'] else ""

                    await event.answer(
                        f"🚫 Вы заблокированы!\n"
                        f"До разблока осталось: {hours}ч {minutes}м{reason_text}"
                    )
                    return
            else:
                await event.answer("🚫 Вы заблокированы навсегда!")
                return

        return await handler(event, data)


# --- 5. КЛАВИАТУРЫ ---
def get_main_kb():
    b = ReplyKeyboardBuilder()
    b.row(types.KeyboardButton(text="🆘 Создать обращение"))
    b.row(types.KeyboardButton(text="⭐️ Оставить отзыв"), types.KeyboardButton(text="📊 Стена отзывов"))
    b.row(types.KeyboardButton(text="👤 Мой профиль"))
    return b.as_markup(resize_keyboard=True)


def get_categories_kb():
    b = InlineKeyboardBuilder()
    categories = ["🛠 Тех. вопрос", "💬 Общение", "💰 Поддержка", "📱 Другое"]
    for cat in categories:
        b.button(text=cat, callback_data=f"cat_{cat}")
    b.adjust(2)
    return b.as_markup()


def get_cancel_kb():
    b = ReplyKeyboardBuilder()
    b.add(types.KeyboardButton(text="❌ Отмена"))
    return b.as_markup(resize_keyboard=True)


def get_admin_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📊 Статистика", callback_data="admin_stats")
    b.button(text="📢 Рассылка", callback_data="admin_broadcast")
    b.button(text="📁 Экспорт", callback_data="admin_export")
    b.button(text="🔄 Очистить кэш", callback_data="admin_clear_cache")
    b.button(text="🔄 Синхронизация", callback_data="admin_sync")
    b.adjust(2)
    return b.as_markup()


# --- 6. УТИЛИТЫ И ЭФФЕКТЫ ---
async def send_with_typing(chat_id: int, text: str, bot: Bot,
                           parse_mode: str = "HTML",
                           reply_markup: types.ReplyKeyboardMarkup = None,
                           delay: float = 0.05):
    try:
        await bot.send_chat_action(chat_id, "typing")
        await asyncio.sleep(min(len(text) * 0.03, 2.0))

        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in send_with_typing: {e}")
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )


async def send_photo_with_typing(chat_id: int, photo_url: str, caption: str, bot: Bot,
                                 parse_mode: str = "HTML",
                                 reply_markup: types.ReplyKeyboardMarkup = None):
    try:
        await bot.send_chat_action(chat_id, "upload_photo")
        await asyncio.sleep(1)

        photo = URLInputFile(photo_url)
        return await bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error sending photo: {e}")
        return await send_with_typing(chat_id, caption, bot, parse_mode, reply_markup)


def parse_time(time_str: str) -> Optional[timedelta]:
    time_str = time_str.lower()

    if time_str == "перманентно":
        return None

    multipliers = {
        'd': 86400,
        'h': 3600,
        'm': 60,
        's': 1
    }

    total_seconds = 0
    num = ''

    for char in time_str:
        if char.isdigit():
            num += char
        elif char in multipliers:
            if num:
                total_seconds += int(num) * multipliers[char]
                num = ''
        else:
            return None

    return timedelta(seconds=total_seconds)


def format_timedelta(td: timedelta) -> str:
    if td is None:
        return "навсегда"

    total_seconds = int(td.total_seconds())
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60

    parts = []
    if days > 0:
        parts.append(f"{days}д")
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0:
        parts.append(f"{minutes}м")

    return ' '.join(parts) if parts else "0м"


# --- 7. ЛОГИКА ТИКЕТОВ И ФОРУМА ---
async def init_ticket(uid: int, bot: Bot, category: str):
    user = await db_engine.get_user(uid=uid)
    if not user:
        logger.error(f"User {uid} not found in database")
        return None

    if user.get('topic_id'):
        logger.info(f"User {uid} already has active topic {user['topic_id']}, closing it")
        try:
            await bot.send_message(
                ADMIN_GROUP_ID,
                f"🔒 <b>Пользователь завершил диалог</b>\n\n"
                f"👤 Клиент: <code>{user['anon_id']}</code>\n"
                f"🆔 User ID: <code>{uid}</code>\n"
                f"📅 Завершено: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"<i>Новое обращение будет создано в новом топике.</i>",
                message_thread_id=user['topic_id'],
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Error sending closure message: {e}")

        await db_engine.close_ticket(uid)

    try:
        logger.info(f"Creating forum topic for user {uid}, category: {category}")

        topic = await bot.create_forum_topic(
            ADMIN_GROUP_ID,
            f"{category} | {user['anon_id']}"
        )

        logger.info(f"Topic created: {topic.message_thread_id}")

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET topic_id = ? WHERE user_id = ?",
                             (topic.message_thread_id, uid))
            await db.commit()

            # Синхронизируем с Supabase
            if USE_SUPABASE:
                try:
                    update_data = {
                        'user_id': uid,
                        'topic_id': topic.message_thread_id
                    }
                    supabase.table('users').update(update_data).eq('user_id', uid).execute()
                except Exception as e:
                    logger.error(f"Ошибка синхронизации topic_id: {e}")

        ticket_card = (
            f"🚀 <b>НОВАЯ ЗАЯВКА В ПОДДЕРЖКУ</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"📁 Категория: <b>{category}</b>\n"
            f"👤 Клиент: <code>{user['anon_id']}</code>\n"
            f"🆔 User ID: <code>{uid}</code>\n"
            f"📊 Сообщений: {user['msg_count']}\n"
            f"⚠️ Варны: {user['warns']}/3\n"
            f"━━━━━━━━━━━━━━\n"
            f"📢 Администраторы, сформировано новое обращение!"
        )

        await bot.send_message(
            ADMIN_GROUP_ID,
            ticket_card,
            message_thread_id=topic.message_thread_id,
            parse_mode="HTML"
        )

        logger.info(f"Ticket card sent for user {uid}")
        return topic.message_thread_id

    except Exception as e:
        logger.error(f"Ticket Init Error for user {uid}: {e}")
        return None


# --- 8. КОПИРОВАНИЕ СООБЩЕНИЙ ---
async def safe_set_reaction(
    bot: Bot, 
    chat_id: int, 
    message_id: int, 
    emoji: str
) -> bool:
    """Безопасно установить реакцию на сообщение"""
    try:
        supported_emojis = ["👍", "👎", "❤", "🔥", "🥰", "👏", "😁", "🤔", "🤯", "😱",
                            "🤬", "😢", "🎉", "🤩", "🤮", "💩", "🙏", "👌", "🕊", "🤡",
                            "🥱", "🥴", "😍", "🐳", "❤‍🔥", "🌚", "🌭", "💯", "🤣", "⚡",
                            "🍌", "🏆", "💔", "🤨", "😐", "🍓", "🍾", "💋", "🖕", "😈",
                            "😴", "😭", "🤓", "👻", "👨‍💻", "👀", "🎃", "🙈", "😇", "😨",
                            "🤝", "✍", "🤗", "🫡", "🎅", "🎄", "☃", "💅", "🤪", "🗿",
                            "🆒", "💘", "🙉", "🦄", "😘", "💊", "🙊", "😎", "👾", "🤷‍♂",
                            "🤷", "🤷‍♀", "😡"]

        # Улучшенная логика замены emoji
        if emoji not in supported_emojis:
            if emoji in ["✅", "📨", "👤"]:
                emoji = "👍"
            elif emoji in ["❌", "🚫"]:
                emoji = "👎"
            else:
                emoji = "👍"

        await bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)]
        )
        return True
        
    except TelegramBadRequest as e:
        error_msg = str(e)
        if "REACTION_INVALID" in error_msg:
            logger.warning(f"Invalid reaction emoji: {emoji}")
        elif "message to set reaction not found" in error_msg:
            logger.warning(f"Message not found for reaction: {chat_id}/{message_id}")
        else:
            logger.error(f"BadRequest setting reaction: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Error setting reaction: {e}")
        return False


async def copy_message_to_admin(bot: Bot, user_id: int, message: Message, topic_id: int):
    try:
        user = await db_engine.get_user(uid=user_id)
        if not user:
            logger.error(f"User {user_id} not found for copying")
            return None

        logger.info(f"Copying message from user {user_id} to topic {topic_id}")

        header = f"👤 <b>{user['anon_id']}</b>\n━━━━━━━━━━━━━━"

        if message.text:
            text_content = message.html_text if hasattr(message, 'html_text') else message.text
            formatted_text = f"{header}\n{text_content}"
            sent_msg = await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=formatted_text,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.photo:
            caption_content = message.html_text if hasattr(message,
                                                           'html_text') and message.caption else message.caption
            caption = f"{header}\n{caption_content or ''}"
            sent_msg = await bot.send_photo(
                chat_id=ADMIN_GROUP_ID,
                photo=message.photo[-1].file_id,
                caption=caption,
                parse_mode="HTML" if caption_content else None,
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.video:
            caption = f"{header}\n{message.caption or ''}"
            sent_msg = await bot.send_video(
                chat_id=ADMIN_GROUP_ID,
                video=message.video.file_id,
                caption=caption,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.document:
            caption = f"{header}\n{message.caption or ''}"
            sent_msg = await bot.send_document(
                chat_id=ADMIN_GROUP_ID,
                document=message.document.file_id,
                caption=caption,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.audio:
            caption = f"{header}\n{message.caption or ''}"
            sent_msg = await bot.send_audio(
                chat_id=ADMIN_GROUP_ID,
                audio=message.audio.file_id,
                caption=caption,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.voice:
            caption = f"{header}\n(голосовое сообщение)"
            sent_msg = await bot.send_voice(
                chat_id=ADMIN_GROUP_ID,
                voice=message.voice.file_id,
                caption=caption,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.sticker:
            await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=header,
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            sent_msg = await bot.send_sticker(
                chat_id=ADMIN_GROUP_ID,
                sticker=message.sticker.file_id,
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.animation:
            caption = f"{header}\n{message.caption or ''}"
            sent_msg = await bot.send_animation(
                chat_id=ADMIN_GROUP_ID,
                animation=message.animation.file_id,
                caption=caption,
                parse_mode="HTML" if message.caption else None,
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.video_note:
            await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=f"{header}\n(видеосообщение)",
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            sent_msg = await bot.send_video_note(
                chat_id=ADMIN_GROUP_ID,
                video_note=message.video_note.file_id,
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.location:
            sent_msg = await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=f"{header}\n📍 Локация",
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.contact:
            contact = message.contact
            sent_msg = await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=f"{header}\n📱 Контакт: {contact.first_name} {contact.last_name or ''}\n📞 Телефон: {contact.phone_number}",
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        elif message.poll:
            poll = message.poll
            sent_msg = await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=f"{header}\n📊 Опрос: {poll.question}",
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

        else:
            content_type = str(message.content_type).replace("ContentType.", "")
            sent_msg = await bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=f"{header}\n📎 Тип контента: {content_type}\n{message.caption or ''}",
                parse_mode="HTML",
                message_thread_id=topic_id
            )
            return sent_msg

    except Exception as e:
        logger.error(f"Error copying message to admin: {e}")
        return None


async def copy_message_to_user(bot: Bot, user_id: int, message: Message):
    try:
        logger.info(f"Copying message from admin to user {user_id}")

        if message.text:
            text_to_send = message.html_text if hasattr(message, 'html_text') else message.text
            sent_msg = await bot.send_message(
                chat_id=user_id,
                text=text_to_send,
                parse_mode="HTML" if hasattr(message, 'html_text') else None
            )
            return sent_msg

        elif message.photo:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_photo(
                chat_id=user_id,
                photo=message.photo[-1].file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.video:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_video(
                chat_id=user_id,
                video=message.video.file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.document:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_document(
                chat_id=user_id,
                document=message.document.file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.audio:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_audio(
                chat_id=user_id,
                audio=message.audio.file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.voice:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_voice(
                chat_id=user_id,
                voice=message.voice.file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.sticker:
            sent_msg = await bot.send_sticker(
                chat_id=user_id,
                sticker=message.sticker.file_id
            )
            return sent_msg

        elif message.animation:
            caption = message.html_text if hasattr(message, 'html_text') and message.caption else message.caption
            sent_msg = await bot.send_animation(
                chat_id=user_id,
                animation=message.animation.file_id,
                caption=caption,
                parse_mode="HTML" if hasattr(message, 'html_text') and message.caption else None
            )
            return sent_msg

        elif message.video_note:
            sent_msg = await bot.send_video_note(
                chat_id=user_id,
                video_note=message.video_note.file_id
            )
            return sent_msg

        elif message.location:
            location = message.location
            sent_msg = await bot.send_location(
                chat_id=user_id,
                latitude=location.latitude,
                longitude=location.longitude
            )
            return sent_msg

        elif message.contact:
            contact = message.contact
            sent_msg = await bot.send_contact(
                chat_id=user_id,
                phone_number=contact.phone_number,
                first_name=contact.first_name,
                last_name=contact.last_name or ""
            )
            return sent_msg

        elif message.poll:
            poll = message.poll
            sent_msg = await bot.send_poll(
                chat_id=user_id,
                question=poll.question,
                options=[option.text for option in poll.options],
                is_anonymous=poll.is_anonymous,
                type=poll.type
            )
            return sent_msg

        else:
            content_type = str(message.content_type).replace("ContentType.", "")
            fallback_text = f"📎 <b>Сообщение от администратора</b>\n"
            fallback_text += f"Тип: {content_type}\n"

            if message.caption:
                fallback_text += f"\n{message.caption}"

            sent_msg = await bot.send_message(
                chat_id=user_id,
                text=fallback_text,
                parse_mode="HTML"
            )
            return sent_msg

    except TelegramForbiddenError:
        raise
    except Exception as e:
        logger.error(f"Error copying message to user: {e}")
        raise


# --- 9. ХЕНДЛЕРЫ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
dp.message.middleware(GuardMiddleware())


# --- START ---
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    ref = int(command.args) if command.args and command.args.isdigit() else None

    if ref:
        logger.info(f"Реферальный переход: {ref} -> {message.from_user.id}")

    await db_engine.register(message.from_user.id, ref)

    welcome_text = (
            "👋 <b>Привет, путник мира!</b>\n\n"
            "Знакомо чувство, когда после эпичной битвы хочется отдохнуть и поболтать с кем-то по душам? "
            "Или когда уже не хочется жить из-за тимейтов, которые идут на слив и пикают кого попало?\n\n"
            "<b><a href='https://t.me/Darius_will_bot'>Теперь у тебя есть личный помощник! "
            "Представляем бота поддержки, который всегда готов выслушать все твои проблемы и несчастья и поддержать.</a></b>\n\n"
            "<b><a href='https://t.me/moral_support_ML'>Здесь ты сможешь более подробно ознакомится о каждом нашем персонаже и о самом мире</a></b>"
        )

    try:
        await send_photo_with_typing(
            chat_id=message.chat.id,
            photo_url=START_PHOTO_URL,
            caption=welcome_text,
            bot=bot,
            parse_mode="HTML",
            reply_markup=get_main_kb()
        )
    except:
        await send_with_typing(
            chat_id=message.chat.id,
            text=welcome_text,
            bot=bot,
            parse_mode="HTML",
            reply_markup=get_main_kb()
        )

    if message.from_user.id == OWNER_ID:
        await asyncio.sleep(1)
        await send_with_typing(
            chat_id=message.chat.id,
            text="👑 <b>Панель администратора активирована</b>",
            bot=bot,
            parse_mode="HTML",
            reply_markup=get_admin_kb()
        )


# --- СОЗДАНИЕ ОБРАЩЕНИЯ ---
@dp.message(F.text == "🆘 Создать обращение")
async def process_cat_selection(message: Message, state: FSMContext):
    await state.set_state(BotStates.choosing_category)
    await send_with_typing(
        chat_id=message.chat.id,
        text="📁 <b>Выберите категорию вашего вопроса:</b>",
        bot=bot,
        parse_mode="HTML",
        reply_markup=get_categories_kb()
    )


@dp.callback_query(F.data.startswith("cat_"))
async def process_cat_callback(call: CallbackQuery, state: FSMContext):
    category = call.data.split("_", 1)[1]

    logger.info(f"User {call.from_user.id} selected category: {category}")

    await bot.send_chat_action(call.message.chat.id, "typing")
    await asyncio.sleep(1)

    tid = await init_ticket(call.from_user.id, bot, category)

    if tid:
        await state.set_state(BotStates.writing_issue)

        await call.message.edit_text(
            f"✅ <b>Категория '{category}' выбрана!</b>\n\n"
            f"📝 Теперь напишите ваш вопрос в чат.\n"
            f"<i>Администраторы получат уведомление и ответят вам здесь.</i>",
            parse_mode="HTML"
        )

        await send_with_typing(
            chat_id=call.message.chat.id,
            text="✍️ <b>Ожидаю ваше сообщение...</b>\n\n"
                 "<i>Напишите ваш вопрос или прикрепите файл.\n"
                 "Используйте кнопку '❌ Отмена' если передумали.</i>",
            bot=bot,
            parse_mode="HTML",
            reply_markup=get_cancel_kb()
        )
    else:
        await call.message.edit_text(
            "❌ <b>Не удалось создать обращение.</b>\n"
            "Пожалуйста, попробуйте позже или обратитесь к администратору.",
            parse_mode="HTML"
        )
    await call.answer()


# --- ПРОФИЛЬ ---
@dp.message(F.text == "👤 Мой профиль")
async def process_profile(message: Message):
    u = await db_engine.get_user(uid=message.from_user.id)

    if not u:
        await message.answer("❌ Ваш профиль не найден.")
        return

    await bot.send_chat_action(message.chat.id, "typing")
    await asyncio.sleep(0.5)

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (message.from_user.id,)) as c:
            refs = (await c.fetchone())[0]

        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT reason, created_at FROM warns_history 
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT 3
        """, (message.from_user.id,)) as c:
            warns_history = await c.fetchall()

    me = await bot.get_me()
    profile_text = (
        f"👤 <b>ВАШ АККАУНТ</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{u['anon_id']}</code>\n"
        f"⚠️ Предупреждения: <b>{u['warns']}/3</b>\n"
        f"👥 Рефералы: <b>{refs}</b>\n"
        f"📩 Сообщений: <b>{u['msg_count']}</b>\n"
        f"📅 Регистрация: <b>{u['created_at'][:10]}</b>\n"
    )

    if warns_history:
        profile_text += f"\n<b>Последние предупреждения:</b>\n"
        for warn in warns_history:
            date = warn['created_at'][:16]
            reason = warn['reason'] or "без причины"
            profile_text += f"▫️ {date}: {reason}\n"

    profile_text += (
        f"━━━━━━━━━━━━━━\n"
        f"🔗 <b>Ссылка для друзей:</b>\n"
        f"<code>https://t.me/{me.username}?start={message.from_user.id}</code>"
    )

    await send_with_typing(
        chat_id=message.chat.id,
        text=profile_text,
        bot=bot,
        parse_mode="HTML"
    )


# --- СТЕНА ОТЗЫВОВ ---
@dp.message(F.text == "📊 Стена отзывов")
@dp.message(Command("reviews"))
async def process_reviews_wall(message: Message):
    await bot.send_chat_action(message.chat.id, "typing")
    await asyncio.sleep(1)

    reviews_stats = await db_engine.get_reviews_stats()
    latest_reviews = await db_engine.get_latest_reviews(10)

    res = "🏆 <b>РЕЙТИНГ АДМИНИСТРАЦИИ:</b>\n"
    for i, a in enumerate(reviews_stats['top_admins'], 1):
        stars = "⭐" * round(a['avg_r'])
        res += f"{i}. {a['admin_alias']} — {round(a['avg_r'], 1)} {stars} ({a['cnt']} отз.)\n"

    res += f"\n📊 <b>Общая статистика:</b>\n"
    res += f"Всего отзывов: {reviews_stats['total_count']}\n"
    res += f"Средний рейтинг: {round(reviews_stats['avg_rating'] or 0, 2)}/5\n"

    res += "\n💬 <b>ПОСЛЕДНИЕ ОТЗЫВЫ:</b>\n"
    for r in latest_reviews:
        anon_id = r['anon_id'] if 'anon_id' in r else (await db_engine.get_user(uid=r['user_id']))['anon_id']
        comment_preview = r['comment'][:50] + "..." if len(r['comment']) > 50 else r['comment']
        res += f"▫️ <b>{r['admin_alias']}</b> ({r['rating']}⭐)\n"
        res += f"   👤 {anon_id}: <i>{comment_preview}</i>\n"

    await send_with_typing(
        chat_id=message.chat.id,
        text=res,
        bot=bot,
        parse_mode="HTML"
    )


# --- ОТМЕНА ---
@dp.message(F.text == "❌ Отмена")
async def process_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()

    if current_state in [BotStates.writing_issue, BotStates.choosing_category]:
        u = await db_engine.get_user(uid=message.from_user.id)

        if u and u.get('topic_id'):
            try:
                await bot.send_message(
                    ADMIN_GROUP_ID,
                    f"🔒 <b>Пользователь завершил диалог</b>\n\n"
                    f"👤 Клиент: <code>{u['anon_id']}</code>\n"
                    f"🆔 User ID: <code>{message.from_user.id}</code>\n"
                    f"📅 Завершено: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"<i>Диалог завершен пользователем через кнопку 'Отмена'.</i>",
                    message_thread_id=u['topic_id'],
                    parse_mode="HTML"
                )
                logger.info(f"User {message.from_user.id} closed ticket {u['topic_id']}")
            except Exception as e:
                logger.error(f"Error sending closure message: {e}")

            await db_engine.close_ticket(message.from_user.id)

    await state.clear()
    await send_with_typing(
        chat_id=message.chat.id,
        text="🏠 <b>Возврат в главное меню.</b>\n\n"
             "<i>Диалог с поддержкой завершен. Вы можете создать новое обращение в любое время.</i>",
        bot=bot,
        parse_mode="HTML",
        reply_markup=get_main_kb()
    )


# --- СИСТЕМА ОТЗЫВОВ ---
@dp.message(F.text == "⭐️ Оставить отзыв")
async def process_rev_1(message: Message, state: FSMContext):
    await state.set_state(BotStates.rev_adm)
    await send_with_typing(
        chat_id=message.chat.id,
        text=(
            "👤 <b>Кому из админов оставить отзыв?</b>\n\n"
            "Напишите имя или псевдоним администратора.\n"
            "<i>Пример: Иван, Алексей, Поддержка, Модератор</i>"
        ),
        bot=bot,
        parse_mode="HTML",
        reply_markup=get_cancel_kb()
    )


@dp.message(BotStates.rev_adm)
async def process_rev_2(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        return await process_cancel(message, state)

    await state.update_data(adm=message.text.strip())
    await state.set_state(BotStates.rev_rate)

    kb = InlineKeyboardBuilder()
    for i in range(1, 6):
        kb.button(text=f"{'⭐' * i}", callback_data=f"set_rate_{i}")
    kb.adjust(5)

    await send_with_typing(
        chat_id=message.chat.id,
        text=(
            f"📊 <b>Оцените {message.text.strip()}:</b>\n\n"
            f"Выберите количество звёзд от 1 до 5\n"
            f"<i>1 — плохо, 5 — отлично</i>"
        ),
        bot=bot,
        parse_mode="HTML",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(BotStates.rev_rate, F.data.startswith("set_rate_"))
async def process_rev_3(call: CallbackQuery, state: FSMContext):
    rate = int(call.data.split("_")[-1])
    await state.update_data(rate=rate)
    await state.set_state(BotStates.rev_msg)

    data = await state.get_data()
    await call.message.edit_text(
        f"✍️ <b>Напишите текст отзыва:</b>\n\n"
        f"👤 Админ: <b>{data['adm']}</b>\n"
        f"⭐ Оценка: <b>{'⭐' * rate}</b>\n\n"
        f"<i>Опишите ваш опыт взаимодействия...</i>",
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(BotStates.rev_msg)
async def process_rev_4(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        return await process_cancel(message, state)

    data = await state.get_data()
    uid = message.from_user.id

    review_id = await db_engine.add_review(uid, data['adm'], data['rate'], message.text)

    u = await db_engine.get_user(uid=uid)
    rev_msg = (
        f"🌟 <b>НОВЫЙ ОТЗЫВ #{review_id}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"👤 Клиент: <code>{u['anon_id']}</code>\n"
        f"🎯 Админ: <b>{data['adm']}</b>\n"
        f"⭐ Оценка: {'⭐' * data['rate']}\n"
        f"📝 Текст отзыва:\n<i>{message.text}</i>"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"approve_rev_{review_id}")
    kb.button(text="🗑 Удалить", callback_data=f"rem_rev_{review_id}")
    kb.adjust(2)

    await bot.send_message(
        ADMIN_GROUP_ID,
        rev_msg,
        message_thread_id=REVIEWS_TOPIC_ID,
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )

    await state.clear()
    await send_with_typing(
        chat_id=message.chat.id,
        text=(
            "✅ <b>Спасибо за ваш отзыв!</b>\n\n"
            "Ваш отзыв передан на модерацию.\n"
            "После проверки администратором он появится на стене отзывов."
        ),
        bot=bot,
        parse_mode="HTML",
        reply_markup=get_main_kb()
    )


# --- ОДОБРЕНИЕ/УДАЛЕНИЕ ОТЗЫВА ---
@dp.callback_query(F.data.startswith("approve_rev_"))
async def process_rev_approve(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    rid = call.data.split("_")[-1]
    await call.message.edit_text(f"✅ Отзыв #{rid} одобрен и опубликован.")
    await call.answer("Отзыв одобрен!")


@dp.callback_query(F.data.startswith("rem_rev_"))
async def process_rev_del(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    rid = call.data.split("_")[-1]

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM reviews WHERE id = ?", (rid,))
        await db.commit()

    if USE_SUPABASE:
        try:
            supabase.table('reviews').delete().eq('id', int(rid)).execute()
        except Exception as e:
            logger.error(f"Ошибка удаления отзыва из Supabase: {e}")

    await call.message.edit_text(f"🗑 Отзыв #{rid} удален администратором.")
    await call.answer("Отзыв удален!")


# --- РАССЫЛКА ---
@dp.callback_query(F.data == "admin_broadcast")
async def start_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    await state.set_state(BotStates.broadcasting)
    await call.message.edit_text(
        "📢 <b>Режим рассылки активирован</b>\n\n"
        "Отправьте сообщение для рассылки:\n"
        "• Текст\n"
        "• Фото с подписью\n"
        "• Видео\n"
        "• Документ\n"
        "• Аудио\n\n"
        "<i>Сообщение будет отправлено всем активным пользователям.</i>",
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(Command("cancel"), BotStates.broadcasting)
async def cancel_broadcast(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Рассылка отменена.", reply_markup=get_admin_kb())


@dp.message(BotStates.broadcasting)
async def process_broadcast_content(message: Message, state: FSMContext):
    await state.update_data(broadcast_message=message)
    await state.set_state(BotStates.broadcast_confirm)

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Начать рассылку", callback_data="confirm_broadcast")
    kb.button(text="❌ Отмена", callback_data="cancel_broadcast")
    kb.adjust(1)

    content_type = message.content_type
    preview = message.text or message.caption or f"Сообщение типа: {content_type}"
    preview = preview[:200] + "..." if len(preview) > 200 else preview

    await message.answer(
        f"📢 <b>Подтверждение рассылки</b>\n\n"
        f"📁 Тип: <b>{content_type}</b>\n"
        f"📝 Содержимое:\n{preview}\n\n"
        f"<b>Будет отправлено всем активным пользователям.</b>",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "confirm_broadcast", BotStates.broadcast_confirm)
async def confirm_broadcast(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("🔄 <b>Начинаю рассылку...</b>", parse_mode="HTML")

    data = await state.get_data()
    message_to_send = data['broadcast_message']

    user_ids = await db_engine.get_all_active_users()

    total = len(user_ids)
    success = 0
    failed = 0
    start_time = time.time()

    progress_msg = await call.message.answer(f"📊 Прогресс: 0/{total}")

    for index, user_id in enumerate(user_ids, 1):
        try:
            await copy_message_to_user(bot, user_id, message_to_send)
            success += 1

            if index % 10 == 0:
                await progress_msg.edit_text(
                    f"📊 Прогресс: {index}/{total}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {failed}"
                )
                await asyncio.sleep(0.05)

        except TelegramForbiddenError:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))
                await db.commit()

            failed += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            index -= 1
        except Exception as e:
            logger.error(f"Broadcast error for {user_id}: {e}")
            failed += 1

    content = message_to_send.text or message_to_send.caption or ""
    await db_engine.save_broadcast_stats(call.from_user.id, message_to_send.content_type, content, success, failed)

    total_time = time.time() - start_time
    await progress_msg.delete()

    await call.message.answer(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 Статистика:\n"
        f"• Всего получателей: {total}\n"
        f"• Успешно отправлено: {success}\n"
        f"• Не удалось отправить: {failed}\n"
        f"• Время выполнения: {total_time:.1f} сек.\n"
        f"• Скорость: {total / max(total_time, 0.1):.1f} сообщ/сек.",
        parse_mode="HTML",
        reply_markup=get_admin_kb()
    )

    await state.clear()


@dp.callback_query(F.data == "cancel_broadcast", BotStates.broadcast_confirm)
async def cancel_broadcast_callback(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ Рассылка отменена.", reply_markup=get_admin_kb())


# --- СТАТИСТИКА АДМИНА ---
@dp.callback_query(F.data == "admin_stats")
@dp.message(F.chat.id == ADMIN_GROUP_ID, Command("stats"))
async def adm_stats(message: Union[Message, CallbackQuery]):
    if isinstance(message, CallbackQuery):
        msg = message.message
        user_id = message.from_user.id
    else:
        msg = message
        user_id = message.from_user.id

    if user_id != OWNER_ID:
        if isinstance(message, CallbackQuery):
            await message.answer("❌ Доступ запрещен!", show_alert=True)
        return

    if isinstance(message, CallbackQuery):
        await bot.send_chat_action(msg.chat.id, "typing")
    await asyncio.sleep(1)

    active_users = await db_engine.get_active_users_count()
    today_users = await db_engine.get_today_users()
    avg_messages = await db_engine.get_avg_messages()
    top_referrers = await db_engine.get_top_referrers(5)
    daily_stats = await db_engine.get_daily_stats(7)
    reviews_stats = await db_engine.get_reviews_stats()

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1") as c: banned = (await c.fetchone())[0]
        async with db.execute("SELECT SUM(msg_count) FROM users") as c: total_msgs = (await c.fetchone())[0] or 0
        async with db.execute("SELECT SUM(warns) FROM users") as c: total_warns = (await c.fetchone())[0] or 0
        async with db.execute("SELECT COUNT(*) FROM users WHERE warns > 0") as c: warned_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals") as c: ref_total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE DATE(last_seen) = DATE('now') AND is_active = 1") as c: active_today = \
        (await c.fetchone())[0]

        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT anon_id, msg_count FROM users ORDER BY msg_count DESC LIMIT 5") as c:
            top_senders = await c.fetchall()

    stats_text = (
        f"📊 <b>СТАТИСТИКА СИСТЕМЫ</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Пользователи:</b>\n"
        f"• Всего: <b>{total}</b>\n"
        f"• Активных: <b>{active_users}</b>\n"
        f"• Активных сегодня: <b>{active_today}</b>\n"
        f"• Заблокированных: <b>{banned}</b>\n"
        f"• Новых сегодня: <b>{today_users}</b>\n"
        f"• Рефералов: <b>{ref_total}</b>\n\n"

        f"💬 <b>Сообщения:</b>\n"
        f"• Всего: <b>{total_msgs}</b>\n"
        f"• Среднее на пользователя: <b>{round(avg_messages, 1)}</b>\n\n"

        f"⚠️ <b>Предупреждения:</b>\n"
        f"• Всего варнов: <b>{total_warns}</b>\n"
        f"• Пользователей с варнами: <b>{warned_users}</b>\n\n"

        f"⭐ <b>Отзывы:</b>\n"
        f"• Всего: <b>{reviews_stats['total_count']}</b>\n"
        f"• Средний рейтинг: <b>{round(reviews_stats['avg_rating'] or 0, 2)}/5</b>\n\n"
    )

    if top_referrers:
        stats_text += f"👥 <b>Топ рефереров:</b>\n"
        for i, ref in enumerate(top_referrers, 1):
            if isinstance(ref, dict):
                anon_id = ref.get('anon_id') or f"ID:{ref.get('referrer_id')}"
                count = ref.get('count', 0)
            else:
                anon_id = ref['anon_id'] or f"ID:{ref['referrer_id']}"
                count = ref['count']
            stats_text += f"{i}. {anon_id}: {count} чел.\n"
        stats_text += "\n"

    if top_senders:
        stats_text += f"🏆 <b>Топ отправителей:</b>\n"
        for i, user in enumerate(top_senders, 1):
            stats_text += f"{i}. {user['anon_id']}: {user['msg_count']} сообщ.\n"
        stats_text += "\n"

    if daily_stats:
        stats_text += f"📈 <b>Регистрации за неделю:</b>\n"
        for stat in daily_stats:
            if isinstance(stat, dict):
                date = stat['date'][5:]
                count = stat['registrations']
            else:
                date = stat[0][5:]
                count = stat[1]
            stats_text += f"• {date}: {count} чел.\n"

    stats_text += f"\n🌐 <b>База данных:</b> {'Supabase + Локальная' if USE_SUPABASE else 'Локальная'}"

    if isinstance(message, CallbackQuery):
        await msg.edit_text(stats_text, parse_mode="HTML")
        await message.answer()
    else:
        await msg.answer(stats_text, parse_mode="HTML")


# --- КОМАНДЫ МОДЕРАЦИИ ---
@dp.message(F.chat.id == ADMIN_GROUP_ID, Command("warn"), F.is_topic_message)
async def adm_warn(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID:
        return

    args = command.args or ""
    parts = args.split(maxsplit=2)

    if len(parts) < 2:
        await message.answer(
            "⚠️ <b>Использование:</b>\n"
            "/warn <время> <причина>\n\n"
            "Примеры:\n"
            "/warn 1h Спам\n"
            "/warn 30m Грубость\n"
            "/warn 2d Нарушение правил",
            parse_mode="HTML"
        )
        return

    time_str, reason = parts[0], parts[1]

    u = await db_engine.get_user(tid=message.message_thread_id)
    if not u:
        return await message.answer("❌ Пользователь не найден!")

    w_count = await db_engine.add_warn(u['user_id'], message.from_user.id, reason)

    warn_msg = (
        f"⚠️ Пользователю <code>{u['anon_id']}</code> выдан варн ({w_count}/3).\n"
        f"Причина: <i>{reason}</i>"
    )
    await message.answer(warn_msg, parse_mode="HTML")

    user_notify = (
        f"⚠️ <b>Вам выдано предупреждение ({w_count}/3)!</b>\n\n"
        f"📋 Причина: <i>{reason}</i>\n\n"
        f"Пожалуйста, соблюдайте правила общения.\n"
        f"<i>При получении 3 предупреждений — автоматическая блокировка.</i>"
    )
    await bot.send_message(u['user_id'], user_notify, parse_mode="HTML")

    if w_count >= 3:
        await db_engine.update_user_ban(u['user_id'], True, None, '3 предупреждения')
        await message.answer(f"🚫 <b>Лимит предупреждений достигнут!</b> Пользователь забанен.")
        await bot.send_message(u['user_id'], "🚫 <b>Вы заблокированы за получение 3 предупреждений.</b>",
                               parse_mode="HTML")


@dp.message(F.chat.id == ADMIN_GROUP_ID, Command("ban"), F.is_topic_message)
async def adm_ban(message: Message, command: CommandObject):
    if message.from_user.id != OWNER_ID:
        return

    args = command.args or ""
    parts = args.split(maxsplit=2)

    if not parts:
        await message.answer(
            "🚫 <b>Использование:</b>\n"
            "/ban <время> [причина]\n\n"
            "Примеры:\n"
            "/ban 1d Спам\n"
            "/ban 2h Грубость\n"
            "/ban перманентно Нарушение правил",
            parse_mode="HTML"
        )
        return

    time_str = parts[0]
    reason = parts[1] if len(parts) > 1 else "Нарушение правил"

    u = await db_engine.get_user(tid=message.message_thread_id)
    if not u:
        return await message.answer("❌ Пользователь не найден!")

    ban_duration = parse_time(time_str)

    if ban_duration is None:
        ban_until = None
        ban_duration_text = "навсегда"
    else:
        ban_until = (datetime.now() + ban_duration).isoformat()
        ban_duration_text = format_timedelta(ban_duration)

    await db_engine.update_user_ban(u['user_id'], True, ban_until, reason)

    admin_msg = (
        f"🚫 Пользователь <code>{u['anon_id']}</code> забанен.\n"
        f"⏰ Длительность: <b>{ban_duration_text}</b>\n"
        f"📋 Причина: <i>{reason}</i>"
    )
    await message.answer(admin_msg, parse_mode="HTML")

    if ban_until:
        user_msg = (
            f"🚫 <b>Вы заблокированы!</b>\n\n"
            f"⏰ Длительность: <b>{ban_duration_text}</b>\n"
            f"📋 Причина: <i>{reason}</i>\n\n"
            f"<i>Блокировка будет снята автоматически по истечении времени.</i>"
        )
    else:
        user_msg = (
            f"🚫 <b>Вы заблокированы навсегда!</b>\n\n"
            f"📋 Причина: <i>{reason}</i>\n\n"
            f"<i>Обратитесь к администратору для разблокировки.</i>"
        )

    await bot.send_message(u['user_id'], user_msg, parse_mode="HTML")


@dp.message(F.chat.id == ADMIN_GROUP_ID, Command("unban"), F.is_topic_message)
async def adm_unban(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    u = await db_engine.get_user(tid=message.message_thread_id)
    if not u:
        return await message.answer("❌ Пользователь не найден!")

    await db_engine.update_user_ban(u['user_id'], False, None, None)

    await message.answer(f"✅ Пользователь <code>{u['anon_id']}</code> разбанен.", parse_mode="HTML")
    await bot.send_message(u['user_id'], "✅ <b>Ваша блокировка снята!</b>", parse_mode="HTML")


# --- ЭКСПОРТ ДАННЫХ ---
@dp.callback_query(F.data == "admin_export")
async def adm_export(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    await call.message.edit_text("📥 <b>Готовлю отчет...</b>", parse_mode="HTML")

    await bot.send_chat_action(call.message.chat.id, "typing")
    await asyncio.sleep(2)

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute("SELECT * FROM users ORDER BY created_at DESC") as c:
            users = await c.fetchall()

        async with db.execute("SELECT * FROM reviews ORDER BY created_at DESC") as c:
            reviews = await c.fetchall()

        async with db.execute("SELECT * FROM warns_history ORDER BY created_at DESC") as c:
            warns = await c.fetchall()

        async with db.execute("SELECT * FROM referrals ORDER BY created_at DESC") as c:
            referrals = await c.fetchall()

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Отчет системы</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; text-align: center; margin-bottom: 30px; }}
            h2 {{ color: #3498db; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 40px; }}
            .summary {{ background: #ecf0f1; padding: 20px; border-radius: 8px; margin-bottom: 30px; }}
            .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
            .summary-item {{ background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .summary-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; margin: 5px 0; }}
            .summary-label {{ color: #7f8c8d; font-size: 14px; }}
            table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #3498db; color: white; font-weight: bold; }}
            tr:nth-child(even) {{ background-color: #f8f9fa; }}
            tr:hover {{ background-color: #f1f8ff; }}
            .status-banned {{ color: #e74c3c; font-weight: bold; }}
            .status-active {{ color: #27ae60; font-weight: bold; }}
            .rating-stars {{ color: #f39c12; }}
            .timestamp {{ font-size: 12px; color: #95a5a6; }}
            @media print {{
                body {{ background: white; }}
                .container {{ box-shadow: none; }}
                .no-print {{ display: none; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Отчет системы</h1>
            <div class="summary">
                <div class="summary-grid">
                    <div class="summary-item">
                        <div class="summary-label">Всего пользователей</div>
                        <div class="summary-value">{len(users)}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Отзывов</div>
                        <div class="summary-value">{len(reviews)}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Предупреждений</div>
                        <div class="summary-value">{len(warns)}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Рефералов</div>
                        <div class="summary-value">{len(referrals)}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Дата отчета</div>
                        <div class="summary-value">{datetime.now().strftime("%d.%m.%Y %H:%M")}</div>
                    </div>
                </div>
            </div>
    """

    html_content += "<h2>👥 Пользователи</h2>"
    html_content += """
    <table>
        <tr>
            <th>ID</th>
            <th>Anon ID</th>
            <th>Регистрация</th>
            <th>Последняя активность</th>
            <th>Сообщения</th>
            <th>Варны</th>
            <th>Статус</th>
            <th>Реферал</th>
        </tr>
    """

    for user in users:
        status = "BANNED" if user['is_banned'] else ("ACTIVE" if user['is_active'] else "INACTIVE")
        status_class = "status-banned" if user['is_banned'] else "status-active"
        last_seen = user['last_seen'][:19] if user['last_seen'] else "никогда"

        html_content += f"""
        <tr>
            <td>{user['user_id']}</td>
            <td><b>{user['anon_id']}</b></td>
            <td>{user['created_at'][:19]}</td>
            <td class="timestamp">{last_seen}</td>
            <td>{user['msg_count']}</td>
            <td>{user['warns']}</td>
            <td class="{status_class}">{status}</td>
            <td>{user['referrer_id'] or '-'}</td>
        </tr>
        """

    html_content += "</table>"

    if referrals:
        html_content += "<h2>👥 Рефералы</h2>"
        html_content += """
        <table>
            <tr>
                <th>ID</th>
                <th>Реферер</th>
                <th>Приглашенный</th>
                <th>Дата</th>
            </tr>
        """

        for ref in referrals:
            html_content += f"""
            <tr>
                <td>{ref['id']}</td>
                <td>{ref['referrer_id']}</td>
                <td>{ref['referred_id']}</td>
                <td class="timestamp">{ref['created_at'][:19]}</td>
            </tr>
            """

        html_content += "</table>"

    if reviews:
        html_content += "<h2>⭐ Отзывы</h2>"
        html_content += """
        <table>
            <tr>
                <th>ID</th>
                <th>Пользователь</th>
                <th>Админ</th>
                <th>Оценка</th>
                <th>Комментарий</th>
                <th>Дата</th>
            </tr>
        """

        for review in reviews:
            stars = "★" * review['rating'] + "☆" * (5 - review['rating'])
            html_content += f"""
            <tr>
                <td>{review['id']}</td>
                <td>{review['user_id']}</td>
                <td><b>{review['admin_alias']}</b></td>
                <td class="rating-stars">{stars} ({review['rating']}/5)</td>
                <td>{review['comment']}</td>
                <td class="timestamp">{review['created_at'][:19]}</td>
            </tr>
            """

        html_content += "</table>"

    if warns:
        html_content += "<h2>⚠️ История предупреждений</h2>"
        html_content += """
        <table>
            <tr>
                <th>ID</th>
                <th>Пользователь</th>
                <th>Админ</th>
                <th>Причина</th>
                <th>Дата</th>
            </tr>
        """

        for warn in warns:
            html_content += f"""
            <tr>
                <td>{warn['id']}</td>
                <td>{warn['user_id']}</td>
                <td>{warn['admin_id']}</td>
                <td>{warn['reason'] or 'Не указана'}</td>
                <td class="timestamp">{warn['created_at'][:19]}</td>
            </tr>
            """

        html_content += "</table>"

    html_content += """
            <div class="no-print" style="margin-top: 40px; text-align: center; color: #95a5a6; font-size: 12px;">
                <p>Отчет сгенерирован автоматически системой Spok Elite Support</p>
                <p>База данных: {DB_NAME}{' + Supabase' if USE_SUPABASE else ''}</p>
                <p>Для обновления данных перезапустите генерацию отчета</p>
            </div>
        </div>
    </body>
    </html>
    """

    file = BufferedInputFile(html_content.encode('utf-8'),
                             filename=f"spok_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")

    await call.message.answer_document(
        document=file,
        caption=(
            "📊 <b>Детальный отчет системы</b>\n\n"
            f"📅 Сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"👥 Пользователей: {len(users)}\n"
            f"⭐ Отзывов: {len(reviews)}\n"
            f"⚠️ Предупреждений: {len(warns)}\n"
            f"👥 Рефералов: {len(referrals)}\n"
            f"🌐 База данных: {'Supabase + Локальная' if USE_SUPABASE else 'Локальная'}"
        ),
        parse_mode="HTML"
    )
    await call.answer()


# --- СИНХРОНИЗАЦИЯ ---
@dp.callback_query(F.data == "admin_sync")
async def admin_sync(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    if not USE_SUPABASE:
        return await call.answer("❌ Supabase не настроен!", show_alert=True)

    await call.message.edit_text("🔄 <b>Начинаю синхронизацию...</b>", parse_mode="HTML")

    try:
        # Синхронизируем пользователей
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users") as c:
                users = await c.fetchall()

        synced = 0
        errors = 0

        for user in users:
            try:
                user_dict = dict(user)
                user_data = {
                    'user_id': user_dict['user_id'],
                    'anon_id': user_dict['anon_id'],
                    'topic_id': user_dict.get('topic_id'),
                    'referrer_id': user_dict.get('referrer_id'),
                    'warns': user_dict['warns'],
                    'is_banned': bool(user_dict['is_banned']),
                    'ban_until': user_dict.get('ban_until'),
                    'ban_reason': user_dict.get('ban_reason'),
                    'is_active': bool(user_dict['is_active']),
                    'msg_count': user_dict['msg_count'],
                    'created_at': user_dict['created_at'],
                    'last_seen': user_dict['last_seen'],
                    'source_db': 'sqlite'
                }

                existing = supabase.table('users').select('*').eq('user_id', user_dict['user_id']).execute()

                if existing.data and len(existing.data) > 0:
                    supabase.table('users').update(user_data).eq('user_id', user_dict['user_id']).execute()
                else:
                    supabase.table('users').insert(user_data).execute()

                synced += 1

            except Exception as e:
                errors += 1
                logger.error(f"Ошибка синхронизации пользователя {user_dict.get('user_id')}: {e}")

        await call.message.answer(
            f"✅ <b>Синхронизация завершена!</b>\n\n"
            f"📊 Результаты:\n"
            f"• Пользователей синхронизировано: {synced}\n"
            f"• Ошибок: {errors}\n"
            f"• База данных: Supabase + Локальная",
            parse_mode="HTML",
            reply_markup=get_admin_kb()
        )
    except Exception as e:
        await call.message.answer(
            f"❌ <b>Ошибка синхронизации:</b>\n\n<code>{str(e)}</code>",
            parse_mode="HTML",
            reply_markup=get_admin_kb()
        )

    await call.answer()


# --- GATEWAY ПЕРЕПИСКИ ---
@dp.message(F.chat.type == "private")
async def gateway_u2a(message: Message, state: FSMContext):
    if message.content_type in [
        ContentType.FORUM_TOPIC_CREATED,
        ContentType.FORUM_TOPIC_EDITED,
        ContentType.FORUM_TOPIC_CLOSED,
        ContentType.FORUM_TOPIC_REOPENED,
        ContentType.GENERAL_FORUM_TOPIC_HIDDEN,
        ContentType.GENERAL_FORUM_TOPIC_UNHIDDEN
    ]:
        return

    current_state = await state.get_state()

    if current_state == BotStates.writing_issue:
        u = await db_engine.get_user(uid=message.from_user.id)
        if not u or not u.get('topic_id'):
            await message.answer("❌ Сначала создайте обращение через меню!")
            return

        try:
            copied_message = await copy_message_to_admin(bot, message.from_user.id, message, u['topic_id'])

            if copied_message:
                await db_engine.increment_message_count(message.from_user.id)

                try:
                    await message.react([ReactionTypeEmoji(emoji="✅")])
                except:
                    logger.warning(f"Cannot react to user message {message.message_id}")

                logger.info(f"Message from user {message.from_user.id} copied to admin")
            else:
                logger.error(f"Failed to copy message from user {message.from_user.id} to admin")
                try:
                    await message.react([ReactionTypeEmoji(emoji="❌")])
                except:
                    pass

        except Exception as e:
            logger.error(f"U2A gateway error: {e}")
            try:
                await message.react([ReactionTypeEmoji(emoji="❌")])
            except:
                pass
        return

    protected_buttons = ["🆘 Создать обращение", "⭐️ Оставить отзыв", "📊 Стена отзывов", "👤 Мой профиль", "❌ Отмена"]

    if message.text and (message.text.startswith("/") or message.text in protected_buttons):
        return

    await message.answer(
        "⚠️ <b>Сначала создайте обращение!</b>\n\n"
        "1. Нажмите кнопку <b>'🆘 Создать обращение'</b>\n"
        "2. Выберите категорию вопроса\n"
        "3. Напишите ваш вопрос в чат\n\n"
        "<i>После этого администраторы смогут вам ответить.</i>",
        parse_mode="HTML",
        reply_markup=get_main_kb()
    )


@dp.message(F.chat.id == ADMIN_GROUP_ID, F.is_topic_message)
async def gateway_a2u(message: Message):
    if message.text and message.text.startswith("/"):
        return

    if message.content_type in [
        ContentType.FORUM_TOPIC_CREATED,
        ContentType.FORUM_TOPIC_EDITED,
        ContentType.FORUM_TOPIC_CLOSED,
        ContentType.FORUM_TOPIC_REOPENED,
        ContentType.GENERAL_FORUM_TOPIC_HIDDEN,
        ContentType.GENERAL_FORUM_TOPIC_UNHIDDEN
    ]:
        return

    u = await db_engine.get_user(tid=message.message_thread_id)
    if u:
        try:
            await copy_message_to_user(bot, u['user_id'], message)
            await safe_set_reaction(bot, ADMIN_GROUP_ID, message.message_id, "✅")
            logger.info(f"Message from admin copied to user {u['user_id']}")

        except TelegramForbiddenError:
            logger.warning(f"User {u['user_id']} blocked the bot")
            await safe_set_reaction(bot, ADMIN_GROUP_ID, message.message_id, "❌")

            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (u['user_id'],))
                await db.commit()

                if USE_SUPABASE:
                    try:
                        update_data = {
                            'user_id': u['user_id'],
                            'is_active': False
                        }
                        supabase.table('users').update(update_data).eq('user_id', u['user_id']).execute()
                    except Exception as e:
                        logger.error(f"Ошибка синхронизации деактивации: {e}")

        except Exception as e:
            logger.error(f"A2U gateway error: {e}")
            await safe_set_reaction(bot, ADMIN_GROUP_ID, message.message_id, "❌")


# --- ОЧИСТКА КЭША ---
@dp.callback_query(F.data == "admin_clear_cache")
async def clear_cache(call: CallbackQuery):
    if call.from_user.id != OWNER_ID:
        return await call.answer("❌ Доступ запрещен!", show_alert=True)

    try:
        if isinstance(dp.storage, MemoryStorage):
            dp.storage._data.clear()
            dp.storage._chat_data.clear()
            dp.storage._user_data.clear()

        import gc
        gc.collect()

        await call.answer("✅ Кэш очищен!", show_alert=True)
    except Exception as e:
        logger.error(f"Clear cache error: {e}")
        await call.answer("❌ Ошибка очистки кэша!", show_alert=True)


# --- ЗАПУСК ---
async def on_start():
    await db_engine.initialize()
    logger.info("✅ SYSTEM ONLINE (V15 + Supabase)")

    await bot.set_my_commands([
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="reviews", description="📊 Смотреть отзывы"),
        BotCommand(command="stats", description="📈 Статистика (админ)"),
    ])

    if OWNER_ID:
        try:
            await bot.send_message(
                OWNER_ID,
                "🤖 <b>Бот успешно запущен!</b>\n\n"
                f"Версия: <code>v15 - Supabase Integration</code>\n"
                f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                f"Локальная база: {DB_NAME}\n"
                f"Supabase: {'✅ Подключен' if USE_SUPABASE else '❌ Не подключен'}\n\n"
                f"✅ Копирование сообщений вместо пересылки\n"
                f"✅ Стикеры видны админам (копируются)\n"
                f"✅ Все медиафайлы копируются\n"
                f"✅ Автоматическое закрытие тикетов\n"
                f"✅ Логирование рефералов\n"
                f"✅ Реакции подтверждения\n"
                f"✅ Новые топики для каждого обращения\n"
                f"✅ Интеграция с Supabase",
                parse_mode="HTML"
            )
        except:
            pass


async def main():
    await on_start()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹ Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"💥 Critical error: {e}")
