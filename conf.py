# -*- coding: utf-8 -*-
import os
import re
import uuid
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest

import asyncpg
from asyncpg import Pool

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)

# =============================== CONFIG =======================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", ""))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
TZ = ZoneInfo(TIMEZONE)

# ========================== DATABASE CONNECTION ================================

db_pool: Optional[Pool] = None

async def init_db():
    """Инициализация пула подключений к базе данных"""
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=60
    )
    return db_pool

async def close_db():
    """Закрытие пула подключений"""
    global db_pool
    if db_pool:
        await db_pool.close()

# =============================== HELPERS =======================================

async def safe_edit_message(message: Message, text: str, reply_markup=None, parse_mode=None):
    """Безопасное редактирование сообщения с обработкой ошибки 'message is not modified'."""
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

def now_kyiv() -> datetime:
    """Возвращает текущее время в timezone Киева С timezone info (aware)
    PostgreSQL TIMESTAMPTZ корректно обрабатывает aware datetime"""
    from datetime import timezone as tz_module
    utc_now = datetime.now(tz_module.utc)
    # Конвертируем в киевское время и возвращаем aware datetime
    return utc_now.astimezone(TZ)

def iso_dt(dt: Optional[datetime] = None) -> str:
    """Конвертирует datetime в строку в киевском времени"""
    dt = dt or now_kyiv()
    # Если datetime имеет timezone info, конвертируем в киевское время
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%Y-%m-%d %H:%M")

def parse_dt(s: str) -> Optional[datetime]:
    """Парсит строку в aware datetime (с timezone info для Киева)"""
    try:
        naive_dt = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        # Корректно локализуем в киевскую зону (учитывает DST)
        # Используем конструктор datetime с tzinfo напрямую
        return datetime(
            naive_dt.year, naive_dt.month, naive_dt.day,
            naive_dt.hour, naive_dt.minute, naive_dt.second,
            naive_dt.microsecond, tzinfo=TZ
        )
    except Exception:
        return None

def fmt_date(dt: datetime) -> str:
    """Форматирует дату в киевском времени"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%d.%m.%Y")

def fmt_time(dt: datetime) -> str:
    """Форматирует время в киевском времени"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%H:%M")

def short_uuid(n: int = 8) -> str:
    return uuid.uuid4().hex[:n]

PHONE_RE = re.compile(r"^(?:\+?38)?0?\d{9}$|^380\d{9}$")

def normalize_phone(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("380") and len(digits) == 12:
        return digits
    if digits.startswith("0") and len(digits) == 10:
        return "38" + digits
    if len(digits) == 9:
        return "380" + digits
    return None

def a2i(v: Any, default: int = 0) -> int:
    try:
        return int(str(v).strip()) if v is not None else default
    except Exception:
        return default

# =============================== DATABASE LAYER ==================================

async def messages_get(key: str, lang: str = "uk") -> str:
    """Получение сообщения из БД по ключу"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT text FROM messages WHERE key = $1 AND lang = $2",
                key, lang
            )
            if row:
                return str(row['text']).replace("\\n", "\n")
    except Exception:
        pass

    FALLBACKS = {
        "invite.title": "Запрошення на конференцію: {title}",
        "invite.body": "Шановний(-а) {name}!\n\nЗапрошуємо Вас на конференцію: {title}\n🗓 Дата: {date}\n⏰ Час: {time} (за київським часом)\nℹ️ {description}\n\nБудь ласка, підтвердіть Вашу участь за допомогою кнопок нижче.",
        "reminder.60m": "⏰ Нагадування: через 1 годину почнеться конференція {title}.\n🔗 Посилання для підключення: {link}",
        "feedback.ask": "Дякуємо за участь у конференції «{title}»!\n\nБудь ласка, оцініть захід за шкалою від 1 до 5 зірок.\nВи також можете залишити коментар.",
        "reminder.24h": "🔔 Нагадування: завтра о {time} відбудеться конференція {title}.\n🔗 Посилання для підключення: {link}",
        "update.notice": "🛠 Інформація про зміни\n\nУ конференції «{title}» відбулися зміни:\n{what}\n\nДякуємо за розуміння!",
        "cancel.notice": "❌ Інформація про скасування\n\nКонференцію «{title}» скасовано.\nМи повідомимо Вас про нову дату найближчим часом.",
        "help.body": "Вітаємо!\n\nЦей бот призначений для надсилання запрошень на наші онлайн-конференції.\n\nВи отримуватимете запрошення та нагадування про заходи.\n\nКнопки під повідомленням:\n• ✅ Так, буду — підтвердити участь (Ви отримаєте нагадування за 24 години та за 1 годину до початку)\n• 🚫 Не зможу — повідомити про відсутність (Ви зможете обрати альтернативну дату)\n• 🔔 Нагадати за 24 год — якщо Ви ще не визначилися",
    }
    return FALLBACKS.get(key, "")

async def log_action(action: str, client_id: Optional[int] = None,
               event_id: Optional[int] = None, details: str = "") -> None:
    """Запись действия в лог"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO delivery_log (ts, client_id, event_id, action, details)
                   VALUES ($1, $2, $3, $4, $5)""",
                now_kyiv(), client_id, event_id, action, details
            )
    except Exception as e:
        print(f"Error logging action: {e}")

async def has_log(action: str, client_id: int, event_id: int) -> bool:
    """Проверка наличия записи в логе"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT 1 FROM delivery_log
                   WHERE action = $1 AND client_id = $2 AND event_id = $3
                   LIMIT 1""",
                action, client_id, event_id
            )
            return row is not None
    except Exception:
        return False

async def get_eventtypes_active() -> List[Dict[str, Any]]:
    """Получение активных типов событий"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM event_types WHERE active = TRUE"
        )
        return [dict(row) for row in rows]

async def get_eventtype_by_code(type_code: int) -> Optional[Dict[str, Any]]:
    """Получение типа события по коду"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM event_types WHERE type_code = $1 AND active = TRUE",
            type_code
        )
        return dict(row) if row else None

def client_id_for_tg(tg_user_id: int) -> str:
    """Генерация client_id (для обратной совместимости, теперь используем INT)"""
    return f"cl_{tg_user_id}"

async def get_client_by_tg(tg_user_id: int) -> Optional[Dict[str, Any]]:
    """Получение клиента по Telegram ID"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE tg_user_id = $1",
            tg_user_id
        )
        return dict(row) if row else None

async def upsert_client(tg_user_id: int, full_name: str, phone: str, status: str = "active") -> Dict[str, Any]:
    """Создание или обновление клиента"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO clients (tg_user_id, phone, full_name, status, created_at, last_seen_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (tg_user_id)
               DO UPDATE SET
                   phone = EXCLUDED.phone,
                   full_name = EXCLUDED.full_name,
                   status = EXCLUDED.status,
                   last_seen_at = EXCLUDED.last_seen_at
               RETURNING *""",
            tg_user_id, phone, full_name, status, now, now
        )
        client = dict(row)
        await log_action("client_registered", client_id=client['client_id'], details=f"tg={tg_user_id}")
        return client

async def touch_client_seen(tg_user_id: int) -> None:
    """Обновление времени последнего визита"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET last_seen_at = $1 WHERE tg_user_id = $2",
            now_kyiv(), tg_user_id
        )

async def list_active_clients() -> List[Dict[str, Any]]:
    """Получение списка активных клиентов"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM clients WHERE status = 'active'"
        )
        return [dict(row) for row in rows]

async def create_event(type_code: int, title: str, description: str, start_at: str,
                 duration_min: int, link: str, created_by: int) -> Dict[str, Any]:
    """Создание события"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO events (type, title, description, start_at, duration_min, link, created_by, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING *""",
            type_code, title, description, parse_dt(start_at), duration_min, link, created_by, now_kyiv()
        )
        event = dict(row)
        event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
        await log_action("event_created", event_id=event['event_id'], details=f"type={type_code}")
        return event

async def get_all_events() -> List[Dict[str, Any]]:
    """Получение всех событий"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM events ORDER BY start_at")
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def get_event_by_id(event_id: int) -> Optional[Dict[str, Any]]:
    """Получение события по ID"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM events WHERE event_id = $1",
            event_id
        )
        if row:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            return event
        return None

async def update_event_field(event_id: int, field: str, value: Any) -> None:
    """Обновление поля события"""
    # Защита от SQL injection - используем белый список полей
    allowed_fields = {'title', 'description', 'start_at', 'duration_min', 'link'}
    if field not in allowed_fields:
        return

    async with db_pool.acquire() as conn:
        if field == 'start_at':
            value = parse_dt(value) if isinstance(value, str) else value

        query = f"UPDATE events SET {field} = $1 WHERE event_id = $2"
        await conn.execute(query, value, event_id)
        await log_action("event_updated", event_id=event_id, details=f"{field}={value}")

async def delete_event(event_id: int) -> None:
    """Удаление события"""
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM events WHERE event_id = $1", event_id)
        await log_action("event_canceled", event_id=event_id, details="deleted")

def event_start_dt(event: Dict[str, Any]) -> Optional[datetime]:
    """Получение datetime начала события (aware datetime в киевском времени)"""
    start_at = event.get("start_at")
    if isinstance(start_at, datetime):
        # Если есть timezone info — конвертируем в киевское время
        if start_at.tzinfo:
            return start_at.astimezone(TZ)
        else:
            # Корректная локализация naive datetime (предполагаем, что это уже киевское время)
            return datetime(
                start_at.year, start_at.month, start_at.day,
                start_at.hour, start_at.minute, start_at.second,
                start_at.microsecond, tzinfo=TZ
            )
    if isinstance(start_at, str):
        return parse_dt(start_at)
    return None

async def list_future_events_sorted() -> List[Dict[str, Any]]:
    """Получение будущих событий, отсортированных по дате"""
    now = now_kyiv()
    one_day_ago = now - timedelta(days=1)
    async with db_pool.acquire() as conn:
        # Убираем ::timestamp cast - asyncpg корректно обрабатывает aware datetime для TIMESTAMPTZ
        rows = await conn.fetch(
            "SELECT * FROM events WHERE start_at >= $1 ORDER BY start_at",
            one_day_ago
        )
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def list_alternative_events_same_type(type_code: int, exclude_event_id: int) -> List[Dict[str, Any]]:
    """Получение альтернативных событий того же типа"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM events
               WHERE type = $1 AND event_id != $2 AND start_at >= $3
               ORDER BY start_at""",
            type_code, exclude_event_id, now
        )
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def mark_attendance(event_id: int, client_id: int, attended: bool = True) -> None:
    """Отметка посещения"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO attendance (event_id, client_id, attended, marked_at)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET attended = EXCLUDED.attended, marked_at = EXCLUDED.marked_at""",
            event_id, client_id, attended, now_kyiv()
        )
        await log_action("attendance_marked", client_id=client_id, event_id=event_id, details=f"attended={attended}")

    # Проверяем, это type_code 4 и клиент посетил (attended=True)?
    if attended:
        event = await get_event_by_id(event_id)
        if event and event.get('type') == 4:
            # Считаем сколько раз клиент посетил type_code 4
            count = await count_client_attendance_for_type(client_id, 4)

            # Если это 3-е или больше посещение - отправляем опрос
            if count >= 3:
                await send_documents_collected_survey(client_id)

async def attendance_clear_for_event(event_id: int, mode: str = "zero") -> int:
    """Очистка записей о посещении для события"""
    async with db_pool.acquire() as conn:
        if mode == "delete":
            result = await conn.execute("DELETE FROM attendance WHERE event_id = $1", event_id)
        else:
            result = await conn.execute(
                "UPDATE attendance SET attended = FALSE, marked_at = $1 WHERE event_id = $2",
                now_kyiv(), event_id
            )

        touched = int(result.split()[-1]) if result else 0
        await log_action("attendance_cleared_on_cancel", event_id=event_id, details=f"mode={mode}; rows={touched}")
        return touched

async def client_has_attended_type(client_id: int, type_code: int) -> bool:
    """Проверка, посещал ли клиент событие данного типа"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT 1 FROM attendance a
               JOIN events e ON a.event_id = e.event_id
               WHERE a.client_id = $1 AND e.type = $2 AND a.attended = TRUE
               LIMIT 1""",
            client_id, type_code
        )
        return row is not None

async def count_client_attendance_for_type(client_id: int, type_code: int) -> int:
    """Подсчет количества посещений клиентом конференций определенного типа"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT COUNT(*) as count
               FROM attendance a
               JOIN events e ON a.event_id = e.event_id
               WHERE a.client_id = $1 AND e.type = $2 AND a.attended = TRUE""",
            client_id, type_code
        )
        return row['count'] if row else 0

async def count_client_confirmed_today_by_type(client_id: int, type_code: int) -> int:
    """Подсчет количества подтвержденных событий (rsvp='going') СЕГОДНЯ для данного type_code"""
    today_start = now_kyiv().replace(hour=0, minute=0, second=0, microsecond=0)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT COUNT(*) as count
               FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1
                 AND e.type = $2
                 AND r.rsvp = 'going'
                 AND r.rsvp_at >= $3""",
            client_id, type_code, today_start
        )
        return row['count'] if row else 0

async def get_client_by_id(client_id: int) -> Optional[Dict[str, Any]]:
    """Получение клиента по client_id"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE client_id = $1",
            client_id
        )
        return dict(row) if row else None

async def set_documents_collected(client_id: int, value: bool = True) -> None:
    """Установка флага 'документы собраны'"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET documents_collected = $1 WHERE client_id = $2",
            value, client_id
        )
    await log_action("documents_collected_flag_set", client_id=client_id, details=f"value={value}")

async def send_documents_collected_survey(client_id: int) -> None:
    """Отправка опроса после 3+ посещения type_code 4"""
    client = await get_client_by_id(client_id)
    if not client:
        return

    tg_id = client.get('tg_user_id')
    if not tg_id:
        return

    text = (
        "Ви вже відвідали кілька конференцій зі збору документів! 🎉\n\n"
        "Чи зібрали ви всі необхідні документи?"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✅ Так, все зібрано",
            callback_data=f"docs_survey:yes:{client_id}"
        )],
        [InlineKeyboardButton(
            text="📋 Ні, потрібна допомога",
            callback_data=f"docs_survey:no:{client_id}"
        )]
    ])

    try:
        await bot.send_message(chat_id=int(tg_id), text=text, reply_markup=keyboard)
        await log_action("documents_survey_sent", client_id=client_id)
    except Exception as e:
        await log_action("documents_survey_error", client_id=client_id, details=str(e))

async def rsvp_upsert(event_id: int, client_id: int, rsvp: Optional[str] = None,
                remind_24h: Optional[bool] = None,
                reminded_24h: Optional[bool] = None,
                reminded_60m: Optional[bool] = None) -> None:
    """Создание или обновление RSVP"""
    async with db_pool.acquire() as conn:
        # Сначала получаем текущие значения, если запись существует
        current = await conn.fetchrow(
            "SELECT * FROM rsvp WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )

        # Используем текущие значения, если новые не предоставлены
        if current:
            rsvp_val = rsvp if rsvp is not None else current['rsvp']
            remind_24h_val = remind_24h if remind_24h is not None else current['remind_24h']
            reminded_24h_val = reminded_24h if reminded_24h is not None else current['reminded_24h']
            reminded_60m_val = reminded_60m if reminded_60m is not None else current['reminded_60m']
        else:
            rsvp_val = rsvp or ""
            remind_24h_val = remind_24h or False
            reminded_24h_val = reminded_24h or False
            reminded_60m_val = reminded_60m or False

        await conn.execute(
            """INSERT INTO rsvp (event_id, client_id, rsvp, remind_24h, reminded_24h, reminded_60m, rsvp_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET
                   rsvp = EXCLUDED.rsvp,
                   remind_24h = EXCLUDED.remind_24h,
                   reminded_24h = EXCLUDED.reminded_24h,
                   reminded_60m = EXCLUDED.reminded_60m,
                   rsvp_at = EXCLUDED.rsvp_at""",
            event_id, client_id, rsvp_val, remind_24h_val, reminded_24h_val, reminded_60m_val, now_kyiv()
        )

async def rsvp_get_for_event(event_id: int) -> List[Dict[str, Any]]:
    """Получение RSVP для события"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM rsvp WHERE event_id = $1",
            event_id
        )
        return [dict(row) for row in rows]

async def rsvp_get_for_client(client_id: int) -> List[Dict[str, Any]]:
    """Получение RSVP для клиента"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM rsvp WHERE client_id = $1",
            client_id
        )
        return [dict(row) for row in rows]

async def client_has_active_invite_for_type(client_id: int, type_code: int) -> bool:
    """Проверка наличия активного приглашения для типа события"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT 1 FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1
                 AND e.type = $2
                 AND e.start_at >= $3
                 AND (r.rsvp = '' OR r.rsvp = 'going')
               LIMIT 1""",
            client_id, type_code, now
        )
        return row is not None

async def client_has_confirmed_event_at_time(client_id: int, start_dt: datetime, duration_min: int) -> bool:
    """Проверка, есть ли у клиента подтвержденная конференция на это же время"""
    async with db_pool.acquire() as conn:
        # Проверяем пересечение интервалов времени
        # Новая конференция: [start_dt, start_dt + duration_min]
        # Существующая: [e.start_at, e.start_at + e.duration_min]
        end_dt = start_dt + timedelta(minutes=duration_min)

        row = await conn.fetchrow(
            """SELECT 1 FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1
                 AND r.rsvp = 'going'
                 AND e.start_at < $2
                 AND (e.start_at + (e.duration_min || ' minutes')::INTERVAL) > $3
               LIMIT 1""",
            client_id, end_dt, start_dt
        )
        return row is not None

async def is_earliest_upcoming_event_of_type(event: Dict[str, Any]) -> bool:
    """Проверка, является ли событие самым ранним предстоящим событием данного типа"""
    now = now_kyiv()
    event_type = event.get('type')
    dt_this = event_start_dt(event)

    if not dt_this:
        return False

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT event_id FROM events
               WHERE type = $1 AND start_at >= $2
               ORDER BY start_at
               LIMIT 1""",
            event_type, now
        )
        return row and row['event_id'] == event.get('event_id')

async def feedback_get(event_id: int, client_id: int) -> Optional[Dict[str, Any]]:
    """Получение отзыва"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM feedback WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )
        return dict(row) if row else None

async def feedback_upsert(event_id: int, client_id: int, stars: Optional[int] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """Создание или обновление отзыва"""
    async with db_pool.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT * FROM feedback WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )

        if current:
            stars_val = stars if stars is not None else current['stars']
            comment_val = comment if comment is not None else current['comment']
            owner_val = current['owner']
        else:
            stars_val = stars or 0
            comment_val = comment or ""
            owner_val = ""

        row = await conn.fetchrow(
            """INSERT INTO feedback (event_id, client_id, stars, comment, owner, created_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET
                   stars = EXCLUDED.stars,
                   comment = EXCLUDED.comment
               RETURNING *""",
            event_id, client_id, stars_val, comment_val, owner_val, now_kyiv()
        )
        return dict(row)

async def feedback_assign_owner(event_id: int, client_id: int, owner: str) -> None:
    """Назначение ответственного за отзыв"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE feedback SET owner = $1 WHERE event_id = $2 AND client_id = $3",
            owner, event_id, client_id
        )

async def try_get_tg_from_client_id(client_id: int) -> Optional[int]:
    """Получение Telegram ID по client_id"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT tg_user_id FROM clients WHERE client_id = $1",
            client_id
        )
        return row['tg_user_id'] if row else None

async def get_event_statistics(event_id: int) -> Dict[str, Any]:
    """Получение статистики по событию"""
    async with db_pool.acquire() as conn:
        # Количество отправленных приглашений
        invitations_sent = await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_log WHERE action = 'invite_sent' AND event_id = $1",
            event_id
        )

        # Подтвержденные участники
        confirmed = await conn.fetch(
            """SELECT c.client_id, c.full_name, c.phone
               FROM rsvp r
               JOIN clients c ON r.client_id = c.client_id
               WHERE r.event_id = $1 AND r.rsvp = 'going'""",
            event_id
        )

        confirmed_clients = [
            {
                "client_id": row['client_id'],
                "full_name": row['full_name'] or "—",
                "phone": row['phone'] or "—"
            }
            for row in confirmed
        ]

        return {
            "invitations_sent": invitations_sent or 0,
            "confirmed_count": len(confirmed_clients),
            "confirmed_clients": confirmed_clients
        }

async def build_types_overview_text(cli: Dict[str, Any]) -> str:
    """Построение обзорного текста по типам событий"""
    rows = await get_eventtypes_active()
    if not rows:
        return "📋 Ваш прогрес конференцій:\n\nНа даний момент немає активних типів конференцій."

    num_emojis = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣"}
    client_id = cli['client_id']

    lines = []
    done_count = 0
    for rt in rows:
        tcode = rt.get("type_code")
        title = str(rt.get("title"))
        num = num_emojis.get(tcode, "▪️")
        attended = await client_has_attended_type(client_id, tcode)

        if attended:
            flag = "✅"
            done_count += 1
        else:
            flag = "❌"
        lines.append(f"{num} {title} — {flag}")

    total = len(rows)
    text = (
        "📋 Ваш прогрес конференцій:\n\n"
        + "\n".join(lines)
        + f"\n\n━━━━━━━━━━━━━━━\n✅ Пройдено: {done_count} з {total}"
    )
    return text

async def get_client_statistics(client_id: int) -> Dict[str, Any]:
    """Получение статистики клиента"""
    async with db_pool.acquire() as conn:
        # Количество посещенных конференций
        attended_count = await conn.fetchval(
            "SELECT COUNT(*) FROM attendance WHERE client_id = $1 AND attended = TRUE",
            client_id
        )

        # Список посещенных событий с деталями
        attended_events = await conn.fetch(
            """SELECT e.event_id, e.title, e.start_at, e.type
               FROM attendance a
               JOIN events e ON a.event_id = e.event_id
               WHERE a.client_id = $1 AND a.attended = TRUE
               ORDER BY e.start_at DESC""",
            client_id
        )

        # Количество подтвержденных (но не посещенных еще) конференций
        confirmed_count = await conn.fetchval(
            """SELECT COUNT(*) FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1 AND r.rsvp = 'going' AND e.start_at >= $2""",
            client_id, now_kyiv()
        )

        # Список подтвержденных будущих событий
        confirmed_events = await conn.fetch(
            """SELECT e.event_id, e.title, e.start_at, e.type
               FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1 AND r.rsvp = 'going' AND e.start_at >= $2
               ORDER BY e.start_at""",
            client_id, now_kyiv()
        )

        # Типы конференций, которые клиент посетил
        attended_types = await conn.fetch(
            """SELECT DISTINCT e.type, et.title
               FROM attendance a
               JOIN events e ON a.event_id = e.event_id
               JOIN event_types et ON e.type = et.type_code
               WHERE a.client_id = $1 AND a.attended = TRUE""",
            client_id
        )

        # Все типы конференций
        all_types = await get_eventtypes_active()

        return {
            "attended_count": attended_count or 0,
            "attended_events": [dict(row) for row in attended_events],
            "confirmed_count": confirmed_count or 0,
            "confirmed_events": [dict(row) for row in confirmed_events],
            "attended_types": [dict(row) for row in attended_types],
            "total_types": len(all_types),
            "completed_types": len(attended_types)
        }

async def list_clients_by_filter(filter_type: str = "all") -> List[Dict[str, Any]]:
    """Получение списка клиентов по фильтру"""
    async with db_pool.acquire() as conn:
        if filter_type == "all":
            # Все активные клиенты
            rows = await conn.fetch(
                """SELECT c.*,
                   (SELECT COUNT(*) FROM attendance a WHERE a.client_id = c.client_id AND a.attended = TRUE) as attended_count
                   FROM clients c
                   WHERE c.status = 'active'
                   ORDER BY c.last_seen_at DESC"""
            )
        elif filter_type == "completed":
            # Клиенты, прошедшие все типы конференций
            all_types = await get_eventtypes_active()
            total_types = len(all_types)

            rows = await conn.fetch(
                """SELECT c.*,
                   COUNT(DISTINCT e.type) as completed_types,
                   COUNT(*) as attended_count
                   FROM clients c
                   JOIN attendance a ON c.client_id = a.client_id
                   JOIN events e ON a.event_id = e.event_id
                   WHERE c.status = 'active' AND a.attended = TRUE
                   GROUP BY c.client_id
                   HAVING COUNT(DISTINCT e.type) >= $1
                   ORDER BY c.last_seen_at DESC""",
                total_types
            )
        elif filter_type == "active":
            # Клиенты с подтвержденными будущими конференциями
            rows = await conn.fetch(
                """SELECT DISTINCT c.*,
                   (SELECT COUNT(*) FROM attendance a WHERE a.client_id = c.client_id AND a.attended = TRUE) as attended_count
                   FROM clients c
                   JOIN rsvp r ON c.client_id = r.client_id
                   JOIN events e ON r.event_id = e.event_id
                   WHERE c.status = 'active'
                   AND r.rsvp = 'going'
                   AND e.start_at >= $1
                   ORDER BY c.last_seen_at DESC""",
                now_kyiv()
            )
        elif filter_type == "never":
            # Клиенты, которые не были ни на одной конференции
            rows = await conn.fetch(
                """SELECT c.*, 0 as attended_count
                   FROM clients c
                   WHERE c.status = 'active'
                   AND NOT EXISTS (
                       SELECT 1 FROM attendance a
                       WHERE a.client_id = c.client_id AND a.attended = TRUE
                   )
                   ORDER BY c.created_at DESC"""
            )
        else:
            rows = []

        return [dict(row) for row in rows]

# ===================== NEW FEATURES: INFO, BROADCAST, MOTIVATIONAL =============

async def get_client_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """Отримання клієнта по номеру телефону"""
    normalized = normalize_phone(phone)
    if not normalized:
        return None

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE phone = $1",
            normalized
        )
        return dict(row) if row else None

async def get_client_full_info(client_id: int) -> Dict[str, Any]:
    """Отримання повної інформації про клієнта для команди /info"""
    async with db_pool.acquire() as conn:
        # Основна інформація про клієнта
        client = await conn.fetchrow(
            "SELECT * FROM clients WHERE client_id = $1", client_id
        )
        if not client:
            return None

        client_data = dict(client)

        # Історія конференцій (з деталями типів)
        conferences_history = await conn.fetch(
            """SELECT e.event_id, e.title, e.type, et.title AS type_name,
                      e.start_at, a.attended, a.marked_at, r.rsvp
               FROM events e
               LEFT JOIN event_types et ON e.type = et.type_code
               LEFT JOIN attendance a ON e.event_id = a.event_id AND a.client_id = $1
               LEFT JOIN rsvp r ON e.event_id = r.event_id AND r.client_id = $1
               WHERE (a.attended = TRUE OR r.rsvp IN ('going', 'declined'))
               ORDER BY e.start_at DESC""",
            client_id
        )

        # Історія запрошень з delivery_log
        invitations_history = await conn.fetch(
            """SELECT dl.ts, dl.event_id, dl.action, dl.details, e.title
               FROM delivery_log dl
               LEFT JOIN events e ON dl.event_id = e.event_id
               WHERE dl.client_id = $1
                 AND dl.action IN ('invite_sent', 'rsvp_yes', 'rsvp_no', 'reminded_24h', 'reminded_60m')
               ORDER BY dl.ts DESC
               LIMIT 20""",
            client_id
        )

        # Оцінки та коментарі
        feedback_list = await conn.fetch(
            """SELECT e.title, e.start_at, f.stars, f.comment, f.created_at, f.owner
               FROM feedback f
               JOIN events e ON f.event_id = e.event_id
               WHERE f.client_id = $1
               ORDER BY e.start_at DESC""",
            client_id
        )

        # Статистика - рахуємо всі запрошення з логів відправки
        total_invites = await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_log WHERE client_id = $1 AND action = 'invite_sent'",
            client_id
        )

        attended_count = await conn.fetchval(
            "SELECT COUNT(*) FROM attendance WHERE client_id = $1 AND attended = TRUE",
            client_id
        )

        declined_count = await conn.fetchval(
            "SELECT COUNT(*) FROM rsvp WHERE client_id = $1 AND rsvp = 'declined'",
            client_id
        )

        return {
            "client": client_data,
            "conferences": [dict(row) for row in conferences_history],
            "invitations": [dict(row) for row in invitations_history],
            "feedback": [dict(row) for row in feedback_list],
            "total_invites": total_invites or 0,
            "attended_count": attended_count or 0,
            "declined_count": declined_count or 0
        }

async def format_client_info_message(info: Dict[str, Any]) -> str:
    """Форматування повідомлення з інформацією про клієнта"""
    client = info["client"]
    conferences = info["conferences"]
    feedback = info["feedback"]
    invitations = info["invitations"]

    # Персональні дані
    status_emoji = "✅ Активний" if client['status'] == 'active' else "❌ Неактивний"

    text = f"""📊 ІНФОРМАЦІЯ ПРО КЛІЄНТА

👤 Персональні дані:
• ПІБ: {client['full_name']}
• Телефон: {client['phone']}
• Telegram ID: {client['tg_user_id']}
• Статус: {status_emoji}
• Реєстрація: {fmt_date(client['created_at'])} {fmt_time(client['created_at'])}
• Остання активність: {fmt_date(client['last_seen_at'])} {fmt_time(client['last_seen_at'])}

📈 Статистика:
• Всього запрошень: {info['total_invites']}
• Відвідано: {info['attended_count']} конференцій
• Відхилено: {info['declined_count']} запрошень
"""

    text += "\n━━━━━━━━━━━━━━━━━━━━━\n\n"

    # Історія конференцій по типах
    if conferences:
        text += "📅 ІСТОРІЯ КОНФЕРЕНЦІЙ (по типах):\n\n"

        # Групуємо по типах
        by_type = {}
        for conf in conferences:
            type_code = conf['type']
            if type_code not in by_type:
                by_type[type_code] = []
            by_type[type_code].append(conf)

        type_icons = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣"}

        for type_code in sorted(by_type.keys()):
            confs = by_type[type_code]
            type_name = confs[0].get('type_name', f'Тип {type_code}')
            icon = type_icons.get(type_code, "▪️")

            text += f"{icon} {type_name.upper()}\n"

            for conf in confs:
                if conf.get('attended'):
                    visit_emoji = "✅"
                    date_str = fmt_date(conf['start_at']) + " " + fmt_time(conf['start_at'])
                    text += f"   {visit_emoji} {date_str} — Відвідав\n"

                    # Шукаємо оцінку для цієї конференції
                    fb = next((f for f in feedback if f['title'] == conf['title']), None)
                    if fb and fb.get('stars'):
                        stars = "⭐️" * fb['stars']
                        text += f"   {stars} Оцінка: {fb['stars']} зірок\n"
                        if fb.get('comment'):
                            comment_preview = fb['comment'][:50] + "..." if len(fb['comment']) > 50 else fb['comment']
                            text += f"   💬 \"{comment_preview}\"\n"
                        if fb.get('owner'):
                            text += f"   👤 Скаргу взяв у роботу: {fb['owner']}\n"
                elif conf.get('rsvp') == 'declined':
                    text += f"   ❌ {fmt_date(conf['start_at'])} — Відмовився\n"

            text += "\n"

    text += "━━━━━━━━━━━━━━━━━━━━━\n\n"

    # Історія запрошень (скорочена)
    if invitations:
        text += "📬 ІСТОРІЯ ЗАПРОШЕНЬ (останні 10):\n\n"

        for i, inv in enumerate(invitations[:10], 1):
            action_text = {
                'invite_sent': '📨 Запрошення',
                'rsvp_yes': '✅ Підтвердив',
                'rsvp_no': '❌ Відмовився',
                'reminded_24h': '🔔 Нагадування 24г',
                'reminded_60m': '🔔 Нагадування 60хв'
            }.get(inv['action'], inv['action'])

            event_title = inv.get('title', 'невідома подія')
            text += f"{i}. {iso_dt(inv['ts'])} — {action_text} ({event_title})\n"

        text += "\n━━━━━━━━━━━━━━━━━━━━━\n\n"

    # Всі оцінки та коментарі
    if feedback:
        text += "⭐️ ВСІ ОЦІНКИ ТА КОМЕНТАРІ:\n\n"

        for fb in feedback:
            text += f"{fmt_date(fb['start_at'])} — {fb['title']}\n"
            stars = "⭐️" * fb['stars']
            text += f"{stars} ({fb['stars']}/5)\n"
            if fb.get('comment'):
                text += f"💬 {fb['comment']}\n"
            if fb.get('owner'):
                text += f"👤 В роботі у: {fb['owner']}\n"
            text += "\n"

        # Середня оцінка
        avg_rating = sum(f['stars'] for f in feedback) / len(feedback)
        text += f"━━━━━━━━━━━━━━━━━━━━━\n\n📊 СЕРЕДНЯ ОЦІНКА: {avg_rating:.1f}/5\n"

    return text

async def get_broadcast_segment_clients(segment: str) -> List[Dict[str, Any]]:
    """Отримання списку клієнтів для певного сегменту розсилки"""
    async with db_pool.acquire() as conn:
        if segment == "all":
            # Всі активні клієнти
            rows = await conn.fetch(
                "SELECT * FROM clients WHERE status = 'active' ORDER BY created_at DESC"
            )

        elif segment == "never":
            # Ніколи не відвідували
            rows = await conn.fetch(
                """SELECT c.*
                   FROM clients c
                   WHERE c.status = 'active'
                   AND NOT EXISTS (
                       SELECT 1 FROM attendance a
                       WHERE a.client_id = c.client_id AND a.attended = TRUE
                   )
                   ORDER BY c.created_at DESC"""
            )

        elif segment.startswith("type_"):
            # Відвідали певний тип конференції
            type_code = int(segment.split("_")[1])
            rows = await conn.fetch(
                """SELECT DISTINCT c.*
                   FROM clients c
                   JOIN attendance a ON c.client_id = a.client_id
                   JOIN events e ON a.event_id = e.event_id
                   WHERE c.status = 'active'
                   AND e.type = $1
                   AND a.attended = TRUE
                   ORDER BY c.created_at DESC""",
                type_code
            )

        elif segment == "completed":
            # Відвідали ВСІ типи
            total_types = await conn.fetchval(
                "SELECT COUNT(*) FROM event_types WHERE active = TRUE"
            )
            rows = await conn.fetch(
                """SELECT c.*
                   FROM clients c
                   JOIN attendance a ON c.client_id = a.client_id
                   JOIN events e ON a.event_id = e.event_id
                   WHERE c.status = 'active' AND a.attended = TRUE
                   GROUP BY c.client_id
                   HAVING COUNT(DISTINCT e.type) >= $1
                   ORDER BY c.last_seen_at DESC""",
                total_types
            )

        elif segment == "inactive_30":
            # Неактивні 30+ днів БЕЗ тих, хто завершив всі типи
            total_types = await conn.fetchval(
                "SELECT COUNT(*) FROM event_types WHERE active = TRUE"
            )
            rows = await conn.fetch(
                """SELECT c.*
                   FROM clients c
                   WHERE c.status = 'active'
                   AND c.last_seen_at < NOW() - INTERVAL '30 days'
                   AND (
                       SELECT COUNT(DISTINCT e.type)
                       FROM attendance a
                       JOIN events e ON a.event_id = e.event_id
                       WHERE a.client_id = c.client_id AND a.attended = TRUE
                   ) < $1
                   ORDER BY c.last_seen_at ASC""",
                total_types
            )

        elif segment == "low_ratings":
            # З низькими оцінками (<4)
            rows = await conn.fetch(
                """SELECT DISTINCT c.*
                   FROM clients c
                   JOIN feedback f ON c.client_id = f.client_id
                   WHERE c.status = 'active'
                   AND f.stars < 4
                   ORDER BY c.created_at DESC"""
            )

        else:
            rows = []

        return [dict(row) for row in rows]

async def get_inactive_clients_for_motivation() -> List[Dict[str, Any]]:
    """Отримання неактивних клієнтів для мотивуючих повідомлень"""
    async with db_pool.acquire() as conn:
        total_types = await conn.fetchval(
            "SELECT COUNT(*) FROM event_types WHERE active = TRUE"
        )

        rows = await conn.fetch(
            """SELECT c.client_id, c.full_name, c.phone, c.tg_user_id, c.last_seen_at, c.created_at,
                   COUNT(DISTINCT a.event_id) AS attended_count,
                   MAX(e.start_at) AS last_event_date
               FROM clients c
               LEFT JOIN attendance a ON c.client_id = a.client_id AND a.attended = TRUE
               LEFT JOIN events e ON a.event_id = e.event_id
               WHERE c.status = 'active'
               AND c.created_at < NOW() - INTERVAL '7 days'
               AND NOT EXISTS (
                   SELECT 1 FROM rsvp r
                   JOIN events e2 ON r.event_id = e2.event_id
                   WHERE r.client_id = c.client_id
                   AND r.rsvp = 'going'
                   AND e2.start_at > NOW()
               )
               GROUP BY c.client_id
               HAVING COUNT(DISTINCT e.type) < $1
               AND (MAX(e.start_at) IS NULL OR MAX(e.start_at) < NOW() - INTERVAL '30 days')
               AND COUNT(DISTINCT a.event_id) < 3""",
            total_types
        )

        return [dict(row) for row in rows]

async def get_last_motivational_message(client_id: int) -> Optional[Dict[str, Any]]:
    """Отримання останнього мотивуючого повідомлення для клієнта"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT ts, details
               FROM delivery_log
               WHERE client_id = $1 AND action = 'motivational_sent'
               ORDER BY ts DESC
               LIMIT 1""",
            client_id
        )
        if row:
            import json
            details = json.loads(row['details']) if isinstance(row['details'], str) else row['details']
            return {"ts": row['ts'], "details": details}
        return None

async def send_broadcast_to_clients(clients: List[Dict[str, Any]], message_text: str,
                                   segment: str, manager_id: int,
                                   progress_callback=None) -> Dict[str, Any]:
    """Відправка розсилки клієнтам з прогресом"""
    import json

    total = len(clients)
    sent = 0
    failed = 0
    blocked = []

    for i, client in enumerate(clients):
        try:
            await bot.send_message(client['tg_user_id'], message_text, parse_mode=None)
            sent += 1

            # Логування
            await log_action(
                "broadcast_sent",
                client_id=client['client_id'],
                details=json.dumps({"segment": segment, "manager_id": manager_id})
            )

            # Затримка для rate limiting (30 msg/sec = ~35ms)
            await asyncio.sleep(0.035)

        except TelegramForbiddenError:
            # Клієнт заблокував бота
            failed += 1
            blocked.append(client)

            # Позначаємо клієнта неактивним
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE clients SET status = 'inactive' WHERE client_id = $1",
                    client['client_id']
                )

            await log_action(
                "broadcast_failed",
                client_id=client['client_id'],
                details="blocked_bot"
            )

        except Exception as e:
            failed += 1
            await log_action(
                "broadcast_failed",
                client_id=client['client_id'],
                details=str(e)
            )

        # Callback для оновлення прогресу
        if progress_callback and (i + 1) % 10 == 0:
            await progress_callback(i + 1, total)

    return {
        "total": total,
        "sent": sent,
        "failed": failed,
        "blocked": blocked
    }

# Глобальна змінна для контролю мотивуючих повідомлень
MOTIVATIONAL_ENABLED = True

async def send_motivational_messages():
    """Відправка мотивуючих повідомлень неактивним клієнтам (запускається в scheduler)"""
    import json

    if not MOTIVATIONAL_ENABLED:
        return

    try:
        inactive_clients = await get_inactive_clients_for_motivation()

        for client in inactive_clients:
            # Отримати останнє мотивуюче повідомлення
            last_motivational = await get_last_motivational_message(client['client_id'])

            if last_motivational is None:
                # Перше повідомлення
                next_key = "motivational.1"
                days_since_registration = (now_kyiv() - client['created_at']).days

                if days_since_registration < 7:
                    continue
            else:
                # Перевірити, чи пройшло 3 дні
                days_since_last = (now_kyiv() - last_motivational['ts']).days

                if days_since_last < 3:
                    continue

                # Наступне повідомлення
                last_key = last_motivational['details'].get('message_key', 'motivational.1')
                last_number = int(last_key.split('.')[1])

                if last_number >= 5:
                    continue  # Вже відправили всі 5

                next_key = f"motivational.{last_number + 1}"

            # Відправити повідомлення
            message_text = await messages_get(next_key, 'uk')

            if not message_text:
                continue

            try:
                await bot.send_message(client['tg_user_id'], message_text, parse_mode=None)

                # Логуємо
                await log_action(
                    "motivational_sent",
                    client_id=client['client_id'],
                    details=json.dumps({"message_key": next_key})
                )

            except TelegramForbiddenError:
                # Клієнт заблокував бота
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE clients SET status = 'inactive' WHERE client_id = $1",
                        client['client_id']
                    )

            except Exception:
                pass

    except Exception as e:
        print(f"Error in send_motivational_messages: {e}")

async def get_motivational_statistics(days: int = 30) -> Dict[str, Any]:
    """Отримання статистики мотивуючих повідомлень"""
    import json

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT
                   details,
                   COUNT(*) as sent_count,
                   COUNT(CASE
                       WHEN EXISTS (
                           SELECT 1 FROM attendance a
                           JOIN events e ON a.event_id = e.event_id
                           WHERE a.client_id = dl.client_id
                             AND a.attended = TRUE
                             AND e.start_at BETWEEN dl.ts AND dl.ts + INTERVAL '7 days'
                       ) THEN 1
                   END) as conversion_count
               FROM delivery_log dl
               WHERE dl.action = 'motivational_sent'
               AND dl.ts >= NOW() - INTERVAL '1 day' * $1
               GROUP BY details
               ORDER BY details""",
            days
        )

        stats = []
        for row in rows:
            details = json.loads(row['details']) if isinstance(row['details'], str) else row['details']
            message_key = details.get('message_key', 'unknown')

            stats.append({
                "message_key": message_key,
                "sent_count": row['sent_count'],
                "conversion_count": row['conversion_count'],
                "conversion_rate": (row['conversion_count'] / row['sent_count'] * 100) if row['sent_count'] > 0 else 0
            })

        return {"stats": stats, "days": days}

# ============================== KEYBOARDS ======================================

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати конференцію", callback_data="admin:add")],
        [InlineKeyboardButton(text="📞 Кастомна конференція", callback_data="admin:custom")],
        [InlineKeyboardButton(text="📋 Список конференцій", callback_data="admin:list:0")],
        [InlineKeyboardButton(text="👥 Клієнти", callback_data="admin:clients:menu")],
        [InlineKeyboardButton(text="📢 Розсилка", callback_data="broadcast:menu")],
        [InlineKeyboardButton(text="💬 Мотивуючі повідомлення", callback_data="motivational:menu")],
    ])

def kb_rsvp(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Буду", callback_data=f"rsvp:{event_id}:going"),
            InlineKeyboardButton(text="❌ Не зможу", callback_data=f"rsvp:{event_id}:declined"),
        ]
    ])

def kb_event_actions(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ℹ️ Інфо", callback_data=f"admin:info:{event_id}")],
        [InlineKeyboardButton(text="✏️ Змінити", callback_data=f"admin:edit:{event_id}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data=f"admin:cancel:{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_edit_event_menu(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Назва", callback_data=f"admin:edit:{event_id}:field:title")],
        [InlineKeyboardButton(text="✏️ Опис", callback_data=f"admin:edit:{event_id}:field:description")],
        [InlineKeyboardButton(text="🗓 Дата/час", callback_data=f"admin:edit:{event_id}:field:start_at")],
        [InlineKeyboardButton(text="⏱ Тривалість (хв)", callback_data=f"admin:edit:{event_id}:field:duration_min")],
        [InlineKeyboardButton(text="🔗 Посилання", callback_data=f"admin:edit:{event_id}:field:link")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_cancel_confirm(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Так, скасувати", callback_data=f"admin:cancel:{event_id}:yes")],
        [InlineKeyboardButton(text="⬅️ Ні, назад", callback_data=f"admin:edit:{event_id}")],
    ])

def kb_claim_feedback(event_id: int, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛠 Беру в роботу", callback_data=f"claim:{event_id}:{client_id}")],
    ])

def kb_event_info(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Оновити", callback_data=f"admin:info:{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin:event:{event_id}")],
    ])

def kb_client_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📋 Мої конференції")]],
        resize_keyboard=True
    )

def kb_clients_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Всі клієнти", callback_data="admin:clients:list:all:0")],
        [InlineKeyboardButton(text="✅ Пройшли всі конфи", callback_data="admin:clients:list:completed:0")],
        [InlineKeyboardButton(text="🔄 Активні (є майбутні)", callback_data="admin:clients:list:active:0")],
        [InlineKeyboardButton(text="❌ Не були ні на одній", callback_data="admin:clients:list:never:0")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")],
    ])

def kb_client_detail(client_id: int, status: str = "active", documents_collected: bool = False) -> InlineKeyboardMarkup:
    # Кнопка блокировки/разблокировки
    if status == "active":
        toggle_btn = InlineKeyboardButton(text="🚫 Заблокувати від розсилок", callback_data=f"admin:client:block:{client_id}")
    else:
        toggle_btn = InlineKeyboardButton(text="✅ Розблокувати розсилки", callback_data=f"admin:client:unblock:{client_id}")

    return InlineKeyboardMarkup(inline_keyboard=[
        [toggle_btn],
        [InlineKeyboardButton(text="⬅️ Назад до списку", callback_data="admin:clients:menu")],
    ])

# Клавіатури для розсилок
def kb_broadcast_segments() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Всі активні клієнти", callback_data="broadcast:segment:all")],
        [InlineKeyboardButton(text="2️⃣ Ніколи не відвідували", callback_data="broadcast:segment:never")],
        [InlineKeyboardButton(text="3️⃣ Відвідали ЗБІР ДОКУМЕНТІВ (тип 1)", callback_data="broadcast:segment:type_1")],
        [InlineKeyboardButton(text="4️⃣ Відвідали СЛУЖБА БЕЗПЕКИ (тип 2)", callback_data="broadcast:segment:type_2")],
        [InlineKeyboardButton(text="5️⃣ Відвідали ПІДГОТОВКА ІСТОРІЇ (тип 3)", callback_data="broadcast:segment:type_3")],
        [InlineKeyboardButton(text="6️⃣ Відвідали ДОКУМЕНТИ РАЗОМ (тип 4)", callback_data="broadcast:segment:type_4")],
        [InlineKeyboardButton(text="7️⃣ Відвідали ВСІ типи (завершили)", callback_data="broadcast:segment:completed")],
        [InlineKeyboardButton(text="8️⃣ Неактивні 30+ днів", callback_data="broadcast:segment:inactive_30")],
        [InlineKeyboardButton(text="9️⃣ З низькими оцінками (<4)", callback_data="broadcast:segment:low_ratings")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin:home")],
    ])

def kb_broadcast_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Продовжити", callback_data="broadcast:confirm:yes")],
        [InlineKeyboardButton(text="🔙 Обрати інший", callback_data="broadcast:menu")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin:home")],
    ])

def kb_broadcast_preview() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 ЗАПУСТИТИ", callback_data="broadcast:send:confirm")],
        [InlineKeyboardButton(text="✏️ Редагувати", callback_data="broadcast:edit:text")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin:home")],
    ])

# Клавіатури для мотивуючих повідомлень
def kb_motivational_menu() -> InlineKeyboardMarkup:
    global MOTIVATIONAL_ENABLED
    toggle_text = "⏸ Призупинити розсилку" if MOTIVATIONAL_ENABLED else "▶️ Увімкнути розсилку"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Редагувати тексти", callback_data="motivational:edit:menu")],
        [InlineKeyboardButton(text="📊 Статистика відправок", callback_data="motivational:stats")],
        [InlineKeyboardButton(text=toggle_text, callback_data="motivational:toggle")],
        [InlineKeyboardButton(text="🧪 Тестова відправка", callback_data="motivational:test:menu")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin:home")],
    ])

def kb_motivational_edit_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Повідомлення №1 (день 7)", callback_data="motivational:edit:1")],
        [InlineKeyboardButton(text="2️⃣ Повідомлення №2 (день 10)", callback_data="motivational:edit:2")],
        [InlineKeyboardButton(text="3️⃣ Повідомлення №3 (день 13)", callback_data="motivational:edit:3")],
        [InlineKeyboardButton(text="4️⃣ Повідомлення №4 (день 16)", callback_data="motivational:edit:4")],
        [InlineKeyboardButton(text="5️⃣ Повідомлення №5 (день 19)", callback_data="motivational:edit:5")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="motivational:menu")],
    ])

def kb_motivational_edit_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Зберегти", callback_data="motivational:save:yes")],
        [InlineKeyboardButton(text="✏️ Редагувати", callback_data="motivational:save:edit")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="motivational:menu")],
    ])

def kb_motivational_test_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Повідомлення №1", callback_data="motivational:test:1")],
        [InlineKeyboardButton(text="2️⃣ Повідомлення №2", callback_data="motivational:test:2")],
        [InlineKeyboardButton(text="3️⃣ Повідомлення №3", callback_data="motivational:test:3")],
        [InlineKeyboardButton(text="4️⃣ Повідомлення №4", callback_data="motivational:test:4")],
        [InlineKeyboardButton(text="5️⃣ Повідомлення №5", callback_data="motivational:test:5")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="motivational:menu")],
    ])

def kb_custom_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Надіслати", callback_data="custom:confirm:yes")],
        [InlineKeyboardButton(text="✏️ Змінити номери", callback_data="custom:confirm:edit")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin:home")],
    ])

# ============================== STATE / MEMORY =================================

ADMINS: set[int] = set()

class RegisterSG(StatesGroup):
    wait_name = State()
    wait_phone = State()

class AdminAddSG(StatesGroup):
    menu = State()
    wait_title = State()
    wait_desc = State()
    wait_start_at = State()
    wait_duration = State()
    wait_link = State()

class AdminEditFieldSG(StatesGroup):
    wait_value = State()

class FeedbackSG(StatesGroup):
    wait_comment = State()

class BroadcastSG(StatesGroup):
    wait_message = State()
    preview = State()

class MotivationalEditSG(StatesGroup):
    wait_text = State()
    preview = State()

class CustomConfSG(StatesGroup):
    wait_title = State()
    wait_desc = State()
    wait_start_at = State()
    wait_duration = State()
    wait_link = State()
    wait_phones = State()
    confirm = State()

# ================================ BOT/DP =======================================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=str(TZ))

# =============================== HANDLERS ======================================

@dp.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await touch_client_seen(m.from_user.id)
    args = (m.text or "").split(maxsplit=1)
    arg = ""
    if len(args) > 1:
        arg = args[1].strip()

    # Адмін-режим
    if arg.startswith("admin_"):
        pwd = arg.split("admin_", 1)[1]
        if pwd == ADMIN_PASSWORD:
            ADMINS.add(m.from_user.id)
            await m.answer("Вітаю в адмін-панелі.", reply_markup=kb_admin_main())
            return
        else:
            await m.answer("Невірний пароль для адмін-панелі.")
            return

    # Клієнтський режим
    cli = await get_client_by_tg(m.from_user.id)
    if not cli or not cli.get("full_name") or not cli.get("phone"):
        await state.set_state(RegisterSG.wait_name)
        await m.answer("Доброго дня! Будь ласка, вкажіть Ваше прізвище, ім'я та по батькові.")
        return

    await send_welcome_and_types_list(m, cli)

async def send_welcome_and_types_list(m: Message, cli: Dict[str, Any]):
    text = await build_types_overview_text(cli)
    await m.answer(text, reply_markup=kb_client_main())

@dp.message(Command("help"))
async def cmd_help(m: Message):
    text = await messages_get("help.body")
    await m.answer(text)

# ---------- Реєстрація клієнта ----------

@dp.message(RegisterSG.wait_name)
async def reg_wait_name(m: Message, state: FSMContext):
    full_name = (m.text or "").strip()
    if len(full_name) < 3:
        await m.answer("Будь ласка, введіть повне прізвище, ім'я та по батькові.")
        return
    await state.update_data(full_name=full_name)
    await state.set_state(RegisterSG.wait_phone)
    await m.answer("Будь ласка, вкажіть номер телефону у форматі 380XXXXXXXXX:")

@dp.message(RegisterSG.wait_phone)
async def reg_wait_phone(m: Message, state: FSMContext):
    phone = normalize_phone(m.text or "")
    if not phone:
        await m.answer("Невірний формат номера. Приклад: 380671234567. Будь ласка, спробуйте ще раз:")
        return
    data = await state.get_data()
    cli = await upsert_client(m.from_user.id, data["full_name"], phone)
    await state.clear()
    await send_welcome_and_types_list(m, cli)
    await send_pending_invites_to_new_client(cli)

@dp.message(F.text == "📋 Мої конференції")
async def show_my_conferences(m: Message):
    cli = await get_client_by_tg(m.from_user.id)
    if not cli:
        await m.answer("Будь ласка, зареєструйтеся за допомогою команди /start.", reply_markup=kb_client_main())
        return
    text = await build_types_overview_text(cli)
    await m.answer(text, reply_markup=kb_client_main())

# ---------- Адмін меню / додати / список / редагування ----------

@dp.callback_query(F.data == "admin:add")
async def admin_add(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    types = await get_eventtypes_active()
    if not types:
        await q.message.edit_text("Немає активних типів конференцій.", reply_markup=kb_admin_main())
        await q.answer()
        return
    buttons = [[InlineKeyboardButton(text=t["title"], callback_data=f"admin:add:type:{t['type_code']}")] for t in types]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")])
    await q.message.edit_text("Оберіть тип конференції:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:add:type:"))
async def admin_add_select_type(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    type_code = int(q.data.split(":")[-1])
    et = await get_eventtype_by_code(type_code)
    if not et:
        await q.message.edit_text("Тип не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return
    payload = {
        "type_code": type_code,
        "type_title": et["title"],
        "title": et["title"],
        "description": et["description"],
    }
    await state.set_state(AdminAddSG.menu)
    await state.update_data(**payload)
    await q.message.edit_text(
        f"Базові дані підставлено з довідника:\n"
        f"• Тип: {payload['type_title']}\n• Назва: {payload['title']}\n• Опис: {payload['description']}\n\n"
        f"Можете підправити та натиснути «➡️ Далі».",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")],
        ])
    )
    await q.answer()

@dp.callback_query(F.data == "admin:add:edit_title")
async def admin_add_edit_title(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_title)
    await q.message.edit_text("Надішліть нову назву конференції:")
    await q.answer()

@dp.message(AdminAddSG.wait_title)
async def admin_add_wait_title(m: Message, state: FSMContext):
    title = (m.text or "").strip()
    await state.update_data(title=title)
    data = await state.get_data()
    await state.set_state(AdminAddSG.menu)
    await m.answer(
        f"Назву оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\n"
        f"Натисніть «➡️ Далі» або змініть інше поле.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
        ])
    )

@dp.callback_query(F.data == "admin:add:edit_desc")
async def admin_add_edit_desc(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_desc)
    await q.message.edit_text("Надішліть новий опис конференції:")
    await q.answer()

@dp.message(AdminAddSG.wait_desc)
async def admin_add_wait_desc(m: Message, state: FSMContext):
    desc = (m.text or "").strip()
    await state.update_data(description=desc)
    data = await state.get_data()
    await state.set_state(AdminAddSG.menu)
    await m.answer(
        f"Опис оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\n"
        f"Натисніть «➡️ Далі» або змініть інше поле.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
        ])
    )

@dp.callback_query(F.data == "admin:add:next")
async def admin_add_next(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_start_at)
    await q.message.edit_text("Вкажіть дату та час початку у форматі: YYYY-MM-DD HH:MM (Київ). Напр.: 2025-10-05 15:00")
    await q.answer()

@dp.message(AdminAddSG.wait_start_at)
async def admin_add_wait_start_at(m: Message, state: FSMContext):
    dt = parse_dt(m.text or "")
    if not dt:
        await m.answer("Невірний формат. Приклад: 2025-10-05 15:00 (Київ). Спробуйте ще раз:")
        return
    await state.update_data(start_at=iso_dt(dt))
    await state.set_state(AdminAddSG.wait_duration)
    await m.answer("Вкажіть тривалість у хвилинах (ціле число):")

@dp.message(AdminAddSG.wait_duration)
async def admin_add_wait_duration(m: Message, state: FSMContext):
    try:
        dur = int((m.text or "").strip())
        if dur <= 0:
            raise ValueError()
    except Exception:
        await m.answer("Вкажіть додатне ціле число хвилин. Спробуйте ще раз:")
        return
    await state.update_data(duration_min=dur)
    await state.set_state(AdminAddSG.wait_link)
    await m.answer("Вставте посилання на конференцію (URL):")

@dp.message(AdminAddSG.wait_link)
async def admin_add_wait_link(m: Message, state: FSMContext):
    link = (m.text or "").strip()
    data = await state.get_data()
    created = await create_event(
        type_code=int(data["type_code"]),
        title=data["title"],
        description=data["description"],
        start_at=data["start_at"],
        duration_min=int(data["duration_min"]),
        link=link,
        created_by=m.from_user.id,
    )
    await send_initial_invites_for_event(created)
    await state.clear()
    await m.answer(
        f"✅ Подію створено:\n"
        f"• {created['title']}\n"
        f"• Дата/час: {created['start_at']} (Київ)\n"
        f"• Тривалість: {created['duration_min']} хв\n"
        f"• Посилання: {created['link']}\n",
        reply_markup=kb_admin_main()
    )

@dp.callback_query(F.data == "admin:home")
async def admin_home(q: CallbackQuery):
    # Додаємо користувача в ADMINS якщо він натискає кнопки адмін-панелі
    ADMINS.add(q.from_user.id)
    await q.message.edit_text("Адмін-панель:", reply_markup=kb_admin_main())
    await q.answer()

@dp.callback_query(F.data.startswith("admin:list:"))
async def admin_list(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    page = int(q.data.split(":")[-1])
    events = await list_future_events_sorted()
    per = 10
    total = len(events)
    start = page * per
    end = start + per
    subset = events[start:end]
    if not subset and page != 0:
        page = 0
        start, end = 0, per
        subset = events[start:end]
    buttons = []
    for e in subset:
        dt = event_start_dt(e)
        dt_str = dt.strftime("%Y-%m-%d %H:%M") if dt else "—"
        buttons.append([InlineKeyboardButton(text=f"{e['title']} — {dt_str}", callback_data=f"admin:event:{e['event_id']}")])
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:list:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:list:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🏠 Головне меню", callback_data="admin:home")])
    await q.message.edit_text(f"Список конференцій (усього: {total}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:event:"))
async def admin_event_open(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    event_id = int(parts[-1])
    e = await get_event_by_id(event_id)
    if not e:
        await q.message.edit_text("Подію не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return
    await q.message.edit_text(
        f"Подія:\n• {e['title']}\n• Опис: {e['description']}\n• Початок: {e['start_at']}\n"
        f"• Тривалість: {e['duration_min']} хв\n• Посилання: {e['link']}",
        reply_markup=kb_event_actions(event_id)
    )
    await q.answer()

@dp.callback_query(F.data.startswith("admin:info:"))
async def admin_info(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    event_id = int(parts[-1])
    e = await get_event_by_id(event_id)
    if not e:
        await q.message.edit_text("Подію не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return

    stats = await get_event_statistics(event_id)

    text = (
        f"ℹ️ Статистика події\n\n"
        f"📌 Подія: {e['title']}\n"
        f"🗓 Початок: {e['start_at']}\n\n"
        f"📊 Статистика:\n"
        f"• Відправлено запрошень: {stats['invitations_sent']}\n"
        f"• Підтвердили участь: {stats['confirmed_count']}\n"
    )

    if stats['confirmed_clients']:
        text += f"\n✅ Підтвердили участь:\n"
        for i, cli in enumerate(stats['confirmed_clients'], 1):
            text += f"{i}. {cli['full_name']} ({cli['phone']})\n"
    else:
        text += f"\n⚠️ Ще ніхто не підтвердив участь\n"

    await q.message.edit_text(text, reply_markup=kb_event_info(event_id))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:edit:"))
async def admin_edit(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) == 3:
        event_id = int(parts[-1])
        await q.message.edit_text("Оберіть поле для редагування:", reply_markup=kb_edit_event_menu(event_id))
        await q.answer()
        return
    if len(parts) == 5 and parts[3] == "field":
        event_id = int(parts[2])
        field = parts[4]
        await state.set_state(AdminEditFieldSG.wait_value)
        await state.update_data(event_id=event_id, field=field)
        prompts = {
            "title": "Введіть нову назву:",
            "description": "Введіть новий опис:",
            "start_at": "Введіть нову дату/час у форматі YYYY-MM-DD HH:MM:",
            "duration_min": "Введіть нову тривалість у хвилинах:",
            "link": "Вставте нове посилання на конференцію:",
        }
        await q.message.edit_text(prompts.get(field, "Введіть значення:"))
        await q.answer()

@dp.message(AdminEditFieldSG.wait_value)
async def admin_edit_field_value(m: Message, state: FSMContext):
    data = await state.get_data()
    event_id = data.get("event_id")
    field = data.get("field")

    if field in {"title", "description", "link"}:
        val = (m.text or "").strip()
        await update_event_field(event_id, field, val)
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()

        if field == "title":
            await notify_event_update(event_id, f"Оновлено назву: {val}")
        elif field == "description":
            await notify_event_update(event_id, "Оновлено опис.")
        elif field == "link":
            await notify_event_update(event_id, f"Оновлено посилання: {val}")
        return

    if field == "start_at":
        dt = parse_dt(m.text or "")
        if not dt:
            await m.answer("Невірний формат. Приклад: 2025-10-05 15:00. Спробуйте ще раз:")
            return
        await update_event_field(event_id, "start_at", iso_dt(dt))
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()
        await notify_event_update(event_id, f"Змінено дату/час: {fmt_date(dt)} о {fmt_time(dt)} (Київ)")
        return

    if field == "duration_min":
        try:
            dur = int((m.text or "").strip())
            if dur <= 0:
                raise ValueError()
        except Exception:
            await m.answer("Введіть додатне ціле число. Спробуйте ще раз:")
            return
        await update_event_field(event_id, "duration_min", dur)
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()
        await notify_event_update(event_id, f"Змінено тривалість: {dur} хв")
        return

@dp.callback_query(F.data.startswith("admin:cancel:"))
async def admin_cancel(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) == 3:
        event_id = int(parts[-1])
        await q.message.edit_text("Підтвердити скасування події?", reply_markup=kb_cancel_confirm(event_id))
        await q.answer()
        return
    if len(parts) == 4 and parts[-1] == "yes":
        event_id = int(parts[2])
        await notify_event_cancel(event_id)
        await attendance_clear_for_event(event_id, mode="zero")
        await delete_event(event_id)
        await q.message.edit_text("✅ Подію скасовано, відмітки відвідування скинуто.", reply_markup=kb_admin_main())
        await q.answer()
        return

# ---------- Управление клиентами ----------

@dp.callback_query(F.data == "admin:clients:menu")
async def admin_clients_menu(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    await q.message.edit_text("👥 Управління клієнтами:\n\nОберіть категорію:", reply_markup=kb_clients_menu())
    await q.answer()

@dp.callback_query(F.data.startswith("admin:clients:list:"))
async def admin_clients_list(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return

    parts = q.data.split(":")
    if len(parts) != 5:
        await q.answer()
        return

    filter_type = parts[3]
    page = int(parts[4])

    clients = await list_clients_by_filter(filter_type)

    filter_names = {
        "all": "Всі клієнти",
        "completed": "Пройшли всі конференції",
        "active": "Активні (є майбутні конференції)",
        "never": "Не були ні на одній конференції"
    }

    per = 10
    total = len(clients)
    start = page * per
    end = start + per
    subset = clients[start:end]

    if not subset and page != 0:
        page = 0
        start, end = 0, per
        subset = clients[start:end]

    buttons = []
    for c in subset:
        name = c.get('full_name', 'Без імені')
        attended = c.get('attended_count', 0)
        buttons.append([
            InlineKeyboardButton(
                text=f"{name} ({attended} конф.)",
                callback_data=f"admin:client:view:{c['client_id']}"
            )
        ])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:clients:list:{filter_type}:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:clients:list:{filter_type}:{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:clients:menu")])

    text = f"👥 {filter_names.get(filter_type, 'Клієнти')}\n\nВсього: {total}"
    await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:client:view:"))
async def admin_client_view(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return

    parts = q.data.split(":")
    if len(parts) != 4:
        await q.answer()
        return

    client_id = int(parts[3])

    # Получаем информацию о клиенте
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT * FROM clients WHERE client_id = $1",
            client_id
        )

    if not client:
        await q.message.edit_text("❌ Клієнта не знайдено.", reply_markup=kb_clients_menu())
        await q.answer()
        return

    client = dict(client)
    stats = await get_client_statistics(client_id)

    # Формируем текст с информацией
    client_status = client.get('status', 'active')
    status_emoji = "🚫" if client_status == "blocked" else "✅"
    status_text = "ЗАБЛОКОВАНИЙ" if client_status == "blocked" else "Активний"

    docs_collected = client.get('documents_collected', False)
    docs_emoji = "✅" if docs_collected else "📋"
    docs_text = "Зібрані" if docs_collected else "Не зібрані"

    text = f"👤 Профіль клієнта\n\n"
    text += f"📝 ПІБ: {client.get('full_name', '—')}\n"
    text += f"📞 Телефон: {client.get('phone', '—')}\n"
    text += f"🆔 Telegram ID: {client.get('tg_user_id', '—')}\n"
    text += f"{status_emoji} Статус розсилок: {status_text}\n"
    text += f"{docs_emoji} Документи: {docs_text}\n"
    text += f"📅 Реєстрація: {fmt_date(client['created_at']) if client.get('created_at') else '—'}\n"
    text += f"👁 Остання активність: {fmt_date(client['last_seen_at']) if client.get('last_seen_at') else '—'}\n\n"

    text += f"📊 Статистика:\n"
    text += f"• Відвідано конференцій: {stats['attended_count']}\n"
    text += f"• Підтверджено майбутніх: {stats['confirmed_count']}\n"
    text += f"• Пройдено типів: {stats['completed_types']}/{stats['total_types']}\n\n"

    # Типы конференций
    if stats['attended_types']:
        text += f"✅ Пройдені типи конференцій:\n"
        for at in stats['attended_types']:
            text += f"  • {at['title']}\n"
        text += "\n"

    # Посещенные события
    if stats['attended_events']:
        text += f"📋 Відвідані конференції (останні 5):\n"
        for i, ev in enumerate(stats['attended_events'][:5], 1):
            dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
            text += f"{i}. {ev['title']} ({dt_str})\n"
        if len(stats['attended_events']) > 5:
            text += f"   ...та ще {len(stats['attended_events']) - 5}\n"
        text += "\n"

    # Будущие подтвержденные события
    if stats['confirmed_events']:
        text += f"🔜 Підтверджені майбутні конференції:\n"
        for ev in stats['confirmed_events']:
            dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
            text += f"  • {ev['title']} ({dt_str})\n"

    await q.message.edit_text(text, reply_markup=kb_client_detail(client_id, client_status, docs_collected))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:client:block:"))
async def admin_client_block(q: CallbackQuery):
    """Блокировка клиента от рассылок"""
    if q.from_user.id not in ADMINS:
        await q.answer()
        return

    parts = q.data.split(":")
    if len(parts) != 4:
        await q.answer()
        return

    client_id = int(parts[3])

    # Обновляем статус клиента
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET status = 'blocked' WHERE client_id = $1",
            client_id
        )

    await log_action("client_blocked", client_id=client_id, details=f"by_admin:{q.from_user.id}")
    await q.answer("✅ Клієнта заблоковано від розсилок")

    # Обновляем экран клиента
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT * FROM clients WHERE client_id = $1",
            client_id
        )

    if client:
        client = dict(client)
        stats = await get_client_statistics(client_id)

        client_status = client.get('status', 'active')
        status_emoji = "🚫" if client_status == "blocked" else "✅"
        status_text = "ЗАБЛОКОВАНИЙ" if client_status == "blocked" else "Активний"

        docs_collected = client.get('documents_collected', False)
        docs_emoji = "✅" if docs_collected else "📋"
        docs_text = "Зібрані" if docs_collected else "Не зібрані"

        text = f"👤 Профіль клієнта\n\n"
        text += f"📝 ПІБ: {client.get('full_name', '—')}\n"
        text += f"📞 Телефон: {client.get('phone', '—')}\n"
        text += f"🆔 Telegram ID: {client.get('tg_user_id', '—')}\n"
        text += f"{status_emoji} Статус розсилок: {status_text}\n"
        text += f"{docs_emoji} Документи: {docs_text}\n"
        text += f"📅 Реєстрація: {fmt_date(client['created_at']) if client.get('created_at') else '—'}\n"
        text += f"👁 Остання активність: {fmt_date(client['last_seen_at']) if client.get('last_seen_at') else '—'}\n\n"

        text += f"📊 Статистика:\n"
        text += f"• Відвідано конференцій: {stats['attended_count']}\n"
        text += f"• Підтверджено майбутніх: {stats['confirmed_count']}\n"
        text += f"• Пройдено типів: {stats['completed_types']}/{stats['total_types']}\n\n"

        if stats['attended_types']:
            text += f"✅ Пройдені типи конференцій:\n"
            for at in stats['attended_types']:
                text += f"  • {at['title']}\n"
            text += "\n"

        if stats['attended_events']:
            text += f"📋 Відвідані конференції (останні 5):\n"
            for i, ev in enumerate(stats['attended_events'][:5], 1):
                dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
                text += f"{i}. {ev['title']} ({dt_str})\n"
            if len(stats['attended_events']) > 5:
                text += f"   ...та ще {len(stats['attended_events']) - 5}\n"
            text += "\n"

        if stats['confirmed_events']:
            text += f"🔜 Підтверджені майбутні конференції:\n"
            for ev in stats['confirmed_events']:
                dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
                text += f"  • {ev['title']} ({dt_str})\n"

        await q.message.edit_text(text, reply_markup=kb_client_detail(client_id, client_status, docs_collected))

@dp.callback_query(F.data.startswith("admin:client:unblock:"))
async def admin_client_unblock(q: CallbackQuery):
    """Разблокировка клиента для рассылок"""
    if q.from_user.id not in ADMINS:
        await q.answer()
        return

    parts = q.data.split(":")
    if len(parts) != 4:
        await q.answer()
        return

    client_id = int(parts[3])

    # Обновляем статус клиента
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET status = 'active' WHERE client_id = $1",
            client_id
        )

    await log_action("client_unblocked", client_id=client_id, details=f"by_admin:{q.from_user.id}")
    await q.answer("✅ Клієнта розблоковано для розсилок")

    # Обновляем экран клиента
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT * FROM clients WHERE client_id = $1",
            client_id
        )

    if client:
        client = dict(client)
        stats = await get_client_statistics(client_id)

        client_status = client.get('status', 'active')
        status_emoji = "🚫" if client_status == "blocked" else "✅"
        status_text = "ЗАБЛОКОВАНИЙ" if client_status == "blocked" else "Активний"

        docs_collected = client.get('documents_collected', False)
        docs_emoji = "✅" if docs_collected else "📋"
        docs_text = "Зібрані" if docs_collected else "Не зібрані"

        text = f"👤 Профіль клієнта\n\n"
        text += f"📝 ПІБ: {client.get('full_name', '—')}\n"
        text += f"📞 Телефон: {client.get('phone', '—')}\n"
        text += f"🆔 Telegram ID: {client.get('tg_user_id', '—')}\n"
        text += f"{status_emoji} Статус розсилок: {status_text}\n"
        text += f"{docs_emoji} Документи: {docs_text}\n"
        text += f"📅 Реєстрація: {fmt_date(client['created_at']) if client.get('created_at') else '—'}\n"
        text += f"👁 Остання активність: {fmt_date(client['last_seen_at']) if client.get('last_seen_at') else '—'}\n\n"

        text += f"📊 Статистика:\n"
        text += f"• Відвідано конференцій: {stats['attended_count']}\n"
        text += f"• Підтверджено майбутніх: {stats['confirmed_count']}\n"
        text += f"• Пройдено типів: {stats['completed_types']}/{stats['total_types']}\n\n"

        if stats['attended_types']:
            text += f"✅ Пройдені типи конференцій:\n"
            for at in stats['attended_types']:
                text += f"  • {at['title']}\n"
            text += "\n"

        if stats['attended_events']:
            text += f"📋 Відвідані конференції (останні 5):\n"
            for i, ev in enumerate(stats['attended_events'][:5], 1):
                dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
                text += f"{i}. {ev['title']} ({dt_str})\n"
            if len(stats['attended_events']) > 5:
                text += f"   ...та ще {len(stats['attended_events']) - 5}\n"
            text += "\n"

        if stats['confirmed_events']:
            text += f"🔜 Підтверджені майбутні конференції:\n"
            for ev in stats['confirmed_events']:
                dt_str = fmt_date(ev['start_at']) if ev.get('start_at') else '—'
                text += f"  • {ev['title']} ({dt_str})\n"

        await q.message.edit_text(text, reply_markup=kb_client_detail(client_id, client_status, docs_collected))

# ---------- RSVP ----------

@dp.callback_query(F.data.startswith("rsvp:"))
async def cb_rsvp(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    _, event_id_str, action = parts
    event_id = int(event_id_str)

    cli = await get_client_by_tg(q.from_user.id)
    if not cli:
        await safe_edit_message(q.message, "Будь ласка, зареєструйтеся за допомогою команди /start.")
        await q.answer()
        return

    client_id = cli["client_id"]
    event = await get_event_by_id(event_id)
    if not event:
        await safe_edit_message(q.message, "Конференцію не знайдено.")
        await q.answer()
        return

    # Проверяем, не началась ли уже конференция
    dt = event_start_dt(event)
    if dt and dt <= now_kyiv():
        await safe_edit_message(
            q.message,
            q.message.text + "\n\n⚠️ Конференція вже почалася. Підтвердження участі недоступне."
        )
        await q.answer("Конференція вже почалася")
        return

    if action == "going":
        # Проверяем, нет ли конфликта времени с другими подтвержденными конференциями
        dt = event_start_dt(event)
        duration = event.get("duration_min", 60)
        if dt and await client_has_confirmed_event_at_time(client_id, dt, duration):
            await safe_edit_message(
                q.message,
                q.message.text + "\n\n⚠️ У Вас вже є підтверджена конференція на цей час. Не можна підтвердити участь у двох конференціях одночасно."
            )
            await q.answer("Конфлікт часу")
            return

        await rsvp_upsert(event_id, client_id, rsvp="going")
        await log_action("rsvp_yes", client_id=client_id, event_id=event_id, details="")

        # Сохраняем исходное сообщение и добавляем подтверждение
        original_text = q.message.text or ""
        new_text = original_text + "\n\n✅ Дякуємо! Вашу участь підтверджено."
        await safe_edit_message(q.message, new_text)
        await q.answer()
        return

    if action == "declined":
        await rsvp_upsert(event_id, client_id, rsvp="declined")
        await log_action("rsvp_no", client_id=client_id, event_id=event_id, details="")

        alt = await list_alternative_events_same_type(a2i(event.get("type")), event_id)
        if not alt:
            # Сохраняем исходное сообщение и добавляем ответ
            original_text = q.message.text or ""
            new_text = original_text + "\n\n❌ Дякуємо за відповідь. Ми надішлемо Вам запрошення на іншу дату."
            await safe_edit_message(q.message, new_text)
        else:
            rows = []
            for a in alt[:8]:
                dt = event_start_dt(a)
                when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else a.get('start_at', '')
                rows.append([InlineKeyboardButton(text=when, callback_data=f"alt:pick:{a['event_id']}")])
            rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="noop")])

            title_for_info = event.get("title", "конференція")
            # Сохраняем исходное сообщение
            original_text = q.message.text or ""
            new_text = original_text + f"\n\nАльтернативні дати проведення конференції «{title_for_info}»:"
            await safe_edit_message(
                q.message,
                new_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
            )
    await q.answer()
    return

@dp.callback_query(F.data.startswith("claim:"))
async def claim_feedback(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    _, event_id_str, client_id_str = parts
    event_id = int(event_id_str)
    client_id = int(client_id_str)

    owner = f"@{q.from_user.username}" if q.from_user and q.from_user.username else f"id:{q.from_user.id}"
    await feedback_assign_owner(event_id, client_id, owner)
    await log_action("complaint_taken", client_id=client_id, event_id=event_id, details=f"owner={owner}")

    # Сохраняем исходное сообщение и добавляем информацию о взятии в работу
    original_text = q.message.text or ""
    new_text = original_text + f"\n\n✅ Взято в роботу ({owner})"
    await q.message.edit_text(new_text)
    await q.answer()

@dp.callback_query(F.data.startswith("alt:pick:"))
async def alt_pick(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return

    alt_event_id = int(parts[2])
    cli = await get_client_by_tg(q.from_user.id)
    if not cli:
        await q.message.edit_text("Будь ласка, зареєструйтеся за допомогою команди /start.")
        await q.answer()
        return

    client_id = cli["client_id"]
    alt_event = await get_event_by_id(alt_event_id)
    if not alt_event:
        await q.message.edit_text("На жаль, обрану дату не знайдено.")
        await q.answer()
        return

    # Проверяем конфликт времени
    dt = event_start_dt(alt_event)
    duration = alt_event.get("duration_min", 60)
    if dt and await client_has_confirmed_event_at_time(client_id, dt, duration):
        await q.message.edit_text(
            "⚠️ У Вас вже є підтверджена конференція на цей час. Будь ласка, оберіть іншу дату або скасуйте попередню конференцію."
        )
        await q.answer("Конфлікт часу")
        return

    await rsvp_upsert(alt_event_id, client_id, rsvp="going")
    await log_action("rsvp_alt_yes", client_id=client_id, event_id=alt_event_id, details="picked_alternative")

    dt = event_start_dt(alt_event)
    when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else alt_event.get("start_at", "")
    await q.message.edit_text(
        f"✅ Дякуємо! Вашу участь підтверджено.\n\n"
        f"Конференція: {alt_event.get('title','')}\n"
        f"🗓 Дата та час: {when}\n"
        f"🔗 Посилання: {alt_event.get('link','')}"
    )
    await q.answer()

@dp.callback_query(F.data == "noop")
async def noop(q: CallbackQuery):
    await q.answer()

# ---------- POST-EVENT SURVEY (опрос "Удалось ли присоединиться?") ----------

@dp.callback_query(F.data.startswith("post_survey:"))
async def handle_post_event_survey(q: CallbackQuery):
    """Обработка ответа на опрос после конференции"""
    parts = q.data.split(":")
    if len(parts) < 4:
        await q.answer("Помилка")
        return

    action = parts[1]  # "yes" или "no"
    event_id = int(parts[2])
    client_id = int(parts[3])

    # Проверяем, что это тот же пользователь
    client = await get_client_by_id(client_id)
    if not client or client.get('tg_user_id') != q.from_user.id:
        await q.answer("Помилка: невідповідність користувача")
        return

    event = await get_event_by_id(event_id)
    if not event:
        await q.answer("Подія не знайдена")
        return

    if action == "yes":
        # Клиент был на конференции - отмечаем посещение
        await mark_attendance(event_id, client_id, True)
        await q.message.edit_text("Дякуємо за участь! 🎉\n\nОчікуйте запит на оцінку конференції.")
        await log_action("post_event_survey_response", client_id=client_id, event_id=event_id, details="attended=yes")

        # Отправляем опрос с оценкой
        tg_id = client.get('tg_user_id')
        if tg_id:
            text = f"Будь ласка, оцініть конференцію «{event['title']}»:"
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="1 ⭐", callback_data=f"fb:{event_id}:{client_id}:1"),
                InlineKeyboardButton(text="2 ⭐", callback_data=f"fb:{event_id}:{client_id}:2"),
                InlineKeyboardButton(text="3 ⭐", callback_data=f"fb:{event_id}:{client_id}:3"),
                InlineKeyboardButton(text="4 ⭐", callback_data=f"fb:{event_id}:{client_id}:4"),
                InlineKeyboardButton(text="5 ⭐", callback_data=f"fb:{event_id}:{client_id}:5"),
            ]])
            try:
                await bot.send_message(chat_id=int(tg_id), text=text, reply_markup=kb)
                await log_action("feedback_requested_after_survey", client_id=client_id, event_id=event_id)
            except Exception:
                pass
    else:
        # Клиент не был - оставляем attended=FALSE
        await mark_attendance(event_id, client_id, False)
        await q.message.edit_text(
            "Дякуємо за відповідь! 🙏\n\n"
            "Нічого страшного! Ви отримаєте нове запрошення, коли наступна конференція цього типу буде заплановано.\n\n"
            "Ми завжди раді бачити вас! 💙💛"
        )
        await log_action("post_event_survey_response", client_id=client_id, event_id=event_id, details="attended=no")

    await q.answer()

# ---------- DOCUMENTS SURVEY (опрос о сборе документов) ----------

@dp.callback_query(F.data.startswith("docs_survey:"))
async def handle_documents_survey(q: CallbackQuery):
    """Обработка ответа на опрос о сборе документов"""
    parts = q.data.split(":")
    if len(parts) < 3:
        await q.answer("Помилка")
        return

    action = parts[1]  # "yes" или "no"
    client_id = int(parts[2])

    # Проверяем, что это тот же пользователь
    client = await get_client_by_id(client_id)
    if not client or client.get('tg_user_id') != q.from_user.id:
        await q.answer("Помилка: невідповідність користувача")
        return

    if action == "yes":
        # Клиент собрал документы - больше не присылаем приглашения
        await set_documents_collected(client_id, True)
        await q.message.edit_text(
            "✅ Чудово! Ви більше не отримуватимете запрошення на конференції зі збору документів.\n\n"
            "Бажаємо успіху в подальших кроках! 🎉"
        )
        await log_action("documents_survey_response", client_id=client_id, details="collected=yes")
    else:
        # Клиент хочет продолжать получать приглашения
        await q.message.edit_text(
            "🔄 Без проблем! Ви продовжите отримувати запрошення на конференції зі збору документів.\n\n"
            "Ми раді допомогти вам зібрати всі необхідні документи! 📋"
        )
        await log_action("documents_survey_response", client_id=client_id, details="collected=no")

    await q.answer()

# ---------- FEEDBACK (зірки + коментар) ----------

async def route_low_feedback(event_id: int, client_id: int, stars: int, comment: str):
    cli_tg = await try_get_tg_from_client_id(client_id)
    cli_row = await get_client_by_tg(cli_tg) if cli_tg else None
    full_name = cli_row["full_name"] if cli_row else str(client_id)
    phone = cli_row["phone"] if cli_row else "—"
    event = await get_event_by_id(event_id) or {}

    text = (
        f"⚠️ Низька оцінка події\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {full_name} (tg_id={cli_tg})\n"
        f"• Телефон: {phone}\n"
        f"• Оцінка: {stars}\n"
        f"• Коментар: {comment or '—'}"
    )
    kb = kb_claim_feedback(event_id, client_id)

    try:
        await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb, parse_mode=None)
        await log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}")
        return
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after + 1)
        try:
            await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb, parse_mode=None)
            await log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}/after_retry")
            return
        except Exception as ex2:
            await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"retry_fail:{type(ex2).__name__}")
    except (TelegramForbiddenError, TelegramBadRequest) as ex:
        await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"{type(ex).__name__}:{ex}")
    except Exception as ex:
        await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"unknown:{type(ex).__name__}")

    if ADMINS:
        for admin_id in list(ADMINS):
            try:
                await bot.send_message(chat_id=admin_id, text="(фолбэк) " + text, reply_markup=kb, parse_mode=None)
                await log_action("feedback_low_notified_admin_dm", client_id=client_id, event_id=event_id, details=f"to_admin:{admin_id}")
            except Exception as ex:
                await log_action("feedback_low_admin_dm_fail", client_id=client_id, event_id=event_id, details=f"{admin_id}:{type(ex).__name__}")

async def route_low_feedback_comment_update(event_id: int, client_id: int, comment: str):
    cli_tg = await try_get_tg_from_client_id(client_id)
    event = await get_event_by_id(event_id) or {}
    text = (
        f"📝 Доповнення до скарги\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {client_id} (tg_id={cli_tg})\n"
        f"• Коментар: {comment or '—'}"
    )
    try:
        await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, parse_mode=None)
        await log_action("low_fb_comment_update_sent", client_id=client_id, event_id=event_id, details="")
    except Exception as e:
        await log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")

@dp.callback_query(F.data.startswith("fb:"))
async def fb_callbacks(q: CallbackQuery, state: FSMContext):
    data = q.data or ""

    # Выбор звёзд: fb:<event_id>:<client_id>:<stars>
    if data.startswith("fb:") and data.count(":") == 3 and not data.startswith("fb:comment:") and not data.startswith("fb:skip:"):
        _, event_id_str, client_id_str, stars_str = data.split(":")
        event_id = int(event_id_str)
        client_id = int(client_id_str)
        stars = int(stars_str)

        await feedback_upsert(event_id, client_id, stars=stars)

        if stars < 4:
            try:
                await route_low_feedback(event_id, client_id, stars, "")
                await log_action("low_fb_alert_sent", client_id=client_id, event_id=event_id, details=f"stars={stars}")
            except Exception as e:
                await log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")

        prompt = f"Дякуємо! Вашу оцінку {stars}⭐️ збережено.\nБажаєте додати коментар?"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Написати коментар", callback_data=f"fb:comment:{event_id}:{client_id}")],
            [InlineKeyboardButton(text="⏭ Пропустити", callback_data=f"fb:skip:{event_id}:{client_id}")]
        ])
        await q.message.edit_text(prompt, reply_markup=kb)
        await q.answer()
        return

    if data.startswith("fb:skip:"):
        await q.message.edit_text("Дякуємо за Ваш відгук! ✅")
        await q.answer()
        return

    if data.startswith("fb:comment:"):
        _, _, event_id_str, client_id_str = data.split(":")
        event_id = int(event_id_str)
        client_id = int(client_id_str)

        tg_id = await try_get_tg_from_client_id(client_id)
        if not tg_id or not q.from_user or q.from_user.id != int(tg_id):
            await q.message.edit_text("Будь ласка, введіть коментар у приватному діалозі з ботом.")
            await q.answer()
            return
        await state.set_state(FeedbackSG.wait_comment)
        await state.update_data(event_id=event_id, client_id=client_id)
        await q.message.edit_text("Будь ласка, надішліть Ваш коментар одним повідомленням.\nДля пропуску надішліть символ «-».")
        await q.answer()
        return

@dp.message(FeedbackSG.wait_comment)
async def fb_wait_comment(m: Message, state: FSMContext):
    data = await state.get_data()
    event_id = data["event_id"]
    client_id = data["client_id"]

    comment = (m.text or "").strip()
    if comment == "-":
        comment = ""

    saved = await feedback_upsert(event_id, client_id, comment=comment)
    stars = a2i(saved.get("stars"), 0)

    await m.answer("Дякуємо! Ваш відгук збережено. ✅")
    await state.clear()

    if stars and stars < 4 and comment:
        await route_low_feedback_comment_update(event_id, client_id, comment)

# =============================== NOTIFY HELPERS ================================

async def notify_event_update(event_id: int, what: str):
    event = await get_event_by_id(event_id)
    if not event:
        return
    templ = await messages_get("update.notice")
    body = templ.format(title=event["title"], what=what)
    for r in await rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = await try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                # Проверка статуса клиента
                client = await get_client_by_tg(tg_id)
                if not client or client.get('status') != 'active':
                    continue
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def notify_event_cancel(event_id: int):
    event = await get_event_by_id(event_id)
    if not event:
        return
    templ = await messages_get("cancel.notice")
    body = templ.format(title=event["title"])
    for r in await rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = await try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                # Проверка статуса клиента
                client = await get_client_by_tg(tg_id)
                if not client or client.get('status') != 'active':
                    continue
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def send_initial_invites_for_event(event: Dict[str, Any]):
    """Рассылка начальных приглашений на событие

    Специальные правила:
    - type_code 4 (ДОКУМЕНТИ ЗБИРАЄМО РАЗОМ) отправляется только тем,
      кто уже посетил type_code 1 (Конференція зі збору документів для суду)
    """
    event_id = event.get("event_id")
    dt = event_start_dt(event)
    if not dt:
        await log_action("invite_skip", event_id=event_id, details="No valid datetime")
        return

    if not await is_earliest_upcoming_event_of_type(event):
        await log_action("invite_skip", event_id=event_id, details="Not earliest event of type")
        return

    type_code = event.get("type")
    active_clients = await list_active_clients()

    await log_action("invite_process_start", event_id=event_id, details=f"Processing {len(active_clients)} active clients, type={type_code}")

    sent_count = 0
    skip_reasons = {}

    for cli in active_clients:
        cid = cli.get("client_id")
        tg_id = cli.get("tg_user_id")

        if not cid or not tg_id:
            skip_reasons["no_cid_or_tg"] = skip_reasons.get("no_cid_or_tg", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"no_cid_or_tg")
            continue

        # Проверка: уже посещал (пропускаем для type_code 4 - они могут посещать многократно)
        if type_code != 4:
            if await client_has_attended_type(cid, type_code):
                skip_reasons["already_attended"] = skip_reasons.get("already_attended", 0) + 1
                await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"already_attended type={type_code}")
                continue

        if await client_has_active_invite_for_type(cid, type_code):
            skip_reasons["has_active_invite"] = skip_reasons.get("has_active_invite", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"has_active_invite type={type_code}")
            continue

        # Специальная проверка: type_code 4 только для тех, кто посетил type_code 1
        if type_code == 4:
            if not await client_has_attended_type(cid, 1):
                skip_reasons["type4_requires_type1"] = skip_reasons.get("type4_requires_type1", 0) + 1
                await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"type4 requires type1 attendance")
                continue

            # Проверка: если клиент уже собрал все документы - не приглашаем
            if cli.get('documents_collected'):
                skip_reasons["documents_already_collected"] = skip_reasons.get("documents_already_collected", 0) + 1
                await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"documents already collected")
                continue

        # Проверка лимита подтверждений в день
        confirmed_today = await count_client_confirmed_today_by_type(cid, type_code)
        if type_code == 1:
            # Для type_code=1: максимум 1 подтверждение в день
            if confirmed_today >= 1:
                skip_reasons["type1_daily_limit"] = skip_reasons.get("type1_daily_limit", 0) + 1
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details=f"type1 daily limit reached: {confirmed_today}/1")
                continue
        else:
            # Для остальных: максимум 2 подтверждения в день
            if confirmed_today >= 2:
                skip_reasons["daily_limit"] = skip_reasons.get("daily_limit", 0) + 1
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details=f"daily limit reached: {confirmed_today}/2 for type={type_code}")
                continue

        # Проверка на пересечение времени с другими подтвержденными конференциями
        if await client_has_confirmed_event_at_time(cid, dt, event.get("duration_min", 60)):
            skip_reasons["time_conflict"] = skip_reasons.get("time_conflict", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"time_conflict at {iso_dt(dt)}")
            continue

        if await has_log("invite_sent", cid, event_id):
            skip_reasons["already_sent"] = skip_reasons.get("already_sent", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"already_sent")
            continue

        body = (await messages_get("invite.body")).format(
            name=cli.get("full_name","Клієнт"),
            title=event["title"],
            date=fmt_date(dt),
            time=fmt_time(dt),
            description=event["description"]
        )

        try:
            title_msg = await messages_get("invite.title")
            await bot.send_message(chat_id=int(tg_id), text=title_msg.format(title=event["title"]))
            await bot.send_message(chat_id=int(tg_id), text=body, reply_markup=kb_rsvp(event_id))
            await rsvp_upsert(event_id, cid, rsvp="")
            await log_action("invite_sent", client_id=cid, event_id=event_id, details="immediate")
            sent_count += 1
        except TelegramRetryAfter as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"RetryAfter {e.retry_after}s")
            skip_reasons["telegram_retry_after"] = skip_reasons.get("telegram_retry_after", 0) + 1
        except TelegramForbiddenError:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"ForbiddenError: user blocked bot")
            skip_reasons["user_blocked_bot"] = skip_reasons.get("user_blocked_bot", 0) + 1
        except TelegramBadRequest as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"BadRequest: {str(e)}")
            skip_reasons["telegram_bad_request"] = skip_reasons.get("telegram_bad_request", 0) + 1
        except Exception as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"{type(e).__name__}: {str(e)}")
            skip_reasons["other_error"] = skip_reasons.get("other_error", 0) + 1

    await log_action("invite_process_complete", event_id=event_id, details=f"Sent={sent_count}, Skipped={skip_reasons}")


async def send_pending_invites_to_new_client(cli: Dict[str, Any]):
    """Рассылка приглашений на предстоящие конференции для нового/только что зарегистрированного клиента.

    Когда клиент регистрируется после создания события,
    он должен получить приглашения на актуальные конференции.
    Для каждого типа конференции выбирается самое раннее предстоящее событие.
    Применяются те же правила eligibility, что и в send_initial_invites_for_event.
    """
    cid = cli.get("client_id")
    tg_id = cli.get("tg_user_id")

    if not cid or not tg_id:
        return

    now = now_kyiv()

    # Получаем самое раннее предстоящее событие каждого типа (исключаем кастомные type=0)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT ON (type) *
               FROM events
               WHERE start_at >= $1 AND type != 0
               ORDER BY type, start_at""",
            now
        )

    if not rows:
        return

    await log_action("new_client_invite_start", client_id=cid,
                     details=f"Checking {len(rows)} event types")

    sent_count = 0

    for row in rows:
        event = dict(row)
        event_id = event.get("event_id")
        type_code = event.get("type")
        dt = event_start_dt(event)

        if not dt:
            continue

        # Проверка: уже посещал (пропускаем для type_code 4 — они могут посещать многократно)
        if type_code != 4:
            if await client_has_attended_type(cid, type_code):
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details="new_client: already_attended")
                continue

        if await client_has_active_invite_for_type(cid, type_code):
            await log_action("invite_skip", client_id=cid, event_id=event_id,
                            details="new_client: has_active_invite")
            continue

        # type_code 4 — только для тех, кто посетил type_code 1
        if type_code == 4:
            if not await client_has_attended_type(cid, 1):
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details="new_client: type4_requires_type1")
                continue
            if cli.get('documents_collected'):
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details="new_client: documents_collected")
                continue

        # Лимит подтверждений в день
        confirmed_today = await count_client_confirmed_today_by_type(cid, type_code)
        if type_code == 1:
            if confirmed_today >= 1:
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details="new_client: type1_daily_limit")
                continue
        else:
            if confirmed_today >= 2:
                await log_action("invite_skip", client_id=cid, event_id=event_id,
                                details="new_client: daily_limit")
                continue

        # Проверка пересечения времени
        if await client_has_confirmed_event_at_time(cid, dt, event.get("duration_min", 60)):
            await log_action("invite_skip", client_id=cid, event_id=event_id,
                            details="new_client: time_conflict")
            continue

        if await has_log("invite_sent", cid, event_id):
            await log_action("invite_skip", client_id=cid, event_id=event_id,
                            details="new_client: already_sent")
            continue

        # Конвертируем start_at для форматирования (если ещё datetime)
        if isinstance(event.get('start_at'), datetime):
            event['start_at'] = iso_dt(event['start_at'])

        body = (await messages_get("invite.body")).format(
            name=cli.get("full_name", "Клієнт"),
            title=event["title"],
            date=fmt_date(dt),
            time=fmt_time(dt),
            description=event["description"]
        )

        try:
            title_msg = await messages_get("invite.title")
            await bot.send_message(chat_id=int(tg_id), text=title_msg.format(title=event["title"]))
            await bot.send_message(chat_id=int(tg_id), text=body, reply_markup=kb_rsvp(event_id))
            await rsvp_upsert(event_id, cid, rsvp="")
            await log_action("invite_sent", client_id=cid, event_id=event_id,
                            details="new_client_registration")
            sent_count += 1
        except TelegramRetryAfter as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id,
                            details=f"new_client: RetryAfter {e.retry_after}s")
        except TelegramForbiddenError:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id,
                            details="new_client: ForbiddenError")
        except TelegramBadRequest as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id,
                            details=f"new_client: BadRequest: {str(e)}")
        except Exception as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id,
                            details=f"new_client: {type(e).__name__}: {str(e)}")

    if sent_count > 0:
        await log_action("new_client_invite_complete", client_id=cid,
                        details=f"Sent={sent_count}")


async def send_custom_invites(event: Dict[str, Any], clients: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Відправка запрошень на кастомну конференцію конкретним клієнтам (без перевірок eligibility)."""
    event_id = event.get("event_id")
    dt = event_start_dt(event)
    if not dt:
        return {"sent": 0, "failed": len(clients)}

    sent = 0
    failed = 0

    for cli in clients:
        cid = cli.get("client_id")
        tg_id = cli.get("tg_user_id")
        if not cid or not tg_id:
            failed += 1
            continue

        body = (await messages_get("invite.body")).format(
            name=cli.get("full_name", "Клієнт"),
            title=event["title"],
            date=fmt_date(dt),
            time=fmt_time(dt),
            description=event.get("description", "")
        )

        try:
            title_msg = await messages_get("invite.title")
            await bot.send_message(chat_id=int(tg_id), text=title_msg.format(title=event["title"]))
            await bot.send_message(chat_id=int(tg_id), text=body, reply_markup=kb_rsvp(event_id))
            await rsvp_upsert(event_id, cid, rsvp="")
            await log_action("invite_sent", client_id=cid, event_id=event_id, details="custom_conf")
            sent += 1
        except TelegramForbiddenError:
            failed += 1
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details="ForbiddenError")
        except Exception as e:
            failed += 1
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=str(e))

    return {"sent": sent, "failed": failed}


# =================== NEW HANDLERS: /info, BROADCAST, MOTIVATIONAL ==============

# Обробник команди /info
@dp.message(Command("info"))
async def cmd_info(m: Message, state: FSMContext):
    """Команда /info +380********* для отримання інформації про клієнта"""
    if m.from_user.id not in ADMINS:
        await m.answer("❌ Ця команда доступна тільки адміністраторам.")
        return

    # Парсинг номеру телефону
    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        await m.answer("📞 Використання: /info +380123456789")
        return

    phone = args[1].strip()

    # Шукаємо клієнта
    client = await get_client_by_phone(phone)

    if not client:
        await m.answer(f"❌ Клієнта з номером {phone} не знайдено в базі даних.")
        return

    # Отримуємо повну інформацію
    info = await get_client_full_info(client['client_id'])

    if not info:
        await m.answer("❌ Помилка отримання інформації про клієнта.")
        return

    # Форматуємо та відправляємо
    text = await format_client_info_message(info)

    # Telegram обмежує повідомлення до 4096 символів
    if len(text) > 4096:
        # Розбиваємо на частини
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await m.answer(part, parse_mode=None)
    else:
        await m.answer(text, parse_mode=None)

# Обробники розсилок
@dp.callback_query(F.data == "broadcast:menu")
async def broadcast_menu(c: CallbackQuery, state: FSMContext):
    # Додаємо користувача в ADMINS
    ADMINS.add(c.from_user.id)

    await state.clear()
    await c.message.edit_text(
        "🎯 ОБЕРІТЬ СЕГМЕНТ КЛІЄНТІВ:",
        reply_markup=kb_broadcast_segments()
    )

@dp.callback_query(F.data.startswith("broadcast:segment:"))
async def broadcast_select_segment(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    segment = c.data.split(":")[2]

    # Отримуємо клієнтів для сегменту
    clients = await get_broadcast_segment_clients(segment)

    if not clients:
        await c.message.edit_text(
            f"⚠️ Для сегменту '{segment}' не знайдено жодного клієнта.\n\nОберіть інший сегмент.",
            reply_markup=kb_broadcast_segments()
        )
        return

    # Зберігаємо сегмент та клієнтів
    await state.update_data(segment=segment, clients=clients)

    # Назва сегменту
    segment_names = {
        "all": "Всі активні клієнти",
        "never": "Ніколи не відвідували конференції",
        "type_1": "Відвідали ЗБІР ДОКУМЕНТІВ (тип 1)",
        "type_2": "Відвідали СЛУЖБА БЕЗПЕКИ (тип 2)",
        "type_3": "Відвідали ПІДГОТОВКА ІСТОРІЇ (тип 3)",
        "type_4": "Відвідали ДОКУМЕНТИ РАЗОМ (тип 4)",
        "completed": "Відвідали ВСІ типи (завершили)",
        "inactive_30": "Неактивні 30+ днів (не завершили)",
        "low_ratings": "З низькими оцінками (<4)"
    }

    segment_name = segment_names.get(segment, segment)

    # Показуємо попередній перегляд
    preview_text = f"🎯 Сегмент: {segment_name}\n\n📊 Знайдено клієнтів: {len(clients)}\n\n📋 Приклади (перші 5):\n"
    for i, client in enumerate(clients[:5], 1):
        preview_text += f"{i}. {client['full_name']} ({client['phone']}) — рег. {fmt_date(client['created_at'])}\n"

    if len(clients) > 5:
        preview_text += f"...\n\n⚠️ Переконайтесь, що обрано правильний сегмент!"

    await c.message.edit_text(preview_text, reply_markup=kb_broadcast_confirm())

@dp.callback_query(F.data == "broadcast:confirm:yes")
async def broadcast_confirm_yes(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    await c.message.edit_text(
        "✍️ Напишіть текст повідомлення для розсилки:\n\n"
        "Підтримуються:\n"
        "• Текст (до 4096 символів)\n"
        "• Emoji\n"
        "• Посилання\n\n"
        "🚫 Надішліть /cancel для скасування",
        reply_markup=None
    )
    await state.set_state(BroadcastSG.wait_message)

@dp.message(BroadcastSG.wait_message, F.text == "/cancel")
async def broadcast_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Розсилка скасована.", reply_markup=kb_admin_main())

@dp.message(BroadcastSG.wait_message)
async def broadcast_receive_message(m: Message, state: FSMContext):
    if m.from_user.id not in ADMINS:
        return

    message_text = m.text

    if len(message_text) > 4096:
        await m.answer("⚠️ Повідомлення занадто довге. Максимум 4096 символів.")
        return

    # Зберігаємо текст
    await state.update_data(message_text=message_text)
    data = await state.get_data()
    clients = data.get('clients', [])

    # Показуємо попередній перегляд
    preview = f"👀 ПОПЕРЕДНІЙ ПЕРЕГЛЯД ПОВІДОМЛЕННЯ:\n\n────────────────────────\n{message_text}\n────────────────────────\n\n"
    preview += f"📊 Буде надіслано: {len(clients)} клієнтам\n"
    preview += f"⏱ Приблизний час: ~{len(clients) * 0.035 / 60:.0f} хвилин"

    await m.answer(preview, reply_markup=kb_broadcast_preview(), parse_mode=None)
    await state.set_state(BroadcastSG.preview)

@dp.callback_query(F.data == "broadcast:edit:text", BroadcastSG.preview)
async def broadcast_edit_text(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    await c.message.edit_text(
        "✍️ Напишіть новий текст повідомлення для розсилки:\n\n🚫 Надішліть /cancel для скасування"
    )
    await state.set_state(BroadcastSG.wait_message)

@dp.callback_query(F.data == "broadcast:send:confirm", BroadcastSG.preview)
async def broadcast_send_confirm(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    data = await state.get_data()
    clients = data.get('clients', [])
    message_text = data.get('message_text', '')
    segment = data.get('segment', 'unknown')

    if not clients or not message_text:
        await c.answer("❌ Помилка: дані розсилки не знайдено")
        return

    # Починаємо розсилку
    progress_msg = await c.message.edit_text(f"⏳ Запускаю розсилку...\n\n📨 Надіслано: 0/{len(clients)} (0%)")

    # Callback для оновлення прогресу
    async def update_progress(sent, total):
        percentage = int(sent / total * 100)
        bar_length = 20
        filled = int(bar_length * sent / total)
        bar = "▓" * filled + "░" * (bar_length - filled)

        text = f"⏳ Відправка...\n\n📨 Надіслано: {sent}/{total} ({percentage}%)\n{bar}"
        try:
            await progress_msg.edit_text(text)
        except:
            pass

    # Запускаємо розсилку
    start_time = datetime.now()
    result = await send_broadcast_to_clients(
        clients, message_text, segment, c.from_user.id, update_progress
    )
    end_time = datetime.now()

    duration = (end_time - start_time).total_seconds()

    # Фінальний звіт
    report = f"✅ РОЗСИЛКА ЗАВЕРШЕНА!\n\n📊 Результати:\n"
    report += f"✅ Успішно надіслано: {result['sent']}\n"
    report += f"❌ Помилка доставки: {result['failed']}\n"

    if result['blocked']:
        report += f"  └─ Бот заблоковано: {len(result['blocked'])}\n"

    report += f"\n⏱ Час виконання: {int(duration // 60)} хвилин {int(duration % 60)} секунд\n"

    if result['blocked']:
        report += "\n━━━━━━━━━━━━━━━━━━━━━\n\n📋 Заблокували бота (позначені неактивними):\n"
        for i, client in enumerate(result['blocked'][:10], 1):
            report += f"{i}. {client['full_name']} ({client['phone']})\n"
        if len(result['blocked']) > 10:
            report += f"... та ще {len(result['blocked']) - 10}\n"

    report += "\n💾 Збережено в delivery_log"

    await progress_msg.edit_text(report, reply_markup=kb_admin_main())
    await state.clear()

# Обробники мотивуючих повідомлень
@dp.callback_query(F.data == "motivational:menu")
async def motivational_menu(c: CallbackQuery, state: FSMContext):
    # Додаємо користувача в ADMINS
    ADMINS.add(c.from_user.id)

    await state.clear()
    await c.message.edit_text(
        "📢 МОТИВУЮЧІ ПОВІДОМЛЕННЯ\n\nОберіть дію:",
        reply_markup=kb_motivational_menu()
    )

@dp.callback_query(F.data == "motivational:toggle")
async def motivational_toggle(c: CallbackQuery):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    global MOTIVATIONAL_ENABLED
    MOTIVATIONAL_ENABLED = not MOTIVATIONAL_ENABLED

    status = "УВІМКНЕНО" if MOTIVATIONAL_ENABLED else "ПРИЗУПИНЕНО"
    emoji = "✅" if MOTIVATIONAL_ENABLED else "⏸"

    await c.message.edit_text(
        f"{emoji} Мотивуючі повідомлення {status}\n\n"
        f"Розсилка {'відновлена' if MOTIVATIONAL_ENABLED else 'призупинена'}.",
        reply_markup=kb_motivational_menu()
    )

@dp.callback_query(F.data == "motivational:stats")
async def motivational_stats(c: CallbackQuery):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    stats = await get_motivational_statistics(30)

    if not stats['stats']:
        await c.message.edit_text(
            "📊 СТАТИСТИКА МОТИВУЮЧИХ ПОВІДОМЛЕНЬ\n\n"
            "За останні 30 днів:\n\n"
            "⚠️ Немає даних про відправлення.",
            reply_markup=kb_motivational_menu()
        )
        return

    text = "📊 СТАТИСТИКА МОТИВУЮЧИХ ПОВІДОМЛЕНЬ\n\nЗа останні 30 днів:\n\n"

    total_sent = 0
    total_conversions = 0

    for stat in stats['stats']:
        msg_num = stat['message_key'].split('.')[-1]
        text += f"Повідомлення №{msg_num}: {stat['sent_count']} відправок → {stat['conversion_count']} конверсій ({stat['conversion_rate']:.1f}%)\n"
        total_sent += stat['sent_count']
        total_conversions += stat['conversion_count']

    if total_sent > 0:
        total_rate = (total_conversions / total_sent * 100)
        text += f"\n━━━━━━━━━━━━━━━━━━━━\n\n📈 Всього конверсій: {total_conversions} ({total_rate:.1f}%)\n"
        text += f"👥 Всього отримувачів: {total_sent}\n\n"
        text += "Конверсія = клієнт відвідав конференцію протягом 7 днів після повідомлення"

    await c.message.edit_text(text, reply_markup=kb_motivational_menu())

@dp.callback_query(F.data == "motivational:edit:menu")
async def motivational_edit_menu(c: CallbackQuery):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    await c.message.edit_text(
        "✏️ РЕДАГУВАННЯ МОТИВУЮЧИХ ПОВІДОМЛЕНЬ\n\nОберіть повідомлення для редагування:",
        reply_markup=kb_motivational_edit_menu()
    )

@dp.callback_query(F.data.startswith("motivational:edit:") and F.data[-1].isdigit())
async def motivational_edit_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    msg_num = c.data.split(":")[-1]
    msg_key = f"motivational.{msg_num}"

    # Отримуємо поточний текст
    current_text = await messages_get(msg_key, 'uk')

    await state.update_data(message_key=msg_key, message_num=msg_num)

    await c.message.edit_text(
        f"📝 ПОВІДОМЛЕННЯ №{msg_num}\n\nПоточний текст:\n────────────────────────\n{current_text}\n────────────────────────\n\n"
        f"Надішліть новий текст або /cancel для скасування:",
        reply_markup=None
    )

    await state.set_state(MotivationalEditSG.wait_text)

@dp.message(MotivationalEditSG.wait_text, F.text == "/cancel")
async def motivational_edit_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Редагування скасовано.", reply_markup=kb_motivational_menu())

@dp.message(MotivationalEditSG.wait_text)
async def motivational_edit_receive_text(m: Message, state: FSMContext):
    if m.from_user.id not in ADMINS:
        return

    new_text = m.text
    await state.update_data(new_text=new_text)
    data = await state.get_data()
    msg_num = data.get('message_num')

    preview = f"👀 ПОПЕРЕДНІЙ ПЕРЕГЛЯД\n\nПовідомлення №{msg_num}:\n────────────────────────\n{new_text}\n────────────────────────"

    await m.answer(preview, reply_markup=kb_motivational_edit_confirm(), parse_mode=None)
    await state.set_state(MotivationalEditSG.preview)

@dp.callback_query(F.data == "motivational:save:edit", MotivationalEditSG.preview)
async def motivational_save_edit(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    data = await state.get_data()
    msg_num = data.get('message_num')

    await c.message.edit_text(
        f"✏️ Надішліть новий текст для повідомлення №{msg_num}:",
        reply_markup=None
    )
    await state.set_state(MotivationalEditSG.wait_text)

@dp.callback_query(F.data == "motivational:save:yes", MotivationalEditSG.preview)
async def motivational_save_yes(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    data = await state.get_data()
    msg_key = data.get('message_key')
    msg_num = data.get('message_num')
    new_text = data.get('new_text')

    # Оновлюємо в БД
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE messages SET text = $1 WHERE key = $2 AND lang = 'uk'",
            new_text, msg_key
        )

    await c.message.edit_text(
        f"✅ Текст повідомлення №{msg_num} оновлено!",
        reply_markup=kb_motivational_menu()
    )
    await state.clear()

@dp.callback_query(F.data == "motivational:test:menu")
async def motivational_test_menu(c: CallbackQuery):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    await c.message.edit_text(
        "🧪 ТЕСТОВА ВІДПРАВКА\n\nОберіть повідомлення для тестування:",
        reply_markup=kb_motivational_test_menu()
    )

@dp.callback_query(F.data.startswith("motivational:test:") and F.data[-1].isdigit())
async def motivational_test_send(c: CallbackQuery):
    if c.from_user.id not in ADMINS:
        await c.answer("❌ Доступ заборонено")
        return

    msg_num = c.data.split(":")[-1]
    msg_key = f"motivational.{msg_num}"

    # Отримуємо текст
    text = await messages_get(msg_key, 'uk')

    if not text:
        await c.answer("❌ Повідомлення не знайдено")
        return

    # Відправляємо менеджеру
    try:
        await bot.send_message(c.from_user.id, text, parse_mode=None)
        await c.answer(f"✅ Тестове повідомлення №{msg_num} надіслано!", show_alert=True)
    except Exception as e:
        await c.answer(f"❌ Помилка: {e}", show_alert=True)

# ========================= CUSTOM CONFERENCE HANDLERS ==========================

@dp.callback_query(F.data == "admin:custom")
async def admin_custom_start(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    ADMINS.add(q.from_user.id)
    await state.set_state(CustomConfSG.wait_title)
    await q.message.edit_text("📞 Кастомна конференція\n\nВведіть назву конференції:")
    await q.answer()

@dp.message(CustomConfSG.wait_title)
async def custom_wait_title(m: Message, state: FSMContext):
    title = (m.text or "").strip()
    if len(title) < 3:
        await m.answer("Назва занадто коротка. Спробуйте ще раз:")
        return
    await state.update_data(title=title)
    await state.set_state(CustomConfSG.wait_desc)
    await m.answer("Введіть опис конференції:")

@dp.message(CustomConfSG.wait_desc)
async def custom_wait_desc(m: Message, state: FSMContext):
    desc = (m.text or "").strip()
    await state.update_data(description=desc)
    await state.set_state(CustomConfSG.wait_start_at)
    await m.answer("Вкажіть дату та час початку у форматі: YYYY-MM-DD HH:MM\nНапр.: 2025-10-05 15:00")

@dp.message(CustomConfSG.wait_start_at)
async def custom_wait_start_at(m: Message, state: FSMContext):
    dt = parse_dt(m.text or "")
    if not dt:
        await m.answer("Невірний формат. Приклад: 2025-10-05 15:00. Спробуйте ще раз:")
        return
    await state.update_data(start_at=iso_dt(dt))
    await state.set_state(CustomConfSG.wait_duration)
    await m.answer("Вкажіть тривалість у хвилинах:")

@dp.message(CustomConfSG.wait_duration)
async def custom_wait_duration(m: Message, state: FSMContext):
    try:
        dur = int((m.text or "").strip())
        if dur <= 0:
            raise ValueError()
    except Exception:
        await m.answer("Введіть додатне ціле число. Спробуйте ще раз:")
        return
    await state.update_data(duration_min=dur)
    await state.set_state(CustomConfSG.wait_link)
    await m.answer("Вставте посилання на конференцію (URL):")

@dp.message(CustomConfSG.wait_link)
async def custom_wait_link(m: Message, state: FSMContext):
    link = (m.text or "").strip()
    await state.update_data(link=link)
    await state.set_state(CustomConfSG.wait_phones)
    await m.answer(
        "Введіть номери телефонів клієнтів:\n"
        "• Кожен номер з нового рядка або через кому\n"
        "• Формати: 380XXXXXXXXX, 0XXXXXXXXX, +380XXXXXXXXX\n\n"
        "Приклад:\n380671234567\n380952345678"
    )

@dp.message(CustomConfSG.wait_phones)
async def custom_wait_phones(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    parts = re.split(r"[\n,;]+", raw)

    found_clients = []
    not_found_lines = []

    for part in parts:
        phone_raw = part.strip()
        if not phone_raw:
            continue
        phone = normalize_phone(phone_raw)
        if not phone:
            not_found_lines.append(f"❌ {phone_raw} — невірний формат")
            continue
        client = await get_client_by_phone(phone)
        if not client:
            not_found_lines.append(f"❌ {phone_raw} — не знайдено в базі")
        else:
            # Deduplicate by client_id
            if not any(c['client_id'] == client['client_id'] for c in found_clients):
                found_clients.append(client)

    data = await state.get_data()
    dt = parse_dt(data['start_at'])
    date_str = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else data['start_at']
    total = len(found_clients) + len(not_found_lines)

    text = (
        f"👀 ПОПЕРЕДНІЙ ПЕРЕГЛЯД:\n"
        f"─────────────────────\n"
        f"Назва: {data['title']}\n"
        f"Опис: {data['description']}\n"
        f"Дата: {date_str}\n"
        f"Тривалість: {data['duration_min']} хв\n"
        f"Посилання: {data['link']}\n\n"
        f"Знайдено клієнтів: {len(found_clients)} з {total}\n"
    )
    for cli in found_clients:
        text += f"✅ {cli['full_name']} ({cli['phone']})\n"
    for line in not_found_lines:
        text += f"{line}\n"

    if not found_clients:
        text += "\n⚠️ Жодного клієнта не знайдено. Перевірте номери."
        await m.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✏️ Ввести номери знову", callback_data="custom:confirm:edit")],
                [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin:home")],
            ]),
            parse_mode=None
        )
        return

    await state.update_data(found_client_ids=[c['client_id'] for c in found_clients])
    await state.set_state(CustomConfSG.confirm)
    await m.answer(text, reply_markup=kb_custom_confirm(), parse_mode=None)

@dp.callback_query(F.data == "custom:confirm:edit")
async def custom_confirm_edit(q: CallbackQuery, state: FSMContext):
    await q.answer()
    ADMINS.add(q.from_user.id)
    await state.set_state(CustomConfSG.wait_phones)
    await q.message.edit_text(
        "Введіть номери телефонів знову:\n"
        "• Кожен номер з нового рядка або через кому\n"
        "• Формати: 380XXXXXXXXX, 0XXXXXXXXX, +380XXXXXXXXX"
    )

@dp.callback_query(F.data == "custom:confirm:yes")
async def custom_confirm_yes(q: CallbackQuery, state: FSMContext):
    await q.answer()
    ADMINS.add(q.from_user.id)

    data = await state.get_data()
    client_ids = data.get('found_client_ids')

    if not client_ids or 'title' not in data:
        await q.message.edit_text(
            "❌ Сесія застаріла або дані втрачено. Почніть знову.",
            reply_markup=kb_admin_main()
        )
        await state.clear()
        return

    try:
        event = await create_event(
            type_code=0,
            title=data['title'],
            description=data['description'],
            start_at=data['start_at'],
            duration_min=int(data['duration_min']),
            link=data['link'],
            created_by=q.from_user.id,
        )
    except Exception as e:
        await q.message.edit_text(
            f"❌ Помилка створення події: {e}\n\nСпробуйте ще раз.",
            reply_markup=kb_admin_main()
        )
        await state.clear()
        return

    clients = []
    for cid in client_ids:
        cli = await get_client_by_id(cid)
        if cli:
            clients.append(cli)

    await q.message.edit_text(f"⏳ Надсилаю запрошення {len(clients)} клієнтам...")

    try:
        result = await send_custom_invites(event, clients)
    except Exception as e:
        await q.message.edit_text(
            f"❌ Помилка розсилки: {e}",
            reply_markup=kb_admin_main()
        )
        await state.clear()
        return

    await state.clear()

    report = (
        f"✅ Кастомну конференцію створено!\n\n"
        f"📌 {event['title']}\n"
        f"🗓 {event['start_at']}\n\n"
        f"📊 Результати розсилки:\n"
        f"✅ Надіслано: {result['sent']}\n"
        f"❌ Помилка доставки: {result['failed']}"
    )
    await q.message.edit_text(report, reply_markup=kb_admin_main())


# =============================== SCHEDULER TICK ================================

async def scheduler_tick():
    try:
        now = now_kyiv()

        # ДЛЯ ТЕСТИРОВАНИЯ: уменьшенные интервалы
        #REM_24H = 2*60      # 2 минуты вместо 24 часов (напоминание за "24ч")
        #REM_60M = 1*60      # 1 минута вместо 1 часа (напоминание за "1ч")
        #FEEDBACK_DELAY = 1*60   # 1 минута после окончания
        #JITTER = 30             # 30 секунд для точности срабатывания

        # ДЛЯ ПРОДАКШЕНА раскомментируй:
        REM_24H = 60*60        # Напоминание за 1 час
        REM_60M = 10*60        # Напоминание за 10 минут
        FEEDBACK_DELAY = 5*60
        JITTER = 60

        for e in await list_future_events_sorted():
            dt = event_start_dt(e)
            if not dt:
                continue

            diff = (dt - now).total_seconds()

            # Напоминание за 24 часа
            if abs(diff - REM_24H) <= JITTER:
                for r in await rsvp_get_for_event(e["event_id"]):
                    cid = r.get("client_id")
                    tg_id = await try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue
                    # Проверка статуса клиента
                    client = await get_client_by_tg(tg_id)
                    if not client or client.get('status') != 'active':
                        continue
                    if r.get("reminded_24h"):
                        continue
                    if str(r.get("rsvp")) == "going":
                        body = (await messages_get("reminder.24h")).format(
                            title=e["title"], time=fmt_time(dt), link=e["link"]
                        )
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=body)
                            await rsvp_upsert(e["event_id"], cid, reminded_24h=True)
                            await log_action("remind_24h_sent", client_id=cid, event_id=e["event_id"], details="prod_24h")
                        except Exception:
                            pass

            # Напоминание за 60 минут
            if abs(diff - REM_60M) <= JITTER:
                for r in await rsvp_get_for_event(e["event_id"]):
                    cid = r.get("client_id")
                    tg_id = await try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue
                    # Проверка статуса клиента
                    client = await get_client_by_tg(tg_id)
                    if not client or client.get('status') != 'active':
                        continue
                    if r.get("reminded_60m"):
                        continue
                    if str(r.get("rsvp")) == "going":
                        body = (await messages_get("reminder.60m")).format(title=e["title"], link=e["link"])
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=body)
                            await rsvp_upsert(e["event_id"], cid, reminded_60m=True)
                            await log_action("remind_60m_sent", client_id=cid, event_id=e["event_id"], details="prod_60m")
                        except Exception:
                            pass

            # Опрос "Удалось присоединиться?" после окончания (кроме type_code 4)
            end_dt = dt + timedelta(minutes=a2i(e.get("duration_min")))
            post_end = (now - end_dt).total_seconds()
            if abs(post_end - FEEDBACK_DELAY) <= JITTER:
                if await has_log("post_event_survey_requested", 0, e["event_id"]):
                    continue

                # СРАЗУ логируем ДО отправки, чтобы избежать дублей
                await log_action("post_event_survey_requested", event_id=e["event_id"], details=f"delay={FEEDBACK_DELAY}")

                # Для type_code 4 оставляем старую логику (опрос о документах)
                if e.get("type") == 4:
                    continue

                # Ищем клиентов с rsvp='going' которым еще не отправляли опрос
                async with db_pool.acquire() as conn:
                    rows_rsvp = await conn.fetch(
                        """SELECT r.client_id, r.event_id
                           FROM rsvp r
                           WHERE r.event_id = $1
                             AND r.rsvp = 'going'
                             AND (r.post_event_survey_sent IS NULL OR r.post_event_survey_sent = FALSE)""",
                        e["event_id"]
                    )

                    for r in rows_rsvp:
                        cid = r.get("client_id")
                        tg_id = await try_get_tg_from_client_id(cid)
                        if not tg_id:
                            continue
                        # Проверка статуса клиента
                        client = await get_client_by_tg(tg_id)
                        if not client or client.get('status') != 'active':
                            continue

                        text = (
                            f"Вітаємо! 👋\n\n"
                            f"Конференція «{e['title']}» завершилася.\n\n"
                            f"Чи вдалося вам приєднатися?"
                        )
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="✅ Так, був(ла)",
                                                callback_data=f"post_survey:yes:{e['event_id']}:{cid}")],
                            [InlineKeyboardButton(text="❌ Ні, не зміг(ла)",
                                                callback_data=f"post_survey:no:{e['event_id']}:{cid}")]
                        ])
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=text, reply_markup=kb)
                            # Помечаем что опрос отправлен
                            await conn.execute(
                                "UPDATE rsvp SET post_event_survey_sent = TRUE WHERE event_id = $1 AND client_id = $2",
                                e["event_id"], cid
                            )
                            await log_action("post_event_survey_sent", client_id=cid, event_id=e["event_id"])
                        except Exception:
                            pass

        # Мотивуючі повідомлення (запускаємо кожну годину)
        if now.minute == 0:  # На початку кожної години
            await send_motivational_messages()

    except Exception as e:
        import traceback
        print(f"Error in scheduler_tick: {e}\n{traceback.format_exc()}")

# ================================ STARTUP ======================================

async def on_startup():
    await init_db()
    scheduler.add_job(scheduler_tick, "interval", seconds=60, id="tick", replace_existing=True)
    scheduler.start()

async def on_shutdown():
    await close_db()

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
