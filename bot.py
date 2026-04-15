import os
import re
import json
import html
import random
import sqlite3
import string
import logging
from datetime import datetime, timedelta
from itertools import count

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

try:
    from telegram import CopyTextButton
except ImportError:
    CopyTextButton = None  # type: ignore[misc, assignment]

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("Не найдена переменная окружения BOT_TOKEN")

ADMIN_IDS = {
    8463296102,
}

MANAGER_USER_ID = 8423978061
DEFAULT_MANAGER_URL = f"tg://user?id={MANAGER_USER_ID}"

# ВАЖНО: замени на реальный chat_id группы менеджеров
ORDER_GROUP_ID = int(os.getenv("ORDER_GROUP_ID", "-1003913158040"))

# SQLite в каталоге контейнера без постоянного диска = после каждого деплоя НОВАЯ пустая база
# (весь каталог, настройки, ссылки, file_id фото). Прод: DATABASE_URL (Postgres) или volume + SQLITE_PATH.
DB_PATH = os.getenv("SQLITE_PATH", "rndm.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DEFAULT_BARAHOLKI_URL = "https://t.me/your_channel/1"
DEFAULT_PROJECTS_URL = "https://t.me/your_channel/2"
DEFAULT_GIVEAWAYS_URL = "https://t.me/your_channel/3"
DEFAULT_VK_URL = "https://vk.ru/rndm196"

# Дефолты для *первого* запуска (пустая БД). После деплоя без сохранённой БД снова подтянутся отсюда.
# Стабильное фото: задай WELCOME_PHOTO_URL (https://…) в переменных окружения на сервере.
DEFAULT_WELCOME_CAPTION = (
    "🔥 Добро пожаловать в RNDM SHOP\n\n"
    "Мы на рынке уже более года и собрали 1000+ отзывов от наших клиентов 💬\n\n"
    "У нас ты найдёшь:\n"
    "💨 одноразки\n"
    "🧪 жидкости\n"
    "⚡ pod-системы\n"
    "⚙️ расходники и многое другое\n\n"
    "💸 по доступным ценам и с быстрым оформлением\n\n"
    "🚚 Доставка по Екатеринбургу\n"
    "⌚ Работаем 24/7\n\n"
    "👇 Используй кнопки ниже для навигации"
)

CATEGORY_ORDER = [
    "devices",
    "liquids",
    "disposables",
    "plates",
    "supplies",
    "sale",
]

CATEGORY_LABELS = {
    "devices": "⚡ УСТРОЙСТВА",
    "liquids": "💧 ЖИДКОСТИ",
    "disposables": "🔥 ОДНОРАЗКИ",
    "plates": "🧊 ШАЙБЫ/ПЛАСТИНКИ",
    "supplies": "🛠 РАСХОДНИКИ",
    "sale": "💸 СЛИВ/СКИДКИ",
}

DEFAULT_ITEMS = [
    {
        "item_key": "xros",
        "category_key": "devices",
        "label": "XROS",
        "description": "Популярные устройства линейки XROS.\nНаличие цветов уточнять у менеджеров после заказа 💜",
        "image": "",
        "sort_order": 1,
        "price": 2190,
    },
    {
        "item_key": "aegis_hero_3",
        "category_key": "devices",
        "label": "AEGIS HERO 3",
        "description": "Наличие цветов уточнять у менеджеров после заказа 💜",
        "image": "",
        "sort_order": 2,
        "price": 3190,
    },
    {
        "item_key": "pasito_2",
        "category_key": "devices",
        "label": "PASITO 2",
        "description": "Наличие цветов уточнять у менеджеров после заказа 💜",
        "image": "",
        "sort_order": 3,
        "price": 0,
    },
]

DEFAULT_PICKUP_POINTS = [
    "Академ",
    "Сортировка",
    "Центр",
    "Юго-Запад",
    "Железнодорожный",
    "ЖБИ",
    "Верхняя Пышма",
]

_state = count()

ADMIN_BROADCAST_WAITING = next(_state)
ADMIN_BARAHOLKI_WAITING = next(_state)
ADMIN_PROJECTS_WAITING = next(_state)
ADMIN_GIVEAWAYS_WAITING = next(_state)
ADMIN_MANAGER_WAITING = next(_state)

ADMIN_ADD_ITEM_CATEGORY_WAITING = next(_state)
ADMIN_ADD_ITEM_NAME_WAITING = next(_state)
ADMIN_ADD_ITEM_DESC_WAITING = next(_state)
ADMIN_ADD_ITEM_PRICE_WAITING = next(_state)
ADMIN_ADD_ITEM_IMAGE_WAITING = next(_state)

ADMIN_RENAME_ITEM_CATEGORY_WAITING = next(_state)
ADMIN_RENAME_ITEM_SELECT_WAITING = next(_state)
ADMIN_RENAME_ITEM_NEW_NAME_WAITING = next(_state)

ADMIN_EDIT_DESC_CATEGORY_WAITING = next(_state)
ADMIN_EDIT_DESC_SELECT_WAITING = next(_state)
ADMIN_EDIT_DESC_NEW_WAITING = next(_state)

ADMIN_EDIT_IMAGE_CATEGORY_WAITING = next(_state)
ADMIN_EDIT_IMAGE_SELECT_WAITING = next(_state)
ADMIN_EDIT_IMAGE_NEW_WAITING = next(_state)

ADMIN_EDIT_PRICE_CATEGORY_WAITING = next(_state)
ADMIN_EDIT_PRICE_SELECT_WAITING = next(_state)
ADMIN_EDIT_PRICE_NEW_WAITING = next(_state)

ADMIN_DELETE_ITEM_CATEGORY_WAITING = next(_state)
ADMIN_DELETE_ITEM_SELECT_WAITING = next(_state)

ADMIN_REORDER_ITEM_CATEGORY_WAITING = next(_state)
ADMIN_REORDER_ITEM_WAITING = next(_state)

ADMIN_ADD_PICKUP_NAME_WAITING = next(_state)
ADMIN_RENAME_PICKUP_SELECT_WAITING = next(_state)
ADMIN_RENAME_PICKUP_NEW_WAITING = next(_state)
ADMIN_DELETE_PICKUP_SELECT_WAITING = next(_state)
ADMIN_REORDER_PICKUP_WAITING = next(_state)

ADMIN_SET_CATEGORY_PHOTO_CATEGORY_WAITING = next(_state)
ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING = next(_state)
ADMIN_CLEAR_CATEGORY_PHOTO_CATEGORY_WAITING = next(_state)

ADMIN_INFO_BLOCK_SELECT_WAITING = next(_state)
ADMIN_INFO_BLOCK_ACTION_WAITING = next(_state)
ADMIN_INFO_BLOCK_TEXT_WAITING = next(_state)
ADMIN_INFO_BLOCK_PHOTO_WAITING = next(_state)
ADMIN_REF_GIVEAWAY_WAITING = next(_state)
ADMIN_GIVEAWAY_CREATE_TEXT_WAITING = next(_state)
ADMIN_GIVEAWAY_CREATE_DESC_WAITING = next(_state)
ADMIN_GIVEAWAY_CREATE_IMAGE_WAITING = next(_state)
ADMIN_GIVEAWAY_CREATE_BUTTONS_WAITING = next(_state)
ADMIN_GIVEAWAY_FINISH_WAITING = next(_state)
ADMIN_GIVEAWAY_AUTOBROADCAST_PER_DAY_WAITING = next(_state)
ADMIN_AUTOPOST_TEXT_WAITING = next(_state)
ADMIN_AUTOPOST_PHOTO_WAITING = next(_state)
ADMIN_AUTOPOST_BUTTON_WAITING = next(_state)
ADMIN_AUTOPOST_INTERVAL_WAITING = next(_state)
ADMIN_BLACKLIST_WAITING = next(_state)
ADMIN_CATEGORY_DISCOUNT_WAITING = next(_state)
ADMIN_WELCOME_MENU_WAITING = next(_state)
ADMIN_WELCOME_TEXT_WAITING = next(_state)
ADMIN_WELCOME_PHOTO_WAITING = next(_state)
ADMIN_WELCOME_BUTTONS_WAITING = next(_state)
ADMIN_REFERRAL_HUB_PHOTO_WAITING = next(_state)
ADMIN_CREATE_PROMO_CODE_WAITING = next(_state)
ADMIN_CREATE_PROMO_DISCOUNT_WAITING = next(_state)

ORDER_PROMOCODE_WAITING = next(_state)
ORDER_CHOICE_WAITING = next(_state)
ORDER_DELIVERY_PHONE_WAITING = next(_state)
ORDER_DELIVERY_USERNAME_WAITING = next(_state)
ORDER_DELIVERY_ADDRESS_WAITING = next(_state)
ORDER_DELIVERY_TIME_WAITING = next(_state)

ORDER_PICKUP_POINT_WAITING = next(_state)
ORDER_PICKUP_PHONE_WAITING = next(_state)
ORDER_PICKUP_TIME_WAITING = next(_state)
ORDER_PICKUP_USERNAME_WAITING = next(_state)

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


USE_POSTGRES = bool(DATABASE_URL)
if USE_POSTGRES:
    try:
        import psycopg
    except Exception as e:
        raise RuntimeError(
            "DATABASE_URL задан, но модуль psycopg не установлен. "
            "Добавь зависимость psycopg[binary] в requirements и задеплой заново."
        ) from e


class DBCursor:
    def __init__(self, raw_cursor, use_postgres: bool, connection):
        self.raw_cursor = raw_cursor
        self.use_postgres = use_postgres
        self.connection = connection

    def execute(self, query: str, params: tuple | list | None = None):
        sql = query.replace("?", "%s") if self.use_postgres else query
        try:
            if params is None:
                self.raw_cursor.execute(sql)
            else:
                self.raw_cursor.execute(sql, params)
        except Exception:
            # Для PostgreSQL откатываем текущую транзакцию, иначе соединение
            # остаётся в aborted-состоянии и следующие запросы "зависают"/падают.
            if self.use_postgres:
                self.connection.rollback()
            raise
        return self

    def fetchone(self):
        return self.raw_cursor.fetchone()

    def fetchall(self):
        return self.raw_cursor.fetchall()

    @property
    def lastrowid(self):
        return getattr(self.raw_cursor, "lastrowid", None)

    @property
    def rowcount(self):
        return getattr(self.raw_cursor, "rowcount", -1)


if USE_POSTGRES:
    conn = psycopg.connect(DATABASE_URL)
    # Автокоммит снижает риск зависания сценариев при единичных ошибках запросов.
    conn.autocommit = True
    cursor = DBCursor(conn.cursor(), use_postgres=True, connection=conn)
    logger.info("Подключение к PostgreSQL активно")
    logger.info(
        "Данные в PostgreSQL не в образе бота. Если после деплоя всё обнуляется — "
        "проверь, что DATABASE_URL один и тот же и это не одноразовый/ephemeral Postgres."
    )
else:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = DBCursor(conn.cursor(), use_postgres=False, connection=conn)
    _db_abs = os.path.abspath(DB_PATH)
    logger.info("Используется SQLite: %s", _db_abs)
    logger.warning(
        "БД: SQLite на диске приложения. При деплое на Railway/Fly и т.п. без постоянного тома "
        "файл базы каждый раз новый — пропадают товары, фото, ссылки, пользователи, заказы. "
        "Для продакшена: задай DATABASE_URL (Postgres плагин) ИЛИ Railway Volume + SQLITE_PATH вроде /data/rndm.db"
    )


def now_iso() -> str:
    return datetime.now().isoformat()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def ensure_column(table_name: str, column_name: str, column_def: str) -> None:
    if USE_POSTGRES:
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            """,
            (table_name, column_name),
        )
        exists = cursor.fetchone() is not None
    else:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        exists = column_name in columns

    if not exists:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()


def ensure_postgres_cart_items_primary_key() -> None:
    """Без PRIMARY KEY (user_id, item_id) INSERT ... ON CONFLICT в PostgreSQL падает — корзина не сохраняется."""
    if not USE_POSTGRES:
        return
    try:
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_schema = 'public'
              AND table_name = 'cart_items'
              AND constraint_type = 'PRIMARY KEY'
            """
        )
        if cursor.fetchone():
            return
        cursor.execute(
            """
            SELECT user_id, item_id, SUM(quantity) AS total_qty
            FROM cart_items
            GROUP BY user_id, item_id
            HAVING COUNT(*) > 1
            """
        )
        dup_rows = cursor.fetchall()
        for row in dup_rows:
            uid, iid, total_qty = int(row[0]), int(row[1]), int(row[2])
            cursor.execute(
                "DELETE FROM cart_items WHERE user_id = ? AND item_id = ?",
                (uid, iid),
            )
            cursor.execute(
                "INSERT INTO cart_items (user_id, item_id, quantity) VALUES (?, ?, ?)",
                (uid, iid, total_qty),
            )
        cursor.execute("ALTER TABLE cart_items ADD PRIMARY KEY (user_id, item_id)")
        conn.commit()
        logger.info("cart_items: добавлен PRIMARY KEY (user_id, item_id)")
    except Exception:
        logger.exception("cart_items: не удалось восстановить PRIMARY KEY")


def ensure_postgres_orders_status_updated_by_bigint() -> None:
    """status_updated_by был INTEGER — Telegram user_id часто > 2^31-1, UPDATE статуса заказа падал."""
    if not USE_POSTGRES:
        return
    try:
        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'orders'
              AND column_name = 'status_updated_by'
            """
        )
        row = cursor.fetchone()
        if not row:
            return
        dt = (row[0] or "").lower()
        if dt == "bigint":
            return
        if dt in ("integer", "smallint"):
            cursor.execute(
                "ALTER TABLE orders ALTER COLUMN status_updated_by TYPE BIGINT USING status_updated_by::bigint"
            )
            conn.commit()
            logger.info("orders.status_updated_by → BIGINT (для больших Telegram user_id)")
    except Exception:
        logger.exception("Не удалось привести orders.status_updated_by к BIGINT")


def init_database() -> None:
    if USE_POSTGRES:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_seen TEXT,
                last_spin TEXT,
                referred_by BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                discount INTEGER NOT NULL,
                used INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                used_at TEXT,
                owner_user_id BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS items (
                item_id BIGSERIAL PRIMARY KEY,
                item_key TEXT UNIQUE NOT NULL,
                category_key TEXT NOT NULL,
                label TEXT NOT NULL,
                description TEXT NOT NULL,
                image TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                price INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pickup_points (
                pickup_id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                sort_order INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cart_items (
                user_id BIGINT NOT NULL,
                item_id BIGINT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, item_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                order_type TEXT NOT NULL,
                pickup_point TEXT,
                phone TEXT,
                contact_username TEXT,
                address TEXT,
                delivery_time TEXT,
                items_text TEXT NOT NULL,
                total_sum INTEGER NOT NULL DEFAULT 0,
                comment TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS referrals (
                inviter_id BIGINT NOT NULL,
                invited_id BIGINT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                PRIMARY KEY (inviter_id, invited_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS referral_winners (
                winner_id BIGINT NOT NULL,
                invites_count INTEGER NOT NULL DEFAULT 0,
                selected_at TEXT NOT NULL,
                selected_by BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaways (
                giveaway_id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                text_value TEXT NOT NULL,
                photo TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                created_by BIGINT NOT NULL,
                finished_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaway_referrals (
                giveaway_id BIGINT NOT NULL,
                inviter_id BIGINT NOT NULL,
                invited_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (giveaway_id, invited_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaway_winners (
                giveaway_id BIGINT NOT NULL,
                winner_id BIGINT NOT NULL,
                invites_count INTEGER NOT NULL DEFAULT 0,
                selected_at TEXT NOT NULL,
                selected_by BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auto_posts (
                post_id BIGSERIAL PRIMARY KEY,
                text_value TEXT NOT NULL,
                photo TEXT,
                button_text TEXT,
                button_url TEXT,
                interval_hours INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_sent_at TEXT,
                next_send_at TEXT,
                sent_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                created_by BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS broadcast_logs (
                log_id BIGSERIAL PRIMARY KEY,
                kind TEXT NOT NULL,
                post_id BIGINT,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                created_by BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                added_at TEXT NOT NULL,
                added_by BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS customer_ratings (
                rating_id BIGSERIAL PRIMARY KEY,
                order_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                manager_id BIGINT NOT NULL,
                rating INTEGER NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS category_discounts (
                category_key TEXT PRIMARY KEY,
                discount_percent INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                updated_by BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS action_logs (
                log_id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                action_type TEXT NOT NULL,
                payload TEXT,
                created_at TEXT NOT NULL
            )
            """,
        ]
    else:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_seen TEXT,
                last_spin TEXT,
                referred_by INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                discount INTEGER NOT NULL,
                used INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                used_at TEXT,
                owner_user_id INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_key TEXT UNIQUE NOT NULL,
                category_key TEXT NOT NULL,
                label TEXT NOT NULL,
                description TEXT NOT NULL,
                image TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                price INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pickup_points (
                pickup_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sort_order INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cart_items (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, item_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                order_type TEXT NOT NULL,
                pickup_point TEXT,
                phone TEXT,
                contact_username TEXT,
                address TEXT,
                delivery_time TEXT,
                items_text TEXT NOT NULL,
                total_sum INTEGER NOT NULL DEFAULT 0,
                comment TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS referrals (
                inviter_id INTEGER NOT NULL,
                invited_id INTEGER NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                PRIMARY KEY (inviter_id, invited_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS referral_winners (
                winner_id INTEGER NOT NULL,
                invites_count INTEGER NOT NULL DEFAULT 0,
                selected_at TEXT NOT NULL,
                selected_by INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaways (
                giveaway_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                text_value TEXT NOT NULL,
                photo TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                finished_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaway_referrals (
                giveaway_id INTEGER NOT NULL,
                inviter_id INTEGER NOT NULL,
                invited_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (giveaway_id, invited_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS giveaway_winners (
                giveaway_id INTEGER NOT NULL,
                winner_id INTEGER NOT NULL,
                invites_count INTEGER NOT NULL DEFAULT 0,
                selected_at TEXT NOT NULL,
                selected_by INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auto_posts (
                post_id INTEGER PRIMARY KEY AUTOINCREMENT,
                text_value TEXT NOT NULL,
                photo TEXT,
                button_text TEXT,
                button_url TEXT,
                interval_hours INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_sent_at TEXT,
                next_send_at TEXT,
                sent_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                created_by INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS broadcast_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                post_id INTEGER,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                created_by INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                added_at TEXT NOT NULL,
                added_by INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS customer_ratings (
                rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                manager_id INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS category_discounts (
                category_key TEXT PRIMARY KEY,
                discount_percent INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                updated_by INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS action_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action_type TEXT NOT NULL,
                payload TEXT,
                created_at TEXT NOT NULL
            )
            """,
        ]

    for statement in statements:
        cursor.execute(statement)

    conn.commit()
    ensure_column("items", "price", "INTEGER NOT NULL DEFAULT 0")
    ensure_column("users", "referred_by", "INTEGER")
    ensure_column("orders", "status", "TEXT NOT NULL DEFAULT 'new'")
    ensure_column("orders", "status_updated_at", "TEXT")
    ensure_column("orders", "status_updated_by", "BIGINT")
    ensure_column("orders", "order_subtotal", "INTEGER")
    ensure_column("orders", "promo_code", "TEXT")
    ensure_column("orders", "discount_percent", "INTEGER NOT NULL DEFAULT 0")
    ensure_column("orders", "discount_amount", "INTEGER NOT NULL DEFAULT 0")
    ensure_column("broadcast_logs", "blocked", "INTEGER NOT NULL DEFAULT 0")
    ensure_column("broadcast_logs", "details", "TEXT")
    ensure_postgres_cart_items_primary_key()
    ensure_postgres_orders_status_updated_by_bigint()
    try:
        cursor.execute("UPDATE orders SET order_subtotal = total_sum WHERE order_subtotal IS NULL")
        conn.commit()
    except Exception:
        logger.exception("orders.order_subtotal backfill")


init_database()

ensure_column("giveaways", "buttons_json", "TEXT NOT NULL DEFAULT '[]'")
ensure_column("giveaways", "results_broadcast_at", "TEXT")
ensure_column("giveaways", "autobroadcast_enabled", "INTEGER NOT NULL DEFAULT 0")
ensure_column("giveaways", "autobroadcast_per_day", "INTEGER NOT NULL DEFAULT 2")
ensure_column("giveaways", "autobroadcast_last_at", "TEXT")
ensure_column("referrals", "qualified_at", "TEXT")
ensure_column("promocodes", "admin_global", "INTEGER NOT NULL DEFAULT 0")

GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY = 1
GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY = 48
GIVEAWAY_AUTOBROADCAST_DEFAULT_PER_DAY = 2


def set_setting(key: str, value: str) -> None:
    cursor.execute(
        """
        INSERT INTO settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
        """,
        (key, value),
    )
    conn.commit()


def get_setting(key: str, default: str = "") -> str:
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else default


def is_valid_inline_button_url(url: str) -> bool:
    """Telegram принимает для url-кнопок только http(s) и tg://."""
    u = (url or "").strip()
    return u.startswith(("http://", "https://", "tg://"))


def category_image_setting_key(category_key: str) -> str:
    return f"category_image:{category_key}"


def get_category_image(category_key: str) -> str:
    return get_setting(category_image_setting_key(category_key), "")


def set_category_image(category_key: str, file_id: str) -> None:
    set_setting(category_image_setting_key(category_key), file_id)


def clear_category_image(category_key: str) -> None:
    cursor.execute("DELETE FROM settings WHERE key = ?", (category_image_setting_key(category_key),))
    conn.commit()


INFO_BLOCK_LABELS = {
    "vk": "📱 Наш VK",
    "baraholki": "🛒 Наши барахолки",
    "projects": "🚀 Наши проекты",
    "giveaways": "🎁 Розыгрыши",
}

INFO_BLOCK_DEFAULTS = {
    "vk": "📱 Наш VK\n\nЗдесь ты можешь следить за нашими точками и новостями во VK.",
    "baraholki": "🛒 Наши барахолки\n\nАктуальная информация по нашим барахолкам.",
    "projects": "🚀 Наши проекты\n\nАктуальная информация по нашим проектам.",
    "giveaways": "🎁 Розыгрыши\n\nПока нет активного розыгрыша — загляни позже.",
}


INFO_BLOCK_URL_SETTINGS = {
    "vk": "vk_url",
    "baraholki": "baraholki_url",
    "projects": "projects_url",
    "giveaways": "giveaways_url",
}

INFO_BLOCK_BUTTON_LABELS = {
    "vk": "📱 Открыть VK",
    "baraholki": "🛒 Перейти в барахолки",
    "projects": "🚀 Открыть проекты",
    "giveaways": "🎁 Смотреть розыгрыши",
}

def get_info_block_text(block_key: str) -> str:
    return get_setting(f"info_text:{block_key}", INFO_BLOCK_DEFAULTS.get(block_key, "ℹ️ Информация скоро появится."))


def set_info_block_text(block_key: str, value: str) -> None:
    set_setting(f"info_text:{block_key}", value)


def get_info_block_photo(block_key: str) -> str:
    return get_setting(f"info_photo:{block_key}", "")


def set_info_block_photo(block_key: str, file_id: str) -> None:
    set_setting(f"info_photo:{block_key}", file_id)


def parse_info_block_from_label(label: str):
    cleaned = label.strip()
    cleaned_simple = simplify_menu_label(cleaned)
    for key, value in INFO_BLOCK_LABELS.items():
        if value == cleaned or simplify_menu_label(value) == cleaned_simple:
            return key
    return None


def info_block_choice_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [[INFO_BLOCK_LABELS[key]] for key in ["vk", "baraholki", "projects", "giveaways"]]
    keyboard.append(["⬅️ Назад"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def info_block_action_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📝 Изменить текст", "🖼 Изменить фото"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


def get_info_block_url(block_key: str) -> str:
    setting_key = INFO_BLOCK_URL_SETTINGS.get(block_key)
    if not setting_key:
        return ""
    return get_setting(setting_key, "")


def get_info_block_reply_markup(block_key: str):
    url = get_info_block_url(block_key).strip()
    if not is_valid_inline_button_url(url):
        return None

    button_label = INFO_BLOCK_BUTTON_LABELS.get(block_key, "🔗 Открыть")
    return InlineKeyboardMarkup([[InlineKeyboardButton(button_label, url=url[:2000])]])


async def show_info_block_message(target_message, block_key: str):
    title = INFO_BLOCK_LABELS.get(block_key, "ℹ️ Информация")
    text_value = get_info_block_text(block_key)
    photo = get_info_block_photo(block_key)
    caption = text_value or title
    reply_markup = get_info_block_reply_markup(block_key)

    if photo:
        try:
            await target_message.reply_photo(photo=photo, caption=caption, reply_markup=reply_markup)
            return
        except Exception:
            logger.exception("Ошибка при открытии инфо-блока %s", block_key)

    await target_message.reply_text(caption, reply_markup=reply_markup)


for key, value in {
    "baraholki_url": DEFAULT_BARAHOLKI_URL,
    "projects_url": DEFAULT_PROJECTS_URL,
    "giveaways_url": DEFAULT_GIVEAWAYS_URL,
    "vk_url": DEFAULT_VK_URL,
    "manager_url": DEFAULT_MANAGER_URL,
}.items():
    if not get_setting(key):
        set_setting(key, value)

cursor.execute("SELECT COUNT(*) FROM items")
if cursor.fetchone()[0] == 0:
    for item in DEFAULT_ITEMS:
        cursor.execute(
            """
            INSERT INTO items (item_key, category_key, label, description, image, sort_order, price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["item_key"],
                item["category_key"],
                item["label"],
                item["description"],
                item["image"],
                item["sort_order"],
                item["price"],
            ),
        )
    conn.commit()

cursor.execute("SELECT COUNT(*) FROM pickup_points")
if cursor.fetchone()[0] == 0:
    for index, name in enumerate(DEFAULT_PICKUP_POINTS, start=1):
        cursor.execute(
            "INSERT INTO pickup_points (name, sort_order) VALUES (?, ?)",
            (name, index),
        )
    conn.commit()

for block_key, default_text in INFO_BLOCK_DEFAULTS.items():
    if not get_setting(f"info_text:{block_key}"):
        set_info_block_text(block_key, default_text)

if not get_setting("welcome_caption"):
    set_setting("welcome_caption", DEFAULT_WELCOME_CAPTION)
if not get_setting("welcome_photo"):
    _welcome_photo_default = os.getenv("WELCOME_PHOTO_URL", "").strip()
    set_setting("welcome_photo", _welcome_photo_default)
if not get_setting("welcome_buttons_json"):
    _reviews_url = os.getenv("WELCOME_REVIEWS_URL", "").strip() or get_setting("vk_url", DEFAULT_VK_URL)
    _default_btns = [{"text": "💜Наши отзывы", "url": _reviews_url}]
    set_setting("welcome_buttons_json", json.dumps(_default_btns, ensure_ascii=False))


def get_welcome_caption() -> str:
    return get_setting("welcome_caption", DEFAULT_WELCOME_CAPTION)


def set_welcome_caption_value(text: str) -> None:
    set_setting("welcome_caption", text)


def get_welcome_photo() -> str:
    return get_setting("welcome_photo", "")


def set_welcome_photo_value(file_id: str) -> None:
    set_setting("welcome_photo", file_id)


def clear_welcome_photo_value() -> None:
    set_setting("welcome_photo", "")


REFERRAL_HUB_PHOTO_KEY = "referral_hub_photo"


def get_referral_hub_photo() -> str:
    return (get_setting(REFERRAL_HUB_PHOTO_KEY, "") or "").strip()


def set_referral_hub_photo(file_id: str) -> None:
    set_setting(REFERRAL_HUB_PHOTO_KEY, (file_id or "").strip())


def clear_referral_hub_photo() -> None:
    set_setting(REFERRAL_HUB_PHOTO_KEY, "")


def get_welcome_buttons_raw() -> list:
    raw = get_setting("welcome_buttons_json", "[]")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return data


def set_welcome_buttons_raw(rows: list) -> None:
    set_setting("welcome_buttons_json", json.dumps(rows, ensure_ascii=False))


def build_welcome_inline_keyboard() -> InlineKeyboardMarkup | None:
    rows = get_welcome_buttons_raw()
    keyboard = []
    for item in rows:
        if isinstance(item, dict):
            text = (item.get("text") or "").strip()
            url = (item.get("url") or "").strip()
        else:
            continue
        if not text or not is_valid_inline_button_url(url):
            continue
        keyboard.append([InlineKeyboardButton(text[:64], url=url[:2000])])
    return InlineKeyboardMarkup(keyboard) if keyboard else None


async def send_welcome_screen(target_message, user_id: int) -> None:
    caption = get_welcome_caption() or "Добро пожаловать!"
    photo = get_welcome_photo()
    markup = build_welcome_inline_keyboard()
    kb = main_keyboard(user_id)
    try:
        if photo:
            welcome_msg = await target_message.reply_photo(
                photo=photo, caption=caption, reply_markup=markup
            )
        else:
            welcome_msg = await target_message.reply_text(caption, reply_markup=markup)
    except Exception:
        logger.exception("Ошибка отправки приветствия")
        welcome_msg = await target_message.reply_text(caption, reply_markup=markup)
    # В одном сообщении нельзя совместить inline-кнопки и reply-клавиатуру — второе сообщение без видимого текста.
    try:
        await welcome_msg.reply_text("\u2060", reply_markup=kb)
    except Exception:
        logger.exception("Ошибка установки главной клавиатуры (невидимый текст)")
        try:
            await welcome_msg.reply_text("⌨️ Главное меню", reply_markup=kb)
        except Exception:
            logger.exception("Ошибка установки главной клавиатуры (запасной текст)")
            await target_message.reply_text("⌨️ Главное меню", reply_markup=kb)


def save_user(user) -> bool:
    cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user.id,))
    is_new_user = cursor.fetchone() is None
    cursor.execute(
        """
        INSERT INTO users (user_id, username, first_name, last_seen, last_spin, referred_by)
        VALUES (?, ?, ?, ?, NULL, NULL)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_seen = excluded.last_seen
        """,
        (user.id, user.username, user.first_name, now_iso()),
    )
    conn.commit()
    return is_new_user


def parse_referrer_id(raw_ref: str):
    if not raw_ref:
        return None
    match = re.fullmatch(r"(?:ref_)?(\d+)", raw_ref.strip().lower())
    if not match:
        return None
    return int(match.group(1))


def register_referral_if_valid(invited_user_id: int, raw_ref: str) -> bool:
    referrer_id = parse_referrer_id(raw_ref)
    if not referrer_id or referrer_id == invited_user_id:
        return False

    cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (referrer_id,))
    if not cursor.fetchone():
        return False

    cursor.execute("SELECT referred_by FROM users WHERE user_id = ?", (invited_user_id,))
    row = cursor.fetchone()
    if not row or row[0]:
        return False

    cursor.execute("SELECT 1 FROM referrals WHERE invited_id = ?", (invited_user_id,))
    if cursor.fetchone():
        return False

    cursor.execute(
        "INSERT INTO referrals (inviter_id, invited_id, created_at) VALUES (?, ?, ?)",
        (referrer_id, invited_user_id, now_iso()),
    )
    cursor.execute("UPDATE users SET referred_by = ? WHERE user_id = ?", (referrer_id, invited_user_id))
    conn.commit()
    track_giveaway_referral_if_active(referrer_id, invited_user_id)
    return True


def get_referral_hub_counts(user_id: int) -> tuple[int, int]:
    """Один запрос: переходы по ссылке и число приглашённых с первым заказом (согласованные цифры)."""
    cursor.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(CASE WHEN qualified_at IS NOT NULL THEN 1 ELSE 0 END), 0)
        FROM referrals
        WHERE inviter_id = ?
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def get_referrals_count(user_id: int) -> int:
    all_ref, _ = get_referral_hub_counts(user_id)
    return all_ref


def get_referral_top(limit: int = 20):
    cursor.execute(
        """
        SELECT
            r.inviter_id,
            COALESCE(u.username, ''),
            COALESCE(u.first_name, ''),
            COUNT(r.invited_id) AS invites_count
        FROM referrals r
        LEFT JOIN users u ON u.user_id = r.inviter_id
        WHERE r.qualified_at IS NOT NULL
        GROUP BY r.inviter_id, u.username, u.first_name
        ORDER BY invites_count DESC, r.inviter_id ASC
        LIMIT ?
        """,
        (limit,),
    )
    return cursor.fetchall()


def get_qualified_referrals_count(user_id: int) -> int:
    """Сколько приглашённых совершили первый заказ (один раз на аккаунт)."""
    _, q = get_referral_hub_counts(user_id)
    return q


def referral_tier_from_qualified_count(n: int) -> tuple[str, str, int]:
    """Ключ ранга, эмодзи, персональная скидка % (друзей с 1-м заказом). BRONZE 0–19: 0%, SILVER 20–40: 10%."""
    n = max(0, int(n))
    if n < 20:
        return ("BRONZE", "🥉", 0)
    if n < 41:
        return ("SILVER", "🥈", 10)
    if n < 61:
        return ("GOLD", "🥇", 12)
    if n < 101:
        return ("BIGSTAR", "🌟", 15)
    return ("GLOBAL", "🌍", 20)


def referral_next_tier_hint(qualified: int) -> str:
    """Краткая строка «до следующего ранга»."""
    q = int(qualified)
    if q < 20:
        return f"До 🥈 SILVER: ещё {20 - q} с заказом"
    if q < 41:
        return f"До 🥇 GOLD: ещё {41 - q} с заказом"
    if q < 61:
        return f"До 🌟 BIGSTAR: ещё {61 - q} с заказом"
    if q < 101:
        return f"До 🌍 GLOBAL: ещё {101 - q} с заказом"
    return "Максимальный ранг 🌍"


def referral_next_milestone(qualified: int) -> tuple[int | None, str]:
    """Порог следующего ранга (для прогресс-бара) и короткая метка."""
    q = max(0, int(qualified))
    if q < 20:
        return 20, "SILVER"
    if q < 41:
        return 41, "GOLD"
    if q < 61:
        return 61, "BIGSTAR"
    if q < 101:
        return 101, "GLOBAL"
    return None, "MAX"


def referral_progress_bar_html(qualified: int, width: int = 14) -> str:
    """Визуальная полоса прогресса до следующего ранга (HTML)."""
    q = max(0, int(qualified))
    target, label = referral_next_milestone(q)
    if target is None:
        return (
            "<b>📈 Прогресс</b>\n"
            "<code>████████████████</code>\n"
            "🏆 <i>Максимальный ранг — дальше только больше друзей в статистике.</i>"
        )
    filled = int(round(min(1.0, q / target) * width)) if target else 0
    filled = max(0, min(width, filled))
    bar = "█" * filled + "░" * (width - filled)
    return (
        f"<b>📈 Прогресс до {html_esc(label)}</b>\n"
        f"<code>{bar}</code>  <b>{q}</b> / <b>{target}</b>\n"
        f"<i>{html_esc(referral_next_tier_hint(q))}</i>"
    )


def _format_short_date(iso_ts: str | None) -> str:
    if not (iso_ts or "").strip():
        return "—"
    raw = iso_ts.strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return raw[:16] if len(raw) > 16 else raw


def _order_type_ru(order_type: str | None) -> str:
    if (order_type or "").strip() == "delivery":
        return "Доставка"
    return "Самовывоз"


def get_inviter_referrals_detail(inviter_id: int) -> list[tuple]:
    """Строки: invited_id, ref_at, qualified_at, username, first_name, order_id, order_type, items_text, total_sum, order_at."""
    cursor.execute(
        """
        SELECT
            r.invited_id,
            r.created_at,
            r.qualified_at,
            COALESCE(u.username, ''),
            COALESCE(u.first_name, ''),
            fo.order_id,
            fo.order_type,
            fo.items_text,
            fo.total_sum,
            fo.created_at
        FROM referrals r
        LEFT JOIN users u ON u.user_id = r.invited_id
        LEFT JOIN orders fo ON fo.user_id = r.invited_id
            AND fo.order_id = (
                SELECT MIN(oi.order_id) FROM orders oi WHERE oi.user_id = r.invited_id
            )
        WHERE r.inviter_id = ?
        ORDER BY
            CASE WHEN r.qualified_at IS NOT NULL THEN 0 ELSE 1 END,
            r.created_at DESC
        """,
        (inviter_id,),
    )
    return cursor.fetchall()


REFERRAL_CABINET_CHUNK = 3600


def build_referral_cabinet_html_chunks(inviter_id: int) -> list[str]:
    """Сообщения HTML для личного кабинета (разбивка по лимиту Telegram)."""
    rows = get_inviter_referrals_detail(inviter_id)
    if not rows:
        return [
            "📊 <b>Личный кабинет</b>\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Пока <b>никто не переходил</b> по твоей ссылке.\n\n"
            "Поделись ссылкой из раздела «Получить халяву» — здесь появятся друзья и их первые заказы."
        ]

    blocks: list[str] = []
    for i, row in enumerate(rows, start=1):
        (
            invited_id,
            ref_at,
            qualified_at,
            username,
            first_name,
            order_id,
            order_type,
            items_text,
            total_sum,
            order_at,
        ) = row
        name = (first_name or "").strip() or "Без имени"
        uname = (username or "").strip()
        who = f"@{html_esc(uname)}" if uname else html_esc(name)
        block_lines = [
            f"<b>{i}.</b> {who}",
            f"🆔 <code>{invited_id}</code>",
            f"📎 Переход по ссылке: {_format_short_date(ref_at)}",
        ]
        if qualified_at:
            block_lines.append("✅ <b>В ранге</b> (первый заказ засчитан)")
            if order_id is not None:
                block_lines.append(f"🧾 Заказ №<code>{order_id}</code> · {_format_short_date(order_at)}")
                block_lines.append(f"📦 Тип: {_order_type_ru(order_type)}")
                ts = int(total_sum or 0)
                block_lines.append(f"💰 К оплате: <b>{ts} ₽</b>")
                raw_items = (items_text or "").strip()
                if raw_items:
                    snippet = raw_items.replace("\n", " · ")
                    if len(snippet) > 320:
                        snippet = snippet[:320] + "…"
                    block_lines.append(f"🛒 <b>Состав:</b>\n{html_esc(snippet)}")
            else:
                block_lines.append("<i>Детали заказа не найдены в базе.</i>")
        else:
            block_lines.append("⏳ <b>Пока без первого заказа</b> — в ранг не засчитано")

        blocks.append("\n".join(block_lines))

    header_main = (
        "📊 <b>Личный кабинет</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        f"<i>По ссылке человек: {len(rows)}. В ранг — только с первым заказом.</i>\n\n"
    )
    header_next = "📊 <b>Личный кабинет</b> <i>(продолжение)</i>\n━━━━━━━━━━━━━━━━\n\n"

    messages: list[str] = []
    cur = header_main
    sep = "\n\n──────────────\n\n"
    for i, b in enumerate(blocks):
        prefix = "" if i == 0 else sep
        nxt = cur + prefix + b
        if len(nxt) > REFERRAL_CABINET_CHUNK and cur != header_main:
            messages.append(cur)
            cur = header_next + b
        else:
            cur = nxt
    messages.append(cur)

    total = len(messages)
    if total > 1:
        messages = [f"{m}\n\n<i>сообщение {i} из {total}</i>" for i, m in enumerate(messages, start=1)]
    return messages


def get_inviter_personal_discount_percent(user_id: int) -> int:
    q = get_qualified_referrals_count(user_id)
    _, _, pct = referral_tier_from_qualified_count(q)
    return pct


def maybe_qualify_referral_on_first_order(invited_user_id: int) -> None:
    """Первый заказ приглашённого → засчитываем другу пригласившего (один раз)."""
    cursor.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (invited_user_id,))
    if int(cursor.fetchone()[0] or 0) != 1:
        return
    cursor.execute(
        "UPDATE referrals SET qualified_at = ? WHERE invited_id = ? AND qualified_at IS NULL",
        (now_iso(), invited_user_id),
    )
    if cursor.rowcount:
        conn.commit()
        log_action(invited_user_id, "referral_qualified", "first_order")


def get_active_giveaway():
    cursor.execute(
        """
        SELECT
            giveaway_id,
            title,
            text_value,
            photo,
            created_at,
            COALESCE(buttons_json, '[]') AS buttons_json,
            COALESCE(autobroadcast_enabled, 0) AS autobroadcast_enabled,
            COALESCE(autobroadcast_per_day, ?) AS autobroadcast_per_day,
            autobroadcast_last_at
        FROM giveaways
        WHERE is_active = 1
        ORDER BY giveaway_id DESC
        LIMIT 1
        """,
        (GIVEAWAY_AUTOBROADCAST_DEFAULT_PER_DAY,),
    )
    return cursor.fetchone()


def get_giveaway_by_id(giveaway_id: int):
    cursor.execute(
        """
        SELECT giveaway_id, title, text_value, photo, finished_at, COALESCE(buttons_json, '[]'), results_broadcast_at
        FROM giveaways
        WHERE giveaway_id = ?
        """,
        (giveaway_id,),
    )
    return cursor.fetchone()


def get_broadcast_recipient_user_ids() -> list[int]:
    cursor.execute(
        """
        SELECT u.user_id FROM users u
        WHERE NOT EXISTS (SELECT 1 FROM blacklist b WHERE b.user_id = u.user_id)
        """
    )
    return [row[0] for row in cursor.fetchall()]


def giveaway_buttons_markup_from_json(buttons_json_raw: str | None) -> InlineKeyboardMarkup | None:
    raw = (buttons_json_raw or "").strip() or "[]"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, list):
        return None
    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        text = (item.get("text") or "").strip()
        url = (item.get("url") or "").strip()
        if not text or not is_valid_inline_button_url(url):
            continue
        rows.append([InlineKeyboardButton(text[:64], url=url[:2000])])
    return InlineKeyboardMarkup(rows) if rows else None


def parse_giveaway_buttons_lines(body: str) -> tuple[list[dict] | None, list[str]]:
    raw_lines = body.splitlines()
    parsed: list[dict] = []
    errors: list[str] = []
    for i, line in enumerate(raw_lines, 1):
        line = line.strip()
        if not line:
            continue
        if "|" not in line:
            errors.append(f"Строка {i}: нет разделителя |")
            continue
        left, right = line.split("|", 1)
        btn_text = left.strip()
        url = right.strip()
        if not btn_text or not url:
            errors.append(f"Строка {i}: пустой текст или ссылка")
            continue
        if not is_valid_inline_button_url(url):
            errors.append(f"Строка {i}: URL должен начинаться с http://, https:// или tg://")
            continue
        parsed.append({"text": btn_text, "url": url})
    return parsed, errors


def get_giveaway_winners_rows(giveaway_id: int):
    cursor.execute(
        """
        SELECT gw.winner_id, gw.invites_count, COALESCE(u.username, ''), COALESCE(u.first_name, '')
        FROM giveaway_winners gw
        LEFT JOIN users u ON u.user_id = gw.winner_id
        WHERE gw.giveaway_id = ?
        ORDER BY gw.invites_count DESC, gw.winner_id ASC
        """,
        (giveaway_id,),
    )
    return cursor.fetchall()


def build_giveaway_results_caption(giveaway_id: int) -> str | None:
    row = get_giveaway_by_id(giveaway_id)
    if not row:
        return None
    _, title, _, _, _, _, _ = row
    winners = get_giveaway_winners_rows(giveaway_id)
    lines = [
        f"🏁 Итоги розыгрыша: {title}",
        "",
        "Победители:",
    ]
    if not winners:
        lines.append("— список пуст")
    else:
        for winner_id, _invites_count, username, first_name in winners:
            uname = f"@{username}" if username else "—"
            who = first_name or "—"
            lines.append(f"• {who} ({uname}) — ID {winner_id}")
    lines.extend(["", "Спасибо всем за участие! 💜"])
    return "\n".join(lines)


def get_giveaway_top(giveaway_id: int, limit: int = 20):
    cursor.execute(
        """
        SELECT
            gr.inviter_id,
            COALESCE(u.username, ''),
            COALESCE(u.first_name, ''),
            COUNT(gr.invited_id) AS invites_count
        FROM giveaway_referrals gr
        LEFT JOIN users u ON u.user_id = gr.inviter_id
        WHERE gr.giveaway_id = ?
        GROUP BY gr.inviter_id, u.username, u.first_name
        ORDER BY invites_count DESC, gr.inviter_id ASC
        LIMIT ?
        """,
        (giveaway_id, limit),
    )
    return cursor.fetchall()


def get_giveaway_referrals_count(giveaway_id: int, inviter_id: int) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM giveaway_referrals WHERE giveaway_id = ? AND inviter_id = ?",
        (giveaway_id, inviter_id),
    )
    return cursor.fetchone()[0]


def track_giveaway_referral_if_active(inviter_id: int, invited_id: int) -> None:
    giveaway = get_active_giveaway()
    if not giveaway:
        return

    giveaway_id = giveaway[0]
    cursor.execute(
        """
        INSERT INTO giveaway_referrals (giveaway_id, inviter_id, invited_id, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(giveaway_id, invited_id) DO NOTHING
        """,
        (giveaway_id, inviter_id, invited_id, now_iso()),
    )
    conn.commit()


def autopost_button_markup(button_text: str, button_url: str):
    text = (button_text or "").strip()
    url = (button_url or "").strip()
    if not text or not is_valid_inline_button_url(url):
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton(text[:64], url=url[:2000])]])


def log_action(user_id: int | None, action_type: str, payload: str = "") -> None:
    cursor.execute(
        "INSERT INTO action_logs (user_id, action_type, payload, created_at) VALUES (?, ?, ?, ?)",
        (user_id, action_type, payload[:1000], now_iso()),
    )
    conn.commit()


def is_user_blacklisted(user_id: int) -> bool:
    cursor.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


def get_category_discount_percent(category_key: str) -> int:
    cursor.execute("SELECT discount_percent FROM category_discounts WHERE category_key = ?", (category_key,))
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def calc_discounted_price(base_price: int, category_key: str) -> int:
    if base_price <= 0:
        return base_price
    discount = get_category_discount_percent(category_key)
    if discount <= 0:
        return base_price
    return max(int(round(base_price * (100 - discount) / 100.0)), 0)


def rating_summary_for_user(user_id: int) -> tuple[float, int]:
    cursor.execute("SELECT COALESCE(AVG(rating), 0), COUNT(*) FROM customer_ratings WHERE user_id = ?", (user_id,))
    avg_value, count_value = cursor.fetchone()
    return float(avg_value or 0), int(count_value or 0)


def build_ref_link(bot_username: str, user_id: int) -> str:
    if not bot_username:
        return ""
    return f"https://t.me/{bot_username}?start={user_id}"


def build_giveaway_announce_caption(
    bot_username: str, user_id: int, giveaway_id: int, title: str, text_value: str
) -> str:
    """Текст анонса в HTML (parse_mode=HTML). Иначе Markdown ломается на _, *, [ в title/text_value."""
    ref_link = build_ref_link(bot_username, user_id)
    my_count = get_giveaway_referrals_count(giveaway_id, user_id)
    t = html.escape(str(title) if title is not None else "")
    body = html.escape(str(text_value) if text_value is not None else "")
    link = html.escape(ref_link) if ref_link else "—"
    return (
        f"🎁 <b>{t}</b>\n\n"
        f"{body}\n\n"
        f"Твои приглашения в этом розыгрыше: <b>{my_count}</b>\n"
        f"Твоя ссылка для участия:\n<code>{link}</code>"
    )


GIVEAWAY_PHOTO_CAPTION_MAX = 1024
GIVEAWAY_TEXT_MESSAGE_MAX = 4096


def build_giveaway_announce_caption_plain(
    bot_username: str, user_id: int, giveaway_id: int, title: str, text_value: str
) -> str:
    """Тот же смысл без HTML — если API отклоняет разметку или нужен запасной вариант."""
    ref_link = build_ref_link(bot_username, user_id)
    my_count = get_giveaway_referrals_count(giveaway_id, user_id)
    t = str(title) if title is not None else ""
    body = str(text_value) if text_value is not None else ""
    link = ref_link or "—"
    return (
        f"🎁 {t}\n\n"
        f"{body}\n\n"
        f"Твои приглашения в этом розыгрыше: {my_count}\n"
        f"Твоя ссылка для участия:\n{link}"
    )


def _fit_giveaway_caption(
    bot_username: str,
    user_id: int,
    giveaway_id: int,
    title: str,
    text_value: str,
    max_len: int,
    *,
    use_html: bool,
) -> str:
    """Укорачивает описание, затем заголовок, пока длина текста не станет ≤ max_len."""
    t_raw = str(title) if title is not None else ""
    tv_raw = str(text_value) if text_value is not None else ""

    def build(t_part: str, tv_part: str) -> str:
        if use_html:
            return build_giveaway_announce_caption(bot_username, user_id, giveaway_id, t_part, tv_part)
        return build_giveaway_announce_caption_plain(bot_username, user_id, giveaway_id, t_part, tv_part)

    full = build(t_raw, tv_raw)
    if len(full) <= max_len:
        return full

    lo, hi = 0, len(tv_raw)
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        suffix = "…" if mid < len(tv_raw) else ""
        cand = build(t_raw, tv_raw[:mid] + suffix)
        if len(cand) <= max_len:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1
    if best is not None:
        return best

    lo, hi = 0, len(t_raw)
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        suffix = "…" if mid < len(t_raw) else ""
        cand = build(t_raw[:mid] + suffix, "")
        if len(cand) <= max_len:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1
    if best is not None:
        return best
    return build("🎁", "…")


def ru_times_per_day_word(n: int) -> str:
    """Склонение: 1 раз, 2 раза, 5 раз, 11 раз, 22 раза."""
    n = int(n)
    n10, n100 = n % 10, n % 100
    if n10 == 1 and n100 != 11:
        return "раз"
    if 2 <= n10 <= 4 and n100 not in (12, 13, 14):
        return "раза"
    return "раз"


def giveaway_autobroadcast_interval_seconds(per_day: int) -> float:
    n = max(GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY, min(int(per_day or GIVEAWAY_AUTOBROADCAST_DEFAULT_PER_DAY), GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY))
    return 86400.0 / n


def format_giveaway_autobroadcast_interval_ru(per_day: int) -> str:
    sec = giveaway_autobroadcast_interval_seconds(per_day)
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    if h and m:
        return f"~{h} ч {m} мин"
    if h:
        return f"~{h} ч"
    return f"~{m} мин"


def my_referrals_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["📊 Личный кабинет"], ["⬅️ Назад"]],
        resize_keyboard=True,
    )


def update_last_spin(user_id: int) -> None:
    cursor.execute(
        "UPDATE users SET last_spin = ?, last_seen = ? WHERE user_id = ?",
        (now_iso(), now_iso(), user_id),
    )
    conn.commit()


def can_spin(user_id: int) -> bool:
    cursor.execute("SELECT last_spin FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return True
    last_spin = datetime.fromisoformat(row[0])
    return datetime.now() - last_spin >= timedelta(hours=48)


def generate_code() -> str:
    return "RNDM-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def get_discount() -> int:
    # 10% шанс = 5%; остальные 95% распределяем поровну между 1/2/3/5/7.
    return random.choices([1, 2, 3, 5, 7, 10], weights=[19, 19, 19, 19, 19, 5], k=1)[0]


def is_code_active(created_at: str) -> bool:
    created = datetime.fromisoformat(created_at)
    return datetime.now() - created <= timedelta(hours=12)


def slugify_name(name: str) -> str:
    allowed = string.ascii_lowercase + string.digits + "_"
    base = name.lower().strip().replace(" ", "_")
    cleaned = "".join(ch for ch in base if ch in allowed)
    return cleaned or "item"


def simplify_menu_label(value: str) -> str:
    cleaned = re.sub(r"^[^\wА-Яа-яЁё0-9]+", "", value.strip(), flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower()


def generate_unique_item_key(label: str) -> str:
    base = slugify_name(label)
    candidate = base
    index = 1
    while True:
        cursor.execute("SELECT 1 FROM items WHERE item_key = ?", (candidate,))
        if not cursor.fetchone():
            return candidate
        candidate = f"{base}_{index}"
        index += 1


def parse_category_from_label(label: str):
    cleaned = label.strip()
    cleaned_simple = simplify_menu_label(cleaned)
    for key, value in CATEGORY_LABELS.items():
        if value == cleaned or simplify_menu_label(value) == cleaned_simple:
            return key
    return None


def format_price(price: int) -> str:
    if price <= 0:
        return "Цена уточняется"
    return f"{price} ₽"


def parse_price(text: str):
    cleaned = text.strip().replace("₽", "").replace("р", "").replace(" ", "")
    if not cleaned.isdigit():
        return None
    return int(cleaned)


def is_valid_phone(phone: str) -> bool:
    return re.fullmatch(r"\+7\d{10}", phone.strip()) is not None


def is_valid_username(username: str) -> bool:
    return re.fullmatch(r"@[A-Za-z0-9_]{5,32}", username.strip()) is not None


def get_items_by_category(category_key: str):
    cursor.execute(
        """
        SELECT item_id, item_key, category_key, label, description, image, sort_order, price
        FROM items
        WHERE category_key = ?
        ORDER BY sort_order ASC, label ASC
        """,
        (category_key,),
    )
    return cursor.fetchall()


def get_item(item_id: int):
    cursor.execute(
        """
        SELECT item_id, item_key, category_key, label, description, image, sort_order, price
        FROM items
        WHERE item_id = ?
        """,
        (item_id,),
    )
    return cursor.fetchone()


def get_item_by_label(category_key: str, label: str):
    cursor.execute(
        """
        SELECT item_id, item_key, category_key, label, description, image, sort_order, price
        FROM items
        WHERE category_key = ? AND label = ?
        """,
        (category_key, label),
    )
    return cursor.fetchone()


def get_next_item_order(category_key: str) -> int:
    cursor.execute(
        "SELECT COALESCE(MAX(sort_order), 0) + 1 FROM items WHERE category_key = ?",
        (category_key,),
    )
    return cursor.fetchone()[0]


def add_item(category_key: str, label: str, description: str, image: str, price: int) -> int:
    item_key = generate_unique_item_key(label)
    sort_order = get_next_item_order(category_key)
    if USE_POSTGRES:
        cursor.execute(
            """
            INSERT INTO items (item_key, category_key, label, description, image, sort_order, price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING item_id
            """,
            (item_key, category_key, label, description, image, sort_order, price),
        )
        new_item_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            """
            INSERT INTO items (item_key, category_key, label, description, image, sort_order, price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (item_key, category_key, label, description, image, sort_order, price),
        )
        new_item_id = cursor.lastrowid
    conn.commit()
    return new_item_id


def update_item_label(item_id: int, new_label: str) -> None:
    cursor.execute("UPDATE items SET label = ? WHERE item_id = ?", (new_label, item_id))
    conn.commit()


def update_item_description(item_id: int, new_description: str) -> None:
    cursor.execute("UPDATE items SET description = ? WHERE item_id = ?", (new_description, item_id))
    conn.commit()


def update_item_image(item_id: int, new_image: str) -> None:
    cursor.execute("UPDATE items SET image = ? WHERE item_id = ?", (new_image, item_id))
    conn.commit()


def update_item_price(item_id: int, new_price: int) -> None:
    cursor.execute("UPDATE items SET price = ? WHERE item_id = ?", (new_price, item_id))
    conn.commit()


def delete_item(item_id: int) -> None:
    cursor.execute("DELETE FROM cart_items WHERE item_id = ?", (item_id,))
    cursor.execute("DELETE FROM items WHERE item_id = ?", (item_id,))
    conn.commit()


def move_item(item_id: int, direction: str) -> bool:
    current = get_item(item_id)
    if not current:
        return False

    _, _, category_key, _, _, _, current_order, _ = current

    if direction == "up":
        cursor.execute(
            """
            SELECT item_id, sort_order
            FROM items
            WHERE category_key = ? AND sort_order < ?
            ORDER BY sort_order DESC
            LIMIT 1
            """,
            (category_key, current_order),
        )
    else:
        cursor.execute(
            """
            SELECT item_id, sort_order
            FROM items
            WHERE category_key = ? AND sort_order > ?
            ORDER BY sort_order ASC
            LIMIT 1
            """,
            (category_key, current_order),
        )

    neighbor = cursor.fetchone()
    if not neighbor:
        return False

    neighbor_id, neighbor_order = neighbor

    cursor.execute("UPDATE items SET sort_order = ? WHERE item_id = ?", (neighbor_order, item_id))
    cursor.execute("UPDATE items SET sort_order = ? WHERE item_id = ?", (current_order, neighbor_id))
    conn.commit()
    return True


def get_pickup_points():
    cursor.execute(
        """
        SELECT pickup_id, name, sort_order
        FROM pickup_points
        ORDER BY sort_order ASC, name ASC
        """
    )
    return cursor.fetchall()


def get_pickup_point_by_name(name: str):
    cursor.execute(
        "SELECT pickup_id, name, sort_order FROM pickup_points WHERE name = ?",
        (name,),
    )
    return cursor.fetchone()


def get_pickup_point(pickup_id: int):
    cursor.execute(
        "SELECT pickup_id, name, sort_order FROM pickup_points WHERE pickup_id = ?",
        (pickup_id,),
    )
    return cursor.fetchone()


def get_next_pickup_order() -> int:
    cursor.execute("SELECT COALESCE(MAX(sort_order), 0) + 1 FROM pickup_points")
    return cursor.fetchone()[0]


def add_pickup_point(name: str) -> None:
    cursor.execute(
        "INSERT INTO pickup_points (name, sort_order) VALUES (?, ?)",
        (name, get_next_pickup_order()),
    )
    conn.commit()


def rename_pickup_point(pickup_id: int, new_name: str) -> None:
    cursor.execute("UPDATE pickup_points SET name = ? WHERE pickup_id = ?", (new_name, pickup_id))
    conn.commit()


def delete_pickup_point(pickup_id: int) -> None:
    cursor.execute("DELETE FROM pickup_points WHERE pickup_id = ?", (pickup_id,))
    conn.commit()


def move_pickup_point(pickup_id: int, direction: str) -> bool:
    current = get_pickup_point(pickup_id)
    if not current:
        return False

    _, _, current_order = current

    if direction == "up":
        cursor.execute(
            """
            SELECT pickup_id, sort_order
            FROM pickup_points
            WHERE sort_order < ?
            ORDER BY sort_order DESC
            LIMIT 1
            """,
            (current_order,),
        )
    else:
        cursor.execute(
            """
            SELECT pickup_id, sort_order
            FROM pickup_points
            WHERE sort_order > ?
            ORDER BY sort_order ASC
            LIMIT 1
            """,
            (current_order,),
        )

    neighbor = cursor.fetchone()
    if not neighbor:
        return False

    neighbor_id, neighbor_order = neighbor

    cursor.execute("UPDATE pickup_points SET sort_order = ? WHERE pickup_id = ?", (neighbor_order, pickup_id))
    cursor.execute("UPDATE pickup_points SET sort_order = ? WHERE pickup_id = ?", (current_order, neighbor_id))
    conn.commit()
    return True


def add_to_cart(user_id: int, item_id: int, quantity: int = 1) -> None:
    # Без составного PK в PostgreSQL ON CONFLICT(...) падает; UPSERT через SELECT надёжнее для SQLite и PG.
    cursor.execute(
        "SELECT 1 FROM cart_items WHERE user_id = ? AND item_id = ?",
        (user_id, item_id),
    )
    if cursor.fetchone():
        cursor.execute(
            """
            UPDATE cart_items
            SET quantity = quantity + ?
            WHERE user_id = ? AND item_id = ?
            """,
            (quantity, user_id, item_id),
        )
    else:
        cursor.execute(
            """
            INSERT INTO cart_items (user_id, item_id, quantity)
            VALUES (?, ?, ?)
            """,
            (user_id, item_id, quantity),
        )
    conn.commit()
    log_action(user_id, "cart_add", f"item={item_id};qty={quantity}")


def remove_from_cart(user_id: int, item_id: int) -> None:
    cursor.execute("DELETE FROM cart_items WHERE user_id = ? AND item_id = ?", (user_id, item_id))
    conn.commit()
    log_action(user_id, "cart_remove", f"item={item_id}")


def clear_cart(user_id: int) -> None:
    cursor.execute("DELETE FROM cart_items WHERE user_id = ?", (user_id,))
    conn.commit()
    log_action(user_id, "cart_clear", "")


def get_cart(user_id: int):
    cursor.execute(
        """
        SELECT ci.item_id, ci.quantity, i.label, i.price, i.category_key
        FROM cart_items ci
        JOIN items i ON i.item_id = ci.item_id
        WHERE ci.user_id = ?
        ORDER BY i.label ASC
        """,
        (user_id,),
    )
    rows = cursor.fetchall()
    result = []
    for item_id, quantity, label, base_price, category_key in rows:
        price = calc_discounted_price(base_price, category_key)
        result.append((item_id, quantity, label, price, category_key))
    return result


def cart_total(user_id: int) -> int:
    rows = get_cart(user_id)
    return sum(price * quantity for _, quantity, _, price, _ in rows)


def cart_text(user_id: int) -> str:
    rows = get_cart(user_id)
    if not rows:
        return "🛒 *Корзина пока пустая.*"

    q_ok = get_qualified_referrals_count(user_id)
    tier_key, tier_em, ref_pct = referral_tier_from_qualified_count(q_ok)
    lines = [
        "🛒 *ТВОЯ КОРЗИНА*\n",
        f"{tier_em} *Ранг:* {tier_key} · скидка *{ref_pct}%* · друзей с заказом: *{q_ok}*\n",
    ]
    for _, quantity, label, price, _ in rows:
        if price > 0:
            lines.append(f"• {label} — {quantity} шт × {price} ₽ = {price * quantity} ₽")
        else:
            lines.append(f"• {label} — {quantity} шт × цена уточняется")

    total = cart_total(user_id)
    lines.append("")
    lines.append(f"*Итого:* {format_price(total)}" if total > 0 else "*Итого:* цена уточняется")
    return "\n".join(lines)


def cart_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✅ Оформить заказ", callback_data="cart_checkout")]]

    for item_id, _, label, _, _ in get_cart(user_id):
        rows.append([InlineKeyboardButton(f"❌ Удалить: {label}", callback_data=f"cart_remove:{item_id}")])

    rows.append([InlineKeyboardButton("🗑 Очистить корзину", callback_data="cart_clear")])
    rows.append([InlineKeyboardButton("⬅️ Назад в ассортимент", callback_data="assortment_menu")])
    return InlineKeyboardMarkup(rows)


def manager_keyboard() -> InlineKeyboardMarkup:
    configured = get_setting("manager_url", DEFAULT_MANAGER_URL).strip()
    url = configured if is_valid_inline_button_url(configured) else DEFAULT_MANAGER_URL
    return InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать менеджеру", url=url)]])


def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        ["🛍 Ассортимент", "🎰 Крутить скидку"],
        ["📦 История заказов", "💬 Менеджер"],
        ["🛒 Наши барахолки", "🚀 Наши проекты"],
        ["🎁 Розыгрыши", "📱 Наш VK"],
        ["🛒 Корзина", "🎁 Получить халяву"],
    ]
    if is_admin(user_id):
        keyboard.append(["⚙️ Админка"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🛍 Редактор каталога", "📣 Рассылки"],
            ["🎁 Розыгрыши (админ)", "👥 Клиенты"],
            ["🎫 Создать промокод"],
            ["🔗 Ссылки и инфо", "📊 Аналитика"],
            ["👋 Экран приветствия"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


def admin_welcome_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📝 Текст приветствия", "🖼 Фото приветствия"],
            ["🔗 Кнопки под постом", "🗑 Убрать фото"],
            ["👁 Предпросмотр", "↩️ Админка"],
            ["🛑 Прервать сценарий"],
        ],
        resize_keyboard=True,
    )



def admin_catalog_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["➕ Добавить кнопку", "✏️ Переименовать кнопку"],
            ["📝 Изменить описание", "🖼 Изменить фото"],
            ["💰 Изменить цену", "🗑 Удалить кнопку"],
            ["📍 Точки самовывоза", "↕️ Порядок кнопок"],
            ["🖼 Фото категорий", "🗑 Удалить фото категории"],
            ["🏷 Акции категорий"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def admin_broadcast_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📢 Рассылка", "🤖 Авто-рассылки"],
            ["📋 Активные авто-рассылки"],
            ["🛑 Прервать сценарий"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def admin_giveaways_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🎯 Создать розыгрыш", "🏁 Завершить розыгрыш"],
            ["📣 Авторассылка анонса"],
            ["🎁 Реф. розыгрыш"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def admin_giveaway_autobroadcast_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["▶️ Включить авторассылку", "⏹ Выключить авторассылку"],
            ["⚙ Раз в сутки"],
            ["📤 Разослать анонс сейчас (1 раз)"],
            ["📊 Статус авторассылки"],
            ["↩️ К розыгрышам"],
        ],
        resize_keyboard=True,
    )


def admin_clients_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🚫 Черный список"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def admin_links_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🗂 Инфо-блоки", "💬 Ссылка на менеджера"],
            ["🛒 Ссылка на барахолки", "🚀 Ссылка на проекты"],
            ["🎁 Ссылка на розыгрыши"],
            ["🖼 Фото: Получить халяву"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def admin_referral_hub_photo_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["🗑 Убрать фото"], ["↩️ Админка"]],
        resize_keyboard=True,
    )


def admin_analytics_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📊 Статистика", "📈 Аналитика PRO"],
            ["↩️ Админка"],
        ],
        resize_keyboard=True,
    )


def pickup_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["➕ Добавить точку", "✏️ Переименовать точку"],
            ["🗑 Удалить точку", "↕️ Порядок точек"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


def admin_category_choice_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [[CATEGORY_LABELS[key]] for key in CATEGORY_ORDER]
    keyboard.append(["⬅️ Назад"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def category_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(CATEGORY_LABELS[key], callback_data=f"category:{key}")] for key in CATEGORY_ORDER]
    rows.append([InlineKeyboardButton("🛒 Открыть корзину", callback_data="cart_open")])
    return InlineKeyboardMarkup(rows)


def item_menu_keyboard(category_key: str) -> InlineKeyboardMarkup:
    rows = []
    category_discount = get_category_discount_percent(category_key)
    for item_id, _, _, label, _, _, _, base_price in get_items_by_category(category_key):
        price = calc_discounted_price(base_price, category_key)
        suffix = f" — {price} ₽" if price > 0 else ""
        if category_discount > 0 and base_price > 0:
            suffix += f" (-{category_discount}%)"
        rows.append([InlineKeyboardButton(f"{label}{suffix}", callback_data=f"item:{item_id}")])

    rows.append([InlineKeyboardButton("🛒 Корзина", callback_data="cart_open")])
    rows.append([InlineKeyboardButton("⬅️ Назад к категориям", callback_data="assortment_menu")])
    return InlineKeyboardMarkup(rows)


def item_card_keyboard(item_id: int, category_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🛒 В корзину", callback_data=f"add_to_cart:{item_id}")],
            [InlineKeyboardButton("⚡ Купить сейчас", callback_data=f"buy_now:{item_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"open_category:{category_key}")],
        ]
    )


def order_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚚 Доставка", callback_data="checkout_delivery")],
            [InlineKeyboardButton("📍 Самовывоз", callback_data="checkout_pickup")],
        ]
    )


def pickup_points_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(name, callback_data=f"pickup_select:{int(pickup_id)}")]
        for pickup_id, name, _ in get_pickup_points()
    ]
    return InlineKeyboardMarkup(rows)


ORDER_STATUS_META = {
    "new": ("🆕", "НОВЫЙ"),
    "accepted": ("✅", "ПРИНЯТ"),
    "in_progress": ("🚚", "В РАБОТЕ"),
    "done": ("🎉", "ВЫДАН"),
    "canceled": ("❌", "ОТМЕНЁН"),
}


def order_status_keyboard(
    order_id: int, client_user_id: int, address_plain: str | None = None
) -> InlineKeyboardMarkup:
    rows = []
    addr = (address_plain or "").strip()
    if addr and CopyTextButton is not None:
        rows.append(
            [InlineKeyboardButton("📋 Скопировать адрес", copy_text=CopyTextButton(text=addr[:256]))]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton("✅ Принят", callback_data=f"order_status:{order_id}:accepted"),
                InlineKeyboardButton("🚚 В работе", callback_data=f"order_status:{order_id}:in_progress"),
            ],
            [
                InlineKeyboardButton("🎉 Выдан", callback_data=f"order_status:{order_id}:done"),
                InlineKeyboardButton("❌ Отменён", callback_data=f"order_status:{order_id}:canceled"),
            ],
            [
                InlineKeyboardButton("⭐1", callback_data=f"order_rate:{order_id}:1"),
                InlineKeyboardButton("⭐3", callback_data=f"order_rate:{order_id}:3"),
                InlineKeyboardButton("⭐5", callback_data=f"order_rate:{order_id}:5"),
            ],
            [InlineKeyboardButton("👤 Профиль клиента", url=f"tg://user?id={client_user_id}")],
        ]
    )
    return InlineKeyboardMarkup(rows)


def html_esc(s) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def format_claiming_manager_html(user) -> str:
    if not user or not getattr(user, "id", None):
        return ""
    parts: list[str] = []
    name = " ".join(
        p
        for p in [getattr(user, "first_name", None) or "", getattr(user, "last_name", None) or ""]
        if p
    ).strip()
    if name:
        parts.append(html_esc(name))
    un = getattr(user, "username", None)
    if un:
        parts.append(html_esc("@" + un))
    if not parts:
        parts.append(html_esc(str(user.id)))
    return " · ".join(parts)


def render_order_group_header_html(order_id: int, status: str, claimed_by_user) -> str:
    icon, title = ORDER_STATUS_META.get(status, ORDER_STATUS_META["new"])
    status_label = html_esc(title)
    line = f"{icon} <b>{status_label}</b> ЗАКАЗ #{order_id}"
    if claimed_by_user is not None and getattr(claimed_by_user, "id", None):
        mgr = format_claiming_manager_html(claimed_by_user)
        if mgr:
            line += f" · <b>🔥 ВЗЯЛ(А):</b> <code>{mgr}</code>"
    return line


def fetch_order_row_for_manager_card(order_id: int) -> tuple | None:
    cursor.execute(
        """
        SELECT order_id, user_id, username, first_name, order_type, pickup_point, phone,
               contact_username, address, delivery_time, items_text, total_sum, created_at,
               COALESCE(status, 'new'), status_updated_by,
               order_subtotal, promo_code, discount_percent, discount_amount
        FROM orders WHERE order_id = ?
        """,
        (order_id,),
    )
    return cursor.fetchone()


def build_manager_order_message_html(order_row: tuple, claimed_by_user) -> tuple[str, InlineKeyboardMarkup]:
    (
        order_id,
        user_id,
        username,
        first_name,
        order_type,
        pickup_point,
        phone,
        contact_username,
        address,
        delivery_time,
        items_text,
        total_sum,
        created_at,
        status,
        _status_updated_by,
        order_subtotal,
        promo_code,
        discount_percent,
        discount_amount,
    ) = order_row

    subtotal = order_subtotal if order_subtotal is not None else total_sum
    header = render_order_group_header_html(order_id, status, claimed_by_user)

    username_line = f"@{username}" if username else "нет username"
    rating_avg, rating_count = rating_summary_for_user(user_id)
    rating_text = f"{rating_avg:.2f}/5 ({rating_count} оценок)" if rating_count > 0 else "пока нет"

    q_ref = get_qualified_referrals_count(user_id)
    rk, r_em, r_pct = referral_tier_from_qualified_count(q_ref)
    ref_rank_line = (
        f"⭐️ <b>Реф. ранг клиента:</b> {r_em} <b>{html_esc(rk)}</b> · "
        f"персональная скидка <b>{r_pct}%</b> · с заказом друзей: <b>{q_ref}</b>"
    )

    total_line = format_price(subtotal) if subtotal > 0 else "цена уточняется"
    final_line = format_price(total_sum) if total_sum > 0 else "цена уточняется"

    blocks: list[str] = [
        header,
        "",
        f"Тип: {'Доставка' if order_type == 'delivery' else 'Самовывоз'}",
        f"Клиент: {html_esc(first_name or '-')}",
        f"Username: {html_esc(username_line)}",
        f"User ID: {user_id}",
        f"Телефон: {html_esc(phone)}",
        f"Контактный username: {html_esc(contact_username)}",
        f"Рейтинг клиента: {html_esc(rating_text)}",
        ref_rank_line,
    ]

    if order_type == "delivery":
        addr_plain = (address or "").strip()
        blocks.append(f"📍 <b>Адрес:</b> <code>{html_esc(addr_plain)}</code>")
    else:
        blocks.append(f"Точка самовывоза: {html_esc(pickup_point)}")

    if promo_code:
        blocks.append(f"Промокод: {html_esc(promo_code)}")
        blocks.append(f"Скидка: -{int(discount_percent or 0)}%")
        blocks.append(f"Размер скидки: {int(discount_amount or 0)} ₽")
    elif int(discount_percent or 0) > 0:
        blocks.append(f"Скидка по рангу: -{int(discount_percent)}%")
        blocks.append(f"Размер скидки: {int(discount_amount or 0)} ₽")

    try:
        order_time_str = datetime.fromisoformat(created_at).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        order_time_str = str(created_at)

    blocks.extend(
        [
            f"Время: {html_esc(delivery_time)}",
            "",
            "Товары:",
            html_esc(items_text),
            "",
            f"Сумма до скидки: {html_esc(total_line)}",
            f"Итого к оплате: {html_esc(final_line)}",
            f"Время заказа: {html_esc(order_time_str)}",
        ]
    )

    text = "\n".join(blocks)
    addr_copy = (address or "").strip() if order_type == "delivery" else None
    markup = order_status_keyboard(order_id, user_id, address_plain=addr_copy)
    return text, markup


async def open_category_view(target_message, category_key: str):
    category_title = CATEGORY_LABELS.get(category_key, "КАТЕГОРИЯ")
    caption = f"📂 *{category_title}*\n\nВыбирай позицию ниже 👇"
    reply_markup = item_menu_keyboard(category_key)
    category_image = get_category_image(category_key)

    if category_image:
        try:
            await target_message.reply_photo(
                photo=category_image,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
            return
        except Exception:
            logger.exception("Ошибка при открытии фото категории %s", category_key)

    await target_message.reply_text(
        caption,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )


async def safe_send(update: Update, text: str, **kwargs):
    if update.message:
        await update.message.reply_text(text, **kwargs)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_user_blacklisted(user.id):
        await safe_send(update, "⛔ Доступ к боту ограничен. Обратись к менеджеру.")
        return
    is_new_user = save_user(user)
    if is_new_user and context.args:
        register_referral_if_valid(user.id, context.args[0])
    log_action(user.id, "start", "entry")
    if update.message:
        await send_welcome_screen(update.message, user.id)


async def assortment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🛍 *АССОРТИМЕНТ RNDM SHOP*\n\nВыбирай категорию ниже 👇",
        parse_mode="Markdown",
        reply_markup=category_menu_keyboard(),
    )


async def show_item(query, item_id: int):
    if not query.message:
        logger.warning("show_item: callback без message (item_id=%s)", item_id)
        return
    item = get_item(item_id)
    if not item:
        await query.message.reply_text("❌ Позиция не найдена.")
        return

    item_id, _, category_key, label, description, image, _, base_price = item
    price = calc_discounted_price(base_price, category_key)
    discount = get_category_discount_percent(category_key)
    discount_line = ""
    if discount > 0 and base_price > 0:
        discount_line = f"\n🏷 Акция категории: -{discount}% (было {base_price} ₽)"
    caption = f"*{label}*\n\n{description}\n\n💰 *Цена:* {format_price(price)}{discount_line}"

    if not image:
        await query.message.reply_text(
            caption,
            parse_mode="Markdown",
            reply_markup=item_card_keyboard(item_id, category_key),
        )
        return

    try:
        await query.message.reply_photo(
            photo=image,
            caption=caption,
            parse_mode="Markdown",
            reply_markup=item_card_keyboard(item_id, category_key),
        )
    except Exception:
        logger.exception("Ошибка при открытии товара item_id=%s, image=%s", item_id, image)
        await query.message.reply_text(
            f"{caption}\n\n⚠️ Фото товара не загрузилось.",
            parse_mode="Markdown",
            reply_markup=item_card_keyboard(item_id, category_key),
        )


async def open_cart_message(target_message, user_id: int):
    await target_message.reply_text(
        cart_text(user_id),
        parse_mode="Markdown",
        reply_markup=cart_keyboard(user_id),
    )


async def _callback_text_or_dm(context: ContextTypes.DEFAULT_TYPE, query, text: str, **kwargs):
    """Если сообщение с кнопкой удалено — пишем в личку, иначе ответом в чат."""
    try:
        if query.message:
            await query.message.reply_text(text, **kwargs)
        elif query.from_user:
            await context.bot.send_message(chat_id=query.from_user.id, text=text, **kwargs)
    except Exception:
        logger.exception("Не удалось отправить ответ по callback (reply/DM)")


async def _checkout_reply_after_query(context: ContextTypes.DEFAULT_TYPE, query, text: str, **kwargs):
    """Ответ в чат после callback; если сообщение с кнопкой недоступно — в личку."""
    try:
        if query.message:
            await query.message.reply_text(text, **kwargs)
        elif query.from_user:
            await context.bot.send_message(chat_id=query.from_user.id, text=text, **kwargs)
    except Exception:
        logger.exception("Не удалось отправить сообщение шага checkout")


async def assortment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    raw = query.data
    if not isinstance(raw, str):
        try:
            await query.answer("Открой товар заново из меню 🛍 Ассортимент.", show_alert=True)
        except Exception:
            logger.exception("answer callback (невалидные callback_data)")
        return

    # Для add_to_cart отвечаем на query один раз — с текстом в toast (answer нельзя вызывать дважды).
    skip_early_answer = raw.startswith("add_to_cart:")
    if not skip_early_answer:
        try:
            await query.answer()
        except Exception:
            logger.exception("assortment_callback: ранний query.answer")

    user = update.effective_user
    if not user:
        if skip_early_answer:
            try:
                await query.answer("Не удалось определить профиль. Открой бота заново.", show_alert=True)
            except Exception:
                logger.exception("assortment_callback: answer без user")
        return

    if query.data == "assortment_menu":
        await query.message.reply_text(
            "🛍 *АССОРТИМЕНТ RNDM SHOP*\n\nВыбирай категорию ниже 👇",
            parse_mode="Markdown",
            reply_markup=category_menu_keyboard(),
        )
        return

    if query.data == "cart_open":
        await open_cart_message(query.message, user.id)
        return

    if query.data == "cart_clear":
        clear_cart(user.id)
        await query.message.reply_text("🗑 Корзина очищена.", reply_markup=category_menu_keyboard())
        return

    if query.data.startswith("cart_remove:"):
        item_id = int(query.data.split(":", 1)[1])
        remove_from_cart(user.id, item_id)
        await open_cart_message(query.message, user.id)
        return

    if query.data.startswith("category:"):
        category_key = query.data.split(":", 1)[1]
        await open_category_view(query.message, category_key)
        return

    if query.data.startswith("item:"):
        item_id = int(query.data.split(":", 1)[1])
        await show_item(query, item_id)
        return

    if query.data.startswith("open_category:"):
        category_key = query.data.split(":", 1)[1]
        await open_category_view(query.message, category_key)
        return

    if query.data.startswith("add_to_cart:"):
        suffix = query.data.split(":", 1)[-1].strip()
        try:
            item_id = int(suffix)
        except ValueError:
            try:
                await query.answer("Кнопка устарела. Зайди в 🛍 Ассортимент и открой товар снова.", show_alert=True)
            except Exception:
                logger.exception("add_to_cart: answer после ValueError")
            return
        if not get_item(item_id):
            try:
                await query.answer("Позиции нет в каталоге. Открой ассортимент заново.", show_alert=True)
            except Exception:
                logger.exception("add_to_cart: answer item missing")
            await _callback_text_or_dm(
                context,
                query,
                "❌ Этой позиции нет в каталоге (после обновления базы открой ассортимент заново из меню 🛍).",
            )
            return
        try:
            add_to_cart(user.id, item_id)
        except Exception:
            logger.exception("Ошибка add_to_cart user=%s item=%s", user.id, item_id)
            try:
                await query.answer("Не удалось сохранить корзину.", show_alert=True)
            except Exception:
                logger.exception("add_to_cart: answer после ошибки БД")
            await _callback_text_or_dm(
                context,
                query,
                "❌ Не удалось сохранить корзину. Попробуй ещё раз или напиши менеджеру.",
            )
            return
        try:
            await query.answer("✅ В корзине", show_alert=False)
        except Exception:
            logger.exception("add_to_cart: answer после успеха")
        await _callback_text_or_dm(context, query, "✅ Товар добавлен в корзину.")
        return


def clear_order_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in [
        "checkout_mode",
        "checkout_buy_now_item_id",
        "checkout_pickup_point",
        "checkout_phone",
        "checkout_username",
        "checkout_address",
        "checkout_time",
        "checkout_promocode",
        "checkout_promo_percent",
        "checkout_referral_percent",
        "checkout_discount_percent",
        "checkout_discount_amount",
        "checkout_total_before_discount",
        "checkout_total_after_discount",
    ]:
        context.user_data.pop(key, None)


def collect_checkout_items(user_id: int, buy_now_item_id):
    if buy_now_item_id:
        item = get_item(buy_now_item_id)
        if not item:
            return []
        final_price = calc_discounted_price(item[7], item[2])
        return [{
            "item_id": item[0],
            "label": item[3],
            "price": final_price,
            "quantity": 1,
        }]

    result = []
    for item_id, quantity, label, price, _ in get_cart(user_id):
        result.append({
            "item_id": item_id,
            "label": label,
            "price": price,
            "quantity": quantity,
        })
    return result


def recalculate_checkout_totals(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Скидка заказа = max(промокод %, персональный % по реф. рангу пригласившего)."""
    items = collect_checkout_items(user_id, context.user_data.get("checkout_buy_now_item_id"))
    total_sum = build_total_sum(items)
    promo_pct = int(context.user_data.get("checkout_promo_percent") or 0)
    ref_pct = get_inviter_personal_discount_percent(user_id)
    eff = promo_pct if promo_pct > ref_pct else ref_pct
    context.user_data["checkout_referral_percent"] = ref_pct
    context.user_data["checkout_discount_percent"] = eff
    final_sum, discount_amount = apply_discount_to_total(total_sum, eff)
    context.user_data["checkout_total_before_discount"] = total_sum
    context.user_data["checkout_total_after_discount"] = final_sum
    context.user_data["checkout_discount_amount"] = discount_amount


def build_items_text(items: list[dict]) -> str:
    lines = []
    for item in items:
        if item["price"] > 0:
            lines.append(f'{item["label"]} — {item["quantity"]} шт × {item["price"]} ₽ = {item["quantity"] * item["price"]} ₽')
        else:
            lines.append(f'{item["label"]} — {item["quantity"]} шт × цена уточняется')
    return "\n".join(lines)


def build_total_sum(items: list[dict]) -> int:
    return sum(item["price"] * item["quantity"] for item in items)


def get_promocode(code: str):
    cursor.execute(
        """
        SELECT code, discount, used, created_at, used_at, owner_user_id,
               COALESCE(admin_global, 0)
        FROM promocodes
        WHERE code = ?
        """,
        (code.strip().upper(),),
    )
    return cursor.fetchone()


def validate_promocode_for_user(code: str, user_id: int):
    promo = get_promocode(code)
    if not promo:
        return False, "❌ Промокод не найден.", None

    promo_code, discount, used, created_at, used_at, owner_user_id, admin_global = promo

    if owner_user_id and owner_user_id != user_id:
        return False, "⛔ Этот промокод принадлежит другому пользователю.", None

    if used:
        return False, "⚠️ Этот промокод уже использован.", None

    if not int(admin_global or 0) and not is_code_active(created_at):
        return False, "⌛ Срок действия промокода истёк.", None

    return True, "✅ Промокод применён.", discount


def mark_promocode_used(code: str) -> None:
    cursor.execute(
        "UPDATE promocodes SET used = 1, used_at = ? WHERE code = ?",
        (now_iso(), code.strip().upper()),
    )
    conn.commit()


def apply_discount_to_total(total_sum: int, discount_percent: int) -> tuple[int, int]:
    if total_sum <= 0 or discount_percent <= 0:
        return total_sum, 0

    discount_amount = int(round(total_sum * discount_percent / 100.0))
    final_sum = max(total_sum - discount_amount, 0)
    return final_sum, discount_amount


def promocode_skip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏭ Пропустить", callback_data="skip_promocode")]]
    )


async def send_order_to_managers(context: ContextTypes.DEFAULT_TYPE, user, items: list[dict]) -> int:
    order_type = context.user_data.get("checkout_mode")
    pickup_point = context.user_data.get("checkout_pickup_point")
    phone = context.user_data.get("checkout_phone")
    contact_username = context.user_data.get("checkout_username")
    address = context.user_data.get("checkout_address")
    delivery_time = context.user_data.get("checkout_time")
    promocode_raw = context.user_data.get("checkout_promocode")
    promo_pct = int(context.user_data.get("checkout_promo_percent") or 0)
    ref_pct = get_inviter_personal_discount_percent(user.id)
    if promo_pct > ref_pct:
        discount_percent = promo_pct
        promo_db = (promocode_raw or "").strip().upper() if promocode_raw else None
    else:
        discount_percent = ref_pct
        promo_db = None

    items_text = build_items_text(items)
    total_sum = build_total_sum(items)
    final_sum, discount_amount = apply_discount_to_total(total_sum, discount_percent)

    if USE_POSTGRES:
        cursor.execute(
            """
            INSERT INTO orders (
                user_id, username, first_name, order_type, pickup_point, phone,
                contact_username, address, delivery_time, items_text, total_sum, created_at,
                order_subtotal, promo_code, discount_percent, discount_amount
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING order_id
            """,
            (
                user.id,
                user.username,
                user.first_name,
                order_type,
                pickup_point,
                phone,
                contact_username,
                address,
                delivery_time,
                items_text,
                final_sum,
                now_iso(),
                total_sum,
                promo_db,
                int(discount_percent or 0),
                int(discount_amount or 0),
            ),
        )
        order_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            """
            INSERT INTO orders (
                user_id, username, first_name, order_type, pickup_point, phone,
                contact_username, address, delivery_time, items_text, total_sum, created_at,
                order_subtotal, promo_code, discount_percent, discount_amount
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user.id,
                user.username,
                user.first_name,
                order_type,
                pickup_point,
                phone,
                contact_username,
                address,
                delivery_time,
                items_text,
                final_sum,
                now_iso(),
                total_sum,
                promo_db,
                int(discount_percent or 0),
                int(discount_amount or 0),
            ),
        )
        order_id = cursor.lastrowid
    conn.commit()
    maybe_qualify_referral_on_first_order(user.id)
    log_action(user.id, "order_created", f"order_id={order_id};total={final_sum};type={order_type}")

    row = fetch_order_row_for_manager_card(order_id)
    if not row:
        logger.error("send_order_to_managers: заказ %s не найден после INSERT", order_id)
        return order_id

    manager_text, order_markup = build_manager_order_message_html(row, None)
    await context.bot.send_message(
        chat_id=ORDER_GROUP_ID,
        text=manager_text,
        parse_mode="HTML",
        reply_markup=order_markup,
    )
    return order_id


async def order_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    async def answer_once(text: str | None = None, *, show_alert: bool = False):
        try:
            await query.answer(text=text, show_alert=show_alert)
        except Exception:
            logger.exception("order_status: answerCallbackQuery")

    parts = query.data.split(":") if isinstance(query.data, str) else []
    if len(parts) != 3:
        await answer_once("Некорректные данные кнопки", show_alert=True)
        return

    _, order_id_raw, new_status = parts
    if new_status not in ORDER_STATUS_META:
        await answer_once("Неизвестный статус", show_alert=True)
        return

    if not order_id_raw.isdigit():
        await answer_once("Некорректный номер заказа", show_alert=True)
        return

    order_id = int(order_id_raw)
    manager_id = query.from_user.id if query.from_user else 0

    cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
    row = cursor.fetchone()
    if not row:
        await answer_once("Заказ не найден", show_alert=True)
        return
    client_user_id = row[0]

    try:
        cursor.execute(
            "UPDATE orders SET status = ?, status_updated_at = ?, status_updated_by = ? WHERE order_id = ?",
            (new_status, now_iso(), manager_id, order_id),
        )
        conn.commit()
        log_action(manager_id, "order_status", f"order={order_id};status={new_status}")
    except Exception:
        logger.exception("order_status: ошибка UPDATE orders order_id=%s", order_id)
        await answer_once("Не удалось сохранить статус в БД. Напиши разработчику.", show_alert=True)
        return

    message = query.message
    toast = "Статус обновлён"
    if message:
        row = fetch_order_row_for_manager_card(order_id)
        if row:
            updated_text, order_markup = build_manager_order_message_html(row, query.from_user)
            try:
                await message.edit_text(
                    updated_text,
                    reply_markup=order_markup,
                    parse_mode="HTML",
                )
            except Exception:
                logger.exception("Не удалось обновить сообщение заказа order_id=%s", order_id)
                toast = "Статус сохранён. Не удалось обновить текст в чате."
        else:
            toast = "Статус сохранён. Не удалось перечитать заказ из БД."

    await answer_once(toast[:200] if toast else None)


async def order_rate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    async def answer_once(text: str | None = None, *, show_alert: bool = False):
        try:
            await query.answer(text=text, show_alert=show_alert)
        except Exception:
            logger.exception("order_rate: answerCallbackQuery")

    parts = query.data.split(":") if isinstance(query.data, str) else []
    if len(parts) != 3:
        await answer_once("Некорректные данные кнопки", show_alert=True)
        return
    _, order_id_raw, rating_raw = parts
    if not order_id_raw.isdigit() or not rating_raw.isdigit():
        await answer_once("Некорректные данные оценки", show_alert=True)
        return

    order_id = int(order_id_raw)
    rating = int(rating_raw)
    if rating < 1 or rating > 5:
        await answer_once("Некорректная оценка", show_alert=True)
        return

    manager_id = query.from_user.id if query.from_user else 0
    cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
    row = cursor.fetchone()
    if not row:
        await answer_once("Заказ не найден", show_alert=True)
        return
    client_user_id = row[0]
    cursor.execute(
        """
        INSERT INTO customer_ratings (order_id, user_id, manager_id, rating, comment, created_at)
        VALUES (?, ?, ?, ?, '', ?)
        """,
        (order_id, client_user_id, manager_id, rating, now_iso()),
    )
    conn.commit()
    log_action(manager_id, "order_rate", f"order={order_id};client={client_user_id};rating={rating}")
    avg_value, cnt_value = rating_summary_for_user(client_user_id)
    msg = f"Оценка сохранена: {rating}/5. Ср: {avg_value:.2f} ({cnt_value})"
    await answer_once(msg[:200])


async def rate_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not context.args:
        await safe_send(update, "Использование: /ratecomment ORDER_ID текст комментария")
        return
    if len(context.args) < 2 or not context.args[0].isdigit():
        await safe_send(update, "Пример: /ratecomment 123 Клиент просит перезвонить вечером")
        return

    order_id = int(context.args[0])
    comment = " ".join(context.args[1:]).strip()
    cursor.execute("SELECT user_id FROM orders WHERE order_id = ?", (order_id,))
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Заказ не найден.")
        return
    client_user_id = row[0]
    cursor.execute(
        """
        INSERT INTO customer_ratings (order_id, user_id, manager_id, rating, comment, created_at)
        VALUES (?, ?, ?, 0, ?, ?)
        """,
        (order_id, client_user_id, update.effective_user.id, comment, now_iso()),
    )
    conn.commit()
    await safe_send(update, "✅ Комментарий к клиенту сохранён.")


async def begin_checkout(query, context: ContextTypes.DEFAULT_TYPE, buy_now_item_id=None):
    items = collect_checkout_items(query.from_user.id, buy_now_item_id)
    if not items:
        await _checkout_reply_after_query(context, query, "🛒 Корзина пустая.")
        return ConversationHandler.END

    clear_order_context(context)
    context.user_data["checkout_buy_now_item_id"] = buy_now_item_id

    items_text = build_items_text(items)
    total_sum = build_total_sum(items)
    total_line = format_price(total_sum) if total_sum > 0 else "цена уточняется"

    context.user_data["checkout_promocode"] = None
    context.user_data["checkout_promo_percent"] = 0
    recalculate_checkout_totals(context, query.from_user.id)
    ref_pct = int(context.user_data.get("checkout_referral_percent") or 0)
    q_ok = get_qualified_referrals_count(query.from_user.id)
    tier_key, tier_em, _ = referral_tier_from_qualified_count(q_ok)
    after_ref = int(context.user_data.get("checkout_total_after_discount") or total_sum)
    pay_hint = format_price(after_ref) if after_ref > 0 else "цена уточняется"
    ref_line = (
        f"{tier_em} *Твой ранг:* {tier_key} · скидка по рангу *{ref_pct}%*\n"
        f"_{referral_next_tier_hint(q_ok)}_\n"
        f"При оформлении действует *лучшая* из скидок: промокод или ранг.\n"
        f"Сейчас к оплате с учётом ранга: *{pay_hint}* (если без промокода).\n\n"
    )

    await _checkout_reply_after_query(
        context,
        query,
        f"🧾 ОФОРМЛЕНИЕ ЗАКАЗА\n\n"
        f"{ref_line}"
        f"Товары:\n{items_text}\n\n"
        f"Итого без скидки: {total_line}\n\n"
        f"Если у тебя есть промокод — отправь его сейчас сообщением.\n"
        f"Если промокода нет, нажми кнопку ниже:",
        reply_markup=promocode_skip_keyboard(),
    )
    return ORDER_PROMOCODE_WAITING


async def checkout_promocode_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    code = update.message.text.strip().upper()

    ok, message, discount = validate_promocode_for_user(code, user.id)
    if not ok:
        await safe_send(
            update,
            f"{message}\n\nОтправь другой промокод или нажми /cancel для отмены заказа."
        )
        return ORDER_PROMOCODE_WAITING

    context.user_data["checkout_promocode"] = code
    context.user_data["checkout_promo_percent"] = int(discount)
    recalculate_checkout_totals(context, user.id)
    eff = int(context.user_data.get("checkout_discount_percent") or 0)
    ref_pct = int(context.user_data.get("checkout_referral_percent") or 0)
    final_sum = int(context.user_data.get("checkout_total_after_discount") or 0)
    discount_amount = int(context.user_data.get("checkout_discount_amount") or 0)
    if int(discount) > ref_pct:
        src = f"промокод *-{discount}%*"
    elif ref_pct > int(discount):
        src = f"ранг *-{ref_pct}%* (выгоднее промокода)"
    else:
        src = f"промокод и ранг *-{eff}%*"

    await safe_send(
        update,
        f"✅ Учтено: {src}\n"
        f"Итоговая скидка: *-{eff}%*\n"
        f"Размер скидки: *{discount_amount} ₽*\n"
        f"Итого к оплате: *{final_sum} ₽*\n\n"
        f"Теперь выбери тип заказа:",
        parse_mode="Markdown",
        reply_markup=order_type_keyboard(),
    )
    return ORDER_CHOICE_WAITING


async def checkout_skip_promocode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    context.user_data["checkout_promocode"] = None
    context.user_data["checkout_promo_percent"] = 0
    recalculate_checkout_totals(context, query.from_user.id)

    await _checkout_reply_after_query(
        context,
        query,
        "Ок, продолжаем без промокода.\n\nВыбери тип заказа:",
        reply_markup=order_type_keyboard(),
    )
    return ORDER_CHOICE_WAITING


async def start_checkout_from_cart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    return await begin_checkout(query, context, buy_now_item_id=None)


async def start_checkout_buy_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    item_id = int(query.data.split(":", 1)[1])
    return await begin_checkout(query, context, buy_now_item_id=item_id)


async def checkout_choose_delivery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["checkout_mode"] = "delivery"
    await _checkout_reply_after_query(context, query, "1) Ваш телефон в формате +7XXXXXXXXXX")
    return ORDER_DELIVERY_PHONE_WAITING


async def checkout_delivery_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not is_valid_phone(phone):
        await safe_send(update, "❌ Неверный формат. Пример: +79991234567")
        return ORDER_DELIVERY_PHONE_WAITING

    context.user_data["checkout_phone"] = phone
    await safe_send(update, "2) Ваш юзернейм в Telegram для связи.\nПример: @ivan1997")
    return ORDER_DELIVERY_USERNAME_WAITING


async def checkout_delivery_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip()
    if not is_valid_username(username):
        await safe_send(update, "❌ Неверный формат. Пример: @ivan1997")
        return ORDER_DELIVERY_USERNAME_WAITING

    context.user_data["checkout_username"] = username
    await safe_send(update, "3) Ваш адрес (Район, улица, дом)")
    return ORDER_DELIVERY_ADDRESS_WAITING


async def checkout_delivery_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    address = update.message.text.strip()
    if len(address) < 5:
        await safe_send(update, "❌ Адрес слишком короткий.")
        return ORDER_DELIVERY_ADDRESS_WAITING

    context.user_data["checkout_address"] = address
    await safe_send(update, "4) Укажи удобное время")
    return ORDER_DELIVERY_TIME_WAITING


async def checkout_delivery_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    delivery_time = update.message.text.strip()
    if len(delivery_time) < 2:
        await safe_send(update, "❌ Укажи время нормально.")
        return ORDER_DELIVERY_TIME_WAITING

    context.user_data["checkout_time"] = delivery_time

    user = update.effective_user
    items = collect_checkout_items(user.id, context.user_data.get("checkout_buy_now_item_id"))
    if not items:
        await safe_send(update, "❌ Не удалось собрать товары для заказа.")
        clear_order_context(context)
        return ConversationHandler.END

    try:
        order_id = await send_order_to_managers(context, user, items)
    except Exception as e:
        await safe_send(update, f"⚠️ Не удалось отправить заказ в группу.\nПроверь ORDER_GROUP_ID.\n\nОшибка: {e}")
        clear_order_context(context)
        return ConversationHandler.END

    promo_pct = int(context.user_data.get("checkout_promo_percent") or 0)
    ref_pct = get_inviter_personal_discount_percent(user.id)
    promocode = context.user_data.get("checkout_promocode")
    if promocode and promo_pct > ref_pct:
        mark_promocode_used(promocode)

    if not context.user_data.get("checkout_buy_now_item_id"):
        clear_cart(user.id)

    await safe_send(
        update,
        f"✅ Заказ #{order_id} отправлен менеджерам.\nС тобой скоро свяжутся.",
        reply_markup=main_keyboard(user.id),
    )
    clear_order_context(context)
    return ConversationHandler.END


async def checkout_choose_pickup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["checkout_mode"] = "pickup"

    points = get_pickup_points()
    if not points:
        await _checkout_reply_after_query(context, query, "❌ Нет доступных точек самовывоза.")
        clear_order_context(context)
        return ConversationHandler.END

    await _checkout_reply_after_query(
        context, query, "Выбери точку самовывоза:", reply_markup=pickup_points_keyboard()
    )
    return ORDER_PICKUP_POINT_WAITING


async def checkout_pickup_point(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    raw = query.data
    if not isinstance(raw, str):
        await _checkout_reply_after_query(context, query, "❌ Устаревшая кнопка. Начни оформление заново из корзины.")
        return ORDER_PICKUP_POINT_WAITING
    try:
        pickup_id = int(raw.split(":", 1)[1].strip())
    except (ValueError, IndexError):
        await _checkout_reply_after_query(context, query, "❌ Некорректные данные точки. Выбери точку снова.")
        return ORDER_PICKUP_POINT_WAITING

    pickup_point = get_pickup_point(pickup_id)
    if not pickup_point:
        await _checkout_reply_after_query(context, query, "❌ Точка не найдена.")
        return ORDER_PICKUP_POINT_WAITING

    context.user_data["checkout_pickup_point"] = pickup_point[1]
    await _checkout_reply_after_query(context, query, "1) Ваш номер телефона в формате +7XXXXXXXXXX")
    return ORDER_PICKUP_PHONE_WAITING


async def checkout_pickup_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not is_valid_phone(phone):
        await safe_send(update, "❌ Неверный формат. Пример: +79991234567")
        return ORDER_PICKUP_PHONE_WAITING

    context.user_data["checkout_phone"] = phone
    await safe_send(update, "2) Укажите удобное время")
    return ORDER_PICKUP_TIME_WAITING


async def checkout_pickup_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pickup_time = update.message.text.strip()
    if len(pickup_time) < 2:
        await safe_send(update, "❌ Укажи время нормально.")
        return ORDER_PICKUP_TIME_WAITING

    context.user_data["checkout_time"] = pickup_time
    await safe_send(update, "3) Ваш юзернейм в Telegram.\nПример: @ivan1997")
    return ORDER_PICKUP_USERNAME_WAITING


async def checkout_pickup_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip()
    if not is_valid_username(username):
        await safe_send(update, "❌ Неверный формат. Пример: @ivan1997")
        return ORDER_PICKUP_USERNAME_WAITING

    context.user_data["checkout_username"] = username

    user = update.effective_user
    items = collect_checkout_items(user.id, context.user_data.get("checkout_buy_now_item_id"))
    if not items:
        await safe_send(update, "❌ Не удалось собрать товары для заказа.")
        clear_order_context(context)
        return ConversationHandler.END

    try:
        order_id = await send_order_to_managers(context, user, items)
    except Exception as e:
        await safe_send(update, f"⚠️ Не удалось отправить заказ в группу.\nПроверь ORDER_GROUP_ID.\n\nОшибка: {e}")
        clear_order_context(context)
        return ConversationHandler.END

    promo_pct = int(context.user_data.get("checkout_promo_percent") or 0)
    ref_pct = get_inviter_personal_discount_percent(user.id)
    promocode = context.user_data.get("checkout_promocode")
    if promocode and promo_pct > ref_pct:
        mark_promocode_used(promocode)

    if not context.user_data.get("checkout_buy_now_item_id"):
        clear_cart(user.id)

    await safe_send(
        update,
        f"✅ Заказ #{order_id} отправлен менеджерам.\nС тобой скоро свяжутся.",
        reply_markup=main_keyboard(user.id),
    )
    clear_order_context(context)
    return ConversationHandler.END


async def show_cart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    await safe_send(
        update,
        cart_text(user.id),
        parse_mode="Markdown",
        reply_markup=cart_keyboard(user.id),
    )


async def show_order_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)

    cursor.execute(
        """
        SELECT order_id, order_type, pickup_point, address, total_sum, created_at
        FROM orders
        WHERE user_id = ?
        ORDER BY order_id DESC
        LIMIT 10
        """,
        (user.id,),
    )
    rows = cursor.fetchall()

    if not rows:
        await safe_send(update, "📦 История заказов пока пустая.", reply_markup=main_keyboard(user.id))
        return

    lines = ["📦 *ТВОИ ПОСЛЕДНИЕ ЗАКАЗЫ*\n"]
    for order_id, order_type, pickup_point, address, total_sum, created_at in rows:
        order_type_text = "Доставка" if order_type == "delivery" else "Самовывоз"
        place = address if order_type == "delivery" else pickup_point
        total_text = format_price(total_sum) if total_sum > 0 else "цена уточняется"
        dt = datetime.fromisoformat(created_at).strftime("%d.%m.%Y %H:%M")
        lines.append(f"• Заказ #{order_id}")
        lines.append(f"  Тип: {order_type_text}")
        lines.append(f"  Куда: {place}")
        lines.append(f"  Сумма: {total_text}")
        lines.append(f"  Когда: {dt}\n")

    await safe_send(update, "\n".join(lines), parse_mode="Markdown", reply_markup=main_keyboard(user.id))


async def spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)

    if not can_spin(user.id):
        await safe_send(
            update,
            "⏳ *Ты уже крутил скидку за последние 48 часов.*\n\nПопробуй позже 😈",
            parse_mode="Markdown",
        )
        return

    discount = get_discount()
    code = generate_code()
    created_at = now_iso()

    cursor.execute(
        """
        INSERT INTO promocodes (code, discount, used, created_at, used_at, owner_user_id)
        VALUES (?, ?, 0, ?, NULL, ?)
        """,
        (code, discount, created_at, user.id),
    )
    conn.commit()
    update_last_spin(user.id)

    await safe_send(update, "🎰 Крутим твою скидку...")
    await safe_send(
        update,
        f"💥 *ТЕБЕ ВЫПАЛО: -{discount}%*\n\n"
        f"Твой промокод: `{code}`\n"
        f"⏳ Действует *12 часов*\n\n"
        f"*Как активировать:*\n"
        f"1. Добавь товары в корзину.\n"
        f"2. Нажми *Оформить заказ*.\n"
        f"3. На шаге с промокодом отправь этот код сообщением: `{code}`\n"
        f"4. Бот сам применит скидку к заказу.",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def open_info_block(update: Update, block_key: str):
    save_user(update.effective_user)
    if update.message:
        await show_info_block_message(update.message, block_key)


async def vk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_info_block(update, "vk")


async def baraholki(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_info_block(update, "baraholki")


async def projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_info_block(update, "projects")


async def giveaways(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    active = get_active_giveaway()
    if not active:
        await safe_send(
            update,
            "🎁 Пока нет активного розыгрыша.\n\nСледи за обновлениями — как только что-то запустим, анонс будет здесь, в боте.",
            reply_markup=main_keyboard(update.effective_user.id),
        )
        return

    giveaway_id, title, text_value, photo, _, buttons_json, _, _, _ = active
    uname = context.bot.username or ""
    uid = update.effective_user.id
    markup = giveaway_buttons_markup_from_json(buttons_json)
    if update.message:
        if photo:
            cap = _fit_giveaway_caption(
                uname, uid, giveaway_id, title, text_value, GIVEAWAY_PHOTO_CAPTION_MAX, use_html=True
            )
            try:
                await update.message.reply_photo(
                    photo=photo, caption=cap, parse_mode="HTML", reply_markup=markup
                )
                return
            except Exception:
                logger.exception("Ошибка отправки фото активного розыгрыша")
                try:
                    plain = _fit_giveaway_caption(
                        uname, uid, giveaway_id, title, text_value, GIVEAWAY_PHOTO_CAPTION_MAX, use_html=False
                    )
                    await update.message.reply_photo(photo=photo, caption=plain, reply_markup=markup)
                    return
                except Exception:
                    logger.exception("Повтор без HTML тоже не удался")
        text = _fit_giveaway_caption(
            uname, uid, giveaway_id, title, text_value, GIVEAWAY_TEXT_MESSAGE_MAX, use_html=True
        )
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=markup)


async def manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "💬 *Связь с менеджером*\n\nПиши по кнопке ниже.",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def my_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    all_ref, q_ok = get_referral_hub_counts(user.id)
    tier_key, tier_em, disc = referral_tier_from_qualified_count(q_ok)
    ref_link = build_ref_link(context.bot.username, user.id)
    hint = html_esc(referral_next_tier_hint(q_ok))
    progress_block = referral_progress_bar_html(q_ok)

    text = (
        "🎁 <b>Получить халяву</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"{tier_em} <b>Твой ранг:</b> {html_esc(tier_key)}\n"
        f"<b>Скидка по рангу:</b> {disc}% "
        "<i>(с промокодом выбирается лучшая)</i>\n\n"
        f"{progress_block}\n\n"
        f"<b>Статистика</b>\n"
        f"· по ссылке зашли: <b>{all_ref}</b>\n"
        f"· с первым заказом: <b>{q_ok}</b>\n"
        f"<i>{hint}</i>\n\n"
        "Когда друг оформляет <b>первый</b> заказ, он попадает в твой ранг "
        "(один раз с одного аккаунта). Детали — в «Личный кабинет».\n\n"
    )
    if ref_link:
        text += f"<b>Твоя ссылка</b>\n<code>{html_esc(ref_link)}</code>"
    else:
        text += "Ссылка временно недоступна, попробуй позже."

    msg = update.message
    if not msg:
        return
    photo_id = get_referral_hub_photo()
    if photo_id and len(text) > 1000:
        text = (
            "🎁 <b>Получить халяву</b>\n"
            "━━━━━━━━━━━━━━━━\n\n"
            f"{tier_em} <b>Ранг:</b> {html_esc(tier_key)} · <b>скидка {disc}%</b>\n"
            f"{progress_block}\n\n"
            f"👥 <b>{all_ref}</b> по ссылке · ✅ <b>{q_ok}</b> с заказом\n"
            f"<i>{hint}</i>\n\n"
            "<i>Подробности и состав заказов друзей — кнопка «Личный кабинет».</i>\n\n"
        )
        if ref_link:
            text += f"<code>{html_esc(ref_link)}</code>"
        else:
            text += "Ссылка недоступна."
    if photo_id:
        try:
            await msg.reply_photo(
                photo=photo_id,
                caption=text,
                parse_mode="HTML",
                reply_markup=my_referrals_keyboard(),
            )
            return
        except Exception:
            logger.exception("my_referrals: не удалось отправить фото экрана халявы")
    await msg.reply_text(text, parse_mode="HTML", reply_markup=my_referrals_keyboard())


async def referral_personal_cabinet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детальный список приглашённых и первых заказов (личный кабинет)."""
    user = update.effective_user
    save_user(user)
    msg = update.message
    if not msg:
        return
    chunks = build_referral_cabinet_html_chunks(user.id)
    for chunk in chunks:
        await msg.reply_text(chunk, parse_mode="HTML", reply_markup=my_referrals_keyboard())


async def admin_referral_hub_photo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(
        update,
        "🖼 *Фото для «Получить халяву»*\n\n"
        "Пользователи увидят это изображение вместе с текстом (ранг, статистика, ссылка) — "
        "текст пойдёт *подписью* к фото.\n\n"
        "Отправь *фото* сообщением.\n"
        "Чтобы сбросить — «🗑 Убрать фото» или слово `убрать`.\n"
        "«↩️ Админка» — выход без изменений.",
        parse_mode="Markdown",
        reply_markup=admin_referral_hub_photo_keyboard(),
    )
    return ADMIN_REFERRAL_HUB_PHOTO_WAITING


async def admin_referral_hub_photo_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    if not update.message:
        return ADMIN_REFERRAL_HUB_PHOTO_WAITING

    if update.message.text:
        raw = update.message.text.strip()
        low = raw.lower()
        if raw == "🗑 Убрать фото" or low in ("убрать", "удалить", "сбросить"):
            clear_referral_hub_photo()
            await safe_send(
                update,
                "✅ Фото экрана «Получить халяву» убрано.",
                reply_markup=admin_links_keyboard(),
            )
            return ConversationHandler.END
        if raw in ADMIN_ESCAPE_LABELS:
            return await admin_escape_conversation(update, context)

    if update.message.photo:
        set_referral_hub_photo(update.message.photo[-1].file_id)
        await safe_send(
            update,
            "✅ Фото сохранено. Пользователи увидят его при нажатии «🎁 Получить халяву».",
            reply_markup=admin_links_keyboard(),
        )
        return ConversationHandler.END

    await safe_send(
        update,
        "❌ Отправь фото или нажми «🗑 Убрать фото».",
        reply_markup=admin_referral_hub_photo_keyboard(),
    )
    return ADMIN_REFERRAL_HUB_PHOTO_WAITING


async def admin_info_blocks_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    context.user_data.pop("info_block_key", None)
    await safe_send(
        update,
        "🗂 Выбери блок, который хочешь изменить.",
        reply_markup=info_block_choice_keyboard(),
    )
    return ADMIN_INFO_BLOCK_SELECT_WAITING


async def admin_info_blocks_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    block_key = parse_info_block_from_label(update.message.text.strip())
    if not block_key:
        await safe_send(update, "❌ Выбери блок кнопкой ниже.", reply_markup=info_block_choice_keyboard())
        return ADMIN_INFO_BLOCK_SELECT_WAITING

    context.user_data["info_block_key"] = block_key
    await safe_send(
        update,
        f"Выбран блок: {INFO_BLOCK_LABELS[block_key]}\nЧто изменить?",
        reply_markup=info_block_action_keyboard(),
    )
    return ADMIN_INFO_BLOCK_ACTION_WAITING


async def admin_info_blocks_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    block_key = context.user_data.get("info_block_key")
    if not block_key:
        return ConversationHandler.END

    text_value = update.message.text.strip()
    if text_value == "📝 Изменить текст":
        await safe_send(update, f"📝 Отправь новый текст для блока {INFO_BLOCK_LABELS[block_key]}.")
        return ADMIN_INFO_BLOCK_TEXT_WAITING
    if text_value == "🖼 Изменить фото":
        await safe_send(update, f"🖼 Отправь новое фото для блока {INFO_BLOCK_LABELS[block_key]}.")
        return ADMIN_INFO_BLOCK_PHOTO_WAITING

    await safe_send(update, "❌ Выбери действие кнопкой ниже.", reply_markup=info_block_action_keyboard())
    return ADMIN_INFO_BLOCK_ACTION_WAITING


async def admin_info_blocks_save_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    block_key = context.user_data.get("info_block_key")
    if not block_key:
        return ConversationHandler.END

    set_info_block_text(block_key, update.message.text)
    context.user_data.pop("info_block_key", None)
    await safe_send(update, "✅ Текст блока обновлён.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_info_blocks_save_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    block_key = context.user_data.get("info_block_key")
    if not block_key:
        return ConversationHandler.END

    if not update.message or not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_INFO_BLOCK_PHOTO_WAITING

    set_info_block_photo(block_key, update.message.photo[-1].file_id)
    context.user_data.pop("info_block_key", None)
    await safe_send(update, "✅ Фото блока обновлено.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_send(update, "⛔ У тебя нет доступа.")
        return

    await safe_send(
        update,
        "⚙️ *Админка*\n\nВыбери раздел управления.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


async def admin_submenu_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выход из вложенного админ-сценария по «↩️/⤴️ Админка» или «⚙️ Админка»."""
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(
        update,
        "⚙️ *Админка*\n\nВыбери раздел управления.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_open_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "🛍 Раздел: редактор каталога", reply_markup=admin_catalog_keyboard())


async def admin_open_broadcasts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "📣 Раздел: рассылки", reply_markup=admin_broadcast_keyboard())


async def admin_open_giveaways(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "🎁 Раздел: розыгрыши", reply_markup=admin_giveaways_keyboard())


def _giveaway_autobroadcast_status_lines(active: tuple | None) -> list[str]:
    if not active:
        return ["❌ Нет активного розыгрыша — авторассылка недоступна."]
    gid, title, _, _, _, _, en, per_day, last_at = active
    per_day = max(
        GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY,
        min(int(per_day or GIVEAWAY_AUTOBROADCAST_DEFAULT_PER_DAY), GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY),
    )
    interval_hr = format_giveaway_autobroadcast_interval_ru(per_day)
    lines = [
        f"🎁 Активный розыгрыш: *{title}* (ID `{gid}`)",
        f"📣 Авторассылка: *{'включена' if en else 'выключена'}*",
        f"📅 Частота: *{per_day}* {ru_times_per_day_word(per_day)} в сутки (интервал {interval_hr})",
        "Текст и кнопки — как у пользователя в «🎁 Розыгрыши» (у каждого своя ссылка и счётчик).",
    ]
    if last_at:
        lines.append(f"🕐 Последний автозапуск: `{last_at}`")
    else:
        lines.append("🕐 Последний автозапуск: ещё не было")
    return lines


async def admin_giveaway_autobroadcast_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    active = get_active_giveaway()
    intro = (
        "📣 *Авторассылка анонса розыгрыша*\n\n"
        "Бот периодически рассылает **тот же пост**, что пользователь видит по кнопке «🎁 Розыгрыши» "
        "(описание, фото, кнопки, персональная реферальная ссылка и число приглашений).\n\n"
        "Получатели — все клиенты из базы, кроме чёрного списка.\n\n"
        "Кнопка «📤 Разослать анонс сейчас» шлёт тот же пост *один раз сразу*, без ожидания таймера.\n"
    )
    body = "\n".join(_giveaway_autobroadcast_status_lines(active))
    await safe_send(
        update,
        intro + "\n" + body,
        parse_mode="Markdown",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )


async def admin_giveaway_autobroadcast_enable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    active = get_active_giveaway()
    if not active:
        await safe_send(update, "❌ Нет активного розыгрыша.", reply_markup=admin_giveaways_keyboard())
        return
    gid = active[0]
    cursor.execute(
        """
        UPDATE giveaways
        SET autobroadcast_enabled = 1, autobroadcast_last_at = NULL
        WHERE giveaway_id = ? AND is_active = 1
        """,
        (gid,),
    )
    conn.commit()
    log_action(update.effective_user.id, "giveaway_autobroadcast_enable", f"giveaway_id={gid}")
    await safe_send(
        update,
        "✅ Авторассылка включена. Первая рассылка — при ближайшей проверке (до ~1 мин), дальше по интервалу.",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )


async def admin_giveaway_autobroadcast_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    active = get_active_giveaway()
    if not active:
        await safe_send(update, "❌ Нет активного розыгрыша.", reply_markup=admin_giveaways_keyboard())
        return
    gid = active[0]
    cursor.execute(
        "UPDATE giveaways SET autobroadcast_enabled = 0 WHERE giveaway_id = ? AND is_active = 1",
        (gid,),
    )
    conn.commit()
    log_action(update.effective_user.id, "giveaway_autobroadcast_disable", f"giveaway_id={gid}")
    await safe_send(update, "⏹ Авторассылка выключена.", reply_markup=admin_giveaway_autobroadcast_keyboard())


async def admin_giveaway_autobroadcast_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    active = get_active_giveaway()
    await safe_send(
        update,
        "\n".join(_giveaway_autobroadcast_status_lines(active)),
        parse_mode="Markdown",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )


async def admin_giveaway_autobroadcast_per_day_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    if not get_active_giveaway():
        await safe_send(update, "❌ Нет активного розыгрыша.", reply_markup=admin_giveaways_keyboard())
        return ConversationHandler.END
    await safe_send(
        update,
        f"⚙ Введи число от {GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY} до {GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY} — "
        "сколько раз в сутки отправлять анонс (равные интервалы).\n\n"
        "Например: `2` — примерно дважды в сутки, `12` — каждые ~2 ч.\n\n/cancel — отмена.",
        parse_mode="Markdown",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )
    return ADMIN_GIVEAWAY_AUTOBROADCAST_PER_DAY_WAITING


async def admin_giveaway_autobroadcast_per_day_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    active = get_active_giveaway()
    if not active:
        await safe_send(update, "❌ Нет активного розыгрыша.", reply_markup=admin_giveaways_keyboard())
        return ConversationHandler.END
    raw_in = (update.message.text or "").strip()
    if re.fullmatch(r"\u21a9\uFE0F? К розыгрышам", raw_in):
        await admin_open_giveaways(update, context)
        return ConversationHandler.END
    try:
        n = int(raw_in)
    except ValueError:
        await safe_send(
            update,
            "❌ Нужно целое число. Попробуй ещё раз или /cancel.",
            reply_markup=admin_giveaway_autobroadcast_keyboard(),
        )
        return ADMIN_GIVEAWAY_AUTOBROADCAST_PER_DAY_WAITING
    if not GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY <= n <= GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY:
        await safe_send(
            update,
            f"❌ Допустимо от {GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY} до {GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY}.",
            reply_markup=admin_giveaway_autobroadcast_keyboard(),
        )
        return ADMIN_GIVEAWAY_AUTOBROADCAST_PER_DAY_WAITING
    gid = active[0]
    cursor.execute(
        "UPDATE giveaways SET autobroadcast_per_day = ? WHERE giveaway_id = ? AND is_active = 1",
        (n, gid),
    )
    conn.commit()
    log_action(update.effective_user.id, "giveaway_autobroadcast_per_day", f"giveaway_id={gid};per_day={n}")
    await safe_send(
        update,
        f"✅ Установлено: {n} {ru_times_per_day_word(n)} в сутки (интервал {format_giveaway_autobroadcast_interval_ru(n)}).",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )
    return ConversationHandler.END


async def admin_open_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "👥 Раздел: клиенты", reply_markup=admin_clients_keyboard())


async def admin_open_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "🔗 Раздел: ссылки и инфо", reply_markup=admin_links_keyboard())


async def admin_open_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id):
        await safe_send(update, "📊 Раздел: аналитика", reply_markup=admin_analytics_keyboard())


async def admin_pickup_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await safe_send(
        update,
        "📍 *Точки самовывоза*\n\nУправляй точками ниже.",
        parse_mode="Markdown",
        reply_markup=pickup_admin_keyboard(),
    )


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    users_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM promocodes")
    codes_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM promocodes WHERE used = 1")
    used_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM items")
    items_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM orders")
    orders_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM pickup_points")
    pickup_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM referrals")
    referrals_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE qualified_at IS NOT NULL")
    referrals_qualified = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT inviter_id) FROM referrals")
    inviters_count = cursor.fetchone()[0]

    await safe_send(
        update,
        f"📊 *Статистика*\n\n"
        f"Пользователей: *{users_count}*\n"
        f"Товарных кнопок: *{items_count}*\n"
        f"Точек самовывоза: *{pickup_count}*\n"
        f"Реф. переходов: *{referrals_count}*\n"
        f"Реф. с первым заказом: *{referrals_qualified}*\n"
        f"Активных рефереров: *{inviters_count}*\n"
        f"Выдано промокодов: *{codes_count}*\n"
        f"Использовано промокодов: *{used_count}*\n"
        f"Заказов: *{orders_count}*",
        parse_mode="Markdown",
    )


async def admin_advanced_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    cursor.execute("SELECT COUNT(*) FROM orders")
    orders_count = cursor.fetchone()[0]
    cursor.execute("SELECT COALESCE(SUM(total_sum), 0) FROM orders WHERE total_sum > 0")
    total_revenue = cursor.fetchone()[0] or 0
    cursor.execute("SELECT COUNT(*) FROM auto_posts")
    auto_posts_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM auto_posts WHERE is_active = 1")
    auto_posts_active = cursor.fetchone()[0]
    cursor.execute("SELECT COALESCE(SUM(sent), 0), COALESCE(SUM(failed), 0), COALESCE(SUM(blocked), 0) FROM broadcast_logs")
    sent_sum, failed_sum, blocked_sum = cursor.fetchone()

    cursor.execute("SELECT item_id, label FROM items")
    items = cursor.fetchall()
    top_pairs = []
    for item_id, label in items:
        cursor.execute("SELECT COUNT(*) FROM orders WHERE items_text LIKE ?", (f"%{label}%",))
        count_value = cursor.fetchone()[0]
        if count_value > 0:
            top_pairs.append((label, count_value))
    top_pairs.sort(key=lambda x: (-x[1], x[0]))
    top_pairs = top_pairs[:5]
    top_text = "\n".join([f"• {label}: {cnt}" for label, cnt in top_pairs]) if top_pairs else "• Пока нет данных"

    await safe_send(
        update,
        f"📈 *Аналитика PRO*\n\n"
        f"Заказов: *{orders_count}*\n"
        f"Общая прибыль: *{total_revenue} ₽*\n"
        f"Автопостов всего: *{auto_posts_count}*\n"
        f"Автопостов активных: *{auto_posts_active}*\n"
        f"Доставлено рассылок: *{sent_sum}*\n"
        f"Ошибок рассылок: *{failed_sum}*\n\n"
        f"Блокировок бота: *{blocked_sum}*\n\n"
        f"*Топ товаров по заказам:*\n{top_text}",
        parse_mode="Markdown",
    )


async def admin_blacklist_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(
        update,
        "🚫 Формат:\nadd USER_ID причина\nremove USER_ID\nlist",
    )
    return ADMIN_BLACKLIST_WAITING


async def admin_blacklist_manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if raw.lower() == "list":
        cursor.execute("SELECT user_id, COALESCE(reason, '') FROM blacklist ORDER BY added_at DESC LIMIT 30")
        rows = cursor.fetchall()
        if not rows:
            await safe_send(update, "Список пуст.")
            return ADMIN_BLACKLIST_WAITING
        lines = ["🚫 Черный список:"]
        for user_id, reason in rows:
            lines.append(f"• {user_id} — {reason or 'без причины'}")
        await safe_send(update, "\n".join(lines))
        return ADMIN_BLACKLIST_WAITING

    parts = raw.split(maxsplit=2)
    if len(parts) < 2:
        await safe_send(update, "❌ Неверный формат.")
        return ADMIN_BLACKLIST_WAITING

    action, user_id_raw = parts[0].lower(), parts[1]
    if not user_id_raw.isdigit():
        await safe_send(update, "❌ USER_ID должен быть числом.")
        return ADMIN_BLACKLIST_WAITING
    target_id = int(user_id_raw)

    if action == "add":
        reason = parts[2] if len(parts) > 2 else ""
        cursor.execute(
            """
            INSERT INTO blacklist (user_id, reason, added_at, added_by)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET reason = EXCLUDED.reason, added_at = EXCLUDED.added_at, added_by = EXCLUDED.added_by
            """,
            (target_id, reason, now_iso(), update.effective_user.id),
        )
        conn.commit()
        log_action(update.effective_user.id, "blacklist_add", f"user={target_id};reason={reason}")
        await safe_send(update, f"✅ Пользователь {target_id} добавлен в ЧС.")
        return ADMIN_BLACKLIST_WAITING

    if action == "remove":
        cursor.execute("DELETE FROM blacklist WHERE user_id = ?", (target_id,))
        conn.commit()
        log_action(update.effective_user.id, "blacklist_remove", f"user={target_id}")
        await safe_send(update, f"✅ Пользователь {target_id} удалён из ЧС.")
        return ADMIN_BLACKLIST_WAITING

    await safe_send(update, "❌ Используй add/remove/list.")
    return ADMIN_BLACKLIST_WAITING


async def admin_category_discount_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    text = "🏷 Формат:\nКАТЕГОРИЯ = 20\nили\nКАТЕГОРИЯ = off\n\nДоступно:\n"
    text += "\n".join([f"• {v}" for v in CATEGORY_LABELS.values()])
    await safe_send(update, text)
    return ADMIN_CATEGORY_DISCOUNT_WAITING


async def admin_category_discount_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if "=" not in raw:
        await safe_send(update, "❌ Формат: КАТЕГОРИЯ = 20 / off")
        return ADMIN_CATEGORY_DISCOUNT_WAITING
    label, value = [x.strip() for x in raw.split("=", 1)]
    category_key = parse_category_from_label(label)
    if not category_key:
        await safe_send(update, "❌ Категория не найдена.")
        return ADMIN_CATEGORY_DISCOUNT_WAITING

    if value.lower() == "off":
        percent = 0
    elif value.isdigit() and 0 <= int(value) <= 95:
        percent = int(value)
    else:
        await safe_send(update, "❌ Процент должен быть 0..95 или off.")
        return ADMIN_CATEGORY_DISCOUNT_WAITING

    cursor.execute(
        """
        INSERT INTO category_discounts (category_key, discount_percent, updated_at, updated_by)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(category_key) DO UPDATE SET
            discount_percent = EXCLUDED.discount_percent,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
        """,
        (category_key, percent, now_iso(), update.effective_user.id),
    )
    conn.commit()
    await safe_send(update, f"✅ Акция для {CATEGORY_LABELS[category_key]}: -{percent}%")
    return ADMIN_CATEGORY_DISCOUNT_WAITING


async def admin_create_giveaway_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🎯 Отправь название розыгрыша.")
    return ADMIN_GIVEAWAY_CREATE_TEXT_WAITING


async def admin_create_giveaway_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_giveaway_title"] = update.message.text.strip()
    await safe_send(update, "📝 Отправь текст описания розыгрыша (без фото — фото добавим на следующем шаге).")
    return ADMIN_GIVEAWAY_CREATE_DESC_WAITING


async def admin_create_giveaway_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        await safe_send(update, "❌ Нужен текст описания.")
        return ADMIN_GIVEAWAY_CREATE_DESC_WAITING
    context.user_data["new_giveaway_text"] = update.message.text.strip()
    await safe_send(
        update,
        "🖼 Отправь *фото* для анонса розыгрыша или напиши `skip`, если фото не нужно.",
    )
    return ADMIN_GIVEAWAY_CREATE_IMAGE_WAITING


async def admin_create_giveaway_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ADMIN_GIVEAWAY_CREATE_IMAGE_WAITING
    t = (update.message.text or "").strip().lower()
    if t == "skip":
        context.user_data["new_giveaway_photo"] = ""
    elif update.message.photo:
        context.user_data["new_giveaway_photo"] = update.message.photo[-1].file_id
    else:
        await safe_send(update, "❌ Отправь фото или напиши `skip`.")
        return ADMIN_GIVEAWAY_CREATE_IMAGE_WAITING
    await safe_send(
        update,
        "🔗 Кнопки под постом: каждая строка «Текст | URL» (http/https/tg://).\n"
        "Несколько кнопок — несколько строк. Напиши `skip`, если кнопки не нужны.",
    )
    return ADMIN_GIVEAWAY_CREATE_BUTTONS_WAITING


async def admin_create_giveaway_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = context.user_data.get("new_giveaway_title")
    text_value = context.user_data.get("new_giveaway_text")
    if not title or not text_value:
        return ConversationHandler.END
    if not update.message or not update.message.text:
        await safe_send(update, "❌ Отправь строки кнопок или `skip`.")
        return ADMIN_GIVEAWAY_CREATE_BUTTONS_WAITING

    body = update.message.text.strip()
    if body.lower() == "skip":
        buttons_json = "[]"
    else:
        parsed, errors = parse_giveaway_buttons_lines(body)
        if errors:
            await safe_send(update, "❌ Ошибки:\n" + "\n".join(errors) + "\n\nПопробуй ещё раз.")
            return ADMIN_GIVEAWAY_CREATE_BUTTONS_WAITING
        buttons_json = json.dumps(parsed, ensure_ascii=False)

    photo = context.user_data.get("new_giveaway_photo") or ""

    cursor.execute("UPDATE giveaways SET is_active = 0 WHERE is_active = 1")
    if USE_POSTGRES:
        cursor.execute(
            """
            INSERT INTO giveaways (title, text_value, photo, is_active, created_at, created_by, finished_at, buttons_json)
            VALUES (?, ?, ?, 1, ?, ?, NULL, ?)
            RETURNING giveaway_id
            """,
            (title, text_value, photo, now_iso(), update.effective_user.id, buttons_json),
        )
        giveaway_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            """
            INSERT INTO giveaways (title, text_value, photo, is_active, created_at, created_by, finished_at, buttons_json)
            VALUES (?, ?, ?, 1, ?, ?, NULL, ?)
            """,
            (title, text_value, photo, now_iso(), update.effective_user.id, buttons_json),
        )
        giveaway_id = cursor.lastrowid
    conn.commit()

    for key in ("new_giveaway_title", "new_giveaway_text", "new_giveaway_photo"):
        context.user_data.pop(key, None)
    await safe_send(update, f"✅ Розыгрыш создан и активирован. ID: {giveaway_id}", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_finish_giveaway_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    active = get_active_giveaway()
    if not active:
        await safe_send(update, "❌ Сейчас нет активного розыгрыша.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    giveaway_id, title, _, _, _, _, _, _, _ = active
    context.user_data["finish_giveaway_id"] = giveaway_id
    top_rows = get_giveaway_top(giveaway_id, limit=20)
    lines = [f"🏁 Завершение розыгрыша: *{title}* (ID {giveaway_id})\n", "Топ участников:"]
    for idx, (inviter_id, username, first_name, invites_count) in enumerate(top_rows, start=1):
        uname = f"@{username}" if username else "-"
        lines.append(f"{idx}. {first_name or '-'} ({uname}) — ID `{inviter_id}` — *{invites_count}*")
    lines.append("\nОтправь ID победителя (или несколько через запятую).")
    await safe_send(update, "\n".join(lines), parse_mode="Markdown")
    return ADMIN_GIVEAWAY_FINISH_WAITING


async def admin_finish_giveaway_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    giveaway_id = context.user_data.get("finish_giveaway_id")
    if not giveaway_id:
        return ConversationHandler.END

    winner_ids = [int(x) for x in re.findall(r"\d+", update.message.text)]
    if not winner_ids:
        await safe_send(update, "❌ Отправь хотя бы один корректный user_id.")
        return ADMIN_GIVEAWAY_FINISH_WAITING

    winner_ids = list(dict.fromkeys(winner_ids))
    for winner_id in winner_ids:
        invites_count = get_giveaway_referrals_count(giveaway_id, winner_id)
        cursor.execute(
            """
            INSERT INTO giveaway_winners (giveaway_id, winner_id, invites_count, selected_at, selected_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (giveaway_id, winner_id, invites_count, now_iso(), update.effective_user.id),
        )
        try:
            await context.bot.send_message(
                chat_id=winner_id,
                text="🎉 Ты выбран победителем розыгрыша! Скоро с тобой свяжется менеджер.",
            )
        except Exception:
            logger.exception("Не удалось уведомить победителя giveaway winner_id=%s", winner_id)

    cursor.execute("UPDATE giveaways SET is_active = 0, finished_at = ? WHERE giveaway_id = ?", (now_iso(), giveaway_id))
    conn.commit()
    context.user_data.pop("finish_giveaway_id", None)

    caption_body = build_giveaway_results_caption(giveaway_id) or "Итоги розыгрыша"
    g_row = get_giveaway_by_id(giveaway_id)
    photo = (g_row[3] or "").strip() if g_row else ""
    intro = (
        "✅ Розыгрыш завершён, победители зафиксированы.\n\n"
        "👁 Предпросмотр рассылки — так увидят сообщение пользователи (кроме чёрного списка):\n"
        "────────"
    )
    tail = "────────\nНажми «Разослать» или «Без рассылки»."
    full_text = f"{intro}\n\n{caption_body}\n\n{tail}"
    link_mk = giveaway_buttons_markup_from_json(g_row[5] if g_row else None)
    link_rows = list(link_mk.inline_keyboard) if link_mk else []
    preview_kb = InlineKeyboardMarkup(
        list(link_rows)
        + [
            [InlineKeyboardButton("📤 Разослать всем клиентам", callback_data=f"gwb:{giveaway_id}")],
            [InlineKeyboardButton("❌ Без рассылки", callback_data=f"gwx:{giveaway_id}")],
        ]
    )
    if update.message:
        try:
            if photo:
                await update.message.reply_photo(photo=photo, caption=full_text, reply_markup=preview_kb)
            else:
                await update.message.reply_text(full_text, reply_markup=preview_kb)
        except Exception:
            logger.exception("Предпросмотр итогов розыгрыша")
            await update.message.reply_text(f"{intro}\n\n{caption_body}\n\n{tail}", reply_markup=preview_kb)
    await safe_send(update, "Кнопки под предпросмотром: разослать или отменить рассылку.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def giveaway_results_broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.from_user or not is_admin(query.from_user.id):
        return
    raw = query.data
    if not isinstance(raw, str) or not raw.startswith("gwb:"):
        return
    try:
        gid = int(raw.split(":", 1)[1])
    except (ValueError, IndexError):
        try:
            await query.answer("Некорректные данные", show_alert=True)
        except Exception:
            pass
        return

    row = get_giveaway_by_id(gid)
    if not row:
        try:
            await query.answer("Розыгрыш не найден.", show_alert=True)
        except Exception:
            pass
        return
    if row[6]:
        try:
            await query.answer("Рассылка уже была отправлена.", show_alert=True)
        except Exception:
            pass
        return
    if not row[4]:
        try:
            await query.answer("Розыгрыш ещё не завершён.", show_alert=True)
        except Exception:
            pass
        return

    caption = build_giveaway_results_caption(gid)
    if not caption:
        try:
            await query.answer("Не удалось сформировать текст.", show_alert=True)
        except Exception:
            pass
        return

    try:
        await query.answer("Рассылка выполняется…", show_alert=False)
    except Exception:
        logger.exception("giveaway broadcast: первый answer")

    photo = (row[3] or "").strip()
    btn_markup = giveaway_buttons_markup_from_json(row[5])
    user_ids = get_broadcast_recipient_user_ids()
    sent = failed = blocked = 0
    reason_stats: dict[str, int] = {}
    for uid in user_ids:
        try:
            if photo:
                await context.bot.send_photo(chat_id=uid, photo=photo, caption=caption, reply_markup=btn_markup)
            else:
                await context.bot.send_message(chat_id=uid, text=caption, reply_markup=btn_markup)
            sent += 1
        except Exception as e:
            failed += 1
            msg = str(e).lower()
            if "blocked" in msg or "forbidden" in msg:
                blocked += 1
            reason = str(e).split(":", 1)[0][:80]
            reason_stats[reason] = reason_stats.get(reason, 0) + 1

    now = now_iso()
    cursor.execute(
        "UPDATE giveaways SET results_broadcast_at = ? WHERE giveaway_id = ? AND results_broadcast_at IS NULL",
        (now, gid),
    )
    conn.commit()
    details = "; ".join(f"{k}={v}" for k, v in reason_stats.items()) if reason_stats else ""
    cursor.execute(
        """
        INSERT INTO broadcast_logs (kind, post_id, sent, failed, blocked, details, created_at, created_by)
        VALUES ('giveaway_results', ?, ?, ?, ?, ?, ?, ?)
        """,
        (gid, sent, failed, blocked, details, now, query.from_user.id),
    )
    conn.commit()
    log_action(query.from_user.id, "giveaway_results_broadcast", f"giveaway={gid};sent={sent};failed={failed}")

    try:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=(
                f"✅ Итоги розыгрыша #{gid} разосланы.\n"
                f"Отправлено: {sent}\nОшибок: {failed}\nНедоставка/блок: {blocked}"
            ),
        )
    except Exception:
        logger.exception("Не удалось отправить отчёт админу о рассылке розыгрыша")


async def giveaway_results_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.from_user or not is_admin(query.from_user.id):
        return
    raw = query.data
    if not isinstance(raw, str) or not raw.startswith("gwx:"):
        return
    try:
        await query.answer("Рассылка итогов отменена. Победители уже сохранены.", show_alert=False)
    except Exception:
        logger.exception("giveaway_results_skip answer")


async def admin_autopost_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(
        update,
        "🤖 Отправь текст автопоста.\n\n"
        "Выйти из сценария: /cancel, /stop, /admin_stop или кнопка «🛑 Прервать сценарий».",
    )
    return ADMIN_AUTOPOST_TEXT_WAITING


async def admin_autopost_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["autopost_text"] = update.message.text
    await safe_send(
        update,
        "🖼 Отправь фото для поста или напиши `skip`.\n"
        "Выйти: /cancel, /stop или «🛑 Прервать сценарий».",
    )
    return ADMIN_AUTOPOST_PHOTO_WAITING


async def admin_autopost_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.photo:
        context.user_data["autopost_photo"] = update.message.photo[-1].file_id
    elif update.message.text.strip().lower() == "skip":
        context.user_data["autopost_photo"] = ""
    else:
        await safe_send(update, "❌ Отправь фото или `skip`.")
        return ADMIN_AUTOPOST_PHOTO_WAITING

    await safe_send(
        update,
        "🔗 Отправь кнопку в формате: Текст | URL\n"
        "Или `skip`.\n"
        "Выйти: /cancel, /stop или «🛑 Прервать сценарий».",
    )
    return ADMIN_AUTOPOST_BUTTON_WAITING


async def admin_autopost_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if raw.lower() == "skip":
        context.user_data["autopost_button_text"] = ""
        context.user_data["autopost_button_url"] = ""
    else:
        if "|" not in raw:
            await safe_send(update, "❌ Формат: Текст | URL")
            return ADMIN_AUTOPOST_BUTTON_WAITING
        button_text, button_url = [x.strip() for x in raw.split("|", 1)]
        if not button_text or not is_valid_inline_button_url(button_url):
            await safe_send(update, "❌ URL должен начинаться с http://, https:// или tg://")
            return ADMIN_AUTOPOST_BUTTON_WAITING
        context.user_data["autopost_button_text"] = button_text
        context.user_data["autopost_button_url"] = button_url

    await safe_send(update, "⏱ Интервал в часах (например 24):")
    return ADMIN_AUTOPOST_INTERVAL_WAITING


async def admin_autopost_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if not raw.isdigit() or int(raw) <= 0:
        await safe_send(update, "❌ Укажи целое число часов (1, 6, 24...).")
        return ADMIN_AUTOPOST_INTERVAL_WAITING

    interval_hours = int(raw)
    next_send_at = now_iso()
    text_value = context.user_data.get("autopost_text", "")
    photo = context.user_data.get("autopost_photo", "")
    button_text = context.user_data.get("autopost_button_text", "")
    button_url = context.user_data.get("autopost_button_url", "")

    cursor.execute(
        """
        INSERT INTO auto_posts (
            text_value, photo, button_text, button_url, interval_hours, is_active,
            last_sent_at, next_send_at, sent_count, created_at, created_by
        )
        VALUES (?, ?, ?, ?, ?, 1, NULL, ?, 0, ?, ?)
        """,
        (text_value, photo, button_text, button_url, interval_hours, next_send_at, now_iso(), update.effective_user.id),
    )
    conn.commit()

    for key in ["autopost_text", "autopost_photo", "autopost_button_text", "autopost_button_url"]:
        context.user_data.pop(key, None)
    await safe_send(update, "✅ Авто-рассылка создана и активирована.", reply_markup=admin_broadcast_keyboard())
    return ConversationHandler.END


def _build_autopost_card_text(row) -> str:
    post_id, text_value, photo, interval_hours, is_active, next_send_at, last_sent_at, sent_count = row
    preview = (text_value or "").replace("\n", " ").strip()
    if len(preview) > 100:
        preview = preview[:100] + "…"
    status = "▶ Активна" if is_active else "⏸ На паузе"
    photo_mark = "🖼 фото" if photo else "📄 только текст"
    return (
        f"📌 Пост #{post_id} ({photo_mark})\n"
        f"Статус: {status}\n"
        f"Интервал: {interval_hours} ч.\n"
        f"Циклов отправки: {sent_count}\n"
        f"Следующая: {next_send_at or '—'}\n"
        f"Последняя: {last_sent_at or '—'}\n"
        f"Текст: {preview or '—'}"
    )


def _autopost_card_markup(post_id: int, is_active: int) -> InlineKeyboardMarkup:
    if is_active:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⏸ Пауза", callback_data=f"autopost_pause:{post_id}"),
                    InlineKeyboardButton("🗑 Удалить", callback_data=f"autopost_delete:{post_id}"),
                ],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("▶ Возобновить", callback_data=f"autopost_resume:{post_id}"),
                InlineKeyboardButton("🗑 Удалить", callback_data=f"autopost_delete:{post_id}"),
            ],
        ]
    )


def _fetch_autopost_row(post_id: int):
    cursor.execute(
        """
        SELECT post_id, text_value, photo, interval_hours, is_active, next_send_at, last_sent_at, sent_count
        FROM auto_posts
        WHERE post_id = ?
        """,
        (post_id,),
    )
    return cursor.fetchone()


async def admin_autopost_list_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    if not update.message:
        return ConversationHandler.END

    cursor.execute(
        """
        SELECT post_id, text_value, photo, interval_hours, is_active, next_send_at, last_sent_at, sent_count
        FROM auto_posts
        ORDER BY post_id DESC
        """
    )
    rows = cursor.fetchall()
    if not rows:
        await safe_send(update, "📋 Авто-рассылок пока нет.", reply_markup=admin_broadcast_keyboard())
        return ConversationHandler.END

    await safe_send(update, f"📋 Всего автопостов: {len(rows)}", reply_markup=admin_broadcast_keyboard())

    for row in rows:
        text = _build_autopost_card_text(row)
        markup = _autopost_card_markup(row[0], row[4])
        await update.message.reply_text(text, reply_markup=markup)

    return ConversationHandler.END


async def autopost_manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()

    parts = query.data.split(":")
    if len(parts) != 2:
        return
    action, pid_raw = parts[0], parts[1]
    if not pid_raw.isdigit():
        return
    post_id = int(pid_raw)

    if action == "autopost_pause":
        cursor.execute("UPDATE auto_posts SET is_active = 0 WHERE post_id = ?", (post_id,))
        conn.commit()
        log_action(query.from_user.id, "autopost_pause", f"post_id={post_id}")
    elif action == "autopost_resume":
        cursor.execute(
            "UPDATE auto_posts SET is_active = 1, next_send_at = COALESCE(next_send_at, ?) WHERE post_id = ?",
            (now_iso(), post_id),
        )
        conn.commit()
        log_action(query.from_user.id, "autopost_resume", f"post_id={post_id}")
    elif action == "autopost_delete":
        cursor.execute("DELETE FROM auto_posts WHERE post_id = ?", (post_id,))
        conn.commit()
        log_action(query.from_user.id, "autopost_delete", f"post_id={post_id}")
        try:
            await query.edit_message_text(f"🗑 Пост #{post_id} удалён.")
        except Exception:
            logger.exception("Не удалось обновить сообщение после удаления автопоста")
        return
    else:
        return

    row = _fetch_autopost_row(post_id)
    if not row:
        try:
            await query.edit_message_text("❌ Пост не найден (возможно удалён).")
        except Exception:
            pass
        return

    try:
        await query.edit_message_text(
            _build_autopost_card_text(row),
            reply_markup=_autopost_card_markup(row[0], row[4]),
        )
    except Exception:
        logger.exception("Не удалось обновить карточку автопоста post_id=%s", post_id)


async def admin_ref_giveaway_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    top_rows = get_referral_top(limit=20)
    if not top_rows:
        await safe_send(update, "Пока нет приглашений.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    lines = ["🎁 *Реферальный рейтинг (топ-20)*\n"]
    for idx, (inviter_id, username, first_name, invites_count) in enumerate(top_rows, start=1):
        safe_username = re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", username or "")
        safe_first_name = re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", first_name or "")
        uname = f"@{safe_username}" if safe_username else "-"
        name = safe_first_name or "-"
        lines.append(f"{idx}. {name} ({uname}) — ID `{inviter_id}` — приглашений: *{invites_count}*")

    lines.append(
        "\nОтправь ID победителя или несколько ID через запятую/пробел.\n"
        "Пример: `123456789, 987654321`\n\n/cancel — отмена"
    )
    await safe_send(update, "\n".join(lines), parse_mode="Markdown")
    return ADMIN_REF_GIVEAWAY_WAITING


async def admin_ref_giveaway_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    raw_text = update.message.text.strip()
    parts = [p for p in re.split(r"[\s,;]+", raw_text) if p]
    if not parts:
        await safe_send(update, "❌ Отправь хотя бы один числовой user_id.")
        return ADMIN_REF_GIVEAWAY_WAITING

    winner_ids = []
    for part in parts:
        if not part.isdigit():
            await safe_send(update, f"❌ Некорректный ID: {part}")
            return ADMIN_REF_GIVEAWAY_WAITING
        winner_ids.append(int(part))

    winner_ids = list(dict.fromkeys(winner_ids))
    result_lines = ["✅ Победители зафиксированы:\n"]

    for winner_id in winner_ids:
        cursor.execute(
            """
            SELECT COALESCE(username, ''), COALESCE(first_name, '')
            FROM users
            WHERE user_id = ?
            """,
            (winner_id,),
        )
        user_row = cursor.fetchone()
        if not user_row:
            result_lines.append(f"• ID {winner_id}: пользователь не найден")
            continue

        invites_count = get_referrals_count(winner_id)
        cursor.execute(
            """
            INSERT INTO referral_winners (winner_id, invites_count, selected_at, selected_by)
            VALUES (?, ?, ?, ?)
            """,
            (winner_id, invites_count, now_iso(), update.effective_user.id),
        )
        conn.commit()

        username, first_name = user_row
        safe_username = re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", username or "")
        safe_first_name = re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", first_name or "")
        uname = f"@{safe_username}" if safe_username else "-"
        result_lines.append(f"• {safe_first_name or '-'} ({uname}) — ID {winner_id}")

        try:
            await context.bot.send_message(
                chat_id=winner_id,
                text="🎉 Поздравляем! Ты выбран победителем реферального розыгрыша. Скоро свяжется менеджер.",
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление победителю %s", winner_id)

    await safe_send(update, "\n".join(result_lines), reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(
        update,
        "📢 Отправь пост рассылки: текст или фото с подписью.\n"
        "Отмена: /cancel, /stop, /admin_stop или «🛑 Прервать сценарий».",
    )
    return ADMIN_BROADCAST_WAITING


async def admin_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ADMIN_BROADCAST_WAITING
    text = update.message.text or update.message.caption or ""
    photo = update.message.photo[-1].file_id if update.message.photo else ""
    if not text and not photo:
        await safe_send(update, "❌ Нужен текст или фото.")
        return ADMIN_BROADCAST_WAITING
    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    sent = 0
    failed = 0
    blocked = 0
    reason_stats = {}
    for user_id in user_ids:
        try:
            if photo:
                await context.bot.send_photo(chat_id=user_id, photo=photo, caption=text or None)
            else:
                await context.bot.send_message(chat_id=user_id, text=text)
            sent += 1
        except Exception as e:
            failed += 1
            msg = str(e).lower()
            if "blocked" in msg or "forbidden" in msg:
                blocked += 1
            reason = str(e).split(":", 1)[0][:80]
            reason_stats[reason] = reason_stats.get(reason, 0) + 1

    cursor.execute(
        """
        INSERT INTO broadcast_logs (kind, post_id, sent, failed, blocked, details, created_at, created_by)
        VALUES ('manual', NULL, ?, ?, ?, ?, ?, ?)
        """,
        (sent, failed, blocked, "; ".join([f"{k}={v}" for k, v in reason_stats.items()]), now_iso(), update.effective_user.id),
    )
    conn.commit()
    log_action(update.effective_user.id, "broadcast_manual", f"sent={sent};failed={failed};blocked={blocked}")

    await safe_send(
        update,
        f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}\nБлокировок: {blocked}",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def send_giveaway_announce_broadcast(
    context: ContextTypes.DEFAULT_TYPE, row: tuple
) -> tuple[int, int, int, dict[str, int]]:
    """Один проход: анонс активного розыгрыша всем получателям (как «🎁 Розыгрыши»)."""
    gid, title, text_value, photo, _, buttons_json, _, _, _ = row
    bot_username = context.bot.username or ""
    markup = giveaway_buttons_markup_from_json(buttons_json)
    user_ids = get_broadcast_recipient_user_ids()
    if not user_ids:
        return 0, 0, 0, {}

    sent = failed = blocked = 0
    reason_stats: dict[str, int] = {}
    for uid in user_ids:
        has_photo = bool((photo or "").strip())
        cap_limit = GIVEAWAY_PHOTO_CAPTION_MAX if has_photo else GIVEAWAY_TEXT_MESSAGE_MAX
        caption = _fit_giveaway_caption(
            bot_username, uid, gid, title, text_value, cap_limit, use_html=True
        )
        try:
            if has_photo:
                await context.bot.send_photo(
                    chat_id=uid, photo=photo, caption=caption, parse_mode="HTML", reply_markup=markup
                )
            else:
                await context.bot.send_message(
                    chat_id=uid, text=caption, parse_mode="HTML", reply_markup=markup
                )
            sent += 1
        except Exception as e:
            err_s = str(e).lower()
            retry_plain = any(
                x in err_s
                for x in (
                    "parse",
                    "entity",
                    "can't parse",
                    "caption is too long",
                    "message is too long",
                    "text must be encoded",
                )
            )
            if retry_plain:
                try:
                    plain = _fit_giveaway_caption(
                        bot_username, uid, gid, title, text_value, cap_limit, use_html=False
                    )
                    if has_photo:
                        await context.bot.send_photo(
                            chat_id=uid, photo=photo, caption=plain, reply_markup=markup
                        )
                    else:
                        await context.bot.send_message(chat_id=uid, text=plain, reply_markup=markup)
                    sent += 1
                    continue
                except Exception as e2:
                    e = e2
                    err_s = str(e).lower()
            failed += 1
            if "blocked" in err_s or "forbidden" in err_s:
                blocked += 1
            reason = str(e).split(":", 1)[0][:80]
            reason_stats[reason] = reason_stats.get(reason, 0) + 1
            if failed <= 3:
                logger.warning("giveaway announce: uid=%s err=%s", uid, e)
    return sent, failed, blocked, reason_stats


async def process_giveaway_autobroadcast(context: ContextTypes.DEFAULT_TYPE):
    row = get_active_giveaway()
    if not row:
        return
    gid, _, _, _, _, _, ab_en, ab_per_day, ab_last = row
    if not ab_en:
        return
    per_day = max(
        GIVEAWAY_AUTOBROADCAST_MIN_PER_DAY,
        min(int(ab_per_day or GIVEAWAY_AUTOBROADCAST_DEFAULT_PER_DAY), GIVEAWAY_AUTOBROADCAST_MAX_PER_DAY),
    )
    need_sec = giveaway_autobroadcast_interval_seconds(per_day)
    now = datetime.now()
    if ab_last:
        try:
            last_dt = datetime.fromisoformat(ab_last)
        except ValueError:
            last_dt = None
        if last_dt and (now - last_dt).total_seconds() < need_sec:
            return

    sent, failed, blocked, reason_stats = await send_giveaway_announce_broadcast(context, row)
    if sent == 0 and failed == 0:
        return

    ts = now_iso()
    cursor.execute(
        "UPDATE giveaways SET autobroadcast_last_at = ? WHERE giveaway_id = ? AND is_active = 1",
        (ts, gid),
    )
    cursor.execute(
        """
        INSERT INTO broadcast_logs (kind, post_id, sent, failed, blocked, details, created_at, created_by)
        VALUES ('giveaway_auto', ?, ?, ?, ?, ?, ?, NULL)
        """,
        (gid, sent, failed, blocked, "; ".join(f"{k}={v}" for k, v in reason_stats.items()), ts),
    )
    conn.commit()


async def admin_giveaway_autobroadcast_once_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    row = get_active_giveaway()
    if not row:
        await safe_send(update, "❌ Нет активного розыгрыша.", reply_markup=admin_giveaways_keyboard())
        return
    gid = row[0]
    if not get_broadcast_recipient_user_ids():
        await safe_send(
            update,
            "❌ Нет получателей: в базе нет пользователей или все в чёрном списке.",
            reply_markup=admin_giveaway_autobroadcast_keyboard(),
        )
        return
    await safe_send(
        update,
        "⏳ Рассылаю анонс всем клиентам (кроме чёрного списка)… Это может занять время.",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )
    sent, failed, blocked, reason_stats = await send_giveaway_announce_broadcast(context, row)
    ts = now_iso()
    cursor.execute(
        "UPDATE giveaways SET autobroadcast_last_at = ? WHERE giveaway_id = ? AND is_active = 1",
        (ts, gid),
    )
    details = "; ".join(f"{k}={v}" for k, v in reason_stats.items())
    cursor.execute(
        """
        INSERT INTO broadcast_logs (kind, post_id, sent, failed, blocked, details, created_at, created_by)
        VALUES ('giveaway_once', ?, ?, ?, ?, ?, ?, ?)
        """,
        (gid, sent, failed, blocked, details, ts, update.effective_user.id),
    )
    conn.commit()
    log_action(update.effective_user.id, "giveaway_announce_once", f"giveaway_id={gid};sent={sent};failed={failed}")
    detail_line = ""
    if reason_stats:
        detail_line = "\n" + "\n".join(f"• {k}: {v}" for k, v in sorted(reason_stats.items(), key=lambda x: -x[1]))
    await safe_send(
        update,
        f"✅ Разовая рассылка анонса завершена.\n"
        f"Отправлено: {sent}\nОшибок: {failed}\nНедоставка/блок: {blocked}"
        f"{detail_line}",
        reply_markup=admin_giveaway_autobroadcast_keyboard(),
    )


async def process_auto_posts(context: ContextTypes.DEFAULT_TYPE):
    cursor.execute(
        """
        SELECT post_id, text_value, photo, button_text, button_url, interval_hours
        FROM auto_posts
        WHERE is_active = 1 AND (next_send_at IS NULL OR next_send_at <= ?)
        ORDER BY post_id ASC
        """,
        (now_iso(),),
    )
    posts = cursor.fetchall()
    if posts:
        cursor.execute("SELECT user_id FROM users")
        user_ids = [row[0] for row in cursor.fetchall()]
        if user_ids:
            for post_id, text_value, photo, button_text, button_url, interval_hours in posts:
                sent = 0
                failed = 0
                blocked = 0
                reason_stats = {}
                markup = autopost_button_markup(button_text, button_url)

                for user_id in user_ids:
                    try:
                        if photo:
                            await context.bot.send_photo(
                                chat_id=user_id, photo=photo, caption=text_value, reply_markup=markup
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=user_id, text=text_value, reply_markup=markup
                            )
                        sent += 1
                    except Exception as e:
                        failed += 1
                        msg = str(e).lower()
                        if "blocked" in msg or "forbidden" in msg:
                            blocked += 1
                        reason = str(e).split(":", 1)[0][:80]
                        reason_stats[reason] = reason_stats.get(reason, 0) + 1

                next_send = (datetime.now() + timedelta(hours=interval_hours)).isoformat()
                cursor.execute(
                    """
                    UPDATE auto_posts
                    SET last_sent_at = ?, next_send_at = ?, sent_count = sent_count + ?
                    WHERE post_id = ?
                    """,
                    (now_iso(), next_send, sent, post_id),
                )
                cursor.execute(
                    """
                    INSERT INTO broadcast_logs (kind, post_id, sent, failed, blocked, details, created_at, created_by)
                    VALUES ('auto', ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        post_id,
                        sent,
                        failed,
                        blocked,
                        "; ".join([f"{k}={v}" for k, v in reason_stats.items()]),
                        now_iso(),
                    ),
                )
                conn.commit()

    await process_giveaway_autobroadcast(context)


async def admin_baraholki_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🛒 Отправь новую ссылку на барахолки. /cancel для отмены")
    return ADMIN_BARAHOLKI_WAITING


async def admin_baraholki_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("baraholki_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на барахолки обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_projects_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🚀 Отправь новую ссылку на проекты. /cancel для отмены")
    return ADMIN_PROJECTS_WAITING


async def admin_projects_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("projects_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на проекты обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_giveaways_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🎁 Отправь новую ссылку на розыгрыши. /cancel для отмены")
    return ADMIN_GIVEAWAYS_WAITING


async def admin_giveaways_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("giveaways_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на розыгрыши обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_manager_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "💬 Отправь новую ссылку на менеджера. /cancel для отмены")
    return ADMIN_MANAGER_WAITING


async def admin_manager_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("manager_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на менеджера обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_add_item_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "➕ Выбери категорию, куда добавить кнопку.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_ADD_ITEM_CATEGORY_WAITING


async def admin_add_item_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_ADD_ITEM_CATEGORY_WAITING

    context.user_data["admin_item_category"] = category_key
    await safe_send(update, "✏️ Теперь отправь название новой кнопки.")
    return ADMIN_ADD_ITEM_NAME_WAITING


async def admin_add_item_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["admin_item_name"] = update.message.text.strip()
    await safe_send(update, "📝 Теперь отправь описание для этой кнопки.")
    return ADMIN_ADD_ITEM_DESC_WAITING


async def admin_add_item_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["admin_item_desc"] = update.message.text
    await safe_send(update, "💰 Теперь отправь цену в рублях.\nПример: 1990")
    return ADMIN_ADD_ITEM_PRICE_WAITING


async def admin_add_item_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    price = parse_price(update.message.text)
    if price is None:
        await safe_send(update, "❌ Цена должна быть числом. Пример: 1990")
        return ADMIN_ADD_ITEM_PRICE_WAITING

    context.user_data["admin_item_price"] = price
    await safe_send(update, "🖼 Теперь отправь фото для этой кнопки.")
    return ADMIN_ADD_ITEM_IMAGE_WAITING


async def admin_add_item_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_ADD_ITEM_IMAGE_WAITING

    category_key = context.user_data.get("admin_item_category")
    name = context.user_data.get("admin_item_name")
    desc = context.user_data.get("admin_item_desc")
    price = context.user_data.get("admin_item_price")

    if not category_key or not name or desc is None or price is None:
        await safe_send(update, "❌ Данные потерялись.")
        return ConversationHandler.END

    file_id = update.message.photo[-1].file_id
    add_item(category_key, name, desc, file_id, price)

    for key in ["admin_item_category", "admin_item_name", "admin_item_desc", "admin_item_price"]:
        context.user_data.pop(key, None)

    await safe_send(update, "✅ Кнопка добавлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_rename_item_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "✏️ Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_RENAME_ITEM_CATEGORY_WAITING


async def admin_rename_item_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_RENAME_ITEM_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["rename_category_key"] = category_key
    text = "Отправь *точное название* кнопки, которую нужно переименовать:\n\n"
    text += "\n".join([f"• {item[3]}" for item in items])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_RENAME_ITEM_SELECT_WAITING


async def admin_rename_item_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("rename_category_key")
    item = get_item_by_label(category_key, update.message.text.strip())
    if not item:
        await safe_send(update, "❌ Кнопка не найдена. Отправь точное название.")
        return ADMIN_RENAME_ITEM_SELECT_WAITING

    context.user_data["rename_item_id"] = item[0]
    await safe_send(update, "✏️ Отправь новое название.")
    return ADMIN_RENAME_ITEM_NEW_NAME_WAITING


async def admin_rename_item_new_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    item_id = context.user_data.get("rename_item_id")
    if not item_id:
        return ConversationHandler.END

    update_item_label(item_id, update.message.text.strip())
    context.user_data.pop("rename_category_key", None)
    context.user_data.pop("rename_item_id", None)

    await safe_send(update, "✅ Название кнопки обновлено.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_edit_desc_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "📝 Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_EDIT_DESC_CATEGORY_WAITING


async def admin_edit_desc_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_EDIT_DESC_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["edit_desc_category_key"] = category_key
    text = "Отправь *точное название* кнопки, у которой нужно изменить описание:\n\n"
    text += "\n".join([f"• {item[3]}" for item in items])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_EDIT_DESC_SELECT_WAITING


async def admin_edit_desc_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_desc_category_key")
    item = get_item_by_label(category_key, update.message.text.strip())
    if not item:
        await safe_send(update, "❌ Кнопка не найдена. Отправь точное название.")
        return ADMIN_EDIT_DESC_SELECT_WAITING

    context.user_data["edit_desc_item_id"] = item[0]
    await safe_send(update, "📝 Отправь новое описание.")
    return ADMIN_EDIT_DESC_NEW_WAITING


async def admin_edit_desc_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    item_id = context.user_data.get("edit_desc_item_id")
    if not item_id:
        return ConversationHandler.END

    update_item_description(item_id, update.message.text)
    context.user_data.pop("edit_desc_category_key", None)
    context.user_data.pop("edit_desc_item_id", None)

    await safe_send(update, "✅ Описание обновлено.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_edit_image_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🖼 Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_EDIT_IMAGE_CATEGORY_WAITING


async def admin_edit_image_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_EDIT_IMAGE_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["edit_image_category_key"] = category_key
    text = "Отправь *точное название* кнопки, у которой нужно изменить фото:\n\n"
    text += "\n".join([f"• {item[3]}" for item in items])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_EDIT_IMAGE_SELECT_WAITING


async def admin_edit_image_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_image_category_key")
    item = get_item_by_label(category_key, update.message.text.strip())
    if not item:
        await safe_send(update, "❌ Кнопка не найдена. Отправь точное название.")
        return ADMIN_EDIT_IMAGE_SELECT_WAITING

    context.user_data["edit_image_item_id"] = item[0]
    await safe_send(update, "🖼 Отправь новое фото.")
    return ADMIN_EDIT_IMAGE_NEW_WAITING


async def admin_edit_image_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    item_id = context.user_data.get("edit_image_item_id")
    if not item_id:
        return ConversationHandler.END

    if not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_EDIT_IMAGE_NEW_WAITING

    file_id = update.message.photo[-1].file_id
    update_item_image(item_id, file_id)

    context.user_data.pop("edit_image_category_key", None)
    context.user_data.pop("edit_image_item_id", None)

    await safe_send(update, "✅ Фото обновлено.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_edit_price_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "💰 Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_EDIT_PRICE_CATEGORY_WAITING


async def admin_edit_price_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_EDIT_PRICE_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["edit_price_category_key"] = category_key
    text = "Отправь *точное название* кнопки, у которой нужно изменить цену:\n\n"
    text += "\n".join([f"• {item[3]} — {format_price(item[7])}" for item in items])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_EDIT_PRICE_SELECT_WAITING


async def admin_edit_price_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_price_category_key")
    item = get_item_by_label(category_key, update.message.text.strip())
    if not item:
        await safe_send(update, "❌ Кнопка не найдена. Отправь точное название.")
        return ADMIN_EDIT_PRICE_SELECT_WAITING

    context.user_data["edit_price_item_id"] = item[0]
    await safe_send(update, "💰 Отправь новую цену в рублях.\nПример: 1990")
    return ADMIN_EDIT_PRICE_NEW_WAITING


async def admin_edit_price_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    item_id = context.user_data.get("edit_price_item_id")
    if not item_id:
        return ConversationHandler.END

    price = parse_price(update.message.text)
    if price is None:
        await safe_send(update, "❌ Цена должна быть числом. Пример: 1990")
        return ADMIN_EDIT_PRICE_NEW_WAITING

    update_item_price(item_id, price)
    context.user_data.pop("edit_price_category_key", None)
    context.user_data.pop("edit_price_item_id", None)

    await safe_send(update, "✅ Цена обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_delete_item_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "🗑 Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_DELETE_ITEM_CATEGORY_WAITING


async def admin_delete_item_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_DELETE_ITEM_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["delete_category_key"] = category_key
    text = "Отправь *точное название* кнопки, которую нужно удалить:\n\n"
    text += "\n".join([f"• {item[3]}" for item in items])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_DELETE_ITEM_SELECT_WAITING


async def admin_delete_item_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("delete_category_key")
    item = get_item_by_label(category_key, update.message.text.strip())
    if not item:
        await safe_send(update, "❌ Кнопка не найдена. Отправь точное название.")
        return ADMIN_DELETE_ITEM_SELECT_WAITING

    delete_item(item[0])
    context.user_data.pop("delete_category_key", None)

    await safe_send(update, "✅ Кнопка удалена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_reorder_item_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "↕️ Выбери категорию.", reply_markup=admin_category_choice_keyboard())
    return ADMIN_REORDER_ITEM_CATEGORY_WAITING


async def admin_reorder_item_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_REORDER_ITEM_CATEGORY_WAITING

    items = get_items_by_category(category_key)
    if not items:
        await safe_send(update, "❌ В этой категории пока нет кнопок.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    context.user_data["reorder_category_key"] = category_key
    text = "Отправь сообщение в формате:\n\nНАЗВАНИЕ КНОПКИ = up\nили\nНАЗВАНИЕ КНОПКИ = down\n\n"
    text += "Текущий порядок:\n"
    for item in items:
        text += f"{item[6]}. {item[3]}\n"

    await safe_send(update, text)
    return ADMIN_REORDER_ITEM_WAITING


async def admin_reorder_item_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("reorder_category_key")
    raw = update.message.text.strip()

    if "=" not in raw:
        await safe_send(update, "❌ Формат: НАЗВАНИЕ КНОПКИ = up/down")
        return ADMIN_REORDER_ITEM_WAITING

    label, direction = [part.strip() for part in raw.split("=", 1)]
    direction = direction.lower()

    if direction not in {"up", "down"}:
        await safe_send(update, "❌ Направление должно быть up или down.")
        return ADMIN_REORDER_ITEM_WAITING

    item = get_item_by_label(category_key, label)
    if not item:
        await safe_send(update, "❌ Кнопка не найдена.")
        return ADMIN_REORDER_ITEM_WAITING

    ok = move_item(item[0], direction)
    if not ok:
        await safe_send(update, "⚠️ Дальше двигать уже некуда.")
        return ADMIN_REORDER_ITEM_WAITING

    context.user_data.pop("reorder_category_key", None)
    await safe_send(update, "✅ Порядок кнопок обновлён.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_add_pickup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await safe_send(update, "➕ Отправь название новой точки самовывоза.")
    return ADMIN_ADD_PICKUP_NAME_WAITING


async def admin_add_pickup_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if len(name) < 2:
        await safe_send(update, "❌ Название слишком короткое.")
        return ADMIN_ADD_PICKUP_NAME_WAITING

    add_pickup_point(name)
    await safe_send(update, "✅ Точка самовывоза добавлена.", reply_markup=pickup_admin_keyboard())
    return ConversationHandler.END


async def admin_rename_pickup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    points = get_pickup_points()
    if not points:
        await safe_send(update, "❌ Нет точек самовывоза.", reply_markup=pickup_admin_keyboard())
        return ConversationHandler.END

    text = "Отправь *точное название* точки, которую нужно переименовать:\n\n"
    text += "\n".join([f"• {point[1]}" for point in points])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_RENAME_PICKUP_SELECT_WAITING


async def admin_rename_pickup_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    point = get_pickup_point_by_name(update.message.text.strip())
    if not point:
        await safe_send(update, "❌ Точка не найдена.")
        return ADMIN_RENAME_PICKUP_SELECT_WAITING

    context.user_data["rename_pickup_id"] = point[0]
    await safe_send(update, "✏️ Отправь новое название точки.")
    return ADMIN_RENAME_PICKUP_NEW_WAITING


async def admin_rename_pickup_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pickup_id = context.user_data.get("rename_pickup_id")
    if not pickup_id:
        return ConversationHandler.END

    rename_pickup_point(pickup_id, update.message.text.strip())
    context.user_data.pop("rename_pickup_id", None)

    await safe_send(update, "✅ Точка переименована.", reply_markup=pickup_admin_keyboard())
    return ConversationHandler.END


async def admin_delete_pickup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    points = get_pickup_points()
    if not points:
        await safe_send(update, "❌ Нет точек самовывоза.", reply_markup=pickup_admin_keyboard())
        return ConversationHandler.END

    text = "Отправь *точное название* точки, которую нужно удалить:\n\n"
    text += "\n".join([f"• {point[1]}" for point in points])
    await safe_send(update, text, parse_mode="Markdown")
    return ADMIN_DELETE_PICKUP_SELECT_WAITING


async def admin_delete_pickup_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    point = get_pickup_point_by_name(update.message.text.strip())
    if not point:
        await safe_send(update, "❌ Точка не найдена.")
        return ADMIN_DELETE_PICKUP_SELECT_WAITING

    delete_pickup_point(point[0])
    await safe_send(update, "✅ Точка удалена.", reply_markup=pickup_admin_keyboard())
    return ConversationHandler.END


async def admin_reorder_pickup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    points = get_pickup_points()
    if not points:
        await safe_send(update, "❌ Нет точек самовывоза.", reply_markup=pickup_admin_keyboard())
        return ConversationHandler.END

    text = "Отправь сообщение в формате:\n\nНАЗВАНИЕ ТОЧКИ = up\nили\nНАЗВАНИЕ ТОЧКИ = down\n\n"
    text += "Текущий порядок:\n"
    for point in points:
        text += f"{point[2]}. {point[1]}\n"

    await safe_send(update, text)
    return ADMIN_REORDER_PICKUP_WAITING


async def admin_reorder_pickup_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if "=" not in raw:
        await safe_send(update, "❌ Формат: НАЗВАНИЕ ТОЧКИ = up/down")
        return ADMIN_REORDER_PICKUP_WAITING

    label, direction = [part.strip() for part in raw.split("=", 1)]
    direction = direction.lower()

    if direction not in {"up", "down"}:
        await safe_send(update, "❌ Направление должно быть up или down.")
        return ADMIN_REORDER_PICKUP_WAITING

    point = get_pickup_point_by_name(label)
    if not point:
        await safe_send(update, "❌ Точка не найдена.")
        return ADMIN_REORDER_PICKUP_WAITING

    ok = move_pickup_point(point[0], direction)
    if not ok:
        await safe_send(update, "⚠️ Дальше двигать уже некуда.")
        return ADMIN_REORDER_PICKUP_WAITING

    await safe_send(update, "✅ Порядок точек обновлён.", reply_markup=pickup_admin_keyboard())
    return ConversationHandler.END


async def admin_set_category_photo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    await safe_send(
        update,
        "🖼 Выбери категорию, для которой нужно установить фото.",
        reply_markup=admin_category_choice_keyboard(),
    )
    return ADMIN_SET_CATEGORY_PHOTO_CATEGORY_WAITING


async def admin_set_category_photo_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_SET_CATEGORY_PHOTO_CATEGORY_WAITING

    context.user_data["category_photo_key"] = category_key
    current_photo = get_category_image(category_key)
    extra = "\nСейчас фото уже установлено — новое фото заменит старое." if current_photo else ""
    await safe_send(update, f"🖼 Отправь фото для категории {CATEGORY_LABELS[category_key]}.{extra}")
    return ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING


async def admin_set_category_photo_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("category_photo_key")
    if not category_key:
        await safe_send(update, "❌ Категория потерялась. Начни заново.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    if not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING

    file_id = update.message.photo[-1].file_id
    set_category_image(category_key, file_id)
    context.user_data.pop("category_photo_key", None)

    await safe_send(
        update,
        f"✅ Фото для категории {CATEGORY_LABELS[category_key]} обновлено.",
    )
    await safe_send(update, "Предпросмотр категории:")
    await open_category_view(update.message, category_key)
    await safe_send(update, "Возвращаю в админку.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_set_category_photo_image_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(
        update,
        "❌ Нужно отправить именно фото категории (не текст).",
    )
    return ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING


async def admin_clear_category_photo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    await safe_send(
        update,
        "🗑 Выбери категорию, у которой нужно удалить фото.",
        reply_markup=admin_category_choice_keyboard(),
    )
    return ADMIN_CLEAR_CATEGORY_PHOTO_CATEGORY_WAITING


async def admin_clear_category_photo_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = parse_category_from_label(update.message.text.strip())
    if not category_key:
        await safe_send(update, "❌ Выбери категорию кнопкой ниже.", reply_markup=admin_category_choice_keyboard())
        return ADMIN_CLEAR_CATEGORY_PHOTO_CATEGORY_WAITING

    clear_category_image(category_key)
    await safe_send(
        update,
        f"✅ Фото для категории {CATEGORY_LABELS[category_key]} удалено.",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


def _format_welcome_buttons_preview() -> str:
    rows = get_welcome_buttons_raw()
    if not rows:
        return "— нет —"
    lines = []
    for item in rows:
        if isinstance(item, dict):
            t = (item.get("text") or "").strip()
            u = (item.get("url") or "").strip()
            if t or u:
                lines.append(f"• {t} → {u}")
    return "\n".join(lines) if lines else "— нет —"


async def admin_create_promo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    context.user_data.pop("admin_new_promo_code", None)
    await safe_send(
        update,
        "🎫 *Создание промокода*\n\n"
        "Отправь код одним сообщением (латиница и цифры, 4–24 символа), "
        "или напиши `авто` — подберу свободный в формате `RNDM-XXXXXX`.\n\n"
        "Такой промокод может использовать *любой* клиент, без ограничения 12 часов.\n"
        "/cancel — отмена.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ADMIN_CREATE_PROMO_CODE_WAITING


async def admin_create_promo_code_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    if not update.message or not update.message.text:
        await safe_send(update, "Нужен текст кодом.", reply_markup=admin_keyboard())
        return ADMIN_CREATE_PROMO_CODE_WAITING
    raw = update.message.text.strip()
    if raw in ADMIN_ESCAPE_LABELS:
        return await admin_escape_conversation(update, context)
    low = raw.lower()
    if low == "авто":
        code = generate_code()
        for _ in range(80):
            if not get_promocode(code):
                break
            code = generate_code()
        else:
            await safe_send(update, "❌ Не удалось сгенерировать уникальный код. Попробуй ввести свой.", reply_markup=admin_keyboard())
            return ADMIN_CREATE_PROMO_CODE_WAITING
    else:
        code = re.sub(r"[^A-Za-z0-9_-]", "", raw).upper()
        if len(code) < 4 or len(code) > 24:
            await safe_send(
                update,
                "❌ Длина кода 4–24 символа, только буквы, цифры, `-` и `_`.",
                reply_markup=admin_keyboard(),
            )
            return ADMIN_CREATE_PROMO_CODE_WAITING
        if get_promocode(code):
            await safe_send(update, "❌ Такой код уже есть. Введи другой или `авто`.", reply_markup=admin_keyboard())
            return ADMIN_CREATE_PROMO_CODE_WAITING
    context.user_data["admin_new_promo_code"] = code
    await safe_send(
        update,
        f"Код: `{code}`\n\nВведи скидку в процентах (целое число от *1* до *100*).",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ADMIN_CREATE_PROMO_DISCOUNT_WAITING


async def admin_create_promo_discount_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    code = context.user_data.get("admin_new_promo_code")
    if not code:
        await safe_send(update, "❌ Сессия сброшена. Начни с «🎫 Создать промокод».", reply_markup=admin_keyboard())
        return ConversationHandler.END
    if not update.message or not update.message.text:
        await safe_send(update, "Введи число — процент скидки.", reply_markup=admin_keyboard())
        return ADMIN_CREATE_PROMO_DISCOUNT_WAITING
    raw = update.message.text.strip()
    if raw in ADMIN_ESCAPE_LABELS:
        context.user_data.pop("admin_new_promo_code", None)
        return await admin_escape_conversation(update, context)
    try:
        pct = int(raw)
    except ValueError:
        await safe_send(update, "❌ Нужно целое число от 1 до 100.", reply_markup=admin_keyboard())
        return ADMIN_CREATE_PROMO_DISCOUNT_WAITING
    if not 1 <= pct <= 100:
        await safe_send(update, "❌ Скидка только от 1 до 100%.", reply_markup=admin_keyboard())
        return ADMIN_CREATE_PROMO_DISCOUNT_WAITING
    if get_promocode(code):
        context.user_data.pop("admin_new_promo_code", None)
        await safe_send(update, "❌ Код занят. Начни заново.", reply_markup=admin_keyboard())
        return ConversationHandler.END
    created_at = now_iso()
    cursor.execute(
        """
        INSERT INTO promocodes (code, discount, used, created_at, used_at, owner_user_id, admin_global)
        VALUES (?, ?, 0, ?, NULL, NULL, 1)
        """,
        (code, pct, created_at),
    )
    conn.commit()
    context.user_data.pop("admin_new_promo_code", None)
    log_action(update.effective_user.id, "admin_create_promo", f"code={code};discount={pct}")
    await safe_send(
        update,
        f"✅ Промокод `{code}` создан: скидка *{pct}%*.\n"
        f"Любой пользователь может ввести его при оформлении заказа (одноразово, пока не использован).",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_welcome_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_send(update, "⛔ У тебя нет доступа.")
        return ConversationHandler.END
    await safe_send(
        update,
        "👋 *Экран приветствия*\n\n"
        "То, что пользователь видит по команде /start: фото (по желанию), подпись и кнопки-ссылки.\n"
        "Настрой через меню ниже.",
        parse_mode="Markdown",
        reply_markup=admin_welcome_keyboard(),
    )
    return ADMIN_WELCOME_MENU_WAITING


async def admin_welcome_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        await safe_send(update, "Выбери пункт меню кнопкой ниже.", reply_markup=admin_welcome_keyboard())
        return ADMIN_WELCOME_MENU_WAITING
    text = update.message.text.strip()
    if text in ADMIN_ESCAPE_LABELS:
        return await admin_escape_conversation(update, context)
    if text == "📝 Текст приветствия":
        await safe_send(
            update,
            "Отправь текст подписи к приветственному сообщению (можно несколько строк).",
        )
        return ADMIN_WELCOME_TEXT_WAITING
    if text == "🖼 Фото приветствия":
        await safe_send(update, "Отправь фото для экрана приветствия.")
        return ADMIN_WELCOME_PHOTO_WAITING
    if text == "🔗 Кнопки под постом":
        await safe_send(
            update,
            "Отправь список кнопок: каждая строка — одна кнопка:\n"
            "Текст на кнопке | https://ссылка\n\n"
            "Допустимы ссылки http://, https:// или tg:// (например tg://user?id=123).\n"
            "Чтобы убрать все кнопки, отправь одно слово: пусто\n\n"
            "Текущие кнопки:\n"
            f"{_format_welcome_buttons_preview()}",
        )
        return ADMIN_WELCOME_BUTTONS_WAITING
    if text == "🗑 Убрать фото":
        clear_welcome_photo_value()
        await safe_send(
            update,
            "✅ Фото приветствия убрано.",
            reply_markup=admin_welcome_keyboard(),
        )
        return ADMIN_WELCOME_MENU_WAITING
    if text == "👁 Предпросмотр":
        await send_welcome_screen(update.message, update.effective_user.id)
        await safe_send(update, "Выше — предпросмотр.", reply_markup=admin_welcome_keyboard())
        return ADMIN_WELCOME_MENU_WAITING
    await safe_send(update, "Выбери пункт меню кнопкой ниже.", reply_markup=admin_welcome_keyboard())
    return ADMIN_WELCOME_MENU_WAITING


async def admin_welcome_save_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        await safe_send(update, "Нужен текст сообщением.")
        return ADMIN_WELCOME_TEXT_WAITING
    if update.message.text.strip() in ADMIN_ESCAPE_LABELS:
        return await admin_escape_conversation(update, context)
    set_welcome_caption_value(update.message.text.strip())
    await safe_send(update, "✅ Текст приветствия сохранён.", reply_markup=admin_welcome_keyboard())
    return ADMIN_WELCOME_MENU_WAITING


async def admin_welcome_save_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.photo:
        set_welcome_photo_value(update.message.photo[-1].file_id)
        await safe_send(update, "✅ Фото приветствия сохранено.", reply_markup=admin_welcome_keyboard())
        return ADMIN_WELCOME_MENU_WAITING
    if update.message and update.message.text and update.message.text.strip() in ADMIN_ESCAPE_LABELS:
        return await admin_escape_conversation(update, context)
    await safe_send(update, "❌ Нужно отправить фото (или «↩️ Админка» / /cancel для выхода).")
    return ADMIN_WELCOME_PHOTO_WAITING


async def admin_welcome_save_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        await safe_send(update, "Нужен текст со списком кнопок.")
        return ADMIN_WELCOME_BUTTONS_WAITING
    body = update.message.text.strip()
    if body in ADMIN_ESCAPE_LABELS:
        return await admin_escape_conversation(update, context)
    if body.lower() == "пусто":
        set_welcome_buttons_raw([])
        await safe_send(update, "✅ Все кнопки убраны.", reply_markup=admin_welcome_keyboard())
        return ADMIN_WELCOME_MENU_WAITING
    raw_lines = body.splitlines()
    parsed = []
    errors = []
    for i, line in enumerate(raw_lines, 1):
        line = line.strip()
        if not line:
            continue
        if "|" not in line:
            errors.append(f"Строка {i}: нет разделителя |")
            continue
        left, right = line.split("|", 1)
        btn_text = left.strip()
        url = right.strip()
        if not btn_text or not url:
            errors.append(f"Строка {i}: пустой текст или ссылка")
            continue
        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("tg://")):
            errors.append(f"Строка {i}: ссылка должна начинаться с http://, https:// или tg://")
            continue
        parsed.append({"text": btn_text, "url": url})
    if errors:
        await safe_send(
            update,
            "❌ Ошибки:\n" + "\n".join(errors) + "\n\nПопробуй ещё раз.",
        )
        return ADMIN_WELCOME_BUTTONS_WAITING
    set_welcome_buttons_raw(parsed)
    await safe_send(
        update,
        f"✅ Сохранено кнопок: {len(parsed)}.",
        reply_markup=admin_welcome_keyboard(),
    )
    return ADMIN_WELCOME_MENU_WAITING


async def check_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_send(update, "Использование: /check RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute(
        """
        SELECT code, discount, used, created_at, used_at, owner_user_id, COALESCE(admin_global, 0)
        FROM promocodes WHERE code = ?
        """,
        (code,),
    )
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Промокод не найден")
        return

    _, discount, used, created_at, used_at, owner_user_id, admin_g = row
    if used:
        await safe_send(update, f"⚠️ Промокод уже использован\nСкидка: -{discount}%\nКогда использован: {used_at}")
        return

    if not int(admin_g or 0) and not is_code_active(created_at):
        await safe_send(update, f"⌛ Промокод просрочен\nСкидка была: -{discount}%\nСоздан: {created_at}")
        return

    kind = "админский (без срока)" if int(admin_g or 0) else "крутилка (12 ч)"
    await safe_send(
        update,
        f"✅ Промокод активен ({kind})\nСкидка: -{discount}%\nСоздан: {created_at}\nВладелец user_id: {owner_user_id}",
    )


async def use_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_send(update, "Использование: /use RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute(
        "SELECT used, created_at, discount, COALESCE(admin_global, 0) FROM promocodes WHERE code = ?",
        (code,),
    )
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Промокод не найден")
        return

    used, created_at, discount, admin_g = row
    if used:
        await safe_send(update, "⚠️ Промокод уже использован")
        return

    if not int(admin_g or 0) and not is_code_active(created_at):
        await safe_send(update, "⌛ Промокод просрочен")
        return

    cursor.execute("UPDATE promocodes SET used = 1, used_at = ? WHERE code = ?", (now_iso(), code))
    conn.commit()
    await safe_send(update, f"✅ Промокод {code} активирован\nСкидка: -{discount}%")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    uid = update.effective_user.id
    if is_admin(uid):
        await safe_send(
            update,
            "❌ Сценарий отменён.\n"
            "Повторно: /cancel, /stop или /admin_stop — выход в корень админки.",
            reply_markup=admin_keyboard(),
        )
    else:
        await safe_send(update, "❌ Действие отменено.", reply_markup=main_keyboard(uid))
    return ConversationHandler.END


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await safe_send(update, "⬅️ Возвращаю в главное меню.", reply_markup=main_keyboard(update.effective_user.id))
    return ConversationHandler.END


ADMIN_ESCAPE_LABELS = frozenset(
    {
        "⬅️ Назад",
        "📣 Рассылки",
        "🛍 Редактор каталога",
        "🎁 Розыгрыши (админ)",
        "👥 Клиенты",
        "🔗 Ссылки и инфо",
        "📊 Аналитика",
        "👋 Экран приветствия",
        "↩️ Админка",
        "⤴️ Админка",
        "⚙️ Админка",
        "📢 Рассылка",
        "🤖 Авто-рассылки",
        "📋 Активные авто-рассылки",
        "🛑 Прервать сценарий",
        "📍 Точки самовывоза",
        "↩️ К розыгрышам",
        "📣 Авторассылка анонса",
    }
)


class _AdminEscapeNavFilter(filters.MessageFilter):
    def filter(self, message):
        if message is None or getattr(message, "text", None) is None:
            return False
        user = getattr(message, "from_user", None)
        if user is None or not is_admin(user.id):
            return False
        return message.text.strip() in ADMIN_ESCAPE_LABELS


async def admin_escape_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return ConversationHandler.END
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    text = update.message.text.strip()
    if text not in ADMIN_ESCAPE_LABELS:
        return ConversationHandler.END
    if text == "📋 Активные авто-рассылки":
        context.user_data.clear()
        return await admin_autopost_list_screen(update, context)
    context.user_data.clear()
    if text == "⬅️ Назад":
        await safe_send(update, "⬅️ Возвращаю в главное меню.", reply_markup=main_keyboard(update.effective_user.id))
        return ConversationHandler.END
    if text in ("🛑 Прервать сценарий", "↩️ Админка", "⤴️ Админка", "⚙️ Админка"):
        await safe_send(
            update,
            "⚙️ *Админка*\n\nВыбери раздел управления.",
            parse_mode="Markdown",
            reply_markup=admin_keyboard(),
        )
        return ConversationHandler.END
    if text == "📣 Рассылки":
        await safe_send(update, "📣 Раздел: рассылки", reply_markup=admin_broadcast_keyboard())
        return ConversationHandler.END
    if text in ("📢 Рассылка", "🤖 Авто-рассылки"):
        await safe_send(update, "📣 Раздел: рассылки", reply_markup=admin_broadcast_keyboard())
        return ConversationHandler.END
    if text == "🛍 Редактор каталога":
        await admin_open_catalog(update, context)
        return ConversationHandler.END
    if text == "🎁 Розыгрыши (админ)":
        await admin_open_giveaways(update, context)
        return ConversationHandler.END
    if text == "📣 Авторассылка анонса":
        await admin_giveaway_autobroadcast_panel(update, context)
        return ConversationHandler.END
    if text == "👥 Клиенты":
        await admin_open_clients(update, context)
        return ConversationHandler.END
    if text == "🔗 Ссылки и инфо":
        await admin_open_links(update, context)
        return ConversationHandler.END
    if text == "📊 Аналитика":
        await admin_open_analytics(update, context)
        return ConversationHandler.END
    if text == "👋 Экран приветствия":
        await safe_send(
            update,
            "👋 Нажми снова «👋 Экран приветствия» в корне админки.",
            reply_markup=admin_keyboard(),
        )
        return ConversationHandler.END
    if text == "📍 Точки самовывоза":
        await admin_pickup_panel(update, context)
        return ConversationHandler.END
    if text == "↩️ К розыгрышам":
        await admin_open_giveaways(update, context)
        return ConversationHandler.END
    return ConversationHandler.END


admin_escape_fallback = MessageHandler(_AdminEscapeNavFilter(), admin_escape_conversation)

ADMIN_CONV_FALLBACKS = [
    CommandHandler("cancel", cancel),
    CommandHandler("stop", cancel),
    CommandHandler("admin_stop", cancel),
    admin_escape_fallback,
]


async def pickup_select_stale_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Если checkout не обработал pickup_select (нет сессии) — отвечаем на callback, иначе крутится загрузка."""
    query = update.callback_query
    if not query:
        return
    try:
        await query.answer(
            "Сессия оформления сброшена. Открой 🛒 Корзина → Оформить заказ снова.",
            show_alert=True,
        )
    except Exception:
        logger.exception("pickup_select_stale_fallback: answer")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if USE_POSTGRES:
        try:
            conn.rollback()
        except Exception:
            logger.exception("Не удалось откатить транзакцию PostgreSQL после ошибки")
    logger.exception("Ошибка в обработке апдейта:", exc_info=context.error)


def main():
    app = Application.builder().token(TOKEN).concurrent_updates(False).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", check_code))
    app.add_handler(CommandHandler("use", use_code))
    app.add_handler(CommandHandler("ratecomment", rate_comment))
    app.add_handler(CommandHandler("cancel", cancel))

    # ВАЖНО: conversation handlers должны регистрироваться раньше обычных MessageHandler.
    # Иначе кнопки внутри админских сценариев (например "📱 Наш VK" в инфо-блоках)
    # будут перехватываться публичными обработчиками и редактирование не сработает.

    app.add_handler(
        CallbackQueryHandler(
            assortment_callback,
            pattern=r"^(category:.+|item:\d+|open_category:.+|assortment_menu|add_to_cart:\d+|cart_open|cart_remove:\d+|cart_clear)$",
        )
    )
    app.add_handler(CallbackQueryHandler(order_status_callback, pattern=r"^order_status:\d+:[a-z_]+$"))
    app.add_handler(CallbackQueryHandler(order_rate_callback, pattern=r"^order_rate:\d+:\d+$"))
    app.add_handler(CallbackQueryHandler(giveaway_results_broadcast_callback, pattern=r"^gwb:\d+$"))
    app.add_handler(CallbackQueryHandler(giveaway_results_skip_callback, pattern=r"^gwx:\d+$"))
    app.add_handler(CallbackQueryHandler(autopost_manage_callback, pattern=r"^autopost_(pause|resume|delete):\d+$"))

    checkout_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_checkout_from_cart, pattern=r"^cart_checkout$"),
            CallbackQueryHandler(start_checkout_buy_now, pattern=r"^buy_now:\d+$"),
        ],
        states={
            ORDER_PROMOCODE_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_promocode_input),
                CallbackQueryHandler(checkout_skip_promocode, pattern=r"^skip_promocode$"),
            ],
            ORDER_CHOICE_WAITING: [
                CallbackQueryHandler(checkout_choose_delivery, pattern=r"^checkout_delivery$"),
                CallbackQueryHandler(checkout_choose_pickup, pattern=r"^checkout_pickup$"),
            ],
            ORDER_DELIVERY_PHONE_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_delivery_phone)],
            ORDER_DELIVERY_USERNAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_delivery_username)],
            ORDER_DELIVERY_ADDRESS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_delivery_address)],
            ORDER_DELIVERY_TIME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_delivery_time)],
            ORDER_PICKUP_POINT_WAITING: [CallbackQueryHandler(checkout_pickup_point, pattern=r"^pickup_select:\d+$")],
            ORDER_PICKUP_PHONE_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_pickup_phone)],
            ORDER_PICKUP_TIME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_pickup_time)],
            ORDER_PICKUP_USERNAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkout_pickup_username)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS, MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main)],
        per_chat=False,
        per_user=True,
    )

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📢 Рассылка$"), admin_broadcast_start)],
        states={ADMIN_BROADCAST_WAITING: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send),
            MessageHandler(filters.PHOTO, admin_broadcast_send),
        ]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    baraholki_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🛒 Ссылка на барахолки$"), admin_baraholki_start)],
        states={ADMIN_BARAHOLKI_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_baraholki_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    projects_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🚀 Ссылка на проекты$"), admin_projects_start)],
        states={ADMIN_PROJECTS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_projects_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    giveaways_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎁 Ссылка на розыгрыши$"), admin_giveaways_start)],
        states={ADMIN_GIVEAWAYS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_giveaways_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    manager_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^💬 Ссылка на менеджера$"), admin_manager_start)],
        states={ADMIN_MANAGER_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_manager_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    add_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^➕ Добавить кнопку$"), admin_add_item_start)],
        states={
            ADMIN_ADD_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_item_category)],
            ADMIN_ADD_ITEM_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_item_name)],
            ADMIN_ADD_ITEM_DESC_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_item_desc)],
            ADMIN_ADD_ITEM_PRICE_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_item_price)],
            ADMIN_ADD_ITEM_IMAGE_WAITING: [MessageHandler(filters.PHOTO, admin_add_item_image)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    rename_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^✏️ Переименовать кнопку$"), admin_rename_item_start)],
        states={
            ADMIN_RENAME_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_category)],
            ADMIN_RENAME_ITEM_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_select)],
            ADMIN_RENAME_ITEM_NEW_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_new_name)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    edit_desc_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📝 Изменить описание$"), admin_edit_desc_start)],
        states={
            ADMIN_EDIT_DESC_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_category)],
            ADMIN_EDIT_DESC_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_select)],
            ADMIN_EDIT_DESC_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_new)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    edit_image_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Изменить фото$"), admin_edit_image_start)],
        states={
            ADMIN_EDIT_IMAGE_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_image_category)],
            ADMIN_EDIT_IMAGE_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_image_select)],
            ADMIN_EDIT_IMAGE_NEW_WAITING: [MessageHandler(filters.PHOTO, admin_edit_image_new)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    category_photo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Фото категорий$"), admin_set_category_photo_start)],
        states={
            ADMIN_SET_CATEGORY_PHOTO_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_set_category_photo_category)],
            ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING: [
                MessageHandler(filters.PHOTO, admin_set_category_photo_image),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_set_category_photo_image_text),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    clear_category_photo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить фото категории$"), admin_clear_category_photo_start)],
        states={
            ADMIN_CLEAR_CATEGORY_PHOTO_CATEGORY_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_clear_category_photo_category)
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    edit_price_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^💰 Изменить цену$"), admin_edit_price_start)],
        states={
            ADMIN_EDIT_PRICE_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_category)],
            ADMIN_EDIT_PRICE_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_select)],
            ADMIN_EDIT_PRICE_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_new)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    delete_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить кнопку$"), admin_delete_item_start)],
        states={
            ADMIN_DELETE_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_item_category)],
            ADMIN_DELETE_ITEM_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_item_select)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    reorder_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^↕️ Порядок кнопок$"), admin_reorder_item_start)],
        states={
            ADMIN_REORDER_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_item_category)],
            ADMIN_REORDER_ITEM_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_item_save)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    info_blocks_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗂 Инфо-блоки$"), admin_info_blocks_start)],
        states={
            ADMIN_INFO_BLOCK_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_info_blocks_select)],
            ADMIN_INFO_BLOCK_ACTION_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_info_blocks_action)],
            ADMIN_INFO_BLOCK_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_info_blocks_save_text)],
            ADMIN_INFO_BLOCK_PHOTO_WAITING: [
                MessageHandler(filters.PHOTO, admin_info_blocks_save_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_info_blocks_save_photo),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS, MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main)],
    )

    referral_hub_photo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Фото: Получить халяву$"), admin_referral_hub_photo_start)],
        states={
            ADMIN_REFERRAL_HUB_PHOTO_WAITING: [
                MessageHandler(filters.PHOTO, admin_referral_hub_photo_save),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_referral_hub_photo_save),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    add_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^➕ Добавить точку$"), admin_add_pickup_start)],
        states={ADMIN_ADD_PICKUP_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_pickup_name)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    rename_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^✏️ Переименовать точку$"), admin_rename_pickup_start)],
        states={
            ADMIN_RENAME_PICKUP_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_pickup_select)],
            ADMIN_RENAME_PICKUP_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_pickup_new)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    delete_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить точку$"), admin_delete_pickup_start)],
        states={ADMIN_DELETE_PICKUP_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_pickup_select)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    reorder_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^↕️ Порядок точек$"), admin_reorder_pickup_start)],
        states={ADMIN_REORDER_PICKUP_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_pickup_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    ref_giveaway_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎁 Реф. розыгрыш$"), admin_ref_giveaway_start)],
        states={ADMIN_REF_GIVEAWAY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ref_giveaway_pick)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    create_giveaway_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎯 Создать розыгрыш$"), admin_create_giveaway_start)],
        states={
            ADMIN_GIVEAWAY_CREATE_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_giveaway_text)],
            ADMIN_GIVEAWAY_CREATE_DESC_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_giveaway_desc)],
            ADMIN_GIVEAWAY_CREATE_IMAGE_WAITING: [
                MessageHandler(filters.PHOTO, admin_create_giveaway_image),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_giveaway_image),
            ],
            ADMIN_GIVEAWAY_CREATE_BUTTONS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_giveaway_buttons)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    finish_giveaway_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🏁 Завершить розыгрыш$"), admin_finish_giveaway_start)],
        states={ADMIN_GIVEAWAY_FINISH_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_finish_giveaway_pick)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    giveaway_autobroadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^⚙ Раз в сутки$"), admin_giveaway_autobroadcast_per_day_start)],
        states={
            ADMIN_GIVEAWAY_AUTOBROADCAST_PER_DAY_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_giveaway_autobroadcast_per_day_save),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    autopost_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🤖 Авто-рассылки$"), admin_autopost_start)],
        states={
            ADMIN_AUTOPOST_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_autopost_text)],
            ADMIN_AUTOPOST_PHOTO_WAITING: [
                MessageHandler(filters.PHOTO, admin_autopost_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_autopost_photo),
            ],
            ADMIN_AUTOPOST_BUTTON_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_autopost_button)],
            ADMIN_AUTOPOST_INTERVAL_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_autopost_interval)],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    blacklist_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🚫 Черный список$"), admin_blacklist_start)],
        states={ADMIN_BLACKLIST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_blacklist_manage)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    category_discount_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🏷 Акции категорий$"), admin_category_discount_start)],
        states={ADMIN_CATEGORY_DISCOUNT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_category_discount_save)]},
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    create_promo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎫 Создать промокод$"), admin_create_promo_start)],
        states={
            ADMIN_CREATE_PROMO_CODE_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_promo_code_step),
            ],
            ADMIN_CREATE_PROMO_DISCOUNT_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_create_promo_discount_step),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    welcome_screen_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^👋 Экран приветствия$"), admin_welcome_open)],
        states={
            ADMIN_WELCOME_MENU_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_welcome_menu),
            ],
            ADMIN_WELCOME_TEXT_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_welcome_save_text),
            ],
            ADMIN_WELCOME_PHOTO_WAITING: [
                MessageHandler(filters.PHOTO, admin_welcome_save_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_welcome_save_photo),
            ],
            ADMIN_WELCOME_BUTTONS_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_welcome_save_buttons),
            ],
        },
        fallbacks=[*ADMIN_CONV_FALLBACKS],
    )

    app.add_handler(checkout_conv)
    app.add_handler(CallbackQueryHandler(pickup_select_stale_fallback, pattern=r"^pickup_select:\d+$"))
    app.add_handler(info_blocks_conv)
    app.add_handler(referral_hub_photo_conv)
    app.add_handler(create_promo_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(baraholki_conv)
    app.add_handler(projects_conv)
    app.add_handler(giveaways_conv)
    app.add_handler(manager_conv)
    app.add_handler(add_item_conv)
    app.add_handler(rename_item_conv)
    app.add_handler(edit_desc_conv)
    app.add_handler(edit_image_conv)
    app.add_handler(category_photo_conv)
    app.add_handler(clear_category_photo_conv)
    app.add_handler(edit_price_conv)
    app.add_handler(delete_item_conv)
    app.add_handler(reorder_item_conv)
    app.add_handler(add_pickup_conv)
    app.add_handler(rename_pickup_conv)
    app.add_handler(delete_pickup_conv)
    app.add_handler(reorder_pickup_conv)
    app.add_handler(ref_giveaway_conv)
    app.add_handler(create_giveaway_conv)
    app.add_handler(finish_giveaway_conv)
    app.add_handler(giveaway_autobroadcast_conv)
    app.add_handler(autopost_conv)
    app.add_handler(blacklist_conv)
    app.add_handler(category_discount_conv)
    app.add_handler(welcome_screen_conv)

    # Срабатывают, когда пользователь не в активном ConversationHandler (внутри сценария — fallbacks).
    app.add_handler(CommandHandler("stop", cancel))
    app.add_handler(CommandHandler("admin_stop", cancel))

    app.add_handler(MessageHandler(filters.Regex(r"^🛍 Ассортимент$"), assortment))
    app.add_handler(MessageHandler(filters.Regex(r"^🛒 Корзина$"), show_cart))
    app.add_handler(MessageHandler(filters.Regex(r"^📦 История заказов$"), show_order_history))
    app.add_handler(MessageHandler(filters.Regex(r"^🎰 Крутить скидку$"), spin))
    app.add_handler(MessageHandler(filters.Regex(r"^🎁 Получить халяву$"), my_referrals))
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Личный кабинет$"), referral_personal_cabinet))
    app.add_handler(MessageHandler(filters.Regex(r"^💬 Менеджер$"), manager))
    app.add_handler(MessageHandler(filters.Regex(r"^📱 Наш VK$"), vk))
    app.add_handler(MessageHandler(filters.Regex(r"^🛒 Наши барахолки$"), baraholki))
    app.add_handler(MessageHandler(filters.Regex(r"^🚀 Наши проекты$"), projects))
    app.add_handler(MessageHandler(filters.Regex(r"^🎁 Розыгрыши$"), giveaways))
    app.add_handler(MessageHandler(filters.Regex(r"^⚙️ Админка$"), admin_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^↩️ Админка$"), admin_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^🛍 Редактор каталога$"), admin_open_catalog))
    app.add_handler(MessageHandler(filters.Regex(r"^📣 Рассылки$"), admin_open_broadcasts))
    app.add_handler(MessageHandler(filters.Regex(r"^📋 Активные авто-рассылки$"), admin_autopost_list_screen))
    app.add_handler(MessageHandler(filters.Regex(r"^🎁 Розыгрыши \(админ\)$"), admin_open_giveaways))
    app.add_handler(MessageHandler(filters.Regex(r"^📣 Авторассылка анонса$"), admin_giveaway_autobroadcast_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^▶️ Включить авторассылку$"), admin_giveaway_autobroadcast_enable))
    app.add_handler(MessageHandler(filters.Regex(r"^⏹ Выключить авторассылку$"), admin_giveaway_autobroadcast_disable))
    app.add_handler(
        MessageHandler(filters.Regex(r"^📤 Разослать анонс сейчас \(1 раз\)$"), admin_giveaway_autobroadcast_once_now)
    )
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Статус авторассылки$"), admin_giveaway_autobroadcast_status))
    # Вне ConversationHandler fallback эта кнопка иначе ни к чему не привязана.
    app.add_handler(MessageHandler(filters.Regex(r"^\u21a9\uFE0F? К розыгрышам$"), admin_open_giveaways))
    app.add_handler(MessageHandler(filters.Regex(r"^👥 Клиенты$"), admin_open_clients))
    app.add_handler(MessageHandler(filters.Regex(r"^🔗 Ссылки и инфо$"), admin_open_links))
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Аналитика$"), admin_open_analytics))
    app.add_handler(MessageHandler(filters.Regex(r"^📍 Точки самовывоза$"), admin_pickup_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Статистика$"), admin_stats))
    app.add_handler(MessageHandler(filters.Regex(r"^📈 Аналитика PRO$"), admin_advanced_stats))
    app.add_handler(MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main))

    if app.job_queue:
        app.job_queue.run_repeating(process_auto_posts, interval=60, first=20)

    app.add_error_handler(error_handler)

    # В логах Railway «верх» часто обрезан — дублируем режим БД рядом с финальным сообщением.
    if USE_POSTGRES:
        logger.info("Старт бота: режим БД = PostgreSQL")
        print("Старт бота: режим БД = PostgreSQL", flush=True)
    else:
        _p = os.path.abspath(DB_PATH)
        logger.warning("Старт бота: режим БД = SQLite (%s)", _p)
        print(f"Старт бота: режим БД = SQLite ({_p})", flush=True)

    print("RNDM SHOP bot запущен...", flush=True)
    app.run_polling(poll_interval=1, timeout=10, drop_pending_updates=True)


if __name__ == "__main__":
    main() 