import os
import re
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

DB_PATH = "rndm.db"

DEFAULT_BARAHOLKI_URL = "https://t.me/your_channel/1"
DEFAULT_PROJECTS_URL = "https://t.me/your_channel/2"
DEFAULT_GIVEAWAYS_URL = "https://t.me/your_channel/3"
DEFAULT_VK_URL = "https://vk.ru/rndm196"

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

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now().isoformat()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def ensure_column(table_name: str, column_name: str, column_def: str) -> None:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()


cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_seen TEXT,
        last_spin TEXT,
        referred_by INTEGER
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS promocodes (
        code TEXT PRIMARY KEY,
        discount INTEGER NOT NULL,
        used INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        used_at TEXT,
        owner_user_id INTEGER
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """
)

cursor.execute(
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
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS pickup_points (
        pickup_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        sort_order INTEGER NOT NULL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS cart_items (
        user_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (user_id, item_id)
    )
    """
)

cursor.execute(
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
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS referrals (
        inviter_id INTEGER NOT NULL,
        invited_id INTEGER NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        PRIMARY KEY (inviter_id, invited_id)
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS referral_winners (
        winner_id INTEGER NOT NULL,
        invites_count INTEGER NOT NULL DEFAULT 0,
        selected_at TEXT NOT NULL,
        selected_by INTEGER NOT NULL
    )
    """
)

conn.commit()

ensure_column("items", "price", "INTEGER NOT NULL DEFAULT 0")
ensure_column("users", "referred_by", "INTEGER")


def set_setting(key: str, value: str) -> None:
    cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def get_setting(key: str, default: str = "") -> str:
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else default


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
    "giveaways": "🎁 Розыгрыши\n\nАктуальная информация по нашим розыгрышам.",
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
    for key, value in INFO_BLOCK_LABELS.items():
        if value == label:
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
    url = get_info_block_url(block_key)
    if not url:
        return None

    button_label = INFO_BLOCK_BUTTON_LABELS.get(block_key, "🔗 Открыть")
    return InlineKeyboardMarkup([[InlineKeyboardButton(button_label, url=url)]])


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
    return True


def get_referrals_count(user_id: int) -> int:
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE inviter_id = ?", (user_id,))
    return cursor.fetchone()[0]


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
        GROUP BY r.inviter_id
        ORDER BY invites_count DESC, r.inviter_id ASC
        LIMIT ?
        """,
        (limit,),
    )
    return cursor.fetchall()


def build_ref_link(bot_username: str, user_id: int) -> str:
    if not bot_username:
        return ""
    return f"https://t.me/{bot_username}?start={user_id}"


def my_referrals_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["⬅️ Назад"]], resize_keyboard=True)


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
    return 10 if random.random() < 0.05 else 5


def is_code_active(created_at: str) -> bool:
    created = datetime.fromisoformat(created_at)
    return datetime.now() - created <= timedelta(hours=12)


def slugify_name(name: str) -> str:
    allowed = string.ascii_lowercase + string.digits + "_"
    base = name.lower().strip().replace(" ", "_")
    cleaned = "".join(ch for ch in base if ch in allowed)
    return cleaned or "item"


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
    for key, value in CATEGORY_LABELS.items():
        if value == label:
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
    cursor.execute(
        """
        INSERT INTO items (item_key, category_key, label, description, image, sort_order, price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (item_key, category_key, label, description, image, sort_order, price),
    )
    conn.commit()
    return cursor.lastrowid


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
    cursor.execute(
        """
        INSERT INTO cart_items (user_id, item_id, quantity)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, item_id) DO UPDATE SET quantity = quantity + excluded.quantity
        """,
        (user_id, item_id, quantity),
    )
    conn.commit()


def remove_from_cart(user_id: int, item_id: int) -> None:
    cursor.execute("DELETE FROM cart_items WHERE user_id = ? AND item_id = ?", (user_id, item_id))
    conn.commit()


def clear_cart(user_id: int) -> None:
    cursor.execute("DELETE FROM cart_items WHERE user_id = ?", (user_id,))
    conn.commit()


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
    return cursor.fetchall()


def cart_total(user_id: int) -> int:
    rows = get_cart(user_id)
    return sum(price * quantity for _, quantity, _, price, _ in rows)


def cart_text(user_id: int) -> str:
    rows = get_cart(user_id)
    if not rows:
        return "🛒 *Корзина пока пустая.*"

    lines = ["🛒 *ТВОЯ КОРЗИНА*\n"]
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
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬 Написать менеджеру", url=get_setting("manager_url", DEFAULT_MANAGER_URL))]]
    )


def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        ["🛍 Ассортимент", "🛒 Корзина"],
        ["📦 История заказов", "🎰 Крутить скидку"],
        ["👥 Пригласить друзей"],
        ["🛒 Наши барахолки", "🚀 Наши проекты"],
        ["🎁 Розыгрыши", "💬 Менеджер"],
        ["📱 Наш VK"],
    ]
    if is_admin(user_id):
        keyboard.append(["⚙️ Админка"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["➕ Добавить кнопку", "✏️ Переименовать кнопку"],
            ["📝 Изменить описание", "🖼 Изменить фото"],
            ["💰 Изменить цену", "🗑 Удалить кнопку"],
            ["🖼 Фото категорий", "🗂 Инфо-блоки"],
            ["🗑 Удалить фото категории"],
            ["📍 Точки самовывоза", "↕️ Порядок кнопок"],
            ["📢 Рассылка", "📊 Статистика"],
            ["🎁 Реф. розыгрыш"],
            ["💬 Ссылка на менеджера", "🛒 Ссылка на барахолки"],
            ["🚀 Ссылка на проекты", "🎁 Ссылка на розыгрыши"],
            ["⬅️ Назад"],
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
    for item_id, _, _, label, _, _, _, price in get_items_by_category(category_key):
        suffix = f" — {price} ₽" if price > 0 else ""
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
    rows = [[InlineKeyboardButton(name, callback_data=f"pickup_select:{pickup_id}")] for pickup_id, name, _ in get_pickup_points()]
    return InlineKeyboardMarkup(rows)


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
    is_new_user = save_user(user)
    if is_new_user and context.args:
        register_referral_if_valid(user.id, context.args[0])
    await safe_send(
        update,
        "🔥 *Добро пожаловать в RNDM SHOP!*\n\nВыбирай нужный раздел ниже 👇",
        parse_mode="Markdown",
        reply_markup=main_keyboard(user.id),
    )


async def assortment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🛍 *АССОРТИМЕНТ RNDM SHOP*\n\nВыбирай категорию ниже 👇",
        parse_mode="Markdown",
        reply_markup=category_menu_keyboard(),
    )


async def show_item(query, item_id: int):
    item = get_item(item_id)
    if not item:
        await query.message.reply_text("❌ Позиция не найдена.")
        return

    item_id, _, category_key, label, description, image, _, price = item
    caption = f"*{label}*\n\n{description}\n\n💰 *Цена:* {format_price(price)}"

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


async def assortment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    user = update.effective_user

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
        item_id = int(query.data.split(":", 1)[1])
        add_to_cart(user.id, item_id)
        await query.message.reply_text("✅ Товар добавлен в корзину.")
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
        return [{
            "item_id": item[0],
            "label": item[3],
            "price": item[7],
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
        SELECT code, discount, used, created_at, used_at, owner_user_id
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

    promo_code, discount, used, created_at, used_at, owner_user_id = promo

    if owner_user_id and owner_user_id != user_id:
        return False, "⛔ Этот промокод принадлежит другому пользователю.", None

    if used:
        return False, "⚠️ Этот промокод уже использован.", None

    if not is_code_active(created_at):
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
    promocode = context.user_data.get("checkout_promocode")
    discount_percent = context.user_data.get("checkout_discount_percent", 0)

    items_text = build_items_text(items)
    total_sum = build_total_sum(items)
    final_sum, discount_amount = apply_discount_to_total(total_sum, discount_percent)

    cursor.execute(
        """
        INSERT INTO orders (
            user_id, username, first_name, order_type, pickup_point, phone,
            contact_username, address, delivery_time, items_text, total_sum, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        ),
    )
    conn.commit()
    order_id = cursor.lastrowid

    username_line = f"@{user.username}" if user.username else "нет username"
    total_line = format_price(total_sum) if total_sum > 0 else "цена уточняется"
    final_line = format_price(final_sum) if final_sum > 0 else "цена уточняется"

    manager_text = (
        f"🆕 НОВЫЙ ЗАКАЗ #{order_id}\n\n"
        f"Тип: {'Доставка' if order_type == 'delivery' else 'Самовывоз'}\n"
        f"Клиент: {user.first_name or '-'}\n"
        f"Username: {username_line}\n"
        f"User ID: {user.id}\n"
        f"Телефон: {phone}\n"
        f"Контактный username: {contact_username}\n"
    )

    if order_type == "delivery":
        manager_text += f"Адрес: {address}\n"
    else:
        manager_text += f"Точка самовывоза: {pickup_point}\n"

    if promocode:
        manager_text += f"Промокод: {promocode}\n"
        manager_text += f"Скидка: -{discount_percent}%\n"
        manager_text += f"Размер скидки: {discount_amount} ₽\n"

    manager_text += (
        f"Время: {delivery_time}\n\n"
        f"Товары:\n{items_text}\n\n"
        f"Сумма до скидки: {total_line}\n"
        f"Итого к оплате: {final_line}\n"
        f"Время заказа: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
    )

    await context.bot.send_message(chat_id=ORDER_GROUP_ID, text=manager_text)
    return order_id


async def begin_checkout(query, context: ContextTypes.DEFAULT_TYPE, buy_now_item_id=None):
    items = collect_checkout_items(query.from_user.id, buy_now_item_id)
    if not items:
        await query.message.reply_text("🛒 Корзина пустая.")
        return ConversationHandler.END

    clear_order_context(context)
    context.user_data["checkout_buy_now_item_id"] = buy_now_item_id

    items_text = build_items_text(items)
    total_sum = build_total_sum(items)
    total_line = format_price(total_sum) if total_sum > 0 else "цена уточняется"

    context.user_data["checkout_promocode"] = None
    context.user_data["checkout_discount_percent"] = 0
    context.user_data["checkout_discount_amount"] = 0
    context.user_data["checkout_total_before_discount"] = total_sum
    context.user_data["checkout_total_after_discount"] = total_sum

    await query.message.reply_text(
        f"🧾 ОФОРМЛЕНИЕ ЗАКАЗА\n\n"
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

    items = collect_checkout_items(user.id, context.user_data.get("checkout_buy_now_item_id"))
    total_sum = build_total_sum(items)
    final_sum, discount_amount = apply_discount_to_total(total_sum, discount)

    context.user_data["checkout_promocode"] = code
    context.user_data["checkout_discount_percent"] = discount
    context.user_data["checkout_discount_amount"] = discount_amount
    context.user_data["checkout_total_before_discount"] = total_sum
    context.user_data["checkout_total_after_discount"] = final_sum

    await safe_send(
        update,
        f"✅ Промокод применён: *{code}*\n"
        f"Скидка: *-{discount}%*\n"
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
    context.user_data["checkout_discount_percent"] = 0
    context.user_data["checkout_discount_amount"] = 0

    items = collect_checkout_items(query.from_user.id, context.user_data.get("checkout_buy_now_item_id"))
    total_sum = build_total_sum(items)
    context.user_data["checkout_total_before_discount"] = total_sum
    context.user_data["checkout_total_after_discount"] = total_sum

    await query.message.reply_text(
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
    await query.message.reply_text("1) Ваш телефон в формате +7XXXXXXXXXX")
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

    promocode = context.user_data.get("checkout_promocode")
    if promocode:
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
        await query.message.reply_text("❌ Нет доступных точек самовывоза.")
        clear_order_context(context)
        return ConversationHandler.END

    await query.message.reply_text("Выбери точку самовывоза:", reply_markup=pickup_points_keyboard())
    return ORDER_PICKUP_POINT_WAITING


async def checkout_pickup_point(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    pickup_id = int(query.data.split(":", 1)[1])
    pickup_point = get_pickup_point(pickup_id)
    if not pickup_point:
        await query.message.reply_text("❌ Точка не найдена.")
        return ORDER_PICKUP_POINT_WAITING

    context.user_data["checkout_pickup_point"] = pickup_point[1]
    await query.message.reply_text("1) Ваш номер телефона в формате +7XXXXXXXXXX")
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

    promocode = context.user_data.get("checkout_promocode")
    if promocode:
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
    await open_info_block(update, "giveaways")


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
    invites_count = get_referrals_count(user.id)
    ref_link = build_ref_link(context.bot.username, user.id)

    text = (
        "👥 *Твоя реферальная программа*\n\n"
        "Приглашай друзей в бота по своей ссылке и участвуй в розыгрышах.\n\n"
        f"Твои приглашения: *{invites_count}*\n\n"
    )
    if ref_link:
        text += f"Твоя ссылка:\n`{ref_link}`"
    else:
        text += "Ссылка временно недоступна, попробуй позже."

    await safe_send(
        update,
        text,
        parse_mode="Markdown",
        reply_markup=my_referrals_keyboard(),
    )


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
        "⚙️ *Админка*\n\nУправление товарами, ценами, точками самовывоза и настройками.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


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
    cursor.execute("SELECT COUNT(DISTINCT inviter_id) FROM referrals")
    inviters_count = cursor.fetchone()[0]

    await safe_send(
        update,
        f"📊 *Статистика*\n\n"
        f"Пользователей: *{users_count}*\n"
        f"Товарных кнопок: *{items_count}*\n"
        f"Точек самовывоза: *{pickup_count}*\n"
        f"Реф. приглашений: *{referrals_count}*\n"
        f"Активных рефереров: *{inviters_count}*\n"
        f"Выдано промокодов: *{codes_count}*\n"
        f"Использовано промокодов: *{used_count}*\n"
        f"Заказов: *{orders_count}*",
        parse_mode="Markdown",
    )


async def admin_ref_giveaway_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    top_rows = get_referral_top(limit=20)
    if not top_rows:
        await safe_send(update, "Пока нет приглашений.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    lines = ["🎁 *Реферальный рейтинг (топ-20)*\n"]
    for idx, (inviter_id, username, first_name, invites_count) in enumerate(top_rows, start=1):
        uname = f"@{username}" if username else "-"
        name = first_name or "-"
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
        uname = f"@{username}" if username else "-"
        result_lines.append(f"• {first_name or '-'} ({uname}) — ID {winner_id}, приглашений: {invites_count}")

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
    await safe_send(update, "📢 Отправь текст рассылки одним сообщением. /cancel для отмены")
    return ADMIN_BROADCAST_WAITING


async def admin_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=text)
            sent += 1
        except Exception:
            failed += 1

    await safe_send(
        update,
        f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


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
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


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


async def check_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_send(update, "Использование: /check RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute(
        "SELECT code, discount, used, created_at, used_at, owner_user_id FROM promocodes WHERE code = ?",
        (code,),
    )
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Промокод не найден")
        return

    _, discount, used, created_at, used_at, owner_user_id = row
    if used:
        await safe_send(update, f"⚠️ Промокод уже использован\nСкидка: -{discount}%\nКогда использован: {used_at}")
        return

    if not is_code_active(created_at):
        await safe_send(update, f"⌛ Промокод просрочен\nСкидка была: -{discount}%\nСоздан: {created_at}")
        return

    await safe_send(update, f"✅ Промокод активен\nСкидка: -{discount}%\nСоздан: {created_at}\nВладелец user_id: {owner_user_id}")


async def use_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_send(update, "Использование: /use RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute("SELECT used, created_at, discount FROM promocodes WHERE code = ?", (code,))
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Промокод не найден")
        return

    used, created_at, discount = row
    if used:
        await safe_send(update, "⚠️ Промокод уже использован")
        return

    if not is_code_active(created_at):
        await safe_send(update, "⌛ Промокод просрочен")
        return

    cursor.execute("UPDATE promocodes SET used = 1, used_at = ? WHERE code = ?", (now_iso(), code))
    conn.commit()
    await safe_send(update, f"✅ Промокод {code} активирован\nСкидка: -{discount}%")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await safe_send(update, "❌ Действие отменено.", reply_markup=main_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await safe_send(update, "⬅️ Возвращаю в главное меню.", reply_markup=main_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Ошибка в обработке апдейта:", exc_info=context.error)


def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", check_code))
    app.add_handler(CommandHandler("use", use_code))
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
        fallbacks=[CommandHandler("cancel", cancel), MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main)],
    )

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📢 Рассылка$"), admin_broadcast_start)],
        states={ADMIN_BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    baraholki_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🛒 Ссылка на барахолки$"), admin_baraholki_start)],
        states={ADMIN_BARAHOLKI_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_baraholki_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    projects_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🚀 Ссылка на проекты$"), admin_projects_start)],
        states={ADMIN_PROJECTS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_projects_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    giveaways_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎁 Ссылка на розыгрыши$"), admin_giveaways_start)],
        states={ADMIN_GIVEAWAYS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_giveaways_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    manager_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^💬 Ссылка на менеджера$"), admin_manager_start)],
        states={ADMIN_MANAGER_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_manager_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
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
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    rename_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^✏️ Переименовать кнопку$"), admin_rename_item_start)],
        states={
            ADMIN_RENAME_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_category)],
            ADMIN_RENAME_ITEM_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_select)],
            ADMIN_RENAME_ITEM_NEW_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_item_new_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    edit_desc_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📝 Изменить описание$"), admin_edit_desc_start)],
        states={
            ADMIN_EDIT_DESC_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_category)],
            ADMIN_EDIT_DESC_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_select)],
            ADMIN_EDIT_DESC_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_desc_new)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    edit_image_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Изменить фото$"), admin_edit_image_start)],
        states={
            ADMIN_EDIT_IMAGE_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_image_category)],
            ADMIN_EDIT_IMAGE_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_image_select)],
            ADMIN_EDIT_IMAGE_NEW_WAITING: [MessageHandler(filters.PHOTO, admin_edit_image_new)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    category_photo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Фото категорий$"), admin_set_category_photo_start)],
        states={
            ADMIN_SET_CATEGORY_PHOTO_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_set_category_photo_category)],
            ADMIN_SET_CATEGORY_PHOTO_IMAGE_WAITING: [MessageHandler(filters.PHOTO, admin_set_category_photo_image)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    clear_category_photo_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить фото категории$"), admin_clear_category_photo_start)],
        states={
            ADMIN_CLEAR_CATEGORY_PHOTO_CATEGORY_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_clear_category_photo_category)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    edit_price_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^💰 Изменить цену$"), admin_edit_price_start)],
        states={
            ADMIN_EDIT_PRICE_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_category)],
            ADMIN_EDIT_PRICE_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_select)],
            ADMIN_EDIT_PRICE_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_price_new)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    delete_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить кнопку$"), admin_delete_item_start)],
        states={
            ADMIN_DELETE_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_item_category)],
            ADMIN_DELETE_ITEM_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_item_select)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    reorder_item_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^↕️ Порядок кнопок$"), admin_reorder_item_start)],
        states={
            ADMIN_REORDER_ITEM_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_item_category)],
            ADMIN_REORDER_ITEM_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_item_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
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
        fallbacks=[CommandHandler("cancel", cancel), MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main)],
    )

    add_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^➕ Добавить точку$"), admin_add_pickup_start)],
        states={ADMIN_ADD_PICKUP_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_pickup_name)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    rename_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^✏️ Переименовать точку$"), admin_rename_pickup_start)],
        states={
            ADMIN_RENAME_PICKUP_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_pickup_select)],
            ADMIN_RENAME_PICKUP_NEW_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_pickup_new)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    delete_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить точку$"), admin_delete_pickup_start)],
        states={ADMIN_DELETE_PICKUP_SELECT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_pickup_select)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    reorder_pickup_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^↕️ Порядок точек$"), admin_reorder_pickup_start)],
        states={ADMIN_REORDER_PICKUP_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_pickup_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    ref_giveaway_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎁 Реф. розыгрыш$"), admin_ref_giveaway_start)],
        states={ADMIN_REF_GIVEAWAY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ref_giveaway_pick)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(checkout_conv)
    app.add_handler(info_blocks_conv)
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

    app.add_handler(MessageHandler(filters.Regex(r"^🛍 Ассортимент$"), assortment))
    app.add_handler(MessageHandler(filters.Regex(r"^🛒 Корзина$"), show_cart))
    app.add_handler(MessageHandler(filters.Regex(r"^📦 История заказов$"), show_order_history))
    app.add_handler(MessageHandler(filters.Regex(r"^🎰 Крутить скидку$"), spin))
    app.add_handler(MessageHandler(filters.Regex(r"^👥 Пригласить друзей$"), my_referrals))
    app.add_handler(MessageHandler(filters.Regex(r"^💬 Менеджер$"), manager))
    app.add_handler(MessageHandler(filters.Regex(r"^📱 Наш VK$"), vk))
    app.add_handler(MessageHandler(filters.Regex(r"^🛒 Наши барахолки$"), baraholki))
    app.add_handler(MessageHandler(filters.Regex(r"^🚀 Наши проекты$"), projects))
    app.add_handler(MessageHandler(filters.Regex(r"^🎁 Розыгрыши$"), giveaways))
    app.add_handler(MessageHandler(filters.Regex(r"^⚙️ Админка$"), admin_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^📍 Точки самовывоза$"), admin_pickup_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Статистика$"), admin_stats))
    app.add_handler(MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main))

    app.add_error_handler(error_handler)

    print("RNDM SHOP bot запущен...")
    app.run_polling(poll_interval=1, timeout=10, drop_pending_updates=True)


if __name__ == "__main__":
    main()