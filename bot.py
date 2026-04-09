import os
import random
import sqlite3
import string
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
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

DB_PATH = "rndm.db"

DEFAULT_BARAHOLKI_URL = "https://t.me/your_channel/1"
DEFAULT_PROJECTS_URL = "https://t.me/your_channel/2"
DEFAULT_GIVEAWAYS_URL = "https://t.me/your_channel/3"
DEFAULT_MANAGER_URL = "tg://user?id=8423978061"

DEFAULT_CATEGORIES = [
    {
        "category_key": "devices",
        "label": "⚡ УСТРОЙСТВА",
        "text": """⚡ *УСТРОЙСТВА*

Выбирай нужный бренд ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=USTROYSTVA",
        "sort_order": 1,
    },
    {
        "category_key": "liquids",
        "label": "💧 ЖИДКОСТИ",
        "text": """💧 *ЖИДКОСТИ*

Выбирай нужный бренд ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=ZHIDKOSTI",
        "sort_order": 2,
    },
    {
        "category_key": "disposables",
        "label": "🔥 ОДНОРАЗКИ",
        "text": """🔥 *ОДНОРАЗКИ*

Выбирай нужный бренд ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=ODNORAZKI",
        "sort_order": 3,
    },
    {
        "category_key": "plates",
        "label": "🧊 ШАЙБЫ/ПЛАСТИНКИ",
        "text": """🧊 *ШАЙБЫ/ПЛАСТИНКИ*

Выбирай нужный раздел ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=PLATES",
        "sort_order": 4,
    },
    {
        "category_key": "supplies",
        "label": "🛠 РАСХОДНИКИ",
        "text": """🛠 *РАСХОДНИКИ*

Выбирай нужный раздел ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=RASHODNIKI",
        "sort_order": 5,
    },
    {
        "category_key": "sale",
        "label": "💸 СЛИВ/СКИДКИ",
        "text": """💸 *СЛИВ/СКИДКИ*

Выбирай нужный раздел ниже 👇""",
        "image": "https://via.placeholder.com/1200x800.png?text=SALE",
        "sort_order": 6,
    },
]

DEFAULT_SUBCATEGORIES = [
    {
        "subcategory_key": "xros",
        "parent_category_key": "devices",
        "label": "XROS",
        "text": """⚡ *XROS*

Популярные под-устройства линейки XROS.
Уточняй наличие, цвет и цену у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=XROS",
        "sort_order": 1,
    },
    {
        "subcategory_key": "aegis",
        "parent_category_key": "devices",
        "label": "AEGIS",
        "text": """⚡ *AEGIS*

Надёжные устройства AEGIS.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=AEGIS",
        "sort_order": 2,
    },
    {
        "subcategory_key": "pasito",
        "parent_category_key": "devices",
        "label": "PASITO",
        "text": """⚡ *PASITO*

Устройства линейки PASITO.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=PASITO",
        "sort_order": 3,
    },
    {
        "subcategory_key": "duall",
        "parent_category_key": "liquids",
        "label": "DUALL",
        "text": """💧 *DUALL*

Жидкости DUALL.
Уточняй вкусы и наличие у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=DUALL",
        "sort_order": 1,
    },
    {
        "subcategory_key": "trava",
        "parent_category_key": "liquids",
        "label": "TRAVA",
        "text": """💧 *TRAVA*

Жидкости TRAVA.
Уточняй вкусы и наличие у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=TRAVA",
        "sort_order": 2,
    },
    {
        "subcategory_key": "skala",
        "parent_category_key": "liquids",
        "label": "SKALA",
        "text": """💧 *SKALA*

Жидкости SKALA.
Уточняй вкусы и наличие у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=SKALA",
        "sort_order": 3,
    },
    {
        "subcategory_key": "vozol",
        "parent_category_key": "disposables",
        "label": "VOZOL",
        "text": """🔥 *VOZOL*

Одноразовые устройства VOZOL.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=VOZOL",
        "sort_order": 1,
    },
    {
        "subcategory_key": "waka",
        "parent_category_key": "disposables",
        "label": "WAKA",
        "text": """🔥 *WAKA*

Одноразовые устройства WAKA.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=WAKA",
        "sort_order": 2,
    },
    {
        "subcategory_key": "nancy",
        "parent_category_key": "disposables",
        "label": "NANCY",
        "text": """🔥 *NANCY*

Одноразовые устройства NANCY.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=NANCY",
        "sort_order": 3,
    },
    {
        "subcategory_key": "dlta",
        "parent_category_key": "plates",
        "label": "DLTA",
        "text": """🧊 *DLTA*

Шайбы/пластинки DLTA.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=DLTA",
        "sort_order": 1,
    },
    {
        "subcategory_key": "mad_plate",
        "parent_category_key": "plates",
        "label": "MAD",
        "text": """🧊 *MAD*

Шайбы/пластинки MAD.
Уточняй наличие и цены у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=MAD",
        "sort_order": 2,
    },
    {
        "subcategory_key": "cartridges",
        "parent_category_key": "supplies",
        "label": "КАРТРИДЖИ",
        "text": """🛠 *КАРТРИДЖИ*

Уточняй наличие и совместимость у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=CARTRIDGES",
        "sort_order": 1,
    },
    {
        "subcategory_key": "coils",
        "parent_category_key": "supplies",
        "label": "ИСПАРИТЕЛИ",
        "text": """🛠 *ИСПАРИТЕЛИ*

Уточняй наличие и совместимость у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=COILS",
        "sort_order": 2,
    },
    {
        "subcategory_key": "sale_devices",
        "parent_category_key": "sale",
        "label": "СКИДКИ НА УСТРОЙСТВА",
        "text": """💸 *СКИДКИ НА УСТРОЙСТВА*

Актуальные скидки уточняй у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=SALE+DEVICES",
        "sort_order": 1,
    },
    {
        "subcategory_key": "sale_disposables",
        "parent_category_key": "sale",
        "label": "СКИДКИ НА ОДНОРАЗКИ",
        "text": """💸 *СКИДКИ НА ОДНОРАЗКИ*

Актуальные скидки уточняй у менеджера.""",
        "image": "https://via.placeholder.com/1200x800.png?text=SALE+DISPOSABLES",
        "sort_order": 2,
    },
]

(
    ADMIN_BROADCAST_WAITING,
    ADMIN_BARAHOLKI_WAITING,
    ADMIN_PROJECTS_WAITING,
    ADMIN_GIVEAWAYS_WAITING,
    ADMIN_MANAGER_WAITING,
    ADMIN_CATEGORY_TEXT_WAITING,
    ADMIN_CATEGORY_IMAGE_WAITING,
    ADMIN_NEW_CATEGORY_NAME_WAITING,
    ADMIN_NEW_CATEGORY_TEXT_WAITING,
    ADMIN_NEW_CATEGORY_IMAGE_WAITING,
    ADMIN_DELETE_CATEGORY_WAITING,
    ADMIN_RENAME_CATEGORY_WAITING,
    ADMIN_REORDER_CATEGORY_WAITING,
) = range(13)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_seen TEXT,
        last_spin TEXT
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
    CREATE TABLE IF NOT EXISTS categories (
        category_key TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        text TEXT NOT NULL,
        image TEXT NOT NULL,
        sort_order INTEGER NOT NULL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS subcategories (
        subcategory_key TEXT PRIMARY KEY,
        parent_category_key TEXT NOT NULL,
        label TEXT NOT NULL,
        text TEXT NOT NULL,
        image TEXT NOT NULL,
        sort_order INTEGER NOT NULL
    )
    """
)

conn.commit()


def set_setting(key: str, value: str) -> None:
    cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def get_setting(key: str, default: str = "") -> str:
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else default


for key, value in {
    "baraholki_url": DEFAULT_BARAHOLKI_URL,
    "projects_url": DEFAULT_PROJECTS_URL,
    "giveaways_url": DEFAULT_GIVEAWAYS_URL,
    "manager_url": DEFAULT_MANAGER_URL,
}.items():
    if not get_setting(key):
        set_setting(key, value)

cursor.execute("SELECT COUNT(*) FROM categories")
categories_count = cursor.fetchone()[0]

if categories_count == 0:
    for item in DEFAULT_CATEGORIES:
        cursor.execute(
            """
            INSERT INTO categories (category_key, label, text, image, sort_order)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                item["category_key"],
                item["label"],
                item["text"],
                item["image"],
                item["sort_order"],
            ),
        )
    conn.commit()

cursor.execute("SELECT COUNT(*) FROM subcategories")
subcategories_count = cursor.fetchone()[0]

if subcategories_count == 0:
    for item in DEFAULT_SUBCATEGORIES:
        cursor.execute(
            """
            INSERT INTO subcategories (
                subcategory_key,
                parent_category_key,
                label,
                text,
                image,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                item["subcategory_key"],
                item["parent_category_key"],
                item["label"],
                item["text"],
                item["image"],
                item["sort_order"],
            ),
        )
    conn.commit()


def now_iso() -> str:
    return datetime.now().isoformat()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def save_user(user) -> None:
    cursor.execute(
        """
        INSERT INTO users (user_id, username, first_name, last_seen, last_spin)
        VALUES (?, ?, ?, ?, NULL)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_seen = excluded.last_seen
        """,
        (user.id, user.username, user.first_name, now_iso()),
    )
    conn.commit()


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
    return datetime.now() - last_spin > timedelta(hours=24)


def generate_code() -> str:
    return "RNDM-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def get_discount() -> int:
    rand = random.randint(1, 100)
    if rand <= 60:
        return 10
    if rand <= 90:
        return 20
    return 40


def is_code_active(created_at: str) -> bool:
    created = datetime.fromisoformat(created_at)
    return datetime.now() - created <= timedelta(hours=2)


def slugify_category_name(name: str) -> str:
    allowed = string.ascii_lowercase + string.digits + "_"
    base = name.lower().strip().replace(" ", "_")
    cleaned = "".join(ch for ch in base if ch in allowed)
    if not cleaned:
        cleaned = "category"
    return cleaned


def generate_unique_category_key(label: str) -> str:
    base = slugify_category_name(label)
    candidate = base
    index = 1

    while True:
        cursor.execute("SELECT 1 FROM categories WHERE category_key = ?", (candidate,))
        exists = cursor.fetchone()
        if not exists:
            return candidate
        candidate = f"{base}_{index}"
        index += 1


def get_categories():
    cursor.execute(
        """
        SELECT category_key, label, text, image, sort_order
        FROM categories
        ORDER BY sort_order ASC, label ASC
        """
    )
    return cursor.fetchall()


def get_category(category_key: str):
    cursor.execute(
        """
        SELECT category_key, label, text, image, sort_order
        FROM categories
        WHERE category_key = ?
        """,
        (category_key,),
    )
    return cursor.fetchone()


def get_next_category_order() -> int:
    cursor.execute("SELECT COALESCE(MAX(sort_order), 0) + 1 FROM categories")
    return cursor.fetchone()[0]


def update_category_label(category_key: str, new_label: str) -> None:
    cursor.execute(
        "UPDATE categories SET label = ? WHERE category_key = ?",
        (new_label, category_key),
    )
    conn.commit()


def update_category_text(category_key: str, new_text: str) -> None:
    cursor.execute(
        "UPDATE categories SET text = ? WHERE category_key = ?",
        (new_text, category_key),
    )
    conn.commit()


def update_category_image(category_key: str, new_image: str) -> None:
    cursor.execute(
        "UPDATE categories SET image = ? WHERE category_key = ?",
        (new_image, category_key),
    )
    conn.commit()


def add_category(label: str, text: str, image: str) -> str:
    category_key = generate_unique_category_key(label)
    sort_order = get_next_category_order()

    cursor.execute(
        """
        INSERT INTO categories (category_key, label, text, image, sort_order)
        VALUES (?, ?, ?, ?, ?)
        """,
        (category_key, label, text, image, sort_order),
    )
    conn.commit()
    return category_key


def delete_category(category_key: str) -> None:
    cursor.execute("DELETE FROM categories WHERE category_key = ?", (category_key,))
    cursor.execute("DELETE FROM subcategories WHERE parent_category_key = ?", (category_key,))
    conn.commit()


def move_category(category_key: str, direction: str) -> bool:
    current = get_category(category_key)
    if not current:
        return False

    _, _, _, _, current_order = current

    if direction == "up":
        cursor.execute(
            """
            SELECT category_key, sort_order FROM categories
            WHERE sort_order < ?
            ORDER BY sort_order DESC
            LIMIT 1
            """,
            (current_order,),
        )
    else:
        cursor.execute(
            """
            SELECT category_key, sort_order FROM categories
            WHERE sort_order > ?
            ORDER BY sort_order ASC
            LIMIT 1
            """,
            (current_order,),
        )

    neighbor = cursor.fetchone()
    if not neighbor:
        return False

    neighbor_key, neighbor_order = neighbor

    cursor.execute(
        "UPDATE categories SET sort_order = ? WHERE category_key = ?",
        (neighbor_order, category_key),
    )
    cursor.execute(
        "UPDATE categories SET sort_order = ? WHERE category_key = ?",
        (current_order, neighbor_key),
    )
    conn.commit()
    return True


def get_subcategories(parent_category_key: str):
    cursor.execute(
        """
        SELECT subcategory_key, parent_category_key, label, text, image, sort_order
        FROM subcategories
        WHERE parent_category_key = ?
        ORDER BY sort_order ASC, label ASC
        """,
        (parent_category_key,),
    )
    return cursor.fetchall()


def get_subcategory(subcategory_key: str):
    cursor.execute(
        """
        SELECT subcategory_key, parent_category_key, label, text, image, sort_order
        FROM subcategories
        WHERE subcategory_key = ?
        """,
        (subcategory_key,),
    )
    return cursor.fetchone()


def has_subcategories(parent_category_key: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM subcategories WHERE parent_category_key = ? LIMIT 1",
        (parent_category_key,),
    )
    return cursor.fetchone() is not None


def manager_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬 Написать менеджеру", url=get_setting("manager_url", DEFAULT_MANAGER_URL))]]
    )


def assortment_menu_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for category_key, label, text, image, sort_order in get_categories():
        rows.append([InlineKeyboardButton(label, callback_data=f"category:{category_key}")])
    return InlineKeyboardMarkup(rows)


def subcategory_menu_keyboard(parent_category_key: str) -> InlineKeyboardMarkup:
    rows = []

    for subcategory_key, _, label, text, image, sort_order in get_subcategories(parent_category_key):
        rows.append([InlineKeyboardButton(label, callback_data=f"subcategory:{subcategory_key}")])

    rows.append([InlineKeyboardButton("⬅️ Назад к категориям", callback_data="assortment_menu")])
    return InlineKeyboardMarkup(rows)


def category_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 Уточнить наличие", url=get_setting("manager_url", DEFAULT_MANAGER_URL))],
            [InlineKeyboardButton("⬅️ Назад к категориям", callback_data="assortment_menu")],
        ]
    )


def post_link_keyboard(button_text: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(button_text, url=url)],
            [InlineKeyboardButton("💬 Написать менеджеру", url=get_setting("manager_url", DEFAULT_MANAGER_URL))],
        ]
    )


def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        ["🛍 Ассортимент", "🎰 Крутить скидку"],
        ["🛒 Наши барахолки", "🚀 Наши проекты"],
        ["🎁 Розыгрыши", "💬 Менеджер"],
    ]
    if is_admin(user_id):
        keyboard.append(["⚙️ Админка"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📢 Рассылка", "🛒 Ссылка на барахолки"],
            ["🚀 Ссылка на проекты", "🎁 Ссылка на розыгрыши"],
            ["💬 Ссылка на менеджера", "📝 Категории ассортимента"],
            ["➕ Добавить кнопку", "✏️ Переименовать кнопку"],
            ["🗑 Удалить кнопку", "↕️ Порядок кнопок"],
            ["📊 Статистика"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


def admin_categories_keyboard() -> ReplyKeyboardMarkup:
    keyboard = []

    categories = get_categories()
    for category_key, label, text, image, sort_order in categories:
        keyboard.append([f"📝 Текст: {label}"])
        keyboard.append([f"🖼 Фото: {label}"])

    keyboard.append(["⬅️ Назад"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def safe_send(update: Update, text: str, **kwargs):
    if update.message:
        await update.message.reply_text(text, **kwargs)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    await safe_send(
        update,
        """🔥 *Добро пожаловать в RNDM SHOP!*

Выбирай нужный раздел ниже 👇""",
        parse_mode="Markdown",
        reply_markup=main_keyboard(user.id),
    )


async def assortment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        """🛍 *АССОРТИМЕНТ RNDM SHOP*

Выбирай категорию ниже 👇""",
        parse_mode="Markdown",
        reply_markup=assortment_menu_keyboard(),
    )


async def show_category(query, category_key: str):
    category = get_category(category_key)
    if not category:
        await query.message.reply_text("❌ Категория не найдена.")
        return

    _, label, text, image_value, _ = category

    await query.message.reply_photo(
        photo=image_value,
        caption=text,
        parse_mode="Markdown",
        reply_markup=category_keyboard(),
    )


async def show_subcategory(query, subcategory_key: str):
    item = get_subcategory(subcategory_key)
    if not item:
        await query.message.reply_text("❌ Раздел не найден.")
        return

    _, parent_category_key, label, text, image, _ = item

    await query.message.reply_photo(
        photo=image,
        caption=text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🛒 ЗАКАЗАТЬ", url=get_setting("manager_url", DEFAULT_MANAGER_URL))],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"open_category:{parent_category_key}")],
            ]
        ),
    )


async def assortment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()

    if query.data == "assortment_menu":
        await query.message.reply_text(
            """🛍 *АССОРТИМЕНТ RNDM SHOP*

Выбирай категорию ниже 👇""",
            parse_mode="Markdown",
            reply_markup=assortment_menu_keyboard(),
        )
        return

    if query.data and query.data.startswith("category:"):
        category_key = query.data.split(":", 1)[1]
        category = get_category(category_key)
        if not category:
            await query.message.reply_text("❌ Категория не найдена.")
            return

        if has_subcategories(category_key):
            await query.message.reply_text(
                f"""📂 *{category[1]}*

Выбирай раздел ниже 👇""",
                parse_mode="Markdown",
                reply_markup=subcategory_menu_keyboard(category_key),
            )
            return

        await show_category(query, category_key)
        return

    if query.data and query.data.startswith("subcategory:"):
        subcategory_key = query.data.split(":", 1)[1]
        await show_subcategory(query, subcategory_key)
        return

    if query.data and query.data.startswith("open_category:"):
        category_key = query.data.split(":", 1)[1]
        category = get_category(category_key)
        if not category:
            await query.message.reply_text("❌ Категория не найдена.")
            return

        await query.message.reply_text(
            f"""📂 *{category[1]}*

Выбирай раздел ниже 👇""",
            parse_mode="Markdown",
            reply_markup=subcategory_menu_keyboard(category_key),
        )
        return


async def spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)

    if not can_spin(user.id):
        await safe_send(
            update,
            """⏳ *Ты уже крутил скидку за последние 24 часа.*

Попробуй позже 😈""",
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
        f"""💥 *ТЕБЕ ВЫПАЛО: -{discount}%*

Твой промокод: `{code}`
⏳ Действует *2 часа*""",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def baraholki(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        """🛒 *Наши барахолки*

Жми кнопку ниже 👇""",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🛒 Перейти в барахолки", get_setting("baraholki_url", DEFAULT_BARAHOLKI_URL)),
    )


async def projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        """🚀 *Наши проекты*

Все ссылки — по кнопке ниже.""",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🚀 Открыть проекты", get_setting("projects_url", DEFAULT_PROJECTS_URL)),
    )


async def giveaways(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        """🎁 *Розыгрыши RNDM SHOP*

Жми кнопку ниже.""",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🎁 Смотреть розыгрыши", get_setting("giveaways_url", DEFAULT_GIVEAWAYS_URL)),
    )


async def manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        """💬 *Связь с менеджером*

Пиши по кнопке ниже.""",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_send(update, "⛔ У тебя нет доступа.")
        return

    await safe_send(
        update,
        """⚙️ *Админка RNDM SHOP*

Выбери действие ниже.""",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


async def admin_categories_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    await safe_send(
        update,
        """📝 *Настройка категорий ассортимента*

Можно отдельно менять текст и отдельно фото для каждой категории.""",
        parse_mode="Markdown",
        reply_markup=admin_categories_keyboard(),
    )


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_send(update, "⛔ У тебя нет доступа.")
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    users_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM promocodes")
    codes_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM promocodes WHERE used = 1")
    used_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM categories")
    category_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM subcategories")
    subcategory_count = cursor.fetchone()[0]

    await safe_send(
        update,
        f"""📊 *Статистика*

Пользователей: *{users_count}*
Выдано промокодов: *{codes_count}*
Использовано промокодов: *{used_count}*
Категорий: *{category_count}*
Подкатегорий: *{subcategory_count}*""",
        parse_mode="Markdown",
    )


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
        f"""✅ Рассылка завершена.
Отправлено: {sent}
Ошибок: {failed}""",
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


async def admin_category_text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    button_text = update.message.text.strip()
    prefix = "📝 Текст: "
    if not button_text.startswith(prefix):
        return ConversationHandler.END

    category_label = button_text[len(prefix):].strip()

    cursor.execute(
        "SELECT category_key FROM categories WHERE label = ?",
        (category_label,),
    )
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Категория не найдена.")
        return ConversationHandler.END

    category_key = row[0]
    context.user_data["edit_category_key"] = category_key

    await safe_send(update, f"📝 Отправь новый текст для категории {category_label}. /cancel для отмены")
    return ADMIN_CATEGORY_TEXT_WAITING


async def admin_category_text_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_category_key")
    if not category_key:
        return ConversationHandler.END

    category = get_category(category_key)
    if not category:
        await safe_send(update, "❌ Категория не найдена.")
        return ConversationHandler.END

    update_category_text(category_key, update.message.text)

    await safe_send(
        update,
        f"✅ Текст для категории {category[1]} обновлён.",
        reply_markup=admin_categories_keyboard(),
    )
    return ConversationHandler.END


async def admin_category_image_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    button_text = update.message.text.strip()
    prefix = "🖼 Фото: "
    if not button_text.startswith(prefix):
        return ConversationHandler.END

    category_label = button_text[len(prefix):].strip()

    cursor.execute(
        "SELECT category_key FROM categories WHERE label = ?",
        (category_label,),
    )
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Категория не найдена.")
        return ConversationHandler.END

    category_key = row[0]
    context.user_data["edit_category_key"] = category_key

    await safe_send(update, f"🖼 Отправь фото для категории {category_label}. /cancel для отмены")
    return ADMIN_CATEGORY_IMAGE_WAITING


async def admin_category_image_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_category_key")
    if not category_key:
        return ConversationHandler.END

    category = get_category(category_key)
    if not category:
        await safe_send(update, "❌ Категория не найдена.")
        return ConversationHandler.END

    if not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_CATEGORY_IMAGE_WAITING

    photo = update.message.photo[-1]
    file_id = photo.file_id

    update_category_image(category_key, file_id)

    await safe_send(
        update,
        f"✅ Фото для категории {category[1]} обновлено.",
        reply_markup=admin_categories_keyboard(),
    )
    return ConversationHandler.END


async def admin_add_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    await safe_send(update, "➕ Отправь название новой кнопки. /cancel для отмены")
    return ADMIN_NEW_CATEGORY_NAME_WAITING


async def admin_add_category_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_category_label"] = update.message.text.strip()
    await safe_send(update, "📝 Теперь отправь текст внутри новой категории.")
    return ADMIN_NEW_CATEGORY_TEXT_WAITING


async def admin_add_category_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_category_text"] = update.message.text
    await safe_send(update, "🖼 Теперь отправь фото для новой категории.")
    return ADMIN_NEW_CATEGORY_IMAGE_WAITING


async def admin_add_category_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await safe_send(update, "❌ Нужно отправить именно фото.")
        return ADMIN_NEW_CATEGORY_IMAGE_WAITING

    label = context.user_data.get("new_category_label")
    text = context.user_data.get("new_category_text")

    if not label or not text:
        await safe_send(update, "❌ Данные новой категории потерялись.")
        return ConversationHandler.END

    photo = update.message.photo[-1]
    file_id = photo.file_id

    add_category(label=label, text=text, image=file_id)

    context.user_data.pop("new_category_label", None)
    context.user_data.pop("new_category_text", None)

    await safe_send(
        update,
        f"✅ Новая кнопка *{label}* добавлена.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_delete_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    categories = get_categories()
    if not categories:
        await safe_send(update, "❌ Нет категорий для удаления.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    text = "🗑 Отправь точное название кнопки, которую нужно удалить:\n\n"
    text += "\n".join([f"• {label}" for _, label, _, _, _ in categories])

    await safe_send(update, text)
    return ADMIN_DELETE_CATEGORY_WAITING


async def admin_delete_category_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = update.message.text.strip()

    cursor.execute("SELECT category_key FROM categories WHERE label = ?", (label,))
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Категория не найдена.")
        return ADMIN_DELETE_CATEGORY_WAITING

    category_key = row[0]
    delete_category(category_key)

    await safe_send(update, f"✅ Кнопка *{label}* удалена.", parse_mode="Markdown", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_rename_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    categories = get_categories()
    if not categories:
        await safe_send(update, "❌ Нет категорий.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    text = "✏️ Отправь сообщение в формате:\n\nСТАРОЕ НАЗВАНИЕ = НОВОЕ НАЗВАНИЕ\n\n"
    text += "Доступные кнопки:\n"
    text += "\n".join([f"• {label}" for _, label, _, _, _ in categories])

    await safe_send(update, text)
    return ADMIN_RENAME_CATEGORY_WAITING


async def admin_rename_category_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()

    if "=" not in raw:
        await safe_send(update, "❌ Формат: СТАРОЕ НАЗВАНИЕ = НОВОЕ НАЗВАНИЕ")
        return ADMIN_RENAME_CATEGORY_WAITING

    old_label, new_label = [part.strip() for part in raw.split("=", 1)]

    cursor.execute("SELECT category_key FROM categories WHERE label = ?", (old_label,))
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Старая категория не найдена.")
        return ADMIN_RENAME_CATEGORY_WAITING

    update_category_label(row[0], new_label)

    await safe_send(
        update,
        f"✅ Кнопка *{old_label}* переименована в *{new_label}*.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_reorder_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END

    categories = get_categories()
    if not categories:
        await safe_send(update, "❌ Нет категорий.", reply_markup=admin_keyboard())
        return ConversationHandler.END

    text = "↕️ Отправь сообщение в формате:\n\nНАЗВАНИЕ КНОПКИ = up\nили\nНАЗВАНИЕ КНОПКИ = down\n\n"
    text += "Текущий порядок:\n"
    for _, label, _, _, order in categories:
        text += f"{order}. {label}\n"

    await safe_send(update, text)
    return ADMIN_REORDER_CATEGORY_WAITING


async def admin_reorder_category_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()

    if "=" not in raw:
        await safe_send(update, "❌ Формат: НАЗВАНИЕ КНОПКИ = up/down")
        return ADMIN_REORDER_CATEGORY_WAITING

    label, direction = [part.strip() for part in raw.split("=", 1)]
    direction = direction.lower()

    if direction not in {"up", "down"}:
        await safe_send(update, "❌ Направление должно быть up или down.")
        return ADMIN_REORDER_CATEGORY_WAITING

    cursor.execute("SELECT category_key FROM categories WHERE label = ?", (label,))
    row = cursor.fetchone()
    if not row:
        await safe_send(update, "❌ Категория не найдена.")
        return ADMIN_REORDER_CATEGORY_WAITING

    ok = move_category(row[0], direction)
    if not ok:
        await safe_send(update, "⚠️ Дальше двигать уже некуда.")
        return ADMIN_REORDER_CATEGORY_WAITING

    await safe_send(update, "✅ Порядок кнопок обновлён.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "❌ Действие отменено.", reply_markup=main_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "⬅️ Возвращаю в главное меню.", reply_markup=main_keyboard(update.effective_user.id))


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
        await safe_send(
            update,
            f"""⚠️ Промокод уже использован
Скидка: -{discount}%
Когда использован: {used_at}"""
        )
        return

    if not is_code_active(created_at):
        await safe_send(
            update,
            f"""⌛ Промокод просрочен
Скидка была: -{discount}%
Создан: {created_at}"""
        )
        return

    await safe_send(
        update,
        f"""✅ Промокод активен
Скидка: -{discount}%
Создан: {created_at}
Владелец user_id: {owner_user_id}"""
    )


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

    await safe_send(
        update,
        f"""✅ Промокод {code} активирован
Скидка: -{discount}%"""
    )


def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", check_code))
    app.add_handler(CommandHandler("use", use_code))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(MessageHandler(filters.Regex(r"^🛍 Ассортимент$"), assortment))
    app.add_handler(MessageHandler(filters.Regex(r"^🎰 Крутить скидку$"), spin))
    app.add_handler(MessageHandler(filters.Regex(r"^🛒 Наши барахолки$"), baraholki))
    app.add_handler(MessageHandler(filters.Regex(r"^🚀 Наши проекты$"), projects))
    app.add_handler(MessageHandler(filters.Regex(r"^🎁 Розыгрыши$"), giveaways))
    app.add_handler(MessageHandler(filters.Regex(r"^💬 Менеджер$"), manager))
    app.add_handler(MessageHandler(filters.Regex(r"^⚙️ Админка$"), admin_panel))
    app.add_handler(MessageHandler(filters.Regex(r"^📝 Категории ассортимента$"), admin_categories_menu))
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Статистика$"), admin_stats))
    app.add_handler(MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main))

    app.add_handler(
        CallbackQueryHandler(
            assortment_callback,
            pattern=r"^(category:|subcategory:|open_category:|assortment_menu)"
        )
    )

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📢 Рассылка$"), admin_broadcast_start)],
        states={
            ADMIN_BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    baraholki_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🛒 Ссылка на барахолки$"), admin_baraholki_start)],
        states={
            ADMIN_BARAHOLKI_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_baraholki_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    projects_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🚀 Ссылка на проекты$"), admin_projects_start)],
        states={
            ADMIN_PROJECTS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_projects_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    giveaways_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🎁 Ссылка на розыгрыши$"), admin_giveaways_start)],
        states={
            ADMIN_GIVEAWAYS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_giveaways_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    manager_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^💬 Ссылка на менеджера$"), admin_manager_start)],
        states={
            ADMIN_MANAGER_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_manager_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    category_text_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📝 Текст: "), admin_category_text_start)],
        states={
            ADMIN_CATEGORY_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_category_text_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    category_image_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🖼 Фото: "), admin_category_image_start)],
        states={
            ADMIN_CATEGORY_IMAGE_WAITING: [MessageHandler(filters.PHOTO, admin_category_image_save)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    add_category_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^➕ Добавить кнопку$"), admin_add_category_start)],
        states={
            ADMIN_NEW_CATEGORY_NAME_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_category_name)],
            ADMIN_NEW_CATEGORY_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_category_text)],
            ADMIN_NEW_CATEGORY_IMAGE_WAITING: [MessageHandler(filters.PHOTO, admin_add_category_image)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    delete_category_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🗑 Удалить кнопку$"), admin_delete_category_start)],
        states={
            ADMIN_DELETE_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_delete_category_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    rename_category_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^✏️ Переименовать кнопку$"), admin_rename_category_start)],
        states={
            ADMIN_RENAME_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rename_category_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    reorder_category_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^↕️ Порядок кнопок$"), admin_reorder_category_start)],
        states={
            ADMIN_REORDER_CATEGORY_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reorder_category_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(broadcast_conv)
    app.add_handler(baraholki_conv)
    app.add_handler(projects_conv)
    app.add_handler(giveaways_conv)
    app.add_handler(manager_conv)
    app.add_handler(category_text_conv)
    app.add_handler(category_image_conv)
    app.add_handler(add_category_conv)
    app.add_handler(delete_category_conv)
    app.add_handler(rename_category_conv)
    app.add_handler(reorder_category_conv)

    print("RNDM SHOP bot запущен...")
    app.run_polling(
        poll_interval=1,
        timeout=10,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()