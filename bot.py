import os
import random
import sqlite3
import string
from datetime import datetime, timedelta

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

DB_PATH = "rndm.db"

DEFAULT_BARAHOLKI_URL = "https://t.me/your_channel/1"
DEFAULT_PROJECTS_URL = "https://t.me/your_channel/2"
DEFAULT_GIVEAWAYS_URL = "https://t.me/your_channel/3"
DEFAULT_MANAGER_URL = "https://t.me/your_manager"

CATEGORY_KEYS = {
    "devices": "⚡ УСТРОЙСТВА",
    "liquids": "💧 ЖИДКОСТИ",
    "disposables": "🔥 ОДНОРАЗКИ",
    "plates": "🧊 ШАЙБЫ/ПЛАСТИНКИ",
    "supplies": "🛠 РАСХОДНИКИ",
    "sale": "💸 СЛИВ/СКИДКИ",
}

DEFAULT_CATEGORY_TEXTS = {
    "devices": "⚡ *УСТРОЙСТВА*

Добавь сюда актуальные устройства, например:
• XROS
• AEGIS
• PASITO

Чтобы обновить — зайди в админку.",
    "liquids": "💧 *ЖИДКОСТИ*

Добавь сюда актуальные жидкости, например:
• DUALL
• TRAVA
• SKALA",
    "disposables": "🔥 *ОДНОРАЗКИ*

Добавь сюда актуальные одноразки, например:
• VOZOL
• WAKA
• NANCY",
    "plates": "🧊 *ШАЙБЫ/ПЛАСТИНКИ*

Добавь сюда актуальные шайбы / пластинки.",
    "supplies": "🛠 *РАСХОДНИКИ*

Добавь сюда актуальные расходники.",
    "sale": "💸 *СЛИВ / СКИДКИ*

Добавь сюда позиции со сливом и скидками.",
}

DEFAULT_CATEGORY_IMAGES = {
    "devices": "https://via.placeholder.com/1200x800?text=USTROYSTVA",
    "liquids": "https://via.placeholder.com/1200x800?text=ZHIDKOSTI",
    "disposables": "https://via.placeholder.com/1200x800?text=ODNORAZKI",
    "plates": "https://via.placeholder.com/1200x800?text=SHAYBY",
    "supplies": "https://via.placeholder.com/1200x800?text=RASHODNIKI",
    "sale": "https://via.placeholder.com/1200x800?text=SALE",
}

(
    ADMIN_BROADCAST_WAITING,
    ADMIN_PROJECTS_WAITING,
    ADMIN_BARAHOLKI_WAITING,
    ADMIN_GIVEAWAYS_WAITING,
    ADMIN_MANAGER_WAITING,
    ADMIN_CATEGORY_TEXT_WAITING,
    ADMIN_CATEGORY_IMAGE_WAITING,
) = range(7)


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

for category_key, text in DEFAULT_CATEGORY_TEXTS.items():
    if not get_setting(f"category_text_{category_key}"):
        set_setting(f"category_text_{category_key}", text)
    if not get_setting(f"category_image_{category_key}"):
        set_setting(f"category_image_{category_key}", DEFAULT_CATEGORY_IMAGES[category_key])



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



def manager_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬 Написать менеджеру", url=get_setting("manager_url", DEFAULT_MANAGER_URL))]]
    )



def assortment_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⚡ УСТРОЙСТВА", callback_data="category:devices")],
            [InlineKeyboardButton("💧 ЖИДКОСТИ", callback_data="category:liquids")],
            [InlineKeyboardButton("🔥 ОДНОРАЗКИ", callback_data="category:disposables")],
            [InlineKeyboardButton("🧊 ШАЙБЫ/ПЛАСТИНКИ", callback_data="category:plates")],
            [InlineKeyboardButton("🛠 РАСХОДНИКИ", callback_data="category:supplies")],
            [InlineKeyboardButton("💸 СЛИВ/СКИДКИ", callback_data="category:sale")],
        ]
    )



def category_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 Уточнить наличие", url=get_setting("manager_url", DEFAULT_MANAGER_URL))],
            [InlineKeyboardButton("⬅️ Назад к категориям", callback_data="assortment_menu")],
        ]
    )



def post_link_keyboard(text: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(text, url=url)],
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
            ["📊 Статистика"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )



def admin_categories_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["⚡ Текст: Устройства", "💧 Текст: Жидкости"],
            ["🔥 Текст: Одноразки", "🧊 Текст: Шайбы/Пластинки"],
            ["🛠 Текст: Расходники", "💸 Текст: Слив/Скидки"],
            ["🖼 Фото: Устройства", "🖼 Фото: Жидкости"],
            ["🖼 Фото: Одноразки", "🖼 Фото: Шайбы/Пластинки"],
            ["🖼 Фото: Расходники", "🖼 Фото: Слив/Скидки"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


CATEGORY_TEXT_BUTTONS = {
    "⚡ Текст: Устройства": "devices",
    "💧 Текст: Жидкости": "liquids",
    "🔥 Текст: Одноразки": "disposables",
    "🧊 Текст: Шайбы/Пластинки": "plates",
    "🛠 Текст: Расходники": "supplies",
    "💸 Текст: Слив/Скидки": "sale",
}

CATEGORY_IMAGE_BUTTONS = {
    "🖼 Фото: Устройства": "devices",
    "🖼 Фото: Жидкости": "liquids",
    "🖼 Фото: Одноразки": "disposables",
    "🖼 Фото: Шайбы/Пластинки": "plates",
    "🖼 Фото: Расходники": "supplies",
    "🖼 Фото: Слив/Скидки": "sale",
}


async def safe_send(update: Update, text: str, **kwargs):
    if update.message:
        await update.message.reply_text(text, **kwargs)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    await safe_send(
        update,
        "🔥 *Добро пожаловать в RNDM SHOP!*

"
        "Выбирай нужный раздел ниже 👇",
        reply_markup=main_keyboard(user.id),
        parse_mode="Markdown",
    )


async def assortment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🛍 *АССОРТИМЕНТ RNDM SHOP*

Выбирай категорию ниже 👇",
        parse_mode="Markdown",
        reply_markup=assortment_menu_keyboard(),
    )


async def show_category(query, category_key: str):
    image_url = get_setting(f"category_image_{category_key}", DEFAULT_CATEGORY_IMAGES[category_key])
    text = get_setting(f"category_text_{category_key}", DEFAULT_CATEGORY_TEXTS[category_key])
    await query.message.reply_photo(
        photo=image_url,
        caption=text,
        parse_mode="Markdown",
        reply_markup=category_keyboard(),
    )


async def assortment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "assortment_menu":
        await query.message.reply_text(
            "🛍 *АССОРТИМЕНТ RNDM SHOP*

Выбирай категорию ниже 👇",
            parse_mode="Markdown",
            reply_markup=assortment_menu_keyboard(),
        )
        return

    if query.data.startswith("category:"):
        category_key = query.data.split(":", 1)[1]
        if category_key in CATEGORY_KEYS:
            await show_category(query, category_key)


async def spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)

    if not can_spin(user.id):
        await safe_send(
            update,
            "⏳ *Ты уже крутил скидку за последние 24 часа.*

Попробуй позже 😈",
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
        f"💥 *ТЕБЕ ВЫПАЛО: -{discount}%*

"
        f"Твой промокод: `{code}`
"
        f"⏳ Действует *2 часа*",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def baraholki(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🛒 *Наши барахолки*

Жми кнопку ниже 👇",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🛒 Перейти в барахолки", get_setting("baraholki_url", DEFAULT_BARAHOLKI_URL)),
    )


async def projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🚀 *Наши проекты*

Все ссылки — по кнопке ниже.",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🚀 Открыть проекты", get_setting("projects_url", DEFAULT_PROJECTS_URL)),
    )


async def giveaways(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "🎁 *Розыгрыши RNDM SHOP*

Жми кнопку ниже.",
        parse_mode="Markdown",
        reply_markup=post_link_keyboard("🎁 Смотреть розыгрыши", get_setting("giveaways_url", DEFAULT_GIVEAWAYS_URL)),
    )


async def manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    await safe_send(
        update,
        "💬 *Связь с менеджером*

Пиши по кнопке ниже.",
        parse_mode="Markdown",
        reply_markup=manager_keyboard(),
    )


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_send(update, "⛔ У тебя нет доступа.")
        return
    await safe_send(
        update,
        "⚙️ *Админка RNDM SHOP*

Выбери действие ниже.",
        parse_mode="Markdown",
        reply_markup=admin_keyboard(),
    )


async def admin_categories_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await safe_send(
        update,
        "📝 *Настройка категорий ассортимента*

"
        "Можно отдельно менять текст и отдельно фото для каждой категории.",
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

    await safe_send(
        update,
        f"📊 *Статистика*

Пользователей: *{users_count}*
Выдано промокодов: *{codes_count}*
Использовано промокодов: *{used_count}*",
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

    await safe_send(update, f"✅ Рассылка завершена.
Отправлено: {sent}
Ошибок: {failed}", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_projects_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "🚀 Отправь новую ссылку на проекты. /cancel для отмены")
    return ADMIN_PROJECTS_WAITING


async def admin_projects_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("projects_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на проекты обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_baraholki_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "🛒 Отправь новую ссылку на барахолки. /cancel для отмены")
    return ADMIN_BARAHOLKI_WAITING


async def admin_baraholki_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("baraholki_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на барахолки обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_giveaways_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "🎁 Отправь новую ссылку на розыгрыши. /cancel для отмены")
    return ADMIN_GIVEAWAYS_WAITING


async def admin_giveaways_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("giveaways_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на розыгрыши обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_manager_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, "💬 Отправь новую ссылку на менеджера. /cancel для отмены")
    return ADMIN_MANAGER_WAITING


async def admin_manager_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("manager_url", update.message.text.strip())
    await safe_send(update, "✅ Ссылка на менеджера обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_category_text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    button_text = update.message.text
    category_key = CATEGORY_TEXT_BUTTONS.get(button_text)
    if not category_key:
        return ConversationHandler.END
    context.user_data["edit_category_key"] = category_key
    context.user_data["edit_category_mode"] = "text"
    await safe_send(update, f"📝 Отправь новый текст для категории {CATEGORY_KEYS[category_key]}. /cancel для отмены")
    return ADMIN_CATEGORY_TEXT_WAITING


async def admin_category_text_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_category_key")
    if not category_key:
        return ConversationHandler.END
    set_setting(f"category_text_{category_key}", update.message.text)
    await safe_send(update, f"✅ Текст для категории {CATEGORY_KEYS[category_key]} обновлён.", reply_markup=admin_categories_keyboard())
    return ConversationHandler.END


async def admin_category_image_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    button_text = update.message.text
    category_key = CATEGORY_IMAGE_BUTTONS.get(button_text)
    if not category_key:
        return ConversationHandler.END
    context.user_data["edit_category_key"] = category_key
    context.user_data["edit_category_mode"] = "image"
    await safe_send(update, f"🖼 Отправь новую ссылку на фото для категории {CATEGORY_KEYS[category_key]}. /cancel для отмены")
    return ADMIN_CATEGORY_IMAGE_WAITING


async def admin_category_image_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category_key = context.user_data.get("edit_category_key")
    if not category_key:
        return ConversationHandler.END
    set_setting(f"category_image_{category_key}", update.message.text.strip())
    await safe_send(update, f"✅ Фото для категории {CATEGORY_KEYS[category_key]} обновлено.", reply_markup=admin_categories_keyboard())
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
        await safe_send(update, f"⚠️ Промокод уже использован
Скидка: -{discount}%
Когда использован: {used_at}")
        return
    if not is_code_active(created_at):
        await safe_send(update, f"⌛ Промокод просрочен
Скидка была: -{discount}%
Создан: {created_at}")
        return
    await safe_send(update, f"✅ Промокод активен
Скидка: -{discount}%
Создан: {created_at}
Владелец user_id: {owner_user_id}")


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
    await safe_send(update, f"✅ Промокод {code} активирован
Скидка: -{discount}%")



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
    app.add_handler(MessageHandler(filters.Regex(r"^📊 Статистика$"), admin_stats))
    app.add_handler(MessageHandler(filters.Regex(r"^📝 Категории ассортимента$"), admin_categories_menu))
    app.add_handler(MessageHandler(filters.Regex(r"^⬅️ Назад$"), back_to_main))

    app.add_handler(CallbackQueryHandler(assortment_callback, pattern=r"^(category:|assortment_menu)"))

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^📢 Рассылка$"), admin_broadcast_start)],
        states={ADMIN_BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    projects_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🚀 Ссылка на проекты$"), admin_projects_start)],
        states={ADMIN_PROJECTS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_projects_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    baraholki_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^🛒 Ссылка на барахолки$"), admin_baraholki_start)],
        states={ADMIN_BARAHOLKI_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_baraholki_save)]},
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

    category_text_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^(⚡ Текст: Устройства|💧 Текст: Жидкости|🔥 Текст: Одноразки|🧊 Текст: Шайбы/Пластинки|🛠 Текст: Расходники|💸 Текст: Слив/Скидки)$"), admin_category_text_start)],
        states={ADMIN_CATEGORY_TEXT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_category_text_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    category_image_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^(🖼 Фото: Устройства|🖼 Фото: Жидкости|🖼 Фото: Одноразки|🖼 Фото: Шайбы/Пластинки|🖼 Фото: Расходники|🖼 Фото: Слив/Скидки)$"), admin_category_image_start)],
        states={ADMIN_CATEGORY_IMAGE_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_category_image_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(broadcast_conv)
    app.add_handler(projects_conv)
    app.add_handler(baraholki_conv)
    app.add_handler(giveaways_conv)
    app.add_handler(manager_conv)
    app.add_handler(category_text_conv)
    app.add_handler(category_image_conv)

    print("RNDM SHOP bot запущен...")
    app.run_polling(
        poll_interval=1,
        timeout=10,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
