 import os
import re
import random
import sqlite3
import string
from datetime import datetime, timedelta
from typing import Optional

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
    8463296102,  # замени на свой Telegram user_id
}

DB_PATH = "rndm.db"
ASSORTMENT_TEXT = (
    "💨 *АССОРТИМЕНТ RNDM*\n\n"
    "Пока здесь примерный текст. Админ может обновлять его через кнопку *Админка* → *Обновить ассортимент*."
)
BARAHOLKI_POST_URL = "https://t.me/your_channel/1"  # ссылка на пост с барахолками и картинкой
PROJECTS_URL = "https://t.me/your_channel/2"  # ссылка на пост / канал с проектами
GIVEAWAYS_URL = "https://t.me/your_channel/3"  # ссылка на пост / канал с розыгрышами

(
    ADMIN_BROADCAST_WAITING,
    ADMIN_ASSORTMENT_WAITING,
    ADMIN_PROJECTS_WAITING,
    ADMIN_BARAHOLKI_WAITING,
    ADMIN_GIVEAWAYS_WAITING,
) = range(5)


# =========================
# DB
# =========================
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
    cursor.execute(
        "REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()



def get_setting(key: str, default: str = "") -> str:
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else default


# defaults
for key, value in {
    "assortment_text": ASSORTMENT_TEXT,
    "baraholki_url": BARAHOLKI_POST_URL,
    "projects_url": PROJECTS_URL,
    "giveaways_url": GIVEAWAYS_URL,
}.items():
    if not get_setting(key):
        set_setting(key, value)


# =========================
# Helpers
# =========================
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
        (
            user.id,
            user.username,
            user.first_name,
            now_iso(),
        ),
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



def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        ["🛍 Ассортимент", "🎰 Крутить скидку"],
        ["🛒 Наши барахолки", "🚀 Наши проекты"],
        ["🎁 Розыгрыши"],
    ]
    if is_admin(user_id):
        keyboard.append(["⚙️ Админка"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)



def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📢 Рассылка", "📝 Обновить ассортимент"],
            ["🔗 Ссылка на барахолки", "🚀 Ссылка на проекты"],
            ["🎁 Ссылка на розыгрыши", "📊 Статистика"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
    )


# =========================
# Public handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    await update.message.reply_text(
        "🔥 *Добро пожаловать в RNDM SHOP!*\n\n"
        "Выбирай нужный раздел ниже 👇",
        reply_markup=main_keyboard(user.id),
        parse_mode="Markdown",
    )


async def assortment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    text = get_setting("assortment_text", ASSORTMENT_TEXT)
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


async def spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)

    if not can_spin(user.id):
        await update.message.reply_text("⏳ Ты уже крутил скидку за последние 24 часа. Попробуй позже.")
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

    await update.message.reply_text("🎰 Крутим твою скидку...")
    await update.message.reply_text(
        f"💥 *ТЕБЕ ВЫПАЛО: -{discount}%*\n\n"
        f"Твой промокод: `{code}`\n"
        f"⏳ Действует 2 часа",
        parse_mode="Markdown",
    )


async def baraholki(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    url = get_setting("baraholki_url", BARAHOLKI_POST_URL)
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛒 Открыть барахолки", url=url)]]
    )
    await update.message.reply_text(
        "🛒 *Наши барахолки*\n\nПереходи по кнопке ниже.",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    url = get_setting("projects_url", PROJECTS_URL)
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚀 Открыть проекты", url=url)]]
    )
    await update.message.reply_text(
        "🚀 *Наши проекты*\n\nСмотри всё по кнопке ниже.",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def giveaways(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)
    url = get_setting("giveaways_url", GIVEAWAYS_URL)
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎁 Открыть розыгрыши", url=url)]]
    )
    await update.message.reply_text(
        "🎁 *Розыгрыши RNDM SHOP*\n\nЖми кнопку ниже.",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


# =========================
# Admin handlers
# =========================
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user(user)
    if not is_admin(user.id):
        await update.message.reply_text("⛔ У тебя нет доступа.")
        return

    await update.message.reply_text(
        "⚙️ *Админка RNDM SHOP*\n\nВыбери действие ниже.",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown",
    )


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ У тебя нет доступа.")
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    users_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM promocodes")
    codes_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM promocodes WHERE used = 1")
    used_count = cursor.fetchone()[0]

    await update.message.reply_text(
        f"📊 *Статистика*\n\n"
        f"Пользователей: *{users_count}*\n"
        f"Выдано промокодов: *{codes_count}*\n"
        f"Использовано промокодов: *{used_count}*",
        parse_mode="Markdown",
    )


async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ У тебя нет доступа.")
        return ConversationHandler.END

    await update.message.reply_text(
        "📢 Отправь текст рассылки одним сообщением.\n\n"
        "Чтобы отменить — нажми /cancel"
    )
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

    await update.message.reply_text(
        f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}",
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_assortment_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ У тебя нет доступа.")
        return ConversationHandler.END

    await update.message.reply_text(
        "📝 Отправь новый текст ассортимента одним сообщением.\n\n"
        "Можно с эмодзи и переносами строк.\n"
        "Чтобы отменить — нажми /cancel"
    )
    return ADMIN_ASSORTMENT_WAITING


async def admin_assortment_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("assortment_text", update.message.text)
    await update.message.reply_text("✅ Ассортимент обновлён.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_projects_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 Отправь новую ссылку на проекты. /cancel для отмены")
    return ADMIN_PROJECTS_WAITING


async def admin_projects_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("projects_url", update.message.text.strip())
    await update.message.reply_text("✅ Ссылка на проекты обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_baraholki_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🛒 Отправь новую ссылку на пост/канал барахолок. /cancel для отмены")
    return ADMIN_BARAHOLKI_WAITING


async def admin_baraholki_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("baraholki_url", update.message.text.strip())
    await update.message.reply_text("✅ Ссылка на барахолки обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_giveaways_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎁 Отправь новую ссылку на розыгрыши. /cancel для отмены")
    return ADMIN_GIVEAWAYS_WAITING


async def admin_giveaways_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("giveaways_url", update.message.text.strip())
    await update.message.reply_text("✅ Ссылка на розыгрыши обновлена.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Действие отменено.", reply_markup=main_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⬅️ Возвращаю в главное меню.", reply_markup=main_keyboard(update.effective_user.id))


async def check_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /check RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute(
        """
        SELECT code, discount, used, created_at, used_at, owner_user_id
        FROM promocodes WHERE code = ?
        """,
        (code,),
    )
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("❌ Промокод не найден")
        return

    _, discount, used, created_at, used_at, owner_user_id = row

    if used:
        await update.message.reply_text(
            f"⚠️ Промокод уже использован\nСкидка: -{discount}%\nКогда использован: {used_at}"
        )
        return

    if not is_code_active(created_at):
        await update.message.reply_text(
            f"⌛ Промокод просрочен\nСкидка была: -{discount}%\nСоздан: {created_at}"
        )
        return

    await update.message.reply_text(
        f"✅ Промокод активен\nСкидка: -{discount}%\nСоздан: {created_at}\nВладелец user_id: {owner_user_id}"
    )


async def use_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /use RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()
    cursor.execute(
        "SELECT used, created_at, discount FROM promocodes WHERE code = ?",
        (code,),
    )
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("❌ Промокод не найден")
        return

    used, created_at, discount = row

    if used:
        await update.message.reply_text("⚠️ Промокод уже использован")
        return

    if not is_code_active(created_at):
        await update.message.reply_text("⌛ Промокод просрочен")
        return

    cursor.execute(
        "UPDATE promocodes SET used = 1, used_at = ? WHERE code = ?",
        (now_iso(), code),
    )
    conn.commit()

    await update.message.reply_text(f"✅ Промокод {code} активирован\nСкидка: -{discount}%")


def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", check_code))
    app.add_handler(CommandHandler("use", use_code))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(MessageHandler(filters.Regex("^🛍 Ассортимент$"), assortment))
    app.add_handler(MessageHandler(filters.Regex("^🎰 Крутить скидку$"), spin))
    app.add_handler(MessageHandler(filters.Regex("^🛒 Наши барахолки$"), baraholki))
    app.add_handler(MessageHandler(filters.Regex("^🚀 Наши проекты$"), projects))
    app.add_handler(MessageHandler(filters.Regex("^🎁 Розыгрыши$"), giveaways))
    app.add_handler(MessageHandler(filters.Regex("^⚙️ Админка$"), admin_panel))
    app.add_handler(MessageHandler(filters.Regex("^📊 Статистика$"), admin_stats))
    app.add_handler(MessageHandler(filters.Regex("^⬅️ Назад$"), back_to_main))

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📢 Рассылка$"), admin_broadcast_start)],
        states={
            ADMIN_BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    assortment_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📝 Обновить ассортимент$"), admin_assortment_start)],
        states={
            ADMIN_ASSORTMENT_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_assortment_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    projects_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🚀 Ссылка на проекты$"), admin_projects_start)],
        states={
            ADMIN_PROJECTS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_projects_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    baraholki_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🔗 Ссылка на барахолки$"), admin_baraholki_start)],
        states={
            ADMIN_BARAHOLKI_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_baraholki_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    giveaways_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🎁 Ссылка на розыгрыши$"), admin_giveaways_start)],
        states={
            ADMIN_GIVEAWAYS_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_giveaways_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(broadcast_conv)
    app.add_handler(assortment_conv)
    app.add_handler(projects_conv)
    app.add_handler(baraholki_conv)
    app.add_handler(giveaways_conv)

    print("RNDM SHOP bot запущен...")
    app.run_polling(
    poll_interval=1,
    timeout=10,
    drop_pending_updates=True
)


if __name__ == "__main__":
    main()
