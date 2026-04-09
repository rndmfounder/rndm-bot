import os
import random
import sqlite3
import string
from datetime import datetime, timedelta

from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("Не найдена переменная окружения BOT_TOKEN")

conn = sqlite3.connect("rndm.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    last_spin TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS promocodes (
    code TEXT PRIMARY KEY,
    discount INTEGER NOT NULL,
    used INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    used_at TEXT,
    owner_user_id INTEGER
)
""")

conn.commit()


def generate_code() -> str:
    return "RNDM-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def get_discount() -> int:
    rand = random.randint(1, 100)
    if rand <= 60:
        return 10
    elif rand <= 90:
        return 20
    return 40


def can_spin(user_id: int) -> bool:
    cursor.execute("SELECT last_spin FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()

    if not row:
        return True

    last_spin = datetime.fromisoformat(row[0])
    return datetime.now() - last_spin > timedelta(hours=24)


def is_code_active(created_at: str) -> bool:
    created = datetime.fromisoformat(created_at)
    return datetime.now() - created <= timedelta(hours=2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["🎰 КРУТИТЬ"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "🎰 КРУТИ СВОЮ СКИДКУ\n\nЖми кнопку и попробуй выбить максимум 😈",
        reply_markup=reply_markup
    )


async def spin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not can_spin(user_id):
        await update.message.reply_text("⏳ Ты уже крутил сегодня. Попробуй позже.")
        return

    discount = get_discount()
    code = generate_code()
    now = datetime.now().isoformat()

    cursor.execute(
        "REPLACE INTO users (user_id, last_spin) VALUES (?, ?)",
        (user_id, now)
    )

    cursor.execute(
        """
        INSERT INTO promocodes (code, discount, used, created_at, used_at, owner_user_id)
        VALUES (?, ?, 0, ?, NULL, ?)
        """,
        (code, discount, now, user_id)
    )
    conn.commit()

    await update.message.reply_text("🎰 Крутим...")
    await update.message.reply_text(
        f"💥 ТЕБЕ ВЫПАЛО: -{discount}%\n\n"
        f"Твой промокод:\n{code}\n\n"
        f"⏳ Действует 2 часа"
    )


async def check_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /check RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()

    cursor.execute(
        """
        SELECT code, discount, used, created_at, used_at, owner_user_id
        FROM promocodes
        WHERE code = ?
        """,
        (code,)
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
        f"✅ Промокод активен\n"
        f"Скидка: -{discount}%\n"
        f"Создан: {created_at}\n"
        f"Владелец user_id: {owner_user_id}"
    )


async def use_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /use RNDM-XXXXXX")
        return

    code = context.args[0].strip().upper()

    cursor.execute(
        """
        SELECT used, created_at, discount
        FROM promocodes
        WHERE code = ?
        """,
        (code,)
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
        (datetime.now().isoformat(), code)
    )
    conn.commit()

    await update.message.reply_text(f"✅ Промокод {code} активирован\nСкидка: -{discount}%")


def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", check_code))
    app.add_handler(CommandHandler("use", use_code))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🎰 КРУТИТЬ$"), spin))

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()