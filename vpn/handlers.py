"""Хендлеры Telegram для раздела VPN.

Переменные окружения (опционально):
  VPN_TRIAL_DAYS — если > 0, при первом заходе в VPN создаётся пробная подписка на N дней.
  VPN_INFO_URL — ссылка на инструкцию (показывается в тексте экрана).

Админ-команды (см. `is_admin` в боте):
  /vpn_grant <user_id> <days> — продлить/выдать подписку.
  /vpn_revoke <user_id> — снять подписку и удалить конфиг из БД.
  /vpn_setconf <user_id> — ответом реплаем на файл .conf загрузить конфиг пользователю.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from vpn import db as vpn_db

log = logging.getLogger(__name__)

_state: dict[str, Any] = {}


def configure(**kwargs: Any) -> None:
    _state.clear()
    _state.update(kwargs)


def _get(name: str) -> Any:
    if name not in _state:
        raise RuntimeError(f"VPN: не вызван configure() — нет {name!r}")
    return _state[name]


def _is_active(sub: dict[str, Any] | None) -> bool:
    if not sub or sub.get("status") != "active":
        return False
    exp = sub.get("expires_at")
    if not exp:
        return True
    try:
        dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) > datetime.now(timezone.utc)
    except Exception:
        return False


def _manager_markup() -> InlineKeyboardMarkup | None:
    get_setting = _get("get_setting")
    default_manager_url = _get("default_manager_url")
    is_valid_inline_button_url = _get("is_valid_inline_button_url")
    url = (get_setting("manager_url", default_manager_url) or default_manager_url).strip()
    if not url or not is_valid_inline_button_url(url):
        url = default_manager_url
    if not is_valid_inline_button_url(url):
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать менеджеру", url=url[:2000])]])


def _vpn_status_text(sub: dict[str, Any] | None) -> str:
    if not sub:
        return "Статус: *нет подписки*\n\nОформление — через менеджера или команду админа `/vpn_grant`."
    active = _is_active(sub)
    exp = sub.get("expires_at") or "—"
    plan = sub.get("plan_code") or "—"
    st = "активна ✅" if active else "не активна ⛔"
    conf = "конфиг выдан (можно скачать кнопкой ниже)" if (active and sub.get("config_text")) else "конфиг ещё не загружен админом"
    return (
        f"🔐 *VPN RNDM*\n\n"
        f"Статус: {st}\n"
        f"Тариф: `{plan}`\n"
        f"Действует до: `{exp}`\n"
        f"{conf}\n"
    )


async def _send_vpn_screen(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    edit_message: bool = False,
) -> None:
    safe_send = _get("safe_send")
    save_user = _get("save_user")
    is_user_blacklisted = _get("is_user_blacklisted")
    log_action = _get("log_action")
    cursor = _get("cursor")
    conn = _get("conn")
    now_iso = _get("now_iso")

    user = update.effective_user
    if not user:
        return
    if is_user_blacklisted(user.id):
        await safe_send(update, "⛔ Доступ к боту ограничен.")
        return
    save_user(user)

    trial_days = int(os.getenv("VPN_TRIAL_DAYS", "0") or "0")
    if vpn_db.upsert_trial_if_absent(cursor, conn, user.id, trial_days, now_iso()):
        log_action(user.id, "vpn", "auto_trial")

    sub = vpn_db.get_subscription(cursor, user.id)
    text = _vpn_status_text(sub)
    info_url = (os.getenv("VPN_INFO_URL") or "").strip()
    if info_url:
        text += f"\n📖 [Инструкция]({info_url})"

    rows: list[list[InlineKeyboardButton]] = []
    if sub and _is_active(sub) and (sub.get("config_text") or "").strip():
        rows.append([InlineKeyboardButton("📥 Скачать .conf", callback_data="vpn:dl")])
    rows.append([InlineKeyboardButton("🔄 Обновить статус", callback_data="vpn:refresh")])
    markup = InlineKeyboardMarkup(rows)
    mgr = _manager_markup()
    if mgr and mgr.inline_keyboard:
        markup.inline_keyboard.extend(mgr.inline_keyboard)

    log_action(user.id, "vpn", "open")

    if edit_message and update.callback_query and update.callback_query.message:
        try:
            await update.callback_query.message.edit_text(
                text, parse_mode="Markdown", reply_markup=markup, disable_web_page_preview=True
            )
            return
        except Exception:
            log.exception("vpn: edit_text")

    await safe_send(
        update,
        text,
        parse_mode="Markdown",
        reply_markup=markup,
        disable_web_page_preview=True,
    )


async def vpn_open_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_vpn_screen(update, context, edit_message=False)


async def vpn_open_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_vpn_screen(update, context, edit_message=False)


async def vpn_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    data = str(query.data)
    user = update.effective_user
    if not user:
        return

    try:
        await query.answer()
    except Exception:
        log.exception("vpn_callback: answer")

    if data == "vpn:refresh":
        await _send_vpn_screen(update, context, edit_message=True)
        return

    if data == "vpn:dl":
        cursor = _get("cursor")
        sub = vpn_db.get_subscription(cursor, user.id)
        if not sub or not _is_active(sub):
            try:
                await query.answer("Подписка не активна.", show_alert=True)
            except Exception:
                pass
            return
        raw = (sub.get("config_text") or "").strip()
        if not raw:
            try:
                await query.answer("Конфиг ещё не загружен.", show_alert=True)
            except Exception:
                pass
            return
        bio = BytesIO(raw.encode("utf-8"))
        bio.name = f"rndm-vpn-{user.id}.conf"
        try:
            await context.bot.send_document(
                chat_id=user.id,
                document=InputFile(bio, filename=bio.name),
                caption="🔐 Конфиг WireGuard. Не пересылай посторонним.",
            )
        except Exception:
            log.exception("vpn: send_document")
            try:
                await query.answer("Не удалось отправить файл в личку.", show_alert=True)
            except Exception:
                pass
        return


def _parse_grant_args(args: list[str]) -> tuple[int, int] | None:
    if len(args) < 2:
        return None
    try:
        uid = int(args[0])
        days = int(args[1])
    except ValueError:
        return None
    if days <= 0 or days > 3650:
        return None
    return uid, days


async def cmd_vpn_grant(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    is_admin = _get("is_admin")
    user = update.effective_user
    if not user or not is_admin(user.id):
        if update.message:
            await update.message.reply_text("⛔ Команда только для админов.")
        return
    args = context.args or []
    parsed = _parse_grant_args(args)
    if not parsed:
        await update.message.reply_text("Использование: `/vpn_grant <user_id> <days>`", parse_mode="Markdown")
        return
    target_id, days = parsed
    cursor = _get("cursor")
    conn = _get("conn")
    now_iso = _get("now_iso")
    vpn_db.grant_days(cursor, conn, target_id, days, "manual", now_iso())
    await update.message.reply_text(f"OK: пользователю `{target_id}` выдано *{days}* дн.", parse_mode="Markdown")


async def cmd_vpn_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    is_admin = _get("is_admin")
    user = update.effective_user
    if not user or not is_admin(user.id):
        if update.message:
            await update.message.reply_text("⛔ Команда только для админов.")
        return
    args = context.args or []
    if len(args) != 1:
        await update.message.reply_text("Использование: `/vpn_revoke <user_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Некорректный user_id.")
        return
    cursor = _get("cursor")
    conn = _get("conn")
    now_iso = _get("now_iso")
    vpn_db.revoke_user(cursor, conn, target_id, now_iso())
    await update.message.reply_text(f"OK: подписка `{target_id}` снята, конфиг очищен.", parse_mode="Markdown")


async def cmd_vpn_setconf(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Админ: ответить на сообщение с .conf или передать конфиг одним сообщением после user_id."""
    is_admin = _get("is_admin")
    user = update.effective_user
    if not user or not is_admin(user.id):
        if update.message:
            await update.message.reply_text("⛔ Команда только для админов.")
        return
    msg = update.message
    if not msg:
        return
    cursor = _get("cursor")
    conn = _get("conn")
    now_iso = _get("now_iso")

    if msg.reply_to_message and msg.reply_to_message.document:
        doc = msg.reply_to_message.document
        if not doc or not doc.file_id:
            await msg.reply_text("Нет файла в реплае.")
            return
        args = context.args or []
        if len(args) != 1:
            await msg.reply_text("Использование: `/vpn_setconf <user_id>` ответом на файл .conf")
            return
        try:
            target_id = int(args[0])
        except ValueError:
            await msg.reply_text("Некорректный user_id.")
            return
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            buf = BytesIO()
            await tg_file.download_to_memory(out=buf)
            text = buf.getvalue().decode("utf-8", errors="replace")
        except Exception:
            log.exception("vpn_setconf: download")
            await msg.reply_text("Не удалось скачать файл.")
            return
        vpn_db.set_config_text(cursor, conn, target_id, text, now_iso())
        await msg.reply_text(f"OK: конфиг для `{target_id}` сохранён.", parse_mode="Markdown")
        return

    args = context.args or []
    if len(args) < 2:
        await msg.reply_text(
            "Использование:\n"
            "• `/vpn_setconf <user_id>` ответом на документ .conf\n"
            "• или `/vpn_setconf <user_id>` и следующим сообщением пришли текст конфига (редко).",
            parse_mode="Markdown",
        )
        return

    await msg.reply_text("Для текста конфига ответь реплаем на файл или используй документ.")


def register_vpn_command_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("vpn", vpn_open_command))
    app.add_handler(CommandHandler("vpn_grant", cmd_vpn_grant))
    app.add_handler(CommandHandler("vpn_revoke", cmd_vpn_revoke))
    app.add_handler(CommandHandler("vpn_setconf", cmd_vpn_setconf))


def register_vpn_early_handlers(app: Application) -> None:
    app.add_handler(CallbackQueryHandler(vpn_callback, pattern=r"^vpn:"), group=-1)


def register_vpn_message_handlers(app: Application) -> None:
    # group=-1: срабатывает до ConversationHandler (оформление заказа не «съест» кнопку).
    app.add_handler(MessageHandler(filters.Regex(r"^🔐 VPN\uFE0F?$"), vpn_open_message), group=-1)
