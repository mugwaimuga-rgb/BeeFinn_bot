import os
import re
import asyncio
import logging
from enum import Enum
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

class State(Enum):
    ADD_AMOUNT = 1
    ADD_CONFIRM = 2

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Операция", callback_data="menu:ops")],
    ])

def ops_menu_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Доход", callback_data="ops:income"),
            InlineKeyboardButton("🔴 Расход", callback_data="ops:expense"),
        ],
        [InlineKeyboardButton("⬅ Назад", callback_data="menu:home")],
    ])

def cancel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
    ])

def money_parse(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t):
        return None
    return float(t)

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text="🏠 Главное меню"):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        await q.edit_message_text(text, reply_markup=main_menu_kb())
    else:
        await update.message.reply_text(text, reply_markup=main_menu_kb())

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Привет! Мини-бот. Выбери действие:")
    return ConversationHandler.END

async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Используйте кнопки меню 👇", reply_markup=main_menu_kb()
    )
    return ConversationHandler.END

async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Отменено ❌")
    return ConversationHandler.END

# --------- шаги сценария доход/расход ----------

async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, ttype: str):
    q = update.callback_query
    await q.answer()
    context.user_data.clear()
    context.user_data["ttype"] = ttype
    label = "ДОХОД 🟢" if ttype == "income" else "РАСХОД 🔴"
    await q.edit_message_text(
        f"Вводим {label}\nСумма (например 350.50):",
        reply_markup=cancel_kb(),
    )
    return State.ADD_AMOUNT

async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму. Пример: 350 или 350.50:",
            reply_markup=cancel_kb(),
        )
        return State.ADD_AMOUNT
    context.user_data["amount"] = val
    label = "Доход 🟢" if context.user_data["ttype"] == "income" else "Расход 🔴"
    await update.message.reply_text(
        f"Проверим:\nТип: {label}\nСумма: {val:.2f}\n\n(ничего не сохраняем, это только тест)\n\nНажмите Отмена, чтобы вернуться в меню.",
        reply_markup=cancel_kb(),
    )
    return State.ADD_CONFIRM

# --------- роутер главного меню ----------

async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "menu:home":
        await q.edit_message_text("🏠 Главное меню:", reply_markup=main_menu_kb())
        return ConversationHandler.END

    if data == "menu:ops":
        await q.edit_message_text("Выберите операцию:", reply_markup=ops_menu_kb())
        return ConversationHandler.END

    if data == "ops:income":
        return await add_entry(update, context, "income")

    if data == "ops:expense":
        return await add_entry(update, context, "expense")

    return ConversationHandler.END

# --------- main ----------

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))

    conv = ConversationHandler(
        entry_points=[],   # ВСЕ входы только через main_menu_router
        states={
            State.ADD_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_amount)
            ],
            State.ADD_CONFIRM: [
                # тут только cancel, сам шаг ничего не делает
                CallbackQueryHandler(on_cancel, pattern=r"^cancel$")
            ],
        },
        fallbacks=[
            CallbackQueryHandler(on_cancel, pattern=r"^cancel$")
        ],
    )
    app.add_handler(conv)

    # Глобальный обработчик меню
    app.add_handler(
        CallbackQueryHandler(
            main_menu_router,
            pattern=r"^(menu:|ops:)"
        )
    )

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text)
    )

    logger.info("Мини-бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
