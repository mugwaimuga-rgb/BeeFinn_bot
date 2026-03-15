import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Операция", callback_data="menu:ops")],
    ])

def ops_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 Доход", callback_data="ops:income")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        [InlineKeyboardButton("⬅ Назад в меню", callback_data="menu:home")],
    ])

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Старт. Нажми кнопку.",
        reply_markup=main_menu_kb()
    )

async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "menu:home":
        await q.edit_message_text(
            "Главное меню.", reply_markup=main_menu_kb()
        )
        return

    if data == "menu:ops":
        await q.edit_message_text(
            "Выбор операции.", reply_markup=ops_menu_kb()
        )
        return

    if data == "ops:income":
        await q.edit_message_text(
            "Тут был бы доход.\nЖми Отмена или Назад.",
            reply_markup=ops_menu_kb()
        )
        return

    if data == "cancel":
        # просто вернём в меню, без состояний
        await q.edit_message_text(
            "Отменено. Меню.",
            reply_markup=main_menu_kb()
        )
        return

async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Жми кнопки.", reply_markup=main_menu_kb()
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(main_menu_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))

    logger.info("Ultra-min bot started")
    app.run_polling()

if __name__ == "__main__":
    main()
