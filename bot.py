import os
import re
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
        [InlineKeyboardButton("🔴 Расход", callback_data="ops:expense")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        [InlineKeyboardButton("⬅ Назад в меню", callback_data="menu:home")],
    ])

def money_parse(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t):
        return None
    return float(t)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Старт. Нажми кнопку.",
        reply_markup=main_menu_kb()
    )

async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    # Любая кнопка сбрасывает ожидание суммы
    context.user_data.pop("await_amount", None)
    context.user_data.pop("ttype", None)

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
        context.user_data["await_amount"] = True
        context.user_data["ttype"] = "income"
        await q.edit_message_text(
            "Вводим ДОХОД 🟢\nНапиши сумму сообщением (например 350.50):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
            ]),
        )
        return

    if data == "ops:expense":
        context.user_data["await_amount"] = True
        context.user_data["ttype"] = "expense"
        await q.edit_message_text(
            "Вводим РАСХОД 🔴\nНапиши сумму сообщением (например 350.50):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
            ]),
        )
        return

    if data == "cancel":
        await q.edit_message_text(
            "Отменено. Меню.",
            reply_markup=main_menu_kb()
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Если мы не ждём сумму — просим жать кнопки
    if not context.user_data.get("await_amount"):
        await update.message.reply_text(
            "Жми кнопки 👇", reply_markup=main_menu_kb()
        )
        return

    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму. Пример: 350 или 350.50:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
            ]),
        )
        return

    ttype = context.user_data.get("ttype")
    label = "Доход 🟢" if ttype == "income" else "Расход 🔴"

    # здесь НИЧЕГО не сохраняем в БД — только проверка сценария
    context.user_data.clear()
    await update.message.reply_text(
        f"Принял.\nТип: {label}\nСумма: {val:.2f}\n\nПока ничего не сохраняю, это тест.\nВернуться в меню:",
        reply_markup=main_menu_kb()
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(main_menu_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started (no ConversationHandler)")
    app.run_polling()

if __name__ == "__main__":
    main()
