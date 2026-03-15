import os
import re
import sqlite3
import asyncio
import logging
from datetime import datetime

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

DB_PATH = "/app/data/finance.db" if os.path.exists("/app/data") else "finance.db"

DEFAULT_WALLETS = ["Наличные", "Сбер", "Тинькофф", "Альфа"]
CATEGORIES_EXPENSE = ["Еда", "Транспорт", "Дом", "Связь", "Здоровье", "Развлечения", "Другое"]
CATEGORIES_INCOME = ["Зарплата", "Подработка", "Подарок", "Возврат", "Другое"]

# ---------- БД ----------

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wallets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, name)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ttype TEXT NOT NULL,
                name TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, ttype, name)
            )
        """)
        conn.commit()
    finally:
        conn.close()

def seed_db(user_id: int):
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db_connection()
    try:
        for name in DEFAULT_WALLETS:
            conn.execute(
                "INSERT OR IGNORE INTO wallets(user_id, name, is_active, created_at) "
                "VALUES (?,?,1,?)",
                (user_id, name, now),
            )
        for n in CATEGORIES_EXPENSE:
            conn.execute(
                "INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, created_at) "
                "VALUES (?,?,?,1,?)",
                (user_id, "expense", n, now),
            )
        for n in CATEGORIES_INCOME:
            conn.execute(
                "INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, created_at) "
                "VALUES (?,?,?,1,?)",
                (user_id, "income", n, now),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Seed DB error: {e}")
        conn.rollback()
    finally:
        conn.close()

async def get_wallets(user_id: int):
    def _get():
        conn = get_db_connection()
        try:
            return conn.execute(
                "SELECT id, name FROM wallets WHERE user_id=? AND is_active=1 ORDER BY id",
                (user_id,),
            ).fetchall()
        finally:
            conn.close()
    return await asyncio.to_thread(_get)

async def get_categories(user_id: int, ttype: str):
    def _get():
        conn = get_db_connection()
        try:
            return conn.execute(
                "SELECT id, name FROM categories WHERE user_id=? AND ttype=? AND is_active=1 ORDER BY name",
                (user_id, ttype),
            ).fetchall()
        finally:
            conn.close()
    return await asyncio.to_thread(_get)

# ---------- вспомогательные ----------

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

def cancel_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])

def money_parse(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t):
        return None
    return float(t)

# ---------- хендлеры ----------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    await update.message.reply_text(
        "Старт. Нажми кнопку.",
        reply_markup=main_menu_kb()
    )

async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = q.from_user.id

    # страховка от «висячих» кнопок
    valid_prefixes = ("menu:", "ops:", "cancel", "w:", "c:")
    if not data.startswith(valid_prefixes):
        await q.answer("Не понимаю эту кнопку", show_alert=True)
        return

    if data == "menu:home":
        context.user_data.clear()
        await q.edit_message_text(
            "Главное меню.", reply_markup=main_menu_kb()
        )
        return

    if data == "menu:ops":
        context.user_data.clear()
        await q.edit_message_text(
            "Выбор операции.", reply_markup=ops_menu_kb()
        )
        return

    if data == "cancel":
        context.user_data.clear()
        await q.edit_message_text(
            "Отменено. Меню.",
            reply_markup=main_menu_kb()
        )
        return

    # шаг 1: выбор типа операции
    if data == "ops:income":
        context.user_data.clear()
        context.user_data["step"] = "amount"
        context.user_data["ttype"] = "income"
        await q.edit_message_text(
            "Вводим ДОХОД 🟢\nНапиши сумму сообщением (например 350.50):",
            reply_markup=cancel_kb(),
        )
        return

    if data == "ops:expense":
        context.user_data.clear()
        context.user_data["step"] = "amount"
        context.user_data["ttype"] = "expense"
        await q.edit_message_text(
            "Вводим РАСХОД 🔴\nНапиши сумму сообщением (например 350.50):",
            reply_markup=cancel_kb(),
        )
        return

    # шаг 2: выбор кошелька
    if data.startswith("w:"):
        if context.user_data.get("step") != "wallet":
            await q.answer("Неожиданная кнопка кошелька", show_alert=True)
            return
        wid = int(data.split(":")[1])
        context.user_data["wallet_id"] = wid
        context.user_data["step"] = "category"

        ttype = context.user_data.get("ttype") or "expense"
        cats = await get_categories(uid, ttype)
        if not cats:
            await q.edit_message_text(
                "Нет категорий. (пока без добавления)\nОтмена → меню.",
                reply_markup=cancel_kb(),
            )
            return

        rows = [
            [InlineKeyboardButton(n, callback_data=f"c:{cid}")]
            for cid, n in cats
        ]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text(
            "Выбери категорию:", reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    # шаг 3: выбор категории
    if data.startswith("c:"):
        if context.user_data.get("step") != "category":
            await q.answer("Неожиданная кнопка категории", show_alert=True)
            return
        cid = int(data.split(":")[1])
        context.user_data["category_id"] = cid
        context.user_data["step"] = "done"

        ttype = context.user_data.get("ttype")
        amount = context.user_data.get("amount")
        wallet_id = context.user_data.get("wallet_id")

        async def fetch_names():
            conn = get_db_connection()
            try:
                wname = conn.execute(
                    "SELECT name FROM wallets WHERE id=?",
                    (wallet_id,),
                ).fetchone()
                cname = conn.execute(
                    "SELECT name FROM categories WHERE id=?",
                    (cid,),
                ).fetchone()
                return (wname[0] if wname else "?"), (cname[0] if cname else "?")
            finally:
                conn.close()

        wname, cname = await asyncio.to_thread(fetch_names)

        label = "Доход 🟢" if ttype == "income" else "Расход 🔴"
        await q.edit_message_text(
            f"Проверим:\n"
            f"Тип: {label}\n"
            f"Сумма: {amount:.2f}\n"
            f"Кошелёк: {wname}\n"
            f"Категория: {cname}\n\n"
            f"(пока не сохраняю в БД, это тест)\n"
            f"Нажми ❌ Отмена, чтобы вернуться в меню.",
            reply_markup=cancel_kb(),
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("step")

    if step != "amount":
        await update.message.reply_text(
            "Жми кнопки 👇", reply_markup=main_menu_kb()
        )
        return

    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму. Пример: 350 или 350.50:",
            reply_markup=cancel_kb(),
        )
        return

    context.user_data["amount"] = val
    context.user_data["step"] = "wallet"

    uid = update.message.from_user.id
    wallets = await get_wallets(uid)
    if not wallets:
        await update.message.reply_text(
            "Нет кошельков. (пока без добавления)\nОтмена → меню.",
            reply_markup=cancel_kb(),
        )
        return

    rows = [
        [InlineKeyboardButton(n, callback_data=f"w:{wid}")]
        for wid, n in wallets
    ]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])

    await update.message.reply_text(
        "Выбери кошелёк:", reply_markup=InlineKeyboardMarkup(rows)
    )

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(
        CallbackQueryHandler(
            main_menu_router,
            pattern=r"^(menu:|ops:|cancel|w:|c:)"
        )
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Bot started (wallet + category, no ConversationHandler)")
    app.run_polling()

if __name__ == "__main__":
    main()
