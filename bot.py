import os
import re
import uuid
import sqlite3
import warnings
import html
import asyncio
import logging
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters
)

# --- КОНФИГУРАЦИЯ ---
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=UserWarning, module="telegram.ext")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден в переменных окружения!")

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_PATH = "/app/data/finance.db" if os.path.exists("/app/data") else "finance.db"

DEFAULT_WALLETS = ["Наличные", "Сбер", "Тинькофф", "Альфа"]
CATEGORIES_EXPENSE = ["Еда", "Транспорт", "Дом", "Связь", "Здоровье", "Развлечения", "Другое"]
CATEGORIES_INCOME = ["Зарплата", "Подработка", "Подарок", "Возврат", "Другое"]
# Системные категории скрыты от пользователя, используются внутри
SYSTEM_CATEGORIES = ["Перевод", "Корректировка", "Кредит/Долг", "Платёж по долгу"]

# --- СОСТОЯНИЯ ---
class State:
    ADD_AMOUNT, ADD_WALLET, ADD_CATEGORY, ADD_NOTE, ADD_CONFIRM = range(5)
    CAT_ADD_NAME, CAT_DEL_PICK = range(5, 7)
    TR_FROM, TR_TO, TR_AMOUNT, TR_NOTE, TR_CONFIRM = range(7, 12)
    ADJ_WALLET, ADJ_TARGET, ADJ_NOTE, ADJ_CONFIRM = range(12, 16)
    W_ADD_NAME, W_ARCH_PICK = range(16, 18)
    DEBT_MENU, DEBT_NAME, DEBT_AMOUNT, DEBT_WALLET = range(18, 22)
    DEBT_PAY_PICK, DEBT_PAY_AMOUNT, DEBT_PAY_WALLET = range(22, 25)
    DEBT_ADJ_PICK, DEBT_ADJ_TARGET, DEBT_ADJ_NOTE, DEBT_ADJ_CONFIRM = range(25, 29)

# --- БАЗА ДАННЫХ ---
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS wallets(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            name TEXT NOT NULL, is_active INTEGER DEFAULT 1, created_at TEXT NOT NULL,
            UNIQUE(user_id, name))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            ttype TEXT NOT NULL, name TEXT NOT NULL, is_active INTEGER DEFAULT 1,
            is_system INTEGER DEFAULT 0, created_at TEXT NOT NULL,
            UNIQUE(user_id, ttype, name))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS debts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            debt_type TEXT NOT NULL, name TEXT NOT NULL,
            total_amount REAL NOT NULL, current_balance REAL NOT NULL,
            is_active INTEGER DEFAULT 1, created_at TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            ttype TEXT NOT NULL, amount REAL NOT NULL, wallet_id INTEGER NOT NULL,
            category TEXT NOT NULL, note TEXT, transfer_id TEXT, debt_id INTEGER,
            created_at TEXT NOT NULL)""")
        conn.commit()
    except Exception as e:
        logger.error(f"DB Init Error: {e}")
        raise
    finally:
        conn.close()

def seed_db(user_id: int):
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    try:
        for name in DEFAULT_WALLETS:
            conn.execute("INSERT OR IGNORE INTO wallets(user_id,name,is_active,created_at) VALUES (?,?,1,?)", (user_id, name, now))
        for n in CATEGORIES_EXPENSE:
            conn.execute("INSERT OR IGNORE INTO categories(user_id,ttype,name,is_active,is_system,created_at) VALUES (?,?,?,?,0,?)", (user_id, "expense", n, 1, now))
        for n in CATEGORIES_INCOME:
            conn.execute("INSERT OR IGNORE INTO categories(user_id,ttype,name,is_active,is_system,created_at) VALUES (?,?,?,?,0,?)", (user_id, "income", n, 1, now))
        # Системные категории добавляем, но помечаем как system=1
        for cat in SYSTEM_CATEGORIES:
            conn.execute("INSERT OR IGNORE INTO categories(user_id,ttype,name,is_active,is_system,created_at) VALUES (?,?,?,?,1,?)", (user_id, "expense", cat, 1, now))
            conn.execute("INSERT OR IGNORE INTO categories(user_id,ttype,name,is_active,is_system,created_at) VALUES (?,?,?,?,1,?)", (user_id, "income", cat, 1, now))
        conn.commit()
    except Exception as e:
        logger.error(f"Seed Error: {e}")
    finally:
        conn.close()

# --- УТИЛИТЫ ---
def money_parse(text: str):
    if not text: return None
    t = text.strip().replace(",", ".").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t): return None
    val = float(t)
    return val if val >= 0 else None

async def get_wallets(uid, active=True):
    conn = get_db()
    try:
        q = "SELECT id,name FROM wallets WHERE user_id=? AND is_active=1 ORDER BY id" if active else "SELECT id,name,is_active FROM wallets WHERE user_id=? ORDER BY id"
        return conn.execute(q, (uid,)).fetchall()
    finally: conn.close()

async def wallet_balance(uid, wid):
    conn = get_db()
    try:
        inc = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND wallet_id=? AND ttype='income'", (uid, wid)).fetchone()[0]
        exp = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND wallet_id=? AND ttype='expense'", (uid, wid)).fetchone()[0]
        return float(inc) - float(exp)
    finally: conn.close()

async def get_categories(uid, ttype, system=False):
    conn = get_db()
    try:
        q = "SELECT id,name FROM categories WHERE user_id=? AND ttype=? AND is_active=1 AND is_system=0 ORDER BY name" if not system else "SELECT id,name FROM categories WHERE user_id=? AND ttype=? AND is_active=1 ORDER BY name"
        return conn.execute(q, (uid, ttype)).fetchall()
    finally: conn.close()

def month_name(m): return ["Янв","Фев","Мар","Апр","Май","Июн","Июл","Авг","Сен","Окт","Ноя","Дек"][m-1]

# --- КЛАВИАТУРЫ ---
def main_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("➕ Операция", callback_data="menu:ops")],
        [InlineKeyboardButton("👛 Кошельки", callback_data="menu:wallets"), InlineKeyboardButton("📊 Статистика", callback_data="menu:stats")],
        [InlineKeyboardButton("💳 Долги", callback_data="menu:debts")]])

def ops_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🟢 Доход", callback_data="ops:income"), InlineKeyboardButton("🔴 Расход", callback_data="ops:expense")],
        [InlineKeyboardButton("🔁 Перевод", callback_data="ops:transfer")], [InlineKeyboardButton("⬅ Назад", callback_data="menu:home")]])

def wallets_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛠 Исправить баланс", callback_data="wallet:adjust")],
        [InlineKeyboardButton("➕ Добавить", callback_data="wallet:add"), InlineKeyboardButton("🗄 Архив", callback_data="wallet:archive")],
        [InlineKeyboardButton("⬅ Назад", callback_data="menu:home")]])

def cancel_kb(back=True):
    rows = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
    if back: rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)

def confirm_kb(has_note):
    txt = "📝 Изменить" if has_note else "📝 Комментарий"
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Сохранить", callback_data="confirm:save")],
        [InlineKeyboardButton(txt, callback_data="confirm:add_note")],
        [InlineKeyboardButton("⬅ Назад", callback_data="confirm:back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])

async def wallets_inline(uid, prefix, back=True):
    ws = await get_wallets(uid)
    rows = [[InlineKeyboardButton(n, callback_data=f"{prefix}:{wid}")] for wid, n in ws]
    if back: rows.append([InlineKeyboardButton("⬅ Назад", callback_data="back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    else: rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)

async def cats_inline(uid, ttype):
    cats = await get_categories(uid, ttype)
    rows, row = [], []
    for cid, name in cats:
        row.append(InlineKeyboardButton(name, callback_data=f"catpick:{name}"))
        if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("➕ Добавить", callback_data="category:add"), InlineKeyboardButton("🗑 Удалить", callback_data="category:del")])
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)

# --- ОБРАБОТЧИКИ ---
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text="🏠 Главное меню:"):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, reply_markup=main_kb(), parse_mode="HTML")
    else:
        await update.message.reply_text(text, reply_markup=main_kb(), parse_mode="HTML")

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await asyncio.to_thread(seed_db, update.message.from_user.id)
    await show_menu(update, context, "Привет! Я помогу вести учёт финансов 💸\nВыбери действие:")
    return ConversationHandler.END

async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Используйте кнопки меню 👇", reply_markup=main_kb(), parse_mode="HTML")
    return None

async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Отменено ❌")
    return ConversationHandler.END

# --- ДОХОД / РАСХОД ---
async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, ttype: str):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    context.user_data["ttype"] = ttype
    label = "ДОХОД 🟢" if ttype == "income" else "РАСХОД 🔴"
    await update.callback_query.edit_message_text(f"Вводим {label}\nСумма (например 350.50):", reply_markup=cancel_kb(False), parse_mode="HTML")
    return State.ADD_AMOUNT

async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму. Пример: 350 или 350.50:", reply_markup=cancel_kb(False))
        return State.ADD_AMOUNT
    context.user_data["amount"] = val
    kb = await wallets_inline(update.message.from_user.id, "wallet", True)
    await update.message.reply_text("Выбери кошелёк:", reply_markup=kb)
    return State.ADD_WALLET

async def add_wallet_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "back":
        await q.edit_message_text("Введи сумму:", reply_markup=cancel_kb(False))
        return State.ADD_AMOUNT
    context.user_data["wallet_id"] = int(q.data.split(":")[1])
    kb = await cats_inline(q.from_user.id, context.user_data["ttype"])
    await q.edit_message_text("Выбери статью:", reply_markup=kb)
    return State.ADD_CATEGORY

async def add_category_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "back":
        kb = await wallets_inline(q.from_user.id, "wallet", True)
        await q.edit_message_text("Выбери кошелёк:", reply_markup=kb)
        return State.ADD_WALLET
    if q.data == "category:add":
        await q.edit_message_text("Название новой статьи:", reply_markup=cancel_kb(False))
        return State.CAT_ADD_NAME
    if q.data == "category:del":
        cats = await get_categories(q.from_user.id, context.user_data["ttype"])
        rows = [[InlineKeyboardButton(n, callback_data=f"catdel:{cid}")] for cid, n in cats]
        rows.append([InlineKeyboardButton("⬅ Назад", callback_data="catdel_back")])
        await q.edit_message_text("Какую удалить?", reply_markup=InlineKeyboardMarkup(rows))
        return State.CAT_DEL_PICK
    
    context.user_data["category"] = q.data.split(":")[1]
    label = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"Проверим:\nТип: {label}\nСумма: {context.user_data['amount']:.2f}\nСтатья: {html.escape(context.user_data['category'])}\nКоммент: {note}\n\nСохранить?"
    await q.edit_message_text(msg, reply_markup=confirm_kb(bool(context.user_data.get("note"))), parse_mode="HTML")
    return State.ADD_CONFIRM

async def cat_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Название не может быть пустым:", reply_markup=cancel_kb(False))
        return State.CAT_ADD_NAME
    uid, ttype = update.message.from_user.id, context.user_data["ttype"]
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    try:
        conn.execute("INSERT INTO categories(user_id,ttype,name,is_active,is_system,created_at) VALUES (?,?,?,?,0,?)", (uid, ttype, name, 1, now))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.execute("UPDATE categories SET is_active=1 WHERE user_id=? AND ttype=? AND name=?", (uid, ttype, name))
        conn.commit()
    finally: conn.close()
    kb = await cats_inline(uid, ttype)
    await update.message.reply_text("Статья добавлена! Выбери её:", reply_markup=kb)
    return State.ADD_CATEGORY

async def cat_del_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "catdel_back":
        kb = await cats_inline(q.from_user.id, context.user_data["ttype"])
        await q.edit_message_text("Выбери статью:", reply_markup=kb)
        return State.ADD_CATEGORY
    cid = int(q.data.split(":")[1])
    conn = get_db()
    try:
        sys = conn.execute("SELECT is_system FROM categories WHERE id=? AND user_id=?", (cid, q.from_user.id)).fetchone()
        if sys and sys[0] == 1:
            await q.edit_message_text("Системную категорию нельзя удалить.", reply_markup=cancel_kb(False))
            return State.CAT_DEL_PICK
        conn.execute("UPDATE categories SET is_active=0 WHERE id=? AND user_id=?", (cid, q.from_user.id))
        conn.commit()
    finally: conn.close()
    kb = await cats_inline(q.from_user.id, context.user_data["ttype"])
    await q.edit_message_text("Удалено. Выбери статью:", reply_markup=kb)
    return State.ADD_CATEGORY

async def add_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "confirm:back":
        kb = await cats_inline(q.from_user.id, context.user_data["ttype"])
        await q.edit_message_text("Выбери статью:", reply_markup=kb)
        return State.ADD_CATEGORY
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.ADD_NOTE
    if q.data == "confirm:save":
        now = datetime.now().isoformat(timespec="seconds")
        uid = q.from_user.id
        conn = get_db()
        try:
            conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,note,created_at) VALUES (?,?,?,?,?,?,?)",
                (uid, context.user_data["ttype"], context.user_data["amount"], context.user_data["wallet_id"], context.user_data["category"], context.user_data.get("note"), now))
            conn.commit()
        finally: conn.close()
        await q.edit_message_text("Сохранено ✅", reply_markup=main_kb())
        return ConversationHandler.END

async def add_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    label = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"Проверим:\nТип: {label}\nСумма: {context.user_data['amount']:.2f}\nСтатья: {html.escape(context.user_data['category'])}\nКоммент: {note}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(True), parse_mode="HTML")
    return State.ADD_CONFIRM

# --- ПЕРЕВОД ---
async def transfer_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    kb = await wallets_inline(uid, "from", False)
    await update.callback_query.edit_message_text("🔁 Перевод\nИз какого кошелька?", reply_markup=kb, parse_mode="HTML")
    return State.TR_FROM

async def tr_pick_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    context.user_data["from_wallet_id"] = int(q.data.split(":")[1])
    kb = await wallets_inline(q.from_user.id, "to", True)
    await q.edit_message_text("В какой кошелёк?", reply_markup=kb)
    return State.TR_TO

async def tr_pick_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "back":
        kb = await wallets_inline(q.from_user.id, "from", False)
        await q.edit_message_text("Из какого кошелька?", reply_markup=kb)
        return State.TR_FROM
    to_id = int(q.data.split(":")[1])
    if to_id == context.user_data.get("from_wallet_id"):
        kb = await wallets_inline(q.from_user.id, "to", True)
        await q.edit_message_text("Нельзя в тот же кошелёк! Выбери другой:", reply_markup=kb)
        return State.TR_TO
    context.user_data["to_wallet_id"] = to_id
    context.user_data["note"] = None
    await q.edit_message_text("Сумма перевода:", reply_markup=cancel_kb(False))
    return State.TR_AMOUNT

async def tr_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return State.TR_AMOUNT
    context.user_data["amount"] = val
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"Перевод: {val:.2f}\nКоммент: {note}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(bool(context.user_data.get("note"))), parse_mode="HTML")
    return State.TR_CONFIRM

async def tr_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    val = context.user_data["amount"]
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"Перевод: {val:.2f}\nКоммент: {note}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(True), parse_mode="HTML")
    return State.TR_CONFIRM

async def tr_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Сумма перевода:", reply_markup=cancel_kb(False))
        return State.TR_AMOUNT
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.TR_NOTE
    if q.data == "confirm:save":
        uid = q.from_user.id
        now = datetime.now().isoformat(timespec="seconds")
        tid = str(uuid.uuid4())
        amt = context.user_data["amount"]
        conn = get_db()
        try:
            conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,note,transfer_id,created_at) VALUES (?,?,?,?,?,?,?,?)", (uid, "expense", amt, context.user_data["from_wallet_id"], "Перевод", context.user_data.get("note"), tid, now))
            conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,note,transfer_id,created_at) VALUES (?,?,?,?,?,?,?,?)", (uid, "income", amt, context.user_data["to_wallet_id"], "Перевод", context.user_data.get("note"), tid, now))
            conn.commit()
        finally: conn.close()
        await q.edit_message_text("Перевод сохранён ✅", reply_markup=main_kb())
        return ConversationHandler.END

# --- СТАТИСТИКА ---
async def send_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE, year: int, month: int):
    q = update.callback_query
    uid = q.from_user.id
    start = f"{year}-{month:02d}-01T00:00:00"
    end_y, end_m = (year+1, 1) if month == 12 else (year, month+1)
    end = f"{end_y}-{end_m:02d}-01T00:00:00"
    conn = get_db()
    try:
        inc = conn.execute("SELECT category,SUM(amount) FROM transactions WHERE user_id=? AND ttype='income' AND created_at>=? AND created_at<? AND category NOT IN (SELECT name FROM categories WHERE user_id=? AND is_system=1) GROUP BY category", (uid,start,end,uid)).fetchall()
        exp = conn.execute("SELECT category,SUM(amount) FROM transactions WHERE user_id=? AND ttype='expense' AND created_at>=? AND created_at<? AND category NOT IN (SELECT name FROM categories WHERE user_id=? AND is_system=1) GROUP BY category", (uid,start,end,uid)).fetchall()
    finally: conn.close()
    
    msg = f"📊 <b>Статистика за {month_name(month)} {year}</b>\n<b>🟢 Доходы:</b>\n"
    total_inc = sum(a for _,a in inc)
    msg += "\n".join([f" • {html.escape(c)}: {a:.2f}" for c,a in inc]) if inc else " Нет записей"
    msg += f"\n  <b>Итого:</b> {total_inc:.2f}\n\n<b>🔴 Расходы:</b>\n"
    total_exp = sum(a for _,a in exp)
    msg += "\n".join([f" • {html.escape(c)}: {a:.2f}" for c,a in exp]) if exp else " Нет записей"
    msg += f"\n  <b>Итого:</b> {total_exp:.2f}\n\n⚖️ <b>Баланс:</b> {total_inc-total_exp:.2f}"
    
    prev_m, prev_y = (month-1, year) if month > 1 else (12, year-1)
    next_m, next_y = (month+1, year) if month < 12 else (1, year+1)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Пред.", callback_data=f"stat:{prev_y}:{prev_m}"), InlineKeyboardButton("След. ➡", callback_data=f"stat:{next_y}:{next_m}")], [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])
    await q.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")

# --- КОШЕЛЬКИ ---
async def adjust_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    kb = await wallets_inline(uid, "adjustwallet", False)
    await update.callback_query.edit_message_text("Какой кошелёк корректируем?", reply_markup=kb)
    return State.ADJ_WALLET

async def adj_pick_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    context.user_data["wallet_id"] = int(q.data.split(":")[1])
    context.user_data["note"] = None
    await q.edit_message_text("Сколько сейчас по факту?", reply_markup=cancel_kb(False))
    return State.ADJ_TARGET

async def adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    target = money_parse(update.message.text)
    if target is None:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return State.ADJ_TARGET
    wid = context.user_data["wallet_id"]
    current = await wallet_balance(uid, wid)
    delta = target - current
    context.user_data.update({"target": target, "current": current, "delta": delta})
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"В базе: {current:.2f}\nПо факту: {target:.2f}\nДельта: {delta:.2f}\nКоммент: {note}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(bool(context.user_data.get("note"))), parse_mode="HTML")
    return State.ADJ_CONFIRM

async def adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    current, target, delta = context.user_data["current"], context.user_data["target"], context.user_data["delta"]
    note = html.escape(context.user_data.get("note") or "—")
    msg = f"В базе: {current:.2f}\nПо факту: {target:.2f}\nДельта: {delta:.2f}\nКоммент: {note}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(True), parse_mode="HTML")
    return State.ADJ_CONFIRM

async def adj_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Сколько по факту?", reply_markup=cancel_kb(False))
        return State.ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.ADJ_NOTE
    if q.data == "confirm:save":
        delta = context.user_data["delta"]
        if abs(delta) < 1e-9:
            await q.edit_message_text("Суммы совпадают ✅", reply_markup=main_kb())
            return ConversationHandler.END
        ttype = "income" if delta > 0 else "expense"
        now = datetime.now().isoformat(timespec="seconds")
        uid = q.from_user.id
        conn = get_db()
        try:
            conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,note,created_at) VALUES (?,?,?,?,?,?,?)", (uid, ttype, abs(delta), context.user_data["wallet_id"], "Корректировка", context.user_data.get("note"), now))
            conn.commit()
        finally: conn.close()
        await q.edit_message_text("Корректировка сохранена ✅", reply_markup=main_kb())
        return ConversationHandler.END

async def wallet_add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await asyncio.to_thread(seed_db, q.from_user.id)
    context.user_data.clear()
    await q.edit_message_text("Название нового кошелька:", reply_markup=cancel_kb(False))
    return State.W_ADD_NAME

async def wallet_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Название не может быть пустым:", reply_markup=cancel_kb(False))
        return State.W_ADD_NAME
    uid = update.message.from_user.id
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db()
    try:
        conn.execute("INSERT INTO wallets(user_id,name,is_active,created_at) VALUES (?,?,1,?)", (uid, name, now))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.execute("UPDATE wallets SET is_active=1 WHERE user_id=? AND name=?", (uid, name))
        conn.commit()
    finally: conn.close()
    await update.message.reply_text("Добавлен ✅", reply_markup=wallets_kb())
    return ConversationHandler.END

async def wallet_archive_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    wallets = await get_wallets(q.from_user.id)
    if not wallets:
        await q.edit_message_text("Нет активных кошельков.", reply_markup=wallets_kb())
        return ConversationHandler.END
    rows = [[InlineKeyboardButton(n, callback_data=f"warch:{wid}")] for wid, n in wallets]
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="menu:wallets")])
    await q.edit_message_text("Какой архивируем?", reply_markup=InlineKeyboardMarkup(rows))
    return State.W_ARCH_PICK

async def wallet_archive_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "menu:wallets": return await show_menu(update, context)
    conn = get_db()
    try:
        conn.execute("UPDATE wallets SET is_active=0 WHERE user_id=? AND id=?", (q.from_user.id, int(q.data.split(":")[1])))
        conn.commit()
    finally: conn.close()
    await q.edit_message_text("Архивирован ✅", reply_markup=wallets_kb())
    return ConversationHandler.END

# --- ДОЛГИ ---
def debts_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("➕ Взять в долг", callback_data="debt:add:my_debt")],
        [InlineKeyboardButton("➕ Дать в долг", callback_data="debt:add:owed_to_me")],
        [InlineKeyboardButton("💳 Платёж", callback_data="debt:pay")],
        [InlineKeyboardButton("🛠 Исправить", callback_data="debt:adjust")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])

async def debts_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
    conn = get_db()
    try:
        my = conn.execute("SELECT name,current_balance FROM debts WHERE user_id=? AND debt_type='my_debt' AND is_active=1 AND current_balance>0", (uid,)).fetchall()
        owed = conn.execute("SELECT name,current_balance FROM debts WHERE user_id=? AND debt_type='owed_to_me' AND is_active=1 AND current_balance>0", (uid,)).fetchall()
    finally: conn.close()
    msg = "💳 <b>Я должен:</b>\n" + ("\n".join([f"• {html.escape(n)}: {b:.2f}" for n,b in my]) if my else "• Нет")
    msg += "\n\n🤝 <b>Мне должны:</b>\n" + ("\n".join([f"• {html.escape(n)}: {b:.2f}" for n,b in owed]) if owed else "• Нет")
    if update.callback_query: await update.callback_query.edit_message_text(msg, reply_markup=debts_kb(), parse_mode="HTML")
    else: await update.message.reply_text(msg, reply_markup=debts_kb(), parse_mode="HTML")
    return State.DEBT_MENU

async def debt_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "menu:home": return await show_menu(update, context)
    if q.data.startswith("debt:add:"):
        context.user_data["debt_type"] = q.data.split(":")[2]
        txt = "Название кредита:" if context.user_data["debt_type"]=="my_debt" else "Кому даём в долг:"
        await q.edit_message_text(txt, reply_markup=cancel_kb(False))
        return State.DEBT_NAME
    if q.data == "debt:pay":
        conn = get_db()
        try: debts = conn.execute("SELECT id,name,current_balance FROM debts WHERE user_id=? AND is_active=1 AND current_balance>0", (q.from_user.id,)).fetchall()
        finally: conn.close()
        if not debts:
            await q.edit_message_text("Нет активных долгов.", reply_markup=debts_kb())
            return State.DEBT_MENU
        rows = [[InlineKeyboardButton(f"{html.escape(n)} ({b:.0f})", callback_data=f"debtpay:{did}")] for did,n,b in debts]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text("По какому долгу платим?", reply_markup=InlineKeyboardMarkup(rows))
        return State.DEBT_PAY_PICK
    if q.data == "debt:adjust":
        conn = get_db()
        try: debts = conn.execute("SELECT id,name,current_balance FROM debts WHERE user_id=? AND is_active=1", (q.from_user.id,)).fetchall()
        finally: conn.close()
        if not debts:
            await q.edit_message_text("Нет долгов для корректировки.", reply_markup=debts_kb())
            return State.DEBT_MENU
        rows = [[InlineKeyboardButton(f"{html.escape(n)} ({b:.0f})", callback_data=f"debtadj:{did}")] for did,n,b in debts]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text("Выбери долг:", reply_markup=InlineKeyboardMarkup(rows))
        return State.DEBT_ADJ_PICK
    return State.DEBT_MENU

async def debt_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["debt_name"] = update.message.text.strip()
    await update.message.reply_text("Сумма долга:", reply_markup=cancel_kb(False))
    return State.DEBT_AMOUNT

async def debt_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return State.DEBT_AMOUNT
    context.user_data["debt_amount"] = val
    txt = "На какой кошелёк поступило?" if context.user_data["debt_type"]=="my_debt" else "С какого кошелька дали?"
    kb = await wallets_inline(update.message.from_user.id, "debtwallet", False)
    await update.message.reply_text(txt, reply_markup=kb)
    return State.DEBT_WALLET

async def debt_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    wid = int(q.data.split(":")[1])
    amt = context.user_data["debt_amount"]
    now = datetime.now().isoformat(timespec="seconds")
    uid = q.from_user.id
    conn = get_db()
    try:
        cur = conn.execute("INSERT INTO debts(user_id,debt_type,name,total_amount,current_balance,created_at) VALUES (?,?,?,?,?,?)", (uid, context.user_data["debt_type"], context.user_data["debt_name"], amt, amt, now))
        debt_id = cur.lastrowid
        ttype = "income" if context.user_data["debt_type"]=="my_debt" else "expense"
        conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,debt_id,created_at) VALUES (?,?,?,?,?,?,?)", (uid, ttype, amt, wid, "Кредит/Долг", debt_id, now))
        conn.commit()
    finally: conn.close()
    await q.edit_message_text("Долг оформлен ✅", reply_markup=main_kb())
    return ConversationHandler.END

async def debt_pay_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    context.user_data["pay_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Сумма платежа:", reply_markup=cancel_kb(False))
    return State.DEBT_PAY_AMOUNT

async def debt_pay_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return State.DEBT_PAY_AMOUNT
    context.user_data["pay_amount"] = val
    kb = await wallets_inline(update.message.from_user.id, "paywallet", False)
    await update.message.reply_text("С какого кошелька платим?", reply_markup=kb)
    return State.DEBT_PAY_WALLET

async def debt_pay_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    wid = int(q.data.split(":")[1])
    amt = context.user_data["pay_amount"]
    did = context.user_data["pay_debt_id"]
    now = datetime.now().isoformat(timespec="seconds")
    uid = q.from_user.id
    conn = get_db()
    try:
        debt = conn.execute("SELECT debt_type,current_balance FROM debts WHERE id=?", (did,)).fetchone()
        if not debt:
            await q.edit_message_text("Долг не найден.", reply_markup=main_kb())
            return ConversationHandler.END
        debt_type, current_balance = debt
        new_bal = max(0, current_balance - amt)
        conn.execute("UPDATE debts SET current_balance=? WHERE id=?", (new_bal, did))
        ttype = "expense" if debt_type=="my_debt" else "income"
        conn.execute("INSERT INTO transactions(user_id,ttype,amount,wallet_id,category,debt_id,created_at) VALUES (?,?,?,?,?,?,?)", (uid, ttype, amt, wid, "Платёж по долгу", did, now))
        conn.commit()
    finally: conn.close()
    await q.edit_message_text(f"Платёж учтён ✅ Остаток: {new_bal:.2f}", reply_markup=main_kb())
    return ConversationHandler.END

async def debt_adj_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    context.user_data["adj_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Фактический остаток долга:", reply_markup=cancel_kb(False))
    return State.DEBT_ADJ_TARGET

async def debt_adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return State.DEBT_ADJ_TARGET
    did = context.user_data["adj_debt_id"]
    conn = get_db()
    try: old = conn.execute("SELECT current_balance,name FROM debts WHERE id=?", (did,)).fetchone()
    finally: conn.close()
    if not old:
        await update.message.reply_text("Долг не найден.", reply_markup=main_kb())
        return ConversationHandler.END
    context.user_data.update({"adj_debt_target": val, "adj_debt_old": old[0]})
    delta = val - old[0]
    msg = f"Долг: {html.escape(old[1])}\nВ базе: {old[0]:.2f}\nПо факту: {val:.2f}\nРазница: {delta:.2f}\n\nСохранить?"
    await update.message.reply_text(msg, reply_markup=confirm_kb(False), parse_mode="HTML")
    return State.DEBT_ADJ_CONFIRM

async def debt_adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip()
    await update.message.reply_text("Коммент добавлен. Сохранить?", reply_markup=confirm_kb(True))
    return State.DEBT_ADJ_CONFIRM

async def debt_adj_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel": return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Фактический остаток:", reply_markup=cancel_kb(False))
        return State.DEBT_ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.DEBT_ADJ_NOTE
    if q.data == "confirm:save":
        val = context.user_data["adj_debt_target"]
        did = context.user_data["adj_debt_id"]
        conn = get_db()
        try:
            conn.execute("UPDATE debts SET current_balance=? WHERE id=?", (val, did))
            conn.commit()
        finally: conn.close()
        await q.edit_message_text("Скорректировано ✅", reply_markup=main_kb())
        return ConversationHandler.END

# --- ROUTER ---
async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "menu:home":
        await q.edit_message_text("🏠 Главное меню:", reply_markup=main_kb())
        return ConversationHandler.END
    if data == "menu:ops":
        await q.edit_message_text("Выберите операцию:", reply_markup=ops_kb())
        return ConversationHandler.END
    if data in ("ops:income", "ops:expense"):
        return await add_entry(update, context, "income" if data=="ops:income" else "expense")
    if data == "ops:transfer": return await transfer_entry(update, context)
    if data == "menu:stats":
        now = datetime.now()
        await send_statistics(update, context, now.year, now.month)
        return ConversationHandler.END
    if data.startswith("stat:"):
        _, y, m = data.split(":")
        await send_statistics(update, context, int(y), int(m))
        return ConversationHandler.END
    if data == "menu:wallets":
        uid = q.from_user.id
        wallets = await get_wallets(uid, False)
        lines = []
        total = 0
        for wid, n, act in wallets:
            bal = await wallet_balance(uid, wid)
            if act:
                lines.append(f"{html.escape(n)}: {bal:.2f}")
                total += bal
        lines.append(f"\n<b>Итого: {total:.2f}</b>")
        await q.edit_message_text("\n".join(lines), reply_markup=wallets_kb(), parse_mode="HTML")
        return ConversationHandler.END
    if data == "wallet:adjust": return await adjust_entry(update, context)
    if data == "wallet:add": return await wallet_add_entry(update, context)
    if data == "wallet:archive": return await wallet_archive_entry(update, context)
    if data == "menu:debts": return await debts_entry(update, context)
    return ConversationHandler.END

# --- MAIN ---
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(main_menu_router, pattern=r"^(menu:|ops:|wallet:|stat:|debt:|category|catpick:|catdel:|catdel_back|confirm|from|to|debtpay:|debtadj:|adjustwallet:|paywallet:|debtwallet:|warch:|back|cancel)")],
        states={
            State.ADD_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_amount)],
            State.ADD_WALLET: [CallbackQueryHandler(add_wallet_pick)],
            State.ADD_CATEGORY: [CallbackQueryHandler(add_category_pick)],
            State.ADD_CONFIRM: [CallbackQueryHandler(add_confirm_buttons)],
            State.ADD_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_note)],
            State.CAT_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_add_name)],
            State.CAT_DEL_PICK: [CallbackQueryHandler(cat_del_pick)],
            State.TR_FROM: [CallbackQueryHandler(tr_pick_from)],
            State.TR_TO: [CallbackQueryHandler(tr_pick_to)],
            State.TR_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tr_amount)],
            State.TR_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tr_note)],
            State.TR_CONFIRM: [CallbackQueryHandler(tr_confirm_buttons)],
            State.ADJ_WALLET: [CallbackQueryHandler(adj_pick_wallet)],
            State.ADJ_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, adj_target)],
            State.ADJ_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, adj_note)],
            State.ADJ_CONFIRM: [CallbackQueryHandler(adj_confirm_buttons)],
            State.DEBT_MENU: [CallbackQueryHandler(debt_menu_handler)],
            State.DEBT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_name)],
            State.DEBT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_amount)],
            State.DEBT_WALLET: [CallbackQueryHandler(debt_wallet)],
            State.DEBT_PAY_PICK: [CallbackQueryHandler(debt_pay_pick)],
            State.DEBT_PAY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_pay_amount)],
            State.DEBT_PAY_WALLET: [CallbackQueryHandler(debt_pay_wallet)],
            State.DEBT_ADJ_PICK: [CallbackQueryHandler(debt_adj_pick)],
            State.DEBT_ADJ_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_target)],
            State.DEBT_ADJ_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_note)],
            State.DEBT_ADJ_CONFIRM: [CallbackQueryHandler(debt_adj_confirm)],
            State.W_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_name)],
            State.W_ARCH_PICK: [CallbackQueryHandler(wallet_archive_pick)],
        },
        fallbacks=[CallbackQueryHandler(on_cancel, pattern=r"^cancel$")],
    )
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))
    logger.info("✅ Бот запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()
