import os
import re
import uuid
import sqlite3
import warnings
import html
import asyncio
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters
)

# Отключаем предупреждения PTB
warnings.filterwarnings("ignore", category=UserWarning, module="telegram.ext")

# 🔐 ТОЛЬКО через переменную окружения — без fallback!
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден в переменных окружения!")

# 👑 Ваш Telegram ID для админ-прав
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Путь к БД для Bothost
DB_PATH = "/app/data/finance.db" if os.path.exists("/app/data") else "finance.db"

DEFAULT_WALLETS = ["Наличные", "Сбер", "Тинькофф", "Альфа"]
CATEGORIES_EXPENSE = ["Еда", "Транспорт", "Дом", "Связь", "Здоровье", "Развлечения", "Другое"]
CATEGORIES_INCOME = ["Зарплата", "Подработка", "Подарок", "Возврат", "Другое"]

# Состояния (States)
(
    ADD_AMOUNT, ADD_WALLET, ADD_CATEGORY, ADD_NOTE, ADD_CONFIRM,
    CAT_ADD_NAME, CAT_DEL_PICK,
    TR_FROM, TR_TO, TR_AMOUNT, TR_NOTE, TR_CONFIRM,
    ADJ_WALLET, ADJ_TARGET, ADJ_NOTE, ADJ_CONFIRM,
    W_ADD_NAME, W_ARCH_PICK,
    DEBT_MENU, DEBT_NAME, DEBT_AMOUNT, DEBT_WALLET,
    DEBT_PAY_PICK, DEBT_PAY_AMOUNT, DEBT_PAY_WALLET,
    DEBT_ADJ_PICK, DEBT_ADJ_TARGET, DEBT_ADJ_NOTE, DEBT_ADJ_CONFIRM
) = range(29)

# ---------------- База Данных ----------------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db_connection()
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
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
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, ttype, name)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS debts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            debt_type TEXT NOT NULL,
            name TEXT NOT NULL,
            total_amount REAL NOT NULL,
            current_balance REAL NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ttype TEXT NOT NULL,
            amount REAL NOT NULL,
            wallet_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            note TEXT,
            transfer_id TEXT,
            debt_id INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY(wallet_id) REFERENCES wallets(id),
            FOREIGN KEY(debt_id) REFERENCES debts(id)
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
            conn.execute("INSERT OR IGNORE INTO wallets(user_id, name, is_active, created_at) VALUES (?,?,1,?)", (user_id, name, now))
        for n in CATEGORIES_EXPENSE:
            conn.execute("INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, created_at) VALUES (?,?,?,?,?)", (user_id, "expense", n, 1, now))
        for n in CATEGORIES_INCOME:
            conn.execute("INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, created_at) VALUES (?,?,?,?,?)", (user_id, "income", n, 1, now))
        conn.commit()
    finally:
        conn.close()

# ---------------- Вспомогательные функции ----------------
def money_parse(text: str):
    if not text:
        return None
    t = text.strip().replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t):
        return None
    val = float(t)
    return val if val >= 0 else None

def get_wallets(user_id: int, active_only=True):
    conn = get_db_connection()
    try:
        query = "SELECT id, name FROM wallets WHERE user_id=? AND is_active=1 ORDER BY id" if active_only else "SELECT id, name, is_active FROM wallets WHERE user_id=? ORDER BY id"
        return conn.execute(query, (user_id,)).fetchall()
    finally:
        conn.close()

def wallet_balance(user_id: int, wallet_id: int) -> float:
    conn = get_db_connection()
    try:
        inc = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND wallet_id=? AND ttype='income'", (user_id, wallet_id)).fetchone()[0]
        exp = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND wallet_id=? AND ttype='expense'", (user_id, wallet_id)).fetchone()[0]
        return float(inc) - float(exp)
    finally:
        conn.close()

def all_balances(user_id: int):
    wallets = get_wallets(user_id, active_only=False)
    out, total = [], 0.0
    for wid, name, is_active in wallets:
        bal = wallet_balance(user_id, wid)
        out.append((wid, name, bool(is_active), bal))
        if is_active:
            total += bal
    return out, total

def get_categories(user_id: int, ttype: str):
    conn = get_db_connection()
    try:
        return conn.execute("SELECT id, name FROM categories WHERE user_id=? AND ttype=? AND is_active=1 ORDER BY name", (user_id, ttype)).fetchall()
    finally:
        conn.close()

def get_month_name(m: int):
    return ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"][m-1]

# ---------------- Клавиатуры (Меню) ----------------
def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Операция", callback_data="menu:ops")],
        [InlineKeyboardButton("👛 Кошельки", callback_data="menu:wallets"), InlineKeyboardButton("📊 Статистика", callback_data="menu:stats")],
        [InlineKeyboardButton("💳 Долги / Кредиты", callback_data="menu:debts")],
    ])

def ops_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢➕ Доход", callback_data="ops:income"), InlineKeyboardButton("🔴➖ Расход", callback_data="ops:expense")],
        [InlineKeyboardButton("🔁 Перевод", callback_data="ops:transfer")],
        [InlineKeyboardButton("⬅ Назад в меню", callback_data="menu:home")],
    ])

def wallets_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛠 Исправить баланс", callback_data="w:adjust")],
        [InlineKeyboardButton("➕ Добавить кошелёк", callback_data="w:add")],
        [InlineKeyboardButton("🗄 Архивировать кошелёк", callback_data="w:archive")],
        [InlineKeyboardButton("⬅ Назад в меню", callback_data="menu:home")],
    ])

def cancel_kb(back_to_menu=True):
    rows = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
    if back_to_menu:
        rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)

def kb_confirm(note_exists: bool):
    txt = "📝 Изменить коммент" if note_exists else "📝 Комментарий"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Сохранить", callback_data="confirm:save")],
        [InlineKeyboardButton(txt, callback_data="confirm:add_note")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="confirm:back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
    ])

def kb_wallet_pick(user_id: int, prefix: str, add_back=True):
    rows = [[InlineKeyboardButton(name, callback_data=f"{prefix}:{wid}")] for wid, name in get_wallets(user_id, True)]
    back_row = [InlineKeyboardButton("⬅️ Назад", callback_data="back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")] if add_back else [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
    rows.append(back_row)
    return InlineKeyboardMarkup(rows)

def kb_categories(user_id: int, ttype: str):
    cats = get_categories(user_id, ttype)
    rows, row = [], []
    for cid, name in cats:
        row.append(InlineKeyboardButton(name, callback_data=f"catpick:{name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("➕ Добавить статью", callback_data="cat:manage:add"), InlineKeyboardButton("🗑 Удалить статью", callback_data="cat:manage:del")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text="🏠 Главное меню:"):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, reply_markup=main_menu_kb(), parse_mode="HTML")
    else:
        await update.message.reply_text(text, reply_markup=main_menu_kb(), parse_mode="HTML")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await asyncio.to_thread(seed_db, update.message.from_user.id)
    await show_menu(update, context, "Привет! Я помогу тебе вести учет финансов 💸\nВыбери действие:")
    return ConversationHandler.END

async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Я не понимаю текстовые сообщения вне команд.\nПожалуйста, воспользуйтесь кнопками меню 👇", reply_markup=main_menu_kb(), parse_mode="HTML")
    return None

async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Действие отменено ❌\nВыберите действие:")
    return ConversationHandler.END

# ---------------- СТАТИСТИКА ----------------
async def send_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE, year: int, month: int):
    q = update.callback_query
    user_id = q.from_user.id
    start_str = f"{year}-{month:02d}-01T00:00:00"
    end_y, end_m = (year + 1, 1) if month == 12 else (year, month + 1)
    end_str = f"{end_y}-{end_m:02d}-01T00:00:00"
    
    def fetch_stats():
        conn = get_db_connection()
        try:
            inc = conn.execute("SELECT category, SUM(amount) FROM transactions WHERE user_id=? AND ttype='income' AND created_at >= ? AND created_at < ? AND category != 'Перевод' AND category != 'Корректировка' GROUP BY category", (user_id, start_str, end_str)).fetchall()
            exp = conn.execute("SELECT category, SUM(amount) FROM transactions WHERE user_id=? AND ttype='expense' AND created_at >= ? AND created_at < ? AND category != 'Перевод' AND category != 'Корректировка' GROUP BY category", (user_id, start_str, end_str)).fetchall()
            return inc, exp
        finally:
            conn.close()
    
    inc, exp = await asyncio.to_thread(fetch_stats)
    
    msg = f"📊 <b>Статистика за {get_month_name(month)} {year}</b>\n"
    msg += "<b>🟢 Доходы:</b>\n"
    total_inc = sum(amt for cat, amt in inc)
    if inc:
        for cat, amt in inc:
            msg += f" • {html.escape(cat)}: {amt:.2f}\n"
    else:
        msg += " Нет записей\n"
    msg += f"  <b>Итого:</b> {total_inc:.2f}\n\n"
    msg += "<b>🔴 Расходы:</b>\n"
    total_exp = sum(amt for cat, amt in exp)
    if exp:
        for cat, amt in exp:
            msg += f" • {html.escape(cat)}: {amt:.2f}\n"
    else:
        msg += " Нет записей\n"
    msg += f"  <b>Итого:</b> {total_exp:.2f}\n\n"
    msg += f"⚖️ <b>Баланс за период:</b> {total_inc - total_exp:.2f}"
    
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Пред. месяц", callback_data=f"stat:{prev_y}:{prev_m}"), InlineKeyboardButton("След. ➡️", callback_data=f"stat:{next_y}:{next_m}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="menu:home")]
    ])
    await q.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")

# ---------------- ДОХОД / РАСХОД ----------------
async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, preset_type: str):
    user_id = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, user_id)
    context.user_data.clear()
    context.user_data["ttype"] = preset_type
    type_ru = "ДОХОД 🟢" if preset_type == "income" else "РАСХОД 🔴"
    await update.callback_query.edit_message_text(f"Вводим {type_ru}\nВведи сумму (например 350.50):", reply_markup=cancel_kb(False), parse_mode="HTML")
    return ADD_AMOUNT

async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму. Пример: 350 или 350.50:", reply_markup=cancel_kb(False))
        return ADD_AMOUNT
    context.user_data["amount"] = val
    await update.message.reply_text("Выбери кошелёк:", reply_markup=kb_wallet_pick(update.message.from_user.id, "w", add_back=True))
    return ADD_WALLET

async def add_wallet_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        await q.edit_message_text("Введи сумму:", reply_markup=cancel_kb(False))
        return ADD_AMOUNT
    context.user_data["wallet_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Выбери статью:", reply_markup=kb_categories(q.from_user.id, context.user_data["ttype"]))
    return ADD_CATEGORY

async def add_category_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        await q.edit_message_text("Выбери кошелёк:", reply_markup=kb_wallet_pick(q.from_user.id, "w", add_back=True))
        return ADD_WALLET
    if q.data == "cat:manage:add":
        await q.edit_message_text("Введи название новой статьи (короткой фразой):", reply_markup=cancel_kb(False))
        return CAT_ADD_NAME
    if q.data == "cat:manage:del":
        cats = get_categories(q.from_user.id, context.user_data["ttype"])
        rows = [[InlineKeyboardButton(name, callback_data=f"catdel:{cid}")] for cid, name in cats]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="catdel_back")])
        await q.edit_message_text("Какую статью удалить?", reply_markup=InlineKeyboardMarkup(rows))
        return CAT_DEL_PICK
    context.user_data["category"] = q.data.split(":")[1]
    ttype_ru = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get('note') or "—")
    await q.edit_message_text(f"Проверим:\nТип: {ttype_ru}\nСумма: {context.user_data['amount']:.2f}\nСтатья: {html.escape(context.user_data['category'])}\nКоммент: {note}\n\nСохранить?", reply_markup=kb_confirm(bool(context.user_data.get("note"))), parse_mode="HTML")
    return ADD_CONFIRM

async def cat_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    conn = get_db_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, created_at) VALUES (?,?,?,?,?)", (update.message.from_user.id, context.user_data["ttype"], name, 1, datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    finally:
        conn.close()
    await update.message.reply_text("Статья добавлена! Теперь выбери её:", reply_markup=kb_categories(update.message.from_user.id, context.user_data["ttype"]))
    return ADD_CATEGORY

async def cat_del_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "catdel_back":
        await q.edit_message_text("Выбери статью:", reply_markup=kb_categories(q.from_user.id, context.user_data["ttype"]))
        return ADD_CATEGORY
    cid = int(q.data.split(":")[1])
    conn = get_db_connection()
    try:
        conn.execute("UPDATE categories SET is_active=0 WHERE id=? AND user_id=?", (cid, q.from_user.id))
        conn.commit()
    finally:
        conn.close()
    await q.edit_message_text("Статья удалена. Выбери статью:", reply_markup=kb_categories(q.from_user.id, context.user_data["ttype"]))
    return ADD_CATEGORY

async def add_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Выбери статью:", reply_markup=kb_categories(q.from_user.id, context.user_data["ttype"]))
        return ADD_CATEGORY
    if q.data == "confirm:add_note":
        await q.edit_message_text("Напиши комментарий:", reply_markup=cancel_kb(False))
        return ADD_NOTE
    if q.data == "confirm:save":
        now = datetime.now().isoformat(timespec="seconds")
        conn = get_db_connection()
        try:
            conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, note, created_at) VALUES (?,?,?,?,?,?,?)", (q.from_user.id, context.user_data["ttype"], context.user_data["amount"], context.user_data["wallet_id"], context.user_data["category"], context.user_data.get("note"), now))
            conn.commit()
        finally:
            conn.close()
        await q.edit_message_text("Операция сохранена ✅", reply_markup=main_menu_kb())
        return ConversationHandler.END

async def add_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    ttype_ru = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get('note') or "—")
    await update.message.reply_text(f"Проверим:\nТип: {ttype_ru}\nСумма: {context.user_data['amount']:.2f}\nСтатья: {html.escape(context.user_data['category'])}\nКоммент: {note}\n\nСохранить?", reply_markup=kb_confirm(True), parse_mode="HTML")
    return ADD_CONFIRM

# ---------------- ПЕРЕВОД МЕЖДУ КОШЕЛЬКАМИ ----------------
async def transfer_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, user_id)
    context.user_data.clear()
    kb = kb_wallet_pick(user_id, "from", add_back=False)
    await update.callback_query.edit_message_text("🔁 Перевод\nИз какого кошелька переводим?", reply_markup=kb, parse_mode="HTML")
    return TR_FROM

async def tr_pick_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    _, wid = q.data.split(":", 1)
    context.user_data["from_wallet_id"] = int(wid)
    kb = kb_wallet_pick(q.from_user.id, "to", add_back=True)
    await q.edit_message_text("В какой кошелёк переводим?", reply_markup=kb)
    return TR_TO

async def tr_pick_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        kb = kb_wallet_pick(q.from_user.id, "from", add_back=False)
        await q.edit_message_text("Из какого кошелька переводим?", reply_markup=kb)
        return TR_FROM
    _, wid = q.data.split(":", 1)
    to_id = int(wid)
    if to_id == context.user_data.get("from_wallet_id"):
        kb = kb_wallet_pick(q.from_user.id, "to", add_back=True)
        await q.edit_message_text("Нельзя перевести в тот же кошелёк! Выбери другой:", reply_markup=kb)
        return TR_TO
    context.user_data["to_wallet_id"] = to_id
    context.user_data["note"] = None
    await q.edit_message_text("Введи сумму перевода:", reply_markup=cancel_kb(False))
    return TR_AMOUNT

async def tr_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text("Не понял сумму. Пример: 1000 или 1000.50:", reply_markup=cancel_kb(False))
        return TR_AMOUNT
    context.user_data["amount"] = val
    note_show = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(f"Перевод: {val:.2f}\nКомментарий: {note_show}\n\nСохранить?", reply_markup=kb_confirm(note_exists=bool(context.user_data.get("note"))), parse_mode="HTML")
    return TR_CONFIRM

async def tr_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    note = update.message.text.strip()
    context.user_data["note"] = note if note else None
    val = context.user_data["amount"]
    note_show = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(f"Перевод: {val:.2f}\nКомментарий: {note_show}\n\nСохранить?", reply_markup=kb_confirm(note_exists=bool(context.user_data.get("note"))), parse_mode="HTML")
    return TR_CONFIRM

async def tr_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Введи сумму перевода:", reply_markup=cancel_kb(False))
        return TR_AMOUNT
    if q.data == "confirm:add_note":
        await q.edit_message_text("Напиши комментарий к переводу:", reply_markup=cancel_kb(False))
        return TR_NOTE
    if q.data == "confirm:save":
        user_id = q.from_user.id
        now = datetime.now().isoformat(timespec="seconds")
        transfer_id = str(uuid.uuid4())
        amt = context.user_data["amount"]
        conn = get_db_connection()
        try:
            conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, note, transfer_id, created_at) VALUES (?,?,?,?,?,?,?,?)", (user_id, "expense", amt, context.user_data["from_wallet_id"], "Перевод", context.user_data.get("note"), transfer_id, now))
            conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, note, transfer_id, created_at) VALUES (?,?,?,?,?,?,?,?)", (user_id, "income", amt, context.user_data["to_wallet_id"], "Перевод", context.user_data.get("note"), transfer_id, now))
            conn.commit()
        finally:
            conn.close()
        await q.edit_message_text("Перевод сохранён ✅", reply_markup=main_menu_kb())
        return ConversationHandler.END

# ---------------- Долги и Кредиты ----------------
def kb_debts_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Взять в долг / Кредит", callback_data="debt:add:my_debt")],
        [InlineKeyboardButton("➕ Дать в долг", callback_data="debt:add:owed_to_me")],
        [InlineKeyboardButton("💳 Внести платеж", callback_data="debt:pay")],
        [InlineKeyboardButton("🛠 Исправить остаток", callback_data="debt:adjust")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="menu:home")]
    ])

async def debts_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id if update.callback_query else update.message.from_user.id
    def fetch_debts():
        conn = get_db_connection()
        try:
            my_debts = conn.execute("SELECT name, current_balance FROM debts WHERE user_id=? AND debt_type='my_debt' AND is_active=1 AND current_balance > 0", (user_id,)).fetchall()
            owed_me = conn.execute("SELECT name, current_balance FROM debts WHERE user_id=? AND debt_type='owed_to_me' AND is_active=1 AND current_balance > 0", (user_id,)).fetchall()
            return my_debts, owed_me
        finally:
            conn.close()
    
    my_debts, owed_me = await asyncio.to_thread(fetch_debts)
    
    msg = "💳 <b>Мои долги и кредиты (я должен):</b>\n"
    msg += "\n".join([f"• {html.escape(n)}: {b:.2f}" for n, b in my_debts]) if my_debts else "• Нет активных долгов"
    msg += "\n\n🤝 <b>Должны мне:</b>\n"
    msg += "\n".join([f"• {html.escape(n)}: {b:.2f}" for n, b in owed_me]) if owed_me else "• Никто не должен"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=kb_debts_menu(), parse_mode="HTML")
    else:
        await update.message.reply_text(msg, reply_markup=kb_debts_menu(), parse_mode="HTML")
    return DEBT_MENU

async def debt_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "menu:home":
        return await show_menu(update, context)
    if q.data.startswith("debt:add:"):
        context.user_data["debt_type"] = q.data.split(":")[2]
        txt = "Название кредита (например: Сбербанк Ипотека):" if context.user_data["debt_type"] == "my_debt" else "Кому даем в долг (Имя):"
        await q.edit_message_text(txt, reply_markup=cancel_kb(False))
        return DEBT_NAME
    if q.data == "debt:pay":
        def fetch_pay_debts():
            conn = get_db_connection()
            try:
                return conn.execute("SELECT id, name, current_balance FROM debts WHERE user_id=? AND is_active=1 AND current_balance > 0", (q.from_user.id,)).fetchall()
            finally:
                conn.close()
        debts = await asyncio.to_thread(fetch_pay_debts)
        if not debts:
            await q.edit_message_text("Нет активных долгов для оплаты.", reply_markup=kb_debts_menu())
            return DEBT_MENU
        rows = [[InlineKeyboardButton(f"{html.escape(n)} ({b:.0f})", callback_data=f"dpay:{did}")] for did, n, b in debts]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text("По какому долгу вносим платеж/возврат?", reply_markup=InlineKeyboardMarkup(rows))
        return DEBT_PAY_PICK
    if q.data == "debt:adjust":
        def fetch_adj_debts():
            conn = get_db_connection()
            try:
                return conn.execute("SELECT id, name, current_balance FROM debts WHERE user_id=? AND is_active=1", (q.from_user.id,)).fetchall()
            finally:
                conn.close()
        debts = await asyncio.to_thread(fetch_adj_debts)
        if not debts:
            await q.edit_message_text("Нет долгов для корректировки.", reply_markup=kb_debts_menu())
            return DEBT_MENU
        rows = [[InlineKeyboardButton(f"{html.escape(n)} ({b:.0f})", callback_data=f"dadj:{did}")] for did, n, b in debts]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text("Выбери долг для корректировки:", reply_markup=InlineKeyboardMarkup(rows))
        return DEBT_ADJ_PICK

async def debt_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["debt_name"] = update.message.text.strip()
    await update.message.reply_text("Введите сумму долга:", reply_markup=cancel_kb(False))
    return DEBT_AMOUNT

async def debt_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму. Пример: 10000", reply_markup=cancel_kb(False))
        return DEBT_AMOUNT
    context.user_data["debt_amount"] = val
    txt = "На какой кошелек поступили деньги?" if context.user_data["debt_type"] == "my_debt" else "С какого кошелька дали в долг?"
    await update.message.reply_text(txt, reply_markup=kb_wallet_pick(update.message.from_user.id, "dw", add_back=False))
    return DEBT_WALLET

async def debt_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    wid = int(q.data.split(":")[1])
    amt = context.user_data["debt_amount"]
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db_connection()
    try:
        cur = conn.execute("INSERT INTO debts(user_id, debt_type, name, total_amount, current_balance, created_at) VALUES (?,?,?,?,?,?)", (q.from_user.id, context.user_data["debt_type"], context.user_data["debt_name"], amt, amt, now))
        debt_id = cur.lastrowid
        ttype = "income" if context.user_data["debt_type"] == "my_debt" else "expense"
        conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, debt_id, created_at) VALUES (?,?,?,?,?,?,?)", (q.from_user.id, ttype, amt, wid, "Кредит/Долг", debt_id, now))
        conn.commit()
    finally:
        conn.close()
    await q.edit_message_text("Долг оформлен ✅", reply_markup=main_menu_kb())
    return ConversationHandler.END

async def debt_pay_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    context.user_data["pay_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Сумма платежа/возврата:", reply_markup=cancel_kb(False))
    return DEBT_PAY_AMOUNT

async def debt_pay_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if not val:
        await update.message.reply_text("Не понял сумму:", reply_markup=cancel_kb(False))
        return DEBT_PAY_AMOUNT
    context.user_data["pay_amount"] = val
    await update.message.reply_text("С какого кошелька платим (или на какой вернули долг)?", reply_markup=kb_wallet_pick(update.message.from_user.id, "pw", add_back=False))
    return DEBT_PAY_WALLET

async def debt_pay_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    wid = int(q.data.split(":")[1])
    amt = context.user_data["pay_amount"]
    did = context.user_data["pay_debt_id"]
    now = datetime.now().isoformat(timespec="seconds")
    def process_debt_payment():
        conn = get_db_connection()
        try:
            dtype = conn.execute("SELECT debt_type, current_balance FROM debts WHERE id=?", (did,)).fetchone()
            if not dtype:
                return None
            new_bal = max(0, dtype[1] - amt)
            conn.execute("UPDATE debts SET current_balance=? WHERE id=?", (new_bal, did))
            ttype = "expense" if dtype[0] == "my_debt" else "income"
            conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, debt_id, created_at) VALUES (?,?,?,?,?,?,?)", (q.from_user.id, ttype, amt, wid, "Платеж по долгу", did, now))
            conn.commit()
            return new_bal
        finally:
            conn.close()
    
    new_bal = await asyncio.to_thread(process_debt_payment)
    if new_bal is None:
        return await on_cancel(update, context)
    
    await q.edit_message_text(f"Платеж учтен ✅ Остаток долга: {new_bal:.2f}", reply_markup=main_menu_kb())
    return ConversationHandler.END

# ---------------- Корректировка долгов ----------------
async def debt_adj_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    context.user_data["adj_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Введи, какой сейчас по факту остаток долга (например, 5000):", reply_markup=cancel_kb(False))
    return DEBT_ADJ_TARGET

async def debt_adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text("Не понял сумму. Введи ещё раз:", reply_markup=cancel_kb(False))
        return DEBT_ADJ_TARGET
    did = context.user_data["adj_debt_id"]
    def fetch_debt_info():
        conn = get_db_connection()
        try:
            return conn.execute("SELECT current_balance, name FROM debts WHERE id=?", (did,)).fetchone()
        finally:
            conn.close()
    
    old_bal = await asyncio.to_thread(fetch_debt_info)
    
    context.user_data["adj_debt_target"] = val
    context.user_data["adj_debt_old"] = old_bal[0]
    delta = val - old_bal[0]
    await update.message.reply_text(f"Долг: {html.escape(old_bal[1])}\nВ базе: {old_bal[0]:.2f}\nПо факту: {val:.2f}\nРазница: {delta:.2f}\n\nСохранить корректировку?", reply_markup=kb_confirm(False), parse_mode="HTML")
    return DEBT_ADJ_CONFIRM

async def debt_adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip()
    await update.message.reply_text("Коммент добавлен. Сохранить корректировку?", reply_markup=kb_confirm(True))
    return DEBT_ADJ_CONFIRM

async def debt_adj_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Введи фактический остаток долга:", reply_markup=cancel_kb(False))
        return DEBT_ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Введи комментарий:", reply_markup=cancel_kb(False))
        return DEBT_ADJ_NOTE
    if q.data == "confirm:save":
        val = context.user_data["adj_debt_target"]
        did = context.user_data["adj_debt_id"]
        conn = get_db_connection()
        try:
            conn.execute("UPDATE debts SET current_balance=? WHERE id=?", (val, did))
            conn.commit()
        finally:
            conn.close()
        await q.edit_message_text("Остаток долга скорректирован ✅", reply_markup=main_menu_kb())
        return ConversationHandler.END

# ---------------- КОРРЕКТИРОВКА КОШЕЛЬКОВ ----------------
async def adjust_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, user_id)
    context.user_data.clear()
    kb = kb_wallet_pick(user_id, "adjw", add_back=False)
    await update.callback_query.edit_message_text("Какой кошелёк корректируем?", reply_markup=kb)
    return ADJ_WALLET

async def adj_pick_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    _, wid = q.data.split(":", 1)
    context.user_data["wallet_id"] = int(wid)
    context.user_data["note"] = None
    await q.edit_message_text("Введи, сколько на этом кошельке СЕЙЧАС по факту (например: 12500):", reply_markup=cancel_kb(False))
    return ADJ_TARGET

async def adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    target_val = money_parse(update.message.text)
    if target_val is None:
        await update.message.reply_text("Не понял сумму. Пример: 12500 или 12500.50:", reply_markup=cancel_kb(False))
        return ADJ_TARGET
    wid = context.user_data["wallet_id"]
    current = wallet_balance(user_id, wid)
    delta = target_val - current
    context.user_data["target"] = target_val
    context.user_data["current"] = current
    context.user_data["delta"] = delta
    note_show = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(f"В базе: {current:.2f}\nПо факту: {target_val:.2f}\nДельта: {delta:.2f}\nКоммент: {note_show}\n\nСохранить?", reply_markup=kb_confirm(bool(context.user_data.get("note"))), parse_mode="HTML")
    return ADJ_CONFIRM

async def adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    note = update.message.text.strip()
    context.user_data["note"] = note if note else None
    current, target_val, delta = context.user_data["current"], context.user_data["target"], context.user_data["delta"]
    note_show = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(f"В базе: {current:.2f}\nПо факту: {target_val:.2f}\nДельта: {delta:.2f}\nКоммент: {note_show}\n\nСохранить?", reply_markup=kb_confirm(note_exists=True), parse_mode="HTML")
    return ADJ_CONFIRM

async def adj_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Введи, сколько сейчас по факту:", reply_markup=cancel_kb(False))
        return ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Напиши комментарий к корректировке:", reply_markup=cancel_kb(False))
        return ADJ_NOTE
    if q.data == "confirm:save":
        delta = context.user_data["delta"]
        if abs(delta) < 1e-9:
            await q.edit_message_text("Суммы совпадают, корректировка не нужна ✅", reply_markup=main_menu_kb())
            return ConversationHandler.END
        ttype = "income" if delta > 0 else "expense"
        now = datetime.now().isoformat(timespec="seconds")
        conn = get_db_connection()
        try:
            conn.execute("INSERT INTO transactions(user_id, ttype, amount, wallet_id, category, note, created_at) VALUES (?,?,?,?,?,?,?)", (q.from_user.id, ttype, abs(delta), context.user_data["wallet_id"], "Корректировка", context.user_data.get("note"), now))
            conn.commit()
        finally:
            conn.close()
        await q.edit_message_text("Корректировка сохранена ✅", reply_markup=main_menu_kb())
        return ConversationHandler.END

# ---------------- КОШЕЛЬКИ ----------------
async def wallet_add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await asyncio.to_thread(seed_db, q.from_user.id)
    context.user_data.clear()
    await q.edit_message_text("Введи название нового кошелька:", reply_markup=cancel_kb(False))
    return W_ADD_NAME

async def wallet_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Название пустое. Введи ещё раз:", reply_markup=cancel_kb(False))
        return W_ADD_NAME
    user_id = update.message.from_user.id
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO wallets(user_id, name, is_active, created_at) VALUES (?,?,1,?)", (user_id, name, datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    except sqlite3.IntegrityError:
        await update.message.reply_text("Такой кошелёк уже есть. Введи другое:", reply_markup=cancel_kb(False))
        return W_ADD_NAME
    finally:
        conn.close()
    await update.message.reply_text("Кошелёк добавлен ✅", reply_markup=wallets_menu_kb())
    return ConversationHandler.END

async def wallet_archive_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    wallets = get_wallets(q.from_user.id, active_only=True)
    if not wallets:
        await q.edit_message_text("Нет активных кошельков.", reply_markup=wallets_menu_kb())
        return ConversationHandler.END
    rows = [[InlineKeyboardButton(name, callback_data=f"warch:{wid}")] for wid, name in wallets]
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="menu:wallets")])
    await q.edit_message_text("Какой кошелёк архивируем?", reply_markup=InlineKeyboardMarkup(rows))
    return W_ARCH_PICK

async def wallet_archive_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "menu:wallets":
        return await show_menu(update, context)
    conn = get_db_connection()
    try:
        conn.execute("UPDATE wallets SET is_active=0 WHERE user_id=? AND id=?", (q.from_user.id, int(q.data.split(":")[1])))
        conn.commit()
    finally:
        conn.close()
    await q.edit_message_text("Кошелёк архивирован ✅", reply_markup=wallets_menu_kb())
    return ConversationHandler.END

# ---------------- ROUTER ----------------
async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "menu:home":
        await q.edit_message_text("🏠 Главное меню:", reply_markup=main_menu_kb())
        return ConversationHandler.END
    if data == "menu:ops":
        await q.edit_message_text("Выберите тип операции:", reply_markup=ops_menu_kb())
        return ConversationHandler.END
    if data == "ops:income":
        return await add_entry(update, context, "income")
    if data == "ops:expense":
        return await add_entry(update, context, "expense")
    if data == "ops:transfer":
        return await transfer_entry(update, context)
    if data == "menu:stats":
        now = datetime.now()
        await send_statistics(update, context, now.year, now.month)
        return ConversationHandler.END
    if data.startswith("stat:"):
        _, y, m = data.split(":")
        await send_statistics(update, context, int(y), int(m))
        return ConversationHandler.END
    if data == "menu:wallets":
        user_id = q.from_user.id
        def calc_balances():
            return all_balances(user_id)
        balances, total = await asyncio.to_thread(calc_balances)
        lines = [f"{html.escape(n)}: {b:.2f}" for _, n, is_act, b in balances if is_act]
        lines.append(f"\n<b>Итого: {total:.2f}</b>")
        await q.edit_message_text("\n".join(lines), reply_markup=wallets_menu_kb(), parse_mode="HTML")
        return ConversationHandler.END
    if data == "w:adjust":
        return await adjust_entry(update, context)
    if data == "w:add":
        return await wallet_add_entry(update, context)
    if data == "w:archive":
        return await wallet_archive_entry(update, context)
    if data == "menu:debts":
        return await debts_entry(update, context)
    return ConversationHandler.END

# ---------------- MAIN ----------------
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    
    conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(main_menu_router, pattern=r"^(menu:|ops:|w:|stat:|debt:|cat|confirm|from|to|dpay|dadj|adjw|pw|dw|warch|back|cancel)"),
        ],
        states={
            ADD_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_amount)],
            ADD_WALLET: [CallbackQueryHandler(add_wallet_pick)],
            ADD_CATEGORY: [CallbackQueryHandler(add_category_pick)],
            ADD_CONFIRM: [CallbackQueryHandler(add_confirm_buttons)],
            ADD_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_note)],
            CAT_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_add_name)],
            CAT_DEL_PICK: [CallbackQueryHandler(cat_del_pick)],
            TR_FROM: [CallbackQueryHandler(tr_pick_from)],
            TR_TO: [CallbackQueryHandler(tr_pick_to)],
            TR_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tr_amount)],
            TR_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tr_note)],
            TR_CONFIRM: [CallbackQueryHandler(tr_confirm_buttons)],
            ADJ_WALLET: [CallbackQueryHandler(adj_pick_wallet)],
            ADJ_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, adj_target)],
            ADJ_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, adj_note)],
            ADJ_CONFIRM: [CallbackQueryHandler(adj_confirm_buttons)],
            DEBT_MENU: [CallbackQueryHandler(debt_menu_handler)],
            DEBT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_name)],
            DEBT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_amount)],
            DEBT_WALLET: [CallbackQueryHandler(debt_wallet)],
            DEBT_PAY_PICK: [CallbackQueryHandler(debt_pay_pick)],
            DEBT_PAY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_pay_amount)],
            DEBT_PAY_WALLET: [CallbackQueryHandler(debt_pay_wallet)],
            DEBT_ADJ_PICK: [CallbackQueryHandler(debt_adj_pick)],
            DEBT_ADJ_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_target)],
            DEBT_ADJ_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_note)],
            DEBT_ADJ_CONFIRM: [CallbackQueryHandler(debt_adj_confirm)],
            W_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_name)],
            W_ARCH_PICK: [CallbackQueryHandler(wallet_archive_pick)],
        },
        fallbacks=[
            CallbackQueryHandler(on_cancel, pattern=r"^cancel$"),
        ],
    )
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))
    
    print("✅ Бот успешно запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()
