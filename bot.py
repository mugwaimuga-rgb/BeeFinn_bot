import os
import re
import uuid
import sqlite3
import warnings
import html
import asyncio
import logging
from datetime import datetime
from enum import Enum

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    LabeledPrice,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    PreCheckoutQueryHandler,
    filters,
)

# ---------------- НАСТРОЙКИ ----------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=UserWarning, module="telegram.ext")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден в переменных окружения!")

DB_PATH = "/app/data/finance.db" if os.path.exists("/app/data") else "finance.db"

DEFAULT_WALLETS = ["Наличные", "Сбер", "Тинькофф", "Альфа"]
CATEGORIES_EXPENSE = [
    "Еда",
    "Транспорт",
    "Дом",
    "Связь",
    "Здоровье",
    "Развлечения",
    "Другое",
]
CATEGORIES_INCOME = ["Зарплата", "Подработка", "Подарок", "Возврат", "Другое"]
SYSTEM_CATEGORIES = ["Перевод", "Корректировка", "Кредит/Долг", "Платёж по долгу"]


class State(Enum):
    ADD_AMOUNT = 1
    ADD_WALLET = 2
    ADD_CATEGORY = 3
    ADD_NOTE = 4
    ADD_CONFIRM = 5
    CAT_ADD_NAME = 6
    CAT_DEL_PICK = 7

    TR_FROM = 8
    TR_TO = 9
    TR_AMOUNT = 10
    TR_NOTE = 11
    TR_CONFIRM = 12

    ADJ_WALLET = 13
    ADJ_TARGET = 14
    ADJ_NOTE = 15
    ADJ_CONFIRM = 16

    W_ADD_NAME = 17
    W_ARCH_PICK = 18

    DEBT_MENU = 19
    DEBT_NAME = 20
    DEBT_AMOUNT = 21
    DEBT_WALLET = 22
    DEBT_PAY_PICK = 23
    DEBT_PAY_AMOUNT = 24
    DEBT_PAY_WALLET = 25
    DEBT_ADJ_PICK = 26
    DEBT_ADJ_TARGET = 27
    DEBT_ADJ_NOTE = 28
    DEBT_ADJ_CONFIRM = 29


# ---------------- БАЗА ДАННЫХ ----------------


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, name)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS categories(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ttype TEXT NOT NULL,
                name TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                is_system INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, ttype, name)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS debts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                debt_type TEXT NOT NULL,
                name TEXT NOT NULL,
                total_amount REAL NOT NULL,
                current_balance REAL NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
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
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Init DB error: {e}")
        raise
    finally:
        conn.close()


def seed_db(user_id: int):
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_db_connection()
    try:
        for name in DEFAULT_WALLETS:
            conn.execute(
                """
                INSERT OR IGNORE INTO wallets(user_id, name, is_active, created_at)
                VALUES(?, ?, 1, ?)
                """,
                (user_id, name, now),
            )

        for n in CATEGORIES_EXPENSE:
            conn.execute(
                """
                INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, is_system, created_at)
                VALUES(?, 'expense', ?, 1, 0, ?)
                """,
                (user_id, n, now),
            )

        for n in CATEGORIES_INCOME:
            conn.execute(
                """
                INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, is_system, created_at)
                VALUES(?, 'income', ?, 1, 0, ?)
                """,
                (user_id, n, now),
            )

        for cat in SYSTEM_CATEGORIES:
            conn.execute(
                """
                INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, is_system, created_at)
                VALUES(?, 'expense', ?, 1, 1, ?)
                """,
                (user_id, cat, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO categories(user_id, ttype, name, is_active, is_system, created_at)
                VALUES(?, 'income', ?, 1, 1, ?)
                """,
                (user_id, cat, now),
            )

        conn.commit()
    except Exception as e:
        logger.error(f"Seed DB error for user {user_id}: {e}")
        conn.rollback()
    finally:
        conn.close()


# ---------------- ВСПОМОГАТЕЛЬНЫЕ ----------------


def money_parse(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", t):
        return None
    val = float(t)
    return val if val >= 0 else None


async def get_wallets(user_id: int, active_only=True):
    def _get():
        conn = get_db_connection()
        try:
            if active_only:
                return conn.execute(
                    "SELECT id, name FROM wallets WHERE user_id=? AND is_active=1 ORDER BY id",
                    (user_id,),
                ).fetchall()
            return conn.execute(
                "SELECT id, name, is_active FROM wallets WHERE user_id=? ORDER BY id",
                (user_id,),
            ).fetchall()
        finally:
            conn.close()

    return await asyncio.to_thread(_get)


async def wallet_balance(user_id: int, wallet_id: int) -> float:
    def _balance():
        conn = get_db_connection()
        try:
            inc = conn.execute(
                """
                SELECT COALESCE(SUM(amount),0) FROM transactions
                WHERE user_id=? AND wallet_id=? AND ttype='income'
                """,
                (user_id, wallet_id),
            ).fetchone()[0]
            exp = conn.execute(
                """
                SELECT COALESCE(SUM(amount),0) FROM transactions
                WHERE user_id=? AND wallet_id=? AND ttype='expense'
                """,
                (user_id, wallet_id),
            ).fetchone()[0]
            return float(inc) - float(exp)
        finally:
            conn.close()

    return await asyncio.to_thread(_balance)


async def get_categories(user_id: int, ttype: str, include_system=False):
    def _get():
        conn = get_db_connection()
        try:
            if include_system:
                return conn.execute(
                    """
                    SELECT id, name FROM categories
                    WHERE user_id=? AND ttype=? AND is_active=1
                    ORDER BY name
                    """,
                    (user_id, ttype),
                ).fetchall()
            return conn.execute(
                """
                    SELECT id, name FROM categories
                    WHERE user_id=? AND ttype=? AND is_active=1 AND is_system=0
                    ORDER BY name
                    """,
                (user_id, ttype),
            ).fetchall()
        finally:
            conn.close()

    return await asyncio.to_thread(_get)


def get_month_name(m: int):
    return [
        "Январь",
        "Февраль",
        "Март",
        "Апрель",
        "Май",
        "Июнь",
        "Июль",
        "Август",
        "Сентябрь",
        "Октябрь",
        "Ноябрь",
        "Декабрь",
    ][m - 1]


# ---------------- КЛАВИАТУРЫ ----------------


def main_menu_kb():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Операция", callback_data="menu:ops")],
            [
                InlineKeyboardButton("👛 Кошельки", callback_data="menu:wallets"),
                InlineKeyboardButton("📊 Статистика", callback_data="menu:stats"),
            ],
            [InlineKeyboardButton("💳 Долги", callback_data="menu:debts")],
        ]
    )


def ops_menu_kb():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟢 Доход", callback_data="ops:income"),
                InlineKeyboardButton("🔴 Расход", callback_data="ops:expense"),
            ],
            [InlineKeyboardButton("🔁 Перевод", callback_data="ops:transfer")],
            [InlineKeyboardButton("⬅ Назад", callback_data="menu:home")],
        ]
    )


def wallets_menu_kb():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🛠 Исправить баланс", callback_data="wallet:adjust")],
            [InlineKeyboardButton("➕ Добавить", callback_data="wallet:add")],
            [InlineKeyboardButton("🗄 Архив", callback_data="wallet:archive")],
            [InlineKeyboardButton("⬅ Назад", callback_data="menu:home")],
        ]
    )


def cancel_kb(back=True):
    rows = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
    if back:
        rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def kb_confirm(has_note: bool):
    txt = "📝 Изменить" if has_note else "📝 Комментарий"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Сохранить", callback_data="confirm:save")],
            [InlineKeyboardButton(txt, callback_data="confirm:add_note")],
            [
                InlineKeyboardButton("⬅ Назад", callback_data="confirm:back"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel"),
            ],
        ]
    )


async def kb_wallets(uid: int, prefix: str, back=True):
    wallets = await get_wallets(uid, True)
    rows = [
        [InlineKeyboardButton(n, callback_data=f"{prefix}:{wid}")] for wid, n in wallets
    ]
    if back:
        rows.append(
            [
                InlineKeyboardButton("⬅ Назад", callback_data="back"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


async def kb_cats(uid: int, ttype: str):
    cats = await get_categories(uid, ttype, include_system=False)
    rows, row = [], []
    for cid, name in cats:
        row.append(InlineKeyboardButton(name, callback_data=f"catpick:{name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton("➕ Добавить", callback_data="category:add"),
            InlineKeyboardButton("🗑 Удалить", callback_data="category:del"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton("⬅ Назад", callback_data="back"),
            InlineKeyboardButton("❌ Отмена", callback_data="cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)


# ---------------- ОБЩИЕ ХЕНДЛЕРЫ ----------------


async def show_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text="🏠 Главное меню:"
):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        await q.edit_message_text(text, reply_markup=main_menu_kb(), parse_mode="HTML")
    else:
        await update.message.reply_text(
            text, reply_markup=main_menu_kb(), parse_mode="HTML"
        )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    context.user_data.clear()
    await asyncio.to_thread(seed_db, uid)
    await show_menu(
        update, context, "Привет! Я помогу вести учёт финансов 💸\nВыбери действие:"
    )
    return ConversationHandler.END


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Используйте кнопки меню 👇", reply_markup=main_menu_kb(), parse_mode="HTML"
    )
    return ConversationHandler.END


async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Отменено ❌")
    return ConversationHandler.END


# ---------------- ДОХОД / РАСХОД ----------------


async def add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, ttype: str):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    context.user_data["ttype"] = ttype
    label = "ДОХОД 🟢" if ttype == "income" else "РАСХОД 🔴"
    await update.callback_query.edit_message_text(
        f"Вводим {label}\nСумма (например 350.50):",
        reply_markup=cancel_kb(False),
        parse_mode="HTML",
    )
    return State.ADD_AMOUNT


async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму. Пример: 350 или 350.50:", reply_markup=cancel_kb(False)
        )
        return State.ADD_AMOUNT
    context.user_data["amount"] = val
    kb = await kb_wallets(update.message.from_user.id, "wallet", True)
    await update.message.reply_text("Выбери кошелёк:", reply_markup=kb)
    return State.ADD_WALLET


async def add_wallet_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        await q.edit_message_text("Введи сумму:", reply_markup=cancel_kb(False))
        return State.ADD_AMOUNT
    context.user_data["wallet_id"] = int(q.data.split(":")[1])
    kb = await kb_cats(q.from_user.id, context.user_data["ttype"])
    await q.edit_message_text("Выбери статью:", reply_markup=kb)
    return State.ADD_CATEGORY


async def add_category_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        kb = await kb_wallets(q.from_user.id, "wallet", True)
        await q.edit_message_text("Выбери кошелёк:", reply_markup=kb)
        return State.ADD_WALLET
    if q.data == "category:add":
        await q.edit_message_text(
            "Название новой статьи:", reply_markup=cancel_kb(False)
        )
        return State.CAT_ADD_NAME
    if q.data == "category:del":
        cats = await get_categories(
            q.from_user.id, context.user_data["ttype"], include_system=False
        )
        rows = [
            [InlineKeyboardButton(n, callback_data=f"catdel:{cid}")]
            for cid, n in cats
        ]
        rows.append([InlineKeyboardButton("⬅ Назад", callback_data="catdel_back")])
        await q.edit_message_text(
            "Какую удалить?", reply_markup=InlineKeyboardMarkup(rows)
        )
        return State.CAT_DEL_PICK

    context.user_data["category"] = q.data.split(":")[1]
    label = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get("note") or "—")
    await q.edit_message_text(
        f"Проверим:\n"
        f"Тип: {label}\n"
        f"Сумма: {context.user_data['amount']:.2f}\n"
        f"Статья: {html.escape(context.user_data['category'])}\n"
        f"Коммент: {note}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(bool(context.user_data.get("note"))),
        parse_mode="HTML",
    )
    return State.ADD_CONFIRM


async def cat_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text(
            "Название не может быть пустым. Введите снова:",
            reply_markup=cancel_kb(False),
        )
        return State.CAT_ADD_NAME

    uid = update.message.from_user.id
    ttype = context.user_data["ttype"]
    now = datetime.now().isoformat(timespec="seconds")

    def _add():
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO categories(user_id, ttype, name, is_active, is_system, created_at)
                VALUES(?,?,?,1,0,?)
                """,
                (uid, ttype, name, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.execute(
                """
                UPDATE categories SET is_active=1
                WHERE user_id=? AND ttype=? AND name=?
                """,
                (uid, ttype, name),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Error adding category: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        await asyncio.to_thread(_add)
    except Exception:
        await update.message.reply_text(
            "Ошибка при добавлении категории. Попробуйте позже.",
            reply_markup=cancel_kb(False),
        )
        return State.CAT_ADD_NAME

    kb = await kb_cats(uid, ttype)
    await update.message.reply_text("Статья добавлена! Выбери её:", reply_markup=kb)
    return State.ADD_CATEGORY


async def cat_del_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "catdel_back":
        kb = await kb_cats(q.from_user.id, context.user_data["ttype"])
        await q.edit_message_text("Выбери статью:", reply_markup=kb)
        return State.ADD_CATEGORY

    cid = int(q.data.split(":")[1])
    uid = q.from_user.id

    def _del():
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT is_system FROM categories WHERE id=? AND user_id=?",
                (cid, uid),
            ).fetchone()
            if row and row[0] == 1:
                return False
            conn.execute(
                "UPDATE categories SET is_active=0 WHERE id=? AND user_id=?",
                (cid, uid),
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting category: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        success = await asyncio.to_thread(_del)
    except Exception:
        await q.edit_message_text(
            "Ошибка при удалении. Попробуйте позже.",
            reply_markup=cancel_kb(False),
        )
        return State.CAT_DEL_PICK

    if not success:
        await q.edit_message_text(
            "Эту категорию нельзя удалить (системная).",
            reply_markup=cancel_kb(False),
        )
        return State.CAT_DEL_PICK

    kb = await kb_cats(uid, context.user_data["ttype"])
    await q.edit_message_text("Удалено. Выбери статью:", reply_markup=kb)
    return State.ADD_CATEGORY


async def add_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        kb = await kb_cats(q.from_user.id, context.user_data["ttype"])
        await q.edit_message_text("Выбери статью:", reply_markup=kb)
        return State.ADD_CATEGORY
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.ADD_NOTE

    if q.data == "confirm:save":
        now = datetime.now().isoformat(timespec="seconds")
        uid = q.from_user.id

        def _save():
            conn = get_db_connection()
            try:
                conn.execute(
                    """
                    INSERT INTO transactions(
                        user_id, ttype, amount, wallet_id, category, note, created_at
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        uid,
                        context.user_data["ttype"],
                        context.user_data["amount"],
                        context.user_data["wallet_id"],
                        context.user_data["category"],
                        context.user_data.get("note"),
                        now,
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error saving transaction: {e}")
                conn.rollback()
                raise
            finally:
                conn.close()

        try:
            await asyncio.to_thread(_save)
            await q.edit_message_text("Сохранено ✅", reply_markup=main_menu_kb())
        except Exception:
            await q.edit_message_text(
                "Ошибка при сохранении. Попробуйте позже.",
                reply_markup=main_menu_kb(),
            )
        return ConversationHandler.END


async def add_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    label = "Расход 🔴" if context.user_data["ttype"] == "expense" else "Доход 🟢"
    note = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(
        f"Проверим:\n"
        f"Тип: {label}\n"
        f"Сумма: {context.user_data['amount']:.2f}\n"
        f"Статья: {html.escape(context.user_data['category'])}\n"
        f"Коммент: {note}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(True),
        parse_mode="HTML",
    )
    return State.ADD_CONFIRM


# ---------------- ПЕРЕВОД ----------------


async def transfer_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    kb = await kb_wallets(uid, "from", False)
    await update.callback_query.edit_message_text(
        "🔁 Перевод\nИз какого кошелька?", reply_markup=kb, parse_mode="HTML"
    )
    return State.TR_FROM


async def tr_pick_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    context.user_data["from_wallet_id"] = int(q.data.split(":")[1])
    kb = await kb_wallets(q.from_user.id, "to", True)
    await q.edit_message_text("В какой кошелёк?", reply_markup=kb)
    return State.TR_TO


async def tr_pick_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "back":
        kb = await kb_wallets(q.from_user.id, "from", False)
        await q.edit_message_text("Из какого кошелька?", reply_markup=kb)
        return State.TR_FROM

    to_id = int(q.data.split(":")[1])
    if to_id == context.user_data.get("from_wallet_id"):
        kb = await kb_wallets(q.from_user.id, "to", True)
        await q.edit_message_text(
            "Нельзя в тот же кошелёк! Выбери другой:", reply_markup=kb
        )
        return State.TR_TO

    context.user_data["to_wallet_id"] = to_id
    context.user_data["note"] = None
    await q.edit_message_text("Сумма перевода:", reply_markup=cancel_kb(False))
    return State.TR_AMOUNT


async def tr_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму:", reply_markup=cancel_kb(False)
        )
        return State.TR_AMOUNT
    context.user_data["amount"] = val
    note = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(
        f"Перевод: {val:.2f}\nКоммент: {note}\n\nСохранить?",
        reply_markup=kb_confirm(bool(context.user_data.get("note"))),
        parse_mode="HTML",
    )
    return State.TR_CONFIRM


async def tr_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    val = context.user_data["amount"]
    note = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(
        f"Перевод: {val:.2f}\nКоммент: {note}\n\nСохранить?",
        reply_markup=kb_confirm(True),
        parse_mode="HTML",
    )
    return State.TR_CONFIRM


async def tr_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Сумма перевода:", reply_markup=cancel_kb(False))
        return State.TR_AMOUNT
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.TR_NOTE

    if q.data == "confirm:save":
        uid = q.from_user.id
        now = datetime.now().isoformat(timespec="seconds")
        amt = context.user_data["amount"]

        for _ in range(3):
            tid = str(uuid.uuid4())

            def _insert():
                conn = get_db_connection()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    exists = conn.execute(
                        "SELECT 1 FROM transactions WHERE transfer_id=?",
                        (tid,),
                    ).fetchone()
                    if exists:
                        conn.rollback()
                        return None

                    conn.execute(
                        """
                        INSERT INTO transactions(
                            user_id, ttype, amount, wallet_id, category, note, transfer_id, created_at
                        ) VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            uid,
                            "expense",
                            amt,
                            context.user_data["from_wallet_id"],
                            "Перевод",
                            context.user_data.get("note"),
                            tid,
                            now,
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO transactions(
                            user_id, ttype, amount, wallet_id, category, note, transfer_id, created_at
                        ) VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            uid,
                            "income",
                            amt,
                            context.user_data["to_wallet_id"],
                            "Перевод",
                            context.user_data.get("note"),
                            tid,
                            now,
                        ),
                    )
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"Error in transfer: {e}")
                    conn.rollback()
                    raise
                finally:
                    conn.close()

            try:
                result = await asyncio.to_thread(_insert)
                if result is None:
                    continue
                await q.edit_message_text(
                    "Перевод сохранён ✅", reply_markup=main_menu_kb()
                )
                return ConversationHandler.END
            except Exception:
                await q.edit_message_text(
                    "Ошибка при сохранении перевода. Попробуйте позже.",
                    reply_markup=main_menu_kb(),
                )
                return ConversationHandler.END

        await q.edit_message_text(
            "Не удалось сгенерировать уникальный идентификатор. Попробуйте ещё раз.",
            reply_markup=main_menu_kb(),
        )
        return ConversationHandler.END


# ---------------- СТАТИСТИКА ----------------


async def send_statistics(
    update: Update, context: ContextTypes.DEFAULT_TYPE, year: int, month: int
):
    q = update.callback_query
    uid = q.from_user.id
    start = f"{year}-{month:02d}-01T00:00:00"
    end_y, end_m = (year + 1, 1) if month == 12 else (year, month + 1)
    end = f"{end_y}-{end_m:02d}-01T00:00:00"

    def fetch():
        conn = get_db_connection()
        try:
            inc = conn.execute(
                """
                SELECT category, SUM(amount) FROM transactions
                WHERE user_id=? AND ttype='income' AND created_at>=? AND created_at<?
                  AND category NOT IN (
                    SELECT name FROM categories WHERE user_id=? AND is_system=1
                  )
                GROUP BY category
                """,
                (uid, start, end, uid),
            ).fetchall()

            exp = conn.execute(
                """
                SELECT category, SUM(amount) FROM transactions
                WHERE user_id=? AND ttype='expense' AND created_at>=? AND created_at<?
                  AND category NOT IN (
                    SELECT name FROM categories WHERE user_id=? AND is_system=1
                  )
                GROUP BY category
                """,
                (uid, start, end, uid),
            ).fetchall()
            return inc, exp
        finally:
            conn.close()

    inc, exp = await asyncio.to_thread(fetch)
    msg = f"📊 <b>Статистика за {get_month_name(month)} {year}</b>\n<b>🟢 Доходы:</b>\n"
    total_inc = sum(a for _, a in inc)
    if inc:
        msg += "\n".join(f" • {html.escape(c)}: {a:.2f}" for c, a in inc)
    else:
        msg += " Нет записей"
    msg += f"\n  <b>Итого:</b> {total_inc:.2f}\n\n<b>🔴 Расходы:</b>\n"
    total_exp = sum(a for _, a in exp)
    if exp:
        msg += "\n".join(f" • {html.escape(c)}: {a:.2f}" for c, a in exp)
    else:
        msg += " Нет записей"
    msg += (
        f"\n  <b>Итого:</b> {total_exp:.2f}\n\n"
        f"⚖️ <b>Баланс:</b> {total_inc - total_exp:.2f}"
    )

    prev_m, prev_y = (month - 1, year) if month > 1 else (12, year - 1)
    next_m, next_y = (month + 1, year) if month < 12 else (1, year + 1)
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅ Пред.", callback_data=f"stat:{prev_y}:{prev_m}"
                ),
                InlineKeyboardButton(
                    "След. ➡", callback_data=f"stat:{next_y}:{next_m}"
                ),
            ],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )
    await q.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")


# ---------------- КОШЕЛЬКИ ----------------


async def adjust_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await asyncio.to_thread(seed_db, uid)
    context.user_data.clear()
    kb = await kb_wallets(uid, "adjustwallet", False)
    await update.callback_query.edit_message_text(
        "Какой кошелёк корректируем?", reply_markup=kb
    )
    return State.ADJ_WALLET


async def adj_pick_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    context.user_data["wallet_id"] = int(q.data.split(":")[1])
    context.user_data["note"] = None
    await q.edit_message_text("Сколько сейчас по факту?", reply_markup=cancel_kb(False))
    return State.ADJ_TARGET


async def adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    target = money_parse(update.message.text)
    if target is None:
        await update.message.reply_text(
            "Не понял сумму:", reply_markup=cancel_kb(False)
        )
        return State.ADJ_TARGET

    wid = context.user_data["wallet_id"]
    current = await wallet_balance(uid, wid)
    delta = target - current
    context.user_data.update(
        {"target": target, "current": current, "delta": delta}
    )
    note = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(
        f"В базе: {current:.2f}\n"
        f"По факту: {target:.2f}\n"
        f"Дельта: {delta:.2f}\n"
        f"Коммент: {note}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(bool(context.user_data.get("note"))),
        parse_mode="HTML",
    )
    return State.ADJ_CONFIRM


async def adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    current = context.user_data["current"]
    target = context.user_data["target"]
    delta = context.user_data["delta"]
    note = html.escape(context.user_data.get("note") or "—")
    await update.message.reply_text(
        f"В базе: {current:.2f}\n"
        f"По факту: {target:.2f}\n"
        f"Дельта: {delta:.2f}\n"
        f"Коммент: {note}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(True),
        parse_mode="HTML",
    )
    return State.ADJ_CONFIRM


async def adj_confirm_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text("Сколько по факту?", reply_markup=cancel_kb(False))
        return State.ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.ADJ_NOTE

    if q.data == "confirm:save":
        delta = context.user_data["delta"]
        if abs(delta) < 1e-9:
            await q.edit_message_text(
                "Суммы совпадают ✅", reply_markup=main_menu_kb()
            )
            return ConversationHandler.END

        ttype = "income" if delta > 0 else "expense"
        now = datetime.now().isoformat(timespec="seconds")
        uid = q.from_user.id

        def _save():
            conn = get_db_connection()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT INTO transactions(
                        user_id, ttype, amount, wallet_id, category, note, created_at
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        uid,
                        ttype,
                        abs(delta),
                        context.user_data["wallet_id"],
                        "Корректировка",
                        context.user_data.get("note"),
                        now,
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error in adjustment: {e}")
                conn.rollback()
                raise
            finally:
                conn.close()

        try:
            await asyncio.to_thread(_save)
            await q.edit_message_text(
                "Корректировка сохранена ✅", reply_markup=main_menu_kb()
            )
        except Exception:
            await q.edit_message_text(
                "Ошибка при сохранении. Попробуйте позже.",
                reply_markup=main_menu_kb(),
            )
        return ConversationHandler.END


async def wallet_add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await asyncio.to_thread(seed_db, q.from_user.id)
    context.user_data.clear()
    await q.edit_message_text(
        "Название нового кошелька:", reply_markup=cancel_kb(False)
    )
    return State.W_ADD_NAME


async def wallet_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text(
            "Название не может быть пустым. Введите снова:",
            reply_markup=cancel_kb(False),
        )
        return State.W_ADD_NAME

    uid = update.message.from_user.id
    now = datetime.now().isoformat(timespec="seconds")

    def _add():
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO wallets(user_id, name, is_active, created_at)
                VALUES(?,?,1,?)
                """,
                (uid, name, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.execute(
                """
                UPDATE wallets SET is_active=1
                WHERE user_id=? AND name=?
                """,
                (uid, name),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Error adding wallet: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        await asyncio.to_thread(_add)
    except Exception:
        await update.message.reply_text(
            "Ошибка при добавлении кошелька. Попробуйте позже.",
            reply_markup=cancel_kb(False),
        )
        return State.W_ADD_NAME

    await update.message.reply_text("Добавлен ✅", reply_markup=wallets_menu_kb())
    return ConversationHandler.END


async def wallet_archive_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    wallets = await get_wallets(q.from_user.id, True)
    if not wallets:
        await q.edit_message_text(
            "Нет активных кошельков.", reply_markup=wallets_menu_kb()
        )
        return ConversationHandler.END

    rows = [
        [InlineKeyboardButton(n, callback_data=f"warch:{wid}")]
        for wid, n in wallets
    ]
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="menu:wallets")])
    await q.edit_message_text(
        "Какой архивируем?", reply_markup=InlineKeyboardMarkup(rows)
    )
    return State.W_ARCH_PICK


async def wallet_archive_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "menu:wallets":
        return await show_menu(update, context)

    wid = int(q.data.split(":")[1])

    def _archive():
        conn = get_db_connection()
        try:
            conn.execute(
                """
                UPDATE wallets SET is_active=0
                WHERE user_id=? AND id=?
                """,
                (q.from_user.id, wid),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Error archiving wallet: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        await asyncio.to_thread(_archive)
        await q.edit_message_text(
            "Архивирован ✅", reply_markup=wallets_menu_kb()
        )
    except Exception:
        await q.edit_message_text(
            "Ошибка при архивации. Попробуйте позже.",
            reply_markup=wallets_menu_kb(),
        )
    return ConversationHandler.END


# ---------------- ДОЛГИ ----------------


def kb_debts_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Взять в долг", callback_data="debt:add:my_debt")],
            [
                InlineKeyboardButton(
                    "➕ Дать в долг", callback_data="debt:add:owed_to_me"
                )
            ],
            [InlineKeyboardButton("💳 Платёж", callback_data="debt:pay")],
            [InlineKeyboardButton("🛠 Исправить", callback_data="debt:adjust")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


async def debts_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = (
        update.callback_query.from_user.id
        if update.callback_query
        else update.message.from_user.id
    )

    def fetch():
        conn = get_db_connection()
        try:
            my = conn.execute(
                """
                SELECT name, current_balance FROM debts
                WHERE user_id=? AND debt_type='my_debt'
                  AND is_active=1 AND current_balance>0
                """,
                (uid,),
            ).fetchall()
            owed = conn.execute(
                """
                SELECT name, current_balance FROM debts
                WHERE user_id=? AND debt_type='owed_to_me'
                  AND is_active=1 AND current_balance>0
                """,
                (uid,),
            ).fetchall()
            return my, owed
        finally:
            conn.close()

    my, owed = await asyncio.to_thread(fetch)
    msg = "💳 <b>Я должен:</b>\n"
    msg += (
        "\n".join(f"• {html.escape(n)}: {b:.2f}" for n, b in my)
        if my
        else "• Нет"
    )
    msg += "\n\n🤝 <b>Мне должны:</b>\n"
    msg += (
        "\n".join(f"• {html.escape(n)}: {b:.2f}" for n, b in owed)
        if owed
        else "• Нет"
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(
            msg, reply_markup=kb_debts_menu(), parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            msg, reply_markup=kb_debts_menu(), parse_mode="HTML"
        )
    return State.DEBT_MENU


async def debt_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "menu:home":
        return await show_menu(update, context)

    if q.data.startswith("debt:add:"):
        context.user_data["debt_type"] = q.data.split(":")[2]
        txt = (
            "Название кредита:"
            if context.user_data["debt_type"] == "my_debt"
            else "Кому даём в долг:"
        )
        await q.edit_message_text(txt, reply_markup=cancel_kb(False))
        return State.DEBT_NAME

    if q.data == "debt:pay":

        def fetch_pay():
            conn = get_db_connection()
            try:
                return conn.execute(
                    """
                    SELECT id, name, current_balance FROM debts
                    WHERE user_id=? AND is_active=1 AND current_balance>0
                    """,
                    (q.from_user.id,),
                ).fetchall()
            finally:
                conn.close()

        debts = await asyncio.to_thread(fetch_pay)
        if not debts:
            await q.edit_message_text(
                "Нет активных долгов.", reply_markup=kb_debts_menu()
            )
            return State.DEBT_MENU

        rows = [
            [
                InlineKeyboardButton(
                    f"{html.escape(n)} ({b:.0f})", callback_data=f"debtpay:{did}"
                )
            ]
            for did, n, b in debts
        ]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text(
            "По какому долгу платим?", reply_markup=InlineKeyboardMarkup(rows)
        )
        return State.DEBT_PAY_PICK

    if q.data == "debt:adjust":

        def fetch_adj():
            conn = get_db_connection()
            try:
                return conn.execute(
                    """
                    SELECT id, name, current_balance FROM debts
                    WHERE user_id=? AND is_active=1
                    """,
                    (q.from_user.id,),
                ).fetchall()
            finally:
                conn.close()

        debts = await asyncio.to_thread(fetch_adj)
        if not debts:
            await q.edit_message_text(
                "Нет долгов для корректировки.", reply_markup=kb_debts_menu()
            )
            return State.DEBT_MENU

        rows = [
            [
                InlineKeyboardButton(
                    f"{html.escape(n)} ({b:.0f})", callback_data=f"debtadj:{did}"
                )
            ]
            for did, n, b in debts
        ]
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await q.edit_message_text(
            "Выбери долг:", reply_markup=InlineKeyboardMarkup(rows)
        )
        return State.DEBT_ADJ_PICK

    return State.DEBT_MENU


async def debt_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["debt_name"] = update.message.text.strip()
    await update.message.reply_text(
        "Сумма долга:", reply_markup=cancel_kb(False)
    )
    return State.DEBT_AMOUNT


async def debt_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму:", reply_markup=cancel_kb(False)
        )
        return State.DEBT_AMOUNT

    context.user_data["debt_amount"] = val
    txt = (
        "На какой кошелёк поступило?"
        if context.user_data["debt_type"] == "my_debt"
        else "С какого кошелька дали?"
    )
    kb = await kb_wallets(update.message.from_user.id, "debtwallet", False)
    await update.message.reply_text(txt, reply_markup=kb)
    return State.DEBT_WALLET


async def debt_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)

    wid = int(q.data.split(":")[1])
    amt = context.user_data["debt_amount"]
    now = datetime.now().isoformat(timespec="seconds")
    uid = q.from_user.id

    def _save():
        conn = get_db_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                INSERT INTO debts(
                    user_id, debt_type, name, total_amount, current_balance, created_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    uid,
                    context.user_data["debt_type"],
                    context.user_data["debt_name"],
                    amt,
                    amt,
                    now,
                ),
            )
            debt_id = cur.lastrowid
            ttype = "income" if context.user_data["debt_type"] == "my_debt" else "expense"
            conn.execute(
                """
                INSERT INTO transactions(
                    user_id, ttype, amount, wallet_id, category, debt_id, created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (uid, ttype, amt, wid, "Кредит/Долг", debt_id, now),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving debt: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        await asyncio.to_thread(_save)
        await q.edit_message_text("Долг оформлен ✅", reply_markup=main_menu_kb())
    except Exception:
        await q.edit_message_text(
            "Ошибка при сохранении долга. Попробуйте позже.",
            reply_markup=main_menu_kb(),
        )
    return ConversationHandler.END


async def debt_pay_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)

    context.user_data["pay_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text("Сумма платежа:", reply_markup=cancel_kb(False))
    return State.DEBT_PAY_AMOUNT


async def debt_pay_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму:", reply_markup=cancel_kb(False)
        )
        return State.DEBT_PAY_AMOUNT

    context.user_data["pay_amount"] = val
    kb = await kb_wallets(update.message.from_user.id, "paywallet", False)
    await update.message.reply_text(
        "С какого кошелька платим?", reply_markup=kb
    )
    return State.DEBT_PAY_WALLET


async def debt_pay_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)

    wid = int(q.data.split(":")[1])
    amt = context.user_data["pay_amount"]
    did = context.user_data["pay_debt_id"]
    now = datetime.now().isoformat(timespec="seconds")
    uid = q.from_user.id

    def _process():
        conn = get_db_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT debt_type, current_balance FROM debts WHERE id=?",
                (did,),
            ).fetchone()
            if not row:
                conn.rollback()
                return None

            debt_type, current_balance = row
            if amt > current_balance:
                overpayment = True
                new_bal = 0
            else:
                overpayment = False
                new_bal = current_balance - amt

            conn.execute(
                "UPDATE debts SET current_balance=? WHERE id=?", (new_bal, did)
            )
            ttype = "expense" if debt_type == "my_debt" else "income"

            conn.execute(
                """
                INSERT INTO transactions(
                    user_id, ttype, amount, wallet_id, category, debt_id, created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (uid, ttype, amt, wid, "Платёж по долгу", did, now),
            )
            conn.commit()
            return new_bal, overpayment
        except Exception as e:
            logger.error(f"Error in debt payment: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        result = await asyncio.to_thread(_process)
        if result is None:
            await q.edit_message_text("Долг не найден.", reply_markup=main_menu_kb())
            return ConversationHandler.END

        new_bal, overpayment = result
        msg = f"Платёж учтён ✅ Остаток: {new_bal:.2f}"
        if overpayment:
            msg += (
                "\n⚠️ Сумма платежа превышала остаток долга. Остаток обнулён."
            )
        await q.edit_message_text(msg, reply_markup=main_menu_kb())
    except Exception:
        await q.edit_message_text(
            "Ошибка при проведении платежа. Попробуйте позже.",
            reply_markup=main_menu_kb(),
        )
    return ConversationHandler.END


async def debt_adj_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel":
        return await on_cancel(update, context)

    context.user_data["adj_debt_id"] = int(q.data.split(":")[1])
    await q.edit_message_text(
        "Фактический остаток долга:", reply_markup=cancel_kb(False)
    )
    return State.DEBT_ADJ_TARGET


async def debt_adj_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = money_parse(update.message.text)
    if val is None:
        await update.message.reply_text(
            "Не понял сумму:", reply_markup=cancel_kb(False)
        )
        return State.DEBT_ADJ_TARGET

    did = context.user_data["adj_debt_id"]

    def fetch_info():
        conn = get_db_connection()
        try:
            return conn.execute(
                "SELECT current_balance, name FROM debts WHERE id=?",
                (did,),
            ).fetchone()
        finally:
            conn.close()

    row = await asyncio.to_thread(fetch_info)
    if not row:
        await update.message.reply_text("Долг не найден.", reply_markup=main_menu_kb())
        return ConversationHandler.END

    current, name = row[0], row[1]
    context.user_data.update(
        {"adj_debt_target": val, "adj_debt_old": current, "adj_debt_name": name}
    )
    delta = val - current
    await update.message.reply_text(
        f"Долг: {html.escape(name)}\n"
        f"В базе: {current:.2f}\n"
        f"По факту: {val:.2f}\n"
        f"Разница: {delta:.2f}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(False),
        parse_mode="HTML",
    )
    return State.DEBT_ADJ_CONFIRM


async def debt_adj_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["note"] = update.message.text.strip() or None
    name = context.user_data["adj_debt_name"]
    old = context.user_data["adj_debt_old"]
    val = context.user_data["adj_debt_target"]
    delta = val - old
    await update.message.reply_text(
        f"Долг: {html.escape(name)}\n"
        f"В базе: {old:.2f}\n"
        f"По факту: {val:.2f}\n"
        f"Разница: {delta:.2f}\n\n"
        f"Сохранить?",
        reply_markup=kb_confirm(True),
        parse_mode="HTML",
    )
    return State.DEBT_ADJ_CONFIRM


async def debt_adj_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "cancel":
        return await on_cancel(update, context)
    if q.data == "confirm:back":
        await q.edit_message_text(
            "Фактический остаток долга:", reply_markup=cancel_kb(False)
        )
        return State.DEBT_ADJ_TARGET
    if q.data == "confirm:add_note":
        await q.edit_message_text("Комментарий:", reply_markup=cancel_kb(False))
        return State.DEBT_ADJ_NOTE

    if q.data == "confirm:save":
        val = context.user_data["adj_debt_target"]
        did = context.user_data["adj_debt_id"]

        def _save():
            conn = get_db_connection()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE debts SET current_balance=? WHERE id=?", (val, did)
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error adjusting debt: {e}")
                conn.rollback()
                raise
            finally:
                conn.close()

        try:
            await asyncio.to_thread(_save)
            await q.edit_message_text(
                "Скорректировано ✅", reply_markup=main_menu_kb()
            )
        except Exception:
            await q.edit_message_text(
                "Ошибка при корректировке. Попробуйте позже.",
                reply_markup=main_menu_kb(),
            )
        return ConversationHandler.END


# ---------------- ROUTER ГЛАВНОГО МЕНЮ ----------------


async def main_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    # Главное меню
    if data == "menu:home":
        await q.edit_message_text("🏠 Главное меню:", reply_markup=main_menu_kb())
        return ConversationHandler.END

    # Блок операций
    if data == "menu:ops":
        await q.edit_message_text("Выберите операцию:", reply_markup=ops_menu_kb())
        return ConversationHandler.END

    if data == "ops:income":
        return await add_entry(update, context, "income")
    if data == "ops:expense":
        return await add_entry(update, context, "expense")

    if data == "ops:transfer":
        return await transfer_entry(update, context)

    # Статистика
    if data == "menu:stats":
        now = datetime.now()
        await send_statistics(update, context, now.year, now.month)
        return ConversationHandler.END

    if data.startswith("stat:"):
        _, y, m = data.split(":")
        await send_statistics(update, context, int(y), int(m))
        return ConversationHandler.END

    # Кошельки
    if data == "menu:wallets":
        uid = q.from_user.id

        async def calc():
            wallets = await get_wallets(uid, False)
            res = []
            for row in wallets:
                if len(row) == 2:
                    wid, name = row
                    is_act = 1
                else:
                    wid, name, is_act = row
                bal = await wallet_balance(uid, wid)
                res.append((name, is_act, bal))
            return res

        balances = await calc()
        active = [(n, b) for n, act, b in balances if act]
        total = sum(b for _, b in active)
        lines = [f"{html.escape(n)}: {b:.2f}" for n, b in active]
        lines.append(f"\n<b>Итого: {total:.2f}</b>")
        await q.edit_message_text(
            "\n".join(lines), reply_markup=wallets_menu_kb(), parse_mode="HTML"
        )
        return ConversationHandler.END

    if data == "wallet:adjust":
        return await adjust_entry(update, context)
    if data == "wallet:add":
        return await wallet_add_entry(update, context)
    if data == "wallet:archive":
        return await wallet_archive_entry(update, context)

    # Долги
    if data == "menu:debts":
        return await debts_entry(update, context)

    return ConversationHandler.END


# ---------------- MAIN ----------------


def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))

    conv = ConversationHandler(
        entry_points=[],  # ВСЕ входы только через main_menu_router и прочие
        states={
            State.ADD_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_amount)
            ],
            State.ADD_WALLET: [CallbackQueryHandler(add_wallet_pick)],
            State.ADD_CATEGORY: [CallbackQueryHandler(add_category_pick)],
            State.ADD_CONFIRM: [CallbackQueryHandler(add_confirm_buttons)],
            State.ADD_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_note)],
            State.CAT_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cat_add_name)
            ],
            State.CAT_DEL_PICK: [CallbackQueryHandler(cat_del_pick)],
            State.TR_FROM: [CallbackQueryHandler(tr_pick_from)],
            State.TR_TO: [CallbackQueryHandler(tr_pick_to)],
            State.TR_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tr_amount)
            ],
            State.TR_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tr_note)],
            State.TR_CONFIRM: [CallbackQueryHandler(tr_confirm_buttons)],
            State.ADJ_WALLET: [CallbackQueryHandler(adj_pick_wallet)],
            State.ADJ_TARGET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, adj_target)
            ],
            State.ADJ_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, adj_note)],
            State.ADJ_CONFIRM: [CallbackQueryHandler(adj_confirm_buttons)],
            State.W_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_name)
            ],
            State.W_ARCH_PICK: [CallbackQueryHandler(wallet_archive_pick)],
            State.DEBT_MENU: [CallbackQueryHandler(debt_menu_handler)],
            State.DEBT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_name)
            ],
            State.DEBT_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_amount)
            ],
            State.DEBT_WALLET: [CallbackQueryHandler(debt_wallet)],
            State.DEBT_PAY_PICK: [CallbackQueryHandler(debt_pay_pick)],
            State.DEBT_PAY_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_pay_amount)
            ],
            State.DEBT_PAY_WALLET: [CallbackQueryHandler(debt_pay_wallet)],
            State.DEBT_ADJ_PICK: [CallbackQueryHandler(debt_adj_pick)],
            State.DEBT_ADJ_TARGET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_target)
            ],
            State.DEBT_ADJ_NOTE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_adj_note)
            ],
            State.DEBT_ADJ_CONFIRM: [CallbackQueryHandler(debt_adj_confirm)],
        },
        fallbacks=[CallbackQueryHandler(on_cancel, pattern=r"^cancel$")],
    )
    app.add_handler(conv)

    app.add_handler(
        CallbackQueryHandler(
            main_menu_router,
            pattern=r"^(menu:|ops:|wallet:|stat:|debt:)",
        )
    )

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text)
    )

    logger.info("✅ Бот запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
