"""
Telegram-бот для автоматической выдачи файлов (читов) по диплинкам.
Стек: Python 3.10+, python-telegram-bot 20.x, aiosqlite, asyncio.

Установка:
    pip install "python-telegram-bot[job-queue]" aiosqlite

Запуск:
    python bot.py
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

import aiosqlite
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    filters,
    MessageHandler,
)

# ---------------------------------------------------------------------------
# Конфигурация — не менять
# ---------------------------------------------------------------------------

BOT_TOKEN          = "8422981242:AAE0R6dt07RkG-dYR4QPEpWtHoCV4qP9YQU"
MAIN_CHANNEL_ID    = -1003954362324
MAIN_CHANNEL_URL   = "https://t.me/ApkSided"
ARCHIVE_CHANNEL_ID = -1004292919952
ADMIN_ID           = 8325037674
DB_PATH            = "cheat_bot.db"

# ---------------------------------------------------------------------------
# Состояния ConversationHandler
# ---------------------------------------------------------------------------

(
    STATE_WAIT_KEY,
    STATE_WAIT_FILE,
    STATE_BROADCAST,
    STATE_ADD_CHANNEL_ID,
    STATE_ADD_CHANNEL_URL,
    STATE_ADD_CHANNEL_NAME,
) = range(6)

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# База данных
# ---------------------------------------------------------------------------

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username    TEXT,
                reg_date    TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                cheat_key TEXT UNIQUE NOT NULL,
                file_id   TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS required_channels (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id  INTEGER UNIQUE NOT NULL,
                channel_url TEXT NOT NULL,
                title       TEXT NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        # Главный канал всегда присутствует
        await db.execute(
            """
            INSERT OR IGNORE INTO required_channels (channel_id, channel_url, title, active)
            VALUES (?, ?, ?, 1)
            """,
            (MAIN_CHANNEL_ID, MAIN_CHANNEL_URL, "ApkSided (основной)"),
        )
        await db.commit()
    logger.info("БД инициализирована: %s", DB_PATH)


async def register_user(telegram_id: int, username: Optional[str]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (telegram_id, username, reg_date) VALUES (?, ?, ?)",
            (telegram_id, username, datetime.utcnow().isoformat()),
        )
        await db.commit()


async def get_file_id(cheat_key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT file_id FROM files WHERE cheat_key = ?", (cheat_key,)
        ) as cursor:
            row = await cursor.fetchone()
    return row[0] if row else None


async def save_file(cheat_key: str, file_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO files (cheat_key, file_id) VALUES (?, ?)",
            (cheat_key, file_id),
        )
        await db.commit()


async def get_stats() -> tuple:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            users_count = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM files") as cur:
            files_count = (await cur.fetchone())[0]
    return users_count, files_count


async def get_all_user_ids() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT telegram_id FROM users") as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]


# --- Каналы ---

async def get_active_channels() -> list:
    """Возвращает список активных каналов: [(channel_id, channel_url, title), ...]"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT channel_id, channel_url, title FROM required_channels WHERE active = 1"
        ) as cur:
            return await cur.fetchall()


async def get_all_channels() -> list:
    """Все каналы (активные и нет): [(id, channel_id, channel_url, title, active), ...]"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, channel_id, channel_url, title, active FROM required_channels ORDER BY id"
        ) as cur:
            return await cur.fetchall()


async def add_channel(channel_id: int, channel_url: str, title: str) -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO required_channels (channel_id, channel_url, title, active) VALUES (?, ?, ?, 1)",
                (channel_id, channel_url, title),
            )
            await db.commit()
        return True
    except Exception:
        return False


async def remove_channel(channel_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM required_channels WHERE channel_id = ? AND channel_id != ?",
            (channel_id, MAIN_CHANNEL_ID),
        )
        await db.commit()
    return True


async def toggle_channel(channel_id: int) -> Optional[int]:
    """Переключает active. Возвращает новый статус или None если не найден."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT active FROM required_channels WHERE channel_id = ?", (channel_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        new_status = 0 if row[0] == 1 else 1
        await db.execute(
            "UPDATE required_channels SET active = ? WHERE channel_id = ?",
            (new_status, channel_id),
        )
        await db.commit()
    return new_status

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

async def check_all_subscriptions(bot: Bot, user_id: int) -> list:
    """
    Проверяет подписку на все активные каналы.
    Возвращает список каналов на которые НЕ подписан: [(channel_id, channel_url, title), ...]
    """
    channels   = await get_active_channels()
    not_subbed = []
    for channel_id, channel_url, title in channels:
        try:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status not in ("member", "administrator", "creator"):
                not_subbed.append((channel_id, channel_url, title))
        except BadRequest as e:
            logger.warning("Ошибка проверки канала %d: %s", channel_id, e)
    return not_subbed


def build_subscribe_keyboard(missing: list, cheat_key: str, bot_username: str) -> InlineKeyboardMarkup:
    buttons = []
    for _, url, title in missing:
        buttons.append([InlineKeyboardButton("📢 {}".format(title), url=url)])
    buttons.append([InlineKeyboardButton(
        "✅ Я подписался — скачать",
        url="https://t.me/{}?start={}".format(bot_username, cheat_key),
    )])
    return InlineKeyboardMarkup(buttons)


def build_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",        callback_data="admin_stats")],
        [InlineKeyboardButton("➕ Добавить чит",      callback_data="admin_add_cheat")],
        [InlineKeyboardButton("📢 Рассылка",          callback_data="admin_broadcast")],
        [InlineKeyboardButton("📋 Список каналов",    callback_data="admin_list_channels")],
        [InlineKeyboardButton("➕ Добавить канал",    callback_data="admin_add_channel")],
    ])

# ---------------------------------------------------------------------------
# USER — /start
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user      = update.effective_user
    args      = context.args
    cheat_key = args[0].strip() if args else None

    await register_user(user.id, user.username)
    logger.info("user_id=%d key=%s", user.id, cheat_key)

    if not cheat_key:
        await update.message.reply_text(
            "👋 Привет! Переходи по ссылкам из нашего канала @ApkSided, "
            "чтобы скачивать файлы."
        )
        return

    bot_info = await context.bot.get_me()
    missing  = await check_all_subscriptions(context.bot, user.id)

    if missing:
        channels_text = "\n".join("• <b>{}</b>".format(t) for _, _, t in missing)
        await update.message.reply_text(
            "⚠️ Для скачивания файла необходимо подписаться на:\n\n{}\n\n"
            "После подписки нажми кнопку ниже 👇".format(channels_text),
            reply_markup=build_subscribe_keyboard(missing, cheat_key, bot_info.username),
            parse_mode=ParseMode.HTML,
        )
        return

    file_id = await get_file_id(cheat_key)
    if file_id is None:
        await update.message.reply_text("❌ Файл не найден или ссылка устарела.")
        return

    try:
        await context.bot.send_document(
            chat_id=user.id,
            document=file_id,
            caption="✅ Ваш файл готов к скачиванию!",
        )
        logger.info("Файл '%s' отправлен user_id=%d", cheat_key, user.id)
    except BadRequest as e:
        logger.error("Ошибка отправки '%s': %s", cheat_key, e)
        await update.message.reply_text("⚠️ Не удалось отправить файл. Попробуйте позже.")

# ---------------------------------------------------------------------------
# ARCHIVE — перехват документов из архив-канала
# ---------------------------------------------------------------------------

async def handle_archive_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.channel_post
    if not message or not message.document:
        return

    cheat_key = context.bot_data.get("pending_cheat_key")
    if not cheat_key:
        return

    file_id = message.document.file_id
    await save_file(cheat_key, file_id)
    context.bot_data.pop("pending_cheat_key", None)

    bot_info  = await context.bot.get_me()
    deep_link = "https://t.me/{}?start={}".format(bot_info.username, cheat_key)

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            "✅ Чит <code>{}</code> сохранён из архив-канала!\n\n"
            "🔗 Готовая ссылка для публикации в @ApkSided:\n"
            "<code>{}</code>"
        ).format(cheat_key, deep_link),
        parse_mode=ParseMode.HTML,
    )
    logger.info("Архив: '%s' file_id=%s сохранён", cheat_key, file_id)

# ---------------------------------------------------------------------------
# ADMIN — панель
# ---------------------------------------------------------------------------

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "🔧 <b>Панель администратора</b>\n\nВыберите действие:",
        reply_markup=build_admin_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def cb_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    users_count, files_count = await get_stats()
    await query.message.reply_text(
        "📊 <b>Статистика бота</b>\n\n"
        "👤 Пользователей: <b>{}</b>\n"
        "📁 Читов в базе:  <b>{}</b>".format(users_count, files_count),
        parse_mode=ParseMode.HTML,
    )

# ---------------------------------------------------------------------------
# ADMIN — список каналов и управление
# ---------------------------------------------------------------------------

async def cb_list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query    = update.callback_query
    await query.answer()
    channels = await get_all_channels()

    if not channels:
        await query.message.reply_text("Каналов нет.")
        return

    lines = ["📋 <b>Каналы обязательной подписки:</b>\n"]
    for row_id, ch_id, ch_url, title, active in channels:
        status = "✅ активен" if active else "⏸ отключён"
        lines.append(
            "{} — <b>{}</b>\n"
            "   ID: <code>{}</code>\n"
            "   {} | /ch_toggle_{} | /ch_del_{}".format(
                status, title, ch_id, ch_url, ch_id, ch_id
            )
        )

    await query.message.reply_text(
        "\n\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

# ---------------------------------------------------------------------------
# ADMIN — добавление канала (ConversationHandler)
# ---------------------------------------------------------------------------

async def cb_add_channel_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "➕ <b>Добавление канала обязательной подписки</b>\n\n"
        "Шаг 1/3 — отправь <b>числовой ID</b> канала.\n\n"
        "Как узнать ID: перешли любое сообщение из канала боту @username_to_id_bot\n\n"
        "/cancel — отменить.",
        parse_mode=ParseMode.HTML,
    )
    return STATE_ADD_CHANNEL_ID


async def fsm_channel_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    try:
        ch_id = int(text)
    except ValueError:
        await update.message.reply_text(
            "⚠️ Это не число. Введи числовой ID канала (например: <code>-1001234567890</code>):",
            parse_mode=ParseMode.HTML,
        )
        return STATE_ADD_CHANNEL_ID

    context.user_data["new_ch_id"] = ch_id
    await update.message.reply_text(
        "Шаг 2/3 — отправь <b>ссылку</b> на канал (например: <code>https://t.me/mychannel</code>):",
        parse_mode=ParseMode.HTML,
    )
    return STATE_ADD_CHANNEL_URL


async def fsm_channel_get_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    url = update.message.text.strip()
    if not url.startswith("https://t.me/"):
        await update.message.reply_text(
            "⚠️ Ссылка должна начинаться с <code>https://t.me/</code>. Попробуй ещё раз:",
            parse_mode=ParseMode.HTML,
        )
        return STATE_ADD_CHANNEL_URL

    context.user_data["new_ch_url"] = url
    await update.message.reply_text(
        "Шаг 3/3 — отправь <b>название</b> канала (будет показано пользователям):",
        parse_mode=ParseMode.HTML,
    )
    return STATE_ADD_CHANNEL_NAME


async def fsm_channel_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title  = update.message.text.strip()
    ch_id  = context.user_data["new_ch_id"]
    ch_url = context.user_data["new_ch_url"]

    ok = await add_channel(ch_id, ch_url, title)
    if ok:
        await update.message.reply_text(
            "✅ Канал <b>{}</b> (<code>{}</code>) добавлен!\n"
            "Теперь пользователи должны подписаться на него перед скачиванием.".format(title, ch_id),
            parse_mode=ParseMode.HTML,
        )
        logger.info("Добавлен канал: %s (%d)", title, ch_id)
    else:
        await update.message.reply_text(
            "⚠️ Не удалось добавить канал. Возможно, такой ID уже есть в базе."
        )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# ADMIN — команды управления каналами (/ch_toggle_ID, /ch_del_ID)
# ---------------------------------------------------------------------------

async def cmd_ch_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        ch_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /ch_toggle_<ID>")
        return

    new_status = await toggle_channel(ch_id)
    if new_status is None:
        await update.message.reply_text("❌ Канал не найден.")
        return

    status_text = "✅ активирован" if new_status == 1 else "⏸ отключён"
    await update.message.reply_text(
        "Канал <code>{}</code> теперь {}.".format(ch_id, status_text),
        parse_mode=ParseMode.HTML,
    )


async def cmd_ch_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        ch_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /ch_del_<ID>")
        return

    if ch_id == MAIN_CHANNEL_ID:
        await update.message.reply_text("❌ Основной канал нельзя удалить.")
        return

    await remove_channel(ch_id)
    await update.message.reply_text(
        "🗑 Канал <code>{}</code> удалён из обязательной подписки.".format(ch_id),
        parse_mode=ParseMode.HTML,
    )
    logger.info("Удалён канал: %d", ch_id)

# ---------------------------------------------------------------------------
# ADMIN — добавление чита (ConversationHandler)
# ---------------------------------------------------------------------------

async def cb_add_cheat_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "➕ <b>Добавление нового чита</b>\n\n"
        "Введите уникальный текстовый ключ (например: <code>pubg_v1</code>).\n"
        "Разрешены: латиница, цифры, дефис, подчёркивание.",
        parse_mode=ParseMode.HTML,
    )
    return STATE_WAIT_KEY


async def fsm_receive_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cheat_key = update.message.text.strip().lower()

    if not re.fullmatch(r"[a-z0-9_\-]+", cheat_key):
        await update.message.reply_text(
            "⚠️ Недопустимые символы. Только латиница, цифры, дефис, подчёркивание.\n"
            "Попробуйте ещё раз:"
        )
        return STATE_WAIT_KEY

    context.user_data["cheat_key"]         = cheat_key
    context.bot_data["pending_cheat_key"]  = cheat_key

    await update.message.reply_text(
        "✅ Ключ <code>{}</code> принят.\n\n"
        "📁 Теперь загрузите файл одним из двух способов:\n"
        "• Опубликуйте его в архив-канале <code>{}</code>\n"
        "• Или отправьте файл прямо сюда в этот чат\n\n"
        "/cancel — отменить.".format(cheat_key, ARCHIVE_CHANNEL_ID),
        parse_mode=ParseMode.HTML,
    )
    return STATE_WAIT_FILE


async def fsm_receive_file_dm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cheat_key = context.user_data.get("cheat_key")
    file_id   = update.message.document.file_id

    await save_file(cheat_key, file_id)
    context.bot_data.pop("pending_cheat_key", None)

    bot_info  = await context.bot.get_me()
    deep_link = "https://t.me/{}?start={}".format(bot_info.username, cheat_key)

    await update.message.reply_text(
        "✅ Чит <code>{}</code> успешно сохранён!\n\n"
        "🔗 Готовая ссылка:\n<code>{}</code>".format(cheat_key, deep_link),
        parse_mode=ParseMode.HTML,
    )
    logger.info("ЛС: '%s' file_id=%s сохранён", cheat_key, file_id)
    return ConversationHandler.END


async def fsm_wrong_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "⚠️ Ожидается <b>файл (документ)</b>.\n"
        "Пришлите файл сюда или загрузите его в архив-канал.",
        parse_mode=ParseMode.HTML,
    )
    return STATE_WAIT_FILE


async def fsm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.bot_data.pop("pending_cheat_key", None)
    await update.message.reply_text("❌ Операция отменена.")
    return ConversationHandler.END

# ---------------------------------------------------------------------------
# ADMIN — рассылка (ConversationHandler)
# ---------------------------------------------------------------------------

async def cb_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "📢 <b>Рассылка</b>\n\n"
        "Отправьте сообщение для рассылки.\n"
        "Поддерживаются: текст, фото, документ.\n\n"
        "/cancel — отменить.",
        parse_mode=ParseMode.HTML,
    )
    return STATE_BROADCAST


async def fsm_do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_ids = await get_all_user_ids()
    total    = len(user_ids)

    if total == 0:
        await update.message.reply_text("📭 В базе нет пользователей.")
        return ConversationHandler.END

    await update.message.reply_text(
        "🚀 Начинаю рассылку для <b>{}</b> пользователей...".format(total),
        parse_mode=ParseMode.HTML,
    )

    success_count = 0
    error_count   = 0

    for user_id in user_ids:
        try:
            await update.message.copy(chat_id=user_id)
            success_count += 1
        except Forbidden:
            error_count += 1
        except (BadRequest, Exception) as e:
            error_count += 1
            logger.warning("Рассылка user_id=%d: %s", user_id, e)
        await asyncio.sleep(0.05)

    await update.message.reply_text(
        "✅ <b>Рассылка завершена</b>\n\n"
        "📨 Всего: <b>{}</b>\n"
        "✔️ Успешно: <b>{}</b>\n"
        "❌ Ошибок: <b>{}</b>".format(total, success_count, error_count),
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END

# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def run() -> None:
    await init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # --- Добавление чита ---
    add_cheat_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_add_cheat_start, pattern="^admin_add_cheat$")],
        states={
            STATE_WAIT_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, fsm_receive_key),
            ],
            STATE_WAIT_FILE: [
                MessageHandler(filters.Document.ALL, fsm_receive_file_dm),
                MessageHandler(filters.TEXT & ~filters.COMMAND, fsm_wrong_type),
            ],
        },
        fallbacks=[CommandHandler("cancel", fsm_cancel)],
        per_user=True, per_chat=True,
    )

    # --- Добавление канала ---
    add_channel_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_add_channel_start, pattern="^admin_add_channel$")],
        states={
            STATE_ADD_CHANNEL_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, fsm_channel_get_id)],
            STATE_ADD_CHANNEL_URL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, fsm_channel_get_url)],
            STATE_ADD_CHANNEL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, fsm_channel_get_name)],
        },
        fallbacks=[CommandHandler("cancel", fsm_cancel)],
        per_user=True, per_chat=True,
    )

    # --- Рассылка ---
    broadcast_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_broadcast_start, pattern="^admin_broadcast$")],
        states={
            STATE_BROADCAST: [
                MessageHandler(
                    (filters.TEXT | filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND,
                    fsm_do_broadcast,
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", fsm_cancel)],
        per_user=True, per_chat=True,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CallbackQueryHandler(cb_stats,         pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(cb_list_channels, pattern="^admin_list_channels$"))
    app.add_handler(add_cheat_conv)
    app.add_handler(add_channel_conv)
    app.add_handler(broadcast_conv)

    # Динамические команды /ch_toggle_ID и /ch_del_ID
    app.add_handler(CommandHandler("ch_toggle", cmd_ch_toggle))
    app.add_handler(CommandHandler("ch_del",    cmd_ch_del))

    # Перехват из архив-канала
    app.add_handler(
        MessageHandler(
            filters.Chat(ARCHIVE_CHANNEL_ID) & filters.Document.ALL,
            handle_archive_file,
        )
    )

    logger.info(
        "Бот запущен | @ApkSided (%d) | Архив: %d | Админ: %d",
        MAIN_CHANNEL_ID, ARCHIVE_CHANNEL_ID, ADMIN_ID,
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(run())
