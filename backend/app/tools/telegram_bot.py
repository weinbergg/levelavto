from __future__ import annotations

import os
import subprocess
from typing import Optional

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters

from ..db import SessionLocal
from ..services.parser_control_service import ParserControlService
from ..services.parsing_data_service import ParsingDataService
import asyncio

STOP_FILE = os.environ.get("EMAVTO_STOP_FILE", "/tmp/emavto_stop")
CHUNK_PAGES = int(os.environ.get("CHUNK_PAGES", "10"))
CHUNK_PAUSE_SEC = int(os.environ.get("CHUNK_PAUSE_SEC", "60"))
CHUNK_MAX_RUNTIME_SEC = int(os.environ.get("CHUNK_MAX_RUNTIME_SEC", "3600"))
CHUNK_TOTAL_PAGES = int(os.environ.get("CHUNK_TOTAL_PAGES", "0"))

MODE_FULL = "full"
MODE_INCREMENTAL = "incremental"

def build_chunk_cmd(mode: str = MODE_FULL, start_page: str | None = None) -> list[str]:
    cmd = [
        "python",
        "-m",
        "backend.app.tools.emavto_chunk_runner",
        "--chunk-pages",
        str(CHUNK_PAGES),
        "--pause-sec",
        str(CHUNK_PAUSE_SEC),
        "--max-runtime-sec",
        str(CHUNK_MAX_RUNTIME_SEC),
        "--total-pages",
        str(CHUNK_TOTAL_PAGES),
        "--mode",
        mode,
    ]
    if start_page:
        cmd.extend(["--start-page", start_page])
    return cmd
POLL_INTERVAL = int(os.environ.get("EMAVTO_PROGRESS_POLL_SEC", "60"))

running_proc: Optional[subprocess.Popen] = None
monitor_task: Optional[asyncio.Task] = None
subscribers: set[str] = set()
SET_CHUNKS = 1


def allowed_user(update: Update) -> bool:
    allowed_ids = os.environ.get("TELEGRAM_ALLOWED_IDS")
    if not allowed_ids:
        return True
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    return chat_id in {x.strip() for x in allowed_ids.split(",") if x.strip()}


def add_subscriber(chat_id: str) -> None:
    subscribers.add(chat_id)


def remove_subscriber(chat_id: str) -> None:
    subscribers.discard(chat_id)


def get_progress() -> str | None:
    db = SessionLocal()
    try:
        ds = ParsingDataService(db)
        return ds.get_progress("emavto_klg.last_page_full")
    finally:
        db.close()


def format_status() -> str:
    db = SessionLocal()
    try:
        pcs = ParserControlService(db)
        runs = pcs.list_recent_parser_runs(limit=1)
        run_info = "Нет запусков"
        if runs:
            r = runs[0]
            run_info = (
                f"Run #{r.id} status={r.status} "
                f"seen={r.total_seen} ins={r.inserted} upd={r.updated} "
                f"started={r.started_at} finished={r.finished_at}"
            )
        ds = ParsingDataService(db)
        progress = ds.get_progress("emavto_klg.last_page_full")
        progress_line = f"last_page_full={progress or '—'}"
        return f"{run_info}\n{progress_line}"
    finally:
        db.close()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    add_subscriber(chat_id)
    await show_menu(update)


async def show_menu(update: Update) -> None:
    keyboard = [
        [InlineKeyboardButton("Статус", callback_data="status")],
        [InlineKeyboardButton("Запуск (текущие настройки)", callback_data="start_parse")],
        [
            InlineKeyboardButton("Полный с начала", callback_data="start_full"),
            InlineKeyboardButton("Обновление каталога", callback_data="start_incremental"),
        ],
        [InlineKeyboardButton("Настроить чанки", callback_data="set_chunks")],
        [InlineKeyboardButton("Стоп (стоп-файл)", callback_data="stop_parse")],
    ]
    text = (
        f"Бот готов.\n"
        f"Текущие параметры:\n"
        f"pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}s, runtime={CHUNK_MAX_RUNTIME_SEC}s, total={CHUNK_TOTAL_PAGES}, mode=full\n"
        f"Кнопками ниже можно менять и запускать."
    )
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    text = format_status()
    await update.message.reply_text(text)


async def cmd_start_parse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global running_proc, monitor_task
    if not allowed_user(update):
        return
    if running_proc and running_proc.poll() is None:
        await update.message.reply_text("Уже запущен.")
        return
    # удаляем стоп-файл
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    try:
        running_proc = subprocess.Popen(build_chunk_cmd(MODE_FULL))
        await update.message.reply_text(
            f"Запуск парсинга. PID={running_proc.pid} (pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}s, runtime={CHUNK_MAX_RUNTIME_SEC}s, total={CHUNK_TOTAL_PAGES})"
        )
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
        monitor_task = context.application.create_task(
            monitor_progress(context))
    except Exception as exc:
        await update.message.reply_text(f"Ошибка запуска: {exc}")


async def cmd_stop_parse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    try:
        with open(STOP_FILE, "w", encoding="utf-8") as f:
            f.write("stop")
        msg = f"Стоп-файл создан: {STOP_FILE}. Текущий чанк завершится и остановится."
    except Exception as exc:
        msg = f"Не удалось создать стоп-файл: {exc}"
    await update.message.reply_text(msg)


async def cmd_start_full(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_with_mode(update, context, MODE_FULL, start_page="1")


async def cmd_start_incremental(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_with_mode(update, context, MODE_INCREMENTAL, start_page=None)


async def start_with_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, start_page: str | None) -> None:
    global running_proc, monitor_task
    if not allowed_user(update):
        return
    if running_proc and running_proc.poll() is None:
        await update.message.reply_text("Уже запущен.")
        return
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    try:
        cmd = build_chunk_cmd(mode=mode, start_page=start_page)
        running_proc = subprocess.Popen(cmd)
        await update.message.reply_text(
            f"Запуск ({mode}) PID={running_proc.pid} pages={CHUNK_PAGES} pause={CHUNK_PAUSE_SEC}s runtime={CHUNK_MAX_RUNTIME_SEC}s total={CHUNK_TOTAL_PAGES} start_page={start_page or 'auto'}"
        )
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
        monitor_task = context.application.create_task(monitor_progress(context))
    except Exception as exc:
        await update.message.reply_text(f"Ошибка запуска: {exc}")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    add_subscriber(chat_id)
    await update.message.reply_text("Подписка на уведомления включена.")


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    remove_subscriber(chat_id)
    await update.message.reply_text("Подписка на уведомления отключена.")


async def set_chunks_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not allowed_user(update):
        return ConversationHandler.END
    await update.message.reply_text(
        f"Введите chunk_pages (число) и необязательно pause_sec/runtime_sec/total_pages через пробел.\n"
        f"Пример: 12 45 3000 0  (pages=12, pause=45, runtime=3000, total=0)\n"
        f"Сейчас: pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}, runtime={CHUNK_MAX_RUNTIME_SEC}, total={CHUNK_TOTAL_PAGES}"
    )
    return SET_CHUNKS


async def set_chunks_apply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    global CHUNK_PAGES, CHUNK_PAUSE_SEC, CHUNK_MAX_RUNTIME_SEC, CHUNK_TOTAL_PAGES
    if not allowed_user(update):
        return ConversationHandler.END
    parts = (update.message.text or "").strip().split()
    try:
        if len(parts) >= 1:
            CHUNK_PAGES = max(1, int(parts[0]))
        if len(parts) >= 2:
            CHUNK_PAUSE_SEC = max(0, int(parts[1]))
        if len(parts) >= 3:
            CHUNK_MAX_RUNTIME_SEC = max(300, int(parts[2]))
        if len(parts) >= 4:
            CHUNK_TOTAL_PAGES = int(parts[3])
        await update.message.reply_text(
            f"Ок. pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}, runtime={CHUNK_MAX_RUNTIME_SEC}, total={CHUNK_TOTAL_PAGES}"
        )
    except Exception:
        await update.message.reply_text("Не понял формат. Пример: 12 45 3000 0")
    return ConversationHandler.END


async def broadcast(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    for chat_id in list(subscribers):
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            # если чат недоступен — убираем
            remove_subscriber(chat_id)


async def monitor_progress(context: ContextTypes.DEFAULT_TYPE) -> None:
    last_page = get_progress()
    try:
        while running_proc and running_proc.poll() is None:
            await asyncio.sleep(POLL_INTERVAL)
            cur = get_progress()
            if cur and cur != last_page:
                await broadcast(context, f"Парсинг: достигли страницы {cur}")
                last_page = cur
        # процесс завершился
        rc = running_proc.poll() if running_proc else None
        cur = get_progress()
        if rc is None:
            return
        if rc == 0:
            await broadcast(context, f"Парсинг завершён. last_page={cur or '—'}")
        else:
            await broadcast(context, f"Парсинг остановлен с ошибкой (rc={rc}). last_page={cur or '—'}")
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        await broadcast(context, f"Мониторинг упал: {exc}")


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("start_parse", cmd_start_parse))
    application.add_handler(CommandHandler("start_full", cmd_start_full))
    application.add_handler(CommandHandler("start_incremental", cmd_start_incremental))
    application.add_handler(CommandHandler("stop_parse", cmd_stop_parse))
    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    application.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("set_chunks", set_chunks_prompt)],
            states={SET_CHUNKS: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_chunks_apply)]},
            fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
        )
    )
    application.add_handler(
        MessageHandler(filters.Regex("^/menu$"), lambda u, c: show_menu(u))
    )
    application.add_handler(
        MessageHandler(filters.Regex("^/set_chunks$"), lambda u, c: set_chunks_prompt(u, c))
    )
    application.add_handler(
        MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, lambda u, c: None)
    )

    print("Bot started. Commands: /start /status /start_parse /stop_parse")
    application.run_polling()


if __name__ == "__main__":
    main()
