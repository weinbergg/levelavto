from __future__ import annotations

import os
import sys
import glob
import subprocess
import json
from typing import Optional, Dict, List, Any
from pathlib import Path
import datetime as dt

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from ..db import SessionLocal
from ..services.parser_control_service import ParserControlService
from ..services.parsing_data_service import ParsingDataService
from ..services.calculator_config_service import CalculatorConfigService
from ..services.calculator_extractor import CalculatorExtractor
from ..utils.recommended_config import load_config, save_config
import asyncio
from .telegram_bot_jobs import format_status as format_job_status

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
AWAITING_CHUNKS = "awaiting_chunks"
AWAITING_CALC = "awaiting_calc_upload"
CALC_PENDING = "calc_pending"
AWAITING_RECO = "awaiting_recommended"
MOBILE_DOWNLOAD_DIR = Path(os.environ.get("MOBILE_DOWNLOAD_DIR", "/app/tmp"))
MOBILE_FILENAME = "mobilede_active_offers.csv"
MOBILE_SCRIPT = ["bash", "scripts/fetch_mobilede_csv.sh"]
EMAVTO_SCRIPT = ["bash", "scripts/run_emavto_job.sh"]
JOBS_DIR = Path("backend/app/runtime/jobs")


async def reply(update: Update, text: str) -> None:
    msg = update.message or (
        update.callback_query.message if update.callback_query else None)
    if msg:
        await msg.reply_text(text)


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


def load_job_result(job: str) -> Dict[str, Any] | None:
    path = JOBS_DIR / f"{job}_last.json"
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def summarize_job(job: str) -> str:
    data = load_job_result(job)
    return format_job_status(job, data) if data else f"{job}: нет данных"


def summarize_errors() -> str:
    msgs = []
    for job in ("mobilede", "emavto"):
        data = load_job_result(job)
        errs = (data or {}).get("errors") or []
        if errs:
            msgs.append(f"{job}:\n" + "\n".join(errs[:10]))
    return "\n\n".join(msgs) if msgs else "Ошибок не зафиксировано."


async def run_job(script: List[str], job: str, update: Update) -> None:
    await reply(update, f"Запускаю {job}...")
    try:
        rc = subprocess.run(script, check=False, capture_output=True, text=True)
        if rc.returncode != 0:
            await reply(update, f"{job} завершился с ошибкой rc={rc.returncode}\n{rc.stderr[:4000]}")
            return
    except Exception as exc:
        await reply(update, f"Ошибка запуска {job}: {exc}")
        return
    summary = summarize_job(job)
    await reply(update, f"{job} завершён.\n{summary}")


def _fmt_val(val) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        if val.is_integer():
            return f"{int(val)}"
        return f"{val:.2f}".rstrip("0").rstrip(".")
    return str(val)


def _fmt_int(val: Any) -> str:
    try:
        return f"{int(val):,}".replace(",", " ")
    except Exception:
        return "—"


def format_reco_config(cfg: Dict[str, Any]) -> str:
    return (
        "Рекомендуемые авто — текущие диапазоны:\n"
        f"Возраст до: {_fmt_val(cfg.get('max_age_years'))} лет\n"
        f"Цена: {_fmt_int(cfg.get('price_min'))} — {_fmt_int(cfg.get('price_max'))} ₽\n"
        f"Пробег до: {_fmt_int(cfg.get('mileage_max'))} км"
    )


async def show_recommended_menu(update: Update) -> None:
    cfg = load_config()
    keyboard = [
        [InlineKeyboardButton("Возраст (лет)", callback_data="reco_age")],
        [InlineKeyboardButton("Цена мин/макс", callback_data="reco_price")],
        [InlineKeyboardButton("Пробег макс", callback_data="reco_mileage")],
        [InlineKeyboardButton("Назад", callback_data="menu")],
    ]
    text = format_reco_config(cfg)
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


def _diff_map(old: Dict[str, float], new: Dict[str, float], label: str) -> List[str]:
    changes: List[str] = []
    keys = set(old.keys()) | set(new.keys())
    for k in sorted(keys):
        if old.get(k) != new.get(k):
            changes.append(f"{label}.{k}: {_fmt_val(old.get(k))} → {_fmt_val(new.get(k))}")
    return changes


def _table_to_map(rows: List[Dict[str, any]], key_fields: List[str], val_field: str) -> Dict[str, any]:
    res = {}
    for r in rows or []:
        key = "|".join(str(r.get(f)) for f in key_fields)
        res[key] = r.get(val_field)
    return res


def build_calc_diff(old: Dict[str, any] | None, new: Dict[str, any]) -> tuple[list[str], int]:
    old = old or {}
    blocks: List[str] = []
    total = 0

    def add_block(title: str, entries: List[str]):
        nonlocal total
        blocks.append(f"\n{title}:")
        if not entries:
            blocks.append("• без изменений")
        else:
            total += len(entries)
            blocks.extend([f"• {e}" for e in entries])

    old_scen = old.get("scenarios", {}) if old else {}
    new_scen = new.get("scenarios", {})

    # under 3
    add_block(
        "до 3 лет — расходы",
        _diff_map(old_scen.get("under_3", {}).get("expenses", {}),
                  new_scen.get("under_3", {}).get("expenses", {}), "expenses"),
    )
    add_block(
        "до 3 лет — пошлина EUR/cc",
        _diff_map(
            _table_to_map(old_scen.get("under_3", {}).get("duty_by_cc", []), ["from", "to"], "eur_per_cc"),
            _table_to_map(new_scen.get("under_3", {}).get("duty_by_cc", []), ["from", "to"], "eur_per_cc"),
            "duty",
        ),
    )

    # 3-5
    add_block(
        "3–5 лет — расходы",
        _diff_map(old_scen.get("3_5", {}).get("expenses", {}),
                  new_scen.get("3_5", {}).get("expenses", {}), "expenses"),
    )
    add_block(
        "3–5 лет — пошлина EUR/cc",
        _diff_map(
            _table_to_map(old_scen.get("3_5", {}).get("duty_by_cc", []), ["from", "to"], "eur_per_cc"),
            _table_to_map(new_scen.get("3_5", {}).get("duty_by_cc", []), ["from", "to"], "eur_per_cc"),
            "duty",
        ),
    )

    # electric
    add_block(
        "Электро — расходы",
        _diff_map(old_scen.get("electric", {}).get("expenses", {}),
                  new_scen.get("electric", {}).get("expenses", {}), "expenses"),
    )
    add_block(
        "Электро — акциз (₽/л.с.)",
        _diff_map(
            _table_to_map(old_scen.get("electric", {}).get("excise_by_hp", []), ["from_hp", "to_hp"], "rub_per_hp"),
            _table_to_map(new_scen.get("electric", {}).get("excise_by_hp", []), ["from_hp", "to_hp"], "rub_per_hp"),
            "excise",
        ),
    )
    add_block(
        "Электро — суммы по мощности/возрасту",
        _diff_map(
            _table_to_map(old_scen.get("electric", {}).get("power_fee", []), ["from_hp", "to_hp", "age_bucket"], "rub"),
            _table_to_map(new_scen.get("electric", {}).get("power_fee", []), ["from_hp", "to_hp", "age_bucket"], "rub"),
            "power_fee",
        ),
    )
    return blocks, total


def format_calc_diff_message(old_payload: Dict[str, any] | None, new_payload: Dict[str, any], limit: int = 80) -> tuple[str, int]:
    blocks, total = build_calc_diff(old_payload, new_payload)
    shown = 0
    lines: List[str] = ["Изменения в конфиге калькулятора"]
    for line in blocks:
        if line.startswith("\n"):
            lines.append(line)
            continue
        if line.startswith("•"):
            if shown >= limit:
                break
            shown += 1
        lines.append(line)
    if total > shown:
        lines.append(f"• ... ещё {total - shown} изменений")
    return "\n".join(lines), total


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    add_subscriber(chat_id)
    await show_menu(update)


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    await show_menu(update)


async def show_menu(update: Update) -> None:
    keyboard = [
        [InlineKeyboardButton("Обновить mobile.de", callback_data="run_mobilede")],
        [InlineKeyboardButton("Обновить emavto", callback_data="run_emavto")],
        [InlineKeyboardButton("Статус обновлений", callback_data="status_updates")],
        [InlineKeyboardButton("Рекомендуемые — диапазоны", callback_data="reco_menu")],
    ]
    text = "Бот готов. Выберите действие:"
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    text = format_status()
    await reply(update, text)


async def cmd_updates_status(update: Update) -> None:
    if not allowed_user(update):
        return
    await reply(update, f"{summarize_job('mobilede')}\n\n{summarize_job('emavto')}")


async def cmd_start_parse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global running_proc, monitor_task
    if not allowed_user(update):
        return
    if running_proc and running_proc.poll() is None:
        await reply(update, "Уже запущен.")
        return
    # удаляем стоп-файл
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    try:
        running_proc = subprocess.Popen(build_chunk_cmd(MODE_FULL))
        await reply(update,
                    f"Запуск парсинга. PID={running_proc.pid} (pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}s, runtime={CHUNK_MAX_RUNTIME_SEC}s, total={CHUNK_TOTAL_PAGES})"
                    )
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
        monitor_task = context.application.create_task(
            monitor_progress(context))
    except Exception as exc:
        await reply(update, f"Ошибка запуска: {exc}")


async def cmd_stop_parse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    try:
        with open(STOP_FILE, "w", encoding="utf-8") as f:
            f.write("stop")
        msg = f"Стоп-файл создан: {STOP_FILE}. Текущий чанк завершится и остановится."
    except Exception as exc:
        msg = f"Не удалось создать стоп-файл: {exc}"
    await reply(update, msg)


async def cmd_start_full(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_with_mode(update, context, MODE_FULL, start_page="1")


async def cmd_start_incremental(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_with_mode(update, context, MODE_INCREMENTAL, start_page=None)


async def start_with_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, start_page: str | None) -> None:
    global running_proc, monitor_task
    if not allowed_user(update):
        return
    if running_proc and running_proc.poll() is None:
        await reply(update, "Уже запущен.")
        return
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    try:
        cmd = build_chunk_cmd(mode=mode, start_page=start_page)
        running_proc = subprocess.Popen(cmd)
        await reply(update,
                    f"Запуск ({mode}) PID={running_proc.pid} pages={CHUNK_PAGES} pause={CHUNK_PAUSE_SEC}s runtime={CHUNK_MAX_RUNTIME_SEC}s total={CHUNK_TOTAL_PAGES} start_page={start_page or 'auto'}"
                    )
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
        monitor_task = context.application.create_task(
            monitor_progress(context))
    except Exception as exc:
        await reply(update, f"Ошибка запуска: {exc}")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    add_subscriber(chat_id)
    await reply(update, "Подписка на уведомления включена.")


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    remove_subscriber(chat_id)
    await reply(update, "Подписка на уведомления отключена.")


async def set_chunks_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not allowed_user(update):
        return 0
    context.user_data[AWAITING_CHUNKS] = True
    await reply(update,
                f"Введите chunk_pages и опционально pause_sec runtime_sec total_pages через пробел.\n"
                f"Пример: 12 45 3000 0\n"
                f"Сейчас: pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}, runtime={CHUNK_MAX_RUNTIME_SEC}, total={CHUNK_TOTAL_PAGES}"
                )
    return 0


async def set_chunks_apply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    global CHUNK_PAGES, CHUNK_PAUSE_SEC, CHUNK_MAX_RUNTIME_SEC, CHUNK_TOTAL_PAGES
    if not allowed_user(update):
        return 0
    # если ждём ввод диапазонов рекомендуемых — не трогаем
    if context.user_data.get(AWAITING_RECO):
        return await apply_reco_edit(update, context)
    if not context.user_data.get(AWAITING_CHUNKS):
        return 0
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
        await reply(
            update,
            f"Ок. pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}, runtime={CHUNK_MAX_RUNTIME_SEC}, total={CHUNK_TOTAL_PAGES}",
        )
    except Exception:
        await reply(update, "Не понял формат. Пример: 12 45 3000 0")
    context.user_data[AWAITING_CHUNKS] = False
    return 0


async def set_reco_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, field: str) -> None:
    context.user_data[AWAITING_RECO] = field
    if field == "age":
        await reply(update, "Введите максимальный возраст авто (лет), например: 5")
    elif field == "price":
        await reply(update, "Введите цену min и max через пробел, например: 1200000 4000000")
    elif field == "mileage":
        await reply(update, "Введите максимальный пробег (км), например: 80000")
    else:
        context.user_data[AWAITING_RECO] = None


async def apply_reco_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    field = context.user_data.get(AWAITING_RECO)
    if not field:
        return 0
    text = (update.message.text or "").strip() if update.message else ""
    if not text:
        await reply(update, "Пустое значение. Повторите ввод.")
        return 0
    cfg = load_config()
    try:
        if field == "age":
            cfg["max_age_years"] = max(1, int(text))
        elif field == "price":
            parts = [p for p in text.replace("—", " ").replace("-", " ").split() if p]
            if len(parts) < 2:
                await reply(update, "Нужны два числа: min и max.")
                return 0
            price_min = int(parts[0])
            price_max = int(parts[1])
            if price_min < 0 or price_max < 0:
                await reply(update, "Цена должна быть неотрицательной.")
                return 0
            if price_min > price_max:
                price_min, price_max = price_max, price_min
            cfg["price_min"] = price_min
            cfg["price_max"] = price_max
        elif field == "mileage":
            cfg["mileage_max"] = max(0, int(text))
        else:
            return 0
    except ValueError:
        await reply(update, "Не удалось распознать число. Пример: 1200000 4000000")
        return 0
    save_config(cfg)
    context.user_data[AWAITING_RECO] = None
    await reply(update, f"Готово.\n{format_reco_config(cfg)}")
    return 0


async def cb_calc_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not allowed_user(update):
        return
    db = SessionLocal()
    try:
        svc = CalculatorConfigService(db)
        cfg = svc.latest()
        if not cfg:
            await reply(update, "Конфиг калькулятора не найден.")
            return
        meta = cfg.payload.get("meta", {})
        msg = (
            f"Калькулятор v{cfg.version} (source={cfg.source or '—'})\n"
            f"EUR по умолчанию: {meta.get('eur_rate_default','?')}\n"
            f"Дата: {cfg.created_at}"
        )
        await reply(update, msg)
    finally:
        db.close()


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not allowed_user(update):
        return 0
    if not context.user_data.get(AWAITING_CALC):
        return 0
    doc = update.message.document if update.message else None
    if not doc:
        await reply(update, "Пришлите файл Excel.")
        return 0
    file = await doc.get_file()
    import tempfile
    import os
    path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            path = tmp.name
            await file.download_to_drive(path)
        extractor = CalculatorExtractor(Path(path))
        payload = extractor.extract()
        db = SessionLocal()
        try:
            svc = CalculatorConfigService(db)
            old = svc.latest()
            diff_text, total_changes = format_calc_diff_message(old.payload if old else None, payload)
            context.user_data[CALC_PENDING] = {
                "payload": payload,
                "filename": doc.file_name,
                "diff_total": total_changes,
            }
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Применить", callback_data="calc_apply")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="calc_cancel")],
                ]
            )
            await reply(update, diff_text)
            await update.message.reply_text("Сохранить новую версию?", reply_markup=kb)
        finally:
            db.close()
    except ValueError as exc:
        await reply(update, f"Ошибка в Excel: {exc}")
    except Exception as exc:
        await reply(update, f"Не удалось обработать файл: {exc}")
    finally:
        context.user_data[AWAITING_CALC] = False
        if path and os.path.exists(path):
            os.unlink(path)
    return 0


def mobile_latest_file() -> Optional[Path]:
    candidates = list(MOBILE_DOWNLOAD_DIR.glob("mobilede_active_offers_*.csv"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def mobile_status_summary() -> str:
    latest = mobile_latest_file()
    if not latest:
        return "Файлы mobile.de не найдены."
    stat = latest.stat()
    mtime = dt.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
    size_mb = stat.st_size / (1024 * 1024)
    try:
        import subprocess
        out = subprocess.check_output(["wc", "-l", str(latest)], text=True)
        lines = out.strip().split()[0]
    except Exception:
        lines = "?"
    return f"Последний: {latest.name} ({size_mb:.1f} MB, ~{lines} строк), сохранён {mtime}"


async def cmd_mobile_status(update: Update) -> None:
    await reply(update, summarize_job("mobilede"))


async def cmd_mobile_download(update: Update) -> None:
    await reply(update, "Скачиваем и импортируем mobile.de CSV...")
    await run_job(MOBILE_SCRIPT, "mobilede", update)


async def cmd_mobile_import(update: Update) -> None:
    await cmd_mobile_download(update)


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
    async def post_init(app: Application) -> None:
        await app.bot.set_my_commands(
            [
                BotCommand("start", "Запуск и меню"),
                BotCommand("menu", "Показать меню"),
            ]
        )
        await app.bot.set_chat_menu_button(MenuButtonCommands())

    application = Application.builder().token(token).post_init(post_init).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("start_parse", cmd_start_parse))
    application.add_handler(CommandHandler("start_full", cmd_start_full))
    application.add_handler(CommandHandler(
        "start_incremental", cmd_start_incremental))
    application.add_handler(CommandHandler("stop_parse", cmd_stop_parse))
    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    application.add_handler(CommandHandler("set_chunks", set_chunks_prompt))
    application.add_handler(CommandHandler("mobile_status", cmd_mobile_status))
    application.add_handler(CommandHandler("mobile_import", cmd_mobile_import))
    application.add_handler(CommandHandler("calc_config", lambda u, c: c.application.create_task(cb_calc_show(u, c))))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, set_chunks_apply))
    application.add_handler(MessageHandler(
        filters.Document.ALL, handle_document))

    async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not allowed_user(update):
            await update.callback_query.answer("Нет доступа", show_alert=True)
            return
        q = update.callback_query
        data = q.data
        await q.answer()
        if data == "status":
            await q.message.reply_text(format_status())
        elif data == "start_parse":
            await cmd_start_parse(update, context)
        elif data == "start_full":
            await start_with_mode(update, context, MODE_FULL, start_page="1")
        elif data == "start_incremental":
            await start_with_mode(update, context, MODE_INCREMENTAL, start_page=None)
        elif data == "set_chunks":
            context.user_data[AWAITING_CHUNKS] = True
            await q.message.reply_text(
                f"Введите chunk_pages и опционально pause_sec runtime_sec total_pages через пробел.\n"
                f"Сейчас: pages={CHUNK_PAGES}, pause={CHUNK_PAUSE_SEC}, runtime={CHUNK_MAX_RUNTIME_SEC}, total={CHUNK_TOTAL_PAGES}"
            )
        elif data == "stop_parse":
            await cmd_stop_parse(update, context)
        elif data == "menu":
            await show_menu(update)
        elif data == "status_updates":
            await cmd_updates_status(update)
        elif data == "reco_menu":
            await show_recommended_menu(update)
        elif data == "reco_age":
            await set_reco_prompt(update, context, "age")
        elif data == "reco_price":
            await set_reco_prompt(update, context, "price")
        elif data == "reco_mileage":
            await set_reco_prompt(update, context, "mileage")
        elif data == "run_mobilede":
            await run_job(MOBILE_SCRIPT, "mobilede", update)
        elif data == "run_emavto":
            await run_job(EMAVTO_SCRIPT, "emavto", update)
        elif data == "mobile_status":
            await q.message.reply_text(summarize_job("mobilede"))
        elif data == "emavto_status":
            await q.message.reply_text(summarize_job("emavto"))
        elif data == "last_errors":
            await q.message.reply_text(summarize_errors())
        elif data == "calc_menu":
            kb = [
                [InlineKeyboardButton("Показать текущие", callback_data="calc_show")],
                [InlineKeyboardButton("Загрузить Excel", callback_data="calc_upload")],
                [InlineKeyboardButton("Назад", callback_data="menu")],
            ]
            await q.message.reply_text("Калькулятор: выберите действие.", reply_markup=InlineKeyboardMarkup(kb))
        elif data == "calc_show":
            await cb_calc_show(update, context)
        elif data == "calc_upload":
            context.user_data[AWAITING_CALC] = True
            await q.message.reply_text("Пришлите Excel (3 листа: до 3-х, 3-5, Электро).")
        elif data == "calc_apply":
            pending = context.user_data.get(CALC_PENDING)
            if not pending:
                await q.message.reply_text("Нет загруженного файла для применения.")
                return
            db = SessionLocal()
            try:
                svc = CalculatorConfigService(db)
                cfg = svc.create(payload=pending["payload"], source="bot_upload", comment=pending.get("filename"))
                await q.message.reply_text(f"Новая версия сохранена: v{cfg.version} (изменений: {pending.get('diff_total')})")
            except Exception as exc:
                await q.message.reply_text(f"Не удалось сохранить: {exc}")
            finally:
                db.close()
            context.user_data[CALC_PENDING] = None
            context.user_data[AWAITING_CALC] = False
        elif data == "calc_cancel":
            context.user_data[CALC_PENDING] = None
            context.user_data[AWAITING_CALC] = False
            await q.message.reply_text("Загрузка отменена.")

    application.add_handler(CallbackQueryHandler(cb_handler))

    print("Bot started. Commands: /start /menu /status /start_parse /stop_parse")
    application.run_polling()


if __name__ == "__main__":
    main()
