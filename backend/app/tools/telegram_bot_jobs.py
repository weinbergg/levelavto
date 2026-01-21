from __future__ import annotations

import json
import os
from typing import List, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext


JOB_LABELS = {
    "mobilede": "Mobile.de",
    "emavto": "M-Auto / Emavto",
}


def load_last(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def format_status(job: str, data: Optional[dict]) -> str:
    if not data:
        return f"{JOB_LABELS.get(job, job)}: нет данных"
    status = data.get("status")
    dur = data.get("duration_sec", 0)
    stats = data.get("stats", {})
    if status is None:
        # legacy mobilede_last.json shape
        if any(k in data for k in ("seen", "inserted", "updated", "deactivated")):
            status = "ok" if data.get("seen") else "unknown"
            stats = {
                "cars_total_processed": data.get("seen"),
                "cars_inserted": data.get("inserted"),
                "cars_updated": data.get("updated"),
                "cars_deactivated": data.get("deactivated"),
                "cars_without_photos": data.get("no_photo"),
            }
        else:
            status = "unknown"
    if not stats and isinstance(data.get("stats"), dict):
        stats = data.get("stats", {})
    files = data.get("files", {})
    parts = [
        f"{JOB_LABELS.get(job, job)}",
        f"Статус: {'✅' if status == 'ok' else '❌'} ({status})",
        f"Длительность: {dur}s",
    ]
    if files:
        parts.append(f"CSV дата: {files.get('csv_date','')}")
    if stats:
        processed = stats.get("cars_total_processed")
        inserted = stats.get("cars_inserted")
        updated = stats.get("cars_updated")
        skipped = None
        if processed is not None and inserted is not None and updated is not None:
            skipped = max(0, int(processed) - int(inserted) - int(updated))
        parts.append(
            "processed={processed}, inserted={inserted}, updated={updated}, deactivated={deactivated}, no_photo={no_photo}".format(
                processed=stats.get("cars_total_processed"),
                inserted=stats.get("cars_inserted"),
                updated=stats.get("cars_updated"),
                deactivated=stats.get("cars_deactivated"),
                no_photo=stats.get("cars_without_photos"),
            )
        )
        parts.append(
            "Добавлено={ins}, обновлено={upd}, пропущено={skip}".format(
                ins=stats.get("cars_inserted", "—"),
                upd=stats.get("cars_updated", "—"),
                skip=skipped if skipped is not None else "—",
            )
        )
    return "\n".join([p for p in parts if p])


def build_menu() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("Обновить Mobile.de", callback_data="run_mobilede")],
        [InlineKeyboardButton("Обновить M-Auto", callback_data="run_emavto")],
        [
            InlineKeyboardButton("Статус Mobile.de", callback_data="status_mobilede"),
            InlineKeyboardButton("Статус M-Auto", callback_data="status_emavto"),
        ],
        [InlineKeyboardButton("Последние ошибки", callback_data="last_errors")],
    ]
    return InlineKeyboardMarkup(buttons)


def handle_status(job: str, update: Update, context: CallbackContext) -> None:
    base = os.path.join("backend", "app", "runtime", "jobs")
    path = os.path.join(base, f"{job}_last.json")
    data = load_last(path)
    text = format_status(job, data)
    update.effective_chat.send_message(text=text)
