from __future__ import annotations

import json
import os
import sys
import time
import requests


def load_result(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def format_message(job: str, data: dict) -> str:
    status = data.get("status", "unknown")
    dur = data.get("duration_sec", 0)
    stats = data.get("stats", {})
    files = data.get("files", {})
    icon = "⚪"
    if status == "ok":
        icon = "✅"
    elif status == "running":
        icon = "⏳"
    elif status in ("fail", "error"):
        icon = "❌"
    parts = [
        f"{job}: {icon} {status}",
        f"Длительность: {dur}s",
    ]
    if files:
        parts.append(f"Файл: {files.get('csv_date','')}")
    if stats:
        parts.append(
            "Всего: {processed}, добавлено: {ins}, обновлено: {upd}, снято: {deact}, без фото: {no_photo}".format(
                processed=stats.get("cars_total_processed", "—"),
                ins=stats.get("cars_inserted", "—"),
                upd=stats.get("cars_updated", "—"),
                deact=stats.get("cars_deactivated", "—"),
                no_photo=stats.get("cars_without_photos", "—"),
            )
        )
    if data.get("errors"):
        parts.append("Ошибки:\n" + "\n".join(data["errors"]))
    return "\n".join([p for p in parts if p])


def send_tg(msg: str) -> None:
    dry = os.getenv("TELEGRAM_DRY_RUN", "0") == "1"
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_ALLOWED_IDS", "").split(",")[0]
    if dry:
        print("[dry-run tg]\n", msg)
        return
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN or CHAT_ID not set", file=sys.stderr)
        sys.exit(1)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg})
    if resp.status_code != 200:
        print("Failed to send TG message", resp.text, file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 3:
        print("Usage: python -m backend.app.tools.notify_tg --job <name> --result <path>")
        sys.exit(1)
    job = None
    path = None
    args = sys.argv[1:]
    for i in range(0, len(args), 2):
        if args[i] == "--job":
            job = args[i + 1]
        elif args[i] == "--result":
            path = args[i + 1]
    if not job or not path:
        print("Usage: --job <name> --result <path>")
        sys.exit(1)
    data = load_result(path)
    msg = format_message(job, data)
    send_tg(msg)


if __name__ == "__main__":
    main()
