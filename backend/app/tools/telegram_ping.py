from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from backend.app.utils.telegram import resolve_telegram_chat_id, send_telegram_message


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a diagnostic Telegram message using current env")
    parser.add_argument("--message", default="", help="custom message body")
    parser.add_argument("--dry-run", action="store_true", help="print resolved env and message without sending")
    args = parser.parse_args()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = resolve_telegram_chat_id().strip()
    stamp = datetime.now(timezone.utc).isoformat()
    text = args.message.strip() or f"LevelAvto Telegram ping OK\nutc: {stamp}"

    print(
        "[telegram_ping] token_present=%d chat_id_present=%d chat_id=%s"
        % (1 if token else 0, 1 if chat_id else 0, chat_id or "-"),
        flush=True,
    )
    print(f"[telegram_ping] message={text}", flush=True)

    if args.dry_run:
        return 0

    if not token or not chat_id:
        print("[telegram_ping] missing TELEGRAM_BOT_TOKEN or chat id", file=sys.stderr, flush=True)
        return 1

    ok = send_telegram_message(token, chat_id, text)
    print(f"[telegram_ping] sent={1 if ok else 0}", flush=True)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
