from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, Optional


def format_daily_report(payload: Dict[str, Any]) -> str:
    lines = [
        "LevelAvto nightly update",
        f"dataset_version: {payload.get('dataset_version', '-')}",
        f"rates: EUR={payload.get('eur_rate', '-')} USD={payload.get('usd_rate', '-')}",
    ]
    stats = payload.get("import_stats") or {}
    if stats:
        lines.append(
            "import: seen={seen} inserted={inserted} updated={updated} deactivated={deactivated}".format(
                seen=stats.get("seen", "-"),
                inserted=stats.get("inserted", "-"),
                updated=stats.get("updated", "-"),
                deactivated=stats.get("deactivated", "-"),
            )
        )
    totals = payload.get("totals") or {}
    if totals:
        lines.append(f"active_total: {totals.get('active_total', '-')}")
    by_source = payload.get("by_source") or {}
    if by_source:
        parts = [f"{k}={v}" for k, v in by_source.items()]
        lines.append("by_source: " + ", ".join(parts))
    elapsed = payload.get("elapsed_sec")
    if elapsed is not None:
        lines.append(f"elapsed_sec: {elapsed}")
    return "\n".join(lines)


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False
