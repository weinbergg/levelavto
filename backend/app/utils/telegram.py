from __future__ import annotations

import json
import os
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
        deactivation_allowed = stats.get("deactivation_allowed")
        if (
            stats.get("deactivate_mode")
            or stats.get("deactivate_reason")
            or deactivation_allowed is not None
        ):
            allowed_label = "-"
            if deactivation_allowed is True:
                allowed_label = "yes"
            elif deactivation_allowed is False:
                allowed_label = "no"
            lines.append(
                "deactivation: mode={mode} allowed={allowed} prev_seen={prev} reason={reason}".format(
                    mode=stats.get("deactivate_mode", "-"),
                    allowed=allowed_label,
                    prev=stats.get("deactivate_previous_seen", "-"),
                    reason=stats.get("deactivate_reason", "-"),
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


def telegram_enabled() -> bool:
    raw = str(os.environ.get("TELEGRAM_ENABLED", "1") or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def resolve_telegram_chat_id() -> str:
    return (
        os.environ.get("TELEGRAM_ADMIN_CHAT_ID")
        or os.environ.get("TELEGRAM_CHAT_ID")
        or (
            os.environ.get("TELEGRAM_ALLOWED_IDS", "").split(",")[0].strip()
            if os.environ.get("TELEGRAM_ALLOWED_IDS")
            else ""
        )
    )
