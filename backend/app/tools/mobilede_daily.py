from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import subprocess
import shutil
import sys
import time
from pathlib import Path

import requests
from backend.app.utils.redis_cache import redis_delete_by_pattern, bump_dataset_version
from backend.app.utils.telegram import format_daily_report, send_telegram_message


HOST = os.getenv("MOBILEDE_HOST", "https://parsers1-valdez.auto-parser.ru")
LOGIN = os.getenv("MOBILEDE_LOGIN") or os.getenv("MOBILEDE_USER")
PASSWORD = os.getenv("MOBILEDE_PASSWORD") or os.getenv("MOBILEDE_PASS")
FILENAME = "mobilede_active_offers.csv"
DOWNLOAD_DIR = Path(os.getenv("MOBILEDE_TMP_DIR", "/app/tmp"))
KEEP_CSV = os.getenv("KEEP_CSV", "0") == "1"
MIN_FREE_GB = int(os.getenv("MOBILEDE_MIN_FREE_GB", "20"))


def rotate_backups(directory: Path, keep: int = 5) -> None:
    files = sorted(
        directory.glob("mobilede_active_offers_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in files[keep:]:
        try:
            old.unlink()
        except OSError:
            pass


def download_file(for_date: dt.date, dest: Path) -> None:
    if not LOGIN or not PASSWORD:
        raise RuntimeError("MOBILEDE_LOGIN/MOBILEDE_PASSWORD must be set in environment")
    try:
        usage = shutil.disk_usage(dest.parent)
        if usage.free < MIN_FREE_GB * 1024 * 1024 * 1024:
            raise RuntimeError(
                f"Not enough disk space in {dest.parent}: need {MIN_FREE_GB}GB free"
            )
    except Exception as exc:
        raise RuntimeError(f"Disk space check failed: {exc}") from exc
    url = f"{HOST}/mobilede/{for_date:%Y-%m-%d}/{FILENAME}"
    auth_header = base64.b64encode(f"{LOGIN}:{PASSWORD}".encode()).decode()
    headers = {"authorization": f"Basic {auth_header}"}
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def run_import(
    file_path: Path,
    trigger: str = "auto-daily",
    limit: int | None = None,
    allow_deactivate: bool = False,
    stats_file: Path | None = None,
) -> None:
    cmd = [
        sys.executable,
        "-m",
        "backend.app.tools.mobilede_csv_import",
        "--file",
        str(file_path),
        "--trigger",
        trigger,
    ]
    if stats_file:
        cmd += ["--stats-file", str(stats_file)]
    if not allow_deactivate:
        cmd.append("--skip-deactivate")
    if limit:
        cmd += ["--limit", str(limit)]
    subprocess.run(cmd, check=True)


def update_price_cache() -> None:
    """
    После загрузки файла обновляем price_rub_cached по курсу ЦБ/ENV.
    """
    from backend.app.db import SessionLocal
    from backend.app.services.cars_service import CarsService

    db = SessionLocal()
    try:
        svc = CarsService(db)
        rates = svc.get_fx_rates() or {"EUR": 95.0, "USD": 85.0, "RUB": 1.0}
        eur = rates.get("EUR", 95.0)
        usd = rates.get("USD", 85.0)
        # обновляем батчами
        from backend.app.models import Car
        batch = 5000
        offset = 0
        while True:
            cars = db.query(Car).with_entities(Car.id, Car.price, Car.currency).offset(offset).limit(batch).all()
            if not cars:
                break
            for cid, price, curr in cars:
                if price is None or curr is None:
                    continue
                curr_l = curr.lower()
                if curr_l == "rub":
                    rub = float(price)
                elif curr_l == "usd":
                    rub = float(price) * usd
                else:
                    rub = float(price) * eur
                db.query(Car).filter(Car.id == cid).update({"price_rub_cached": rub}, synchronize_session=False)
            db.commit()
            offset += batch
    finally:
        db.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch daily mobile.de CSV and import")
    ap.add_argument(
        "--date", help="YYYY-MM-DD; default today UTC", default=None)
    ap.add_argument("--keep", type=int, default=5,
                    help="Number of backups to keep")
    ap.add_argument("--skip-import", action="store_true",
                    help="Download only, skip import")
    ap.add_argument("--limit", type=int, default=None,
                    help="Optional limit for import (debug)")
    ap.add_argument("--skip-cache", action="store_true",
                    help="Skip price_rub_cached update")
    ap.add_argument(
        "--allow-deactivate",
        action="store_true",
        help="Allow deactivating missing cars (only for full/verified feeds)",
    )
    args = ap.parse_args()

    if args.date:
        run_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        run_date = dt.datetime.utcnow().date()
    started_ts = time.time()

    print(f"[mobilede_daily] daily start date={run_date:%Y-%m-%d}", flush=True)
    target = DOWNLOAD_DIR / f"mobilede_active_offers_{run_date:%Y-%m-%d}.csv"
    download_file(run_date, target)
    if KEEP_CSV:
        rotate_backups(DOWNLOAD_DIR, keep=args.keep)

    if not args.skip_import:
        stats_file = DOWNLOAD_DIR / "mobilede_import_stats.json"
        run_import(
            target,
            trigger="auto-daily",
            limit=args.limit,
            allow_deactivate=args.allow_deactivate,
            stats_file=stats_file,
        )
        if not args.skip_cache:
            update_price_cache()
        deleted = 0
        deleted += redis_delete_by_pattern("cars_count:*")
        deleted += redis_delete_by_pattern("cars_list:*")
        deleted += redis_delete_by_pattern("filter_ctx_*")
        new_ver = bump_dataset_version()
        print(f"[mobilede_daily] redis invalidated keys={deleted} dataset_version={new_ver}")
        if not KEEP_CSV:
            try:
                target.unlink()
            except OSError:
                pass
        # Telegram report (optional)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            stats = {}
            try:
                if stats_file.exists():
                    stats = json.loads(stats_file.read_text(encoding="utf-8"))
            except Exception:
                stats = {}
            print("[mobilede_daily] daily stats collected", flush=True)
            try:
                from backend.app.db import SessionLocal
                from backend.app.models import Car, Source
                from sqlalchemy import func

                with SessionLocal() as db:
                    total_active = db.query(func.count(Car.id)).filter(Car.is_available.is_(True)).scalar() or 0
                    rows = (
                        db.query(Source.key, func.count(Car.id))
                        .join(Car, Car.source_id == Source.id)
                        .filter(Car.is_available.is_(True))
                        .group_by(Source.key)
                        .all()
                    )
                    by_source = {k or "unknown": int(v) for k, v in rows}
            except Exception:
                total_active = 0
                by_source = {}
            try:
                from backend.app.services.cars_service import CarsService

                with SessionLocal() as db:
                    rates = CarsService(db).get_fx_rates() or {}
            except Exception:
                rates = {}
            report_payload = {
                "dataset_version": new_ver,
                "eur_rate": rates.get("EUR"),
                "usd_rate": rates.get("USD"),
                "import_stats": stats if isinstance(stats, dict) else {},
                "totals": {"active_total": total_active},
                "by_source": by_source,
                "elapsed_sec": int(time.time() - started_ts),
            }
            msg = format_daily_report(report_payload)
            print("[mobilede_daily] telegram send attempt", flush=True)
            ok = send_telegram_message(token, chat_id, msg)
            if ok:
                print("[mobilede_daily] telegram send ok", flush=True)
            else:
                print("[mobilede_daily] telegram send error", flush=True)
        else:
            print("[mobilede_daily] telegram disabled: missing TELEGRAM_BOT_TOKEN/CHAT_ID", flush=True)


if __name__ == "__main__":
    main()
