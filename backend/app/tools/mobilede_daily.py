from __future__ import annotations

import argparse
import base64
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import requests


HOST = os.getenv("MOBILEDE_HOST", "https://parsers1-valdez.auto-parser.ru")
LOGIN = os.getenv("MOBILEDE_LOGIN") or os.getenv("MOBILEDE_USER")
PASSWORD = os.getenv("MOBILEDE_PASSWORD") or os.getenv("MOBILEDE_PASS")
FILENAME = "mobilede_active_offers.csv"
DOWNLOAD_DIR = Path("/app/tmp")


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

    target = DOWNLOAD_DIR / f"mobilede_active_offers_{run_date:%Y-%m-%d}.csv"
    download_file(run_date, target)
    rotate_backups(DOWNLOAD_DIR, keep=args.keep)

    if not args.skip_import:
        run_import(target, trigger="auto-daily", limit=args.limit, allow_deactivate=args.allow_deactivate)
        if not args.skip_cache:
            update_price_cache()


if __name__ == "__main__":
    main()
