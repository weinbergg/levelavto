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
from sqlalchemy import select

from backend.app.importing.mobilede_csv import iter_mobilede_csv_rows
from backend.app.models import ParserRun, ParserRunSource, Source
from backend.app.parsing.config import load_sites_config
from backend.app.utils.feed_deactivation import should_deactivate_feed
from backend.app.utils.redis_cache import redis_delete_by_pattern, bump_dataset_version
from backend.app.utils.telegram import (
    format_daily_report,
    resolve_telegram_chat_id,
    send_telegram_message,
    telegram_enabled,
)


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
    # Ensure target directory exists before disk usage check.
    dest.parent.mkdir(parents=True, exist_ok=True)
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
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def estimate_feed_seen(file_path: Path) -> int:
    return sum(1 for _ in iter_mobilede_csv_rows(str(file_path)))


def get_previous_success_seen(source_key: str) -> int | None:
    from backend.app.db import SessionLocal

    with SessionLocal() as db:
        source_id = db.execute(
            select(Source.id).where(Source.key == source_key)
        ).scalar_one_or_none()
        if source_id is None:
            return None
        row = db.execute(
            select(ParserRunSource.total_seen)
            .join(ParserRun, ParserRun.id == ParserRunSource.parser_run_id)
            .where(
                ParserRunSource.source_id == source_id,
                ParserRun.status == "success",
                ParserRunSource.total_seen > 0,
            )
            .order_by(ParserRun.started_at.desc())
            .limit(1)
        ).first()
        if not row:
            return None
        return int(row[0] or 0) or None


def preflight_deactivation_guard(
    *,
    file_path: Path,
    source_key: str,
    deactivate_mode: str,
    min_ratio: float,
    min_seen: int,
) -> tuple[int, int | None, bool, str]:
    current_seen = estimate_feed_seen(file_path)
    previous_seen = get_previous_success_seen(source_key)
    allow_deactivate, deactivate_reason = should_deactivate_feed(
        mode=deactivate_mode,
        current_seen=current_seen,
        previous_seen=previous_seen,
        min_ratio=min_ratio,
        min_seen=min_seen,
    )
    return current_seen, previous_seen, allow_deactivate, deactivate_reason


def run_import(
    file_path: Path,
    trigger: str = "auto-daily",
    limit: int | None = None,
    deactivate_mode: str = "auto",
    deactivate_min_ratio: float = 0.93,
    deactivate_min_seen: int = 100_000,
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
    cmd += ["--deactivate-mode", deactivate_mode]
    cmd += ["--deactivate-min-ratio", str(deactivate_min_ratio)]
    cmd += ["--deactivate-min-seen", str(deactivate_min_seen)]
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


def recalc_eu_calc_cache(since_minutes: int | None = None, only_missing: bool = True) -> None:
    from backend.app.db import SessionLocal
    from backend.app.models import Car
    from backend.app.services.cars_service import CarsService
    from datetime import datetime, timedelta

    updated = skipped = errors = 0
    with SessionLocal() as db:
        svc = CarsService(db)
        q = db.query(Car.id).filter(Car.is_available.is_(True), ~Car.country.like("KR%"))
        if only_missing:
            q = q.filter(Car.total_price_rub_cached.is_(None))
        if since_minutes:
            since_ts = datetime.utcnow() - timedelta(minutes=since_minutes)
            q = q.filter(Car.updated_at >= since_ts)
        total = q.count()
        batch = 2000
        offset = 0
        while True:
            ids = [r[0] for r in q.order_by(Car.id.asc()).offset(offset).limit(batch).all()]
            if not ids:
                break
            cars = db.query(Car).filter(Car.id.in_(ids)).all()
            for car in cars:
                try:
                    res = svc.ensure_calc_cache(car)
                    if res is None:
                        skipped += 1
                        continue
                    updated += 1
                except Exception:
                    errors += 1
            db.commit()
            offset += batch
    print(
        f"[mobilede_daily] recalc_eu_calc_cache total={total} updated={updated} skipped={skipped} errors={errors}",
        flush=True,
    )


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
        help="Legacy override: force deactivation for this run.",
    )
    ap.add_argument(
        "--deactivate-mode",
        choices=("auto", "force", "skip"),
        default=os.getenv("MOBILEDE_DEACTIVATE_MODE", "auto"),
        help="auto compares current feed with previous successful import; force always deactivates; skip disables deactivation.",
    )
    ap.add_argument(
        "--deactivate-min-ratio",
        type=float,
        default=float(os.getenv("MOBILEDE_DEACTIVATE_MIN_RATIO", "0.93")),
        help="Minimum current/previous feed ratio required before auto deactivation is allowed.",
    )
    ap.add_argument(
        "--deactivate-min-seen",
        type=int,
        default=int(os.getenv("MOBILEDE_DEACTIVATE_MIN_SEEN", "100000")),
        help="Minimum feed size required before auto deactivation is allowed.",
    )
    ap.add_argument(
        "--strict-deactivation-guard",
        action="store_true",
        default=os.getenv("MOBILEDE_STRICT_DEACTIVATION_GUARD", "0") == "1",
        help="Abort before import when auto deactivation would be skipped; prevents adding fresh cars without deactivating disappeared ones.",
    )
    args = ap.parse_args()
    cfg = load_sites_config().get("mobile_de")

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
        deactivate_mode = "force" if args.allow_deactivate else args.deactivate_mode
        if args.strict_deactivation_guard and deactivate_mode == "auto":
            current_seen, previous_seen, allow_deactivate, deactivate_reason = preflight_deactivation_guard(
                file_path=target,
                source_key=cfg.key,
                deactivate_mode=deactivate_mode,
                min_ratio=max(0.0, min(float(args.deactivate_min_ratio), 1.0)),
                min_seen=max(0, int(args.deactivate_min_seen)),
            )
            print(
                "[mobilede_daily] strict deactivation preflight "
                f"current_seen={current_seen} previous_seen={previous_seen or 'n/a'} "
                f"decision={'allow' if allow_deactivate else 'block'} reason={deactivate_reason}",
                flush=True,
            )
            if not allow_deactivate:
                token = os.getenv("TELEGRAM_BOT_TOKEN")
                chat_id = resolve_telegram_chat_id()
                if telegram_enabled() and token and chat_id:
                    send_telegram_message(
                        token,
                        chat_id,
                        (
                            "LevelAvto nightly blocked before import\n"
                            f"deactivation_guard: mode={deactivate_mode} current_seen={current_seen} "
                            f"prev_seen={previous_seen or '-'} reason={deactivate_reason}"
                        ),
                    )
                raise RuntimeError(
                    "Daily import blocked by strict deactivation guard: "
                    f"{deactivate_reason} (current_seen={current_seen}, previous_seen={previous_seen or 'n/a'})"
                )
        run_import(
            target,
            trigger="auto-daily",
            limit=args.limit,
            deactivate_mode=deactivate_mode,
            deactivate_min_ratio=max(0.0, min(float(args.deactivate_min_ratio), 1.0)),
            deactivate_min_seen=max(0, int(args.deactivate_min_seen)),
            stats_file=stats_file,
        )
        if not args.skip_cache:
            update_price_cache()
        if os.getenv("RUN_EU_CALC_AFTER_DAILY", "1") == "1":
            since_min = int(os.getenv("EU_CALC_SINCE_MIN", "180")) if os.getenv("EU_CALC_SINCE_MIN") else 180
            recalc_eu_calc_cache(since_minutes=since_min, only_missing=True)
        deleted = 0
        deleted += redis_delete_by_pattern("cars_count:*")
        deleted += redis_delete_by_pattern("cars_list:*")
        deleted += redis_delete_by_pattern("filter_ctx_*")
        new_ver = bump_dataset_version()
        print(f"[mobilede_daily] redis invalidated keys={deleted} dataset_version={new_ver}")
        if os.getenv("RUN_DQC_AFTER_DAILY", "1") != "0":
            try:
                # Import lazily so a syntax error in the QA script can never
                # block the actual daily import. The script returns 0/1/2;
                # we surface that in the daily print but never raise.
                from backend.app.scripts.data_quality_check import main as dqc_main
                dqc_rc = dqc_main()
                print(f"[mobilede_daily] data_quality_check rc={dqc_rc}", flush=True)
            except Exception as exc:
                print(f"[mobilede_daily] data_quality_check error: {exc}", flush=True)
        if not KEEP_CSV:
            try:
                target.unlink()
            except OSError:
                pass
        if os.getenv("RUN_CLEANUP_AFTER_DAILY", "1") == "1":
            cleanup_script = Path(__file__).resolve().parents[3] / "scripts" / "cleanup_tmp_files.sh"
            if cleanup_script.exists():
                try:
                    subprocess.run(["/bin/bash", str(cleanup_script)], check=False)
                    print("[mobilede_daily] cleanup_tmp_files.sh completed", flush=True)
                except Exception as exc:
                    print(f"[mobilede_daily] cleanup_tmp_files.sh failed: {exc}", flush=True)
        # Telegram report (optional)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = resolve_telegram_chat_id()
        if telegram_enabled() and token and chat_id:
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
