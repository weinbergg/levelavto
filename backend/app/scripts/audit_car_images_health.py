from __future__ import annotations

import argparse
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import func, select

from backend.app.db import SessionLocal
from backend.app.models import Car, CarImage, Source
from backend.app.utils.thumbs import normalize_classistatic_url


@dataclass
class ProbeResult:
    ok: bool
    status: int
    reason: str
    checked_url: Optional[str]


def _normalize_image_url(url: str | None) -> Optional[str]:
    raw = (url or "").strip()
    if not raw:
        return None
    normalized = normalize_classistatic_url(raw)
    if normalized:
        return normalized
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("http://"):
        return f"https://{raw[7:]}"
    if raw.startswith("https://") or raw.startswith("/media/"):
        return raw
    return None


def _probe(url: str, timeout_sec: float) -> ProbeResult:
    # Local media path - no remote probe required.
    if url.startswith("/media/"):
        return ProbeResult(ok=True, status=200, reason="local_media", checked_url=url)
    headers = {"User-Agent": "levelavto-image-audit/1.0"}
    try:
        resp = requests.head(url, timeout=timeout_sec, allow_redirects=True, headers=headers)
        if resp.status_code < 400:
            return ProbeResult(ok=True, status=int(resp.status_code), reason="ok_head", checked_url=url)
    except requests.RequestException:
        pass
    try:
        resp = requests.get(
            url,
            timeout=timeout_sec,
            allow_redirects=True,
            headers={**headers, "Range": "bytes=0-1024"},
            stream=True,
        )
        status = int(resp.status_code)
        if status < 400:
            return ProbeResult(ok=True, status=status, reason="ok_get", checked_url=url)
        return ProbeResult(ok=False, status=status, reason=f"http_{status}", checked_url=url)
    except requests.RequestException as exc:
        return ProbeResult(ok=False, status=0, reason=str(exc)[:140], checked_url=url)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit/fix broken car_images URLs for detail gallery stability")
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default=None, help="ISO code, e.g. AT")
    ap.add_argument("--source-key", default="mobile")
    ap.add_argument("--limit-cars", type=int, default=5000)
    ap.add_argument("--max-images-per-car", type=int, default=30)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--fix-delete-broken", action="store_true")
    ap.add_argument("--fix-sync-thumbnail", action="store_true")
    ap.add_argument("--clear-thumbnail-when-no-valid", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report-json", default="/app/artifacts/audit_car_images_health.json")
    ap.add_argument("--report-csv", default="/app/artifacts/audit_car_images_health.csv")
    args = ap.parse_args()

    country_filter = (args.country or "").strip().upper() or None

    checked = 0
    broken = 0
    deleted = 0
    thumbnails_updated = 0
    thumbnails_cleared = 0
    rows_report: list[dict] = []

    with SessionLocal() as db:
        car_q = (
            db.query(Car.id)
            .join(Source, Source.id == Car.source_id)
            .filter(
                Car.is_available.is_(True),
                func.lower(Source.key).like(f"%{args.source_key.lower()}%"),
            )
        )
        if args.region.upper() == "EU":
            car_q = car_q.filter(Car.country != "RU", ~Car.country.like("KR%"))
        elif args.region.upper() == "KR":
            car_q = car_q.filter(Car.country.like("KR%"))
        if country_filter:
            car_q = car_q.filter(func.upper(Car.country) == country_filter)
        car_ids = [int(v[0]) for v in car_q.order_by(Car.id.asc()).limit(max(1, args.limit_cars)).all()]
        if not car_ids:
            print("[audit_car_images_health] no cars matched")
            return

        img_rows = db.execute(
            select(CarImage.id, CarImage.car_id, CarImage.url)
            .where(CarImage.car_id.in_(car_ids))
            .order_by(CarImage.car_id.asc(), CarImage.position.asc(), CarImage.id.asc())
        ).all()

        per_car_count: dict[int, int] = {}
        payload: list[tuple[int, int, str]] = []
        for image_id, car_id, raw_url in img_rows:
            c = per_car_count.get(int(car_id), 0)
            if c >= args.max_images_per_car:
                continue
            per_car_count[int(car_id)] = c + 1
            payload.append((int(image_id), int(car_id), str(raw_url or "")))

        broken_image_ids: list[int] = []
        first_valid_by_car: dict[int, str] = {}
        cars_with_any_valid: set[int] = set()

        def run_probe(item: tuple[int, int, str]) -> dict:
            image_id, car_id, raw = item
            normalized = _normalize_image_url(raw)
            if not normalized:
                return {
                    "image_id": image_id,
                    "car_id": car_id,
                    "raw_url": raw,
                    "checked_url": None,
                    "status": 0,
                    "ok": False,
                    "reason": "invalid_format",
                }
            pr = _probe(normalized, timeout_sec=args.timeout)
            return {
                "image_id": image_id,
                "car_id": car_id,
                "raw_url": raw,
                "checked_url": pr.checked_url,
                "status": pr.status,
                "ok": pr.ok,
                "reason": pr.reason,
            }

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(run_probe, item) for item in payload]
            for fut in as_completed(futures):
                row = fut.result()
                checked += 1
                if not row["ok"]:
                    broken += 1
                    broken_image_ids.append(int(row["image_id"]))
                else:
                    cid = int(row["car_id"])
                    cars_with_any_valid.add(cid)
                    if cid not in first_valid_by_car and row.get("checked_url"):
                        first_valid_by_car[cid] = str(row["checked_url"])
                rows_report.append(row)

        if not args.dry_run and (args.fix_delete_broken or args.fix_sync_thumbnail):
            if args.fix_delete_broken and broken_image_ids:
                db.query(CarImage).filter(CarImage.id.in_(broken_image_ids)).delete(synchronize_session=False)
                deleted = len(broken_image_ids)

            if args.fix_sync_thumbnail:
                cars = db.query(Car).filter(Car.id.in_(car_ids)).all()
                for car in cars:
                    target = first_valid_by_car.get(int(car.id))
                    current = (car.thumbnail_url or "").strip()
                    if target:
                        if current != target:
                            car.thumbnail_url = target
                            thumbnails_updated += 1
                    elif args.clear_thumbnail_when_no_valid and current:
                        car.thumbnail_url = None
                        thumbnails_cleared += 1
            db.commit()

    rows_report.sort(key=lambda r: (0 if not r["ok"] else 1, int(r["car_id"]), int(r["image_id"])))
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "region": args.region,
        "country": country_filter,
        "source_key": args.source_key,
        "cars_limit": args.limit_cars,
        "images_checked": checked,
        "images_broken": broken,
        "broken_pct": round((broken / checked * 100.0), 2) if checked else 0.0,
        "images_deleted": deleted,
        "thumbnails_updated": thumbnails_updated,
        "thumbnails_cleared": thumbnails_cleared,
        "dry_run": bool(args.dry_run),
    }

    json_path = Path(args.report_json)
    csv_path = Path(args.report_csv)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps({"summary": summary, "rows": rows_report}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "image_id",
                "car_id",
                "status",
                "ok",
                "reason",
                "raw_url",
                "checked_url",
            ],
        )
        writer.writeheader()
        for row in rows_report:
            writer.writerow(row)

    print(
        "[audit_car_images_health] "
        f"checked={checked} broken={broken} broken_pct={summary['broken_pct']} "
        f"deleted={deleted} thumb_updated={thumbnails_updated} thumb_cleared={thumbnails_cleared} "
        f"json={json_path} csv={csv_path}"
    )


if __name__ == "__main__":
    main()
