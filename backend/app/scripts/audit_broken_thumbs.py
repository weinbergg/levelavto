from __future__ import annotations

import argparse
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests
from sqlalchemy import select

from backend.app.db import SessionLocal
from backend.app.models.car import Car
from backend.app.models.car_image import CarImage
from backend.app.utils.thumbs import normalize_classistatic_url, pick_classistatic_thumb


@dataclass
class UrlProbeResult:
    ok: bool
    status: int
    checked_url: str


def _normalize_thumb_url(url: str | None) -> str | None:
    if not url:
        return None
    normalized = normalize_classistatic_url(url)
    return pick_classistatic_thumb(normalized)


def _probe_url(url: str, timeout_sec: float = 6.0) -> UrlProbeResult:
    headers = {"User-Agent": "levelavto-thumb-audit/1.0"}
    try:
        resp = requests.head(url, timeout=timeout_sec, allow_redirects=True, headers=headers)
        if resp.status_code < 400:
            return UrlProbeResult(ok=True, status=int(resp.status_code), checked_url=url)
    except requests.RequestException:
        pass

    try:
        resp = requests.get(
            url,
            timeout=timeout_sec,
            allow_redirects=True,
            headers={**headers, "Range": "bytes=0-2048"},
            stream=True,
        )
        return UrlProbeResult(ok=resp.status_code < 400, status=int(resp.status_code), checked_url=url)
    except requests.RequestException:
        return UrlProbeResult(ok=False, status=0, checked_url=url)


def _iter_candidate_urls(db, car_id: int) -> Iterable[str]:
    rows = db.execute(
        select(CarImage.url)
        .where(CarImage.car_id == car_id, CarImage.url.is_not(None))
        .order_by(CarImage.id.asc())
        .limit(30)
    ).all()
    for (url,) in rows:
        normalized = _normalize_thumb_url(url)
        if normalized:
            yield normalized


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit broken thumbnail URLs and optionally fix them")
    ap.add_argument("--region", type=str, default=None, help="EU|KR|RU")
    ap.add_argument("--country", type=str, default=None, help="country code, e.g. DE")
    ap.add_argument("--limit", type=int, default=3000)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--timeout", type=float, default=6.0)
    ap.add_argument("--fix", action="store_true", help="replace broken thumbnail_url from car_images")
    ap.add_argument(
        "--clear-unrecoverable",
        action="store_true",
        help="if --fix and replacement not found: set thumbnail_url=NULL",
    )
    ap.add_argument("--include-missing", action="store_true", help="also process rows with empty thumbnail_url")
    ap.add_argument("--report-json", default="/app/artifacts/broken_thumbs_report.json")
    ap.add_argument("--report-csv", default="/app/artifacts/broken_thumbs_report.csv")
    args = ap.parse_args()

    report_rows: list[dict] = []
    checked = 0
    broken = 0
    replaced = 0
    cleared = 0

    with SessionLocal() as db:
        q = db.query(Car).filter(Car.is_available.is_(True))
        if not args.include_missing:
            q = q.filter(Car.thumbnail_url.is_not(None), Car.thumbnail_url != "")
        if args.country:
            q = q.filter(Car.country == args.country.strip().upper())
        elif args.region:
            region = args.region.strip().upper()
            if region == "KR":
                q = q.filter(Car.country.like("KR%"))
            elif region == "EU":
                q = q.filter(Car.country.not_like("KR%"), Car.country != "RU")
            elif region == "RU":
                q = q.filter(Car.country == "RU")

        rows = q.order_by(Car.updated_at.desc()).limit(max(1, args.limit)).all()
        payload: list[tuple[int, str | None, str | None, str | None]] = []
        for car in rows:
            payload.append((car.id, car.thumbnail_url, car.source_url, car.country))

        def run_probe(row: tuple[int, str | None, str | None, str | None]):
            car_id, raw_thumb, src, country = row
            normalized = _normalize_thumb_url(raw_thumb)
            if not normalized:
                return {
                    "car_id": car_id,
                    "country": country,
                    "source_url": src,
                    "old_thumbnail_url": raw_thumb,
                    "checked_url": None,
                    "status": 0,
                    "broken": True,
                    "reason": "missing_thumbnail",
                }
            res = _probe_url(normalized, timeout_sec=args.timeout)
            return {
                "car_id": car_id,
                "country": country,
                "source_url": src,
                "old_thumbnail_url": raw_thumb,
                "checked_url": normalized,
                "status": res.status,
                "broken": not res.ok,
                "reason": "http_error" if not res.ok else None,
            }

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(run_probe, row) for row in payload]
            for fut in as_completed(futures):
                row = fut.result()
                checked += 1
                if row["broken"]:
                    broken += 1
                report_rows.append(row)

        if args.fix and report_rows:
            by_id = {int(r["car_id"]): r for r in report_rows if r.get("broken")}
            for car_id, row in by_id.items():
                car = db.query(Car).filter(Car.id == car_id).first()
                if not car:
                    continue
                replacement: Optional[str] = None
                for cand in _iter_candidate_urls(db, car_id):
                    probe = _probe_url(cand, timeout_sec=args.timeout)
                    if probe.ok:
                        replacement = cand
                        break
                if replacement and replacement != (car.thumbnail_url or ""):
                    car.thumbnail_url = replacement
                    replaced += 1
                    row["new_thumbnail_url"] = replacement
                    row["fix"] = "replaced"
                elif replacement:
                    row["new_thumbnail_url"] = replacement
                    row["fix"] = "already_ok_after_normalize"
                elif args.clear_unrecoverable:
                    car.thumbnail_url = None
                    cleared += 1
                    row["new_thumbnail_url"] = None
                    row["fix"] = "cleared"
                else:
                    row["fix"] = "not_fixed"
            db.commit()

    report_rows.sort(key=lambda x: (0 if x.get("broken") else 1, int(x.get("car_id") or 0)))
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checked": checked,
        "broken": broken,
        "broken_pct": round((broken / checked * 100.0), 2) if checked else 0.0,
        "fixed_replaced": replaced,
        "fixed_cleared": cleared,
        "fix_mode": bool(args.fix),
        "rows": report_rows,
    }

    json_path = Path(args.report_json)
    csv_path = Path(args.report_csv)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "car_id",
                "country",
                "status",
                "broken",
                "reason",
                "source_url",
                "old_thumbnail_url",
                "checked_url",
                "new_thumbnail_url",
                "fix",
            ],
        )
        writer.writeheader()
        for row in report_rows:
            writer.writerow(row)

    print(
        f"[audit_broken_thumbs] checked={checked} broken={broken} replaced={replaced} "
        f"cleared={cleared} json={json_path} csv={csv_path}"
    )


if __name__ == "__main__":
    main()

