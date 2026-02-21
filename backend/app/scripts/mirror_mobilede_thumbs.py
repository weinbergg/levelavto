from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import requests
from PIL import Image
from sqlalchemy import func, select

from backend.app.db import SessionLocal
from backend.app.models import Car, CarImage, Source
from backend.app.utils.thumbs import normalize_classistatic_url


def _media_root() -> Path:
    return Path(__file__).resolve().parents[3] / "фото-видео"


def _normalize(url: str | None) -> str | None:
    normalized = normalize_classistatic_url(url)
    if normalized:
        return normalized
    raw = (url or "").strip()
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("http://"):
        return f"https://{raw[7:]}"
    if raw.startswith("https://"):
        return raw
    return None


def _uniq(values: Iterable[str | None]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _target_paths(base_dir: Path, car_id: int, src_url: str) -> tuple[Path, str]:
    digest = hashlib.sha1(src_url.encode("utf-8")).hexdigest()[:12]
    sub = str(car_id % 1000).zfill(3)
    rel = Path("машины") / "mirror" / sub / f"{car_id}_{digest}.webp"
    abs_path = base_dir / rel
    web_path = "/" + Path("media", rel).as_posix()
    return abs_path, web_path


def _download_and_convert(
    *,
    car_id: int,
    candidates: list[str],
    base_dir: Path,
    timeout_sec: float,
    max_bytes: int,
    max_width: int,
    quality: int,
) -> dict:
    last_error = "no_candidates"
    for src in candidates:
        dst_path, web_path = _target_paths(base_dir, car_id, src)
        if dst_path.exists() and dst_path.stat().st_size > 0:
            return {"id": car_id, "ok": True, "web_path": web_path, "url": src, "cached": True}
        try:
            with requests.get(src, timeout=(3.0, timeout_sec), stream=True, allow_redirects=True) as resp:
                if resp.status_code != 200:
                    last_error = f"http_{resp.status_code}"
                    continue
                buf = io.BytesIO()
                total = 0
                for chunk in resp.iter_content(chunk_size=128 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError("too_large")
                    buf.write(chunk)
            buf.seek(0)
            img = Image.open(buf).convert("RGB")
            if max_width > 0 and img.width > max_width:
                h = int(img.height * max_width / img.width)
                img = img.resize((max_width, h), Image.LANCZOS)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst_path.with_suffix(".tmp.webp")
            img.save(tmp, format="WEBP", quality=quality, method=6)
            tmp.replace(dst_path)
            return {"id": car_id, "ok": True, "web_path": web_path, "url": src, "cached": False}
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)[:200]
            continue
    return {"id": car_id, "ok": False, "error": last_error}


def main() -> None:
    ap = argparse.ArgumentParser(description="Mirror mobile.de thumbnails into local /media storage")
    ap.add_argument("--region", default="EU")
    ap.add_argument("--source-key", default="mobile")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--max-bytes", type=int, default=8_000_000)
    ap.add_argument("--max-width", type=int, default=1024)
    ap.add_argument("--quality", type=int, default=80)
    ap.add_argument("--only-missing-local", action="store_true")
    ap.add_argument("--updated-since-hours", type=int, default=None)
    ap.add_argument("--report-json", default=None)
    ap.add_argument("--report-csv", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.region.upper() != "EU":
        raise SystemExit("Only region=EU is supported in this script")

    base_dir = _media_root()
    scanned = 0
    checked = 0
    mirrored = 0
    failed = 0
    already_local = 0
    updated_rows = 0
    last_id = 0
    problems: list[dict] = []
    report_rows: list[dict] = []

    with SessionLocal() as db:
        since_ts = None
        if args.updated_since_hours and args.updated_since_hours > 0:
            since_ts = datetime.utcnow() - timedelta(hours=args.updated_since_hours)
        while checked < args.limit:
            q = (
                select(
                    Car.id,
                    Car.thumbnail_url,
                    Car.thumbnail_local_path,
                )
                .join(Source, Source.id == Car.source_id)
                .where(
                    Car.id > last_id,
                    Car.is_available.is_(True),
                    func.lower(Source.key).like(f"%{args.source_key.lower()}%"),
                    Car.country != "KR",
                )
                .order_by(Car.id.asc())
                .limit(args.batch)
            )
            if args.only_missing_local:
                q = q.where(
                    (Car.thumbnail_local_path.is_(None))
                    | (Car.thumbnail_local_path == "")
                )
            if since_ts is not None:
                q = q.where(Car.updated_at >= since_ts)

            rows = db.execute(q).all()
            if not rows:
                break
            scanned += len(rows)
            last_id = rows[-1][0]

            car_ids = [r[0] for r in rows]
            first_images = dict(
                db.execute(
                    select(CarImage.car_id, func.min(CarImage.url))
                    .where(CarImage.car_id.in_(car_ids))
                    .group_by(CarImage.car_id)
                ).all()
            )

            tasks = []
            updates: dict[int, str] = {}
            for car_id, thumb_url, local_path in rows:
                if checked >= args.limit:
                    break
                checked += 1
                local = (local_path or "").strip()
                if local.startswith("/media/") and (base_dir / local.removeprefix("/media/")).exists():
                    already_local += 1
                    continue
                candidates = _uniq(
                    [
                        _normalize(thumb_url),
                        _normalize(first_images.get(car_id)),
                    ]
                )
                tasks.append((car_id, candidates))

            if not tasks:
                continue

            with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
                fut_map = {
                    ex.submit(
                        _download_and_convert,
                        car_id=car_id,
                        candidates=candidates,
                        base_dir=base_dir,
                        timeout_sec=args.timeout,
                        max_bytes=args.max_bytes,
                        max_width=args.max_width,
                        quality=args.quality,
                    ): car_id
                    for car_id, candidates in tasks
                }
                for fut in as_completed(fut_map):
                    result = fut.result()
                    car_id = result["id"]
                    if result.get("ok"):
                        mirrored += 1
                        updates[car_id] = result["web_path"]
                        report_rows.append(
                            {
                                "car_id": car_id,
                                "status": "ok",
                                "url": result.get("url"),
                                "thumbnail_local_path": result["web_path"],
                                "cached": bool(result.get("cached")),
                            }
                        )
                    else:
                        failed += 1
                        err = result.get("error") or "failed"
                        problems.append({"car_id": car_id, "error": err})
                        report_rows.append(
                            {
                                "car_id": car_id,
                                "status": "failed",
                                "url": "",
                                "thumbnail_local_path": "",
                                "cached": False,
                                "error": err,
                            }
                        )

            if updates and not args.dry_run:
                for cid, local_path in updates.items():
                    db.query(Car).filter(Car.id == cid).update(
                        {"thumbnail_local_path": local_path},
                        synchronize_session=False,
                    )
                    updated_rows += 1
                db.commit()

    summary = {
        "scanned": scanned,
        "checked": checked,
        "mirrored": mirrored,
        "failed": failed,
        "already_local": already_local,
        "updated_rows": updated_rows,
        "dry_run": bool(args.dry_run),
    }
    if args.report_json:
        Path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
        payload = {**summary, "problems": problems[:1000]}
        Path(args.report_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.report_csv:
        csv_path = Path(args.report_csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["car_id", "status", "url", "thumbnail_local_path", "cached", "error"]
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for row in report_rows:
                w.writerow({k: row.get(k, "") for k in fieldnames})

    print(
        "[mirror_mobilede_thumbs] "
        f"scanned={scanned} checked={checked} mirrored={mirrored} failed={failed} "
        f"already_local={already_local} updated_rows={updated_rows} dry_run={int(args.dry_run)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
