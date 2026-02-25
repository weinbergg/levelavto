from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from PIL import Image
from sqlalchemy import func, select

from backend.app.db import SessionLocal
from backend.app.models import Car, CarImage, Source
from backend.app.utils.telegram import send_telegram_message
from backend.app.utils.thumbs import normalize_classistatic_url


def _media_root() -> Path:
    return Path(__file__).resolve().parents[3] / "фото-видео"


def _normalize(url: str | None) -> str | None:
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
    if raw.startswith("https://"):
        return raw
    if raw.startswith("/media/"):
        return raw
    return None


def _target_paths(
    base_dir: Path,
    car_id: int,
    image_id: int,
    src_url: str,
    fmt: str,
) -> tuple[Path, str]:
    digest = hashlib.sha1(src_url.encode("utf-8")).hexdigest()[:12]
    sub = str(car_id % 1000).zfill(3)
    rel = Path("машины") / "gallery_mirror" / sub / f"{car_id}_{image_id}_{digest}.{fmt}"
    abs_path = base_dir / rel
    web_path = "/" + Path("media", rel).as_posix()
    return abs_path, web_path


def _download_convert(
    *,
    image_id: int,
    car_id: int,
    src_url: str,
    base_dir: Path,
    timeout_sec: float,
    max_bytes: int,
    max_width: int,
    quality: int,
    fmt: str,
) -> dict:
    if src_url.startswith("/media/"):
        return {
            "image_id": image_id,
            "car_id": car_id,
            "ok": True,
            "web_path": src_url,
            "cached": True,
            "source": "already_local",
        }
    dst_abs, dst_web = _target_paths(base_dir, car_id, image_id, src_url, fmt)
    if dst_abs.exists() and dst_abs.stat().st_size > 0:
        return {
            "image_id": image_id,
            "car_id": car_id,
            "ok": True,
            "web_path": dst_web,
            "cached": True,
            "source": "cache_hit",
        }
    try:
        with requests.get(src_url, timeout=(3.0, timeout_sec), stream=True, allow_redirects=True) as resp:
            if resp.status_code != 200:
                return {"image_id": image_id, "car_id": car_id, "ok": False, "error": f"http_{resp.status_code}"}
            buf = io.BytesIO()
            total = 0
            for chunk in resp.iter_content(chunk_size=128 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    return {"image_id": image_id, "car_id": car_id, "ok": False, "error": "too_large"}
                buf.write(chunk)
        buf.seek(0)
        img = Image.open(buf).convert("RGB")
        if max_width > 0 and img.width > max_width:
            h = int(img.height * max_width / img.width)
            img = img.resize((max_width, h), Image.LANCZOS)
        dst_abs.parent.mkdir(parents=True, exist_ok=True)
        tmp_abs = dst_abs.with_suffix(f".tmp.{fmt}")
        save_fmt = "JPEG" if fmt in {"jpg", "jpeg"} else "WEBP"
        save_kwargs = {"format": save_fmt, "quality": quality}
        if save_fmt == "WEBP":
            save_kwargs["method"] = 6
        img.save(tmp_abs, **save_kwargs)
        tmp_abs.replace(dst_abs)
        return {
            "image_id": image_id,
            "car_id": car_id,
            "ok": True,
            "web_path": dst_web,
            "cached": False,
            "source": "download",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "image_id": image_id,
            "car_id": car_id,
            "ok": False,
            "error": str(exc)[:180],
        }


def main() -> None:
    ap = argparse.ArgumentParser(description="Mirror car_images to local /media and rewrite URLs")
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default="AT")
    ap.add_argument("--source-key", default="mobile")
    ap.add_argument("--limit-cars", type=int, default=5000)
    ap.add_argument("--max-images-per-car", type=int, default=20)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--timeout", type=float, default=6.0)
    ap.add_argument("--max-bytes", type=int, default=8_000_000)
    ap.add_argument("--max-width", type=int, default=1280)
    ap.add_argument("--quality", type=int, default=76)
    ap.add_argument("--format", choices=["webp", "jpg"], default="webp")
    ap.add_argument("--delete-unmirrored", action="store_true", help="delete image rows failed to mirror")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--telegram", action="store_true")
    ap.add_argument("--telegram-interval", type=int, default=300)
    ap.add_argument("--log-interval", type=int, default=60)
    ap.add_argument("--report-json", default="/app/artifacts/mirror_car_images_local.json")
    args = ap.parse_args()

    country = (args.country or "").strip().upper() or None
    base_dir = _media_root()
    started = time.time()
    last_log = 0.0
    last_tg = 0.0

    token = os.getenv("TELEGRAM_BOT_TOKEN") if args.telegram else None
    chat_id = os.getenv("TELEGRAM_CHAT_ID") if args.telegram else None

    total = 0
    checked = 0
    mirrored = 0
    failed = 0
    rewritten = 0
    deleted = 0
    thumb_updated = 0
    problems: list[dict] = []

    def snapshot() -> tuple[float, int, float, str]:
        elapsed = max(time.time() - started, 1.0)
        rate = checked / elapsed if checked else 0.0
        remain = max(total - checked, 0)
        eta_sec = (remain / rate) if rate > 0 else None
        if eta_sec is None:
            eta = "n/a"
        else:
            s = int(eta_sec)
            h = s // 3600
            m = (s % 3600) // 60
            ss = s % 60
            eta = f"{h}h {m}m {ss}s" if h else (f"{m}m {ss}s" if m else f"{ss}s")
        pct = (checked * 100.0 / total) if total else 0.0
        return pct, remain, rate, eta

    def notify(stage: str) -> None:
        nonlocal last_tg
        if not token or not chat_id:
            return
        now = time.time()
        if stage not in {"start", "done"} and now - last_tg < max(30, args.telegram_interval):
            return
        pct, remain, rate, eta = snapshot()
        msg = (
            f"mirror_car_images_local {stage}\n"
            f"checked={checked}/{total or '?'} ({pct:.2f}%)\n"
            f"mirrored={mirrored} failed={failed} rewritten={rewritten} deleted={deleted}\n"
            f"thumb_updated={thumb_updated} rate={rate:.2f} img/s remaining={remain if total else '?'} eta={eta}"
        )
        if send_telegram_message(token, chat_id, msg):
            last_tg = now

    def log(stage: str) -> None:
        nonlocal last_log
        now = time.time()
        if stage not in {"start", "done"} and now - last_log < max(5, args.log_interval):
            return
        pct, remain, rate, eta = snapshot()
        print(
            "[mirror_car_images_local] "
            f"stage={stage} checked={checked}/{total or '?'} ({pct:.2f}%) "
            f"mirrored={mirrored} failed={failed} rewritten={rewritten} deleted={deleted} "
            f"thumb_updated={thumb_updated} rate={rate:.2f}/s remaining={remain if total else '?'} eta={eta}",
            flush=True,
        )
        last_log = now

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
        if country:
            car_q = car_q.filter(func.upper(Car.country) == country)
        car_ids = [int(v[0]) for v in car_q.order_by(Car.id.asc()).limit(max(1, args.limit_cars)).all()]
        if not car_ids:
            print("[mirror_car_images_local] no cars matched", flush=True)
            return

        rows = db.execute(
            select(CarImage.id, CarImage.car_id, CarImage.url, CarImage.position, CarImage.is_primary)
            .where(CarImage.car_id.in_(car_ids))
            .order_by(CarImage.car_id.asc(), CarImage.position.asc(), CarImage.id.asc())
        ).all()

        per_car_count: dict[int, int] = {}
        payload: list[tuple[int, int, str, int, bool]] = []
        for image_id, car_id, url, pos, is_primary in rows:
            cid = int(car_id)
            count = per_car_count.get(cid, 0)
            if count >= args.max_images_per_car:
                continue
            per_car_count[cid] = count + 1
            payload.append((int(image_id), cid, str(url or ""), int(pos or 0), bool(is_primary)))
        total = len(payload)
        log("start")
        notify("start")

        normalized_payload: list[tuple[int, int, str, int, bool]] = []
        invalid_ids: set[int] = set()
        for image_id, car_id, url, pos, is_primary in payload:
            n = _normalize(url)
            if not n:
                invalid_ids.add(image_id)
                failed += 1
                checked += 1
                problems.append({"image_id": image_id, "car_id": car_id, "error": "invalid_url", "url": url})
                continue
            normalized_payload.append((image_id, car_id, n, pos, is_primary))

        results_by_id: dict[int, dict] = {}
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [
                pool.submit(
                    _download_convert,
                    image_id=image_id,
                    car_id=car_id,
                    src_url=src_url,
                    base_dir=base_dir,
                    timeout_sec=args.timeout,
                    max_bytes=args.max_bytes,
                    max_width=args.max_width,
                    quality=args.quality,
                    fmt=args.format,
                )
                for image_id, car_id, src_url, _, _ in normalized_payload
            ]
            for fut in as_completed(futures):
                res = fut.result()
                image_id = int(res["image_id"])
                results_by_id[image_id] = res
                checked += 1
                if res.get("ok"):
                    mirrored += 1
                else:
                    failed += 1
                    problems.append(
                        {
                            "image_id": image_id,
                            "car_id": int(res.get("car_id") or 0),
                            "error": res.get("error") or "unknown",
                        }
                    )
                log("progress")
                notify("progress")

        if not args.dry_run:
            # Rewrite URLs for mirrored images.
            image_rows = db.query(CarImage).filter(CarImage.id.in_([r[0] for r in payload])).all()
            for row in image_rows:
                res = results_by_id.get(int(row.id))
                if res and res.get("ok") and res.get("web_path"):
                    new_url = str(res["web_path"])
                    if (row.url or "").strip() != new_url:
                        row.url = new_url
                        rewritten += 1
                elif args.delete_unmirrored:
                    db.delete(row)
                    deleted += 1

            # Sync car thumbnail_local_path to first local image.
            local_first_by_car: dict[int, str] = {}
            for image_id, car_id, _, pos, is_primary in normalized_payload:
                res = results_by_id.get(image_id)
                if not res or not res.get("ok"):
                    continue
                web = str(res.get("web_path") or "")
                if not web.startswith("/media/"):
                    continue
                if car_id not in local_first_by_car:
                    local_first_by_car[car_id] = web
                if is_primary or pos == 0:
                    local_first_by_car[car_id] = web

            cars = db.query(Car).filter(Car.id.in_(car_ids)).all()
            for car in cars:
                local = local_first_by_car.get(int(car.id))
                if local:
                    if (car.thumbnail_local_path or "").strip() != local:
                        car.thumbnail_local_path = local
                        thumb_updated += 1

            db.commit()

        log("done")
        notify("done")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "country": country,
        "checked": checked,
        "total": total,
        "mirrored": mirrored,
        "failed": failed,
        "rewritten": rewritten,
        "deleted": deleted,
        "thumb_updated": thumb_updated,
        "dry_run": bool(args.dry_run),
        "format": args.format,
        "quality": args.quality,
        "max_width": args.max_width,
        "problems": problems[:5000],
    }
    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        "[mirror_car_images_local] "
        f"checked={checked} mirrored={mirrored} failed={failed} rewritten={rewritten} "
        f"deleted={deleted} thumb_updated={thumb_updated} json={report_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
