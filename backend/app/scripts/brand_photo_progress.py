from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from backend.app.db import SessionLocal
from backend.app.utils.telegram import send_telegram_message


def _fmt_eta(hours: float | None) -> str:
    if hours is None:
        return "n/a"
    if hours <= 0:
        return "0m"
    total_min = int(hours * 60)
    h = total_min // 60
    m = total_min % 60
    return f"{h}h {m}m" if h else f"{m}m"


def main() -> None:
    ap = argparse.ArgumentParser(description="Brand-level local photo progress report")
    ap.add_argument("--country", default="DE", help="ISO country code, e.g. DE")
    ap.add_argument(
        "--source-key",
        default="all",
        help="substring for sources.key; use 'all' to disable source filter",
    )
    ap.add_argument(
        "--brands",
        default="",
        help="comma-separated brand list (optional). Empty = all brands in scope.",
    )
    ap.add_argument(
        "--rate-img-sec",
        type=float,
        default=0.0,
        help="current real throughput in images/sec for ETA calculation",
    )
    ap.add_argument("--top", type=int, default=20, help="limit rows in TG message")
    ap.add_argument("--telegram", action="store_true")
    ap.add_argument("--report-json", default="/app/artifacts/brand_photo_progress.json")
    ap.add_argument("--report-csv", default="/app/artifacts/brand_photo_progress.csv")
    args = ap.parse_args()

    country = (args.country or "").strip().upper()
    source_key = (args.source_key or "").strip().lower()
    brands = [b.strip() for b in (args.brands or "").split(",") if b.strip()]

    source_filter_sql = ""
    params: dict[str, Any] = {"country": country}
    if source_key not in {"", "all", "*"}:
        source_filter_sql = " AND lower(s.key) LIKE :source_key"
        params["source_key"] = f"%{source_key}%"

    brand_filter_sql = ""
    if brands:
        params["brands"] = brands
        brand_filter_sql = " AND c.brand = ANY(:brands)"

    sql = text(
        f"""
        WITH scope_cars AS (
          SELECT c.id, COALESCE(NULLIF(trim(c.brand), ''), 'UNKNOWN') AS brand
          FROM cars c
          JOIN sources s ON s.id = c.source_id
          WHERE c.is_available = TRUE
            AND upper(c.country) = :country
            {source_filter_sql}
            {brand_filter_sql}
        ),
        img AS (
          SELECT
            ci.car_id,
            COUNT(*) AS images_total,
            COUNT(*) FILTER (WHERE ci.url LIKE '/media/%') AS images_local
          FROM car_images ci
          GROUP BY ci.car_id
        )
        SELECT
          sc.brand AS brand,
          COUNT(*) AS cars_total,
          COUNT(*) FILTER (WHERE COALESCE(i.images_total, 0) > 0) AS cars_with_images,
          COUNT(*) FILTER (
            WHERE COALESCE(i.images_total, 0) > 0
              AND COALESCE(i.images_total, 0) = COALESCE(i.images_local, 0)
          ) AS cars_done_local,
          COALESCE(SUM(i.images_total), 0) AS images_total,
          COALESCE(SUM(i.images_local), 0) AS images_local
        FROM scope_cars sc
        LEFT JOIN img i ON i.car_id = sc.id
        GROUP BY sc.brand
        ORDER BY images_total DESC, cars_total DESC, sc.brand ASC
        """
    )

    with SessionLocal() as db:
        rows = db.execute(sql, params).mappings().all()

    report_rows: list[dict[str, Any]] = []
    total_images = 0
    total_local = 0
    total_cars = 0
    total_done_cars = 0

    for r in rows:
        images_total = int(r["images_total"] or 0)
        images_local = int(r["images_local"] or 0)
        images_left = max(0, images_total - images_local)
        cars_total = int(r["cars_total"] or 0)
        cars_done = int(r["cars_done_local"] or 0)
        pct_img = (images_local * 100.0 / images_total) if images_total else 0.0
        pct_cars = (cars_done * 100.0 / cars_total) if cars_total else 0.0
        eta_hours = (images_left / args.rate_img_sec / 3600.0) if args.rate_img_sec > 0 else None

        row = {
            "brand": str(r["brand"]),
            "cars_total": cars_total,
            "cars_with_images": int(r["cars_with_images"] or 0),
            "cars_done_local": cars_done,
            "cars_done_pct": round(pct_cars, 2),
            "images_total": images_total,
            "images_local": images_local,
            "images_left": images_left,
            "images_local_pct": round(pct_img, 2),
            "eta_hours": round(eta_hours, 2) if eta_hours is not None else None,
            "eta_human": _fmt_eta(eta_hours),
        }
        report_rows.append(row)
        total_images += images_total
        total_local += images_local
        total_cars += cars_total
        total_done_cars += cars_done

    total_left = max(0, total_images - total_local)
    total_eta_hours = (total_left / args.rate_img_sec / 3600.0) if args.rate_img_sec > 0 else None
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "country": country,
        "source_key": args.source_key,
        "brands_filter": brands,
        "brands_count": len(report_rows),
        "cars_total": total_cars,
        "cars_done_local": total_done_cars,
        "cars_done_pct": round((total_done_cars * 100.0 / total_cars), 2) if total_cars else 0.0,
        "images_total": total_images,
        "images_local": total_local,
        "images_left": total_left,
        "images_local_pct": round((total_local * 100.0 / total_images), 2) if total_images else 0.0,
        "rate_img_sec": args.rate_img_sec,
        "eta_hours": round(total_eta_hours, 2) if total_eta_hours is not None else None,
        "eta_human": _fmt_eta(total_eta_hours),
    }

    json_path = Path(args.report_json)
    csv_path = Path(args.report_csv)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps({"summary": summary, "rows": report_rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "brand",
                "cars_total",
                "cars_with_images",
                "cars_done_local",
                "cars_done_pct",
                "images_total",
                "images_local",
                "images_left",
                "images_local_pct",
                "eta_hours",
                "eta_human",
            ],
        )
        writer.writeheader()
        for row in report_rows:
            writer.writerow(row)

    print(
        "[brand_photo_progress] "
        f"country={country} brands={len(report_rows)} "
        f"images_local={total_local}/{total_images} ({summary['images_local_pct']}%) "
        f"eta={summary['eta_human']} json={json_path} csv={csv_path}",
        flush=True,
    )

    if args.telegram:
        import os

        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("[brand_photo_progress] telegram_missing_env", flush=True)
            return
        top_rows = sorted(report_rows, key=lambda x: x["images_left"], reverse=True)[: max(1, args.top)]
        lines = [
            f"brand_photo_progress {country}",
            f"total: {summary['images_local']}/{summary['images_total']} ({summary['images_local_pct']}%)",
            f"left: {summary['images_left']} eta: {summary['eta_human']} @ {args.rate_img_sec:.2f} img/s",
            "",
        ]
        for r in top_rows:
            lines.append(
                f"{r['brand']}: {r['images_local_pct']}% "
                f"({r['images_local']}/{r['images_total']}), left={r['images_left']}, eta={r['eta_human']}"
            )
        send_telegram_message(token, chat_id, "\n".join(lines))


if __name__ == "__main__":
    main()
