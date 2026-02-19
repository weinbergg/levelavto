from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from backend.app.db import SessionLocal
from backend.app.models.car import Car
from backend.app.services.calc_debug import build_calc_debug
from backend.app.services.cars_service import CarsService


def _parse_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for part in raw.replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return sorted(set(out))


def _extract_step(steps: list[dict], name: str) -> float | None:
    for step in steps or []:
        if step.get("name") == name:
            try:
                return float(step.get("value"))
            except Exception:
                return None
    return None


def _build_scope_sql(region: str | None, country: str | None) -> tuple[str, dict[str, Any]]:
    where = "WHERE is_available IS true"
    params: dict[str, Any] = {}
    if country:
        where += " AND upper(country)=:country"
        params["country"] = country.strip().upper()
        return where, params
    if region:
        reg = region.strip().upper()
        if reg == "KR":
            where += " AND upper(country) LIKE 'KR%'"
        elif reg == "EU":
            where += " AND upper(country) <> 'RU' AND upper(country) NOT LIKE 'KR%'"
        elif reg == "RU":
            where += " AND upper(country)='RU'"
    return where, params


def _run_base_counts(db, where: str, params: dict[str, Any]) -> dict[str, int]:
    def one(sql: str) -> int:
        return int(db.execute(text(sql), params).scalar() or 0)

    return {
        "total_available": one(f"SELECT count(*) FROM cars {where}"),
        "missing_total": one(f"SELECT count(*) FROM cars {where} AND total_price_rub_cached IS NULL"),
        "missing_breakdown": one(f"SELECT count(*) FROM cars {where} AND calc_breakdown_json IS NULL"),
        "missing_total_in_breakdown": one(
            "SELECT count(*) FROM cars "
            + where
            + " AND calc_breakdown_json IS NOT NULL AND NOT EXISTS ("
            + "SELECT 1 FROM jsonb_array_elements(calc_breakdown_json::jsonb) e "
            + "WHERE e->>'title'='Итого (RUB)'"
            + ")"
        ),
        "cached_vs_breakdown_mismatch": one(
            "SELECT count(*) FROM cars "
            + where
            + " AND total_price_rub_cached IS NOT NULL AND calc_breakdown_json IS NOT NULL "
            + "AND EXISTS ("
            + "SELECT 1 FROM jsonb_array_elements(calc_breakdown_json::jsonb) e "
            + "WHERE e->>'title'='Итого (RUB)' "
            + "AND abs((e->>'amount_rub')::numeric - total_price_rub_cached) > 1"
            + ")"
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit prices and calculator cache consistency")
    ap.add_argument("--region", default="EU", help="EU|KR|RU|ALL")
    ap.add_argument("--country", default=None, help="country code, e.g. DE")
    ap.add_argument("--ids", default=None, help="comma-separated IDs to verify deeply")
    ap.add_argument("--stride", type=int, default=0, help="deep-check every Nth id in scope")
    ap.add_argument("--max-deep", type=int, default=2000, help="max ids for deep check")
    ap.add_argument("--tolerance-rub", type=float, default=1.0)
    ap.add_argument("--fix", action="store_true", help="force recalculation for mismatches")
    ap.add_argument("--report-json", default="/app/artifacts/price_audit_report.json")
    ap.add_argument("--report-csv", default="/app/artifacts/price_audit_report.csv")
    args = ap.parse_args()

    requested_ids = _parse_ids(args.ids)
    deep_rows: list[dict[str, Any]] = []
    fixed = 0

    with SessionLocal() as db:
        svc = CarsService(db)
        where, where_params = _build_scope_sql(args.region, args.country)
        counts = _run_base_counts(db, where, where_params)

        candidate_ids: list[int] = list(requested_ids)
        if args.stride and args.stride > 0:
            sql = (
                "SELECT id FROM cars "
                + where
                + " AND (id % :stride)=0 ORDER BY id LIMIT :max_deep"
            )
            rows = db.execute(
                text(sql),
                {
                    **where_params,
                    "stride": int(args.stride),
                    "max_deep": int(max(1, args.max_deep)),
                },
            ).fetchall()
            candidate_ids.extend(int(r[0]) for r in rows)

        # Always include claimed IDs from customer if present in DB.
        candidate_ids = sorted(set(candidate_ids))
        if len(candidate_ids) > args.max_deep:
            candidate_ids = candidate_ids[: args.max_deep]

        for car_id in candidate_ids:
            car = db.query(Car).filter(Car.id == car_id).first()
            if not car:
                deep_rows.append({"car_id": car_id, "error": "not_found"})
                continue
            try:
                dbg = build_calc_debug(db, car_id)
                steps = dbg.get("steps") or []
                debug_total = _extract_step(steps, "Итого (RUB)")
                cached_total = float(car.total_price_rub_cached) if car.total_price_rub_cached is not None else None
                diff = None
                mismatch = False
                if debug_total is not None and cached_total is not None:
                    diff = float(debug_total) - float(cached_total)
                    mismatch = abs(diff) > float(args.tolerance_rub)
                elif cached_total is None or debug_total is None:
                    mismatch = True

                row = {
                    "car_id": car.id,
                    "country": car.country,
                    "brand": car.brand,
                    "model": car.model,
                    "cached_total_rub": cached_total,
                    "debug_total_rub": debug_total,
                    "delta_rub": diff,
                    "mismatch": mismatch,
                    "price_source_note": next(
                        (s.get("note") for s in steps if s.get("name") == "price_net_eur"),
                        None,
                    ),
                    "input_price_net_eur": dbg.get("input", {}).get("price_net_eur"),
                }
                if mismatch and args.fix:
                    svc.ensure_calc_cache(car, force=True)
                    fixed += 1
                    row["fixed"] = True
                    row["new_cached_total_rub"] = (
                        float(car.total_price_rub_cached) if car.total_price_rub_cached is not None else None
                    )
                deep_rows.append(row)
            except Exception as exc:
                deep_rows.append({"car_id": car_id, "error": str(exc), "mismatch": True})

        if args.fix and fixed:
            db.commit()

    mismatches = sum(1 for r in deep_rows if r.get("mismatch"))
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {"region": args.region, "country": args.country},
        "counts": counts,
        "deep_checked": len(deep_rows),
        "deep_mismatches": mismatches,
        "fixed": fixed,
        "rows": deep_rows,
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
                "brand",
                "model",
                "cached_total_rub",
                "debug_total_rub",
                "delta_rub",
                "mismatch",
                "fixed",
                "new_cached_total_rub",
                "price_source_note",
                "input_price_net_eur",
                "error",
            ],
        )
        writer.writeheader()
        for row in deep_rows:
            writer.writerow(row)

    print(
        f"[audit_prices] deep_checked={len(deep_rows)} mismatches={mismatches} fixed={fixed} "
        f"json={json_path} csv={csv_path}"
    )


if __name__ == "__main__":
    main()
