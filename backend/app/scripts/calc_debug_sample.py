from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Callable, Dict, List, Optional

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.calc_debug import build_calc_debug


def _age_months(reg_year: Optional[int], reg_month: Optional[int]) -> Optional[int]:
    if not reg_year or not reg_month:
        return None
    try:
        d = date(reg_year, reg_month, 1)
        today = date.today().replace(day=1)
        return max((today.year - d.year) * 12 + (today.month - d.month), 0)
    except Exception:
        return None


def _is_bev(car: Car) -> bool:
    if car.engine_cc and car.engine_cc > 0:
        return False
    if not ((car.power_kw and car.power_kw > 0) or (car.power_hp and car.power_hp > 0)):
        return False
    if not car.engine_type:
        return False
    return "electric" in car.engine_type.lower()


def _is_phev_or_hybrid(car: Car) -> bool:
    if not car.engine_type:
        return False
    t = car.engine_type.lower()
    return ("hybrid" in t) or ("plug" in t) or ("electric" in t and car.engine_cc and car.engine_cc > 0)


def _pick_first(cars: List[Car], pred: Callable[[Car], bool], used: set[int]) -> Optional[Car]:
    for car in cars:
        if car.id in used:
            continue
        if pred(car):
            return car
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eur-rate", type=float, default=91.08)
    parser.add_argument("--usd-rate", type=float, default=None)
    parser.add_argument("--out", type=str, default="artifacts/calc_debug")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--prefer-mobilede", action="store_true", default=True)
    args = parser.parse_args()

    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    with SessionLocal() as db:
        base_query = db.query(Car).filter(Car.is_available.is_(True))
        mobilede = (
            base_query.filter(Car.source_url.ilike("%mobile.de%"))
            .order_by(Car.id.desc())
            .limit(5000)
            .all()
        )
        all_cars = base_query.order_by(Car.id.desc()).limit(5000).all()
        pool = mobilede if args.prefer_mobilede and mobilede else all_cars

        used: set[int] = set()
        selected: List[Dict[str, str | int]] = []

        categories = [
            ("bev", lambda c: _is_bev(c)),
            ("phev_hybrid", lambda c: _is_phev_or_hybrid(c)),
            ("ice_under_3", lambda c: (c.engine_cc and c.engine_cc > 0) and (_age_months(c.registration_year, c.registration_month) is not None) and _age_months(c.registration_year, c.registration_month) < 36),
            ("ice_3_5", lambda c: (c.engine_cc and c.engine_cc > 0) and (_age_months(c.registration_year, c.registration_month) is not None) and 36 <= _age_months(c.registration_year, c.registration_month) <= 60),
            ("ice_over_5", lambda c: (c.engine_cc and c.engine_cc > 0) and (_age_months(c.registration_year, c.registration_month) is not None) and _age_months(c.registration_year, c.registration_month) > 60),
            ("diesel", lambda c: c.engine_type and "diesel" in c.engine_type.lower()),
            ("big_cc", lambda c: c.engine_cc and c.engine_cc >= 3000),
            ("small_cc", lambda c: c.engine_cc and c.engine_cc <= 1600),
        ]

        for name, pred in categories:
            car = _pick_first(pool, pred, used)
            if not car and pool is not all_cars:
                car = _pick_first(all_cars, pred, used)
            if not car:
                continue
            used.add(car.id)
            selected.append({"category": name, "id": car.id})
            if len(selected) >= args.limit:
                break

        manifest = []
        for item in selected:
            car_id = int(item["id"])
            payload = build_calc_debug(db, car_id, eur_rate=args.eur_rate, usd_rate=args.usd_rate)
            path = os.path.join(out_dir, f"{car_id}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            car = payload.get("car", {})
            manifest.append(
                {
                    "category": item["category"],
                    "car_id": car_id,
                    "brand": car.get("brand"),
                    "model": car.get("model"),
                    "engine_cc": car.get("engine_cc"),
                    "engine_type": car.get("engine_type"),
                    "registration_year": car.get("registration_year"),
                    "registration_month": car.get("registration_month"),
                    "source_url": car.get("source_url"),
                    "json_path": path,
                }
            )

        manifest_path = os.path.join(out_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"[calc_debug_sample] wrote {len(manifest)} items to {out_dir}")
        for row in manifest:
            print(f"[calc_debug_sample] {row['category']}: {row['car_id']} {row.get('source_url')}")


if __name__ == "__main__":
    main()
