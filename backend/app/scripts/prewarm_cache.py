import os
import time
from typing import Dict, Any, List, Tuple

from backend.app.db import SessionLocal
from backend.app.services.cars_service import CarsService
from backend.app.routers.catalog import (
    filter_ctx_base,
    filter_ctx_brand,
    filter_ctx_model,
    list_cars,
    TOP_BRANDS,
)
from backend.app.utils.redis_cache import (
    redis_set_json,
    build_total_cars_key,
    build_cars_count_simple_key,
    normalize_filter_params,
    normalize_count_params,
)


def _prewarm_base(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    payload = filter_ctx_base(None, normalized.get("region"), normalized.get("country"), db=db)
    return f"filter_ctx_base:{normalized}", (time.perf_counter() - started) * 1000


def _prewarm_brand(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    payload = filter_ctx_brand(
        None,
        normalized.get("region"),
        normalized.get("country"),
        normalized.get("kr_type"),
        normalized.get("brand"),
        db=db,
    )
    return f"filter_ctx_brand:{normalized}", (time.perf_counter() - started) * 1000


def _prewarm_model(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    payload = filter_ctx_model(
        None,
        normalized.get("region"),
        normalized.get("country"),
        normalized.get("brand"),
        normalized.get("model"),
        db=db,
    )
    return f"filter_ctx_model:{normalized}", (time.perf_counter() - started) * 1000


def main() -> None:
    started = time.perf_counter()
    max_sec = float(os.getenv("PREWARM_MAX_SEC", "0") or 0)
    if not redis_set_json("prewarm_ping", {"ok": True}, ttl_sec=60):
        print("[prewarm] redis unavailable or write-disabled")
        raise SystemExit(2)
    with SessionLocal() as db:
        service = CarsService(db)
        base_tasks: List[Dict[str, Any]] = [
            {},
            {"region": "EU"},
            {"region": "EU", "country": "DE"},
            {"region": "KR"},
        ]
        if os.getenv("INCLUDE_RU_PREWARM") == "1":
            base_tasks.append({"region": "RU"})
        for params in base_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop base tasks")
                break
            key, ms = _prewarm_base(db, params)
            print(f"[prewarm] filter_ctx_base key={key} ms={ms:.2f}")
        eu_countries = ["DE", "AT", "NL", "BE", "FR", "IT", "ES"]
        brand_tasks = []
        for b in TOP_BRANDS:
            brand_tasks.append({"region": "EU", "country": "DE", "brand": b})
        brand_tasks.append({"region": "EU", "country": "AT", "brand": "Cadillac"})
        for params in brand_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop brand tasks")
                break
            key, ms = _prewarm_brand(db, params)
            print(f"[prewarm] filter_ctx_brand key={key} ms={ms:.2f}")
        model_tasks = [
            {"region": "EU", "country": "DE", "brand": "BMW", "model": "X5"},
        ]
        for params in model_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop model tasks")
                break
            key, ms = _prewarm_model(db, params)
            print(f"[prewarm] filter_ctx_model key={key} ms={ms:.2f}")
        total = service.total_cars()
        redis_set_json(build_total_cars_key(), total, ttl_sec=600)
        print(f"[prewarm] total_cars={total}")
        count_keys = [
            {"region": "EU"},
            {"region": "KR"},
        ]
        for c in eu_countries:
            count_keys.append({"region": "EU", "country": c})
        count_keys.append({"region": "EU", "country": "AT"})
        for params in count_keys:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop count prewarm")
                break
            normalized = normalize_count_params(params)
            count = service.count_cars(**normalized)
            cache_key = build_cars_count_simple_key(
                normalized.get("region"),
                normalized.get("country"),
                normalized.get("brand"),
            )
            redis_set_json(cache_key, int(count), ttl_sec=600)
            print(f"[prewarm] cars_count key={cache_key} value={count}")

        def _prewarm_list(params: Dict[str, Any]) -> None:
            list_cars(
                None,
                db=db,
                region=params.get("region"),
                country=params.get("country"),
                eu_country=None,
                brand=params.get("brand"),
                line=None,
                source=None,
                q=None,
                model=None,
                generation=None,
                color=None,
                body_type=None,
                engine_type=None,
                transmission=None,
                drive_type=None,
                num_seats=None,
                doors_count=None,
                emission_class=None,
                efficiency_class=None,
                climatisation=None,
                airbags=None,
                interior_design=None,
                air_suspension=None,
                price_rating_label=None,
                owners_count=None,
                price_min=None,
                price_max=None,
                power_hp_min=None,
                power_hp_max=None,
                engine_cc_min=None,
                engine_cc_max=None,
                year_min=None,
                year_max=None,
                mileage_min=None,
                mileage_max=None,
                kr_type=None,
                reg_year_min=None,
                reg_month_min=None,
                reg_year_max=None,
                reg_month_max=None,
                condition=None,
                sort=None,
                page=1,
                page_size=12,
            )

        list_tasks = [
            {"region": "EU"},
            {"region": "KR"},
            {"region": "EU", "country": "DE"},
            {"region": "EU", "country": "AT"},
        ]
        for params in list_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop list prewarm")
                break
            _prewarm_list(params)
            print(f"[prewarm] cars_list region={params.get('region')} country={params.get('country')}")
        kr_sort_tasks = [
            {"region": "KR", "sort": "price_asc"},
            {"region": "KR", "sort": "price_desc"},
        ]
        for params in kr_sort_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop KR sort prewarm")
                break
            list_cars(
                None,
                db=db,
                region="KR",
                country=None,
                eu_country=None,
                brand=None,
                line=None,
                source=None,
                q=None,
                model=None,
                generation=None,
                color=None,
                body_type=None,
                engine_type=None,
                transmission=None,
                drive_type=None,
                num_seats=None,
                doors_count=None,
                emission_class=None,
                efficiency_class=None,
                climatisation=None,
                airbags=None,
                interior_design=None,
                air_suspension=None,
                price_rating_label=None,
                owners_count=None,
                price_min=None,
                price_max=None,
                power_hp_min=None,
                power_hp_max=None,
                engine_cc_min=None,
                engine_cc_max=None,
                year_min=None,
                year_max=None,
                mileage_min=None,
                mileage_max=None,
                kr_type=None,
                reg_year_min=None,
                reg_month_min=None,
                reg_year_max=None,
                reg_month_max=None,
                condition=None,
                sort=params.get("sort"),
                page=1,
                page_size=24,
            )
            print(f"[prewarm] cars_list region=KR sort={params.get('sort')}")
        for params in brand_tasks:
            if max_sec and (time.perf_counter() - started) > max_sec:
                print("[prewarm] max runtime reached, stop brand list prewarm")
                break
            _prewarm_list(params)
            print(f"[prewarm] cars_list brand={params.get('brand')} region={params.get('region')} country={params.get('country')}")
    print(f"[prewarm] done in {(time.perf_counter()-started)*1000:.2f} ms")


if __name__ == "__main__":
    main()
