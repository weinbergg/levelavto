import os
import time
from typing import Dict, Any, List, Tuple

from backend.app.db import SessionLocal
from backend.app.services.cars_service import CarsService, normalize_brand
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


def _should_stop(started: float, max_sec: float, now: float | None = None) -> bool:
    if not max_sec:
        return False
    current = now if now is not None else time.monotonic()
    return (current - started) > max_sec


def main() -> None:
    started = time.monotonic()
    max_sec = float(os.getenv("PREWARM_MAX_SEC", "600") or 600)
    include_brand_ctx = os.getenv("PREWARM_INCLUDE_BRAND_CTX", "0") == "1"
    include_model_ctx = os.getenv("PREWARM_INCLUDE_MODEL_CTX", "0") == "1"
    include_brand_lists = os.getenv("PREWARM_INCLUDE_BRAND_LISTS", "1") != "0"
    list_sort = os.getenv("PREWARM_LIST_SORT", "price_asc")
    list_page_size = int(os.getenv("PREWARM_LIST_PAGE_SIZE", "12") or 12)
    eu_country = os.getenv("PREWARM_EU_COUNTRY", "DE")
    hot_brands_raw = os.getenv(
        "PREWARM_HOT_BRANDS",
        ",".join(
            [
                "BMW",
                "Audi",
                "Mercedes-Benz",
                "Porsche",
                "Skoda",
                "Toyota",
                "Volkswagen",
                "Volvo",
                "Aston Martin",
                "Bentley",
                "Bugatti",
                "BYD",
                "Cadillac",
                "Ferrari",
                "GMC",
                "Hummer",
                "Hyundai",
                "Jaguar",
                "Jeep",
                "Kia",
                "Lamborghini",
                "Land Rover",
                "Lexus",
                "Lincoln",
                "Lynk&Co",
                "Maybach",
                "Mazda",
                "McLaren",
                "Mini",
                "Rolls-Royce",
                "Tesla",
                "Zeekr",
            ]
        ),
    )
    hot_brands = []
    for raw in hot_brands_raw.split(","):
        b = normalize_brand(raw.strip())
        if b and b not in hot_brands:
            hot_brands.append(b)

    if not redis_set_json("prewarm_ping", {"ok": True}, ttl_sec=60):
        print("[prewarm] redis unavailable or write-disabled")
        raise SystemExit(2)
    if _should_stop(started, max_sec):
        print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
        return
    with SessionLocal() as db:
        service = CarsService(db)
        base_tasks: List[Dict[str, Any]] = [
            {},
            {"region": "EU"},
            {"region": "EU", "country": eu_country},
            {"region": "KR"},
        ]
        if os.getenv("INCLUDE_RU_PREWARM") == "1":
            base_tasks.append({"region": "RU"})
        for params in base_tasks:
            if _should_stop(started, max_sec):
                print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                return
            key, ms = _prewarm_base(db, params)
            print(f"[prewarm] filter_ctx_base key={key} ms={ms:.2f}")
        eu_countries = [eu_country, "AT", "NL", "BE", "FR", "IT", "ES"]
        brand_tasks = [{"region": "EU", "country": eu_country, "brand": b} for b in hot_brands]
        if include_brand_ctx:
            for params in brand_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                    return
                key, ms = _prewarm_brand(db, params)
                print(f"[prewarm] filter_ctx_brand key={key} ms={ms:.2f}")
        model_tasks = [{"region": "EU", "country": eu_country, "brand": "BMW", "model": "X5"}]
        if include_model_ctx:
            for params in model_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                    return
                key, ms = _prewarm_model(db, params)
                print(f"[prewarm] filter_ctx_model key={key} ms={ms:.2f}")
        if _should_stop(started, max_sec):
            print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
            return
        total = service.total_cars()
        redis_set_json(build_total_cars_key(), total, ttl_sec=600)
        print(f"[prewarm] total_cars={total}")
        count_keys = [
            {"region": "EU"},
            {"region": "KR"},
        ]
        for c in eu_countries:
            count_keys.append({"region": "EU", "country": c})
        for brand in hot_brands:
            count_keys.append({"region": "EU", "country": eu_country, "brand": brand})
        for params in count_keys:
            if _should_stop(started, max_sec):
                print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                return
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
                sort=params.get("sort"),
                page=1,
                page_size=int(params.get("page_size") or 12),
            )

        list_tasks = [
            {"region": "EU"},
            {"region": "KR"},
            {"region": "EU", "country": eu_country},
            {"region": "EU", "country": "AT"},
        ]
        for params in list_tasks:
            if _should_stop(started, max_sec):
                print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                return
            params["sort"] = list_sort
            params["page_size"] = list_page_size
            _prewarm_list(params)
            print(f"[prewarm] cars_list region={params.get('region')} country={params.get('country')}")
        kr_sort_tasks = [
            {"region": "KR", "sort": "price_asc"},
            {"region": "KR", "sort": "price_desc"},
        ]
        for params in kr_sort_tasks:
            if _should_stop(started, max_sec):
                print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                return
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
                page_size=max(24, list_page_size),
            )
            print(f"[prewarm] cars_list region=KR sort={params.get('sort')}")
        if include_brand_lists:
            for params in brand_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC (brand lists)")
                    break
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
                    sort=list_sort,
                    page=1,
                    page_size=list_page_size,
                )
                print(
                    f"[prewarm] cars_list brand={params.get('brand')} region={params.get('region')} "
                    f"country={params.get('country')} sort={list_sort} size={list_page_size}"
                )
    print(f"[prewarm] done in {(time.perf_counter()-started)*1000:.2f} ms")


if __name__ == "__main__":
    main()
