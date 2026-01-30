import os
import time
from typing import Dict, Any, List, Tuple

from backend.app.db import SessionLocal
from backend.app.services.cars_service import CarsService
from backend.app.routers.catalog import filter_ctx_base, filter_ctx_brand, filter_ctx_model, list_cars, cars_count
from backend.app.utils.redis_cache import (
    redis_set_json,
    build_total_cars_key,
    build_cars_count_key,
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
            key, ms = _prewarm_base(db, params)
            print(f"[prewarm] filter_ctx_base key={key} ms={ms:.2f}")
        brand_tasks = [
            {"region": "EU", "country": "DE", "brand": "BMW"},
            {"region": "EU", "country": "DE", "brand": "Mercedes-Benz"},
            {"region": "EU", "country": "AT", "brand": "Cadillac"},
        ]
        for params in brand_tasks:
            key, ms = _prewarm_brand(db, params)
            print(f"[prewarm] filter_ctx_brand key={key} ms={ms:.2f}")
        model_tasks = [
            {"region": "EU", "country": "DE", "brand": "BMW", "model": "X5"},
        ]
        for params in model_tasks:
            key, ms = _prewarm_model(db, params)
            print(f"[prewarm] filter_ctx_model key={key} ms={ms:.2f}")
        total = service.total_cars()
        redis_set_json(build_total_cars_key(), total, ttl_sec=600)
        print(f"[prewarm] total_cars={total}")
        count_keys = [
            {"region": "EU"},
            {"region": "KR"},
            {"region": "EU", "country": "DE"},
            {"region": "EU", "country": "AT"},
        ]
        for params in count_keys:
            normalized = normalize_count_params(params)
            count = cars_count(None, db=db, **normalized)
            print(f"[prewarm] cars_count key={build_cars_count_key(normalized)} value={count.get('count')}")

        list_tasks = [
            {"region": "EU"},
            {"region": "KR"},
            {"region": "EU", "country": "DE"},
            {"region": "EU", "country": "AT"},
        ]
        for params in list_tasks:
            list_cars(
                None,
                db=db,
                page=1,
                page_size=12,
                sort=None,
                **params,
            )
            print(f"[prewarm] cars_list region={params.get('region')} country={params.get('country')}")
        for params in brand_tasks:
            list_cars(
                None,
                db=db,
                page=1,
                page_size=12,
                sort=None,
                **params,
            )
            print(f"[prewarm] cars_list brand={params.get('brand')} region={params.get('region')} country={params.get('country')}")
    print(f"[prewarm] done in {(time.perf_counter()-started)*1000:.2f} ms")


if __name__ == "__main__":
    main()
