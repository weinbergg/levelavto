import inspect
import os
import time
from typing import Dict, Any, List, Tuple

from starlette.datastructures import QueryParams

from backend.app.db import SessionLocal
from backend.app.services.cars_service import CarsService, normalize_brand
from backend.app.routers.catalog import (
    filter_ctx_base,
    filter_ctx_brand,
    filter_ctx_model,
    filter_payload,
    list_cars,
)
from backend.app.utils.redis_cache import (
    redis_set_json,
    build_total_cars_key,
    build_cars_count_simple_key,
    normalize_filter_params,
    normalize_count_params,
)


def _param_default(param: inspect.Parameter) -> Any:
    default = param.default
    if default is inspect._empty:
        return None
    query_default = getattr(default, "default", inspect._empty)
    if query_default is not inspect._empty:
        return query_default
    return default


def _call_route_with_defaults(fn, /, **overrides):
    kwargs: Dict[str, Any] = {}
    for name, param in inspect.signature(fn).parameters.items():
        if name in overrides:
            kwargs[name] = overrides[name]
            continue
        kwargs[name] = _param_default(param)
    return fn(**kwargs)


def _prewarm_base(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    _call_route_with_defaults(
        filter_ctx_base,
        request=None,
        region=normalized.get("region"),
        country=normalized.get("country"),
        db=db,
    )
    return f"filter_ctx_base:{normalized}", (time.perf_counter() - started) * 1000


def _prewarm_brand(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    _call_route_with_defaults(
        filter_ctx_brand,
        request=None,
        region=normalized.get("region"),
        country=normalized.get("country"),
        kr_type=normalized.get("kr_type"),
        brand=normalized.get("brand"),
        db=db,
    )
    return f"filter_ctx_brand:{normalized}", (time.perf_counter() - started) * 1000


def _prewarm_model(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_filter_params(params)
    _call_route_with_defaults(
        filter_ctx_model,
        request=None,
        region=normalized.get("region"),
        country=normalized.get("country"),
        brand=normalized.get("brand"),
        model=normalized.get("model"),
        db=db,
    )
    return f"filter_ctx_model:{normalized}", (time.perf_counter() - started) * 1000


class _QueryRequest:
    def __init__(self, params: Dict[str, Any]) -> None:
        flat: List[tuple[str, str]] = []
        for key, raw in params.items():
            if raw in (None, ""):
                continue
            if isinstance(raw, list):
                for item in raw:
                    if item in (None, ""):
                        continue
                    flat.append((str(key), str(item)))
                continue
            flat.append((str(key), str(raw)))
        self.query_params = QueryParams(flat)


def _prewarm_payload(db, params: Dict[str, Any]) -> Tuple[str, float]:
    started = time.perf_counter()
    normalized = normalize_count_params(params)
    filter_payload(request=_QueryRequest(normalized), db=db)
    return f"filter_payload:{normalized}", (time.perf_counter() - started) * 1000


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
    include_payload_ctx = os.getenv("PREWARM_INCLUDE_PAYLOAD", "1") != "0"
    include_brand_lists = os.getenv("PREWARM_INCLUDE_BRAND_LISTS", "1") != "0"
    include_brand_counts = os.getenv("PREWARM_INCLUDE_BRAND_COUNTS", "1") != "0"
    include_engine_lists = os.getenv("PREWARM_INCLUDE_ENGINE_LISTS", "1") != "0"
    include_country_sweep = os.getenv("PREWARM_COUNTRY_SWEEP", "0") == "1"
    list_sort = os.getenv("PREWARM_LIST_SORT", "price_asc")
    list_sorts_raw = os.getenv("PREWARM_LIST_SORTS", "").strip()
    list_page_size = int(os.getenv("PREWARM_LIST_PAGE_SIZE", "12") or 12)
    eu_country = os.getenv("PREWARM_EU_COUNTRY", "DE")
    engine_types_raw = os.getenv("PREWARM_ENGINE_TYPES", "diesel,electric,hybrid").strip()
    brand_regions_raw = os.getenv("PREWARM_BRAND_REGIONS", "EU")
    default_hot_brands = ",".join(
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
    )
    hot_brands_raw = os.getenv("PREWARM_HOT_BRANDS")
    if not hot_brands_raw:
        hot_brands_raw = default_hot_brands
    country_sweep_raw = os.getenv("PREWARM_COUNTRIES", "AT,NL,BE,FR,IT,ES")
    country_sweep = [c.strip().upper() for c in country_sweep_raw.split(",") if c.strip()]
    list_sorts = [s.strip() for s in list_sorts_raw.split(",") if s.strip()] or [list_sort]
    engine_types = [s.strip().lower() for s in engine_types_raw.split(",") if s.strip()]
    brand_regions = []
    for item in brand_regions_raw.split(","):
        region = item.strip().upper()
        if region and region not in brand_regions:
            brand_regions.append(region)
    if not brand_regions:
        brand_regions = ["EU"]
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
        if include_payload_ctx:
            payload_tasks: List[Dict[str, Any]] = [
                {"region": "EU"},
                {"region": "EU", "country": eu_country},
                {"region": "KR"},
            ]
            for params in payload_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                    return
                key, ms = _prewarm_payload(db, params)
                print(f"[prewarm] filter_payload key={key} ms={ms:.2f}")
        priority_countries = [eu_country]
        brand_tasks: List[Dict[str, Any]] = []
        for region_name in brand_regions:
            for brand in hot_brands:
                task: Dict[str, Any] = {"brand": brand}
                if region_name in {"EU", "KR"}:
                    task["region"] = region_name
                if region_name == "EU":
                    task["country"] = eu_country
                brand_tasks.append(task)
        if include_brand_ctx:
            for params in brand_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                    return
                key, ms = _prewarm_brand(db, params)
                print(f"[prewarm] filter_ctx_brand key={key} ms={ms:.2f}")
        model_tasks = [
            {"region": "EU", "country": eu_country, "brand": "BMW", "model": "X5"},
            {"region": "KR", "brand": "BMW", "model": "X5"},
        ]
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
        for c in priority_countries:
            count_keys.append({"region": "EU", "country": c})
        if include_country_sweep:
            for c in country_sweep:
                if c not in priority_countries:
                    count_keys.append({"region": "EU", "country": c})
        if include_brand_counts:
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
            for sort_name in list_sorts:
                _call_route_with_defaults(
                    list_cars,
                    request=None,
                    db=db,
                    region=params.get("region"),
                    country=params.get("country"),
                    brand=params.get("brand"),
                    engine_type=params.get("engine_type"),
                    sort=sort_name,
                    page=1,
                    page_size=int(params.get("page_size") or 12),
                )
                print(
                    f"[prewarm] cars_list region={params.get('region')} country={params.get('country')} "
                    f"brand={params.get('brand')} engine_type={params.get('engine_type')} "
                    f"sort={sort_name} size={params.get('page_size') or 12}"
                )

        # Priority: warm broad generic pages first so the main catalog is hot
        # even if brand prewarming is cut short by PREWARM_MAX_SEC.
        list_tasks = [
            {"region": "EU"},
            {"region": "KR"},
            {"region": "EU", "country": eu_country},
        ]
        for params in list_tasks:
            if _should_stop(started, max_sec):
                print("[prewarm] stop by PREWARM_MAX_SEC", flush=True)
                return
            params["page_size"] = list_page_size
            _prewarm_list(params)
        if include_engine_lists:
            engine_list_tasks = []
            for engine_type in engine_types:
                engine_list_tasks.append({"region": "EU", "country": eu_country, "engine_type": engine_type})
            for params in engine_list_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC (engine lists)")
                    break
                params["page_size"] = list_page_size
                _prewarm_list(params)
        if include_brand_lists:
            for params in brand_tasks:
                if _should_stop(started, max_sec):
                    print("[prewarm] stop by PREWARM_MAX_SEC (brand lists)")
                    break
                params["page_size"] = list_page_size
                _prewarm_list(params)
    print(f"[prewarm] done in {(time.monotonic()-started)*1000:.2f} ms")


if __name__ == "__main__":
    main()
