import os
import time
from typing import Dict, Any, List, Tuple

from backend.app.db import SessionLocal
from backend.app.services.cars_service import CarsService
from backend.app.routers.pages import _build_filter_context
from backend.app.utils.redis_cache import redis_set_json


def _prewarm_filter(service: CarsService, params: Dict[str, Any], include_payload: bool) -> Tuple[str, float]:
    started = time.perf_counter()
    ctx = _build_filter_context(service, service.db, include_payload=include_payload, params=params)
    cache_key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
        str(params.get("brand") or ""),
        str(params.get("model") or ""),
        str(params.get("color") or ""),
        str(params.get("engine_type") or ""),
        str(params.get("transmission") or ""),
        str(params.get("body_type") or ""),
        str(params.get("drive_type") or ""),
        str(params.get("reg_year") or ""),
        include_payload,
    ) if params else ("home", include_payload)
    redis_set_json(f"filter_ctx:{cache_key}", ctx, ttl_sec=900)
    return str(cache_key), (time.perf_counter() - started) * 1000


def main() -> None:
    started = time.perf_counter()
    if not redis_set_json("prewarm_ping", {"ok": True}, ttl_sec=60):
        print("[prewarm] redis unavailable")
        raise SystemExit(2)
    with SessionLocal() as db:
        service = CarsService(db)
        tasks: List[Tuple[Dict[str, Any], bool]] = [
            ({}, False),
            ({"region": "EU"}, False),
            ({"region": "EU", "country": "DE"}, False),
            ({"region": "KR"}, False),
            ({"region": "EU", "country": "DE"}, True),
            ({"region": "EU"}, True),
            ({"region": "KR"}, True),
        ]
        # optional RU if needed
        if os.getenv("INCLUDE_RU_PREWARM") == "1":
            tasks.append(({"region": "RU"}, False))
            tasks.append(({"region": "RU"}, True))
        for params, include_payload in tasks:
            key, ms = _prewarm_filter(service, params, include_payload)
            print(f"[prewarm] filter_ctx key={key} ms={ms:.2f}")
        total = service.total_cars()
        redis_set_json("total_cars:all", total, ttl_sec=600)
        print(f"[prewarm] total_cars={total}")
    print(f"[prewarm] done in {(time.perf_counter()-started)*1000:.2f} ms")


if __name__ == "__main__":
    main()
