import json
import logging
import os
import time
from typing import Any, Optional, Dict, Tuple

import redis


logger = logging.getLogger(__name__)
_redis_client: Optional[redis.Redis] = None
_redis_disabled_until: float = 0.0
_redis_write_disabled_until: float = 0.0
_redis_write_disabled_reason: Optional[str] = None


def _now() -> float:
    return time.time()


def _mark_redis_disabled(reason: str, seconds: int = 60) -> None:
    global _redis_disabled_until
    _redis_disabled_until = _now() + seconds
    logger.warning("redis disabled for %ss: %s", seconds, reason)


def _mark_redis_write_disabled(reason: str, seconds: int = 300) -> None:
    global _redis_write_disabled_until, _redis_write_disabled_reason
    _redis_write_disabled_until = _now() + seconds
    _redis_write_disabled_reason = reason
    logger.warning("redis write disabled for %ss: %s", seconds, reason)


def get_redis() -> Optional[redis.Redis]:
    global _redis_client, _redis_disabled_until
    if _redis_disabled_until and _redis_disabled_until > _now():
        return None
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    if _redis_client is None:
        try:
            _redis_client = redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.5,
                retry_on_timeout=False,
                health_check_interval=30,
            )
            _redis_client.ping()
        except Exception as exc:
            logger.warning("redis unavailable: %s", exc)
            _mark_redis_disabled(str(exc))
            _redis_client = None
            return None
    return _redis_client


def build_filter_ctx_key(params: Optional[Dict[str, Any]], include_payload: bool) -> str:
    if not params:
        key = ("home", include_payload)
    else:
        key = (
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
        )
    return f"filter_ctx:{key}"


def build_total_cars_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "total_cars:all"
    key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
        str(params.get("brand") or ""),
        str(params.get("model") or ""),
    )
    return f"total_cars:{key}"


def build_cars_count_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "cars_count:all"
    cleaned = normalize_count_params(params)
    items = tuple(sorted((str(k), str(v)) for k, v in cleaned.items()))
    return f"cars_count:{items}"


def build_cars_count_simple_key(region: Optional[str], country: Optional[str], brand: Optional[str]) -> str:
    return "cars_count:{r}:{c}:{b}".format(
        r=region or "all",
        c=country or "all",
        b=brand or "all",
    )


def build_cars_list_key(
    region: Optional[str],
    country: Optional[str],
    brand: Optional[str],
    sort: Optional[str],
    page: int,
    page_size: int,
) -> str:
    return "cars_list:{r}:{c}:{b}:{sort}:{page}:{size}".format(
        r=region or "all",
        c=country or "all",
        b=brand or "all",
        sort=sort or "none",
        page=page,
        size=page_size,
    )


def build_filter_payload_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "filter_payload:all"
    key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
        str(params.get("brand") or ""),
        str(params.get("model") or ""),
    )
    return f"filter_payload:{key}"


def build_filter_ctx_base_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "filter_ctx_base:all"
    key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
    )
    return f"filter_ctx_base:{key}"


def build_filter_ctx_brand_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "filter_ctx_brand:all"
    key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
        str(params.get("kr_type") or ""),
        str(params.get("brand") or ""),
    )
    return f"filter_ctx_brand:{key}"


def build_filter_ctx_model_key(params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return "filter_ctx_model:all"
    key = (
        str(params.get("region") or ""),
        str(params.get("country") or ""),
        str(params.get("kr_type") or ""),
        str(params.get("brand") or ""),
        str(params.get("model") or ""),
    )
    return f"filter_ctx_model:{key}"


def normalize_filter_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not params:
        return {}
    # Backward-compat: map eu_country -> country (read-only alias).
    if not params.get("country") and params.get("eu_country"):
        params = {**params, "country": params.get("eu_country")}
    keys = [
        "region",
        "country",
        "kr_type",
        "brand",
        "model",
        "color",
        "engine_type",
        "transmission",
        "body_type",
        "drive_type",
        "reg_year",
    ]
    cleaned: Dict[str, Any] = {}
    for key in keys:
        val = params.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            val = val.strip()
            if not val:
                continue
            if key in {"region", "country", "kr_type"}:
                val = val.upper()
        cleaned[key] = val
    # Ensure alias is not propagated.
    cleaned.pop("eu_country", None)
    return cleaned


def normalize_count_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not params:
        return {}
    if not params.get("country") and params.get("eu_country"):
        params = {**params, "country": params.get("eu_country")}
    keys = [
        "region",
        "country",
        "brand",
        "model",
        "color",
        "engine_type",
        "transmission",
        "body_type",
        "drive_type",
        "kr_type",
        "price_min",
        "price_max",
        "mileage_min",
        "mileage_max",
        "reg_year_min",
        "reg_year_max",
        "year_min",
        "year_max",
        "engine_cc_min",
        "engine_cc_max",
        "power_hp_min",
        "power_hp_max",
        "condition",
    ]
    cleaned: Dict[str, Any] = {}
    for key in keys:
        val = params.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            val = val.strip()
            if not val:
                continue
            if key in {"region", "country", "kr_type"}:
                val = val.upper()
        cleaned[key] = val
    cleaned.pop("eu_country", None)
    return cleaned


def redis_get_json(key: str) -> Optional[Any]:
    client = get_redis()
    if client is None:
        return None
    try:
        raw = client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.warning("redis get failed: %s", exc)
        return None


def redis_set_json(key: str, value: Any, ttl_sec: int) -> bool:
    global _redis_write_disabled_until, _redis_write_disabled_reason
    if _redis_write_disabled_until and _redis_write_disabled_until > _now():
        logger.warning(
            "redis write skipped (disabled): %s", _redis_write_disabled_reason or "unknown"
        )
        return False
    client = get_redis()
    if client is None:
        return False
    try:
        client.setex(key, ttl_sec, json.dumps(value, ensure_ascii=False))
        return True
    except Exception as exc:
        msg = str(exc)
        if "MISCONF" in msg or "No space left on device" in msg or "ENOSPC" in msg:
            _mark_redis_write_disabled(msg, seconds=300)
        logger.warning("redis set failed: %s", exc)
        return False


def redis_delete_by_pattern(pattern: str) -> int:
    client = get_redis()
    if client is None:
        return 0
    deleted = 0
    try:
        for key in client.scan_iter(match=pattern, count=200):
            try:
                deleted += int(client.delete(key))
            except Exception:
                continue
    except Exception as exc:
        logger.warning("redis scan/delete failed: %s", exc)
    return deleted
