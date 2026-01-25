import json
import logging
import os
from typing import Any, Optional, Dict, Tuple

import redis


logger = logging.getLogger(__name__)
_redis_client: Optional[redis.Redis] = None
_redis_disabled = False


def get_redis() -> Optional[redis.Redis]:
    global _redis_client, _redis_disabled
    if _redis_disabled:
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
            _redis_disabled = True
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
    client = get_redis()
    if client is None:
        return False
    try:
        client.setex(key, ttl_sec, json.dumps(value, ensure_ascii=False))
        return True
    except Exception as exc:
        logger.warning("redis set failed: %s", exc)
        return False
