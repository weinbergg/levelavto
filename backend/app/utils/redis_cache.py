import json
import logging
import os
from typing import Any, Optional

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
            _redis_client = redis.Redis(
                from_url=url,
                decode_responses=True,
                socket_connect_timeout=0.3,
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
