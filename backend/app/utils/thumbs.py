import os
import re
import time
from typing import Optional

import requests

from .redis_cache import get_redis


_RULE_CANDIDATES = ("mo-240.jpg", "mo-360.jpg", "mo-640.jpg")
_RULE_RE = re.compile(r"(rule=)mo-\d+(?:\.jpg)?", re.IGNORECASE)
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _now() -> float:
    return time.time()


def normalize_classistatic_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    raw = url.strip()
    if not raw:
        return None
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("http://"):
        return raw.replace("http://", "https://", 1)
    if raw.startswith("https://"):
        return raw
    if raw.startswith("/api/v1/mo-prod/images/"):
        return f"https://img.classistatic.de{raw}"
    if raw.startswith("api/v1/mo-prod/images/"):
        base = f"https://img.classistatic.de/{raw}"
        return _ensure_rule(base)
    if raw.startswith("img.classistatic.de/"):
        base = f"https://{raw}"
        return _ensure_rule(base)
    if _UUID_RE.match(raw):
        prefix = raw[:2]
        base = f"https://img.classistatic.de/api/v1/mo-prod/images/{prefix}/{raw}"
        return _ensure_rule(base)
    return None


def _ensure_rule(url: str) -> str:
    if "rule=mo-" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}rule=mo-1024.jpg"


def _extract_uuid(url: str) -> Optional[str]:
    base = url.split("?", 1)[0]
    name = base.rsplit("/", 1)[-1]
    if "-" in name and len(name) >= 32:
        return name
    return None


def _apply_rule(url: str, rule: str) -> str:
    if "rule=mo-" in url:
        return _RULE_RE.sub(rf"\1{rule}", url)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}rule={rule}"


def _probe_url(url: str, timeout: tuple[float, float]) -> bool:
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return True
        if resp.status_code in (403, 405):
            resp = requests.get(url, timeout=timeout, stream=True)
            try:
                return resp.status_code == 200
            finally:
                resp.close()
    except Exception:
        return False
    return False


def pick_classistatic_thumb(url: Optional[str], *, ttl_days: int = 14) -> Optional[str]:
    normalized = normalize_classistatic_url(url)
    if not normalized:
        return None
    if "img.classistatic.de" not in normalized or "rule=mo-" not in normalized:
        return normalized
    key_id = _extract_uuid(normalized)
    cache_key = f"img_rule:{key_id}" if key_id else None
    client = get_redis()
    if client and cache_key:
        cached = client.get(cache_key)
        if cached:
            if cached == "none":
                return None
            return _apply_rule(normalized, cached)
    timeout = (0.25, 0.6)
    for rule in _RULE_CANDIDATES:
        candidate = _apply_rule(normalized, rule)
        if _probe_url(candidate, timeout):
            if client and cache_key:
                client.setex(cache_key, ttl_days * 86400, rule)
            return candidate
    # All known rules failed: cache negative result and let caller fallback to placeholder.
    if client and cache_key:
        client.setex(cache_key, max(3600, ttl_days * 86400 // 4), "none")
    return None
