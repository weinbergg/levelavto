from __future__ import annotations

import json
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlencode, urlsplit


HOME_RECOMMENDATION_BLOCKS_CONTENT_KEY = "home_recommendation_blocks"
HOME_RECOMMENDATION_BLOCKS_MAX = 20
HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT = 8
HOME_RECOMMENDATION_BLOCK_LIMIT_MIN = 4
HOME_RECOMMENDATION_BLOCK_LIMIT_MAX = 12

_BLOCK_QUERY_MULTI_KEYS = {"line", "source"}
_BLOCK_QUERY_ALLOWED_SORTS = {
    "price_asc",
    "price_desc",
    "year_desc",
    "year_asc",
    "mileage_asc",
    "mileage_desc",
    "reg_desc",
    "reg_asc",
    "listing_desc",
    "listing_asc",
}
_BLOCK_QUERY_KEYS: List[str] = [
    "region",
    "country",
    "kr_type",
    "brand",
    "model",
    "generation",
    "q",
    "line",
    "source",
    "color",
    "engine_type",
    "transmission",
    "body_type",
    "drive_type",
    "price_min",
    "price_max",
    "power_hp_min",
    "power_hp_max",
    "engine_cc_min",
    "engine_cc_max",
    "year_min",
    "year_max",
    "mileage_min",
    "mileage_max",
    "reg_year_min",
    "reg_month_min",
    "reg_year_max",
    "reg_month_max",
    "condition",
    "num_seats",
    "doors_count",
    "emission_class",
    "efficiency_class",
    "climatisation",
    "airbags",
    "interior_design",
    "interior_color",
    "interior_material",
    "vat_reclaimable",
    "air_suspension",
    "price_rating_label",
    "owners_count",
    "sort",
    "hide_no_local_photo",
]


def _trim_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_limit(value: Any) -> int:
    try:
        limit = int(str(value or "").strip() or HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT)
    except Exception:
        limit = HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT
    return max(HOME_RECOMMENDATION_BLOCK_LIMIT_MIN, min(HOME_RECOMMENDATION_BLOCK_LIMIT_MAX, limit))


def _coerce_enabled(value: Any) -> bool:
    raw = _trim_text(value).lower()
    return raw not in {"", "0", "false", "off", "no"}


def _extract_query_pairs(value: Any) -> List[tuple[str, str]]:
    raw = _trim_text(value)
    if not raw:
        return []
    if raw.startswith("?"):
        raw = raw[1:]
    else:
        parts = urlsplit(raw)
        if parts.query:
            raw = parts.query
    return [(str(key or "").strip(), str(val or "").strip()) for key, val in parse_qsl(raw, keep_blank_values=False)]


def normalize_block_query(value: Any) -> str:
    pairs = _extract_query_pairs(value)
    if not pairs:
        return ""

    bucket: Dict[str, List[str]] = {key: [] for key in _BLOCK_QUERY_KEYS}
    for raw_key, raw_val in pairs:
        key = raw_key.strip()
        if key not in bucket:
            continue
        val = raw_val.strip()
        if not val:
            continue
        if key in {"region", "country", "kr_type"}:
            val = val.upper()
        elif key == "sort":
            val = val.lower()
            if val not in _BLOCK_QUERY_ALLOWED_SORTS:
                continue
        if key in _BLOCK_QUERY_MULTI_KEYS:
            bucket[key].append(val)
        else:
            bucket[key] = [val]

    has_scope = any(bucket[key] for key in ("region", "country", "kr_type"))
    if not has_scope:
        bucket["region"] = ["EU"]
    if not bucket["sort"]:
        bucket["sort"] = ["price_asc"]

    normalized_pairs: List[tuple[str, str]] = []
    for key in _BLOCK_QUERY_KEYS:
        values = bucket.get(key) or []
        if not values:
            continue
        if key in _BLOCK_QUERY_MULTI_KEYS:
            for item in values:
                normalized_pairs.append((key, item))
            continue
        normalized_pairs.append((key, values[-1]))
    return urlencode(normalized_pairs, doseq=True)


def load_home_recommendation_blocks(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return []
        return load_home_recommendation_blocks(parsed)
    if not isinstance(raw, list):
        return []

    blocks: List[Dict[str, Any]] = []
    for index, item in enumerate(raw[:HOME_RECOMMENDATION_BLOCKS_MAX], start=1):
        if not isinstance(item, dict):
            continue
        query = normalize_block_query(item.get("query"))
        title = _trim_text(item.get("title")) or f"Подборка {index}"
        if not query:
            continue
        blocks.append(
            {
                "title": title[:120],
                "query": query,
                "limit": _coerce_limit(item.get("limit")),
                "enabled": _coerce_enabled(item.get("enabled", True)),
            }
        )
    return blocks


def build_home_recommendation_blocks(
    titles: List[Any],
    queries: List[Any],
    limits: List[Any],
    enabled_flags: List[Any],
) -> List[Dict[str, Any]]:
    rows = max(len(titles), len(queries), len(limits), len(enabled_flags))
    blocks: List[Dict[str, Any]] = []
    for index in range(rows):
        title = _trim_text(titles[index]) if index < len(titles) else ""
        query = normalize_block_query(queries[index]) if index < len(queries) else ""
        enabled = _coerce_enabled(enabled_flags[index]) if index < len(enabled_flags) else True
        limit = _coerce_limit(limits[index]) if index < len(limits) else HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT
        if not title and not query:
            continue
        if not query:
            continue
        blocks.append(
            {
                "title": title[:120] or f"Подборка {len(blocks) + 1}",
                "query": query,
                "limit": limit,
                "enabled": enabled,
            }
        )
        if len(blocks) >= HOME_RECOMMENDATION_BLOCKS_MAX:
            break
    return blocks


def serialize_home_recommendation_blocks(blocks: List[Dict[str, Any]]) -> str:
    return json.dumps(load_home_recommendation_blocks(blocks), ensure_ascii=False, indent=2)
