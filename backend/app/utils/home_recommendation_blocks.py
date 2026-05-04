from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit


HOME_RECOMMENDATION_BLOCKS_CONTENT_KEY = "home_recommendation_blocks"
HOME_RECOMMENDATION_BLOCKS_MAX = 20
HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT = 8
HOME_RECOMMENDATION_BLOCK_LIMIT_MIN = 4
HOME_RECOMMENDATION_BLOCK_LIMIT_MAX = 12

_LEGACY_QUERY_MULTI_KEYS = {"line", "source"}
_LEGACY_QUERY_KEYS: List[str] = [
    "region",
    "country",
    "kr_type",
    "brand",
    "model",
    "color",
    "price_min",
    "price_max",
    "mileage_max",
    "reg_year_min",
    "reg_year_max",
    "power_hp_max",
    "engine_cc_max",
    "line",
]


def _trim_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_limit(value: Any) -> int:
    try:
        limit = int(_trim_text(value) or HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT)
    except Exception:
        limit = HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT
    return max(HOME_RECOMMENDATION_BLOCK_LIMIT_MIN, min(HOME_RECOMMENDATION_BLOCK_LIMIT_MAX, limit))


def _coerce_enabled(value: Any) -> bool:
    raw = _trim_text(value).lower()
    return raw not in {"", "0", "false", "off", "no"}


def _coerce_int(value: Any) -> Optional[int]:
    raw = _trim_text(value)
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    raw = _trim_text(value)
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _parse_lines(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = str(value).splitlines()
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        parts = [part.strip() for part in text.split("|")]
        while len(parts) < 3:
            parts.append("")
        normalized = "|".join(parts[:3])
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _parse_filter_tokens(value: Any) -> List[str]:
    """Free-form tokens from textarea (comma, semicolon or newline separated)."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_items = [str(x).strip() for x in value if str(x or "").strip()]
    else:
        raw_items = []
        for piece in re.split(r"[\n,;]+", str(value)):
            t = piece.strip()
            if t:
                raw_items.append(t)
    seen: set[str] = set()
    out: List[str] = []
    for t in raw_items:
        key = t.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def _parse_car_ids(value: Any) -> List[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        tokens = value
    else:
        tokens = str(value).replace("\n", ",").split(",")
    out: List[int] = []
    seen: set[int] = set()
    for token in tokens:
        raw = str(token or "").strip()
        if not raw:
            continue
        try:
            car_id = int(raw)
        except Exception:
            continue
        if car_id <= 0 or car_id in seen:
            continue
        seen.add(car_id)
        out.append(car_id)
    return out


def _extract_legacy_query_pairs(value: Any) -> Dict[str, List[str]]:
    raw = _trim_text(value)
    if not raw:
        return {}
    if raw.startswith("?"):
        raw = raw[1:]
    else:
        parts = urlsplit(raw)
        if parts.query:
            raw = parts.query
    if not raw:
        return {}
    pairs = parse_qsl(raw, keep_blank_values=False)
    bucket: Dict[str, List[str]] = {key: [] for key in _LEGACY_QUERY_KEYS}
    for raw_key, raw_val in pairs:
        key = str(raw_key or "").strip()
        if key not in bucket:
            continue
        val = str(raw_val or "").strip()
        if not val:
            continue
        if key in {"region", "country", "kr_type"}:
            val = val.upper()
        if key in _LEGACY_QUERY_MULTI_KEYS:
            bucket[key].append(val)
        else:
            bucket[key] = [val]
    return bucket


def _lines_from_legacy_query(query: Any) -> List[str]:
    bucket = _extract_legacy_query_pairs(query)
    if not bucket:
        return []
    lines = _parse_lines(bucket.get("line") or [])
    if lines:
        return lines
    brand = (bucket.get("brand") or [None])[-1]
    model = (bucket.get("model") or [None])[-1]
    if brand or model:
        return _parse_lines([f"{brand or ''}|{model or ''}|"])
    return []


def _build_block_dict(
    *,
    title: Any,
    limit: Any,
    enabled: Any,
    lines_value: Any,
    models_value: Any,
    colors_value: Any,
    price_min: Any,
    price_max: Any,
    mileage_max: Any,
    reg_year_min: Any,
    reg_year_max: Any,
    power_hp_max: Any,
    engine_cc_max: Any,
    car_ids_value: Any,
) -> Optional[Dict[str, Any]]:
    lines = _parse_lines(lines_value)
    models = _parse_filter_tokens(models_value)
    colors = _parse_filter_tokens(colors_value)
    car_ids = _parse_car_ids(car_ids_value)
    block_title = _trim_text(title)
    if not block_title and not lines and not car_ids and not models and not colors:
        return None
    if not lines and not car_ids and not models and not colors:
        return None
    cleaned = {
        "title": (block_title or "Подборка")[:120],
        "limit": _coerce_limit(limit),
        "enabled": _coerce_enabled(enabled),
        "lines": lines,
        "models": models,
        "colors": colors,
        "price_min": _coerce_float(price_min),
        "price_max": _coerce_float(price_max),
        "mileage_max": _coerce_int(mileage_max),
        "reg_year_min": _coerce_int(reg_year_min),
        "reg_year_max": _coerce_int(reg_year_max),
        "power_hp_max": _coerce_float(power_hp_max),
        "engine_cc_max": _coerce_int(engine_cc_max),
        "car_ids": car_ids,
    }
    cleaned["lines_text"] = "\n".join(cleaned["lines"])
    cleaned["models_text"] = "\n".join(cleaned["models"])
    cleaned["colors_text"] = "\n".join(cleaned["colors"])
    cleaned["car_ids_text"] = ", ".join(str(car_id) for car_id in cleaned["car_ids"])
    return cleaned


def _block_from_legacy_query(item: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    bucket = _extract_legacy_query_pairs(item.get("query"))
    if not bucket:
        return None
    return _build_block_dict(
        title=item.get("title") or f"Подборка {index}",
        limit=item.get("limit"),
        enabled=item.get("enabled", True),
        lines_value=_lines_from_legacy_query(item.get("query")),
        models_value=bucket.get("model"),
        colors_value=bucket.get("color"),
        price_min=(bucket.get("price_min") or [None])[-1],
        price_max=(bucket.get("price_max") or [None])[-1],
        mileage_max=(bucket.get("mileage_max") or [None])[-1],
        reg_year_min=(bucket.get("reg_year_min") or [None])[-1],
        reg_year_max=(bucket.get("reg_year_max") or [None])[-1],
        power_hp_max=(bucket.get("power_hp_max") or [None])[-1],
        engine_cc_max=(bucket.get("engine_cc_max") or [None])[-1],
        car_ids_value=item.get("car_ids"),
    )


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
        if "query" in item:
            block = _block_from_legacy_query(item, index)
        else:
            block = _build_block_dict(
                title=item.get("title") or f"Подборка {index}",
                limit=item.get("limit"),
                enabled=item.get("enabled", True),
                lines_value=item.get("lines"),
                models_value=item.get("models"),
                colors_value=item.get("colors"),
                price_min=item.get("price_min"),
                price_max=item.get("price_max"),
                mileage_max=item.get("mileage_max"),
                reg_year_min=item.get("reg_year_min"),
                reg_year_max=item.get("reg_year_max"),
                power_hp_max=item.get("power_hp_max"),
                engine_cc_max=item.get("engine_cc_max"),
                car_ids_value=item.get("car_ids"),
            )
        if block is None:
            continue
        blocks.append(block)
    return blocks


def build_home_recommendation_blocks(
    titles: List[Any],
    limits: List[Any],
    enabled_flags: List[Any],
    lines_values: List[Any],
    models_values: List[Any],
    colors_values: List[Any],
    price_mins: List[Any],
    price_maxs: List[Any],
    mileage_maxs: List[Any],
    reg_year_mins: List[Any],
    reg_year_maxs: List[Any],
    power_hp_maxs: List[Any],
    engine_cc_maxs: List[Any],
    car_ids_values: List[Any],
) -> List[Dict[str, Any]]:
    rows = max(
        len(titles),
        len(limits),
        len(enabled_flags),
        len(lines_values),
        len(models_values),
        len(colors_values),
        len(price_mins),
        len(price_maxs),
        len(mileage_maxs),
        len(reg_year_mins),
        len(reg_year_maxs),
        len(power_hp_maxs),
        len(engine_cc_maxs),
        len(car_ids_values),
    )
    blocks: List[Dict[str, Any]] = []
    for index in range(rows):
        block = _build_block_dict(
            title=titles[index] if index < len(titles) else "",
            limit=limits[index] if index < len(limits) else HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT,
            enabled=enabled_flags[index] if index < len(enabled_flags) else True,
            lines_value=lines_values[index] if index < len(lines_values) else "",
            models_value=models_values[index] if index < len(models_values) else "",
            colors_value=colors_values[index] if index < len(colors_values) else "",
            price_min=price_mins[index] if index < len(price_mins) else "",
            price_max=price_maxs[index] if index < len(price_maxs) else "",
            mileage_max=mileage_maxs[index] if index < len(mileage_maxs) else "",
            reg_year_min=reg_year_mins[index] if index < len(reg_year_mins) else "",
            reg_year_max=reg_year_maxs[index] if index < len(reg_year_maxs) else "",
            power_hp_max=power_hp_maxs[index] if index < len(power_hp_maxs) else "",
            engine_cc_max=engine_cc_maxs[index] if index < len(engine_cc_maxs) else "",
            car_ids_value=car_ids_values[index] if index < len(car_ids_values) else "",
        )
        if block is None:
            continue
        blocks.append(block)
        if len(blocks) >= HOME_RECOMMENDATION_BLOCKS_MAX:
            break
    return blocks


def serialize_home_recommendation_blocks(blocks: List[Dict[str, Any]]) -> str:
    normalized = []
    for block in load_home_recommendation_blocks(blocks):
        normalized.append(
            {
                "title": block["title"],
                "limit": block["limit"],
                "enabled": block["enabled"],
                "lines": block["lines"],
                "models": block["models"],
                "colors": block["colors"],
                "price_min": block["price_min"],
                "price_max": block["price_max"],
                "mileage_max": block["mileage_max"],
                "reg_year_min": block["reg_year_min"],
                "reg_year_max": block["reg_year_max"],
                "power_hp_max": block["power_hp_max"],
                "engine_cc_max": block["engine_cc_max"],
                "car_ids": block["car_ids"],
            }
        )
    return json.dumps(normalized, ensure_ascii=False, indent=2)


def build_block_catalog_query(block: Dict[str, Any]) -> str:
    params: List[tuple[str, str]] = [("region", "EU")]
    for line in block.get("lines") or []:
        params.append(("line", str(line)))
    scalar_map = {
        "price_min": block.get("price_min"),
        "price_max": block.get("price_max"),
        "mileage_max": block.get("mileage_max"),
        "reg_year_min": block.get("reg_year_min"),
        "reg_year_max": block.get("reg_year_max"),
        "power_hp_max": block.get("power_hp_max"),
        "engine_cc_max": block.get("engine_cc_max"),
    }
    for key, value in scalar_map.items():
        if value in (None, "", []):
            continue
        params.append((key, str(value)))
    models = block.get("models") or []
    if models:
        params.append(("model", ",".join(str(m) for m in models)))
    colors = block.get("colors") or []
    if colors:
        params.append(("color", ",".join(str(c) for c in colors)))
    params.append(("sort", "price_asc"))
    return urlencode(params, doseq=True)
