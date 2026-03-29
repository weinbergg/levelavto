from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import math
import re


_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[._+-][a-z0-9]+)?", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_GENERIC_STOPWORDS = {
    "hud",
    "led",
    "laser",
    "pano",
    "panorama",
    "kamera",
    "camera",
    "softclose",
    "pack",
    "paket",
    "line",
    "pro",
    "plus",
    "premium",
    "exclusive",
    "edition",
    "facelift",
}
_IMPORTANT_WORDS = {
    "autobiography",
    "vogue",
    "sv",
    "sport",
    "performance",
    "diesel",
    "petrol",
    "hybrid",
    "phev",
    "electric",
    "quattro",
    "xdrive",
    "4x4",
    "awd",
    "fwd",
    "rwd",
    "tdi",
    "tsi",
    "tfsi",
    "dci",
    "cdi",
    "gpl",
    "lpg",
    "amg",
    "rs",
    "gt",
    "gts",
    "turbo",
}


def normalize_spec_text(value: Any) -> str:
    raw = str(value or "").strip().casefold()
    if not raw:
        return ""
    raw = raw.replace("&", " and ")
    raw = raw.replace("/", " ")
    raw = raw.replace("|", " ")
    raw = raw.replace(",", " ")
    raw = raw.replace("(", " ")
    raw = raw.replace(")", " ")
    raw = raw.replace("[", " ")
    raw = raw.replace("]", " ")
    raw = raw.replace("{", " ")
    raw = raw.replace("}", " ")
    raw = raw.replace("“", " ").replace("”", " ").replace('"', " ")
    raw = raw.replace("'", " ").replace("’", " ")
    raw = _MULTISPACE_RE.sub(" ", raw)
    return raw.strip()


def normalize_engine_type(value: Any) -> str:
    raw = normalize_spec_text(value)
    if not raw:
        return ""
    if "electric" in raw or re.search(r"\bev\b", raw):
        return "electric"
    if "hybrid" in raw or "plug in" in raw or "plug-in" in raw or "phev" in raw:
        return "hybrid"
    if "diesel" in raw:
        return "diesel"
    if "petrol" in raw or "benzin" in raw or "gasoline" in raw or "benzina" in raw:
        return "petrol"
    if "lpg" in raw or re.search(r"\bgpl\b", raw):
        return "lpg"
    return raw


def normalize_body_type(value: Any) -> str:
    return normalize_spec_text(value)


def normalized_power_hp(power_hp: Any, power_kw: Any) -> Optional[float]:
    hp = _to_float(power_hp)
    if hp and hp > 0:
        return round(hp, 1)
    kw = _to_float(power_kw)
    if kw and kw > 0:
        return round(kw * 1.35962, 1)
    return None


def normalized_power_kw(power_hp: Any, power_kw: Any) -> Optional[float]:
    kw = _to_float(power_kw)
    if kw and kw > 0:
        return round(kw, 2)
    hp = _to_float(power_hp)
    if hp and hp > 0:
        return round(hp / 1.35962, 2)
    return None


def has_complete_raw_specs(
    engine_type: Any,
    engine_cc: Any,
    power_hp: Any,
    power_kw: Any,
) -> bool:
    is_electric = normalize_engine_type(engine_type) == "electric"
    cc = _to_int(engine_cc)
    hp = normalized_power_hp(power_hp, power_kw)
    if is_electric:
        return hp is not None
    return cc is not None and hp is not None


def build_variant_key(
    brand: Any,
    model: Any,
    variant: Any = None,
    source_payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    payload = source_payload or {}
    brand_tokens = set(_tokenize(normalize_spec_text(brand)))
    model_tokens = set(_tokenize(normalize_spec_text(model)))
    tokens: list[str] = []
    for raw in (
        variant,
        payload.get("sub_title"),
        payload.get("title"),
    ):
        if not raw:
            continue
        for token in _tokenize(normalize_spec_text(raw)):
            if token in brand_tokens or token in model_tokens:
                continue
            if token in _GENERIC_STOPWORDS:
                continue
            if len(token) == 1:
                continue
            if _is_informative_variant_token(token):
                if token not in tokens:
                    tokens.append(token)
        if tokens:
            break
    if not tokens:
        return None
    return "|".join(tokens[:4])


def build_reference_signature(
    *,
    brand: Any,
    model: Any,
    variant: Any = None,
    engine_type: Any = None,
    body_type: Any = None,
    year: Any = None,
    source_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "brand_norm": normalize_spec_text(brand),
        "model_norm": normalize_spec_text(model),
        "variant_key": build_variant_key(brand, model, variant, source_payload),
        "engine_type_norm": normalize_engine_type(engine_type),
        "body_type_norm": normalize_body_type(body_type),
        "year": _to_int(year),
    }


def choose_reference_consensus(
    candidates: Iterable[Dict[str, Any]],
    *,
    target_year: Optional[int],
    has_variant_key: bool,
) -> Optional[Dict[str, Any]]:
    usable: list[Dict[str, Any]] = []
    tuples: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for item in candidates:
        cc = _to_int(item.get("engine_cc"))
        hp = normalized_power_hp(item.get("power_hp"), item.get("power_kw"))
        kw = normalized_power_kw(item.get("power_hp"), item.get("power_kw"))
        if cc is None and hp is None:
            continue
        year = _to_int(item.get("year"))
        source_car_id = _to_int(item.get("source_car_id"))
        tuple_key = (cc, round(hp or 0, 1) if hp is not None else None, round(kw or 0, 2) if kw is not None else None)
        distance = 999 if target_year is None or year is None else abs(year - target_year)
        usable.append(
            {
                "tuple_key": tuple_key,
                "year": year,
                "distance": distance,
                "source_car_id": source_car_id,
                "engine_cc": cc,
                "power_hp": hp,
                "power_kw": kw,
            }
        )
        bucket = tuples.get(tuple_key)
        if bucket is None:
            tuples[tuple_key] = {"count": 1}
        else:
            bucket["count"] += 1
    if not usable or len(tuples) != 1:
        return None

    best = sorted(
        usable,
        key=lambda item: (
            item["distance"],
            0 if target_year is not None and item["year"] == target_year else 1,
            -(item["source_car_id"] or 0),
        ),
    )[0]
    support_count = int(tuples.get(best["tuple_key"], {}).get("count") or 0)
    if has_variant_key and target_year is not None and best["year"] == target_year:
        confidence = "high"
        rule = "variant_exact_year_exact"
    elif has_variant_key:
        confidence = "medium"
        rule = "variant_exact_year_window"
    elif target_year is not None and best["year"] == target_year and support_count >= 2:
        confidence = "medium"
        rule = "model_exact_year_exact_consensus"
    else:
        return None
    return {
        "engine_cc": best["engine_cc"],
        "power_hp": best["power_hp"],
        "power_kw": best["power_kw"],
        "source_car_id": best["source_car_id"],
        "confidence": confidence,
        "rule": rule,
        "support_count": support_count,
    }


def _tokenize(value: str) -> list[str]:
    if not value:
        return []
    return [token for token in _TOKEN_RE.findall(value) if token]


def _is_informative_variant_token(token: str) -> bool:
    if any(ch.isdigit() for ch in token) and any(ch.isalpha() for ch in token):
        return True
    return token in _IMPORTANT_WORDS


def _to_int(value: Any) -> Optional[int]:
    if value in (None, "", 0):
        return None
    try:
        num = int(float(value))
    except Exception:
        return None
    return num if num > 0 else None


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", 0):
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num) or num <= 0:
        return None
    return num
