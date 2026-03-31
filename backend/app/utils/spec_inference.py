from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import math
import re


_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[._+-][a-z0-9]+)?", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_ENGINE_CC_RE = re.compile(r"\b([1-9][0-9]{3,4})\s*(?:cc|ccm|cm3|cm³)\b", re.IGNORECASE)
_ENGINE_LITER_RE = re.compile(
    r"\b([0-9](?:[.,][0-9])?)\s*(?:l|liter|litre)\b",
    re.IGNORECASE,
)
_ENGINE_LITER_CONTEXT_RE = re.compile(
    r"\b([0-9](?:[.,][0-9])?)\s*(?:"
    r"turbo|hybrid|diesel|petrol|benzin|benzina|gasoline|engine|motor|"
    r"v[0-9]|i[0-9]|tdi|tsi|tfsi|fsi|dci|cdi|mjt|multijet|ecoboost|tce"
    r")\b",
    re.IGNORECASE,
)
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
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", raw):
        return ""
    if (
        "based on" in raw
        or "emission" in raw
        or "co2" in raw
        or "co₂" in raw
        or "consumption" in raw
        or "combined" in raw
    ):
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
    return ""


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


def filter_candidates_by_target_power(
    candidates: Iterable[Dict[str, Any]],
    target_power_hp: Any,
) -> list[Dict[str, Any]]:
    target_hp = _to_float(target_power_hp)
    if target_hp is None or target_hp <= 0:
        return list(candidates)
    tolerance = max(12.0, target_hp * 0.02)
    matched: list[Dict[str, Any]] = []
    for item in candidates:
        hp = normalized_power_hp(item.get("power_hp"), item.get("power_kw"))
        if hp is None:
            continue
        if abs(hp - target_hp) <= tolerance:
            matched.append(item)
    return matched


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


def variant_primary_token(value: Any) -> Optional[str]:
    raw = str(value or "").strip().casefold()
    if not raw:
        return None
    token = raw.split("|", 1)[0] if "|" in raw else raw
    token = normalize_spec_text(token)
    return token or None


def choose_reference_consensus(
    candidates: Iterable[Dict[str, Any]],
    *,
    target_year: Optional[int],
    has_variant_key: bool,
    need_engine_cc: bool = True,
    need_power: bool = True,
) -> Optional[Dict[str, Any]]:
    usable: list[Dict[str, Any]] = []
    tuples: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for item in candidates:
        cc = _to_int(item.get("engine_cc"))
        hp = normalized_power_hp(item.get("power_hp"), item.get("power_kw"))
        kw = normalized_power_kw(item.get("power_hp"), item.get("power_kw"))
        if need_engine_cc and cc is None:
            continue
        if need_power and hp is None and kw is None:
            continue
        if not need_engine_cc and not need_power:
            continue
        year = _to_int(item.get("year"))
        source_car_id = _to_int(item.get("source_car_id"))
        tuple_key = (
            cc if need_engine_cc else "__skip_cc__",
            round(hp or 0, 1) if need_power and hp is not None else ("__skip_hp__" if not need_power else None),
            round(kw or 0, 2) if need_power and kw is not None else ("__skip_kw__" if not need_power else None),
        )
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
    if not usable:
        return None
    second_count = 0
    if need_engine_cc and need_power:
        if len(tuples) != 1:
            return None
        selected_tuple_key = next(iter(tuples))
    else:
        ranked_tuples = sorted(
            tuples.items(),
            key=lambda item: (
                -(int(item[1].get("count") or 0)),
                item[0],
            ),
        )
        selected_tuple_key, selected_meta = ranked_tuples[0]
        support_count = int(selected_meta.get("count") or 0)
        second_count = int(ranked_tuples[1][1].get("count") or 0) if len(ranked_tuples) > 1 else 0
        if support_count < 2 or support_count <= second_count:
            return None
    best_candidates = [item for item in usable if item["tuple_key"] == selected_tuple_key]
    best = sorted(
        best_candidates,
        key=lambda item: (
            item["distance"],
            0 if target_year is not None and item["year"] == target_year else 1,
            -(item["source_car_id"] or 0),
        ),
    )[0]
    support_count = int(tuples.get(selected_tuple_key, {}).get("count") or 0)
    if has_variant_key and target_year is not None and best["year"] == target_year:
        confidence = "high"
        rule = "variant_exact_year_exact"
    elif has_variant_key:
        confidence = "medium"
        rule = "variant_exact_year_window"
    elif target_year is not None and best["year"] == target_year and support_count >= 2:
        confidence = "medium"
        rule = "model_exact_year_exact_consensus"
    elif target_year is None and support_count >= 2 and second_count == 0:
        confidence = "medium"
        rule = "model_exact_consensus"
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


def infer_engine_cc_from_text(*values: Any) -> Optional[int]:
    scored: dict[int, int] = {}
    first_seen: list[int] = []
    for value in values:
        text = normalize_spec_text(value)
        if not text:
            continue
        for match in _ENGINE_CC_RE.finditer(text):
            candidate = _to_int(match.group(1))
            if candidate is None or candidate < 600 or candidate > 10000:
                continue
            if candidate not in scored:
                first_seen.append(candidate)
                scored[candidate] = 0
            scored[candidate] += 3
        for regex in (_ENGINE_LITER_RE, _ENGINE_LITER_CONTEXT_RE):
            for match in regex.finditer(text):
                raw_value = str(match.group(1) or "").replace(",", ".")
                try:
                    liters = float(raw_value)
                except ValueError:
                    continue
                if liters < 0.6 or liters > 9.9:
                    continue
                candidate = int(round(liters * 1000))
                if candidate not in scored:
                    first_seen.append(candidate)
                    scored[candidate] = 0
                scored[candidate] += 1
    if not scored:
        return None
    best = sorted(
        scored.items(),
        key=lambda item: (
            -item[1],
            first_seen.index(item[0]),
        ),
    )[0][0]
    return best


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
