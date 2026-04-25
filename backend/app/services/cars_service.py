from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select, func, and_, or_, case, cast, String, text, literal, not_, Integer
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import JSONB
import logging
from cachetools import TTLCache
import unicodedata

logger = logging.getLogger(__name__)
import re
import os
import requests
import time
from ..models import Car, Source, FeaturedCar
from ..utils.localization import display_color
from ..utils.color_groups import color_family_group_keys, normalize_color_family_key, normalize_color_group_key
from ..utils.country_map import normalize_country_code
from ..utils.redis_cache import build_cars_count_key, redis_get_json, redis_set_json
from ..utils.registration_defaults import get_missing_registration_default
from ..utils.filter_values import normalize_csv_values, split_csv_values
from ..utils.taxonomy import (
    body_aliases,
    normalize_color,
    normalize_body_type,
    normalize_fuel,
    ru_color,
    ru_fuel,
    ru_transmission,
    build_engine_type_options,
    color_aliases,
    fuel_aliases,
    color_hex,
    is_color_base,
    interior_color_aliases,
    interior_material_aliases,
    parse_interior_trim_token,
)
from ..utils.breakdown_labels import label_for
from ..utils.price_utils import ceil_to_step, get_round_step_rub, raw_price_to_rub
from .calculator_config_service import CalculatorConfigService
from .calculator import get_util_fee_rub as legacy_util_fee_rub
from .calculator_runtime import EstimateRequest, calculate, is_bev
from .customs_config import calc_util_fee_rub, get_customs_config

BRAND_ALIASES = {
    "alfa": "Alfa Romeo",
    "alfa romeo": "Alfa Romeo",
    "mercedes": "Mercedes-Benz",
    "mercedes benz": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz",
    "lynk co": "Lynk&Co",
    "lynk&co": "Lynk&Co",
    "landrover": "Land Rover",
    "rolls royce": "Rolls-Royce",
    "rolls-royce": "Rolls-Royce",
}


_FREE_TEXT_FUEL_MAP = {
    "дизель": "diesel",
    "дизельный": "diesel",
    "дизельные": "diesel",
    "дизельное": "diesel",
    "diesel": "diesel",
    "бензин": "petrol",
    "бенз": "petrol",
    "petrol": "petrol",
    "gasoline": "petrol",
    "hybrid": "hybrid",
    "гибрид": "hybrid",
    "электро": "electric",
    "электр": "electric",
    "electric": "electric",
    "ev": "electric",
}


def normalize_brand(value: Optional[str]) -> str:
    if not value:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    return BRAND_ALIASES.get(raw.lower(), raw)


def canonicalize_free_text_filters(
    *,
    q: Optional[str] = None,
    engine_type: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    normalized_engine_type = normalize_csv_values(engine_type) or engine_type
    normalized_q = str(q or "").strip()
    if normalized_engine_type or not normalized_q:
        return (normalized_q or None, normalized_engine_type)

    q_tokens = [t for t in re.split(r"[\s,]+", normalized_q.lower()) if t]
    if len(q_tokens) != 1:
        return normalized_q or None, normalized_engine_type

    token = q_tokens[0]
    mapped = _FREE_TEXT_FUEL_MAP.get(token)
    if mapped:
        return None, mapped
    if token.startswith("дизел"):
        return None, "diesel"
    return normalized_q or None, normalized_engine_type


_MODEL_WS_RE = re.compile(r"\s+")
_MODEL_QUOTES_RE = re.compile(r"[\"'`“”‘’]+")
_MODEL_PARENS_RE = re.compile(r"\([^)]{0,40}\)")
_MODEL_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_MODEL_GEN_TOKEN_RE = re.compile(r"^[a-z]{1,3}\d{2,3}[a-z]?$")
_MODEL_NUMERIC_ORDINAL_RE = re.compile(r"^\d+(?:st|nd|rd|th)$")
_BENTLEY_POWER_MODEL_RE = re.compile(r"^(?P<model>.+?)@@hp(?P<hp>\d{2,4})$")
_BENTLEY_POWER_LABEL_RE = re.compile(r"^(?P<model>.+?)\s+(?P<hp>\d{2,4})\s*л\.с\.?$", re.IGNORECASE)

_MODEL_NOISE_PREFIXES = (
    "the all new ",
    "all new ",
    "all-new ",
    "the new ",
    "new ",
)

_MODEL_FALLBACK_STOPWORDS = {
    "gasoline",
    "petrol",
    "diesel",
    "hybrid",
    "hev",
    "phev",
    "electric",
    "ev",
    "turbo",
    "awd",
    "4wd",
    "2wd",
    "fwd",
    "rwd",
    "xdrive",
    "quattro",
    "gdi",
    "gde",
    "gdi",
    "lpi",
    "lpg",
    "signature",
    "noblesse",
    "prestige",
    "premium",
    "exclusive",
    "luxury",
    "special",
    "edition",
    "limited",
    "modern",
    "smart",
    "value",
    "plus",
    "inspiration",
    "calligraphy",
    "manufacturer",
    "china",
    "sport",
    "sports",
    "sportline",
    "line",
    "amg",
    "seater",
    "door",
    "doors",
}

_MODEL_TOKEN_EQUIVALENTS = {
    "gt": {"gt", "gran", "turismo"},
    "gran": {"gran", "gt"},
    "turismo": {"turismo", "gt"},
    "coupe": {"coupe"},
    "cabriolet": {"cabriolet", "convertible"},
    "convertible": {"convertible", "cabriolet"},
    "series": {"series", "serie", "seria", "серия", "er"},
    "class": {"class", "klasse", "classe", "klasa"},
}

_MODEL_OPTIONAL_TAIL_TOKENS = {"series", "class"}
_PORSCHE_MODEL_ALIASES = {
    "taican": "Taycan",
    "taykan": "Taycan",
    "caiman": "Cayman",
}


def normalize_model_label(value: Optional[str]) -> str:
    raw = str(value or "").replace("\xa0", " ").strip()
    if not raw:
        return ""
    return _MODEL_WS_RE.sub(" ", raw)


def model_lookup_key(value: Optional[str]) -> str:
    return normalize_model_label(value).casefold()


def _strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )


def _fold_model_text(value: Optional[str]) -> str:
    text = normalize_model_label(value)
    if not text:
        return ""
    text = text.translate(str.maketrans({"–": "-", "—": "-", "／": "/", "·": " "}))
    text = _MODEL_QUOTES_RE.sub("", text)
    text = _MODEL_PARENS_RE.sub(" ", text)
    text = normalize_model_label(text)
    return text.strip(" -/.,;")


def _model_search_tokens(value: Optional[str]) -> List[str]:
    text = _strip_accents(_fold_model_text(value)).lower()
    if not text:
        return []
    text = _MODEL_NON_ALNUM_RE.sub(" ", text)
    tokens = [token for token in text.split() if token]
    expanded: List[str] = []
    for token in tokens:
        if re.fullmatch(r"x\d+m", token):
            expanded.extend([token[:-1], "m"])
            continue
        expanded.append(token)
    return expanded


def _dedupe_model_values(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        label = normalize_model_label(value)
        key = model_lookup_key(label)
        if not label or key in seen:
            continue
        seen.add(key)
        out.append(label)
    return out


def _starts_with_noise_prefix(value: str) -> str:
    text = value
    changed = True
    while changed:
        changed = False
        lower = text.lower()
        for prefix in _MODEL_NOISE_PREFIXES:
            if lower.startswith(prefix):
                text = text[len(prefix):].strip()
                changed = True
                break
    return text


def brand_variants(value: Optional[str]) -> List[str]:
    norm = normalize_brand(value)
    if not norm:
        return []
    variants = {norm}
    if norm == "Alfa Romeo":
        variants.add("Alfa")
    if norm == "Mercedes-Benz":
        variants.update({"Mercedes", "Mercedes Benz"})
    if norm == "Lynk&Co":
        variants.add("Lynk Co")
    if norm == "Land Rover":
        variants.add("LandRover")
    if norm == "Rolls-Royce":
        variants.add("Rolls Royce")
    return sorted(variants, key=lambda v: v.lower())


def _record_value(record: Any, field: str) -> Any:
    if isinstance(record, dict):
        return record.get(field)
    return getattr(record, field, None)


def effective_engine_cc_value(record: Any) -> Any:
    value = _record_value(record, "engine_cc")
    if value is not None:
        return value
    return _record_value(record, "inferred_engine_cc")


def effective_power_hp_value(record: Any) -> Any:
    value = _record_value(record, "power_hp")
    if value is not None:
        return value
    return _record_value(record, "inferred_power_hp")


def effective_power_kw_value(record: Any) -> Any:
    value = _record_value(record, "power_kw")
    if value is not None:
        return value
    return _record_value(record, "inferred_power_kw")


def electric_vehicle_hint_text(record: Any) -> str:
    parts: list[str] = []
    for field in ("brand", "model", "variant", "source_url", "engine_type"):
        value = _record_value(record, field)
        if value not in (None, ""):
            parts.append(str(value))
    payload = _record_value(record, "source_payload")
    if isinstance(payload, dict):
        for key in (
            "title",
            "sub_title",
            "description",
            "model",
            "engine_type",
            "envkv_engine_type",
            "envkv_consumption_fuel",
            "full_fuel_type",
        ):
            value = payload.get(key)
            if value not in (None, ""):
                parts.append(str(value))
    return " | ".join(parts)


class CarsService:
    _eu_model_donor_cache: TTLCache = TTLCache(maxsize=256, ttl=600)

    def __init__(self, db: Session) -> None:
        self.db = db
        self.logger = logging.getLogger(__name__)
        self._filtered_models_cache: Dict[tuple, List[Dict[str, Any]]] = {}
        self._resolved_model_alias_cache: Dict[tuple, List[str]] = {}

    def _available_expr(self):
        return Car.is_available.is_(True)

    @staticmethod
    def _registration_defaulted_expr():
        payload_json = cast(Car.source_payload, JSONB)
        return (
            func.coalesce(
                func.jsonb_extract_path_text(payload_json, "registration_defaulted"),
                "false",
            )
            == "true"
        )

    @classmethod
    def _registration_year_defaulted_expr(cls):
        payload_json = cast(Car.source_payload, JSONB)
        fallback_year, _ = get_missing_registration_default()
        # EU rows should trust the parsed first-registration year when it exists.
        # The defaulted flags were introduced to protect KR imports where fallback
        # registration values were historically persisted into the real columns.
        # Some older EU payloads can still carry default markers, but treating
        # those flags as authoritative would wrongly collapse first-registration
        # filters back to model-year filters.
        year_defaulted_flag = and_(
            Car.country.like("KR%"),
            func.coalesce(
                func.jsonb_extract_path_text(payload_json, "registration_year_defaulted"),
                "false",
            )
            == "true",
        )
        # Legacy rows only had the generic registration_defaulted flag. Treat them
        # as year-defaulted only for KR rows, where old imports persisted fallback
        # years into the real columns. EU rows could have only the month defaulted
        # while still carrying the generic legacy flag.
        legacy_year_defaulted = and_(
            Car.country.like("KR%"),
            cls._registration_defaulted_expr(),
            Car.registration_year.is_not(None),
            Car.registration_year == fallback_year,
            func.coalesce(
                cast(func.jsonb_extract_path_text(payload_json, "registration_default_year"), String),
                "",
            )
            == str(fallback_year),
        )
        return or_(year_defaulted_flag, legacy_year_defaulted)

    @classmethod
    def _registration_month_defaulted_expr(cls):
        payload_json = cast(Car.source_payload, JSONB)
        _, fallback_month = get_missing_registration_default()
        month_defaulted_flag = and_(
            Car.country.like("KR%"),
            func.coalesce(
                func.jsonb_extract_path_text(payload_json, "registration_month_defaulted"),
                "false",
            )
            == "true",
        )
        legacy_month_defaulted = and_(
            Car.country.like("KR%"),
            cls._registration_defaulted_expr(),
            Car.registration_month.is_not(None),
            Car.registration_month == fallback_month,
            func.coalesce(
                cast(func.jsonb_extract_path_text(payload_json, "registration_default_month"), String),
                "",
            )
            == str(fallback_month),
        )
        return or_(month_defaulted_flag, legacy_month_defaulted)

    @classmethod
    def _registration_uses_model_year_expr(cls):
        return or_(
            cls._registration_year_defaulted_expr(),
            Car.registration_year.is_(None),
        )

    @classmethod
    def _registration_uses_fallback_month_expr(cls):
        return or_(
            cls._registration_uses_model_year_expr(),
            cls._registration_month_defaulted_expr(),
            Car.registration_month.is_(None),
        )

    @classmethod
    def _effective_registration_year_expr(cls):
        return case(
            (cls._registration_uses_model_year_expr(), Car.year),
            else_=Car.registration_year,
        )

    @classmethod
    def _effective_registration_month_floor_expr(cls):
        return case(
            (cls._registration_uses_fallback_month_expr(), 12),
            else_=Car.registration_month,
        )

    @classmethod
    def _effective_registration_month_ceil_expr(cls):
        return case(
            (cls._registration_uses_fallback_month_expr(), 1),
            else_=Car.registration_month,
        )

    def _normalized_model_expr(self):
        # Keep normalization conservative: collapse whitespace/case only, so
        # identical EU/KR models merge without accidentally collapsing variants.
        return func.lower(
            func.regexp_replace(
                func.trim(func.coalesce(Car.model, "")),
                r"\s+",
                " ",
                "g",
            )
        )

    def _brand_model_alias_label(
        self,
        brand: str,
        raw_model: str,
        *,
        donors: Optional[List[str]] = None,
    ) -> str:
        norm_brand = normalize_brand(brand).strip().upper()
        if norm_brand != "PORSCHE":
            return ""
        donor_list = donors if donors is not None else []
        donor_map = {
            model_lookup_key(donor): normalize_model_label(donor)
            for donor in donor_list
            if normalize_model_label(donor)
        }
        for token in _model_search_tokens(raw_model):
            canonical = _PORSCHE_MODEL_ALIASES.get(token.lower())
            if not canonical:
                continue
            return donor_map.get(model_lookup_key(canonical), canonical)
        return ""

    def _eu_model_donors(self, brand: str) -> List[str]:
        norm_brand = normalize_brand(brand).strip()
        if not norm_brand or self.db is None:
            return []
        cache_key = norm_brand.casefold()
        cached = self._eu_model_donor_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        rows = self.facet_counts(field="model", filters={"region": "EU", "brand": norm_brand})
        donors = _dedupe_model_values(
            [
                str(row.get("value") or "").strip()
                for row in rows
                if str(row.get("value") or "").strip() and str(row.get("value") or "").strip().casefold() != "other"
            ]
        )
        self._eu_model_donor_cache[cache_key] = donors
        return list(donors)

    def _remove_brand_prefix(self, brand: str, raw_model: str) -> str:
        text = _starts_with_noise_prefix(_fold_model_text(raw_model))
        if not text:
            return ""
        brand_tokens = _dedupe_model_values(brand_variants(brand) + [brand, brand.replace("-", " ")])
        for variant in brand_tokens:
            folded = _fold_model_text(variant)
            if not folded:
                continue
            pattern = re.compile(rf"^{re.escape(folded)}(?:\s+|[-/])", re.IGNORECASE)
            text = pattern.sub("", text).strip()
        if brand.casefold() == "mercedes-benz":
            text = re.sub(r"^benz(?:\s+|[-/])", "", text, flags=re.IGNORECASE).strip()
        return _starts_with_noise_prefix(text)

    def _model_token_present(self, raw_tokens: List[str], donor_token: str) -> bool:
        if not donor_token:
            return False
        donor = donor_token.lower()
        variants = set(_MODEL_TOKEN_EQUIVALENTS.get(donor, {donor}))
        for raw_token in raw_tokens:
            raw = raw_token.lower()
            if raw in variants:
                return True
            if donor.isdigit() and raw.startswith(donor):
                return True
            if re.fullmatch(r"[a-z]+\d+[a-z]*", donor) and raw.startswith(donor):
                return True
            if len(donor) >= 3 and raw.startswith(donor):
                return True
        return False

    def _match_eu_model_donor(self, raw_model: str, donors: List[str]) -> Optional[str]:
        raw_tokens = _model_search_tokens(raw_model)
        if not raw_tokens:
            return None
        best_label: Optional[str] = None
        best_score: tuple[int, int, int, int, tuple] | None = None
        for donor in donors:
            donor_label = normalize_model_label(donor)
            if not donor_label:
                continue
            donor_tokens = _model_search_tokens(donor_label)
            if not donor_tokens:
                continue
            matched_tokens: List[str] = []
            missing_tokens: List[str] = []
            for token in donor_tokens:
                if self._model_token_present(raw_tokens, token):
                    matched_tokens.append(token)
                else:
                    missing_tokens.append(token)
            if not matched_tokens:
                continue
            core_matches = [token for token in matched_tokens if token.lower() not in _MODEL_OPTIONAL_TAIL_TOKENS]
            if missing_tokens:
                if not all(token.lower() in _MODEL_OPTIONAL_TAIL_TOKENS for token in missing_tokens):
                    continue
                if not core_matches:
                    continue
            score = (
                len(core_matches),
                len(matched_tokens),
                -len(missing_tokens),
                len("".join(donor_tokens)),
                self._natural_text_key(donor_label),
            )
            if best_score is None or score > best_score:
                best_score = score
                best_label = donor_label
        return best_label

    def _fallback_model_label(self, brand: str, raw_model: str) -> str:
        cleaned = self._remove_brand_prefix(brand, raw_model)
        tokens = _model_search_tokens(cleaned)
        if not tokens:
            return normalize_model_label(raw_model)

        if brand.casefold() == "bmw":
            for token in tokens:
                if re.fullmatch(r"[1-8]\d{2}[a-z]{0,2}", token):
                    return token[:3]
                if re.fullmatch(r"m\d{1,3}[a-z]?", token):
                    if len(token) > 1:
                        return "M" + token[1:]
                if re.fullmatch(r"ix\d?", token):
                    if token == "ix":
                        return "iX"
                    return "iX" + token[2:]
                if re.fullmatch(r"i\d", token):
                    return token
                if re.fullmatch(r"x\dm?", token):
                    if token.endswith("m") and len(token) == 3:
                        return token[:2].upper() + " M"
                    return token.upper()
                if re.fullmatch(r"z\d", token):
                    return token.upper()
            series_match = re.search(r"\b([1-8])\s*(?:series|serie|seria|серия|er)\b", " ".join(tokens))
            if series_match:
                return f"{series_match.group(1)} серия"

        raw_words = _fold_model_text(cleaned).split()
        keep: List[str] = []
        for word in raw_words:
            search_word = _strip_accents(word).lower().strip(" -/.,;")
            if not search_word:
                continue
            if _MODEL_NUMERIC_ORDINAL_RE.fullmatch(search_word):
                break
            if _MODEL_GEN_TOKEN_RE.fullmatch(search_word):
                break
            if search_word in _MODEL_FALLBACK_STOPWORDS:
                break
            if re.fullmatch(r"\d+(?:\.\d+)?", search_word):
                break
            keep.append(word)
            if len(keep) >= 2 and re.search(r"\d", keep[0]):
                break
            if len(keep) >= 3:
                break
        if keep:
            return normalize_model_label(" ".join(keep))
        return normalize_model_label(cleaned or raw_model)

    def _canonical_model_label(self, brand: str, raw_model: str, *, donors: Optional[List[str]] = None) -> str:
        label = normalize_model_label(raw_model)
        if not label:
            return ""
        donor_list = donors if donors is not None else self._eu_model_donors(brand)
        alias_label = self._brand_model_alias_label(brand, raw_model, donors=donor_list)
        if alias_label:
            return alias_label
        matched = self._match_eu_model_donor(raw_model, donor_list)
        if matched:
            return matched
        return self._fallback_model_label(brand, raw_model)

    def _power_hp_expr(self):
        return func.coalesce(Car.power_hp, Car.inferred_power_hp)

    def _power_hp_bucket_expr(self):
        power_expr = self._power_hp_expr()
        return cast(func.round(power_expr / literal(10.0)) * literal(10), Integer)

    def _parse_bentley_power_model_token(self, value: Optional[str]) -> Optional[Tuple[str, int]]:
        raw = normalize_model_label(value)
        if not raw:
            return None
        match = _BENTLEY_POWER_MODEL_RE.fullmatch(raw) or _BENTLEY_POWER_LABEL_RE.fullmatch(raw)
        if not match:
            return None
        base_model = normalize_model_label(match.group("model"))
        hp_raw = match.group("hp")
        if not base_model or not hp_raw:
            return None
        try:
            hp = int(hp_raw)
        except Exception:
            return None
        if hp <= 0:
            return None
        return base_model, hp

    def _bentley_power_model_value(self, model_label: str, hp: int) -> str:
        return f"{normalize_model_label(model_label)}@@hp{int(hp)}"

    def _bentley_power_model_label(self, model_label: str, hp: int) -> str:
        return f"{normalize_model_label(model_label)} {int(hp)} л.с."

    def _bentley_power_models(self, brand: str) -> Dict[str, List[Dict[str, Any]]]:
        norm_brand = normalize_brand(brand).strip()
        if norm_brand.upper() != "BENTLEY" or self.db is None:
            return {}
        power_expr = self._power_hp_expr()
        power_bucket_expr = self._power_hp_bucket_expr()
        variants_lc = [value.lower() for value in brand_variants(norm_brand) if value]
        if not variants_lc:
            return {}
        rows = self.db.execute(
            select(
                Car.model,
                power_bucket_expr.label("power_bucket"),
                func.count().label("count"),
            )
            .where(
                self._available_expr(),
                func.lower(func.trim(Car.brand)).in_(variants_lc),
                Car.model.is_not(None),
                Car.model != "",
                power_expr.is_not(None),
            )
            .group_by(Car.model, power_bucket_expr)
            .order_by(Car.model.asc(), power_bucket_expr.asc())
        ).all()
        if not rows:
            return {}
        split_rows: Dict[str, List[Dict[str, Any]]] = {}
        for raw_model, power_bucket, count in rows:
            if raw_model is None or power_bucket is None:
                continue
            hp = int(power_bucket)
            if hp <= 0:
                continue
            label = self._canonical_model_label(norm_brand, str(raw_model))
            if not label:
                continue
            split_rows.setdefault(label, []).append(
                {
                    "value": self._bentley_power_model_value(label, hp),
                    "label": self._bentley_power_model_label(label, hp),
                    "count": int(count or 0),
                    "aliases": [],
                    "base_model": label,
                    "power_hp_bucket": hp,
                }
            )
        out: Dict[str, List[Dict[str, Any]]] = {}
        for label, items in split_rows.items():
            unique: Dict[str, Dict[str, Any]] = {}
            for item in items:
                unique[item["value"]] = item
            deduped = sorted(
                unique.values(),
                key=lambda item: (int(item.get("power_hp_bucket") or 0), self._natural_text_key(item.get("label") or "")),
            )
            if len(deduped) > 1:
                out[label] = deduped
        return out

    def _resolve_model_aliases(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
        model: Optional[str] = None,
    ) -> List[str]:
        label = normalize_model_label(model)
        if not label:
            return []
        norm_brand = normalize_brand(brand).strip() if brand else ""
        if not norm_brand or self.db is None:
            return [label]
        cache_key = (
            (region or "").upper(),
            normalize_country_code(country) if country else "",
            (kr_type or "").upper(),
            norm_brand.casefold(),
            model_lookup_key(label),
        )
        cached = self._resolved_model_alias_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        target_key = model_lookup_key(label)
        try:
            models = self.models_for_brand_filtered(
                region=region,
                country=country,
                kr_type=kr_type,
                brand=norm_brand,
            )
        except Exception:
            self.logger.exception("resolve_model_aliases_failed brand=%s model=%s", norm_brand, label)
            return [label]
        for item in models:
            option_keys = [model_lookup_key(item.get("value"))]
            option_keys.extend(model_lookup_key(alias) for alias in item.get("aliases") or [])
            if target_key not in option_keys:
                continue
            aliases = _dedupe_model_values([*(item.get("aliases") or []), str(item.get("value") or "")])
            resolved = aliases or [label]
            groups = self.build_model_groups(brand=norm_brand, models=models)
            for group in groups:
                group_label = normalize_model_label(group.get("label"))
                group_key = model_lookup_key(group_label)
                group_models = group.get("models") or []
                if group_key != target_key or len(group_models) <= 1:
                    continue
                expanded = _dedupe_model_values(
                    [
                        *(alias for row in group_models for alias in (row.get("aliases") or [])),
                        *(str(row.get("value") or "") for row in group_models),
                    ]
                )
                if expanded:
                    resolved = expanded
                break
            self._resolved_model_alias_cache[cache_key] = list(resolved)
            return resolved
        self._resolved_model_alias_cache[cache_key] = [label]
        return [label]

    def _model_filter_clause(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
        model: Optional[str] = None,
    ):
        label = normalize_model_label(model)
        if not label:
            return None
        bentley_token = self._parse_bentley_power_model_token(label)
        if bentley_token is not None:
            base_model, hp = bentley_token
            bentley_variants = [value.lower() for value in brand_variants("Bentley") if value]
            clauses = [self._normalized_model_expr() == model_lookup_key(base_model)]
            if bentley_variants:
                clauses.append(func.lower(func.trim(Car.brand)).in_(bentley_variants))
            clauses.append(self._power_hp_bucket_expr() == hp)
            return and_(*clauses)
        aliases = self._resolve_model_aliases(
            region=region,
            country=country,
            kr_type=kr_type,
            brand=brand,
            model=label,
        )
        keys = [model_lookup_key(alias) for alias in aliases if alias]
        keys = list(dict.fromkeys(key for key in keys if key))
        if not keys:
            return self._normalized_model_expr() == model_lookup_key(label)
        if len(keys) == 1:
            return self._normalized_model_expr() == keys[0]
        return self._normalized_model_expr().in_(keys)

    def _fuel_source_expr(self):
        payload_json = cast(Car.source_payload, JSONB)
        return func.coalesce(
            func.nullif(func.jsonb_extract_path_text(payload_json, "full_fuel_type"), ""),
            func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_engine_type"), ""),
            func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_consumption_fuel"), ""),
            func.nullif(func.jsonb_extract_path_text(payload_json, "fuel_raw"), ""),
            func.nullif(func.jsonb_extract_path_text(payload_json, "engine_raw"), ""),
            func.nullif(Car.engine_type, ""),
        )

    def _stored_fuel_expr(self):
        return func.lower(func.trim(Car.engine_type))

    def _payload_fuel_search_expr(self):
        payload_json = cast(Car.source_payload, JSONB)
        return func.lower(
            func.coalesce(
                func.nullif(func.jsonb_extract_path_text(payload_json, "full_fuel_type"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_engine_type"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_consumption_fuel"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "fuel_raw"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "engine_raw"), ""),
                "",
            )
        )

    def _fuel_search_expr(self):
        return func.lower(func.coalesce(self._fuel_source_expr(), ""))

    def _fuel_like_any(self, expr, terms: List[str]):
        return or_(*[expr.like(f"%{term}%") for term in terms if term])

    def _fuel_filter_clause(self, raw_value: str):
        key = normalize_fuel(raw_value) or str(raw_value or "").strip().lower()
        fuel_expr = self._fuel_search_expr()
        stored_fuel_expr = self._stored_fuel_expr()
        payload_fuel_expr = self._payload_fuel_search_expr()
        stored_fuel_missing = or_(Car.engine_type.is_(None), func.trim(Car.engine_type) == "")

        def _stored_exact_any(values: List[str]):
            return or_(*[stored_fuel_expr == value for value in values if value])

        def _payload_branch(positive: List[str], negative: Optional[List[str]] = None):
            clauses = [stored_fuel_missing, self._fuel_like_any(payload_fuel_expr, positive)]
            if negative:
                clauses.append(not_(self._fuel_like_any(payload_fuel_expr, negative)))
            return and_(*clauses)

        if key == "petrol":
            return or_(
                _stored_exact_any(["petrol", "gasoline", "benzin", "benzina", "бензин"]),
                _payload_branch(
                    ["petrol", "gasoline", "benzin", "benzina", "бензин"],
                    ["hybrid", "гибрид", "plug-in", "phev", "electric", "электро"],
                ),
            )
        if key == "diesel":
            return or_(
                _stored_exact_any(["diesel", "дизель"]),
                _payload_branch(
                    ["diesel", "дизель"],
                    ["hybrid", "гибрид", "plug-in", "phev", "electric", "электро"],
                ),
            )
        if key == "electric":
            return or_(
                _stored_exact_any(["electric", "elektro", "электро", "ev"]),
                _payload_branch(
                    ["electric", "elektro", "электро"],
                    ["hybrid", "гибрид", "plug-in", "phev"],
                ),
            )
        if key == "ethanol":
            return self._fuel_like_any(fuel_expr, ["ethanol", "e85", "ffv", "flexfuel", "flex fuel", "этанол"])
        if key == "hybrid_diesel":
            return or_(
                self._fuel_like_any(fuel_expr, ["hybrid (diesel/electric)", "дизель + электро"]),
                and_(
                    self._fuel_like_any(fuel_expr, ["hybrid", "гибрид"]),
                    self._fuel_like_any(fuel_expr, ["diesel", "дизель"]),
                    not_(self._fuel_like_any(fuel_expr, ["plug-in", "phev"])),
                ),
            )
        if key == "hybrid":
            return or_(
                _stored_exact_any(["hybrid", "гибрид"]),
                _payload_branch(["hybrid (petrol/electric)", "бензин + электро", "vollhybrid"]),
                and_(
                    stored_fuel_missing,
                    self._fuel_like_any(payload_fuel_expr, ["hybrid", "гибрид"]),
                    not_(self._fuel_like_any(payload_fuel_expr, ["diesel", "дизель", "plug-in", "phev"])),
                ),
            )
        if key == "hydrogen":
            return self._fuel_like_any(fuel_expr, ["hydrogen", "fuel cell", "водород"])
        if key == "lpg":
            return self._fuel_like_any(fuel_expr, ["lpg", "propane", "propan", "пропан"])
        if key == "cng":
            return self._fuel_like_any(
                fuel_expr,
                ["natural gas", "cng", "methane", "metano", "erdgas", "природный газ", "метан"],
            )
        if key == "phev":
            return self._fuel_like_any(fuel_expr, ["plug-in hybrid", "plug in hybrid", "plugin hybrid", "phev", "подключаем"])
        if key == "other":
            return or_(fuel_expr == "other", fuel_expr.like("other,%"), fuel_expr.like("другое%"))
        aliases = fuel_aliases(raw_value)
        if aliases:
            return or_(*[fuel_expr.like(f"%{alias.lower()}%") for alias in aliases])
        return fuel_expr == str(raw_value or "").strip().lower()

    EU_COUNTRIES = {
        "DE", "AT", "FR", "IT", "ES", "NL", "BE", "PL", "CZ", "SE", "FI",
        "NO", "DK", "PT", "GR", "CH", "LU", "IE", "GB", "HU", "SK", "SI",
        "HR", "RO", "BG", "EE", "LV", "LT", "MT", "CY", "IS", "LI", "MC",
        "SM", "AD",
    }
    KOREA_SOURCE_HINTS = ("emavto", "encar", "m-auto", "m_auto")
    EUROPE_SOURCE_PREFIX = "mobile"

    def _source_ids_for_hints(self, hints: tuple[str, ...]) -> List[int]:
        if not hints:
            return []
        key_expr = func.lower(Source.key)
        conds = [key_expr.like(f"%{hint}%") for hint in hints]
        return self.db.execute(select(Source.id).where(or_(*conds))).scalars().all()

    def _source_ids_for_europe(self) -> List[int]:
        key_expr = func.lower(Source.key)
        return self.db.execute(
            select(Source.id).where(
                or_(
                    key_expr.like(f"%{self.EUROPE_SOURCE_PREFIX}%"),
                    func.upper(Source.country).in_(self.EU_COUNTRIES),
                )
            )
        ).scalars().all()

    def available_eu_countries(self) -> List[str]:
        rows = self.db.execute(
            select(func.distinct(Car.country))
            .where(self._available_expr(), Car.country.is_not(None))
        ).scalars().all()
        countries: List[str] = []
        seen = set()
        for c in rows:
            code = normalize_country_code(c)
            if not code:
                continue
            if code in self.EU_COUNTRIES and code not in seen:
                countries.append(code)
                seen.add(code)
        return countries

    def has_korea(self) -> bool:
        rows = self.db.execute(
            select(func.distinct(Car.country))
            .where(self._available_expr(), Car.country.is_not(None))
        ).scalars().all()
        for c in rows:
            code = normalize_country_code(c)
            if code == "KR":
                return True
        return bool(self._source_ids_for_hints(self.KOREA_SOURCE_HINTS))

    def has_korea_market_type_data(self) -> bool:
        count = self.db.execute(
            select(func.count(Car.id)).where(
                self._available_expr(),
                Car.country.like("KR%"),
                Car.kr_market_type.is_not(None),
                func.lower(Car.kr_market_type).in_(["domestic", "import"]),
            )
        ).scalar()
        return bool(int(count or 0))

    def available_regions(self) -> List[str]:
        regions: List[str] = []
        if self.available_eu_countries() or self._source_ids_for_europe():
            regions.append("EU")
        if self.has_korea():
            regions.append("KR")
        return regions

    _fx_cache: dict | None = None
    _fx_cache_ts: float | None = None
    _count_cache: TTLCache = TTLCache(maxsize=1024, ttl=120)

    def _can_fast_count(
        self,
        *,
        region: Optional[str],
        country: Optional[str],
        brand: Optional[str],
        model: Optional[str],
        lines: Optional[List[str]],
        source_key: Optional[str | List[str]],
        q: Optional[str],
        generation: Optional[str],
        color: Optional[str],
        price_min: Optional[float],
        price_max: Optional[float],
        year_min: Optional[int],
        year_max: Optional[int],
        mileage_min: Optional[int],
        mileage_max: Optional[int],
        reg_year_min: Optional[int],
        reg_month_min: Optional[int],
        reg_year_max: Optional[int],
        reg_month_max: Optional[int],
        body_type: Optional[str],
        engine_type: Optional[str],
        transmission: Optional[str],
        drive_type: Optional[str],
        num_seats: Optional[str],
        doors_count: Optional[str],
        emission_class: Optional[str],
        efficiency_class: Optional[str],
        climatisation: Optional[str],
        airbags: Optional[str],
        interior_design: Optional[str],
        interior_color: Optional[str],
        interior_material: Optional[str],
        vat_reclaimable: Optional[str],
        air_suspension: Optional[bool],
        price_rating_label: Optional[str],
        owners_count: Optional[str],
        power_hp_min: Optional[float],
        power_hp_max: Optional[float],
        engine_cc_min: Optional[int],
        engine_cc_max: Optional[int],
        condition: Optional[str],
        kr_type: Optional[str],
    ) -> bool:
        if model:
            return False
        if any(
            [
                lines,
                source_key,
                q,
                generation,
                color,
                price_min,
                price_max,
                year_min,
                year_max,
                mileage_min,
                mileage_max,
                reg_year_min,
                reg_month_min,
                reg_year_max,
                reg_month_max,
                body_type,
                engine_type,
                transmission,
                drive_type,
                num_seats,
                doors_count,
                emission_class,
                efficiency_class,
                climatisation,
                airbags,
                interior_design,
                interior_color,
                interior_material,
                vat_reclaimable,
                air_suspension,
                price_rating_label,
                owners_count,
                power_hp_min,
                power_hp_max,
                engine_cc_min,
                engine_cc_max,
                condition,
                kr_type,
            ]
        ):
            return False
        return True

    def _fast_count(
        self,
        *,
        region: str,
        country: Optional[str],
        brand: Optional[str],
        model: Optional[str],
    ) -> Optional[int]:
        region_norm = region.upper().strip() if region else None
        if region_norm not in ("EU", "KR", None):
            return None
        brand_norm = normalize_brand(brand).strip() if brand else None
        model_norm = model.strip() if model else None
        country_norm = normalize_country_code(country) if country else None
        if country_norm == "EU":
            country_norm = None
        if country_norm == "KR" and region_norm not in (None, "KR"):
            return None
        brand_variants_list = brand_variants(brand_norm) if brand_norm else []
        if model_norm and brand_norm:
            stmt = text(
                """
                SELECT COALESCE(SUM(total), 0) AS total
                FROM car_counts_model
                WHERE (:region IS NULL OR region = :region)
                  AND (:country IS NULL OR country = :country)
                  AND LOWER(TRIM(brand)) = ANY(:brand_variants_lc)
                  AND model = :model
                """
            )
        elif brand_norm:
            stmt = text(
                """
                SELECT COALESCE(SUM(total), 0) AS total
                FROM car_counts_brand
                WHERE (:region IS NULL OR region = :region)
                  AND (:country IS NULL OR country = :country)
                  AND LOWER(TRIM(brand)) = ANY(:brand_variants_lc)
                """
            )
        else:
            stmt = text(
                """
                SELECT COALESCE(SUM(total), 0) AS total
                FROM car_counts_core
                WHERE (:region IS NULL OR region = :region)
                  AND (:country IS NULL OR country = :country)
                """
            )
        try:
            row = self.db.execute(
                stmt,
                {
                    "region": region_norm,
                    "country": country_norm,
                    "brand": brand_norm or None,
                    "model": model_norm or None,
                    "brand_variants_lc": [v.lower() for v in (brand_variants_list or [brand_norm] if brand_norm else []) if v],
                },
            ).first()
        except ProgrammingError:
            self.db.rollback()
            self.logger.exception("fast_count_failed region=%s", region_norm)
            return None
        except Exception:
            self.logger.exception("fast_count_failed region=%s", region_norm)
            return None
        return int(row[0]) if row else None

    def _facet_where(self, filters: Dict[str, Any], params: Dict[str, Any]) -> List[str]:
        clauses = []
        for key, col in filters.items():
            val = params.get(key)
            if val is None or val == "":
                continue
            clauses.append(f"{col} = :{key}")
        return clauses

    def _facet_counts_from_cars(self, *, field: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        col_map = {
            "brand": Car.brand,
            "model": Car.model,
            "color": Car.color,
            "color_group": func.coalesce(Car.color_group, literal("other")),
            "engine_type": self._fuel_source_expr(),
            "transmission": Car.transmission,
            "body_type": Car.body_type,
            "drive_type": Car.drive_type,
            "country": func.upper(Car.country),
        }
        if hasattr(Car, "reg_year"):
            col_map["reg_year"] = getattr(Car, "reg_year")
        if field == "region":
            eu_sources = self._source_ids_for_europe()
            kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
            region_expr = case(
                (func.upper(Car.country).like("KR%"), literal("KR")),
                (Car.source_id.in_(kr_sources), literal("KR")),
                (Car.source_id.in_(eu_sources), literal("EU")) if eu_sources else (func.upper(Car.country).in_(self.EU_COUNTRIES), literal("EU")),
                else_=func.upper(Car.country),
            )
            col = region_expr
        else:
            col = col_map.get(field)
        if col is None:
            return []
        conditions = [self._available_expr()]
        region = filters.get("region")
        country = filters.get("country")
        kr_type = filters.get("kr_type")
        brand = filters.get("brand")
        model = filters.get("model")
        if country:
            c = normalize_country_code(country)
            if c == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                conds = [Car.country.like("KR%")]
                if kr_sources:
                    conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*conds))
            elif c == "EU":
                region = "EU"
            elif c:
                conditions.append(Car.country == c)
        if region:
            r = region.upper()
            if r == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                conds = [Car.country.like("KR%")]
                if kr_sources:
                    conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*conds))
            elif r == "EU":
                eu_sources = self._source_ids_for_europe()
                if eu_sources:
                    conditions.append(Car.source_id.in_(eu_sources))
                else:
                    conditions.append(Car.country.in_(self.EU_COUNTRIES))
        if kr_type:
            kt_raw = str(kr_type).upper()
            kt = None
            if kt_raw in ("KR_INTERNAL", "DOMESTIC"):
                kt = "domestic"
            elif kt_raw in ("KR_IMPORT", "IMPORT"):
                kt = "import"
            if kt:
                conditions.append(func.lower(Car.kr_market_type) == kt)
                if not region and not country:
                    kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                    conds = [Car.country.like("KR%")]
                    if kr_sources:
                        conds.append(Car.source_id.in_(kr_sources))
                    conditions.append(or_(*conds))
        if brand:
            b = normalize_brand(brand).strip()
            if b:
                variants = brand_variants(b)
                variants_lc = [v.lower() for v in variants if v]
                if variants_lc:
                    conditions.append(func.lower(func.trim(Car.brand)).in_(variants_lc))
        if model:
            clause = self._model_filter_clause(
                region=region,
                country=country,
                kr_type=kr_type,
                brand=brand,
                model=model,
            )
            if clause is not None:
                conditions.append(clause)
        if field not in ("region", "country", "brand", "model", "engine_type"):
            val = filters.get(field)
            if val:
                conditions.append(getattr(Car, field) == val)
        stmt = (
            select(col.label("value"), func.count().label("count"))
            .select_from(Car)
            .where(and_(*conditions))
            .group_by(col)
            .order_by(func.count().desc())
        )
        rows = self.db.execute(stmt).all()
        out = []
        for value, count in rows:
            if value is None or value == "":
                continue
            out.append({"value": value, "count": int(count)})
        if field == "brand":
            merged: Dict[str, int] = {}
            for row in out:
                norm = normalize_brand(row["value"])
                if not norm:
                    continue
                merged[norm] = merged.get(norm, 0) + int(row["count"])
            out = [{"value": k, "count": v} for k, v in merged.items()]
            out = sorted(out, key=lambda x: (-x["count"], x["value"].lower()))
        return out

    def facet_counts(self, *, field: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        if filters.get("model"):
            return self._facet_counts_from_cars(field=field, filters=filters)
        if field in {"color_group", "engine_type"}:
            return self._facet_counts_from_cars(field=field, filters=filters)
        table_map = {
            "region": ("car_counts_core", "region", {"region"}),
            "country": ("car_counts_core", "country", {"region"}),
            "brand": ("car_counts_brand", "brand", {"region", "country"}),
            "model": ("car_counts_model", "model", {"region", "country", "brand"}),
            "color": ("car_counts_color", "color", {"region", "country", "brand"}),
            "engine_type": ("car_counts_engine_type", "engine_type", {"region", "country", "brand"}),
            "transmission": ("car_counts_transmission", "transmission", {"region", "country", "brand"}),
            "body_type": ("car_counts_body_type", "body_type", {"region", "country", "brand"}),
            "drive_type": ("car_counts_drive_type", "drive_type", {"region", "country", "brand"}),
            "price_bucket": ("car_counts_price_bucket", "price_bucket", {"region", "country", "brand"}),
            "mileage_bucket": ("car_counts_mileage_bucket", "mileage_bucket", {"region", "country", "brand"}),
            "reg_year": ("car_counts_reg_year", "reg_year", {"region", "country"}),
        }
        if field not in table_map:
            return []

        table, col, allowed_filters = table_map[field]
        params: Dict[str, Any] = {}
        filters_norm = {
            "region": filters.get("region"),
            "country": normalize_country_code(filters.get("country")) if filters.get("country") else None,
            "brand": normalize_brand(filters.get("brand")).strip() if filters.get("brand") else None,
            "model": filters.get("model"),
            "color": filters.get("color"),
            "engine_type": filters.get("engine_type"),
            "transmission": filters.get("transmission"),
            "body_type": filters.get("body_type"),
            "drive_type": filters.get("drive_type"),
            "price_bucket": filters.get("price_bucket"),
            "mileage_bucket": filters.get("mileage_bucket"),
            "reg_year": filters.get("reg_year"),
        }
        for k, v in filters_norm.items():
            if v is not None and v != "":
                params[k] = v
        brand_list = None
        if params.get("brand"):
            brand_list = brand_variants(params["brand"])
            if brand_list:
                params["brand_variants_lc"] = [v.lower() for v in brand_list if v]

        where_clauses = []
        for key in allowed_filters:
            if key == "brand" and brand_list:
                where_clauses.append("LOWER(TRIM(brand)) = ANY(:brand_variants_lc)")
                continue
            if key in params:
                where_clauses.append(f"{key} = :{key}")
        if field not in ("region", "country", "brand", "model", "engine_type"):
            if field in params:
                where_clauses.append(f"{col} = :{field}")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        order_sql = "count DESC, value"
        if field == "reg_year":
            order_sql = "value DESC"

        query = text(
            f"""
            SELECT {col} AS value, SUM(total) AS count
            FROM {table}
            {where_sql}
            GROUP BY {col}
            HAVING SUM(total) > 0
            ORDER BY {order_sql}
            """
        )
        try:
            rows = self.db.execute(query, params).all()
        except ProgrammingError as exc:
            self.db.rollback()
            self.logger.warning(
                "facet_counts_fallback field=%s missing_table=%s", field, table
            )
            return self._facet_counts_from_cars(field=field, filters=filters_norm)
        out = []
        for value, count in rows:
            if value is None or value == "":
                continue
            out.append({"value": value, "count": int(count)})
        if field == "brand":
            merged: Dict[str, int] = {}
            for row in out:
                norm = normalize_brand(row["value"])
                if not norm:
                    continue
                merged[norm] = merged.get(norm, 0) + int(row["count"])
            out = [{"value": k, "count": v} for k, v in merged.items()]
            out = sorted(out, key=lambda x: (-x["count"], x["value"].lower()))
        return out

    def get_fx_rates(self, *, allow_fetch: bool = True) -> dict | None:
        now = time.time()
        ttl_sec = 3600
        if self._fx_cache and self._fx_cache_ts and now - self._fx_cache_ts < ttl_sec:
            return self._fx_cache
        if not allow_fetch:
            return self._fx_cache or {}
        fx_add_rub = float(os.environ.get("FX_ADD_RUB", "1.0"))
        eur_env = float(os.environ.get("EURO_RATE", "95.0")) + fx_add_rub
        usd_env = float(os.environ.get("USD_RATE", "85.0")) + fx_add_rub
        cny_env = float(os.environ.get("CNY_RATE", "12.0")) + fx_add_rub
        eur, usd, cny = eur_env, usd_env, cny_env
        cached = self._fx_cache

        def _rate(data: dict, code: str, default: float) -> float:
            try:
                row = data["Valute"][code]
                value = float(row["Value"])
                nominal = float(row.get("Nominal") or 1.0) or 1.0
                return (value / nominal) + fx_add_rub
            except Exception:
                return default

        try:
            res = requests.get(
                "https://www.cbr-xml-daily.ru/daily_json.js",
                timeout=(0.3, 0.7),
            )
            data = res.json()
            eur = _rate(data, "EUR", eur_env)
            usd = _rate(data, "USD", usd_env)
            cny = _rate(data, "CNY", cny_env)
            self._fx_cache = {"EUR": eur, "USD": usd, "CNY": cny, "RUB": 1.0}
            self._fx_cache_ts = now
            return self._fx_cache
        except Exception:
            if cached:
                return cached
            return {"EUR": eur_env, "USD": usd_env, "CNY": cny_env, "RUB": 1.0}

    def _raw_price_rub_expr(self):
        rates = self.get_fx_rates() or {}
        fx_eur = float(rates.get("EUR") or 0)
        fx_usd = float(rates.get("USD") or 0)
        fx_cny = float(rates.get("CNY") or 0)
        currency_expr = func.upper(func.coalesce(Car.currency, ""))
        raw_price = Car.price
        clauses: list[tuple[Any, Any]] = []
        if fx_eur > 0:
            clauses.append((and_(raw_price.is_not(None), currency_expr == "EUR"), raw_price * literal(fx_eur)))
        if fx_usd > 0:
            clauses.append((and_(raw_price.is_not(None), currency_expr == "USD"), raw_price * literal(fx_usd)))
        if fx_cny > 0:
            clauses.append((and_(raw_price.is_not(None), currency_expr == "CNY"), raw_price * literal(fx_cny)))
        clauses.append((and_(raw_price.is_not(None), currency_expr.in_(["RUB", "₽"])), raw_price))
        return case(*clauses, else_=None)

    def _display_price_rub_expr(self):
        total_expr = case(
            (
                and_(
                    Car.total_price_rub_cached.is_not(None),
                    Car.total_price_rub_cached > 0,
                ),
                Car.total_price_rub_cached,
            ),
            else_=None,
        )
        price_expr = case(
            (
                and_(
                    Car.price_rub_cached.is_not(None),
                    Car.price_rub_cached > 0,
                ),
                Car.price_rub_cached,
            ),
            else_=None,
        )
        raw_expr = self._raw_price_rub_expr()
        raw_positive_expr = case(
            (
                and_(
                    raw_expr.is_not(None),
                    raw_expr > 0,
                ),
                raw_expr,
            ),
            else_=None,
        )
        return func.coalesce(
            total_expr,
            price_expr,
            raw_positive_expr,
        )

    def _catalog_inline_price_refresh_enabled(self) -> bool:
        return os.getenv("CATALOG_INLINE_PRICE_REFRESH", "0") != "0"

    def _should_catalog_inline_price_refresh(
        self,
        *,
        page: int | None = None,
        page_size: int | None = None,
    ) -> bool:
        raw_flag = os.getenv("CATALOG_INLINE_PRICE_REFRESH")
        if raw_flag is not None:
            return self._catalog_inline_price_refresh_enabled()

        try:
            default_enabled = os.getenv("CATALOG_INLINE_PRICE_REFRESH_DEFAULT", "1") != "0"
        except Exception:
            default_enabled = True
        if not default_enabled:
            return False

        try:
            max_page = max(1, int(os.getenv("CATALOG_INLINE_PRICE_REFRESH_MAX_PAGE", "3") or 3))
        except Exception:
            max_page = 3
        try:
            max_page_size = max(
                1,
                int(os.getenv("CATALOG_INLINE_PRICE_REFRESH_MAX_PAGE_SIZE", "24") or 24),
            )
        except Exception:
            max_page_size = 24

        try:
            page_num = max(1, int(page or 1))
        except Exception:
            page_num = 1
        try:
            size_num = max(1, int(page_size or 12))
        except Exception:
            size_num = 12
        return page_num <= max_page and size_num <= max_page_size

    def _build_list_conditions(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
        lines: Optional[List[str]] = None,
        source_key: Optional[str | List[str]] = None,
        q: Optional[str] = None,
        model: Optional[str] = None,
        generation: Optional[str] = None,
        color: Optional[str] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        mileage_min: Optional[int] = None,
        mileage_max: Optional[int] = None,
        reg_year_min: Optional[int] = None,
        reg_month_min: Optional[int] = None,
        reg_year_max: Optional[int] = None,
        reg_month_max: Optional[int] = None,
        body_type: Optional[str] = None,
        engine_type: Optional[str] = None,
        transmission: Optional[str] = None,
        drive_type: Optional[str] = None,
        num_seats: Optional[str] = None,
        doors_count: Optional[str] = None,
        emission_class: Optional[str] = None,
        efficiency_class: Optional[str] = None,
        climatisation: Optional[str] = None,
        airbags: Optional[str] = None,
        interior_design: Optional[str] = None,
        interior_color: Optional[str] = None,
        interior_material: Optional[str] = None,
        vat_reclaimable: Optional[str] = None,
        air_suspension: Optional[bool] = None,
        price_rating_label: Optional[str] = None,
        owners_count: Optional[str] = None,
        power_hp_min: Optional[float] = None,
        power_hp_max: Optional[float] = None,
        engine_cc_min: Optional[int] = None,
        engine_cc_max: Optional[int] = None,
        condition: Optional[str] = None,
        hide_no_local_photo: bool = False,
        exclude_fields: Optional[set[str]] = None,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        exclude = exclude_fields or set()
        conditions = [self._available_expr()]
        explicit_country = country

        if region and not country and region.upper() == "KR":
            country = "KR"

        if lines and "line" not in exclude:
            line_conditions = []
            brand_field = func.lower(func.trim(Car.brand))
            model_field = func.lower(func.trim(Car.model))
            variant_field = func.lower(func.trim(Car.variant))
            for line in lines:
                parts = [p.strip() for p in (line or "").split("|")]
                while len(parts) < 3:
                    parts.append("")
                b, m, v = parts[0], parts[1], parts[2]
                group = []
                norm_b = normalize_brand(b).strip().strip(".,;") if b else ""
                bentley_token = (
                    self._parse_bentley_power_model_token(m)
                    if norm_b.upper() == "BENTLEY" and m
                    else None
                )
                if b:
                    variants = brand_variants(norm_b) if norm_b else []
                    if variants:
                        group.append(
                            or_(*[brand_field.like(func.lower(f"%{item}%")) for item in variants])
                        )
                    else:
                        group.append(brand_field.like(func.lower(f"%{b}%")))
                if m:
                    if bentley_token is not None:
                        base_model, hp = bentley_token
                        group.append(self._normalized_model_expr() == model_lookup_key(base_model))
                        group.append(self._power_hp_bucket_expr() == hp)
                    else:
                        group.append(model_field.like(func.lower(f"%{m}%")))
                if v:
                    group.append(variant_field.like(func.lower(f"%{v}%")))
                if group:
                    line_conditions.append(and_(*group))
            if line_conditions:
                conditions.append(or_(*line_conditions))

        explicit_country_code = normalize_country_code(explicit_country) if explicit_country else None
        if country:
            c = normalize_country_code(country)
            if c == "EU":
                country = None
                if not region:
                    region = "EU"
            elif c == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                kr_conds = [Car.country.like("KR%")]
                if kr_sources:
                    kr_conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*kr_conds))
            elif c:
                conditions.append(Car.country == c)

        if region:
            r = region.upper()
            if r == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                conds = [Car.country.like("KR%")]
                if kr_sources:
                    conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*conds))
            elif r == "EU":
                # For explicit EU country routes like DE/NL the country predicate is already
                # selective enough. Keeping the broad source_id IN (...) here hurts the planner.
                if explicit_country_code not in self.EU_COUNTRIES:
                    eu_sources = self._source_ids_for_europe()
                    if eu_sources:
                        conditions.append(Car.source_id.in_(eu_sources))
                    else:
                        conditions.append(Car.country.in_(self.EU_COUNTRIES))

        if kr_type and "kr_type" not in exclude:
            kt_raw = str(kr_type).upper()
            kt = None
            if kt_raw in ("KR_INTERNAL", "DOMESTIC"):
                kt = "domestic"
            elif kt_raw in ("KR_IMPORT", "IMPORT"):
                kt = "import"
            if kt:
                conditions.append(func.lower(Car.kr_market_type) == kt)
                if not region and not country:
                    kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                    conds = [Car.country.like("KR%")]
                    if kr_sources:
                        conds.append(Car.source_id.in_(kr_sources))
                    conditions.append(or_(*conds))

        if brand and "brand" not in exclude:
            b = normalize_brand(brand).strip().strip(".,;")
            if b:
                variants = brand_variants(b)
                variants_lc = [v.lower() for v in variants if v]
                if variants_lc:
                    conditions.append(func.lower(func.trim(Car.brand)).in_(variants_lc))

        if "q" not in exclude:
            q, engine_type = canonicalize_free_text_filters(q=q, engine_type=engine_type)

        if q and "q" not in exclude:
            tokens = [t for t in re.split(r"[\s,]+", q.strip().lower()) if t]
            payload_text = func.lower(cast(Car.source_payload, String))
            token_groups = []
            fuel_map = {
                "дизель": ["diesel"],
                "дизельный": ["diesel"],
                "дизельные": ["diesel"],
                "дизельное": ["diesel"],
                "diesel": ["diesel"],
                "бензин": ["petrol", "gasoline", "benzin"],
                "бенз": ["petrol", "gasoline", "benzin"],
                "hybrid": ["hybrid"],
                "гибрид": ["hybrid"],
                "электро": ["electric", "ev"],
                "электр": ["electric", "ev"],
                "electric": ["electric", "ev"],
            }
            drive_tokens = {"4x4", "4х4", "4wd", "awd", "full", "полный", "полныйпривод"}
            for token in tokens:
                conds = []
                if token in fuel_map or token.startswith("дизел"):
                    mapped = fuel_map.get(token, fuel_map["дизель"])
                    for item in mapped:
                        conds.append(func.lower(Car.engine_type).like(f"%{item}%"))
                        conds.append(payload_text.like(f"%{item}%"))
                elif token in drive_tokens:
                    conds.append(func.lower(Car.drive_type).like("%awd%"))
                    conds.append(func.lower(Car.drive_type).like("%4wd%"))
                    conds.append(payload_text.like("%four-wheel%"))
                    conds.append(payload_text.like("%all wheel%"))
                    conds.append(payload_text.like("%4x4%"))
                elif token.startswith("панор") or token.startswith("panor"):
                    conds.append(payload_text.like("%panor%"))
                else:
                    like = f"%{token}%"
                    conds.extend(
                        [
                            func.lower(Car.brand).like(like),
                            func.lower(Car.model).like(like),
                            func.lower(Car.variant).like(like),
                            func.lower(Car.generation).like(like),
                            func.lower(Car.body_type).like(like),
                            func.lower(Car.engine_type).like(like),
                            func.lower(Car.transmission).like(like),
                            func.lower(Car.drive_type).like(like),
                            func.lower(Car.color).like(like),
                            payload_text.like(like),
                        ]
                    )
                if conds:
                    token_groups.append(or_(*conds))
            if token_groups:
                conditions.append(and_(*token_groups))

        if model and "model" not in exclude:
            clause = self._model_filter_clause(
                region=region,
                country=country,
                kr_type=kr_type,
                brand=brand,
                model=model,
            )
            if clause is not None:
                conditions.append(clause)

        payload_json = cast(Car.source_payload, JSONB)
        payload_text = func.lower(cast(Car.source_payload, String))

        if num_seats and "num_seats" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "num_seats") == str(num_seats))
        if doors_count and "doors_count" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "doors_count") == str(doors_count))
        if emission_class and "emission_class" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "emission_class") == emission_class)
        if efficiency_class and "efficiency_class" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "efficiency_class") == efficiency_class)
        if climatisation and "climatisation" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "climatisation") == climatisation)
        if airbags and "airbags" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "airbags") == airbags)
        interior_payload_expr = func.lower(
            func.coalesce(func.jsonb_extract_path_text(payload_json, "interior_design"), "")
        )
        interior_description_expr = func.lower(func.coalesce(Car.description, ""))

        def _interior_alias_clause(aliases: List[str]) -> Any:
            alias_conditions = []
            for alias in aliases:
                like = f"%{alias.lower()}%"
                alias_conditions.append(interior_payload_expr.like(like))
                alias_conditions.append(interior_description_expr.like(like))
            return or_(*alias_conditions) if alias_conditions else None
        if interior_design and "interior_design" not in exclude:
            trim_token_conditions = []
            for trim_value in split_csv_values(interior_design):
                trim_material_key, trim_color_key = parse_interior_trim_token(trim_value)
                trim_conditions = []
                if trim_material_key:
                    aliases = interior_material_aliases(trim_material_key)
                    clause = _interior_alias_clause(aliases)
                    if clause is not None:
                        trim_conditions.append(clause)
                if trim_color_key:
                    aliases = interior_color_aliases(trim_color_key)
                    clause = _interior_alias_clause(aliases)
                    if clause is not None:
                        trim_conditions.append(clause)
                if trim_conditions:
                    trim_token_conditions.append(and_(*trim_conditions))
                else:
                    trim_token_conditions.append(func.jsonb_extract_path_text(payload_json, "interior_design") == trim_value)
            if trim_token_conditions:
                conditions.append(or_(*trim_token_conditions))
        if interior_color and "interior_color" not in exclude:
            color_conditions = []
            for color_value in split_csv_values(interior_color):
                aliases = interior_color_aliases(color_value)
                clause = _interior_alias_clause(aliases)
                if clause is not None:
                    color_conditions.append(clause)
            if color_conditions:
                conditions.append(or_(*color_conditions))
        if interior_material and "interior_material" not in exclude:
            material_conditions = []
            for material_value in split_csv_values(interior_material):
                aliases = interior_material_aliases(material_value)
                clause = _interior_alias_clause(aliases)
                if clause is not None:
                    material_conditions.append(clause)
            if material_conditions:
                conditions.append(or_(*material_conditions))
        if vat_reclaimable and "vat_reclaimable" not in exclude:
            vat_raw = str(vat_reclaimable).strip().lower()
            vat_nt = func.jsonb_extract_path_text(payload_json, "price_eur_nt")
            vat_pct = func.jsonb_extract_path_text(payload_json, "vat")
            if vat_raw in {"1", "true", "yes", "y", "refund", "with", "возмещается"}:
                conditions.append(
                    or_(
                        vat_nt.is_not(None),
                        vat_pct.is_not(None),
                    )
                )
            elif vat_raw in {"0", "false", "no", "n", "without", "не возмещается"}:
                conditions.append(
                    and_(
                        vat_nt.is_(None),
                        vat_pct.is_(None),
                    )
                )
        if air_suspension and "air_suspension" not in exclude:
            conditions.append(
                or_(
                    payload_text.like("%air suspension%"),
                    payload_text.like("%air_suspension%"),
                    payload_text.like("%pneum%"),
                    payload_text.like("%пневмо%"),
                )
            )
        if price_rating_label and "price_rating_label" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "price_rating_label") == price_rating_label)
        if owners_count and "owners_count" not in exclude:
            conditions.append(func.jsonb_extract_path_text(payload_json, "owners_count") == str(owners_count))

        if generation and "generation" not in exclude:
            conditions.append(func.lower(Car.generation).like(func.lower(f"%{generation.strip()}%")))

        if color and "color" not in exclude:
            color_values = split_csv_values(color)
            color_conditions = []
            for color_value in color_values:
                group_key = normalize_color_group_key(color_value)
                if group_key:
                    if group_key == "other":
                        color_conditions.append(or_(Car.color_group == "other", Car.color_group.is_(None)))
                    else:
                        color_conditions.append(Car.color_group == group_key)
                    continue
                family_key = normalize_color_family_key(color_value)
                if family_key:
                    group_keys = color_family_group_keys(family_key)
                    if group_keys:
                        if "other" in group_keys:
                            named_groups = [group_key for group_key in group_keys if group_key != "other"]
                            if named_groups:
                                color_conditions.append(
                                    or_(
                                        Car.color_group.in_(named_groups),
                                        Car.color_group == "other",
                                        Car.color_group.is_(None),
                                    )
                                )
                            else:
                                color_conditions.append(
                                    or_(Car.color_group == "other", Car.color_group.is_(None))
                                )
                        else:
                            color_conditions.append(Car.color_group.in_(group_keys))
                        continue
                aliases = color_aliases(color_value)
                if aliases:
                    color_conditions.append(
                        or_(*[func.lower(Car.color).like(f"%{alias}%") for alias in aliases])
                    )
                else:
                    color_conditions.append(func.lower(Car.color) == color_value.lower())
            if color_conditions:
                conditions.append(or_(*color_conditions))

        if source_key and "source" not in exclude:
            keys: List[str] = []
            if isinstance(source_key, str):
                keys = [k.strip() for k in source_key.split(",") if k.strip()]
            else:
                keys = [k.strip() for k in source_key if k and k.strip()]
            if keys:
                src_ids = self.db.execute(select(Source.id).where(Source.key.in_(keys))).scalars().all()
                if src_ids:
                    conditions.append(Car.source_id.in_(src_ids))

        if (price_min is not None or price_max is not None) and "price" not in exclude:
            price_expr = self._display_price_rub_expr()
            conditions.append(price_expr.is_not(None))
            if price_min is not None:
                conditions.append(price_expr >= price_min)
            if price_max is not None:
                conditions.append(price_expr <= price_max)

        if year_min is not None and "year_min" not in exclude:
            conditions.append(Car.year >= year_min)
        if year_max is not None and "year_max" not in exclude:
            conditions.append(Car.year <= year_max)
        if mileage_min is not None and "mileage_min" not in exclude:
            conditions.append(Car.mileage >= mileage_min)
        if mileage_max is not None and "mileage_max" not in exclude:
            conditions.append(Car.mileage <= mileage_max)

        reg_year_expr = self._effective_registration_year_expr()
        reg_month_floor_expr = self._effective_registration_month_floor_expr()
        reg_month_ceil_expr = self._effective_registration_month_ceil_expr()

        if reg_year_min is not None and "reg_year_min" not in exclude:
            if reg_month_min is not None and "reg_month_min" not in exclude:
                conditions.append(
                    or_(
                        reg_year_expr > reg_year_min,
                        and_(
                            reg_year_expr == reg_year_min,
                            reg_month_floor_expr >= reg_month_min,
                        ),
                    )
                )
            else:
                conditions.append(reg_year_expr >= reg_year_min)

        if reg_year_max is not None and "reg_year_max" not in exclude:
            if reg_month_max is not None and "reg_month_max" not in exclude:
                conditions.append(
                    or_(
                        reg_year_expr < reg_year_max,
                        and_(
                            reg_year_expr == reg_year_max,
                            reg_month_ceil_expr <= reg_month_max,
                        ),
                    )
                )
            else:
                conditions.append(reg_year_expr <= reg_year_max)

        if body_type and "body_type" not in exclude:
            body_expr = func.lower(func.trim(Car.body_type))
            body_conditions = []
            for raw_body in split_csv_values(body_type):
                aliases = body_aliases(raw_body)
                if aliases:
                    body_conditions.append(or_(*[body_expr == alias.lower() for alias in aliases]))
                else:
                    body_conditions.append(body_expr == raw_body.lower())
            if body_conditions:
                conditions.append(or_(*body_conditions))
        if engine_type and "engine_type" not in exclude:
            engine_conditions = []
            for raw_engine in split_csv_values(engine_type):
                clause = self._fuel_filter_clause(raw_engine)
                if clause is not None:
                    engine_conditions.append(clause)
            if engine_conditions:
                conditions.append(or_(*engine_conditions))
        if transmission and "transmission" not in exclude:
            transmission_expr = func.lower(func.trim(Car.transmission))
            transmission_values = [value.lower() for value in split_csv_values(transmission)]
            if transmission_values:
                conditions.append(or_(*[transmission_expr == value for value in transmission_values]))
        if drive_type and "drive_type" not in exclude:
            drive_expr = func.lower(func.trim(Car.drive_type))
            drive_values = [value.lower() for value in split_csv_values(drive_type)]
            if drive_values:
                conditions.append(or_(*[drive_expr == value for value in drive_values]))
        if power_hp_min is not None and "power_hp_min" not in exclude:
            conditions.append(Car.power_hp >= power_hp_min)
        if power_hp_max is not None and "power_hp_max" not in exclude:
            conditions.append(Car.power_hp <= power_hp_max)
        if engine_cc_min is not None and "engine_cc_min" not in exclude:
            conditions.append(Car.engine_cc >= engine_cc_min)
        if engine_cc_max is not None and "engine_cc_max" not in exclude:
            conditions.append(Car.engine_cc <= engine_cc_max)
        if condition and "condition" not in exclude:
            cond = condition.strip().lower()
            if cond == "new":
                conditions.append(Car.mileage.is_not(None))
                conditions.append(Car.mileage <= 100)
            elif cond == "used":
                conditions.append(Car.mileage.is_not(None))
                conditions.append(Car.mileage > 100)

        strict_local_photo_mode = bool(hide_no_local_photo and (region or "").upper() == "EU")
        if strict_local_photo_mode:
            conditions.append(
                and_(
                    Car.thumbnail_local_path.is_not(None),
                    Car.thumbnail_local_path != "",
                )
            )

        return conditions, {
            "region": region,
            "country": country,
            "engine_type": engine_type,
            "q": q,
            "strict_local_photo_mode": strict_local_photo_mode,
        }

    def list_cars(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
        lines: Optional[List[str]] = None,
        source_key: Optional[str | List[str]] = None,
        q: Optional[str] = None,
        model: Optional[str] = None,
        generation: Optional[str] = None,
        color: Optional[str] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        mileage_min: Optional[int] = None,
        mileage_max: Optional[int] = None,
        reg_year_min: Optional[int] = None,
        reg_month_min: Optional[int] = None,
        reg_year_max: Optional[int] = None,
        reg_month_max: Optional[int] = None,
        body_type: Optional[str] = None,
        engine_type: Optional[str] = None,
        transmission: Optional[str] = None,
        drive_type: Optional[str] = None,
        num_seats: Optional[str] = None,
        doors_count: Optional[str] = None,
        emission_class: Optional[str] = None,
        efficiency_class: Optional[str] = None,
        climatisation: Optional[str] = None,
        airbags: Optional[str] = None,
        interior_design: Optional[str] = None,
        interior_color: Optional[str] = None,
        interior_material: Optional[str] = None,
        vat_reclaimable: Optional[str] = None,
        air_suspension: Optional[bool] = None,
        price_rating_label: Optional[str] = None,
        owners_count: Optional[str] = None,
        power_hp_min: Optional[float] = None,
        power_hp_max: Optional[float] = None,
        engine_cc_min: Optional[int] = None,
        engine_cc_max: Optional[int] = None,
        condition: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        light: bool = False,
        count_only: bool = False,
        use_fast_count: bool = True,
        hide_no_local_photo: bool = False,
    ) -> Tuple[List[Car] | List[dict], int]:
        normalized_color = normalize_csv_values(color) or color
        normalized_interior_design = normalize_csv_values(interior_design) or interior_design
        normalized_interior_color = normalize_csv_values(interior_color) or interior_color
        normalized_interior_material = normalize_csv_values(interior_material) or interior_material
        conditions, resolved = self._build_list_conditions(
            region=region,
            country=country,
            kr_type=kr_type,
            brand=brand,
            lines=lines,
            source_key=source_key,
            q=q,
            model=model,
            generation=generation,
            color=normalized_color,
            price_min=price_min,
            price_max=price_max,
            year_min=year_min,
            year_max=year_max,
            mileage_min=mileage_min,
            mileage_max=mileage_max,
            reg_year_min=reg_year_min,
            reg_month_min=reg_month_min,
            reg_year_max=reg_year_max,
            reg_month_max=reg_month_max,
            body_type=body_type,
            engine_type=engine_type,
            transmission=transmission,
            drive_type=drive_type,
            num_seats=num_seats,
            doors_count=doors_count,
            emission_class=emission_class,
            efficiency_class=efficiency_class,
            climatisation=climatisation,
            airbags=airbags,
            interior_design=normalized_interior_design,
            interior_color=normalized_interior_color,
            interior_material=normalized_interior_material,
            vat_reclaimable=vat_reclaimable,
            air_suspension=air_suspension,
            price_rating_label=price_rating_label,
            owners_count=owners_count,
            power_hp_min=power_hp_min,
            power_hp_max=power_hp_max,
            engine_cc_min=engine_cc_min,
            engine_cc_max=engine_cc_max,
            condition=condition,
            hide_no_local_photo=hide_no_local_photo,
        )
        region = resolved.get("region")
        country = resolved.get("country")
        engine_type = resolved.get("engine_type")
        q = resolved.get("q")
        strict_local_photo_mode = bool(resolved.get("strict_local_photo_mode"))

        where_expr = and_(*conditions) if conditions else None
        price_sensitive = (
            price_min is not None
            or price_max is not None
            or sort in {"price_asc", "price_desc"}
        )
        lazy_price_refresh_allowed = (
            not count_only
            and page <= 1
            and page_size <= 40
            and any([region, country, brand, model])
            and not any(
                [
                    lines,
                    source_key,
                    q,
                    generation,
                    normalized_color,
                    body_type,
                    engine_type,
                    transmission,
                    drive_type,
                    num_seats,
                    doors_count,
                    emission_class,
                    efficiency_class,
                    climatisation,
                    airbags,
                    normalized_interior_design,
                    normalized_interior_color,
                    normalized_interior_material,
                    vat_reclaimable,
                    air_suspension,
                    price_rating_label,
                    owners_count,
                    power_hp_min is not None,
                    power_hp_max is not None,
                    engine_cc_min is not None,
                    engine_cc_max is not None,
                    year_min is not None,
                    year_max is not None,
                    mileage_min is not None,
                    mileage_max is not None,
                    reg_year_min is not None,
                    reg_month_min is not None,
                    reg_year_max is not None,
                    reg_month_max is not None,
                    condition,
                    price_min is not None,
                    price_max is not None,
                    kr_type,
                ]
            )
        )
        if (
            where_expr is not None
            and price_sensitive
            and lazy_price_refresh_allowed
            and self._should_catalog_inline_price_refresh(page=page, page_size=page_size)
        ):
            self._refresh_price_sensitive_candidates(
                where_expr,
                sort=sort,
                page=page,
                page_size=page_size,
            )

        # cached count for repeated requests
        count_key = (
            region,
            country,
            brand,
            tuple(lines) if lines else None,
            tuple(source_key) if isinstance(source_key, list) else source_key,
            q,
            model,
            generation,
            normalized_color,
            price_min,
            price_max,
            mileage_min,
            mileage_max,
            reg_year_min,
            reg_month_min,
            reg_year_max,
            reg_month_max,
            body_type,
            engine_type,
            transmission,
            drive_type,
            num_seats,
            doors_count,
            emission_class,
            efficiency_class,
            climatisation,
            airbags,
            normalized_interior_design,
            normalized_interior_color,
            normalized_interior_material,
            vat_reclaimable,
            air_suspension,
            price_rating_label,
            owners_count,
            condition,
            kr_type,
            "1" if strict_local_photo_mode else "0",
        )
        redis_count_key = None
        total = self._count_cache.get(count_key)
        if total is None and os.getenv("CATALOG_REDIS_COUNT_CACHE", "1") != "0":
            redis_params = {
                "region": region,
                "country": country,
                "brand": brand,
                "model": model,
                "generation": generation,
                "color": normalized_color,
                "price_min": price_min,
                "price_max": price_max,
                "mileage_min": mileage_min,
                "mileage_max": mileage_max,
                "reg_year_min": reg_year_min,
                "reg_month_min": reg_month_min,
                "reg_year_max": reg_year_max,
                "reg_month_max": reg_month_max,
                "body_type": body_type,
                "engine_type": engine_type,
                "transmission": transmission,
                "drive_type": drive_type,
                "num_seats": num_seats,
                "doors_count": doors_count,
                "emission_class": emission_class,
                "efficiency_class": efficiency_class,
                "climatisation": climatisation,
                "airbags": airbags,
                "interior_design": normalized_interior_design,
                "interior_color": normalized_interior_color,
                "interior_material": normalized_interior_material,
                "vat_reclaimable": vat_reclaimable,
                "air_suspension": air_suspension,
                "price_rating_label": price_rating_label,
                "owners_count": owners_count,
                "condition": condition,
                "kr_type": kr_type,
                "q": q,
                "line": "|".join(lines or []),
                "source": ",".join(source_key) if isinstance(source_key, list) else source_key,
                "year_min": year_min,
                "year_max": year_max,
                "power_hp_min": power_hp_min,
                "power_hp_max": power_hp_max,
                "engine_cc_min": engine_cc_min,
                "engine_cc_max": engine_cc_max,
                "hide_no_local_photo": "1" if strict_local_photo_mode else "0",
            }
            redis_count_key = build_cars_count_key(redis_params)
            redis_total = redis_get_json(redis_count_key)
            if redis_total is not None:
                try:
                    total = int(redis_total)
                    self._count_cache[count_key] = total
                except Exception:
                    total = None
        total_t0 = time.perf_counter()
        if total is not None:
            if (use_fast_count and not strict_local_photo_mode) and total == 0 and self._can_fast_count(
                region=region,
                country=country,
                brand=brand,
                model=model,
                lines=lines,
                source_key=source_key,
                q=q,
                generation=generation,
                color=normalized_color,
                price_min=price_min,
                price_max=price_max,
                year_min=year_min,
                year_max=year_max,
                mileage_min=mileage_min,
                mileage_max=mileage_max,
                reg_year_min=reg_year_min,
                reg_month_min=reg_month_min,
                reg_year_max=reg_year_max,
                reg_month_max=reg_month_max,
                body_type=body_type,
                engine_type=engine_type,
                transmission=transmission,
                drive_type=drive_type,
                num_seats=num_seats,
                doors_count=doors_count,
                emission_class=emission_class,
                efficiency_class=efficiency_class,
                climatisation=climatisation,
                airbags=airbags,
                interior_design=interior_design,
                interior_color=interior_color,
                interior_material=interior_material,
                vat_reclaimable=vat_reclaimable,
                air_suspension=air_suspension,
                price_rating_label=price_rating_label,
                owners_count=owners_count,
                power_hp_min=power_hp_min,
                power_hp_max=power_hp_max,
                engine_cc_min=engine_cc_min,
                engine_cc_max=engine_cc_max,
                condition=condition,
                kr_type=kr_type,
            ):
                fast_total = self._fast_count(
                    region=region or "",
                    country=country,
                    brand=brand,
                    model=model,
                )
                if fast_total is not None and fast_total > 0:
                    total = fast_total
                    self._count_cache[count_key] = total
        if total is None:
            total = None
            if (use_fast_count and not strict_local_photo_mode) and self._can_fast_count(
                region=region,
                country=country,
                brand=brand,
                model=model,
                lines=lines,
                source_key=source_key,
                q=q,
                generation=generation,
                color=normalized_color,
                price_min=price_min,
                price_max=price_max,
                year_min=year_min,
                year_max=year_max,
                mileage_min=mileage_min,
                mileage_max=mileage_max,
                reg_year_min=reg_year_min,
                reg_month_min=reg_month_min,
                reg_year_max=reg_year_max,
                reg_month_max=reg_month_max,
                body_type=body_type,
                engine_type=engine_type,
                transmission=transmission,
                drive_type=drive_type,
                num_seats=num_seats,
                doors_count=doors_count,
                emission_class=emission_class,
                efficiency_class=efficiency_class,
                climatisation=climatisation,
                airbags=airbags,
                interior_design=interior_design,
                interior_color=interior_color,
                interior_material=interior_material,
                vat_reclaimable=vat_reclaimable,
                air_suspension=air_suspension,
                price_rating_label=price_rating_label,
                owners_count=owners_count,
                power_hp_min=power_hp_min,
                power_hp_max=power_hp_max,
                engine_cc_min=engine_cc_min,
                engine_cc_max=engine_cc_max,
                condition=condition,
                kr_type=kr_type,
            ):
                total = self._fast_count(
                    region=region or "",
                    country=country,
                    brand=brand,
                    model=model,
                )
            if total is None:
                total_stmt = select(func.count()).select_from(Car).where(where_expr)
                total = self.db.execute(total_stmt).scalar_one()
            self._count_cache[count_key] = total
            if redis_count_key:
                redis_set_json(redis_count_key, int(total), ttl_sec=1800)
            elapsed = time.perf_counter() - total_t0
            if elapsed > 2:
                self.logger.warning("count_slow total=%.3fs filters=%s", elapsed, count_key)
            elif elapsed > 1:
                self.logger.info("count_warn total=%.3fs filters=%s", elapsed, count_key)

        if count_only:
            return [], int(total or 0)

        order_clause = []
        if sort == "price_asc":
            price_expr = self._display_price_rub_expr()
            missing_price = price_expr.is_(None)
            order_clause = [
                missing_price.asc(),
                price_expr.asc().nullslast(),
                Car.id.asc(),
            ]
        elif sort == "price_desc":
            price_expr = self._display_price_rub_expr()
            missing_price = price_expr.is_(None)
            order_clause = [
                missing_price.asc(),
                price_expr.desc().nullslast(),
                Car.id.asc(),
            ]
        elif sort == "year_desc":
            order_clause = [Car.year.desc().nullslast(), Car.id.desc()]
        elif sort == "year_asc":
            order_clause = [Car.year.asc().nullslast(), Car.id.desc()]
        elif sort == "mileage_asc":
            order_clause = [Car.mileage.asc().nullslast(), Car.id.desc()]
        elif sort == "mileage_desc":
            order_clause = [Car.mileage.desc().nullslast(), Car.id.desc()]
        elif sort == "reg_desc":
            order_clause = [Car.reg_sort_key.desc().nullslast(), Car.id.desc()]
        elif sort == "reg_asc":
            order_clause = [Car.reg_sort_key.asc().nullslast(), Car.id.desc()]
        elif sort == "listing_desc":
            order_clause = [Car.listing_sort_ts.desc().nullslast(), Car.id.desc()]
        elif sort == "listing_asc":
            order_clause = [Car.listing_sort_ts.asc().nullslast(), Car.id.desc()]
        else:
            # default: цена сначала дешевые
            price_expr = self._display_price_rub_expr()
            order_clause = [price_expr.asc().nullslast(), Car.id.desc()]

        thumb_rank = case(
            (
                or_(
                    and_(Car.thumbnail_local_path.is_not(None), Car.thumbnail_local_path != ""),
                    and_(Car.thumbnail_url.is_not(None), Car.thumbnail_url != ""),
                ),
                1,
            ),
            else_=0,
        ).desc()
        # For large price sorts in light mode, avoid extra DB sorting by thumbnail rank
        # to keep first-page latency low. We'll push no-photo items to the end in-memory.
        use_thumb_rank = not light or sort not in ("price_asc", "price_desc")
        if light:
            stmt = (
                select(
                    Car.id,
                    Car.brand,
                    Car.model,
                    Car.variant,
                    Car.year,
                    Car.registration_year,
                    Car.registration_month,
                    Car.mileage,
                    Car.total_price_rub_cached,
                    Car.price_rub_cached,
                    Car.calc_breakdown_json,
                    Car.calc_updated_at,
                    Car.updated_at,
                    Car.spec_inferred_at,
                    Car.price,
                    Car.currency,
                    Car.thumbnail_url,
                    Car.thumbnail_local_path,
                    Car.country,
                    Car.source_id,
                    Car.color,
                    Car.body_type,
                    Car.engine_type,
                    Car.transmission,
                    Car.drive_type,
                    Car.engine_cc,
                    Car.power_hp,
                    Car.power_kw,
                    Car.inferred_engine_cc,
                    Car.inferred_power_hp,
                    Car.inferred_power_kw,
                )
                .where(where_expr)
                .order_by(*(([thumb_rank] if use_thumb_rank else [])), *order_clause)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        else:
            stmt = (
                select(Car)
                .where(where_expr)
                .order_by(thumb_rank, *order_clause)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        if os.environ.get("CAR_API_TIMING", "0") == "1" and os.environ.get("CAR_API_SQL", "0") == "1":
            try:
                compiled = stmt.compile(compile_kwargs={"literal_binds": True})
                print(f"API_CARS_SQL {compiled}", flush=True)
            except Exception:
                self.logger.exception("api_cars_sql_failed")
        items_t0 = time.perf_counter()
        if light:
            items = [dict(row) for row in self.db.execute(stmt).mappings().all()]
        else:
            items = list(self.db.execute(stmt).scalars().all())
        # Keep no-photo cards at the end without forcing expensive DB sort for light/price queries.
        try:
            if items and light:
                with_thumb = []
                without_thumb = []
                for row in items:
                    thumb = str(row.get("thumbnail_local_path") or row.get("thumbnail_url") or "").strip()
                    if thumb and thumb != "/static/img/no-photo.svg":
                        with_thumb.append(row)
                    else:
                        without_thumb.append(row)
                if without_thumb:
                    items = with_thumb + without_thumb
        except Exception:
            self.logger.exception("reorder_no_photo_failed")
        # Guard against stale/undercounted fast_count: ensure total >= offset+items
        try:
            offset = (page - 1) * page_size
            if total is not None and total < (offset + len(items)):
                total_stmt = select(func.count()).select_from(Car).where(where_expr)
                total = self.db.execute(total_stmt).scalar_one()
                self._count_cache[count_key] = total
        except Exception:
            self.logger.exception("count_recheck_failed")
        elapsed_items = time.perf_counter() - items_t0
        if elapsed_items > 2:
            self.logger.warning(
                "list_slow total=%.3fs sort=%s page=%s size=%s filters=%s",
                elapsed_items,
                sort,
                page,
                page_size,
                count_key,
            )
        elif elapsed_items > 1:
            self.logger.info(
                "list_warn total=%.3fs sort=%s page=%s size=%s filters=%s",
                elapsed_items,
                sort,
                page,
                page_size,
                count_key,
            )
        if items:
            if light:
                for row in items:
                    if not isinstance(row, dict):
                        continue
                    row["engine_cc"] = effective_engine_cc_value(row)
                    row["power_hp"] = effective_power_hp_value(row)
                    row["power_kw"] = effective_power_kw_value(row)
            if self._should_catalog_inline_price_refresh(page=page, page_size=page_size):
                try:
                    if light:
                        self._lazy_recalc_light_items(items)
                    else:
                        self._lazy_recalc_items(items)
                except Exception:
                    self.logger.exception("lazy_recalc_failed")
        return items, total

    def _extract_breakdown_version(self, breakdown: list[dict], title: str) -> str | None:
        for row in breakdown or []:
            if row.get("title") == title:
                return row.get("version")
        return None

    def _fx_signature(self, rates: dict[str, Any] | None = None) -> str | None:
        fx = rates if rates is not None else (self.get_fx_rates() or {})
        try:
            eur = float(fx.get("EUR") or 0)
            usd = float(fx.get("USD") or 0)
        except Exception:
            return None
        if eur <= 0 and usd <= 0:
            return None
        return f"eur:{eur:.4f}|usd:{usd:.4f}"

    def _load_lazy_recalc_versions(self) -> tuple[bool, str | None, str | None, str | None]:
        lazy_enabled = os.getenv("LAZY_RECALC_ENABLED", "1") != "0"
        if not lazy_enabled:
            return False, None, None, None
        customs_version = None
        try:
            from backend.app.services.customs_config import get_customs_config

            customs_version = get_customs_config().version
        except Exception:
            customs_version = None

        cfg_version = None
        try:
            cfg_svc = CalculatorConfigService(self.db)
            cfg = None
            yaml_paths = [
                Path("/app/backend/app/config/calculator.yml"),
                Path("/app/config/calculator.yml"),
                Path(__file__).resolve().parent.parent / "config" / "calculator.yml",
            ]
            for p in yaml_paths:
                cfg = cfg_svc.ensure_default_from_yaml(p)
                if cfg:
                    break
            if cfg:
                cfg_version = cfg.payload.get("meta", {}).get("version")
        except Exception:
            cfg_version = None
        return lazy_enabled, cfg_version, customs_version, self._fx_signature()

    def _needs_recalc_for_versions(
        self,
        record: Any,
        cfg_version: str | None,
        customs_version: str | None,
        fx_signature: str | None,
        *,
        lazy_enabled: bool,
    ) -> bool:
        if not lazy_enabled:
            return False
        total_price_rub_cached = _record_value(record, "total_price_rub_cached")
        calc_breakdown_json = _record_value(record, "calc_breakdown_json")
        calc_updated_at = _record_value(record, "calc_updated_at")
        updated_at = _record_value(record, "updated_at")
        spec_inferred_at = _record_value(record, "spec_inferred_at")
        if total_price_rub_cached is None or calc_breakdown_json is None:
            return True
        try:
            if float(total_price_rub_cached) <= 0:
                raw_price = _record_value(record, "price")
                price_rub_cached = _record_value(record, "price_rub_cached")
                if (
                    (raw_price is not None and float(raw_price) > 0)
                    or (price_rub_cached is not None and float(price_rub_cached) > 0)
                ):
                    return True
        except Exception:
            return True
        if calc_updated_at is not None and updated_at is not None:
            if calc_updated_at < updated_at:
                return True
        if calc_updated_at is not None and spec_inferred_at is not None:
            if calc_updated_at < spec_inferred_at:
                return True
        breakdown = calc_breakdown_json or []
        if customs_version and self._extract_breakdown_version(breakdown, "__customs_version") != customs_version:
            return True
        if cfg_version and self._extract_breakdown_version(breakdown, "__config_version") != cfg_version:
            return True
        if fx_signature and self._extract_breakdown_version(breakdown, "__fx_signature") != fx_signature:
            return True
        return False

    def _lazy_recalc_items(self, items: List[Car]) -> None:
        lazy_enabled, cfg_version, customs_version, fx_signature = self._load_lazy_recalc_versions()
        if not lazy_enabled:
            return

        for car in items:
            if self._needs_recalc_for_versions(
                car,
                cfg_version,
                customs_version,
                fx_signature,
                lazy_enabled=lazy_enabled,
            ):
                try:
                    self.ensure_calc_cache(car, force=True)
                except Exception:
                    self.logger.exception("lazy_recalc_item_failed car=%s", getattr(car, "id", None))

    def _merge_light_row_from_car(self, row: Dict[str, Any], car: Car) -> None:
        row["brand"] = car.brand
        row["model"] = car.model
        row["variant"] = car.variant
        row["year"] = car.year
        row["registration_year"] = car.registration_year
        row["registration_month"] = car.registration_month
        row["mileage"] = car.mileage
        row["price"] = car.price
        row["currency"] = car.currency
        row["country"] = car.country
        row["source_id"] = car.source_id
        row["thumbnail_url"] = car.thumbnail_url
        row["thumbnail_local_path"] = car.thumbnail_local_path
        row["color"] = car.color
        row["body_type"] = car.body_type
        row["engine_type"] = car.engine_type
        row["transmission"] = car.transmission
        row["drive_type"] = car.drive_type
        row["engine_cc"] = car.engine_cc
        row["power_hp"] = car.power_hp
        row["power_kw"] = car.power_kw
        row["inferred_engine_cc"] = car.inferred_engine_cc
        row["inferred_power_hp"] = car.inferred_power_hp
        row["inferred_power_kw"] = car.inferred_power_kw
        row["engine_cc"] = effective_engine_cc_value(row)
        row["power_hp"] = effective_power_hp_value(row)
        row["power_kw"] = effective_power_kw_value(row)
        row["total_price_rub_cached"] = car.total_price_rub_cached
        row["price_rub_cached"] = car.price_rub_cached
        row["calc_breakdown_json"] = car.calc_breakdown_json
        row["calc_updated_at"] = car.calc_updated_at
        row["updated_at"] = car.updated_at
        row["spec_inferred_at"] = car.spec_inferred_at

    def sync_light_rows_from_db(self, items: List[Dict[str, Any]], *, refresh_prices: bool = False) -> int:
        if not items:
            return 0
        ids: list[int] = []
        seen_ids: set[int] = set()
        for row in items:
            if not isinstance(row, dict):
                continue
            car_id = row.get("id")
            if not isinstance(car_id, int) or car_id in seen_ids:
                continue
            ids.append(car_id)
            seen_ids.add(car_id)
        if not ids:
            return 0
        cars = self.db.execute(select(Car).where(Car.id.in_(ids))).scalars().all()
        cars_by_id = {car.id: car for car in cars}
        refreshed = 0
        if refresh_prices:
            _, cfg_version, customs_version, fx_signature = self._load_lazy_recalc_versions()
            for car_id in ids:
                car = cars_by_id.get(car_id)
                if car is None:
                    continue
                if not self._needs_visible_price_refresh(
                    car,
                    cfg_version,
                    customs_version,
                    fx_signature,
                ):
                    continue
                try:
                    before_total = car.total_price_rub_cached
                    before_calc = car.calc_updated_at
                    self.ensure_calc_cache(car, force=False)
                    if car.total_price_rub_cached != before_total or car.calc_updated_at != before_calc:
                        refreshed += 1
                except Exception:
                    self.logger.exception("visible_price_refresh_failed car=%s", car_id)
        merged = 0
        for row in items:
            if not isinstance(row, dict):
                continue
            car_id = row.get("id")
            if not isinstance(car_id, int):
                continue
            car = cars_by_id.get(car_id)
            if car is None:
                continue
            self._merge_light_row_from_car(row, car)
            merged += 1
        if refreshed:
            self.logger.info(
                "visible_price_refresh refreshed=%s items=%s",
                refreshed,
                len(ids),
            )
        return refreshed

    def _lazy_recalc_light_items(self, items: List[Dict[str, Any]]) -> None:
        lazy_enabled, cfg_version, customs_version, fx_signature = self._load_lazy_recalc_versions()
        if not lazy_enabled:
            return
        candidate_ids: list[int] = []
        for row in items:
            if not isinstance(row, dict):
                continue
            if self._needs_recalc_for_versions(
                row,
                cfg_version,
                customs_version,
                fx_signature,
                lazy_enabled=lazy_enabled,
            ):
                car_id = row.get("id")
                if isinstance(car_id, int):
                    candidate_ids.append(car_id)
        if not candidate_ids:
            return
        cars = (
            self.db.execute(select(Car).where(Car.id.in_(candidate_ids)))
            .scalars()
            .all()
        )
        cars_by_id = {car.id: car for car in cars}
        for car_id in candidate_ids:
            car = cars_by_id.get(car_id)
            if car is None:
                continue
            try:
                self.ensure_calc_cache(car, force=True)
            except Exception:
                self.logger.exception("lazy_recalc_light_item_failed car=%s", car_id)
        for row in items:
            if not isinstance(row, dict):
                continue
            car_id = row.get("id")
            if not isinstance(car_id, int):
                continue
            car = cars_by_id.get(car_id)
            if car is None:
                continue
            self._merge_light_row_from_car(row, car)

    def _needs_visible_price_refresh(
        self,
        record: Any,
        cfg_version: str | None,
        customs_version: str | None,
        fx_signature: str | None,
    ) -> bool:
        if self._needs_recalc_for_versions(
            record,
            cfg_version,
            customs_version,
            fx_signature,
            lazy_enabled=True,
        ):
            return True
        breakdown = _record_value(record, "calc_breakdown_json") or []
        has_without_util_marker = any(
            isinstance(row, dict) and row.get("title") == "__without_util_fee"
            for row in breakdown
        )
        if not has_without_util_marker:
            return False
        effective_engine_cc = effective_engine_cc_value(record)
        effective_power_hp = effective_power_hp_value(record)
        effective_power_kw = effective_power_kw_value(record)
        electric = is_bev(
            effective_engine_cc,
            float(effective_power_kw) if effective_power_kw is not None else None,
            float(effective_power_hp) if effective_power_hp is not None else None,
            _record_value(record, "engine_type"),
            brand=_record_value(record, "brand"),
            model=_record_value(record, "model"),
            variant=_record_value(record, "variant"),
            text_hint=electric_vehicle_hint_text(record),
        )
        if electric:
            return effective_power_hp is not None or effective_power_kw is not None
        return effective_engine_cc is not None and (
            effective_power_hp is not None or effective_power_kw is not None
        )

    def refresh_visible_price_cache(self, items: List[Any]) -> int:
        return self.sync_light_rows_from_db(
            [item for item in items if isinstance(item, dict)],
            refresh_prices=True,
        )

    def _refresh_price_sensitive_candidates(
        self,
        where_expr: Any,
        *,
        sort: Optional[str],
        page: int,
        page_size: int,
    ) -> int:
        lazy_enabled = os.getenv("LAZY_RECALC_ENABLED", "1") != "0"
        if not lazy_enabled:
            return 0
        try:
            batch_size = max(
                int(os.getenv("PRICE_SENSITIVE_RECALC_BATCH", "120")),
                max(1, int(page or 1)) * max(1, int(page_size or 20)),
            )
        except Exception:
            batch_size = max(120, max(1, int(page or 1)) * max(1, int(page_size or 20)))
        price_expr = self._display_price_rub_expr()
        if sort == "price_desc":
            order_clause = [price_expr.desc().nullslast(), Car.id.asc()]
        else:
            order_clause = [price_expr.asc().nullslast(), Car.id.asc()]
        candidate_ids = [
            int(car_id)
            for car_id in self.db.execute(
                select(Car.id)
                .where(where_expr)
                .order_by(*order_clause)
                .limit(batch_size)
            ).scalars().all()
            if car_id is not None
        ]
        if not candidate_ids:
            return 0
        cars = (
            self.db.execute(select(Car).where(Car.id.in_(candidate_ids)))
            .scalars()
            .all()
        )
        cars_by_id = {car.id: car for car in cars}
        refreshed = 0
        for car_id in candidate_ids:
            car = cars_by_id.get(car_id)
            if car is None:
                continue
            try:
                before_total = car.total_price_rub_cached
                before_calc = car.calc_updated_at
                self.ensure_calc_cache(car, force=False)
                if car.total_price_rub_cached != before_total or car.calc_updated_at != before_calc:
                    refreshed += 1
            except Exception:
                self.logger.exception("price_sensitive_recalc_failed car=%s", car_id)
        if refreshed:
            self.logger.info(
                "price_sensitive_recalc refreshed=%s sort=%s page=%s size=%s",
                refreshed,
                sort,
                page,
                page_size,
            )
        return refreshed

    def _maybe_infer_specs_for_calc(self, car: Car) -> bool:
        effective_engine_cc = effective_engine_cc_value(car)
        effective_power_hp = effective_power_hp_value(car)
        effective_power_kw = effective_power_kw_value(car)
        if effective_engine_cc is not None and (effective_power_hp is not None or effective_power_kw is not None):
            return False
        try:
            from .car_spec_inference_service import CarSpecInferenceService
        except Exception:
            self.logger.exception("calc_infer_import_failed car=%s", getattr(car, "id", None))
            return False
        try:
            year_window = max(0, int(os.getenv("SPEC_INFERENCE_YEAR_WINDOW", "2")))
        except Exception:
            year_window = 2
        try:
            inference_service = CarSpecInferenceService(self.db)
            inference = inference_service.infer_specs_for_car(car, year_window=year_window)
        except Exception:
            self.logger.exception("calc_infer_lookup_failed car=%s", getattr(car, "id", None))
            return False
        if not inference:
            return False
        try:
            inference_service._apply_inferred_specs(car, inference)
            self.db.commit()
            self.logger.info(
                "calc_inferred_specs car=%s rule=%s confidence=%s",
                getattr(car, "id", None),
                inference.get("rule"),
                inference.get("confidence"),
            )
            return True
        except Exception:
            self.db.rollback()
            self.logger.exception("calc_infer_apply_failed car=%s", getattr(car, "id", None))
            return False

    def ensure_calc_cache(self, car: Car, *, force: bool = False) -> dict | None:
        if not car:
            return None
        lazy_enabled = os.getenv("LAZY_RECALC_ENABLED", "1") != "0"
        customs_version = None
        cfg_version: str | None = None
        eur_rate: float | None = None
        usd_rate: float | None = None
        fx_signature: str | None = None
        try:
            from backend.app.services.customs_config import get_customs_config

            customs_version = get_customs_config().version
        except Exception:
            customs_version = None

        def _extract_version(breakdown: list[dict], title: str) -> str | None:
            for row in breakdown or []:
                if row.get("title") == title:
                    return row.get("version")
            return None

        def _upsert_version(breakdown: list[dict], title: str, version: str) -> None:
            if not version:
                return
            for row in breakdown:
                if row.get("title") == title:
                    row["version"] = version
                    return
            breakdown.append({"title": title, "amount_rub": 0, "version": version})

        def _has_without_util_marker(breakdown: list[dict] | None) -> bool:
            return any(
                isinstance(row, dict) and row.get("title") == "__without_util_fee"
                for row in (breakdown or [])
            )

        def _needs_recalc(cfg_version: str | None) -> bool:
            if not lazy_enabled:
                return False
            if car.total_price_rub_cached is None or car.calc_breakdown_json is None:
                return True
            try:
                if float(car.total_price_rub_cached) <= 0 and (
                    (car.price is not None and float(car.price) > 0)
                    or (car.price_rub_cached is not None and float(car.price_rub_cached) > 0)
                ):
                    return True
            except Exception:
                return True
            if car.calc_updated_at is not None and car.updated_at is not None:
                if car.calc_updated_at < car.updated_at:
                    return True
            if car.calc_updated_at is not None and car.spec_inferred_at is not None:
                if car.calc_updated_at < car.spec_inferred_at:
                    return True
            breakdown = car.calc_breakdown_json or []
            if customs_version and _extract_version(breakdown, "__customs_version") != customs_version:
                return True
            if cfg_version and _extract_version(breakdown, "__config_version") != cfg_version:
                return True
            if fx_signature and _extract_version(breakdown, "__fx_signature") != fx_signature:
                return True
            try:
                total_cached = float(car.total_price_rub_cached or 0)
                price_cached = float(car.price_rub_cached or 0)
            except Exception:
                total_cached = 0
                price_cached = 0
            if (
                total_cached > 0
                and price_cached > 0
                and abs(total_cached - price_cached) < 1
                and not _has_without_util_marker(breakdown)
            ):
                effective_engine_cc_local = effective_engine_cc_value(car)
                effective_power_hp_local = effective_power_hp_value(car)
                effective_power_kw_local = effective_power_kw_value(car)
                has_power_local = bool(
                    (effective_power_hp_local is not None and float(effective_power_hp_local) > 0)
                    or (effective_power_kw_local is not None and float(effective_power_kw_local) > 0)
                )
                if has_power_local and (
                    effective_engine_cc_local is None
                    or is_bev(
                        effective_engine_cc_local,
                        float(effective_power_kw_local) if effective_power_kw_local is not None else None,
                        float(effective_power_hp_local) if effective_power_hp_local is not None else None,
                        car.engine_type,
                        brand=car.brand,
                        model=car.model,
                        variant=car.variant,
                        text_hint=electric_vehicle_hint_text(car),
                    )
                ):
                    return True
            return False

        def _fallback_total(reason: str) -> dict | None:
            # Derive fallback from current source price and current FX first; only then use cached RUB.
            fx_local = self.get_fx_rates() or {}
            eur = fx_local.get("EUR") or eur_rate or 95.0
            usd = fx_local.get("USD") or usd_rate or 85.0
            cny = fx_local.get("CNY") or 12.0
            total = None
            cur_local = str(car.currency or "EUR").strip().upper()
            if car.price is not None and float(car.price) > 0:
                if cur_local == "EUR":
                    total = float(car.price) * float(eur)
                elif cur_local == "USD":
                    total = float(car.price) * float(usd)
                elif cur_local == "CNY":
                    total = float(car.price) * float(cny)
                elif cur_local in ("RUB", "₽"):
                    total = float(car.price)
            elif car.price_rub_cached is not None and float(car.price_rub_cached) > 0:
                total = float(car.price_rub_cached)
            if total is None or total <= 0:
                return None
            car.total_price_rub_cached = total
            if car.calc_breakdown_json is None:
                car.calc_breakdown_json = []
            _upsert_version(car.calc_breakdown_json, "__without_util_fee", "1")
            _upsert_version(car.calc_breakdown_json, "__config_version", cfg_version or "")
            _upsert_version(car.calc_breakdown_json, "__customs_version", customs_version or "")
            _upsert_version(car.calc_breakdown_json, "__fx_signature", fx_signature or "")
            car.calc_updated_at = datetime.utcnow()
            self.db.commit()
            self.logger.info("calc_fallback_total car=%s reason=%s", car.id, reason)
            return {"total_rub": total, "breakdown": car.calc_breakdown_json or []}

        def _calc_kr_age_bucket(*, bev: bool) -> str:
            if bev:
                return "electric"
            if reg_fallback_missing:
                return "under_3"
            try:
                reg_dt = datetime(int(reg_year), int(reg_month), 1)
                now_dt = datetime.utcnow().replace(day=1)
                age_months = max(0, (now_dt.year - reg_dt.year) * 12 + (now_dt.month - reg_dt.month))
            except Exception:
                return "under_3"
            return "under_3" if age_months < 36 else "3_5"

        def _calculate_kr_total(*, bev: bool) -> dict | None:
            base_rub = raw_price_to_rub(
                used_price,
                used_currency,
                fx_eur=eur_rate,
                fx_usd=usd_rate,
                fx_cny=cny_rate,
            )
            if base_rub is None and car.price_rub_cached is not None:
                base_rub = float(car.price_rub_cached)
            if base_rub is None:
                return None

            commission_rub = float(base_rub) * 0.03
            util_rub: int | None = None
            without_util_fee = False
            age_bucket = _calc_kr_age_bucket(bev=bev)
            engine_cc_val = int(effective_engine_cc) if effective_engine_cc is not None else None
            power_hp_val = float(effective_power_hp) if effective_power_hp is not None else None
            power_kw_val = float(effective_power_kw) if effective_power_kw is not None else None
            has_power = bool((power_kw_val and power_kw_val > 0) or (power_hp_val and power_hp_val > 0))

            try:
                customs_cfg = get_customs_config()
                if bev:
                    if has_power:
                        util_rub = calc_util_fee_rub(
                            engine_cc=engine_cc_val or 0,
                            kw=power_kw_val,
                            hp=int(power_hp_val) if power_hp_val is not None else None,
                            cfg=customs_cfg,
                            age_bucket="electric",
                        )
                    else:
                        without_util_fee = True
                else:
                    if engine_cc_val is not None and has_power:
                        util_rub = calc_util_fee_rub(
                            engine_cc=engine_cc_val,
                            kw=power_kw_val,
                            hp=int(power_hp_val) if power_hp_val is not None else None,
                            cfg=customs_cfg,
                            age_bucket=age_bucket,
                        )
                    elif engine_cc_val is not None:
                        util_rub = int(legacy_util_fee_rub(engine_cc_val, age_bucket))
                    else:
                        without_util_fee = True
            except Exception:
                self.logger.exception("calc_kr_util_failed car=%s src=%s", car.id, getattr(car.source, "key", None))
                without_util_fee = True
                util_rub = None

            total_rub = float(base_rub) + float(commission_rub) + float(util_rub or 0)
            total_rub = float(ceil_to_step(total_rub, get_round_step_rub()) or total_rub)
            breakdown = [
                {"title": "emavto_price", "amount": float(base_rub), "currency": "RUB"},
                {"title": "kr_commission", "amount": float(commission_rub), "currency": "RUB"},
            ]
            if util_rub is not None:
                breakdown.append({"title": "util_fee", "amount": int(util_rub), "currency": "RUB"})
            breakdown.append({"title": "total_rub", "amount": total_rub, "currency": "RUB"})
            return {
                "scenario": f"kr_{age_bucket}",
                "total_rub": total_rub,
                "breakdown": breakdown,
                "euro_rate_used": float(eur_rate),
                "without_util_fee": without_util_fee,
            }
        # базовые цены из source_payload
        payload = car.source_payload or {}
        price_gross = payload.get("price_eur")
        price_net = payload.get("price_eur_nt")
        vat_pct = payload.get("vat")
        used_price = None
        used_currency = "EUR"
        vat_reclaim = False
        if price_net is not None:
            used_price = float(price_net)
            try:
                vat_reclaim = bool(
                    (vat_pct is not None and float(vat_pct) > 0)
                    or (price_gross is not None and float(price_net) < float(price_gross))
                )
            except Exception:
                vat_reclaim = True
        elif price_gross is not None:
            used_price = float(price_gross)
        else:
            used_price = car.price
            used_currency = car.currency or "EUR"
        if used_price is None or float(used_price) <= 0:
            self.logger.info("calc_skip_no_price car=%s src=%s", car.id, getattr(car.source, "key", None))
            return _fallback_total("no_price")
        reg_fallback_missing = False
        fallback_reg_year, fallback_reg_month = get_missing_registration_default()
        # Business rule: if registration date is missing, treat the car as registered from the fallback period.
        if car.registration_year and car.registration_month:
            reg_year = int(car.registration_year)
            reg_month = int(car.registration_month)
        else:
            reg_year = fallback_reg_year
            reg_month = fallback_reg_month
            reg_fallback_missing = True
        # кеш
        cfg_svc = CalculatorConfigService(self.db)
        cfg = None
        yaml_paths = [
            Path("/app/backend/app/config/calculator.yml"),
            Path("/app/config/calculator.yml"),
            Path(__file__).resolve().parent.parent / "config" / "calculator.yml",
        ]
        for p in yaml_paths:
            cfg = cfg_svc.ensure_default_from_yaml(p)
            if cfg:
                break
        if not cfg:
            # fallback to legacy Excel bootstrap only if YAML is missing
            base_paths = [
                Path("/app/Калькулятор Авто под заказ.xlsx"),
                Path("/mnt/data/Калькулятор Авто под заказ.xlsx"),
                Path(__file__).resolve().parent.parent / "resources" / "Калькулятор Авто под заказ.xlsx",
            ]
            for p in base_paths:
                cfg = cfg_svc.ensure_default_from_path(p)
                if cfg:
                    break
        if not cfg:
            return None
        cfg_version = cfg.payload.get("meta", {}).get("version")
        fx = self.get_fx_rates() or {}
        eur_rate = fx.get("EUR") or cfg.payload.get("meta", {}).get("eur_rate_default") or 95.0
        usd_rate = fx.get("USD") or cfg.payload.get("meta", {}).get("usd_rate_default") or 85.0
        cny_rate = fx.get("CNY") or 12.0
        fx_signature = self._fx_signature({"EUR": eur_rate, "USD": usd_rate, "CNY": cny_rate})
        effective_engine_cc = effective_engine_cc_value(car)
        effective_power_hp = effective_power_hp_value(car)
        effective_power_kw = effective_power_kw_value(car)
        fallback_cache_might_be_recoverable = _has_without_util_marker(car.calc_breakdown_json) and (
            effective_engine_cc is None or (effective_power_hp is None and effective_power_kw is None)
        )
        if (
            not force
            and car.total_price_rub_cached is not None
            and car.calc_breakdown_json is not None
            and car.calc_updated_at is not None
            and car.updated_at is not None
            and car.calc_updated_at >= car.updated_at
            and not _needs_recalc(cfg_version)
            and not fallback_cache_might_be_recoverable
        ):
            return {
                "total_rub": float(car.total_price_rub_cached),
                "breakdown": car.calc_breakdown_json or [],
                "vat_reclaim": vat_reclaim,
                "used_price": used_price,
                "used_currency": used_currency,
            }
        cur = str(used_currency or "EUR").strip().upper()
        price_net_eur = None
        if cur == "EUR":
            price_net_eur = used_price
        elif cur in ("RUB", "₽"):
            if eur_rate:
                price_net_eur = float(used_price) / float(eur_rate)
        elif cur == "USD":
            if eur_rate and usd_rate:
                price_net_eur = float(used_price) * (float(usd_rate) / float(eur_rate))
        elif cur == "CNY":
            if eur_rate and cny_rate:
                price_net_eur = float(used_price) * (float(cny_rate) / float(eur_rate))
        if price_net_eur is None:
            return _fallback_total("no_price_net_eur")
        if (
            effective_engine_cc is None
            or (effective_power_hp is None and effective_power_kw is None)
        ) and self._maybe_infer_specs_for_calc(car):
            effective_engine_cc = effective_engine_cc_value(car)
            effective_power_hp = effective_power_hp_value(car)
            effective_power_kw = effective_power_kw_value(car)
        engine_type = (car.engine_type or "").lower()
        is_electric = is_bev(
            effective_engine_cc,
            float(effective_power_kw) if effective_power_kw is not None else None,
            float(effective_power_hp) if effective_power_hp is not None else None,
            car.engine_type,
            brand=car.brand,
            model=car.model,
            variant=car.variant,
            text_hint=electric_vehicle_hint_text(car),
        )
        is_korea = str(car.country or "").upper().startswith("KR")
        if is_korea:
            result = _calculate_kr_total(bev=is_electric)
            if result is None:
                self.logger.info("calc_skip_kr_no_base_price car=%s src=%s", car.id, getattr(car.source, "key", None))
                return _fallback_total("kr_no_base_price")
        else:
            result = None
        if is_electric and not (effective_power_hp or effective_power_kw):
            if result is None:
                self.logger.info("calc_skip_no_power car=%s src=%s", car.id, getattr(car.source, "key", None))
                return _fallback_total("no_power")
        if not is_electric and not effective_engine_cc:
            if result is None:
                self.logger.info("calc_skip_no_cc car=%s src=%s", car.id, getattr(car.source, "key", None))
                return _fallback_total("no_engine_cc")
        if result is None:
            scenario = None
            if is_electric:
                scenario = "electric"
            elif reg_fallback_missing:
                scenario = "under_3"
            req = EstimateRequest(
                scenario=scenario,
                price_net_eur=price_net_eur,
                eur_rate=eur_rate,
                engine_cc=effective_engine_cc,
                power_hp=float(effective_power_hp) if effective_power_hp is not None else None,
                power_kw=float(effective_power_kw) if effective_power_kw is not None else None,
                is_electric=is_electric,
                reg_year=reg_year,
                reg_month=reg_month,
            )
            try:
                result = calculate(cfg.payload, req)
            except Exception:
                self.logger.exception("calc_failed car=%s src=%s", car.id, getattr(car.source, "key", None))
                return _fallback_total("calc_failed")
        display = []
        label_map = cfg.payload.get("label_map", {})
        for item in result.get("breakdown", []):
            title = item.get("title") or ""
            if "итого" in title.lower():
                continue
            cur = (item.get("currency") or "RUB").upper()
            amt = float(item.get("amount") or 0)
            rub = amt
            if cur == "EUR" and eur_rate:
                rub = amt * eur_rate
            display.append({
                "title": label_map.get(title, label_for(title)),
                "amount_rub": rub,
            })
        if cfg_version:
            _upsert_version(display, "__config_version", cfg_version)
        if customs_version:
            _upsert_version(display, "__customs_version", customs_version)
        if fx_signature:
            _upsert_version(display, "__fx_signature", fx_signature)
        if result.get("without_util_fee"):
            _upsert_version(display, "__without_util_fee", "1")
        total_rub = float(result.get("total_rub") or 0)
        car.total_price_rub_cached = total_rub
        car.calc_breakdown_json = display
        car.calc_updated_at = datetime.utcnow()
        self.db.commit()
        return {"total_rub": total_rub, "breakdown": display, "vat_reclaim": vat_reclaim, "used_price": used_price, "used_currency": used_currency}

    def get_car(self, car_id: int) -> Optional[Car]:
        stmt = select(Car).options(selectinload(Car.images)).where(Car.id == car_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def brands(self, country: Optional[str] = None) -> List[str]:
        filters: Dict[str, Any] = {"country": country} if country else {}
        rows = self.facet_counts(field="brand", filters=filters)
        brands = []
        seen: set[str] = set()
        for row in rows:
            val = row.get("value")
            if not val:
                continue
            norm = normalize_brand(val)
            if not norm or norm in seen:
                continue
            brands.append(norm)
            seen.add(norm)
        return sorted(brands, key=lambda v: v.lower())

    def brand_stats(self) -> List[Dict[str, Any]]:
        rows = self.facet_counts(field="brand", filters={})
        return [
            {"brand": normalize_brand(r["value"]), "count": int(r["count"])}
            for r in rows
            if r.get("value")
        ]

    def body_type_stats(self) -> List[Dict[str, Any]]:
        rows = self.facet_counts(field="body_type", filters={})
        return [{"body_type": r["value"], "count": int(r["count"])} for r in rows if r.get("value")]

    def transmissions(self) -> List[str]:
        stmt = (
            select(func.distinct(Car.transmission))
            .where(self._available_expr(), Car.transmission.is_not(None))
            .order_by(Car.transmission.asc())
        )
        return [row[0] for row in self.db.execute(stmt).all() if row[0]]

    def transmission_options(self) -> List[Dict[str, Any]]:
        rows = self.facet_counts(field="transmission", filters={})
        out: List[Dict[str, Any]] = []
        for row in rows:
            val = row.get("value")
            if not val:
                continue
            label = ru_transmission(val) or val
            out.append({"value": val, "label": label, "count": int(row.get("count") or 0)})
        return out

    def engine_types(self) -> List[Dict[str, Any]]:
        rows = self.facet_counts(field="engine_type", filters={})
        return sorted(
            build_engine_type_options(rows),
            key=lambda item: (-int(item.get("count") or 0), str(item.get("label") or item.get("value") or "").casefold()),
        )

    def payload_values(
        self,
        key: str,
        limit: int = 120,
        source_ids: Optional[List[int]] = None,
    ) -> List[str]:
        if not key:
            return []
        stmt = (
            select(Car.source_payload)
            .where(self._available_expr(), Car.source_payload.is_not(None))
        )
        if source_ids is None:
            stmt = stmt.join(Source, Car.source_id == Source.id).where(Source.key == "mobile_de")
        else:
            if not source_ids:
                return []
            stmt = stmt.where(Car.source_id.in_(source_ids))
        stmt = stmt.execution_options(stream_results=True)
        seen: set[str] = set()
        results: List[str] = []
        scanned = 0
        max_scan = 50000
        for payload in self.db.execute(stmt).scalars().yield_per(1000):
            scanned += 1
            if not payload or key not in payload:
                if scanned >= max_scan:
                    break
                continue
            val = payload.get(key)
            items = val if isinstance(val, list) else [val]
            for item in items:
                if item is None:
                    continue
                text = str(item).strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                results.append(text)
                if len(results) >= limit:
                    break
            if len(results) >= limit or scanned >= max_scan:
                break
        return sorted(results)

    def payload_values_bulk(
        self,
        keys: List[str],
        limit: int = 120,
        source_ids: Optional[List[int]] = None,
        max_scan: int = 50000,
    ) -> Dict[str, List[str]]:
        if not keys:
            return {}
        stmt = (
            select(Car.source_payload)
            .where(self._available_expr(), Car.source_payload.is_not(None))
        )
        if source_ids is None:
            stmt = stmt.join(Source, Car.source_id == Source.id).where(Source.key == "mobile_de")
        else:
            if not source_ids:
                return {k: [] for k in keys}
            stmt = stmt.where(Car.source_id.in_(source_ids))
        stmt = stmt.execution_options(stream_results=True)
        buckets: Dict[str, set[str]] = {k: set() for k in keys}
        scanned = 0
        for payload in self.db.execute(stmt).scalars().yield_per(1000):
            scanned += 1
            if not payload:
                if scanned >= max_scan:
                    break
                continue
            for key in keys:
                if key not in payload:
                    continue
                items = payload.get(key)
                values = items if isinstance(items, list) else [items]
                bucket = buckets[key]
                if len(bucket) >= limit:
                    continue
                for item in values:
                    if item is None:
                        continue
                    text = str(item).strip()
                    if not text:
                        continue
                    bucket.add(text)
                    if len(bucket) >= limit:
                        break
            if scanned >= max_scan:
                break
            if all(len(buckets[k]) >= limit for k in keys):
                break
        return {k: sorted(list(v)) for k, v in buckets.items()}

    def payload_values_bulk_filtered(
        self,
        keys: List[str],
        *,
        limit: int = 120,
        max_scan: int = 50000,
        **filters: Any,
    ) -> Dict[str, List[str]]:
        if not keys:
            return {}
        conditions, _ = self._build_list_conditions(**filters)
        stmt = (
            select(Car.source_payload)
            .where(and_(*conditions), Car.source_payload.is_not(None))
            .execution_options(stream_results=True)
        )
        buckets: Dict[str, set[str]] = {k: set() for k in keys}
        scanned = 0
        for payload in self.db.execute(stmt).scalars().yield_per(1000):
            scanned += 1
            if not payload:
                if scanned >= max_scan:
                    break
                continue
            for key in keys:
                if key not in payload:
                    continue
                items = payload.get(key)
                values = items if isinstance(items, list) else [items]
                bucket = buckets[key]
                if len(bucket) >= limit:
                    continue
                for item in values:
                    if item is None:
                        continue
                    text = str(item).strip()
                    if not text:
                        continue
                    bucket.add(text)
                    if len(bucket) >= limit:
                        break
            if scanned >= max_scan:
                break
            if all(len(buckets[key]) >= limit for key in keys):
                break
        return {key: sorted(list(values)) for key, values in buckets.items()}

    def facet_counts_filtered(
        self,
        *,
        field: str,
        limit: int = 120,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        col_map = {
            "brand": Car.brand,
            "country": func.upper(Car.country),
            "body_type": Car.body_type,
            "engine_type": self._fuel_source_expr(),
            "transmission": Car.transmission,
            "drive_type": Car.drive_type,
            "generation": Car.generation,
            "color": Car.color,
        }
        col = col_map.get(field)
        if col is None:
            return []
        conditions, _ = self._build_list_conditions(**filters)
        stmt = (
            select(col.label("value"), func.count().label("count"))
            .select_from(Car)
            .where(and_(*conditions), col.is_not(None), cast(col, String) != "")
            .group_by(col)
            .order_by(func.count().desc(), col.asc())
            .limit(limit)
        )
        rows = self.db.execute(stmt).all()
        out: List[Dict[str, Any]] = []
        seen_country: set[str] = set()
        merged_brands: Dict[str, int] = {}
        for value, count in rows:
            if value is None or value == "":
                continue
            if field == "brand":
                norm = normalize_brand(value)
                if not norm:
                    continue
                merged_brands[norm] = merged_brands.get(norm, 0) + int(count)
                continue
            if field == "country":
                code = normalize_country_code(value)
                if not code or code in seen_country:
                    continue
                seen_country.add(code)
                out.append({"value": code, "count": int(count)})
                continue
            out.append({"value": value, "count": int(count)})
        if field == "brand":
            return sorted(
                [{"value": value, "count": count} for value, count in merged_brands.items()],
                key=lambda item: (-item["count"], str(item["value"]).lower()),
            )
        return out

    def source_ids_for_region(self, region: str) -> List[int]:
        if not region:
            return []
        key = region.strip().upper()
        if key == "EU":
            return self._source_ids_for_europe()
        if key == "KR":
            return self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
        return []

    def has_air_suspension(self) -> bool:
        payload_text = func.lower(cast(Car.source_payload, String))
        stmt = (
            select(Car.id)
            .where(
                self._available_expr(),
                Car.source_payload.is_not(None),
                or_(
                    payload_text.like("%air suspension%"),
                    payload_text.like("%air_suspension%"),
                    payload_text.like("%pneum%"),
                    payload_text.like("%пневмо%"),
                ),
            )
            .limit(1)
        )
        return self.db.execute(stmt).first() is not None

    def models_for_brand(self, brand: str) -> List[Dict[str, Any]]:
        if not brand:
            return []
        cache = getattr(self, "_models_cache", {})
        key = brand.lower()
        entry = cache.get(key) if cache else None
        if entry and (datetime.utcnow().timestamp() - entry["ts"] < 300):
            return entry["data"]
        norm_brand = normalize_brand(brand).strip()
        if not norm_brand:
            return []
        stmt = text(
            """
            SELECT model, SUM(total) AS count
            FROM car_counts_model
            WHERE LOWER(TRIM(brand)) = ANY(:brand_variants_lc) AND model IS NOT NULL AND model <> ''
            GROUP BY model
            ORDER BY count DESC, model ASC
            LIMIT 200
            """
        )
        try:
            rows = self.db.execute(
                stmt,
                {"brand_variants_lc": [v.lower() for v in brand_variants(norm_brand)]},
            ).all()
            models = [{"model": r[0], "count": int(r[1])} for r in rows if r[0]]
        except ProgrammingError:
            self.db.rollback()
            variants_lc = [v.lower() for v in brand_variants(norm_brand) if v]
            fb_stmt = (
                select(Car.model, func.count())
                .where(self._available_expr(), func.lower(func.trim(Car.brand)).in_(variants_lc))
                .group_by(Car.model)
                .order_by(func.count().desc(), Car.model.asc())
                .limit(200)
            )
            rows = self.db.execute(fb_stmt).all()
            models = [{"model": r[0], "count": int(r[1])} for r in rows if r[0]]

        def sort_key(item: Dict[str, Any]):
            val = str(item.get("model") or "").strip()
            m = re.match(r"^\\s*(\\d+)", val)
            if m:
                return (0, int(m.group(1)), val.lower())
            return (1, val.lower())

        data = sorted(models, key=sort_key)
        setattr(self, "_models_cache", {**cache, key: {"ts": datetime.utcnow().timestamp(), "data": data}})
        return data

    def models_for_brand_filtered(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not brand:
            return []
        norm_brand = normalize_brand(brand).strip()
        if not norm_brand:
            return []
        cache_key = (
            (region or "").upper(),
            normalize_country_code(country) if country else "",
            (kr_type or "").upper(),
            norm_brand.casefold(),
        )
        cached = self._filtered_models_cache.get(cache_key)
        if cached is not None:
            return [dict(item) for item in cached]
        donors = self._eu_model_donors(norm_brand)
        filters = {
            "region": region,
            "country": country,
            "kr_type": kr_type,
            "brand": norm_brand,
        }
        # Fast path uses aggregated counts tables; fallback to raw cars scan for KR sub-type filtering.
        if kr_type:
            rows = self._facet_counts_from_cars(field="model", filters=filters)
        else:
            rows = self.facet_counts(field="model", filters=filters)
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            raw_value = str(row.get("value") or "").strip()
            label = self._canonical_model_label(norm_brand, raw_value, donors=donors)
            key = model_lookup_key(label)
            if not key or not label:
                continue
            bucket = buckets.get(key)
            if bucket is None:
                bucket = {
                    "value": label,
                    "label": label,
                    "count": 0,
                    "aliases": [],
                }
                buckets[key] = bucket
            bucket["count"] += int(row.get("count", 0) or 0)
            if raw_value and raw_value not in bucket["aliases"]:
                bucket["aliases"].append(raw_value)
            if label and (
                len(label) < len(str(bucket.get("label") or ""))
                or (
                    len(label) == len(str(bucket.get("label") or ""))
                    and label.casefold() < str(bucket.get("label") or "").casefold()
                )
            ):
                bucket["value"] = label
                bucket["label"] = label
        models = list(buckets.values())
        models = sorted(models, key=lambda x: self._natural_text_key(x.get("label") or x.get("value") or ""))
        if norm_brand.upper() == "BENTLEY":
            split_models = self._bentley_power_models(norm_brand)
            if split_models:
                expanded: List[Dict[str, Any]] = []
                for item in models:
                    label = normalize_model_label(item.get("label") or item.get("value") or "")
                    if label and label in split_models:
                        expanded.extend(dict(row) for row in split_models[label])
                    else:
                        expanded.append(item)
                models = expanded
        self._filtered_models_cache[cache_key] = [dict(item) for item in models]
        return models

    def _model_family_key(self, brand: str, model: str) -> str:
        raw = str(model or "").strip()
        if not raw:
            return "other"
        search_tokens = _model_search_tokens(raw)
        token = (search_tokens[0] if search_tokens else re.split(r"\s+", raw, maxsplit=1)[0]).strip(" ,.;")
        if not token:
            return "other"
        brand_norm = normalize_brand(brand).strip().upper()
        token_up = token.upper()
        if brand_norm == "BMW":
            folded = _strip_accents(_fold_model_text(raw)).lower()
            series_match = re.search(r"\b([1-8])\s*(?:series|serie|seria|серия|er)\b", folded)
            if series_match:
                return f"{series_match.group(1)}-series"
            if re.fullmatch(r"[1-8]", token_up):
                return f"{token_up}-series"
            lead_num = re.match(r"^([1-8])\d{2}", token_up)
            if lead_num:
                return f"{lead_num.group(1)}-series"
            if token_up.startswith("IX") or token_up.startswith("I"):
                return "I-series"
            if token_up.startswith("XM") or token_up.startswith("X"):
                return "X-series"
            if token_up.startswith("Z"):
                return "Z-series"
            if token_up.startswith("M"):
                return "M-series"
        if brand_norm == "PORSCHE":
            lead_num = re.match(r"^(\d{3,4})", token_up)
            p911_codes = {"911", "930", "964", "991", "992", "993", "996", "997"}
            if token_up.startswith("911") or (lead_num and lead_num.group(1) in p911_codes):
                return "911-series"
        if brand_norm == "MERCEDES-BENZ" and raw.upper().startswith("AMG GT"):
            return "AMG GT"
        return token
    
    def _natural_text_key(self, value: Any) -> tuple:
        text = str(value or "").strip().casefold()
        parts = re.split(r"(\d+)", text)
        key: list[tuple[int, Any]] = []
        for part in parts:
            if not part:
                continue
            if part.isdigit():
                key.append((0, int(part)))
            else:
                key.append((1, part))
        return tuple(key)

    def _model_family_label(self, brand: str, key: str) -> str:
        norm_brand = normalize_brand(brand).strip().upper() if brand else ""
        if key == "other":
            return "Прочее"
        if norm_brand == "BMW" and key.endswith("-series"):
            prefix = key.replace("-series", "")
            return f"{prefix.upper()} серия"
        if norm_brand == "PORSCHE" and key == "911-series":
            return "Series 911"
        return key

    def _model_group_display_label(
        self,
        brand: str,
        key: str,
        models: List[Dict[str, Any]],
    ) -> str:
        base_label = self._model_family_label(brand, key)
        norm_brand = normalize_brand(brand).strip().upper() if brand else ""
        if (norm_brand == "BMW" and key.endswith("-series")) or (
            norm_brand == "PORSCHE" and key == "911-series"
        ):
            return base_label
        if norm_brand == "BENTLEY":
            candidates = _dedupe_model_values(
                [
                    normalize_model_label(
                        _BENTLEY_POWER_LABEL_RE.sub(
                            lambda match: normalize_model_label(match.group("model")),
                            str(item.get("base_model") or item.get("label") or item.get("value") or "").strip(),
                        )
                    )
                    for item in models
                    if str(item.get("base_model") or item.get("label") or item.get("value") or "").strip()
                ]
            )
            if candidates:
                return min(candidates, key=lambda value: (len(value), self._natural_text_key(value)))
            return base_label
        candidates = _dedupe_model_values(
            [
                str(item.get("label") or item.get("value") or "").strip()
                for item in models
                if str(item.get("label") or item.get("value") or "").strip()
            ]
        )
        if norm_brand == "MERCEDES-BENZ":
            class_candidates = [
                value
                for value in candidates
                if "class" in _model_search_tokens(value)
            ]
            if class_candidates:
                candidates = class_candidates
        if not candidates:
            return base_label
        return min(candidates, key=lambda value: (len(value), self._natural_text_key(value)))

    def build_model_groups(
        self, *, brand: Optional[str], models: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        norm_brand = normalize_brand(brand).strip() if brand else ""
        grouped: Dict[str, Dict[str, Any]] = {}
        for item in models:
            value = str(item.get("value") or item.get("model") or "").strip()
            if not value:
                continue
            key = self._model_family_key(norm_brand, value)
            bucket = grouped.get(key)
            if bucket is None:
                bucket = {"key": key, "label": key, "count": 0, "models": []}
                grouped[key] = bucket
            bucket["models"].append(item)
            bucket["count"] += int(item.get("count") or 0)

        out = list(grouped.values())
        for group in out:
            group["models"] = sorted(
                group["models"],
                key=lambda x: self._natural_text_key(x.get("label") or x.get("value") or ""),
            )
            group["label"] = self._model_group_display_label(
                norm_brand,
                str(group.get("key") or ""),
                group["models"],
            )
        def group_sort_key(item: Dict[str, Any]) -> tuple:
            label = str(item.get("label") or "")
            key = str(item.get("key") or "")
            models_count = len(item.get("models") or [])
            if key == "other" or label.casefold() == "прочее":
                bucket = 3
            elif models_count > 1:
                bucket = 1
            elif label[:1].isdigit():
                bucket = 2
            else:
                bucket = 0
            return (bucket, self._natural_text_key(label))

        out.sort(key=group_sort_key)
        return out

    def drive_types(self) -> List[Dict[str, Any]]:
        rows = self.facet_counts(field="drive_type", filters={})
        out: List[Dict[str, Any]] = []
        mapping = {"awd": "Полный", "4wd": "Полный", "fwd": "Передний", "rwd": "Задний"}
        for row in rows:
            val = row.get("value")
            if not val:
                continue
            key = str(val).strip().lower()
            label = mapping.get(key, val)
            out.append({"value": val, "label": label, "count": int(row.get("count") or 0)})
        return out

    def recommended_auto(
        self,
        *,
        max_age_years: int | None = None,
        price_min: int | None = None,
        price_max: int | None = None,
        mileage_max: int | None = None,
        reg_year_min: int | None = None,
        reg_year_max: int | None = None,
        power_hp_max: int | None = None,
        engine_cc_max: int | None = None,
        body_type: str | None = None,
        limit: int = 12,
    ) -> List[Car]:
        """
        Подбор рекомендуемых без ручных списков.
        Для домашней подборки используем год регистрации/выпуска и эффективные характеристики.
        """
        conditions = [self._available_expr()]
        now_year = func.extract("year", func.now())
        now_month = func.extract("month", func.now())
        reg_year_expr = self._effective_registration_year_expr()
        reg_month_expr = self._effective_registration_month_ceil_expr()
        power_hp_expr = func.coalesce(Car.power_hp, Car.inferred_power_hp)
        engine_cc_expr = func.coalesce(Car.engine_cc, Car.inferred_engine_cc)
        price_expr = self._display_price_rub_expr()

        if max_age_years is not None:
            # возраст в месяцах
            max_months = max_age_years * 12
            age_months = (now_year - reg_year_expr) * 12 + (now_month - reg_month_expr)
            conditions.append(age_months <= max_months)

        if reg_year_min is not None:
            conditions.append(reg_year_expr >= reg_year_min)
        if reg_year_max is not None:
            conditions.append(reg_year_expr <= reg_year_max)
        if price_min is not None:
            conditions.append(price_expr >= price_min)
        if price_max is not None:
            conditions.append(price_expr <= price_max)
        if mileage_max is not None:
            conditions.append(Car.mileage <= mileage_max)
        if power_hp_max is not None:
            conditions.append(power_hp_expr.is_not(None))
            conditions.append(power_hp_expr <= power_hp_max)
        if engine_cc_max is not None:
            conditions.append(engine_cc_expr.is_not(None))
            conditions.append(engine_cc_expr <= engine_cc_max)
        if body_type is not None:
            conditions.append(Car.body_type == (normalize_body_type(body_type) or body_type))

        stmt = (
            select(Car)
            .where(and_(*conditions))
            .order_by(
                price_expr.asc().nullslast(),
                Car.mileage.asc().nullslast(),
                Car.year.desc().nullslast(),
                Car.created_at.desc(),
            )
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def similar_cars(self, car: Car, *, limit: int = 10) -> List[Car]:
        brand_keys = [value.lower() for value in brand_variants(car.brand)]
        model_key = str(car.model or "").strip().lower()
        generation_key = str(car.generation or "").strip().lower()
        body_key = normalize_body_type(car.body_type) or str(car.body_type or "").strip().lower()
        engine_key = normalize_fuel(car.engine_type) or str(car.engine_type or "").strip().lower()
        if not brand_keys and not model_key:
            return []

        def _sort_const(value: int | float):
            # Avoid bare numeric ORDER BY items like `ORDER BY 999999`, which PostgreSQL
            # interprets as select-list ordinals instead of constant expressions.
            return literal(value) + literal(0)

        conditions = [self._available_expr(), Car.id != car.id]
        if brand_keys:
            conditions.append(func.lower(func.coalesce(Car.brand, "")).in_(brand_keys))
        elif model_key:
            conditions.append(func.lower(func.coalesce(Car.model, "")) == model_key)

        target_reg_year = car.registration_year or car.year
        target_power_hp = effective_power_hp_value(car)
        target_engine_cc = effective_engine_cc_value(car)
        target_mileage = car.mileage

        reg_year_expr = self._effective_registration_year_expr()
        power_hp_expr = func.coalesce(Car.power_hp, Car.inferred_power_hp)
        engine_cc_expr = func.coalesce(Car.engine_cc, Car.inferred_engine_cc)
        price_expr = self._display_price_rub_expr()

        model_rank = (
            case(
                (func.lower(func.coalesce(Car.model, "")) == model_key, 0),
                else_=1,
            )
            if model_key
            else _sort_const(1)
        )
        generation_rank = (
            case(
                (func.lower(func.coalesce(Car.generation, "")) == generation_key, 0),
                else_=1,
            )
            if generation_key
            else _sort_const(1)
        )
        body_rank = (
            case(
                (func.lower(func.coalesce(Car.body_type, "")) == body_key, 0),
                else_=1,
            )
            if body_key
            else _sort_const(1)
        )
        engine_rank = (
            case(
                (func.lower(func.coalesce(Car.engine_type, "")) == engine_key, 0),
                else_=1,
            )
            if engine_key
            else _sort_const(1)
        )
        country_rank = case(
            (func.upper(func.coalesce(Car.country, "")) == str(car.country or "").upper(), 0),
            else_=1,
        )
        year_distance = (
            case((reg_year_expr.is_not(None), func.abs(reg_year_expr - int(target_reg_year))), else_=999)
            if target_reg_year is not None
            else _sort_const(999)
        )
        power_distance = (
            case(
                (power_hp_expr.is_not(None), func.abs(power_hp_expr - float(target_power_hp))),
                else_=999999,
            )
            if target_power_hp is not None
            else _sort_const(999999)
        )
        engine_cc_distance = (
            case(
                (engine_cc_expr.is_not(None), func.abs(engine_cc_expr - int(target_engine_cc))),
                else_=999999,
            )
            if target_engine_cc is not None
            else _sort_const(999999)
        )
        mileage_distance = (
            case(
                (Car.mileage.is_not(None), func.abs(Car.mileage - int(target_mileage))),
                else_=999999999,
            )
            if target_mileage is not None
            else _sort_const(999999999)
        )

        stmt = (
            select(Car)
            .where(and_(*conditions))
            .order_by(
                model_rank.asc(),
                generation_rank.asc(),
                body_rank.asc(),
                engine_rank.asc(),
                country_rank.asc(),
                year_distance.asc(),
                power_distance.asc(),
                engine_cc_distance.asc(),
                mileage_distance.asc(),
                price_expr.asc().nullslast(),
                Car.listing_sort_ts.desc().nullslast(),
                Car.created_at.desc(),
            )
            .limit(max(1, min(int(limit or 10), 24)))
        )
        return list(self.db.execute(stmt).scalars().all())

    def top_models_by_brand(self, max_brands: int = 5, top_n: int = 6) -> Dict[str, List[Dict[str, Any]]]:
        brands = [b["brand"] for b in self.brand_stats()[:max_brands]]
        result: Dict[str, List[Dict[str, Any]]] = {}
        for brand in brands:
            result[brand] = self.models_for_brand(brand)[:top_n]
        return result

    def colors(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.color, func.count().label("count"))
            .where(self._available_expr(), Car.color.is_not(None))
            .group_by(Car.color)
        )
        rows = self.db.execute(stmt).all()
        basic: Dict[str, Dict[str, Any]] = {}
        other: Dict[str, Dict[str, Any]] = {}
        for color, cnt in rows:
            if not color:
                continue
            raw = str(color).strip()
            if not raw:
                continue
            norm = normalize_color(raw) or raw.lower()
            if is_color_base(norm):
                entry = basic.get(norm)
                if not entry:
                    label = ru_color(norm) or display_color(norm) or raw
                    entry = {
                        "value": norm,
                        "label": label,
                        "count": 0,
                        "hex": color_hex(norm),
                    }
                    basic[norm] = entry
                entry["count"] += int(cnt)
            if raw.lower() != norm:
                key = raw.lower()
                entry = other.get(key)
                if not entry:
                    label = ru_color(raw) or display_color(raw) or raw
                    entry = {
                        "value": raw,
                        "label": label,
                        "count": 0,
                        "hex": color_hex(norm),
                    }
                    other[key] = entry
                entry["count"] += int(cnt)
        ordered = list(basic.values()) + sorted(other.values(), key=lambda x: x["count"], reverse=True)
        return ordered

    def highlighted_cars(self, limit: int = 8) -> List[Car]:
        # для обратной совместимости используем рекомендованные
        return self.featured_for("recommended", limit=limit, fallback_limit=limit)

    def featured_for(self, placement: str, limit: int = 8, fallback_limit: int | None = None) -> List[Car]:
        stmt = (
            select(Car)
            .join(FeaturedCar, FeaturedCar.car_id == Car.id)
            .where(
                FeaturedCar.placement == placement,
                FeaturedCar.is_active.is_(True),
                self._available_expr(),
            )
            .order_by(FeaturedCar.position.asc(), Car.created_at.desc(), Car.id.desc())
            .limit(limit)
        )
        items = list(self.db.execute(stmt).scalars().all())
        if items or fallback_limit is None:
            return items
        # Fallback to fresh cars when featured is empty
        fallback_stmt = (
            select(Car)
            .where(
                self._available_expr(),
                Car.thumbnail_url.is_not(None),
                Car.thumbnail_url != "",
            )
            .order_by(Car.created_at.desc(), Car.id.desc())
            .limit(fallback_limit)
        )
        return list(self.db.execute(fallback_stmt).scalars().all())

    def recent_with_thumbnails(self, limit: int = 50) -> List[Car]:
        stmt = (
            select(Car)
            .where(
                self._available_expr(),
                Car.thumbnail_url.is_not(None),
                Car.thumbnail_url != "",
            )
            .order_by(Car.created_at.desc(), Car.id.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def total_cars(self, source_keys: Optional[List[str]] = None) -> int:
        conditions = [self._available_expr()]
        if source_keys:
            src_ids = self.db.execute(select(Source.id).where(
                Source.key.in_(source_keys))).scalars().all()
            if src_ids:
                conditions.append(Car.source_id.in_(src_ids))
        stmt = select(func.count()).select_from(Car).where(and_(*conditions))
        return self.db.execute(stmt).scalar_one()

    def count_cars(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        brand: Optional[str] = None,
        model: Optional[str] = None,
        generation: Optional[str] = None,
        color: Optional[str] = None,
        q: Optional[str] = None,
        lines: Optional[List[str]] = None,
        source_key: Optional[str | List[str]] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        power_hp_min: Optional[float] = None,
        power_hp_max: Optional[float] = None,
        engine_cc_min: Optional[int] = None,
        engine_cc_max: Optional[int] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        mileage_min: Optional[int] = None,
        mileage_max: Optional[int] = None,
        kr_type: Optional[str] = None,
        reg_year_min: Optional[int] = None,
        reg_month_min: Optional[int] = None,
        reg_year_max: Optional[int] = None,
        reg_month_max: Optional[int] = None,
        body_type: Optional[str] = None,
        engine_type: Optional[str] = None,
        transmission: Optional[str] = None,
        drive_type: Optional[str] = None,
        num_seats: Optional[str] = None,
        doors_count: Optional[str] = None,
        emission_class: Optional[str] = None,
        efficiency_class: Optional[str] = None,
        climatisation: Optional[str] = None,
        airbags: Optional[str] = None,
        interior_design: Optional[str] = None,
        interior_color: Optional[str] = None,
        interior_material: Optional[str] = None,
        vat_reclaimable: Optional[str] = None,
        air_suspension: Optional[bool] = None,
        price_rating_label: Optional[str] = None,
        owners_count: Optional[str] = None,
        condition: Optional[str] = None,
        hide_no_local_photo: bool = False,
    ) -> int:
        normalized_color = normalize_csv_values(color) or color
        normalized_interior_design = normalize_csv_values(interior_design) or interior_design
        normalized_interior_color = normalize_csv_values(interior_color) or interior_color
        normalized_interior_material = normalize_csv_values(interior_material) or interior_material
        _, total = self.list_cars(
            region=region,
            country=country,
            brand=brand,
            model=model,
            generation=generation,
            color=normalized_color,
            q=q,
            lines=lines,
            source_key=source_key,
            price_min=price_min,
            price_max=price_max,
            power_hp_min=power_hp_min,
            power_hp_max=power_hp_max,
            engine_cc_min=engine_cc_min,
            engine_cc_max=engine_cc_max,
            year_min=year_min,
            year_max=year_max,
            mileage_min=mileage_min,
            mileage_max=mileage_max,
            kr_type=kr_type,
            reg_year_min=reg_year_min,
            reg_month_min=reg_month_min,
            reg_year_max=reg_year_max,
            reg_month_max=reg_month_max,
            body_type=body_type,
            engine_type=engine_type,
            transmission=transmission,
            drive_type=drive_type,
            num_seats=num_seats,
            doors_count=doors_count,
            emission_class=emission_class,
            efficiency_class=efficiency_class,
            climatisation=climatisation,
            airbags=airbags,
            interior_design=normalized_interior_design,
            interior_color=normalized_interior_color,
            interior_material=normalized_interior_material,
            vat_reclaimable=vat_reclaimable,
            air_suspension=air_suspension,
            price_rating_label=price_rating_label,
            owners_count=owners_count,
            condition=condition,
            page=1,
            page_size=1,
            light=True,
            use_fast_count=False,
            hide_no_local_photo=hide_no_local_photo,
        )
        return int(total)

    def price_info(self, car: Car) -> Dict[str, Any]:
        payload = car.source_payload or {}
        price_gross = payload.get("price_eur")
        price_net = payload.get("price_eur_nt")
        vat_pct = payload.get("vat")
        try:
            vat_reclaimable = bool(
                price_net is not None
                and (
                    (vat_pct is not None and float(vat_pct) > 0)
                    or (price_gross is not None and float(price_net) < float(price_gross))
                )
            )
        except Exception:
            vat_reclaimable = bool(price_net is not None)
        return {
            "gross_eur": float(price_gross) if price_gross is not None else None,
            "net_eur": float(price_net) if price_net is not None else None,
            "vat_percent": float(vat_pct) if vat_pct is not None else None,
            "vat_reclaimable": vat_reclaimable,
        }

    def upsert_cars(self, source: Source, parsed: List[Dict[str, Any]]) -> Tuple[int, int]:
        inserted = 0
        updated = 0
        for item in parsed:
            existing = self.db.execute(
                select(Car).where(
                    (Car.source_id == source.id) & (
                        Car.external_id == item["external_id"])
                )
            ).scalar_one_or_none()
            if existing:
                for key, value in item.items():
                    if hasattr(existing, key) and key not in ("id", "created_at"):
                        setattr(existing, key, value)
                existing.is_available = True
                updated += 1
            else:
                car = Car(source_id=source.id, **item)
                self.db.add(car)
                inserted += 1
        self.db.commit()
        return inserted, updated

    def mark_unavailable_except(self, source: Source, external_ids: List[str]) -> int:
        # Mark cars from this source that are not in the latest external_ids as unavailable
        stmt = select(Car).where((Car.source_id == source.id)
                                 & (self._available_expr()))
        cars = self.db.execute(stmt).scalars().all()
        changed = 0
        external_set = set(external_ids)
        for car in cars:
            if car.external_id not in external_set:
                car.is_available = False
                changed += 1
        if changed:
            self.db.commit()
        return changed

    def search_featured_candidates(self, query: str, limit: int = 20) -> List[Car]:
        if not query:
            return []
        q = query.strip()
        stmt = select(Car).where(self._available_expr())
        conds = []
        try:
            cid = int(q)
            conds.append(Car.id == cid)
        except ValueError:
            pass
        like = f"%{q}%"
        conds.append(func.lower(Car.brand).like(func.lower(like)))
        conds.append(func.lower(Car.model).like(func.lower(like)))
        conds.append(func.lower(func.concat(Car.brand, " ", Car.model)).like(func.lower(like)))
        stmt = stmt.where(or_(*conds)).order_by(Car.created_at.desc(), Car.id.desc()).limit(limit)
        return list(self.db.execute(stmt).scalars().all())
