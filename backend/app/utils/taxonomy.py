from __future__ import annotations

import csv
import hashlib
import re
from pathlib import Path
from typing import Dict, Optional, Set, Tuple, List


def _load_taxonomy() -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, Set[str]]]]:
    base = Path(__file__).resolve().parents[1] / "resources"
    csv_path = base / "taxonomy_ru.csv"
    mapping: Dict[str, Dict[str, str]] = {}
    aliases: Dict[str, Dict[str, Set[str]]] = {}
    if not csv_path.exists():
        raise FileNotFoundError(
            f"taxonomy file not found: {csv_path}. Place taxonomy_ru.csv in backend/app/resources/")
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            typ = (row.get("type") or "").strip()
            key = (row.get("key") or "").strip()
            ru = (row.get("ru") or "").strip()
            if not typ or not key or not ru:
                continue
            key_l = key.lower()
            mapping.setdefault(typ, {})[key_l] = ru
            mapping.setdefault(typ, {})[ru.lower()] = ru
            alias_bucket = aliases.setdefault(typ, {})
            alias_bucket.setdefault(key_l, set()).update({key_l, ru.lower()})
            raw_values = (row.get("raw_values") or "").lower()
            if raw_values:
                for v in raw_values.split(";"):
                    vv = v.strip()
                    if vv:
                        mapping.setdefault(typ, {})[vv] = ru
                        alias_bucket.setdefault(key_l, set()).add(vv)
    return mapping, aliases


_TAX, _ALIASES = _load_taxonomy()


def ru_label(category: str, value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return _TAX.get(category, {}).get(value.strip().lower())


def ru_body(body: Optional[str]) -> Optional[str]:
    return ru_label("body_type", body)


def ru_color(color: Optional[str]) -> Optional[str]:
    return ru_label("color", color)


def ru_fuel(fuel: Optional[str]) -> Optional[str]:
    return ru_label("fuel", fuel)


def ru_transmission(val: Optional[str]) -> Optional[str]:
    return ru_label("transmission", val)


def _aliases_for(category: str, value: Optional[str]) -> List[str]:
    if not value:
        return []
    key = value.strip().lower()
    aliases = _ALIASES.get(category, {}).get(key)
    if not aliases:
        return [key]
    return sorted(aliases)


def color_aliases(value: Optional[str]) -> List[str]:
    base = (value or "").strip().lower()
    aliases = set(_aliases_for("color", value))
    keywords = _COLOR_KEYWORDS.get(base)
    if keywords:
        aliases.update(keywords)
    return sorted(aliases)


def fuel_aliases(value: Optional[str]) -> List[str]:
    return _aliases_for("fuel", value)


_COLOR_HEX = {
    "black": "#0d0f14",
    "white": "#f5f6fa",
    "gray": "#6f7683",
    "silver": "#c0c0c0",
    "red": "#d13b3b",
    "blue": "#2f6bd1",
    "green": "#3b8d4c",
    "yellow": "#f0c63c",
    "orange": "#f28f3b",
    "brown": "#7a5234",
    "beige": "#d9c6a5",
    "purple": "#7a4bd8",
    "gold": "#d4af37",
    "pink": "#f472b6",
}

_COLOR_FALLBACK = [
    "#f59e0b",
    "#22c55e",
    "#3b82f6",
    "#ef4444",
    "#a855f7",
    "#14b8a6",
    "#eab308",
    "#f97316",
]

_COLOR_KEYWORDS = {
    "white": ["white", "ivory", "cream", "snow", "pearl", "weiss", "weiß", "bianco", "blanc", "бел", "слон"],
    "black": ["black", "obsidian", "onyx", "noir", "nero", "schwarz", "черн"],
    "gray": ["gray", "grey", "graphite", "anthracite", "slate", "titan", "grau", "сер", "графит"],
    "silver": ["silver", "aluminium", "aluminum", "silber", "steel", "платин", "серебр"],
    "blue": ["blue", "navy", "azure", "sky", "cyan", "blau", "син", "голуб"],
    "red": ["red", "maroon", "burgundy", "ruby", "rot", "красн", "бордов"],
    "green": ["green", "olive", "emerald", "gruen", "grün", "зел"],
    "orange": ["orange", "copper", "bronze", "оранж", "медн"],
    "yellow": ["yellow", "gold", "golden", "gelb", "желт", "зол"],
    "brown": ["brown", "coffee", "chocolate", "cocoa", "braun", "коричн", "шокол", "кофе"],
    "beige": ["beige", "sand", "champagne", "беж", "песочн", "шамп"],
    "purple": ["purple", "violet", "lilac", "lila", "фиол", "пурпур"],
    "pink": ["pink", "rose", "rosé", "роз"],
}

_COLOR_MODIFIERS = {
    "metallic",
    "met",
    "pearl",
    "pearlescent",
    "gloss",
    "matte",
    "matt",
    "mat",
    "diamond",
    "dust",
    "magnetic",
    "crystal",
    "clear",
    "clearcoat",
    "effect",
    "special",
    "uni",
    "solid",
    "coat",
}


def color_hex(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = value.strip().lower()
    if key in _COLOR_HEX:
        return _COLOR_HEX[key]
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(_COLOR_FALLBACK)
    return _COLOR_FALLBACK[idx]


def _normalize_alias(category: str, val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    key = val.strip().lower()
    for canonical, items in _ALIASES.get(category, {}).items():
        for raw in items:
            if raw and raw in key:
                return canonical
    return key


def normalize_color(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    raw = val.strip().lower()
    cleaned = re.sub(r"[_\-.]+", " ", raw)
    cleaned = re.sub(r"[^\w\s]+", " ", cleaned, flags=re.UNICODE)
    tokens = [t for t in cleaned.split() if t and t not in _COLOR_MODIFIERS]
    cleaned = " ".join(tokens) or raw
    for base, keywords in _COLOR_KEYWORDS.items():
        if any(k in cleaned for k in keywords):
            return base
    if "metal" in cleaned or "metallic" in raw:
        return "silver"
    return _normalize_alias("color", cleaned)


def normalize_fuel(val: Optional[str]) -> Optional[str]:
    return _normalize_alias("fuel", val)
