from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional
import re
import unicodedata
import colorsys


@dataclass(frozen=True)
class ColorGroup:
    key: str
    label: str
    patterns: List[str]


@dataclass(frozen=True)
class ColorFamily:
    key: str
    label: str
    group_keys: List[str]
    hex_value: str


_GROUPS: List[ColorGroup] = [
    ColorGroup("black", "Черный", []),
    ColorGroup("white", "Белый", []),
    ColorGroup("gray", "Серый", []),
    ColorGroup("silver", "Серебристый", []),
    ColorGroup("red", "Красный", []),
    ColorGroup("blue", "Синий", []),
    ColorGroup("green", "Зеленый", []),
    ColorGroup("yellow", "Желтый", []),
    ColorGroup("orange", "Оранжевый", []),
    ColorGroup("brown", "Коричневый", []),
    ColorGroup("beige", "Бежевый", []),
    ColorGroup("purple", "Фиолетовый", []),
    ColorGroup("pink", "Розовый", []),
]

_GROUP_ORDER: list[str] = [
    "black",
    "white",
    "gray",
    "silver",
    "red",
    "blue",
    "green",
    "yellow",
    "orange",
    "brown",
    "beige",
    "purple",
    "pink",
    "other",
]

_GROUP_HEX: dict[str, str] = {
    "black": "#0d0f14",
    "white": "#f5f6fa",
    "gray": "#7d8594",
    "silver": "#b8bec8",
    "red": "#c94545",
    "blue": "#2f6bd1",
    "green": "#3b8d4c",
    "yellow": "#e8ad2c",
    "orange": "#e28b25",
    "brown": "#8a5b3d",
    "beige": "#d6be7d",
    "purple": "#7a4bd8",
    "pink": "#d95da4",
    "other": "#8c94a3",
}

_FAMILIES: List[ColorFamily] = [
    ColorFamily("black", "Черный", ["black"], "#0d0f14"),
    ColorFamily("white", "Белый", ["white"], "#f5f6fa"),
    ColorFamily("gray", "Серый / серебристый", ["gray", "silver"], "#7d8594"),
    ColorFamily("red", "Красный / бордовый", ["red", "pink"], "#c94545"),
    ColorFamily("blue", "Синий / голубой", ["blue"], "#2f6bd1"),
    ColorFamily("green", "Зеленый / бирюзовый", ["green"], "#3b8d4c"),
    ColorFamily("yellow", "Желтый / оранжевый", ["yellow", "orange"], "#e8ad2c"),
    ColorFamily("brown", "Коричневый / бежевый", ["brown", "beige"], "#9a7150"),
    ColorFamily("purple", "Фиолетовый", ["purple"], "#7a4bd8"),
    ColorFamily("other", "Другие", ["other"], "#8c94a3"),
]

_STOPWORDS = {
    "metallic", "metalic", "metal", "perla", "perl", "pearl", "mica",
    "glanz", "matt", "matte", "uni", "lack", "lacker", "sonder", "design",
    "effekt", "effect", "edition", "serie", "line", "paket",
}

_KEYWORDS = {
    "black": ["black", "schwarz", "noir", "nero", "obsidian", "onyx", "nuit"],
    "white": ["white", "weiss", "weiß", "bianco", "blanc", "ivory", "elfenbein", "alabaster"],
    "gray": ["gray", "grey", "grau", "anthrazit", "anthracite", "graphit", "graphite", "titan", "titanium", "gunmetal"],
    "silver": ["silver", "silber", "aluminium", "aluminum", "platin", "platinum", "chrom", "chromium"],
    "red": ["red", "rot", "rosso", "rouge", "burgund", "bordeaux", "kirsch", "rubin", "magenta"],
    "blue": ["blue", "blau", "navy", "azure", "azur", "indigo", "saphir", "sapphire", "atlantik", "denim"],
    "green": ["green", "grun", "grün", "verde", "olive", "smaragd", "emerald", "british racing"],
    "yellow": ["yellow", "gelb", "goldgelb", "saffron"],
    "orange": ["orange", "mango"],
    "brown": ["brown", "braun", "marron", "cognac", "mocha", "mokka", "kaffee", "bronze"],
    "beige": ["beige", "sand", "champagne", "gold", "ivory", "cream"],
    "purple": ["purple", "lila", "violet", "aubergine"],
    "pink": ["pink", "rose", "rosé", "rosa"],
}

_LABEL_TO_KEY = {g.label.lower(): g.key for g in _GROUPS}
_LABEL_TO_KEY.update({
    "черный": "black",
    "белый": "white",
    "серый": "gray",
    "серебристый": "silver",
    "красный": "red",
    "синий": "blue",
    "голубой": "blue",
    "зеленый": "green",
    "желтый": "yellow",
    "оранжевый": "orange",
    "коричневый": "brown",
    "бежевый": "beige",
    "фиолетовый": "purple",
    "розовый": "pink",
})

_FAMILY_BY_KEY = {family.key: family for family in _FAMILIES}
_FAMILY_BY_GROUP_KEY = {
    group_key: family.key
    for family in _FAMILIES
    for group_key in family.group_keys
}
_FAMILY_LABEL_TO_KEY = {
    family.label.lower(): family.key
    for family in _FAMILIES
}


def _strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join([c for c in text if not unicodedata.combining(c)])


def _normalize_text(raw: str) -> str:
    text = raw.lower().strip()
    text = text.replace("ß", "ss")
    text = _strip_accents(text)
    text = re.sub(r"[^a-z0-9\\s]+", " ", text)
    parts = [p for p in text.split() if p and p not in _STOPWORDS]
    return " ".join(parts)


def _group_from_hex(color_hex: str) -> Optional[str]:
    if not color_hex:
        return None
    value = color_hex.strip().lower()
    if not re.match(r"^#?[0-9a-f]{6}$", value):
        return None
    if not value.startswith("#"):
        value = f"#{value}"
    r = int(value[1:3], 16) / 255.0
    g = int(value[3:5], 16) / 255.0
    b = int(value[5:7], 16) / 255.0
    h, s, v = colorsys.rgb_to_hsv(r, g, b)
    if s < 0.08:
        if v < 0.15:
            return "black"
        if v > 0.9:
            return "white"
        return "gray"
    hue = h * 360.0
    if hue < 15 or hue >= 345:
        return "red"
    if 15 <= hue < 40:
        return "orange"
    if 40 <= hue < 65:
        return "yellow"
    if 65 <= hue < 160:
        return "green"
    if 160 <= hue < 200:
        return "blue"
    if 200 <= hue < 255:
        return "blue"
    if 255 <= hue < 320:
        return "purple"
    return "pink"


def normalize_color_group(raw: Optional[str], color_hex: Optional[str] = None) -> str:
    if color_hex:
        group = _group_from_hex(color_hex)
        if group:
            return group
    if not raw:
        return "other"
    norm = _normalize_text(raw)
    if not norm:
        return "other"
    if norm in _LABEL_TO_KEY:
        return _LABEL_TO_KEY[norm]
    # keyword matching
    for key, words in _KEYWORDS.items():
        for w in words:
            if w in norm:
                return key
    return "other"


def normalize_color_group_key(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    key = normalize_color_group(raw)
    return key


def color_group_label(key: str) -> str:
    for group in _GROUPS:
        if group.key == key:
            return group.label
    return "Другие"


def color_groups() -> List[ColorGroup]:
    return list(_GROUPS)


def color_families() -> List[ColorFamily]:
    return list(_FAMILIES)


def normalize_color_family_key(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    key = str(raw).strip().lower()
    if not key:
        return None
    if key in _FAMILY_BY_KEY:
        return key
    if key in _FAMILY_LABEL_TO_KEY:
        return _FAMILY_LABEL_TO_KEY[key]
    group_key = normalize_color_group_key(key)
    if group_key and group_key in _FAMILY_BY_GROUP_KEY:
        return _FAMILY_BY_GROUP_KEY[group_key]
    return None


def color_family_group_keys(raw: Optional[str]) -> List[str]:
    family_key = normalize_color_family_key(raw)
    if not family_key:
        return []
    family = _FAMILY_BY_KEY.get(family_key)
    return list(family.group_keys) if family else []


def color_family_label(raw: Optional[str]) -> Optional[str]:
    family_key = normalize_color_family_key(raw)
    if not family_key:
        return None
    family = _FAMILY_BY_KEY.get(family_key)
    return family.label if family else None


def color_family_hex(raw: Optional[str]) -> Optional[str]:
    family_key = normalize_color_family_key(raw)
    if not family_key:
        return None
    family = _FAMILY_BY_KEY.get(family_key)
    return family.hex_value if family else None


def split_color_facets(
    raw_colors: list[dict[str, Any]],
    top_limit: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    group_counts: dict[str, int] = {}

    for raw in raw_colors or []:
        value = str((raw or {}).get("value") or "").strip()
        if not value:
            continue
        count = int((raw or {}).get("count") or 0)
        group_key = normalize_color_group_key(value)
        if not group_key:
            continue
        group_counts[group_key] = group_counts.get(group_key, 0) + count

    ordered_items: list[dict[str, Any]] = []
    for key in _GROUP_ORDER:
        count = int(group_counts.get(key) or 0)
        if count <= 0:
            continue
        ordered_items.append(
            {
                "value": key,
                "label": color_group_label(key),
                "hex": _GROUP_HEX.get(key, _GROUP_HEX["other"]),
                "count": count,
            }
        )

    if top_limit is None or top_limit <= 0:
        return ordered_items, []

    rank = {key: idx for idx, key in enumerate(_GROUP_ORDER)}
    limited_items = sorted(
        ordered_items,
        key=lambda item: (-int(item.get("count") or 0), rank.get(str(item.get("value") or ""), 999)),
    )
    basics = limited_items[:top_limit]
    other = limited_items[top_limit:]
    return basics, other
