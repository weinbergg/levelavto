from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import re
import unicodedata
import colorsys


@dataclass(frozen=True)
class ColorGroup:
    key: str
    label: str
    patterns: List[str]


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
    if key == "other":
        return None
    return key


def color_group_label(key: str) -> str:
    for group in _GROUPS:
        if group.key == key:
            return group.label
    return "Другое"


def color_groups() -> List[ColorGroup]:
    return list(_GROUPS)
