from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import re


@dataclass(frozen=True)
class ColorGroup:
    key: str
    label: str
    patterns: List[str]


_GROUPS: List[ColorGroup] = [
    ColorGroup("black", "Черный", ["black", "schwarz", "noir", "nero"]),
    ColorGroup("white", "Белый", ["white", "weiß", "weiss", "bianco", "blanc"]),
    ColorGroup("gray", "Серый", ["gray", "grey", "grau", "anthracite", "graphite"]),
    ColorGroup("silver", "Серебристый", ["silver", "silber"]),
    ColorGroup("red", "Красный", ["red", "rot", "rouge", "rosso", "burgundy", "bordo"]),
    ColorGroup("blue", "Синий", ["blue", "blau", "azure", "navy"]),
    ColorGroup("green", "Зеленый", ["green", "grün", "verde"]),
    ColorGroup("yellow", "Желтый", ["yellow", "gelb"]),
    ColorGroup("orange", "Оранжевый", ["orange"]),
    ColorGroup("brown", "Коричневый", ["brown", "braun", "marron"]),
    ColorGroup("beige", "Бежевый", ["beige", "champagne", "sand"]),
]


def normalize_color_group(raw: Optional[str]) -> str:
    if not raw:
        return "other"
    text = raw.lower()
    # remove metallic words
    text = re.sub(r"\\bmetallic\\b", "", text)
    for group in _GROUPS:
        for p in group.patterns:
            if re.search(p, text):
                return group.key
    return "other"


def color_group_label(key: str) -> str:
    for group in _GROUPS:
        if group.key == key:
            return group.label
    return "Другое"


def color_groups() -> List[ColorGroup]:
    return list(_GROUPS)
