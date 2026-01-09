from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

_MAP_PATH = Path(__file__).resolve().parents[3] / "localization_maps.json"
with _MAP_PATH.open("r", encoding="utf-8") as f:
    _MAPS = json.load(f)

_REGION_MAP = {k.lower(): v for k, v in _MAPS.get("regions", {}).get("by_source", {}).items()}
_BODY_MAP = {k.lower(): v for k, v in _MAPS.get("body_types", {}).items()}
_COLOR_MAP = {k.lower(): v for k, v in _MAPS.get("colors", {}).items()}


def display_region(source_key: Optional[str]) -> Optional[str]:
    if not source_key:
        return None
    sk = source_key.lower()
    for key, val in _REGION_MAP.items():
        if sk.startswith(key):
            return val
    return None


def display_body(body: Optional[str]) -> Optional[str]:
    if not body:
        return None
    return _BODY_MAP.get(body.strip().lower())


def display_color(color: Optional[str]) -> Optional[str]:
    if not color:
        return None
    key = color.strip().lower()
    # exact match
    if key in _COLOR_MAP:
        return _COLOR_MAP[key]
    return None
