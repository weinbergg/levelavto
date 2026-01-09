from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Optional


def _load_taxonomy() -> Dict[str, Dict[str, str]]:
    base = Path(__file__).resolve().parents[1] / "resources"
    csv_path = base / "taxonomy_ru.csv"
    mapping: Dict[str, Dict[str, str]] = {}
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
            mapping.setdefault(typ, {})[key.lower()] = ru
            raw_values = (row.get("raw_values") or "").lower()
            if raw_values:
                for v in raw_values.split(";"):
                    vv = v.strip()
                    if vv:
                        mapping.setdefault(typ, {})[vv] = ru
    return mapping


_TAX = _load_taxonomy()


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


def normalize_color(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    key = val.strip().lower()
    for raw in _TAX.get("color", {}):
        if raw in key:
            return raw
    return key
