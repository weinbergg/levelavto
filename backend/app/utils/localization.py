from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

# Map file lives in the repository root (sibling of `backend/`).
# In production the repo is bind-mounted at /app, so the default
# parents[3] resolution lands on /app/localization_maps.json.
# Allow override via env var (LOCALIZATION_MAPS_PATH) for non-standard layouts
# (e.g. when running scripts inside an image without the bind mount).
def _resolve_map_path() -> Path:
    env = os.environ.get("LOCALIZATION_MAPS_PATH")
    if env:
        return Path(env)
    base = Path(__file__).resolve().parents[3] / "localization_maps.json"
    if base.exists():
        return base
    # Fallback: try repo-root candidates (covers the dual-layout case where
    # the file lives next to the `backend/` dir but the image was built with
    # context=./backend/ and the file landed under /app/backend/).
    candidates = [
        Path(__file__).resolve().parents[2] / "localization_maps.json",  # /app/backend/localization_maps.json
        Path("/app/localization_maps.json"),
        Path("/app/backend/localization_maps.json"),
    ]
    for c in candidates:
        if c.exists():
            return c
    return base  # let the open() raise a clear FileNotFoundError below


_MAP_PATH = _resolve_map_path()
try:
    with _MAP_PATH.open("r", encoding="utf-8") as f:
        _MAPS = json.load(f)
except FileNotFoundError as exc:
    raise FileNotFoundError(
        f"localization_maps.json not found. Tried {_MAP_PATH}. "
        "Set LOCALIZATION_MAPS_PATH or ensure the file is mounted at /app/."
    ) from exc

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
