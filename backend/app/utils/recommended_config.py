import json
from pathlib import Path
from typing import Dict, Any

DEFAULT_CONFIG: Dict[str, Any] = {
    "max_age_years": 5,
    "price_min": 1_000_000,
    "price_max": 4_000_000,
    "mileage_max": 80_000,
}


def _config_path() -> Path:
    """
    runtime JSON рядом с backend/app/runtime/recommended_ranges.json
    """
    return Path(__file__).resolve().parent.parent / "runtime" / "recommended_ranges.json"


def load_config() -> Dict[str, Any]:
    path = _config_path()
    if not path.exists():
        return DEFAULT_CONFIG.copy()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cfg = DEFAULT_CONFIG.copy()
        if isinstance(data, dict):
            cfg.update({k: v for k, v in data.items() if k in DEFAULT_CONFIG})
        return cfg
    except Exception:
        return DEFAULT_CONFIG.copy()


def save_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = DEFAULT_CONFIG.copy()
    if cfg:
        data.update({k: v for k, v in cfg.items() if k in DEFAULT_CONFIG})
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data
