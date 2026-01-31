from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class DutyRange(BaseModel):
    from_cc: int
    to_cc: int
    eur_per_cc: float

    @field_validator("from_cc", "to_cc")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("from_cc/to_cc must be >= 0")
        return v


class UtilBucket(BaseModel):
    from_cc: int
    to_cc: int
    table: str


class UtilRange(BaseModel):
    from_: float = Field(0, alias="from")
    to: float
    price_rub: float

    @field_validator("from_", "to")
    @classmethod
    def _non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("range must be >= 0")
        return v

    model_config = {"populate_by_name": True, "alias_generator": None}


class UtilTable(BaseModel):
    kw: List[UtilRange]
    hp: List[UtilRange]


class CustomsConfig(BaseModel):
    version: str
    duty_eur_per_cc: List[DutyRange]
    util_cc_buckets: List[UtilBucket]
    selection_rule: str
    util_tables: Dict[str, UtilTable]
    util_tables_under3: Optional[Dict[str, UtilTable]] = None
    util_tables_3_5: Optional[Dict[str, UtilTable]] = None
    util_tables_electric: Optional[Dict[str, UtilTable]] = None

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version(cls, v):
        if v is None:
            return ""
        return str(v)

    @model_validator(mode="after")
    def _validate_ranges(self):
        _validate_no_overlap(self.duty_eur_per_cc, "duty_eur_per_cc")
        _validate_no_overlap(self.util_cc_buckets, "util_cc_buckets")
        return self


def _validate_no_overlap(rows: List[Any], name: str) -> None:
    if not rows:
        raise ValueError(f"{name} is empty")
    ranges = sorted([(r.from_cc, r.to_cc) for r in rows], key=lambda x: (x[0], x[1]))
    last_to = None
    for lo, hi in ranges:
        if hi < lo:
            raise ValueError(f"{name} invalid range {lo}-{hi}")
        if last_to is not None and lo <= last_to:
            if lo <= last_to and lo != last_to + 1:
                raise ValueError(f"{name} ranges overlap: {lo}-{hi}")
        last_to = hi


_CFG_CACHE: Optional[CustomsConfig] = None


def load_customs_config(path: Path) -> CustomsConfig:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return CustomsConfig.model_validate(data)


def get_customs_config() -> CustomsConfig:
    global _CFG_CACHE
    if _CFG_CACHE is not None:
        return _CFG_CACHE
    path = Path("/app/backend/app/config/customs.yml")
    if not path.exists():
        path = Path(__file__).resolve().parent.parent / "config" / "customs.yml"
    _CFG_CACHE = load_customs_config(path)
    return _CFG_CACHE


def _find_range_util(rows: List[UtilRange], val: float) -> Optional[UtilRange]:
    for r in rows:
        if r.from_ <= val <= r.to:
            return r
    return None


def calc_duty_eur(engine_cc: int, cfg: CustomsConfig) -> float:
    for row in cfg.duty_eur_per_cc:
        if row.from_cc <= engine_cc <= row.to_cc:
            return float(engine_cc) * float(row.eur_per_cc)
    raise ValueError("duty_eur_per_cc not found for engine_cc")


def _pick_util_tables(cfg: CustomsConfig, age_bucket: Optional[str]) -> Dict[str, UtilTable]:
    if age_bucket == "under_3" and cfg.util_tables_under3:
        return cfg.util_tables_under3
    if age_bucket == "3_5" and cfg.util_tables_3_5:
        return cfg.util_tables_3_5
    if age_bucket == "electric" and cfg.util_tables_electric:
        return cfg.util_tables_electric
    return cfg.util_tables


def calc_util_fee_rub(
    engine_cc: int,
    kw: Optional[float],
    hp: Optional[int],
    cfg: CustomsConfig,
    age_bucket: Optional[str] = None,
) -> int:
    bucket = None
    for b in cfg.util_cc_buckets:
        if b.from_cc <= engine_cc <= b.to_cc:
            bucket = b
            break
    if bucket is None:
        raise ValueError("util_cc_bucket not found for engine_cc")
    tables = _pick_util_tables(cfg, age_bucket)
    table = tables.get(bucket.table)
    if not table:
        raise ValueError(f"util table {bucket.table} not found")

    use_kw = kw is not None and float(kw) > 0
    if use_kw:
        rng = _find_range_util(table.kw, float(kw))
    else:
        if hp is None:
            raise ValueError("hp is required when kw is not provided")
        rng = _find_range_util(table.hp, float(hp))
    if rng is None:
        raise ValueError("util range not found for provided power")
    return int(rng.price_rub)
