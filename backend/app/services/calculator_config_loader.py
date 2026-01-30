from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
import yaml


class FxOverrides(BaseModel):
    EURRUB: Optional[float] = None
    USDRUB: Optional[float] = None


class FxConfig(BaseModel):
    source: str = "cbr"
    cache_ttl_sec: int = 3600
    overrides: FxOverrides = FxOverrides()


class RuleConfig(BaseModel):
    age_bucket_over_5y_as_3_5: bool = True
    price_mode: str = "netto_preferred"  # netto_preferred | brutto | netto_only


class RangeEurPerCc(BaseModel):
    cc_from: int
    cc_to: int
    eur_per_cc: float

    @field_validator("cc_from", "cc_to")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("cc_from/cc_to must be >= 0")
        return v


class RangeUtilRub(BaseModel):
    cc_from: int
    cc_to: int
    util_rub: float

    @field_validator("cc_from", "cc_to")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("cc_from/cc_to must be >= 0")
        return v


class RangeExciseHp(BaseModel):
    cc_from: int = Field(..., alias="from_hp")
    cc_to: int = Field(..., alias="to_hp")
    rub_per_hp: float


class RangePowerFee(BaseModel):
    age_bucket: str
    cc_from: int = Field(..., alias="from_hp")
    cc_to: int = Field(..., alias="to_hp")
    rub: float


class ExpensesSplit(BaseModel):
    eur: Dict[str, float] = {}
    rub: Dict[str, float] = {}


class ScenarioIce(BaseModel):
    label: str
    expenses: ExpensesSplit
    duty_eur_per_cc_table: List[RangeEurPerCc]
    util_rub_by_engine_cc: List[RangeUtilRub]

    @model_validator(mode="after")
    def _validate_ranges(self):
        _validate_no_overlap(self.duty_eur_per_cc_table, "duty_eur_per_cc_table")
        _validate_no_overlap(self.util_rub_by_engine_cc, "util_rub_by_engine_cc")
        return self


class ScenarioElectric(BaseModel):
    label: str
    expenses: ExpensesSplit
    duty_percent: float
    vat_percent: float
    excise_rub_per_hp_table: List[RangeExciseHp] = []
    power_fee_rub_table: List[RangePowerFee] = []
    util_rub_flat: Optional[float] = None


class Scenarios(BaseModel):
    under_3_ice: ScenarioIce
    age_3_5_ice: ScenarioIce
    electric: ScenarioElectric


class CalculatorConfigYaml(BaseModel):
    version: str
    fx: FxConfig
    rules: RuleConfig
    scenarios: Scenarios


_LABEL_MAP: Dict[str, str] = {
    "bank_transfer_eu": "Банк, за перевод",
    "purchase_netto": "Покупка по НЕТТО",
    "inspection": "Осмотр подборщиком",
    "delivery_eu_minsk": "Доставка Европы- Минска",
    "customs_by": "Таможня РБ",
    "customs_transfer_fee": "Комиссия за перевод денег за таможню",
    "delivery_minsk_moscow": "Доставка Минск- Москва",
    "elpts": "ЭЛПТС",
    "insurance_broker_commission": "Страхование, брокер",
    "investor_fee": "Инвестор",
    "delivery_eu_moscow": "Доставка Европа- МСК",
    "broker_elpts": "Брокер и ЭлПТС",
    "customs_fee": "Таможенный сбор",
}


def _validate_no_overlap(rows: List[Any], name: str) -> None:
    if not rows:
        raise ValueError(f"{name} is empty")
    ranges = sorted([(r.cc_from, r.cc_to) for r in rows], key=lambda x: (x[0], x[1]))
    last_to = None
    for lo, hi in ranges:
        if hi < lo:
            raise ValueError(f"{name} invalid range {lo}-{hi}")
        if last_to is not None and lo <= last_to:
            # overlap or touching is allowed? treat overlap as error
            if lo <= last_to and lo != last_to + 1:
                raise ValueError(f"{name} ranges overlap: {lo}-{hi}")
        last_to = hi


def load_yaml(path: Path) -> CalculatorConfigYaml:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return CalculatorConfigYaml.model_validate(data)


def to_runtime_payload(cfg: CalculatorConfigYaml) -> Dict[str, Any]:
    def _expenses(exp: ExpensesSplit) -> Dict[str, float]:
        out: Dict[str, float] = {}
        out.update(exp.eur or {})
        out.update(exp.rub or {})
        return out

    def _duty(rows: List[RangeEurPerCc]) -> List[Dict[str, Any]]:
        return [{"from": r.cc_from, "to": r.cc_to, "eur_per_cc": r.eur_per_cc} for r in rows]

    def _util(rows: List[RangeUtilRub]) -> List[Dict[str, Any]]:
        return [{"from": r.cc_from, "to": r.cc_to, "rub": r.util_rub} for r in rows]

    def _excise(rows: List[RangeExciseHp]) -> List[Dict[str, Any]]:
        return [{"from_hp": r.cc_from, "to_hp": r.cc_to, "rub_per_hp": r.rub_per_hp} for r in rows]

    def _power_fee(rows: List[RangePowerFee]) -> List[Dict[str, Any]]:
        return [{"age_bucket": r.age_bucket, "from_hp": r.cc_from, "to_hp": r.cc_to, "rub": r.rub} for r in rows]

    eur_default = cfg.fx.overrides.EURRUB if cfg.fx and cfg.fx.overrides else None
    usd_default = cfg.fx.overrides.USDRUB if cfg.fx and cfg.fx.overrides else None

    payload = {
        "meta": {
            "version": cfg.version,
            "eur_rate_default": eur_default,
            "usd_rate_default": usd_default,
        },
        "rules": {
            "age_bucket_over_5y_as_3_5": cfg.rules.age_bucket_over_5y_as_3_5,
            "price_mode": cfg.rules.price_mode,
        },
        "label_map": _LABEL_MAP,
        "scenarios": {
            "under_3": {
                "label": cfg.scenarios.under_3_ice.label,
                "expenses": _expenses(cfg.scenarios.under_3_ice.expenses),
                "duty_by_cc": _duty(cfg.scenarios.under_3_ice.duty_eur_per_cc_table),
                "util_by_cc": _util(cfg.scenarios.under_3_ice.util_rub_by_engine_cc),
            },
            "3_5": {
                "label": cfg.scenarios.age_3_5_ice.label,
                "expenses": _expenses(cfg.scenarios.age_3_5_ice.expenses),
                "duty_by_cc": _duty(cfg.scenarios.age_3_5_ice.duty_eur_per_cc_table),
                "util_by_cc": _util(cfg.scenarios.age_3_5_ice.util_rub_by_engine_cc),
            },
            "electric": {
                "label": cfg.scenarios.electric.label,
                "expenses": _expenses(cfg.scenarios.electric.expenses),
                "duty_percent": cfg.scenarios.electric.duty_percent,
                "vat_percent": cfg.scenarios.electric.vat_percent,
                "excise_by_hp": _excise(cfg.scenarios.electric.excise_rub_per_hp_table),
                "power_fee": _power_fee(cfg.scenarios.electric.power_fee_rub_table),
                "util_rub": cfg.scenarios.electric.util_rub_flat or 0,
            },
        },
    }
    return payload


def load_runtime_payload(path: Path) -> Dict[str, Any]:
    cfg = load_yaml(path)
    return to_runtime_payload(cfg)


def update_config_from_dict(data: Dict[str, Any], path: Path) -> CalculatorConfigYaml:
    """
    Validate and persist calculator config to YAML.
    TODO: hook into admin/bot update flow to persist to DB as well.
    """
    cfg = CalculatorConfigYaml.model_validate(data)
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return cfg
