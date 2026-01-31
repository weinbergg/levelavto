from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
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


class PercentFixed(BaseModel):
    percent: float = 0.0
    fixed: float = 0.0


class RangeExciseHp(BaseModel):
    from_hp: float
    to_hp: float
    rub_per_hp: float


class RangeExciseKw(BaseModel):
    from_kw: float
    to_kw: float
    rub_per_kw: float


class ScenarioUnder3(BaseModel):
    label: str
    bank_transfer_eu: PercentFixed
    purchase_netto: PercentFixed
    inspection: float
    delivery_eu_minsk: float
    customs_by_percent: float
    customs_transfer_fee_percent: float
    delivery_minsk_moscow: float
    elpts: float
    insurance_broker_commission_percent: float
    investor_fee: float = 0.0
    broker_elpts_rub: float = 0.0
    customs_fee_rub: float = 0.0
    duty_enabled: bool = True


class Scenario3to5(BaseModel):
    label: str
    bank_transfer_eu: PercentFixed
    purchase_netto: PercentFixed
    inspection: float
    delivery_eu_moscow: float
    insurance_broker_commission_percent: float
    broker_elpts_rub: float = 65000.0
    customs_fee_rub: float = 30000.0
    duty_enabled: bool = True


class ScenarioElectric(BaseModel):
    label: str
    bank_transfer_eu: PercentFixed
    purchase_netto: PercentFixed
    inspection: float
    delivery_eu_moscow: float
    insurance_broker_commission_percent: float
    broker_elpts_rub: float = 115000.0
    customs_fee_rub: float = 30000.0
    import_duty_percent: float = 0.15
    vat_percent: float = 0.22
    excise_by_hp: List[RangeExciseHp] = []
    excise_by_kw: List[RangeExciseKw] = []


class Scenarios(BaseModel):
    under_3: ScenarioUnder3
    age_3_5: Scenario3to5
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
    "import_duty": "Ввозная пошлина 15%",
    "excise": "Акциз",
    "vat": "НДС",
    "util_fee": "Утилизационный сбор",
    "duty": "Пошлина",
    "subtotal_eur": "Итого EUR (до перевода)",
    "subtotal_rub": "Итого RUB (до пошлин)",
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
                "label": cfg.scenarios.under_3.label,
                "bank_transfer_eu": cfg.scenarios.under_3.bank_transfer_eu.model_dump(),
                "purchase_netto": cfg.scenarios.under_3.purchase_netto.model_dump(),
                "inspection": cfg.scenarios.under_3.inspection,
                "delivery_eu_minsk": cfg.scenarios.under_3.delivery_eu_minsk,
                "customs_by_percent": cfg.scenarios.under_3.customs_by_percent,
                "customs_transfer_fee_percent": cfg.scenarios.under_3.customs_transfer_fee_percent,
                "delivery_minsk_moscow": cfg.scenarios.under_3.delivery_minsk_moscow,
                "elpts": cfg.scenarios.under_3.elpts,
                "insurance_broker_commission_percent": cfg.scenarios.under_3.insurance_broker_commission_percent,
                "investor_fee": cfg.scenarios.under_3.investor_fee,
                "broker_elpts_rub": cfg.scenarios.under_3.broker_elpts_rub,
                "customs_fee_rub": cfg.scenarios.under_3.customs_fee_rub,
                "duty_enabled": cfg.scenarios.under_3.duty_enabled,
            },
            "3_5": {
                "label": cfg.scenarios.age_3_5.label,
                "bank_transfer_eu": cfg.scenarios.age_3_5.bank_transfer_eu.model_dump(),
                "purchase_netto": cfg.scenarios.age_3_5.purchase_netto.model_dump(),
                "inspection": cfg.scenarios.age_3_5.inspection,
                "delivery_eu_moscow": cfg.scenarios.age_3_5.delivery_eu_moscow,
                "insurance_broker_commission_percent": cfg.scenarios.age_3_5.insurance_broker_commission_percent,
                "broker_elpts_rub": cfg.scenarios.age_3_5.broker_elpts_rub,
                "customs_fee_rub": cfg.scenarios.age_3_5.customs_fee_rub,
                "duty_enabled": cfg.scenarios.age_3_5.duty_enabled,
            },
            "electric": {
                "label": cfg.scenarios.electric.label,
                "bank_transfer_eu": cfg.scenarios.electric.bank_transfer_eu.model_dump(),
                "purchase_netto": cfg.scenarios.electric.purchase_netto.model_dump(),
                "inspection": cfg.scenarios.electric.inspection,
                "delivery_eu_moscow": cfg.scenarios.electric.delivery_eu_moscow,
                "insurance_broker_commission_percent": cfg.scenarios.electric.insurance_broker_commission_percent,
                "broker_elpts_rub": cfg.scenarios.electric.broker_elpts_rub,
                "customs_fee_rub": cfg.scenarios.electric.customs_fee_rub,
                "import_duty_percent": cfg.scenarios.electric.import_duty_percent,
                "vat_percent": cfg.scenarios.electric.vat_percent,
                "excise_by_hp": [r.model_dump() for r in cfg.scenarios.electric.excise_by_hp],
                "excise_by_kw": [r.model_dump() for r in cfg.scenarios.electric.excise_by_kw],
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
