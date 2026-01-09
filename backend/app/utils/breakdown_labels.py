from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

_LABELS: Dict[str, str] = {
    "bank": "Банковские расходы",
    "purchase": "Покупка автомобиля",
    "inspection": "Осмотр",
    "delivery_eu_msk": "Доставка Европа — МСК",
    "delivery": "Доставка",
    "insurance": "Страхование",
    "without_rf": "Без платежей в РФ",
    "broker_elpts": "Брокер и ЭлПТС",
    "customs_fee": "Таможенный сбор",
    "customs_duty": "Таможенная пошлина",
    "Пошлина": "Таможенная пошлина",
    "recycling_fee": "Утилизационный сбор",
    "Утилизационный сбор": "Утилизационный сбор",
    "vat": "НДС",
    "vat_percent": "НДС",
    "excise": "Акциз",
}


def label_for(key: str) -> str:
    return _LABELS.get(key, key.replace("_", " ").capitalize())
