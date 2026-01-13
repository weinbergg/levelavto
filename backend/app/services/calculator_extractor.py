from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Callable, Tuple
import pandas as pd


class CalculatorExtractor:
    """
    Extracts calculator config from Excel "Калькулятор Авто под заказ.xlsx".
    Produces a structured payload for CalculatorConfig.
    """

    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def _norm_sheet_name(name: str) -> str:
        return (
            name.lower()
            .replace(" ", "")
            .replace("-", "")
            .replace("–", "")
            .replace("—", "")
            .replace("_", "")
        )

    def _find_sheet(self, xls: pd.ExcelFile, variants: List[str]) -> str:
        target = {self._norm_sheet_name(v): v for v in variants}
        for name in xls.sheet_names:
            norm = self._norm_sheet_name(name)
            if norm in target:
                return name
        raise ValueError(f"Не найден лист: {variants[0]}")

    def _cleanup_number(self, val):
        if pd.isna(val):
            return None
        if isinstance(val, str):
            val = val.replace(" ", "").replace("\u00a0", "")
            val = val.replace(",", ".")
        try:
            return float(val)
        except Exception:
            return None

    def extract(self) -> Dict[str, Any]:
        xls = pd.ExcelFile(self.path)

        def load(variants: List[str]) -> pd.DataFrame:
            sheet_name = self._find_sheet(xls, variants)
            return pd.read_excel(self.path, sheet_name=sheet_name, header=None)

        df_under3 = load(["до3хлет", "до3-лет", "до3лет", "до3", "до3х"])
        df_3_5 = load(["35лет", "3-5лет", "3–5лет", "3-5", "3–5"])
        df_e = load(["электро", "электро/гибрид", "электрои гибрид"])

        # Known expense rows by label fragments
        def find_expenses(df: pd.DataFrame, mapping: Dict[str, str]) -> Dict[str, float]:
            out = {}
            for key, label in mapping.items():
                for i, row in df.iterrows():
                    if isinstance(row[0], str) and label.lower() in row[0].lower():
                        val = self._cleanup_number(row[1])
                        if val is not None:
                            out[key] = val
                        break
            return out

        expenses_under3 = find_expenses(
            df_under3,
            {
                "bank": "Банк, за перевод",
                "purchase": "Покупка по НЕТТО",
                "inspection": "Осмотр подборщиком",
                "delivery_eu_minsk": "Доставка Европы- Минска",
                "customs_by": "Таможня РБ",
                "transfer_fee": "Комиссия за перевод денег за таможню",
                "delivery_msk": "Доставка Минск- Москва",
                "elpts": "ЭЛПТС",
                "insurance": "Страхование, брокер",
                "investor": "Инвестор",
            },
        )
        expenses_3_5 = find_expenses(
            df_3_5,
            {
                "bank": "Банк, за перевод",
                "purchase": "Покупка по НЕТТО",
                "inspection": "Осмотр подборщиком",
                "delivery_eu_msk": "Доставка Европа- МСК",
                "insurance": "Страхование, брокер",
                "broker_elpts": "Брокер и ЭлПТС",
                "customs_fee": "Таможенный сбор",
            },
        )
        expenses_e = find_expenses(
            df_e,
            {
                "bank": "Банк, за перевод",
                "purchase": "Покупка по НЕТТО",
                "inspection": "Осмотр подборщиком",
                "delivery_eu_msk": "Доставка Европа- МСК",
                "insurance": "Страхование, брокер",
                "investor": "Инвестор",
                "customs_fee": "Таможенный сбор",
                "broker_elpts": "Брокер и ЭлПТС",
            },
        )

        def _find_triplets(
            df: pd.DataFrame,
            predicate: Callable[[float, float, float], bool],
        ) -> List[Tuple[float, float, float]]:
            rows: List[Tuple[float, float, float]] = []
            for i in range(df.shape[0]):
                row = df.iloc[i]
                for c in range(max(df.shape[1] - 2, 0)):
                    a = self._cleanup_number(row[c])
                    b = self._cleanup_number(row[c + 1])
                    rate = self._cleanup_number(row[c + 2])
                    if a is None or b is None or rate is None:
                        continue
                    if b < a:
                        continue
                    if predicate(a, b, rate):
                        rows.append((a, b, rate))
            return rows

        def extract_duty_table(df: pd.DataFrame) -> List[Dict[str, Any]]:
            rows = _find_triplets(
                df,
                lambda a, b, rate: 0 <= a <= 10000 and 0 <= b <= 10000 and 0 < rate < 10,
            )
            if not rows:
                # fallback to known baseline if таблица не найдена
                defaults = [
                    (0, 1000, 1.5),
                    (1001, 1500, 1.7),
                    (1501, 1800, 2.5),
                    (1801, 2300, 2.7),
                    (2301, 3000, 3.0),
                    (3001, 8000, 3.6),
                ]
                return [{"from": a, "to": b, "eur_per_cc": rate} for a, b, rate in defaults]
            return [{"from": a, "to": b, "eur_per_cc": rate} for a, b, rate in rows]

        def extract_excise_table(df: pd.DataFrame) -> List[Dict[str, Any]]:
            rows = _find_triplets(
                df,
                lambda a, b, rate: 0 <= a <= 5000 and 0 <= b <= 5000 and 0 < rate < 10000,
            )
            rows = [(a, b, rate) for a, b, rate in rows if b <= 5000]
            if not rows:
                raise ValueError("Не найдена таблица акциза по л.с. для электро")
            return [{"from_hp": a, "to_hp": b, "rub_per_hp": rate} for a, b, rate in rows]

        def extract_power_fee(df: pd.DataFrame) -> List[Dict[str, Any]]:
            fees: List[Dict[str, Any]] = []
            for i in range(df.shape[0]):
                row = df.iloc[i]
                for c in range(max(df.shape[1] - 3, 0)):
                    a = self._cleanup_number(row[c])
                    b = self._cleanup_number(row[c + 1])
                    under3 = self._cleanup_number(row[c + 2])
                    three5 = self._cleanup_number(row[c + 3])
                    if None in (a, b, under3, three5):
                        continue
                    if b < a or a < 0 or b > 5000:
                        continue
                    if under3 <= 0 and three5 <= 0:
                        continue
                    fees.append({"from_hp": a, "to_hp": b, "age_bucket": "under_3", "rub": under3})
                    fees.append({"from_hp": a, "to_hp": b, "age_bucket": "3_5", "rub": three5})
            if not fees:
                raise ValueError("Не найдена таблица сумм по мощности/возрасту для электро")
            return fees

        def extract_util_flat(df: pd.DataFrame) -> float:
            """
            Берём максимальное числовое значение из строки с "Утилизационный сбор".
            В файле значения могут дублироваться в разных столбцах (база, коэффициенты, рассчитанный итог),
            поэтому выбираем максимальное как итоговую сумму, совпадающую с Excel.
            """
            mask = df.apply(lambda r: r.astype(str).str.contains(
                "утилизационный", case=False, na=False).any(), axis=1)
            util_rows = df[mask]
            if util_rows.empty:
                raise ValueError("Не найдена строка утилизационного сбора")
            nums = pd.to_numeric(util_rows.applymap(
                lambda x: str(x).replace(" ", "").replace("\xa0", "") if x is not None else x).stack(), errors="coerce").dropna()
            if nums.empty:
                raise ValueError("Не найдено числовых значений утилизационного сбора")
            val = float(nums.max())
            return val

        duty_table = extract_duty_table(df_under3)
        util_under3_val = extract_util_flat(df_under3)
        util_3_5_val = extract_util_flat(df_3_5)
        util_electric_val = extract_util_flat(df_e)
        util_under3 = [{"from": 0, "to": 10000, "rub": util_under3_val}]
        util_3_5 = [{"from": 0, "to": 10000, "rub": util_3_5_val}]
        util_electric_rub = util_electric_val

        excise_table = extract_excise_table(df_e)
        electric_excise_rates = excise_table
        electric_power_fee = extract_power_fee(df_e)

        payload = {
            "meta": {
                "eur_rate_default": 94.9564,
                "usd_rate_default": 85.0,
            },
            "scenarios": {
                "under_3": {
                    "label": "до 3 лет",
                    "expenses": expenses_under3,
                    "duty_by_cc": duty_table,
                    "util_by_cc": util_under3,
                    "customs_fee_rub": 0,
                    "broker_elpts_rub": 0,
                },
                "3_5": {
                    "label": "3-5 лет",
                    "expenses": expenses_3_5,
                    "duty_by_cc": duty_table,
                    "util_by_cc": util_3_5,
                    "customs_fee_rub": expenses_3_5.get("customs_fee", 30000),
                    "broker_elpts_rub": expenses_3_5.get("broker_elpts", 115000),
                },
                "electric": {
                    "label": "Электро",
                    "expenses": expenses_e,
                    "duty_percent": 0.15,
                    "vat_percent": 0.22,
                    "customs_fee_rub": expenses_e.get("customs_fee", 30000),
                    "broker_elpts_rub": expenses_e.get("broker_elpts", 115000),
                    "util_rub": util_electric_rub,
                    "excise_by_hp": electric_excise_rates,
                    "power_fee": electric_power_fee,
                },
            },
        }
        self.validate_payload(payload)
        return payload

    @staticmethod
    def validate_payload(payload: Dict[str, Any]) -> None:
        if not payload or "scenarios" not in payload:
            raise ValueError("Пустой payload калькулятора")
        scenarios = payload.get("scenarios", {})
        for key in ("under_3", "3_5", "electric"):
            if key not in scenarios:
                raise ValueError(f"Отсутствует сценарий {key}")

        def ensure_list(name: str, items: List[Dict[str, Any]]):
            if not items:
                raise ValueError(f"Не найдена таблица {name}")

        under3 = scenarios["under_3"]
        if not under3.get("expenses"):
            raise ValueError("Нет расходов для сценария до 3 лет")
        ensure_list("duty_by_cc (до 3)", under3.get("duty_by_cc", []))
        ensure_list("util_by_cc (до 3)", under3.get("util_by_cc", []))

        three5 = scenarios["3_5"]
        if not three5.get("expenses"):
            raise ValueError("Нет расходов для сценария 3-5 лет")
        ensure_list("duty_by_cc (3-5)", three5.get("duty_by_cc", []))
        ensure_list("util_by_cc (3-5)", three5.get("util_by_cc", []))

        electric = scenarios["electric"]
        if not electric.get("expenses"):
            raise ValueError("Нет расходов для сценария Электро")
        ensure_list("excise_by_hp (электро)", electric.get("excise_by_hp", []))
        ensure_list("power_fee (электро)", electric.get("power_fee", []))
