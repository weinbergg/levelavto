from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple
import datetime as dt
import yaml


CUSTOMS_PATH = Path("/app/backend/app/config/customs.yml")


@dataclass
class ApplyStats:
    updated: int = 0
    skipped: int = 0
    errors: int = 0


def _resolve_customs_path() -> Path:
    if CUSTOMS_PATH.exists():
        return CUSTOMS_PATH
    return Path(__file__).resolve().parent.parent / "config" / "customs.yml"


def load_customs_dict() -> Dict[str, Any]:
    path = _resolve_customs_path()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("customs.yml invalid or empty")
    return data


def save_customs_dict(data: Dict[str, Any]) -> None:
    path = _resolve_customs_path()
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def bump_customs_version(data: Dict[str, Any]) -> None:
    today = dt.datetime.now().strftime("%Y_%m_%d")
    data["version"] = str(today)


def _iter_tables(data: Dict[str, Any], age_bucket: str) -> Dict[str, Any]:
    key = {
        "under_3": "util_tables_under3",
        "3_5": "util_tables_3_5",
        "electric": "util_tables_electric",
    }.get(age_bucket, "util_tables")
    tables = data.get(key) or data.get("util_tables") or {}
    if not isinstance(tables, dict):
        return {}
    return tables


_AGE_LABELS_RU = {
    "under_3": "ДО 3 ЛЕТ (новые автомобили)",
    "3_5": "3–5 ЛЕТ (бывшие в употреблении)",
    "electric": "ЭЛЕКТРО / ГИБРИД (любой возраст)",
}

_TABLE_LABELS_RU = {
    "personal": "Физ. лица (для личного пользования)",
    "commercial": "Юр. лица / коммерческое использование",
    "import": "Юр. лица / импорт под перепродажу",
}

_POWER_LABELS_RU = {
    "kw": "кВт",
    "hp": "л.с.",
}


def _table_label_ru(name: str) -> str:
    return _TABLE_LABELS_RU.get(name, name)


def build_util_template(data: Dict[str, Any]) -> str:
    """Build a TXT template that the operator's bookkeeper can edit in Excel.

    The schema (5-tuple key + price) is kept the same — :func:`apply_util_template`
    parses it back — but each section gets a human-readable Russian heading
    so the operator does not need to remember what ``under_3,personal,kw,…``
    means.
    """

    lines: List[str] = [
        "# ════════════════════════════════════════════════════════════════════",
        "# Шаблон ставок утилизационного сбора",
        "#",
        "# Что заполнять:",
        "#   1. Меняйте только ПОСЛЕДНЕЕ число в строке (сумма в рублях).",
        "#   2. Не трогайте первые пять полей — это идентификатор ставки.",
        "#   3. Любая строка, начинающаяся с #, — комментарий, она игнорируется.",
        "#   4. Можно открыть в Excel: разделители — запятая, точка с запятой",
        "#      или табуляция. Файл не зашифрован, можно править блокнотом.",
        "#",
        "# Формат строки (всего 6 значений):",
        "#   age_bucket , cc_table , power_type , от , до , сумма_руб",
        "#",
        "# Возможные значения:",
        "#   age_bucket  : under_3   = до 3 лет (новые)",
        "#                 3_5       = 3–5 лет (б/у)",
        "#                 electric  = электро/гибрид",
        "#   cc_table    : название категории (personal / commercial / …) ",
        "#                 — задаётся в customs.yml, новые названия не появятся",
        "#                 при загрузке этого файла, добавляйте их через ИТ.",
        "#   power_type  : kw  = киловатты",
        "#                 hp  = лошадиные силы",
        "#",
        "# После загрузки изменения попадают в каталог через 1–2 минуты",
        "# (сбрасывается кэш и пересчитывается стоимость).",
        "# ════════════════════════════════════════════════════════════════════",
        "",
    ]

    for age_bucket in ("under_3", "3_5", "electric"):
        tables = _iter_tables(data, age_bucket)
        if not tables:
            continue
        lines.append("")
        lines.append(f"# ─── {_AGE_LABELS_RU.get(age_bucket, age_bucket)} ───")
        for table_name, table in tables.items():
            lines.append(f"#   • Категория: {_table_label_ru(table_name)}")
            for power_type in ("kw", "hp"):
                rows = (table or {}).get(power_type) or []
                if not rows:
                    continue
                lines.append(f"#     ↳ Мощность в {_POWER_LABELS_RU.get(power_type, power_type)}:")
                for row in rows:
                    lines.append(
                        f"{age_bucket},{table_name},{power_type},"
                        f"{row.get('from', 0)},{row.get('to')},{row.get('price_rub')}"
                    )
    lines.append("")
    return "\n".join(lines)


def _strip_inline_comment(line: str) -> str:
    """Drop any ``# …`` tail so operators can annotate rows in the file."""

    hash_idx = line.find("#")
    if hash_idx == -1:
        return line.strip()
    return line[:hash_idx].strip()


def _split_line(line: str) -> List[str]:
    # allow comma, semicolon, or tab
    for sep in (",", ";", "\t"):
        parts = [p.strip() for p in line.split(sep)]
        if len(parts) >= 6:
            return parts
    return [p.strip() for p in line.split(",")]


def apply_util_template(data: Dict[str, Any], content: str) -> ApplyStats:
    stats = ApplyStats()
    cleaned_lines: List[str] = []
    for raw in content.splitlines():
        stripped = _strip_inline_comment(raw)
        if stripped:
            cleaned_lines.append(stripped)
    for ln in cleaned_lines:
        try:
            parts = _split_line(ln)
            if len(parts) < 6:
                stats.skipped += 1
                continue
            age_bucket, table_name, power_type, from_s, to_s, price_s = parts[:6]
            age_bucket = age_bucket.strip()
            power_type = power_type.strip()
            if power_type not in ("kw", "hp"):
                stats.skipped += 1
                continue
            tables_key = {
                "under_3": "util_tables_under3",
                "3_5": "util_tables_3_5",
                "electric": "util_tables_electric",
            }.get(age_bucket, "util_tables")
            tables = data.get(tables_key)
            if tables is None:
                tables = {}
                data[tables_key] = tables
            table = tables.get(table_name)
            if table is None:
                tables[table_name] = {"kw": [], "hp": []}
                table = tables[table_name]
            rows = table.get(power_type)
            if rows is None:
                table[power_type] = []
                rows = table[power_type]
            target_from = float(from_s)
            target_to = float(to_s)
            target_price = float(price_s)
            updated = False
            for row in rows:
                if float(row.get("from", 0)) == target_from and float(row.get("to")) == target_to:
                    row["price_rub"] = target_price
                    updated = True
                    break
            if not updated:
                rows.append({"from": target_from, "to": target_to, "price_rub": target_price})
            stats.updated += 1
        except Exception:
            stats.errors += 1
    return stats

