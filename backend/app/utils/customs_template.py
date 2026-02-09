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


def build_util_template(data: Dict[str, Any]) -> str:
    lines = [
        "# util_fee_template v1",
        "# columns: age_bucket,cc_table,power_type,from,to,price_rub",
        "# age_bucket: under_3 | 3_5 | electric",
        "# power_type: kw | hp",
    ]
    for age_bucket in ("under_3", "3_5", "electric"):
        tables = _iter_tables(data, age_bucket)
        for table_name, table in tables.items():
            for power_type in ("kw", "hp"):
                rows = (table or {}).get(power_type) or []
                for row in rows:
                    lines.append(
                        f"{age_bucket},{table_name},{power_type},{row.get('from',0)},{row.get('to')},{row.get('price_rub')}"
                    )
    lines.append("")
    return "\n".join(lines)


def _split_line(line: str) -> List[str]:
    # allow comma, semicolon, or tab
    for sep in (",", ";", "\t"):
        parts = [p.strip() for p in line.split(sep)]
        if len(parts) >= 6:
            return parts
    return [p.strip() for p in line.split(",")]


def apply_util_template(data: Dict[str, Any], content: str) -> ApplyStats:
    stats = ApplyStats()
    lines = [ln.strip() for ln in content.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    for ln in lines:
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

