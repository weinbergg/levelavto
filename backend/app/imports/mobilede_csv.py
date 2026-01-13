from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Iterator, List, Optional
import csv
import json
import logging


logger = logging.getLogger("mobilede_csv")


@dataclass
class MobileDeCsvRow:
    inner_id: str
    mark: str
    model: str
    title: str
    sub_title: str
    url: str
    price_eur: Optional[Decimal]
    price_eur_nt: Optional[Decimal]
    vat: Optional[str]
    year: Optional[int]
    km_age: Optional[int]
    color: Optional[str]
    owners_count: Optional[int]
    section: Optional[str]
    address: Optional[str]
    options: List[str]
    engine_type: Optional[str]
    displacement: Optional[Decimal]
    horse_power: Optional[int]
    power_kw: Optional[Decimal] = None
    body_type: Optional[str]
    transmission: Optional[str]
    full_fuel_type: Optional[str]
    fuel_consumption: Optional[str]
    co_emission: Optional[str]
    num_seats: Optional[int]
    doors_count: Optional[str]
    emission_class: Optional[str]
    emissions_sticker: Optional[str]
    climatisation: Optional[str]
    park_assists: Optional[str]
    airbags: Optional[str]
    manufacturer_color: Optional[str]
    interior_design: Optional[str]
    efficiency_class: Optional[str]
    first_registration: Optional[str]
    ready_to_drive: Optional[str]
    price_rating_label: Optional[str]
    seller_country: Optional[str]
    created_at: Optional[datetime]
    image_urls: List[str]


def _to_int(value: str | None) -> Optional[int]:
    if value is None:
        return None
    v = value.strip().replace("\xa0", " ")
    if not v:
        return None
    try:
        return int("".join(ch for ch in v if ch.isdigit()))
    except Exception:
        return None


def _to_decimal(value: str | None) -> Optional[Decimal]:
    if value is None:
        return None
    v = value.strip().replace("\xa0", " ")
    if not v:
        return None
    try:
        # keep digits and dot/comma
        normalized = v.replace(",", ".")
        filtered = "".join(
            ch for ch in normalized if ch.isdigit() or ch == ".")
        if not filtered:
            return None
        return Decimal(filtered)
    except Exception:
        return None


def _to_str(value: str | None) -> Optional[str]:
    if value is None:
        return None
    v = value.strip().replace("\xa0", " ")
    return v or None


def _parse_created_at(value: str | None) -> Optional[datetime]:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    # try common formats
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None


def _parse_image_urls(raw: str | None) -> List[str]:
    if not raw:
        return []


def _parse_options(raw: str | None) -> List[str]:
    if not raw:
        return []
    s = raw.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1]
    s = s.replace('""', '"')
    try:
        data = json.loads(s)
        if isinstance(data, list):
            return [str(x).strip() for x in data if isinstance(x, (str, int, float))]
    except Exception as exc:
        logger.warning("Failed to parse options JSON: %r (%s)", raw[:200], exc)
    return []
    s = raw.strip()
    # strip surrounding quotes if any
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1]
    # replace doubled quotes to make valid JSON
    s = s.replace('""', '"')
    try:
        data = json.loads(s)
        urls: List[str] = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    url = item.get("url")
                    if isinstance(url, str) and url.strip():
                        urls.append(url.strip())
                elif isinstance(item, str):
                    if item.strip():
                        urls.append(item.strip())
        return urls
    except Exception as exc:
        logger.warning(
            "Failed to parse image_urls JSON: %r (%s)", raw[:200], exc)
        return []


def iter_mobilede_csv_rows(file_path: str) -> Iterator[MobileDeCsvRow]:
    with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f, delimiter="|", quotechar='"',
                            escapechar=None, strict=False)
        header = next(reader, None)
        # We'll match by name if header available; else assume fixed order
        name_to_idx = {}
        if header:
            name_to_idx = {name.strip(): i for i, name in enumerate(header)}
        for row in reader:
            def get(name: str, idx_fallback: int | None = None) -> Optional[str]:
                if name_to_idx and name in name_to_idx and name_to_idx[name] < len(row):
                    return row[name_to_idx[name]]
                if idx_fallback is not None and idx_fallback < len(row):
                    return row[idx_fallback]
                return None
            inner_id = _to_str(get("inner_id")) or ""
            if not inner_id:
                continue
            yield MobileDeCsvRow(
                inner_id=inner_id,
                mark=_to_str(get("mark")) or "",
                model=_to_str(get("model")) or "",
                title=_to_str(get("title")) or "",
                sub_title=_to_str(get("sub_title")) or "",
                url=_to_str(get("url")) or "",
                price_eur=_to_decimal(get("price_eur")),
                price_eur_nt=_to_decimal(get("price_eur_nt")),
                vat=_to_str(get("vat")),
                year=_to_int(get("year")),
                km_age=_to_int(get("km_age")),
                color=_to_str(get("color")),
                owners_count=_to_int(get("owners_count")),
                section=_to_str(get("section")),
                address=_to_str(get("address")),
                options=_parse_options(get("options")),
                engine_type=_to_str(get("engine_type")),
                displacement=_to_decimal(get("displacement")),
                horse_power=_to_int(get("horse_power")),
                power_kw=_to_decimal(get("power_kw")),
                body_type=_to_str(get("body_type")),
                transmission=_to_str(get("transmission")),
                full_fuel_type=_to_str(get("full_fuel_type")),
                fuel_consumption=_to_str(get("fuel_consumption")),
                co_emission=_to_str(get("co_emission")),
                num_seats=_to_int(get("num_seats")),
                doors_count=_to_str(get("doors_count")),
                emission_class=_to_str(get("emission_class")),
                emissions_sticker=_to_str(get("emissions_sticker")),
                climatisation=_to_str(get("climatisation")),
                park_assists=_to_str(get("park_assists")),
                airbags=_to_str(get("airbags")),
                manufacturer_color=_to_str(get("manufacturer_color")),
                interior_design=_to_str(get("interior_design")),
                efficiency_class=_to_str(get("efficiency_class")),
                first_registration=_to_str(get("first_registration")),
                ready_to_drive=_to_str(get("ready_to_drive")),
                price_rating_label=_to_str(get("price_rating_label")),
                seller_country=_to_str(get("seller_country")),
                created_at=_parse_created_at(get("created_at")),
                image_urls=_parse_image_urls(get("image_urls")),
            )
