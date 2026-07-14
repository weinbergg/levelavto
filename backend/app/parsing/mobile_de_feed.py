from __future__ import annotations

import logging
import os
import re
from typing import Iterable, Iterator, List, Optional, Any, Dict, Set
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from .base import CarParsed
from .config import SiteConfig
from ..importing.mobilede_csv import MobileDeCsvRow
from ..services.cars_service import CarsService, normalize_brand, normalize_model_label
from ..utils.spec_inference import normalize_engine_type as _normalize_engine_type_canonical

_logger = logging.getLogger(__name__)


_MOBILEDE_BRAND_ALIASES: Dict[str, str] = {
    "mercedes": "mercedes-benz",
    "mercedes benz": "mercedes-benz",
    "mercedes-benz": "mercedes-benz",
    "мерседес": "mercedes-benz",
    "мерседес-бенц": "mercedes-benz",
    "мерседес бенц": "mercedes-benz",
    "bmw": "bmw",
    "бмв": "bmw",
}


def _normalize_brand_key(name: Optional[str]) -> str:
    if not name:
        return ""
    n = str(name).strip().lower()
    n = re.sub(r"\s+", " ", n)
    return _MOBILEDE_BRAND_ALIASES.get(n, n)


def _load_mobilede_brand_allowlist() -> Set[str]:
    raw = os.environ.get("MOBILEDE_ALLOWED_BRANDS", "").strip()
    if not raw:
        return set()
    out: Set[str] = set()
    for chunk in raw.replace(";", ",").split(","):
        b = _normalize_brand_key(chunk)
        if b:
            out.add(b)
    return out


class MobileDeFeedParser:
    _POWER_KW_RE = re.compile(r"\b(\d{2,4})(?:[.,]\d+)?\s*k\s*w\b", re.IGNORECASE)
    _POWER_HP_RE = re.compile(r"\b(\d{2,4})(?:[.,]\d+)?\s*(?:cv|ps|hp)\b", re.IGNORECASE)
    _PLACEHOLDER_MODELS = {"other", "others"}
    _MODEL_RECOVERY_DONORS = {
        "MERCEDES-BENZ": [
            "A-Class",
            "B-Class",
            "C-Class",
            "CL-Class",
            "CLA",
            "CLC",
            "CLK",
            "CLS",
            "E-Class",
            "G-Class",
            "GLA",
            "GLB",
            "GLC",
            "GLE",
            "GLK",
            "GLS",
            "M-Class",
            "ML",
            "R-Class",
            "S-Class",
            "SL",
            "SLK",
            "SLC",
            "AMG GT",
            "Citan",
            "Sprinter",
            "V-Class",
            "Vito",
            "Viano",
            "X-Class",
            "EQA",
            "EQB",
            "EQC",
            "EQE",
            "EQS",
            "EQV",
            "Maybach",
        ],
    }

    def __init__(self, site_config: SiteConfig):
        self.config = site_config
        self._model_recovery_service = CarsService(None)  # type: ignore[arg-type]
        # Опциональный allowlist брендов через env MOBILEDE_ALLOWED_BRANDS="BMW,Mercedes-Benz".
        # Работает case-insensitively и понимает алиасы ("Мерседес", "БМВ" и т.п.).
        # При пустом значении фильтр отключён — импортируются все бренды.
        self.allowed_brands: Set[str] = _load_mobilede_brand_allowlist()
        self.skipped_brand_not_allowed: int = 0
        if self.allowed_brands:
            _logger.info(
                "[mobile_de_feed] brand allowlist enabled: %s",
                sorted(self.allowed_brands),
            )

    def _payload_from_row(self, row: MobileDeCsvRow) -> Dict[str, Any]:
        data = asdict(row)
        for key, value in list(data.items()):
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    def _normalize_transmission(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        t = raw.lower()
        if "auto" in t:
            return "Automatic"
        if "schalt" in t or "manual" in t:
            return "Manual"
        return raw

    def _normalize_body(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        mapping = {
            "estatecar": "wagon",
            "van": "van",
            "limousine": "sedan",
            "smallcar": "hatchback",
            "offroad": "suv",
            "cabrio": "cabrio",
        }
        r = raw.lower()
        return mapping.get(r, raw)

    _DISCLAIMER_NOISE = (
        "based on",
        "co2",
        "co₂",
        "emission",
        "consumption",
        "combined",
    )

    @classmethod
    def _classify_fuel_text(cls, text: Optional[str]) -> Optional[str]:
        """Map a free-text snippet to one of the canonical fuel labels.

        Delegates to :func:`backend.app.utils.engine_type.canonicalize_engine_type`
        — the project-wide single source of truth — so parser, backfill
        scripts, upsert defensive guard and the DB CHECK constraint can
        never disagree on what counts as canonical.
        """

        from ..utils.engine_type import canonicalize_engine_type
        return canonicalize_engine_type(text)

    def _normalize_engine(
        self,
        raw: Optional[str],
        full: Optional[str],
        *,
        hint_texts: Iterable[Optional[str]] = (),
    ) -> Optional[str]:
        """Normalize mobile.de fuel-type fields into a canonical label.

        mobile.de's CSV ships ``envkv.consumption_fuel`` and ``full_fuel_type``
        which sometimes contain disclaimer text instead of a real fuel type
        (e.g. ``"Based on CO₂ emissions (combined)"``). We must not pass
        such text through verbatim — it ends up in :attr:`Car.engine_type`
        and breaks every downstream filter that matches on the canonical
        ``hybrid|diesel|petrol|electric`` set.

        Strategy:

        1. Try ``full`` first, then ``raw``: pick whichever yields a
           recognised canonical label.
        2. If both contain disclaimer noise, fall back to ``hint_texts`` —
           variant title, model name, URL slug. For 2026 Porsche Cayennes
           in particular the title slug always contains ``e-hybrid``.
        3. If still nothing matches, return ``None`` — better an empty cell
           than disclaimer noise that hides the row from filters.
        """

        for source in (full, raw, *hint_texts):
            label = self._classify_fuel_text(source)
            if label:
                return label
        return None

    def _resolve_engine_cc(self, row: MobileDeCsvRow) -> Optional[int]:
        raw_cc = row.displacement_orig
        if raw_cc is not None:
            try:
                return int(Decimal(raw_cc).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            except Exception:
                pass
        if row.displacement is None:
            return None
        try:
            return int((Decimal(row.displacement) * Decimal("1000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except Exception:
            return None

    def _resolve_price_eur(self, row: MobileDeCsvRow) -> Optional[float]:
        """Pick the price the end-customer actually pays.

        mobile.de exposes two prices in the CSV:

          * ``price_eur``    — gross price (with German 19% VAT). This
            is what every regular buyer sees on the listing page and
            what they pay to the dealer.
          * ``price_eur_nt`` — net price for VAT-deductible export
            buyers (commercial dealers re-exporting outside the EU).
            About 40 % of mobile.de listings ship this field set.

        The previous priority (price_eur_nt first) silently wrote the
        net price into Car.price for every dealer-listed car — which
        meant our public catalogue showed prices ~16 % below the real
        sticker price (=1 / 1.19 net-to-gross ratio). Verified via
        scripts.audit_csv_vs_db: 40 % of sampled rows had
        DB price = CSV price_eur * 0.840 exactly.

        Fix: gross first, net only as a fallback when gross is
        missing. The net value is still preserved in source_payload
        for any downstream tooling that needs it.
        """

        if row.price_eur is not None:
            try:
                return float(row.price_eur)
            except Exception:
                pass
        if row.price_eur_nt is not None:
            try:
                return float(row.price_eur_nt)
            except Exception:
                pass
        return None

    def _resolve_power_kw(self, row: MobileDeCsvRow) -> Optional[float]:
        if row.power_kw is not None:
            try:
                value = float(row.power_kw)
                if 0 < value <= 2500:
                    return value
            except Exception:
                pass
        if row.horse_power:
            try:
                return float(row.horse_power) / 1.35962
            except Exception:
                return None
        for raw in (row.sub_title, row.title, row.description):
            if not raw:
                continue
            match = self._POWER_KW_RE.search(str(raw))
            if match:
                try:
                    value = float(match.group(1))
                except Exception:
                    continue
                if 0 < value <= 2500:
                    return value
        return None

    def _resolve_power_hp(self, row: MobileDeCsvRow) -> Optional[float]:
        if row.horse_power:
            try:
                value = float(row.horse_power)
                if 0 < value <= 3500:
                    return value
            except Exception:
                pass
        if row.power_kw is not None:
            try:
                value = float(row.power_kw)
                if 0 < value <= 2500:
                    return round(value * 1.35962, 1)
            except Exception:
                pass
        for raw in (row.sub_title, row.title, row.description):
            if not raw:
                continue
            match_hp = self._POWER_HP_RE.search(str(raw))
            if match_hp:
                try:
                    value = float(match_hp.group(1))
                except Exception:
                    continue
                if 0 < value <= 3500:
                    return value
            match_kw = self._POWER_KW_RE.search(str(raw))
            if match_kw:
                try:
                    value_kw = float(match_kw.group(1))
                except Exception:
                    continue
                if 0 < value_kw <= 2500:
                    return round(value_kw * 1.35962, 1)
        return None

    def _detect_drive(self, options: List[str]) -> Optional[str]:
        from ..utils.drive_type import canonicalize_drive_type
        return canonicalize_drive_type(" ".join(options))

    def _parse_first_registration(self, raw: Optional[str]) -> tuple[Optional[int], Optional[int]]:
        if not raw:
            return None, None
        value = str(raw).strip()
        if not value:
            return None, None
        normalized = re.sub(r"\s+", "", value)
        if not normalized or normalized.lower() in {"-", "--", "n/a", "na"}:
            return None, None

        # Strict patterns first — they extract both year and month.
        patterns = [
            r"^(?P<month>\d{1,2})[./-](?P<year>\d{4})$",
            r"^(?P<day>\d{1,2})[./-](?P<month>\d{1,2})[./-](?P<year>\d{4})$",
            r"^(?P<year>\d{4})[./-](?P<month>\d{1,2})$",
            r"^(?P<year>\d{4})[./-](?P<month>\d{1,2})[./-](?P<day>\d{1,2})$",
            r"^(?P<year>\d{4})$",
            # Two-digit year forms — common in some legacy feeds (MM/YY).
            r"^(?P<month>\d{1,2})[./-](?P<yy>\d{2})$",
        ]
        for pattern in patterns:
            match = re.match(pattern, normalized)
            if not match:
                continue
            groups = match.groupdict()
            year_raw = groups.get("year")
            month_raw = groups.get("month")
            yy_raw = groups.get("yy")
            try:
                year = int(year_raw) if year_raw else None
            except Exception:
                year = None
            if year is None and yy_raw is not None:
                try:
                    yy = int(yy_raw)
                    # Cut-off: 50..99 -> 19YY, 00..49 -> 20YY (mobile.de feed
                    # is unlikely to contain pre-1950 cars in two-digit form).
                    year = 1900 + yy if yy >= 50 else 2000 + yy
                except Exception:
                    year = None
            try:
                month = int(month_raw) if month_raw else None
            except Exception:
                month = None
            # Lower bound 1900 — includes oldtimers (1938, 1943 etc.).
            if year is None or year < 1900 or year > 2100:
                continue  # try next pattern instead of bailing out
            if month is not None and (month < 1 or month > 12):
                month = None  # keep the year, drop only the bogus month
            return year, month

        # Tolerant fallback — find any 4-digit year-like substring in
        # the raw value (covers free-form strings like "Erstzulassung 1995").
        m = re.search(r"(19|20)\d{2}", value)
        if m:
            try:
                year = int(m.group(0))
                if 1900 <= year <= 2100:
                    return year, None
            except Exception:
                pass
        return None, None

    def _resolve_model(self, row: MobileDeCsvRow) -> Optional[str]:
        raw_model = normalize_model_label(row.model)
        if raw_model and raw_model.casefold() not in self._PLACEHOLDER_MODELS:
            return raw_model
        brand = normalize_brand(row.mark).strip() if row.mark else ""
        donors = self._MODEL_RECOVERY_DONORS.get(brand.upper(), [])
        if not donors:
            return raw_model or None
        for candidate in (row.sub_title, row.title):
            text = normalize_model_label(candidate)
            if not text:
                continue
            recovered = self._model_recovery_service._match_eu_model_donor(text, donors)
            if recovered:
                return recovered
        return raw_model or None

    def iter_parsed_from_csv(self, rows: Iterable[MobileDeCsvRow]) -> Iterator[CarParsed]:
        for row in rows:
            # Brand allowlist (например, для демо-режима: только BMW/Mercedes).
            if self.allowed_brands:
                brand_key = _normalize_brand_key(row.mark)
                if brand_key not in self.allowed_brands:
                    self.skipped_brand_not_allowed += 1
                    continue
            images: List[str] = list(row.image_urls) if row.image_urls else []
            thumb = images[0] if images else None
            option_values = list(row.options or [])
            if row.features:
                option_values.extend(row.features)
            drive = self._detect_drive(option_values)
            payload = self._payload_from_row(row)
            registration_year, registration_month = self._parse_first_registration(
                row.first_registration
            )
            # skip obviously broken rows
            if not row.mark and not row.model and not row.title:
                continue
            if not row.url:
                continue
            engine_type = self._normalize_engine(
                row.envkv_engine_type or row.engine_type,
                row.envkv_consumption_fuel or row.full_fuel_type,
                hint_texts=(
                    row.sub_title,
                    row.title,
                    row.model,
                    row.url,
                    row.description,
                ),
            )
            power_hp = self._resolve_power_hp(row)
            power_kw = self._resolve_power_kw(row)
            engine_cc = self._resolve_engine_cc(row)
            if engine_type == "electric":
                engine_cc = None
            yield CarParsed(
                source_key=self.config.key,
                external_id=str(row.inner_id),
                country=(row.seller_country or self.config.country or "DE").upper(),
                brand=(row.mark or None),
                model=self._resolve_model(row),
                variant=(row.sub_title or None),
                year=row.year,
                registration_year=registration_year,
                registration_month=registration_month,
                mileage=row.km_age,
                price=self._resolve_price_eur(row),
                currency="EUR",
                listing_date=row.created_at,
                engine_cc=engine_cc,
                power_hp=power_hp,
                power_kw=power_kw,
                body_type=self._normalize_body(row.body_type),
                engine_type=engine_type,
                transmission=self._normalize_transmission(row.transmission),
                drive_type=drive,
                color=row.manufacturer_color or row.color,
                description=row.description,
                vin=None,
                source_url=row.url,
                thumbnail_url=thumb,
                images=images,
                source_payload=payload,
            )
