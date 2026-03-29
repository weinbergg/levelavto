from __future__ import annotations

import csv
import hashlib
import re
from pathlib import Path
from typing import Dict, Optional, Set, Tuple, List, Any

from .localization import display_body, display_color

def _load_taxonomy() -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, Set[str]]]]:
    base = Path(__file__).resolve().parents[1] / "resources"
    csv_path = base / "taxonomy_ru.csv"
    mapping: Dict[str, Dict[str, str]] = {}
    aliases: Dict[str, Dict[str, Set[str]]] = {}
    if not csv_path.exists():
        raise FileNotFoundError(
            f"taxonomy file not found: {csv_path}. Place taxonomy_ru.csv in backend/app/resources/")
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            typ = (row.get("type") or "").strip()
            key = (row.get("key") or "").strip()
            ru = (row.get("ru") or "").strip()
            if not typ or not key or not ru:
                continue
            key_l = key.lower()
            mapping.setdefault(typ, {})[key_l] = ru
            mapping.setdefault(typ, {})[ru.lower()] = ru
            alias_bucket = aliases.setdefault(typ, {})
            alias_bucket.setdefault(key_l, set()).update({key_l, ru.lower()})
            raw_values = (row.get("raw_values") or "").lower()
            if raw_values:
                for v in raw_values.split(";"):
                    vv = v.strip()
                    if vv:
                        mapping.setdefault(typ, {})[vv] = ru
                        alias_bucket.setdefault(key_l, set()).add(vv)
    return mapping, aliases


_TAX, _ALIASES = _load_taxonomy()


def ru_label(category: str, value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return _TAX.get(category, {}).get(value.strip().lower())


def ru_body(body: Optional[str]) -> Optional[str]:
    return ru_label("body_type", body)


def normalize_body_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    variants = {
        raw,
        re.sub(r"\s+", " ", raw.replace("-", " ").replace("_", " ")).strip(),
    }
    aliases = _ALIASES.get("body_type", {})
    for canonical, items in aliases.items():
        normalized_items = {
            item.strip().lower()
            for item in items
            if item and str(item).strip()
        }
        normalized_items.add(canonical.strip().lower())
        normalized_items.add(canonical.replace("_", " ").strip().lower())
        if variants & normalized_items:
            return canonical
    label = ru_body(raw) or display_body(raw)
    if not label:
        return None
    for canonical, mapped in _TAX.get("body_type", {}).items():
        if canonical in aliases and mapped == label:
            return canonical
    return None


def body_aliases(value: Optional[str]) -> List[str]:
    canonical = normalize_body_type(value)
    if not canonical:
        return []
    aliases = set(_aliases_for("body_type", canonical))
    aliases.add(canonical)
    aliases.add(canonical.replace("_", " "))
    return sorted(alias for alias in aliases if alias)


def build_body_type_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        raw = str((row or {}).get("value") or "").strip()
        if not raw:
            continue
        canonical = normalize_body_type(raw)
        if not canonical:
            continue
        entry = buckets.get(canonical)
        if entry is None:
            entry = {
                "value": canonical,
                "label": ru_body(canonical) or display_body(raw) or raw,
                "count": 0,
            }
            buckets[canonical] = entry
        entry["count"] += int((row or {}).get("count") or 0)
    return list(buckets.values())


def ru_color(color: Optional[str]) -> Optional[str]:
    return ru_label("color", color)


def ru_fuel(fuel: Optional[str]) -> Optional[str]:
    return ru_label("fuel", fuel)


def ru_transmission(val: Optional[str]) -> Optional[str]:
    return ru_label("transmission", val)


def ru_drivetrain(val: Optional[str]) -> Optional[str]:
    return ru_label("drivetrain", val)


def _aliases_for(category: str, value: Optional[str]) -> List[str]:
    if not value:
        return []
    key = value.strip().lower()
    aliases = _ALIASES.get(category, {}).get(key)
    if not aliases:
        return [key]
    return sorted(aliases)


def color_aliases(value: Optional[str]) -> List[str]:
    base = (value or "").strip().lower()
    aliases = set(_aliases_for("color", value))
    keywords = _COLOR_KEYWORDS.get(base)
    if keywords:
        aliases.update(keywords)
    return sorted(aliases)


def fuel_aliases(value: Optional[str]) -> List[str]:
    return _aliases_for("fuel", value)


_COLOR_HEX = {
    "black": "#0d0f14",
    "white": "#f5f6fa",
    "gray": "#6f7683",
    "silver": "#c0c0c0",
    "red": "#d13b3b",
    "blue": "#2f6bd1",
    "green": "#3b8d4c",
    "yellow": "#f0c63c",
    "orange": "#f28f3b",
    "brown": "#7a5234",
    "beige": "#d9c6a5",
    "purple": "#7a4bd8",
    "gold": "#d4af37",
    "pink": "#f472b6",
}

_COLOR_FALLBACK = [
    "#f59e0b",
    "#22c55e",
    "#3b82f6",
    "#ef4444",
    "#a855f7",
    "#14b8a6",
    "#eab308",
    "#f97316",
]

_COLOR_KEYWORDS = {
    "white": ["white", "ivory", "cream", "snow", "pearl", "weiss", "weiß", "bianco", "blanc", "бел", "слон"],
    "black": ["black", "obsidian", "onyx", "noir", "nero", "schwarz", "черн"],
    "gray": ["gray", "grey", "graphite", "anthracite", "slate", "titan", "grau", "сер", "графит"],
    "silver": ["silver", "aluminium", "aluminum", "silber", "steel", "платин", "серебр"],
    "blue": ["blue", "navy", "azure", "sky", "cyan", "blau", "син", "голуб"],
    "red": ["red", "maroon", "burgundy", "ruby", "rot", "красн", "бордов"],
    "green": ["green", "olive", "emerald", "gruen", "grün", "зел"],
    "orange": ["orange", "copper", "bronze", "оранж", "медн"],
    "yellow": ["yellow", "gold", "golden", "gelb", "желт", "зол"],
    "brown": ["brown", "coffee", "chocolate", "cocoa", "braun", "коричн", "шокол", "кофе"],
    "beige": ["beige", "sand", "champagne", "беж", "песочн", "шамп"],
    "purple": ["purple", "violet", "lilac", "lila", "фиол", "пурпур"],
    "pink": ["pink", "rose", "rosé", "роз"],
}

COLOR_BASES = set(_COLOR_KEYWORDS.keys())

_COLOR_MODIFIERS = {
    "metallic",
    "met",
    "pearl",
    "pearlescent",
    "gloss",
    "matte",
    "matt",
    "mat",
    "diamond",
    "dust",
    "magnetic",
    "crystal",
    "clear",
    "clearcoat",
    "effect",
    "special",
    "uni",
    "solid",
    "coat",
}


def color_hex(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = value.strip().lower()
    if key in _COLOR_HEX:
        return _COLOR_HEX[key]
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(_COLOR_FALLBACK)
    return _COLOR_FALLBACK[idx]


def _normalize_alias(category: str, val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    key = val.strip().lower()
    for canonical, items in _ALIASES.get(category, {}).items():
        for raw in items:
            if raw and raw in key:
                return canonical
    return key


def normalize_color(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    raw = val.strip().lower()
    cleaned = re.sub(r"[_\-.]+", " ", raw)
    cleaned = re.sub(r"[^\w\s]+", " ", cleaned, flags=re.UNICODE)
    tokens = [t for t in cleaned.split() if t and t not in _COLOR_MODIFIERS]
    cleaned = " ".join(tokens) or raw
    for base, keywords in _COLOR_KEYWORDS.items():
        if any(k in cleaned for k in keywords):
            return base
    if "metal" in cleaned or "metallic" in raw:
        return "silver"
    return _normalize_alias("color", cleaned)


def is_color_base(val: Optional[str]) -> bool:
    if not val:
        return False
    return val.strip().lower() in COLOR_BASES


_INTERIOR_COLOR_LABELS: Dict[str, str] = {
    "anthracite": "Антрацит",
    "charcoal": "Графит",
    "cognac": "Коньячный",
    "camel": "Песочно-коричневый",
    "taupe": "Серо-бежевый",
    "tan": "Песочный",
    "ivory": "Слоновая кость",
    "cream": "Кремовый",
    "burgundy": "Бордовый",
    "black": "Чёрный",
    "white": "Белый",
    "gray": "Серый",
    "silver": "Серебристый",
    "blue": "Синий",
    "red": "Красный",
    "green": "Зелёный",
    "yellow": "Жёлтый",
    "orange": "Оранжевый",
    "beige": "Бежевый",
    "brown": "Коричневый",
    "purple": "Фиолетовый",
    "pink": "Розовый",
}

_INTERIOR_COLOR_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "anthracite": ("anthracite", "anthrazit", "антрацит"),
    "charcoal": ("charcoal", "graphite", "graphit", "графит"),
    "cognac": ("cognac", "коньяк", "коньячный"),
    "camel": ("camel", "camelbraun", "camel brown", "песочно-коричнев", "кемел"),
    "taupe": ("taupe", "серо-беж", "грейдж"),
    "tan": ("tan", "песочный"),
    "ivory": ("ivory", "elfenbein", "слоновая кость"),
    "cream": ("cream", "кремов"),
    "burgundy": ("burgundy", "bordeaux", "бордов"),
    "black": ("black", "schwarz", "nero", "noir", "черн"),
    "white": ("white", "weiss", "weiß", "blanc", "bianco", "бел"),
    "gray": ("gray", "grey", "grau", "сер", "графитов"),
    "silver": ("silver", "silber", "серебр"),
    "blue": ("blue", "blau", "azure", "azur", "син", "голуб"),
    "red": ("red", "rot", "rosso", "rouge", "красн"),
    "green": ("green", "grün", "gruen", "verde", "зел"),
    "yellow": ("yellow", "gelb", "желт"),
    "orange": ("orange", "оранж"),
    "beige": ("beige", "sand", "champagne", "беж", "песочн", "шамп"),
    "brown": ("brown", "braun", "marron", "коричн", "шокол", "кофе"),
    "purple": ("purple", "violet", "lila", "фиолет", "пурпур"),
    "pink": ("pink", "rose", "rosa", "роз"),
}

_INTERIOR_MATERIAL_LABELS: Dict[str, str] = {
    "leather": "Кожа",
    "partial_leather": "Частичная кожа",
    "eco_leather": "Экокожа",
    "alcantara": "Алькантара",
    "fabric": "Ткань",
    "velour": "Велюр",
    "suede": "Замша",
    "microfiber": "Микрофибра",
    "vinyl": "Винил",
    "other": "Другое",
}

_INTERIOR_MATERIAL_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "partial_leather": ("partial leather", "part leather", "teilleder", "частичная кожа"),
    "eco_leather": ("leatherette", "synthetic leather", "kunstleder", "экокожа", "искусственная кожа"),
    "alcantara": ("alcantara", "алькантара"),
    "microfiber": ("mikrofaser", "mikrofibre", "microfibre", "microfiber", "микрофибра"),
    "velour": ("velour", "velours", "велюр"),
    "suede": ("suede", "замша"),
    "vinyl": ("vinyl", "винил"),
    "fabric": ("cloth", "fabric", "stoff", "textile", "tissu", "ткан"),
    "leather": ("full leather", "vollleder", "leder", "leather", "nappa", "napa", "кожа"),
    "other": ("other", "not specified", "другое", "не указано"),
}

_INTERIOR_COLOR_ORDER = tuple(_INTERIOR_COLOR_LABELS.keys())
_INTERIOR_MATERIAL_ORDER = tuple(_INTERIOR_MATERIAL_LABELS.keys())


def _normalize_interior_text(raw: Optional[str]) -> str:
    text = (raw or "").strip().lower()
    if not text:
        return ""
    text = text.replace("ß", "ss")
    text = re.sub(r"[_\-/.,;|()+]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _match_keywords(text: str, variants: Dict[str, Tuple[str, ...]], order: Tuple[str, ...]) -> Optional[str]:
    if not text:
        return None
    for key in order:
        keywords = variants.get(key) or ()
        if any(keyword in text for keyword in keywords):
            return key
    return None


def interior_color_key(value: Optional[str]) -> Optional[str]:
    raw = _normalize_interior_text(value)
    if not raw:
        return None
    if raw in _INTERIOR_COLOR_LABELS:
        return raw
    translated = translate_payload_value("color", raw)
    if translated:
        translated_key = _normalize_interior_text(translated)
        for key, label in _INTERIOR_COLOR_LABELS.items():
            if translated_key == _normalize_interior_text(label):
                return key
    matched = _match_keywords(raw, _INTERIOR_COLOR_KEYWORDS, _INTERIOR_COLOR_ORDER)
    if matched:
        return matched
    normalized = normalize_color(raw)
    if normalized in _INTERIOR_COLOR_LABELS:
        return normalized
    return None


def interior_material_key(value: Optional[str]) -> Optional[str]:
    raw = _normalize_interior_text(value)
    if not raw:
        return None
    if raw in _INTERIOR_MATERIAL_LABELS:
        return raw
    return _match_keywords(raw, _INTERIOR_MATERIAL_KEYWORDS, _INTERIOR_MATERIAL_ORDER)


def interior_color_label(value: Optional[str]) -> Optional[str]:
    key = interior_color_key(value)
    if not key:
        return None
    return _INTERIOR_COLOR_LABELS.get(key)


def interior_material_label(value: Optional[str]) -> Optional[str]:
    key = interior_material_key(value)
    if not key:
        return None
    return _INTERIOR_MATERIAL_LABELS.get(key)


def interior_color_aliases(value: Optional[str]) -> List[str]:
    key = interior_color_key(value)
    if not key:
        raw = _normalize_interior_text(value)
        return [raw] if raw else []
    aliases = set(_INTERIOR_COLOR_KEYWORDS.get(key) or ())
    aliases.add(key)
    label = _INTERIOR_COLOR_LABELS.get(key)
    if label:
        aliases.add(label.lower())
    return sorted(alias for alias in aliases if alias)


def interior_material_aliases(value: Optional[str]) -> List[str]:
    key = interior_material_key(value)
    if not key:
        raw = _normalize_interior_text(value)
        return [raw] if raw else []
    aliases = set(_INTERIOR_MATERIAL_KEYWORDS.get(key) or ())
    aliases.add(key)
    label = _INTERIOR_MATERIAL_LABELS.get(key)
    if label:
        aliases.add(label.lower())
    return sorted(alias for alias in aliases if alias)


def build_interior_options(values: List[Any], kind: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()
    if kind == "color":
        key_fn = interior_color_key
        label_fn = interior_color_label
        order = _INTERIOR_COLOR_ORDER
    else:
        key_fn = interior_material_key
        label_fn = interior_material_label
        order = _INTERIOR_MATERIAL_ORDER
    for value in values or []:
        key = key_fn(value)
        if not key or key in seen:
            continue
        seen.add(key)
        items.append({"value": key, "label": label_fn(key) or key})
    order_index = {key: idx for idx, key in enumerate(order)}
    items.sort(key=lambda item: (order_index.get(item["value"], 999), item["label"].casefold()))
    return items


def normalize_fuel(val: Optional[str]) -> Optional[str]:
    return _normalize_alias("fuel", val)


_FREE_TEXT_EXACT = {
    "a/c (man.)": "кондиционер",
    "air conditioner": "кондиционер",
    "automatic air conditioning": "автоматический климат-контроль",
    "automatic climate control": "климат-контроль",
    "automatic climate control, 2 zones": "климат-контроль, 2 зоны",
    "automatic climate control, 3 zones": "климат-контроль, 3 зоны",
    "automatic climate control, 4 zones": "климат-контроль, 4 зоны",
    "automatic climatisation": "климат-контроль",
    "automatic climatisation, 2 zones": "климат-контроль, 2 зоны",
    "automatic climatisation, 3 zones": "климат-контроль, 3 зоны",
    "automatic climatisation, 4 zones": "климат-контроль, 4 зоны",
    "airbags": "подушки безопасности",
    "driver airbag": "подушка безопасности водителя",
    "passenger airbag": "подушка безопасности пассажира",
    "front airbags": "фронтальные подушки безопасности",
    "front and side airbags": "фронтальные и боковые подушки",
    "front, side and more airbags": "фронтальные, боковые и дополнительные подушки",
    "front and side and more airbags": "фронтальные, боковые и дополнительные подушки",
    "parking sensors front and rear": "парктроники спереди и сзади",
    "front and rear parking sensors": "парктроники спереди и сзади",
    "parking assists": "ассистенты парковки",
    "park assist": "ассистент парковки",
    "front, rear": "спереди и сзади",
    "360° camera": "камера 360°",
    "rear, front, 360° camera": "камеры спереди, сзади и 360°",
    "front, rear, 360° camera": "камеры спереди, сзади и 360°",
    "rear view camera": "камера заднего вида",
    "backup camera": "камера заднего вида",
    "reverse camera": "камера заднего вида",
    "full leather": "кожа",
    "part leather": "частичная кожа",
    "partial leather": "частичная кожа",
    "leather": "кожа",
    "leatherette": "экокожа",
    "synthetic leather": "экокожа",
    "cloth": "ткань",
    "fabric": "ткань",
    "velour": "велюр",
    "velours": "велюр",
    "alcantara": "алькантара",
    "nappa": "наппа",
    "suede": "замша",
    "vinyl": "винил",
    "other": "другое",
    "not specified": "не указано",
    "leder": "кожа",
    "vollleder": "кожа",
    "teilleder": "частичная кожа",
    "stoff": "ткань",
    "kunstleder": "экокожа",
    "mikrofaser": "микрофибра",
    "mikrofibre": "микрофибра",
    "microfibre": "микрофибра",
    "microfiber": "микрофибра",
    "anthracite": "антрацит",
    "charcoal": "графит",
    "cognac": "коньячный",
    "cream": "кремовый",
    "ivory": "слоновая кость",
    "taupe": "серо-бежевый",
    "tan": "песочный",
    "camel": "песочно-коричневый",
    "burgundy": "бордовый",
    "no_rating": "нет оценки",
    "very_good_price": "отличная цена",
    "good_price": "хорошая цена",
    "average_price": "средняя цена",
    "high_price": "высокая цена",
    "reasonable_price": "адекватная цена",
    "increased_price": "завышенная цена",
    "dealer": "дилер",
    "private": "частное лицо",
    "automatic": "автомат",
    "manual": "механика",
    "awd": "полный привод",
    "4x4": "полный привод",
    "fwd": "передний привод",
    "rwd": "задний привод",
    "abs": "ABS",
    "alarm system": "сигнализация",
    "alloy wheels": "легкосплавные диски",
    "apple carplay": "Apple CarPlay",
    "android auto": "Android Auto",
    "air suspension": "пневмоподвеска",
    "navigation system": "навигация",
    "heated seats": "подогрев сидений",
    "heated steering wheel": "подогрев руля",
    "led headlights": "LED-фары",
    "cruise control": "круиз-контроль",
    "adaptive cruise control": "адаптивный круиз-контроль",
    "lane change assist": "ассистент смены полосы",
    "blind spot assist": "контроль слепых зон",
    "panoramic roof": "панорамная крыша",
    "sunroof": "люк",
    "keyless central locking": "бесключевой доступ",
    "isofix": "ISOFIX",
    "dab radio": "DAB-радио",
    "bluetooth": "Bluetooth",
    "head-up display": "проекционный дисплей",
    "hill-start assist": "помощь при старте в гору",
    "start-stop system": "система старт-стоп",
    "trailer coupling": "фаркоп",
    "tinted windows": "тонировка",
    "warranty": "гарантия",
    "full service history": "полная сервисная история",
    "non-smoker vehicle": "в салоне не курили",
    "rain sensor": "датчик дождя",
    "light sensor": "датчик света",
    "tyre pressure monitoring": "контроль давления в шинах",
    "usb port": "USB",
    "touchscreen": "сенсорный экран",
    "black": "чёрный",
    "blue": "синий",
    "brown": "коричневый",
    "beige": "бежевый",
    "white": "белый",
    "grey": "серый",
    "gray": "серый",
    "silver": "серебристый",
    "red": "красный",
    "green": "зелёный",
    "yellow": "жёлтый",
    "orange": "оранжевый",
    "other, e10-enabled": "бензин (E10)",
    "ethanol (ffv, e85, etc.)": "этанол (E85/FFV)",
}

_FREE_TEXT_REPLACEMENTS = (
    ("climatisation", "климат-контроль"),
    ("climatization", "климат-контроль"),
    ("climate control", "климат-контроль"),
    ("airbags", "подушки безопасности"),
    ("navigation", "навигация"),
    ("sport package", "спорт-пакет"),
    ("park assist", "ассистент парковки"),
    ("360° camera", "камера 360°"),
    ("rear view camera", "камера заднего вида"),
    ("backup camera", "камера заднего вида"),
    ("reverse camera", "камера заднего вида"),
    ("parking sensors", "парктроники"),
    ("multifunction steering wheel", "мульти-руль"),
    ("partial leather", "частичная кожа"),
    ("full leather", "кожа"),
    ("leatherette", "экокожа"),
    ("synthetic leather", "экокожа"),
    ("leder", "кожа"),
    ("vollleder", "кожа"),
    ("teilleder", "частичная кожа"),
    ("stoff", "ткань"),
    ("kunstleder", "экокожа"),
    ("mikrofaser", "микрофибра"),
    ("mikrofibre", "микрофибра"),
    ("microfibre", "микрофибра"),
    ("microfiber", "микрофибра"),
    ("velours", "велюр"),
    ("anthracite", "антрацит"),
    ("charcoal", "графит"),
    ("cognac", "коньячный"),
    ("cream", "кремовый"),
    ("ivory", "слоновая кость"),
    ("taupe", "серо-бежевый"),
    ("tan", "песочный"),
    ("camel", "песочно-коричневый"),
    ("burgundy", "бордовый"),
)


def translate_payload_value(field: Optional[str], value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    raw = value.strip()
    if not raw:
        return None
    low = raw.lower()
    field_key = (field or "").strip().lower()

    if field_key in {"engine_type", "fuel"}:
        return _FREE_TEXT_EXACT.get(low) or ru_fuel(raw) or ru_fuel(normalize_fuel(raw)) or raw
    if field_key == "transmission":
        return ru_transmission(raw) or _FREE_TEXT_EXACT.get(low) or raw
    if field_key in {"drive_type", "drivetrain"}:
        return ru_drivetrain(raw) or _FREE_TEXT_EXACT.get(low) or raw
    if field_key == "body_type":
        return ru_body(raw) or display_body(raw) or raw
    if field_key in {"color", "manufacturer_color"}:
        normalized_color = normalize_color(raw)
        return (
            ru_color(raw)
            or display_color(raw)
            or (ru_color(normalized_color) if normalized_color else None)
            or (display_color(normalized_color) if normalized_color else None)
            or raw
        )

    if low in _FREE_TEXT_EXACT:
        return _FREE_TEXT_EXACT[low]

    if re.search(r"[,/;|]", raw):
        tokens = re.split(r"(\s*[,/;|]\s*)", raw)
        translated_tokens: List[str] = []
        changed = False
        for token in tokens:
            if not token:
                continue
            if re.fullmatch(r"\s*[,/;|]\s*", token):
                translated_tokens.append(token)
                continue
            stripped = token.strip()
            translated = translate_payload_value(field, stripped) or stripped
            if translated != stripped:
                changed = True
            translated_tokens.append(translated)
        if changed:
            return "".join(translated_tokens)

    mapped = (
        ru_fuel(raw)
        or ru_fuel(normalize_fuel(raw))
        or ru_transmission(raw)
        or ru_drivetrain(raw)
        or ru_body(raw)
        or ru_color(raw)
        or display_color(raw)
    )
    if mapped:
        return mapped

    normalized_color = normalize_color(raw)
    if normalized_color:
        color_label = ru_color(normalized_color) or display_color(normalized_color)
        if color_label:
            return color_label

    translated = raw
    changed = False
    for src, dst in _FREE_TEXT_REPLACEMENTS:
        if src in translated.lower():
            translated = re.sub(re.escape(src), dst, translated, flags=re.IGNORECASE)
            changed = True
    return translated if changed else raw


def build_labeled_options(values: List[Any], field: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for value in values or []:
        raw = str(value or "").strip()
        if not raw:
            continue
        key = raw.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "value": raw,
                "label": translate_payload_value(field, raw) or raw,
            }
        )
    return out
