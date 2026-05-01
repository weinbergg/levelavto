"""Human-friendly TXT template for the «Рекомендуемые на главной» list.

The format is intentionally simple — one car ID per line — so the operator's
bookkeeper can edit it in any text editor or Excel without dealing with
JSON/YAML. We emit annotated comments next to every ID (year + brand + model
+ country + mileage) so the operator can re-read the file three months
later and know what each ID actually refers to.

Lines starting with ``#`` and any inline ``# …`` tail are stripped during
parsing — this is what lets us put car titles right next to the IDs.
"""

from __future__ import annotations

from typing import List, Sequence


def _row_comment(car) -> str:
    parts: List[str] = []
    year = getattr(car, "year", None)
    if year:
        parts.append(str(year))
    brand = getattr(car, "brand", None)
    model = getattr(car, "model", None)
    title = " ".join(p for p in (brand, model) if p)
    if title:
        parts.append(title)
    country = getattr(car, "country", None)
    if country:
        parts.append(str(country))
    mileage = getattr(car, "mileage", None)
    if mileage:
        parts.append(f"{int(mileage):,} км".replace(",", " "))
    return " — ".join(parts)


def build_featured_template(featured_items: Sequence) -> str:
    """Return TXT contents listing every pinned car with a Russian header.

    ``featured_items`` is a sequence of :class:`FeaturedCar` ORM objects
    (each with a ``car`` relationship loaded). The output is suitable for
    re-uploading via ``/admin/featured/upload`` — :func:`parse_featured_template`
    safely strips comments and annotations.
    """

    lines: List[str] = [
        "# ════════════════════════════════════════════════════════════════════",
        "# Список «Рекомендуемые автомобили» на главной странице",
        "#",
        "# Что заполнять:",
        "#   • Один ID машины на строку. ID видно в админке и в URL карточки",
        "#     (https://levelavto.ru/car/<ID>).",
        "#   • Можно ставить заметки — всё, что после # на строке, игнорируется.",
        "#   • Пустые строки и строки, начинающиеся с #, не считаются.",
        "#   • Порядок строк = порядок карточек на главной (первая — слева).",
        "#",
        "# При загрузке файла список ПОЛНОСТЬЮ заменит текущий. Если хотите",
        "# временно отключить кого-то — закомментируйте строку, добавив # в начало.",
        "#",
        "# На главную помещается до 20 машин. Лишние строки игнорируются.",
        "# ════════════════════════════════════════════════════════════════════",
        "",
    ]

    if not featured_items:
        lines.append("# Список пуст. Добавьте ID машин ниже:")
        lines.append("# 12345   # 2024 BMW X5 — Германия — 12 000 км")
        lines.append("")
        return "\n".join(lines)

    lines.append("# текущий список (для удобства — комментарии справа от ID):")
    lines.append("")
    for item in featured_items:
        car = getattr(item, "car", None)
        car_id = getattr(item, "car_id", None) or (getattr(car, "id", None) if car else None)
        if not car_id:
            continue
        comment = _row_comment(car) if car else ""
        if comment:
            lines.append(f"{car_id}   # {comment}")
        else:
            lines.append(str(car_id))
    lines.append("")
    return "\n".join(lines)


def parse_featured_template(raw: str) -> List[int]:
    """Parse the TXT template back into a list of car IDs.

    Strips ``#`` comments (full-line and inline), tolerates commas, semicolons,
    tabs and whitespace as separators. Preserves order, drops duplicates.
    """

    out: List[int] = []
    seen: set = set()
    for line in (raw or "").splitlines():
        hash_idx = line.find("#")
        if hash_idx != -1:
            line = line[:hash_idx]
        for token in line.replace(",", " ").replace(";", " ").split():
            token = token.strip()
            if not token.isdigit():
                continue
            cid = int(token)
            if cid in seen:
                continue
            seen.add(cid)
            out.append(cid)
    return out
