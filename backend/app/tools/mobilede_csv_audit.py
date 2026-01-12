from __future__ import annotations

import argparse
from collections import Counter
from typing import Iterable

from ..importing.mobilede_csv import iter_mobilede_csv_rows, MobileDeCsvRow


def _count_missing_images(row: MobileDeCsvRow) -> bool:
    return not row.image_urls


def _parse_brand_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _normalize_brand(brand: str) -> str:
    return brand.strip().lower()


def audit(rows: Iterable[MobileDeCsvRow], brands_check: list[str]) -> None:
    total_rows = 0
    distinct_ids: set[str] = set()
    brand_counts: Counter[str] = Counter()
    country_counts: Counter[str] = Counter()

    missing_price = 0
    missing_brand = 0
    missing_model = 0
    missing_reg = 0
    missing_images = 0

    for row in rows:
        total_rows += 1
        if row.inner_id:
            distinct_ids.add(row.inner_id)

        if row.mark:
            brand_counts[row.mark.strip()] += 1
        else:
            missing_brand += 1

        if row.model:
            pass
        else:
            missing_model += 1

        if row.seller_country:
            country_counts[row.seller_country.strip()] += 1

        has_price = row.price_eur is not None or row.price_eur_nt is not None
        if not has_price:
            missing_price += 1

        has_reg = bool(row.first_registration) or row.year is not None
        if not has_reg:
            missing_reg += 1

        if _count_missing_images(row):
            missing_images += 1

    print(f"total_rows={total_rows}")
    print(f"distinct_external_id(inner_id)={len(distinct_ids)}")
    print(f"missing_price={missing_price}")
    print(f"missing_brand={missing_brand}")
    print(f"missing_model={missing_model}")
    print(f"missing_reg_date={missing_reg}")
    print(f"missing_images={missing_images}")

    print("\nTop-20 brands:")
    for brand, cnt in brand_counts.most_common(20):
        print(f"  {brand}: {cnt}")

    if country_counts:
        print("\nTop-20 seller_country:")
        for country, cnt in country_counts.most_common(20):
            print(f"  {country}: {cnt}")

    if brands_check:
        want = {_normalize_brand(b): b for b in brands_check}
        have = {_normalize_brand(b) for b in brand_counts.keys()}
        missing = [want[key] for key in want if key not in have]
        print("\nBrand check:")
        if missing:
            print("  missing_in_csv=" + ", ".join(missing))
        else:
            print("  all_present")


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit mobile.de CSV feed stats (no DB writes).")
    ap.add_argument(
        "--file",
        default="backend/app/imports/mobilede_active_offers.csv",
        help="Path to mobilede_active_offers.csv (delimiter '|')",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit of rows to scan",
    )
    ap.add_argument(
        "--brands",
        default=None,
        help="Comma-separated brand list to verify presence in CSV",
    )
    args = ap.parse_args()

    rows = iter_mobilede_csv_rows(args.file)
    if args.limit:
        from itertools import islice

        rows = islice(rows, args.limit)
    brands_check = _parse_brand_list(args.brands)
    audit(rows, brands_check)


if __name__ == "__main__":
    main()
