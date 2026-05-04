"""Diagnose unparseable `first_registration` values in the mobile.de CSV.

Walks the daily mobile.de CSV feed and runs the production
`MobileDeFeedParser._parse_first_registration` against each row's
`first_registration` field. Reports:

  * how many rows have a non-empty `first_registration`
  * how many of those the parser FAILS to convert into a year
  * the top-N raw values that are responsible for those failures
  * a few example inner_id's per failing pattern so you can eyeball
    them in the source.

Use after every parser change to confirm the unparseable bucket shrunk.

Usage::

    docker compose exec -T web python -m backend.app.scripts.diagnose_first_registration \\
        --file /app/tmp/mobilede_active_offers_2026-05-03.csv \\
        --top 30
"""

from __future__ import annotations

import argparse
from collections import Counter
from typing import Optional

from ..importing.mobilede_csv import iter_mobilede_csv_rows
from ..parsing.config import load_sites_config
from ..parsing.mobile_de_feed import MobileDeFeedParser


def _make_parser() -> MobileDeFeedParser:
    cfg = load_sites_config().get("mobile_de")
    if not cfg:
        raise RuntimeError("mobile_de parser config not found in sites config")
    return MobileDeFeedParser(cfg)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to mobilede_active_offers_*.csv")
    ap.add_argument("--top", type=int, default=20,
                    help="How many distinct unparseable raw values to print")
    ap.add_argument("--examples", type=int, default=3,
                    help="How many sample inner_id's to print per unparseable value")
    args = ap.parse_args()

    parser = _make_parser()

    total = 0
    has_value = 0
    parsed_ok = 0
    failed: Counter[str] = Counter()
    examples: dict[str, list[str]] = {}

    for row in iter_mobilede_csv_rows(args.file):
        total += 1
        raw = row.first_registration
        if not raw or not str(raw).strip():
            continue
        has_value += 1
        year, _ = parser._parse_first_registration(raw)
        if year is not None:
            parsed_ok += 1
            continue
        # Failure — record the raw value (trimmed to keep the table tidy).
        key = str(raw).strip()[:80]
        failed[key] += 1
        if examples.setdefault(key, []) and len(examples[key]) >= args.examples:
            continue
        examples[key].append(str(row.inner_id))

    print(f"\nTotal CSV rows:                       {total}")
    print(f"With non-empty first_registration:    {has_value}")
    print(f"Parsed into a year:                   {parsed_ok}")
    print(f"FAILED to parse:                      {has_value - parsed_ok} "
          f"({(has_value - parsed_ok) * 100.0 / max(has_value, 1):.2f}% of non-empty)")

    if not failed:
        print("\nNo failing values — parser handles the entire feed.")
        return

    print(f"\nTop-{args.top} unparseable raw values:")
    print(f"  {'count':>8}  {'raw':<60}  examples")
    for raw, count in failed.most_common(args.top):
        sample_ids = ", ".join(examples.get(raw, [])[:args.examples])
        print(f"  {count:>8}  {raw!r:<60}  {sample_ids}")


if __name__ == "__main__":
    main()
