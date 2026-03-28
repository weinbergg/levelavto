from __future__ import annotations

import argparse

from backend.app.db import SessionLocal
from backend.app.services.car_spec_inference_service import CarSpecInferenceService


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=None)
    ap.add_argument("--country", default=None)
    ap.add_argument("--source-key", default=None)
    ap.add_argument("--since-minutes", type=int, default=None)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--chunk", type=int, default=50000)
    ap.add_argument("--year-window", type=int, default=2)
    ap.add_argument("--full-rebuild", action="store_true")
    ap.add_argument("--skip-reference", action="store_true")
    ap.add_argument("--skip-infer", action="store_true")
    args = ap.parse_args()

    with SessionLocal() as db:
        svc = CarSpecInferenceService(db)
        if not args.skip_reference:
            ref_stats = svc.refresh_reference(
                region=args.region,
                country=args.country,
                source_key=args.source_key,
                since_minutes=args.since_minutes,
                batch=args.batch,
                chunk=args.chunk,
                full_rebuild=args.full_rebuild,
            )
            print(
                "[refresh_spec_inference] reference "
                f"total={ref_stats['total']} processed={ref_stats['processed']} "
                f"upserted={ref_stats['upserted']} deleted={ref_stats['deleted']}",
                flush=True,
            )
        if not args.skip_infer:
            infer_stats = svc.infer_missing_specs(
                region=args.region,
                country=args.country,
                source_key=args.source_key,
                since_minutes=args.since_minutes,
                batch=args.batch,
                chunk=args.chunk,
                year_window=args.year_window,
            )
            print(
                "[refresh_spec_inference] infer "
                f"total={infer_stats['total']} processed={infer_stats['processed']} "
                f"matched={infer_stats['matched']} cleared={infer_stats['cleared']} "
                f"unmatched={infer_stats['unmatched']}",
                flush=True,
            )


if __name__ == "__main__":
    main()
