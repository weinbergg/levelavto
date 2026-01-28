from __future__ import annotations

import argparse
import json

from backend.app.db import SessionLocal
from backend.app.services.calc_debug import build_calc_debug


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--car-id", type=int, required=True)
    parser.add_argument("--eur", type=float, default=None)
    parser.add_argument("--usd", type=float, default=None)
    parser.add_argument("--scenario", type=str, default=None)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        payload = build_calc_debug(
            db,
            car_id=args.car_id,
            eur_rate=args.eur,
            usd_rate=args.usd,
            scenario=args.scenario,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()
