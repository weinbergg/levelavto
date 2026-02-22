from __future__ import annotations

import argparse
from pathlib import Path

from backend.app.db import SessionLocal
from backend.app.models import Car


def _media_root() -> Path:
    return Path(__file__).resolve().parents[3] / "фото-видео"


def _resolve_local_file(local_path: str) -> Path | None:
    raw = (local_path or "").strip()
    if not raw.startswith("/media/"):
        return None
    rel = raw.removeprefix("/media/").strip("/")
    if not rel:
        return None
    return _media_root() / rel


def main() -> None:
    ap = argparse.ArgumentParser(description="Clear broken thumbnail_local_path values")
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--limit", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    checked = 0
    missing = 0
    cleared = 0
    last_id = 0
    limit = args.limit if args.limit and args.limit > 0 else None

    with SessionLocal() as db:
        while True:
            q = (
                db.query(Car.id, Car.thumbnail_local_path)
                .filter(
                    Car.id > last_id,
                    Car.thumbnail_local_path.is_not(None),
                    Car.thumbnail_local_path != "",
                )
                .order_by(Car.id.asc())
                .limit(args.batch)
            )
            rows = q.all()
            if not rows:
                break
            last_id = rows[-1][0]
            for car_id, local_path in rows:
                if limit is not None and checked >= limit:
                    break
                checked += 1
                path = _resolve_local_file(str(local_path or ""))
                if path is None or path.exists():
                    continue
                missing += 1
                if not args.dry_run:
                    db.query(Car).filter(Car.id == car_id).update(
                        {"thumbnail_local_path": None},
                        synchronize_session=False,
                    )
                    cleared += 1
            if not args.dry_run:
                db.commit()
            if limit is not None and checked >= limit:
                break

    print(
        "[clear_missing_local_thumbs] "
        f"checked={checked} missing={missing} cleared={cleared} dry_run={int(args.dry_run)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
