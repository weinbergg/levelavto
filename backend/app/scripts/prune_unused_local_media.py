from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlalchemy import func, select

from backend.app.db import SessionLocal
from backend.app.models import Car, CarImage


def media_root() -> Path:
    return Path(__file__).resolve().parents[3] / "фото-видео"


def to_rel(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value.startswith("/media/"):
        return None
    rel = value.removeprefix("/media/").strip("/")
    return rel or None


def iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*") if p.is_file()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Delete local media files not referenced by active cars")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report-json", default="/app/artifacts/prune_unused_local_media.json")
    args = ap.parse_args()

    base_dir = media_root()
    gallery_dir = base_dir / "машины" / "gallery_mirror"
    thumbs_dir = base_dir / "машины" / "mirror"

    keep: set[str] = set()
    with SessionLocal() as db:
        image_rows = db.execute(
            select(CarImage.url)
            .join(Car, Car.id == CarImage.car_id)
            .where(Car.is_available.is_(True), CarImage.url.like("/media/%"))
        ).scalars().all()
        keep.update(filter(None, (to_rel(v) for v in image_rows)))

        thumb_rows = db.execute(
            select(Car.thumbnail_local_path)
            .where(
                Car.is_available.is_(True),
                Car.thumbnail_local_path.is_not(None),
                Car.thumbnail_local_path != "",
                Car.thumbnail_local_path.like("/media/%"),
            )
        ).scalars().all()
        keep.update(filter(None, (to_rel(v) for v in thumb_rows)))

    deleted = 0
    kept = 0
    reclaimed_bytes = 0
    scanned = 0

    for root in (gallery_dir, thumbs_dir):
        for path in iter_files(root):
            scanned += 1
            rel = path.relative_to(base_dir).as_posix()
            if rel in keep:
                kept += 1
                continue
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            reclaimed_bytes += size
            deleted += 1
            if not args.dry_run:
                try:
                    path.unlink()
                except OSError:
                    pass

    if not args.dry_run:
        for root in (gallery_dir, thumbs_dir):
            if not root.exists():
                continue
            for path in sorted((p for p in root.rglob("*") if p.is_dir()), reverse=True):
                try:
                    path.rmdir()
                except OSError:
                    pass

    report = {
        "base_dir": str(base_dir),
        "scanned": scanned,
        "kept": kept,
        "deleted": deleted,
        "reclaimed_bytes": reclaimed_bytes,
        "dry_run": bool(args.dry_run),
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[prune_unused_local_media] "
        f"scanned={scanned} kept={kept} deleted={deleted} "
        f"reclaimed_bytes={reclaimed_bytes} dry_run={int(args.dry_run)} "
        f"json={report_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
