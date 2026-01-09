from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple


def try_get_image_size(path: Path) -> Tuple[int | None, int | None]:
    try:
        from PIL import Image  # type: ignore

        with Image.open(path) as img:
            return img.width, img.height
    except Exception:
        return None, None


def human_mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 2)


def main() -> int:
    root = Path(__file__).resolve().parents[3] / "фото-видео" / "машины"
    if not root.exists():
        print(f"[error] folder not found: {root}")
        return 1

    files: List[Path] = [p for p in root.glob("**/*") if p.is_file()]
    if not files:
        print("[warn] no files in folder")
        return 0

    files_sorted = sorted(files, key=lambda p: p.stat().st_size, reverse=True)
    top = files_sorted[:50]

    print(f"Total files: {len(files)} in {root}")
    print("Top 50 by size (MB, WxH):")
    for p in top:
        size_mb = human_mb(p.stat().st_size)
        w, h = try_get_image_size(p)
        wh = f"{w}x{h}" if w and h else "unknown"
        print(f"{size_mb:7.2f} MB  {wh:>12}  {p.relative_to(root)}")

    first_n = 36
    subset = files_sorted[:first_n]
    total_subset_mb = human_mb(sum(p.stat().st_size for p in subset))
    avg_subset_mb = total_subset_mb / len(subset) if subset else 0
    print(f"\nFirst {first_n}: total {total_subset_mb:.2f} MB, avg {avg_subset_mb:.2f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
