from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Tuple

SIZES: Tuple[int, int] = (320, 640)
QUALITY = 75


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def iter_images(src_root: Path) -> Iterable[Path]:
    exts = {".jpg", ".jpeg", ".png", ".webp", ".JPG", ".JPEG", ".PNG", ".WEBP"}
    for p in src_root.glob("**/*"):
        if p.is_file() and p.suffix in exts:
            yield p


def make_thumb(src: Path, dst: Path, width: int) -> None:
    from PIL import Image  # type: ignore

    with Image.open(src) as img:
        w, h = img.size
        ratio = width / float(w)
        new_size = (width, int(h * ratio))
        img = img.convert("RGB")
        img.thumbnail(new_size, Image.LANCZOS)
        img.save(
            dst,
            format="WEBP",
            quality=QUALITY,
            method=6,
            optimize=True,
        )


def main() -> int:
    root = Path(__file__).resolve().parents[3] / "фото-видео"
    src_root = root / "машины"
    dst_root = root / "машины_thumbs"
    if not src_root.exists():
        print(f"[error] source folder not found: {src_root}")
        return 1
    ensure_dir(dst_root)

    total = 0
    skipped = 0
    done = 0
    for src in iter_images(src_root):
        total += 1
        base = src.stem
        for width in SIZES:
            dst = dst_root / f"{base}__w{width}.webp"
            if dst.exists():
                skipped += 1
                continue
            try:
                make_thumb(src, dst, width)
                done += 1
                print(f"[ok] {dst}")
            except Exception as e:
                print(f"[err] {src} -> {e}")
    print(f"processed={total}, generated={done}, skipped_existing={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
