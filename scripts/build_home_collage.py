#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import re
from collections import deque
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageOps


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
SKIP_DIR_NAMES = {
    "для публикации",
    "для_публикации",
    "публикация",
    "publish",
}


def numeric_sort_key(path: Path) -> tuple:
    parts = re.split(r"(\d+)", path.stem.casefold())
    key: list[tuple[int, object]] = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    return tuple(key)


def thumb_base_and_width(path: Path) -> tuple[str, int] | None:
    match = re.match(r"(.+)__w(\d+)$", path.stem, re.IGNORECASE)
    if not match:
        return None
    return match.group(1), int(match.group(2))


def iter_image_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in IMAGE_EXTS:
            continue
        if any(part.startswith(".") for part in path.parts):
            continue
        yield path


def group_key_for(path: Path, root: Path) -> str:
    rel = path.relative_to(root)
    parts = list(rel.parts[:-1])
    if not parts:
        thumb_info = thumb_base_and_width(path)
        if thumb_info:
            return f"thumb:{thumb_info[0].casefold()}"
        return f"file:{path.stem.casefold()}"
    cleaned: list[str] = []
    for idx, part in enumerate(parts):
        cleaned.append(part)
        if idx >= 2:
            break
    return "/".join(cleaned)


def select_group_images(paths: list[Path], max_per_group: int) -> list[Path]:
    thumb_variants: list[tuple[int, Path]] = []
    for path in paths:
        info = thumb_base_and_width(path)
        if info is None:
            thumb_variants = []
            break
        thumb_variants.append((info[1], path))
    if thumb_variants:
        best = max(thumb_variants, key=lambda item: (item[0], numeric_sort_key(item[1])))
        return [best[1]]

    ordered = sorted(paths, key=numeric_sort_key)
    if len(ordered) <= max_per_group:
        return ordered
    selected: list[Path] = []
    last_idx = -1
    for slot in range(max_per_group):
        idx = round(slot * (len(ordered) - 1) / max(max_per_group - 1, 1))
        idx = max(idx, last_idx + 1)
        idx = min(idx, len(ordered) - 1)
        selected.append(ordered[idx])
        last_idx = idx
    deduped: list[Path] = []
    seen = set()
    for path in selected:
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    return deduped


def interleave_groups(
    grouped: dict[str, list[Path]],
    *,
    limit: int,
    min_gap: int,
    seed: int,
) -> list[tuple[str, Path]]:
    rng = random.Random(seed)
    group_names = list(grouped.keys())
    rng.shuffle(group_names)
    queues = {name: list(paths) for name, paths in grouped.items()}
    output: list[tuple[str, Path]] = []
    recent = deque(maxlen=max(0, min_gap))
    cursor = 0

    while len(output) < limit:
        available = [name for name in group_names if queues.get(name)]
        if not available:
            break
        allowed = [name for name in available if name not in recent]
        candidates = allowed or available
        picked = None
        for offset in range(len(group_names)):
            name = group_names[(cursor + offset) % len(group_names)]
            if name in candidates:
                picked = name
                cursor = (cursor + offset + 1) % len(group_names)
                break
        if picked is None:
            break
        path = queues[picked].pop(0)
        output.append((picked, path))
        recent.append(picked)
        if not queues[picked]:
            group_names = [name for name in group_names if name != picked]
            if group_names:
                cursor %= len(group_names)
            else:
                break

    return output


def convert_image(src: Path, dst: Path, *, max_width: int, quality: int) -> tuple[int, int]:
    with Image.open(src) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        elif img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.getchannel("A"))
            img = bg
        if img.width > max_width:
            ratio = max_width / float(img.width)
            img = img.resize((max_width, max(1, int(round(img.height * ratio)))), Image.Resampling.LANCZOS)
        dst.parent.mkdir(parents=True, exist_ok=True)
        img.save(dst, format="WEBP", quality=quality, method=6)
        return img.width, img.height


def main() -> None:
    parser = argparse.ArgumentParser(description="Build curated home collage assets from nested photo folders.")
    parser.add_argument("--source", default="фото-видео/машины_thumbs")
    parser.add_argument("--output", default="backend/app/static/home-collage")
    parser.add_argument("--prefix", default="collage")
    parser.add_argument("--limit", type=int, default=240)
    parser.add_argument("--max-per-group", type=int, default=4)
    parser.add_argument("--min-gap", type=int, default=10)
    parser.add_argument("--max-width", type=int, default=640)
    parser.add_argument("--quality", type=int, default=62)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    source_root = (repo_root / args.source).resolve()
    output_root = (repo_root / args.output).resolve()

    if not source_root.exists():
        raise SystemExit(f"source_not_found: {source_root}")

    grouped_paths: dict[str, list[Path]] = {}
    for path in iter_image_files(source_root):
        key = group_key_for(path, source_root)
        if any(part.casefold() in SKIP_DIR_NAMES for part in key.split("/")):
            continue
        grouped_paths.setdefault(key, []).append(path)

    curated_groups: dict[str, list[Path]] = {}
    for key, paths in grouped_paths.items():
        selected = select_group_images(paths, args.max_per_group)
        if selected:
            curated_groups[key] = selected

    ordered = interleave_groups(
        curated_groups,
        limit=args.limit,
        min_gap=args.min_gap,
        seed=args.seed,
    )

    if args.clean and output_root.exists():
        for path in output_root.iterdir():
            if path.is_file():
                path.unlink()

    manifest: list[dict[str, object]] = []
    for idx, (group, src) in enumerate(ordered, start=1):
        filename = f"{args.prefix}-{idx:04d}.webp"
        dst = output_root / filename
        width, height = convert_image(
            src,
            dst,
            max_width=args.max_width,
            quality=args.quality,
        )
        manifest.append(
            {
                "file": filename,
                "group": group,
                "source": src.relative_to(source_root).as_posix(),
                "width": width,
                "height": height,
            }
        )

    (output_root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = {
        "source_root": str(source_root),
        "output_root": str(output_root),
        "groups_total": len(grouped_paths),
        "groups_used": len(curated_groups),
        "images_written": len(manifest),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
