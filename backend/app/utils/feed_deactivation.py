from __future__ import annotations

from typing import Optional, Tuple


def should_deactivate_feed(
    *,
    mode: str,
    current_seen: int,
    previous_seen: Optional[int],
    min_ratio: float,
    min_seen: int,
) -> Tuple[bool, str]:
    normalized_mode = (mode or "auto").strip().lower()
    if normalized_mode == "skip":
        return False, "mode=skip"
    if current_seen <= 0:
        return False, "current_seen=0"
    if normalized_mode == "force":
        return True, "mode=force"
    if current_seen < max(1, int(min_seen or 0)):
        return False, f"current_seen<{int(min_seen or 0)}"
    if previous_seen is None or previous_seen <= 0:
        return False, "previous_seen_missing"
    ratio = float(current_seen) / float(previous_seen)
    if ratio < float(min_ratio):
        return False, f"ratio={ratio:.4f}<{float(min_ratio):.4f}"
    return True, f"ratio={ratio:.4f}>={float(min_ratio):.4f}"
