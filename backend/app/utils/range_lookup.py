from __future__ import annotations

from typing import Iterable, Mapping, Any, Optional


def lookup_range(
    value: float,
    rows: Iterable[Mapping[str, Any]],
    from_key: str,
    to_key: str,
    value_key: str,
    inclusive_end: bool = True,
) -> Optional[Any]:
    for row in rows:
        lo = float(row[from_key])
        hi = float(row[to_key])
        if inclusive_end:
            if lo <= value <= hi:
                return row[value_key]
        else:
            if lo <= value < hi:
                return row[value_key]
    return None
