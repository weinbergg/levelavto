import math
from typing import Optional


def ceil_to_step(value: Optional[float], step: int) -> Optional[float]:
    if value is None:
        return None
    step_f = float(step)
    return float(math.ceil(float(value) / step_f) * step_f)
