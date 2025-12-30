from __future__ import annotations

import threading
import time
from typing import Optional


class TokenBucket:
    def __init__(self, *, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(rate_per_sec, 0.01)
        # Ensure capacity is at least 1 token to avoid deadlock at low rates
        default_cap = max(self.rate * 2, 1.0)
        self.capacity = capacity if capacity is not None else default_cap
        if self.capacity < 1.0:
            self.capacity = 1.0
        self.tokens = self.capacity
        self.lock = threading.Lock()
        self.last = time.monotonic()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.tokens = min(
                    self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    self.last = now
                    return
                needed = (1 - self.tokens) / self.rate
                self.last = now
            time.sleep(min(needed, 1.0))


__all__ = ["TokenBucket"]
