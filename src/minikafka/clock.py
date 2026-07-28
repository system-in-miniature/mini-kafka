from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


@dataclass(slots=True)
class ManualClock:
    _now_ms: int = 0

    def now_ms(self) -> int:
        return self._now_ms

    def advance_ms(self, amount: int) -> None:
        if amount < 0:
            raise ValueError("clock cannot move backwards")
        self._now_ms += amount


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000
