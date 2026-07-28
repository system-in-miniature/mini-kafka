from __future__ import annotations

from enum import Enum


class LifecycleState(str, Enum):
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


class FailureInjector:
    def __init__(self) -> None:
        self._next_flush: BaseException | None = None

    def fail_next_flush(self, error: BaseException) -> None:
        self._next_flush = error

    def before_flush(self) -> None:
        if self._next_flush is None:
            return
        error = self._next_flush
        self._next_flush = None
        raise error
