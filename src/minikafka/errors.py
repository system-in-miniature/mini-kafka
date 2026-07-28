from __future__ import annotations


class MiniKafkaError(Exception):
    code = "MINIKAFKA_ERROR"


class OffsetOutOfRange(MiniKafkaError):
    code = "OFFSET_OUT_OF_RANGE"

    def __init__(self, requested: int, start: int, end: int) -> None:
        super().__init__(
            f"offset {requested} outside retained range [{start}, {end}]"
        )
        self.requested = requested
        self.start = start
        self.end = end
