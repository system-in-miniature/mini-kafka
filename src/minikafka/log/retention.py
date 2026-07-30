from __future__ import annotations

from minikafka.clock import Clock
from minikafka.log.partition_log import PartitionLog


class RetentionManager:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock

    def apply(
        self,
        log: PartitionLog,
        *,
        retention_ms: int | None,
        retention_bytes: int | None,
    ) -> tuple[int, ...]:
        if retention_ms is not None and retention_ms < 0:
            raise ValueError("retention_ms cannot be negative")
        if retention_bytes is not None and retention_bytes <= 0:
            raise ValueError("retention_bytes must be positive")

        closed = list(log.closed_segments)
        selected = []
        if retention_ms is not None:
            boundary = self.clock.now_ms() - retention_ms
            for segment in closed:
                if segment.max_timestamp_ms >= boundary:
                    break
                selected.append(segment)

        if retention_bytes is not None:
            total = log.size_bytes - sum(
                segment.total_size_bytes
                for segment in selected
            )
            for segment in closed[len(selected):]:
                if total <= retention_bytes:
                    break
                selected.append(segment)
                total -= segment.total_size_bytes

        return log.delete_closed_segments(
            tuple(segment.base_offset for segment in selected)
        )
