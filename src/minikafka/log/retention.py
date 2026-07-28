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
        selected: set[int] = set()
        if retention_ms is not None:
            boundary = self.clock.now_ms() - retention_ms
            selected.update(
                segment.base_offset
                for segment in closed
                if segment.max_timestamp_ms < boundary
            )

        if retention_bytes is not None:
            total = log.size_bytes - sum(
                segment.total_size_bytes
                for segment in closed
                if segment.base_offset in selected
            )
            for segment in closed:
                if total <= retention_bytes:
                    break
                if segment.base_offset in selected:
                    continue
                selected.add(segment.base_offset)
                total -= segment.total_size_bytes

        return log.delete_closed_segments(tuple(sorted(selected)))
