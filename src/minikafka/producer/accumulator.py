from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass

from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.errors import ProducerBufferFull


@dataclass(slots=True)
class PendingRecord:
    topic_partition: TopicPartition
    record: Record
    future: asyncio.Future[object]
    estimated_bytes: int


class BatchAccumulator:
    def __init__(
        self,
        *,
        batch_size: int,
        linger_ms: int,
        max_buffer_bytes: int,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if linger_ms < 0:
            raise ValueError("linger_ms cannot be negative")
        if max_buffer_bytes <= 0:
            raise ValueError("max_buffer_bytes must be positive")
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.max_buffer_bytes = max_buffer_bytes
        self.total_bytes = 0
        self._pending: dict[TopicPartition, list[PendingRecord]] = defaultdict(list)
        self._first_enqueue_ms: dict[TopicPartition, int] = {}
        self._partition_bytes: dict[TopicPartition, int] = defaultdict(int)

    def add(self, pending: PendingRecord, now_ms: int) -> bool:
        if self.total_bytes + pending.estimated_bytes > self.max_buffer_bytes:
            raise ProducerBufferFull("producer accumulator is full")
        tp = pending.topic_partition
        if not self._pending[tp]:
            self._first_enqueue_ms[tp] = now_ms
        self._pending[tp].append(pending)
        self._partition_bytes[tp] += pending.estimated_bytes
        self.total_bytes += pending.estimated_bytes
        return self._partition_bytes[tp] >= self.batch_size

    def due_partitions(self, now_ms: int) -> tuple[TopicPartition, ...]:
        return tuple(
            sorted(
                tp
                for tp, first_ms in self._first_enqueue_ms.items()
                if self._pending[tp] and now_ms - first_ms >= self.linger_ms
            )
        )

    def partitions(self) -> tuple[TopicPartition, ...]:
        return tuple(sorted(tp for tp, records in self._pending.items() if records))

    def pop(self, tp: TopicPartition) -> tuple[PendingRecord, ...]:
        records = tuple(self._pending.pop(tp, ()))
        if not records:
            return ()
        size = self._partition_bytes.pop(tp)
        self.total_bytes -= size
        self._first_enqueue_ms.pop(tp, None)
        return records
