"""Key compaction and tombstone retention for closed log segments.

The policy corresponds to Kafka ``cleanup.policy=compact`` and
``delete.retention.ms``; MiniKafka rebuilds a whole closed-segment view so the
crash-safety boundary is easy to inspect.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace

from minikafka.clock import Clock
from minikafka.core.batch import RecordBatch
from minikafka.log.partition_log import PartitionLog


@dataclass(frozen=True, slots=True)
class CompactionResult:
    source_segments: tuple[int, ...]
    records_before: int
    records_after: int


class LogCompactor:
    def __init__(
        self,
        clock: Clock,
        *,
        delete_retention_ms: int,
        before_swap: Callable[[], None] | None = None,
    ) -> None:
        if delete_retention_ms < 0:
            raise ValueError("delete_retention_ms cannot be negative")
        self.clock = clock
        self.delete_retention_ms = delete_retention_ms
        self.before_swap = before_swap

    def compact(self, log: PartitionLog) -> CompactionResult:
        source = tuple(log.closed_segments)
        if not source:
            return CompactionResult((), 0, 0)
        latest = self._latest_offsets(
            located.batch
            for segment in log.segments
            for located in segment.iter_batches()
        )
        compacted: list[RecordBatch] = []
        records_before = 0
        records_after = 0
        for segment in source:
            for located in segment.iter_batches():
                batch = located.batch
                records_before += len(batch.records)
                rewritten = self._compact_batch(batch, latest)
                if rewritten is not None:
                    compacted.append(rewritten)
                    records_after += len(rewritten.records)
        # Kafka's log cleaner rewrites segment files in the background. This
        # project instead fsyncs a sibling directory, renames live -> backup,
        # then replacement -> live, with rollback on an in-process exception.
        # Each rename is atomic but the pair is not: a process crash between
        # them can leave only the backup. The simpler boundary still avoids
        # publishing partially written segment files and preserves offset gaps.
        log.install_compacted_closed(
            tuple(compacted),
            before_swap=self.before_swap,
        )
        return CompactionResult(
            tuple(segment.base_offset for segment in source),
            records_before,
            records_after,
        )

    @staticmethod
    def _latest_offsets(
        batches: Iterable[RecordBatch],
    ) -> dict[bytes, int]:
        latest: dict[bytes, int] = {}
        for batch in batches:
            if batch.base_offset is None or batch.offset_deltas is None:
                continue
            for delta, record in zip(
                batch.offset_deltas,
                batch.records,
                strict=True,
            ):
                if record.key is not None:
                    latest[record.key] = batch.base_offset + delta
        return latest

    def _compact_batch(
        self,
        batch: RecordBatch,
        latest: dict[bytes, int],
    ) -> RecordBatch | None:
        if batch.control is not None:
            return batch
        if batch.base_offset is None or batch.offset_deltas is None:
            return batch
        records = []
        deltas = []
        for delta, record in zip(
            batch.offset_deltas,
            batch.records,
            strict=True,
        ):
            offset = batch.base_offset + delta
            keep = record.key is None or latest.get(record.key) == offset
            if (
                keep
                and record.key is not None
                and record.value is None
                and self.clock.now_ms() - record.timestamp_ms
                > self.delete_retention_ms
            ):
                keep = False
            if keep:
                records.append(record)
                deltas.append(delta)
        if not records:
            return None
        return replace(
            batch,
            records=tuple(records),
            offset_deltas=tuple(deltas),
        )
