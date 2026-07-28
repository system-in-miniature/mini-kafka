from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

from minikafka.clock import Clock
from minikafka.core.batch import RecordBatch
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Header, Record
from minikafka.errors import UnknownPartition
from minikafka.producer.accumulator import BatchAccumulator, PendingRecord
from minikafka.producer.partitioner import Partitioner
from minikafka.replication.model import AckMode


@dataclass(frozen=True, slots=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int | None
    base_offset: int | None
    timestamp_ms: int


class Producer:
    def __init__(
        self,
        cluster: object,
        clock: Clock,
        *,
        batch_size: int,
        linger_ms: int,
        max_buffer_bytes: int,
        acks: AckMode | str | int,
        producer_id: int = -1,
        producer_epoch: int = -1,
    ) -> None:
        self.cluster = cluster
        self.clock = clock
        self.partitioner = Partitioner()
        self.acks = AckMode.parse(acks)
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self._sequences: dict[TopicPartition, int] = {}
        self.accumulator = BatchAccumulator(
            batch_size=batch_size,
            linger_ms=linger_ms,
            max_buffer_bytes=max_buffer_bytes,
        )
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def send(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None = None,
        timestamp_ms: int | None = None,
        headers: Iterable[Header] = (),
        partition: int | None = None,
    ) -> asyncio.Future[RecordMetadata]:
        if self._closed:
            raise RuntimeError("producer is closed")
        topic_metadata = self.cluster.topic(topic)
        partition_count = len(topic_metadata.partitions)
        selected = (
            self.partitioner.choose(partition_count, key)
            if partition is None
            else partition
        )
        if selected not in topic_metadata.partitions:
            raise UnknownPartition(f"{topic}-{selected}")
        tp = TopicPartition(topic, selected)
        timestamp = self.clock.now_ms() if timestamp_ms is None else timestamp_ms
        record = Record(key, value, timestamp, tuple(headers))
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RecordMetadata] = loop.create_future()
        estimated_bytes = 39 + len(key or b"") + len(value or b"")
        pending = PendingRecord(
            topic_partition=tp,
            record=record,
            future=future,
            estimated_bytes=estimated_bytes,
        )
        full = self.accumulator.add(pending, self.clock.now_ms())
        if full:
            self._schedule_flush(tp)
        return future

    def _schedule_flush(self, tp: TopicPartition) -> None:
        task = asyncio.create_task(self._flush_partition(tp))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _flush_partition(self, tp: TopicPartition) -> None:
        pending = self.accumulator.pop(tp)
        if not pending:
            return
        batch = RecordBatch.unassigned(
            tuple(item.record for item in pending),
            producer_id=self.producer_id,
            producer_epoch=self.producer_epoch,
            base_sequence=(
                self._sequences.get(tp, 0)
                if self.producer_id >= 0
                else -1
            ),
        )
        try:
            appended = await self.cluster.append_batch(tp, batch, self.acks)
        except Exception as error:  # noqa: BLE001 - forward batch failure
            for item in pending:
                if not item.future.done():
                    item.future.set_exception(error)
            return
        if self.producer_id >= 0:
            self._sequences[tp] = batch.last_sequence + 1
        for delta, item in enumerate(pending):
            if not item.future.done():
                item.future.set_result(
                    RecordMetadata(
                        topic=tp.topic,
                        partition=tp.partition,
                        offset=(
                            None
                            if appended.base_offset is None
                            else appended.base_offset + delta
                        ),
                        base_offset=appended.base_offset,
                        timestamp_ms=item.record.timestamp_ms,
                    )
                )
        if any(item.record.key is None for item in pending):
            self.partitioner.on_batch_closed(
                len(self.cluster.topic(tp.topic).partitions),
                tp.partition,
            )

    async def run_due_flushes(self) -> None:
        due = self.accumulator.due_partitions(self.clock.now_ms())
        await asyncio.gather(*(self._flush_partition(tp) for tp in due))

    async def flush(self) -> None:
        while self.accumulator.partitions() or self._tasks:
            self._tasks = {task for task in self._tasks if not task.done()}
            partitions = self.accumulator.partitions()
            if partitions:
                await asyncio.gather(
                    *(self._flush_partition(tp) for tp in partitions)
                )
            if self._tasks:
                await asyncio.gather(*tuple(self._tasks))

    async def close(self) -> None:
        if self._closed:
            return
        await self.flush()
        self._closed = True
