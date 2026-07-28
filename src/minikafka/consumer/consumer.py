from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from minikafka.core.metadata import TopicPartition
from minikafka.core.record import StoredRecord
from minikafka.errors import OffsetOutOfRange

if TYPE_CHECKING:
    from minikafka.core.cluster import BrokerCluster


class Consumer:
    def __init__(
        self,
        cluster: BrokerCluster,
        group_id: str,
        *,
        auto_offset_reset: str,
    ) -> None:
        if not group_id:
            raise ValueError("group_id cannot be empty")
        if auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError("auto_offset_reset must be earliest, latest, or error")
        self.cluster = cluster
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self._assignment: tuple[TopicPartition, ...] = ()
        self._positions: dict[TopicPartition, int] = {}
        self._closed = False

    @property
    def assignment(self) -> tuple[TopicPartition, ...]:
        return self._assignment

    def assign(self, partitions: Iterable[TopicPartition]) -> None:
        assigned = tuple(sorted(set(partitions)))
        for tp in assigned:
            self.cluster.partition_metadata(tp)
        self._assignment = assigned
        self._positions = {
            tp: position
            for tp, position in self._positions.items()
            if tp in assigned
        }

    async def _initial_position(self, tp: TopicPartition) -> int:
        committed = await self.committed(tp)
        if committed is not None:
            return committed
        if self.auto_offset_reset == "earliest":
            return self.cluster.log_start_offset(tp)
        if self.auto_offset_reset == "latest":
            return self.cluster.visible_end(tp)
        raise OffsetOutOfRange(
            -1,
            self.cluster.log_start_offset(tp),
            self.cluster.visible_end(tp),
        )

    async def poll(self, max_records: int) -> tuple[StoredRecord, ...]:
        if self._closed:
            raise RuntimeError("consumer is closed")
        if max_records < 0:
            raise ValueError("max_records cannot be negative")
        result: list[StoredRecord] = []
        for tp in self._assignment:
            if len(result) >= max_records:
                break
            position = self._positions.get(tp)
            if position is None:
                position = await self._initial_position(tp)
                self._positions[tp] = position
            try:
                fetched = self.cluster.fetch(
                    tp,
                    position,
                    max_records - len(result),
                )
            except OffsetOutOfRange:
                if self.auto_offset_reset == "error":
                    raise
                position = (
                    self.cluster.log_start_offset(tp)
                    if self.auto_offset_reset == "earliest"
                    else self.cluster.visible_end(tp)
                )
                self._positions[tp] = position
                fetched = self.cluster.fetch(
                    tp,
                    position,
                    max_records - len(result),
                )
            if fetched:
                self._positions[tp] = fetched[-1].offset + 1
                result.extend(fetched)
        return tuple(result)

    def position(self, tp: TopicPartition) -> int:
        try:
            return self._positions[tp]
        except KeyError as error:
            raise ValueError(f"no position initialized for {tp}") from error

    def seek(self, tp: TopicPartition, offset: int) -> None:
        if tp not in self._assignment:
            raise ValueError(f"{tp} is not assigned")
        if offset < 0:
            raise ValueError("offset cannot be negative")
        self._positions[tp] = offset

    async def committed(self, tp: TopicPartition) -> int | None:
        return await self.cluster.offsets.get(self.group_id, tp)

    async def commit(self) -> None:
        await self.cluster.offsets.commit(
            self.group_id,
            dict(self._positions),
        )

    async def lag(self, tp: TopicPartition) -> int:
        return max(0, self.cluster.visible_end(tp) - self.position(tp))

    async def close(self) -> None:
        self._closed = True
