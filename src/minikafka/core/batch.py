from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from minikafka.core.record import Record
from minikafka.errors import InvalidRecord


class ControlType(str, Enum):
    COMMIT = "commit"
    ABORT = "abort"


@dataclass(frozen=True, slots=True)
class RecordBatch:
    records: tuple[Record, ...]
    offset_deltas: tuple[int, ...] | None = None
    last_offset_delta: int | None = None
    base_offset: int | None = None
    leader_epoch: int = -1
    producer_id: int = -1
    producer_epoch: int = -1
    base_sequence: int = -1
    transactional_id: str | None = None
    control: ControlType | None = None
    format_version: int = 1

    def __post_init__(self) -> None:
        if not self.records and self.control is None:
            raise InvalidRecord("data batch must contain at least one record")
        if self.control is not None and self.records:
            raise InvalidRecord("control batch cannot contain user records")
        if self.control is not None and self.transactional_id is None:
            raise InvalidRecord("control batch requires a transaction ID")
        if self.base_offset is not None and self.base_offset < 0:
            raise InvalidRecord("base offset cannot be negative")
        offset_deltas = self.offset_deltas
        if offset_deltas is None:
            offset_deltas = tuple(range(len(self.records)))
            object.__setattr__(self, "offset_deltas", offset_deltas)
        if len(offset_deltas) != len(self.records):
            raise InvalidRecord("offset delta count must match record count")
        if any(delta < 0 for delta in offset_deltas):
            raise InvalidRecord("offset deltas cannot be negative")
        if tuple(sorted(set(offset_deltas))) != offset_deltas:
            raise InvalidRecord("offset deltas must be strictly increasing")
        last_offset_delta = self.last_offset_delta
        if last_offset_delta is None:
            last_offset_delta = (
                max(offset_deltas, default=-1)
                if self.control is None
                else 0
            )
            object.__setattr__(
                self,
                "last_offset_delta",
                last_offset_delta,
            )
        if last_offset_delta < 0:
            raise InvalidRecord("last offset delta cannot be negative")
        if offset_deltas and offset_deltas[-1] > last_offset_delta:
            raise InvalidRecord("record offset exceeds last offset delta")

    @classmethod
    def unassigned(
        cls,
        records: tuple[Record, ...],
        *,
        producer_id: int = -1,
        producer_epoch: int = -1,
        base_sequence: int = -1,
        transactional_id: str | None = None,
    ) -> RecordBatch:
        return cls(
            records=tuple(records),
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=base_sequence,
            transactional_id=transactional_id,
        )

    @classmethod
    def control_marker(
        cls, *, transaction_id: str, control: ControlType
    ) -> RecordBatch:
        return cls(
            records=(),
            transactional_id=transaction_id,
            control=control,
        )

    def assign(self, base_offset: int, leader_epoch: int) -> RecordBatch:
        if self.base_offset is not None:
            raise InvalidRecord("batch is already assigned")
        return replace(
            self,
            base_offset=base_offset,
            leader_epoch=leader_epoch,
        )

    @property
    def logical_count(self) -> int:
        if self.last_offset_delta is None:
            raise InvalidRecord("batch has no last offset delta")
        return self.last_offset_delta + 1

    @property
    def next_offset(self) -> int:
        if self.base_offset is None:
            raise InvalidRecord("batch must be assigned")
        return self.base_offset + self.logical_count

    @property
    def last_sequence(self) -> int:
        if self.base_sequence < 0:
            return -1
        return self.base_sequence + self.logical_count - 1

    @property
    def max_timestamp_ms(self) -> int:
        if not self.records:
            return -1
        return max(record.timestamp_ms for record in self.records)
