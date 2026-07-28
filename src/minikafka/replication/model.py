from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from minikafka.core.batch import RecordBatch


class AckMode(str, Enum):
    NONE = "0"
    LEADER = "1"
    ALL = "all"

    @classmethod
    def parse(cls, value: AckMode | str | int) -> AckMode:
        if isinstance(value, cls):
            return value
        normalized = str(value)
        if normalized == "-1":
            normalized = "all"
        try:
            return cls(normalized)
        except ValueError as error:
            raise ValueError("acks must be 0, 1, or all") from error


class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"


@dataclass(frozen=True, slots=True)
class ProduceResult:
    batch: RecordBatch
    base_offset: int | None
    next_offset: int
    offsets_known: bool = True


@dataclass(frozen=True, slots=True)
class ReplicaState:
    broker_id: int
    leo: int
    last_fetch_ms: int
    in_sync: bool
