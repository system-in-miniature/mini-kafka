from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from minikafka.core.metadata import TopicPartition


class TransactionState(str, Enum):
    ONGOING = "ongoing"
    PREPARE_COMMIT = "prepare_commit"
    COMPLETE_COMMIT = "complete_commit"
    PREPARE_ABORT = "prepare_abort"
    COMPLETE_ABORT = "complete_abort"


@dataclass(slots=True)
class TransactionData:
    transaction_id: str
    state: TransactionState = TransactionState.ONGOING
    partitions: set[TopicPartition] = field(default_factory=set)
    first_offsets: dict[TopicPartition, int] = field(default_factory=dict)
    staged_offsets: dict[str, dict[TopicPartition, int]] = field(
        default_factory=dict
    )
