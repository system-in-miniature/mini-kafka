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


class InvalidRecord(MiniKafkaError):
    code = "INVALID_RECORD"


class CorruptBatch(MiniKafkaError):
    code = "CORRUPT_BATCH"


class CorruptIndex(MiniKafkaError):
    code = "CORRUPT_INDEX"


class StorageError(MiniKafkaError):
    code = "STORAGE_ERROR"


class TopicAlreadyExists(MiniKafkaError):
    code = "TOPIC_ALREADY_EXISTS"


class UnknownTopic(MiniKafkaError):
    code = "UNKNOWN_TOPIC"


class UnknownPartition(MiniKafkaError):
    code = "UNKNOWN_PARTITION"


class ProducerBufferFull(MiniKafkaError):
    code = "PRODUCER_BUFFER_FULL"


class RebalanceInProgress(MiniKafkaError):
    code = "REBALANCE_IN_PROGRESS"


class IllegalGeneration(MiniKafkaError):
    code = "ILLEGAL_GENERATION"

    def __init__(self, actual: int, expected: int) -> None:
        super().__init__(f"generation {actual} does not match {expected}")


class NotPartitionOwner(MiniKafkaError):
    code = "NOT_PARTITION_OWNER"


class UnknownMember(MiniKafkaError):
    code = "UNKNOWN_MEMBER"


class NotEnoughReplicas(MiniKafkaError):
    code = "NOT_ENOUGH_REPLICAS"


class NotEnoughReplicasAfterAppend(MiniKafkaError):
    code = "NOT_ENOUGH_REPLICAS_AFTER_APPEND"


class NotInSyncReplica(MiniKafkaError):
    code = "NOT_IN_SYNC_REPLICA"


class FencedLeaderEpoch(MiniKafkaError):
    code = "FENCED_LEADER_EPOCH"


class ProducerFenced(MiniKafkaError):
    code = "PRODUCER_FENCED"


class OutOfOrderSequence(MiniKafkaError):
    code = "OUT_OF_ORDER_SEQUENCE"

    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(f"expected sequence {expected}, got {actual}")
        self.expected = expected
        self.actual = actual
