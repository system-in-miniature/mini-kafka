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
