from pathlib import Path

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.compaction import LogCompactor
from minikafka.log.partition_log import PartitionLog


def compactable_log(tmp_path: Path) -> PartitionLog:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=160,
        index_interval_bytes=1,
    )
    return PartitionLog.open(tmp_path / "state-0", config)


def append_records(
    log: PartitionLog,
    records: tuple[tuple[bytes | None, bytes | None, int], ...],
) -> None:
    log.append(
        RecordBatch.unassigned(
            tuple(Record(key, value, timestamp) for key, value, timestamp in records)
        )
    )


def test_compaction_preserves_latest_key_and_original_offsets(
    tmp_path: Path,
) -> None:
    log = compactable_log(tmp_path)
    append_records(log, ((b"a", b"1", 0), (b"b", b"x", 1)))
    log.roll()
    append_records(log, ((b"a", b"2", 2), (None, b"event", 3)))
    log.roll()
    append_records(log, ((b"active", b"keep", 4),))

    LogCompactor(ManualClock(10), delete_retention_ms=1000).compact(log)

    assert [
        (record.offset, record.key, record.value)
        for record in log.fetch(0, 10)
    ] == [
        (1, b"b", b"x"),
        (2, b"a", b"2"),
        (3, None, b"event"),
        (4, b"active", b"keep"),
    ]
    assert log.leo == 5
    log.close()


def test_recent_tombstone_is_retained(tmp_path: Path) -> None:
    log = compactable_log(tmp_path)
    append_records(log, ((b"a", b"1", 0), (b"a", None, 100)))
    log.roll()
    append_records(log, ((b"active", b"x", 101),))

    LogCompactor(ManualClock(500), delete_retention_ms=1000).compact(log)

    records = log.fetch(0, 10)
    assert [(record.offset, record.value) for record in records[:-1]] == [
        (1, None)
    ]
    log.close()


def test_expired_tombstone_removes_key_history(tmp_path: Path) -> None:
    log = compactable_log(tmp_path)
    append_records(log, ((b"a", b"1", 0), (b"a", None, 100)))
    log.roll()
    append_records(log, ((b"active", b"x", 2000),))

    LogCompactor(ManualClock(2000), delete_retention_ms=1000).compact(log)

    assert [(record.key, record.value) for record in log.fetch(0, 10)] == [
        (b"active", b"x")
    ]
    log.close()


def test_active_segment_is_not_cleaned(tmp_path: Path) -> None:
    log = compactable_log(tmp_path)
    append_records(log, ((b"a", b"old", 0),))
    log.roll()
    append_records(log, ((b"a", b"active-new", 1), (b"a", b"active-newer", 2)))

    LogCompactor(ManualClock(3), delete_retention_ms=1000).compact(log)

    assert [(record.offset, record.value) for record in log.fetch(0, 10)] == [
        (1, b"active-new"),
        (2, b"active-newer"),
    ]
    log.close()
