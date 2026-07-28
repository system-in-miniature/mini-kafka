from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import OffsetOutOfRange
from minikafka.log.partition_log import PartitionLog
from minikafka.log.retention import RetentionManager


def append(log: PartitionLog, value: bytes, timestamp_ms: int) -> None:
    log.append(
        RecordBatch.unassigned(
            (Record(key=None, value=value, timestamp_ms=timestamp_ms),)
        )
    )


def rolled_log(tmp_path: Path) -> PartitionLog:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=80,
        index_interval_bytes=1,
    )
    log = PartitionLog.open(tmp_path / "events-0", config)
    for timestamp in (0, 50, 150, 250):
        append(log, bytes([timestamp % 256]) * 20, timestamp)
    assert len(log.closed_segments) >= 3
    return log


def test_time_retention_deletes_closed_segments_only(tmp_path: Path) -> None:
    log = rolled_log(tmp_path)
    active_base = log.active.base_offset
    manager = RetentionManager(ManualClock(300))

    deleted = manager.apply(log, retention_ms=100, retention_bytes=None)

    assert deleted == (0, 1, 2)
    assert log.active.base_offset == active_base
    assert log.log_start_offset == active_base
    with pytest.raises(OffsetOutOfRange):
        log.fetch(0, 1)
    log.close()


def test_size_retention_deletes_oldest_segments_first(tmp_path: Path) -> None:
    log = rolled_log(tmp_path)
    newest_closed = log.closed_segments[-1].base_offset
    target = log.active.size_bytes + log.closed_segments[-1].size_bytes + 20

    deleted = RetentionManager(ManualClock()).apply(
        log,
        retention_ms=None,
        retention_bytes=target,
    )

    assert deleted
    assert deleted == tuple(sorted(deleted))
    assert newest_closed not in deleted
    assert log.log_start_offset > 0
    log.close()


def test_retention_result_survives_restart(tmp_path: Path) -> None:
    log = rolled_log(tmp_path)
    directory = log.directory
    config = log.config
    RetentionManager(ManualClock(300)).apply(
        log,
        retention_ms=100,
        retention_bytes=None,
    )
    start = log.log_start_offset
    log.close()

    reopened = PartitionLog.open(directory, config)

    assert reopened.log_start_offset == start
    assert [record.offset for record in reopened.fetch(start, 10)] == [start]
    reopened.close()
