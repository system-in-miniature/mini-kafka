from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.compaction import LogCompactor
from minikafka.log.partition_log import PartitionLog


def test_failure_before_swap_leaves_original_log_authoritative(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=100,
        index_interval_bytes=1,
    )
    directory = tmp_path / "state-0"
    log = PartitionLog.open(directory, config)
    for value in (b"one", b"two", b"three"):
        log.append(
            RecordBatch.unassigned((Record(b"k", value, 0),))
        )
    original = [(record.offset, record.value) for record in log.fetch(0, 10)]

    def fail() -> None:
        raise OSError("injected before swap")

    with pytest.raises(OSError, match="injected"):
        LogCompactor(
            ManualClock(),
            delete_retention_ms=1000,
            before_swap=fail,
        ).compact(log)

    assert [(record.offset, record.value) for record in log.fetch(0, 10)] == original
    log.close()


def test_compacted_log_survives_restart(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=100,
        index_interval_bytes=1,
    )
    directory = tmp_path / "state-0"
    log = PartitionLog.open(directory, config)
    for value in (b"one", b"two", b"three"):
        log.append(RecordBatch.unassigned((Record(b"k", value, 0),)))
    LogCompactor(ManualClock(), delete_retention_ms=1000).compact(log)
    expected = [(record.offset, record.value) for record in log.fetch(0, 10)]
    log.close()

    reopened = PartitionLog.open(directory, config)

    assert [(record.offset, record.value) for record in reopened.fetch(0, 10)] == expected
    reopened.close()
