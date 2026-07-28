from pathlib import Path

import pytest

from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import InvalidRecord, OffsetOutOfRange
from minikafka.log.partition_log import PartitionLog


def config(tmp_path: Path, *, segment_max_bytes: int = 120) -> MiniKafkaConfig:
    return MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=segment_max_bytes,
        index_interval_bytes=1,
    )


def batch(*values: bytes) -> RecordBatch:
    return RecordBatch.unassigned(
        tuple(Record(None, value, index) for index, value in enumerate(values))
    )


def test_partition_log_rolls_and_fetches_across_segments(tmp_path: Path) -> None:
    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    for value in (b"a" * 40, b"b" * 40, b"c" * 40):
        log.append(batch(value))

    assert len(log.segments) >= 2
    assert [record.value for record in log.fetch(1, max_records=10)] == [
        b"b" * 40,
        b"c" * 40,
    ]
    assert log.leo == 3
    log.close()


def test_fetch_enforces_retained_range_and_end_offset(tmp_path: Path) -> None:
    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    log.append(batch(b"a", b"b", b"c"))

    assert [record.offset for record in log.fetch(0, 10, end_offset=2)] == [0, 1]
    assert log.fetch(3, 10) == ()
    with pytest.raises(OffsetOutOfRange):
        log.fetch(4, 1)
    log.close()


def test_truncate_requires_batch_boundary(tmp_path: Path) -> None:
    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    log.append(batch(b"a", b"b"))
    log.append(batch(b"c"))

    with pytest.raises(InvalidRecord, match="batch boundary"):
        log.truncate_to(1)

    log.truncate_to(2)
    assert log.leo == 2
    assert [record.value for record in log.fetch(0, 10)] == [b"a", b"b"]
    log.close()


def test_read_batches_respects_byte_budget_but_returns_one_large_batch(
    tmp_path: Path,
) -> None:
    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    log.append(batch(b"a" * 100))
    log.append(batch(b"b"))

    batches = log.read_batches(0, max_bytes=1)

    assert len(batches) == 1
    assert batches[0].base_offset == 0
    log.close()
