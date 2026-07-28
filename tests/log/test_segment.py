from pathlib import Path

import pytest

from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import InvalidRecord
from minikafka.log.segment import Segment


def assigned_batch(base_offset: int, values: tuple[bytes, ...]) -> RecordBatch:
    return RecordBatch.unassigned(
        tuple(Record(None, value, base_offset + index) for index, value in enumerate(values))
    ).assign(base_offset, leader_epoch=0)


def test_segment_appends_and_scans_from_offset(tmp_path: Path) -> None:
    segment = Segment.create(
        tmp_path,
        base_offset=0,
        index_interval_bytes=1,
    )
    segment.append(assigned_batch(0, (b"a", b"b")))
    segment.append(assigned_batch(2, (b"c",)))

    records = segment.scan(1)

    assert [(record.offset, record.value) for record in records] == [
        (1, b"b"),
        (2, b"c"),
    ]
    assert segment.leo == 3
    assert segment.max_timestamp_ms == 2
    segment.close()


def test_segment_requires_contiguous_batches(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)

    with pytest.raises(InvalidRecord, match="expected base offset 0"):
        segment.append(assigned_batch(1, (b"x",)))

    segment.close()


def test_segment_flushes_log_and_index(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=1)
    segment.append(assigned_batch(0, (b"x",)))

    segment.flush()

    assert segment.log_path.stat().st_size == segment.size_bytes
    assert segment.index_path.stat().st_size > 0
    segment.close()
