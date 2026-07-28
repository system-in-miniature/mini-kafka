from pathlib import Path

import pytest

from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import StorageError
from minikafka.log.segment import Segment


def assigned_batch(base_offset: int, values: tuple[bytes, ...]) -> RecordBatch:
    return RecordBatch.unassigned(
        tuple(
            Record(None, value, base_offset + index)
            for index, value in enumerate(values)
        )
    ).assign(base_offset, leader_epoch=0)


def test_active_tail_is_truncated_to_last_valid_batch(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    segment.append(assigned_batch(0, (b"safe",)))
    valid_size = segment.size_bytes
    log_path = segment.log_path
    segment.close()
    with log_path.open("ab") as file:
        file.write(b"MKB1\x00\x00\x00\x20partial")

    recovered = Segment.open(
        tmp_path,
        0,
        index_interval_bytes=8,
        active=True,
    )

    assert recovered.size_bytes == valid_size
    assert recovered.leo == 1
    assert [(record.offset, record.value) for record in recovered.scan(0)] == [
        (0, b"safe")
    ]
    recovered.close()


def test_active_crc_failure_is_truncated(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    segment.append(assigned_batch(0, (b"safe",)))
    valid_size = segment.size_bytes
    segment.append(assigned_batch(1, (b"break",)))
    log_path = segment.log_path
    segment.close()
    data = bytearray(log_path.read_bytes())
    data[-1] ^= 0x01
    log_path.write_bytes(data)

    recovered = Segment.open(tmp_path, 0, 8, active=True)

    assert recovered.size_bytes == valid_size
    assert recovered.leo == 1
    recovered.close()


def test_closed_segment_corruption_is_not_silently_repaired(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    segment.append(assigned_batch(0, (b"history",)))
    log_path = segment.log_path
    segment.close()
    data = bytearray(log_path.read_bytes())
    data[-1] ^= 0x01
    log_path.write_bytes(data)

    with pytest.raises(StorageError, match="closed segment"):
        Segment.open(tmp_path, 0, 8, active=False)


def test_missing_index_is_rebuilt_from_log(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, 0, index_interval_bytes=1)
    segment.append(assigned_batch(0, (b"a",)))
    segment.append(assigned_batch(1, (b"b",)))
    index_path = segment.index_path
    segment.close()
    index_path.unlink()

    recovered = Segment.open(tmp_path, 0, 1, active=True)

    assert recovered.index.entries[0] == (0, 0)
    assert len(recovered.index.entries) == 2
    recovered.close()
