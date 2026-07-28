from pathlib import Path

import pytest

from minikafka.errors import CorruptIndex
from minikafka.log.index import OffsetIndex


def test_sparse_index_returns_floor_position(tmp_path: Path) -> None:
    path = tmp_path / "000.index"
    index = OffsetIndex.create(path, base_offset=100)
    index.append(offset=100, position=0)
    index.append(offset=110, position=512)
    index.append(offset=125, position=900)

    assert index.floor_position(99) == 0
    assert index.floor_position(100) == 0
    assert index.floor_position(117) == 512
    assert index.floor_position(125) == 900

    index.close()
    reopened = OffsetIndex.open(path, base_offset=100)
    assert reopened.floor_position(125) == 900
    reopened.close()


def test_index_rejects_non_monotonic_entries(tmp_path: Path) -> None:
    index = OffsetIndex.create(tmp_path / "000.index", base_offset=0)
    index.append(offset=5, position=20)

    with pytest.raises(ValueError, match="monotonic"):
        index.append(offset=4, position=30)

    with pytest.raises(ValueError, match="monotonic"):
        index.append(offset=6, position=20)

    index.close()


def test_index_rejects_partial_entry(tmp_path: Path) -> None:
    path = tmp_path / "000.index"
    path.write_bytes(b"\x00\x00\x00")

    with pytest.raises(CorruptIndex, match="partial"):
        OffsetIndex.open(path, base_offset=0)


def test_index_can_be_truncated_and_flushed(tmp_path: Path) -> None:
    path = tmp_path / "000.index"
    index = OffsetIndex.create(path, base_offset=0)
    index.append(0, 0)
    index.append(10, 100)
    index.append(20, 200)

    index.truncate_to(offset=15)
    index.flush()
    index.close()

    reopened = OffsetIndex.open(path, base_offset=0)
    assert reopened.entries == ((0, 0), (10, 100))
    reopened.close()
