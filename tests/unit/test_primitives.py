from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.errors import OffsetOutOfRange


def test_manual_clock_is_deterministic() -> None:
    clock = ManualClock(100)

    clock.advance_ms(25)

    assert clock.now_ms() == 125


def test_manual_clock_cannot_move_backwards() -> None:
    clock = ManualClock(100)

    with pytest.raises(ValueError, match="backwards"):
        clock.advance_ms(-1)


def test_config_rejects_invalid_segment_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="segment_max_bytes"):
        MiniKafkaConfig(data_dir=tmp_path, segment_max_bytes=0)


def test_error_carries_stable_code() -> None:
    error = OffsetOutOfRange(requested=3, start=5, end=9)

    assert error.code == "OFFSET_OUT_OF_RANGE"
    assert str(error) == "offset 3 outside retained range [5, 9]"
