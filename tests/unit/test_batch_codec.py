from __future__ import annotations

from dataclasses import replace

import pytest

from minikafka.core.batch import ControlType, RecordBatch
from minikafka.core.batch_codec import decode_batch, encode_batch
from minikafka.core.record import Header, Record
from minikafka.errors import CorruptBatch, InvalidRecord


def sample_batch() -> RecordBatch:
    return RecordBatch.unassigned(
        records=(
            Record(
                key=b"k\x00",
                value=b"v\xff",
                timestamp_ms=7,
                headers=(Header("trace", b"\x00\x01"),),
            ),
            Record(key=b"k2", value=None, timestamp_ms=8),
        ),
        producer_id=4,
        producer_epoch=2,
        base_sequence=9,
        transactional_id="tx-α",
    ).assign(base_offset=12, leader_epoch=3)


def test_batch_round_trip_preserves_binary_records() -> None:
    batch = sample_batch()

    decoded = decode_batch(encode_batch(batch))

    assert decoded == batch
    assert decoded.next_offset == 14
    assert decoded.last_sequence == 10


def test_crc_detects_payload_corruption() -> None:
    encoded = bytearray(encode_batch(sample_batch()))
    encoded[-1] ^= 0x01

    with pytest.raises(CorruptBatch, match="CRC"):
        decode_batch(bytes(encoded))


def test_decoder_rejects_truncated_frame() -> None:
    encoded = encode_batch(sample_batch())

    with pytest.raises(CorruptBatch, match="length"):
        decode_batch(encoded[:-1])


def test_unassigned_batch_cannot_be_encoded() -> None:
    batch = RecordBatch.unassigned((Record(None, b"x", 1),))

    with pytest.raises(InvalidRecord, match="assigned"):
        encode_batch(batch)


def test_data_batch_must_contain_records() -> None:
    with pytest.raises(InvalidRecord, match="at least one"):
        RecordBatch.unassigned(())


def test_control_batch_round_trip_has_no_user_records() -> None:
    batch = RecordBatch.control_marker(
        transaction_id="tx-1",
        control=ControlType.COMMIT,
    ).assign(20, 4)

    decoded = decode_batch(encode_batch(batch))

    assert decoded.control is ControlType.COMMIT
    assert decoded.records == ()
    assert decoded.next_offset == 21


def test_unknown_format_version_is_rejected() -> None:
    encoded = encode_batch(sample_batch())
    changed = replace(sample_batch(), format_version=99)

    with pytest.raises(InvalidRecord, match="format version"):
        encode_batch(changed)

    assert encoded.startswith(b"MKB1")
