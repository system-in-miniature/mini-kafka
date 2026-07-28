from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

from minikafka.core.batch import ControlType, RecordBatch
from minikafka.core.record import Header, Record
from minikafka.errors import CorruptBatch, InvalidRecord

MAGIC = b"MKB1"
FORMAT_VERSION = 1
FRAME_HEADER = struct.Struct(">4sII")
PAYLOAD_HEADER = struct.Struct(">BqIqiiBHI")
RECORD_HEADER = struct.Struct(">qiiH")
HEADER_KEY_LENGTH = struct.Struct(">H")
HEADER_VALUE_LENGTH = struct.Struct(">i")

FLAG_TRANSACTIONAL = 1 << 0
FLAG_COMMIT = 1 << 1
FLAG_ABORT = 1 << 2


def _pack_optional_bytes(value: bytes | None) -> tuple[int, bytes]:
    if value is None:
        return -1, b""
    return len(value), value


def encode_batch(batch: RecordBatch) -> bytes:
    if batch.base_offset is None:
        raise InvalidRecord("batch must be assigned before encoding")
    if batch.format_version != FORMAT_VERSION:
        raise InvalidRecord(
            f"unsupported format version {batch.format_version}"
        )

    flags = 0
    if batch.transactional_id is not None:
        flags |= FLAG_TRANSACTIONAL
    if batch.control is ControlType.COMMIT:
        flags |= FLAG_COMMIT
    elif batch.control is ControlType.ABORT:
        flags |= FLAG_ABORT

    transaction_id = (
        b""
        if batch.transactional_id is None
        else batch.transactional_id.encode("utf-8")
    )
    payload = bytearray(
        PAYLOAD_HEADER.pack(
            batch.format_version,
            batch.base_offset,
            batch.leader_epoch,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            flags,
            len(transaction_id),
            len(batch.records),
        )
    )
    payload.extend(transaction_id)
    for record in batch.records:
        key_length, key = _pack_optional_bytes(record.key)
        value_length, value = _pack_optional_bytes(record.value)
        payload.extend(
            RECORD_HEADER.pack(
                record.timestamp_ms,
                key_length,
                value_length,
                len(record.headers),
            )
        )
        payload.extend(key)
        payload.extend(value)
        for header in record.headers:
            header_key = header.key.encode("utf-8")
            if len(header_key) > 0xFFFF:
                raise InvalidRecord("header key is too large")
            header_value_length, header_value = _pack_optional_bytes(
                header.value
            )
            payload.extend(HEADER_KEY_LENGTH.pack(len(header_key)))
            payload.extend(header_key)
            payload.extend(HEADER_VALUE_LENGTH.pack(header_value_length))
            payload.extend(header_value)

    checksum = zlib.crc32(payload) & 0xFFFFFFFF
    return FRAME_HEADER.pack(MAGIC, len(payload), checksum) + payload


@dataclass(slots=True)
class _Reader:
    data: memoryview
    position: int = 0

    def read(self, length: int) -> bytes:
        if length < 0 or self.position + length > len(self.data):
            raise CorruptBatch("batch length exceeds available bytes")
        start = self.position
        self.position += length
        return bytes(self.data[start : self.position])

    def unpack(self, format_: struct.Struct) -> tuple[object, ...]:
        return format_.unpack(self.read(format_.size))

    @property
    def remaining(self) -> int:
        return len(self.data) - self.position


def _read_optional(reader: _Reader, length: int) -> bytes | None:
    if length == -1:
        return None
    if length < -1:
        raise CorruptBatch("negative field length")
    return reader.read(length)


def decode_batch(encoded: bytes, *, max_batch_bytes: int = 16_777_216) -> RecordBatch:
    if len(encoded) < FRAME_HEADER.size:
        raise CorruptBatch("batch length is shorter than frame header")
    magic, payload_length, expected_crc = FRAME_HEADER.unpack_from(encoded)
    if magic != MAGIC:
        raise CorruptBatch("invalid batch magic")
    if payload_length > max_batch_bytes:
        raise CorruptBatch("batch length exceeds configured maximum")
    if len(encoded) != FRAME_HEADER.size + payload_length:
        raise CorruptBatch("batch length does not match frame")
    payload = encoded[FRAME_HEADER.size :]
    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        raise CorruptBatch("batch CRC mismatch")

    reader = _Reader(memoryview(payload))
    (
        version,
        base_offset,
        leader_epoch,
        producer_id,
        producer_epoch,
        base_sequence,
        flags,
        transaction_id_length,
        record_count,
    ) = reader.unpack(PAYLOAD_HEADER)
    if version != FORMAT_VERSION:
        raise CorruptBatch(f"unsupported format version {version}")
    transaction_bytes = reader.read(transaction_id_length)
    try:
        transaction_id = (
            transaction_bytes.decode("utf-8")
            if flags & FLAG_TRANSACTIONAL
            else None
        )
    except UnicodeDecodeError as error:
        raise CorruptBatch("invalid transaction ID encoding") from error
    if transaction_id is None and transaction_bytes:
        raise CorruptBatch("transaction ID present without transactional flag")

    records: list[Record] = []
    for _ in range(record_count):
        timestamp_ms, key_length, value_length, header_count = reader.unpack(
            RECORD_HEADER
        )
        key = _read_optional(reader, key_length)
        value = _read_optional(reader, value_length)
        headers: list[Header] = []
        for _ in range(header_count):
            (header_key_length,) = reader.unpack(HEADER_KEY_LENGTH)
            header_key_bytes = reader.read(header_key_length)
            (header_value_length,) = reader.unpack(HEADER_VALUE_LENGTH)
            header_value = _read_optional(reader, header_value_length)
            try:
                header_key = header_key_bytes.decode("utf-8")
            except UnicodeDecodeError as error:
                raise CorruptBatch("invalid header key encoding") from error
            headers.append(Header(header_key, header_value))
        records.append(Record(key, value, timestamp_ms, tuple(headers)))
    if reader.remaining:
        raise CorruptBatch("batch contains trailing bytes")

    control_bits = flags & (FLAG_COMMIT | FLAG_ABORT)
    if control_bits == (FLAG_COMMIT | FLAG_ABORT):
        raise CorruptBatch("batch has conflicting control flags")
    control = (
        ControlType.COMMIT
        if control_bits == FLAG_COMMIT
        else ControlType.ABORT
        if control_bits == FLAG_ABORT
        else None
    )
    try:
        return RecordBatch(
            records=tuple(records),
            base_offset=base_offset,
            leader_epoch=leader_epoch,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=base_sequence,
            transactional_id=transaction_id,
            control=control,
            format_version=version,
        )
    except InvalidRecord as error:
        raise CorruptBatch(str(error)) from error
