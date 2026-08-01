# Stage 02 · 二进制安全 Record Batch

### 目标

实现二进制安全 Record Batch，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/core/__init__.py`
    - `src/minikafka/core/batch.py`
    - `src/minikafka/core/batch_codec.py`
    - `src/minikafka/core/record.py`
    - `src/minikafka/errors.py`
    - `tests/unit/test_batch_codec.py`

### 当前遇到的问题

Broker 需要一种既能保存任意字节、又能在 Record 进入日志前发现损坏的持久帧。

### 测试契约

#### 先看会坏在哪里

Round-trip 测试包含二进制 Key、Value 与 Header；损坏和截断测试证明 Decoder 必须拒绝看似合理的残缺数据。

??? note "文件差异：tests/unit/test_batch_codec.py"
    ```diff
    diff --git a/tests/unit/test_batch_codec.py b/tests/unit/test_batch_codec.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..bbd23d5e4d25ac6cbcd280238fd369889f3a14dc
    --- /dev/null
    +++ b/tests/unit/test_batch_codec.py
    @@ -0,0 +1,88 @@
    +from __future__ import annotations
    +
    +from dataclasses import replace
    +
    +import pytest
    +
    +from minikafka.core.batch import ControlType, RecordBatch
    +from minikafka.core.batch_codec import decode_batch, encode_batch
    +from minikafka.core.record import Header, Record
    +from minikafka.errors import CorruptBatch, InvalidRecord
    +
    +
    +def sample_batch() -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        records=(
    +            Record(
    +                key=b"k\x00",
    +                value=b"v\xff",
    +                timestamp_ms=7,
    +                headers=(Header("trace", b"\x00\x01"),),
    +            ),
    +            Record(key=b"k2", value=None, timestamp_ms=8),
    +        ),
    +        producer_id=4,
    +        producer_epoch=2,
    +        base_sequence=9,
    +        transactional_id="tx-α",
    +    ).assign(base_offset=12, leader_epoch=3)
    +
    +
    +def test_batch_round_trip_preserves_binary_records() -> None:
    +    batch = sample_batch()
    +
    +    decoded = decode_batch(encode_batch(batch))
    +
    +    assert decoded == batch
    +    assert decoded.next_offset == 14
    +    assert decoded.last_sequence == 10
    +
    +
    +def test_crc_detects_payload_corruption() -> None:
    +    encoded = bytearray(encode_batch(sample_batch()))
    +    encoded[-1] ^= 0x01
    +
    +    with pytest.raises(CorruptBatch, match="CRC"):
    +        decode_batch(bytes(encoded))
    +
    +
    +def test_decoder_rejects_truncated_frame() -> None:
    +    encoded = encode_batch(sample_batch())
    +
    +    with pytest.raises(CorruptBatch, match="length"):
    +        decode_batch(encoded[:-1])
    +
    +
    +def test_unassigned_batch_cannot_be_encoded() -> None:
    +    batch = RecordBatch.unassigned((Record(None, b"x", 1),))
    +
    +    with pytest.raises(InvalidRecord, match="assigned"):
    +        encode_batch(batch)
    +
    +
    +def test_data_batch_must_contain_records() -> None:
    +    with pytest.raises(InvalidRecord, match="at least one"):
    +        RecordBatch.unassigned(())
    +
    +
    +def test_control_batch_round_trip_has_no_user_records() -> None:
    +    batch = RecordBatch.control_marker(
    +        transaction_id="tx-1",
    +        control=ControlType.COMMIT,
    +    ).assign(20, 4)
    +
    +    decoded = decode_batch(encode_batch(batch))
    +
    +    assert decoded.control is ControlType.COMMIT
    +    assert decoded.records == ()
    +    assert decoded.next_offset == 21
    +
    +
    +def test_unknown_format_version_is_rejected() -> None:
    +    encoded = encode_batch(sample_batch())
    +    changed = replace(sample_batch(), format_version=99)
    +
    +    with pytest.raises(InvalidRecord, match="format version"):
    +        encode_batch(changed)
    +
    +    assert encoded.startswith(b"MKB1")
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Round-trip 测试包含二进制 Key、Value 与 Header；损坏和截断测试证明 Decoder 必须拒绝看似合理的残缺数据。

**关键测试语句**

```python
assert decoded == batch
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Record 是用户数据；RecordBatch 是追加与复制单元；Framing 显式记录长度；CRC 校验编码后的 Payload 而非 Python 对象。

### 为什么需要这个机制

Broker 需要一种既能保存任意字节、又能在 Record 进入日志前发现损坏的持久帧。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。

### 机制板块

#### 二进制安全 Record Batch机制

编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。

??? note "文件差异：src/minikafka/core/batch.py"
    ```diff
    diff --git a/src/minikafka/core/batch.py b/src/minikafka/core/batch.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..781df292087fc594bac0d22060e67abdb8a3d6dd
    --- /dev/null
    +++ b/src/minikafka/core/batch.py
    @@ -0,0 +1,94 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass, replace
    +from enum import Enum
    +
    +from minikafka.core.record import Record
    +from minikafka.errors import InvalidRecord
    +
    +
    +class ControlType(str, Enum):
    +    COMMIT = "commit"
    +    ABORT = "abort"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RecordBatch:
    +    records: tuple[Record, ...]
    +    base_offset: int | None = None
    +    leader_epoch: int = -1
    +    producer_id: int = -1
    +    producer_epoch: int = -1
    +    base_sequence: int = -1
    +    transactional_id: str | None = None
    +    control: ControlType | None = None
    +    format_version: int = 1
    +
    +    def __post_init__(self) -> None:
    +        if not self.records and self.control is None:
    +            raise InvalidRecord("data batch must contain at least one record")
    +        if self.control is not None and self.records:
    +            raise InvalidRecord("control batch cannot contain user records")
    +        if self.control is not None and self.transactional_id is None:
    +            raise InvalidRecord("control batch requires a transaction ID")
    +        if self.base_offset is not None and self.base_offset < 0:
    +            raise InvalidRecord("base offset cannot be negative")
    +
    +    @classmethod
    +    def unassigned(
    +        cls,
    +        records: tuple[Record, ...],
    +        *,
    +        producer_id: int = -1,
    +        producer_epoch: int = -1,
    +        base_sequence: int = -1,
    +        transactional_id: str | None = None,
    +    ) -> RecordBatch:
    +        return cls(
    +            records=tuple(records),
    +            producer_id=producer_id,
    +            producer_epoch=producer_epoch,
    +            base_sequence=base_sequence,
    +            transactional_id=transactional_id,
    +        )
    +
    +    @classmethod
    +    def control_marker(
    +        cls, *, transaction_id: str, control: ControlType
    +    ) -> RecordBatch:
    +        return cls(
    +            records=(),
    +            transactional_id=transaction_id,
    +            control=control,
    +        )
    +
    +    def assign(self, base_offset: int, leader_epoch: int) -> RecordBatch:
    +        if self.base_offset is not None:
    +            raise InvalidRecord("batch is already assigned")
    +        return replace(
    +            self,
    +            base_offset=base_offset,
    +            leader_epoch=leader_epoch,
    +        )
    +
    +    @property
    +    def logical_count(self) -> int:
    +        return max(1, len(self.records))
    +
    +    @property
    +    def next_offset(self) -> int:
    +        if self.base_offset is None:
    +            raise InvalidRecord("batch must be assigned")
    +        return self.base_offset + self.logical_count
    +
    +    @property
    +    def last_sequence(self) -> int:
    +        if self.base_sequence < 0:
    +            return -1
    +        return self.base_sequence + self.logical_count - 1
    +
    +    @property
    +    def max_timestamp_ms(self) -> int:
    +        if not self.records:
    +            return -1
    +        return max(record.timestamp_ms for record in self.records)
    ```

??? note "文件差异：src/minikafka/core/batch_codec.py"
    ```diff
    diff --git a/src/minikafka/core/batch_codec.py b/src/minikafka/core/batch_codec.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..31f84355ba9c039f48aa1a557c716a91b771f483
    --- /dev/null
    +++ b/src/minikafka/core/batch_codec.py
    @@ -0,0 +1,208 @@
    +from __future__ import annotations
    +
    +import struct
    +import zlib
    +from dataclasses import dataclass
    +
    +from minikafka.core.batch import ControlType, RecordBatch
    +from minikafka.core.record import Header, Record
    +from minikafka.errors import CorruptBatch, InvalidRecord
    +
    +MAGIC = b"MKB1"
    +FORMAT_VERSION = 1
    +FRAME_HEADER = struct.Struct(">4sII")
    +PAYLOAD_HEADER = struct.Struct(">BqIqiiBHI")
    +RECORD_HEADER = struct.Struct(">qiiH")
    +HEADER_KEY_LENGTH = struct.Struct(">H")
    +HEADER_VALUE_LENGTH = struct.Struct(">i")
    +
    +FLAG_TRANSACTIONAL = 1 << 0
    +FLAG_COMMIT = 1 << 1
    +FLAG_ABORT = 1 << 2
    +
    +
    +def _pack_optional_bytes(value: bytes | None) -> tuple[int, bytes]:
    +    if value is None:
    +        return -1, b""
    +    return len(value), value
    +
    +
    +def encode_batch(batch: RecordBatch) -> bytes:
    +    if batch.base_offset is None:
    +        raise InvalidRecord("batch must be assigned before encoding")
    +    if batch.format_version != FORMAT_VERSION:
    +        raise InvalidRecord(
    +            f"unsupported format version {batch.format_version}"
    +        )
    +
    +    flags = 0
    +    if batch.transactional_id is not None:
    +        flags |= FLAG_TRANSACTIONAL
    +    if batch.control is ControlType.COMMIT:
    +        flags |= FLAG_COMMIT
    +    elif batch.control is ControlType.ABORT:
    +        flags |= FLAG_ABORT
    +
    +    transaction_id = (
    +        b""
    +        if batch.transactional_id is None
    +        else batch.transactional_id.encode("utf-8")
    +    )
    +    payload = bytearray(
    +        PAYLOAD_HEADER.pack(
    +            batch.format_version,
    +            batch.base_offset,
    +            batch.leader_epoch,
    +            batch.producer_id,
    +            batch.producer_epoch,
    +            batch.base_sequence,
    +            flags,
    +            len(transaction_id),
    +            len(batch.records),
    +        )
    +    )
    +    payload.extend(transaction_id)
    +    for record in batch.records:
    +        key_length, key = _pack_optional_bytes(record.key)
    +        value_length, value = _pack_optional_bytes(record.value)
    +        payload.extend(
    +            RECORD_HEADER.pack(
    +                record.timestamp_ms,
    +                key_length,
    +                value_length,
    +                len(record.headers),
    +            )
    +        )
    +        payload.extend(key)
    +        payload.extend(value)
    +        for header in record.headers:
    +            header_key = header.key.encode("utf-8")
    +            if len(header_key) > 0xFFFF:
    +                raise InvalidRecord("header key is too large")
    +            header_value_length, header_value = _pack_optional_bytes(
    +                header.value
    +            )
    +            payload.extend(HEADER_KEY_LENGTH.pack(len(header_key)))
    +            payload.extend(header_key)
    +            payload.extend(HEADER_VALUE_LENGTH.pack(header_value_length))
    +            payload.extend(header_value)
    +
    +    checksum = zlib.crc32(payload) & 0xFFFFFFFF
    +    return FRAME_HEADER.pack(MAGIC, len(payload), checksum) + payload
    +
    +
    +@dataclass(slots=True)
    +class _Reader:
    +    data: memoryview
    +    position: int = 0
    +
    +    def read(self, length: int) -> bytes:
    +        if length < 0 or self.position + length > len(self.data):
    +            raise CorruptBatch("batch length exceeds available bytes")
    +        start = self.position
    +        self.position += length
    +        return bytes(self.data[start : self.position])
    +
    +    def unpack(self, format_: struct.Struct) -> tuple[object, ...]:
    +        return format_.unpack(self.read(format_.size))
    +
    +    @property
    +    def remaining(self) -> int:
    +        return len(self.data) - self.position
    +
    +
    +def _read_optional(reader: _Reader, length: int) -> bytes | None:
    +    if length == -1:
    +        return None
    +    if length < -1:
    +        raise CorruptBatch("negative field length")
    +    return reader.read(length)
    +
    +
    +def decode_batch(encoded: bytes, *, max_batch_bytes: int = 16_777_216) -> RecordBatch:
    +    if len(encoded) < FRAME_HEADER.size:
    +        raise CorruptBatch("batch length is shorter than frame header")
    +    magic, payload_length, expected_crc = FRAME_HEADER.unpack_from(encoded)
    +    if magic != MAGIC:
    +        raise CorruptBatch("invalid batch magic")
    +    if payload_length > max_batch_bytes:
    +        raise CorruptBatch("batch length exceeds configured maximum")
    +    if len(encoded) != FRAME_HEADER.size + payload_length:
    +        raise CorruptBatch("batch length does not match frame")
    +    payload = encoded[FRAME_HEADER.size :]
    +    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
    +    if actual_crc != expected_crc:
    +        raise CorruptBatch("batch CRC mismatch")
    +
    +    reader = _Reader(memoryview(payload))
    +    (
    +        version,
    +        base_offset,
    +        leader_epoch,
    +        producer_id,
    +        producer_epoch,
    +        base_sequence,
    +        flags,
    +        transaction_id_length,
    +        record_count,
    +    ) = reader.unpack(PAYLOAD_HEADER)
    +    if version != FORMAT_VERSION:
    +        raise CorruptBatch(f"unsupported format version {version}")
    +    transaction_bytes = reader.read(transaction_id_length)
    +    try:
    +        transaction_id = (
    +            transaction_bytes.decode("utf-8")
    +            if flags & FLAG_TRANSACTIONAL
    +            else None
    +        )
    +    except UnicodeDecodeError as error:
    +        raise CorruptBatch("invalid transaction ID encoding") from error
    +    if transaction_id is None and transaction_bytes:
    +        raise CorruptBatch("transaction ID present without transactional flag")
    +
    +    records: list[Record] = []
    +    for _ in range(record_count):
    +        timestamp_ms, key_length, value_length, header_count = reader.unpack(
    +            RECORD_HEADER
    +        )
    +        key = _read_optional(reader, key_length)
    +        value = _read_optional(reader, value_length)
    +        headers: list[Header] = []
    +        for _ in range(header_count):
    +            (header_key_length,) = reader.unpack(HEADER_KEY_LENGTH)
    +            header_key_bytes = reader.read(header_key_length)
    +            (header_value_length,) = reader.unpack(HEADER_VALUE_LENGTH)
    +            header_value = _read_optional(reader, header_value_length)
    +            try:
    +                header_key = header_key_bytes.decode("utf-8")
    +            except UnicodeDecodeError as error:
    +                raise CorruptBatch("invalid header key encoding") from error
    +            headers.append(Header(header_key, header_value))
    +        records.append(Record(key, value, timestamp_ms, tuple(headers)))
    +    if reader.remaining:
    +        raise CorruptBatch("batch contains trailing bytes")
    +
    +    control_bits = flags & (FLAG_COMMIT | FLAG_ABORT)
    +    if control_bits == (FLAG_COMMIT | FLAG_ABORT):
    +        raise CorruptBatch("batch has conflicting control flags")
    +    control = (
    +        ControlType.COMMIT
    +        if control_bits == FLAG_COMMIT
    +        else ControlType.ABORT
    +        if control_bits == FLAG_ABORT
    +        else None
    +    )
    +    try:
    +        return RecordBatch(
    +            records=tuple(records),
    +            base_offset=base_offset,
    +            leader_epoch=leader_epoch,
    +            producer_id=producer_id,
    +            producer_epoch=producer_epoch,
    +            base_sequence=base_sequence,
    +            transactional_id=transaction_id,
    +            control=control,
    +            format_version=version,
    +        )
    +    except InvalidRecord as error:
    +        raise CorruptBatch(str(error)) from error
    ```

??? note "文件差异：src/minikafka/core/record.py"
    ```diff
    diff --git a/src/minikafka/core/record.py b/src/minikafka/core/record.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..89c05c89222a0ecc78739226f169b2b6e7783db1
    --- /dev/null
    +++ b/src/minikafka/core/record.py
    @@ -0,0 +1,28 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Header:
    +    key: str
    +    value: bytes | None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Record:
    +    key: bytes | None
    +    value: bytes | None
    +    timestamp_ms: int
    +    headers: tuple[Header, ...] = ()
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class StoredRecord:
    +    topic: str
    +    partition: int
    +    offset: int
    +    key: bytes | None
    +    value: bytes | None
    +    timestamp_ms: int
    +    headers: tuple[Header, ...] = ()
    ```

??? note "文件差异：src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index b070c1827c7bb9fbd283e12f21ff8525531d52d2..0f6804c028768cd9afa88d49f1b4d40f8f5985e7 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -15,3 +15,11 @@ class OffsetOutOfRange(MiniKafkaError):
             self.requested = requested
             self.start = start
             self.end = end
    +
    +
    +class InvalidRecord(MiniKafkaError):
    +    code = "INVALID_RECORD"
    +
    +
    +class CorruptBatch(MiniKafkaError):
    +    code = "CORRUPT_BATCH"
    ```

**是什么，为什么现在需要**

Record 是用户数据；RecordBatch 是追加与复制单元；Framing 显式记录长度；CRC 校验编码后的 Payload 而非 Python 对象。

**在运行时做什么**

编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。

**关键语句理解**

长度检查先建立安全读取边界；之后 CRC 与语义字段才值得信任。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/core/__init__.py`**

    ```diff
    diff --git a/src/minikafka/core/__init__.py b/src/minikafka/core/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..ac8d68a0a89d2529ed0536c7556b90fe5a4e1e4f
    --- /dev/null
    +++ b/src/minikafka/core/__init__.py
    @@ -0,0 +1 @@
    +"""Core MiniKafka domain values."""
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-record-batches/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

长度检查先建立安全读取边界；之后 CRC 与语义字段才值得信任。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/02-record-batches/stage.patch)
